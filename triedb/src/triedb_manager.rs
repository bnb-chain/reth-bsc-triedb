//! TrieDB Manager for managing global TrieDB instances
//!
//! This module provides a singleton manager for TrieDB instances,
//! allowing global access to a shared TrieDB across the application.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use alloy_primitives::B256;
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig};
use super::TrieDB;
use rust_eth_triedb_state_trie::node::init_empty_root_node;
use rust_eth_triedb_common::{DiffLayer, TrieNode};
use rust_eth_triedb_state_trie::node::DiffLayers;
use tracing::info;

// Global singleton for active_triedb flag - can only be initialized once
static ACTIVE_TRIEDB: OnceLock<bool> = OnceLock::new();

// Enable the active_triedb flag
pub fn enable_triedb() {
    if let Some(&current_value) = ACTIVE_TRIEDB.get() {
        if !current_value {
            panic!("TrieDB is already disabled. Cannot enable it after it has been disabled.");
        }
        // Already enabled, nothing to do
        return;
    }
    ACTIVE_TRIEDB.get_or_init(|| true);
}

// Disable the active_triedb flag
pub fn disable_triedb() {
    if let Some(&current_value) = ACTIVE_TRIEDB.get() {
        if current_value {
            panic!("TrieDB is already enabled. Cannot disable it after it has been enabled.");
        }
        // Already disabled, nothing to do
        return;
    }
    ACTIVE_TRIEDB.get_or_init(|| false);
}

// Check if the active_triedb flag is enabled
pub fn is_triedb_active() -> bool {
    ACTIVE_TRIEDB.get().map_or(false, |&b| b)
}

// ---------------------------------------------------------------------------
// Layer Tree — state-root-indexed DiffLayer chain (like geth's 128-layer tree)
// ---------------------------------------------------------------------------

const LAYER_TREE_MAX_ENTRIES: usize = 256;

struct LayerEntry {
    parent_root: B256,
    diff_layer: Arc<DiffLayer>,
}

struct LayerTree {
    layers: HashMap<B256, LayerEntry>,
    insertion_order: VecDeque<B256>,
    max_entries: usize,
    /// Cached flattened DiffLayer for a given start_root + generation.
    /// Avoids re-flattening when the same parent_root is queried multiple
    /// times within a single block (e.g., multiple miner candidates).
    flat_cache: Option<(B256, u64, Arc<DiffLayer>)>,
    /// Monotonically increasing counter, bumped on every insert/eviction
    /// so that stale flat_cache entries are invalidated.
    generation: u64,
}

impl LayerTree {
    fn new(max_entries: usize) -> Self {
        Self {
            layers: HashMap::with_capacity(max_entries),
            insertion_order: VecDeque::with_capacity(max_entries),
            max_entries,
            flat_cache: None,
            generation: 0,
        }
    }

    fn insert(&mut self, state_root: B256, parent_root: B256, diff_layer: Arc<DiffLayer>) {
        if self.layers.contains_key(&state_root) {
            return;
        }
        while self.layers.len() >= self.max_entries {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.layers.remove(&oldest);
            }
        }
        self.layers.insert(state_root, LayerEntry { parent_root, diff_layer });
        self.insertion_order.push_back(state_root);
        self.generation += 1;
    }

    /// Walk the parent chain from `start_root` and return a **single flat
    /// DiffLayer** that merges all ancestors.  The result is cached so that
    /// repeated calls with the same `start_root` within the same generation
    /// (i.e. before any new insert/eviction) are O(1).
    fn collect_ancestors(&mut self, start_root: B256) -> DiffLayers {
        // Fast path: cache hit
        if let Some((cached_root, cached_gen, ref cached_flat)) = self.flat_cache {
            if cached_root == start_root && cached_gen == self.generation {
                return DiffLayers { diff_layers: vec![cached_flat.clone()] };
            }
        }

        // Walk parent chain, flattening into a single HashMap.
        // Walk order is newest → oldest.  We use `entry().or_insert_with()`
        // so the first (= newest) value for each key wins.
        let mut flat_nodes: HashMap<Vec<u8>, Arc<TrieNode>> = HashMap::new();
        let mut flat_roots: HashMap<B256, B256> = HashMap::new();
        let mut current = start_root;
        let mut depth = 0u32;

        for _ in 0..self.max_entries {
            match self.layers.get(&current) {
                Some(entry) => {
                    for (k, v) in entry.diff_layer.diff_nodes.iter() {
                        flat_nodes.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                    for (k, v) in entry.diff_layer.diff_storage_roots.iter() {
                        flat_roots.entry(*k).or_insert(*v);
                    }
                    current = entry.parent_root;
                    depth += 1;
                }
                None => break,
            }
        }

        if flat_nodes.is_empty() && flat_roots.is_empty() {
            self.flat_cache = None;
            return DiffLayers::default();
        }

        let flat = Arc::new(DiffLayer::new(
            Arc::new(flat_nodes),
            Arc::new(flat_roots),
        ));
        self.flat_cache = Some((start_root, self.generation, flat.clone()));

        tracing::debug!(
            target: "triedb::timing",
            depth,
            flat_nodes = flat.diff_nodes.len(),
            flat_storage_roots = flat.diff_storage_roots.len(),
            "collect_ancestors flattened"
        );

        DiffLayers { diff_layers: vec![flat] }
    }

    fn len(&self) -> usize {
        self.layers.len()
    }
}

static LAYER_TREE: OnceLock<Mutex<LayerTree>> = OnceLock::new();

fn get_layer_tree() -> &'static Mutex<LayerTree> {
    LAYER_TREE.get_or_init(|| Mutex::new(LayerTree::new(LAYER_TREE_MAX_ENTRIES)))
}

/// Insert a committed DiffLayer into the global Layer Tree.
pub fn layer_tree_insert(state_root: B256, parent_root: B256, diff_layer: Arc<DiffLayer>) {
    get_layer_tree().lock().unwrap().insert(state_root, parent_root, diff_layer);
}

/// Collect ancestor DiffLayers by walking the parent chain from `start_root`.
pub fn layer_tree_collect_ancestors(start_root: B256) -> DiffLayers {
    get_layer_tree().lock().unwrap().collect_ancestors(start_root)
}

/// Current number of entries in the Layer Tree.
pub fn layer_tree_len() -> usize {
    get_layer_tree().lock().unwrap().len()
}

// ---------------------------------------------------------------------------
// Global TrieDB Manager
// ---------------------------------------------------------------------------

/// Global TrieDB Manager
///
/// A singleton manager that maintains a single TrieDB instance
/// accessible throughout the application lifecycle.
pub struct TrieDBManager {
    triedb: TrieDB<PathDB>,
}

// Global singleton instance - automatically initialized on first access
static MANAGER_INSTANCE: OnceLock<TrieDBManager> = OnceLock::new();

/// Initialize the global manager instance.
/// 
/// This function must be called once at application startup before any calls to `get_global_triedb()`.
/// The `path` parameter specifies the database path for the TrieDB instance.
/// 
/// # Behavior
/// - On the first call, initializes the manager with the provided path.
/// - On subsequent calls, the path parameter is ignored and the existing instance is returned.
/// 
/// # Arguments
/// * `path` - Path to the database directory
/// 
/// # ⚠️ Important: Single Initialization Pattern
/// # Panics
/// This function will panic if `init_global_manager()` has been called twice.
pub fn init_global_triedb_manager(path: &str) {
    // Panic if already initialized
    if MANAGER_INSTANCE.get().is_some() {
        panic!("TrieDB has already been initialized. It can only be initialized once.");
    }
    
    init_empty_root_node();
    MANAGER_INSTANCE.get_or_init(|| {
        let path_str = path.to_string();
        TrieDBManager::new(&path_str)
    });
    info!(target: "reth::cli", "TrieDB initialized with path: {path}");
    enable_triedb();
}

// Get the initialized manager instance
fn get_manager() -> &'static TrieDBManager {
    MANAGER_INSTANCE.get()
        .expect("Global TrieDB manager not initialized. Call init_global_manager() first.")
}

/// Get the global TrieDB instance.
/// 
/// This function returns a clone of the global TrieDB instance.
/// The global manager must be initialized first by calling `init_global_manager()`.
/// 
/// # Panics
/// 
/// This function will panic if `init_global_manager()` has not been called first.
pub fn get_global_triedb() -> TrieDB<PathDB> {
    get_manager().get_triedb()
}

impl TrieDBManager {
    /// Create a new TrieDBManager with the given database path
    /// 
    /// # Arguments
    /// * `path` - Path to the database directory
    fn new(path: &str) -> Self {
        let pathdb = PathDB::new(path, PathProviderConfig::default())
            .expect("Failed to create PathDB");

        let triedb = TrieDB::new(pathdb);
        Self {
            triedb,
        }
    }

    /// Get a reference to the managed TrieDB instance
    pub fn get_triedb(&self) -> TrieDB<PathDB> {
        self.triedb.clone()
    }
}

