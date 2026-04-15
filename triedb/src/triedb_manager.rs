//! TrieDB Manager for managing global TrieDB instances
//!
//! This module provides a singleton manager for TrieDB instances,
//! allowing global access to a shared TrieDB across the application.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use alloy_primitives::B256;
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig};
use super::TrieDB;
use rust_eth_triedb_state_trie::node::{init_empty_root_node, Node, DiffLayers};
use rust_eth_triedb_common::DiffLayer;
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
}

impl LayerTree {
    fn new(max_entries: usize) -> Self {
        Self {
            layers: HashMap::with_capacity(max_entries),
            insertion_order: VecDeque::with_capacity(max_entries),
            max_entries,
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
    }

    fn collect_ancestors(&self, start_root: B256) -> DiffLayers {
        let mut result = DiffLayers::default();
        let mut current = start_root;
        for _ in 0..self.max_entries {
            match self.layers.get(&current) {
                Some(entry) => {
                    result.insert_difflayer(entry.diff_layer.clone());
                    current = entry.parent_root;
                }
                None => break,
            }
        }
        result
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
// Cached account trie root node (2-slot, keyed by state root)
// ---------------------------------------------------------------------------

static CACHED_ROOTS: OnceLock<Mutex<Vec<(B256, Arc<Node>)>>> = OnceLock::new();

fn cached_roots_lock() -> &'static Mutex<Vec<(B256, Arc<Node>)>> {
    CACHED_ROOTS.get_or_init(|| Mutex::new(Vec::with_capacity(2)))
}

/// Store a pre-resolved account trie root node keyed by root hash.
pub fn set_cached_account_trie_root(root_hash: B256, root_node: Arc<Node>) {
    let mut guard = cached_roots_lock().lock().unwrap();
    if let Some(entry) = guard.iter_mut().find(|(h, _)| *h == root_hash) {
        entry.1 = root_node;
        return;
    }
    if guard.len() >= 2 {
        guard.remove(0);
    }
    guard.push((root_hash, root_node));
}

/// Clone the cached root node for the given root hash (non-destructive).
pub fn get_cached_account_trie_root(root_hash: B256) -> Option<Arc<Node>> {
    let guard = cached_roots_lock().lock().unwrap();
    guard.iter()
        .find(|(h, _)| *h == root_hash)
        .map(|(_, node)| Arc::clone(node))
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

