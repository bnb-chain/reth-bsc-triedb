//! TrieDB Manager for managing global TrieDB instances
//!
//! This module provides a singleton manager for TrieDB instances,
//! allowing global access to a shared TrieDB across the application.

use std::sync::{Mutex, OnceLock, Arc};
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig};
use super::TrieDB;
use rust_eth_triedb_state_trie::node::{init_empty_root_node, Node};
use alloy_primitives::B256;
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

/// Cached account trie root nodes for cross-block reuse.
///
/// Stores up to 2 entries (parent_root + new_root) so that:
/// - Same-parent repeated miner builds hit the parent_root cache
/// - Next block's first build hits the new_root cache
///
/// Uses Arc<Node> which is a cheap ref-count clone. The actual resolved
/// trie nodes stay alive in memory as long as any Arc reference exists.
static CACHED_ACCOUNT_TRIE_ROOTS: OnceLock<Mutex<Vec<(B256, Arc<Node>)>>> = OnceLock::new();

const MAX_CACHED_ROOTS: usize = 2;

fn cached_roots_lock() -> &'static Mutex<Vec<(B256, Arc<Node>)>> {
    CACHED_ACCOUNT_TRIE_ROOTS.get_or_init(|| Mutex::new(Vec::with_capacity(MAX_CACHED_ROOTS)))
}

/// Store a pre-resolved account trie root node keyed by root hash.
/// Overwrites an existing entry with the same root_hash; evicts oldest if full.
pub fn set_cached_account_trie_root(root_hash: B256, root_node: Arc<Node>) {
    let mut guard = cached_roots_lock().lock().unwrap();
    // Update in place if same root already cached (refresh the node).
    if let Some(entry) = guard.iter_mut().find(|(h, _)| *h == root_hash) {
        entry.1 = root_node;
        return;
    }
    // Evict oldest if at capacity.
    if guard.len() >= MAX_CACHED_ROOTS {
        guard.remove(0);
    }
    guard.push((root_hash, root_node));
}

/// Clone the cached root node for the given root hash (non-destructive).
/// Returns None if not cached.
pub fn take_cached_account_trie_root(root_hash: B256) -> Option<Arc<Node>> {
    let guard = cached_roots_lock().lock().unwrap();
    guard.iter()
        .find(|(h, _)| *h == root_hash)
        .map(|(_, node)| Arc::clone(node))
}

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

