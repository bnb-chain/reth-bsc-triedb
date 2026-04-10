//! TrieDB Manager for managing global TrieDB instances
//!
//! This module provides a singleton manager for TrieDB instances,
//! allowing global access to a shared TrieDB across the application.

use std::sync::{OnceLock};
use rust_eth_triedb_pathdb::{PathDB, PathProviderConfig};
// use rust_eth_triedb_snapshotdb::{SnapshotDB, PathProviderConfig as SnapshotPathProviderConfig};
use super::TrieDB;
use rust_eth_triedb_state_trie::node::init_empty_root_node;
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

