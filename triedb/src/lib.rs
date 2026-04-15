//! Trie database library for Ethereum state management.

// Note: Global allocator is configured by the parent project (reth-bsc)
// This crate supports jemalloc feature for dependency resolution but doesn't define global allocator

pub mod triedb;
pub mod triedb_basic;
pub mod triedb_manager;
pub mod triedb_metrics;
pub mod triedb_disk;
pub mod triedb_reth;

#[cfg(test)]
mod triedb_test;

// Re-export main types
pub use triedb::TrieDB;
pub use triedb::TrieDBError;
pub use triedb_reth::{TrieDBHashedPostState, TrieDBPrefetchState};
pub use triedb_manager::{
    init_global_triedb_manager, get_global_triedb, disable_triedb,
    layer_tree_insert, layer_tree_collect_ancestors, layer_tree_len,
    set_cached_account_trie_root, get_cached_account_trie_root,
};
