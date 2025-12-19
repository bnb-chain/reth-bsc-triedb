//! Secure state trie implementation for reth
//!
//! This crate provides a BSC-style secure trie implementation that wraps a trie with key hashing.
//! In a secure trie, all access operations hash the key using keccak256 to prevent calling code
//! from creating long chains of nodes that increase access time.

// Note: Global allocator is configured in the main triedb crate to avoid conflicts
// This crate still supports jemalloc feature for dependency resolution

/// Core trie implementation
pub mod trie;
/// Traits for secure trie operations
pub mod traits;
/// Secure trie identifier and builder
pub mod secure_trie;
/// State trie implementation
pub mod state_trie;
/// Trie hasher
pub mod trie_hasher;
/// Trie change tracer (Geth-compatible semantics)
pub mod trie_tracer;
/// Trie committer (collects dirty nodes during commit)
pub mod trie_committer;

#[cfg(test)]
mod trie_test;

// Re-export from common crate
pub use rust_eth_triedb_common::{
    StateAccount,
    encoding,
    node::{NodeSet, Node, FullNode, ShortNode, HashNode, ValueNode, NodeFlag, init_empty_root_node, get_empty_root_node},
    TrieNode, DiffLayer, DiffLayers,
};

pub use state_trie::StateTrie;
pub use traits::SecureTrieTrait;
// Re-export TrieNode, DiffLayer, DiffLayers from common crate
pub use secure_trie::{SecureTrieId, SecureTrieBuilder, SecureTrieError};
