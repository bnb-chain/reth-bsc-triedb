//! Secure state trie implementation for reth
//!
//! This crate provides a BSC-style secure trie implementation that wraps a trie with key hashing.
//! In a secure trie, all access operations hash the key using keccak256 to prevent calling code
//! from creating long chains of nodes that increase access time.

// Note: Global allocator is configured in the main triedb crate to avoid conflicts
// This crate still supports jemalloc feature for dependency resolution

/// Key encoding utilities for trie operations
pub mod encoding;
/// Node structures for trie implementation
pub mod node;
/// Core trie implementation
pub mod trie;
/// Traits for secure trie operations
pub mod traits;
/// Account structure and implementation
pub mod account;
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

pub use state_trie::StateTrie;
pub use account::StateAccount;
pub use traits::SecureTrieTrait;
pub use node::NodeSet;
// Re-export TrieNode, DiffLayer, DiffLayers from common crate
pub use secure_trie::{SecureTrieId, SecureTrieBuilder, SecureTrieError};
pub use rust_eth_triedb_common::{TrieNode, DiffLayer, DiffLayers};
