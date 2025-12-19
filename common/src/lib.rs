//! Common traits and types for reth trie database implementations.
//!
//! This crate provides common interfaces and types that are shared across
//! different trie database implementations.

/// Database traits for trie operations.
mod traits;
pub use traits::TrieDatabase;

/// DiffLayer types for tracking trie node changes.
mod difflayer;
pub use difflayer::{Leaf, TrieNode, DiffLayer, DiffLayers, TRIE_STATE_ROOT_KEY, TRIE_STATE_BLOCK_NUMBER_KEY};

/// Key encoding utilities for trie operations
pub mod encoding;

/// State account structure
pub mod account;
pub use account::StateAccount;

/// Node structures for trie implementation
pub mod node;
