//! DiffLayer types for tracking trie node changes.
//!
//! This module provides types for representing trie nodes and diff layers
//! used in tracking modifications during trie operations.

use std::sync::Arc;
use std::collections::HashMap;
use alloy_primitives::B256;

// Trie state storage keys
pub const TRIE_STATE_ROOT_KEY: &[u8] = b"state_root";
pub const TRIE_STATE_BLOCK_NUMBER_KEY: &[u8] = b"block_number";

/// Represents a trie node with its hash and encoded data
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrieNode {
    /// Node hash, empty for deleted node
    pub hash: Option<B256>,
    /// Encoded node data, empty for deleted node
    pub blob: Option<Vec<u8>>,
}

impl TrieNode {
    /// Creates a new trie node
    pub fn new(hash: Option<B256>, blob: Option<Vec<u8>>) -> Self {
        Self { hash, blob }
    }

    /// Returns true if this node is marked as deleted
    pub fn is_deleted(&self) -> bool {
        self.blob.is_none() || (self.blob.is_some() && self.blob.as_ref().unwrap().is_empty())
    }

    /// Returns the total memory size used by this node
    pub fn size(&self) -> usize {
        if self.is_deleted() {
            return 0;
        }
        self.blob.as_ref().unwrap().len() + 32 // 32 bytes for hash
    }
}

/// Represents a leaf node with its blob and parent hash
#[derive(Debug, Clone)]
pub struct Leaf {
    /// Raw blob of leaf
    #[allow(dead_code)]
    pub blob: Vec<u8>,
    /// Hash of parent node
    #[allow(dead_code)]
    pub parent: B256,
}


/// DiffLayer is a collection of updated trie nodes and storage roots for a special block
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffLayer {
    /// A map of trie node path prefixes to their corresponding trie nodes.
    pub diff_nodes: Arc<HashMap<Vec<u8>, Arc<TrieNode>>>,

    /// A map of account address hashes to their corresponding storage trie roots.
    pub diff_storage_roots: Arc<HashMap<B256, B256>>,
}

impl DiffLayer {
    /// Create a new diff layer
    pub fn new(diff_nodes: Arc<HashMap<Vec<u8>, Arc<TrieNode>>>, diff_storage_roots: Arc<HashMap<B256, B256>>) -> Self {
        Self { diff_nodes, diff_storage_roots }
    }

    /// Get a trie node by prefix
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        self.diff_nodes.get(prefix).cloned()
    }

    /// Get a storage root by hased address
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        self.diff_storage_roots.get(&hased_address).copied()
    }

    /// Returns true if the diff layer is empty
    pub fn is_empty(&self) -> bool {
        self.diff_nodes.is_empty() && self.diff_storage_roots.is_empty()
    }

    pub fn debug_diff_storage_roots(&self) -> String {
        let mut diff_storage_roots_str = String::new();
        for (hased_address, root) in self.diff_storage_roots.iter() {
            diff_storage_roots_str.push_str(&format!("hased_address: {}, root: {}\n", hased_address, root));
        }
        diff_storage_roots_str
    }
}

/// A collection of diff layers for uncommitted blocks in the trie state.
///
/// `DiffLayers` maintains a stack of `DiffLayer` instances, where each layer
/// represents the state changes for a specific block. Lookups scan from front
/// to back (newest first), so the latest state takes precedence.
///
/// Uses `&[u8]` parameters to avoid per-call Vec allocations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffLayers {
    pub diff_layers: Vec<Arc<DiffLayer>>,
}

impl DiffLayers {
    /// Insert a diff layer into the collection.
    pub fn insert_difflayer(&mut self, difflayer: Arc<DiffLayer>) {
        self.diff_layers.push(difflayer);
    }

    /// Get a trie node by prefix — scans layers newest-first, O(layers) per call.
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        for difflayer in &self.diff_layers {
            if let Some(node) = difflayer.get_trie_nodes(prefix) {
                return Some(node);
            }
        }
        None
    }

    /// Get a storage root by hashed address — scans layers newest-first.
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        for difflayer in &self.diff_layers {
            if let Some(root) = difflayer.get_storage_root(hased_address) {
                return Some(root);
            }
        }
        None
    }

    /// Returns true if the diff layers are empty.
    pub fn is_empty(&self) -> bool {
        self.diff_layers.is_empty()
    }
}
