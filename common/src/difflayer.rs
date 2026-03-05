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
    ///
    /// The key is the path prefix (as a byte vector) that uniquely identifies
    /// the location of the node in the trie structure. The value is an `Arc<TrieNode>`
    /// containing the node's hash and encoded data.
    ///
    /// This map tracks all trie nodes that have been modified, inserted, or deleted
    /// in the current block. Nodes marked as deleted will have `None` for both
    /// hash and blob fields in the `TrieNode`.
    ///
    /// # Example
    /// ```
    /// // A path prefix might represent: [0x01, 0x23, 0x45] for a node at depth 3
    /// ```
    pub diff_nodes: Arc<HashMap<Vec<u8>, Arc<TrieNode>>>,
    
    /// A map of account address hashes to their corresponding storage trie roots.
    ///
    /// The key is the Keccak-256 hash of an account address (`B256`), and the value
    /// is the root hash of that account's storage trie (`B256`).
    ///
    /// This map tracks all storage trie roots that have been modified in the current
    /// block. When an account's storage is updated, its storage root changes, and
    /// this change is recorded here.
    ///
    /// # Note
    /// Only accounts whose storage has been modified in this block will have entries
    /// in this map. Unmodified accounts are not included.
    pub diff_storage_roots: Arc<HashMap<B256, B256>>,
}

impl DiffLayer {
    /// Create a new diff layer
    pub fn new(diff_nodes: Arc<HashMap<Vec<u8>, Arc<TrieNode>>>, diff_storage_roots: Arc<HashMap<B256, B256>>) -> Self {
        Self { diff_nodes: diff_nodes.clone(), diff_storage_roots: diff_storage_roots.clone() }
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
/// All inserted layers are merged into flat `Arc<HashMap>`s so that lookups
/// are O(1) and cloning is O(1) (just Arc ref-count bumps).  `Arc::make_mut`
/// gives COW semantics: the inner maps are only deep-copied when a mutating
/// `insert_difflayer` is called while other clones still exist.
///
/// **Insertion order contract**: callers must insert layers newest-first.
/// The first write for a given key is kept (`or_insert`), so the newest
/// layer's value wins — matching the old linear-scan semantics.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct DiffLayers {
    /// Flattened view of all diff_nodes across layers (newest wins).
    merged_nodes: Arc<HashMap<Vec<u8>, Arc<TrieNode>>>,
    /// Flattened view of all diff_storage_roots across layers (newest wins).
    merged_storage_roots: Arc<HashMap<B256, B256>>,
    /// Number of layers that have been merged.
    len: usize,
}

impl DiffLayers {
    /// Insert a diff layer and incrementally merge it into the flat maps.
    ///
    /// Callers insert newest-first, so we use `or_insert` to keep the first
    /// (= newest) write for each key, matching the old linear-scan semantics.
    ///
    /// Uses `Arc::make_mut` for COW: if this is the only live reference the
    /// maps are mutated in-place; otherwise a single deep-copy is made first.
    pub fn insert_difflayer(&mut self, difflayer: Arc<DiffLayer>) {
        let nodes = Arc::make_mut(&mut self.merged_nodes);
        for (k, v) in difflayer.diff_nodes.iter() {
            nodes.entry(k.clone()).or_insert_with(|| v.clone());
        }
        let roots = Arc::make_mut(&mut self.merged_storage_roots);
        for (k, v) in difflayer.diff_storage_roots.iter() {
            roots.entry(*k).or_insert(*v);
        }
        self.len += 1;
    }

    /// Get a trie node by prefix — single HashMap probe.
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        self.merged_nodes.get(prefix).cloned()
    }

    /// Get a storage root by hashed address — single HashMap probe.
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        self.merged_storage_roots.get(&hased_address).copied()
    }

    /// Returns true if no layers have been merged.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

