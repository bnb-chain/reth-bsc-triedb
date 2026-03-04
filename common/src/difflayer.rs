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
/// `DiffLayers` maintains a stack of `DiffLayer` instances, where each layer
/// represents the state changes for a specific block. This structure is used
/// to track incremental modifications to the trie before they are committed
/// to persistent storage.
///
/// The layers are ordered chronologically, with the most recent block's
/// diff layer at the front of the vector (index 0). When querying for nodes
/// or storage roots, the search proceeds from the front (most recent) to the
/// back (oldest), ensuring that the latest state takes precedence over older layers.
///
/// # Usage
///
/// This structure is typically used during block processing to accumulate
/// state changes across multiple blocks before committing them to disk.
/// Each block adds a new `DiffLayer` to the front of the collection, and when
/// blocks are finalized, the layers can be merged and persisted.
///
/// # Thread Safety
///
/// The use of `Arc<DiffLayer>` allows for efficient sharing of diff layers
/// across multiple readers without cloning the entire layer data. However,
/// this structure itself is not thread-safe and should be protected by
/// appropriate synchronization primitives if used in concurrent contexts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffLayers {
    /// An ordered collection of diff layers, one per uncommitted block.
    ///
    /// The vector maintains diff layers in reverse chronological order, with the
    /// most recent block's diff layer at index 0 and the oldest block's
    /// diff layer at the end of the vector.
    ///
    /// Each `DiffLayer` is wrapped in an `Arc` to enable efficient sharing
    /// and cloning without deep copying the underlying data structures.
    /// This is particularly important for performance when dealing with
    /// large state changes across multiple blocks.
    ///
    /// # Lookup Behavior
    ///
    /// When searching for a trie node or storage root, the lookup starts
    /// from the front of the vector (most recent layer at index 0) and proceeds
    /// forward. This ensures that newer state changes override older ones,
    /// maintaining the correct view of the current state.
    ///
    /// # Example
    /// ```
    /// // Layers are ordered: [block_102, block_101, block_100]
    /// // Querying for a node will check block_102 first, then block_101, then block_100
    /// ```
    pub diff_layers: Vec<Arc<DiffLayer>>,
}

impl DiffLayers {
    /// Insert a diff layer into the collection
    pub fn insert_difflayer(&mut self, difflayer: Arc<DiffLayer>) {
        self.diff_layers.push(difflayer);
    }

    /// Get a trie node by prefix
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        for difflayer in &self.diff_layers {
            if let Some(node) = difflayer.get_trie_nodes(prefix) {
                return Some(node);
            }
        }
        None
    }

    /// Get a storage root by hased address
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        for difflayer in &self.diff_layers {
            if let Some(root) = difflayer.get_storage_root(hased_address) {
                return Some(root);
            }
        }
        None
    }

    /// Returns true if the diff layers are empty
    pub fn is_empty(&self) -> bool {
        self.diff_layers.is_empty()
    }
}

