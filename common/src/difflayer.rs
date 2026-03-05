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
/// Layers are inserted via `insert_difflayer` (O(1) push). On first lookup the
/// layers are merged into flat `HashMap`s (one-time O(total_entries) cost) that
/// are shared across all clones via `Arc<OnceLock>`, so subsequent lookups and
/// clones are both O(1). First-inserted layer wins for duplicate keys.
pub struct DiffLayers {
    diff_layers: Vec<Arc<DiffLayer>>,
    /// Lazily built flattened node map shared across clones.
    flat_nodes: Arc<std::sync::OnceLock<Arc<HashMap<Vec<u8>, Arc<TrieNode>>>>>,
    /// Lazily built flattened storage-root map shared across clones.
    flat_storage_roots: Arc<std::sync::OnceLock<Arc<HashMap<B256, B256>>>>,
}

impl Clone for DiffLayers {
    fn clone(&self) -> Self {
        Self {
            diff_layers: self.diff_layers.clone(),
            flat_nodes: self.flat_nodes.clone(),
            flat_storage_roots: self.flat_storage_roots.clone(),
        }
    }
}

impl Default for DiffLayers {
    fn default() -> Self {
        Self {
            diff_layers: Vec::new(),
            flat_nodes: Arc::new(std::sync::OnceLock::new()),
            flat_storage_roots: Arc::new(std::sync::OnceLock::new()),
        }
    }
}

impl std::fmt::Debug for DiffLayers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffLayers")
            .field("layers", &self.diff_layers.len())
            .field("flat_nodes_ready", &self.flat_nodes.get().is_some())
            .field("flat_storage_roots_ready", &self.flat_storage_roots.get().is_some())
            .finish()
    }
}

impl PartialEq for DiffLayers {
    fn eq(&self, other: &Self) -> bool {
        self.diff_layers == other.diff_layers
    }
}

impl Eq for DiffLayers {}

impl DiffLayers {
    /// Insert a diff layer into the collection.
    pub fn insert_difflayer(&mut self, difflayer: Arc<DiffLayer>) {
        self.diff_layers.push(difflayer);
        // Invalidate caches — new OnceLocks since OnceLock cannot be reset.
        self.flat_nodes = Arc::new(std::sync::OnceLock::new());
        self.flat_storage_roots = Arc::new(std::sync::OnceLock::new());
    }

    /// Get a trie node by prefix — O(1) after first call.
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        let flat = self.flat_nodes.get_or_init(|| {
            let total: usize = self.diff_layers.iter().map(|l| l.diff_nodes.len()).sum();
            let mut nodes = HashMap::with_capacity(total);
            for layer in &self.diff_layers {
                for (k, v) in layer.diff_nodes.iter() {
                    nodes.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            Arc::new(nodes)
        });
        flat.get(prefix).cloned()
    }

    /// Get a storage root by hashed address — O(1) after first call.
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        let flat = self.flat_storage_roots.get_or_init(|| {
            let total: usize = self.diff_layers.iter().map(|l| l.diff_storage_roots.len()).sum();
            let mut roots = HashMap::with_capacity(total);
            for layer in &self.diff_layers {
                for (k, v) in layer.diff_storage_roots.iter() {
                    roots.entry(*k).or_insert(*v);
                }
            }
            Arc::new(roots)
        });
        flat.get(&hased_address).copied()
    }

    /// Returns true if the diff layers are empty.
    pub fn is_empty(&self) -> bool {
        self.diff_layers.is_empty()
    }
}

