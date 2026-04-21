//! DiffLayer types for tracking trie node changes.
//!
//! This module provides types for representing trie nodes and diff layers
//! used in tracking modifications during trie operations.

use std::sync::Arc;
use std::sync::OnceLock;
use std::collections::HashMap;
use alloy_primitives::B256;

/// Threshold above which DiffLayers builds a merged index for O(1) lookup.
/// Below this, the linear scan over diff_layers is cheaper than the index
/// construction cost. 1M entries is roughly where 256-layer linear scan
/// across ~5000 entries/layer starts to be worth amortizing.
const MERGED_INDEX_BUILD_THRESHOLD: usize = 1_000_000;

// Trie state storage keys
pub const TRIE_STATE_ROOT_KEY: &[u8] = b"state_root";
pub const TRIE_STATE_BLOCK_NUMBER_KEY: &[u8] = b"block_number";

/// Represents a trie node with its hash and encoded data
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// Creates a default trie node
    pub fn default() -> Self {
        Self { hash: None, blob: None }
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
        self.diff_nodes.get(prefix).map(|node: &Arc<TrieNode>| node.clone())
    }

    /// Get a storage root by hased address
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        self.diff_storage_roots.get(&hased_address).map(|root| *root)
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
/// Internal merged index over all diff_layers. Built lazily on first query
/// when aggregated node count >= `MERGED_INDEX_BUILD_THRESHOLD`.
///
/// Semantics: values from newer layers (lower index in `diff_layers`) win over
/// older layers. `or_insert_with` preserves this: we iterate from newest to
/// oldest and only insert if key not yet present. Deleted-node entries (with
/// `blob = None`) are stored faithfully - they MUST win over live entries in
/// older layers (this is critical for correctness; see `bad block` notes).
#[derive(Debug)]
struct MergedIndex {
    trie_nodes: HashMap<Vec<u8>, Arc<TrieNode>>,
    storage_roots: HashMap<B256, B256>,
}

/// DiffLayers is a stack of DiffLayer instances from a chain of uncommitted blocks.
///
/// Lookups (`get_trie_nodes`, `get_storage_root`) use a lazy O(1) merged index
/// for large aggregate sizes, falling back to linear scan for small sizes where
/// scanning is cheaper than index construction.
///
/// # Invariants required by the caller
///
/// - Layers must be inserted in newest-first order (`diff_layers[0]` newest).
/// - Once `get_trie_nodes` or `get_storage_root` has been called, `insert_difflayer`
///   MUST NOT be called on that instance (the merged index would go stale).
///   In practice the engine tree's `merged_difflayer_by_hash` finalizes construction
///   before returning the DiffLayers, so this is naturally upheld.
pub struct DiffLayers {
    /// An ordered collection of diff layers, one per uncommitted block.
    ///
    /// The vector maintains diff layers in reverse chronological order, with the
    /// most recent block's diff layer at index 0 and the oldest block's
    /// diff layer at the end of the vector.
    ///
    /// Each `DiffLayer` is wrapped in an `Arc` to enable efficient sharing
    /// and cloning without deep copying the underlying data structures.
    pub diff_layers: Vec<Arc<DiffLayer>>,

    /// Lazily-built merged index across all diff_layers. Only built when
    /// aggregate size justifies it (see `MERGED_INDEX_BUILD_THRESHOLD`).
    /// `None` after build means "use linear scan" (size below threshold).
    /// `Some(index)` means "use the O(1) index".
    ///
    /// Not serialized / not compared / not cloned (a clone starts with empty cache
    /// to preserve the invariant that mutation after caching is forbidden).
    merged_index: OnceLock<Option<Arc<MergedIndex>>>,
}

// Manual Clone: a clone starts with an empty merged_index cache. Safe because
// subsequent queries on the clone will rebuild the cache from diff_layers.
impl Clone for DiffLayers {
    fn clone(&self) -> Self {
        Self {
            diff_layers: self.diff_layers.clone(),
            merged_index: OnceLock::new(),
        }
    }
}

impl Default for DiffLayers {
    fn default() -> Self {
        Self {
            diff_layers: Vec::new(),
            merged_index: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for DiffLayers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiffLayers")
            .field("diff_layers", &self.diff_layers)
            .field("merged_index_built", &self.merged_index.get().is_some())
            .finish()
    }
}

impl PartialEq for DiffLayers {
    fn eq(&self, other: &Self) -> bool {
        // Only compare layers; merged_index is a cache.
        self.diff_layers == other.diff_layers
    }
}

impl Eq for DiffLayers {}

impl DiffLayers {
    /// Insert a diff layer into the collection.
    ///
    /// **Callers must insert layers in reverse chronological order** (newest block first),
    /// so that `diff_layers[0]` always holds the most recent layer.
    /// The engine tree achieves this by walking from the parent block backwards through
    /// its ancestors, inserting each layer via this method in that order.
    ///
    /// **Must NOT be called after `get_trie_nodes` or `get_storage_root`** has been
    /// invoked on this instance. Doing so would leave the cached merged index stale
    /// (debug_assertions build panics in that case; release build silently returns
    /// incorrect data, which is a bad-block risk).
    pub fn insert_difflayer(&mut self, difflayer: Arc<DiffLayer>) {
        // Safety net: if we've already built the merged index, inserting more
        // layers would corrupt it. Panic in debug builds.
        debug_assert!(
            self.merged_index.get().is_none(),
            "insert_difflayer called after merged index was built; this would cause bad blocks"
        );
        self.diff_layers.push(difflayer);
    }

    /// Aggregate size across all layers. Used to decide whether to build the merged index.
    fn aggregate_node_count(&self) -> usize {
        self.diff_layers.iter().map(|l| l.diff_nodes.len()).sum()
    }

    /// Build the merged index (called lazily by `merged_index_or_build`).
    /// Newest-wins: iterate from newest (index 0) to oldest and use `or_insert_with`
    /// so the first insert (from the newest layer) wins.
    ///
    /// Preserves deleted-node entries faithfully (they survive into the index).
    fn build_merged_index(&self) -> Arc<MergedIndex> {
        let total_nodes = self.aggregate_node_count();
        let total_roots: usize = self.diff_layers.iter().map(|l| l.diff_storage_roots.len()).sum();
        let mut trie_nodes: HashMap<Vec<u8>, Arc<TrieNode>> =
            HashMap::with_capacity(total_nodes);
        let mut storage_roots: HashMap<B256, B256> =
            HashMap::with_capacity(total_roots);
        // diff_layers[0] is newest; iterating in-order + or_insert_with ==> newest wins.
        for layer in &self.diff_layers {
            for (key, value) in layer.diff_nodes.iter() {
                trie_nodes.entry(key.clone()).or_insert_with(|| value.clone());
            }
            for (addr, root) in layer.diff_storage_roots.iter() {
                storage_roots.entry(*addr).or_insert(*root);
            }
        }
        Arc::new(MergedIndex { trie_nodes, storage_roots })
    }

    /// Ensure merged index is initialised (if appropriate by size) and return it.
    /// Returns `None` when the aggregate is too small to warrant an index -
    /// callers fall back to linear scan.
    fn merged_index_or_build(&self) -> Option<Arc<MergedIndex>> {
        self.merged_index
            .get_or_init(|| {
                if self.aggregate_node_count() < MERGED_INDEX_BUILD_THRESHOLD {
                    None
                } else {
                    Some(self.build_merged_index())
                }
            })
            .clone()
    }

    /// Get a trie node by prefix
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        if let Some(index) = self.merged_index_or_build() {
            // O(1) lookup on the aggregated index.
            return index.trie_nodes.get(prefix).cloned();
        }
        // Small aggregate: linear scan is cheaper than building an index.
        for difflayer in &self.diff_layers {
            if let Some(node) = difflayer.get_trie_nodes(prefix) {
                return Some(node);
            }
        }
        None
    }

    /// Get a storage root by hased address
    pub fn get_storage_root(&self, hased_address: B256) -> Option<B256> {
        if let Some(index) = self.merged_index_or_build() {
            return index.storage_roots.get(&hased_address).copied();
        }
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

