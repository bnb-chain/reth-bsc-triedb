//! DiffLayer types for tracking trie node changes.
//!
//! This module provides types for representing trie nodes and diff layers
//! used in tracking modifications during trie operations.

use std::sync::Arc;
use std::collections::HashMap;
use alloy_primitives::B256;
use fastbloom::BloomFilter;
use crate::lookup_stats::TRIE_DIFFLAYER_HITS;

// Trie state storage keys
pub const TRIE_STATE_ROOT_KEY: &[u8] = b"state_root";
pub const TRIE_STATE_BLOCK_NUMBER_KEY: &[u8] = b"block_number";

/// Target false-positive rate for the per-layer bloom filter on `diff_nodes`.
/// Chosen so that lookups miss in <1% of layers spuriously, while keeping
/// the filter to ~12 bits per entry (~2 KB for a 1500-key block).
const DIFF_LAYER_BLOOM_FPR: f64 = 0.01;

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

/// Build a bloom filter over the keys of `diff_nodes`.
///
/// The filter is sized for the current map size at ~1% false-positive rate.
/// Empty maps still produce a (tiny) filter so the query path is uniform.
fn build_diff_nodes_bloom(diff_nodes: &HashMap<Vec<u8>, Arc<TrieNode>>) -> BloomFilter {
    let n = diff_nodes.len().max(1);
    let mut bloom = BloomFilter::with_false_pos(DIFF_LAYER_BLOOM_FPR).expected_items(n);
    for key in diff_nodes.keys() {
        bloom.insert(key.as_slice());
    }
    bloom
}

/// DiffLayer is a collection of updated trie nodes and storage roots for a special block
#[derive(Clone, Debug)]
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

    /// Bloom filter over `diff_nodes` keys, built at construction time.
    ///
    /// Acts as a fast pre-check inside `get_trie_nodes`: if the bloom reports
    /// "not contains" we return `None` without touching the HashMap. False
    /// positives degrade to a normal HashMap lookup with zero correctness
    /// impact. Sized to ~1% FPR over `diff_nodes.len()` at construction —
    /// ~12 bits per entry, roughly 2 KB for a 1500-key block.
    ///
    /// Wrapped in `Arc` so `DiffLayer::clone()` (frequently invoked when the
    /// engine tree hands layers off to prefetchers / state-root computers)
    /// stays a refcount bump rather than a deep copy of the filter.
    pub diff_nodes_bloom: Arc<BloomFilter>,
}

impl Default for DiffLayer {
    fn default() -> Self {
        let diff_nodes = Arc::new(HashMap::new());
        let diff_storage_roots = Arc::new(HashMap::new());
        let diff_nodes_bloom = Arc::new(build_diff_nodes_bloom(&diff_nodes));
        Self { diff_nodes, diff_storage_roots, diff_nodes_bloom }
    }
}

// Two DiffLayers are equal iff they have the same `diff_nodes` and
// `diff_storage_roots` contents. The bloom filter is fully derived from
// `diff_nodes` and is intentionally excluded from this comparison — two
// equal-content layers may have non-bit-identical bloom internals (e.g.
// due to insertion order or capacity rounding), but they are functionally
// indistinguishable to callers. Currently only used in tests.
impl PartialEq for DiffLayer {
    fn eq(&self, other: &Self) -> bool {
        self.diff_nodes == other.diff_nodes
            && self.diff_storage_roots == other.diff_storage_roots
    }
}
impl Eq for DiffLayer {}

impl DiffLayer {
    /// Create a new diff layer.
    ///
    /// Builds a bloom filter over `diff_nodes` keys eagerly so that subsequent
    /// `get_trie_nodes` lookups can skip non-resident layers in O(bloom-hash)
    /// instead of O(HashMap-hash + cache-miss).
    pub fn new(diff_nodes: Arc<HashMap<Vec<u8>, Arc<TrieNode>>>, diff_storage_roots: Arc<HashMap<B256, B256>>) -> Self {
        let diff_nodes_bloom = Arc::new(build_diff_nodes_bloom(&diff_nodes));
        Self { diff_nodes, diff_storage_roots, diff_nodes_bloom }
    }

    /// Get a trie node by prefix.
    ///
    /// The bloom filter pre-check turns a miss into a few ns of hashing
    /// instead of a HashMap probe + cache miss. False positives fall through
    /// to the HashMap and behave exactly as before.
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        if !self.diff_nodes_bloom.contains(prefix) {
            return None;
        }
        self.diff_nodes.get(prefix).cloned()
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
    /// Insert a diff layer into the collection.
    ///
    /// **Callers must insert layers in reverse chronological order** (newest block first),
    /// so that `diff_layers[0]` always holds the most recent layer.
    /// The engine tree achieves this by walking from the parent block backwards through
    /// its ancestors, inserting each layer via this method in that order.
    pub fn insert_difflayer(&mut self, difflayer: Arc<DiffLayer>) {
        self.diff_layers.push(difflayer);
    }

    /// Get a trie node by prefix.
    ///
    /// Walks layers newest-first; each layer's bloom filter short-circuits
    /// the HashMap lookup on misses, so the common "scan all layers, find
    /// nothing" path is O(layers × bloom-hash) instead of O(layers × HashMap-hash).
    ///
    /// On a hit, bumps the global `TRIE_DIFFLAYER_HITS` counter — a single
    /// padded atomic increment — so the per-block lookup-path probe in the
    /// builder can attribute this query to the DiffLayer tier. Bloom rejects
    /// and HashMap-miss-after-bloom-pass do NOT touch the counter; only
    /// successful node returns count, keeping the hot path branchless.
    pub fn get_trie_nodes(&self, prefix: &[u8]) -> Option<Arc<TrieNode>> {
        for difflayer in &self.diff_layers {
            if !difflayer.diff_nodes_bloom.contains(prefix) {
                continue;
            }
            if let Some(node) = difflayer.diff_nodes.get(prefix) {
                TRIE_DIFFLAYER_HITS.increment();
                return Some(node.clone());
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
