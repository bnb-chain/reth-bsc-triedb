//! Core trie implementation for secure trie operations.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use alloy_primitives::{B256};
use alloy_trie::EMPTY_ROOT_HASH;
use rust_eth_triedb_common::TrieDatabase;
use tracing::Level;
use crate::trie_committer::Committer;
use super::encoding::{common_prefix_length, key_to_nibbles, TrieNodeKeyBuf};
use super::node::{Node, NodeFlag, FullNode, ShortNode, NodeSet, TrieNode, DiffLayers};
use super::secure_trie::{SecureTrieId, SecureTrieError};
use super::trie_hasher::Hasher;
use super::trie_tracer::TrieTracer;

/// Result of `get_internal`: (value, updated_node, was_resolved).
type GetInternalResult = Result<(Option<Vec<u8>>, Arc<Node>, bool), SecureTrieError>;

/// Aggregated stats for diagnosing trie update performance.
///
/// This is intentionally lightweight (counters + byte totals + coarse timings),
/// and is collected only when debug-level tracing is enabled.
#[derive(Clone, Debug, Default)]
pub struct TrieUpdateStatsSnapshot {
    pub update_calls: u64,
    pub delete_calls: u64,

    pub key_to_nibbles_calls: u64,
    pub key_to_nibbles_us: u64,

    pub value_bytes_total: u64,
    pub value_alloc_bytes_total: u64,

    pub insert_internal_calls: u64,
    pub delete_internal_calls: u64,

    pub shortnode_split_count: u64,
    pub fullnode_collapse_count: u64,

    pub resolve_calls: u64,
    pub resolve_difflayer_hits: u64,
    pub resolve_db_hits: u64,
    pub resolve_us: u64,
    pub resolve_decode_us: u64,
    pub resolve_blob_bytes_total: u64,

    pub node_key_alloc_count: u64,
    pub node_key_alloc_bytes_total: u64,
}

#[derive(Clone, Debug, Default)]
struct TrieUpdateStats {
    enabled: bool,
    update_calls: u64,
    delete_calls: u64,

    key_to_nibbles_calls: u64,
    key_to_nibbles_us: u64,

    value_bytes_total: u64,
    value_alloc_bytes_total: u64,

    insert_internal_calls: u64,
    delete_internal_calls: u64,

    shortnode_split_count: u64,
    fullnode_collapse_count: u64,

    resolve_calls: u64,
    resolve_difflayer_hits: u64,
    resolve_db_hits: u64,
    resolve_us: u64,
    resolve_decode_us: u64,
    resolve_blob_bytes_total: u64,

    node_key_alloc_count: u64,
    node_key_alloc_bytes_total: u64,
}

impl TrieUpdateStats {
    fn reset(&mut self) {
        let enabled = self.enabled;
        *self = Self::default();
        self.enabled = enabled;
    }

    fn snapshot(&self) -> TrieUpdateStatsSnapshot {
        TrieUpdateStatsSnapshot {
            update_calls: self.update_calls,
            delete_calls: self.delete_calls,
            key_to_nibbles_calls: self.key_to_nibbles_calls,
            key_to_nibbles_us: self.key_to_nibbles_us,
            value_bytes_total: self.value_bytes_total,
            value_alloc_bytes_total: self.value_alloc_bytes_total,
            insert_internal_calls: self.insert_internal_calls,
            delete_internal_calls: self.delete_internal_calls,
            shortnode_split_count: self.shortnode_split_count,
            fullnode_collapse_count: self.fullnode_collapse_count,
            resolve_calls: self.resolve_calls,
            resolve_difflayer_hits: self.resolve_difflayer_hits,
            resolve_db_hits: self.resolve_db_hits,
            resolve_us: self.resolve_us,
            resolve_decode_us: self.resolve_decode_us,
            resolve_blob_bytes_total: self.resolve_blob_bytes_total,
            node_key_alloc_count: self.node_key_alloc_count,
            node_key_alloc_bytes_total: self.node_key_alloc_bytes_total,
        }
    }
}

/// Core trie implementation
#[derive(Clone, Debug)]
pub struct Trie<DB> {
    root: Arc<Node>,
    owner: B256,
    committed: bool,
    unhashed: usize,
    uncommitted: usize,
    pub tracer: TrieTracer,
    database: DB,
    difflayers: Option<DiffLayers>,
    update_stats: TrieUpdateStats,
    /// When true, skip tracer tracking (on_read/on_insert/on_delete).
    /// Used during prefetch where the tracer data is discarded afterward.
    skip_tracer: bool,
}

/// Basic Trie operations
impl<DB> Trie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Creates a new trie with the given identifier and database
    pub fn new(id: &SecureTrieId, database: DB, difflayer: Option<&DiffLayers>) -> Result<Self, SecureTrieError> {
        let mut tr = Self {
            root: Node::empty_root(),
            owner: id.owner,
            committed: false,
            unhashed: 0,
            uncommitted: 0,
            tracer: TrieTracer::new(),
            database,
            difflayers: difflayer.cloned(),
            update_stats: TrieUpdateStats::default(),
            skip_tracer: false,
        };

        // Check if this is an empty trie (root is EmptyRootHash or ZERO)
        let root = if id.state_root == alloy_trie::EMPTY_ROOT_HASH || id.state_root == B256::ZERO {
            Node::empty_root()
        } else {
            tr.resolve_and_track(&id.state_root, &[])?
        };
        tr.root = root;
        Ok(tr)
    }

    /// Resets aggregated update stats for this trie instance.
    pub fn reset_update_stats(&mut self) {
        self.update_stats.reset();
    }

    /// Takes a snapshot of aggregated update stats, resetting them afterward.
    ///
    /// Returns `None` if stats collection is not enabled (debug-level tracing disabled).
    pub fn take_update_stats_snapshot(&mut self) -> Option<TrieUpdateStatsSnapshot> {
        if !self.update_stats.enabled {
            return None;
        }
        let snap = self.update_stats.snapshot();
        self.update_stats.reset();
        Some(snap)
    }

    #[inline]
    fn stats_enable_for_call(&mut self) {
        self.update_stats.enabled = tracing::enabled!(Level::DEBUG);
    }

    // NOTE: prefix cloning and key-slice-to-vec allocations were eliminated
    // by the push/truncate and &[u8] optimizations.

    /// Creates a new flag for the trie
    pub fn new_flag(&self) -> NodeFlag {
        NodeFlag::default()
    }

    /// Sets the skip_tracer flag.
    /// When true, tracer tracking (on_read/on_insert/on_delete) is skipped.
    /// Used during prefetch where the tracer data is discarded afterward.
    pub fn set_skip_tracer(&mut self, skip: bool) {
        self.skip_tracer = skip;
    }

    /// Gets the root node of the trie
    pub fn root(&self) -> &Arc<Node> {
        &self.root
    }

    /// Gets the root hash of the trie
    pub fn hash(&mut self) -> B256 {
        if self.root == Node::empty_root() {
            return EMPTY_ROOT_HASH;
        }
        let hasher = Hasher::new(self.unhashed > 100);
        let(hashed, cached) = hasher.hash(self.root.clone(), true);
        
        self.root = cached;
        if let Node::Hash(h) = &*hashed {
            *h
        } else {
            panic!("Expected Hash node, got: {:?}", hashed);
        }
    }

    pub fn commit(&mut self, collect_leaf: bool) -> Result<(B256, Option<Arc<NodeSet>>), SecureTrieError> {
        if matches!(&*self.root, Node::Empty) {
            let paths = self.tracer.deleted_nodes();
            if paths.is_empty() {
                self.committed = true;
                return Ok((EMPTY_ROOT_HASH, None));
            }

            let mut nodes = NodeSet::new(self.owner);
            for path in paths {
                nodes.add_node(path.as_slice(), Arc::new(TrieNode::default()));
            }
            self.committed = true;
            return Ok((EMPTY_ROOT_HASH, Some(Arc::new(nodes))));
        }

        let root_hash = self.hash();

        let(hash_node, dirty) = self.root.cache();
        if !dirty {
            self.root = Arc::new(Node::Hash(hash_node.unwrap()));
            self.committed = true;
            return Ok((root_hash, None));
        }

        let nodes = Arc::new(Mutex::new(NodeSet::new(self.owner)));
        {
            let mut nodeset = nodes.lock().unwrap();
            for path in self.tracer.deleted_nodes() {
                nodeset.add_node(path.as_slice(), Arc::new(TrieNode::default()));
            }
        }

        {
            self.root = Committer::new(nodes.clone(), &self.tracer, collect_leaf)
                .commit(
                    self.root.clone(), 
                    self.unhashed > 100
                );
        }

        // Extract the final NodeSet for returning
        let nodeset = {
            let guard = nodes.lock().unwrap();
            Arc::new(guard.clone())
        };
        self.uncommitted = 0;
        self.committed = true;

        Ok((root_hash, Some(nodeset)))
    }
}

/// Trie interface
impl<DB> Trie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Gets a value from the trie by key
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, SecureTrieError> {
        // Check if trie is already committed
        if self.committed {
            return Err(SecureTrieError::AlreadyCommitted);
        }

        // Convert key to nibbles + terminator format
        let nibbles_key = key_to_nibbles(key);

        // Get value from internal trie structure
        let (value, new_root, did_resolve) = self.get_internal(
            self.root.clone(),
            &nibbles_key,
            0
        )?;

        // Update root if it was resolved (CoW optimization)
        if did_resolve {
            self.root = new_root;
        }

        // Return the found value (or None if not found)
        Ok(value)
    }

    /// Updates a value in the trie by key
    pub fn update(&mut self, key: &[u8], value: &[u8]) -> Result<(), SecureTrieError> {
        // Check if trie is already committed
        if self.committed {
            return Err(SecureTrieError::AlreadyCommitted);
        }

        self.stats_enable_for_call();
        if self.update_stats.enabled {
            self.update_stats.update_calls += 1;
            self.update_stats.value_bytes_total += value.len() as u64;
            if !value.is_empty() {
                // `Node::Value(value.to_vec())` allocates a new Vec copy of `value`.
                self.update_stats.value_alloc_bytes_total += value.len() as u64;
            }
        }

        // Update trie statistics
        self.unhashed += 1;
        self.uncommitted += 1;

        // Create value node from input value
        let value_node = if value.is_empty() {
            None
        } else {
            Some(Node::Value(value.to_vec()))
        };

        // Convert key to nibbles + terminator format
        let nibbles_key = if self.update_stats.enabled {
            self.update_stats.key_to_nibbles_calls += 1;
            let start = Instant::now();
            let n = key_to_nibbles(key);
            self.update_stats.key_to_nibbles_us += start.elapsed().as_micros() as u64;
            n
        } else {
            key_to_nibbles(key)
        };

        // Handle empty value (delete) vs non-empty (insert)
        let mut prefix = Vec::new();
        if let Some(vn) = value_node {
            let (_, new_root) = self.insert_internal(
                self.root.clone(),
                &mut prefix,
                &nibbles_key,
                Arc::new(vn)
            )?;
            self.root = new_root;
        } else {
            let (_, new_root) = self.delete_internal(
                self.root.clone(),
                &mut prefix,
                &nibbles_key)?;
            self.root = new_root;
        }

        Ok(())
    }

    /// Deletes a value from the trie by key
    pub fn delete(&mut self, key: &[u8]) -> Result<(), SecureTrieError> {
        // Check if trie is already committed
        if self.committed {
            return Err(SecureTrieError::AlreadyCommitted);
        }

        self.stats_enable_for_call();
        if self.update_stats.enabled {
            self.update_stats.delete_calls += 1;
        }

        // Update trie statistics
        self.unhashed += 1;
        self.uncommitted += 1;

        // Convert key to nibbles + terminator format
        let nibbles_key = if self.update_stats.enabled {
            self.update_stats.key_to_nibbles_calls += 1;
            let start = Instant::now();
            let n = key_to_nibbles(key);
            self.update_stats.key_to_nibbles_us += start.elapsed().as_micros() as u64;
            n
        } else {
            key_to_nibbles(key)
        };

        // Delete the value from the trie
        let mut prefix = Vec::new();
        let (_, new_root) = self.delete_internal(
            self.root.clone(),
            &mut prefix,
            &nibbles_key
        )?;

        self.root = new_root;
        Ok(())
    }
}

/// Trie internal implementation
impl<DB> Trie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Internal function to get a value from the trie.
    /// Takes nibbles_key as a shared reference to avoid allocations on each recursive call.
    /// Returns: (value, new_node, resolved)
    fn get_internal(
        &mut self, node: Arc<Node>,
        nibbles_key: &[u8],
        pos: usize
    ) -> GetInternalResult {
        match &*node {
            Node::Empty => {
                Ok((None, node, false))
            }

            Node::Value(value) => {
                Ok((Some(value.clone()), node, false))
            }

            Node::Short(short) => {
                if !nibbles_key[pos..].starts_with(&short.key) {
                    return Ok((None, node, false));
                }

                let (value, new_child, resolved) = self.get_internal(
                    short.val.clone(),
                    nibbles_key,
                    pos + short.key.len()
                )?;

                if resolved {
                    let mut new_short = short.to_mutable_copy_with_cow();
                    new_short.set_value(&new_child);
                    Ok((value, Arc::new(Node::Short(Arc::new(new_short))), true))
                } else {
                    Ok((value, node, false))
                }
            }

            Node::Full(full) => {
                let nibble = nibbles_key[pos] as usize;
                let (value, new_child, resolved) = self.get_internal(
                    full.get_child(nibble),
                    nibbles_key,
                    pos + 1
                )?;

                if resolved {
                    let mut new_full = full.to_mutable_copy_with_cow();
                    new_full.set_child(nibble, &new_child);
                    Ok((value, Arc::new(Node::Full(Arc::new(new_full))), true))
                } else {
                    Ok((value, node, false))
                }
            }

            Node::Hash(hash) => {
                let resolved_node = self.resolve_and_track(
                    hash,
                    &nibbles_key[..pos]
                )?;
                let (value, new_node, _) = self.get_internal(resolved_node, nibbles_key, pos)?;
                Ok((value, new_node, true))
            }
        }
    }

    /// Internal function to insert a value into the trie.
    /// Uses a mutable prefix buffer with push/truncate to avoid cloning at every level.
    /// Takes nibbles_key as a shared slice to avoid Vec allocation per recursion.
    fn insert_internal(
        &mut self, node: Arc<Node>,
        prefix: &mut Vec<u8>,
        nibbles_key: &[u8],
        value: Arc<Node>
    ) -> Result<(bool, Arc<Node>), SecureTrieError> {
        if self.update_stats.enabled {
            self.update_stats.insert_internal_calls += 1;
        }
        // Base case: reached the end of the key
        if nibbles_key.is_empty() {
            match &*node {
                Node::Value(existing_value) => {
                    if let Node::Value(new_value) = &*value {
                        if existing_value == new_value {
                            return Ok((false, node));
                        } else {
                            return Ok((true, value));
                        }
                    } else {
                        return Ok((true, value));
                    }
                }
                _ => {
                    return Ok((true, value));
                }
            }
        }

        match &*node {
            Node::Short(short) => {
                let matchlen = common_prefix_length(nibbles_key, &short.key);

                if matchlen == short.key.len() {
                    let prefix_len = prefix.len();
                    prefix.extend_from_slice(&nibbles_key[..matchlen]);

                    let (dirty, new_child) = self.insert_internal(
                        short.val.clone(),
                        prefix,
                        &nibbles_key[matchlen..],
                        value
                    )?;
                    prefix.truncate(prefix_len);

                    if !dirty {
                        return Ok((false, node));
                    } else {
                        let new_short_arc = Arc::new(Node::Short(Arc::new(ShortNode {
                            key: short.key.clone(),
                            val: new_child,
                            flags: self.new_flag(),
                        })));
                        return Ok((true, new_short_arc));
                    }
                }

                // Create a branch node to split the short node
                if self.update_stats.enabled {
                    self.update_stats.shortnode_split_count += 1;
                }
                let mut branch = Box::new(FullNode::new());

                // Insert the short node's remaining key into the branch
                {
                    let prefix_len = prefix.len();
                    prefix.extend_from_slice(&short.key[..matchlen + 1]);
                    let (_, new_child1) = self.insert_internal(
                        Node::empty_root(),
                        prefix,
                        &short.key[matchlen + 1..],
                        short.val.clone()
                    )?;
                    prefix.truncate(prefix_len);
                    branch.set_child(short.key[matchlen] as usize, new_child1.as_ref());
                }

                // Insert the new key into the branch
                {
                    let prefix_len = prefix.len();
                    prefix.extend_from_slice(&nibbles_key[..matchlen + 1]);
                    let (_, new_child2) = self.insert_internal(
                        Node::empty_root(),
                        prefix,
                        &nibbles_key[matchlen + 1..],
                        value
                    )?;
                    prefix.truncate(prefix_len);
                    branch.set_child(nibbles_key[matchlen] as usize, new_child2.as_ref());
                }

                if matchlen == 0 {
                    return Ok((true, Arc::new(Node::Full(Arc::from(branch)))));
                }

                let new_short_arc = Arc::new(Node::Short(Arc::new(ShortNode {
                    key: nibbles_key[..matchlen].to_vec(),
                    val: Arc::new(Node::Full(Arc::from(branch))),
                    flags: self.new_flag(),
                })));

                // Trace the insert operation
                if !self.skip_tracer {
                    let prefix_len = prefix.len();
                    prefix.extend_from_slice(&nibbles_key[..matchlen]);
                    self.tracer.on_insert(&prefix[..]);
                    prefix.truncate(prefix_len);
                }

                Ok((true, new_short_arc))
            }

            Node::Full(full) => {
                let prefix_len = prefix.len();
                prefix.push(nibbles_key[0]);

                let child = full.get_child(nibbles_key[0] as usize);
                let (dirty, new_child) = self.insert_internal(
                    child,
                    prefix,
                    &nibbles_key[1..],
                    value
                )?;
                prefix.truncate(prefix_len);

                if !dirty {
                    Ok((false, node))
                } else {
                    let mut new_full = full.to_mutable_copy_with_cow();
                    new_full.flags = self.new_flag();
                    new_full.set_child(nibbles_key[0] as usize, &new_child);

                    Ok((true, Arc::new(Node::Full(Arc::new(new_full)))))
                }
            }

            Node::Empty => {
                if !self.skip_tracer {
                    self.tracer.on_insert(&prefix[..]);
                }
                Ok((true, Arc::new(Node::Short(Arc::new(ShortNode::new(nibbles_key.to_vec(), value.as_ref()))))))
            }

            Node::Hash(hash) => {
                let resolved_node = self.resolve_and_track(hash, prefix)?;
                let (dirty, new_node) = self.insert_internal(
                    resolved_node.clone(),
                    prefix,
                    nibbles_key,
                    value
                )?;

                if !dirty {
                    Ok((false, resolved_node))
                } else {
                    Ok((true, new_node))
                }
            }

            Node::Value(_) => {
                panic!("Value node should not be in the trie");
            }
        }
    }

    /// Internal function to delete a value from the trie.
    /// Uses a mutable prefix buffer with push/truncate to avoid cloning at every level.
    /// Takes nibbles_key as a shared slice to avoid Vec allocation per recursion.
    pub fn delete_internal(
        &mut self,
        node: Arc<Node>,
        prefix: &mut Vec<u8>,
        nibbles_key: &[u8]
    ) -> Result<(bool, Arc<Node>), SecureTrieError> {
        if self.update_stats.enabled {
            self.update_stats.delete_internal_calls += 1;
        }

        match &*node {
            Node::Short(short) => {
                let matchlen = common_prefix_length(nibbles_key, &short.key);

                if matchlen < short.key.len() {
                    return Ok((false, node.clone()));
                }

                // Complete key match - delete this node
                if matchlen == nibbles_key.len() {
                    if !self.skip_tracer {
                        self.tracer.on_delete(&prefix[..]);
                    }
                    return Ok((true, Node::empty_root()));
                }

                // Partial match - continue deletion in child node
                let prefix_len = prefix.len();
                prefix.extend_from_slice(&nibbles_key[..short.key.len()]);

                let (dirty, new_child) = self.delete_internal(
                    short.val.clone(),
                    prefix,
                    &nibbles_key[short.key.len()..]
                )?;
                prefix.truncate(prefix_len);

                if !dirty {
                    return Ok((false, node.clone()));
                }

                match &*new_child {
                    Node::Short(new_child_short) => {
                        // Trace the delete operation
                        if !self.skip_tracer {
                            let prefix_len = prefix.len();
                            prefix.extend_from_slice(&short.key);
                            self.tracer.on_delete(&prefix[..]);
                            prefix.truncate(prefix_len);
                        }

                        // Merge keys when child is also a ShortNode
                        let mut merged_key = short.key.clone();
                        merged_key.extend(&new_child_short.key);

                        let new_short_arc = Arc::new(Node::Short(Arc::new(ShortNode {
                            key: merged_key,
                            val: new_child_short.val.clone(),
                            flags: self.new_flag(),
                        })));
                        Ok((true, new_short_arc))
                    }
                    _ => {
                        let new_short_arc = Arc::new(Node::Short(Arc::new(ShortNode {
                            key: short.key.clone(),
                            val: new_child,
                            flags: self.new_flag(),
                        })));
                        Ok((true, new_short_arc))
                    }
                }
            }

            Node::Full(full) => {
                let child_index = nibbles_key[0] as usize;

                let prefix_len = prefix.len();
                prefix.push(nibbles_key[0]);

                let (dirty, new_child) = self.delete_internal(
                    full.get_child(child_index),
                    prefix,
                    &nibbles_key[1..],
                )?;
                prefix.truncate(prefix_len);

                if !dirty {
                    return Ok((false, node.clone()));
                }

                let mut new_full = full.to_mutable_copy_with_cow();
                new_full.flags = self.new_flag();
                new_full.set_child(child_index, &new_child);
                let full_copy = new_full.clone();

                match &*new_child {
                    Node::Empty => {
                        let mut non_empty_pos = -1i32;
                        let mut non_empty_count = 0;

                        for (i, child) in full_copy.children.iter().enumerate() {
                            if !matches!(&**child, Node::Empty) {
                                non_empty_count += 1;
                                if non_empty_pos == -1 {
                                    non_empty_pos = i as i32;
                                } else {
                                    non_empty_pos = -2;
                                    break;
                                }
                            }
                        }

                        if non_empty_pos >= 0 && non_empty_count == 1 {
                            if self.update_stats.enabled {
                                self.update_stats.fullnode_collapse_count += 1;
                            }

                            if non_empty_pos != 16 {
                                let prefix_len = prefix.len();
                                prefix.push(non_empty_pos as u8);
                                let resolved_child = self.resolve(
                                    full_copy.get_child(non_empty_pos as usize),
                                    prefix
                                )?;

                                if let Node::Short(child_short) = &*resolved_child {
                                    if !self.skip_tracer {
                                        self.tracer.on_delete(&prefix[..]);
                                    }
                                    prefix.truncate(prefix_len);

                                    let mut merged_key = vec![non_empty_pos as u8];
                                    merged_key.extend(&child_short.key);

                                    let new_short_arc = Arc::new(Node::Short(Arc::new(ShortNode {
                                        key: merged_key,
                                        val: child_short.val.clone(),
                                        flags: self.new_flag(),
                                    })));
                                    return Ok((true, new_short_arc));
                                }
                                prefix.truncate(prefix_len);
                            }

                            let new_short_arc = Arc::new(Node::Short(Arc::new(ShortNode {
                                key: vec![non_empty_pos as u8],
                                val: full_copy.get_child(non_empty_pos as usize),
                                flags: self.new_flag(),
                            })));
                            Ok((true, new_short_arc))
                        } else {
                            let new_full_arc = Arc::new(Node::Full(Arc::new(full_copy)));
                            Ok((true, new_full_arc))
                        }
                    }
                    _ => {
                        let new_full_arc = Arc::new(Node::Full(Arc::new(full_copy)));
                        Ok((true, new_full_arc))
                    }
                }
            }

            Node::Value(_) => {
                Ok((true, Node::empty_root()))
            }

            Node::Empty => {
                Ok((false, Node::empty_root()))
            }

            Node::Hash(hash) => {
                let resolved_node = self.resolve_and_track(hash, prefix)?;
                let resolved_node_backup = resolved_node.clone();

                let (dirty, new_node) = self.delete_internal(
                    resolved_node,
                    prefix,
                    nibbles_key
                )?;

                if !dirty {
                    Ok((false, resolved_node_backup))
                } else {
                    Ok((true, new_node))
                }
            }
        }
    }
}

// Trie Helper operations
impl<DB> Trie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{

    /// Resolves a node from a hash
    pub fn resolve(&mut self, node: Arc<Node> , prefix: &[u8]) -> Result<Arc<Node>, SecureTrieError> {
        match &*node {
            Node::Hash(hash) => {
                self.resolve_and_track(hash, prefix)
            }
            _ => {
                Ok(node)
            }
        }
    }

    /// Resolves a hash and tracks it in the difflayer.
    /// Optimized: conditional timing, skip tracer when `skip_tracer` is set.
    pub fn resolve_and_track(&mut self, hash: &B256, prefix: &[u8]) -> Result<Arc<Node>, SecureTrieError> {
        let enabled = self.update_stats.enabled;
        if enabled {
            self.update_stats.resolve_calls += 1;
        }
        // Only take a timestamp when stats are actually enabled
        let resolve_start = if enabled { Some(Instant::now()) } else { None };

        let key_buf = if self.owner == B256::ZERO {
            TrieNodeKeyBuf::account(prefix)
        } else {
            TrieNodeKeyBuf::storage(self.owner.as_slice(), prefix)
        };
        let key = key_buf.as_slice();

        if enabled {
            self.update_stats.node_key_alloc_count += 1;
            self.update_stats.node_key_alloc_bytes_total += key.len() as u64;
        }

        // 1. Check if the hash is in the difflayer (no key clone needed)
        if let Some(difflayers) = &self.difflayers {
            if let Some(node) = difflayers.get_trie_nodes(key) {
                if enabled {
                    self.update_stats.resolve_difflayer_hits += 1;
                }
                if node.is_deleted() {
                    if let Some(start) = resolve_start {
                        self.update_stats.resolve_us += start.elapsed().as_micros() as u64;
                    }
                    return Ok(Node::empty_root());
                }
                let blob = node.blob.as_ref().unwrap();
                if enabled {
                    self.update_stats.resolve_blob_bytes_total += blob.len() as u64;
                }
                let decode_start = if enabled { Some(Instant::now()) } else { None };
                let decoded = Node::must_decode_node(Some(*hash), blob);
                if let Some(ds) = decode_start {
                    self.update_stats.resolve_decode_us += ds.elapsed().as_micros() as u64;
                }
                // Only track in tracer when not in prefetch mode
                if !self.skip_tracer {
                    self.tracer.on_read_ref(prefix, blob);
                }
                if let Some(start) = resolve_start {
                    self.update_stats.resolve_us += start.elapsed().as_micros() as u64;
                }
                return Ok(decoded);
            }
        }

        // 2. Check if the hash is in the database
        if let Some(node_blob) = self.database.get_trie_node(key).map_err(|e| SecureTrieError::Database(format!("{:?}", e)))? {
            if enabled {
                self.update_stats.resolve_db_hits += 1;
                self.update_stats.resolve_blob_bytes_total += node_blob.len() as u64;
            }
            let decode_start = if enabled { Some(Instant::now()) } else { None };
            let decoded = Node::must_decode_node(Some(*hash), &node_blob);
            if let Some(ds) = decode_start {
                self.update_stats.resolve_decode_us += ds.elapsed().as_micros() as u64;
            }
            if !self.skip_tracer {
                self.tracer.on_read(prefix, node_blob);
            }
            if let Some(start) = resolve_start {
                self.update_stats.resolve_us += start.elapsed().as_micros() as u64;
            }
            return Ok(decoded);
        }

        if let Some(start) = resolve_start {
            self.update_stats.resolve_us += start.elapsed().as_micros() as u64;
        }
        let owner_hex = format!("0x{:x}", self.owner);
        let prefix_hex = prefix.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        let key_hex = key.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        Err(SecureTrieError::Database(format!("missing trie node: owner: {}, prefix: 0x{}, key: 0x{}", owner_hex, prefix_hex, key_hex)))
    }

}
// Debug implementation for Trie
impl<DB> Trie<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Debug method to print the trie structure in a tree format
    pub fn debug_print(&self) {
        println!("=== TRIE STRUCTURE ===");
        self.debug_print_node(&self.root, "", true);
        println!("=====================");
    }

    /// Internal method to recursively print node structure
    fn debug_print_node(&self, node: &Arc<Node>, prefix: &str, is_last: bool) {
        let connector = if is_last { "└── " } else { "├── " };

        match &**node {
            Node::Empty => {
                println!("{}{}EmptyRoot", prefix, connector);
            }
            Node::Value(value) => {
                println!("{}{}Value: {}",
                    prefix, connector,
                    hex::encode(value));
            }
            Node::Short(short) => {
                println!("{}{}Short: key={}",
                    prefix, connector,
                    hex::encode(&short.key));
                let new_prefix = format!("{}    ", prefix);
                self.debug_print_node(&short.val, &new_prefix, true);
            }
            Node::Full(full) => {
                println!("{}{}Full:", prefix, connector);
                let new_prefix = format!("{}    ", prefix);

                // Print non-empty children
                let mut non_empty_count = 0;
                for child in full.children.iter() {
                    if !matches!(&**child, Node::Empty) {
                        non_empty_count += 1;
                    }
                }

                let mut current_count = 0;
                for (i, child) in full.children.iter().enumerate() {
                    if !matches!(&**child, Node::Empty) {
                        current_count += 1;
                        let is_last_child = current_count == non_empty_count;

                        let child_prefix = if i == 16 {
                            format!("{}[VALUE]", new_prefix)
                        } else {
                            format!("{}[{:x}]", new_prefix, i)
                        };

                        self.debug_print_node(child, &child_prefix, is_last_child);
                    }
                }

                if non_empty_count == 0 {
                    println!("{}    (no children)", new_prefix);
                }
            }
            Node::Hash(hash) => {
                println!("{}{}Hash: {:02x?}", prefix, connector, hash);
            }
        }
    }
}
