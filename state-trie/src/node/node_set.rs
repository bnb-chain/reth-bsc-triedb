//! Node set implementation for tracking modified trie nodes during commit operations.
//!
//! This module provides functionality for collecting and managing nodes that
//! have been modified during trie operations, enabling efficient batch commits.

use std::sync::Arc;
use std::collections::HashMap;

use alloy_primitives::B256;
use rust_eth_triedb_common::{Leaf, TrieNode};
use crate::encoding;

/// NodeSet contains a set of nodes collected during the commit operation.
/// Each node is keyed by path. It's not thread-safe to use.
#[derive(Clone)]
pub struct NodeSet {
    /// Owner hash (zero for account trie, account address hash for storage tries)
    pub owner: B256,
    /// Leaf nodes
    leaves: Vec<Arc<Leaf>>,
    /// Node map keyed by path
    pub nodes: HashMap<String, Arc<TrieNode>>,
    /// Count of updated and inserted nodes
    pub updates: usize,
    /// Count of deleted nodes
    pub deletes: usize,
    /// Diff layer
    pub difflayer: Box<HashMap<Vec<u8>, Arc<TrieNode>>>,
}

impl NodeSet {
    /// Creates a new node set
    pub fn new(owner: B256) -> Self {
        Self {
            owner,
            leaves: Vec::new(),
            nodes: HashMap::new(),
            updates: 0,
            deletes: 0,
            difflayer: Box::new(HashMap::new()),
        }
    }

    /// Adds a node to the set
    pub fn add_node(&mut self, path: &[u8], node: Arc<TrieNode>) {
        let path_str = String::from_utf8_lossy(path).to_string();

        // Add the new node
        if node.is_deleted() {
            self.deletes += 1;
        } else {
            self.updates += 1;
        }

        if self.owner == B256::ZERO {
            self.difflayer.insert(encoding::account_trie_node_key(path), node.clone());
        } else {
            self.difflayer.insert(encoding::storage_trie_node_key(self.owner.as_slice(), path), node.clone());
        }
        self.nodes.insert(path_str, node);
    }

    /// Adds a leaf node to the set
    pub fn add_leaf(&mut self, parent: B256, blob: Vec<u8>) {
        self.leaves.push(Arc::new(Leaf { blob, parent }));
    }

    /// Returns the number of dirty nodes in the set
    pub fn size(&self) -> (usize, usize) {
        (self.updates, self.deletes)
    }

    /// Returns the number of collected leaf blobs
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Returns a reference to the nodes map
    pub fn nodes(&self) -> &HashMap<String, Arc<TrieNode>> {
        &self.nodes
    }

    /// MergeSet merges this 'set' with 'other'. It assumes that the sets are disjoint,
    /// and thus does not deduplicate data (count deletes, dedup leaves etc).
    pub fn merge_set(&mut self, other: &NodeSet) -> Result<(), String> {
        if self.owner != other.owner {
            return Err(format!(
                "nodesets belong to different owner are not mergeable {:?}-{:?}",
                self.owner, other.owner
            ));
        }

        self.nodes.extend(other.nodes.clone());
        self.leaves.extend(other.leaves.clone());
        self.updates += other.updates;
        self.deletes += other.deletes;
        // Merge difflayer as well
        for (key, node) in other.difflayer.iter() {
            self.difflayer.insert(key.clone(), node.clone());
        }

        Ok(())
    }

    /// Returns true if the node set is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.leaves.is_empty()
    }

    /// Clears all nodes and leaves
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.leaves.clear();
        self.difflayer.clear();
        self.updates = 0;
        self.deletes = 0;
    }

    /// Calculates a deterministic hash of the entire `NodeSet` contents.
    pub fn signature(&self) -> B256 {
        use alloy_primitives::{keccak256};

        let mut buf: Vec<u8> = Vec::new();

        // 1. owner
        buf.extend_from_slice(self.owner.as_slice());

        // 2. leaves (sorted)
        let mut leaves_sorted = self.leaves.clone();
        leaves_sorted.sort_by(|a, b| {
            let cmp_parent = a.parent.cmp(&b.parent);
            if cmp_parent == std::cmp::Ordering::Equal {
                a.blob.cmp(&b.blob)
            } else {
                cmp_parent
            }
        });

        for leaf in leaves_sorted {
            // parent
            buf.extend_from_slice(leaf.parent.as_slice());
            // blob data
            buf.extend_from_slice(&leaf.blob);
        }

        // 3. nodes (sorted by key)
        let mut nodes_sorted: Vec<(&String, &Arc<TrieNode>)> = self.nodes.iter().collect();
        nodes_sorted.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        for (key, node) in nodes_sorted {
            // key length and bytes
            let key_bytes = key.as_bytes();
            buf.extend_from_slice(key_bytes);

            // hash field
            match node.hash {
                Some(h) => {
                    buf.push(1u8);
                    buf.extend_from_slice(h.as_slice());
                }
                None => {}
            }

            // blob field
            match &node.blob {
                Some(b) => {
                    buf.push(1u8);
                    buf.extend_from_slice(b);
                }
                None => {},
            }
        }

        // 4. updates & deletes
        buf.extend_from_slice(&(self.updates as u64).to_be_bytes());
        buf.extend_from_slice(&(self.deletes as u64).to_be_bytes());

        // 5. hash
        keccak256(&buf)
    }
}

impl std::fmt::Debug for NodeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== NodeSet Debug Info ===")?;
        writeln!(f, "Owner: {:?}", self.owner)?;
        writeln!(f, "Total nodes: {} (updates: {}, deletes: {})", self.nodes.len(), self.updates, self.deletes)?;
        
        if !self.leaves.is_empty() {
            writeln!(f, "Leaves ({}):", self.leaves.len())?;
            for (i, leaf) in self.leaves.iter().enumerate() {
                writeln!(f, "  [{}] Parent: {:?}, Blob size: {}", i, leaf.parent, leaf.blob.len())?;
            }
        }
        
        if !self.nodes.is_empty() {
            writeln!(f, "Nodes:")?;
            let mut paths: Vec<_> = self.nodes.keys().collect();
            paths.sort();
            
            for path in paths {
                if let Some(node) = self.nodes.get(path) {
                    if node.is_deleted() {
                        writeln!(f, "  Path: {:x?} -> DELETED", path.as_bytes())?;
                    } else {
                        let hash_str = match node.hash {
                            Some(h) => format!("{:?}", h),
                            None => "None".to_string(),
                        };
                        let blob_size = node.blob.as_ref().map(|b| b.len()).unwrap_or(0);
                        writeln!(f, "  Path: {:x?} -> Hash: {}, Blob size: {}", 
                            path.as_bytes(), hash_str, blob_size)?;
                    }
                }
            }
        }
        writeln!(f, "=== End NodeSet Debug ===")
    }
}


/// MergedNodeSet is a set of node sets that are merged together.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MergedNodeSet {
    pub sets: HashMap<B256, Arc<NodeSet>>,
    pub difflayer: HashMap<Vec<u8>, Arc<TrieNode>>,
}

impl MergedNodeSet {
    /// Create a new merged node set
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self { sets: HashMap::new(), difflayer: HashMap::new() }
    }

    /// Merge a node set into the merged set
    #[allow(dead_code)]
    pub fn merge(&mut self, other: Arc<NodeSet>) -> Result<(), String> {
        if self.sets.contains_key(&other.owner) {
            return Err(format!("repeated nodeset to merge, owner: {:?} already exists", other.owner));
        }
        self.sets.insert(other.owner, other.clone());
        self.difflayer.extend(other.difflayer.iter().map(|(key, node)| (key.clone(), node.clone())));
        Ok(())
    }

    /// Convert the merged node set to a difflayer, consuming self
    pub fn to_diff_nodes(&self) -> Arc<HashMap<Vec<u8>, Arc<TrieNode>>> {
        Arc::new(self.difflayer.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b256(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn make_node(hash_byte: u8, blob_bytes: &[u8]) -> Arc<TrieNode> {
       Arc::new(TrieNode::new(Some(b256(hash_byte)), Some(blob_bytes.to_vec())))
    }

    #[test]
    fn nodeset_add_and_size() {
        let mut set = NodeSet::new(B256::ZERO);
        assert_eq!(set.size(), (0, 0));

        set.add_node(b"abc", make_node(1, b"v1"));
        set.add_node(b"def", Arc::new(TrieNode::new(Some(B256::ZERO), Some(Vec::new())))); // deleted
        assert_eq!(set.size(), (1, 1));
        assert_eq!(set.nodes().len(), 2);
    }
}
