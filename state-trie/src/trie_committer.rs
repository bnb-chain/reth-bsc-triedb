//! Trie committer is used for the trie commit operation.
//! It captures all dirty nodes during commit and keeps them cached in insertion order.
//! It is used to collect all the nodes that are modified during the commit operation.
//! It is also used to collect all the leaves that are modified during the commit operation.
//! It is used to collect all the nodes that are deleted during the commit operation.
//! It is used to collect all the nodes that are inserted during the commit operation.
//! It is used to collect all the nodes that are updated during the commit operation.

use std::sync::{Arc, Mutex};

use alloy_primitives::B256;

use crate::node::{Node, FullNode, NodeSet, TrieNode};
use crate::trie_tracer::TrieTracer;
use crate::encoding::hex_to_compact;

/// A buffered write produced inside `Committer::store` and drained into
/// the shared `NodeSet` exactly once by `Committer::flush`.
#[derive(Debug)]
struct PendingWrite {
    path: Vec<u8>,
    node: Arc<TrieNode>,
    /// `Some` iff `collect_leaf` is on and this node is a Short with a Value child.
    leaf: Option<(B256, Vec<u8>)>,
}

/// Committer is used for the trie commit operation.
/// It captures all dirty nodes during commit and keeps them cached in insertion order.
#[derive(Debug)]
pub struct Committer<'a> {
    pub nodes: Arc<Mutex<NodeSet>>,
    pub tracer: &'a TrieTracer,
    pub collect_leaf: bool,
    /// Per-Committer buffer flushed in `commit()` to amortize NodeSet mutex
    /// acquisition and HashMap resizes. Each Committer (root or per-branch
    /// child in parallel commit) owns its own buffer.
    pending: Vec<PendingWrite>,
}

impl<'a> Committer<'a> {
    /// Creates a new committer.
    pub fn new(nodeset: Arc<Mutex<NodeSet>>, tracer: &'a TrieTracer, collect_leaf: bool) -> Self {
        Self {
            nodes: nodeset,
            tracer,
            collect_leaf,
            pending: Vec::with_capacity(4096),
        }
    }

    /// Commit a node and return the hash of the committed node.
    pub fn commit(&mut self, node: Arc<Node>, parallel: bool) -> Arc<Node> {
        let node = self.commit_internal(vec![], node, parallel);
        self.flush();
        match node.as_ref() {
            Node::Hash(_) => node,
            _ => panic!("Node is not a hash"),
        }
    }

    /// Drain `self.pending` into the shared NodeSet under a single lock.
    ///
    /// Called once at the end of `commit` for the root Committer, and once
    /// per child Committer in `commit_children` before its NodeSet is moved
    /// into the parent via `merge_set_owned`.
    fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut nodeset = self.nodes.lock().unwrap();
        for w in self.pending.drain(..) {
            nodeset.add_node(w.path.as_slice(), w.node);
            if let Some((hash, blob)) = w.leaf {
                nodeset.add_leaf(hash, blob);
            }
        }
    }
}

impl<'a> Committer<'a> {
    /// Recursively commits the subtree rooted at `node`.
    fn commit_internal(
        &mut self,
        path: Vec<u8>,
        node: Arc<Node>,
        parallel: bool) -> Arc<Node> {

        let (hash_opt, dirty) = node.cache();
        if let (Some(hash), false) = (hash_opt, dirty) {
            // Node already has a cached hash and is not dirty → return hash node directly
            return Arc::new(Node::Hash(hash));
        }

        match node.as_ref() {
            Node::Short(short) => {
                let mut collapsed = short.to_mutable_copy_with_cow();

                if let Node::Full(_) = short.val.as_ref() {
                    let mut path_ext = path.clone();
                    path_ext.extend(short.key.as_slice());

                    collapsed.val = self.commit_internal(
                        path_ext,
                        short.val.clone(),
                        false);
                }

                collapsed.key = hex_to_compact(short.key.as_slice());

                // Single Arc — store() returns either the committed hash node
                // or this exact Arc for embedded nodes. Either way, no rewrap.
                let node_arc = Arc::new(Node::Short(Arc::new(collapsed)));
                self.store(path, node_arc)
            }
            Node::Full(full) => {
                let hashed_children = self.commit_children(
                    path.clone(),
                    full.clone(),
                    parallel);

                let mut collapsed = full.to_mutable_copy_with_cow();
                collapsed.children = hashed_children;

                let node_arc = Arc::new(Node::Full(Arc::new(collapsed)));
                self.store(path, node_arc)
            }
            Node::Hash(_) => {
                node
            }
            _ => {
                panic!("Node is not a short or full node to commit");
            }
        }
    }

    /// Commit the children of a full node.
    #[allow(dead_code)]
    fn commit_children(
        &mut self,
        path: Vec<u8>,
        full: Arc<FullNode>,
        parallel: bool,
    ) -> [Arc<Node>; 17] {
        let mut children: [Arc<Node>; 17] = std::array::from_fn(|_| Node::empty_root());

        if parallel {
            use rayon::prelude::*;

            let collect_leaf = self.collect_leaf;
            let owner = {
                let guard = self.nodes.lock().unwrap();
                guard.owner
            };

            // Perform child commits in parallel, collecting their resulting node and NodeSet
            let results: Vec<(usize, Arc<Node>)> = (0usize..16)
                .into_par_iter()
                .filter_map(|i| {
                    let child = full.children[i].clone();
                    if matches!(child.as_ref(), Node::Empty) {
                        return Some((i, Node::empty_root()));
                    }

                    // Local nodeset & committer for the child branch
                    let child_set = Arc::new(Mutex::new(NodeSet::new(owner)));
                    let mut child_committer = Committer::new(
                        child_set,
                        self.tracer,
                        collect_leaf);

                    let mut path_child = path.clone();
                    path_child.push(i as u8);

                    let committed_child = child_committer
                        .commit_internal(
                            path_child,
                            child,
                            false);

                    // Flush this child's local buffer into its NodeSet before
                    // we move the NodeSet into the parent.
                    child_committer.flush();

                    // Move the child NodeSet into the parent without cloning
                    // each entry. `std::mem::take` leaves a Default NodeSet
                    // behind (owner=ZERO), but `child_committer` is dropped
                    // at the end of this closure so that is harmless.
                    {
                        let mut child_lock = child_committer.nodes.lock().unwrap();
                        let child_owned: NodeSet = std::mem::take(&mut *child_lock);
                        drop(child_lock);
                        let mut nodeset_parent = self.nodes.lock().unwrap();
                        nodeset_parent.merge_set_owned(child_owned)
                            .expect("owner mismatch while merging nodesets");
                    }
                    Some((i, committed_child))
                })
                .collect();

            for (i, committed_child) in results {
                children[i] = committed_child;
            }
        } else {
            for i in 0..16 {
                if let Node::Empty = full.children[i].as_ref() {
                    continue;
                }

                let mut path_child = path.clone();
                path_child.push(i as u8); // i is a hex digit, so it's 1 byte

                children[i] = self.commit_internal(
                    path_child,
                    full.children[i].clone(),
                    false);
            }
        }


        if let Node::Value(_) = full.children[16].as_ref() {
            children[16] = full.children[16].clone();
        }

        children
    }



    /// Store the node and add it to the modified nodeset.
    /// If leaf collection is enabled, leaf nodes will be tracked in the modified nodeset as well.
    ///
    /// Writes are buffered into `self.pending` and drained by `flush()` at
    /// the end of commit. The RLP-encoded bytes are reused from the Hasher's
    /// `flags.encoded` cache when present, falling back to `node_to_bytes`.
    fn store(&mut self, path: Vec<u8>, node: Arc<Node>) -> Arc<Node> {
        let (hash, _) = node.cache();

        if hash.is_none() {
            if self.tracer.access_list().contains_key(path.as_slice()) {
                self.pending.push(PendingWrite {
                    path,
                    node: Arc::new(TrieNode::default()),
                    leaf: None,
                });
            }
            return node;
        }

        // Reuse the bytes the Hasher already produced. Fall back to a fresh
        // encode only on the unusual path where the cache was not populated.
        let node_bytes = match node.as_ref() {
            Node::Short(short) => short.flags.encoded.clone()
                .unwrap_or_else(|| Node::node_to_bytes(node.clone())),
            Node::Full(full) => full.flags.encoded.clone()
                .unwrap_or_else(|| Node::node_to_bytes(node.clone())),
            _ => Node::node_to_bytes(node.clone()),
        };

        let leaf = if self.collect_leaf {
            if let Node::Short(short) = node.as_ref() {
                if let Node::Value(value) = short.val.as_ref() {
                    Some((hash.unwrap(), value.clone()))
                } else { None }
            } else { None }
        } else { None };

        self.pending.push(PendingWrite {
            path,
            node: Arc::new(TrieNode::new(hash, Some(node_bytes))),
            leaf,
        });

        Arc::new(Node::Hash(hash.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{init_empty_root_node, ShortNode};

    /// When `flags.encoded` is `None` on the input (e.g. a Committer used
    /// without running the Hasher first), `store` must still produce correct
    /// blob bytes by falling back to `node_to_bytes`. This guards the legacy
    /// path C-2 leaves in place as a safety net.
    #[test]
    fn committer_falls_back_when_encoded_absent() {
        init_empty_root_node();

        // Build a Short with a hash already set but no encoded cache.
        let key = vec![0x20u8, 0xAB, 0xCD];
        let val = vec![0xEEu8; 80];
        let short = ShortNode::new(key, &Node::Value(val));
        let mut short_mut = short.to_mutable_copy_with_cow();
        short_mut.flags.hash = Some(B256::repeat_byte(0x99));
        short_mut.flags.encoded = None;
        let arc = Arc::new(Node::Short(Arc::new(short_mut)));

        let tracer = TrieTracer::new();
        let nodes = Arc::new(Mutex::new(NodeSet::new(B256::ZERO)));
        let mut committer = Committer::new(nodes.clone(), &tracer, false);

        let _hn = committer.store(vec![0xAA], arc);
        // Without flush, pending holds the write.
        assert_eq!(committer.pending.len(), 1);
        let written_blob = committer.pending[0].node.blob.as_ref().expect("blob should be set");
        assert!(!written_blob.is_empty(), "fallback should produce non-empty bytes");

        committer.flush();
        let guard = nodes.lock().unwrap();
        assert_eq!(guard.size().0, 1, "should have stored exactly 1 node via fallback");
    }
}
