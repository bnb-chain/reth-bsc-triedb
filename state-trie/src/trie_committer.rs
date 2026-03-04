//! Trie committer is used for the trie commit operation.
//! It captures all dirty nodes during commit and keeps them cached in insertion order.
//! It is used to collect all the nodes that are modified during the commit operation.
//! It is also used to collect all the leaves that are modified during the commit operation.
//! It is used to collect all the nodes that are deleted during the commit operation.
//! It is used to collect all the nodes that are inserted during the commit operation.
//! It is used to collect all the nodes that are updated during the commit operation.

use std::sync::{Arc, Mutex};

use crate::node::{Node, FullNode, NodeSet, TrieNode};
use crate::trie_tracer::TrieTracer;
use crate::encoding::hex_to_compact;

/// Committer is used for the trie commit operation.
/// It captures all dirty nodes during commit and keeps them cached in insertion order.
#[derive(Debug)]
pub struct Committer<'a> {
    pub nodes: Arc<Mutex<NodeSet>>,
    pub tracer: &'a TrieTracer,
    pub collect_leaf: bool,
}

impl<'a> Committer<'a> {
    /// Creates a new committer.
    pub fn new(nodeset: Arc<Mutex<NodeSet>>, tracer: &'a TrieTracer, collect_leaf: bool) -> Self {
        Self { nodes: nodeset, tracer, collect_leaf }
    }

    /// Commit a node and return the hash of the committed node.
    pub fn commit(&mut self, node: Arc<Node>, parallel: bool) -> Arc<Node> {
        let node = self.commit_internal(vec![], node, parallel);
        match node.as_ref() {
            Node::Hash(_) => {
                node
            }
            _ => panic!("Node is not a hash"),
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

                let hn = self.store(
                    path.clone(), 
                    Arc::new(Node::Short(Arc::new(collapsed.clone()))));

                if let Node::Hash(hash) = hn.as_ref() {
                    let committed_node = Arc::new(Node::Hash(*hash));
                    return committed_node;
                }

                Arc::new(Node::Short(Arc::new(collapsed)))
            }
            Node::Full(full) => {
                let hashed_children = self.commit_children(
                    path.clone(), 
                    full.clone(), 
                    parallel);

                let mut collapsed = full.to_mutable_copy_with_cow();
                collapsed.children = hashed_children;

                let hn = self.store(
                    path.clone(), 
                    Arc::new(Node::Full(Arc::new(collapsed.clone()))));

                if let Node::Hash(hash) = hn.as_ref() {
                    return Arc::new(Node::Hash(*hash));
                }

                Arc::new(Node::Full(Arc::new(collapsed)))
            }
            Node::Hash(_) => {
                node
            }
            _ => {
                panic!("Node is not a short or full node to commit");
            }
        }
    }

    /// Commit the children of a full node (placeholder, not yet implemented).
    #[allow(dead_code)]
    fn commit_children(
        &mut self,
        path: Vec<u8>,
        full: Arc<FullNode>,
        parallel: bool,
    ) -> [Arc<Node>; 17] {
        let mut children: [Arc<Node>; 17] = std::array::from_fn(|_| Node::empty_root());

        // Only use parallel commit when there are enough non-empty children
        // to justify the rayon task scheduling overhead.
        let non_empty_count = if parallel {
            full.children.iter().take(16).filter(|c| !matches!(c.as_ref(), Node::Empty)).count()
        } else {
            0
        };

        if parallel && non_empty_count >= 2 {
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

                     // Build child path
                    let mut path_child = path.clone();
                    path_child.push(i as u8);
                    
                    let committed_child = child_committer
                        .commit_internal(
                            path_child, 
                            child, 
                            false);
                    
                    {
                        let nodeset = child_committer.nodes.lock().unwrap();
                        let mut nodeset_parent = self.nodes.lock().unwrap();
                        nodeset_parent.merge_set(&nodeset)
                            .expect("owner mismatch while merging nodesets");
                    }
                    Some((i, committed_child))
                })
                .collect();

            for (i, committed_child) in results {
                children[i] = committed_child;
            }
        } else {
            for (i, child) in full.children.iter().enumerate().take(16) {
                if let Node::Empty = child.as_ref() {
                    continue;
                }

                let mut path_child = path.clone();
                path_child.push(i as u8); // i is a hex digit, so it's 1 byte

                children[i] = self.commit_internal(
                    path_child,
                    child.clone(),
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
    fn store(&mut self, path: Vec<u8>, node: Arc<Node>) -> Arc<Node> {
        let (hash, _) = node.cache();

        if hash.is_none() {
            if self.tracer.access_list().contains_key(path.as_slice()) {
                let mut nodeset = self.nodes.lock().unwrap();
                nodeset.add_node(path.as_slice(), Arc::new(TrieNode::default()));
            }
            return node;
        }

        {
            let node_bytes = Node::node_to_bytes(node.clone());
            let mut nodeset = self.nodes.lock().unwrap();
            nodeset.add_node(path.as_slice(), Arc::new(TrieNode::new(hash, Some(node_bytes))));
        }

        if self.collect_leaf {
            if let Node::Short(short) = node.as_ref() {
                if let Node::Value(value) = short.val.as_ref() {
                    let mut nodeset = self.nodes.lock().unwrap();
                    nodeset.add_leaf(hash.unwrap(), value.clone());
                }
            }
        }
        
        Arc::new(Node::Hash(hash.unwrap()))
    }
}

