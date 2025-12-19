//! Node module for BSC-style trie nodes
//!
//! This module contains the implementation of different node types used in the trie:
//! - `Node`: Main enum containing all node types
//! - `FullNode`: Node with 17 children (16 hex digits + value)
//! - `ShortNode`: Extension or leaf node with key and value
//! - `HashNode`: Reference to another node by hash
//! - `ValueNode`: Leaf node containing actual data
//! - `NodeFlag`: Flags for caching and dirty state
//! - `NodeSet`: Collection of modified nodes during commit operations


/// Node types
pub mod full_node;
pub mod node;
pub mod node_set;
pub mod short_node;

// Node encoding/decoding utilities
pub mod rlp_raw;

// Re-export main types
pub use full_node::FullNode;
pub use node::{HashNode, Node, NodeFlag, ValueNode, init_empty_root_node, get_empty_root_node};
pub use node_set::{NodeSet, MergedNodeSet};
pub use short_node::ShortNode;
// Re-export TrieNode, DiffLayer, DiffLayers from common crate
pub use crate::{TrieNode, DiffLayer, DiffLayers};
