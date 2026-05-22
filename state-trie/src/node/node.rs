//! Main node types and traits for trie operations.
//!
//! This module contains the core Node enum and NodeFlag
//! structure that are shared across all node implementations.

#[allow(unused_imports)]
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable, Error as RlpError};
use std::sync::{Arc, OnceLock};
use alloy_primitives::B256;

use super::{FullNode, ShortNode};
use super::rlp_raw::*;

/// Node flags for caching and dirty state
#[derive(Debug, Clone, PartialEq)]
pub struct NodeFlag {
    /// Cached hash of the node
    pub hash: Option<HashNode>,
    /// Whether the node has been modified
    pub dirty: bool,
    /// RLP-encoded bytes of this node, populated by Hasher when the node
    /// is hashed (>= 32 bytes). Consumed by Committer::store to avoid a
    /// second encode pass. None for embedded nodes (< 32 bytes).
    pub encoded: Option<Vec<u8>>,
}

impl Default for NodeFlag {
    fn default() -> Self {
        Self {
            hash: None,
            dirty: true,
            encoded: None,
        }
    }
}

impl NodeFlag {
    /// Sets the dirty flag and returns self for chaining
    pub fn with_dirty(mut self, dirty: bool) -> Self {
        self.dirty = dirty;
        self
    }
}

/// Hash node (reference to another node)
/// A hash node is a reference to another node by its hash, used for
/// efficient storage and retrieval in the trie.
pub type HashNode = B256;

/// Value node (leaf value)
/// A value node is a leaf node that contains the actual data stored
/// in the trie, representing the end of a trie path.
pub type ValueNode = Vec<u8>;

static EMPTY_ROOT_NODE: OnceLock<Arc<Node>> = OnceLock::new();

// Initialize the empty root node.
/// 
/// This function must be called once at application startup before any calls to `Node::empty_root()`.
/// Returns an error if the empty root node has already been initialized.
pub fn init_empty_root_node() {
    EMPTY_ROOT_NODE.get_or_init(|| Arc::new(Node::Empty));
}

/// Get the initialized empty root node instance.
/// 
/// This function returns a reference to the pre-initialized empty root node.
/// The empty root node must be initialized first by calling `init_empty_root_node()`.
/// 
/// # Panics
/// 
/// This function will panic if `init_empty_root_node()` has not been called first.
pub fn get_empty_root_node() -> &'static Arc<Node> {
    EMPTY_ROOT_NODE.get()
        .expect("Empty root node not initialized. Call init_empty_root_node() first.")
}

/// Node types in the ETH-style trie
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    /// Empty root node
    Empty,
    /// Full node with 17 children
    Full(Arc<FullNode>),
    /// Short node (extension or leaf)
    Short(Arc<ShortNode>),
    /// Hash node (reference to another node)
    Hash(HashNode),
    /// Value node (leaf value)
    Value(ValueNode),
}

impl Node {
    /// Get the empty root node
    pub fn empty_root() -> Arc<Node> {
        get_empty_root_node().clone()
    }

    /// Get the cached hash and dirty state
    pub fn cache(&self) -> (Option<HashNode>, bool) {
        match self {
            Node::Full(full) => return full.cache(),
            Node::Short(short) => return short.cache(),
            Node::Hash(_) => return (None, false),
            Node::Value(_) => return (None, false),
            Node::Empty => return (None, false),
        }
    }

    /// Encodes a node to RLP bytes.
    pub fn node_to_bytes(node: Arc<Node>) -> Vec<u8> {
        match node.as_ref() {
            Node::Full(full) => full.to_rlp(),
            Node::Short(short) => short.to_rlp(),
            Node::Hash(_) => panic!("Hash node should not be encoded"),
            Node::Value(_) => panic!("Value node should not be encoded"),
            Node::Empty => panic!("EmptyRoot should not be encoded"),
        }
    }

    /// Must decode node - panics on error
    pub fn must_decode_node(hash: Option<B256>, buf: &[u8]) -> Arc<Node> {
        Node::decode_node(hash, buf).unwrap_or_else(|e| {
            let hash_hex = hash.map(|h| format!("0x{:x}", h)).unwrap_or_else(|| "None".to_string());
            panic!("Failed to decode trie node: {:?}, hash: {}", e, hash_hex);
        })
    }

    /// Decodes an RLP-encoded trie node.
    pub fn decode_node(hash: Option<B256>, buf: &[u8]) -> Result<Arc<Node>, RlpError> {
        if buf.is_empty() {
            return Err(RlpError::InputTooShort);
        }

        let (elements, _) = split_list(buf)
            .map_err(|_| RlpError::Custom("Split list failed"))?;
        let element_count = count_values(elements)
            .map_err(|_| RlpError::Custom("Invalid elements count"))?;
        match element_count {
            2 => {
                let short_node = ShortNode::from_rlp(elements, hash)?;
                Ok(Arc::new(Node::Short(Arc::new(short_node))))
            }
            17 => {
                let full_node = FullNode::from_rlp(elements, hash)?;
                let node = Arc::new(Node::Full(Arc::new(full_node)));
                Ok(node)
            }
            _ => {
                Err(RlpError::Custom("Invalid number of list elements"))
            }
        }
    }

    /// Decodes a reference to a node and returns the decoded node and the remaining bytes.
    pub fn decode_ref(buf: &[u8]) -> Result<(Arc<Node>, &[u8]), RlpError> {
        let (kind, val, rest) = split(buf).map_err(|_| RlpError::Custom("split failed"))?;

        match kind {
            // Embedded node; ensure it's smaller than hash length
            Kind::List => {
                let consumed = buf.len().saturating_sub(rest.len());
                const HASH_LEN: usize = 32; // B256 length
                if consumed > HASH_LEN {
                    return Err(RlpError::Custom("oversized embedded node, wants 32 bytes, got more"));
                }
                let n = Node::decode_node(None, buf)?;
                Ok((n, rest))
            }
            // Empty reference
            Kind::String if val.is_empty() => {
                Ok((Node::empty_root(), rest))
            }
            // Hash reference
            Kind::String if val.len() == 32 => {
                let hash = B256::from_slice(val);
                Ok((Arc::new(Node::Hash(hash)), rest))
            }
            // Invalid string length
            _ => Err(RlpError::Custom("invalid RLP string size, want 0 or 32 bytes")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::ShortNode;
    use crate::encoding::*;

    #[test]
    fn fullnode_roundtrip_basic() {
        init_empty_root_node();

        // Build a simple full node with only value (17th) set
        let mut original = FullNode::new();
        let value_bytes = vec![0xAA, 0xBB, 0xCC];
        original.set_child(16, &Node::Value(value_bytes.clone()));

        // Encode to RLP
        let encoded = original.to_rlp();

        // Decode via high-level decode_node
        let decoded = Node::decode_node(None, &encoded).expect("decode_node should succeed");

        // Validate structure and important fields
        match decoded.as_ref() {
            Node::Full(full) => {
                // Children 0..=15 should be EmptyRoot by default
                for i in 0..16 {
                    assert!(matches!(full.get_child(i).as_ref(), Node::Empty));
                }
                // 17th child is value node and should match
                match full.get_child(16).as_ref() {
                    Node::Value(v) => assert_eq!(v, &value_bytes),
                    other => panic!("expected value node at index 16, got {:?}", other),
                }
            }
            other => panic!("expected full node, got {:?}", other),
        }
    }

    #[test]
    fn shortnode_roundtrip_basic() {
        // Build a short node: key is a nibble-terminated path, value is a byte string
        let key_bytes = vec![0x12, 0x34];
        let hex_key = key_to_nibbles(&key_bytes); // includes terminator
        let compact_key = hex_to_compact(&hex_key);

        let original = ShortNode::new(compact_key.clone(), &Node::Value(vec![0xDE, 0xAD, 0xBE, 0xEF]));

        // Encode to RLP
        let encoded = original.to_rlp();

        // Decode via high-level decode_node
        let decoded = Node::decode_node(None, &encoded).expect("decode_node should succeed");

        // Validate it is a Short node with same key and value
        match decoded.as_ref() {
            Node::Short(short) => {
                assert_eq!(short.key, hex_key);
                match short.get_value() {
                    Node::Value(v) => assert_eq!(v, &vec![0xDE, 0xAD, 0xBE, 0xEF]),
                    other => panic!("expected value node, got {:?}", other),
                }
            }
            other => panic!("expected short node, got {:?}", other),
        }
    }

    #[test]
    fn nested_roundtrip_complex() {

    }

    #[test]
    fn fullnode_child1_short_with_1byte_value() {
        init_empty_root_node();
        // Build leaf short with 1-byte value
        let hex_key = key_to_nibbles(&[0x0A]);
        let compact_key = hex_to_compact(&hex_key);
        let short = ShortNode::new(compact_key.clone(), &Node::Value(vec![0x01]));

        // Root full node with child1 = short
        let mut root = FullNode::new();
        root.set_child(1, &Node::Short(Arc::new(short)));

        let encoded = root.to_rlp();
        let decoded = Node::decode_node(None, &encoded).expect("decode_node should succeed");

        match decoded.as_ref() {
            Node::Full(full) => {
                match full.get_child(1).as_ref() {
                    Node::Short(s) => {
                        assert_eq!(s.key, hex_key);
                        match s.get_value() {
                            Node::Value(v) => assert_eq!(v, &vec![0x01]),
                            other => panic!("expected 1-byte value at child1, got {:?}", other),
                        }
                    }
                    other => panic!("expected short at child1, got {:?}", other),
                }
            }
            other => panic!("expected full node root, got {:?}", other),
        }
    }

    #[test]
    fn shortnode_with_fullnode_value_with_1byte_in_17th_child() {
        init_empty_root_node();
        // Inner full node with 17th (index 16) child = 1-byte value
        let mut inner_full = FullNode::new();
        inner_full.set_child(16, &Node::Value(vec![0x02]));

        // Extension short: key without terminator
        let mut dst = vec![0u8; 2];
        let hex_no_term = write_hex_key(&mut dst, &[0x0B]).to_vec();
        let compact_ext_key = hex_to_compact(&hex_no_term);

        let short = ShortNode::new(compact_ext_key.clone(), &Node::Full(Arc::new(inner_full)));

        let encoded = short.to_rlp();
        let decoded = Node::decode_node(None, &encoded).expect("decode_node should succeed");

        match decoded.as_ref() {
            Node::Short(s) => {
                assert_eq!(s.key, hex_no_term);
                match s.get_value() {
                    Node::Full(f) => match f.get_child(16).as_ref() {
                        Node::Value(v) => assert_eq!(v, &vec![0x02]),
                        other => panic!("expected 1-byte value in 17th child, got {:?}", other),
                    },
                    other => panic!("expected full node as short value, got {:?}", other),
                }
            }
            other => panic!("expected short node root, got {:?}", other),
        }
    }
}