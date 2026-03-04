//! Secure trie identifier and builder implementation.

use alloy_primitives::B256;
use rust_eth_triedb_common::TrieDatabase;
use thiserror::Error;
use alloy_trie::EMPTY_ROOT_HASH;
use super::state_trie::StateTrie;
use super::node::DiffLayers;

// use super::state_trie::StateTrie;

/// Secure trie error types
#[derive(Debug, Error)]
pub enum SecureTrieError {
    /// Database operation error
    #[error("Database error: {0}")]
    Database(String),
    /// RLP encoding/decoding error
    #[error("RLP encoding error: {0}")]
    Rlp(#[from] alloy_rlp::Error),
    /// Node not found in trie
    #[error("Node not found")]
    NodeNotFound,
    /// Invalid node data
    #[error("Invalid node")]
    InvalidNode,
    /// Trie already committed
    #[error("Trie already committed")]
    AlreadyCommitted,
    /// Invalid account data
    #[error("Invalid account data")]
    InvalidAccount,
    /// Invalid storage data
    #[error("Invalid storage data")]
    InvalidStorage,
}

/// A unique identifier for a secure trie instance.
///
/// `SecureTrieId` uniquely identifies a specific state trie by combining the
/// state root hash with an optional owner address. This structure is used
/// to distinguish between different trie instances, particularly when managing
/// multiple tries (e.g., state trie and storage tries for different accounts).
///
/// The identifier is essential for:
/// - Tracking the root state of a trie at a specific point in time
/// - Distinguishing between different trie instances in a multi-trie system
/// - Supporting secure trie operations where each trie has a unique identity
///
/// # Field Descriptions
///
/// - `state_root`: The root hash of the trie's state. This uniquely identifies
///   the complete state of the trie at a given point. For an empty trie, this
///   is set to `EMPTY_ROOT_HASH`.
/// - `owner`: An optional owner address that can be used to associate the trie
///   with a specific account or entity. For the main state trie, this is typically
///   `B256::ZERO`. For storage tries, this would be the account address hash.
///
/// # Usage
///
/// This identifier is used when creating or accessing a `StateTrie` instance,
/// allowing the system to correctly identify and load the appropriate trie state
/// from the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecureTrieId {
    /// The root hash of the trie's state.
    ///
    /// This `B256` value uniquely identifies the complete state of the trie.
    /// Any change to the trie's contents will result in a different state root,
    /// making it an ideal identifier for versioning and state tracking.
    ///
    /// For an empty trie, this should be set to `EMPTY_ROOT_HASH`.
    pub state_root: B256,
    
    /// The owner address associated with this trie.
    ///
    /// This field can be used to associate the trie with a specific account or
    /// entity. For the main Ethereum state trie, this is typically `B256::ZERO`.
    /// For account storage tries, this would be the Keccak-256 hash of the
    /// account address.
    ///
    /// The owner field enables the system to distinguish between multiple tries
    /// that might share the same state root but belong to different contexts.
    pub owner: B256,
}

impl Default for SecureTrieId {
    fn default() -> Self {
        Self {
            state_root: EMPTY_ROOT_HASH,
            owner: B256::ZERO,
        }
    }
}

impl SecureTrieId {
    /// Creates a new SecureTrieId with the given state root
    pub fn new(state_root: B256) -> Self {
        Self {
            state_root,
            owner: B256::ZERO,
        }
    }

    /// Sets the owner address for this trie identifier
    pub fn with_owner(mut self, owner: B256) -> Self {
        self.owner = owner;
        self
    }

}

/// Secure trie builder for constructing secure tries
#[derive(Debug)]
pub struct SecureTrieBuilder<DB> {
    #[allow(dead_code)]
    database: DB,
    id: Option<SecureTrieId>,
}

impl<DB> SecureTrieBuilder<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Creates a new secure trie builder
    pub fn new(database: DB) -> Self {
        Self {
            database,
            id: None,
        }
    }

    /// Sets the trie identifier
    pub fn with_id(mut self, id: SecureTrieId) -> Self {
        self.id = Some(id);
        self
    }

    /// Builds the secure trie with difflayer
    pub fn build_with_difflayer(self, difflayer: Option<&DiffLayers>) -> Result<StateTrie<DB>, SecureTrieError> {
        let id = self.id.unwrap_or_default();
        StateTrie::new(id, self.database, difflayer)
    }
}
