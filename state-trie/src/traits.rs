//! Traits for secure trie operations.

use std::{sync::Arc};

use alloy_primitives::{Address, B256, U256};
use super::account::StateAccount;
use super::node::{NodeSet};

/// Error type for secure trie operations
pub type SecureTrieError = super::secure_trie::SecureTrieError;

/// A trait defining the interface for secure trie operations.
///
/// `SecureTrieTrait` provides a unified abstraction for interacting with Ethereum-compatible
/// state tries that use secure key hashing. This trait defines all operations needed to
/// manage accounts and storage in an Ethereum-style state trie, where all keys are
/// hashed using Keccak-256 before being stored.
///
/// This trait is designed to be compatible with Ethereum's state trie specification
/// and can be used with Ethereum-compatible blockchain networks, including BSC and
/// other EVM-compatible chains.
///
/// # Key Features
///
/// - **Account Management**: Get, update, and delete Ethereum accounts from the state trie
/// - **Storage Management**: Read and write storage values for accounts with automatic
///   key hashing
/// - **Hash-based Operations**: Support for operations using pre-hashed keys, which can
///   be more efficient when keys are already hashed
/// - **State Persistence**: Commit operations to persist state changes and obtain the
///   modified node set for database updates
///
/// # Method Categories
///
/// The trait provides two sets of methods:
///
/// 1. **Address-based methods**: Operations that take raw `Address` or `&[u8]` keys,
///    which are automatically hashed internally using Keccak-256.
///
/// 2. **Hash-based methods**: Operations that take pre-hashed `B256` keys, allowing
///    for more efficient operations when keys are already hashed (e.g., when working
///    with storage roots or when keys have been pre-computed).
///
/// # Type Parameters
///
/// * `Error` - The error type returned by trie operations. Implementations should
///   define their own error types that capture operation-specific failures.
///
/// # Thread Safety
///
/// Methods in this trait take `&mut self`, indicating that implementations are not
/// inherently thread-safe. Concurrent access should be protected by appropriate
/// synchronization primitives.
pub trait SecureTrieTrait {
    /// Associated error type
    type Error;

    /// Returns the trie identifier
    fn id(&self) -> &super::secure_trie::SecureTrieId;

    /// Gets an account from the trie by address
    fn get_account(&mut self, address: Address) -> Result<Option<StateAccount>, Self::Error>;

    /// Updates an account in the trie by address
    fn update_account(&mut self, address: Address, account: &StateAccount) -> Result<(), Self::Error>;

    /// Deletes an account from the trie by address
    fn delete_account(&mut self, address: Address) -> Result<(), Self::Error>;

    /// Gets storage value for an account by key
    fn get_storage(&mut self, address: Address, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Updates storage value for an account by key
    fn update_storage(&mut self, address: Address, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Deletes storage value for an account by key
    fn delete_storage(&mut self, address: Address, key: &[u8]) -> Result<(), Self::Error>;

    /// Gets an account from the trie by hash state
    fn get_account_with_hash_state(&mut self, hashed_address: B256) -> Result<Option<StateAccount>, Self::Error>;

    /// Touches an account by hash states
    fn touch_account_with_hash_state(&mut self, hashed_address: B256) -> Result<(), Self::Error>;

    /// Updates an account in the trie by hash state
    fn update_account_with_hash_state(&mut self, hashed_address: B256, account: &StateAccount) -> Result<(), Self::Error>;

    /// Deletes an account from the trie by hash state
    fn delete_account_with_hash_state(&mut self, hashed_address: B256) -> Result<(), Self::Error>;

    /// Gets storage value for an account by key and hash state
    fn get_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Touches an account by hash state
    fn touch_storage_with_hash_state(&mut self, hashed_key: B256) -> Result<(), Self::Error>;

    /// Updates storage value for an account by key and hash state
    fn update_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256, value: &[u8]) -> Result<(), Self::Error>;

    /// Deletes storage value for an account by key and hash state
    fn delete_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<(), Self::Error>;

    /// Updates storage value for an account by key and hash state
    fn update_storage_u256_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256, value: U256) -> Result<(), Self::Error>;

    /// Gets storage value for an account by key and hash state
    fn get_storage_u256_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<Option<U256>, Self::Error>;

    /// Returns the current root hash of the trie
    fn hash(&mut self) -> B256;

    /// Commits the trie and returns the root hash and modified node set
    ///
    /// The `collect_leaf` parameter determines whether to include leaf nodes in the returned node set.
    /// The returned `NodeSet` contains all modified nodes that need to be persisted to disk.
    fn commit(&mut self, collect_leaf: bool) -> Result<(B256, Option<Arc<NodeSet>>), Self::Error>;
}
