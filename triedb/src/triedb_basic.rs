//! Basic operations for TrieDB.

use alloy_primitives::{keccak256, Address, B256};
use alloy_trie::EMPTY_ROOT_HASH;
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieTrait, SecureTrieBuilder};

use crate::triedb::{TrieDB, TrieDBError};

/// Geth-compatible interface functions for TrieDB.
///
/// # Write Operations (Batch Only)
///
/// **Important**: `TrieDB` only supports batch write operations. Individual storage 
/// key-value write operations are **not supported** and will not persist correctly.
///
/// All write operations must be performed through one of the following batch methods:
/// - [`batch_update_and_commit`](crate::triedb_reth::TrieDB::batch_update_and_commit) - 
///   Batch update accounts and storage, then commit all changes atomically
/// - [`commit_hashed_post_state`](crate::triedb_reth::TrieDB::commit_hashed_post_state) - 
///   Commit a complete post-state with all account and storage changes
///
/// # Read Operations (Public API)
///
/// The query functions (`get_account`, `get_storage`) are **public and safe to use**.
/// They support:
/// - Reading account data from the state trie
/// - Reading storage values from account storage tries
/// - **Pre-warming**: These functions can be used to preload and cache frequently
///   accessed tries into memory, improving subsequent batch operation performance.
///   When you call `get_account` or `get_storage`, the underlying tries are loaded
///   and cached, which helps optimize batch operations that access the same data.
///
/// # Usage Pattern
///
/// ```ignore
/// // ✅ Correct: Use batch operations for writes
/// triedb.batch_update_and_commit(root_hash, difflayer, accounts, rebuild_set, storage)?;
///
/// // ✅ Correct: Use query functions for reads and pre-warming
/// let account = triedb.get_account(address)?;
/// let storage_value = triedb.get_storage(address, key)?;
///
/// // ❌ Incorrect: Do not use individual write functions
/// // triedb.update_storage(address, key, value)?;  // Will not correct!
/// ```
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub fn get_account(&mut self, address: Address) -> Result<Option<StateAccount>, TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().get_account(address)?)
    }

    pub fn update_account(&mut self, address: Address, account: &StateAccount) -> Result<(), TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().update_account(address, account)?)
    }

    pub fn delete_account(&mut self, address: Address) -> Result<(), TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().delete_account(address)?)
    }

    pub fn get_storage(&mut self, address: Address, key: &[u8]) -> Result<Option<Vec<u8>>, TrieDBError> {
        let mut storage_trie = self.get_storage_trie(address)?;
        Ok(storage_trie.get_storage(address, key)?)
    }

    #[allow(dead_code)]
    fn update_storage(&mut self, address: Address, key: &[u8], value: &[u8]) -> Result<(), TrieDBError> {
        let mut storage_trie = self.get_storage_trie(address)?;
        Ok(storage_trie.update_storage(address, key, value)?)
    }

    #[allow(dead_code)]
    fn delete_storage(&mut self, address: Address, key: &[u8]) -> Result<(), TrieDBError> {
        let mut storage_trie = self.get_storage_trie(address)?;
        Ok(storage_trie.delete_storage(address, key)?)
    }
}

// Internally helper functions
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Gets the storage trie for an account
    fn get_storage_trie(&mut self, address: Address) -> Result<StateTrie<DB>, TrieDBError> {
        let hashed_address = keccak256(address.as_slice());
        return Ok(self.get_storage_trie_with_hash_state(hashed_address)?);
    }

    /// Gets the storage trie for an hash address
    pub(crate) fn get_storage_trie_with_hash_state(&mut self, hashed_address: B256) -> Result<StateTrie<DB>, TrieDBError> {
        if let Some(storage_trie) = self.storage_tries.get(&hashed_address) {
            return Ok(storage_trie.clone());
        }

        let storage_root = self.get_storage_root_with_hash_state(hashed_address)?;
        let id = SecureTrieId::new(storage_root)
            .with_owner(hashed_address);
        let storage_trie = SecureTrieBuilder::new(self.path_db.clone())
            .with_id(id)
            .build_with_difflayer(self.difflayer.as_ref())?;

        self.storage_tries.insert(hashed_address, storage_trie.clone());
        Ok(storage_trie)
    }

    /// Gets the storage root for an account with hash state
    fn get_storage_root_with_hash_state(&mut self, hashed_address: B256) -> Result<B256, TrieDBError> {
        let account = self.get_account_with_hash_state(hashed_address)?;
        if let Some(acc) = account {
            self.accounts_with_storage_trie.insert(hashed_address, acc);
            Ok(acc.storage_root)
        } else {
            self.accounts_with_storage_trie.insert(hashed_address, StateAccount::default());
            Ok(EMPTY_ROOT_HASH)
        }
    }
}