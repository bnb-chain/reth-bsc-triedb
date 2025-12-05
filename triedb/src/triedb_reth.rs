//! Reth-compatible implementations for TrieDB.

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use std::time::Instant;

use alloy_primitives::{B256, U256, hex};
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::{MergedNodeSet, NodeSet, DiffLayer, DiffLayers};
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieTrait, SecureTrieBuilder};

use crate::triedb::{TrieDB, TrieDBError};

/// Reth-compatible interface functions using hashed keys for TrieDB.
///
/// This module provides interfaces compatible with clients that use hashed keys
/// (Keccak-256 hashes of addresses and storage keys) to access the trie database.
///
/// # Write Operations (Batch Only)
///
/// **Important**: `TrieDB` only supports batch write operations. Individual storage 
/// key-value write operations are **not supported** and will not persist correctly.
///
/// All write operations must be performed through one of the following batch methods:
/// - [`batch_update_and_commit`](Self::batch_update_and_commit) - 
///   Batch update accounts and storage, then commit all changes atomically
/// - [`commit_hashed_post_state`](Self::commit_hashed_post_state) - 
///   Commit a complete post-state with all account and storage changes
///
/// The modification functions (`update_account_with_hash_state`, `delete_account_with_hash_state`, 
/// `update_storage_with_hash_state`, `delete_storage_with_hash_state`) in this module are 
/// **not intended for external use**. They are:
/// - Marked as `#[allow(dead_code)]` or kept internal for internal batch operations
/// - Only modify in-memory state without proper commit handling
/// - Do not update storage roots correctly in the account trie
/// - Do not integrate with the diff layer system
/// - Individual writes would be inefficient and break consistency guarantees
///
/// # Read Operations (Public API)
///
/// The query functions (`get_account_with_hash_state`, `get_storage_with_hash_state`) are 
/// **public and safe to use**. They support:
/// - Reading account data from the state trie using hashed addresses
/// - Reading storage values from account storage tries using hashed keys
/// - **Pre-warming**: These functions can be used to preload and cache frequently
///   accessed tries into memory, improving subsequent batch operation performance.
///   When you call `get_account_with_hash_state` or `get_storage_with_hash_state`, the 
///   underlying tries are loaded and cached, which helps optimize batch operations that 
///   access the same data.
///
/// # Usage Pattern
///
/// ```ignore
/// // ✅ Correct: Use batch operations for writes
/// triedb.batch_update_and_commit(root_hash, difflayer, accounts, rebuild_set, storage)?;
///
/// // ✅ Correct: Use query functions for reads and pre-warming
/// let account = triedb.get_account_with_hash_state(hashed_address)?;
/// let storage_value = triedb.get_storage_with_hash_state(hashed_address, hashed_key)?;
///
/// // ❌ Incorrect: Do not use individual write functions
/// // triedb.update_storage_with_hash_state(hashed_address, hashed_key, value)?;  // Will not persist correctly!
/// ```
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub fn get_account_with_hash_state(&mut self, hashed_address: B256) -> Result<Option<StateAccount>, TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().get_account_with_hash_state(hashed_address)?)
    }

    pub fn update_account_with_hash_state(&mut self, hashed_address: B256, account: &StateAccount) -> Result<(), TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().update_account_with_hash_state(hashed_address, account)?)
    }
    
    pub fn delete_account_with_hash_state(&mut self, hashed_address: B256) -> Result<(), TrieDBError> {
        Ok(self.account_trie.as_mut().unwrap().delete_account_with_hash_state(hashed_address)?)
    }

    pub fn get_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<Option<Vec<u8>>, TrieDBError> {
        let mut storage_trie = self.get_storage_trie_with_hash_state(hashed_address)?;
        Ok(storage_trie.get_storage_with_hash_state(hashed_address, hashed_key)?)
    }

    #[allow(dead_code)]
    fn update_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256, value: &[u8]) -> Result<(), TrieDBError> {
        let mut storage_trie = self.get_storage_trie_with_hash_state(hashed_address)?;
        Ok(storage_trie.update_storage_with_hash_state(hashed_address, hashed_key, value)?)
    }

    #[allow(dead_code)]
    fn delete_storage_with_hash_state(&mut self, hashed_address: B256, hashed_key: B256) -> Result<(), TrieDBError> {
        let mut storage_trie = self.get_storage_trie_with_hash_state(hashed_address)?;
        Ok(storage_trie.delete_storage_with_hash_state(hashed_address, hashed_key)?)
    }
}

#[derive(Default, Clone)]
pub struct TrieDBHashedPostState {
    pub states: HashMap<B256, Option<StateAccount>>,
    pub states_rebuild: HashSet<B256>,
    pub storage_states: HashMap<B256, HashMap<B256, Option<U256>>>
}

/// Compatible with Reth client usage scenarios
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{  
    /// Transfers HashedPostState to triedb structure and commits the changes
    /// Compatible with Reth usage scenarios
    pub fn commit_hashed_post_state(
        &mut self, 
        root_hash: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState) -> 
        Result<(B256, Option<Arc<DiffLayer>>), TrieDBError> {

        let validate_start = Instant::now();

        let (root_hash, node_set, diff_storage_roots) = self.finalise(
            root_hash, 
            difflayer, 
            hashed_post_state.states.clone(), 
            hashed_post_state.states_rebuild.clone(), 
            hashed_post_state.storage_states.clone())?;

        let diff_nodes = (*node_set.to_diff_nodes()).clone();
        let difflayer = Arc::new(DiffLayer::new(diff_nodes, diff_storage_roots));
        
        if difflayer.is_empty() {
            return Ok((root_hash, None));
        }

        self.metrics.record_validate_duration(validate_start.elapsed().as_secs_f64());
        
        Ok((root_hash, Some(difflayer)))      
    }

    /// Batch update the changes and commit
    /// Compatible with Reth usage scenarios
    /// 
    /// 1. Reset the trie db state
    /// 2. Prepare accounts to be updated
    /// 3. Prepare required data to avoid borrowing conflicts for parallel execution
    /// 4. Parallel execution: update accounts and storage simultaneously
    /// 5. Commit the changes
    pub fn finalise(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        states: HashMap<B256, Option<StateAccount>>,
        states_rebuild: HashSet<B256>,
        storage_states: HashMap<B256, HashMap<B256, Option<U256>>>) -> 
        Result<(B256, Arc<MergedNodeSet>, HashMap<B256, B256>), TrieDBError> {
        
        self.state_at(parent_root, difflayer)?;

        let intermediate_root_start = Instant::now();
        let root_hash = self.intermediate_root(states, storage_states, states_rebuild)?;
        self.metrics.record_intermediate_root_duration(intermediate_root_start.elapsed().as_secs_f64());

        let commit_start = Instant::now();
        let (_, node_set) = self.commit(true)?;
        self.metrics.record_commit_duration(commit_start.elapsed().as_secs_f64());

        let diff_storage_roots = self.updated_storage_roots.clone();
        self.clean();

        Ok((root_hash, node_set, diff_storage_roots))
    }

    pub fn intermediate_root(
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>,
        accounts_rebuild: HashSet<B256>) -> 
        Result<B256, TrieDBError> {
        
        let intermediate_state_objects = Instant::now();
        let updated_accounts = self.update_state_objects(accounts, storages, accounts_rebuild)?;        
        self.metrics.record_intermediate_state_objects_duration(intermediate_state_objects.elapsed().as_secs_f64());
        
        for (hashed_address, account) in updated_accounts {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
        }
        let root_hash = self.account_trie.as_mut().unwrap().hash();        
        return Ok(root_hash);
    }

    fn update_state_objects (
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>, 
        accounts_rebuild: HashSet<B256>) -> 
        Result<HashMap<B256, Option<StateAccount>>, TrieDBError> {
       
        let task1_start = Instant::now();
        let mut new_accounts_with_no_storage = HashMap::new();
        let mut new_accounts_with_storages = HashMap::new();
        let mut storage_roots = HashMap::new();
        for (hashed_address, new_account) in accounts {
            if new_account.is_none() {
                new_accounts_with_no_storage.insert(hashed_address, None);
                storage_roots.insert(hashed_address, alloy_trie::EMPTY_ROOT_HASH);
                continue;
            }

            let final_account = if accounts_rebuild.contains(&hashed_address) {
                new_account.unwrap()
            }else {
                let mut new_account = new_account.unwrap();
                new_account.storage_root = self.get_storage_root(hashed_address)?;
                new_account
            };
            storage_roots.insert(hashed_address, final_account.storage_root);

            if storages.contains_key(&hashed_address) {
                new_accounts_with_storages.insert(hashed_address, final_account);
            } else {
                new_accounts_with_no_storage.insert(hashed_address, Some(final_account));
            }
        }
        self.metrics.record_intermediate_state_objects_account_duration(task1_start.elapsed().as_secs_f64());

        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());

        let task2_start = Instant::now();
        let storage_result = storages
            .into_par_iter()
            .map(|(hashed_address, kvs)| {
                // Get storage root from path_db or difflayer (same logic as task 1)
                let mut account = new_accounts_with_storages.get(&hashed_address).unwrap().clone();
                let storage_root = account.storage_root;

                let id = SecureTrieId::new(storage_root)
                    .with_owner(hashed_address);
                let mut storage_trie = SecureTrieBuilder::new(path_db_clone.clone())
                    .with_id(id)
                    .build_with_difflayer(difflayer_clone.as_ref())
                    .map_err(|e| TrieDBError::Database(format!("Failed to build storage trie for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;

                // Parallel execution for kvs within each address
                let kvs_vec: Vec<_> = kvs.into_iter().collect();
                for (hashed_key, new_value) in kvs_vec {
                    if let Some(new_value) = new_value {
                        storage_trie.update_storage_u256_with_hash_state(hashed_address, hashed_key, new_value)
                            .map_err(|e| TrieDBError::Database(format!("Failed to update storage for hashed_address: 0x{}, hashed_key: 0x{}, new_value: {:#x}, error: {}", hex::encode(hashed_address), hex::encode(hashed_key), new_value, e)))?;
                    } else {
                        storage_trie.delete_storage_with_hash_state(hashed_address, hashed_key)
                            .map_err(|e| TrieDBError::Database(format!("Failed to delete storage for hashed_address: 0x{}, hashed_key: 0x{}, error: {}", hex::encode(hashed_address), hex::encode(hashed_key), e)))?;
                    }
                }

                let new_storage_root = storage_trie.hash();
                account.storage_root = new_storage_root;

                Ok((hashed_address, (Some(account), new_storage_root, storage_trie)))
            })
            .collect::<Result<Vec<_>, TrieDBError>>()
            .map(|vec| {
                let mut new_accounts = HashMap::new();
                let mut diff_account_storage_roots = HashMap::new();
                let mut storage_tries = HashMap::new();
                for (hashed_address, (account, storage_root, storage_trie)) in vec {
                    new_accounts.insert(hashed_address, account);
                    diff_account_storage_roots.insert(hashed_address, storage_root);
                    storage_tries.insert(hashed_address, storage_trie);
                }
                (new_accounts, diff_account_storage_roots, storage_tries)
            });
        self.metrics.record_intermediate_state_objects_storage_duration(task2_start.elapsed().as_secs_f64());

        // Merge results
        let (accounts_with_storage, roots_with_storage, storage_tries) = storage_result?;

        new_accounts_with_no_storage.extend(accounts_with_storage);
        storage_roots.extend(roots_with_storage);

        self.storage_tries = storage_tries;
        self.updated_storage_roots = storage_roots;
        
        Ok(new_accounts_with_no_storage)
    }

    pub fn commit(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>), TrieDBError> {        
        let mut merged_node_set = MergedNodeSet::new();
        // Start both tasks in parallel using rayon
        let mut account_trie_clone = self.account_trie.as_mut().unwrap().clone();
        let (account_commit_result, storage_commit_results): (Result<(B256, Option<Arc<NodeSet>>), _>, Vec<(B256, Option<Arc<NodeSet>>)>) = rayon::join(
            || account_trie_clone.commit(true),
            || self.storage_tries
                .par_iter()
                .map(|(hashed_address, trie)| {
                    let (_, node_set) = trie.clone().commit(false).unwrap();
                    (*hashed_address, node_set)
                })
                .collect()
        );
        drop(account_trie_clone);

        let (root_hash, account_node_set) = account_commit_result?;

        if let Some(node_set) = account_node_set {
            merged_node_set.merge(node_set)
                .map_err(|e| TrieDBError::Database(e))?;
        }

        for (_, node_set) in storage_commit_results {
            if let Some(node_set) = node_set {
                merged_node_set.merge(node_set)
                    .map_err(|e| TrieDBError::Database(e))?;
            }
        }
        Ok((root_hash, Arc::new(merged_node_set)))
    }
}


