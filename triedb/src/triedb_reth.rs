//! Reth-compatible implementations for TrieDB.

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use std::time::Instant;

use tracing::debug;
use alloy_primitives::{B256, U256, hex};
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::{MergedNodeSet, DiffLayer, DiffLayers};
use rust_eth_triedb_state_trie::state_trie::StateTrie;
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

#[derive(Default, Clone, Debug)]
pub struct TrieDBHashedPostState {
    pub states: HashMap<B256, Option<StateAccount>>,
    pub states_rebuild: HashSet<B256>,
    pub storage_states: HashMap<B256, HashMap<B256, Option<U256>>>
}

#[derive(Clone, Debug)]
pub struct TrieDBPrefetchState<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    pub account_trie: StateTrie<DB>,
    pub storage_roots: HashMap<B256, B256>,
    pub storage_tries: HashMap<B256, StateTrie<DB>>,
}

/// Compatible with Reth client usage scenarios
impl<DB> TrieDB<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{  
    pub fn finalise(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        states: HashMap<B256, Option<StateAccount>>,
        states_rebuild: HashSet<B256>,
        storage_states: HashMap<B256, HashMap<B256, Option<U256>>>,
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> 
        Result<(B256, Arc<MergedNodeSet>, Arc<HashMap<B256, B256>>), TrieDBError>
    where
        DB: 'static,
    {
        
        self.state_at(parent_root, difflayer, prefetcher)?;
        self.intermediate_inner(states, storage_states, states_rebuild)?;
        return self.commit_inner(true)
    }

    fn intermediate_inner(
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>,
        states_rebuild: HashSet<B256>) -> 
        Result<B256, TrieDBError> {
        
        let total_start = Instant::now();

        let step = Instant::now();
        let updated_accounts = self.update_state_objects(accounts, storages, states_rebuild.clone())?;
        let update_state_objects_ms = step.elapsed().as_millis();
        self.metrics.record_intermediate_state_objects_duration(step.elapsed().as_secs_f64());

        let step = Instant::now();
        let mut account_count = 0u32;

        for hashed_address in states_rebuild {
            self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            account_count += 1;
        }

        for (hashed_address, account) in updated_accounts {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
            account_count += 1;
        }
        let update_account_trie_ms = step.elapsed().as_millis();

        let step = Instant::now();
        let root_hash = self.account_trie.as_mut().unwrap().hash();
        let account_hash_ms = step.elapsed().as_millis();
        self.metrics.record_hash_duration(step.elapsed().as_secs_f64());

        let total_ms = total_start.elapsed().as_millis();
        self.metrics.record_intermediate_root_duration(total_start.elapsed().as_secs_f64());

        debug!(
            target: "triedb::timing",
            total_ms,
            update_state_objects_ms,
            update_account_trie_ms,
            account_hash_ms,
            account_count,
            caller = if self.prefetcher.is_some() { "miner" } else { "import" },
            "intermediate_inner breakdown"
        );
        return Ok(root_hash);
    }

    fn update_state_objects (
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>, 
        states_rebuild: HashSet<B256>) -> 
        Result<HashMap<B256, Option<StateAccount>>, TrieDBError> {
       
        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());
        let accounts_clone = accounts.clone();
        let storages_keys: HashSet<B256> = storages.keys().cloned().collect();
        let storages_for_task2 = storages;
        let metrics_clone = self.metrics.clone();
        let prefetcher_clone = self.prefetcher.clone();

        // Closure to get storage root from difflayer or path_db
        let get_storage_root = |hashed_address: B256| -> Result<B256, TrieDBError> {
            if states_rebuild.contains(&hashed_address) {
                return Ok(alloy_trie::EMPTY_ROOT_HASH);
            }

            if let Some(prefetcher) = &self.prefetcher {
                if let Some(root) = prefetcher.storage_roots.get(&hashed_address) {
                    metrics_clone.increment_storage_root_from_prefetcher_counter();
                    return Ok(*root);
                }
            }

            if let Some(dl) = difflayer_clone.as_ref() {
                if let Some(root) = dl.get_storage_root(hashed_address) {
                    metrics_clone.increment_storage_root_from_difflayer_counter();
                    return Ok(root);
                }
            }
            metrics_clone.increment_storage_root_from_pathdb_counter();
            path_db_clone.get_storage_root(hashed_address)
                .map_err(|e| TrieDBError::Database(format!("Failed to get storage root for hashed_address: 0x{}, error: {:?}", hex::encode(hashed_address), e)))
                .map(|opt| opt.unwrap_or(alloy_trie::EMPTY_ROOT_HASH))
        };

        // Parallel execution: process accounts and storages simultaneously
        let (account_result, storage_result): (
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>), TrieDBError>,
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>, HashMap<B256, StateTrie<DB>>), TrieDBError>
        ) = rayon::join(
            || {
                // Task 1: Process accounts that don't have storage updates (parallel)
                let task1_start = Instant::now();
                let result = accounts_clone
                    .par_iter()
                    .filter(|(hashed_address, _)| !storages_keys.contains(*hashed_address))
                    .map(|(hashed_address, account)| {
                        match account {
                            Some(account) => {
                                let mut new_account = account.clone();
                                let storage_root = get_storage_root(*hashed_address)?;
                                new_account.storage_root = storage_root;
                                Ok((*hashed_address, (Some(new_account), storage_root)))
                            }
                            None => {
                                Ok((*hashed_address, (None, alloy_trie::EMPTY_ROOT_HASH)))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vec| {
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        for (hashed_address, (account, storage_root)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                        }
                        (new_accounts, diff_account_storage_roots)
                    });
                metrics_clone.record_intermediate_state_objects_account_duration(task1_start.elapsed().as_secs_f64());
                result
            },
            || {
                // Task 2: Process accounts with storage updates (parallel)
                let task2_start = Instant::now();
                let result = storages_for_task2
                    .into_par_iter()
                    .map(|(hashed_address, kvs)| {
                        let acct_start = Instant::now();

                        // Try to get storage_trie from prefetcher, otherwise create a new one
                        let is_prefetched = prefetcher_clone.as_ref()
                            .and_then(|p| p.storage_tries.get(&hashed_address))
                            .is_some();
                        if !is_prefetched {
                            let pf_keys_count = prefetcher_clone.as_ref()
                                .map(|p| p.storage_tries.len()).unwrap_or(0);
                            let has_root = prefetcher_clone.as_ref()
                                .and_then(|p| p.storage_roots.get(&hashed_address)).is_some();
                            let caller = if prefetcher_clone.is_some() { "miner" } else { "import" };
                            tracing::debug!(
                                target: "triedb::timing",
                                hashed_address = %hex::encode(&hashed_address.as_slice()[..4]),
                                pf_keys_count,
                                has_root,
                                caller,
                                "storage trie NOT in prefetcher"
                            );
                        }
                        let mut storage_trie = match prefetcher_clone.as_ref()
                            .and_then(|p| p.storage_tries.get(&hashed_address))
                            .cloned()
                        {
                            Some(trie) => trie,
                            None => {
                                let storage_root = get_storage_root(hashed_address)?;
                                let id = SecureTrieId::new(storage_root)
                                    .with_owner(hashed_address);
                                SecureTrieBuilder::new(path_db_clone.clone())
                                    .with_id(id)
                                    .build_with_difflayer(difflayer_clone.as_ref())
                                    .map_err(|e| TrieDBError::Database(format!("Failed to build storage trie for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?
                            }
                        };

                        let kvs_vec: Vec<_> = kvs.into_iter().collect();
                        let slot_count = kvs_vec.len();
                        let miss_before = path_db_clone.trie_miss_breakdown().1;
                        for (hashed_key, new_value) in kvs_vec {
                            if let Some(new_value) = new_value {
                                storage_trie.update_storage_u256_with_hash_state(hashed_address, hashed_key, new_value)
                                    .map_err(|e| TrieDBError::Database(format!("Failed to update storage for hashed_address: 0x{}, hashed_key: 0x{}, new_value: {:#x}, error: {}", hex::encode(hashed_address), hex::encode(hashed_key), new_value, e)))?;
                            } else {
                                storage_trie.delete_storage_with_hash_state(hashed_address, hashed_key)
                                    .map_err(|e| TrieDBError::Database(format!("Failed to delete storage for hashed_address: 0x{}, hashed_key: 0x{}, error: {}", hex::encode(hashed_address), hex::encode(hashed_key), e)))?;
                            }
                        }
                        let miss_after = path_db_clone.trie_miss_breakdown().1;
                        let per_acct_stor_miss = miss_after - miss_before;
                        if per_acct_stor_miss > 5 {
                            let caller = if prefetcher_clone.is_some() { "miner" } else { "import" };
                            tracing::debug!(
                                target: "triedb::timing",
                                hashed_address = %hex::encode(&hashed_address.as_slice()[..4]),
                                slot_count,
                                per_acct_stor_miss,
                                is_prefetched,
                                caller,
                                "storage trie per-account miss"
                            );
                        }

                        let new_storage_root = storage_trie.hash();
                        let updated_account = match accounts_clone.get(&hashed_address) {
                            Some(Some(account)) => {
                                let mut new_account = account.clone();
                                new_account.storage_root = new_storage_root;
                                Some(new_account)
                            }
                            // Account deleted or not present — storage update is moot
                            Some(None) | None => None,
                        };

                        let acct_ms = acct_start.elapsed().as_millis();
                        if acct_ms > 5 {
                            tracing::trace!(
                                target: "triedb::timing",
                                hashed_address = %hex::encode(hashed_address),
                                acct_ms,
                                slot_count,
                                "slow storage trie update"
                            );
                        }

                        Ok((hashed_address, (updated_account, new_storage_root, storage_trie)))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vec| {
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        let mut storage_tries = HashMap::new();
                        for (hashed_address, (account, storage_root, storage_trie)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                            storage_tries.insert(hashed_address, storage_trie);
                        }
                        (new_accounts, diff_account_storage_roots, storage_tries)
                    });
                metrics_clone.record_intermediate_state_objects_storage_duration(task2_start.elapsed().as_secs_f64());
                result
            }
        );

        // Merge results
        let (mut accounts_no_storage, mut roots_no_storage) = account_result?;
        let (accounts_with_storage, roots_with_storage, storage_tries) = storage_result?;

        // Removed verbose result counts log

        accounts_no_storage.extend(accounts_with_storage);
        roots_no_storage.extend(roots_with_storage.into_iter());

        self.storage_tries = storage_tries;
        self.updated_storage_roots = roots_no_storage;
        
        Ok(accounts_no_storage)
    }

    fn commit_inner(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>, Arc<HashMap<B256, B256>>), TrieDBError>
    where
        DB: 'static,
    {
        let caller = if self.prefetcher.is_some() { "miner" } else { "import" };
        let storage_tries_count = self.storage_tries.len();

        let step = Instant::now();
        let (root_hash, node_set) = self.commit_state_objects(true)?;
        let commit_state_objects_ms = step.elapsed().as_millis();
        self.metrics.record_commit_duration(step.elapsed().as_secs_f64());

        let diff_storage_roots = Arc::from(*std::mem::take(&mut self.updated_storage_roots));
        self.clean();

        debug!(
            target: "triedb::timing",
            commit_state_objects_ms,
            storage_tries_count,
            caller,
            "commit_inner breakdown"
        );

        Ok((root_hash, node_set, diff_storage_roots))
    }

    fn commit_state_objects(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>), TrieDBError> {        
        let mut merged_node_set = Box::new(MergedNodeSet::new());

        // Start both tasks in parallel using rayon
        let mut account_trie_clone = self.account_trie.as_mut().unwrap().clone();
        let (account_commit_result, storage_commit_results) = rayon::join(
            || account_trie_clone.commit(true),
            || self.storage_tries
                .par_iter()
                .map(|(hashed_address, trie)| {
                    let (_, node_set) = trie.clone().commit(false)
                        .map_err(|e| TrieDBError::Database(format!("Failed to commit storage trie for hashed_address: 0x{}, error: {:?}", hex::encode(hashed_address), e)))?;
                    Ok::<_, TrieDBError>((*hashed_address, node_set))
                })
                .collect::<Result<Vec<_>, _>>()
        );

        let (root_hash, account_node_set) = account_commit_result?;

        if let Some(node_set) = account_node_set {
            merged_node_set.merge(node_set)
                .map_err(|e| TrieDBError::Database(e))?;
        }

        for (_, node_set) in storage_commit_results? {
            if let Some(node_set) = node_set {
                merged_node_set.merge(node_set)
                    .map_err(|e| TrieDBError::Database(e))?;
            }
        }
        Ok((root_hash, Arc::from(*merged_node_set)))
    }

    pub fn intermediate_and_commit_hashed_post_state(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState, 
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> 
        Result<(B256, Arc<DiffLayer>), TrieDBError>
    where
        DB: 'static,
    {
        let caller = if prefetcher.is_some() { "miner" } else { "import" };
        let total_start = Instant::now();

        let snap0 = self.path_db.trie_cache_snapshot();
        let miss0 = self.path_db.trie_miss_breakdown();
        let resolve0 = rust_eth_triedb_state_trie::resolve_counter_snapshot();

        let step = Instant::now();
        self.state_at(parent_root, difflayer, prefetcher)?;
        let state_at_ms = step.elapsed().as_millis();

        let snap1 = self.path_db.trie_cache_snapshot();
        let miss1 = self.path_db.trie_miss_breakdown();

        let step = Instant::now();
        self.intermediate_inner(
            hashed_post_state.states.clone(),
            hashed_post_state.storage_states.clone(),
            hashed_post_state.states_rebuild.clone())?;
        let intermediate_inner_ms = step.elapsed().as_millis();

        let snap2 = self.path_db.trie_cache_snapshot();
        let miss2 = self.path_db.trie_miss_breakdown();

        let step = Instant::now();
        let result = self.commit(true);
        let commit_ms = step.elapsed().as_millis();

        let snap3 = self.path_db.trie_cache_snapshot();
        let miss3 = self.path_db.trie_miss_breakdown();
        let resolve1 = rust_eth_triedb_state_trie::resolve_counter_snapshot();

        let cache_hits = snap3.0 - snap0.0;
        let cache_misses = snap3.1 - snap0.1;
        let acct_misses = miss3.0 - miss0.0;
        let stor_misses = miss3.1 - miss0.1;

        // Per-phase miss breakdown
        let state_at_misses = (snap1.1 - snap0.1) as i64;
        let intermediate_misses = (snap2.1 - snap1.1) as i64;
        let commit_misses = (snap3.1 - snap2.1) as i64;
        let intermediate_stor = (miss2.1 - miss1.1) as i64;
        let commit_stor = (miss3.1 - miss2.1) as i64;

        // DiffLayer filter rate: how much of all resolve calls get absorbed by DiffLayer
        // versus falling through to PathDB (moka/RocksDB).
        let resolve_total = resolve1.0 - resolve0.0;
        let resolve_difflayer_hit = resolve1.1 - resolve0.1;
        let resolve_fallthrough = resolve_total.saturating_sub(resolve_difflayer_hit);
        let difflayer_filter_pct = if resolve_total > 0 {
            resolve_difflayer_hit * 100 / resolve_total
        } else { 0 };

        debug!(
            target: "triedb::timing",
            total_ms = total_start.elapsed().as_millis(),
            state_at_ms,
            intermediate_inner_ms,
            commit_ms,
            states_count = hashed_post_state.states.len(),
            storage_states_count = hashed_post_state.storage_states.len(),
            cache_hits,
            cache_misses,
            acct_misses,
            stor_misses,
            state_at_misses,
            intermediate_misses,
            commit_misses,
            intermediate_stor,
            commit_stor,
            resolve_total,
            resolve_difflayer_hit,
            resolve_fallthrough,
            difflayer_filter_pct,
            caller,
            "intermediate_and_commit breakdown"
        );
        result
    }

    pub fn intermediate_hashed_post_state(
        &mut self,
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState, 
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>
    ) -> Result<B256, TrieDBError>
    where
        DB: 'static,
    {
        self.state_at(parent_root, difflayer, prefetcher)?;
        return self.intermediate_inner(
            hashed_post_state.states.clone(), 
            hashed_post_state.storage_states.clone(), 
            hashed_post_state.states_rebuild.clone());
    }

    pub fn commit(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<DiffLayer>), TrieDBError> 
    where
        DB: 'static,
    {
        let (root_hash, node_set, diff_storage_roots) = self.commit_inner(true)?;
        let difflayer = Arc::new(DiffLayer::new(node_set.to_diff_nodes(), diff_storage_roots));
        Ok((root_hash, difflayer)) 
    }
}


