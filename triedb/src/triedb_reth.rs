//! Reth-compatible implementations for TrieDB.

use std::sync::{Arc, OnceLock};
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use std::time::Instant;

use alloy_primitives::{B256, U256, hex};
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::{MergedNodeSet, NodeSet, DiffLayer, DiffLayers};
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieTrait, SecureTrieBuilder};
use tracing::debug;

use crate::triedb::{TrieDB, TrieDBError};

/// Dedicated rayon pool for triedb internal parallelism.
///
/// We intentionally avoid Rayon global pool to prevent interference with other subsystems that
/// also use rayon. This makes triedb's parallel sections more predictable under load.
///
/// Thread count is fixed to 48 to avoid interference with other rayon users.
static TRIEDB_RAYON_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

#[inline]
fn triedb_rayon_num_threads() -> usize {
    48
}

#[inline]
fn triedb_rayon_pool() -> &'static rayon::ThreadPool {
    TRIEDB_RAYON_POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(triedb_rayon_num_threads())
            .thread_name(|i| format!("triedb-rayon-{i}"))
            .build()
            .expect("failed to build triedb rayon pool")
    })
}

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
        self.commit_inner(true)
    }

    fn intermediate_inner(
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>,
        states_rebuild: HashSet<B256>) -> 
        Result<B256, TrieDBError> {
        
        let intermediate_root_start = Instant::now();

        let accounts_len = accounts.len();
        let storages_len = storages.len();
        let rebuild_len = states_rebuild.len();

        let intermediate_state_objects = Instant::now();
        let updated_accounts = self.update_state_objects(accounts, storages, states_rebuild.clone())?;
        let update_state_objects_elapsed = intermediate_state_objects.elapsed();
        self.metrics.record_intermediate_state_objects_duration(update_state_objects_elapsed.as_secs_f64());
        
        let rebuild_delete_start = Instant::now();
        for hashed_address in states_rebuild {
            self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
        }
        let rebuild_delete_elapsed = rebuild_delete_start.elapsed();
        
        let apply_accounts_start = Instant::now();
        for (hashed_address, account) in updated_accounts {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
        }
        let apply_accounts_elapsed = apply_accounts_start.elapsed();

        let hash_start = Instant::now();
        let root_hash = self.account_trie.as_mut().unwrap().hash();
        let hash_elapsed = hash_start.elapsed();
        self.metrics.record_intermediate_root_duration(intermediate_root_start.elapsed().as_secs_f64());     

        debug!(
            target: "triedb::intermediate_inner",
            accounts_len,
            storages_len,
            rebuild_len,
            update_state_objects_ms = update_state_objects_elapsed.as_secs_f64() * 1000.0,
            rebuild_delete_ms = rebuild_delete_elapsed.as_secs_f64() * 1000.0,
            apply_accounts_ms = apply_accounts_elapsed.as_secs_f64() * 1000.0,
            account_trie_hash_ms = hash_elapsed.as_secs_f64() * 1000.0,
            total_ms = intermediate_root_start.elapsed().as_secs_f64() * 1000.0,
            "intermediate_inner timing"
        );

        return Ok(root_hash);
    }

    fn update_state_objects (
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>, 
        states_rebuild: HashSet<B256>) -> 
        Result<HashMap<B256, Option<StateAccount>>, TrieDBError> {
        #[derive(Clone, Debug)]
        struct SlowestStorageItem {
            hashed_address: B256,
            kvs_len: usize,
            duration: std::time::Duration,
            apply_kvs_duration: std::time::Duration,
            hash_duration: std::time::Duration,
            trie_update_stats: Option<rust_eth_triedb_state_trie::trie::TrieUpdateStatsSnapshot>,
            prefetch_storage_trie_hit: bool,
            storage_root_source: &'static str,
        }

        #[derive(Clone, Debug, Default)]
        struct TaskTiming {
            queue_delay: std::time::Duration,
            work: std::time::Duration,
            items: usize,
            total_kvs: usize,
            slowest: Option<SlowestStorageItem>,
        }

        let call_start = Instant::now();
        let accounts_len = accounts.len();
        let storages_len = storages.len();

        // Snapshot PathDB trie-node cache counters (if supported by DB backend).
        let (pathdb_cache_stats_present, pathdb_trie_node_cache_hits_start, pathdb_trie_node_cache_misses_start) =
            match self.path_db.trie_node_cache_counters() {
                Some((h, m)) => (true, h, m),
                None => (false, 0u64, 0u64),
            };
        let (
            pathdb_rocksdb_stats_present,
            pathdb_rocksdb_trie_node_get_calls_start,
            pathdb_rocksdb_trie_node_get_found_start,
            pathdb_rocksdb_trie_node_get_not_found_start,
            pathdb_rocksdb_trie_node_get_errors_start,
            pathdb_rocksdb_trie_node_get_us_total_start,
        ) = match self.path_db.trie_node_rocksdb_counters() {
            Some((c, f, n, e, us)) => (true, c, f, n, e, us),
            None => (false, 0u64, 0u64, 0u64, 0u64, 0u64),
        };

        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());
        let accounts_clone = accounts.clone();
        let storages_keys: HashSet<B256> = storages.keys().cloned().collect();
        let storages_for_task2 = storages;
        let metrics_clone = self.metrics.clone();
        let prefetcher_clone = self.prefetcher.clone();

        // Closure to get storage root from difflayer or path_db
        let get_storage_root_with_source =
            |hashed_address: B256| -> Result<(B256, &'static str), TrieDBError> {
            if states_rebuild.contains(&hashed_address) {
                return Ok((alloy_trie::EMPTY_ROOT_HASH, "rebuild"));
            }

            if let Some(prefetcher) = &self.prefetcher {
                if let Some(root) = prefetcher.storage_roots.get(&hashed_address) {
                    return Ok((*root, "prefetcher"));
                }
            }

            if let Some(dl) = difflayer_clone.as_ref() {
                if let Some(root) = dl.get_storage_root(hashed_address) {
                    return Ok((root, "difflayer"));
                }
            }
            path_db_clone.get_storage_root(hashed_address)
                .map_err(|e| TrieDBError::Database(format!("Failed to get storage root for hashed_address: 0x{}, error: {:?}", hex::encode(hashed_address), e)))
                .map(|opt| opt.map(|r| (r, "pathdb")).unwrap_or((alloy_trie::EMPTY_ROOT_HASH, "pathdb-none")))
        };

        // Parallel execution: process accounts and storages simultaneously
        let join_start = Instant::now();
        let ((account_result, task1_timing), (storage_result, task2_timing)): (
            (
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>), TrieDBError>,
            TaskTiming,
            ),
            (
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>, HashMap<B256, StateTrie<DB>>, Option<SlowestStorageItem>), TrieDBError>,
            TaskTiming,
            ),
        ) = triedb_rayon_pool().install(|| rayon::join(
            || {
                // Task 1: Process accounts that don't have storage updates (parallel)
                let closure_start = Instant::now();
                let queue_delay = closure_start.saturating_duration_since(join_start);
                let task1_start = Instant::now();
                let result = accounts_clone
                    .par_iter()
                    .filter(|(hashed_address, _)| !storages_keys.contains(*hashed_address))
                    .map(|(hashed_address, account)| {
                        match account {
                            Some(account) => {
                                let mut new_account = account.clone();
                                let (storage_root, _src) = get_storage_root_with_source(*hashed_address)?;
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
                        let items = vec.len();
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        for (hashed_address, (account, storage_root)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                        }
                        ((new_accounts, diff_account_storage_roots), items)
                    });
                metrics_clone.record_intermediate_state_objects_account_duration(task1_start.elapsed().as_secs_f64());
                let work = task1_start.elapsed();
                let (result, items) = match result {
                    Ok((r, items)) => (Ok(r), items),
                    Err(e) => (Err(e), 0),
                };
                let timing = TaskTiming { queue_delay, work, items, total_kvs: 0, slowest: None };
                (result, timing)
            },
            || {
                // Task 2: Process accounts with storage updates (parallel)
                let closure_start = Instant::now();
                let queue_delay = closure_start.saturating_duration_since(join_start);
                let task2_start = Instant::now();
                let result = storages_for_task2
                    .into_par_iter()
                    .map(|(hashed_address, kvs)| {
                        let item_start = Instant::now();
                        let kvs_len = kvs.len();

                        // Try to get storage_trie from prefetcher, otherwise create a new one
                        let (mut storage_trie, prefetch_storage_trie_hit, storage_root_source) = match prefetcher_clone.as_ref()
                            .and_then(|p| p.storage_tries.get(&hashed_address))
                            .cloned()
                        {
                            Some(trie) => (trie, true, "prefetcher-storage-trie"),
                            None => {
                                // Get storage root from path_db or difflayer
                                let (storage_root, src) = get_storage_root_with_source(hashed_address)?;
                                let id = SecureTrieId::new(storage_root)
                                    .with_owner(hashed_address);
                                let trie = SecureTrieBuilder::new(path_db_clone.clone())
                                    .with_id(id)
                                    .build_with_difflayer(difflayer_clone.as_ref())
                                    .map_err(|e| TrieDBError::Database(format!("Failed to build storage trie for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
                                (trie, false, src)
                            }
                        };
                        
                        // Get storage root from path_db or difflayer (same logic as task 1)
                        // let storage_root = get_storage_root(hashed_address)?;
                        // let id = SecureTrieId::new(storage_root)
                        //     .with_owner(hashed_address);
                        // let mut storage_trie = SecureTrieBuilder::new(path_db_clone.clone())
                        //     .with_id(id)
                        //     .build_with_difflayer(difflayer_clone.as_ref())
                        //     .map_err(|e| TrieDBError::Database(format!("Failed to build storage trie for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;

                        // Apply updates before deletes (Geth-style). This reduces structural churn
                        // (collapse/split) during a batch of writes.
                        let mut updates = Vec::with_capacity(kvs_len);
                        let mut deletes = Vec::new();
                        for (hashed_key, new_value) in kvs {
                            if let Some(v) = new_value {
                                updates.push((hashed_key, v));
                            } else {
                                deletes.push(hashed_key);
                            }
                        }
                        storage_trie.trie_mut().reset_update_stats();
                        let apply_kvs_start = Instant::now();
                        for (hashed_key, new_value) in updates {
                            storage_trie
                                .update_storage_u256_with_hash_state(hashed_address, hashed_key, new_value)
                                .map_err(|e| {
                                    TrieDBError::Database(format!(
                                        "Failed to update storage for hashed_address: 0x{}, hashed_key: 0x{}, new_value: {:#x}, error: {}",
                                        hex::encode(hashed_address),
                                        hex::encode(hashed_key),
                                        new_value,
                                        e
                                    ))
                                })?;
                        }
                        for hashed_key in deletes {
                            storage_trie
                                .delete_storage_with_hash_state(hashed_address, hashed_key)
                                .map_err(|e| {
                                    TrieDBError::Database(format!(
                                        "Failed to delete storage for hashed_address: 0x{}, hashed_key: 0x{}, error: {}",
                                        hex::encode(hashed_address),
                                        hex::encode(hashed_key),
                                        e
                                    ))
                                })?;
                        }
                        let apply_kvs_duration = apply_kvs_start.elapsed();
                        let trie_update_stats = storage_trie.trie_mut().take_update_stats_snapshot();

                        let hash_start = Instant::now();
                        let new_storage_root = storage_trie.hash();
                        let hash_duration = hash_start.elapsed();
                        let mut new_account = accounts_clone.get(&hashed_address).unwrap().unwrap().clone();
                        new_account.storage_root = new_storage_root;

                        let duration = item_start.elapsed();
                        let slow = SlowestStorageItem {
                            hashed_address,
                            kvs_len,
                            duration,
                            apply_kvs_duration,
                            hash_duration,
                            trie_update_stats,
                            prefetch_storage_trie_hit,
                            storage_root_source,
                        };
                        Ok((hashed_address, (Some(new_account), new_storage_root, storage_trie, slow)))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vec| {
                        let items = vec.len();
                        let mut total_kvs = 0usize;
                        let mut slowest: Option<SlowestStorageItem> = None;
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        let mut storage_tries = HashMap::new();
                        for (hashed_address, (account, storage_root, storage_trie, item_slow)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                            storage_tries.insert(hashed_address, storage_trie);
                            total_kvs = total_kvs.saturating_add(item_slow.kvs_len);
                            let replace = slowest
                                .as_ref()
                                .map(|cur| item_slow.duration > cur.duration)
                                .unwrap_or(true);
                            if replace {
                                slowest = Some(item_slow);
                            }
                        }
                        ((new_accounts, diff_account_storage_roots, storage_tries, slowest), items, total_kvs)
                    });
                metrics_clone.record_intermediate_state_objects_storage_duration(task2_start.elapsed().as_secs_f64());
                let work = task2_start.elapsed();
                let (result, items, total_kvs, slowest) = match result {
                    Ok((r, items, total_kvs)) => {
                        let slowest = r.3.clone();
                        (Ok(r), items, total_kvs, slowest)
                    }
                    Err(e) => (Err(e), 0, 0, None),
                };
                let timing = TaskTiming { queue_delay, work, items, total_kvs, slowest };
                (result, timing)
            }
        ));
        let join_elapsed = join_start.elapsed();

        // Merge results
        let (mut accounts_no_storage, mut roots_no_storage) = account_result?;
        let (accounts_with_storage, roots_with_storage, storage_tries, slowest_storage) = storage_result?;

        accounts_no_storage.extend(accounts_with_storage);
        roots_no_storage.extend(roots_with_storage.into_iter());

        self.storage_tries = storage_tries;
        self.updated_storage_roots = roots_no_storage;

        // Log timing and context (including approximate queue delay for each join-branch).
        // Note: queue_delay indicates how long it took for the branch closure to start after
        // `rayon::join` was initiated (a proxy for pool contention/queueing).
        let (pathdb_trie_node_cache_hits, pathdb_trie_node_cache_misses, pathdb_trie_node_cache_hit_ratio) =
            if pathdb_cache_stats_present {
                let (h1, m1) = self.path_db.trie_node_cache_counters().unwrap_or((0, 0));
                let dh = h1.saturating_sub(pathdb_trie_node_cache_hits_start);
                let dm = m1.saturating_sub(pathdb_trie_node_cache_misses_start);
                let denom = dh.saturating_add(dm);
                let ratio = if denom == 0 { 0.0 } else { (dh as f64) / (denom as f64) };
                (dh, dm, ratio)
            } else {
                (0u64, 0u64, 0.0)
            };
        let (
            pathdb_rocksdb_trie_node_get_calls,
            pathdb_rocksdb_trie_node_get_found,
            pathdb_rocksdb_trie_node_get_not_found,
            pathdb_rocksdb_trie_node_get_errors,
            pathdb_rocksdb_trie_node_get_ms_total,
            pathdb_rocksdb_trie_node_get_avg_us,
        ) = if pathdb_rocksdb_stats_present {
            let (c1, f1, n1, e1, us1) = self.path_db.trie_node_rocksdb_counters().unwrap_or((0, 0, 0, 0, 0));
            let dc = c1.saturating_sub(pathdb_rocksdb_trie_node_get_calls_start);
            let df = f1.saturating_sub(pathdb_rocksdb_trie_node_get_found_start);
            let dn = n1.saturating_sub(pathdb_rocksdb_trie_node_get_not_found_start);
            let de = e1.saturating_sub(pathdb_rocksdb_trie_node_get_errors_start);
            let dus = us1.saturating_sub(pathdb_rocksdb_trie_node_get_us_total_start);
            let denom = if dc == 0 { 1 } else { dc };
            let avg_us = (dus as f64) / (denom as f64);
            (dc, df, dn, de, (dus as f64) / 1000.0, avg_us)
        } else {
            (0u64, 0u64, 0u64, 0u64, 0.0, 0.0)
        };

        if let Some(slowest) = slowest_storage.or_else(|| task2_timing.slowest.clone()) {
            let (
                slowest_trie_stats_present,
                slowest_trie_update_calls,
                slowest_trie_delete_calls,
                slowest_trie_key_to_nibbles_us,
                slowest_trie_value_alloc_bytes_total,
                slowest_trie_insert_internal_calls,
                slowest_trie_delete_internal_calls,
                slowest_trie_prefix_clone_bytes_total,
                slowest_trie_key_slice_to_vec_bytes_total,
                slowest_trie_shortnode_split_count,
                slowest_trie_fullnode_collapse_count,
                slowest_trie_resolve_calls,
                slowest_trie_resolve_difflayer_hits,
                slowest_trie_resolve_db_hits,
                slowest_trie_resolve_us,
                slowest_trie_resolve_decode_us,
                slowest_trie_resolve_blob_bytes_total,
                slowest_trie_node_key_alloc_bytes_total,
            ) = slowest
                .trie_update_stats
                .as_ref()
                .map(|s| {
                    (
                        true,
                        s.update_calls,
                        s.delete_calls,
                        s.key_to_nibbles_us,
                        s.value_alloc_bytes_total,
                        s.insert_internal_calls,
                        s.delete_internal_calls,
                        s.prefix_clone_bytes_total,
                        s.key_slice_to_vec_bytes_total,
                        s.shortnode_split_count,
                        s.fullnode_collapse_count,
                        s.resolve_calls,
                        s.resolve_difflayer_hits,
                        s.resolve_db_hits,
                        s.resolve_us,
                        s.resolve_decode_us,
                        s.resolve_blob_bytes_total,
                        s.node_key_alloc_bytes_total,
                    )
                })
                .unwrap_or((
                    false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ));
            debug!(
                target: "triedb::update_state_objects",
                accounts_len,
                storages_len,
                storages_keys_len = storages_keys.len(),
                total_ms = call_start.elapsed().as_secs_f64() * 1000.0,
                join_ms = join_elapsed.as_secs_f64() * 1000.0,
                task1_queue_ms = task1_timing.queue_delay.as_secs_f64() * 1000.0,
                task1_ms = task1_timing.work.as_secs_f64() * 1000.0,
                task1_items = task1_timing.items,
                task2_queue_ms = task2_timing.queue_delay.as_secs_f64() * 1000.0,
                task2_ms = task2_timing.work.as_secs_f64() * 1000.0,
                task2_items = task2_timing.items,
                task2_total_kvs = task2_timing.total_kvs,
                slowest_hashed_address = %hex::encode(slowest.hashed_address),
                slowest_ms = slowest.duration.as_secs_f64() * 1000.0,
                slowest_apply_kvs_ms = slowest.apply_kvs_duration.as_secs_f64() * 1000.0,
                slowest_hash_ms = slowest.hash_duration.as_secs_f64() * 1000.0,
                slowest_kvs = slowest.kvs_len,
                slowest_prefetch_storage_trie_hit = slowest.prefetch_storage_trie_hit,
                slowest_storage_root_source = slowest.storage_root_source,
                pathdb_cache_stats_present,
                pathdb_trie_node_cache_hits,
                pathdb_trie_node_cache_misses,
                pathdb_trie_node_cache_hit_ratio,
                pathdb_rocksdb_stats_present,
                pathdb_rocksdb_trie_node_get_calls,
                pathdb_rocksdb_trie_node_get_found,
                pathdb_rocksdb_trie_node_get_not_found,
                pathdb_rocksdb_trie_node_get_errors,
                pathdb_rocksdb_trie_node_get_ms_total,
                pathdb_rocksdb_trie_node_get_avg_us,
                slowest_trie_stats_present,
                slowest_trie_update_calls,
                slowest_trie_delete_calls,
                slowest_trie_key_to_nibbles_ms = (slowest_trie_key_to_nibbles_us as f64) / 1000.0,
                slowest_trie_value_alloc_bytes_total,
                slowest_trie_insert_internal_calls,
                slowest_trie_delete_internal_calls,
                slowest_trie_prefix_clone_bytes_total,
                slowest_trie_key_slice_to_vec_bytes_total,
                slowest_trie_shortnode_split_count,
                slowest_trie_fullnode_collapse_count,
                slowest_trie_resolve_calls,
                slowest_trie_resolve_difflayer_hits,
                slowest_trie_resolve_db_hits,
                slowest_trie_resolve_ms = (slowest_trie_resolve_us as f64) / 1000.0,
                slowest_trie_resolve_decode_ms = (slowest_trie_resolve_decode_us as f64) / 1000.0,
                slowest_trie_resolve_blob_bytes_total,
                slowest_trie_node_key_alloc_bytes_total,
                "update_state_objects timing"
            );
        } else {
            debug!(
                target: "triedb::update_state_objects",
                accounts_len,
                storages_len,
                storages_keys_len = storages_keys.len(),
                total_ms = call_start.elapsed().as_secs_f64() * 1000.0,
                join_ms = join_elapsed.as_secs_f64() * 1000.0,
                task1_queue_ms = task1_timing.queue_delay.as_secs_f64() * 1000.0,
                task1_ms = task1_timing.work.as_secs_f64() * 1000.0,
                task1_items = task1_timing.items,
                task2_queue_ms = task2_timing.queue_delay.as_secs_f64() * 1000.0,
                task2_ms = task2_timing.work.as_secs_f64() * 1000.0,
                task2_items = task2_timing.items,
                task2_total_kvs = task2_timing.total_kvs,
                pathdb_cache_stats_present,
                pathdb_trie_node_cache_hits,
                pathdb_trie_node_cache_misses,
                pathdb_trie_node_cache_hit_ratio,
                pathdb_rocksdb_stats_present,
                pathdb_rocksdb_trie_node_get_calls,
                pathdb_rocksdb_trie_node_get_found,
                pathdb_rocksdb_trie_node_get_not_found,
                pathdb_rocksdb_trie_node_get_errors,
                pathdb_rocksdb_trie_node_get_ms_total,
                pathdb_rocksdb_trie_node_get_avg_us,
                "update_state_objects timing (no slowest item)"
            );
        }
        
        Ok(accounts_no_storage)
    }

    fn commit_inner(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>, Arc<HashMap<B256, B256>>), TrieDBError>
    where
        DB: 'static,
    {
        let commit_start = Instant::now();
        let storage_tries_len = self.storage_tries.len();
        let updated_storage_roots_len = self.updated_storage_roots.len();

        let commit_state_objects_start = Instant::now();
        let (root_hash, node_set) = self.commit_state_objects(true)?;
        let commit_state_objects_elapsed = commit_state_objects_start.elapsed();
        self.metrics.record_commit_duration(commit_start.elapsed().as_secs_f64());

        let diff_roots_start = Instant::now();
        let diff_storage_roots = Arc::from(*self.updated_storage_roots.clone());
        let diff_roots_elapsed = diff_roots_start.elapsed();

        let clean_start = Instant::now();
        self.clean();
        let clean_elapsed = clean_start.elapsed();

        debug!(
            target: "triedb::commit_inner",
            storage_tries_len,
            updated_storage_roots_len,
            commit_state_objects_ms = commit_state_objects_elapsed.as_secs_f64() * 1000.0,
            diff_storage_roots_clone_ms = diff_roots_elapsed.as_secs_f64() * 1000.0,
            clean_ms = clean_elapsed.as_secs_f64() * 1000.0,
            total_ms = commit_start.elapsed().as_secs_f64() * 1000.0,
            "commit_inner timing"
        );

        Ok((root_hash, node_set, diff_storage_roots))
    }

    fn commit_state_objects(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>), TrieDBError> {        
        let mut merged_node_set = Box::new(MergedNodeSet::new());

        // Start both tasks in parallel using rayon
        let mut account_trie_clone = self.account_trie.as_mut().unwrap().clone();
        let join_start = Instant::now();
        let (account_commit_result, storage_commit_results): (Result<(B256, Option<Arc<NodeSet>>), _>, Vec<(B256, Option<Arc<NodeSet>>)>) =
            triedb_rayon_pool().install(|| rayon::join(
            || account_trie_clone.commit(true),
            || self.storage_tries
                .par_iter()
                .map(|(hashed_address, trie)| {
                    let (_, node_set) = trie.clone().commit(false).unwrap();
                    (*hashed_address, node_set)
                })
                .collect()
        ));
        let join_elapsed = join_start.elapsed();

        let (root_hash, account_node_set) = account_commit_result?;

        let merge_start = Instant::now();
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
        let merge_elapsed = merge_start.elapsed();

        debug!(
            target: "triedb::commit_state_objects",
            storage_tries_len = self.storage_tries.len(),
            join_ms = join_elapsed.as_secs_f64() * 1000.0,
            merge_ms = merge_elapsed.as_secs_f64() * 1000.0,
            total_ms = (join_elapsed + merge_elapsed).as_secs_f64() * 1000.0,
            "commit_state_objects timing"
        );
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
        let call_start = Instant::now();
        let prefetcher_enabled = prefetcher.is_some();
        let states_len = hashed_post_state.states.len();
        let storage_states_len = hashed_post_state.storage_states.len();
        let rebuild_len = hashed_post_state.states_rebuild.len();

        let state_at_start = Instant::now();
        self.state_at(parent_root, difflayer, prefetcher)?;
        let state_at_elapsed = state_at_start.elapsed();

        let intermediate_start = Instant::now();
        self.path_db.reset_trie_node_cache_counters();
        self.intermediate_inner(
            hashed_post_state.states.clone(),
            hashed_post_state.storage_states.clone(),
            hashed_post_state.states_rebuild.clone(),
        )?;
        let intermediate_elapsed = intermediate_start.elapsed();

        let commit_start = Instant::now();
        let out = self.commit(true)?;
        let commit_elapsed = commit_start.elapsed();

        let (trie_node_cache_hits, trie_node_cache_misses) =
            self.path_db.trie_node_cache_counters().unwrap_or((0, 0));
        let trie_node_cache_total = trie_node_cache_hits + trie_node_cache_misses;
        let trie_node_cache_hit_ratio = if trie_node_cache_total > 0 {
            trie_node_cache_hits as f64 / trie_node_cache_total as f64
        } else {
            0.0
        };

        debug!(
            target: "triedb::intermediate_and_commit_hashed_post_state",
            parent_root = %hex::encode(parent_root),
            prefetcher_enabled,
            states_len,
            storage_states_len,
            rebuild_len,
            state_at_ms = state_at_elapsed.as_secs_f64() * 1000.0,
            intermediate_ms = intermediate_elapsed.as_secs_f64() * 1000.0,
            commit_ms = commit_elapsed.as_secs_f64() * 1000.0,
            total_ms = call_start.elapsed().as_secs_f64() * 1000.0,
            trie_node_cache_hits,
            trie_node_cache_misses,
            trie_node_cache_hit_ratio = %format!("{:.4}", trie_node_cache_hit_ratio),
            "intermediate_and_commit_hashed_post_state timing"
        );

        Ok(out)
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

    pub fn intermediate_and_commit_hashed_post_state_v2(
        &mut self, 
        parent_root: B256, 
        difflayer: Option<&DiffLayers>, 
        hashed_post_state: &TrieDBHashedPostState, 
        prefetcher: Option<Arc<TrieDBPrefetchState<DB>>>) -> 
        Result<(B256, Arc<DiffLayer>), TrieDBError>
    where
        DB: 'static,
    {
        self.state_at(parent_root, difflayer, prefetcher)?;

        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());
        let accounts_clone = hashed_post_state.states.clone();
        let storages_keys: HashSet<B256> = hashed_post_state.storage_states.keys().cloned().collect();
        let storages_for_task2 = hashed_post_state.storage_states.clone();
        let metrics_clone = self.metrics.clone();
        let prefetcher_clone = self.prefetcher.clone();

        // Closure to get storage root from difflayer or path_db
        let get_storage_root = |hashed_address: B256| -> Result<B256, TrieDBError> {
            if hashed_post_state.states_rebuild.contains(&hashed_address) {
                return Ok(alloy_trie::EMPTY_ROOT_HASH);
            }

            if let Some(prefetcher) = &self.prefetcher {
                if let Some(root) = prefetcher.storage_roots.get(&hashed_address) {
                    return Ok(*root);
                }
            }

            if let Some(dl) = difflayer_clone.as_ref() {
                if let Some(root) = dl.get_storage_root(hashed_address) {
                    return Ok(root);
                }
            }
            path_db_clone.get_storage_root(hashed_address)
                .map_err(|e| TrieDBError::Database(format!("Failed to get storage root for hashed_address: 0x{}, error: {:?}", hex::encode(hashed_address), e)))
                .map(|opt| opt.unwrap_or(alloy_trie::EMPTY_ROOT_HASH))
        };

        // Parallel execution: process accounts and storages simultaneously
        let (account_result, storage_result): (
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>), TrieDBError>,
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>, Box<MergedNodeSet>), TrieDBError>
        ) = triedb_rayon_pool().install(|| rayon::join(
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

                        // Try to get storage_trie from prefetcher, otherwise create a new one
                        let mut storage_trie = match prefetcher_clone.as_ref()
                            .and_then(|p| p.storage_tries.get(&hashed_address))
                            .cloned()
                        {
                            Some(trie) => trie,
                            None => {
                                // Get storage root from path_db or difflayer
                                let storage_root = get_storage_root(hashed_address)?;
                                let id = SecureTrieId::new(storage_root)
                                    .with_owner(hashed_address);
                                SecureTrieBuilder::new(path_db_clone.clone())
                                    .with_id(id)
                                    .build_with_difflayer(difflayer_clone.as_ref())
                                    .map_err(|e| TrieDBError::Database(format!("Failed to build storage trie for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?
                            }
                        };

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

                        let (new_storage_root, node_set) = storage_trie.commit(false)?;
                        // If account is None (e.g. delete account with storage cleared), we still
                        // record (None, new_storage_root, node_set) so the diff and apply phase are correct.
                        let new_account = accounts_clone
                            .get(&hashed_address)
                            .and_then(|o| o.as_ref().cloned())
                            .map(|mut a| {
                                a.storage_root = new_storage_root;
                                a
                            });

                        Ok((hashed_address, (new_account, new_storage_root, node_set)))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|vec| {
                        let mut new_accounts = HashMap::new();
                        let mut diff_account_storage_roots = Box::new(HashMap::new());
                        let mut merged_node_set = Box::new(MergedNodeSet::new());
                        for (hashed_address, (account, storage_root, node_set)) in vec {
                            new_accounts.insert(hashed_address, account);
                            diff_account_storage_roots.insert(hashed_address, storage_root);
                            if let Some(node_set) = node_set {
                                merged_node_set.merge(node_set).unwrap();
                            }
                        }
                        (new_accounts, diff_account_storage_roots, merged_node_set)
                    });
                metrics_clone.record_intermediate_state_objects_storage_duration(task2_start.elapsed().as_secs_f64());
                result
            }
        ));

        let (mut accounts_no_storage, mut roots_no_storage) = account_result?;
        let (accounts_with_storage, roots_with_storage, mut merged_node_set) = storage_result?;

        accounts_no_storage.extend(accounts_with_storage);
        roots_no_storage.extend(roots_with_storage.into_iter());

        for hashed_address in hashed_post_state.states_rebuild.clone() {
            self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
        }
        
        for (hashed_address, account) in accounts_no_storage {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
        }

        let (root_hash, node_set) = self.account_trie.as_mut().unwrap().commit(true)?;
        if let Some(node_set) = node_set {
            merged_node_set.merge(node_set).unwrap();
        }

        let difflayer = Arc::new(DiffLayer::new(merged_node_set.to_diff_nodes(), Arc::from(*roots_no_storage)));
        self.clean();
        Ok((root_hash, difflayer))
    }
}


