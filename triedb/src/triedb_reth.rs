//! Reth-compatible implementations for TrieDB.

use std::sync::{Arc, Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use alloy_primitives::{B256, U256, hex};
use rust_eth_triedb_common::TrieDatabase;
use rust_eth_triedb_state_trie::node::{MergedNodeSet, NodeSet, DiffLayer, DiffLayers};
use rust_eth_triedb_state_trie::state_trie::StateTrie;
use rust_eth_triedb_state_trie::account::StateAccount;
use rust_eth_triedb_state_trie::{SecureTrieId, SecureTrieTrait, SecureTrieBuilder};

use crate::triedb::{TrieDB, TrieDBError};

/// Dedicated rayon thread-pool for trie batch work.
///
/// This isolates `update_state_objects` (task1/task2) from other rayon work in the process
/// (and vice versa), helping reduce tail-latency spikes caused by cross-component contention.
fn triedb_rayon_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        // Match rayon's global pool size by default, but isolate scheduling.
        let num_threads = 48;
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|i| format!("triedb-rayon-{i}"))
            .build()
            .expect("failed to build triedb rayon thread pool")
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
    /// Prefetched account trie.
    ///
    /// This is actively cache-warmed by the prefetcher task, therefore it must be synchronized.
    /// Callers should `clone()` it when handing off into a hot path to avoid cross-thread mutation.
    pub account_trie: Arc<Mutex<StateTrie<DB>>>,
    /// Best-effort storage root cache populated by the prefetcher task.
    pub storage_roots: Arc<RwLock<HashMap<B256, B256>>>,
    /// Best-effort prefetched storage tries, keyed by hashed address.
    ///
    /// This map is shared between the prefetcher and root-computation. Root computation may
    /// `take()` (remove) entries to transfer ownership and avoid cloning.
    pub storage_tries: Arc<RwLock<HashMap<B256, StateTrie<DB>>>>,
}

impl<DB> TrieDBPrefetchState<DB>
where
    DB: TrieDatabase + Clone + Send + Sync,
    DB::Error: std::fmt::Debug,
{
    /// Create an empty live prefetch state.
    pub fn new(account_trie: StateTrie<DB>) -> Self {
        Self {
            account_trie: Arc::new(Mutex::new(account_trie)),
            storage_roots: Arc::new(RwLock::new(HashMap::new())),
            storage_tries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Clone the prefetched account trie.
    ///
    /// This is intended for handing off into trie-root computation so it can be used without
    /// sharing mutable state with the background prefetcher.
    pub fn account_trie_clone(&self) -> StateTrie<DB> {
        self.account_trie
            .lock()
            .expect("TrieDBPrefetchState.account_trie lock poisoned")
            .clone()
    }

    /// Number of cached storage roots.
    pub fn storage_roots_len(&self) -> usize {
        self.storage_roots
            .read()
            .expect("TrieDBPrefetchState.storage_roots lock poisoned")
            .len()
    }

    /// Number of prefetched storage tries currently available.
    pub fn storage_tries_len(&self) -> usize {
        self.storage_tries
            .read()
            .expect("TrieDBPrefetchState.storage_tries lock poisoned")
            .len()
    }

    /// Returns the storage root for `hashed_address` if present in the prefetch cache.
    pub fn get_storage_root(&self, hashed_address: &B256) -> Option<B256> {
        self.storage_roots
            .read()
            .expect("TrieDBPrefetchState.storage_roots lock poisoned")
            .get(hashed_address)
            .copied()
    }

    /// Returns true if a storage root is cached for `hashed_address`.
    pub fn has_storage_root(&self, hashed_address: &B256) -> bool {
        self.get_storage_root(hashed_address).is_some()
    }

    /// Inserts a storage root if absent.
    pub fn insert_storage_root_if_absent(&self, hashed_address: B256, root: B256) {
        let mut g = self
            .storage_roots
            .write()
            .expect("TrieDBPrefetchState.storage_roots lock poisoned");
        g.entry(hashed_address).or_insert(root);
    }

    /// Inserts a prefetched storage trie if absent.
    pub fn insert_storage_trie_if_absent(&self, hashed_address: B256, trie: StateTrie<DB>) {
        let mut g = self
            .storage_tries
            .write()
            .expect("TrieDBPrefetchState.storage_tries lock poisoned");
        g.entry(hashed_address).or_insert(trie);
    }

    /// Removes and returns a prefetched storage trie, transferring ownership to the caller.
    pub fn take_storage_trie(&self, hashed_address: &B256) -> Option<StateTrie<DB>> {
        self.storage_tries
            .write()
            .expect("TrieDBPrefetchState.storage_tries lock poisoned")
            .remove(hashed_address)
    }
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
        
        let accounts_len = accounts.len();
        let storages_accounts_len = storages.len();
        let storages_slots_len: usize = storages.values().map(|m| m.len()).sum();
        let states_rebuild_len = states_rebuild.len();

        let intermediate_root_start = Instant::now();

        let intermediate_state_objects_start = Instant::now();
        let updated_accounts = self.update_state_objects(accounts, storages, states_rebuild.clone())?;
        let intermediate_state_objects_elapsed = intermediate_state_objects_start.elapsed();
        self.metrics.record_intermediate_state_objects_duration(intermediate_state_objects_elapsed.as_secs_f64());

        // Snapshot counts produced by update_state_objects
        let updated_accounts_len = updated_accounts.len();
        let updated_accounts_delete_len = updated_accounts.values().filter(|v| v.is_none()).count();
        let updated_accounts_update_len = updated_accounts_len - updated_accounts_delete_len;
        let produced_storage_tries_len = self.storage_tries.len();
        let produced_updated_storage_roots_len = self.updated_storage_roots.len();

        let delete_rebuild_start = Instant::now();
        for hashed_address in states_rebuild {
            self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
        }
        let delete_rebuild_elapsed = delete_rebuild_start.elapsed();
        
        let apply_updated_accounts_start = Instant::now();
        for (hashed_address, account) in updated_accounts {
            if let Some(account) = account {
                self.update_account_with_hash_state(hashed_address, &account)
                    .map_err(|e| TrieDBError::Database(format!("Failed to update account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            } else {
                self.delete_account_with_hash_state(hashed_address)
                    .map_err(|e| TrieDBError::Database(format!("Failed to delete account for hashed_address: 0x{}, error: {}", hex::encode(hashed_address), e)))?;
            }
        }
        let apply_updated_accounts_elapsed = apply_updated_accounts_start.elapsed();

        let account_trie_hash_start = Instant::now();
        let root_hash = self.account_trie.as_mut().unwrap().hash();
        let account_trie_hash_elapsed = account_trie_hash_start.elapsed();

        let intermediate_total_elapsed = intermediate_root_start.elapsed();
        self.metrics.record_intermediate_root_duration(intermediate_total_elapsed.as_secs_f64());

        tracing::debug!(
            target: "triedb::reth",
            total_ms = intermediate_total_elapsed.as_millis(),
            update_state_objects_ms = intermediate_state_objects_elapsed.as_millis(),
            delete_rebuild_ms = delete_rebuild_elapsed.as_millis(),
            apply_updated_accounts_ms = apply_updated_accounts_elapsed.as_millis(),
            account_trie_hash_ms = account_trie_hash_elapsed.as_millis(),
            accounts_len,
            storages_accounts_len,
            storages_slots_len,
            states_rebuild_len,
            updated_accounts_len,
            updated_accounts_update_len,
            updated_accounts_delete_len,
            produced_storage_tries_len,
            produced_updated_storage_roots_len,
            "intermediate_inner finished"
        );
        return Ok(root_hash);
    }

    fn update_state_objects (
        &mut self, 
        accounts: HashMap<B256, Option<StateAccount>>,
        storages: HashMap<B256, HashMap<B256, Option<U256>>>, 
        states_rebuild: HashSet<B256>) -> 
        Result<HashMap<B256, Option<StateAccount>>, TrieDBError> {
       
        let update_start = Instant::now();
        let accounts_len = accounts.len();
        let storages_accounts_len = storages.len();
        let storages_slots_len: usize = storages.values().map(|m| m.len()).sum();
        let states_rebuild_len = states_rebuild.len();
        let has_difflayer = self.difflayer.is_some();
        let has_prefetcher = self.prefetcher.is_some();

        // Prepare data for parallel execution
        let path_db_clone = self.path_db.clone();
        let difflayer_clone = self.difflayer.as_ref().map(|d| d.clone());
        let accounts_clone = Arc::new(accounts);
        let states_rebuild = Arc::new(states_rebuild);
        let storages_keys: HashSet<B256> = storages.keys().cloned().collect();
        let storages_for_task2 = storages;
        let metrics_clone = self.metrics.clone();
        let prefetcher_clone = self.prefetcher.clone();
        let prefetcher_roots = self.prefetcher.clone();

        #[derive(Default)]
        struct StorageRootStats {
            rebuild: AtomicUsize,
            prefetcher: AtomicUsize,
            difflayer: AtomicUsize,
            pathdb: AtomicUsize,
            pathdb_miss: AtomicUsize,
        }

        impl StorageRootStats {
            fn snapshot(&self) -> (usize, usize, usize, usize, usize) {
                (
                    self.rebuild.load(Ordering::Relaxed),
                    self.prefetcher.load(Ordering::Relaxed),
                    self.difflayer.load(Ordering::Relaxed),
                    self.pathdb.load(Ordering::Relaxed),
                    self.pathdb_miss.load(Ordering::Relaxed),
                )
            }
        }

        let make_get_storage_root = |stats: Arc<StorageRootStats>| {
            let states_rebuild = Arc::clone(&states_rebuild);
            let prefetcher_roots = prefetcher_roots.clone();
            let difflayer_clone = difflayer_clone.clone();
            let path_db_clone = path_db_clone.clone();
            move |hashed_address: B256| -> Result<B256, TrieDBError> {
                if states_rebuild.contains(&hashed_address) {
                    stats.rebuild.fetch_add(1, Ordering::Relaxed);
                    return Ok(alloy_trie::EMPTY_ROOT_HASH);
                }

                if let Some(prefetcher) = &prefetcher_roots {
                    if let Some(root) = prefetcher.get_storage_root(&hashed_address) {
                        stats.prefetcher.fetch_add(1, Ordering::Relaxed);
                        return Ok(root);
                    }
                }

                if let Some(dl) = difflayer_clone.as_ref() {
                    if let Some(root) = dl.get_storage_root(hashed_address) {
                        stats.difflayer.fetch_add(1, Ordering::Relaxed);
                        return Ok(root);
                    }
                }

                stats.pathdb.fetch_add(1, Ordering::Relaxed);
                path_db_clone
                    .get_storage_root(hashed_address)
                    .map_err(|e| {
                        TrieDBError::Database(format!(
                            "Failed to get storage root for hashed_address: 0x{}, error: {:?}",
                            hex::encode(hashed_address),
                            e
                        ))
                    })
                    .map(|opt| {
                        if opt.is_none() {
                            stats.pathdb_miss.fetch_add(1, Ordering::Relaxed);
                        }
                        opt.unwrap_or(alloy_trie::EMPTY_ROOT_HASH)
                    })
            }
        };

        let get_storage_root_task1_stats = Arc::new(StorageRootStats::default());
        let get_storage_root_task2_stats = Arc::new(StorageRootStats::default());
        let get_storage_root_task1 = make_get_storage_root(Arc::clone(&get_storage_root_task1_stats));
        let get_storage_root_task2 = make_get_storage_root(Arc::clone(&get_storage_root_task2_stats));

        #[derive(Clone, Copy, Debug, Default)]
        struct Task2QueueStats {
            wait_max_ms: u128,
            wait_sum_ms: u128,
            get_root_max_ms: u128,
            build_trie_max_ms: u128,
            apply_kvs_max_ms: u128,
            hash_max_ms: u128,
            total_max_ms: u128,
            slow_trie_logs: usize,
        }

        #[derive(Clone, Copy, Debug)]
        struct Task2SlowestTrie {
            hashed_address: B256,
            kvs_len: usize,
            wait_ms: u128,
            get_root_ms: u128,
            build_trie_ms: u128,
            apply_kvs_ms: u128,
            hash_ms: u128,
            total_ms: u128,
            prefetch_trie_hit: bool,
        }

        // Parallel execution: process accounts and storages simultaneously
        let pool = triedb_rayon_pool();
        let (
            (account_result, task1_elapsed),
            (storage_result, task2_elapsed, task2_queue_stats, task2_slowest_trie),
        ): (
            (
                Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>), TrieDBError>,
                std::time::Duration,
            ),
            (
                Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>, HashMap<B256, StateTrie<DB>>), TrieDBError>,
                std::time::Duration,
                Task2QueueStats,
                Option<Task2SlowestTrie>,
            ),
        ) = pool.install(|| rayon::join(
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
                                let storage_root = get_storage_root_task1(*hashed_address)?;
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
                let elapsed = task1_start.elapsed();
                metrics_clone.record_intermediate_state_objects_account_duration(elapsed.as_secs_f64());
                (result, elapsed)
            },
            || {
                // Task 2: Process accounts with storage updates (parallel)
                let task2_start = Instant::now();
                let storages_for_task2_len = storages_for_task2.len();
                const STORAGE_TRIE_SLOW_LOG_THRESHOLD_MS: u128 = 10;

                // Switch to explicit rayon::scope spawning so we can measure
                // queued_at -> exec_start latency (scheduling/queue wait).
                let (tx, rx) = std::sync::mpsc::channel::<
                    Result<(B256, (Option<StateAccount>, B256, StateTrie<DB>), Task2SlowestTrie), TrieDBError>,
                >();

                rayon::scope(|scope| {
                    for (hashed_address, kvs) in storages_for_task2 {
                        let tx = tx.clone();
                        let queued_at = Instant::now();

                        let path_db_clone = path_db_clone.clone();
                        let difflayer_clone = difflayer_clone.clone();
                        let prefetcher_clone = prefetcher_clone.clone();
                        let states_rebuild = Arc::clone(&states_rebuild);
                        let accounts_clone = Arc::clone(&accounts_clone);
                        let get_storage_root_task2 = &get_storage_root_task2;

                        scope.spawn(move |_| {
                            let exec_start = Instant::now();
                            let wait_ms = exec_start.duration_since(queued_at).as_millis();
                            let kvs_len = kvs.len();
                            let per_trie_start = exec_start;

                            let res: Result<(B256, (Option<StateAccount>, B256, StateTrie<DB>), Task2SlowestTrie), TrieDBError> =
                                (|| {
                                    // Try to get storage_trie from prefetcher, otherwise create a new one
                                    let (mut storage_trie, prefetch_trie_hit, get_root_ms, build_trie_ms) =
                                        match prefetcher_clone
                                            .as_ref()
                                            .and_then(|p| p.take_storage_trie(&hashed_address))
                                        {
                                            Some(trie) => (trie, true, 0u128, 0u128),
                                            None => {
                                                let get_root_start = Instant::now();
                                                let storage_root = get_storage_root_task2(hashed_address)?;
                                                let get_root_ms = get_root_start.elapsed().as_millis();

                                                let build_start = Instant::now();
                                                let id = SecureTrieId::new(storage_root)
                                                    .with_owner(hashed_address);
                                                let trie = SecureTrieBuilder::new(path_db_clone)
                                                    .with_id(id)
                                                    .build_with_difflayer(difflayer_clone.as_ref())
                                                    .map_err(|e| {
                                                        TrieDBError::Database(format!(
                                                            "Failed to build storage trie for hashed_address: 0x{}, error: {}",
                                                            hex::encode(hashed_address),
                                                            e
                                                        ))
                                                    })?;
                                                let build_trie_ms = build_start.elapsed().as_millis();
                                                (trie, false, get_root_ms, build_trie_ms)
                                            }
                                        };

                                    let apply_start = Instant::now();
                                    // Reset diagnostic-only counters to attribute resolve stats to this apply window.
                                    storage_trie.trie_mut().reset_resolve_stats();
                                    let tracer_inserts_before = storage_trie.trie().tracer.inserts().len();
                                    let tracer_deletes_before = storage_trie.trie().tracer.deletes().len();
                                    let tracer_access_before = storage_trie.trie().tracer.access_list().len();
                                    let mut kvs_updates: usize = 0;
                                    let mut kvs_deletes: usize = 0;

                                    // Keep the original semantics (apply updates for this address sequentially),
                                    // but make the iteration order deterministic and locality-friendly.
                                    //
                                    // `HashMap` iteration order is randomized, which tends to thrash trie
                                    // cursor locality (more divergent prefixes) and increases tracer
                                    // bookkeeping. Sorting by hashed key improves cache behavior and tends
                                    // to reduce the `apply_kvs` CPU cost on large batches.
                                    let mut kvs_sorted: Vec<(B256, Option<U256>)> =
                                        kvs.into_iter().collect();
                                    kvs_sorted.sort_unstable_by(|(a, _), (b, _)| a.as_slice().cmp(b.as_slice()));

                                    for (hashed_key, new_value) in kvs_sorted.into_iter() {
                                        if let Some(new_value) = new_value {
                                            kvs_updates += 1;
                                            storage_trie.update_storage_u256_with_hash_state(
                                                hashed_address,
                                                hashed_key,
                                                new_value,
                                            )
                                            .map_err(|e| {
                                                TrieDBError::Database(format!(
                                                    "Failed to update storage for hashed_address: 0x{}, hashed_key: 0x{}, new_value: {:#x}, error: {}",
                                                    hex::encode(hashed_address),
                                                    hex::encode(hashed_key),
                                                    new_value,
                                                    e
                                                ))
                                            })?;
                                        } else {
                                            kvs_deletes += 1;
                                            storage_trie
                                                .delete_storage_with_hash_state(
                                                    hashed_address,
                                                    hashed_key,
                                                )
                                                .map_err(|e| {
                                                    TrieDBError::Database(format!(
                                                        "Failed to delete storage for hashed_address: 0x{}, hashed_key: 0x{}, error: {}",
                                                        hex::encode(hashed_address),
                                                        hex::encode(hashed_key),
                                                        e
                                                    ))
                                                })?;
                                        }
                                    }
                                    let apply_kvs_ms = apply_start.elapsed().as_millis();
                                    if apply_kvs_ms >= 100 {
                                        let storage_root_source = if prefetch_trie_hit {
                                            "prefetch_trie"
                                        } else if states_rebuild.contains(&hashed_address) {
                                            "rebuild"
                                        } else if prefetcher_clone
                                            .as_ref()
                                            .map_or(false, |p| p.has_storage_root(&hashed_address))
                                        {
                                            "prefetch_root"
                                        } else if difflayer_clone
                                            .as_ref()
                                            .and_then(|dl| dl.get_storage_root(hashed_address))
                                            .is_some()
                                        {
                                            "difflayer"
                                        } else {
                                            "pathdb"
                                        };
                                        let prefetch_storage_roots_len = prefetcher_clone
                                            .as_ref()
                                            .map(|p| p.storage_roots_len());
                                        let prefetch_storage_tries_len = prefetcher_clone
                                            .as_ref()
                                            .map(|p| p.storage_tries_len());
                                        let rs = storage_trie.trie().resolve_stats();
                                        let tracer_inserts_after = storage_trie.trie().tracer.inserts().len();
                                        let tracer_deletes_after = storage_trie.trie().tracer.deletes().len();
                                        let tracer_access_after = storage_trie.trie().tracer.access_list().len();
                                        tracing::debug!(
                                            target: "triedb::reth",
                                            hashed_address = ?hashed_address,
                                            kvs_len,
                                            kvs_updates,
                                            kvs_deletes,
                                            apply_kvs_ms,
                                            storage_root_source,
                                            prefetch_storage_roots_len,
                                            prefetch_storage_tries_len,
                                            // resolve path stats during apply window
                                            resolve_calls = rs.calls,
                                            resolve_difflayer_hits = rs.difflayer_hits,
                                            resolve_db_hits = rs.db_hits,
                                            resolve_misses = rs.misses,
                                            resolve_bytes_read = rs.bytes_read,
                                            // tracer sizes (use deltas to understand how much bookkeeping grew)
                                            tracer_inserts_before,
                                            tracer_inserts_after,
                                            tracer_deletes_before,
                                            tracer_deletes_after,
                                            tracer_access_before,
                                            tracer_access_after,
                                            prefetch_trie_hit,
                                            rayon_thread = ?rayon::current_thread_index(),
                                            "storage_trie task2 apply_kvs longtail"
                                        );
                                    }

                                    let hash_start = Instant::now();
                                    let new_storage_root = storage_trie.hash();
                                    let hash_ms = hash_start.elapsed().as_millis();

                                    let mut new_account = accounts_clone
                                        .get(&hashed_address)
                                        .unwrap()
                                        .as_ref()
                                        .unwrap()
                                        .clone();
                                    new_account.storage_root = new_storage_root;

                                    let total_ms = per_trie_start.elapsed().as_millis();
                                    let timing = Task2SlowestTrie {
                                        hashed_address,
                                        kvs_len,
                                        wait_ms,
                                        get_root_ms,
                                        build_trie_ms,
                                        apply_kvs_ms,
                                        hash_ms,
                                        total_ms,
                                        prefetch_trie_hit,
                                    };

                                    if total_ms >= STORAGE_TRIE_SLOW_LOG_THRESHOLD_MS {
                                        let storage_root_source = if prefetch_trie_hit {
                                            "prefetch_trie"
                                        } else if states_rebuild.contains(&hashed_address) {
                                            "rebuild"
                                        } else if prefetcher_clone
                                            .as_ref()
                                            .map_or(false, |p| p.has_storage_root(&hashed_address))
                                        {
                                            "prefetch_root"
                                        } else if difflayer_clone
                                            .as_ref()
                                            .and_then(|dl| dl.get_storage_root(hashed_address))
                                            .is_some()
                                        {
                                            "difflayer"
                                        } else {
                                            "pathdb"
                                        };
                                        tracing::debug!(
                                            target: "triedb::reth",
                                            hashed_address = ?hashed_address,
                                            kvs_len,
                                            wait_ms,
                                            get_root_ms,
                                            build_trie_ms,
                                            apply_kvs_ms,
                                            hash_ms,
                                            total_ms,
                                            prefetch_trie_hit,
                                            storage_root_source,
                                            rayon_thread = ?rayon::current_thread_index(),
                                            "storage_trie task2 slow"
                                        );
                                    }

                                    Ok((
                                        hashed_address,
                                        (Some(new_account), new_storage_root, storage_trie),
                                        timing,
                                    ))
                                })();

                            let _ = tx.send(res);
                        });
                    }
                });

                drop(tx);

                // Collect results.
                let collected: Result<
                    (Vec<(B256, (Option<StateAccount>, B256, StateTrie<DB>))>, Task2QueueStats, Option<Task2SlowestTrie>),
                    TrieDBError,
                > = (|| {
                    let mut vec = Vec::with_capacity(storages_for_task2_len);
                    let mut stats = Task2QueueStats::default();
                    let mut slowest: Option<Task2SlowestTrie> = None;

                    for _ in 0..storages_for_task2_len {
                        let msg = rx.recv().map_err(|e| {
                            TrieDBError::Database(format!("task2 result channel dropped: {e}"))
                        })?;
                        let (hashed_address, data, timing) = msg?;

                        stats.wait_sum_ms += timing.wait_ms;
                        stats.wait_max_ms = stats.wait_max_ms.max(timing.wait_ms);
                        stats.get_root_max_ms = stats.get_root_max_ms.max(timing.get_root_ms);
                        stats.build_trie_max_ms = stats.build_trie_max_ms.max(timing.build_trie_ms);
                        stats.apply_kvs_max_ms = stats.apply_kvs_max_ms.max(timing.apply_kvs_ms);
                        stats.hash_max_ms = stats.hash_max_ms.max(timing.hash_ms);
                        stats.total_max_ms = stats.total_max_ms.max(timing.total_ms);
                        if timing.total_ms >= STORAGE_TRIE_SLOW_LOG_THRESHOLD_MS {
                            stats.slow_trie_logs += 1;
                        }

                        slowest = match slowest {
                            None => Some(timing),
                            Some(prev) => Some(if timing.total_ms >= prev.total_ms { timing } else { prev }),
                        };

                        vec.push((hashed_address, data));
                    }

                    Ok((vec, stats, slowest))
                })();

                let (vec, stats, slowest) = match collected {
                    Ok(v) => v,
                    Err(err) => {
                        let elapsed = task2_start.elapsed();
                        metrics_clone.record_intermediate_state_objects_storage_duration(elapsed.as_secs_f64());
                        return (Err(err), elapsed, Task2QueueStats::default(), None);
                    }
                };

                let result = Ok(vec).map(|vec| {
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
                let elapsed = task2_start.elapsed();
                metrics_clone.record_intermediate_state_objects_storage_duration(elapsed.as_secs_f64());
                (result, elapsed, stats, slowest)
            }
        ));

        let merge_start = Instant::now();
        // Merge results
        let (mut accounts_no_storage, mut roots_no_storage) = account_result?;
        let (accounts_with_storage, roots_with_storage, storage_tries) = storage_result?;

        accounts_no_storage.extend(accounts_with_storage);
        roots_no_storage.extend(roots_with_storage.into_iter());

        self.storage_tries = storage_tries;
        self.updated_storage_roots = roots_no_storage;
        let merge_elapsed = merge_start.elapsed();

        let total_elapsed = update_start.elapsed();
        let (t1_rebuild, t1_prefetch, t1_dl, t1_pathdb, t1_pathdb_miss) =
            get_storage_root_task1_stats.snapshot();
        let (t2_rebuild, t2_prefetch, t2_dl, t2_pathdb, t2_pathdb_miss) =
            get_storage_root_task2_stats.snapshot();

        tracing::debug!(
            target: "triedb::reth",
            total_ms = total_elapsed.as_millis(),
            task1_ms = task1_elapsed.as_millis(),
            task2_ms = task2_elapsed.as_millis(),
            merge_ms = merge_elapsed.as_millis(),
            task2_wait_max_ms = task2_queue_stats.wait_max_ms,
            task2_wait_avg_ms = if storages_accounts_len == 0 { 0 } else { task2_queue_stats.wait_sum_ms / storages_accounts_len as u128 },
            task2_get_root_max_ms = task2_queue_stats.get_root_max_ms,
            task2_build_trie_max_ms = task2_queue_stats.build_trie_max_ms,
            task2_apply_kvs_max_ms = task2_queue_stats.apply_kvs_max_ms,
            task2_hash_max_ms = task2_queue_stats.hash_max_ms,
            task2_total_max_ms = task2_queue_stats.total_max_ms,
            task2_slow_trie_logs = task2_queue_stats.slow_trie_logs,
            task2_slowest_hashed_address = ?task2_slowest_trie.map(|t| t.hashed_address),
            task2_slowest_total_ms = task2_slowest_trie.map(|t| t.total_ms),
            task2_slowest_wait_ms = task2_slowest_trie.map(|t| t.wait_ms),
            task2_slowest_kvs_len = task2_slowest_trie.map(|t| t.kvs_len),
            task2_slowest_prefetch_trie_hit = task2_slowest_trie.map(|t| t.prefetch_trie_hit),
            accounts_len,
            storages_accounts_len,
            storages_slots_len,
            states_rebuild_len,
            has_difflayer,
            has_prefetcher,
            accounts_no_storage_len = accounts_len.saturating_sub(storages_accounts_len),
            updated_accounts_len = self.updated_storage_roots.len(), // same keys as updated_accounts output
            produced_storage_tries_len = self.storage_tries.len(),
            t1_get_root_rebuild = t1_rebuild,
            t1_get_root_prefetcher = t1_prefetch,
            t1_get_root_difflayer = t1_dl,
            t1_get_root_pathdb = t1_pathdb,
            t1_get_root_pathdb_miss = t1_pathdb_miss,
            t2_get_root_rebuild = t2_rebuild,
            t2_get_root_prefetcher = t2_prefetch,
            t2_get_root_difflayer = t2_dl,
            t2_get_root_pathdb = t2_pathdb,
            t2_get_root_pathdb_miss = t2_pathdb_miss,
            "update_state_objects finished"
        );
        
        Ok(accounts_no_storage)
    }

    fn commit_inner(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>, Arc<HashMap<B256, B256>>), TrieDBError>
    where
        DB: 'static,
    {
        let updated_storage_roots_len = self.updated_storage_roots.len();
        let storage_tries_len = self.storage_tries.len();
        let commit_start = Instant::now();
        let (root_hash, node_set) = self.commit_state_objects(true)?;
        let commit_state_objects_elapsed = commit_start.elapsed();
        self.metrics.record_commit_duration(commit_state_objects_elapsed.as_secs_f64());

        let diff_storage_roots_start = Instant::now();
        let diff_storage_roots = Arc::from(*self.updated_storage_roots.clone());
        let diff_storage_roots_elapsed = diff_storage_roots_start.elapsed();

        let clean_start = Instant::now();
        self.clean();
        let clean_elapsed = clean_start.elapsed();

        tracing::debug!(
            target: "triedb::reth",
            commit_state_objects_ms = commit_state_objects_elapsed.as_millis(),
            diff_storage_roots_ms = diff_storage_roots_elapsed.as_millis(),
            clean_ms = clean_elapsed.as_millis(),
            updated_storage_roots_len,
            storage_tries_len,
            "commit_inner finished"
        );

        Ok((root_hash, node_set, diff_storage_roots))
    }

    fn commit_state_objects(&mut self, _collect_leaf: bool) -> Result<(B256, Arc<MergedNodeSet>), TrieDBError> {        
        let commit_state_objects_start = Instant::now();
        let storage_tries_len = self.storage_tries.len();
        let mut merged_node_set = Box::new(MergedNodeSet::new());

        // Start both tasks in parallel using rayon
        let pool = triedb_rayon_pool();
        let mut account_trie_clone = self.account_trie.as_mut().unwrap().clone();
        let ((account_commit_result, account_commit_elapsed), (storage_commit_results, storage_commit_elapsed)): (
            (Result<(B256, Option<Arc<NodeSet>>), _>, std::time::Duration),
            (Vec<(B256, Option<Arc<NodeSet>>)>, std::time::Duration),
        ) = pool.install(|| rayon::join(
            || {
                let start = Instant::now();
                let res = account_trie_clone.commit(true);
                (res, start.elapsed())
            },
            || {
                let start = Instant::now();
                let res = self
                    .storage_tries
                    .par_iter()
                    .map(|(hashed_address, trie)| {
                        let (_, node_set) = trie.clone().commit(false).unwrap();
                        (*hashed_address, node_set)
                    })
                    .collect();
                (res, start.elapsed())
            },
        ));

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

        let total_elapsed = commit_state_objects_start.elapsed();
        // High-signal commit size stats: only compute bytes/deletes for slow commits to avoid
        // adding overhead to the fast path.
        let merged_sets_len = merged_node_set.sets.len();
        let diff_nodes_len = merged_node_set.difflayer.len();
        let slow_commit = total_elapsed.as_millis() >= 50;
        // Split stats between account trie (owner == B256::ZERO) and storage tries (owner != ZERO).
        let (account_updates, account_deletes, account_nodes, account_leaves) = merged_node_set
            .sets
            .get(&B256::ZERO)
            .map(|set| (set.updates, set.deletes, set.nodes.len(), set.leaf_count()))
            .unwrap_or((0, 0, 0, 0));
        let (storage_updates, storage_deletes, storage_nodes, storage_leaves) = merged_node_set
            .sets
            .iter()
            .filter(|(owner, _)| **owner != B256::ZERO)
            .fold((0usize, 0usize, 0usize, 0usize), |(u, d, n, l), (_, set)| {
                (
                    u + set.updates,
                    d + set.deletes,
                    n + set.nodes.len(),
                    l + set.leaf_count(),
                )
            });
        let merged_updates = account_updates + storage_updates;
        let merged_deletes = account_deletes + storage_deletes;
        let merged_nodes = account_nodes + storage_nodes;
        let merged_leaves = account_leaves + storage_leaves;

        // Byte-size estimates of difflayer writes; only compute on slow commits.
        let (account_diff_nodes_len, account_diff_nodes_bytes, account_diff_nodes_deletes) = slow_commit
            .then(|| {
                merged_node_set
                    .sets
                    .get(&B256::ZERO)
                    .map(|set| {
                        let len = set.difflayer.len();
                        let bytes: usize = set.difflayer.values().map(|n| n.size()).sum();
                        let deletes: usize = set.difflayer.values().filter(|n| n.is_deleted()).count();
                        (len, Some(bytes), Some(deletes))
                    })
                    .unwrap_or((0usize, Some(0usize), Some(0usize)))
            })
            .unwrap_or((0usize, None, None));

        let (storage_diff_nodes_len, storage_diff_nodes_bytes, storage_diff_nodes_deletes) = slow_commit
            .then(|| {
                let mut len: usize = 0;
                let mut bytes: usize = 0;
                let mut deletes: usize = 0;
                for (owner, set) in merged_node_set.sets.iter() {
                    if *owner == B256::ZERO {
                        continue;
                    }
                    len += set.difflayer.len();
                    bytes += set.difflayer.values().map(|n| n.size()).sum::<usize>();
                    deletes += set.difflayer.values().filter(|n| n.is_deleted()).count();
                }
                (len, Some(bytes), Some(deletes))
            })
            .unwrap_or((0usize, None, None));

        // For convenience, also keep the total difflayer bytes/deletes.
        let diff_nodes_bytes: Option<usize> = slow_commit
            .then(|| merged_node_set.difflayer.values().map(|n| n.size()).sum());
        let diff_nodes_deletes: Option<usize> = slow_commit
            .then(|| merged_node_set.difflayer.values().filter(|n| n.is_deleted()).count());
        tracing::debug!(
            target: "triedb::reth",
            total_ms = total_elapsed.as_millis(),
            account_commit_ms = account_commit_elapsed.as_millis(),
            storage_commit_ms = storage_commit_elapsed.as_millis(),
            merge_nodesets_ms = merge_elapsed.as_millis(),
            storage_tries_len,
            merged_sets_len,
            merged_updates,
            merged_deletes,
            merged_nodes,
            merged_leaves,
            account_updates,
            account_deletes,
            account_nodes,
            account_leaves,
            storage_updates,
            storage_deletes,
            storage_nodes,
            storage_leaves,
            diff_nodes_len,
            diff_nodes_bytes,
            diff_nodes_deletes,
            account_diff_nodes_len,
            account_diff_nodes_bytes,
            account_diff_nodes_deletes,
            storage_diff_nodes_len,
            storage_diff_nodes_bytes,
            storage_diff_nodes_deletes,
            "commit_state_objects finished"
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
        let start = Instant::now();
        let has_difflayer = difflayer.is_some();
        let has_prefetcher = prefetcher.is_some();
        let accounts_len = hashed_post_state.states.len();
        let storages_accounts_len = hashed_post_state.storage_states.len();
        let storages_slots_len: usize = hashed_post_state.storage_states.values().map(|m| m.len()).sum();
        let states_rebuild_len = hashed_post_state.states_rebuild.len();

        let state_at_start = Instant::now();
        self.state_at(parent_root, difflayer, prefetcher)?;
        let state_at_elapsed = state_at_start.elapsed();

        let intermediate_start = Instant::now();
        self.intermediate_inner(
            hashed_post_state.states.clone(),
            hashed_post_state.storage_states.clone(),
            hashed_post_state.states_rebuild.clone(),
        )?;
        let intermediate_elapsed = intermediate_start.elapsed();

        let commit_start = Instant::now();
        let out = self.commit(true);
        let commit_elapsed = commit_start.elapsed();

        let total_elapsed = start.elapsed();
        tracing::debug!(
            target: "triedb::reth",
            total_ms = total_elapsed.as_millis(),
            state_at_ms = state_at_elapsed.as_millis(),
            intermediate_ms = intermediate_elapsed.as_millis(),
            commit_ms = commit_elapsed.as_millis(),
            has_difflayer,
            has_prefetcher,
            accounts_len,
            storages_accounts_len,
            storages_slots_len,
            states_rebuild_len,
            parent_root = ?parent_root,
            "intermediate_and_commit_hashed_post_state finished"
        );

        out
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
        let start = Instant::now();
        let (root_hash, node_set, diff_storage_roots) = self.commit_inner(true)?;
        let commit_inner_elapsed = start.elapsed();

        let difflayer_start = Instant::now();
        let difflayer = Arc::new(DiffLayer::new(node_set.to_diff_nodes(), diff_storage_roots));
        let difflayer_elapsed = difflayer_start.elapsed();

        tracing::debug!(
            target: "triedb::reth",
            commit_inner_ms = commit_inner_elapsed.as_millis(),
            difflayer_build_ms = difflayer_elapsed.as_millis(),
            "commit finished"
        );
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
                if let Some(root) = prefetcher.get_storage_root(&hashed_address) {
                    return Ok(root);
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
        let pool = triedb_rayon_pool();
        let (account_result, storage_result): (
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>), TrieDBError>,
            Result<(HashMap<B256, Option<StateAccount>>, Box<HashMap<B256, B256>>, Box<MergedNodeSet>), TrieDBError>
        ) = pool.install(|| rayon::join(
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
                            .and_then(|p| p.take_storage_trie(&hashed_address))
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
                        let mut new_account = accounts_clone.get(&hashed_address).unwrap().unwrap().clone();
                        new_account.storage_root = new_storage_root;

                        Ok((hashed_address, (Some(new_account), new_storage_root, node_set)))
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


