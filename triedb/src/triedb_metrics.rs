//! Metrics for TrieDB operations.

use reth_metrics::{
    metrics::{Histogram, Counter},
    Metrics,
};

/// Metrics for the `TrieDB`.
#[derive(Metrics, Clone)]
#[metrics(scope = "rust.eth.triedb")]
pub(crate) struct TrieDBMetrics {
    /// Histogram of intermediate root durations (in seconds)
    pub(crate) intermediate_root_histogram: Histogram,
    /// Histogram of update state objects durations (in seconds)
    pub(crate) intermediate_state_objects_histogram: Histogram,
    /// Histogram of intermediate state objects account durations (in seconds)
    pub(crate) intermediate_state_objects_account_histogram: Histogram,
    /// Histogram of intermediate state objects storage durations (in seconds)
    pub(crate) intermediate_state_objects_storage_histogram: Histogram,
    /// Histogram of account trie hash durations (in seconds)
    pub(crate) hash_histogram: Histogram,
    /// Histogram of commit durations (in seconds)
    pub(crate) commit_histogram: Histogram,
    /// Histogram of flush durations (in seconds)
    pub(crate) flush_histogram: Histogram,

    /// Counter of storage root lookups resolved from prefetcher
    pub(crate) storage_root_from_prefetcher_counter: Counter,
    /// Counter of storage root lookups resolved from diff layer
    pub(crate) storage_root_from_difflayer_counter: Counter,
    /// Counter of storage root lookups resolved from path database
    pub(crate) storage_root_from_pathdb_counter: Counter,
}

impl TrieDBMetrics {
    pub(crate) fn record_intermediate_root_duration(&self, duration: f64) {
        self.intermediate_root_histogram.record(duration);
    }

    pub(crate) fn record_intermediate_state_objects_duration(&self, duration: f64) {
        self.intermediate_state_objects_histogram.record(duration);
    }

    pub(crate) fn record_intermediate_state_objects_account_duration(&self, duration: f64) {
        self.intermediate_state_objects_account_histogram.record(duration);
    }

    pub(crate) fn record_intermediate_state_objects_storage_duration(&self, duration: f64) {
        self.intermediate_state_objects_storage_histogram.record(duration);
    }

    pub(crate) fn record_hash_duration(&self, duration: f64) {
        self.hash_histogram.record(duration);
    }

    pub(crate) fn record_commit_duration(&self, duration: f64) {
        self.commit_histogram.record(duration);
    }

    pub(crate) fn record_flush_duration(&self, duration: f64) {
        self.flush_histogram.record(duration);
    }

    pub(crate) fn increment_storage_root_from_prefetcher_counter(&self) {
        self.storage_root_from_prefetcher_counter.increment(1);
    }

    pub(crate) fn increment_storage_root_from_difflayer_counter(&self) {
        self.storage_root_from_difflayer_counter.increment(1);
    }

    pub(crate) fn increment_storage_root_from_pathdb_counter(&self) {
        self.storage_root_from_pathdb_counter.increment(1);
    }
}

