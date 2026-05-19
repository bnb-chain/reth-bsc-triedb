//! Per-block trie node lookup path counters.
//!
//! Three globally-shared counters track where each `resolve_and_track`
//! style query landed:
//!
//!   - `TRIE_DIFFLAYER_HITS`: query found a node inside the `DiffLayers`
//!     chain (in-memory recent-block diff layers, ≤ 256 layers).
//!   - `TRIE_MOKA_HITS`: query missed every diff layer and was served from
//!     the PathDB moka cache (also in-memory).
//!   - `TRIE_DISK_READS`: query missed both diff layers and moka, so
//!     PathDB went to the RocksDB column family (page-cache or disk I/O).
//!
//! Snapshot + reset is invoked once per block from the builder right after
//! the triedb state-root call returns. The resulting counts and ratios
//! attribute every state-root trie probe to one of the three tiers.
//!
//! Each atomic is padded to a 64-byte cache line so the three counters
//! never share a line. Without this padding, the state-root main thread
//! and the prefetcher would ping-pong the same line on every increment
//! and the measurement itself would dominate the cost (~20 ms/block for
//! 26 K queries/block on Apple silicon under contention).

use std::sync::atomic::{AtomicU64, Ordering};

/// `AtomicU64` aligned to a 64-byte cache line. Used to keep each
/// counter on its own line so concurrent increments from different
/// threads don't false-share.
#[repr(align(64))]
pub struct PaddedAtomicU64(pub AtomicU64);

impl PaddedAtomicU64 {
    pub const fn new(v: u64) -> Self {
        Self(AtomicU64::new(v))
    }

    #[inline(always)]
    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn swap_to_zero(&self) -> u64 {
        self.0.swap(0, Ordering::Relaxed)
    }
}

/// DiffLayer chain hits — query found a node inside one of the in-memory
/// diff layers. Incremented from `DiffLayers::get_trie_nodes`.
pub static TRIE_DIFFLAYER_HITS: PaddedAtomicU64 = PaddedAtomicU64::new(0);

/// PathDB moka cache hits — query missed every diff layer and was served
/// from the in-memory cache below. Incremented from
/// `PathDB::get_raw_trie_node`.
pub static TRIE_MOKA_HITS: PaddedAtomicU64 = PaddedAtomicU64::new(0);

/// PathDB RocksDB reads — query missed both diff layers and the moka
/// cache, so the column-family read was issued (may still hit the OS
/// page cache rather than physical disk). Incremented from
/// `PathDB::get_raw_trie_node` right before the `get_cf_opt` call.
pub static TRIE_DISK_READS: PaddedAtomicU64 = PaddedAtomicU64::new(0);

/// Plain-data snapshot of the three counters for emit to logs.
#[derive(Debug, Default, Clone, Copy)]
pub struct TrieLookupSnapshot {
    pub diff_hits: u64,
    pub moka_hits: u64,
    pub disk_reads: u64,
}

impl TrieLookupSnapshot {
    /// Sum of all three tiers. Equals the number of `get_trie_node`
    /// calls observed since the last reset (the resolve_and_track total
    /// plus any direct PathDB callers like the prefetcher).
    pub fn total(&self) -> u64 {
        self.diff_hits + self.moka_hits + self.disk_reads
    }
}

/// Atomically read all three counters and zero them out, returning the
/// snapshot. Intended to be called once per block at a quiescent
/// boundary so each block's emit captures only its own activity.
pub fn snapshot_and_reset() -> TrieLookupSnapshot {
    TrieLookupSnapshot {
        diff_hits: TRIE_DIFFLAYER_HITS.swap_to_zero(),
        moka_hits: TRIE_MOKA_HITS.swap_to_zero(),
        disk_reads: TRIE_DISK_READS.swap_to_zero(),
    }
}
