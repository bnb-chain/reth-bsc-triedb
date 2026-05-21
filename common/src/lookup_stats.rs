//! Per-block trie node lookup path counter.
//!
//! Exposes one global counter:
//!
//!   - `TRIE_DIFFLAYER_HITS`: bumped from `DiffLayers::get_trie_nodes`
//!     every time the in-memory diff-layer chain serves a lookup
//!     (before any PathDB / RocksDB consultation).
//!
//! Combined with the existing `trie_node_cache_hits` and
//! `trie_node_cache_misses` PathDB counters this gives the full
//! three-tier breakdown of where each `resolve_and_track` call landed:
//!
//!   diff_hit  = TRIE_DIFFLAYER_HITS
//!   moka_hit  = trie_node_cache_hits         (moka served it after diff miss)
//!   disk_read = trie_node_cache_misses       (moka miss, RocksDB consulted)
//!
//! All three rates are derivable in PromQL via `rate(..[1m])`.
//!
//! The counter is on its own cache line to avoid false sharing with any
//! nearby static. Increments use `Relaxed` ordering — pure accounting, no
//! sync semantics.

use std::sync::atomic::{AtomicU64, Ordering};

/// `AtomicU64` aligned to a 64-byte cache line. Padding eliminates
/// false-sharing contention when adjacent atomics are written from
/// many threads concurrently. Used here mainly for symmetry with the
/// pattern; with a single counter the padding is overkill but keeps
/// the door open for future siblings.
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
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// DiffLayer chain hits — bumped when `DiffLayers::get_trie_nodes`
/// returns `Some` (i.e. one of the in-memory diff layers served the
/// lookup, the query never reached PathDB / moka).
pub static TRIE_DIFFLAYER_HITS: PaddedAtomicU64 = PaddedAtomicU64::new(0);
