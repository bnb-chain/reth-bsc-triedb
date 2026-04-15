use std::collections::{HashMap, HashSet};

/// TrieTracer tracks inserted, deleted and accessed trie nodes by their path.
///
/// Semantics mirror geth's tracer in `bsc/trie/tracer.go`:
/// - on_read caches the RLP blob for a path (used to validate effective deletions)
/// - on_insert removes from `deletes` if present (resurrection), otherwise marks in `inserts`
/// - on_delete removes from `inserts` if present (untouched), otherwise marks in `deletes`
/// - deleted_nodes returns only paths that were actually present (exist in `access_list`)
///
/// This type is NOT thread-safe by itself; synchronize externally if needed.
#[derive(Debug, Default, Clone)]
pub struct TrieTracer {
    inserts: HashSet<Vec<u8>>,      // set of node paths inserted
    deletes: HashSet<Vec<u8>>,      // set of node paths deleted
    access_list: HashMap<Vec<u8>, Vec<u8>>, // path -> rlp-encoded blob as loaded from DB
}

impl TrieTracer {
    /// Creates a new empty tracer
    pub fn new() -> Self {
        Self::default()
    }

    /// Tracks a newly loaded trie node and caches its RLP-encoded blob.
    /// The provided `val` is stored as-is without additional cloning.
    pub fn on_read(&mut self, path: impl AsRef<[u8]>, val: Vec<u8>) {
        self.access_list.insert(path.as_ref().to_vec(), val);
    }

    /// Tracks a newly inserted trie node. If the path is currently in the
    /// deletion set (resurrected), remove it from `deletes` instead of adding
    /// to `inserts`.
    pub fn on_insert(&mut self, path: impl AsRef<[u8]>) {
        let key = path.as_ref();
        if self.deletes.remove(key) {
            return;
        }
        self.inserts.insert(key.to_vec());
    }

    /// Tracks a newly deleted trie node. If the path is currently in the
    /// insertion set, remove it from `inserts` instead of adding to `deletes`.
    pub fn on_delete(&mut self, path: impl AsRef<[u8]>) {
        let key = path.as_ref();
        if self.inserts.remove(key) {
            return;
        }
        self.deletes.insert(key.to_vec());
    }

    /// Clears all tracked data.
    pub fn reset(&mut self) {
        self.inserts.clear();
        self.deletes.clear();
        self.access_list.clear();
    }

    /// Returns the list of node paths deleted from the trie that were actually present
    /// (i.e., are known in `access_list`).
    pub fn deleted_nodes(&self) -> Vec<Vec<u8>> {
        let mut paths = Vec::new();
        for path in &self.deletes {
            if self.access_list.contains_key(path) {
                paths.push(path.clone());
            }
        }
        paths
    }

    /// Returns a deep-copied snapshot of the tracer.
    pub fn copy(&self) -> Self {
        self.clone()
    }

    /// Returns references to the internal tracking collections.
    pub fn inserts(&self) -> &HashSet<Vec<u8>> { &self.inserts }
    pub fn deletes(&self) -> &HashSet<Vec<u8>> { &self.deletes }
    pub fn access_list(&self) -> &HashMap<Vec<u8>, Vec<u8>> { &self.access_list }
}

