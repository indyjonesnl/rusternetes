//! Per-pod sync serialization primitive.
//!
//! The kubelet runs at most one `sync_pod` at a time per pod, keyed by
//! `namespace/name` (NOT uid): a recreated pod (same name, new uid — the
//! StatefulSet replacement pattern) must not create/sweep containers while a
//! queued sync of the previous incarnation is still running, or a swept
//! container can be resurrected mid-start (issue #1112). Concurrent same-name
//! syncs would otherwise race the container runtime into "name already in use"
//! conflicts. Same-name pods cannot legitimately coexist in storage, so
//! name-keyed skip-and-retry loses no real concurrency.
//!
//! Extracted from the inline lock in `kubelet::sync_pod` so the load-bearing
//! skip-and-retry behaviour is unit-testable without a full runtime (#1115).

use std::collections::HashSet;
use std::sync::Mutex;

/// Set of pod sync keys currently being processed. A key is held for the
/// duration of one `sync_pod` call via the RAII [`SyncLockGuard`].
#[derive(Default)]
pub struct SyncLocks {
    held: Mutex<HashSet<String>>,
}

/// RAII guard releasing a held sync key on drop (on every return path).
pub struct SyncLockGuard<'a> {
    locks: &'a SyncLocks,
    key: String,
}

impl SyncLocks {
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to acquire the sync lock for `key`. Returns `Some(guard)` if the key
    /// was free — and marks it held until the guard drops — or `None` if another
    /// sync already holds it, in which case the caller skips and a later
    /// reconcile retries. Mirrors upstream's one-goroutine-per-pod worker; the
    /// skip-and-retry is rusternetes' equivalent serialization.
    pub fn try_acquire(&self, key: impl Into<String>) -> Option<SyncLockGuard<'_>> {
        let key = key.into();
        let mut held = self.held.lock().unwrap();
        if held.contains(&key) {
            return None;
        }
        held.insert(key.clone());
        Some(SyncLockGuard { locks: self, key })
    }

    /// Test-only: whether `key` is currently held.
    pub fn is_held(&self, key: &str) -> bool {
        self.held.lock().unwrap().contains(key)
    }
}

impl Drop for SyncLockGuard<'_> {
    fn drop(&mut self) {
        self.locks.held.lock().unwrap().remove(&self.key);
    }
}
