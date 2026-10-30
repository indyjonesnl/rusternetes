//! Unit tests moved from `src/sync_locks.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::HashSet;
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::sync_locks::*;

    #[test]
    fn acquire_then_skip_then_reacquire_after_drop() {
        let locks = SyncLocks::new();

        // First acquire of a free key succeeds and marks it held.
        let g = locks.try_acquire("ns/pod").expect("free key must acquire");
        assert!(locks.is_held("ns/pod"));

        // A second acquire of the same key while held is refused (caller skips).
        assert!(
            locks.try_acquire("ns/pod").is_none(),
            "held key must not re-acquire"
        );

        // A different key is independent.
        let g2 = locks
            .try_acquire("ns/other")
            .expect("distinct key acquires");

        // Dropping the first guard frees only its key.
        drop(g);
        assert!(!locks.is_held("ns/pod"));
        assert!(locks.is_held("ns/other"));

        // Now the key can be acquired again.
        let _g3 = locks
            .try_acquire("ns/pod")
            .expect("key acquires again after release");

        drop(g2);
        assert!(!locks.is_held("ns/other"));
    }

    #[test]
    fn same_name_distinct_uids_share_one_key() {
        // The recreate pattern: name-keyed, so the old incarnation's in-flight
        // sync blocks the new one's sync (the #1112 invariant).
        let locks = SyncLocks::new();
        let _g = locks.try_acquire("default/web-0").unwrap();
        assert!(
            locks.try_acquire("default/web-0").is_none(),
            "same name/key must serialize regardless of pod uid"
        );
    }

    /// Concurrent same-name syncs (#1115): N threads race `try_acquire` on the
    /// same `namespace/name` key behind a barrier; exactly one wins, the rest
    /// skip, and the key is reusable afterwards.
    #[test]
    fn concurrent_same_key_exactly_one_winner() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};

        const N: usize = 16;
        let locks = Arc::new(SyncLocks::new());
        let start = Arc::new(Barrier::new(N));
        let done = Arc::new(Barrier::new(N));
        let wins = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..N)
            .map(|_| {
                let (locks, start, done, wins) =
                    (locks.clone(), start.clone(), done.clone(), wins.clone());
                std::thread::spawn(move || {
                    start.wait();
                    let guard = locks.try_acquire("default/web-0");
                    if guard.is_some() {
                        wins.fetch_add(1, Ordering::SeqCst);
                    }
                    // Hold any winning guard until every thread has tried, so
                    // the attempts genuinely overlap.
                    done.wait();
                    drop(guard);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(wins.load(Ordering::SeqCst), 1, "exactly one sync may run");
        assert!(locks.try_acquire("default/web-0").is_some());
    }
}
