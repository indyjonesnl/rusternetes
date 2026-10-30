//! Unit tests moved from `src/watch_cache.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_storage::StorageBackend;
    pub use rusternetes_storage::{Storage, WatchEvent, WatchStream};
    pub use std::collections::{HashMap, VecDeque};
    pub use std::sync::Arc;
    pub use tokio::sync::{broadcast, RwLock};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use futures::StreamExt as _;
    use rusternetes_api_server::watch_cache::*;
    use rusternetes_storage::StorageBackend;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mock source with a non-zero head revision: the shared loop must start
    /// with `watch_since(head+1)` (never from-now) and the floor must gate
    /// old-RV subscriptions with Err(floor) → 410 → client relists.
    struct FloorSource {
        since_revs: std::sync::Mutex<Vec<i64>>,
    }

    // Backend history is compacted up to and including this revision.
    const COMPACTED_THROUGH: i64 = 30;

    #[async_trait]
    impl WatchSource for FloorSource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            panic!("with a non-zero head revision the loop must use watch_since, not from-now");
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.since_revs.lock().unwrap().push(revision);
            let s = futures::stream::iter(Vec::new()).chain(futures::stream::pending::<
                Result<WatchEvent, rusternetes_common::Error>,
            >());
            Ok(Box::pin(s))
        }

        async fn head_revision(&self) -> i64 {
            100
        }

        async fn is_revision_compacted(&self, revision: i64) -> bool {
            revision <= COMPACTED_THROUGH
        }
    }

    // The FIRST checked subscriber of a prefix defines the ring floor (its
    // list RV) and the backend replay starts from rv+1 — an on-demand prefix
    // (fresh namespace) must serve its very first watch instead of 410ing it
    // just because the GLOBAL head is newer. Later subscribers below the floor
    // are served by backfilling the ring from the backend when it still holds
    // that history (#2383), and get Err(floor) → 410 → relist only when it
    // does not.
    #[tokio::test]
    async fn subscribe_from_checked_gates_on_ring_floor() {
        let source = Arc::new(FloorSource {
            since_revs: std::sync::Mutex::new(Vec::new()),
        });
        let cache = WatchCache::from_source(source.clone());

        // First subscriber at RV 50 (global head is 100): must be SERVED —
        // the watcher is created with floor 50.
        assert!(
            cache
                .subscribe_from_checked("/registry/pods/", 50)
                .await
                .is_ok(),
            "first watch of an on-demand prefix must be served from its own RV"
        );

        // The shared loop connected via watch_since(first_rv + 1), filling the
        // ring from exactly where the first client needs it.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if source.since_revs.lock().unwrap().first() == Some(&51) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("shared loop must start from watch_since(first_rv+1)");

        // A LATER subscriber below the floor whose history the backend has
        // compacted must 410-relist.
        let err = cache
            .subscribe_from_checked("/registry/pods/", 20)
            .await
            .expect_err("RV below the ring floor must 410, not silently under-replay");
        assert_eq!(err, 50);

        // A later subscriber below the floor whose history is still retained
        // is served: the ring is backfilled and the floor lowered (#2383).
        assert!(cache
            .subscribe_from_checked("/registry/pods/", 40)
            .await
            .is_ok());
        let err = cache
            .subscribe_from_checked("/registry/pods/", 25)
            .await
            .expect_err("a compacted RV below the lowered floor must 410");
        assert_eq!(err, 40);

        // At/above the floor → served.
        assert!(cache
            .subscribe_from_checked("/registry/pods/", 50)
            .await
            .is_ok());
    }

    /// Mock backend with real replay semantics: `watch_since(rev)` yields every
    /// retained event with revision >= rev, then tails (stays open).
    struct ReplaySource {
        events: Vec<WatchEvent>,
        head: i64,
    }

    #[async_trait]
    impl WatchSource for ReplaySource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            panic!("non-zero head: loop must use watch_since");
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            let replay: Vec<_> = self
                .events
                .iter()
                .filter(|e| match e {
                    WatchEvent::Added(_, v)
                    | WatchEvent::Modified(_, v)
                    | WatchEvent::Deleted(_, v) => extract_rv(v) >= revision,
                })
                .cloned()
                .map(Ok)
                .collect();
            Ok(Box::pin(futures::stream::iter(replay).chain(
                futures::stream::pending::<Result<WatchEvent, rusternetes_common::Error>>(),
            )))
        }

        async fn head_revision(&self) -> i64 {
            self.head
        }
    }

    // #3071: the e2e framework's waitForServiceAccountInNamespace lists the
    // (empty) prefix at rv R, then watches from R. The controller's create
    // (rv R+2) lands after the list; the watch must replay it EXACTLY once —
    // for the first watcher of the prefix (which creates the shared watch) and
    // for a later watcher, whichever of ring/live broadcast it is served from.
    #[tokio::test]
    async fn watch_from_list_rv_replays_later_create_exactly_once() {
        let key = "/registry/serviceaccounts/ns-a/default".to_string();
        let value = r#"{"metadata":{"name":"default","resourceVersion":"3159"}}"#.to_string();
        let source = Arc::new(ReplaySource {
            events: vec![WatchEvent::Added(key.clone(), value)],
            head: 3159,
        });
        let cache = WatchCache::from_source(source);
        let prefix = "/registry/serviceaccounts/ns-a/";

        for round in 0..3 {
            let (history, rx) = cache
                .subscribe_from_checked(prefix, 3157)
                .await
                .expect("watch from the client's list rv must be served");
            let mut stream = broadcast_to_stream_with_history(history, rx, 3157);
            let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .unwrap_or_else(|_| panic!("round {round}: Added event was skipped"))
                .expect("stream ended")
                .expect("event error");
            assert!(
                matches!(&first, WatchEvent::Added(k, _) if *k == key),
                "round {round}: expected Added, got {first:?}"
            );
            let dup = tokio::time::timeout(Duration::from_millis(300), stream.next()).await;
            assert!(dup.is_err(), "round {round}: event delivered twice");
            // Later rounds hit the populated ring instead of the live path.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Regression (#1165 wedge): a broadcast subscriber that lags must be
    // TERMINATED with a Gone error (→ handler sends a 410 ERROR event, client
    // relists), never silently skipped past — a silently dropped MODIFIED
    // leaves long-lived informers (KCM deployment ReadyReplicas, endpointslice
    // readiness, quota usage) permanently stale.
    #[tokio::test]
    async fn lagged_subscriber_terminates_with_gone() {
        let (tx, rx) = broadcast::channel::<CachedWatchEvent>(2);
        // Overflow the 2-slot ring before the stream consumes anything.
        for rev in 1..=5 {
            let val = Arc::new(format!(
                "{{\"metadata\":{{\"name\":\"k{rev}\",\"resourceVersion\":\"{rev}\"}}}}"
            ));
            tx.send(CachedWatchEvent {
                revision: rev,
                recorded: std::time::Instant::now(),
                event: WatchEventData::Added(format!("/registry/secrets/ns/k{rev}"), val),
            })
            .unwrap();
        }

        let mut stream = broadcast_to_stream(rx);
        let mut yielded = Vec::new();
        let mut got_gone = false;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("stream must not hang after lag")
        {
            match item {
                Ok(ev) => yielded.push(ev),
                Err(rusternetes_common::Error::Gone(msg)) => {
                    assert!(
                        msg.contains("lagged"),
                        "Gone error should describe the lag, got: {msg}"
                    );
                    got_gone = true;
                }
                Err(other) => panic!("unexpected error type: {other}"),
            }
        }
        assert!(
            got_gone,
            "lagged subscriber MUST receive Error::Gone before the stream ends (got {} events, then clean end)",
            yielded.len()
        );
    }

    fn added(rev: i64) -> Result<WatchEvent, rusternetes_common::Error> {
        // The loop parses the RV out of the JSON via extract_rv, so the value
        // MUST carry a `resourceVersion` matching `rev`.
        let json =
            format!("{{\"metadata\":{{\"name\":\"k{rev}\",\"resourceVersion\":\"{rev}\"}}}}");
        Ok(WatchEvent::Added(
            format!("/registry/secrets/ns/k{rev}"),
            json,
        ))
    }

    /// A watch source that models what rhino/etcd does once the resume
    /// revision has been COMPACTED: the watch is cancelled server-side, so the
    /// stream ends immediately having delivered nothing. `watch_from_now` still
    /// works and tails live events.
    ///
    /// Retrying the compacted revision can never succeed — compaction only
    /// moves forward — so a loop that keeps resuming from it leaves the prefix
    /// permanently deaf (#1687).
    struct CompactedResumeSource {
        since_calls: AtomicUsize,
        from_now_calls: AtomicUsize,
    }

    #[async_trait]
    impl WatchSource for CompactedResumeSource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            self.from_now_calls.fetch_add(1, Ordering::SeqCst);
            // Re-based stream: a live event arrives and the stream stays open.
            let s = futures::stream::iter(vec![added(101)]).chain(futures::stream::pending::<
                Result<WatchEvent, rusternetes_common::Error>,
            >());
            Ok(Box::pin(s))
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            _revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.since_calls.fetch_add(1, Ordering::SeqCst);
            // Cancelled by the backend: ends at once, no events, no error.
            let s = futures::stream::iter(Vec::new());
            Ok(Box::pin(s))
        }

        async fn head_revision(&self) -> i64 {
            100
        }

        async fn is_revision_compacted(&self, _revision: i64) -> bool {
            true
        }
    }

    /// Regression for #1687: when the resume revision has been compacted, the
    /// shared prefix loop must re-base onto `watch_from_now` instead of
    /// retrying the dead revision forever. Idle prefixes (CSRs, namespaces,
    /// resourcequotas) fall below rhino's compaction floor and went
    /// permanently deaf — no live event reached any subscriber, which is what
    /// timed out the CSR API-operations conformance watch step.
    #[tokio::test]
    async fn compacted_resume_revision_rebases_instead_of_wedging() {
        let source = Arc::new(CompactedResumeSource {
            since_calls: AtomicUsize::new(0),
            from_now_calls: AtomicUsize::new(0),
        });
        let cache = WatchCache::from_source(source.clone());

        let mut rx = cache
            .subscribe("/registry/certificatesigningrequests/")
            .await;

        // The live event that only a re-based stream can deliver.
        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("compacted resume must re-base, not wedge retrying the dead revision")
            .expect("subscriber must receive the live event");
        assert_eq!(got.revision, 101);
        assert!(
            source.from_now_calls.load(Ordering::SeqCst) >= 1,
            "loop must reconnect via watch_from_now once the resume revision is compacted"
        );
    }

    /// A watch source that models a storage connection that drops after the
    /// first event, while a new object is committed during the gap.
    ///
    /// - `watch_from_now`: first call yields the event at rev 5 then the stream
    ///   ENDS (a dropped connection). Any later `watch_from_now` call yields
    ///   nothing and stays open — this is what the buggy reconnect hits, so it
    ///   never observes the rev-6 create.
    /// - `watch_since(rev)`: yields the gap event at rev 6 then stays open.
    ///   Only the fixed reconnect path calls this.
    struct DroppingSource {
        from_now_calls: AtomicUsize,
        since_revs: std::sync::Mutex<Vec<i64>>,
    }

    #[async_trait]
    impl WatchSource for DroppingSource {
        async fn watch_from_now(&self, _prefix: &str) -> rusternetes_common::Result<WatchStream> {
            let n = self.from_now_calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First connection: deliver rev 5, then the stream ends.
                let s = futures::stream::iter(vec![added(5)]);
                Ok(Box::pin(s))
            } else {
                // Reconnect via from-now (the BUG): the rev-6 create committed
                // during the gap is invisible here. Stay open so the loop
                // doesn't spin.
                let s = futures::stream::iter(Vec::new()).chain(futures::stream::pending::<
                    Result<WatchEvent, rusternetes_common::Error>,
                >());
                Ok(Box::pin(s))
            }
        }

        async fn head_revision(&self) -> i64 {
            0 // from-now first connect, like the memory backend
        }

        async fn watch_since(
            &self,
            _prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.since_revs.lock().unwrap().push(revision);
            // Replay the gap event committed while the stream was down, then
            // stay open.
            let s = futures::stream::iter(vec![added(6)]).chain(futures::stream::pending::<
                Result<WatchEvent, rusternetes_common::Error>,
            >());
            Ok(Box::pin(s))
        }
    }

    /// When the shared backend watch drops and reconnects, an event committed
    /// during the gap MUST still reach subscribers. Regression test for the
    /// cert-manager cainjector flake: the CA-secret create landed in a
    /// reconnect gap, the caBundle never got injected, and every admission
    /// webhook call failed with `UnknownIssuer`.
    #[tokio::test]
    async fn reconnect_replays_events_committed_during_the_gap() {
        let source = Arc::new(DroppingSource {
            from_now_calls: AtomicUsize::new(0),
            since_revs: std::sync::Mutex::new(Vec::new()),
        });
        let cache = WatchCache::from_source(source.clone());

        let mut rx = cache.subscribe("/registry/secrets/").await;

        // Collect revisions until we see rev 6 (the gap event) or time out.
        let mut seen = Vec::new();
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match rx.recv().await {
                    Ok(ev) => {
                        seen.push(ev.revision);
                        if ev.revision == 6 {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        })
        .await;

        assert!(
            result.is_ok(),
            "timed out waiting for the gap event (rev 6); got {seen:?}. \
             The reconnect must replay events committed while the stream was down."
        );
        assert!(
            seen.contains(&5),
            "the pre-drop event (rev 5) should arrive"
        );
        assert!(
            seen.contains(&6),
            "the event committed during the reconnect gap (rev 6) must be replayed"
        );
        assert_eq!(
            *source.since_revs.lock().unwrap(),
            vec![6],
            "reconnect must resume from last_delivered_rev + 1 (5 + 1 = 6)"
        );
    }

    fn make_event(rev: i64) -> CachedWatchEvent {
        CachedWatchEvent {
            event: WatchEventData::Added(format!("k{rev}"), Arc::new("{}".to_string())),
            revision: rev,
            recorded: std::time::Instant::now(),
        }
    }

    #[tokio::test]
    async fn a_write_hot_unwatched_prefix_keeps_replay_coverage() {
        // The kubelet's node informer LISTs nodes, then WATCHes from that
        // LIST's resourceVersion. Between the two, a write-hot prefix advances
        // (node status + leases push ~38 revisions/second on a live cluster).
        //
        // Shrinking such a prefix's ring to HISTORY_IDLE_CAPACITY raises the
        // replay floor above the client's rv, so subscribe_from_checked returns
        // Err -> 410 Expired. The client relists and retries -- but a failed
        // subscribe registers no receiver, so the prefix still looks idle, gets
        // shrunk again, and 410s again: a livelock. On the vanilla-swap
        // api-server leg that stopped the kubelet from EVER watching pods
        // (upstream gates its pod reflector on nodeHasSynced,
        // pkg/kubelet/config/apiserver.go), so no pod ever started (#1890).
        //
        // Memory reclamation must not destroy coverage a client can still
        // legitimately ask for: "no watchers" is not the same as "quiet".
        let storage = Arc::new(StorageBackend::new_memory());
        let cache = WatchCache::new(storage);

        // A shared watcher exists (as at boot) but nobody is subscribed, and
        // the prefix is being written continuously.
        let (tx, rx) = broadcast::channel(1000);
        drop(rx);
        {
            let mut watchers = cache.watchers.write().await;
            watchers.insert("/registry/nodes/".to_string(), tx);
        }
        {
            let mut hist = cache.history.write().await;
            hist.insert(
                "/registry/nodes/".to_string(),
                (0..200).map(make_event).collect(),
            );
            let mut floors = cache.floors.write().await;
            floors.insert("/registry/nodes/".to_string(), 0);
        }
        // Write-hot: the prefix received an event just now (node status and
        // leases push ~38 revisions/second on a live cluster).
        cache
            .last_write
            .write()
            .await
            .insert("/registry/nodes/".to_string(), std::time::Instant::now());

        // A write lands (the trim fires on the write path) and the periodic
        // sweep also runs -- both while the prefix has zero receivers.
        cache.gc_idle_history().await;

        // A client whose LIST saw revision 150 must still be servable: nothing
        // has been compacted, it is merely behind the most recent writes.
        let res = cache.subscribe_from_checked("/registry/nodes/", 150).await;
        assert!(
            res.is_ok(),
            "a watch from an uncompacted revision on a write-hot prefix must be \
             served, not 410'd: floor rose to {:?} (see #1890)",
            res.err()
        );
    }

    #[tokio::test]
    async fn idle_gc_shrinks_only_prefixes_without_receivers() {
        let storage = Arc::new(StorageBackend::new_memory());
        let cache = WatchCache::new(storage);

        // Prefix "a": a watcher with a LIVE receiver — must not be shrunk.
        let (tx_a, rx_a) = broadcast::channel(1000);
        // Prefix "b": a watcher whose receiver was dropped — count 0, eligible.
        let (tx_b, rx_b) = broadcast::channel(1000);
        drop(rx_b);
        {
            let mut watchers = cache.watchers.write().await;
            watchers.insert("a".to_string(), tx_a);
            watchers.insert("b".to_string(), tx_b);
        }
        {
            let mut hist = cache.history.write().await;
            hist.insert("a".to_string(), (0..100).map(make_event).collect());
            hist.insert("b".to_string(), (0..100).map(make_event).collect());
        }

        cache.gc_idle_history().await;

        let hist = cache.history.read().await;
        assert_eq!(
            hist["a"].len(),
            100,
            "a prefix with a live receiver must not be shrunk"
        );
        assert_eq!(
            hist["b"].len(),
            HISTORY_IDLE_CAPACITY,
            "an idle prefix is shrunk to the idle tail"
        );
        // The MOST RECENT events are retained, so recent replay (e.g. the CRD
        // Established MODIFIED) still survives the shrink.
        assert_eq!(hist["b"].back().unwrap().revision, 99);
        assert_eq!(
            hist["b"].front().unwrap().revision,
            100 - HISTORY_IDLE_CAPACITY as i64
        );

        drop(rx_a); // keep the receiver alive across the GC sweep above
    }

    // ---- dynamic capacity: ports of watch_cache_test.go TestDynamicCache /
    // TestCapacityUpperBound (release-1.35) ----

    fn ring_at(
        n: usize,
        interval: std::time::Duration,
    ) -> (VecDeque<CachedWatchEvent>, std::time::Instant) {
        let base = std::time::Instant::now();
        let buf = (0..n)
            .map(|i| {
                let mut e = make_event(i as i64 + 1);
                e.recorded = base + interval * i as u32;
                e
            })
            .collect();
        // The event about to be appended arrives one interval after the last.
        (buf, base + interval * n as u32)
    }

    #[test]
    fn capacity_upper_bound_matches_upstream_table() {
        let d = EVENT_FRESH_DURATION;
        let u = DEFAULT_UPPER_BOUND_CAPACITY;
        assert_eq!(capacity_upper_bound(d), u);
        assert_eq!(capacity_upper_bound(std::time::Duration::from_secs(45)), u);
        assert_eq!(capacity_upper_bound(d * 4), 4 * u);
        assert_eq!(capacity_upper_bound(d * 3), 4 * u);
        assert_eq!(capacity_upper_bound(d * (1 << 20)), u << 14);
    }

    #[test]
    fn dynamic_cache_resizes_like_upstream() {
        let d = EVENT_FRESH_DURATION;
        let over = d + std::time::Duration::from_secs(1);
        // (name, interval, lower, upper, expected capacity)
        let cases: [(&str, std::time::Duration, usize, usize, usize); 5] = [
            ("events inside fresh duration expand", d / 6, 2, 10, 10),
            ("events outside fresh duration, no change", d / 4, 2, 10, 5),
            ("recent quarter stale shrinks", over, 2, 10, 2),
            ("shrink honours lower bound", over, 3, 10, 3),
            ("expand honours upper bound", d / 6, 2, 8, 8),
        ];
        for (name, interval, lower, upper, want) in cases {
            let (buf, now) = ring_at(5, interval);
            assert_eq!(next_capacity(&buf, 5, now, d, lower, upper), want, "{name}");
        }
    }

    #[test]
    fn burst_keeps_whole_history_instead_of_evicting_at_a_fixed_ring() {
        // #3762: a fixed 500 ring 410s a watcher that is >500 events behind;
        // upstream grows the ring while the oldest event is still fresh.
        let mut buf = VecDeque::new();
        let mut cap = LOWER_BOUND_CAPACITY;
        let mut evicted = None;
        for rev in 1..=3000 {
            evicted = evicted.or(append_to_ring(&mut buf, &mut cap, make_event(rev)));
        }
        assert_eq!(evicted, None, "nothing older than 75s may be evicted");
        assert_eq!(buf.len(), 3000);
        assert!(cap >= 3000, "capacity grew to {cap}");
    }

    #[test]
    fn stale_ring_shrinks_and_reports_evicted_revision() {
        let d = EVENT_FRESH_DURATION;
        let over = d + std::time::Duration::from_secs(1);
        let (mut buf, now) = ring_at(LOWER_BOUND_CAPACITY * 2, over);
        let mut cap = LOWER_BOUND_CAPACITY * 2;
        let mut ev = make_event(10_000);
        ev.recorded = now;
        let evicted = append_to_ring(&mut buf, &mut cap, ev);
        assert_eq!(cap, LOWER_BOUND_CAPACITY);
        assert_eq!(buf.len(), LOWER_BOUND_CAPACITY);
        // 201 events held, 100 kept: the newest 99 old ones + the new one.
        assert_eq!(evicted, Some(101));
        assert_eq!(buf.back().unwrap().revision, 10_000);
    }

    #[test]
    fn not_full_ring_never_resizes() {
        let d = EVENT_FRESH_DURATION;
        let (buf, now) = ring_at(4, d / 6);
        assert_eq!(next_capacity(&buf, 5, now, d, 2, 10), 5);
    }
}
