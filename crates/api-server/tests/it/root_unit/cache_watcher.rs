//! Ports of `cacher/watch_cache_test.go::TestSuggestedWatchChannelSize`,
//! `time_budget_test.go::TestTimeBudget` and the `cacheWatcher.add` /
//! `Cacher.dispatchEvent` semantics of `cache_watcher_test.go` / `cacher.go`.
use rusternetes_api_server::cache_watcher::*;
use rusternetes_api_server::watch_cache::{
    CachedWatchEvent, WatchEventData, EVENT_FRESH_DURATION, LOWER_BOUND_CAPACITY,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn ev(rev: i64) -> CachedWatchEvent {
    CachedWatchEvent {
        event: WatchEventData::Added(
            format!("/registry/pods/ns/p{rev}"),
            Arc::new(format!(
                r#"{{"metadata":{{"name":"p{rev}","resourceVersion":"{rev}"}}}}"#
            )),
        ),
        revision: rev,
        recorded: Instant::now(),
    }
}

#[test]
fn suggested_watch_channel_size_table() {
    // (capacity, index_exists, trigger_used, fresh multiplier, expected)
    let d = EVENT_FRESH_DURATION;
    let cases: &[(usize, bool, bool, u32, usize)] = &[
        (100, true, true, 1, 10),
        (100, true, false, 1, 10),
        (100, false, false, 1, 10),
        (750, true, true, 1, 10),
        (750, true, false, 1, 10),
        (750, false, false, 1, 10),
        (7500, true, true, 1, 10),
        (7500, true, false, 1, 100),
        (7500, true, false, 2, 50),
        (7500, false, false, 1, 100),
        (7500, false, false, 2, 50),
        (75000, true, true, 1, 10),
        (75000, true, false, 1, 1000),
        (75000, true, false, 2, 500),
        (75000, false, false, 1, 100),
        (750000, true, true, 1, 10),
        (750000, true, false, 1, 1000),
        (750000, false, false, 1, 100),
    ];
    for &(cap, idx, trig, mult, want) in cases {
        assert_eq!(
            suggested_watch_channel_size(cap, d * mult, idx, trig),
            want,
            "capacity={cap} index={idx} trigger={trig} fresh x{mult}"
        );
    }
}

#[test]
fn time_budget() {
    let t0 = Instant::now();
    let max = Duration::from_millis(200);
    let refresh = Duration::from_millis(50);
    let mut b = TimeBudget::with_limits(t0, max, refresh);
    assert_eq!(b.take_available(t0), Duration::ZERO);
    // Waiting longer than the max budget yields the max.
    let t1 = t0 + Duration::from_secs(10);
    assert_eq!(b.take_available(t1), max);
    // Two seconds accumulate 2*refresh.
    let t2 = t1 + Duration::from_secs(2);
    assert_eq!(b.take_available(t2), 2 * refresh);
    // Return one refresh: only that is available (no time advanced).
    b.return_unused(refresh);
    assert_eq!(b.take_available(t2), refresh);
    // Going back in time with an empty budget yields nothing.
    assert_eq!(
        b.take_available(t2 - Duration::from_secs(120)),
        Duration::ZERO
    );
}

#[tokio::test]
async fn dispatch_reaches_every_watcher() {
    let d = Dispatcher::new();
    let mut a = d.add_watcher(LOWER_BOUND_CAPACITY);
    let mut b = d.add_watcher(LOWER_BOUND_CAPACITY);
    d.dispatch("p", &ev(1)).await;
    assert_eq!(a.recv().await.unwrap().revision, 1);
    assert_eq!(b.recv().await.unwrap().revision, 1);
    assert_eq!(d.receiver_count(), 2);
}

/// A full watcher is terminated; the others (even earlier in the list) are not
/// held up and keep receiving (`dispatchEvent`'s non-blocking first pass).
#[tokio::test]
async fn blocked_watcher_is_terminated_others_continue() {
    let d = Dispatcher::new();
    let mut fast = d.add_watcher(LOWER_BOUND_CAPACITY);
    let mut slow = d.add_watcher(LOWER_BOUND_CAPACITY);
    let n = slow.chan_size() as i64;
    let mut got = Vec::new();
    for rev in 1..=(n + 5) {
        d.dispatch("p", &ev(rev)).await;
        got.push(fast.recv().await.unwrap().revision);
    }
    assert_eq!(got, (1..=n + 5).collect::<Vec<_>>());
    assert!(slow.is_terminated(), "the unresponsive watcher is closed");
    assert!(slow.recv().await.is_none(), "queued events are discarded");
    assert_eq!(d.receiver_count(), 1);
    assert!(!fast.is_terminated());
}

/// `add` waits for a blocked watcher while the timer runs: a consumer that
/// catches up inside the budget is not terminated.
#[tokio::test]
async fn blocked_watcher_that_unblocks_in_time_survives() {
    let d = Arc::new(Dispatcher::new());
    let mut w = d.add_watcher(LOWER_BOUND_CAPACITY);
    let n = w.chan_size() as i64;
    // Let the budget refill to its 100ms maximum (50ms per second elapsed).
    tokio::time::sleep(Duration::from_millis(10)).await;
    for rev in 1..=n {
        d.dispatch("p", &ev(rev)).await;
    }
    // Queue is full. Consume concurrently shortly after the next dispatch
    // starts blocking.
    let consumer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut seen = 0;
        while let Some(e) = w.recv().await {
            seen += 1;
            if e.revision == n + 1 {
                break;
            }
        }
        (seen, w.is_terminated())
    });
    // The budget accumulated so far (~tens of microseconds) may be too small
    // to wait 5ms: give the dispatcher more by spacing the dispatch out.
    tokio::time::sleep(Duration::from_secs(2)).await;
    d.dispatch("p", &ev(n + 1)).await;
    let (seen, terminated) = consumer.await.unwrap();
    assert_eq!(seen, n + 1);
    assert!(!terminated);
}

#[tokio::test]
async fn dropped_consumer_is_forgotten() {
    let d = Dispatcher::new();
    drop(d.add_watcher(LOWER_BOUND_CAPACITY));
    assert_eq!(d.receiver_count(), 0);
    d.dispatch("p", &ev(1)).await;
    d.terminate_all();
}
