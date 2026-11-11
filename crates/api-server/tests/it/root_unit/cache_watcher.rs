//! Ports of `cacher/watch_cache_test.go::TestSuggestedWatchChannelSize`,
//! `time_budget_test.go::TestTimeBudget` and the `cacheWatcher.add` /
//! `Cacher.dispatchEvent` semantics of `cache_watcher_test.go` / `cacher.go`.
use k3rs_api_server::cache_watcher::*;
use k3rs_api_server::watch_cache::{
    CachedWatchEvent, WatchEventData, WatchScope, EVENT_FRESH_DURATION, LOWER_BOUND_CAPACITY,
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

/// `startDispatching` (`cacher.go:1039`): a watcher scoped to a name only sees
/// that name's events; unscoped watchers see everything.
#[tokio::test]
async fn name_scoped_watcher_is_offered_only_its_name() {
    let d = Dispatcher::new();
    let mut all = d.add_watcher(LOWER_BOUND_CAPACITY);
    let scope = WatchScope::new("", "pods", Some("metadata.name=p2"));
    assert_eq!(scope.name.as_deref(), Some("p2"));
    let mut only = d.add_scoped(LOWER_BOUND_CAPACITY, &scope);
    for rev in 1..=3 {
        d.dispatch("/registry/pods/", &ev(rev)).await;
    }
    let got: Vec<_> = vec![
        all.recv().await.unwrap().revision,
        all.recv().await.unwrap().revision,
        all.recv().await.unwrap().revision,
    ];
    assert_eq!(got, [1, 2, 3]);
    assert_eq!(only.recv().await.unwrap().revision, 2);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), only.recv())
            .await
            .is_err(),
        "no other name is offered"
    );
    assert_eq!(d.receiver_count(), 2);
}

#[test]
fn watch_scope_name_comes_from_an_exact_metadata_name_selector() {
    let n = |sel: Option<&str>| WatchScope::new("", "pods", sel).name;
    assert_eq!(n(Some("metadata.name=a")).as_deref(), Some("a"));
    assert_eq!(
        n(Some("metadata.name==a,status.phase=Running")).as_deref(),
        Some("a")
    );
    assert_eq!(n(Some("metadata.name!=a")), None);
    assert_eq!(n(Some("status.phase=Running")), None);
    assert_eq!(n(Some("")), None);
    assert_eq!(n(None), None);
}

/// `cacheWatcher.add` closeFunc: `TerminatedWatchersCounter{group,resource}`.
#[tokio::test]
async fn terminating_a_watcher_counts_it_under_group_and_resource() {
    use k3rs_api_server::cache_metrics::terminated_watchers;
    let d = Dispatcher::new();
    let scope = WatchScope::new("apps", "deployments-terminated-test", None);
    let rx = d.add_scoped(LOWER_BOUND_CAPACITY, &scope);
    let before = terminated_watchers("apps", "deployments-terminated-test");
    for rev in 1..=(rx.chan_size() as i64 + 3) {
        d.dispatch("/registry/deployments/ns/", &ev(rev)).await;
    }
    assert!(rx.is_terminated());
    assert_eq!(
        terminated_watchers("apps", "deployments-terminated-test"),
        before + 1
    );
}

fn pod_ev(kind: &str, name: &str, node: &str, rev: i64) -> CachedWatchEvent {
    let key = format!("/registry/pods/ns/{name}");
    let v = Arc::new(format!(
        r#"{{"metadata":{{"name":"{name}","resourceVersion":"{rev}"}},"spec":{{"nodeName":"{node}"}}}}"#
    ));
    CachedWatchEvent {
        event: match kind {
            "add" => WatchEventData::Added(key, v),
            "mod" => WatchEventData::Modified(key, v),
            _ => WatchEventData::Deleted(key, v),
        },
        revision: rev,
        recorded: Instant::now(),
    }
}

async fn drain(w: &mut WatchReceiver) -> Vec<i64> {
    let mut out = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(20), w.recv()).await {
        out.push(e.revision);
    }
    out
}

#[test]
fn watch_scope_trigger_value_comes_from_the_pods_node_name_selector() {
    let t = |g: &str, r: &str, sel: &str| WatchScope::new(g, r, Some(sel)).trigger_value;
    assert_eq!(t("", "pods", "spec.nodeName=n1").as_deref(), Some("n1"));
    assert_eq!(t("", "pods", "spec.nodeName=").as_deref(), Some(""));
    assert_eq!(t("", "pods", "spec.nodeName!=n1"), None);
    assert_eq!(t("", "pods", "status.phase=Running"), None);
    // Only resources with a TriggerFunc have an index.
    assert_eq!(t("", "secrets", "spec.nodeName=n1"), None);
}

/// `startDispatching` + `triggerValuesThreadUnsafe`: a pod bound to a node is
/// offered to the watcher of its OLD value ("" = unscheduled) as well, so that
/// watcher can turn it into a DELETED; unrelated nodes are not offered it.
#[tokio::test]
async fn trigger_watchers_get_events_for_the_current_and_previous_value() {
    let d = Dispatcher::new();
    let scope = |n: &str| WatchScope::new("", "pods", Some(&format!("spec.nodeName={n}")));
    let mut unscheduled = d.add_scoped(LOWER_BOUND_CAPACITY, &scope(""));
    let mut a = d.add_scoped(LOWER_BOUND_CAPACITY, &scope("a"));
    let mut b = d.add_scoped(LOWER_BOUND_CAPACITY, &scope("b"));
    let mut all = d.add_scoped(LOWER_BOUND_CAPACITY, &WatchScope::new("", "pods", None));
    let p = "/registry/pods/";
    d.dispatch(p, &pod_ev("add", "p1", "", 1)).await; // created unscheduled
    d.dispatch(p, &pod_ev("mod", "p1", "a", 2)).await; // bound to a
    d.dispatch(p, &pod_ev("mod", "p1", "a", 3)).await; // status update on a
    d.dispatch(p, &pod_ev("del", "p1", "a", 4)).await; // deleted
    assert_eq!(drain(&mut unscheduled).await, [1, 2]);
    assert_eq!(drain(&mut a).await, [2, 3, 4]);
    assert_eq!(drain(&mut b).await, Vec::<i64>::new());
    assert_eq!(
        drain(&mut all).await,
        [1, 2, 3, 4],
        "no trigger pinned: every event"
    );
}

/// A key not seen before has an unknown previous value: offered to every
/// trigger watcher (upstream's paranoid branch), then learned.
#[tokio::test]
async fn unknown_previous_value_is_offered_to_every_trigger_watcher() {
    let d = Dispatcher::new();
    let scope = |n: &str| WatchScope::new("", "pods", Some(&format!("spec.nodeName={n}")));
    let mut a = d.add_scoped(LOWER_BOUND_CAPACITY, &scope("a"));
    let mut b = d.add_scoped(LOWER_BOUND_CAPACITY, &scope("b"));
    let p = "/registry/pods/";
    d.dispatch(p, &pod_ev("mod", "p1", "a", 1)).await; // never seen
    d.dispatch(p, &pod_ev("mod", "p1", "a", 2)).await; // now known
    assert_eq!(drain(&mut a).await, [1, 2]);
    assert_eq!(drain(&mut b).await, [1]);
}

/// `suggestedWatchChannelSize` with `indexExists`: a pinned trigger caps the
/// queue at 10, an unpinned watch of an indexed resource may use up to 1000.
#[tokio::test]
async fn trigger_watchers_use_the_small_queue() {
    let d = Dispatcher::new();
    let pinned = d.add_scoped(
        100 * 1024,
        &WatchScope::new("", "pods", Some("spec.nodeName=n")),
    );
    let unpinned = d.add_scoped(100 * 1024, &WatchScope::new("", "pods", None));
    assert_eq!(pinned.chan_size(), 10);
    assert_eq!(unpinned.chan_size(), 1000);
    let other = Dispatcher::new();
    assert_eq!(
        other
            .add_scoped(100 * 1024, &WatchScope::new("", "secrets", None))
            .chan_size(),
        100,
        "no TriggerFunc for secrets: indexExists is false"
    );
}
