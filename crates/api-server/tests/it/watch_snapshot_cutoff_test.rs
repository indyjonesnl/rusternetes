//! #2038: an object written between the watch's subscribe and its initial
//! LIST reaches the client twice (synthetic ADDED from the snapshot, real
//! ADDED from the live stream).
//!
//! Upstream cuts the live stream at the snapshot's resourceVersion:
//! `watchCacheInterval` builds the initial events at the cache's revision and
//! continues from exactly that revision
//! (`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache_interval.go:139-179`),
//! and `cacheWatcher.processInterval` drops `event.ResourceVersion <=
//! resourceVersion` (`cacher/cache_watcher.go`).
//!
//! The race is reproduced deterministically by handing the filter a live
//! stream that already contains the "interleaved" write.

use futures::StreamExt;
use rusternetes_api_server::handlers::watch::{
    skip_events_covered_by_snapshot, snapshot_resource_version,
};
use rusternetes_storage::{WatchEvent, WatchStream};
use serde_json::{json, Value};

fn obj(name: &str, rv: u64) -> Value {
    json!({"metadata": {"name": name, "resourceVersion": rv.to_string()}})
}

fn live(events: Vec<WatchEvent>) -> WatchStream {
    futures::stream::iter(events.into_iter().map(Ok)).boxed()
}

fn added(name: &str, rv: u64) -> WatchEvent {
    WatchEvent::Added(
        format!("/registry/pods/ns/{name}"),
        obj(name, rv).to_string(),
    )
}

fn modified(name: &str, rv: u64) -> WatchEvent {
    WatchEvent::Modified(
        format!("/registry/pods/ns/{name}"),
        obj(name, rv).to_string(),
    )
}

async fn names(stream: WatchStream) -> Vec<String> {
    stream
        .map(|e| match e.unwrap() {
            WatchEvent::Added(k, _) => format!("ADDED {k}"),
            WatchEvent::Modified(k, _) => format!("MODIFIED {k}"),
            WatchEvent::Deleted(k, _) => format!("DELETED {k}"),
        })
        .collect()
        .await
}

#[tokio::test]
async fn snapshot_cutoff_is_highest_listed_resource_version() {
    assert_eq!(snapshot_resource_version(&[]), None);
    assert_eq!(
        snapshot_resource_version(&[obj("a", 4), obj("b", 9), obj("c", 7)]),
        Some(9)
    );
}

#[tokio::test]
async fn live_added_already_in_the_snapshot_is_dropped() {
    // b was written after subscribe and before list: the snapshot has it
    // (rv 9) and so does the live stream.
    let cutoff = snapshot_resource_version(&[obj("a", 4), obj("b", 9)]);
    let stream = skip_events_covered_by_snapshot(
        live(vec![added("b", 9), added("c", 10), modified("b", 11)]),
        cutoff,
    );
    assert_eq!(
        names(stream).await,
        vec![
            "ADDED /registry/pods/ns/c".to_string(),
            "MODIFIED /registry/pods/ns/b".to_string()
        ]
    );
}

#[tokio::test]
async fn without_a_snapshot_nothing_is_dropped() {
    let stream = skip_events_covered_by_snapshot(live(vec![added("b", 1)]), None);
    assert_eq!(names(stream).await, vec!["ADDED /registry/pods/ns/b"]);
}

#[tokio::test]
async fn live_modified_and_deleted_already_in_the_snapshot_are_dropped() {
    // #2223: upstream forwards only events newer than the interval's
    // revision, whatever their type (cache_watcher.go:535-539).
    let stream = skip_events_covered_by_snapshot(
        live(vec![
            modified("a", 8),
            WatchEvent::Deleted("/registry/pods/ns/z".into(), obj("z", 9).to_string()),
            modified("a", 10),
            WatchEvent::Deleted("/registry/pods/ns/y".into(), obj("y", 11).to_string()),
        ]),
        Some(9),
    );
    assert_eq!(
        names(stream).await,
        vec![
            "MODIFIED /registry/pods/ns/a".to_string(),
            "DELETED /registry/pods/ns/y".to_string()
        ]
    );
}

/// End to end against a real store: events landing between the watch
/// subscription and the revision-stamped list are neither duplicated nor
/// reordered; the first event after the snapshot is delivered.
#[tokio::test]
async fn gap_events_between_subscribe_and_list_are_not_duplicated() {
    use rusternetes_api_server::handlers::watch::list_snapshot;
    use rusternetes_api_server::state::ApiServerState;
    use rusternetes_common::{
        auth::TokenManager, authz::AlwaysAllowAuthorizer, observability::MetricsRegistry,
    };
    use rusternetes_storage::{Storage, StorageBackend};
    use std::sync::Arc;

    let backend = Arc::new(StorageBackend::new_memory());
    let state = ApiServerState::new(
        backend.clone(),
        Arc::new(TokenManager::new(b"test-secret")),
        Arc::new(AlwaysAllowAuthorizer),
        Arc::new(MetricsRegistry::new()),
        true,
    );
    let key = |n: &str| format!("/registry/pods/ns/{n}");
    let pod = |n: &str| json!({"metadata": {"name": n, "namespace": "ns"}});

    backend.create(&key("a"), &pod("a")).await.unwrap();
    backend.create(&key("b"), &pod("b")).await.unwrap();
    // Subscribe first (as the handler does), then write in the gap.
    let live_stream = backend.watch("/registry/pods/ns/").await.unwrap();
    let mut a = backend.get::<Value>(&key("a")).await.unwrap();
    a["metadata"]["labels"] = json!({"x": "1"});
    backend.update(&key("a"), &a).await.unwrap(); // MODIFIED in the gap
    backend.delete(&key("b")).await.unwrap(); // DELETED in the gap
    backend.create(&key("c"), &pod("c")).await.unwrap(); // ADDED in the gap

    let (snapshot, cutoff) = list_snapshot(&state, "/registry/pods/ns/").await.unwrap();
    let mut listed: Vec<String> = snapshot
        .iter()
        .map(|v| v["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    listed.sort();
    assert_eq!(listed, vec!["a", "c"], "snapshot reflects the gap writes");

    // A write after the snapshot is the only thing the client may still see.
    backend.create(&key("d"), &pod("d")).await.unwrap();
    let filtered = skip_events_covered_by_snapshot(live_stream, cutoff);
    let got: Vec<String> = filtered
        .take(1)
        .map(|e| match e.unwrap() {
            WatchEvent::Added(k, _) => format!("ADDED {k}"),
            other => panic!("gap event leaked: {other:?}"),
        })
        .collect()
        .await;
    assert_eq!(got, vec!["ADDED /registry/pods/ns/d".to_string()]);
}
