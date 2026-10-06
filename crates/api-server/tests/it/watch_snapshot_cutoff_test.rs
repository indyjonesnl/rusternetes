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
    skip_added_covered_by_snapshot, snapshot_resource_version,
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
    let stream = skip_added_covered_by_snapshot(
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
    let stream = skip_added_covered_by_snapshot(live(vec![added("b", 1)]), None);
    assert_eq!(names(stream).await, vec!["ADDED /registry/pods/ns/b"]);
}
