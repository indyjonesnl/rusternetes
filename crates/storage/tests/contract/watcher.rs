//! Ported from `storage/testing/watcher_tests.go`.

use super::fixtures::{pod, pod_key};
use futures::StreamExt;
use rusternetes_storage::{Storage, WatchEvent};
use std::time::Duration;

/// Ported from `RunTestDeleteTriggerWatch` (`watcher_tests.go`): seed a key,
/// watch from just after the seeding write, delete, and expect a Deleted event.
///
/// Upstream watches from the seeded object's resourceVersion, and the etcd3
/// watcher starts the stream at `WithRev(initialRev + 1)`
/// (`etcd3/watcher.go:380`), so the seeding write is not replayed. A
/// revision-less watch only behaves that way on etcd, which starts at "now";
/// kine replays the create and the Deleted event never arrives first.
///
/// `revisions` is false for backends with no revision concept
/// (`MemoryStorage`), whose `watch_from_revision` ignores the revision anyway
/// (`memory.rs:231`).
pub async fn run_test_delete_trigger_watch<S: Storage>(storage: &S, revisions: bool) {
    let prefix = "/registry/pods/watch-del/";
    let key = pod_key("watch-del", "foo");

    let created: serde_json::Value = storage
        .create(&key, &pod("watch-del", "foo"))
        .await
        .expect("seed failed");

    let mut stream = if revisions {
        let rv: i64 = created["metadata"]["resourceVersion"]
            .as_str()
            .expect("seed returned no resourceVersion")
            .parse()
            .expect("resourceVersion is not an integer");
        storage
            .watch_from_revision(prefix, rv + 1)
            .await
            .expect("watch_from_revision failed")
    } else {
        storage.watch(prefix).await.expect("watch failed")
    };

    // Let the watch establish server-side before mutating.
    tokio::time::sleep(Duration::from_millis(500)).await;

    storage.delete(&key).await.expect("delete failed");

    let event = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("timed out waiting for the delete event")
        .expect("watch stream ended")
        .expect("watch stream error");

    assert!(
        matches!(event, WatchEvent::Deleted(..)),
        "expected a Deleted event, got {event:?}"
    );
}

/// Replay from a resourceVersion: a write that lands between a client's LIST
/// and its WATCH must still be delivered.
///
/// Ported from `RunTestWatchFromZero`/`RunTestWatchFromNonZero`
/// (`storage/testing/watcher_tests.go`): watch from the seeded object's
/// resourceVersion + 1 and the later write arrives, with the seeding write not
/// replayed; watch from the seeded resourceVersion itself and the seeding write
/// is replayed first. etcd3 starts the stream at `WithRev(rv + 1)`
/// (`etcd3/watcher.go:380`) and etcd replays every event with
/// `mod_revision >= start_revision`.
pub async fn run_test_watch_replays_from_revision<S: Storage>(storage: &S) {
    let prefix = "/registry/pods/watch-replay/";
    let key = pod_key("watch-replay", "foo");

    let created: serde_json::Value = storage
        .create(&key, &pod("watch-replay", "foo"))
        .await
        .expect("seed failed");
    let rv1: i64 = created["metadata"]["resourceVersion"]
        .as_str()
        .expect("seed returned no resourceVersion")
        .parse()
        .expect("resourceVersion is not an integer");

    // The write that lands in the LIST -> WATCH window.
    let mut modified = created.clone();
    modified["metadata"]["labels"] = serde_json::json!({"touched": "yes"});
    let updated: serde_json::Value = storage
        .update(&key, &modified)
        .await
        .expect("update failed");
    let rv2: i64 = updated["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(rv2 > rv1);

    // Watch after the seed: the update is replayed, the create is not.
    let mut stream = storage
        .watch_from_revision(prefix, rv1 + 1)
        .await
        .expect("watch_from_revision failed");
    let event = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("timed out: the write between LIST and WATCH was lost")
        .expect("watch stream ended")
        .expect("watch stream error");
    assert!(
        matches!(event, WatchEvent::Modified(..)),
        "expected the replayed Modified event, got {event:?}"
    );

    // Watch from the seed's own revision: the create is replayed first.
    let mut stream = storage
        .watch_from_revision(prefix, rv1)
        .await
        .expect("watch_from_revision failed");
    let event = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("timed out waiting for the replayed create")
        .expect("watch stream ended")
        .expect("watch stream error");
    assert!(
        matches!(event, WatchEvent::Added(..)),
        "expected the replayed Added event, got {event:?}"
    );
}
