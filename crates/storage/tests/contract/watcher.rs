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
