//! `Storage::update_status_cas` — a status write guarded on the caller's
//! resourceVersion (#2151).
//!
//! Upstream: `UpdateStatus` on the object the caller read goes through the
//! generic registry `Store.Update` -> `GuaranteedUpdate`, whose
//! `Preconditions`/`OptimisticPut` compares the key's revision to the caller's
//! resourceVersion and fails with a Conflict
//! (`staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go` `GuaranteedUpdate`;
//! `pkg/controller/disruption/disruption.go` `writePdbStatus`).

use super::fixtures::{pod, pod_key};
use rusternetes_storage::Storage;
use serde_json::Value;

/// Row 1: the caller's resourceVersion is current -> the status lands.
/// Row 2: it is stale -> `Conflict`, and nothing is written.
pub async fn run_test_update_status_cas<S: Storage>(storage: &S) {
    let key = pod_key("test-ns", "status-cas");
    storage
        .create::<Value>(&key, &pod("test-ns", "status-cas"))
        .await
        .expect("create failed");
    let base: Value = storage.get(&key).await.expect("get failed");

    // Row 1: matching resourceVersion succeeds.
    let mut first = base.clone();
    first["status"] = serde_json::json!({ "phase": "Running" });
    let written = storage
        .update_status_cas::<Value>(&key, &first)
        .await
        .expect("a status write from a freshly-read object must succeed");
    assert_eq!(written["status"]["phase"], "Running");
    let stored: Value = storage.get(&key).await.expect("get failed");
    assert_eq!(stored["status"]["phase"], "Running");
    assert_ne!(
        stored["metadata"]["resourceVersion"], base["metadata"]["resourceVersion"],
        "a successful status write must advance the resourceVersion"
    );

    // Row 2: the same (now stale) resourceVersion is rejected and writes nothing.
    let mut second = base.clone();
    second["status"] = serde_json::json!({ "phase": "Failed" });
    let err = storage
        .update_status_cas::<Value>(&key, &second)
        .await
        .expect_err("a status write from a superseded resourceVersion must be rejected");
    assert!(
        matches!(err, rusternetes_common::Error::Conflict(_)),
        "expected Conflict for a superseded resourceVersion, got {err:?}"
    );
    let after: Value = storage.get(&key).await.expect("get failed");
    assert_eq!(after, stored, "a rejected status write must not write");
}

/// The overlapping row: N status writers that all carry the same starting
/// resourceVersion — exactly one wins, the rest get `Conflict`. A
/// check-then-write implementation lets several pass the check; this is the
/// property #2151 exists to close (PDB status vs Pod `/eviction`).
pub async fn run_test_update_status_cas_concurrent<S>(storage: std::sync::Arc<S>)
where
    S: Storage + Send + Sync + 'static,
{
    const ROUNDS: usize = 40;
    const WRITERS: usize = 4;

    for round in 0..ROUNDS {
        let name = format!("status-race-{round}");
        let key = pod_key("test-ns", &name);
        storage
            .create::<Value>(&key, &pod("test-ns", &name))
            .await
            .expect("create failed");
        let base: Value = storage.get(&key).await.expect("get failed");

        let mut handles = Vec::with_capacity(WRITERS);
        for writer in 0..WRITERS {
            let storage = std::sync::Arc::clone(&storage);
            let key = key.clone();
            let mut object = base.clone();
            object["status"] = serde_json::json!({ "message": writer.to_string() });
            handles.push(tokio::spawn(async move {
                storage.update_status_cas::<Value>(&key, &object).await
            }));
        }

        let mut winners = Vec::new();
        for (writer, handle) in handles.into_iter().enumerate() {
            match handle.await.expect("writer task panicked") {
                Ok(_) => winners.push(writer),
                Err(e) => assert!(
                    matches!(e, rusternetes_common::Error::Conflict(_)),
                    "round {round}: writer {writer} must lose with Conflict, got {e:?}"
                ),
            }
        }
        assert_eq!(
            winners.len(),
            1,
            "round {round}: {} of {WRITERS} status writes from the same \
             resourceVersion succeeded, expected exactly 1 (winners: {winners:?})",
            winners.len()
        );
        let stored: Value = storage.get(&key).await.expect("get after race failed");
        assert_eq!(
            stored["status"]["message"],
            winners[0].to_string(),
            "round {round}: the stored status is not the winning writer's"
        );
    }
}
