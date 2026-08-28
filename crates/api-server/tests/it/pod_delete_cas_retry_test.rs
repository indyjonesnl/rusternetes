//! Regression test: DELETE of a pod must not fail with 409 because of the
//! api-server's OWN read-modify-write race.
//!
//! `[sig-node] Kubelet when scheduling a busybox command that always fails in a
//! pod should be possible to delete [NodeConformance] [Conformance]`
//! (test/e2e/common/node/kubelet.go:135) issues a plain
//! `podClient.Delete(name, metav1.DeleteOptions{})` — no preconditions, no
//! resourceVersion — and fails the spec on ANY error. Against rusternetes it
//! got (run 33078268480, 2026-08-27):
//!
//! ```text
//! Unexpected error: deleting Pod:
//!     resourceVersion mismatch: resource was modified (expected: 3560, current: 3561)
//!     code: 409  reason: Conflict
//! ```
//!
//! A pod that has just been scheduled is written by several actors at once
//! (scheduler binding, kubelet status), so the object's resourceVersion moves
//! between the delete handler's read and its write. Graceful deletion is a
//! read-modify-write, and the handler retried it exactly once — one more
//! concurrent write during the retry and the CAS failure reached the client.
//!
//! Upstream never surfaces that: `Store.Delete` runs the graceful-deletion
//! update through `GuaranteedUpdate`, whose `retryOnConflict` loop re-reads and
//! re-applies until it wins or the caller's precondition genuinely fails
//! (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go,
//! `updateForGracefulDeletionAndFinalizers`; registry/store.go's
//! `GuaranteedUpdate` contract in storage/interfaces.go). A conflict against a
//! resourceVersion the CLIENT never supplied is the server's own race to
//! resolve, not a client error.
//!
//! `MemoryStorage::inject_conflicts(n)` makes the next `n` `update()` calls
//! return `Error::Conflict`, standing in for that concurrent writer.

use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": "default" },
        "spec": {
            // 30s (upstream default) keeps the handler on the graceful path:
            // read, stamp deletionTimestamp, write. Grace 0 deletes outright
            // and never touches the CAS window this test is about.
            "terminationGracePeriodSeconds": 30,
            "containers": [ { "name": "c", "image": "busybox" } ]
        }
    })
}

/// One concurrent write during the delete: the handler must absorb it.
#[tokio::test]
async fn pod_delete_retries_a_single_cas_conflict() {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    let key = build_key("pods", Some("default"), "bin-false-single");
    mem.create(&key, &pod("bin-false-single")).await.unwrap();

    mem.inject_conflicts(1);
    let (status, body) = api
        .delete("/api/v1/namespaces/default/pods/bin-false-single")
        .await;

    assert_eq!(
        status.as_u16(),
        200,
        "DELETE must absorb one CAS conflict, got {}: {}",
        status,
        body
    );
}

/// The spec's actual shape: a pod that several writers are touching. One retry
/// is not enough — the handler must keep re-reading until it wins, the way
/// upstream's GuaranteedUpdate does.
#[tokio::test]
async fn pod_delete_retries_repeated_cas_conflicts() {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    let key = build_key("pods", Some("default"), "bin-false-busy");
    mem.create(&key, &pod("bin-false-busy")).await.unwrap();

    // Three writers land between our read and our write.
    mem.inject_conflicts(3);
    let (status, body) = api
        .delete("/api/v1/namespaces/default/pods/bin-false-busy")
        .await;

    assert_eq!(
        status.as_u16(),
        200,
        "DELETE must not surface a CAS conflict the client never asked for \
         (got {}: {}) — this is the [sig-node] 'should be possible to delete' failure",
        status,
        body
    );

    // And the deletion actually took effect: the object carries a
    // deletionTimestamp for the kubelet to act on.
    let stored: Value = mem
        .get(&key)
        .await
        .expect("pod must still exist (graceful)");
    assert!(
        stored
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some(),
        "graceful delete must stamp deletionTimestamp, got {stored}"
    );
}
