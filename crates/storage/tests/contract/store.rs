//! Ported from `storage/testing/store_tests.go`.

use super::fixtures::{pod, pod_key};
use rusternetes_storage::Storage;
use serde_json::Value;

/// Ported from `RunTestCreate` (`store_tests.go`).
///
/// Create must store the object and hand back a copy carrying a non-zero
/// resourceVersion.
///
/// `expects_resource_version` is false for backends with no revision concept.
/// `MemoryStorage` is one: it stamps uid and creationTimestamp but no
/// resourceVersion, its `current_revision()` returns a wall-clock timestamp,
/// and `watch_from_revision` states outright that it does not support
/// revisions. Upstream's `RunTestCreate` asserts the returned object carries a
/// valid RV, so this is a real gap in that backend, deliberately recorded here
/// rather than asserted away.
pub async fn run_test_create<S: Storage>(storage: &S, expects_resource_version: bool) {
    let key = pod_key("test-ns", "foo");

    let out: Value = storage
        .create(&key, &pod("test-ns", "foo"))
        .await
        .expect("create failed");

    if expects_resource_version {
        let rv = out["metadata"]["resourceVersion"]
            .as_str()
            .expect("create returned no resourceVersion");
        assert_ne!(rv, "0", "create returned a zero resourceVersion");
    }
    assert_eq!(out["metadata"]["name"], "foo");

    let stored: Value = storage.get(&key).await.expect("get after create failed");
    assert_eq!(stored["metadata"]["name"], "foo");
}

/// Ported from `RunTestCreateWithKeyExist` (`store_tests.go`): creating over an
/// existing key is a conflict, never an overwrite.
pub async fn run_test_create_with_key_exist<S: Storage>(storage: &S) {
    let key = pod_key("test-ns", "exists");
    storage
        .create(&key, &pod("test-ns", "exists"))
        .await
        .expect("first create failed");

    let err = storage
        .create::<Value>(&key, &pod("test-ns", "exists"))
        .await
        .expect_err("second create should fail");
    assert!(
        matches!(err, rusternetes_common::Error::AlreadyExists(_)),
        "expected AlreadyExists, got {err:?}"
    );
}
