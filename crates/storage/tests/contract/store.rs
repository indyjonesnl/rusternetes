//! Ported from `storage/testing/store_tests.go`.

use super::fixtures::{pod, pod_key};
use rusternetes_storage::Storage;
use serde_json::Value;

/// Ported from `RunTestCreate` (`store_tests.go`).
///
/// Create must store the object and hand back a copy carrying a non-zero
/// resourceVersion.
pub async fn run_test_create<S: Storage>(storage: &S) {
    let key = pod_key("test-ns", "foo");

    let out: Value = storage
        .create(&key, &pod("test-ns", "foo"))
        .await
        .expect("create failed");

    let rv = out["metadata"]["resourceVersion"]
        .as_str()
        .expect("create returned no resourceVersion");
    assert_ne!(rv, "0", "create returned a zero resourceVersion");
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
