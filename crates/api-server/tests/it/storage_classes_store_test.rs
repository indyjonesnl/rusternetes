//! StorageClass and VolumeAttributesClass on the generic Store (#1990
//! step 6): upstream's `pkg/registry/storage/{storageclass,
//! volumeattributesclass}` strategies run inside `Store.Create` and
//! `Store.Update`, and both stores set `ReturnDeletedObject`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SCS: &str = "/apis/storage.k8s.io/v1/storageclasses";
const VACS: &str = "/apis/storage.k8s.io/v1/volumeattributesclasses";

fn sc(name: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
        "metadata": {"name": name}, "provisioner": "example.com/prov"
    })
}

fn vac(name: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttributesClass",
        "metadata": {"name": name}, "driverName": "csi.example.com",
        "parameters": {"iops": "100"}
    })
}

/// `SetDefaults_StorageClass` (pkg/apis/storage/v1/defaults.go:31-41).
#[tokio::test]
async fn a_storage_class_is_defaulted() {
    let api = TestApiServer::new();
    let (s, created) = api.post(SCS, &sc("a")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert_eq!(created["reclaimPolicy"], "Delete", "{created}");
    assert_eq!(created["volumeBindingMode"], "Immediate", "{created}");
}

/// `ReturnDeletedObject: true` (storageclass/storage/storage.go): a delete
/// answers with the StorageClass, not a Status.
#[tokio::test]
async fn deleting_a_storage_class_returns_it() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SCS, &sc("a")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api.delete(&format!("{SCS}/a")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "StorageClass", "{body}");
    assert_eq!(body["metadata"]["name"], "a", "{body}");
}

/// `ValidateStorageClassUpdate` (validation.go) runs on PATCH as well as PUT.
#[tokio::test]
async fn a_patch_cannot_change_the_provisioner() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SCS, &sc("a")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .patch(
            &format!("{SCS}/a"),
            &json!({"provisioner": "example.com/other"}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let (_, stored) = api.get(&format!("{SCS}/a")).await;
    assert_eq!(stored["provisioner"], "example.com/prov", "{stored}");
}

/// `ValidateUpdate` (storageclass/strategy.go) re-runs `ValidateStorageClass`
/// on the new object, so an update cannot introduce an invalid field.
#[tokio::test]
async fn an_update_is_validated_in_full() {
    let api = TestApiServer::new();
    let (s, _) = api.post(SCS, &sc("a")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .patch(
            &format!("{SCS}/a"),
            &json!({"allowedTopologies": [
                {"matchLabelExpressions": [{"key": "zone", "values": ["a"]}]},
                {"matchLabelExpressions": [{"key": "zone", "values": ["a"]}]}
            ]}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

/// `ReturnDeletedObject: true` (volumeattributesclass/storage/storage.go).
#[tokio::test]
async fn deleting_a_volume_attributes_class_returns_it() {
    let api = TestApiServer::new();
    let (s, body) = api.post(VACS, &vac("gold")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let (s, body) = api.delete(&format!("{VACS}/gold")).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "VolumeAttributesClass", "{body}");
}

/// `ValidateVolumeAttributesClassUpdate`: parameters are immutable, on PATCH
/// too.
#[tokio::test]
async fn a_patch_cannot_change_volume_attributes_parameters() {
    let api = TestApiServer::new();
    let (s, _) = api.post(VACS, &vac("gold")).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, body) = api
        .patch(
            &format!("{VACS}/gold"),
            &json!({"parameters": {"iops": "200"}}),
        )
        .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}
