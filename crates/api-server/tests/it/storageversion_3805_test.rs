//! `internal.apiserver.k8s.io/v1alpha1` StorageVersion (#3805).
//!
//! Upstream: `pkg/registry/apiserverinternal/storageversion/strategy.go`
//! (`PrepareForCreate` clears status; `PrepareForUpdate` keeps status;
//! `GetResetFields` main resets `status`, `/status` resets `metadata` +
//! `spec`; `statusStrategy.PrepareForUpdate` keeps spec),
//! `pkg/apis/apiserverinternal/validation/validation.go`, and
//! `pkg/registry/apiserverinternal/rest/storage.go` (alpha group-version, off
//! by default).

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SV: &str = "/apis/internal.apiserver.k8s.io/v1alpha1/storageversions";
const APPLY: &str = "application/apply-patch+yaml";

fn sv(name: &str) -> Value {
    json!({"apiVersion": "internal.apiserver.k8s.io/v1alpha1", "kind": "StorageVersion",
           "metadata": {"name": name}, "spec": {}})
}

fn with_status(mut b: Value) -> Value {
    b["status"] = json!({
        "storageVersions": [{"apiServerID": "a", "encodingVersion": "v1",
                             "decodableVersions": ["v1", "v2"]}],
        "commonEncodingVersion": "v1",
        "conditions": [{"type": "AllEncodingVersionsEqual", "status": "True",
                        "reason": "CommonEncodingVersionSet", "message": "m",
                        "lastTransitionTime": "2026-01-01T00:00:00Z"}]});
    b
}

fn owned(out: &Value, manager: &str) -> Value {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == manager))
        .map(|e| e["fieldsV1"].clone())
        .unwrap_or(Value::Null)
}

async fn apply(api: &TestApiServer, uri: &str, manager: &str, b: &Value) -> (StatusCode, Value) {
    api.send(
        "PATCH",
        &format!("{uri}?fieldManager={manager}"),
        Some(APPLY),
        Some(b),
    )
    .await
}

/// Alpha group-version, off by default: the storage is not installed -> 404.
#[tokio::test]
#[serial_test::serial]
async fn not_served_while_the_gate_is_off() {
    let _g = with_feature(Feature::StorageVersionAPI, false);
    let api = TestApiServer::new();
    let (code, _) = api.post(SV, &sv("core.pods")).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let (code, _) = api.get(SV).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

/// PrepareForCreate: a client-supplied status is dropped.
#[tokio::test]
#[serial_test::serial]
async fn create_clears_status() {
    let _g = with_feature(Feature::StorageVersionAPI, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SV, &with_status(sv("core.pods"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert!(out["status"].get("storageVersions").is_none(), "{out}");
    assert!(out["status"].get("conditions").is_none(), "{out}");
}

/// ValidateStorageVersionName through the create endpoint.
#[tokio::test]
#[serial_test::serial]
async fn create_validates_name() {
    let _g = with_feature(Feature::StorageVersionAPI, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SV, &sv("pods")).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(
        out["details"]["causes"][0]["field"], "metadata.name",
        "{out}"
    );
}

/// PrepareForUpdate: a main update keeps the stored status.
#[tokio::test]
#[serial_test::serial]
async fn update_keeps_status() {
    let _g = with_feature(Feature::StorageVersionAPI, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SV, &sv("core.pods")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{SV}/core.pods");
    let mut put = with_status(out.clone());
    put["metadata"]["labels"] = json!({"x": "y"});
    let (code, out2) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out2}");
    assert!(out2["status"].get("storageVersions").is_none(), "{out2}");
    assert_eq!(out2["metadata"]["labels"]["x"], "y", "{out2}");
}

/// statusStrategy: spec and metadata kept; status validated
/// (ValidateStorageVersionStatusUpdate).
#[tokio::test]
#[serial_test::serial]
async fn status_update_keeps_meta_and_validates() {
    let _g = with_feature(Feature::StorageVersionAPI, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SV, &sv("core.pods")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{SV}/core.pods/status");

    let mut put = with_status(out.clone());
    put["metadata"]["labels"] = json!({"x": "y"});
    let (code, out2) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out2}");
    assert!(out2["metadata"].get("labels").is_none(), "{out2}");
    assert_eq!(out2["status"]["commonEncodingVersion"], "v1", "{out2}");

    // commonEncodingVersion must agree with the reported versions.
    let mut bad = out2.clone();
    bad["status"]["commonEncodingVersion"] = json!("v2");
    let (code, out3) = api.put(&uri, &bad).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out3}");
    assert_eq!(
        out3["details"]["causes"][0]["field"], "status.commonEncodingVersion",
        "{out3}"
    );
}

/// GetResetFields: the main endpoint does not let an applier own `status`.
#[tokio::test]
#[serial_test::serial]
async fn main_apply_does_not_own_status() {
    let _g = with_feature(Feature::StorageVersionAPI, true);
    let api = TestApiServer::new();
    let (code, out) = apply(
        &api,
        &format!("{SV}/core.jobs"),
        "user",
        &with_status(sv("core.jobs")),
    )
    .await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let o = owned(&out, "user");
    assert!(o.get("f:status").is_none(), "{out}");
}

/// GetResetFields: `/status` does not let an applier own `metadata` or `spec`.
#[tokio::test]
#[serial_test::serial]
async fn status_apply_does_not_own_spec_or_metadata() {
    let _g = with_feature(Feature::StorageVersionAPI, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SV, &sv("core.nodes")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, out) = apply(
        &api,
        &format!("{SV}/core.nodes/status"),
        "ctrl",
        &with_status(sv("core.nodes")),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let o = owned(&out, "ctrl");
    assert!(o.get("f:status").is_some(), "{out}");
    assert!(o.get("f:spec").is_none(), "{out}");
    assert!(o.get("f:metadata").is_none(), "{out}");
}
