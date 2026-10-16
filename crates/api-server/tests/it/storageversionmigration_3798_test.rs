//! `storagemigration.k8s.io/v1beta1` StorageVersionMigration (#3798).
//!
//! Upstream: `pkg/registry/storagemigration/storagemigration/strategy.go`
//! (`PrepareForCreate` :66-70 clears status; `GetResetFields` :55-63 main
//! resets `status`, :119-128 `/status` resets `metadata` + `spec`),
//! `pkg/apis/storagemigration/validation/validation.go`, and
//! `pkg/registry/storagemigration/rest/storage_storagemigration.go` (served
//! only under the `StorageVersionMigrator` gate, off in v1.35).

use axum::http::StatusCode;
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SVM: &str = "/apis/storagemigration.k8s.io/v1beta1/storageversionmigrations";
const APPLY: &str = "application/apply-patch+yaml";

fn svm(name: &str) -> Value {
    json!({"apiVersion": "storagemigration.k8s.io/v1beta1", "kind": "StorageVersionMigration",
           "metadata": {"name": name},
           "spec": {"resource": {"group": "example.com", "resource": "myresources"}}})
}

fn with_status(mut b: Value, rv: &str, conds: Value) -> Value {
    b["status"] = json!({"resourceVersion": rv, "conditions": conds});
    b
}

fn running() -> Value {
    json!([{"type": "Running", "status": "True", "reason": "Running",
            "message": "m", "lastTransitionTime": "2026-01-01T00:00:00Z"}])
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

/// storage_storagemigration.go: gate off -> storage not installed -> 404.
#[tokio::test]
#[serial_test::serial]
async fn not_served_while_the_gate_is_off() {
    let _g = with_feature(Feature::StorageVersionMigrator, false);
    let api = TestApiServer::new();
    let (code, _) = api.post(SVM, &svm("a")).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let (code, _) = api.get(SVM).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

/// strategy.go:66-70: a client-supplied status is dropped on create.
#[tokio::test]
#[serial_test::serial]
async fn create_clears_status() {
    let _g = with_feature(Feature::StorageVersionMigrator, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SVM, &with_status(svm("a"), "5", running())).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert!(out["status"].get("resourceVersion").is_none(), "{out}");
    assert!(out["status"].get("conditions").is_none(), "{out}");
}

/// validation.go:33-53 through the create endpoint.
#[tokio::test]
#[serial_test::serial]
async fn create_validates_spec() {
    let _g = with_feature(Feature::StorageVersionMigrator, true);
    let api = TestApiServer::new();
    let mut b = svm("a");
    b["spec"]["resource"]["resource"] = json!("");
    let (code, out) = api.post(SVM, &b).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert_eq!(
        out["details"]["causes"][0]["field"], "spec.resource.resource",
        "{out}"
    );
}

/// validation.go:55-63: spec is immutable; strategy.go:73-77: a main update
/// keeps the stored status.
#[tokio::test]
#[serial_test::serial]
async fn update_spec_immutable_and_status_kept() {
    let _g = with_feature(Feature::StorageVersionMigrator, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SVM, &svm("a")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{SVM}/a");
    let mut put = out.clone();
    put["spec"]["resource"]["group"] = json!("other.example.com");
    let (code, out2) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out2}");

    let mut put = out.clone();
    put["status"] = json!({"resourceVersion": "9"});
    let (code, out3) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out3}");
    assert!(out3["status"].get("resourceVersion").is_none(), "{out3}");
}

/// strategy.go:130-136 + validation.go:65-109 through `/status`.
#[tokio::test]
#[serial_test::serial]
async fn status_update_keeps_spec_and_validates() {
    let _g = with_feature(Feature::StorageVersionMigrator, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SVM, &svm("a")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uri = format!("{SVM}/a/status");

    let mut put = with_status(out.clone(), "123", running());
    put["spec"]["resource"]["resource"] = json!("changed");
    let (code, out2) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::OK, "{out2}");
    assert_eq!(
        out2["spec"]["resource"]["resource"], "myresources",
        "{out2}"
    );
    assert_eq!(out2["status"]["resourceVersion"], "123", "{out2}");

    // status.resourceVersion is immutable once set.
    let put = with_status(out2.clone(), "456", running());
    let (code, out3) = api.put(&uri, &put).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out3}");
}

/// GetResetFields (strategy.go:55-63): the main endpoint does not let an
/// applier own `status`.
#[tokio::test]
#[serial_test::serial]
async fn main_apply_does_not_own_status() {
    let _g = with_feature(Feature::StorageVersionMigrator, true);
    let api = TestApiServer::new();
    let body = with_status(svm("b"), "5", running());
    let (code, out) = apply(&api, &format!("{SVM}/b"), "user", &body).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let o = owned(&out, "user");
    assert!(o.get("f:spec").is_some(), "{out}");
    assert!(o.get("f:status").is_none(), "{out}");
}

/// GetResetFields (strategy.go:119-128): `/status` does not let an applier
/// own `metadata` or `spec`.
#[tokio::test]
#[serial_test::serial]
async fn status_apply_does_not_own_spec_or_metadata() {
    let _g = with_feature(Feature::StorageVersionMigrator, true);
    let api = TestApiServer::new();
    let (code, out) = api.post(SVM, &svm("c")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let body = with_status(svm("c"), "123", running());
    let (code, out) = apply(&api, &format!("{SVM}/c/status"), "ctrl", &body).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let o = owned(&out, "ctrl");
    assert!(o.get("f:status").is_some(), "{out}");
    assert!(o.get("f:spec").is_none(), "{out}");
    assert!(o.get("f:metadata").is_none(), "{out}");
}
