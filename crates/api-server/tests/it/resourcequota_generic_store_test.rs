//! ResourceQuota and `resourcequotas/status` served through the generic Store
//! and endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin ResourceQuota's own:
//! `pkg/registry/core/resourcequota/strategy.go`, its `StatusREST`
//! (storage/storage.go) and the core validators. The lost-update and status
//! validation cases live in `resourcequota_update_lost_update_test` and
//! `resourcequota_status_validation_test`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const RQS: &str = "/api/v1/namespaces/default/resourcequotas";

fn quota(name: &str, hard: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ResourceQuota",
        "metadata": {"name": name},
        "spec": {"hard": hard}
    })
}

async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, _, out) = api
        .send_full(
            method,
            uri,
            body.map(|_| "application/json"),
            None,
            body.map(|b| serde_json::to_vec(b).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    (status, warnings, out)
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:60-63) drops a client-set status, and
/// `WarningsOnCreate` (:87-108) warns on a request above its limit.
#[tokio::test]
async fn create_clears_status_and_warns_on_request_above_limit() {
    let api = TestApiServer::new();
    let mut body = quota("q-create", json!({"requests.cpu": "2", "limits.cpu": "1"}));
    body["status"] = json!({"hard": {"pods": "9"}, "used": {"pods": "9"}});
    let (status, warnings, out) = send(&api, "POST", RQS, Some(&body)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["status"], json!({}), "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("requests.cpu (2) should be less than limits.cpu (1)")),
        "{warnings:?}"
    );
}

/// `ValidateResourceQuota` (validation.go:8068-8075) checks ObjectMeta.
#[tokio::test]
async fn create_validates_object_meta() {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(RQS, &quota("Bad_Name", json!({"pods": "1"})))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.name"), "{out}");
}

/// `ValidateResourceQuotaUpdate` (validation.go:8125-8144) holds a PATCH:
/// the spec is validated and `spec.scopes` is immutable.
#[tokio::test]
async fn a_patch_is_validated() {
    let api = TestApiServer::new();
    let (status, out) = api.post(RQS, &quota("q-patch", json!({"pods": "1"}))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let uri = format!("{RQS}/q-patch");

    let (status, out) = api
        .patch(&uri, &json!({"spec": {"hard": {"pods": "-1"}}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.hard[pods]"), "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"spec": {"scopes": ["BestEffort"]}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.scopes"), "{out}");
}

/// `PrepareForUpdate` (strategy.go:66-70) keeps status on a spec write, and
/// the status strategy (:153-157) keeps spec on a status write.
#[tokio::test]
async fn spec_and_status_writes_keep_the_other_half() {
    let api = TestApiServer::new();
    let (status, obj) = api
        .post(RQS, &quota("q-halves", json!({"pods": "1"})))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{obj}");
    let uri = format!("{RQS}/q-halves");

    let mut body = obj;
    body["spec"]["hard"] = json!({"pods": "5"});
    body["status"] = json!({"hard": {"pods": "5"}, "used": {"pods": "1"}});
    let (status, out) = api.put(&format!("{uri}/status"), &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["hard"]["pods"], "1", "{out}");
    assert_eq!(out["status"]["used"]["pods"], "1", "{out}");

    let mut body = out;
    body["spec"]["hard"] = json!({"pods": "7"});
    body["status"] = json!({});
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["hard"]["pods"], "7", "{out}");
    assert_eq!(out["status"]["used"]["pods"], "1", "{out}");
}

/// `ReturnDeletedObject: true` (storage/storage.go:52): DELETE returns the
/// ResourceQuota.
#[tokio::test]
async fn delete_returns_the_object() {
    let api = TestApiServer::new();
    let (status, obj) = api.post(RQS, &quota("q-del", json!({"pods": "1"}))).await;
    assert_eq!(status, StatusCode::CREATED, "{obj}");
    let (status, out) = api.delete(&format!("{RQS}/q-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "ResourceQuota", "{out}");
    assert_eq!(out["metadata"]["name"], "q-del", "{out}");
}
