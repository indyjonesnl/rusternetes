//! No Status message may expose an internal `/registry/...` storage key.
//!
//! Upstream builds these errors from a *structured* identity, never from a
//! storage path (`apimachinery/pkg/api/errors/errors.go`):
//!
//! ```go
//! func NewNotFound(qualifiedResource schema.GroupResource, name string) *StatusError {
//!     ... Details: &metav1.StatusDetails{Group: ..., Kind: qualifiedResource.Resource, Name: name},
//!         Message: fmt.Sprintf("%s %q not found", qualifiedResource.String(), name),
//! func NewAlreadyExists(qualifiedResource schema.GroupResource, name string) *StatusError {
//!     ... Message: fmt.Sprintf("%s %q already exists", qualifiedResource.String(), name),
//! ```
//!
//! Rusternetes' storage layer raises `Error::NotFound(key)` /
//! `Error::AlreadyExists(key)` with the raw key. `NotFound` was sanitized on
//! the way out; `AlreadyExists` was not, so a duplicate create answered with
//! the literal backend path:
//!
//! ```text
//! "message": "/registry/configmaps/default/kube-root-ca.crt"
//! ```
//!
//! That is how it surfaced in the vanilla-swap leg — the Go root-CA publisher
//! quotes our message straight into its own log:
//!
//! ```text
//! syncing "vs-warmup" failed: /registry/configmaps/vs-warmup/kube-root-ca.crt
//! ```
//!
//! Leaking the key tells a client nothing it can act on, and advertises the
//! storage layout.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn cm(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": name}, "data": {"a": "b"}
    })
}

fn assert_no_key_leak(body: &Value) {
    let rendered = serde_json::to_string(body).expect("serialize");
    assert!(
        !rendered.contains("/registry/"),
        "Status must not expose an internal storage key: {rendered}"
    );
}

#[tokio::test]
async fn duplicate_create_reports_already_exists_without_the_storage_key() {
    let state = TestApiServer::new();
    let uri = "/api/v1/namespaces/default/configmaps";

    let (code, body) = state.post(uri, &cm("kube-root-ca.crt")).await;
    assert_eq!(code, StatusCode::CREATED, "{body}");

    let (code, body) = state.post(uri, &cm("kube-root-ca.crt")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_no_key_leak(&body);
    assert_eq!(
        body["message"],
        json!("configmaps \"kube-root-ca.crt\" already exists"),
        "{body}"
    );
    assert_eq!(body["reason"], json!("AlreadyExists"), "{body}");
}

/// `details` carries the identity upstream puts there: the resource as `kind`
/// and the bare object name as `name` — not the rendered message.
#[tokio::test]
async fn already_exists_details_carry_kind_and_bare_name() {
    let state = TestApiServer::new();
    let uri = "/api/v1/namespaces/default/configmaps";
    let (_, _) = state.post(uri, &cm("dup")).await;
    let (code, body) = state.post(uri, &cm("dup")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["details"]["name"], json!("dup"), "{body}");
    assert_eq!(body["details"]["kind"], json!("configmaps"), "{body}");
}

#[tokio::test]
async fn missing_namespaced_get_reports_not_found_with_a_bare_name() {
    let state = TestApiServer::new();
    let (code, body) = state
        .get("/api/v1/namespaces/default/configmaps/nope")
        .await;
    assert_eq!(code, StatusCode::NOT_FOUND, "{body}");
    assert_no_key_leak(&body);
    assert_eq!(
        body["message"],
        json!("configmaps \"nope\" not found"),
        "{body}"
    );
    // Regression: details.name held the whole rendered message.
    assert_eq!(body["details"]["name"], json!("nope"), "{body}");
    assert_eq!(body["details"]["kind"], json!("configmaps"), "{body}");
}

#[tokio::test]
async fn missing_cluster_scoped_get_reports_not_found_with_a_bare_name() {
    let state = TestApiServer::new();
    let (code, body) = state.get("/api/v1/nodes/nope").await;
    assert_eq!(code, StatusCode::NOT_FOUND, "{body}");
    assert_no_key_leak(&body);
    assert_eq!(body["message"], json!("nodes \"nope\" not found"), "{body}");
    assert_eq!(body["details"]["name"], json!("nope"), "{body}");
    assert_eq!(body["details"]["kind"], json!("nodes"), "{body}");
}

/// A name containing dots and dashes must survive intact — the key parser
/// takes the last segment, it does not split on punctuation.
#[tokio::test]
async fn a_dotted_name_survives_the_key_round_trip() {
    let state = TestApiServer::new();
    let uri = "/api/v1/namespaces/default/configmaps";
    let (_, _) = state.post(uri, &cm("kube-root-ca.crt.v2")).await;
    let (code, body) = state.post(uri, &cm("kube-root-ca.crt.v2")).await;
    assert_eq!(code, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["message"],
        json!("configmaps \"kube-root-ca.crt.v2\" already exists"),
        "{body}"
    );
    assert_eq!(
        body["details"]["name"],
        json!("kube-root-ca.crt.v2"),
        "{body}"
    );
}
