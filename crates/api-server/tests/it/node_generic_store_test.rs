//! Node and `nodes/status` served through the generic Store and endpoint
//! handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin Node's own:
//! `pkg/registry/core/node/strategy.go`, its `StatusREST`
//! (storage/storage.go) and `ValidateNode`/`ValidateNodeUpdate`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NODES: &str = "/api/v1/nodes";

fn node(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name},
        "spec": {"podCIDRs": ["10.0.0.0/24"]},
        "status": {"capacity": {"cpu": "2", "pods": "110"}}
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(NODES, &node(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
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

/// Nodes keep a client-set status on create (strategy.go:95), and
/// `SetDefaults_NodeStatus` (v1/defaults.go:346-354) fills allocatable from
/// capacity.
#[tokio::test]
async fn create_keeps_status_and_defaults_allocatable() {
    let api = TestApiServer::new();
    let out = create(&api, "n-create").await;
    assert_eq!(out["status"]["capacity"]["cpu"], "2", "{out}");
    assert_eq!(out["status"]["allocatable"]["cpu"], "2", "{out}");
}

/// `ValidateNode` (validation.go:7178-7216): ObjectMeta, and taint errors
/// rooted at `metadata.taints`.
#[tokio::test]
async fn create_validates_meta_and_taints() {
    let api = TestApiServer::new();
    let (status, out) = api.post(NODES, &node("Bad_Name")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.name"), "{out}");

    let mut body = node("n-taint");
    body["spec"]["taints"] = json!([{"key": "k", "effect": "Banish"}]);
    let (status, out) = api.post(NODES, &body).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.taints[0].effect"), "{out}");
}

/// `nodeWarnings` (strategy.go:306-311): an ambiguous pod CIDR warns.
#[tokio::test]
async fn an_ambiguous_pod_cidr_warns() {
    let api = TestApiServer::new();
    let mut body = node("n-cidr");
    body["spec"]["podCIDRs"] = json!(["10.0.0.8/24"]);
    let (status, warnings, out) = send(&api, "POST", NODES, Some(&body)).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains(r#"spec.podCIDRs[0]: CIDR value \"10.0.0.8/24\" is ambiguous"#)),
        "{warnings:?}"
    );
}

/// `PrepareForUpdate` (strategy.go:85-92) keeps status on a spec write; the
/// status strategy (:179-187) keeps spec.
#[tokio::test]
async fn spec_and_status_writes_keep_the_other_half() {
    let api = TestApiServer::new();
    let obj = create(&api, "n-halves").await;
    let uri = format!("{NODES}/n-halves");

    let mut body = obj;
    body["spec"]["unschedulable"] = json!(true);
    body["status"]["capacity"] = json!({"cpu": "8"});
    let (status, out) = api.put(&format!("{uri}/status"), &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["spec"].get("unschedulable").is_none(), "{out}");
    assert_eq!(out["status"]["capacity"]["cpu"], "8", "{out}");

    let mut body = out;
    body["spec"]["unschedulable"] = json!(true);
    body["status"]["capacity"] = json!({"cpu": "1"});
    let (status, out) = api.put(&uri, &body).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["unschedulable"], true, "{out}");
    assert_eq!(out["status"]["capacity"]["cpu"], "8", "{out}");
}

/// `/status` runs `ValidateNodeUpdate` (validation.go:7237-7303): capacity
/// must be non-negative, and an extended resource an integer.
#[tokio::test]
async fn status_writes_are_validated() {
    let api = TestApiServer::new();
    create(&api, "n-status").await;
    let uri = format!("{NODES}/n-status/status");
    let (status, out) = api
        .patch(&uri, &json!({"status": {"capacity": {"cpu": "-1"}}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("status.capacity.cpu"), "{out}");

    let (status, out) = api
        .patch(
            &uri,
            &json!({"status": {"capacity": {"example.com/foo": "500m"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("must be an integer"), "{out}");
}

/// A status PATCH merges into the stored maps (the e2e `AddExtendedResource`
/// shape, test/e2e/framework/node/helper.go:184-206), and no longer copies
/// capacity into allocatable — that is the kubelet's job
/// (nodestatus/setters.go:292-316).
#[tokio::test]
async fn a_status_patch_merges_and_leaves_allocatable_alone() {
    let api = TestApiServer::new();
    create(&api, "n-ext").await;
    let (status, out) = api
        .patch(
            &format!("{NODES}/n-ext/status"),
            &json!({"status": {"capacity": {"example.com/foo": "5"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["status"]["capacity"]["cpu"], "2", "{out}");
    assert_eq!(out["status"]["capacity"]["example.com/foo"], "5", "{out}");
    assert!(
        out["status"]["allocatable"]
            .get("example.com/foo")
            .is_none(),
        "{out}"
    );
}

/// `ValidateNodeUpdate` holds a PATCH: podCIDRs are immutable once set.
#[tokio::test]
async fn a_patch_is_validated() {
    let api = TestApiServer::new();
    create(&api, "n-patch").await;
    let (status, out) = api
        .patch(
            &format!("{NODES}/n-patch"),
            &json!({"spec": {"podCIDRs": ["10.1.0.0/24"]}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("node updates may not change podCIDR"),
        "{out}"
    );
}

/// `NewStorage` (storage/storage.go:96-110) leaves `ReturnDeletedObject`
/// unset: a DELETE that removes the node returns a success Status.
#[tokio::test]
async fn delete_returns_a_status() {
    let api = TestApiServer::new();
    create(&api, "n-del").await;
    let (status, out) = api.delete(&format!("{NODES}/n-del")).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Status", "{out}");
    assert_eq!(out["status"], "Success", "{out}");
}
