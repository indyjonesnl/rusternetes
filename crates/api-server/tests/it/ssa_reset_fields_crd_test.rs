//! Server-side apply honours `GetResetFields` for CustomResourceDefinition
//! (+/status) and custom resources (+/status) (#3789).
//!
//! Upstream: `customresourcedefinition/strategy.go:60` (status),
//! `:252` (/status: metadata, spec); `customresource/strategy.go:132`
//! (status, only when the version has a status subresource) and
//! `status_strategy.go:48` (spec).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const APPLY: &str = "application/apply-patch+yaml";
const GROUP: &str = "stable.example.com";
const CRDS: &str = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions";
const WIDGETS: &str = "/apis/stable.example.com/v1/namespaces/default/widgets";

fn owned_keys(out: &Value, manager: &str) -> Vec<String> {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == manager))
        .and_then(|e| e["fieldsV1"].as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
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

fn crd(with_status: bool) -> Value {
    let mut v = json!({
        "name": "v1", "served": true, "storage": true,
        "schema": { "openAPIV3Schema": {
            "type": "object",
            "x-kubernetes-preserve-unknown-fields": true
        } }
    });
    if with_status {
        v["subresources"] = json!({"status": {}});
    }
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("widgets.{GROUP}") },
        "spec": {
            "group": GROUP, "scope": "Namespaced",
            "names": { "plural": "widgets", "kind": "Widget", "listKind": "WidgetList" },
            "versions": [v]
        }
    })
}

fn crd_with_status_field(mut c: Value) -> Value {
    c["status"] = json!({"storedVersions": ["v1"]});
    c
}

#[tokio::test]
async fn crd_main_does_not_own_status() {
    let api = TestApiServer::new();
    let uri = format!("{CRDS}/widgets.{GROUP}");
    let (code, out) = apply(&api, &uri, "user", &crd_with_status_field(crd(false))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let owned = owned_keys(&out, "user");
    assert!(owned.contains(&"f:spec".to_string()), "{out}");
    assert!(!owned.contains(&"f:status".to_string()), "{out}");
}

#[tokio::test]
async fn crd_status_does_not_own_spec_or_metadata() {
    let api = TestApiServer::new();
    let (code, out) = api.post(CRDS, &crd(false)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let mut b = crd_with_status_field(crd(false));
    b["metadata"]["labels"] = json!({"a": "b"});
    let uri = format!("{CRDS}/widgets.{GROUP}/status");
    let (code, out) = apply(&api, &uri, "ctrl", &b).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let owned = owned_keys(&out, "ctrl");
    assert!(owned.contains(&"f:status".to_string()), "{out}");
    assert!(!owned.contains(&"f:spec".to_string()), "{out}");
    assert!(!owned.contains(&"f:metadata".to_string()), "{out}");
}

async fn setup(with_status: bool) -> TestApiServer {
    let api = TestApiServer::new();
    let (code, out) = api.post(CRDS, &crd(with_status)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("widgets.{GROUP}")).await;
    api
}

fn widget(name: &str) -> Value {
    json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": {"name": name},
        "spec": {"size": 1}, "status": {"ready": true}
    })
}

#[tokio::test]
async fn cr_main_does_not_own_status_with_status_subresource() {
    let api = setup(true).await;
    let (code, out) = apply(&api, &format!("{WIDGETS}/w1"), "user", &widget("w1")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let owned = owned_keys(&out, "user");
    assert!(owned.contains(&"f:spec".to_string()), "{out}");
    assert!(!owned.contains(&"f:status".to_string()), "{out}");
}

/// `a.status != nil` (strategy.go:135): without the subresource, `status`
/// is an ordinary field and the manager owns it.
#[tokio::test]
async fn cr_main_owns_status_without_status_subresource() {
    let api = setup(false).await;
    let (code, out) = apply(&api, &format!("{WIDGETS}/w1"), "user", &widget("w1")).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert!(
        owned_keys(&out, "user").contains(&"f:status".to_string()),
        "{out}"
    );
}

#[tokio::test]
async fn cr_status_does_not_own_spec() {
    let api = setup(true).await;
    let mut plain = widget("w2");
    plain.as_object_mut().unwrap().remove("status");
    let (code, out) = api.post(WIDGETS, &plain).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, out) = apply(&api, &format!("{WIDGETS}/w2/status"), "ctrl", &widget("w2")).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let owned = owned_keys(&out, "ctrl");
    assert!(owned.contains(&"f:status".to_string()), "{out}");
    assert!(!owned.contains(&"f:spec".to_string()), "{out}");
}
