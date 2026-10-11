//! #3249: `pods/binding` decodes the Binding strictly per `fieldValidation`.
//!
//! Upstream: `CreateNamedResource` (`staging/src/k8s.io/apiserver/pkg/endpoints/
//! handlers/create.go:119-140`) picks the strict serializer for Warn/Strict,
//! warns on unknown fields for Warn and rejects them for Strict.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

async fn setup(api: &TestApiServer, pod: &str) {
    let (code, _) = api
        .post(
            &format!("/api/v1/namespaces/{NS}/pods"),
            &json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": pod},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}}),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED);
}

async fn bind(api: &TestApiServer, pod: &str, query: &str) -> (StatusCode, Vec<String>, Value) {
    let body = json!({"apiVersion": "v1", "kind": "Binding",
        "metadata": {"name": pod, "namespace": NS},
        "target": {"kind": "Node", "name": "node-a"}, "bogus": 1});
    let (status, headers, _b, value) = api
        .send_full(
            "POST",
            &format!("/api/v1/namespaces/{NS}/pods/{pod}/binding{query}"),
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    (status, warnings, value)
}

#[tokio::test]
async fn binding_unknown_field_strict_is_rejected() {
    let api = TestApiServer::new();
    setup(&api, "p-strict").await;
    let (code, _, body) = bind(&api, "p-strict", "?fieldValidation=Strict").await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.to_string().contains("bogus"), "{body}");
}

#[tokio::test]
async fn binding_unknown_field_warn_default_warns() {
    let api = TestApiServer::new();
    setup(&api, "p-warn").await;
    let (code, warnings, body) = bind(&api, "p-warn", "").await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert!(warnings.iter().any(|w| w.contains("bogus")), "{warnings:?}");
}

#[tokio::test]
async fn binding_unknown_field_ignore_is_silent() {
    let api = TestApiServer::new();
    setup(&api, "p-ign").await;
    let (code, warnings, body) = bind(&api, "p-ign", "?fieldValidation=Ignore").await;
    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert!(warnings.is_empty(), "{warnings:?}");
}
