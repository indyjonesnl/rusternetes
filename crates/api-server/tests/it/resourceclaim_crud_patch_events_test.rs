//! `[sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceClaim` (#3061).
//!
//! The spec (`test/e2e/dra/dra.go:115`, driven by
//! `test/e2e/framework/conformance/conformance.go:808-827`) recreates the
//! claim, patches it with each patch type and expects the informer to see
//! `add,(update,)+`. The apply patch is the step that stalled.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CLAIMS: &str = "/apis/resource.k8s.io/v1/namespaces/default/resourceclaims";

fn claim() -> Value {
    json!({
        "apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaim",
        "metadata": {"name": "test", "labels": {"e2e": "x"}}, "status": {},
        "spec": {"devices": {"requests": [
            {"name": "req-0", "exactly": {"deviceClassName": "dra.example.com"}}
        ]}}
    })
}

#[tokio::test]
async fn apply_patch_adds_the_label_and_records_the_applied_fields() {
    let api = TestApiServer::new();
    let (status, created) = api.post(CLAIMS, &claim()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let patch = json!({
        "apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaim",
        "metadata": {
            "labels": {"test.dra.example.com": "test"},
            "name": "test", "namespace": "default",
            "uid": created["metadata"]["uid"],
        }
    });
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{CLAIMS}/test?fieldManager=test-apply&force=true&fieldValidation=Strict"),
            Some("application/apply-patch+yaml"),
            Some(&patch),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["labels"]["test.dra.example.com"], "test");
    // A label another manager owns survives an apply that does not mention
    // it; the spec's informer selects on it, so losing it turns the update
    // into a DELETED event.
    assert_eq!(out["metadata"]["labels"]["e2e"], "x", "{out}");
    let mf = out["metadata"]["managedFields"].as_array().unwrap();
    let applied = mf.iter().find(|m| m["operation"] == "Apply").unwrap();
    assert!(
        applied["fieldsV1"]["f:metadata"]["f:labels"]["f:test.dra.example.com"].is_object(),
        "{applied}"
    );
    assert_ne!(
        created["metadata"]["resourceVersion"], out["metadata"]["resourceVersion"],
        "{out}"
    );
}

/// The informer's view of the recreate-then-apply step: `ADDED` then at
/// least one `MODIFIED` (conformance.go:826 `^add,(update,)+$`).
#[tokio::test]
async fn a_watch_sees_the_apply_patch_as_a_modification() {
    use futures::StreamExt;
    let api = TestApiServer::new();
    let watch = api
        .respond(
            "GET",
            &format!("{CLAIMS}?watch=true&labelSelector=e2e%3Dx"),
            None,
            None,
        )
        .await;
    let (status, created) = api.post(CLAIMS, &claim()).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let patch = json!({
        "apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaim",
        "metadata": {
            "labels": {"test.dra.example.com": "test"},
            "name": "test", "namespace": "default",
            "uid": created["metadata"]["uid"],
        }
    });
    let (status, out) = api
        .send(
            "PATCH",
            &format!("{CLAIMS}/test?fieldManager=test-apply&force=true&fieldValidation=Strict"),
            Some("application/apply-patch+yaml"),
            Some(&patch),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");

    let mut stream = watch.into_body().into_data_stream();
    let mut buf = String::new();
    let mut types = Vec::new();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(800), async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    types.push(v["type"].as_str().unwrap_or_default().to_string());
                }
            }
        }
    })
    .await;
    assert_eq!(types, ["ADDED", "MODIFIED"], "{types:?}");
}
