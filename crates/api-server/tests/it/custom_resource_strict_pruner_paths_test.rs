//! #3306: a Strict decode of a custom resource fails with the
//! `StrictDecodingError` built from the pruner's paths
//! (`structuralpruning.PruneWithOptions` with `TrackUnknownFieldPaths`,
//! customresource_handler.go:1427-1435, :1338-1342), not from a heuristic walk
//! of `spec` alone.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const GROUP: &str = "stable.example.com";
const PATH: &str = "/apis/stable.example.com/v1/namespaces/default/widgets";

fn crd() -> Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("widgets.{GROUP}") },
        "spec": {
            "group": GROUP, "scope": "Namespaced",
            "names": { "plural": "widgets", "kind": "Widget", "listKind": "WidgetList" },
            "versions": [{
                "name": "v1", "served": true, "storage": true,
                "schema": { "openAPIV3Schema": {
                    "type": "object",
                    "properties": {
                        "spec": { "type": "object", "properties": {
                            "a": { "type": "string" },
                            "items": { "type": "array", "items": { "type": "object",
                                "properties": { "n": { "type": "string" } } } },
                            "free": { "type": "object",
                                "x-kubernetes-preserve-unknown-fields": true }
                        } },
                        "status": { "type": "object", "properties": {
                            "ok": { "type": "boolean" } } }
                    }
                } }
            }]
        }
    })
}

async fn setup() -> TestApiServer {
    let api = TestApiServer::new();
    let (status, out) = api
        .post(
            "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
            &crd(),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api.wait_crd_established(&format!("widgets.{GROUP}")).await;
    api
}

fn widget(extra: Value) -> Value {
    let mut w = json!({
        "apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
        "metadata": { "name": "w1" },
        "spec": { "a": "b" }
    });
    for (k, v) in extra.as_object().unwrap() {
        w[k] = v.clone();
    }
    w
}

async fn strict_message(body: Value) -> (StatusCode, String) {
    let api = setup().await;
    let (status, out) = api
        .post(&format!("{PATH}?fieldValidation=Strict"), &body)
        .await;
    (
        status,
        out["message"].as_str().unwrap_or_default().to_string(),
    )
}

/// The pruner walks `status` too; the heuristic checked `spec` only.
#[tokio::test]
async fn strict_refuses_an_unknown_status_field() {
    let (status, msg) = strict_message(widget(json!({"status": {"ok": true, "bogus": 1}}))).await;
    assert!(status.is_client_error(), "{status} {msg}");
    assert!(
        msg.contains(r#"strict decoding error: unknown field "status.bogus""#),
        "{msg}"
    );
}

/// `NewStrictDecodingError(errs)` carries every path, joined as
/// `strict decoding error: a, b` (runtime/error.go `strictDecodingError.Error`).
#[tokio::test]
async fn strict_reports_every_unknown_path_in_one_error() {
    let (status, msg) = strict_message(widget(json!({
        "spec": {"a": "b", "x": 1, "items": [{"n": "1", "y": 2}]},
        "top": 1
    })))
    .await;
    assert!(status.is_client_error(), "{status} {msg}");
    for p in ["spec.x", "spec.items[0].y", "top"] {
        assert!(
            msg.contains(&format!(r#"unknown field "{p}""#)),
            "{p}: {msg}"
        );
    }
    assert_eq!(msg.matches("strict decoding error").count(), 1, "{msg}");
}

/// A subtree with `x-kubernetes-preserve-unknown-fields` keeps its fields.
#[tokio::test]
async fn strict_accepts_fields_under_preserve_unknown_fields() {
    let (status, msg) = strict_message(widget(json!({
        "spec": {"a": "b", "free": {"anything": {"at": "all"}}}
    })))
    .await;
    assert_eq!(status, StatusCode::CREATED, "{msg}");
}

async fn patch_with(
    api: &TestApiServer,
    content_type: &str,
    directive: &str,
    body: Value,
) -> (StatusCode, Vec<String>, Value) {
    let (status, headers, _, v) = api
        .send_full(
            "PATCH",
            &format!("{PATH}/w1?fieldValidation={directive}&fieldManager=t"),
            Some(content_type),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .map(|h| h.to_str().unwrap().to_string())
        .collect();
    (status, warnings, v)
}

async fn seeded() -> TestApiServer {
    let api = setup().await;
    let (status, out) = api.post(PATH, &widget(json!({}))).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    api
}

/// patch.go:331-363: a Strict JSON/merge patch strict-decodes the patched
/// object, so it reports the pruner's `unknown field "<path>"` errors (all of
/// them), not the apply path's `field not declared in schema`.
#[tokio::test]
async fn strict_merge_patch_reports_pruner_paths() {
    let api = seeded().await;
    let (status, _, v) = patch_with(
        &api,
        "application/merge-patch+json",
        "Strict",
        json!({"spec": {"x": 1}, "top": 2}),
    )
    .await;
    assert!(status.is_client_error(), "{status} {v}");
    let msg = v["message"].as_str().unwrap();
    assert!(msg.contains("strict decoding error: "), "{msg}");
    assert!(msg.contains(r#"unknown field "spec.x""#), "{msg}");
    assert!(msg.contains(r#"unknown field "top""#), "{msg}");
    assert!(!msg.contains("not declared in schema"), "{msg}");
}

/// A server-side apply keeps the typed conversion's message
/// (field_validation.go:458-461).
#[tokio::test]
async fn strict_apply_patch_reports_not_declared_in_schema() {
    let api = seeded().await;
    let (status, _, v) = patch_with(
        &api,
        "application/apply-patch+yaml",
        "Strict",
        json!({"apiVersion": format!("{GROUP}/v1"), "kind": "Widget",
               "metadata": {"name": "w1"}, "unknownField": "u"}),
    )
    .await;
    assert!(status.is_client_error(), "{status} {v}");
    assert!(
        v["message"]
            .as_str()
            .unwrap()
            .contains(".unknownField: field not declared in schema"),
        "{v}"
    );
}

/// Warn applies the patch and warns each pruned path (patch.go:349); Ignore
/// applies it silently.
#[tokio::test]
async fn warn_and_ignore_merge_patch() {
    let api = seeded().await;
    let (status, warnings, v) = patch_with(
        &api,
        "application/merge-patch+json",
        "Warn",
        json!({"spec": {"x": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(
        warnings
            .iter()
            .any(|w| w == r#"299 - "unknown field \"spec.x\"""#),
        "{warnings:?}"
    );
    let (status, warnings, v) = patch_with(
        &api,
        "application/merge-patch+json",
        "Ignore",
        json!({"spec": {"y": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(warnings.is_empty(), "{warnings:?}");
}
