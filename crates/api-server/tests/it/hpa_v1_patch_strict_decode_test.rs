//! A PATCH to an `autoscaling/v1` HorizontalPodAutoscaler strict-decodes the
//! patched document in v1 (#4076). Upstream decodes the patch result into the
//! *versioned* object (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go`
//! :331-363), so a v2-only field such as `spec.metrics` is an unknown field at
//! `autoscaling/v1` and `fieldValidation=Warn` answers a `Warning: 299` header
//! while `Strict` answers 422.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const V1: &str = "/apis/autoscaling/v1/namespaces/default/horizontalpodautoscalers";

async fn patch(query: &str, patch: &Value) -> (StatusCode, Vec<String>, Value) {
    let api = TestApiServer::new();
    let (code, _) = api
        .send(
            "POST",
            V1,
            Some("application/json"),
            Some(&json!({
                "apiVersion": "autoscaling/v1",
                "kind": "HorizontalPodAutoscaler",
                "metadata": {"name": "h"},
                "spec": {
                    "scaleTargetRef": {"kind": "Deployment", "name": "d", "apiVersion": "apps/v1"},
                    "maxReplicas": 3
                }
            })),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED);
    let (status, headers, _bytes, v) = api
        .send_full(
            "PATCH",
            &format!("{V1}/h{query}"),
            Some("application/merge-patch+json"),
            None,
            Some(serde_json::to_vec(patch).unwrap()),
        )
        .await;
    let warnings = headers
        .get_all("warning")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    (status, warnings, v)
}

#[tokio::test]
async fn merge_patch_warn_emits_unknown_field_warning() {
    let (status, warnings, v) = patch(
        "?fieldValidation=Warn",
        &json!({"spec": {"bogusField": "x"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(
        warnings
            .iter()
            .any(|w| w.starts_with("299 ") && w.contains("bogusField")),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn merge_patch_strict_rejects_unknown_field() {
    let (status, _, v) = patch(
        "?fieldValidation=Strict",
        &json!({"spec": {"bogusField": "x"}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
    assert!(v.to_string().contains("bogusField"), "{v}");
}

#[tokio::test]
async fn merge_patch_ignore_emits_no_warning() {
    let (status, warnings, _) = patch(
        "?fieldValidation=Ignore",
        &json!({"spec": {"bogusField": "x"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(warnings.is_empty(), "{warnings:?}");
}

#[tokio::test]
async fn merge_patch_warn_known_v1_fields_is_silent() {
    let (status, warnings, v) = patch(
        "?fieldValidation=Warn",
        &json!({"spec": {"minReplicas": 1, "targetCPUUtilizationPercentage": 50}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(warnings.is_empty(), "{warnings:?}");
}
