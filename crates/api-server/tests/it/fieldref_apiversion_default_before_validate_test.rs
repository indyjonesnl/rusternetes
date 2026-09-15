//! A workload whose container env uses a downward-API `fieldRef` without an
//! explicit `apiVersion` must be admitted: `SetDefaults_ObjectFieldSelector`
//! fills in `"v1"` before validation ever runs.
//!
//! Upstream orders this as decode (which defaults, via the codec's
//! `scheme.Default`) -> `PrepareForCreate` -> `Validate`:
//!   staging/src/k8s.io/apiserver/pkg/registry/rest/create.go:26-28 (BeforeCreate)
//!   pkg/apis/core/v1/defaults.go SetDefaults_ObjectFieldSelector
//!   pkg/apis/core/validation/validation.go validateObjectFieldSelector
//!
//! Because `validateObjectFieldSelector` hard-requires `apiVersion`, a handler
//! that validates *before* defaulting rejects manifests the real API server
//! accepts. cert-manager's three Deployments are exactly this shape, and the
//! inversion took the cert-manager smoke job red:
//!   spec.template.spec.containers[0].env[0].valueFrom.fieldRef.apiVersion:
//!   Required value

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

/// The cert-manager container shape: a downward-API env var with `fieldPath`
/// only, exactly as cert-manager.yaml ships it.
fn pod_template() -> Value {
    json!({
        "metadata": {"labels": {"app": "fr"}},
        "spec": {
            "containers": [{
                "name": "c",
                "image": "busybox",
                "env": [{
                    "name": "POD_NAMESPACE",
                    "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}
                }]
            }]
        }
    })
}

fn field_ref(body: &Value, template_path: &[&str]) -> Value {
    let mut cur = body;
    for p in template_path {
        cur = &cur[p];
    }
    cur["spec"]["containers"][0]["env"][0]["valueFrom"]["fieldRef"].clone()
}

#[tokio::test]
async fn deployment_field_ref_api_version_defaults_to_v1() {
    let state = TestApiServer::new();
    let body_in = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "d-fr"},
        "spec": {
            "selector": {"matchLabels": {"app": "fr"}},
            "template": pod_template()
        }
    });

    let (code, body) = state
        .post(
            &format!("/apis/apps/v1/namespaces/{NS}/deployments"),
            &body_in,
        )
        .await;

    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        field_ref(&body, &["spec", "template"])["apiVersion"],
        json!("v1"),
        "{body}"
    );
}

#[tokio::test]
async fn replicaset_field_ref_api_version_defaults_to_v1() {
    let state = TestApiServer::new();
    let body_in = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "rs-fr"},
        "spec": {
            "selector": {"matchLabels": {"app": "fr"}},
            "template": pod_template()
        }
    });

    let (code, body) = state
        .post(
            &format!("/apis/apps/v1/namespaces/{NS}/replicasets"),
            &body_in,
        )
        .await;

    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        field_ref(&body, &["spec", "template"])["apiVersion"],
        json!("v1"),
        "{body}"
    );
}

#[tokio::test]
async fn pod_template_field_ref_api_version_defaults_to_v1() {
    let state = TestApiServer::new();
    let body_in = json!({
        "apiVersion": "v1",
        "kind": "PodTemplate",
        "metadata": {"name": "pt-fr"},
        "template": pod_template()
    });

    let (code, body) = state
        .post(&format!("/api/v1/namespaces/{NS}/podtemplates"), &body_in)
        .await;

    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        field_ref(&body, &["template"])["apiVersion"],
        json!("v1"),
        "{body}"
    );
}

/// A DaemonSet already defaults before validating; this pins that ordering so a
/// future refactor cannot silently re-invert it.
#[tokio::test]
async fn daemonset_field_ref_api_version_defaults_to_v1() {
    let state = TestApiServer::new();
    let body_in = json!({
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "ds-fr"},
        "spec": {
            "selector": {"matchLabels": {"app": "fr"}},
            "template": pod_template()
        }
    });

    let (code, body) = state
        .post(
            &format!("/apis/apps/v1/namespaces/{NS}/daemonsets"),
            &body_in,
        )
        .await;

    assert_eq!(code, StatusCode::CREATED, "{body}");
    assert_eq!(
        field_ref(&body, &["spec", "template"])["apiVersion"],
        json!("v1"),
        "{body}"
    );
}
