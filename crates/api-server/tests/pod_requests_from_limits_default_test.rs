//! SetDefaults_Pod: a container with explicit limits but no requests has its
//! requests defaulted to those limits, on Pod create.
//!
//! Upstream runs this for `spec.containers` **and** `spec.initContainers`
//! (`pkg/apis/core/v1/defaults.go:164-192` — two identical loops), and only on a
//! standalone Pod, never on an embedded PodTemplateSpec: "we only want this
//! defaulting semantic to take place on a v1.Pod and not a v1.PodTemplate"
//! (defaults.go:166-167).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn pods_uri() -> String {
    format!("/api/v1/namespaces/{NS}/pods")
}

/// A pod whose regular container and init container both declare limits only.
fn pod_with_limits_only(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name},
        "spec": {
            "initContainers": [{
                "name": "init",
                "image": "busybox",
                "resources": {"limits": {"cpu": "250m", "memory": "64Mi"}}
            }],
            "containers": [{
                "name": "app",
                "image": "nginx",
                "resources": {"limits": {"cpu": "500m", "memory": "128Mi"}}
            }]
        }
    })
}

#[tokio::test]
async fn container_requests_default_to_limits() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(&pods_uri(), &pod_with_limits_only("p-app"))
        .await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    let requests = &body["spec"]["containers"][0]["resources"]["requests"];
    assert_eq!(requests["cpu"], json!("500m"), "cpu request: {body}");
    assert_eq!(requests["memory"], json!("128Mi"), "memory request: {body}");
}

/// The init-container loop at `defaults.go:181-192` is a verbatim copy of the
/// container loop above it. Without it a limits-only init container reaches the
/// scheduler and ResourceQuota declaring no requests at all — and since a pod's
/// effective request is `max(init requests, sum(container requests))`
/// (`pkg/api/v1/resource/helpers.go` PodRequests), an init container sized
/// larger than the app containers is charged and scheduled as if it were free.
#[tokio::test]
async fn init_container_requests_default_to_limits() {
    let state = TestApiServer::new();
    let (code, body) = state
        .post(&pods_uri(), &pod_with_limits_only("p-init"))
        .await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    let requests = &body["spec"]["initContainers"][0]["resources"]["requests"];
    assert_eq!(requests["cpu"], json!("250m"), "init cpu request: {body}");
    assert_eq!(
        requests["memory"],
        json!("64Mi"),
        "init memory request: {body}"
    );
}

/// An explicit request wins over the limit — upstream only fills keys that are
/// absent (`if _, exists := ...Requests[key]; !exists`, defaults.go:175).
#[tokio::test]
async fn explicit_requests_are_not_overwritten() {
    let state = TestApiServer::new();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p-explicit"},
        "spec": {"containers": [{
            "name": "app",
            "image": "nginx",
            "resources": {
                "limits": {"cpu": "500m", "memory": "128Mi"},
                "requests": {"cpu": "100m"}
            }
        }]}
    });
    let (code, body) = state.post(&pods_uri(), &pod).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    let requests = &body["spec"]["containers"][0]["resources"]["requests"];
    assert_eq!(requests["cpu"], json!("100m"), "explicit cpu kept: {body}");
    assert_eq!(
        requests["memory"],
        json!("128Mi"),
        "absent memory filled from limit: {body}"
    );
}

/// Deployment/ReplicaSet/etc. pod *templates* must NOT be defaulted — upstream
/// keeps this pass off `SetDefaults_PodSpec` precisely so it never touches a
/// PodTemplateSpec (defaults.go:165-167). Defaulting templates would also change
/// the bytes a ControllerRevision hashes.
#[tokio::test]
async fn pod_templates_are_not_defaulted() {
    let state = TestApiServer::new();
    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "d-tmpl"},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "x"}},
            "template": {
                "metadata": {"labels": {"app": "x"}},
                "spec": {"containers": [{
                    "name": "app",
                    "image": "nginx",
                    "resources": {"limits": {"cpu": "500m"}}
                }]}
            }
        }
    });
    let (code, body) = state
        .post(
            &format!("/apis/apps/v1/namespaces/{NS}/deployments"),
            &deploy,
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    assert!(
        body["spec"]["template"]["spec"]["containers"][0]["resources"]["requests"].is_null(),
        "template requests must stay unset: {body}"
    );
}
