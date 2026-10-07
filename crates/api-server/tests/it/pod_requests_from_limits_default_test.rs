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

// ---- Pod-level resources (PodLevelResources, Beta/on in 1.35) ---------------
//
// `SetDefaults_Pod` continues with `defaultHugePagePodLimits` and
// `defaultPodRequests` once the container-level pass is done
// (`pkg/apis/core/v1/defaults.go:194-199`); cases are ported from
// `TestPodResourcesDefaults` (`pkg/apis/core/v1/defaults_test.go:378`).

/// "pod requests=empty map limits=set, container requests=unset limits=set":
/// pod-level requests default to the aggregated container requests (2m+1m,
/// 1Mi+5Mi), never to the pod limits (5m, 7Mi), which stay untouched.
#[tokio::test]
#[serial_test::serial]
async fn pod_level_requests_default_to_aggregated_container_requests() {
    let _g = rusternetes_common::feature_gates::with_feature(
        rusternetes_common::feature_gates::Feature::PodLevelResources,
        true,
    );
    let state = TestApiServer::new();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p-podlevel"},
        "spec": {
            "resources": {"limits": {"cpu": "5m", "memory": "7Mi"}, "requests": {}},
            "containers": [
                {"name": "a", "image": "nginx",
                 "resources": {"limits": {"cpu": "2m", "memory": "1Mi"}}},
                {"name": "b", "image": "nginx",
                 "resources": {"limits": {"cpu": "1m", "memory": "5Mi"}}}
            ]
        }
    });
    let (code, body) = state.post(&pods_uri(), &pod).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    let res = &body["spec"]["resources"];
    assert_eq!(res["requests"]["cpu"], json!("3m"), "{body}");
    assert_eq!(res["requests"]["memory"], json!("6Mi"), "{body}");
    assert_eq!(
        res["limits"]["cpu"],
        json!("5m"),
        "limits untouched: {body}"
    );
    assert_eq!(res["limits"]["memory"], json!("7Mi"), "{body}");
}

/// "pod requests=unset limits=set, container resources=unset": the pod-level
/// request falls back to the pod-level limit.
#[tokio::test]
#[serial_test::serial]
async fn pod_level_requests_default_to_pod_limits_when_containers_have_none() {
    let _g = rusternetes_common::feature_gates::with_feature(
        rusternetes_common::feature_gates::Feature::PodLevelResources,
        true,
    );
    let state = TestApiServer::new();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p-podlevel-lim"},
        "spec": {
            "resources": {"limits": {"cpu": "2m", "memory": "1Mi"}},
            "containers": [{"name": "a", "image": "nginx"}]
        }
    });
    let (code, body) = state.post(&pods_uri(), &pod).await;
    assert_eq!(code, StatusCode::CREATED, "create must succeed: {body}");
    let res = &body["spec"]["resources"];
    assert_eq!(res["requests"]["cpu"], json!("2m"), "{body}");
    assert_eq!(res["requests"]["memory"], json!("1Mi"), "{body}");
}

/// Gate off: pod-level resources are left exactly as submitted.
#[tokio::test]
#[serial_test::serial]
async fn pod_level_requests_not_defaulted_when_gate_off() {
    let _g = rusternetes_common::feature_gates::with_feature(
        rusternetes_common::feature_gates::Feature::PodLevelResources,
        false,
    );
    let state = TestApiServer::new();
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p-podlevel-off"},
        "spec": {
            "resources": {"limits": {"cpu": "2m"}},
            "containers": [{"name": "a", "image": "nginx"}]
        }
    });
    let (_code, body) = state.post(&pods_uri(), &pod).await;
    assert!(
        body["spec"]["resources"]["requests"].is_null(),
        "gate off must not default pod requests: {body}"
    );
}
