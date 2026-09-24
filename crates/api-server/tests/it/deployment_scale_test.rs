//! `deployments/{name}/scale` served by `ScaleREST` over the Deployment Store
//! (#1995): pkg/registry/apps/deployment/storage/storage.go:283-480.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DEPLOYS: &str = "/apis/apps/v1/namespaces/default/deployments";

fn scale_uri(name: &str) -> String {
    format!("{DEPLOYS}/{name}/scale")
}

fn deploy(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name},
        "spec": {
            "replicas": 2,
            "selector": {
                "matchLabels": {"app": name},
                "matchExpressions": [
                    {"key": "tier", "operator": "In", "values": ["web", "api"]}
                ]
            },
            "template": {
                "metadata": {"labels": {"app": name, "tier": "web"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(DEPLOYS, &deploy(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

/// `scaleFromDeployment` (storage.go:370-393): the parent's identity, its
/// `spec.replicas` / `status.replicas`, and the selector rendered by
/// `LabelSelectorAsSelector(..).String()` — requirements sorted by key,
/// set values sorted.
#[tokio::test]
async fn get_renders_the_deployment_as_a_scale() {
    let api = TestApiServer::new();
    let d = create(&api, "s-get").await;
    let (status, scale) = api.get(&scale_uri("s-get")).await;
    assert_eq!(status, StatusCode::OK, "{scale}");
    assert_eq!(scale["apiVersion"], "autoscaling/v1", "{scale}");
    assert_eq!(scale["kind"], "Scale", "{scale}");
    assert_eq!(scale["metadata"]["name"], "s-get", "{scale}");
    assert_eq!(scale["metadata"]["uid"], d["metadata"]["uid"], "{scale}");
    assert_eq!(
        scale["metadata"]["resourceVersion"], d["metadata"]["resourceVersion"],
        "{scale}"
    );
    assert_eq!(scale["spec"]["replicas"], 2, "{scale}");
    assert_eq!(
        scale["status"]["selector"], "app=s-get,tier in (api,web)",
        "{scale}"
    );
}

/// `ScaleREST.Get` on a missing parent is the Store's NotFound.
#[tokio::test]
async fn get_on_a_missing_deployment_is_a_notfound() {
    let api = TestApiServer::new();
    let (status, body) = api.get(&scale_uri("nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["message"], "deployments.apps \"nope\" not found");
}

/// A scale is a spec change: it runs the Deployment update strategy, so
/// `PrepareForUpdate` bumps the generation (strategy.go:110-128).
#[tokio::test]
async fn put_sets_replicas_and_bumps_the_generation() {
    let api = TestApiServer::new();
    create(&api, "s-put").await;
    let (_, mut scale) = api.get(&scale_uri("s-put")).await;
    scale["spec"]["replicas"] = json!(5);
    let (status, out) = api.put(&scale_uri("s-put"), &scale).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Scale", "{out}");
    assert_eq!(out["spec"]["replicas"], 5, "{out}");

    let (_, d) = api.get(&format!("{DEPLOYS}/s-put")).await;
    assert_eq!(d["spec"]["replicas"], 5, "{d}");
    assert_eq!(d["metadata"]["generation"], 2, "{d}");
}

/// The Scale's `resourceVersion` is copied onto the parent, so a stale one
/// is the storage layer's optimistic-concurrency Conflict.
#[tokio::test]
async fn put_with_a_stale_resource_version_conflicts() {
    let api = TestApiServer::new();
    create(&api, "s-stale").await;
    let (_, stale) = api.get(&scale_uri("s-stale")).await;
    let mut fresh = stale.clone();
    fresh["spec"]["replicas"] = json!(3);
    let (status, out) = api.put(&scale_uri("s-stale"), &fresh).await;
    assert_eq!(status, StatusCode::OK, "{out}");

    let mut stale = stale;
    stale["spec"]["replicas"] = json!(4);
    let (status, out) = api.put(&scale_uri("s-stale"), &stale).await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
}

/// `scaleUpdatedObjectInfo`'s UID precondition (storage.go:460-467).
#[tokio::test]
async fn put_with_a_foreign_uid_conflicts() {
    let api = TestApiServer::new();
    create(&api, "s-uid").await;
    let (_, mut scale) = api.get(&scale_uri("s-uid")).await;
    scale["metadata"]["uid"] = json!("not-the-uid");
    let (status, out) = api.put(&scale_uri("s-uid"), &scale).await;
    assert_eq!(status, StatusCode::CONFLICT, "{out}");
    assert!(
        out["message"]
            .as_str()
            .unwrap_or_default()
            .contains("Precondition failed: UID in precondition: not-the-uid"),
        "{out}"
    );
}

/// `checkName` (endpoints/handlers/rest.go): the body's name must be the
/// URL's.
#[tokio::test]
async fn put_with_a_different_name_is_a_bad_request() {
    let api = TestApiServer::new();
    create(&api, "s-name").await;
    let (_, mut scale) = api.get(&scale_uri("s-name")).await;
    scale["metadata"]["name"] = json!("other");
    let (status, out) = api.put(&scale_uri("s-name"), &scale).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{out}");
}

/// A PATCH is applied to the Scale, not the Deployment, then goes through
/// the same `ScaleREST.Update`.
#[tokio::test]
async fn merge_patch_scales_the_deployment() {
    let api = TestApiServer::new();
    create(&api, "s-patch").await;
    let (status, out) = api
        .patch(&scale_uri("s-patch"), &json!({"spec": {"replicas": 7}}))
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["kind"], "Scale", "{out}");
    assert_eq!(out["spec"]["replicas"], 7, "{out}");
    let (_, d) = api.get(&format!("{DEPLOYS}/s-patch")).await;
    assert_eq!(d["spec"]["replicas"], 7, "{d}");
}

/// `transformResponseObject` serializes for the negotiated media type: the
/// scale client (client-go `scale`) asks for protobuf.
#[tokio::test]
async fn protobuf_accept_gets_a_protobuf_scale() {
    let api = TestApiServer::new();
    create(&api, "s-pb").await;
    let (status, headers, bytes, _) = api
        .send_full(
            "GET",
            &scale_uri("s-pb"),
            None,
            Some("application/vnd.kubernetes.protobuf"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers["content-type"],
        "application/vnd.kubernetes.protobuf"
    );
    assert!(bytes.starts_with(b"k8s\0"), "{bytes:?}");
}
