//! ReplicationController, `replicationcontrollers/status` and
//! `replicationcontrollers/scale` served through the generic Store and
//! endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin ReplicationController's own:
//! `pkg/registry/core/replicationcontroller/strategy.go`, its `StatusREST`
//! and `ScaleREST` (storage/storage.go) and the core validators.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const RCS: &str = "/api/v1/namespaces/default/replicationcontrollers";

fn rc(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "ReplicationController",
        "metadata": {"name": name},
        "spec": {
            "replicas": 2,
            "selector": {"app": name},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(RCS, &rc(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:93-100) drops a client-set status and
/// starts the generation at 1.
#[tokio::test]
async fn create_resets_status_and_generation() {
    let api = TestApiServer::new();
    let mut body = rc("rc-create");
    body["status"] = json!({"replicas": 7, "observedGeneration": 9});
    body["metadata"]["generation"] = json!(42);
    let (status, out) = api.post(RCS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"]["replicas"], 0, "{out}");
    assert!(out["status"].get("observedGeneration").is_none(), "{out}");
}

/// `PrepareForUpdate` (strategy.go:103-122): a spec change bumps the
/// generation, an annotation change does not, and status is kept.
#[tokio::test]
async fn update_bumps_generation_on_spec_only() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "rc-update").await;
    let uri = format!("{RCS}/rc-update");

    obj["metadata"]["annotations"] = json!({"a": "1"});
    obj["status"] = json!({"replicas": 5});
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"]["replicas"], 0, "{out}");

    let mut obj = out;
    obj["spec"]["replicas"] = json!(4);
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
}

/// `ValidateReplicationControllerUpdate` (validation.go:6979-6983) holds a
/// PATCH to the pod template rules.
#[tokio::test]
async fn a_patch_is_validated() {
    let api = TestApiServer::new();
    create(&api, "rc-patch").await;
    let (status, out) = api
        .patch(
            &format!("{RCS}/rc-patch"),
            &json!({"spec": {"template": {"spec": {"containers": [
                {"name": "Bad_Name", "image": "nginx"}
            ]}}}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("spec.template.spec.containers[0].name"),
        "{out}"
    );
}

/// `ValidateUpdate` (strategy.go:156-182): a selector recorded as
/// non-convertible may not change.
#[tokio::test]
async fn a_non_convertible_selector_cannot_change() {
    let api = TestApiServer::new();
    let mut body = rc("rc-nc");
    body["metadata"]["annotations"] =
        json!({"non-convertible.kubernetes.io/selector": "app in (a, b)"});
    let (status, obj) = api.post(RCS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{obj}");
    let mut obj = obj;
    obj["spec"]["selector"] = json!({"app": "rc-nc", "x": "y"});
    obj["spec"]["template"]["metadata"]["labels"] = json!({"app": "rc-nc", "x": "y"});
    let (status, out) = api.put(&format!("{RCS}/rc-nc"), &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("cannot update non-convertible selector"),
        "{out}"
    );
}

/// `rcStatusStrategy.PrepareForUpdate` (strategy.go:244-249): a status PUT
/// writes status and keeps the spec.
#[tokio::test]
async fn status_put_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "rc-status").await;
    obj["spec"]["replicas"] = json!(9);
    obj["status"] = json!({"replicas": 2, "readyReplicas": 1, "availableReplicas": 1});
    let (status, out) = api.put(&format!("{RCS}/rc-status/status"), &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 2, "{out}");
    assert_eq!(out["status"]["replicas"], 2, "{out}");
    assert_eq!(out["status"]["readyReplicas"], 1, "{out}");
}

/// `ValidateReplicationControllerStatus` (validation.go:6992-7014) runs on
/// `/status`, for a PATCH too.
#[tokio::test]
async fn status_writes_validate_the_status() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "rc-badstatus").await;
    let uri = format!("{RCS}/rc-badstatus/status");
    obj["status"] = json!({"replicas": 1, "readyReplicas": 3});
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("cannot be greater than status.replicas"),
        "{out}"
    );

    let (status, out) = api
        .patch(
            &uri,
            &json!({"status": {"replicas": 2, "readyReplicas": 1, "availableReplicas": 2}}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("cannot be greater than readyReplicas"),
        "{out}"
    );
}

/// `scaleFromRC` (storage/storage.go:239-257) and `ScaleREST.Update`: a
/// scale is a spec change, so it bumps the generation.
#[tokio::test]
async fn scale_reads_and_writes_replicas() {
    let api = TestApiServer::new();
    create(&api, "rc-scale").await;
    let uri = format!("{RCS}/rc-scale/scale");
    let (status, mut scale) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{scale}");
    assert_eq!(scale["kind"], "Scale", "{scale}");
    assert_eq!(scale["spec"]["replicas"], 2, "{scale}");
    assert_eq!(scale["status"]["selector"], "app=rc-scale", "{scale}");

    scale["spec"]["replicas"] = json!(6);
    let (status, out) = api.put(&uri, &scale).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 6, "{out}");

    let (_, obj) = api.get(&format!("{RCS}/rc-scale")).await;
    assert_eq!(obj["spec"]["replicas"], 6, "{obj}");
    assert_eq!(obj["metadata"]["generation"], 2, "{obj}");

    let (status, out) = api.patch(&uri, &json!({"spec": {"replicas": -1}})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");

    let (status, out) = api.get(&format!("{RCS}/nope/scale")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    assert_eq!(out["message"], "replicationcontrollers \"nope\" not found");
}

/// v1 orphans by default (strategy.go:59-73): a DELETE without a policy
/// leaves the RC behind an `orphan` finalizer.
#[tokio::test]
async fn delete_orphans_by_default() {
    let api = TestApiServer::new();
    create(&api, "rc-orphan").await;
    let uri = format!("{RCS}/rc-orphan");
    let (status, out) = api.delete(&uri).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    let (status, got) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["metadata"]["finalizers"], json!(["orphan"]), "{got}");
    assert!(got["metadata"]["deletionTimestamp"].is_string(), "{got}");
}

/// The cluster-wide collection serves only LIST and WATCH
/// (apiserver/pkg/endpoints/installer.go:589-596).
#[tokio::test]
async fn the_cluster_wide_collection_has_no_deletecollection() {
    let api = TestApiServer::new();
    create(&api, "rc-all").await;
    let (status, _) = api.delete("/api/v1/replicationcontrollers").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let (status, _) = api.get(&format!("{RCS}/rc-all")).await;
    assert_eq!(status, StatusCode::OK);
}
