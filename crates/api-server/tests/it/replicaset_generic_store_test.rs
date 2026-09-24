//! ReplicaSet, `replicasets/status` and `replicasets/scale` served through
//! the generic Store and endpoint handlers (#1990, #1995).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin ReplicaSet's own:
//! `pkg/registry/apps/replicaset/strategy.go`, its `StatusREST` and
//! `ScaleREST` (storage/storage.go) and the apps validators.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const RSS: &str = "/apis/apps/v1/namespaces/default/replicasets";

fn rs(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "ReplicaSet",
        "metadata": {"name": name},
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(RSS, &rs(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

/// `PrepareForCreate` (strategy.go:80-86) drops a client-set status and
/// starts the generation at 1.
#[tokio::test]
async fn create_resets_status_and_generation() {
    let api = TestApiServer::new();
    let mut body = rs("r-create");
    body["status"] = json!({"replicas": 7, "observedGeneration": 9});
    body["metadata"]["generation"] = json!(42);
    let (status, out) = api.post(RSS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(out["status"]["replicas"], 0, "{out}");
    assert!(out["status"].get("observedGeneration").is_none(), "{out}");
}

/// `PrepareForUpdate` (strategy.go:90-109): a spec change bumps the
/// generation, an annotation change does not, and status is kept.
#[tokio::test]
async fn update_bumps_generation_on_spec_only() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "r-update").await;
    let uri = format!("{RSS}/r-update");

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

/// `ValidateReplicaSetUpdate` (validation.go:763-769): the selector is
/// immutable.
#[tokio::test]
async fn update_rejects_a_selector_change() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "r-sel").await;
    obj["spec"]["selector"] = json!({"matchLabels": {"app": "r-sel", "x": "y"}});
    obj["spec"]["template"]["metadata"]["labels"] = json!({"app": "r-sel", "x": "y"});
    let (status, out) = api.put(&format!("{RSS}/r-sel"), &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.selector"), "{out}");
    assert!(message(&out).contains("field is immutable"), "{out}");
}

/// `rsStatusStrategy.PrepareForUpdate` (strategy.go:209-215): a status PUT
/// writes status and keeps the spec.
#[tokio::test]
async fn status_put_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "r-status").await;
    obj["spec"]["replicas"] = json!(9);
    obj["status"] = json!({"replicas": 2, "readyReplicas": 1, "availableReplicas": 1});
    let (status, out) = api.put(&format!("{RSS}/r-status/status"), &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 2, "{out}");
    assert_eq!(out["status"]["replicas"], 2, "{out}");
    assert_eq!(out["status"]["readyReplicas"], 1, "{out}");
}

/// `ValidateReplicaSetStatus` (validation.go:780-804) runs on `/status`.
#[tokio::test]
async fn status_put_validates_the_status() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "r-badstatus").await;
    obj["status"] = json!({"replicas": 1, "readyReplicas": 3});
    let (status, out) = api.put(&format!("{RSS}/r-badstatus/status"), &obj).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        message(&out).contains("cannot be greater than status.replicas"),
        "{out}"
    );
}

/// A merge PATCH of `/status` goes through the status strategy too.
#[tokio::test]
async fn status_patch_writes_status_only() {
    let api = TestApiServer::new();
    create(&api, "r-spatch").await;
    let (status, out) = api
        .patch(
            &format!("{RSS}/r-spatch/status"),
            &json!({"spec": {"replicas": 8}, "status": {"replicas": 1}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 2, "{out}");
    assert_eq!(out["status"]["replicas"], 1, "{out}");
}

/// `scaleFromReplicaSet` (storage.go:266-288) and `ScaleREST.Update`: a
/// scale is a spec change, so it bumps the generation.
#[tokio::test]
async fn scale_reads_and_writes_replicas() {
    let api = TestApiServer::new();
    create(&api, "r-scale").await;
    let uri = format!("{RSS}/r-scale/scale");
    let (status, mut scale) = api.get(&uri).await;
    assert_eq!(status, StatusCode::OK, "{scale}");
    assert_eq!(scale["kind"], "Scale", "{scale}");
    assert_eq!(scale["spec"]["replicas"], 2, "{scale}");
    assert_eq!(scale["status"]["selector"], "app=r-scale", "{scale}");

    scale["spec"]["replicas"] = json!(6);
    let (status, out) = api.put(&uri, &scale).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["spec"]["replicas"], 6, "{out}");

    let (_, obj) = api.get(&format!("{RSS}/r-scale")).await;
    assert_eq!(obj["spec"]["replicas"], 6, "{obj}");
    assert_eq!(obj["metadata"]["generation"], 2, "{obj}");

    let (status, out) = api.patch(&uri, &json!({"spec": {"replicas": -1}})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// `Store.Update` answers NotFound for the parent before
/// `scaleUpdatedObjectInfo` runs (registry/store.go:645-649).
#[tokio::test]
async fn scale_on_a_missing_replicaset_is_a_notfound() {
    let api = TestApiServer::new();
    let (status, out) = api.get(&format!("{RSS}/nope/scale")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
    assert_eq!(out["message"], "replicasets.apps \"nope\" not found");

    let (status, out) = api
        .put(
            &format!("{RSS}/nope/scale"),
            &json!({
                "apiVersion": "autoscaling/v1", "kind": "Scale",
                "metadata": {"name": "nope", "namespace": "default"},
                "spec": {"replicas": 1}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}
