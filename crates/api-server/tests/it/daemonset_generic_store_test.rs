//! DaemonSet and `daemonsets/status` served through the generic Store and
//! endpoint handlers (#1990).
//!
//! The rules every Store-backed resource shares are pinned by
//! `configmap_generic_store_test`. These pin DaemonSet's own:
//! `pkg/registry/apps/daemonset/strategy.go`, its `StatusREST`
//! (storage/storage.go), the apps validators, and the
//! `deprecated.daemonset.template.generation` annotation the v1 conversion
//! carries the internal `spec.templateGeneration` in
//! (pkg/apis/apps/v1/conversion.go:83-115).

use axum::http::StatusCode;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const DS: &str = "/apis/apps/v1/namespaces/default/daemonsets";
const TEMPLATE_GENERATION: &str = "deprecated.daemonset.template.generation";

fn ds(name: &str) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "DaemonSet",
        "metadata": {"name": name},
        "spec": {
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    })
}

async fn create(api: &TestApiServer, name: &str) -> Value {
    let (status, body) = api.post(DS, &ds(name)).await;
    assert_eq!(status, StatusCode::CREATED, "create {name}: {body}");
    body
}

fn message(body: &Value) -> &str {
    body["message"].as_str().unwrap_or_default()
}

fn template_generation(body: &Value) -> &Value {
    &body["metadata"]["annotations"][TEMPLATE_GENERATION]
}

/// `PrepareForCreate` (strategy.go:70-80) drops a client-set status, starts
/// the generation at 1 and the template generation at 1.
#[tokio::test]
async fn create_resets_status_and_seeds_both_generations() {
    let api = TestApiServer::new();
    let mut body = ds("d-create");
    body["status"] = json!({"desiredNumberScheduled": 7, "numberReady": 3});
    body["metadata"]["generation"] = json!(42);
    let (status, out) = api.post(DS, &body).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(template_generation(&out), "1", "{out}");
    assert_eq!(
        out["status"],
        json!({
            "currentNumberScheduled": 0, "numberMisscheduled": 0,
            "desiredNumberScheduled": 0, "numberReady": 0
        }),
        "{out}"
    );
}

/// `ValidateDaemonSetName` is `NameIsDNSSubdomain` (validation.go:540),
/// so a dotted name is fine where a StatefulSet's would not be.
#[tokio::test]
async fn names_are_dns_subdomains() {
    let api = TestApiServer::new();
    let (status, out) = api.post(DS, &ds("d.dotted")).await;
    assert_eq!(status, StatusCode::CREATED, "{out}");
    let (status, out) = api.post(DS, &ds("D_bad")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("metadata.name"), "{out}");
}

/// `PrepareForUpdate` (strategy.go:83-114). Status and the template
/// generation are the server's. A template change bumps both generations,
/// and any other spec change bumps only `metadata.generation`.
#[tokio::test]
async fn update_bumps_generations_by_what_changed() {
    let api = TestApiServer::new();
    let obj = create(&api, "d-update").await;
    let uri = format!("{DS}/d-update");

    // Metadata only, with a forged template generation and status.
    let mut touched = obj.clone();
    touched["metadata"]["annotations"] = json!({"a": "1", TEMPLATE_GENERATION: "9"});
    touched["status"] = json!({"numberReady": 5});
    let (status, out) = api.put(&uri, &touched).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");
    assert_eq!(template_generation(&out), "1", "{out}");
    assert_eq!(out["status"]["numberReady"], 0, "{out}");

    // A spec change outside the template.
    let mut slower = out;
    slower["spec"]["minReadySeconds"] = json!(5);
    let (status, out) = api.put(&uri, &slower).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
    assert_eq!(template_generation(&out), "1", "{out}");

    // A template change.
    let mut reimaged = out;
    reimaged["spec"]["template"]["spec"]["containers"][0]["image"] = json!("nginx:2");
    let (status, out) = api.put(&uri, &reimaged).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 3, "{out}");
    assert_eq!(template_generation(&out), "2", "{out}");
}

/// A PATCH goes through the same strategy as a PUT. It used to skip update
/// validation and the generation bump.
#[tokio::test]
async fn patch_is_validated_and_bumps_generations() {
    let api = TestApiServer::new();
    create(&api, "d-patch").await;
    let uri = format!("{DS}/d-patch");

    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx:3"}]}}}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["generation"], 2, "{out}");
    assert_eq!(template_generation(&out), "2", "{out}");

    // `ValidateDaemonSetSpecUpdate` (validation.go:402-403): the selector is
    // immutable.
    let (status, out) = api
        .patch(
            &uri,
            &json!({"spec": {
                "selector": {"matchLabels": {"app": "d-patch", "x": "y"}},
                "template": {"metadata": {"labels": {"app": "d-patch", "x": "y"}}}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("spec.selector"), "{out}");
}

/// The storage codec defaults what it decodes, so an object stored without
/// defaults still accepts a metadata-only update and comes back defaulted.
#[tokio::test]
async fn an_undefaulted_stored_object_accepts_a_metadata_update() {
    let api = TestApiServer::new();
    let mut stored = ds("d-raw");
    stored["metadata"]["namespace"] = json!("default");
    let key = build_key("daemonsets", Some("default"), "d-raw");
    let stored: Value = api.storage.create(&key, &stored).await.unwrap();

    let mut update = stored.clone();
    update["metadata"]["annotations"] = json!({"touched": "true"});
    let (status, out) = api.put(&format!("{DS}/d-raw"), &update).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert_eq!(out["metadata"]["annotations"]["touched"], "true", "{out}");
    assert_eq!(
        out["spec"]["updateStrategy"]["type"], "RollingUpdate",
        "{out}"
    );
}

/// `daemonSetStatusStrategy.PrepareForUpdate` (strategy.go:183-187) and
/// `ValidateDaemonSetStatusUpdate` (validation.go:426-438).
#[tokio::test]
async fn status_put_writes_status_keeps_spec_and_validates() {
    let api = TestApiServer::new();
    let mut obj = create(&api, "d-status").await;
    obj["spec"]["minReadySeconds"] = json!(9);
    obj["status"] = json!({
        "currentNumberScheduled": 2, "numberMisscheduled": 0,
        "desiredNumberScheduled": 2, "numberReady": 1, "collisionCount": 2
    });
    let uri = format!("{DS}/d-status/status");
    let (status, out) = api.put(&uri, &obj).await;
    assert_eq!(status, StatusCode::OK, "{out}");
    assert!(out["spec"].get("minReadySeconds").is_none(), "{out}");
    assert_eq!(out["status"]["numberReady"], 1, "{out}");
    assert_eq!(out["metadata"]["generation"], 1, "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"status": {"numberReady": -1}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("status.numberReady"), "{out}");

    let (status, out) = api
        .patch(&uri, &json!({"status": {"collisionCount": 1}}))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(message(&out).contains("cannot be decremented"), "{out}");
}

/// Upstream serves `daemonsets` and `daemonsets/status` only
/// (pkg/registry/apps/rest/storage_apps.go:77-84). A DaemonSet has no
/// replicas, so there is no `/scale`.
#[tokio::test]
async fn there_is_no_scale_subresource() {
    let api = TestApiServer::new();
    create(&api, "d-scale").await;
    let (status, out) = api.get(&format!("{DS}/d-scale/scale")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{out}");
}
