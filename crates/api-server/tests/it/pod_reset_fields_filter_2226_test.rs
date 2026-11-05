//! The field manager honours `GetResetFieldsFilter` (#2226).
//!
//! Upstream: `rest.ResetFieldsFilterStrategy` (apiserver/pkg/registry/rest/
//! rest.go:391-394), handed to the field manager as `merge.Updater.IgnoreFilter`
//! by `endpoints/installer.go:697-711`; `podEphemeralContainersStrategy`
//! (pkg/registry/core/pod/strategy.go:333-340) and `podResizeStrategy`
//! (:437-445) provide a `fieldpath.NewIncludeMatcherFilter`. A manager that
//! writes through `/ephemeralcontainers` owns `spec.ephemeralContainers` and
//! one that writes through `/resize` owns the containers' `resources` and
//! `resizePolicy`, whatever else its request body carried.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PODS: &str = "/api/v1/namespaces/default/pods";
const APPLY: &str = "application/apply-patch+yaml";

fn burstable_pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{
            "name": "c", "image": "busybox",
            "resources": {"requests": {"cpu": "100m"}}
        }]}
    })
}

async fn create(api: &TestApiServer, body: &Value) {
    let (s, created) = api.post(PODS, body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
}

fn fields_of(out: &Value, manager: &str, subresource: &str) -> String {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| {
            a.iter()
                .find(|e| e["manager"] == manager && e["subresource"] == subresource)
        })
        .map(|e| e["fieldsV1"].to_string())
        .unwrap_or_default()
}

/// A manager updating through `/resize` owns `resources`, not the `image` its
/// body also changed (the strategy drops it from the stored object).
#[tokio::test]
async fn resize_update_owns_only_resources_and_resize_policy() {
    let api = TestApiServer::new();
    create(&api, &burstable_pod("p1")).await;
    let (_, mut update) = api.get(&format!("{PODS}/p1")).await;
    update["spec"]["containers"][0]["resources"]["requests"]["cpu"] = json!("200m");
    update["spec"]["containers"][0]["image"] = json!("busybox:2");
    let (s, out) = api
        .put(&format!("{PODS}/p1/resize?fieldManager=resizer"), &update)
        .await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let owned = fields_of(&out, "resizer", "resize");
    assert!(owned.contains("f:resources"), "{owned}");
    assert!(!owned.contains("f:image"), "{owned}");
}

/// A manager updating through `/ephemeralcontainers` owns
/// `spec.ephemeralContainers`, not the label its body also changed.
#[tokio::test]
async fn ephemeral_containers_update_owns_only_ephemeral_containers() {
    let api = TestApiServer::new();
    create(&api, &burstable_pod("p2")).await;
    let (_, mut update) = api.get(&format!("{PODS}/p2")).await;
    update["metadata"]["labels"] = json!({"a": "b"});
    update["spec"]["ephemeralContainers"] = json!([{"name": "debug", "image": "busybox"}]);
    let (s, out) = api
        .put(
            &format!("{PODS}/p2/ephemeralcontainers?fieldManager=debugger"),
            &update,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let owned = fields_of(&out, "debugger", "ephemeralcontainers");
    assert!(owned.contains("f:ephemeralContainers"), "{owned}");
    assert!(!owned.contains("f:labels"), "{owned}");
}

/// Applying through `/resize` owns `resources` only; the applier's other
/// fields (`image`, the container `name` key) are not its.
#[tokio::test]
async fn resize_apply_owns_only_resources() {
    let api = TestApiServer::new();
    create(&api, &burstable_pod("p3")).await;
    let body = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": "p3"},
        "spec": {"containers": [{
            "name": "c", "image": "busybox",
            "resources": {"requests": {"cpu": "300m"}}
        }]}
    });
    let (s, out) = api
        .send(
            "PATCH",
            &format!("{PODS}/p3/resize?fieldManager=applier&force=true"),
            Some(APPLY),
            Some(&body),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{out}");
    let owned = fields_of(&out, "applier", "resize");
    assert!(owned.contains("f:resources"), "{owned}");
    assert!(!owned.contains("f:image"), "{owned}");
    assert!(!owned.contains("f:name"), "{owned}");
}
