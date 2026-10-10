//! Server-side apply honours the strategy's `GetResetFields` (#2138).
//!
//! Upstream: `rest.ResetFieldsStrategy` (apiserver/pkg/registry/rest/rest.go:385),
//! handed to the field manager as `merge.Updater.IgnoreFilter` by
//! `endpoints/installer.go:697-722`. `Updater.Apply` filters the applier's
//! field set through it (structured-merge-diff `merge/update.go` Apply, :234)
//! and `Updater.update` filters the comparison used for conflicts (:92, :127),
//! so an apply on one endpoint neither owns nor conflicts on the fields the
//! other endpoint resets. ServiceCIDR's sets:
//! `pkg/registry/networking/servicecidr/strategy.go:55-66` (`status` on the
//! main resource) and `:129-139` (`spec` on `/status`).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const SC: &str = "/apis/networking.k8s.io/v1/servicecidrs";
const APPLY: &str = "application/apply-patch+yaml";

fn conditions(message: &str) -> Value {
    json!([{
        "type": "Ready", "status": "True",
        "lastTransitionTime": "2020-01-01T00:00:00Z",
        "reason": "RuleApplied", "message": message
    }])
}

fn body(name: &str, extra: Value) -> Value {
    let mut b = json!({
        "apiVersion": "networking.k8s.io/v1", "kind": "ServiceCIDR",
        "metadata": {"name": name}
    });
    for (k, v) in extra.as_object().unwrap() {
        b[k] = v.clone();
    }
    b
}

fn owned_keys(out: &Value, manager: &str) -> Vec<String> {
    out["metadata"]["managedFields"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["manager"] == manager))
        .and_then(|e| e["fieldsV1"].as_object())
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default()
}

async fn apply(api: &TestApiServer, uri: &str, manager: &str, b: &Value) -> (StatusCode, Value) {
    api.send(
        "PATCH",
        &format!("{uri}?fieldManager={manager}"),
        Some(APPLY),
        Some(b),
    )
    .await
}

/// The main resource resets `status`: applying `spec` + `status` owns `spec`
/// only. On create (the `createNewObject` path) as well as on update.
#[tokio::test]
async fn apply_on_main_resource_does_not_own_status() {
    let api = TestApiServer::new();
    let b = body(
        "rf-main",
        json!({"spec": {"cidrs": ["10.50.0.0/24"]}, "status": {"conditions": conditions("a")}}),
    );
    let (code, out) = apply(&api, &format!("{SC}/rf-main"), "user", &b).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert!(
        owned_keys(&out, "user").contains(&"f:spec".to_string()),
        "{out}"
    );
    assert!(
        !owned_keys(&out, "user").contains(&"f:status".to_string()),
        "{out}"
    );

    let (code, out) = apply(&api, &format!("{SC}/rf-main"), "user2", &b).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert!(
        !owned_keys(&out, "user2").contains(&"f:status".to_string()),
        "{out}"
    );
}

/// `/status` resets `spec`: it owns `status` only.
#[tokio::test]
async fn apply_on_status_does_not_own_spec() {
    let api = TestApiServer::new();
    let (code, out) = api
        .post(
            SC,
            &body("rf-status", json!({"spec": {"cidrs": ["10.51.0.0/24"]}})),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{out}");

    let b = body(
        "rf-status",
        json!({"spec": {"cidrs": ["10.51.0.0/24"]}, "status": {"conditions": conditions("a")}}),
    );
    let (code, out) = apply(&api, &format!("{SC}/rf-status/status"), "ctrl", &b).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert!(
        owned_keys(&out, "ctrl").contains(&"f:status".to_string()),
        "{out}"
    );
    assert!(
        !owned_keys(&out, "ctrl").contains(&"f:spec".to_string()),
        "{out}"
    );
}

/// A controller owns `status` through `/status`; a user applying `status`
/// through the main resource neither conflicts nor takes it over.
#[tokio::test]
async fn reset_fields_do_not_conflict_across_endpoints() {
    let api = TestApiServer::new();
    let (code, out) = api
        .post(
            SC,
            &body("rf-conflict", json!({"spec": {"cidrs": ["10.52.0.0/24"]}})),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{out}");

    let status = body(
        "rf-conflict",
        json!({"status": {"conditions": conditions("ctrl")}}),
    );
    let (code, out) = apply(&api, &format!("{SC}/rf-conflict/status"), "ctrl", &status).await;
    assert_eq!(code, StatusCode::OK, "{out}");

    let user = body(
        "rf-conflict",
        json!({"spec": {"cidrs": ["10.52.0.0/24"]}, "status": {"conditions": conditions("user")}}),
    );
    let (code, out) = apply(&api, &format!("{SC}/rf-conflict"), "user", &user).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert!(
        owned_keys(&out, "ctrl").contains(&"f:status".to_string()),
        "{out}"
    );
    assert!(
        !owned_keys(&out, "user").contains(&"f:status".to_string()),
        "{out}"
    );
}

/// Deployment (`pkg/registry/apps/deployment/strategy.go:63-71`) resets
/// `status` on the main resource.
#[tokio::test]
async fn deployment_apply_does_not_own_status() {
    let api = TestApiServer::new();
    let b = json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "rf-d", "namespace": "default"},
        "spec": {
            "selector": {"matchLabels": {"app": "rf"}},
            "template": {
                "metadata": {"labels": {"app": "rf"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        },
        "status": {"replicas": 1}
    });
    let uri = "/apis/apps/v1/namespaces/default/deployments/rf-d";
    let (code, out) = apply(&api, uri, "user", &b).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert!(
        owned_keys(&out, "user").contains(&"f:spec".to_string()),
        "{out}"
    );
    assert!(
        !owned_keys(&out, "user").contains(&"f:status".to_string()),
        "{out}"
    );
}
