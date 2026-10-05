//! Pod strategy pieces of `pkg/registry/core/pod/strategy.go` (#2119):
//! `applySchedulingGatedCondition` (:929), `podutil.DropDisabledPodFields`
//! (pkg/api/pod/util.go:697) and `GetWarningsForPod` (pkg/api/pod/warnings.go:38).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const PODS: &str = "/api/v1/namespaces/default/pods";

fn pod(name: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    })
}

/// strategy.go:929-947: a new gated pod is `PodScheduled=False` with reason
/// `SchedulingGated`.
#[tokio::test]
async fn gated_pod_gets_the_scheduling_gated_condition() {
    let api = TestApiServer::new();
    let mut body = pod("gated");
    body["spec"]["schedulingGates"] = json!([{"name": "example.com/gate"}]);
    let (s, created) = api.post(PODS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let cond = &created["status"]["conditions"][0];
    assert_eq!(cond["type"], "PodScheduled", "{created}");
    assert_eq!(cond["status"], "False", "{created}");
    assert_eq!(cond["reason"], "SchedulingGated", "{created}");
    assert_eq!(
        cond["message"],
        "Scheduling is blocked due to non-empty scheduling gates"
    );
}

#[tokio::test]
async fn ungated_pod_has_no_condition() {
    let api = TestApiServer::new();
    let (s, created) = api.post(PODS, &pod("plain")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert!(created["status"].get("conditions").is_none(), "{created}");
}

/// util.go:1836 `dropDisabledWorkloadRef`: `GenericWorkload` is Alpha and off
/// in 1.35, so a new pod loses `spec.workloadRef`.
#[tokio::test]
async fn workload_ref_is_dropped_while_generic_workload_is_off() {
    let api = TestApiServer::new();
    let mut body = pod("wl");
    body["spec"]["workloadRef"] = json!({"name": "w", "podGroup": "g"});
    let (s, created) = api.post(PODS, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    assert!(created["spec"].get("workloadRef").is_none(), "{created}");
}

/// warnings.go: a duplicate imagePullSecret and a null topology labelSelector
/// each draw a `Warning:` header on create.
#[tokio::test]
async fn create_returns_get_warnings_for_pod() {
    let api = TestApiServer::new();
    let mut body = pod("warn");
    body["spec"]["imagePullSecrets"] = json!([{"name": "a"}, {"name": "a"}]);
    let (s, headers, _, v) = api
        .send_full(
            "POST",
            PODS,
            Some("application/json"),
            None,
            Some(serde_json::to_vec(&body).unwrap()),
        )
        .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let warnings: Vec<String> = headers
        .get_all("warning")
        .iter()
        .map(|h| h.to_str().unwrap().to_string())
        .collect();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("spec.imagePullSecrets[1].name: duplicate name")),
        "{warnings:?}"
    );
}
