//! HorizontalPodAutoscaler on the generic Store (#1990): the strategy rules of
//! `pkg/registry/autoscaling/horizontalpodautoscaler/strategy.go` and the
//! autoscaling/v2 defaults of `pkg/apis/autoscaling/v2/defaults.go`.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const BASE: &str = "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers";

fn hpa(name: &str) -> Value {
    json!({"apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
           "metadata": {"name": name},
           "spec": {"scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
                    "maxReplicas": 5}})
}

/// `PrepareForCreate` clears the status (strategy.go:70-76).
#[tokio::test]
async fn create_clears_status() {
    let api = TestApiServer::new();
    let mut obj = hpa("h1");
    obj["status"] = json!({"currentReplicas": 3, "desiredReplicas": 4});
    let (s, body) = api.post(BASE, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["status"]["currentReplicas"], 0, "{body}");
    assert_eq!(body["status"]["desiredReplicas"], 0, "{body}");
}

/// `SetDefaults_HorizontalPodAutoscaler` (v2/defaults.go): minReplicas 1 and a
/// 80% CPU utilization metric when none is given.
#[tokio::test]
async fn a_default_cpu_metric_is_filled_in() {
    let api = TestApiServer::new();
    let (s, body) = api.post(BASE, &hpa("h1")).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["minReplicas"], 1, "{body}");
    let m = &body["spec"]["metrics"][0];
    assert_eq!(m["type"], "Resource", "{body}");
    assert_eq!(m["resource"]["name"], "cpu");
    assert_eq!(m["resource"]["target"]["type"], "Utilization");
    assert_eq!(m["resource"]["target"]["averageUtilization"], 80);
}

/// `SetDefaults_HorizontalPodAutoscalerBehavior`: a behavior with one rule
/// gets both directions fully populated, and keeps what the user set.
#[tokio::test]
async fn behavior_rules_are_defaulted() {
    let api = TestApiServer::new();
    let mut obj = hpa("h1");
    obj["spec"]["behavior"] = json!({"scaleUp": {"stabilizationWindowSeconds": 30}});
    let (s, body) = api.post(BASE, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    let up = &body["spec"]["behavior"]["scaleUp"];
    assert_eq!(up["stabilizationWindowSeconds"], 30, "{body}");
    assert_eq!(up["selectPolicy"], "Max");
    assert_eq!(up["policies"].as_array().unwrap().len(), 2);
    let down = &body["spec"]["behavior"]["scaleDown"];
    assert_eq!(down["selectPolicy"], "Max", "{body}");
    assert_eq!(down["policies"][0]["type"], "Percent");
    assert_eq!(down["policies"][0]["value"], 100);
    assert!(down.get("stabilizationWindowSeconds").is_none(), "{body}");
}

/// `PrepareForUpdate` keeps the old status (strategy.go:101-108).
#[tokio::test]
async fn update_cannot_set_status() {
    let api = TestApiServer::new();
    let (s, created) = api.post(BASE, &hpa("h1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["status"] = json!({"currentReplicas": 9, "desiredReplicas": 9});
    let (s, body) = api.put(&format!("{BASE}/h1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["currentReplicas"], 0, "{body}");
}

/// `AllowUnconditionalUpdate() == true` (strategy.go:116): a PUT without a
/// resourceVersion is accepted.
#[tokio::test]
async fn a_put_without_resource_version_is_accepted() {
    let api = TestApiServer::new();
    let (s, created) = api.post(BASE, &hpa("h1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("resourceVersion");
    update["spec"]["maxReplicas"] = json!(7);
    let (s, body) = api.put(&format!("{BASE}/h1"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["spec"]["maxReplicas"], 7);
}

/// `AllowCreateOnUpdate() == false`: a PUT to a missing HPA is a 404.
#[tokio::test]
async fn a_put_to_a_missing_hpa_is_not_found() {
    let api = TestApiServer::new();
    let (s, body) = api.put(&format!("{BASE}/missing"), &hpa("missing")).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{body}");
}

/// `ValidateHorizontalPodAutoscaler` runs `NameIsDNSSubdomain` on the name.
#[tokio::test]
async fn an_invalid_name_is_rejected() {
    let api = TestApiServer::new();
    let (s, body) = api.post(BASE, &hpa("Bad_Name")).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"].as_str().unwrap().contains("metadata.name"));
}

/// `autoscalerStatusStrategy.PrepareForUpdate` resets the spec
/// (strategy.go:150-155); the status is written.
#[tokio::test]
async fn status_update_writes_status_and_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(BASE, &hpa("h1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["spec"]["maxReplicas"] = json!(99);
    update["status"] = json!({"currentReplicas": 2, "desiredReplicas": 3});
    let (s, body) = api.put(&format!("{BASE}/h1/status"), &update).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["desiredReplicas"], 3, "{body}");
    assert_eq!(body["spec"]["maxReplicas"], 5, "{body}");
}

/// `ValidateHorizontalPodAutoscalerStatusUpdate`: counts are non-negative.
#[tokio::test]
async fn status_update_rejects_negative_replicas() {
    let api = TestApiServer::new();
    let (s, created) = api.post(BASE, &hpa("h1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut update = created.clone();
    update["status"] = json!({"currentReplicas": -1, "desiredReplicas": 0});
    let (s, body) = api.put(&format!("{BASE}/h1/status"), &update).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("status.currentReplicas"));
}

/// A PATCH of `/status` goes through the status strategy too.
#[tokio::test]
async fn status_patch_keeps_spec() {
    let api = TestApiServer::new();
    let (s, created) = api.post(BASE, &hpa("h1")).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let patch = json!({"spec": {"maxReplicas": 99}, "status": {"desiredReplicas": 4}});
    let (s, body) = api.patch(&format!("{BASE}/h1/status"), &patch).await;
    assert_eq!(s, StatusCode::OK, "{body}");
    assert_eq!(body["status"]["desiredReplicas"], 4, "{body}");
    assert_eq!(body["spec"]["maxReplicas"], 5, "{body}");
}
