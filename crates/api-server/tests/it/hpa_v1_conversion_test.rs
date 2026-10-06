//! autoscaling/v1 HorizontalPodAutoscaler is converted to and from the stored
//! autoscaling/v2 shape (#2100): `Convert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler`
//! and back (pkg/apis/autoscaling/v1/conversion.go).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const V1: &str = "/apis/autoscaling/v1/namespaces/default/horizontalpodautoscalers";
const V2: &str = "/apis/autoscaling/v2/namespaces/default/horizontalpodautoscalers";
const METRICS: &str = "autoscaling.alpha.kubernetes.io/metrics";

fn v1(name: &str, cpu: Option<i64>) -> Value {
    let mut spec = json!({
        "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"},
        "maxReplicas": 5
    });
    if let Some(c) = cpu {
        spec["targetCPUUtilizationPercentage"] = json!(c);
    }
    json!({"apiVersion": "autoscaling/v1", "kind": "HorizontalPodAutoscaler",
           "metadata": {"name": name}, "spec": spec})
}

/// Convert_v1_HorizontalPodAutoscalerSpec_To_...: the percentage becomes the
/// one CPU utilization metric (conversion.go:474-498).
#[tokio::test]
async fn a_v1_target_cpu_percentage_is_stored_as_a_v2_cpu_metric() {
    let api = TestApiServer::new();
    let (s, body) = api.post(V1, &v1("h1", Some(60))).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["apiVersion"], "autoscaling/v1", "{body}");
    assert_eq!(body["spec"]["targetCPUUtilizationPercentage"], 60, "{body}");
    assert!(body["spec"].get("metrics").is_none(), "{body}");

    let (s, v2) = api.get(&format!("{V2}/h1")).await;
    assert_eq!(s, StatusCode::OK, "{v2}");
    assert_eq!(v2["apiVersion"], "autoscaling/v2", "{v2}");
    let m = &v2["spec"]["metrics"];
    assert_eq!(m.as_array().map(|a| a.len()), Some(1), "{v2}");
    assert_eq!(m[0]["type"], "Resource", "{v2}");
    assert_eq!(m[0]["resource"]["name"], "cpu", "{v2}");
    assert_eq!(m[0]["resource"]["target"]["type"], "Utilization", "{v2}");
    assert_eq!(m[0]["resource"]["target"]["averageUtilization"], 60, "{v2}");
}

/// conversion.go:420-433: with no CPU value and no annotation, v1 gets the
/// implicit 80% default.
#[tokio::test]
async fn a_v1_hpa_without_a_target_gets_the_default_cpu_metric() {
    let api = TestApiServer::new();
    let (s, body) = api.post(V1, &v1("h1", None)).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");
    assert_eq!(body["spec"]["targetCPUUtilizationPercentage"], 80, "{body}");
    assert_eq!(body["spec"]["minReplicas"], 1, "{body}");
}

/// conversion.go:289-300: metrics v1 cannot express travel in the
/// `autoscaling.alpha.kubernetes.io/metrics` annotation, v1-shaped.
#[tokio::test]
async fn other_v2_metrics_travel_in_the_annotation_on_v1() {
    let api = TestApiServer::new();
    let mut obj = v1("h1", None);
    obj["apiVersion"] = json!("autoscaling/v2");
    obj["spec"]["metrics"] = json!([
        {"type": "Resource", "resource": {"name": "cpu", "target": {"type": "Utilization", "averageUtilization": 50}}},
        {"type": "Pods", "pods": {"metric": {"name": "qps"}, "target": {"type": "AverageValue", "averageValue": "100"}}}
    ]);
    let (s, body) = api.post(V2, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{body}");

    let (s, got) = api.get(&format!("{V1}/h1")).await;
    assert_eq!(s, StatusCode::OK, "{got}");
    assert_eq!(got["apiVersion"], "autoscaling/v1", "{got}");
    assert_eq!(got["spec"]["targetCPUUtilizationPercentage"], 50, "{got}");
    assert!(got["spec"].get("metrics").is_none(), "{got}");
    let ann: Value =
        serde_json::from_str(got["metadata"]["annotations"][METRICS].as_str().unwrap()).unwrap();
    assert_eq!(
        ann,
        json!([{"type": "Pods", "pods": {"metricName": "qps", "targetAverageValue": "100"}}]),
        "{got}"
    );

    // A PUT of what GET returned restores the same v2 object.
    let (s, put) = api.put(&format!("{V1}/h1"), &got).await;
    assert_eq!(s, StatusCode::OK, "{put}");
    let (_, v2) = api.get(&format!("{V2}/h1")).await;
    let metrics = v2["spec"]["metrics"].as_array().unwrap();
    assert_eq!(metrics.len(), 2, "{v2}");
    // conversion.go:377-396: the annotation's metrics come first, then the CPU one.
    assert_eq!(metrics[0]["pods"]["metric"]["name"], "qps", "{v2}");
    assert_eq!(
        metrics[1]["resource"]["target"]["averageUtilization"], 50,
        "{v2}"
    );
    assert!(
        v2["metadata"]
            .get("annotations")
            .is_none_or(|a| a.get(METRICS).is_none()),
        "{v2}"
    );
}

/// A PUT through v1 changes the percentage in the stored v2 object.
#[tokio::test]
async fn a_v1_put_updates_the_v2_metric() {
    let api = TestApiServer::new();
    let (s, _) = api.post(V1, &v1("h1", Some(60))).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, put) = api.put(&format!("{V1}/h1"), &v1("h1", Some(70))).await;
    assert_eq!(s, StatusCode::OK, "{put}");
    assert_eq!(put["spec"]["targetCPUUtilizationPercentage"], 70, "{put}");
    let (_, v2) = api.get(&format!("{V2}/h1")).await;
    assert_eq!(
        v2["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"], 70,
        "{v2}"
    );
}

/// A merge patch is applied to the v1 shape, then converted back.
#[tokio::test]
async fn a_v1_merge_patch_reaches_the_v2_metric() {
    let api = TestApiServer::new();
    let (s, _) = api.post(V1, &v1("h1", Some(60))).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, p) = api
        .patch(
            &format!("{V1}/h1"),
            &json!({"spec": {"targetCPUUtilizationPercentage": 75}}),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{p}");
    assert_eq!(p["spec"]["targetCPUUtilizationPercentage"], 75, "{p}");
    let (_, v2) = api.get(&format!("{V2}/h1")).await;
    assert_eq!(
        v2["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"], 75,
        "{v2}"
    );
}

/// A v1 LIST is v1-shaped item by item.
#[tokio::test]
async fn a_v1_list_serves_v1_items() {
    let api = TestApiServer::new();
    api.post(V1, &v1("h1", Some(60))).await;
    let (s, list) = api.get(V1).await;
    assert_eq!(s, StatusCode::OK, "{list}");
    assert_eq!(list["apiVersion"], "autoscaling/v1", "{list}");
    assert_eq!(
        list["items"][0]["spec"]["targetCPUUtilizationPercentage"], 60,
        "{list}"
    );
    assert!(list["items"][0]["spec"].get("metrics").is_none(), "{list}");
}

/// Behavior is not a v1 field; it round-trips through the
/// `autoscaling.alpha.kubernetes.io/behavior` annotation (conversion.go:344-355).
#[tokio::test]
async fn behavior_round_trips_through_v1() {
    let api = TestApiServer::new();
    let mut obj = v1("h1", Some(60));
    obj["apiVersion"] = json!("autoscaling/v2");
    obj["spec"]["behavior"] = json!({"scaleDown": {"stabilizationWindowSeconds": 30}});
    let (s, b) = api.post(V2, &obj).await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let (_, got) = api.get(&format!("{V1}/h1")).await;
    assert!(got["spec"].get("behavior").is_none(), "{got}");
    assert!(
        got["metadata"]["annotations"]["autoscaling.alpha.kubernetes.io/behavior"].is_string(),
        "{got}"
    );
    let (s, put) = api.put(&format!("{V1}/h1"), &got).await;
    assert_eq!(s, StatusCode::OK, "{put}");
    let (_, v2) = api.get(&format!("{V2}/h1")).await;
    assert_eq!(
        v2["spec"]["behavior"]["scaleDown"]["stabilizationWindowSeconds"], 30,
        "{v2}"
    );
}

/// Status: `currentCPUUtilizationPercentage` and the conditions annotation.
#[tokio::test]
async fn status_converts_to_v1() {
    let api = TestApiServer::new();
    api.post(V1, &v1("h1", Some(60))).await;
    let (_, mut cur) = api.get(&format!("{V2}/h1")).await;
    cur["status"] = json!({
        "currentReplicas": 2, "desiredReplicas": 3,
        "currentMetrics": [{"type": "Resource", "resource": {"name": "cpu", "current": {"averageUtilization": 42, "averageValue": "100m"}}}],
        "conditions": [{"type": "AbleToScale", "status": "True", "reason": "Ready"}]
    });
    let (s, b) = api.put(&format!("{V2}/h1/status"), &cur).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (_, got) = api.get(&format!("{V1}/h1")).await;
    assert_eq!(
        got["status"]["currentCPUUtilizationPercentage"], 42,
        "{got}"
    );
    assert_eq!(got["status"]["currentReplicas"], 2, "{got}");
    let ann = &got["metadata"]["annotations"];
    assert!(
        ann["autoscaling.alpha.kubernetes.io/current-metrics"].is_string(),
        "{got}"
    );
    assert!(
        ann["autoscaling.alpha.kubernetes.io/conditions"].is_string(),
        "{got}"
    );
}
