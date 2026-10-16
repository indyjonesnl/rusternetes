//! Cases ported from pkg/proxy/healthcheck/healthcheck_test.go:474-943.
use axum::{body::Body, http::Request};
use rusternetes_common::resources::Node;
use rusternetes_kube_proxy::serve_metrics::ProxyHealthServer;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tower::ServiceExt;

fn clock() -> (Arc<Mutex<SystemTime>>, ProxyHealthServer) {
    let time = Arc::new(Mutex::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    ));
    let now = Arc::clone(&time);
    let server = ProxyHealthServer::with_clock(
        Duration::from_secs(10),
        Arc::new(move || *now.lock().unwrap()),
    );
    (time, server)
}

fn node(extra: serde_json::Value) -> Node {
    let mut value = serde_json::json!({"metadata": {"name": "n"}, "spec": {}});
    if let Some(spec) = extra.get("spec") {
        value["spec"] = spec.clone();
    }
    if let Some(deletion) = extra.get("deletionTimestamp") {
        value["metadata"]["deletionTimestamp"] = deletion.clone();
    }
    serde_json::from_value(value).unwrap()
}

async fn get(server: &ProxyHealthServer, path: &str) -> (u16, serde_json::Value) {
    let response = server
        .router()
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(response.headers()["x-content-type-options"], "nosniff");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

// Health() iterates lastUpdatedMap, not queuedMap: queued work before the first
// Updated is ignored upstream (proxy_health.go:151). Preserve that behaviour.
#[tokio::test]
async fn startup_and_queue_timeout_boundary() {
    let (time, server) = clock();
    server.set_node(Some(node(serde_json::json!({}))));
    server.queued_update("IPv4");
    *time.lock().unwrap() += Duration::from_secs(20);
    assert_eq!(get(&server, "/livez").await.0, 200);
    server.updated("IPv4");
    server.queued_update("IPv4");
    *time.lock().unwrap() += Duration::from_secs(9);
    assert_eq!(get(&server, "/livez").await.0, 200);
    server.queued_update("IPv4"); // must not move the oldest queued timestamp
    *time.lock().unwrap() += Duration::from_secs(1);
    let (status, body) = get(&server, "/livez").await;
    assert_eq!(status, 503);
    assert_eq!(body["healthy"], false);
    assert_eq!(body["status"]["IPv4"]["healthy"], false);
    server.updated("IPv4");
    let (status, body) = get(&server, "/livez").await;
    assert_eq!(status, 200);
    assert_eq!(body["lastUpdated"], body["currentTime"]);
    assert!(body.get("nodeEligible").is_none());
}

// healthcheck_test.go:803-883: updating one IP family cannot heal the other.
#[tokio::test]
async fn independent_families_and_healthy_last_updated_override() {
    let (time, server) = clock();
    server.updated("IPv4");
    server.updated("IPv6");
    let (_, original) = get(&server, "/livez").await;
    server.queued_update("IPv4");
    server.queued_update("IPv6");
    *time.lock().unwrap() += Duration::from_secs(25);
    let (status, body) = get(&server, "/livez").await;
    assert_eq!(status, 503);
    assert_eq!(body["lastUpdated"], original["lastUpdated"]);
    server.updated("IPv4");
    let (status, body) = get(&server, "/livez").await;
    assert_eq!(status, 503);
    assert_eq!(body["status"]["IPv4"]["healthy"], true);
    assert_eq!(body["status"]["IPv6"]["healthy"], false);
    server.updated("IPv6");
    *time.lock().unwrap() += Duration::from_secs(100);
    let (status, body) = get(&server, "/livez").await;
    assert_eq!(status, 200); // no pending work stays healthy indefinitely
    assert_eq!(body["lastUpdated"], body["currentTime"]);
    assert_ne!(body["status"]["IPv4"]["lastUpdated"], body["currentTime"]);
}

// healthcheck_test.go:497-551,577-627: only /healthz uses node eligibility.
#[tokio::test]
async fn missing_deleted_and_autoscaler_tainted_nodes() {
    let (_, server) = clock();
    for (node, eligible) in [
        (None, false),
        (Some(node(serde_json::json!({}))), true),
        // proxy_health.go:184 uses time.IsZero, not pointer presence.
        (
            Some(node(
                serde_json::json!({"deletionTimestamp": "0001-01-01T00:00:00Z"}),
            )),
            true,
        ),
        (
            Some(node(
                serde_json::json!({"spec": {"taints": [{"key": "other", "effect": "NoSchedule"}]}}),
            )),
            true,
        ),
        (
            Some(node(
                serde_json::json!({"spec": {"taints": [{"key": "ToBeDeletedByClusterAutoscaler", "effect": "NoSchedule"}]}}),
            )),
            false,
        ),
        (
            Some(node(
                serde_json::json!({"deletionTimestamp": "2024-01-01T00:00:00Z"}),
            )),
            false,
        ),
    ] {
        server.set_node(node);
        let (status, body) = get(&server, "/healthz").await;
        assert_eq!(status, if eligible { 200 } else { 503 });
        assert_eq!(body["nodeEligible"], eligible);
        assert_eq!(body["healthy"], true); // healthy proxier even when node ineligible
        assert!(body.get("status").is_none());
        let (status, body) = get(&server, "/livez").await;
        assert_eq!(status, 200);
        assert!(body.get("nodeEligible").is_none());
    }
}
