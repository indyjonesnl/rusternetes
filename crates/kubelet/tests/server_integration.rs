//! Integration tests for the kubelet HTTP server endpoints.
//! Uses MemoryStorage and a test-mode ServerState (no real Kubelet).

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rusternetes_common::resources::pod::{Pod, PodSpec, PodStatus};
use rusternetes_common::types::Phase;
use rusternetes_kubelet::server::{router, ServerState};
use rusternetes_storage::{Storage, StorageBackend};
use tower::ServiceExt;

async fn fixture(node_name: &str, pods: Vec<Pod>) -> ServerState {
    let storage = Arc::new(StorageBackend::new_memory());
    for p in pods {
        let ns = p
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let name = p.metadata.name.clone();
        storage
            .create(&format!("/registry/pods/{}/{}", ns, name), &p)
            .await
            .expect("write pod fixture");
    }
    ServerState {
        node_name: node_name.to_string(),
        storage,
        kubelet: None,
    }
}

fn pod_on(name: &str, node: &str) -> Pod {
    let mut p = Pod::new(
        name,
        PodSpec {
            node_name: Some(node.to_string()),
            ..Default::default()
        },
    );
    p.metadata.namespace = Some("default".to_string());
    p
}

fn pod_with_phase(name: &str, node: &str, phase: Phase) -> Pod {
    let mut p = pod_on(name, node);
    p.status = Some(PodStatus {
        phase: Some(phase),
        ..Default::default()
    });
    p
}

#[tokio::test]
async fn pods_returns_only_local_node_pods() {
    let state = fixture(
        "node-1",
        vec![
            pod_on("a", "node-1"),
            pod_on("b", "node-2"),
            pod_on("c", "node-1"),
        ],
    )
    .await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/pods").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["kind"].as_str(), Some("PodList"));
    let items = v["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        2,
        "expected 2 pods on node-1, got {}",
        items.len()
    );
    let names: Vec<_> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(names.contains(&"a"));
    assert!(names.contains(&"c"));
}

#[tokio::test]
async fn runningpods_returns_only_running_phase() {
    let state = fixture(
        "node-1",
        vec![
            pod_with_phase("running1", "node-1", Phase::Running),
            pod_with_phase("pending1", "node-1", Phase::Pending),
            pod_with_phase("running2", "node-1", Phase::Running),
            pod_with_phase("running-elsewhere", "node-2", Phase::Running),
        ],
    )
    .await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/runningpods/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["kind"].as_str(), Some("PodList"));
    let items = v["items"].as_array().expect("items array");
    assert_eq!(items.len(), 2);
    let names: Vec<_> = items
        .iter()
        .filter_map(|i| i["metadata"]["name"].as_str())
        .collect();
    assert!(names.contains(&"running1"));
    assert!(names.contains(&"running2"));
}

#[tokio::test]
async fn stats_summary_returns_minimal_cadvisor_shape() {
    let state = fixture("node-1", vec![pod_on("a", "node-1")]).await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/stats/summary").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["node"]["nodeName"].as_str(), Some("node-1"));
    assert!(
        v["node"]["cpu"].is_object(),
        "node.cpu must be object, got {:?}",
        v["node"]["cpu"]
    );
    assert!(
        v["node"]["memory"].is_object(),
        "node.memory must be object"
    );
    assert!(v["pods"].is_array(), "pods must be array");
    assert_eq!(v["pods"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn healthz_returns_ok_in_test_state() {
    let state = fixture("node-1", vec![]).await;
    let app = router(state);
    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"ok");
}
