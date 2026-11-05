//! Router-driven tests for the kubelet's read-only HTTP endpoints
//! beyond `/pods` — that is, `/runningpods/`, `/healthz`, and
//! `/stats/summary`. Existing `/pods` data-assembly coverage lives in
//! `pods_endpoint_test.rs`; this file focuses on the new endpoints
//! upstream `[NodeConformance]` specs poll.
//!
//! The tests exercise the pure data-assembly helpers (`running_pods_for_node`)
//! and the axum router via `tower::ServiceExt::oneshot` so they don't
//! need a live TCP socket.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use rusternetes_common::resources::{Pod, PodSpec, PodStatus};
use rusternetes_common::types::{ObjectMeta, Phase, TypeMeta};
use rusternetes_kubelet::server::{read_only_router, running_pods_for_node, ServerState};
use rusternetes_storage::{build_key, Storage, StorageBackend};
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

fn pod_on_node(name: &str, node: &str, phase: Option<Phase>) -> Pod {
    Pod {
        type_meta: TypeMeta {
            kind: "Pod".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: Some("default".to_string()),
            uid: format!("uid-{name}"),
            ..Default::default()
        },
        spec: Some(PodSpec {
            node_name: Some(node.to_string()),
            ..Default::default()
        }),
        status: phase.map(|p| PodStatus {
            phase: Some(p),
            ..Default::default()
        }),
    }
}

async fn seed(storage: &StorageBackend, pod: &Pod) {
    let key = build_key(
        "pods",
        pod.metadata.namespace.as_deref(),
        &pod.metadata.name,
    );
    storage.create(&key, pod).await.expect("seed pod");
}

fn server_state(storage: Arc<StorageBackend>, node: &str) -> ServerState {
    // `kubelet: None` triggers the test-mode short-circuit in /healthz
    // (always 200). Tests that need the production stale-sync semantics
    // would need to wire a real Kubelet, which requires Docker — out of
    // scope for unit-level coverage.
    ServerState {
        node_name: node.to_string(),
        storage,
        kubelet: None,
    }
}

#[tokio::test]
async fn running_pods_helper_filters_to_running_phase() {
    let storage = Arc::new(StorageBackend::new_memory());
    seed(
        &storage,
        &pod_on_node("run", "node-1", Some(Phase::Running)),
    )
    .await;
    seed(
        &storage,
        &pod_on_node("pend", "node-1", Some(Phase::Pending)),
    )
    .await;
    seed(
        &storage,
        &pod_on_node("succ", "node-1", Some(Phase::Succeeded)),
    )
    .await;
    seed(&storage, &pod_on_node("nostatus", "node-1", None)).await;

    let list = running_pods_for_node(&storage, "node-1")
        .await
        .expect("running list");

    let names: Vec<&str> = list
        .items
        .iter()
        .map(|p| p.metadata.name.as_str())
        .collect();
    assert_eq!(names, vec!["run"]);
}

#[tokio::test]
async fn running_pods_helper_respects_node_binding() {
    let storage = Arc::new(StorageBackend::new_memory());
    seed(
        &storage,
        &pod_on_node("mine", "node-1", Some(Phase::Running)),
    )
    .await;
    seed(
        &storage,
        &pod_on_node("theirs", "node-2", Some(Phase::Running)),
    )
    .await;

    let list = running_pods_for_node(&storage, "node-1")
        .await
        .expect("running list");
    let names: Vec<&str> = list
        .items
        .iter()
        .map(|p| p.metadata.name.as_str())
        .collect();
    assert_eq!(names, vec!["mine"]);
}

#[tokio::test]
async fn running_pods_route_returns_podlist_typemeta() {
    let storage = Arc::new(StorageBackend::new_memory());
    seed(
        &storage,
        &pod_on_node("run", "node-1", Some(Phase::Running)),
    )
    .await;
    let app = read_only_router(server_state(storage, "node-1"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/runningpods/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["kind"], "PodList");
    assert_eq!(v["apiVersion"], "v1");
    let items = v["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["metadata"]["name"], "run");
}

#[tokio::test]
async fn healthz_returns_ok_when_kubelet_handle_absent() {
    // ServerState::kubelet = None mirrors the early-startup / test-mode
    // contract documented in server.rs: /healthz answers 200 when there
    // is no kubelet handle to consult.
    let storage = Arc::new(StorageBackend::new_memory());
    let app = read_only_router(server_state(storage, "node-1"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"ok");
}

#[tokio::test]
async fn stats_summary_emits_node_block_and_pod_refs() {
    let storage = Arc::new(StorageBackend::new_memory());
    seed(&storage, &pod_on_node("a", "node-1", Some(Phase::Running))).await;
    seed(&storage, &pod_on_node("b", "node-1", Some(Phase::Pending))).await;
    seed(
        &storage,
        &pod_on_node("on-other", "node-2", Some(Phase::Running)),
    )
    .await;
    let app = read_only_router(server_state(storage, "node-1"));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/stats/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(v["node"]["nodeName"], "node-1");
    assert!(v["node"]["cpu"].is_object());
    assert!(v["node"]["memory"].is_object());

    let pods = v["pods"].as_array().expect("pods array");
    let mut names: Vec<&str> = pods
        .iter()
        .map(|p| p["podRef"]["name"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
    // No cross-node leakage.
    assert!(!names.contains(&"on-other"));
}

#[tokio::test]
async fn healthz_sub_paths_are_served_by_the_kubelet_router() {
    let storage = Arc::new(StorageBackend::new_memory());
    let app = read_only_router(server_state(storage, "node-1"));
    for p in ["/healthz/ping", "/healthz/log", "/healthz/syncloop"] {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(p).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{p}");
    }
}

/// `NewServer` (server.go:311-328): every path the assembled kubelet server
/// registers is served. The `Container.Handle` paths come from
/// `RegisteredHandlePaths()` (`registered_handle_paths`); the go-restful
/// `WebService` routes (`/pods`, `/stats/`, `/run`, `/exec`, `/attach`,
/// `/portForward`, `/containerLogs`, `/runningpods/`, `/checkpoint/`) are not
/// in that list, so they are named here. A missing route answers axum's empty
/// 404; a handler answers with its own status and a body.
async fn assert_every_registered_path_is_served(enable_debugging_handlers: bool) {
    use rusternetes_kubelet::server::{
        build_server_router, registered_handle_paths, ServerRouterOptions,
    };
    let storage = Arc::new(StorageBackend::new_memory());
    let dir = tempfile::tempdir().unwrap();
    let app = build_server_router(
        server_state(storage, "node-1"),
        Arc::new(
            rusternetes_common::observability::MetricsRegistry::new()
                .with_kubelet_metrics()
                .unwrap(),
        ),
        Arc::new(Default::default()),
        ServerRouterOptions {
            enable_debugging_handlers,
            enable_system_log_handler: true,
            enable_system_log_query: false,
            enable_profiling_handler: true,
            enable_contention_profiling: false,
            enable_debug_flags_handler: true,
            checkpoints_dir: dir.path().join("checkpoints"),
            logs_root: dir.path().to_path_buf(),
        },
    );
    let mut paths: Vec<String> =
        registered_handle_paths(enable_debugging_handlers, true, true, false);
    paths.extend(
        [
            "/pods",
            "/stats/summary",
            "/runningpods/",
            "/run/ns/pod/c",
            "/exec/ns/pod/c",
            "/attach/ns/pod/c",
            "/portForward/ns/pod",
            "/containerLogs/ns/pod/c",
            "/logs/",
            "/debug/pprof/",
        ]
        .map(String::from),
    );
    // `/checkpoint/` exists only with the ContainerCheckpoint gate, inside
    // `InstallAuthRequiredHandlers` (server.go:620-622).
    if enable_debugging_handlers && rusternetes_kubelet::server::checkpoint_enabled() {
        paths.push("/checkpoint/ns/pod/c".to_string());
    }
    for p in paths {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(&p).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(
            !(status == StatusCode::NOT_FOUND && body.is_empty()),
            "{p} is not routed (debugging handlers: {enable_debugging_handlers})"
        );
    }
}

#[tokio::test]
async fn every_registered_path_is_served_with_debugging_handlers() {
    assert_every_registered_path_is_served(true).await;
}

#[tokio::test]
async fn every_registered_path_is_served_without_debugging_handlers() {
    assert_every_registered_path_is_served(false).await;
}
