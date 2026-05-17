//! Kubelet HTTP server exposing the surface expected by upstream node-conformance
//! tests — `/pods`, `/runningpods/`, `/healthz`, `/stats/summary`.
//!
//! Bound to a separate port (default 10250) via `RUSTERNETES_KUBELET_SERVER_PORT`
//! env var; wired up in `main.rs`. The handlers for `/pods` etc. land in
//! subsequent tasks of PR2 — this task brings up the skeleton and `/healthz`.
//!
//! See `docs/superpowers/specs/2026-05-17-node-conformance-design.md`.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};

use crate::kubelet::Kubelet;
use rusternetes_common::resources::pod::Pod;
use rusternetes_common::types::Phase;
use rusternetes_storage::{Storage, StorageBackend};

#[derive(Clone)]
pub struct ServerState {
    pub node_name: String,
    pub storage: Arc<StorageBackend>,
    /// Production code (kubelet `main.rs`) MUST set this to `Some(...)`.
    /// `None` is only for integration tests that don't want to construct a
    /// full `Kubelet`: the `/healthz` handler treats `None` as healthy. If
    /// production code path ever leaves this as `None`, `/healthz` will lie.
    pub kubelet: Option<Arc<Kubelet>>,
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/pods", get(list_pods))
        .route("/runningpods/", get(list_running_pods))
        .route("/stats/summary", get(stats_summary))
        .with_state(state)
}

async fn healthz(State(state): State<ServerState>) -> (StatusCode, &'static str) {
    match &state.kubelet {
        Some(kl) if kl.healthy() => (StatusCode::OK, "ok"),
        Some(_) => (StatusCode::INTERNAL_SERVER_ERROR, "stale"),
        None => (StatusCode::OK, "ok"),
    }
}

async fn list_pods(
    State(state): State<ServerState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let all: Vec<Pod> = state
        .storage
        .list("/registry/pods/")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mine: Vec<&Pod> = all
        .iter()
        .filter(|p| {
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(state.node_name.as_str())
        })
        .collect();
    Ok(Json(serde_json::json!({
        "kind": "PodList",
        "apiVersion": "v1",
        "items": mine,
    })))
}

async fn list_running_pods(
    State(state): State<ServerState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let all: Vec<Pod> = state
        .storage
        .list("/registry/pods/")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let mine: Vec<&Pod> = all
        .iter()
        .filter(|p| {
            let on_this_node = p.spec.as_ref().and_then(|s| s.node_name.as_deref())
                == Some(state.node_name.as_str());
            let is_running = matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running)
            );
            on_this_node && is_running
        })
        .collect();
    Ok(Json(serde_json::json!({
        "kind": "PodList",
        "apiVersion": "v1",
        "items": mine,
    })))
}

async fn stats_summary(
    State(state): State<ServerState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let all: Vec<Pod> = state
        .storage
        .list("/registry/pods/")
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let now = chrono::Utc::now().to_rfc3339();
    let zero_cpu = serde_json::json!({
        "time": now,
        "usageNanoCores": 0u64,
        "usageCoreNanoSeconds": 0u64,
    });
    let zero_mem = serde_json::json!({
        "time": now,
        "availableBytes": 0u64,
        "usageBytes": 0u64,
        "workingSetBytes": 0u64,
        "rssBytes": 0u64,
    });

    let pods_json: Vec<serde_json::Value> = all
        .iter()
        .filter(|p| {
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(state.node_name.as_str())
        })
        .map(|p| {
            serde_json::json!({
                "podRef": {
                    "name": p.metadata.name.clone(),
                    "namespace": p.metadata.namespace.clone(),
                    "uid": p.metadata.uid.clone(),
                },
                "startTime": now,
                "cpu": zero_cpu,
                "memory": zero_mem,
                "containers": [],
            })
        })
        .collect();

    Ok(Json(serde_json::json!({
        "node": {
            "nodeName": state.node_name,
            "startTime": now,
            "cpu": zero_cpu,
            "memory": zero_mem,
        },
        "pods": pods_json,
    })))
}
