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
