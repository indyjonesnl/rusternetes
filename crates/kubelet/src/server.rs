//! Kubelet HTTP server exposing the surface expected by upstream node-conformance
//! tests — `/pods`, `/runningpods/`, `/healthz`, `/stats/summary`.
//!
//! Bound to a separate port (default 10250) via `RUSTERNETES_KUBELET_SERVER_PORT`
//! env var; wired up in `main.rs`. The handlers for `/pods` etc. land in
//! subsequent tasks of PR2 — this task brings up the skeleton and `/healthz`.
//!
//! See `docs/superpowers/specs/2026-05-17-node-conformance-design.md`.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::get, Router};

use crate::kubelet::Kubelet;
use rusternetes_storage::StorageBackend;

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
        .with_state(state)
}

async fn healthz(State(state): State<ServerState>) -> (StatusCode, &'static str) {
    match &state.kubelet {
        Some(kl) if kl.healthy() => (StatusCode::OK, "ok"),
        Some(_) => (StatusCode::INTERNAL_SERVER_ERROR, "stale"),
        None => (StatusCode::OK, "ok"),
    }
}
