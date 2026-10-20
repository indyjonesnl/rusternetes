//! Port of pkg/proxy/healthcheck/proxy_health.go:41-271.
use axum::{http::StatusCode, response::IntoResponse, routing::any, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use rusternetes_common::resources::Node;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

/// Go time.Time's JSON representation is RFC3339Nano (proxy_health.go:43,52-53).
fn timestamp(time: SystemTime) -> String {
    let full = DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Nanos, true);
    let (seconds, nanos) = full.split_once('.').expect("RFC3339 nanosecond format");
    let nanos = nanos.trim_end_matches('Z').trim_end_matches('0');
    if nanos.is_empty() {
        format!("{seconds}Z")
    } else {
        format!("{seconds}.{nanos}Z")
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProxierHealth {
    last_updated: String,
    healthy: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProxyHealth {
    last_updated: String,
    current_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_eligible: Option<bool>,
    healthy: bool,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    status: BTreeMap<String, ProxierHealth>,
}

#[derive(Default)]
struct State {
    updated: BTreeMap<String, SystemTime>,
    queued: BTreeMap<String, SystemTime>,
    node: Option<Node>,
}

/// Shared state for the dedicated /healthz and /livez listener.
/// Pending work, timestamps and the cached node are protected together as in
/// proxy_health.go:79-81. The clock seam mirrors upstream's clock.Clock.
#[derive(Clone)]
pub struct ProxyHealthServer {
    state: Arc<Mutex<State>>,
    timeout: Duration,
    clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
}

impl ProxyHealthServer {
    /// Use the real clock (proxy_health.go:85-86).
    pub fn new(timeout: Duration) -> Self {
        Self::with_clock(timeout, Arc::new(SystemTime::now))
    }

    /// Supply a clock, as upstream newProxyHealthServer does (lines 89-100).
    pub fn with_clock(timeout: Duration, clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            timeout,
            clock,
        }
    }

    /// Clear the pending update and record completion (proxy_health.go:105-110).
    pub fn updated(&self, family: &str) {
        let mut state = self.state.lock().expect("proxy health lock poisoned");
        state.queued.remove(family);
        state.updated.insert(family.to_owned(), (self.clock)());
    }

    /// Keep the oldest pending timestamp (proxy_health.go:117-123).
    pub fn queued_update(&self, family: &str) {
        let mut state = self.state.lock().expect("proxy health lock poisoned");
        state
            .queued
            .entry(family.to_owned())
            .or_insert_with(|| (self.clock)());
    }

    /// Cache the node provided by its informer (pkg/proxy/node.go:175-179).
    pub fn set_node(&self, node: Option<Node>) {
        self.state.lock().expect("proxy health lock poisoned").node = node;
    }

    fn health(&self) -> ProxyHealth {
        let state = self.state.lock().expect("proxy health lock poisoned");
        let now = (self.clock)();
        // Upstream iterates lastUpdatedMap, even when queuedMap contains an
        // as-yet uninitialized family. Do not change this (lines 136-169).
        let status: BTreeMap<_, _> = state
            .updated
            .iter()
            .map(|(family, updated)| {
                let healthy = state.queued.get(family).is_none_or(|queued| {
                    now.duration_since(*queued)
                        .map_or(true, |elapsed| elapsed < self.timeout)
                });
                (
                    family.clone(),
                    ProxierHealth {
                        last_updated: timestamp(*updated),
                        healthy,
                    },
                )
            })
            .collect();
        ProxyHealth {
            last_updated: state
                .updated
                .values()
                .max()
                .copied()
                .map(timestamp)
                .unwrap_or_else(|| "0001-01-01T00:00:00Z".to_owned()),
            current_time: timestamp(now),
            node_eligible: None,
            healthy: status.values().all(|family| family.healthy),
            status,
        }
    }

    fn node_eligible(&self) -> bool {
        // Missing/deleted/autoscaler-tainted nodes are ineligible; other taints
        // have no effect (proxy_health.go:176-192).
        let state = self.state.lock().expect("proxy health lock poisoned");
        state.node.as_ref().is_some_and(|node| {
            node.metadata
                .deletion_timestamp
                .as_ref()
                .is_none_or(|time| {
                    // Go time.Time.IsZero (proxy_health.go:184) accepts an explicit
                    // 0001-01-01T00:00:00Z, not just an absent JSON field.
                    time.timestamp() == -62_135_596_800 && time.timestamp_subsec_nanos() == 0
                })
                && !node
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.taints.as_ref())
                    .is_some_and(|taints| {
                        taints
                            .iter()
                            .any(|taint| taint.key == "ToBeDeletedByClusterAutoscaler")
                    })
        })
    }

    async fn response(self, check_node: bool) -> axum::response::Response {
        let mut health = self.health();
        let eligible = !check_node || self.node_eligible();
        if check_node {
            health.node_eligible = Some(eligible);
        }
        let healthy = health.healthy && eligible;
        let status = if healthy {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        // /healthz alone includes nodeEligible; healthy handlers override only
        // the top-level lastUpdated, preserving per-family timestamps
        // (proxy_health.go:219-243,251-270).
        if healthy {
            health.last_updated = timestamp((self.clock)());
        }
        super::exposition::observe_proxy_probe(check_node, status.as_str());
        match serde_json::to_vec(&health) {
            Ok(body) => (
                status,
                [
                    ("Content-Type", "application/json"),
                    ("X-Content-Type-Options", "nosniff"),
                ],
                body,
            )
                .into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
        }
    }

    /// Install both exact paths, with no method restriction, like Go's mux
    /// (proxy_health.go:196-200).
    pub fn router(&self) -> Router {
        let healthz = self.clone();
        let livez = self.clone();
        Router::new()
            .route("/healthz", any(move || healthz.clone().response(true)))
            .route("/livez", any(move || livez.clone().response(false)))
    }

    /// An empty address disables the listener; otherwise retry failures every
    /// five seconds (server.go:436-456). Bind failures never report success.
    pub fn serve(&self, address: String) {
        if address.is_empty() {
            return;
        }
        self.serve_with_errors(address, None);
    }

    pub(crate) fn serve_with_errors(
        &self,
        address: String,
        errors: Option<tokio::sync::mpsc::UnboundedSender<anyhow::Error>>,
    ) {
        if address.is_empty() {
            return;
        }
        super::spawn_server(address, self.router(), "healthz server failed", errors);
    }

    /// Cache creation/update events and ignore deletions, exactly as upstream
    /// NodeManager.OnNodeChange/OnNodeDelete (pkg/proxy/node.go:175-179,205-210).
    pub(crate) fn watch_node<S: rusternetes_storage::Storage + Send + Sync + 'static>(
        &self,
        storage: Arc<S>,
        name: String,
    ) {
        use futures::StreamExt;
        use rusternetes_storage::WatchEvent;
        let server = self.clone();
        tokio::spawn(async move {
            let key = format!("/registry/nodes/{name}");
            loop {
                // Establish the watch before reading the initial snapshot, so
                // a node update while GET is in flight stays queued for us.
                let stream = storage.watch(&key).await;
                if let Ok(node) = storage.get::<Node>(&key).await {
                    server.set_node(Some(node));
                }
                match stream {
                    Ok(mut stream) => {
                        while let Some(event) = stream.next().await {
                            match event {
                                Ok(
                                    WatchEvent::Added(_, value) | WatchEvent::Modified(_, value),
                                ) => {
                                    if let Ok(node) = serde_json::from_str::<Node>(&value) {
                                        if node.metadata.name == name {
                                            server.set_node(Some(node));
                                        }
                                    }
                                }
                                Ok(WatchEvent::Deleted(_, _)) => {}
                                Err(error) => {
                                    tracing::warn!("proxy node watch failed: {error}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(error) => tracing::warn!("proxy node watch failed: {error}"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
}
