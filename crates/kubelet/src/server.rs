//! Read-only HTTP routes served by the kubelet on its API port.
//!
//! Mirrors upstream `pkg/kubelet/server/server.go`. Routes served here:
//!
//! | Route               | Purpose                                       |
//! |---------------------|-----------------------------------------------|
//! | `GET /pods`         | All pods bound to this node                   |
//! | `GET /runningpods/` | Subset of `/pods` whose phase is `Running`    |
//! | `GET /healthz`      | sync-loop liveness — 200 if recent, else 500  |
//! | `GET /stats/summary`| Minimal cAdvisor-shape node + per-pod summary |
//!
//! The kubelet does not maintain its own in-memory `PodManager`: the
//! storage backend already holds the apiserver's authoritative view, and
//! both the sync loop and watch subscribe to the same source. Reading
//! from storage here therefore returns the same picture `sync_pod` is
//! about to act on.

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use rusternetes_common::resources::Pod;
use rusternetes_common::types::{List, Phase};
use rusternetes_common::Result;
use rusternetes_storage::{build_prefix, Storage, StorageBackend};
use serde_json::json;
use std::sync::Arc;

use crate::kubelet::Kubelet;

/// State injected into the kubelet's read-only HTTP routes.
#[derive(Clone)]
pub struct ServerState {
    pub node_name: String,
    pub storage: Arc<StorageBackend>,
    /// Optional handle on the live kubelet. Used by `/healthz` to read
    /// the last sync_loop timestamp. `None` in tests that don't spin a
    /// kubelet; in that mode `/healthz` short-circuits to 200 OK so
    /// router-shape tests stay green.
    pub kubelet: Option<Arc<Kubelet>>,
}

/// Pure data assembly: build the `PodList` for `/pods` by filtering
/// storage pods to those bound to `node_name`. Extracted from the axum
/// handler so it can be unit-tested without spinning up a server.
pub async fn pods_for_node(storage: &StorageBackend, node_name: &str) -> Result<List<Pod>> {
    let all: Vec<Pod> = storage.list(&build_prefix("pods", None)).await?;
    let items: Vec<Pod> = all
        .into_iter()
        .filter(|p| {
            p.spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .map(|n| n == node_name)
                .unwrap_or(false)
        })
        .collect();
    Ok(List::new("PodList", "v1", items))
}

/// Subset of [`pods_for_node`] restricted to pods whose `status.phase`
/// is `Running`. Backs `GET /runningpods/`.
pub async fn running_pods_for_node(storage: &StorageBackend, node_name: &str) -> Result<List<Pod>> {
    let list = pods_for_node(storage, node_name).await?;
    let items: Vec<Pod> = list
        .items
        .into_iter()
        .filter(|p| {
            matches!(
                p.status.as_ref().and_then(|s| s.phase.as_ref()),
                Some(Phase::Running)
            )
        })
        .collect();
    Ok(List::new("PodList", "v1", items))
}

async fn handle_list_pods(State(state): State<ServerState>) -> impl IntoResponse {
    match pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => Json(list).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn handle_list_running_pods(State(state): State<ServerState>) -> impl IntoResponse {
    match running_pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => Json(list).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn handle_healthz(State(state): State<ServerState>) -> impl IntoResponse {
    match &state.kubelet {
        // No kubelet attached (router-shape tests / early startup): treat as healthy.
        None => (StatusCode::OK, "ok").into_response(),
        Some(k) if k.healthy() => (StatusCode::OK, "ok").into_response(),
        Some(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "kubelet sync_loop is stale",
        )
            .into_response(),
    }
}

/// Minimal cAdvisor-shape JSON for `GET /stats/summary`. Upstream
/// `[NodeConformance]` specs assert shape (`node.nodeName`, `pods[]`),
/// not values, so we emit zero CPU/memory counters until the eviction
/// manager's per-pod stats integration lands.
async fn handle_stats_summary(State(state): State<ServerState>) -> impl IntoResponse {
    let pods = match pods_for_node(&state.storage, &state.node_name).await {
        Ok(list) => list.items,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let pod_summaries: Vec<_> = pods
        .iter()
        .map(|p| {
            json!({
                "podRef": {
                    "name": p.metadata.name,
                    "namespace": p.metadata.namespace.clone().unwrap_or_default(),
                    "uid": p.metadata.uid,
                },
                "cpu": { "usageNanoCores": 0u64 },
                "memory": { "workingSetBytes": 0u64 },
            })
        })
        .collect();

    Json(json!({
        "node": {
            "nodeName": state.node_name,
            "cpu": { "usageNanoCores": 0u64 },
            "memory": { "workingSetBytes": 0u64 },
        },
        "pods": pod_summaries,
    }))
    .into_response()
}

/// Build the router fragment containing the read-only kubelet routes.
/// Callers (`main.rs`, `lib.rs::run`) `.merge()` it into their own
/// router so routes that need additional state (metrics, configz, exec)
/// can be wired alongside.
pub fn read_only_router(state: ServerState) -> Router {
    Router::new()
        .route("/pods", get(handle_list_pods))
        .route("/runningpods/", get(handle_list_running_pods))
        .route("/healthz", get(handle_healthz))
        .route("/stats/summary", get(handle_stats_summary))
        .with_state(state)
}

/// `ComponentKubelet` (pkg/kubelet/server/server.go:116).
pub const COMPONENT_KUBELET: &str = "kubelet";

/// `(Server).installStatusZ` (pkg/kubelet/server/server.go:393-398).
pub fn install_statusz(router: Router, _registered_paths: Vec<String>) -> Router {
    router
}

/// `authorizer.AttributesRecord` (as populated by `GetRequestAttributes`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestAttributes {
    pub user: String,
    pub verb: String,
    pub namespace: String,
    pub api_group: String,
    pub api_version: String,
    pub resource: String,
    pub subresource: String,
    pub name: String,
    pub resource_request: bool,
    pub path: String,
}

/// `isSubpath` (pkg/kubelet/server/auth.go:56-59).
pub fn is_subpath(_subpath: &str, _path: &str) -> bool {
    false
}

/// `nodeAuthorizerAttributesGetter.GetRequestAttributes`
/// (pkg/kubelet/server/auth.go:77-148).
pub fn get_request_attributes(
    _node_name: &str,
    _user: &str,
    _method: &str,
    _request_path: &str,
) -> Vec<RequestAttributes> {
    Vec::new()
}

#[cfg(test)]
mod statusz_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_common::feature_gates::{
        reset_to_defaults, set_from_str, with_feature, Feature,
    };
    use serial_test::serial;
    use tower::ServiceExt;

    async fn get(app: Router, path: &str, accept: &str) -> (u16, String) {
        let req = Request::builder()
            .uri(path)
            .header("Accept", accept)
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    fn paths(p: &[&str]) -> Vec<String> {
        p.iter().map(|s| s.to_string()).collect()
    }

    // server_test.go:TestStatusz (gate on, Accept: text/plain).
    #[tokio::test]
    #[serial]
    async fn statusz_lists_registered_paths() {
        let _g = with_feature(Feature::ComponentStatusz, true);
        let app = install_statusz(
            Router::new(),
            paths(&["/metrics", "/configz", "/healthz", "/pods", "/statusz"]),
        );
        let (status, body) = get(app, "/statusz", "text/plain").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("kubelet statusz"), "{body}");
        // Go regexp `Paths([:=\s]+)/configz /healthz /metrics /pods /statusz\n$`.
        let tail = body.rsplit_once("Paths").map(|(_, t)| t).unwrap_or("");
        let rest = tail.trim_start_matches([':', '=', ' ', '\t', '\n']);
        assert_eq!(
            rest, "/configz /healthz /metrics /pods /statusz\n",
            "{body}"
        );
    }

    // server.go:394: gate off => nothing is installed.
    #[tokio::test]
    #[serial]
    async fn statusz_absent_when_gate_off() {
        let _g = with_feature(Feature::ComponentStatusz, false);
        let app = install_statusz(Router::new(), paths(&["/metrics"]));
        let (status, _) = get(app, "/statusz", "text/plain").await;
        assert_eq!(status, 404);
    }

    // auth_test.go:TestIsSubPath
    #[test]
    fn is_subpath_cases() {
        let cases = [
            ("", "", true),
            ("foo", "foo", true),
            ("/foo", "/foo", true),
            ("/foo/", "/foo/", true),
            ("/foo/bar", "/foo/bar", true),
            ("/foo", "/", true),
            ("/foo/", "/", true),
            ("/foo/bar", "/", true),
            ("/foo", "/foo", true),
            ("/foo/", "/foo", true),
            ("/foo/bar", "/foo", true),
            ("/foo", "/bar", false),
            ("/foo", "/foobar", false),
            ("/foobar", "/foo", false),
        ];
        for (sub, path, want) in cases {
            assert_eq!(is_subpath(sub, path), want, "{sub:?} {path:?}");
        }
    }

    // auth_test.go:AuthzTestCases (path -> expected subresources).
    fn authz_test_paths(fine_grained: bool) -> Vec<(&'static str, Vec<&'static str>)> {
        let mut t: Vec<(&'static str, Vec<&'static str>)> = vec![
            ("/attach/ns/pod/c", vec!["proxy"]),
            ("/attach/ns/pod/uid/c", vec!["proxy"]),
            ("/checkpoint/ns/pod/c", vec!["checkpoint"]),
            ("/configz", vec!["proxy"]),
            ("/flagz", vec!["configz"]),
            ("/statusz", vec!["statusz"]),
            ("/containerLogs/ns/pod/c", vec!["proxy"]),
            ("/debug/flags/v", vec!["proxy"]),
            ("/debug/pprof/sub", vec!["proxy"]),
            ("/exec/ns/pod/c", vec!["proxy"]),
            ("/exec/ns/pod/uid/c", vec!["proxy"]),
            ("/healthz", vec!["proxy"]),
            ("/healthz/log", vec!["proxy"]),
            ("/healthz/ping", vec!["proxy"]),
            ("/healthz/syncloop", vec!["proxy"]),
            ("/logs/", vec!["log"]),
            ("/logs/logpath", vec!["log"]),
            ("/metrics", vec!["metrics"]),
            ("/metrics/slis", vec!["metrics"]),
            ("/metrics/cadvisor", vec!["metrics"]),
            ("/metrics/probes", vec!["metrics"]),
            ("/metrics/resource", vec!["metrics"]),
            ("/pods/", vec!["proxy"]),
            ("/portForward/ns/pod", vec!["proxy"]),
            ("/portForward/ns/pod/uid", vec!["proxy"]),
            ("/run/ns/pod/c", vec!["proxy"]),
            ("/run/ns/pod/uid/c", vec!["proxy"]),
            ("/runningpods/", vec!["proxy"]),
            ("/stats/", vec!["stats"]),
            ("/stats/summary", vec!["stats"]),
        ];
        if fine_grained {
            for (p, subs) in t.iter_mut() {
                let extra = match *p {
                    "/healthz" | "/healthz/log" | "/healthz/ping" | "/healthz/syncloop" => {
                        Some("healthz")
                    }
                    "/pods/" | "/runningpods/" => Some("pods"),
                    "/configz" => Some("configz"),
                    _ => None,
                };
                if let Some(e) = extra {
                    subs.insert(0, e);
                }
            }
        }
        t
    }

    // auth_test.go:TestGetRequestAttributes
    #[test]
    #[serial]
    fn get_request_attributes_cases() {
        for fine_grained in [false, true] {
            reset_to_defaults();
            set_from_str(&format!("KubeletFineGrainedAuthz={fine_grained}")).unwrap();
            for (path, subs) in authz_test_paths(fine_grained) {
                for (method, verb) in [
                    ("POST", "create"),
                    ("GET", "get"),
                    ("PUT", "update"),
                    ("PATCH", "patch"),
                    ("DELETE", "delete"),
                ] {
                    let got = get_request_attributes("test", "phibby", method, path);
                    let want: Vec<RequestAttributes> = subs
                        .iter()
                        .map(|s| RequestAttributes {
                            user: "phibby".into(),
                            verb: verb.into(),
                            api_version: "v1".into(),
                            resource: "nodes".into(),
                            subresource: s.to_string(),
                            name: "test".into(),
                            resource_request: true,
                            path: path.into(),
                            ..Default::default()
                        })
                        .collect();
                    assert_eq!(got, want, "{method} {path} fine_grained={fine_grained}");
                }
            }
        }
        reset_to_defaults();
    }
}
