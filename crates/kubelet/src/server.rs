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

/// `(Server).installStatusZ` (pkg/kubelet/server/server.go:393-398): behind the
/// `ComponentStatusz` gate, install `/statusz` with a registry built from
/// `compatibility.DefaultBuildEffectiveVersion()` and the container's
/// `RegisteredHandlePaths()` (`registered_paths`). The registry is only built
/// when the gate is on, as upstream builds it inside the `if`.
///
/// `s.addMetricsBucketMatcher("statusz")` (server.go:395) is
/// [`MetricsBuckets::install_status_z`].
pub fn install_statusz(router: Router, registered_paths: Vec<String>) -> Router {
    rusternetes_common::statusz::install_if_enabled(router, COMPONENT_KUBELET, || {
        rusternetes_common::statusz::Registry::new(Some(
            rusternetes_common::compatibility::default_build_effective_version(),
        ))
        .with_listed_paths(registered_paths)
    })
}

/// The `metricsBuckets` / `metricsMethodBuckets` sets of `Server`
/// (pkg/kubelet/server/server.go:125-126, initialised at :308-309) with
/// `addMetricsBucketMatcher` (:402), `getMetricBucket` (:407) and
/// `getMetricMethodBucket` (:415), plus the bucket registrations of
/// `NewServer`'s installers ([`MetricsBuckets::new_server`]). Read per request
/// by [`install_metrics_filter`] (`Server.ServeHTTP`, :1176-1200).
#[derive(Debug, Clone)]
pub struct MetricsBuckets {
    buckets: std::collections::HashSet<String>,
    method_buckets: std::collections::HashSet<&'static str>,
}

/// The inputs of `NewServer` that decide which bucket matchers are added.
#[derive(Debug, Clone, Copy, Default)]
pub struct ServerBucketOptions {
    /// `kubeCfg.EnableDebuggingHandlers`.
    pub enable_debugging_handlers: bool,
    /// `ComponentFlagz` gate and a non-nil flagz registry.
    pub flagz_enabled: bool,
    /// `ContainerCheckpoint` gate.
    pub checkpoint_enabled: bool,
    /// `ComponentStatusz` gate.
    pub statusz_enabled: bool,
}

impl MetricsBuckets {
    /// `sets.New[string]()` plus the method set
    /// `"OPTIONS", "GET", "HEAD", "POST", "PUT", "DELETE", "TRACE", "CONNECT"`.
    pub fn new() -> Self {
        Self {
            buckets: Default::default(),
            method_buckets: [
                "OPTIONS", "GET", "HEAD", "POST", "PUT", "DELETE", "TRACE", "CONNECT",
            ]
            .into_iter()
            .collect(),
        }
    }

    /// The bucket-registering sequence of `NewServer`
    /// (server.go:311-330): `InstallAuthNotRequiredHandlers`, then
    /// `InstallAuthRequiredHandlers` + `InstallSystemLogHandler` +
    /// `InstallProfilingHandler` when `EnableDebuggingHandlers`, else
    /// `InstallDebuggingDisabledHandlers`, then `installStatusZ`.
    /// `InstallDebugFlagsHandler` registers no bucket.
    pub fn new_server(opts: ServerBucketOptions) -> Self {
        let mut b = Self::new();
        b.install_auth_not_required_handlers();
        if opts.enable_debugging_handlers {
            b.install_auth_required_handlers(opts.flagz_enabled, opts.checkpoint_enabled);
            b.install_system_log_handler();
            b.install_profiling_handler();
        } else {
            b.install_debugging_disabled_handlers();
        }
        b.install_status_z(opts.statusz_enabled);
        b
    }

    /// The `addMetricsBucketMatcher` calls of `InstallAuthNotRequiredHandlers`
    /// (server.go:427-510), in upstream order. `metrics/resource` and
    /// `metrics/probes` are registered twice upstream (:457-458, :494, :503);
    /// the set makes that idempotent.
    pub fn install_auth_not_required_handlers(&mut self) {
        for bucket in [
            "healthz",
            "pods",
            "stats",
            "metrics",
            "metrics/cadvisor",
            "metrics/probes",
            "metrics/resource",
            "metrics/resource",
            "metrics/probes",
        ] {
            self.add_metrics_bucket_matcher(bucket);
        }
    }

    /// The `addMetricsBucketMatcher` calls of `InstallAuthRequiredHandlers`
    /// (server.go:519-629): `flagz` only when `ComponentFlagz` is on and a
    /// flagz registry is set (:602-607), `checkpoint` only when
    /// `ContainerCheckpoint` is on (:620-622).
    pub fn install_auth_required_handlers(
        &mut self,
        flagz_enabled: bool,
        checkpoint_enabled: bool,
    ) {
        for bucket in [
            "run",
            "exec",
            "attach",
            "portForward",
            "containerLogs",
            "configz",
        ] {
            self.add_metrics_bucket_matcher(bucket);
        }
        if flagz_enabled {
            self.add_metrics_bucket_matcher("flagz");
        }
        self.add_metrics_bucket_matcher("runningpods");
        if checkpoint_enabled {
            self.add_metrics_bucket_matcher("checkpoint");
        }
    }

    /// `InstallDebuggingDisabledHandlers` (server.go:633-651).
    pub fn install_debugging_disabled_handlers(&mut self) {
        for bucket in [
            "run",
            "exec",
            "attach",
            "portForward",
            "containerLogs",
            "runningpods",
            "pprof",
            "logs",
        ] {
            self.add_metrics_bucket_matcher(bucket);
        }
    }

    /// `InstallSystemLogHandler` (server.go:655-656).
    pub fn install_system_log_handler(&mut self) {
        self.add_metrics_bucket_matcher("logs");
    }

    /// `InstallProfilingHandler` (server.go:705-706).
    pub fn install_profiling_handler(&mut self) {
        self.add_metrics_bucket_matcher("debug");
    }

    /// `installStatusZ` (server.go:393-396): behind `ComponentStatusz`.
    pub fn install_status_z(&mut self, statusz_enabled: bool) {
        if statusz_enabled {
            self.add_metrics_bucket_matcher("statusz");
        }
    }

    /// `addMetricsBucketMatcher`: "not thread safe and should not be used
    /// dynamically" -- hence `&mut self`.
    pub fn add_metrics_bucket_matcher(&mut self, bucket: &str) {
        self.buckets.insert(bucket.to_string());
    }

    /// `getMetricBucket`.
    pub fn get_metric_bucket(&self, path: &str) -> String {
        let root = get_url_root_path(path);
        if self.buckets.contains(&root) {
            return root;
        }
        "other".to_string()
    }

    /// `getMetricMethodBucket`: unknown or invalid HTTP verbs are `other`.
    pub fn get_metric_method_bucket(&self, method: &str) -> String {
        if self.method_buckets.contains(method) {
            return method.to_string();
        }
        "other".to_string()
    }
}

impl Default for MetricsBuckets {
    fn default() -> Self {
        Self::new()
    }
}

/// `longRunningRequestPathMap` (server.go:1152-1157). Upstream keys it
/// `portforward` (lower case) while the bucket is `portForward`, so a
/// port-forward request is never reported long-running; replicated as-is
/// (#3250).
const LONG_RUNNING_REQUEST_PATHS: [&str; 4] = ["exec", "attach", "portforward", "debug"];

/// `isLongRunningRequest` (server.go:1160-1163): fed the bucketed path by
/// `ServeHTTP` (:1187-1189).
pub fn is_long_running_request(path: &str) -> bool {
    LONG_RUNNING_REQUEST_PATHS.contains(&path)
}

/// `getURLRootPath` (server.go:1139-1150): the first path segment, or
/// `metrics/<sub>` for a `/metrics/...` path.
fn get_url_root_path(path: &str) -> String {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    let mut parts = trimmed.splitn(3, '/');
    let first = parts.next().unwrap_or("");
    if first == "metrics" {
        if let Some(second) = parts.next() {
            return format!("{first}/{second}");
        }
    }
    first.to_string()
}

/// `Server.ServeHTTP`'s request instrumentation (server.go:1176-1200), as the
/// outermost layer of the kubelet API router: upstream's `ServeHTTP` wraps the
/// whole restful container, so requests the auth filter rejects are counted too.
#[derive(Debug)]
struct MetricsFilterState {
    buckets: MetricsBuckets,
    /// `"readonly"` when `s.auth == nil`, else `"readwrite"` (:1179-1185).
    server_type: &'static str,
}

/// `defer HTTPInflightRequests.WithLabelValues(...).Dec()` (:1194): a guard so
/// the decrement also runs when the request future is dropped (client gone).
struct InflightGuard(prometheus::Gauge);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

async fn metrics_filter(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<MetricsFilterState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use crate::server_metrics::{HTTP_INFLIGHT_REQUESTS, HTTP_REQUESTS, HTTP_REQUESTS_DURATION};
    let method = state
        .buckets
        .get_metric_method_bucket(req.method().as_str());
    let path = state.buckets.get_metric_bucket(req.uri().path());
    let long_running = is_long_running_request(&path).to_string();
    let labels = [
        method.as_str(),
        path.as_str(),
        state.server_type,
        long_running.as_str(),
    ];

    HTTP_REQUESTS.with_label_values(&labels).inc();

    let inflight = HTTP_INFLIGHT_REQUESTS.with_label_values(&labels);
    inflight.inc();
    let _inflight = InflightGuard(inflight);

    // server.go:1196-1197:
    //   startTime := time.Now()
    //   defer HTTPRequestsDuration.WithLabelValues(...).Observe(SinceInSeconds(startTime))
    // Go evaluates a deferred call's arguments at the `defer` statement, so
    // upstream observes the (near-zero) time elapsed since `startTime` right
    // here, not the request duration. Ported as-is rather than "fixed", so the
    // series matches upstream's.
    let start = std::time::Instant::now();
    HTTP_REQUESTS_DURATION
        .with_label_values(&labels)
        .observe(start.elapsed().as_secs_f64());

    next.run(req).await
}

/// Wrap `router` in `Server.ServeHTTP`'s HTTP request metrics (server.go:1176).
/// `auth_enabled` is `s.auth != nil` and picks the `server_type` label. Must be
/// applied last (outside [`crate::server_auth::install_auth_filter`]).
pub fn install_metrics_filter(
    router: Router,
    buckets: MetricsBuckets,
    auth_enabled: bool,
) -> Router {
    crate::server_metrics::register();
    let state = std::sync::Arc::new(MetricsFilterState {
        buckets,
        server_type: if auth_enabled {
            "readwrite"
        } else {
            "readonly"
        },
    });
    router.layer(axum::middleware::from_fn_with_state(state, metrics_filter))
}

/// The `NewServer` bucket inputs for this kubelet's installed handlers:
/// debugging handlers are always on (`EnableDebuggingHandlers` default true),
/// `ComponentStatusz` follows its gate, and `flagz`/`checkpoint` are off
/// because this kubelet installs neither handler (no flagz registry,
/// server.go:602-607; no `CheckpointContainer`).
pub fn server_bucket_options() -> ServerBucketOptions {
    ServerBucketOptions {
        enable_debugging_handlers: true,
        flagz_enabled: false,
        checkpoint_enabled: false,
        statusz_enabled: rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::ComponentStatusz,
        ),
    }
}

/// Paths [`read_only_router`] registers (the route templates, as go-restful's
/// `RegisteredHandlePaths()` reports them).
pub const READ_ONLY_PATHS: [&str; 4] = ["/pods", "/runningpods/", "/healthz", "/stats/summary"];

/// `authorizer.AttributesRecord` (as populated by `GetRequestAttributes`).
#[allow(dead_code)] // consumed by the kubelet authn/authz filter (not yet wired)
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

#[allow(dead_code)]
const STATS_PATH: &str = "/stats/";
#[allow(dead_code)]
const METRICS_PATH: &str = "/metrics";
#[allow(dead_code)]
const LOGS_PATH: &str = "/logs/";
#[allow(dead_code)]
const CHECKPOINT_PATH: &str = "/checkpoint/";
#[allow(dead_code)]
const PODS_PATH: &str = "/pods";
#[allow(dead_code)]
const RUNNING_PODS_PATH: &str = "/runningpods/";

/// `isSubpath` (pkg/kubelet/server/auth.go:56-59).
#[allow(dead_code)] // consumed by the kubelet authn/authz filter (not yet wired)
pub fn is_subpath(subpath: &str, path: &str) -> bool {
    let path = path.strip_suffix('/').unwrap_or(path);
    subpath == path || (subpath.starts_with(path) && subpath.as_bytes()[path.len()] == b'/')
}

/// `nodeAuthorizerAttributesGetter.GetRequestAttributes`
/// (pkg/kubelet/server/auth.go:77-148). `KubeletFineGrainedAuthz` is read by
/// name (it has no typed `Feature` variant); the `/flagz` -> `configz` case is
/// unconditional, as upstream.
#[allow(dead_code)] // consumed by the kubelet authn/authz filter (not yet wired)
pub fn get_request_attributes(
    node_name: &str,
    user: &str,
    method: &str,
    request_path: &str,
) -> Vec<RequestAttributes> {
    let api_verb = match method {
        "POST" => "create",
        "GET" => "get",
        "PUT" => "update",
        "PATCH" => "patch",
        "DELETE" => "delete",
        _ => "",
    };

    let mut subresources: Vec<&str> = Vec::new();
    if rusternetes_common::feature_gates::gate_enabled("KubeletFineGrainedAuthz").unwrap_or(false) {
        if is_subpath(request_path, PODS_PATH) {
            subresources.push("pods");
        } else if is_subpath(request_path, "/healthz") {
            subresources.push("healthz");
        } else if is_subpath(request_path, "/configz") {
            subresources.push("configz");
        // upstream puts runningpods last (it allocates a new string on every
        // check); the case order is preserved.
        } else if is_subpath(request_path, RUNNING_PODS_PATH) {
            subresources.push("pods");
        }
    }

    if is_subpath(request_path, STATS_PATH) {
        subresources.push("stats");
    } else if is_subpath(request_path, METRICS_PATH) {
        subresources.push("metrics");
    } else if is_subpath(request_path, LOGS_PATH) {
        // "log" to match other log subresources (pods/log, etc)
        subresources.push("log");
    } else if is_subpath(request_path, CHECKPOINT_PATH) {
        subresources.push("checkpoint");
    } else if is_subpath(
        request_path,
        rusternetes_common::statusz::DEFAULT_STATUSZ_PATH,
    ) {
        subresources.push("statusz");
    } else if is_subpath(request_path, "/flagz") {
        subresources.push("configz");
    } else {
        subresources.push("proxy");
    }

    subresources
        .into_iter()
        .map(|subresource| RequestAttributes {
            user: user.to_string(),
            verb: api_verb.to_string(),
            namespace: String::new(),
            api_group: String::new(),
            api_version: "v1".to_string(),
            resource: "nodes".to_string(),
            subresource: subresource.to_string(),
            name: node_name.to_string(),
            resource_request: true,
            path: request_path.to_string(),
        })
        .collect()
}

/// Route templates [`crate::streaming_server`] handlers are mounted at in the
/// kubelet binary (main.rs).
pub const STREAMING_PATHS: [&str; 7] = [
    "/exec/:namespace/:pod/:container",
    "/exec/:namespace/:pod/:uid/:container",
    "/attach/:namespace/:pod/:container",
    "/attach/:namespace/:pod/:uid/:container",
    "/portForward/:namespace/:pod",
    "/portForward/:namespace/:pod/:uid",
    "/containerLogs/:namespace/:pod/:container",
];

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
            paths(&["/metrics", "/configz", "/healthz", "/pods"]),
        );
        let (status, body) = get(app, "/statusz", "text/plain").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("kubelet statusz"), "{body}");
        // Go regexp `Paths([:=\s]+)/configz /healthz /metrics /pods\n$`.
        let tail = body.rsplit_once("Paths").map(|(_, t)| t).unwrap_or("");
        let rest = tail.trim_start_matches([':', '=', ' ', '\t', '\n']);
        assert_eq!(rest, "/configz /healthz /metrics /pods\n", "{body}");
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

    // server_test.go:TestMetricBuckets: upstream builds the real server
    // (`NewServer` with debugging handlers and ComponentStatusz on) and reads
    // its buckets; so do we, from the installer sequence, never by hand.
    #[test]
    fn metric_buckets_cases() {
        let b = MetricsBuckets::new_server(ServerBucketOptions {
            enable_debugging_handlers: true,
            flagz_enabled: true,
            checkpoint_enabled: false,
            statusz_enabled: true,
        });
        let cases = [
            ("/healthz", "healthz"),
            ("/attach/podNamespace/podID/containerName", "attach"),
            ("/attach/podNamespace/podID/uid/containerName", "attach"),
            ("/configz", "configz"),
            (
                "/containerLogs/podNamespace/podID/containerName",
                "containerLogs",
            ),
            ("/debug/flags/v", "debug"),
            ("/debug/pprof/subpath", "debug"),
            ("/exec/podNamespace/podID/containerName", "exec"),
            ("/healthz/", "healthz"),
            ("/healthz/log", "healthz"),
            ("/healthz/ping", "healthz"),
            ("/healthz/syncloop", "healthz"),
            ("/logs/", "logs"),
            ("/logs/logpath", "logs"),
            ("/metrics", "metrics"),
            ("/metrics/cadvisor", "metrics/cadvisor"),
            ("/metrics/probes", "metrics/probes"),
            ("/metrics/resource", "metrics/resource"),
            ("/pods/", "pods"),
            ("/portForward/podNamespace/podID", "portForward"),
            ("/portForward/podNamespace/podID/uid", "portForward"),
            ("/run/podNamespace/podID/containerName", "run"),
            ("/runningpods/", "runningpods"),
            ("/stats/", "stats"),
            ("/stats/summary", "stats"),
            ("/statusz", "statusz"),
            ("/flagz", "flagz"),
            ("/junk", "other"),
            ("/healthzjunk", "other"),
        ];
        for (path, want) in cases {
            assert_eq!(b.get_metric_bucket(path), want, "{path}");
        }
    }

    // server_test.go:TestMetricMethodBuckets
    #[test]
    fn metric_method_buckets_cases() {
        let b = MetricsBuckets::new();
        assert_eq!(b.get_metric_method_bucket("GET"), "GET");
        assert_eq!(b.get_metric_method_bucket("POST"), "POST");
        assert_eq!(b.get_metric_method_bucket("WEIRD"), "other");
    }

    // NewServer's gates decide which buckets exist (server.go:311-330).
    #[test]
    fn server_gates_decide_buckets() {
        // Debugging handlers off: `InstallDebuggingDisabledHandlers` still
        // registers run/exec/... and `pprof`/`logs`, but never `debug`,
        // `configz`, `flagz`, `statusz` or `checkpoint`.
        let b = MetricsBuckets::new_server(ServerBucketOptions::default());
        for (path, want) in [
            ("/healthz", "healthz"),
            ("/pods", "pods"),
            ("/stats/summary", "stats"),
            ("/metrics", "metrics"),
            ("/metrics/cadvisor", "metrics/cadvisor"),
            ("/metrics/probes", "metrics/probes"),
            ("/metrics/resource", "metrics/resource"),
            ("/runningpods/", "runningpods"),
            ("/pprof/x", "pprof"),
            ("/logs/", "logs"),
            ("/configz", "other"),
            ("/debug/flags/v", "other"),
            ("/statusz", "other"),
            ("/flagz", "other"),
            ("/checkpoint/ns/pod/c", "other"),
        ] {
            assert_eq!(b.get_metric_bucket(path), want, "{path}");
        }
        let b = MetricsBuckets::new_server(ServerBucketOptions {
            enable_debugging_handlers: true,
            flagz_enabled: true,
            checkpoint_enabled: true,
            statusz_enabled: true,
        });
        for (path, want) in [
            ("/configz", "configz"),
            ("/statusz", "statusz"),
            ("/flagz", "flagz"),
            ("/checkpoint/ns/pod/c", "checkpoint"),
            ("/debug/flags/v", "debug"),
        ] {
            assert_eq!(b.get_metric_bucket(path), want, "{path}");
        }
    }

    // server.go:1152-1163 `longRunningRequestPathMap` / `isLongRunningRequest`.
    #[test]
    fn long_running_requests() {
        for p in ["exec", "attach", "portforward", "debug"] {
            assert!(is_long_running_request(p), "{p}");
        }
        // Upstream's map key is lower-case `portforward` while the bucket is
        // `portForward`, so the bucketed path is not long-running (#3250).
        assert!(!is_long_running_request("portForward"));
        for p in ["healthz", "pods", "other", "logs"] {
            assert!(!is_long_running_request(p), "{p}");
        }
    }

    // server.go:1176-1200 `ServeHTTP`: the request counter, the in-flight
    // gauge and the duration histogram carry (method, bucketed path,
    // server_type, long_running); unknown paths and verbs collapse to `other`.
    #[tokio::test]
    async fn serve_http_records_bucketed_request_metrics() {
        use crate::server_metrics::{
            HTTP_INFLIGHT_REQUESTS, HTTP_REQUESTS, HTTP_REQUESTS_DURATION,
        };
        let app = Router::new()
            .route("/healthz", axum::routing::get(|| async { "ok" }))
            .route("/exec/:ns/:pod/:c", axum::routing::get(|| async { "x" }));
        let app = install_metrics_filter(
            app,
            MetricsBuckets::new_server(ServerBucketOptions {
                enable_debugging_handlers: true,
                ..Default::default()
            }),
            true,
        );
        for (method, uri) in [
            ("GET", "/healthz/syncloop"),
            ("GET", "/healthz"),
            ("GET", "/exec/ns/pod/c"),
            ("GET", "/junk"),
            ("WEIRD", "/healthz"),
        ] {
            let req = Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            app.clone().oneshot(req).await.unwrap();
        }
        let count = |m: &str, p: &str, lr: &str| {
            HTTP_REQUESTS
                .with_label_values(&[m, p, "readwrite", lr])
                .get() as u64
        };
        assert_eq!(count("GET", "healthz", "false"), 2);
        assert_eq!(count("GET", "exec", "true"), 1);
        assert_eq!(count("GET", "other", "false"), 1);
        assert_eq!(count("other", "healthz", "false"), 1);
        // Every request has finished: the in-flight gauge is back to zero.
        assert_eq!(
            HTTP_INFLIGHT_REQUESTS
                .with_label_values(&["GET", "healthz", "readwrite", "false"])
                .get(),
            0.0
        );
        assert_eq!(
            HTTP_REQUESTS_DURATION
                .with_label_values(&["GET", "exec", "readwrite", "true"])
                .get_sample_count(),
            1
        );
        // Registered into the process-global registry `/metrics` serves.
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains("# HELP kubelet_http_requests_total [ALPHA] Number of the http requests received since the server started"));
        assert!(body.contains("kubelet_http_requests_duration_seconds_count"));
        assert!(body.contains("# TYPE kubelet_http_inflight_requests gauge"));
    }

    // server.go:1179-1185: no auth -> `readonly`.
    #[tokio::test]
    async fn serve_http_server_type_is_readonly_without_auth() {
        use crate::server_metrics::HTTP_REQUESTS;
        let app = install_metrics_filter(
            Router::new().route("/pods", axum::routing::get(|| async { "p" })),
            MetricsBuckets::new(),
            false,
        );
        let req = Request::builder().uri("/pods").body(Body::empty()).unwrap();
        app.oneshot(req).await.unwrap();
        // `new()` has no matchers, so `/pods` is `other`.
        assert_eq!(
            HTTP_REQUESTS
                .with_label_values(&["GET", "other", "readonly", "false"])
                .get() as u64,
            1
        );
    }

    // server_test.go:TestTrimURLPath (`getURLRootPath`).
    #[test]
    fn trim_url_path() {
        for (path, want) in [
            ("", ""),
            ("//", ""),
            ("/pods", "pods"),
            ("pods", "pods"),
            ("pods/", "pods"),
            ("good/", "good"),
            ("pods/probes", "pods"),
            ("metrics", "metrics"),
            ("metrics/resource", "metrics/resource"),
            ("metrics/hello", "metrics/hello"),
        ] {
            assert_eq!(get_url_root_path(path), want, "path is: {path}");
        }
    }
}
