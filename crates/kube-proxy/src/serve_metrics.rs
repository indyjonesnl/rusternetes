//! kube-proxy's metrics/status HTTP server.
//!
//! Port of `serveMetrics` (cmd/kube-proxy/app/server.go:459-518) and the
//! `MetricsBindAddress` default (pkg/proxy/apis/config/v1alpha1/defaults.go:52-56).
//! Upstream `healthz.InstallHandler`, SLI reset, legacy metrics and configz
//! are installed before statusz captures ListedPaths. Flagz and runtime
//! profiling/debug flags are tracked in #3556 and #3557.

mod configz;
mod exposition;
mod health;

pub use configz::ConfigRegistry;
pub use health::ProxyHealthServer;

use axum::{
    extract::{RawQuery, State},
    http::{HeaderMap, Method, StatusCode},
    response::IntoResponse,
    routing::any,
    Router,
};
use std::sync::{Arc, OnceLock};

/// config.GroupName (pkg/proxy/apis/config/register.go:25).
pub const CONFIG_GROUP: &str = "kubeproxy.config.k8s.io";

/// Process-wide registrations, like configz.configs (configz.go:52-54).
pub fn config_registry() -> Arc<ConfigRegistry> {
    static REGISTRY: OnceLock<Arc<ConfigRegistry>> = OnceLock::new();
    Arc::clone(REGISTRY.get_or_init(|| Arc::new(ConfigRegistry::default())))
}

/// `ports.ProxyStatusPort` (pkg/cluster/ports/ports.go).
pub const PROXY_STATUS_PORT: u16 = 10249;
/// `defaultMetricsAddress` (defaults.go).
pub const DEFAULT_METRICS_ADDRESS: &str = "127.0.0.1";
/// `kubeProxy` (server.go): the component name statusz is installed with.
pub const KUBE_PROXY: &str = "kube-proxy";

/// Whether Go's `net.SplitHostPort` succeeds (the only thing
/// `AppendPortIfNeeded` uses it for): `host:port` or `[host]:port` with no
/// stray colons or brackets.
fn split_host_port_ok(addr: &str) -> bool {
    let Some(i) = addr.rfind(':') else {
        return false; // missing port in address
    };
    if let Some(rest) = addr.strip_prefix('[') {
        // "[host]:port": the ']' must directly precede the last colon.
        let Some(end) = rest.find(']') else {
            return false; // missing ']' in address
        };
        if end + 1 != i {
            return false;
        }
        !rest[..end].contains(['[', ']']) && !addr[i + 1..].contains([':', '[', ']'])
    } else {
        // Unbracketed: any colon before the last one is a "too many colons".
        !addr[..i].contains([':', '[', ']']) && !addr[i + 1..].contains(['[', ']'])
    }
}

/// `proxyutil.AppendPortIfNeeded` (pkg/proxy/util/utils.go:157-176).
pub fn append_port_if_needed(addr: &str, port: u16) -> String {
    // Return if address is already in "ipv4:port" or "[ipv6]:port" format.
    if split_host_port_ok(addr) {
        return addr.to_string();
    }
    // Simply return for invalid case. This should be caught by validation instead.
    match addr.parse::<std::net::IpAddr>() {
        Err(_) => addr.to_string(),
        Ok(std::net::IpAddr::V4(_)) => format!("{addr}:{port}"),
        Ok(std::net::IpAddr::V6(_)) => format!("[{addr}]:{port}"),
    }
}

/// The `MetricsBindAddress` defaulting of defaults.go:52-56.
pub fn default_metrics_bind_address(addr: &str) -> String {
    if addr.is_empty() {
        format!("{DEFAULT_METRICS_ADDRESS}:{PROXY_STATUS_PORT}")
    } else {
        append_port_if_needed(addr, PROXY_STATUS_PORT)
    }
}

/// No explicit checks are passed to InstallHandler, so only ping is installed
/// (healthz.go:202-205). Verbose is controlled by parameter presence, not value;
/// excludes are repeated parameters, and unmatched names are sorted/quoted
/// (healthz.go:259-265,279-305,322-330).
async fn ping_health(RawQuery(query): RawQuery) -> impl IntoResponse {
    let params: Vec<_> =
        url::form_urlencoded::parse(query.as_deref().unwrap_or_default().as_bytes()).collect();
    let verbose = params.iter().any(|(key, _)| key == "verbose");
    let excluded: std::collections::BTreeSet<_> = params
        .iter()
        .filter(|(key, _)| key == "exclude")
        .map(|(_, value)| value.as_ref())
        .collect();
    let mut body = if excluded.contains("ping") {
        "[+]ping excluded: ok\n".to_owned()
    } else {
        exposition::observe_ping();
        "[+]ping ok\n".to_owned()
    };
    let unknown: Vec<_> = excluded
        .iter()
        .filter(|name| **name != "ping")
        .map(|name| format!("{name:?}"))
        .collect();
    if !unknown.is_empty() {
        body.push_str(&format!(
            "warn: some health checks cannot be excluded: no matches for {}\n",
            unknown.join(",")
        ));
    }
    if verbose {
        body.push_str("healthz check passed\n");
    } else {
        body = "ok".to_owned();
    }
    (
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("X-Content-Type-Options", "nosniff"),
        ],
        body,
    )
}

/// The mux of serveMetrics (server.go:464-493), without its listener.
pub fn metrics_router(proxy_mode: &str) -> Router {
    metrics_router_with_config(proxy_mode, config_registry())
}

/// Install the metrics mux with a config registry (configz.go:65-66).
pub fn metrics_router_with_config(proxy_mode: &str, configs: Arc<ConfigRegistry>) -> Router {
    exposition::initialize();
    let proxy_mode = proxy_mode.to_owned();
    let router = Router::new()
        .route("/healthz", any(ping_health))
        // Individual checks do not observe SLI metrics (healthz.go:335-343).
        .route(
            "/healthz/ping",
            any(|| async { ([("Content-Type", "text/plain; charset=utf-8")], "ok") }),
        )
        .route(
            "/metrics/slis",
            any(|headers: HeaderMap, method: Method| async move {
                exposition::render(headers, true, method)
            }),
        )
        .route(
            "/proxyMode",
            any(move || {
                let proxy_mode = proxy_mode.clone();
                async move {
                    (
                        [
                            ("Content-Type", "text/plain; charset=utf-8"),
                            ("X-Content-Type-Options", "nosniff"),
                        ],
                        proxy_mode,
                    )
                }
            }),
        )
        .route(
            "/metrics",
            any(|headers: HeaderMap, method: Method| async move {
                exposition::render(headers, false, method)
            }),
        )
        .route(
            "/configz",
            any(|State(configs): State<Arc<ConfigRegistry>>| async move {
                match configs.json() {
                    Ok(body) => (
                        [
                            ("Content-Type", "application/json"),
                            ("X-Content-Type-Options", "nosniff"),
                        ],
                        body,
                    )
                        .into_response(),
                    Err(error) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("error marshaling json: {error}\n"),
                    )
                        .into_response(),
                }
            }),
        )
        .with_state(configs);
    // ListedPaths excludes neither healthz/ping nor metrics/slis and is sorted
    // (mux/pathrecorder.go:98-105). Read before statusz.Install (server.go:490).
    rusternetes_common::statusz::install_if_enabled(router, KUBE_PROXY, || {
        rusternetes_common::statusz::Registry::new(Some(
            rusternetes_common::compatibility::default_build_effective_version(),
        ))
        .with_listed_paths(
            vec![
                "/configz",
                "/healthz",
                "/healthz/ping",
                "/metrics",
                "/metrics/slis",
                "/proxyMode",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        )
    })
}

/// Listener settings kept separate from the existing KubeProxyConfig so callers
/// of the library retain their config shape while the standalone CLI can set
/// HealthzBindAddress and BindAddressHardFail (server.go:244-245,551-561).
#[derive(Clone, Debug)]
pub struct DiagnosticsOptions {
    pub healthz_bind_address: String,
    pub bind_address_hard_fail: bool,
}

impl Default for DiagnosticsOptions {
    fn default() -> Self {
        Self {
            healthz_bind_address: "0.0.0.0:10256".to_owned(),
            bind_address_hard_fail: false,
        }
    }
}

/// `serveMetrics` (server.go:459-521): empty disables, otherwise retry every 5s.
pub fn serve_metrics(bind_address: String, proxy_mode: &'static str) {
    serve_metrics_with_errors(bind_address, proxy_mode, None);
}

pub(crate) fn serve_metrics_with_errors(
    bind_address: String,
    proxy_mode: &'static str,
    errors: Option<tokio::sync::mpsc::UnboundedSender<anyhow::Error>>,
) {
    if bind_address.is_empty() {
        return;
    }
    spawn_server(
        bind_address,
        metrics_router(proxy_mode),
        "starting metrics server failed",
        errors,
    );
}

/// server.go:442-456,495-521: retry listener errors every five seconds; hardfail
/// sends one error and never retries. The receiver is handled by run_proxy.
pub(crate) fn spawn_server(
    bind_address: String,
    router: Router,
    label: &'static str,
    errors: Option<tokio::sync::mpsc::UnboundedSender<anyhow::Error>>,
) {
    tokio::spawn(async move {
        loop {
            let error = match tokio::net::TcpListener::bind(&bind_address).await {
                Ok(listener) => axum::serve(listener, router.clone())
                    .await
                    .err()
                    .map(anyhow::Error::from),
                Err(error) => Some(anyhow::Error::from(error)),
            };
            if let Some(error) = error {
                tracing::error!("{label}: {error}");
                if let Some(errors) = &errors {
                    let _ = errors.send(error.context(label));
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_common::feature_gates::{with_feature, Feature};
    use serial_test::serial;
    use tower::ServiceExt;

    async fn get(app: Router, path: &str, accept: &str) -> (u16, String) {
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("Accept", accept)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    // server_test.go:TestStatuszRegistryReceivesListedPaths: the Paths line
    // carries the mux's listed paths (read before /statusz is installed, as
    // `proxyMux.ListedPaths()` is evaluated before `statusz.Install`).
    #[tokio::test]
    #[serial]
    async fn statusz_installed_with_listed_paths() {
        let _g = with_feature(Feature::ComponentStatusz, true);
        let (status, body) = get(metrics_router("iptables"), "/statusz", "text/plain").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("kube-proxy statusz"), "{body}");
        let line = body
            .lines()
            .map(str::trim)
            .find(|l| l.starts_with("Paths"))
            .unwrap_or_else(|| panic!("no Paths line: {body}"));
        let got: Vec<&str> = line.split_whitespace().skip(1).collect();
        // statusz/statusz.go:237-256 aggregates nested handlers to their
        // first path segment before rendering the Paths line.
        assert_eq!(
            got,
            vec!["/configz", "/healthz", "/metrics", "/proxyMode"],
            "{body}"
        );
    }

    // server.go:487: gate off => no /statusz.
    #[tokio::test]
    #[serial]
    async fn statusz_absent_when_gate_off() {
        let _g = with_feature(Feature::ComponentStatusz, false);
        let (status, _) = get(metrics_router("iptables"), "/statusz", "text/plain").await;
        assert_eq!(status, 404);
    }

    // server.go:470-474
    #[tokio::test]
    async fn proxy_mode_handler() {
        let (status, body) = get(metrics_router("iptables"), "/proxyMode", "").await;
        assert_eq!(status, 200);
        assert_eq!(body, "iptables");
    }

    // utils_test.go:TestAppendPortIfNeeded
    #[test]
    fn append_port() {
        for (addr, port, want) in [
            ("0.0.0.0:12345", 23456, "0.0.0.0:12345"),
            ("9.8.7.6", 12345, "9.8.7.6:12345"),
            ("[::]:12345", 23456, "[::]:12345"),
            ("fd00:1::5", 23456, "[fd00:1::5]:23456"),
            ("[fd00:1::5]", 12345, "[fd00:1::5]"),
        ] {
            assert_eq!(append_port_if_needed(addr, port), want, "{addr}");
        }
    }

    // defaults.go:52-56
    #[test]
    fn metrics_bind_address_default() {
        assert_eq!(default_metrics_bind_address(""), "127.0.0.1:10249");
        assert_eq!(default_metrics_bind_address("0.0.0.0"), "0.0.0.0:10249");
        assert_eq!(default_metrics_bind_address("0.0.0.0:99"), "0.0.0.0:99");
    }
}

#[cfg(test)]
mod listener_tests {
    use super::*;

    // serveMetrics/serveHealthz error channels send once and never retry in
    // hardfail mode (server.go:446-450,500-504).
    #[tokio::test]
    async fn hardfail_listener_sends_error_once() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = occupied.local_addr().unwrap().to_string();
        let (errors, mut received) = tokio::sync::mpsc::unbounded_channel();
        spawn_server(
            address,
            Router::new(),
            "healthz server failed",
            Some(errors),
        );
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(error.to_string().contains("healthz server failed"));
        assert!(received.recv().await.is_none());
    }
}
