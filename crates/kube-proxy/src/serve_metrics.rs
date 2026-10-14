//! kube-proxy's metrics/status HTTP server.
//!
//! Port of `serveMetrics` (cmd/kube-proxy/app/server.go:459-518) and the
//! `MetricsBindAddress` default (pkg/proxy/apis/config/v1alpha1/defaults.go:52-56).
//! Only the `/proxyMode` and `/statusz` handlers exist here so far; the
//! `healthz`, `/metrics`, `configz`, `flagz` and profiling handlers `serveMetrics`
//! also installs are not ported yet.

use axum::Router;

/// `ports.ProxyStatusPort` (pkg/cluster/ports/ports.go).
pub const PROXY_STATUS_PORT: u16 = 10249;
/// `defaultMetricsAddress` (defaults.go).
pub const DEFAULT_METRICS_ADDRESS: &str = "127.0.0.1";
/// `kubeProxy` (server.go): the component name statusz is installed with.
pub const KUBE_PROXY: &str = "kube-proxy";

/// `proxyutil.AppendPortIfNeeded` (pkg/proxy/util/utils.go:157-176).
pub fn append_port_if_needed(addr: &str, _port: u16) -> String {
    addr.to_string()
}

/// The `MetricsBindAddress` defaulting of defaults.go:52-56.
pub fn default_metrics_bind_address(addr: &str) -> String {
    addr.to_string()
}

/// The mux of `serveMetrics` (without the listener).
pub fn metrics_router(_proxy_mode: &str) -> Router {
    Router::new()
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
        assert_eq!(got, vec!["/proxyMode"], "{body}");
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
