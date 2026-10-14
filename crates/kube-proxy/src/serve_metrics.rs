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

/// The mux of `serveMetrics` (server.go:463-493), without the listener.
pub fn metrics_router(proxy_mode: &str) -> Router {
    let proxy_mode = proxy_mode.to_string();
    let router = Router::new().route(
        "/proxyMode",
        axum::routing::get(move || {
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
    );
    // `ListedPaths()` is read before `statusz.Install`, so it lists what the
    // mux has registered so far.
    rusternetes_common::statusz::install_if_enabled(router, KUBE_PROXY, || {
        rusternetes_common::statusz::Registry::new(Some(
            rusternetes_common::compatibility::default_build_effective_version(),
        ))
        .with_listed_paths(vec!["/proxyMode".to_string()])
    })
}

/// `serveMetrics` (server.go:459-518): an empty bind address disables the
/// server; otherwise it is (re)started every 5s until it serves
/// (`wait.UntilWithContext(ctx, fn, 5*time.Second)`), logging each failure.
pub fn serve_metrics(bind_address: String, proxy_mode: &'static str) {
    if bind_address.is_empty() {
        return;
    }
    tokio::spawn(async move {
        loop {
            match tokio::net::TcpListener::bind(&bind_address).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, metrics_router(proxy_mode)).await {
                        tracing::error!("starting metrics server failed: {e}");
                    }
                }
                Err(e) => tracing::error!("starting metrics server failed: {e}"),
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
