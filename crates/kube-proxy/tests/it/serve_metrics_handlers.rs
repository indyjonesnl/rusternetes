//! Upstream handler regressions for cmd/kube-proxy/app/server.go:465-481.
use axum::{body::Body, http::Request, Router};
use rusternetes_kube_proxy::serve_metrics::metrics_router;
use tower::ServiceExt;

async fn request(app: Router, method: &str, path: &str) -> (u16, axum::http::HeaderMap, String) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, String::from_utf8(body.to_vec()).unwrap())
}

// apiserver/pkg/server/healthz/healthz_test.go:37-56,99-109: InstallHandler
// without checkers installs ping, and verbose/exclude affect root output only.
#[tokio::test]
#[serial_test::serial]
async fn metrics_healthz_ping_and_query_options() {
    for (path, expected) in [
        ("/healthz", "ok"),
        ("/healthz/ping", "ok"),
        ("/healthz?verbose", "[+]ping ok\nhealthz check passed\n"),
        ("/healthz?verbose=false&exclude=ping", "[+]ping excluded: ok\nhealthz check passed\n"),
        ("/healthz?verbose&exclude=dontexist", "[+]ping ok\nwarn: some health checks cannot be excluded: no matches for \"dontexist\"\nhealthz check passed\n"),
    ] {
        let (status, headers, body) = request(metrics_router("iptables"), "GET", path).await;
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(headers["content-type"], "text/plain; charset=utf-8");
        assert_eq!(body, expected, "{path}");
    }
}

// configz/configz_test.go:38-105: unset registry => {}, JSON, no newline.
#[tokio::test]
#[serial_test::serial]
async fn configz_serves_registered_configs_as_json() {
    for method in ["GET", "POST"] {
        let (status, headers, body) = request(metrics_router("iptables"), method, "/configz").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["x-content-type-options"], "nosniff");
        assert_eq!(body, "{}");
    }
}

// server.go:474 and legacyregistry/registry.go:60-65: expose registry metrics.
#[tokio::test]
#[serial_test::serial]
async fn legacy_metrics_and_separate_sli_registry() {
    rusternetes_common::compat_version_metrics::record_compat_version_info(
        "serve-metrics-regression",
        "1.35",
        "1.35",
        "1.34",
    );
    let app = metrics_router("iptables");
    let (status, _, body) = request(app.clone(), "GET", "/metrics").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("version_info"), "{body}");
    let (status, _, body) = request(app.clone(), "GET", "/healthz").await;
    assert_eq!(status, 200, "{body}");
    let (status, _, body) = request(app.clone(), "GET", "/metrics/slis").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains("kubernetes_healthcheck{name=\"ping\",type=\"healthz\"} 1"),
        "{body}"
    );
    assert!(body.contains("status=\"success\""), "{body}");
    assert!(!body.contains("version_info"), "{body}");
    // component-base/metrics/http.go:77-85: DELETE resets only the SLI registry.
    let (status, _, body) = request(app.clone(), "DELETE", "/metrics/slis").await;
    assert_eq!(status, 200);
    assert_eq!(body, "metrics reset\n");
    let (_, _, body) = request(app.clone(), "GET", "/metrics/slis").await;
    assert!(!body.contains("kubernetes_healthcheck{"), "{body}");
    let (_, _, body) = request(app, "GET", "/metrics").await;
    assert!(body.contains("version_info"), "{body}");
}
