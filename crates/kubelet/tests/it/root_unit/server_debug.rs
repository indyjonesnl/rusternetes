//! Unit tests moved from `src/server_debug.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::body::Bytes;
    pub use axum::http::{header, HeaderValue, Method, StatusCode, Uri};
    pub use axum::response::{IntoResponse, Response};
    pub use axum::routing::any;
    pub use axum::Router;
    pub use rusternetes_kubelet::server::disabled_endpoint;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_kubelet::server_debug::*;
    use tower::ServiceExt;

    async fn call(app: &Router, method: &str, path: &str, body: &str) -> (StatusCode, String) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn echo_setter() -> StringFlagSetter {
        Arc::new(|v| {
            parse_uint32(v)?;
            Ok(format!("successfully set klog.logging.verbosity to {v}"))
        })
    }

    /// TestDisablingLogAndProfilingHandler (server_test.go:1835-1847).
    #[tokio::test]
    async fn disabled_profiling_and_flags_answer_405() {
        let app = install_debug_flags_handler(
            install_profiling_handler(Router::new(), false, false),
            false,
            echo_setter(),
        );
        for (m, p, want) in [
            (
                "GET",
                "/debug/pprof/profile?seconds=2",
                "profiling endpoint is disabled.\n",
            ),
            ("GET", "/debug/pprof/", "profiling endpoint is disabled.\n"),
            (
                "POST",
                "/debug/pprof/heap",
                "profiling endpoint is disabled.\n",
            ),
            ("GET", "/debug/flags/v", "flags endpoint is disabled.\n"),
            ("PUT", "/debug/flags/v", "flags endpoint is disabled.\n"),
        ] {
            let (status, body) = call(&app, m, p, "").await;
            assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{m} {p}");
            assert_eq!(body, want, "{m} {p}");
        }
    }

    #[tokio::test]
    async fn enabled_profiling_serves_pprof() {
        let app = install_profiling_handler(Router::new(), true, false);
        let (s, b) = call(&app, "GET", "/debug/pprof/", "").await;
        assert_eq!(s, StatusCode::OK);
        assert!(b.contains("Types of profiles available"));
        let (s, b) = call(&app, "GET", "/debug/pprof/cmdline", "").await;
        assert_eq!(s, StatusCode::OK);
        assert!(!b.is_empty());
        let (s, b) = call(&app, "GET", "/debug/pprof/symbol", "").await;
        assert_eq!((s, b.as_str()), (StatusCode::OK, "num_symbols: 0\n"));
        let (s, b) = call(&app, "GET", "/debug/pprof/nope", "").await;
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::NOT_FOUND, "Unknown profile\n")
        );
        let (s, _) = call(&app, "GET", "/debug/pprof/profile", "").await;
        assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
        let (s, _) = call(&app, "POST", "/debug/pprof/cmdline", "").await;
        assert_eq!(s, StatusCode::METHOD_NOT_ALLOWED);
    }

    /// `StringFlagPutHandler`: PUT sets, other methods 406, bad value 400.
    #[tokio::test]
    async fn enabled_flags_handler_puts_verbosity() {
        let app = install_debug_flags_handler(Router::new(), true, echo_setter());
        let (s, b) = call(&app, "PUT", "/debug/flags/v", "4").await;
        assert_eq!(
            (s, b.as_str()),
            (
                StatusCode::OK,
                "successfully set klog.logging.verbosity to 4\n"
            )
        );
        let (s, b) = call(&app, "GET", "/debug/flags/v", "").await;
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::NOT_ACCEPTABLE, "unsupported http method\n")
        );
        let (s, b) = call(&app, "PUT", "/debug/flags/v", "abc").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(b, "strconv.ParseUint: parsing \"abc\": invalid syntax\n");
        let (s, b) = call(&app, "PUT", "/debug/flags/v", "99999999999").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(b.contains("value out of range"));
    }

    #[test]
    fn glog_setter_rejects_non_numeric() {
        assert!(glog_setter("-1").is_err());
        assert!(glog_setter("").is_err());
    }
}
