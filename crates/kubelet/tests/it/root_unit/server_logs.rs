//! Unit tests moved from `src/server_logs.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        http::{header, StatusCode, Uri},
        response::{IntoResponse, Response},
        routing::{any, get, MethodRouter},
        Router,
    };
    pub use futures::StreamExt;
    pub use std::path::{Path, PathBuf};
    pub use tokio::io::AsyncReadExt;
}

mod tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use rusternetes_kubelet::server_logs::*;
    use tower::ServiceExt;

    async fn call(app: &Router, method: Method, path: &str) -> (u16, String, Option<String>) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let loc = resp
            .headers()
            .get("location")
            .map(|v| v.to_str().unwrap().to_string());
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).to_string(), loc)
    }

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let outer = tempfile::tempdir().unwrap();
        std::fs::write(outer.path().join("secret"), "TOP-SECRET").unwrap();
        let root = outer.path().join("log");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("kubelet.log"), "line1\n").unwrap();
        std::fs::write(root.join("google.log"), "g\n").unwrap();
        std::fs::create_dir(root.join("pods")).unwrap();
        (outer, root)
    }

    // server_test.go:TestServeLogs -- directory index lists the files.
    #[tokio::test]
    async fn serves_directory_index_and_files() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, false, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/").await;
        assert_eq!(st, 200, "{body}");
        assert!(
            body.contains("<a href=\"kubelet.log\">kubelet.log</a>"),
            "{body}"
        );
        assert!(
            body.contains("<a href=\"google.log\">google.log</a>"),
            "{body}"
        );
        assert!(body.contains("<a href=\"pods/\">pods/</a>"), "{body}");
        let (st, body, _) = call(&app, Method::GET, "/logs/kubelet.log").await;
        assert_eq!((st, body.as_str()), (200, "line1\n"));
        let (st, _, _) = call(&app, Method::GET, "/logs/missing.log").await;
        assert_eq!(st, 404);
    }

    // http.FileServer redirects a directory without trailing slash.
    #[tokio::test]
    async fn directory_without_slash_redirects() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, false, root);
        let (st, _, loc) = call(&app, Method::GET, "/logs/pods").await;
        assert_eq!((st, loc.as_deref()), (301, Some("pods/")));
    }

    // http.FileServer cleans the path: it never escapes the root.
    #[tokio::test]
    async fn path_traversal_stays_inside_root() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, false, root);
        for p in [
            "/logs/../secret",
            "/logs/pods/../../secret",
            "/logs/%2e%2e/secret",
        ] {
            let (st, body, _) = call(&app, Method::GET, p).await;
            assert!(!body.contains("TOP-SECRET"), "{p} leaked: {st} {body}");
        }
    }

    // server_test.go:TestDebuggingDisabledHandlers (/logs/kubelet.log arm).
    #[tokio::test]
    async fn debugging_disabled_overrides_system_log_handler() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), false, true, false, root);
        for m in [Method::GET, Method::POST] {
            let (st, body, _) = call(&app, m, "/logs/kubelet.log").await;
            assert_eq!(
                (st, body.as_str()),
                (405, "Debug endpoints are disabled.\n")
            );
        }
        let (st, body, _) = call(&app, Method::GET, "/logs/").await;
        assert_eq!(
            (st, body.as_str()),
            (405, "Debug endpoints are disabled.\n")
        );
    }

    // server_test.go:TestDisablingLogAndProfilingHandler (/logs arm).
    #[tokio::test]
    async fn system_log_handler_disabled() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, false, false, root);
        for m in [Method::GET, Method::POST] {
            let (st, body, _) = call(&app, m, "/logs/kubelet.log").await;
            assert_eq!((st, body.as_str()), (405, "logs endpoint is disabled.\n"));
        }
    }

    // v1beta1/defaults.go:122-124,280-282: nil => true.
    #[test]
    fn config_defaults_enable_handlers() {
        let c = rusternetes_kubelet::config::KubeletConfiguration::default();
        assert!(c.debugging_handlers_enabled() && c.system_log_handler_enabled());
        let c: rusternetes_kubelet::config::KubeletConfiguration =
            serde_yaml::from_str("enableDebuggingHandlers: false\nenableSystemLogHandler: false\n")
                .unwrap();
        assert!(!c.debugging_handlers_enabled() && !c.system_log_handler_enabled());
    }

    async fn call_h(
        app: &Router,
        path: &str,
        accept_encoding: Option<&str>,
    ) -> (u16, axum::http::HeaderMap, Vec<u8>) {
        let mut req = Request::builder().method(Method::GET).uri(path);
        if let Some(e) = accept_encoding {
            req = req.header("accept-encoding", e);
        }
        let resp = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, headers, body.to_vec())
    }

    // kubelet.go:1784-1812: query mode off => `?query=` is ignored and the
    // plain file server answers.
    #[tokio::test]
    async fn query_ignored_when_query_mode_off() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, false, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/?query=a%2Fb&tailLines=x").await;
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("kubelet.log"), "{body}");
    }

    // newNodeLogQuery errors => 400 with the aggregated message.
    #[tokio::test]
    async fn query_mode_parse_error_is_400() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, true, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/?query=kubelet&tailLines=abc").await;
        assert_eq!(st, 400, "{body}");
        assert!(body.contains("tailLines"), "{body}");
    }

    // A path plus a query => 406 "path not allowed in query mode".
    #[tokio::test]
    async fn query_mode_rejects_path() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, true, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/kubelet.log?query=kubelet").await;
        assert_eq!(
            (st, body.as_str()),
            (406, "path not allowed in query mode\n")
        );
    }

    // nlq.validate() failures => 406 (file with options).
    #[tokio::test]
    async fn query_mode_validation_error_is_406() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, true, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/?query=pods%2Fa.log&tailLines=5").await;
        assert_eq!(st, 406, "{body}");
        assert!(body.contains("cannot specify file with options"), "{body}");
        // Options with no query.
        let (st, body, _) = call(&app, Method::GET, "/logs/?tailLines=5").await;
        assert_eq!(st, 406, "{body}");
        assert!(body.contains("cannot be empty with options"), "{body}");
    }

    // A single-file query is served by the file server at that path.
    #[tokio::test]
    async fn query_mode_single_file_is_served() {
        let (_o, root) = fixture();
        std::fs::write(root.join("pods").join("a.log"), "pod-line\n").unwrap();
        let app = install_logs_handlers(Router::new(), true, true, true, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/?query=pods%2Fa.log").await;
        assert_eq!((st, body.as_str()), (200, "pod-line\n"));
        // A file escaping the log dir fails validation.
        let (st, body, _) = call(&app, Method::GET, "/logs/?query=..%2Fsecret").await;
        assert_eq!(st, 406, "{body}");
        assert!(!body.contains("TOP-SECRET"), "{body}");
    }

    // No recognised parameter => falls back to the file server.
    #[tokio::test]
    async fn query_mode_without_query_serves_files() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, true, root);
        let (st, body, _) = call(&app, Method::GET, "/logs/kubelet.log").await;
        assert_eq!((st, body.as_str()), (200, "line1\n"));
    }

    // journalServer.ServeHTTP headers: plain, then gzip only for an exact
    // `Accept-Encoding: gzip`. The body is whatever the node's journal
    // yields (or the "failed to get logging cmd"/"journal output not
    // available" line), so only the transport is asserted.
    #[tokio::test]
    async fn query_mode_service_streams_with_gzip_negotiation() {
        use std::io::Read;
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, true, root);
        let (st, h, _) = call_h(&app, "/logs/?query=kubelet", None).await;
        assert_eq!(st, 200);
        assert_eq!(h["content-type"], "text/plain;charset=UTF-8");
        assert!(h.get("content-encoding").is_none());

        let (st, h, body) = call_h(&app, "/logs/?query=kubelet", Some("gzip")).await;
        assert_eq!(st, 200);
        assert_eq!(h["content-encoding"], "gzip");
        let mut out = String::new();
        flate2::read::GzDecoder::new(&body[..])
            .read_to_string(&mut out)
            .expect("valid gzip stream");

        let (_, h, _) = call_h(&app, "/logs/?query=kubelet", Some("gzip, deflate")).await;
        assert!(h.get("content-encoding").is_none());
    }

    // validation.go:355-361 ValidateKubeletConfiguration.
    #[test]
    #[serial_test::serial]
    fn config_validation_for_system_log_query() {
        use rusternetes_common::feature_gates::{reset_to_defaults, set_from_str};
        use rusternetes_kubelet::config::KubeletConfiguration;
        reset_to_defaults();
        let cfg = |y: &str| -> KubeletConfiguration {
            serde_yaml::from_str(&format!(
                "apiVersion: kubelet.config.k8s.io/v1beta1\nkind: KubeletConfiguration\n{y}"
            ))
            .unwrap()
        };
        assert!(!KubeletConfiguration::default().system_log_query_enabled());
        let err = cfg("enableSystemLogQuery: true\n").validate().unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: NodeLogQuery feature gate is required for enableSystemLogQuery"
        );
        set_from_str("NodeLogQuery=true").unwrap();
        let err = cfg("enableSystemLogQuery: true\nenableSystemLogHandler: false\n")
            .validate()
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid configuration: enableSystemLogHandler is required for enableSystemLogQuery"
        );
        assert!(cfg("enableSystemLogQuery: true\n").validate().is_ok());
        reset_to_defaults();
    }
}
