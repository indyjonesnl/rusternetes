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
        let app = install_logs_handlers(Router::new(), true, true, root);
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
        let app = install_logs_handlers(Router::new(), true, true, root);
        let (st, _, loc) = call(&app, Method::GET, "/logs/pods").await;
        assert_eq!((st, loc.as_deref()), (301, Some("pods/")));
    }

    // http.FileServer cleans the path: it never escapes the root.
    #[tokio::test]
    async fn path_traversal_stays_inside_root() {
        let (_o, root) = fixture();
        let app = install_logs_handlers(Router::new(), true, true, root);
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
        let app = install_logs_handlers(Router::new(), false, true, root);
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
        let app = install_logs_handlers(Router::new(), true, false, root);
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
}
