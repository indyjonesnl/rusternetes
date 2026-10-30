//! Unit tests moved from `src/deprecation/warnings.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::extract::Request;
    pub use axum::http::{header, HeaderValue};
    pub use axum::middleware::Next;
    pub use axum::response::Response;
    pub use rusternetes_api_server::deprecation::{
        is_deprecated, lookup, should_serve_for_version, warning_message, CURRENT_MAJOR_MINOR,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::deprecation::warnings::*;

    fn run(args: &[(&str, &str)]) -> Vec<String> {
        let mut r = Recorder::new();
        for (a, t) in args {
            r.add_warning(a, t);
        }
        r.headers().to_vec()
    }

    /// `Test_recorder_AddWarning` (filters/warning_test.go:25).
    #[test]
    fn recorder_add_warning() {
        assert!(run(&[]).is_empty());
        assert!(run(&[("", "")]).is_empty());
        assert_eq!(run(&[("", "mytext")]), [r#"299 - "mytext""#]);
        assert_eq!(run(&[("myagent", "mytext")]), [r#"299 myagent "mytext""#]);
        assert_eq!(
            run(&[("myagent", "mytext"), ("myagent2", "mytext")]),
            [r#"299 myagent "mytext""#]
        );
        assert_eq!(
            run(&[("", "mytext1"), ("", "mytext2"), ("", "mytext3")]),
            [
                r#"299 - "mytext1""#,
                r#"299 - "mytext2""#,
                r#"299 - "mytext3""#
            ]
        );
        assert!(run(&[("my agent", "mytext")]).is_empty());
        assert!(run(&[("myagent", "my\ntext")]).is_empty());
    }

    /// `TestTruncation` (filters/warning_test.go:102).
    #[test]
    fn recorder_truncation() {
        let mut r = Recorder::with_limits(25, 5);
        r.add_warning("", "aaaaaaaaaa");
        r.add_warning("-", "aaaaaaaaaa");
        r.add_warning("", "bb");
        r.add_warning("", "ccc");
        r.add_warning("", "Iñtërnâtiô");
        assert_eq!(
            r.headers(),
            [
                r#"299 - "aaaaaaaaaa""#,
                r#"299 - "bb""#,
                r#"299 - "ccc""#,
                r#"299 - "Iñtërnâtiô""#
            ]
        );
        r.add_warning("", "e");
        r.add_warning("", "ffffffffff");
        r.add_warning("", "ffffffffff");
        r.add_warning("", "gggggggggg");
        r.add_warning("", "h");
        assert_eq!(
            r.headers(),
            [
                r#"299 - "aaaaa""#,
                r#"299 - "bb""#,
                r#"299 - "ccc""#,
                r#"299 - "Iñtër""#,
                r#"299 - "e""#,
                r#"299 - "fffff""#,
                r#"299 - "ggggg""#
            ]
        );
    }

    #[test]
    fn served_deprecated_kind_warns() {
        // resource.k8s.io/v1beta1 DeviceClass: deprecated 1.35, removed 1.38.
        let w = endpoint_warnings("resource.k8s.io", "v1beta1", "deviceclasses", "");
        assert_eq!(
            w,
            ["resource.k8s.io/v1beta1 DeviceClass is deprecated in v1.35+, unavailable in v1.38+"]
        );
    }

    /// `removeDeletedKinds`: a removed version has no route, so no warning.
    #[test]
    fn removed_kind_has_no_route_so_no_warning() {
        assert!(endpoint_warnings("apps", "v1beta1", "deployments", "").is_empty());
    }

    #[test]
    fn stable_kind_does_not_warn() {
        assert!(endpoint_warnings("apps", "v1", "deployments", "").is_empty());
    }

    #[test]
    fn parses_what_the_handlers_write() {
        let h = rusternetes_api_server::handlers::validation::format_warning_header(r#"a "b" \c"#);
        assert_eq!(
            parse_warning_header(&h),
            Some(("-".to_string(), r#"a "b" \c"#.to_string()))
        );
    }

    async fn serve(uri: &str, handler_warnings: &'static [&'static str]) -> Vec<String> {
        use axum::{body::Body, http::Request, middleware, routing::get, Router};
        use tower::ServiceExt;
        let app = Router::new()
            .route(
                "/apis/resource.k8s.io/v1beta1/deviceclasses",
                get(move || async move {
                    let mut r = axum::response::Response::new(Body::empty());
                    for w in handler_warnings {
                        r.headers_mut().append(
                            header::WARNING,
                            HeaderValue::from_str(
                                &rusternetes_api_server::handlers::validation::format_warning_header(w),
                            )
                            .unwrap(),
                        );
                    }
                    r
                }),
            )
            .route("/apis/apps/v1/deployments", get(|| async { "ok" }))
            .layer(middleware::from_fn(add_endpoint_warnings));
        let resp = app
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        resp.headers()
            .get_all(header::WARNING)
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect()
    }

    /// AddWarningsHandler: the route's warning is added before the handler's
    /// own, and a repeated text is recorded once.
    #[tokio::test]
    async fn middleware_puts_route_warning_first_and_dedups() {
        const ROUTE: &str =
            "resource.k8s.io/v1beta1 DeviceClass is deprecated in v1.35+, unavailable in v1.38+";
        let got = serve(
            "/apis/resource.k8s.io/v1beta1/deviceclasses",
            &["handler says hi", ROUTE],
        )
        .await;
        assert_eq!(
            got,
            [
                format!("299 - \"{ROUTE}\""),
                "299 - \"handler says hi\"".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn middleware_leaves_stable_routes_alone() {
        assert!(serve("/apis/apps/v1/deployments", &[]).await.is_empty());
    }
}
