//! Unit tests moved from `src/server.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router,
    };
    pub use rusternetes_common::resources::Pod;
    pub use rusternetes_common::types::{List, Phase};
    pub use rusternetes_common::Result;
    pub use rusternetes_kubelet::kubelet::Kubelet;
    pub use rusternetes_storage::{build_prefix, Storage, StorageBackend};
    pub use serde_json::json;
    pub use std::sync::Arc;
}

mod statusz_tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::Request;
    use rusternetes_common::feature_gates::{
        reset_to_defaults, set_from_str, with_feature, Feature,
    };
    use rusternetes_kubelet::server::*;
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
        use rusternetes_kubelet::server_metrics::{
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
        use rusternetes_kubelet::server_metrics::HTTP_REQUESTS;
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

mod debugging_handlers_tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use rusternetes_kubelet::server::*;
    use tower::ServiceExt;

    async fn call(app: &Router, method: Method, path: &str) -> (u16, String) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status().as_u16();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    // server_test.go:TestDebuggingDisabledHandlers. `/logs/kubelet.log` is
    // served by the system-log handler install, not by this fragment.
    #[tokio::test]
    async fn debugging_disabled_handlers_answer_405_on_every_path() {
        let app = install_debugging_disabled_handlers(Router::new());
        let paths = [
            "/run",
            "/exec",
            "/attach",
            "/portForward",
            "/containerLogs",
            "/runningpods",
            "/run/",
            "/exec/",
            "/attach/",
            "/portForward/",
            "/containerLogs/",
            "/runningpods/",
            "/run/xxx",
            "/exec/xxx",
            "/attach/xxx",
            "/debug/pprof/profile",
        ];
        for p in paths {
            for m in [Method::GET, Method::POST] {
                let (status, body) = call(&app, m, p).await;
                assert_eq!(status, 405, "{p}");
                assert_eq!(body, "Debug endpoints are disabled.\n", "{p}");
            }
        }
    }

    // `InstallDebuggingDisabledHandlers` leaves the auth-not-required routes
    // alone and /configz is not served (it is an auth-required handler).
    #[tokio::test]
    async fn disabled_keeps_healthz_and_drops_configz() {
        let state = ServerState {
            node_name: "n".into(),
            storage: Arc::new(StorageBackend::new_memory()),
            kubelet: None,
        };
        let app = install_debugging_disabled_handlers(
            Router::new().merge(auth_not_required_router(state)),
        );
        assert_eq!(call(&app, Method::GET, "/healthz").await.0, 200);
        assert_eq!(call(&app, Method::GET, "/pods").await.0, 200);
        assert_eq!(call(&app, Method::GET, "/configz").await.0, 404);
    }

    #[test]
    fn registered_paths_follow_the_gate() {
        let on = debugging_registered_paths(true, true, true, true);
        assert!(on.contains(&"/runningpods/".to_string()));
        assert!(on.contains(&"/run/:namespace/:pod/:container".to_string()));
        assert!(on.contains(&"/checkpoint/:namespace/:pod/:container".to_string()));
        assert!(!debugging_registered_paths(true, true, false, true)
            .iter()
            .any(|p| p.starts_with("/checkpoint")));
        assert!(on.contains(&"/debug/pprof/{subpath:*}".to_string()));
        assert!(on.contains(&"/debug/flags/v".to_string()));
        let no_prof = debugging_registered_paths(true, true, true, false);
        assert!(no_prof.contains(&"/debug/pprof/".to_string()));
        let off = debugging_registered_paths(false, true, true, true);
        assert!(!off.contains(&"/debug/flags/v".to_string()));
        assert_eq!(off.len(), DEBUGGING_DISABLED_PATHS.len());
        assert!(off.contains(&"/debug/pprof/".to_string()));
    }

    // auth.go GetRequestAttributes: `/run` and `/checkpoint` map to the
    // `proxy` / `checkpoint` subresource with the POST -> `create` verb.
    #[test]
    fn run_and_checkpoint_auth_attributes() {
        let run = get_request_attributes("n", "u", "POST", "/run/ns/pod/c");
        assert_eq!(run.len(), 1);
        assert_eq!(
            (run[0].verb.as_str(), run[0].subresource.as_str()),
            ("create", "proxy")
        );
        let cp = get_request_attributes("n", "u", "POST", "/checkpoint/ns/pod/c");
        assert_eq!(
            (cp[0].verb.as_str(), cp[0].subresource.as_str()),
            ("create", "checkpoint")
        );
    }
}
