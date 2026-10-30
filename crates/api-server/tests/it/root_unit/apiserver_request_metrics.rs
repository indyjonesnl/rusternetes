//! Unit tests moved from `src/apiserver_request_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};
    pub use rusternetes_common::latency_trackers::{with_latency_trackers, LatencyTrackers};
    pub use std::sync::{Arc, LazyLock, Mutex, OnceLock};
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::apiserver_request_metrics::*;
    use rusternetes_api_server::flow_control::metric_test_util::sample;

    fn meta(resource: &str) -> RequestMeta {
        RequestMeta {
            method: "GET".into(),
            path: format!("/api/v1/namespaces/default/{resource}"),
            is_resource_request: true,
            verb: "list".into(),
            namespace: "default".into(),
            api_version: "v1".into(),
            resource: resource.into(),
            ..Default::default()
        }
    }

    // MonitorRequest (metrics.go:619-625): the SLI latency is the elapsed time
    // minus webhook and APF queue-wait time, only when trackers exist and the
    // request is not a dry run.
    #[test]
    fn sli_duration_excludes_webhook_and_queue_wait() {
        use rusternetes_api_server::flow_control::metric_test_util::{sample, sample_sum};
        let l = [("resource", "slithings"), ("component", "apiserver")];
        monitor_request(
            &meta("slithings"),
            "apiserver",
            200,
            Duration::from_secs(5),
            0,
            Some(Duration::from_secs(3)),
        );
        assert_eq!(sample("apiserver_request_sli_duration_seconds", &l), 1.0);
        assert_eq!(
            sample_sum("apiserver_request_sli_duration_seconds", &l),
            2.0
        );
        assert_eq!(
            sample_sum("apiserver_request_slo_duration_seconds", &l),
            2.0
        );
        // No trackers (a watch): not observed.
        monitor_request(
            &meta("slithings"),
            "apiserver",
            200,
            Duration::from_secs(5),
            0,
            None,
        );
        // Dry run: not observed.
        let mut dry = meta("slithings");
        dry.query = "dryRun=All".into();
        monitor_request(
            &dry,
            "apiserver",
            200,
            Duration::from_secs(5),
            0,
            Some(Duration::ZERO),
        );
        assert_eq!(sample("apiserver_request_sli_duration_seconds", &l), 1.0);
    }

    // `WithLatencyTrackers` + `TrackAPFQueueWaitLatency`: instrument_request
    // installs trackers so APF queue wait is subtracted.
    #[tokio::test]
    async fn instrument_request_installs_trackers() {
        use axum::{body::Body, http::Request, middleware, routing::get, Router};
        use tower::ServiceExt;
        async fn handler() -> &'static str {
            rusternetes_common::latency_trackers::track_apf_queue_wait_latency(
                Duration::from_secs(100),
            );
            "ok"
        }
        let app = Router::new()
            .route("/api/v1/namespaces/default/instrthings", get(handler))
            .layer(middleware::from_fn(instrument_request));
        let resp = app
            .oneshot(
                Request::get("/api/v1/namespaces/default/instrthings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let sum = rusternetes_api_server::flow_control::metric_test_util::sample_sum(
            "apiserver_request_sli_duration_seconds",
            &[("resource", "instrthings")],
        );
        // elapsed (~0) - 100s of queue wait.
        assert!(sum < -99.0, "sli sum {sum}");
    }

    // metrics_test.go TestCleanFieldValidation (:300-358).
    #[test]
    fn clean_field_validation_matches_upstream_table() {
        for (q, want) in [
            ("", ""),
            ("foo=bar", ""),
            ("fieldValidation=Ignore", "Ignore"),
            ("fieldValidation=Warn", "Warn"),
            ("fieldValidation=Strict", "Strict"),
            ("fieldValidation=foo", "invalid"),
            ("fieldValidation=Strict&fieldValidation=Ignore", "invalid"),
        ] {
            assert_eq!(clean_field_validation(q), want, "query {q:?}");
        }
    }

    // MonitorRequest (:617-618) creates the (subsystem-less, :139-148) child without
    // observing it (upstream never calls Observe).
    #[test]
    fn monitor_request_creates_unobserved_field_validation_series() {
        use rusternetes_api_server::flow_control::metric_test_util::{has_series, sample};
        let mut m = meta("fvthings");
        m.query = "fieldValidation=Warn".into();
        monitor_request(&m, "apiserver", 200, Duration::from_millis(1), 0, None);
        let l = [("field_validation", "Warn")];
        assert!(has_series("field_validation_request_duration_seconds", &l));
        assert_eq!(sample("field_validation_request_duration_seconds", &l), 0.0);
    }

    // MonitorRequest (:604-607): apiSelfRequestCounter for system:apiserver.
    #[test]
    fn self_request_counted_only_for_apiserver_user() {
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let l = [("verb", "LIST"), ("resource", "selfthings")];
        let mut m = meta("selfthings");
        m.user_name = "system:apiserver".into();
        monitor_request(&m, "apiserver", 200, Duration::ZERO, 0, None);
        assert_eq!(sample("apiserver_selfrequest_total", &l), 1.0);
        m.user_name = "alice".into();
        monitor_request(&m, "apiserver", 200, Duration::ZERO, 0, None);
        assert_eq!(sample("apiserver_selfrequest_total", &l), 1.0);
    }

    // RecordLongRunning (:551-575): gauge up while open, down on drop; the
    // labels use the reported verb, and non-resource requests the path.
    #[test]
    fn long_running_gauge_tracks_open_requests() {
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let mut m = meta("lrthings");
        m.verb = "watch".into();
        m.query = "watch=true".into();
        let l = [
            ("verb", "WATCH"),
            ("resource", "lrthings"),
            ("component", "apiserver"),
        ];
        let g = record_long_running(&m, "apiserver");
        assert_eq!(sample("apiserver_longrunning_requests", &l), 1.0);
        drop(g);
        assert_eq!(sample("apiserver_longrunning_requests", &l), 0.0);
        let np = RequestMeta {
            method: "GET".into(),
            path: "/lrpath".into(),
            ..Default::default()
        };
        let _g = record_long_running(&np, "apiserver");
        assert_eq!(
            sample(
                "apiserver_longrunning_requests",
                &[("subresource", "/lrpath"), ("resource", "")]
            ),
            1.0
        );
    }

    // InstrumentRouteFunc: a watch response holds the gauge until its body
    // is dropped (RecordLongRunning wraps serving, get.go:283).
    #[tokio::test]
    async fn instrument_request_holds_long_running_for_watch_body() {
        use axum::{body::Body, http::Request, middleware, routing::get, Router};
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        use tower::ServiceExt;
        let app = Router::new()
            .route(
                "/api/v1/namespaces/default/lrwatchthings",
                get(|| async { "x" }),
            )
            .layer(middleware::from_fn(instrument_request));
        let resp = app
            .oneshot(
                Request::get("/api/v1/namespaces/default/lrwatchthings?watch=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let l = [("verb", "WATCH"), ("resource", "lrwatchthings")];
        assert_eq!(sample("apiserver_longrunning_requests", &l), 1.0);
        // MonitorRequest runs when the handler returns, i.e. at stream end
        // (metrics.go:645-647): nothing observed while the body is open.
        assert_eq!(sample("apiserver_request_duration_seconds", &l), 0.0);
        assert_eq!(sample("apiserver_request_total", &l), 0.0);
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(resp);
        assert_eq!(sample("apiserver_longrunning_requests", &l), 0.0);
        assert_eq!(sample("apiserver_request_duration_seconds", &l), 1.0);
        assert_eq!(sample("apiserver_request_total", &l), 1.0);
        assert!(
            rusternetes_api_server::flow_control::metric_test_util::sample_sum(
                "apiserver_request_duration_seconds",
                &l
            ) >= 0.03
        );
    }

    // ConnectResource (handlers/rest.go:218) wraps the whole `ServeHTTP` in
    // `RecordLongRunning`; for an upgraded exec/attach/portforward the handler
    // blocks until the connection ends, so the gauge must stay up across the
    // 101 and fall only when the spliced connection closes (#3545).
    #[tokio::test]
    async fn long_running_gauge_held_across_upgraded_connection() {
        use axum::middleware;
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_addr = backend.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = backend.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = s.read(&mut buf).await.unwrap();
            s.write_all(
                b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n",
            )
            .await
            .unwrap();
            let mut line = [0u8; 5];
            s.read_exact(&mut line).await.unwrap();
            s.write_all(&line.to_ascii_uppercase()).await.unwrap();
            // Hold the backend side open until the client hangs up.
            let _ = s.read(&mut line).await;
        });
        let target: axum::http::Uri = format!("http://{backend_addr}/x").parse().unwrap();
        let app = axum::Router::new()
            .fallback(move |req: axum::extract::Request| {
                let t = target.clone();
                async move { rusternetes_streamproxy::proxy_upgrade(t, req).await }
            })
            .layer(middleware::from_fn(instrument_request));
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(proxy, app).await.unwrap() });

        let l = [
            ("verb", "CONNECT"),
            ("resource", "pods"),
            ("subresource", "attach"),
        ];
        let mut c = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
        c.write_all(
            b"GET /api/v1/namespaces/default/pods/lrup/attach HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: x\r\n\r\n",
        )
        .await
        .unwrap();
        let mut hdr = [0u8; 1024];
        let n = c.read(&mut hdr).await.unwrap();
        assert!(std::str::from_utf8(&hdr[..n]).unwrap().contains("101"));
        c.write_all(b"hello").await.unwrap();
        let mut out = [0u8; 5];
        c.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"HELLO");
        // The 101 response is long gone; the connection is still open and
        // the request is not yet observed (it ends with the connection).
        assert_eq!(sample("apiserver_longrunning_requests", &l), 1.0);
        assert_eq!(sample("apiserver_request_duration_seconds", &l), 0.0);
        drop(c);
        for _ in 0..100 {
            if sample("apiserver_longrunning_requests", &l) == 0.0 {
                assert_eq!(sample("apiserver_request_duration_seconds", &l), 1.0);
                assert_eq!(sample("apiserver_request_total", &l), 1.0);
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("gauge not released after the upgraded connection closed");
    }

    // RecordDroppedRequest (metrics.go:508-526): `apiserver_request_total`
    // with the 429 code, verb canonicalised (a collection GET is LIST).
    #[test]
    fn dropped_request_counts_apiserver_request_total_429() {
        let l = [
            ("verb", "LIST"),
            ("resource", "droppedthings"),
            ("scope", "namespace"),
            ("component", "apiserver"),
            ("code", "429"),
        ];
        let before = sample("apiserver_request_total", &l);
        record_dropped_request(&meta("droppedthings"), "apiserver", false);
        assert_eq!(sample("apiserver_request_total", &l), before + 1.0);
    }

    // RecordRequestTermination (metrics.go:532-549).
    #[test]
    fn request_termination_counts_terminations_total() {
        let l = [
            ("verb", "LIST"),
            ("resource", "terminatedthings"),
            ("scope", "namespace"),
            ("component", "apiserver"),
            ("code", "429"),
        ];
        let before = sample("apiserver_request_terminations_total", &l);
        record_request_termination(&meta("terminatedthings"), "apiserver", 429);
        assert_eq!(
            sample("apiserver_request_terminations_total", &l),
            before + 1.0
        );
    }

    // Non-resource requests report the path as subresource and empty
    // group/version/resource (metrics.go:547).
    #[test]
    fn non_resource_termination_uses_path_as_subresource() {
        let req = RequestMeta {
            method: "GET".into(),
            path: "/nonresourcepath".into(),
            verb: "get".into(),
            ..Default::default()
        };
        record_request_termination(&req, "apiserver", 429);
        assert!(
            rusternetes_api_server::flow_control::metric_test_util::has_series(
                "apiserver_request_terminations_total",
                &[("subresource", "/nonresourcepath"), ("scope", "")]
            )
        );
    }

    // requestWatermark (maxinflight.go:56-84, 93-103): records the maximum
    // since the last tick, which `take` returns and resets.
    #[test]
    fn watermark_reports_max_then_resets() {
        let w = Watermark::default();
        w.record_mutating(1);
        w.record_mutating(3);
        w.record_mutating(2);
        w.record_read_only(5);
        assert_eq!(w.take(), (5, 3));
        assert_eq!(w.take(), (0, 0));
    }

    // The gauges exist (readOnly/mutating) after a flush
    // (UpdateInflightRequestMetrics, metrics.go:465-476), for both phases.
    #[test]
    fn flush_sets_inflight_and_inqueue_gauges() {
        note_executing_delta(true, 1);
        note_waiting_delta(false, 1);
        flush_watermarks();
        note_executing_delta(true, -1);
        note_waiting_delta(false, -1);
        for (name, kind) in [
            ("apiserver_current_inflight_requests", "readOnly"),
            ("apiserver_current_inflight_requests", "mutating"),
            ("apiserver_current_inqueue_requests", "readOnly"),
            ("apiserver_current_inqueue_requests", "mutating"),
        ] {
            assert!(
                rusternetes_api_server::flow_control::metric_test_util::has_series(
                    name,
                    &[("request_kind", kind)]
                ),
                "{name}{{request_kind={kind}}}"
            );
        }
    }

    async fn serve(method: &str, path: &str, body: &'static str) -> axum::response::Response {
        use tower::ServiceExt;
        let app = axum::Router::new()
            .fallback(move || async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                body
            })
            .layer(axum::middleware::from_fn(instrument_request));
        app.oneshot(
            axum::http::Request::builder()
                .method(method)
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    // InstrumentRouteFunc -> MonitorRequest (metrics.go:633-650, 599-603):
    // the general layer counts requests with no APF in the chain.
    #[tokio::test]
    async fn instrument_layer_counts_request_total() {
        let l = [
            ("verb", "LIST"),
            ("resource", "instrumentedthings"),
            ("scope", "namespace"),
            ("code", "200"),
        ];
        let before = sample("apiserver_request_total", &l);
        serve("GET", "/api/v1/namespaces/default/instrumentedthings", "ok").await;
        assert_eq!(sample("apiserver_request_total", &l), before + 1.0);
    }

    // Long-running non-watch requests (skipped by APF, :90-95) are counted.
    #[tokio::test]
    async fn instrument_layer_counts_long_running_non_watch() {
        let l = [
            ("verb", "CONNECT"),
            ("resource", "pods"),
            ("subresource", "exec"),
            ("code", "200"),
        ];
        let before = sample("apiserver_request_total", &l);
        serve("GET", "/api/v1/namespaces/default/pods/p/exec", "ok").await;
        assert_eq!(sample("apiserver_request_total", &l), before + 1.0);
    }

    // Requests with no resource info (RequestInfo{Verb: Method, Path}) are
    // counted with empty group/version/resource/scope (metrics.go:591-594).
    #[tokio::test]
    async fn instrument_layer_counts_non_resource_request() {
        let l = [
            ("verb", "GET"),
            ("resource", ""),
            ("scope", ""),
            ("code", "200"),
        ];
        let before = sample("apiserver_request_total", &l);
        serve("GET", "/healthz", "ok").await;
        assert!(sample("apiserver_request_total", &l) >= before + 1.0);
    }

    // requestLatencies (metrics.go:104-118, :609): observed with the elapsed time.
    #[tokio::test]
    async fn instrument_layer_observes_request_duration() {
        use rusternetes_api_server::flow_control::metric_test_util::sample_sum;
        let l = [("resource", "durationthings"), ("verb", "LIST")];
        let before = sample("apiserver_request_duration_seconds", &l);
        serve("GET", "/api/v1/namespaces/default/durationthings", "ok").await;
        assert_eq!(
            sample("apiserver_request_duration_seconds", &l),
            before + 1.0
        );
        assert!(sample_sum("apiserver_request_duration_seconds", &l) >= 0.02);
    }

    // responseSizes (metrics.go:152-162, :625-628): read requests only.
    #[tokio::test]
    async fn instrument_layer_observes_response_size_for_reads_only() {
        use rusternetes_api_server::flow_control::metric_test_util::{has_series, sample_sum};
        serve("GET", "/api/v1/namespaces/default/sizedthings", "12345").await;
        assert_eq!(
            sample_sum(
                "apiserver_response_sizes",
                &[("resource", "sizedthings"), ("verb", "LIST")]
            ),
            5.0
        );
        serve("POST", "/api/v1/namespaces/default/postedthings", "123").await;
        assert!(!has_series(
            "apiserver_response_sizes",
            &[("resource", "postedthings")]
        ));
    }

    // RecordRequestAbort (metrics.go:496-506).
    #[test]
    fn request_abort_counts_aborts_total() {
        let l = [
            ("verb", "LIST"),
            ("resource", "abortedthings"),
            ("scope", "namespace"),
        ];
        let before = sample("apiserver_request_aborts_total", &l);
        record_request_abort(&meta("abortedthings"));
        assert_eq!(sample("apiserver_request_aborts_total", &l), before + 1.0);
    }

    fn deprecated_meta(group: &str, version: &str, resource: &str) -> RequestMeta {
        RequestMeta {
            api_group: group.into(),
            api_version: version.into(),
            ..meta(resource)
        }
    }

    // MonitorRequest (metrics.go:609-615): a request to a deprecated API
    // version sets apiserver_requested_deprecated_apis{...removed_release}=1.
    #[test]
    fn deprecated_api_request_sets_gauge() {
        use rusternetes_api_server::flow_control::metric_test_util::{has_series, sample};
        let l = [
            ("group", "admissionregistration.k8s.io"),
            ("version", "v1alpha1"),
            ("resource", "mutatingadmissionpolicies"),
            ("subresource", ""),
            ("removed_release", "1.38"),
        ];
        assert!(!has_series("apiserver_requested_deprecated_apis", &l));
        monitor_request(
            &deprecated_meta(
                "admissionregistration.k8s.io",
                "v1alpha1",
                "mutatingadmissionpolicies",
            ),
            "apiserver",
            200,
            Duration::ZERO,
            0,
            None,
        );
        assert_eq!(sample("apiserver_requested_deprecated_apis", &l), 1.0);
        // A stable version is not recorded.
        monitor_request(
            &deprecated_meta("apps", "v1", "deployments"),
            "apiserver",
            200,
            Duration::ZERO,
            0,
            None,
        );
        assert!(!has_series(
            "apiserver_requested_deprecated_apis",
            &[("group", "apps"), ("version", "v1")]
        ));
    }

    struct CaptureSink(tokio::sync::Mutex<Vec<rusternetes_common::audit::AuditEvent>>);
    #[async_trait::async_trait]
    impl rusternetes_common::audit::AuditBackend for CaptureSink {
        async fn log(&self, e: rusternetes_common::audit::AuditEvent) -> Result<(), String> {
            self.0.lock().await.push(e);
            Ok(())
        }
        async fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    // MonitorRequest (metrics.go:609-615): audit annotations
    // k8s.io/deprecated="true" and k8s.io/removed-release on a deprecated route.
    #[tokio::test]
    async fn deprecated_api_request_adds_audit_annotations() {
        use axum::{body::Body, http::Request, middleware, routing::get, Router};
        use tower::ServiceExt;
        let sink = Arc::new(CaptureSink(Default::default()));
        let cfg = Arc::new(rusternetes_api_server::audit::AuditConfig {
            policy: rusternetes_api_server::audit::Policy::from_yaml(
                "apiVersion: audit.k8s.io/v1\nkind: Policy\nomitStages: [\"RequestReceived\"]\nrules:\n- level: Metadata\n",
            )
            .unwrap(),
            sink: sink.clone(),
        });
        let app = Router::new()
            .route(
                "/apis/admissionregistration.k8s.io/v1alpha1/mutatingadmissionpolicies",
                get(|| async { "ok" }),
            )
            .layer(middleware::from_fn(instrument_request))
            .layer(middleware::from_fn(move |req, next| {
                let cfg = cfg.clone();
                async move { rusternetes_api_server::audit::with_audit(cfg, req, next).await }
            }));
        let resp = app
            .oneshot(
                Request::get(
                    "/apis/admissionregistration.k8s.io/v1alpha1/mutatingadmissionpolicies",
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let events = sink.0.lock().await;
        let ann = events
            .iter()
            .find_map(|e| e.annotations.clone())
            .expect("annotated event");
        assert_eq!(
            ann.get("k8s.io/deprecated").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            ann.get("k8s.io/removed-release").map(String::as_str),
            Some("1.38")
        );
    }
}
