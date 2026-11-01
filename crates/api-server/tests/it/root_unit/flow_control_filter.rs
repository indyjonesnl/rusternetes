//! Unit tests moved from `src/flow_control_filter.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::body::Body;
    pub use axum::extract::{Request, State};
    pub use axum::http::{header::RETRY_AFTER, HeaderName, HeaderValue, StatusCode};
    pub use axum::middleware::Next;
    pub use axum::response::{IntoResponse, Response};
    pub use futures::{FutureExt, StreamExt};
    pub use rusternetes_api_server::apiserver_request_metrics::{self, RequestMeta};
    pub use rusternetes_api_server::audit::request_info;
    pub use rusternetes_api_server::flow_control::{
        Classification, FlowControlEngine, FlowControlPermit, RequestDigest,
    };
    pub use rusternetes_api_server::flow_control_object_count::ObjectCountTracker;
    pub use rusternetes_api_server::flow_control_watch_tracker::{
        scope_signal, ForgetWatch, InitializationSignal, WatchTracker,
    };
    pub use rusternetes_api_server::flow_control_work_estimator::{
        RequestInfo, WorkEstimator, WorkEstimatorConfig,
    };
    pub use rusternetes_storage::{Storage, StorageBackend};
    pub use std::collections::HashMap;
    pub use std::sync::{Arc, Mutex, OnceLock, RwLock};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use axum::Router;
    use rusternetes_api_server::flow_control_filter::*;
    use rusternetes_api_server::flow_control_work_estimator::Stats;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_storage::memory::MemoryStorage;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tower::ServiceExt;

    // ---- getRequestWaitContext (TestGetRequestWaitContext) ----

    #[test]
    fn wait_limit_one_fourth_of_remaining_deadline() {
        let now = Instant::now();
        let got = request_wait_limit(
            now,
            None,
            Some(now + Duration::from_secs(60)),
            Duration::ZERO,
        );
        assert_eq!(got, Duration::from_secs(15));
    }

    #[test]
    fn wait_limit_counts_from_received_timestamp() {
        let now = Instant::now();
        let received = now - Duration::from_secs(10);
        let got = request_wait_limit(
            now,
            Some(received),
            Some(now + Duration::from_secs(50)),
            Duration::ZERO,
        );
        assert_eq!(got, Duration::from_secs(5));
    }

    #[test]
    fn wait_limit_default_without_deadline() {
        let now = Instant::now();
        let d = Duration::from_secs(15);
        assert_eq!(request_wait_limit(now, None, None, d), d);
        assert_eq!(
            request_wait_limit(now, Some(now - Duration::from_secs(10)), None, d),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn wait_limit_never_exceeds_one_minute() {
        let now = Instant::now();
        assert_eq!(
            request_wait_limit(
                now,
                None,
                Some(now + Duration::from_secs(480)),
                Duration::ZERO
            ),
            Duration::from_secs(60)
        );
        assert_eq!(
            request_wait_limit(now, None, None, Duration::from_secs(120)),
            Duration::from_secs(60)
        );
    }

    // ---- TestDroppedRequestsTracker / ...PLIndependent ----

    fn fake_clock() -> (Arc<AtomicI64>, DroppedRequestsTracker) {
        let t = Arc::new(AtomicI64::new(1_000_000));
        let c = t.clone();
        (
            t,
            DroppedRequestsTracker::with_clock(move || c.load(Ordering::SeqCst)),
        )
    }

    #[test]
    fn dropped_requests_tracker_adapts_retry_after() {
        let (clock, tracker) = fake_clock();
        // (seconds elapsed, dropped requests, expected retryAfter)
        let steps = [
            (0, 5, 1),
            (1, 11, 2),
            (2, 1, 2),
            (3, 11, 4),
            (4, 1, 4),
            (7, 1, 8),
            (11, 1, 8),
            (15, 1, 7),
            (17, 1, 6),
            (21, 14, 5),
            (22, 1, 10),
        ];
        let mut prev = 0;
        for (elapsed, dropped, want) in steps {
            clock.fetch_add(elapsed - prev, Ordering::SeqCst);
            prev = elapsed;
            for _ in 0..dropped {
                tracker.record_dropped_request("pl");
            }
            assert_eq!(tracker.get_retry_after("pl"), want, "at +{elapsed}s");
        }
    }

    #[test]
    fn dropped_requests_tracker_is_per_priority_level() {
        let (clock, tracker) = fake_clock();
        for i in 0..10 {
            tracker.record_dropped_request(&format!("pl-{i}"));
        }
        clock.fetch_add(1, Ordering::SeqCst);
        for i in 0..10 {
            let pl = format!("pl-{i}");
            tracker.record_dropped_request(&pl);
            assert_eq!(tracker.get_retry_after(&pl), 1);
        }
        for _ in 0..5 {
            tracker.record_dropped_request("pl-0");
        }
        clock.fetch_add(1, Ordering::SeqCst);
        for i in 0..10 {
            let pl = format!("pl-{i}");
            tracker.record_dropped_request(&pl);
            assert_eq!(tracker.get_retry_after(&pl), if i == 0 { 2 } else { 1 });
        }
        assert_eq!(tracker.get_retry_after("other-pl"), 1);
    }

    // ---- long-running check ----

    #[test]
    fn long_running_matches_kube_apiserver_sets() {
        assert!(is_long_running("watch", true, "", ""));
        assert!(is_long_running("proxy", true, "", ""));
        assert!(is_long_running("get", true, "exec", ""));
        assert!(is_long_running("get", true, "log", ""));
        assert!(is_long_running("get", false, "", "/debug/pprof/heap"));
        assert!(!is_long_running("get", true, "status", ""));
        assert!(!is_long_running("list", true, "", ""));
        // a subresource only counts for resource requests
        assert!(!is_long_running("get", false, "exec", "/x"));
    }

    // ---- the filter (TestApf*) ----

    /// `limit` seats in total (`catch-all` shares everything), handler runs
    /// `hook` while holding its seat.
    async fn app(
        limit: i64,
        wait: Duration,
        hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync>,
        user: UserInfo,
    ) -> Router {
        let engine = Arc::new(FlowControlEngine::with_limits(
            Arc::new(MemoryStorage::new()),
            limit,
            0,
        ));
        engine.initialize().await.unwrap();
        let filter = Arc::new(ApfFilter::new(engine, wait));
        let ctx = rusternetes_middleware::AuthContext { user };
        Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let hook = hook.clone();
                    async move {
                        if let Some(n) = hook() {
                            n.notified().await;
                        }
                        "ok"
                    }
                }),
            )
            .route(
                "/api/v1/namespaces/default/pods/p/exec",
                get(|| async { "x" }),
            )
            .route("/api/v1/watch/namespaces", get(|| async { "w" }))
            // As in router.rs: InstrumentRouteFunc sits inside APF.
            .layer(axum::middleware::from_fn(
                rusternetes_api_server::apiserver_request_metrics::instrument_request,
            ))
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ))
    }

    fn get_req(path: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    fn none_hook() -> Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> {
        Arc::new(|| None)
    }

    fn bob() -> UserInfo {
        UserInfo {
            username: "bob".into(),
            uid: String::new(),
            groups: vec!["system:authenticated".into()],
            extra: Default::default(),
        }
    }

    fn masters() -> UserInfo {
        UserInfo {
            username: "admin".into(),
            uid: String::new(),
            groups: vec!["system:masters".into()],
            extra: Default::default(),
        }
    }

    #[tokio::test]
    async fn executes_a_request_and_sets_uid_headers() {
        let app = app(10, Duration::from_millis(50), none_hook(), bob()).await;
        let resp = app
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));
        assert!(resp.headers().contains_key(HEADER_FLOW_SCHEMA_UID));
    }

    #[tokio::test]
    async fn rejects_with_429_and_retry_after_when_seats_are_exhausted() {
        // One seat on `catch-all` (a Reject level): a second concurrent
        // request is a 429 (TestApfRejectRequest).
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) == 0).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, bob()).await;
        let a = app.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get(RETRY_AFTER).unwrap(), "1");
        assert!(resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));

        gate.notify_one();
        assert_eq!(held.await.unwrap().unwrap().status(), StatusCode::OK);
        // The seat is free again once the first request finished.
        assert_eq!(
            app.oneshot(get_req("/api/v1/namespaces/default"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn observes_work_estimated_seats() {
        // ObserveWorkEstimatedSeats (priority-and-fairness.go:134), recorded
        // once the estimate is made, for the matched level and flow schema.
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let l = [
            ("priority_level", "catch-all"),
            ("flow_schema", "catch-all"),
        ];
        let before = sample("apiserver_flowcontrol_work_estimated_seats", &l);
        let app = app(10, Duration::from_millis(50), none_hook(), bob()).await;
        let resp = app
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // `catch-all` is shared with the other tests running in this
        // process, so only a lower bound is stable.
        assert!(sample("apiserver_flowcontrol_work_estimated_seats", &l) - before >= 1.0);
    }

    #[tokio::test]
    async fn system_masters_is_exempt_from_the_limit() {
        // TestApfExemptRequest: the `exempt` level never rejects, even with
        // every seat of the server taken.
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) < 3).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, masters()).await;
        let mut tasks = vec![];
        for _ in 0..3 {
            let a = app.clone();
            tasks.push(tokio::spawn(async move {
                a.oneshot(get_req("/api/v1/namespaces/default")).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        // all three are executing concurrently: a fourth is also served
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        gate.notify_waiters();
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap().status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn long_running_requests_skip_the_filter() {
        // TestApfSkipLongRunningRequest: an `exec` is never classified, so it is
        // served with no UID headers even though every seat is taken.
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) == 0).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, bob()).await;
        let a = app.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default/pods/p/exec"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));
        gate.notify_one();
        let _ = held.await;
    }

    #[tokio::test]
    async fn a_watch_is_classified_not_skipped() {
        // Unlike other long-running requests a watch goes through APF
        // (priority-and-fairness.go:90, `&& !isWatchRequest`).
        let app = app(10, Duration::from_millis(50), none_hook(), bob()).await;
        let resp = app
            .oneshot(get_req("/api/v1/watch/namespaces"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().contains_key(HEADER_PRIORITY_LEVEL_UID));
    }

    // priority-and-fairness.go:311-312: a 429 records the dropped request and
    // the termination (TestApfRejectRequest checks both metrics exist).
    #[tokio::test]
    async fn rejected_request_records_dropped_and_termination_metrics() {
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let l = [
            ("verb", "GET"),
            ("resource", "namespaces"),
            ("scope", "resource"),
            ("component", "apiserver"),
            ("code", "429"),
        ];
        let (t0, r0) = (
            sample("apiserver_request_terminations_total", &l),
            sample("apiserver_request_total", &l),
        );
        let gate = Arc::new(Notify::new());
        let first = Arc::new(AtomicUsize::new(0));
        let (g, f) = (gate.clone(), first.clone());
        let hook: Arc<dyn Fn() -> Option<Arc<Notify>> + Send + Sync> =
            Arc::new(move || (f.fetch_add(1, Ordering::SeqCst) == 0).then(|| g.clone()));
        let app = app(1, Duration::from_millis(50), hook, bob()).await;
        let a = app.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(sample("apiserver_request_terminations_total", &l) >= t0 + 1.0);
        assert!(sample("apiserver_request_total", &l) >= r0 + 1.0);
        gate.notify_one();
        held.await.unwrap().unwrap();
    }

    // MonitorRequest (endpoints/metrics/metrics.go:603) via the instrument
    // layer: every served request increments `apiserver_request_total` with its real response code, not
    // just the 429s.
    #[tokio::test]
    async fn served_request_counts_apiserver_request_total_with_status_code() {
        use rusternetes_api_server::flow_control::metric_test_util::sample;
        let l = [
            ("verb", "GET"),
            ("resource", "namespaces"),
            ("scope", "resource"),
            ("component", "apiserver"),
            ("code", "200"),
        ];
        let before = sample("apiserver_request_total", &l);
        let app = app(10, Duration::from_millis(50), none_hook(), bob()).await;
        let resp = app
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(sample("apiserver_request_total", &l) >= before + 1.0);
    }

    // httplog.AddKeyValue(ctx, "apf_pl", truncateLogField(pl.Name)) (:119).
    #[test]
    fn truncate_log_field_caps_at_64() {
        assert_eq!(truncate_log_field("short"), "short");
        let long = "x".repeat(100);
        assert_eq!(truncate_log_field(&long).len(), 64);
    }

    #[tokio::test]
    async fn queued_request_that_waits_too_long_is_a_429() {
        // TestApfCancelWaitRequest, against a level that queues. The filter is
        // built straight on an engine with a `Queue` level.
        use rusternetes_common::resources::flowcontrol::{
            LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration,
            PriorityLevelConfiguration, QueuingConfiguration,
        };
        use rusternetes_common::types::ObjectMeta;
        use rusternetes_common::validation::flowcontrol_bootstrap::mandatory_priority_level_configuration;
        use rusternetes_storage::build_key;

        let mut pl: PriorityLevelConfiguration =
            mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("catch-all");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(5),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues: 1,
                    hand_size: 1,
                    queue_length_limit: 10,
                }),
            }),
        });
        let st = Arc::new(MemoryStorage::new());
        st.create(
            &build_key("prioritylevelconfigurations", None, "catch-all"),
            &pl,
        )
        .await
        .unwrap();
        let engine = Arc::new(FlowControlEngine::with_limits(st, 1, 0));
        engine.initialize().await.unwrap();
        let filter = Arc::new(ApfFilter::new(engine, Duration::from_millis(80)));

        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        let seen = Arc::new(AtomicUsize::new(0));
        let s = seen.clone();
        let router = Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let (g, s) = (g.clone(), s.clone());
                    async move {
                        if s.fetch_add(1, Ordering::SeqCst) == 0 {
                            g.notified().await;
                        }
                        "ok"
                    }
                }),
            )
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ));
        let a = router.clone();
        let held =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/namespaces/default")).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let started = Instant::now();
        let resp = router
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            started.elapsed() >= Duration::from_millis(70),
            "it queued first"
        );
        gate.notify_one();
        let _ = held.await;
    }

    // `getRequestWaitContext` takes 1/4 of the deadline `WithRequestDeadline`
    // put on the request context (priority-and-fairness.go:418-420), not the
    // default wait limit. Through the real timeout middleware via `?timeout=`.
    #[tokio::test]
    async fn queue_wait_is_a_quarter_of_the_request_deadline() {
        use rusternetes_common::resources::flowcontrol::{
            LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration,
            PriorityLevelConfiguration, QueuingConfiguration,
        };
        use rusternetes_common::types::ObjectMeta;
        use rusternetes_common::validation::flowcontrol_bootstrap::mandatory_priority_level_configuration;
        use rusternetes_storage::build_key;

        let mut pl: PriorityLevelConfiguration =
            mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("catch-all");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(5),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues: 1,
                    hand_size: 1,
                    queue_length_limit: 10,
                }),
            }),
        });
        let st = Arc::new(MemoryStorage::new());
        st.create(
            &build_key("prioritylevelconfigurations", None, "catch-all"),
            &pl,
        )
        .await
        .unwrap();
        let engine = Arc::new(FlowControlEngine::with_limits(st, 1, 0));
        engine.initialize().await.unwrap();
        // Default wait limit 5s; `?timeout=400ms` must override it (100ms).
        let filter = Arc::new(ApfFilter::new(engine, Duration::from_secs(5)));

        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        let seen = Arc::new(AtomicUsize::new(0));
        let s = seen.clone();
        let router = Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let (g, s) = (g.clone(), s.clone());
                    async move {
                        if s.fetch_add(1, Ordering::SeqCst) == 0 {
                            g.notified().await;
                        }
                        "ok"
                    }
                }),
            )
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                rusternetes_api_server::request_timeout::request_timeout_middleware,
            ));
        let a = router.clone();
        let held = tokio::spawn(async move {
            a.oneshot(get_req("/api/v1/namespaces/default?timeout=400ms"))
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let started = Instant::now();
        let resp = router
            .oneshot(get_req("/api/v1/namespaces/default?timeout=400ms"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "waited the default limit, not 1/4 of the deadline: {:?}",
            started.elapsed()
        );
        gate.notify_one();
        let _ = held.await;
    }

    #[tokio::test]
    async fn finishing_the_last_request_reaps_a_quiescing_level() {
        // #2803: a removed level that is still busy quiesces; when its last
        // request finishes the filter reaps it (apf_filter.go:171-177).
        use rusternetes_common::resources::flowcontrol::{
            LimitResponse, LimitResponseType, LimitedPriorityLevelConfiguration,
            PriorityLevelConfiguration, QueuingConfiguration,
        };
        use rusternetes_common::types::ObjectMeta;
        use rusternetes_common::validation::flowcontrol_bootstrap::{
            mandatory_flow_schema, mandatory_priority_level_configuration,
        };
        use rusternetes_storage::build_key;

        let mut pl: PriorityLevelConfiguration =
            mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = ObjectMeta::new("a");
        pl.spec.limited = Some(LimitedPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(5),
            lending_concurrency_limit: None,
            lendable_percent: None,
            borrowing_limit_percent: None,
            limit_response: Some(LimitResponse {
                type_: LimitResponseType::Queue,
                queuing: Some(QueuingConfiguration {
                    queues: 4,
                    hand_size: 2,
                    queue_length_limit: 10,
                }),
            }),
        });
        let mut fs = mandatory_flow_schema("catch-all").unwrap();
        fs.metadata = ObjectMeta::new("to-a");
        fs.spec.matching_precedence = 1;
        fs.spec.priority_level_configuration.name = "a".into();
        let st = Arc::new(MemoryStorage::new());
        let pl_key = build_key("prioritylevelconfigurations", None, "a");
        st.create(&pl_key, &pl).await.unwrap();
        st.create(&build_key("flowschemas", None, "to-a"), &fs)
            .await
            .unwrap();
        let engine = Arc::new(FlowControlEngine::with_limits(st.clone(), 100, 0));
        engine.initialize().await.unwrap();
        assert_eq!(
            engine
                .classify(&RequestDigest {
                    user_name: "bob".into(),
                    groups: vec!["system:authenticated".into()],
                    is_resource_request: true,
                    verb: "get".into(),
                    resource: "namespaces".into(),
                    ..Default::default()
                })
                .priority_level,
            "a"
        );
        let filter = Arc::new(ApfFilter::new(engine.clone(), Duration::from_millis(50)));

        let gate = Arc::new(Notify::new());
        let g = gate.clone();
        let ctx = rusternetes_middleware::AuthContext { user: bob() };
        let router = Router::new()
            .route(
                "/api/v1/namespaces/default",
                get(move || {
                    let g = g.clone();
                    async move {
                        g.notified().await;
                        "ok"
                    }
                }),
            )
            .layer(from_fn_with_state(
                filter,
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ));
        let held =
            tokio::spawn(
                async move { router.oneshot(get_req("/api/v1/namespaces/default")).await },
            );
        tokio::time::sleep(Duration::from_millis(50)).await;
        st.delete(&pl_key).await.unwrap();
        engine.initialize().await.unwrap();
        assert!(engine.is_quiescing("a"));
        gate.notify_one();
        assert_eq!(held.await.unwrap().unwrap().status(), StatusCode::OK);
        assert!(engine.queueset_for("a").is_none(), "reaped on finish");
    }

    /// The estimator reads the filter's own tracker: a polled, large `pods`
    /// resource makes a list cost more than the minimum, an unpolled one does
    /// not (`ObjectCountNotFoundErr` -> minimum seats).
    #[tokio::test]
    async fn list_cost_follows_the_polled_object_counts() {
        let st = Arc::new(MemoryStorage::new());
        let engine = Arc::new(FlowControlEngine::with_limits(st.clone(), 100, 0));
        engine.initialize().await.unwrap();
        let filter = ApfFilter::new(engine.clone(), Duration::from_secs(1));
        let c = engine.classify(&RequestDigest {
            user_name: "bob".into(),
            groups: vec!["system:authenticated".into()],
            is_resource_request: true,
            verb: "list".into(),
            resource: "pods".into(),
            ..Default::default()
        });
        let info = RequestInfo {
            verb: "list".into(),
            resource: "pods".into(),
            ..Default::default()
        };
        let cost = |f: &ApfFilter<MemoryStorage>| {
            f.estimator
                .estimate_work(Some(&info), "", &c.flow_schema, &c.priority_level)
                .initial_seats
        };
        assert_eq!(cost(&filter), 1, "unpolled resource costs the minimum");
        filter.object_count_tracker().set(
            "pods",
            Stats {
                object_count: 50_000,
                estimated_average_object_size_bytes: 10_000,
            },
        );
        assert!(cost(&filter) > 1, "a large polled resource costs more");
    }

    // ---- watch initialization + RegisterWatch (priority-and-fairness.go:171-290) ----

    /// A router whose watch route optionally signals initialization, then
    /// keeps the handler alive until `gate` fires (a watch that has sent its
    /// initial events and is still being served).
    async fn watch_app(
        gate: Arc<Notify>,
        initialized: bool,
    ) -> (Router, Arc<ApfFilter<MemoryStorage>>) {
        let engine = Arc::new(FlowControlEngine::with_limits(
            Arc::new(MemoryStorage::new()),
            1,
            0,
        ));
        engine.initialize().await.unwrap();
        let filter = Arc::new(ApfFilter::new(engine, Duration::from_millis(50)));
        let ctx = rusternetes_middleware::AuthContext { user: bob() };
        let router = Router::new()
            .route(
                "/api/v1/watch/namespaces",
                get(move || {
                    let gate = gate.clone();
                    async move {
                        if initialized {
                            rusternetes_api_server::flow_control_watch_tracker::watch_initialized();
                        }
                        gate.notified().await;
                        "w"
                    }
                }),
            )
            .route("/api/v1/namespaces/default", get(|| async { "ok" }))
            .layer(from_fn_with_state(
                filter.clone(),
                priority_and_fairness::<MemoryStorage>,
            ))
            .layer(axum::middleware::from_fn(
                move |mut req: axum::extract::Request, next: Next| {
                    let ctx = ctx.clone();
                    async move {
                        req.extensions_mut().insert(ctx);
                        next.run(req).await
                    }
                },
            ));
        (router, filter)
    }

    fn create_namespace_info() -> RequestInfo {
        RequestInfo {
            verb: "create".into(),
            resource: "namespaces".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_watch_releases_its_seat_when_initialized_not_when_it_returns() {
        // The seat is held "until the request is finished from the APF point
        // of view (which is when its initialization is done)" (:217-218).
        let gate = Arc::new(Notify::new());
        let (app, _) = watch_app(gate.clone(), true).await;
        let a = app.clone();
        let watch =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/watch/namespaces")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // One seat on `catch-all`, and the watch is still being served: a
        // second request is admitted because the watch finished initializing.
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        gate.notify_one();
        let _ = watch.await;
    }

    #[tokio::test]
    async fn an_uninitialized_watch_keeps_its_seat_until_the_handler_returns() {
        // The deferred `watchInitializationSignal.Signal()` (:186-190) only
        // fires once the handler is done, so until then the seat is held.
        let gate = Arc::new(Notify::new());
        let (app, _) = watch_app(gate.clone(), false).await;
        let a = app.clone();
        let watch =
            tokio::spawn(async move { a.oneshot(get_req("/api/v1/watch/namespaces")).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let resp = app
            .clone()
            .oneshot(get_req("/api/v1/namespaces/default"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        gate.notify_one();
        let _ = watch.await;
    }

    #[tokio::test]
    async fn a_watch_is_registered_until_its_response_is_dropped() {
        // `forgetWatch` runs when the watch is over (:196-198), not when the
        // response head is ready.
        let gate = Arc::new(Notify::new());
        gate.notify_one(); // let the handler return its response right away
        let (app, filter) = watch_app(gate, true).await;
        let create = create_namespace_info();
        assert_eq!(
            filter
                .watch_tracker()
                .get_interested_watch_count(Some(&create)),
            0
        );
        let resp = app
            .oneshot(get_req("/api/v1/watch/namespaces"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            filter
                .watch_tracker()
                .get_interested_watch_count(Some(&create)),
            1,
            "registered while the body is alive"
        );
        drop(resp);
        assert_eq!(
            filter
                .watch_tracker()
                .get_interested_watch_count(Some(&create)),
            0,
            "forgotten when the watch ends"
        );
    }

    #[tokio::test]
    async fn the_estimator_sees_the_tracked_watches() {
        // #2775: the work estimator's watch count getter reads the tracker.
        let gate = Arc::new(Notify::new());
        gate.notify_one();
        let (app, filter) = watch_app(gate, true).await;
        let resp = app
            .oneshot(get_req("/api/v1/watch/namespaces"))
            .await
            .unwrap();
        let info = create_namespace_info();
        assert_eq!(filter.watch_count_for_test(&info), 1);
        drop(resp);
        assert_eq!(filter.watch_count_for_test(&info), 0);
    }
    // ---- config controller: event-driven reload (#2811) ----
    // `NewTestableController` + informer handlers (apf_controller.go:308-368)
    // enqueue the config key on every FlowSchema/PriorityLevelConfiguration
    // event; the worker re-digests. A reload therefore does not wait for any
    // poll interval: the resync period here is an hour.

    async fn wait_for<F: Fn() -> bool>(cond: F) -> bool {
        for _ in 0..100 {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cond()
    }

    fn extra_pl(name: &str) -> rusternetes_common::resources::PriorityLevelConfiguration {
        use rusternetes_common::validation::flowcontrol_bootstrap::mandatory_priority_level_configuration;
        let mut pl = mandatory_priority_level_configuration("catch-all").unwrap();
        pl.metadata = rusternetes_common::types::ObjectMeta::new(name);
        pl
    }

    fn ev_mod(k: &str, v: serde_json::Value) -> rusternetes_storage::WatchEvent {
        rusternetes_storage::WatchEvent::Modified(k.into(), v.to_string())
    }

    // Handlers apf_controller.go:308-368: PL update triggers on spec only.
    #[test]
    fn pl_update_triggers_only_on_spec_change() {
        let mut f = ConfigEventFilter::default();
        let add = rusternetes_storage::WatchEvent::Added(
            "k".into(),
            serde_json::json!({"spec": {"a": 1}, "status": {}}).to_string(),
        );
        assert!(f.triggers(false, &add));
        // Status-only change: no trigger.
        assert!(!f.triggers(
            false,
            &ev_mod(
                "k",
                serde_json::json!({"spec": {"a": 1}, "status": {"x": 1}})
            )
        ));
        assert!(f.triggers(false, &ev_mod("k", serde_json::json!({"spec": {"a": 2}}))));
        // A resync-style identical update: no trigger.
        assert!(!f.triggers(false, &ev_mod("k", serde_json::json!({"spec": {"a": 2}}))));
        let del = rusternetes_storage::WatchEvent::Deleted("k".into(), String::new());
        assert!(f.triggers(false, &del));
    }

    // FlowSchema update triggers on spec OR status (:337-358).
    #[test]
    fn fs_update_triggers_on_spec_or_status_change() {
        let mut f = ConfigEventFilter::default();
        let m = |s: i32, st: i32| {
            ev_mod(
                "k",
                serde_json::json!({"spec": {"p": s}, "status": {"c": st}}),
            )
        };
        assert!(f.triggers(true, &m(1, 1)));
        assert!(!f.triggers(true, &m(1, 1)));
        assert!(f.triggers(true, &m(1, 2)));
        assert!(f.triggers(true, &m(2, 2)));
    }

    // processNextWorkItem (:517-520): a failed sync is requeued with the
    // 200ms exponential-failure limiter, success forgets the failures.
    #[tokio::test(start_paused = true)]
    async fn failed_sync_is_requeued_with_exponential_backoff() {
        let calls = Arc::new(Mutex::new(Vec::<tokio::time::Instant>::new()));
        let c = calls.clone();
        let _task = tokio::spawn(run_config_worker(
            move || {
                let c = c.clone();
                async move {
                    let mut c = c.lock().unwrap();
                    c.push(tokio::time::Instant::now());
                    if c.len() < 4 {
                        Err("boom".to_string())
                    } else {
                        Ok(Duration::ZERO)
                    }
                }
            },
            |_prefix: String| async { Some(futures::stream::pending().boxed()) },
        ));
        tokio::time::sleep(Duration::from_secs(5)).await;
        let c = calls.lock().unwrap();
        assert_eq!(c.len(), 4, "no further syncs once it succeeds and idles");
        assert_eq!(c[1] - c[0], Duration::from_millis(200));
        assert_eq!(c[2] - c[1], Duration::from_millis(400));
        assert_eq!(c[3] - c[2], Duration::from_millis(800));
    }

    // `configQueue.Add(0)` dedupes: a burst of events is one sync, and a
    // quiet period runs none (no polling).
    #[tokio::test(start_paused = true)]
    async fn event_burst_is_one_sync_and_idle_never_syncs() {
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rx = Arc::new(Mutex::new(Some(rx)));
        let c = n.clone();
        let _task = tokio::spawn(run_config_worker(
            move || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(Duration::ZERO)
                }
            },
            move |prefix: String| {
                let rx = rx.clone();
                async move {
                    if prefix.contains("prioritylevel") {
                        let rx = rx.lock().unwrap().take().unwrap();
                        Some(tokio_stream::wrappers::UnboundedReceiverStream::new(rx).boxed())
                    } else {
                        Some(futures::stream::pending().boxed())
                    }
                }
            },
        ));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(n.load(Ordering::SeqCst), 1, "initial sync only");
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert_eq!(n.load(Ordering::SeqCst), 1, "idle: no polling");
        for i in 0..5 {
            tx.send(Ok(rusternetes_storage::WatchEvent::Added(
                format!("k{i}"),
                "{}".into(),
            )))
            .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(n.load(Ordering::SeqCst) >= 2);
        assert!(n.load(Ordering::SeqCst) <= 3, "burst coalesced");
    }

    #[tokio::test]
    async fn config_change_applies_without_waiting_for_a_poll() {
        use rusternetes_storage::build_key;
        let st = Arc::new(MemoryStorage::new());
        let engine = Arc::new(FlowControlEngine::with_limits(st.clone(), 100, 0));
        engine.initialize().await.unwrap();
        let _task = spawn_config_reloader(engine.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(engine.concurrency_limit("evented").is_none());
        st.create(
            &build_key("prioritylevelconfigurations", None, "evented"),
            &extra_pl("evented"),
        )
        .await
        .unwrap();
        let e = engine.clone();
        assert!(
            wait_for(move || e.concurrency_limit("evented").is_some()).await,
            "PL create event must reach the engine within the resync period"
        );
        st.delete(&build_key("prioritylevelconfigurations", None, "evented"))
            .await
            .unwrap();
        let e = engine.clone();
        assert!(
            wait_for(move || e.concurrency_limit("evented").is_none()).await,
            "PL delete event must reach the engine"
        );
    }

    // The tracker hook does not depend on APF (config.go:961-970, NewConfig
    // always sets the tracker at config.go:472).
    #[tokio::test]
    async fn tracker_hook_registers_standalone_without_apf() {
        let registry = rusternetes_api_server::post_start_hooks::PostStartHooks::new();
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        register_storage_object_count_tracker_hook(
            &registry,
            Arc::new(ObjectCountTracker::new()),
            stop_rx,
        );
        let names: Vec<String> = registry.checks().into_iter().map(|(n, _)| n).collect();
        assert_eq!(
            names,
            vec!["poststarthook/storage-object-count-tracker-hook"]
        );
    }

    // The APF-off entry points (`main.rs` and `lib.rs::run`) share one helper,
    // so the hook is on `/readyz/poststarthook/...` for both and finishes.
    #[tokio::test]
    async fn tracker_hook_without_apf_helper_registers_and_finishes() {
        let registry = rusternetes_api_server::post_start_hooks::PostStartHooks::new();
        register_object_count_tracker_hook_without_apf(&registry);
        for _ in 0..50 {
            if registry.check("storage-object-count-tracker-hook") == Some(Ok(())) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("storage-object-count-tracker-hook never finished");
    }

    // GenericAPIServer.New (server/config.go:921-970) registers exactly these
    // three hooks when FlowControl and StorageObjectCountTracker are set; each
    // is a `poststarthook/<name>` check that finishes (the hooks return nil
    // after starting their goroutines).
    #[tokio::test]
    async fn apf_post_start_hooks_registered_and_finish() {
        let registry = rusternetes_api_server::post_start_hooks::PostStartHooks::new();
        let engine = Arc::new(FlowControlEngine::with_limits(
            Arc::new(MemoryStorage::new()),
            400,
            200,
        ));
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        register_post_start_hooks(
            &registry,
            engine,
            Arc::new(ObjectCountTracker::new()),
            stop_rx,
        );
        let names: Vec<String> = registry.checks().into_iter().map(|(n, _)| n).collect();
        assert_eq!(
            names,
            vec![
                "poststarthook/priority-and-fairness-config-consumer",
                "poststarthook/priority-and-fairness-filter",
                "poststarthook/storage-object-count-tracker-hook",
            ]
        );
        for _ in 0..100 {
            if registry.checks().iter().all(|(_, r)| r.is_ok()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("hooks never finished: {:?}", registry.checks());
    }
}
