//! Unit tests moved from `src/request_timeout.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::body::Body;
    pub use axum::extract::Request;
    pub use axum::http::{header, StatusCode};
    pub use axum::middleware::Next;
    pub use axum::response::{IntoResponse, Response};
    pub use futures::StreamExt;
    pub use rusternetes_api_server::apiserver_request_metrics::{
        record_request_abort, record_request_post_timeout, record_request_termination,
        request_meta, RequestMeta, POST_TIMEOUT_HANDLER_OK, POST_TIMEOUT_HANDLER_PANIC,
        POST_TIMEOUT_SOURCE_TIMEOUT_HANDLER,
    };
    pub use rusternetes_api_server::flow_control_filter::{
        is_long_running, DEFAULT_REQUEST_TIMEOUT,
    };
    pub use rusternetes_common::cancellation::{
        request_cancellation_with_deadline, with_request_cancellation, RequestCanceller,
    };
    pub use rusternetes_common::types::{Status, StatusDetails};
    pub use std::future::Future;
    pub use std::pin::Pin;
    pub use std::sync::OnceLock;
    pub use std::time::Duration;
    pub use tracing::Instrument;
}

mod tests {
    use super::__parent::*;
    use axum::body::Body;
    use rusternetes_api_server::flow_control::metric_test_util::sample;
    use rusternetes_api_server::request_timeout::*;
    use std::sync::Arc;
    use tower::ServiceExt;

    const MAX: Duration = Duration::from_secs(60);

    // TestParseTimeout (request_deadline_test.go:41-94).
    #[test]
    fn parse_timeout_table() {
        assert_eq!(parse_timeout(Some("timeout=")), Ok(None));
        assert_eq!(parse_timeout(None), Ok(None));
        assert_eq!(
            parse_timeout(Some("timeout=10s")),
            Ok(Some(Duration::from_secs(10)))
        );
        assert_eq!(parse_timeout(Some("timeout=0s")), Ok(Some(Duration::ZERO)));
        let err = parse_timeout(Some("timeout=foo")).unwrap_err();
        assert!(
            err.contains("invalid timeout specified in the request URL"),
            "{err}"
        );
    }

    // TestWithRequestDeadline's deadline selection (:96-170).
    #[test]
    fn deadline_selection() {
        let d = |q| request_deadline(q, MAX).unwrap();
        assert_eq!(d(Some("timeout=15s")), Duration::from_secs(15));
        assert_eq!(d(Some("timeout=0s")), MAX);
        assert_eq!(d(Some("timeout=")), MAX);
        assert_eq!(d(None), MAX);
        assert_eq!(d(Some("timeout=61s")), MAX);
        assert_eq!(d(Some("timeout=-5s")), MAX);
        assert!(request_deadline(Some("timeout=foo"), MAX).is_err());
    }

    async fn serve(uri: &str, handler_sleep: Duration) -> Response {
        let app = axum::Router::new()
            .fallback(move || async move {
                tokio::time::sleep(handler_sleep).await;
                "ok"
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        app.oneshot(
            axum::http::Request::builder()
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    }

    // TestTimeout (timeout_test.go:79-): a request outliving its deadline gets
    // 504 with NewTimeoutError's Status, and RecordRequestTermination (504).
    #[tokio::test]
    async fn slow_request_gets_504_status_and_termination_metric() {
        let l = [
            ("resource", "timedoutthings"),
            ("component", "apiserver"),
            ("code", "504"),
        ];
        let before = sample("apiserver_request_terminations_total", &l);
        let resp = serve(
            "/api/v1/namespaces/default/timedoutthings?timeout=100ms",
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(resp.status(), 504);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["kind"], "Status");
        assert_eq!(v["status"], "Failure");
        assert_eq!(v["code"], 504);
        assert_eq!(v["reason"], "Timeout");
        assert_eq!(
            v["message"],
            "Timeout: request did not complete within the allotted timeout"
        );
        assert_eq!(
            sample("apiserver_request_terminations_total", &l),
            before + 1.0
        );
    }

    #[tokio::test]
    async fn fast_request_is_untouched() {
        let resp = serve("/api/v1/namespaces?timeout=5s", Duration::ZERO).await;
        assert_eq!(resp.status(), 200);
    }

    // "the request is long running, no deadline is expected to be set".
    #[tokio::test]
    async fn long_running_requests_have_no_deadline() {
        let resp = serve(
            "/api/v1/namespaces/default/pods?watch=true&timeout=50ms",
            Duration::from_millis(200),
        )
        .await;
        assert_eq!(resp.status(), 200);
        let resp = serve(
            "/api/v1/namespaces/default/pods/p/exec?timeout=50ms",
            Duration::from_millis(200),
        )
        .await;
        assert_eq!(resp.status(), 200);
    }

    // "the timeout specified is malformed, the request is aborted with HTTP 400".
    #[tokio::test]
    async fn malformed_timeout_is_400() {
        let resp = serve("/api/v1/namespaces?timeout=foo", Duration::ZERO).await;
        assert_eq!(resp.status(), 400);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["reason"], "BadRequest");
        assert!(v["message"]
            .as_str()
            .unwrap()
            .contains("invalid timeout specified in the request URL"));
    }

    // baseTimeoutWriter.timeout (timeout.go:255-275): once the handler has
    // started the response the request cannot be timed out with a 504; the
    // connection is aborted (ErrAbortHandler) and WithPanicRecovery calls
    // RecordRequestAbort (wrap.go:46-48).
    #[tokio::test]
    async fn deadline_during_body_aborts_and_records_abort() {
        let l = [("resource", "abortingthings")];
        let before = sample("apiserver_request_aborts_total", &l);
        let app = axum::Router::new()
            .fallback(|| async {
                let stream = futures::stream::unfold(0u8, |i| async move {
                    if i == 0 {
                        Some((
                            Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"a")),
                            1,
                        ))
                    } else {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        None
                    }
                });
                Body::from_stream(stream)
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/namespaces/default/abortingthings?timeout=100ms")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let res = axum::body::to_bytes(resp.into_body(), usize::MAX).await;
        assert!(res.is_err(), "body read must be aborted");
        assert_eq!(sample("apiserver_request_aborts_total", &l), before + 1.0);
    }

    struct Capture(tokio::sync::Mutex<Vec<rusternetes_common::audit::AuditEvent>>);
    #[async_trait::async_trait]
    impl rusternetes_common::audit::AuditBackend for Capture {
        async fn log(&self, e: rusternetes_common::audit::AuditEvent) -> Result<(), String> {
            self.0.lock().await.push(e);
            Ok(())
        }
        async fn flush(&self) -> Result<(), String> {
            Ok(())
        }
    }

    // TestWithRequestDeadlineWithInvalidTimeoutIsAudited
    // (request_deadline_test.go:256-284) + TestWithFailedRequestAudit
    // (:350-): the 400 is audited as ONE event at stage ResponseStarted whose
    // ResponseStatus carries the code and the statusErr message.
    #[tokio::test]
    async fn malformed_timeout_is_audited() {
        use rusternetes_common::audit::AuditStage;
        let cap = Arc::new(Capture(Default::default()));
        rusternetes_api_server::audit::install_audit(rusternetes_api_server::audit::AuditConfig {
            policy: rusternetes_api_server::audit::Policy::from_yaml(
                "apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n- level: RequestResponse\n",
            )
            .unwrap(),
            sink: cap.clone(),
        });
        let resp = serve(
            "/api/v1/namespaces/n/auditedthings?timeout=foo",
            Duration::ZERO,
        )
        .await;
        assert_eq!(resp.status(), 400);
        assert!(resp.headers().contains_key("Audit-ID"));
        // The sink is process-global: other tests' 400s land here too.
        let ev: Vec<_> = cap
            .0
            .lock()
            .await
            .iter()
            .filter(|e| e.request_uri.contains("auditedthings"))
            .cloned()
            .collect();
        assert_eq!(ev.len(), 1, "{ev:?}");
        assert_eq!(ev[0].stage, AuditStage::ResponseStarted);
        assert_eq!(ev[0].verb, "list");
        let rs = ev[0].response_status.as_ref().unwrap();
        assert_eq!(rs.code, 400);
        assert!(rs
            .message
            .as_deref()
            .unwrap()
            .contains("invalid timeout specified in the request URL"));
    }

    // ---- detached handler / post-timeout (timeout.go:101-148) ----

    /// `apiserver_request_post_timeout_total` carries no per-test label, so the
    /// tests that read it take turns.
    static POST_TIMEOUT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn post_timeout(status: &str) -> f64 {
        sample(
            "apiserver_request_post_timeout_total",
            &[("source", "timeout-handler"), ("status", status)],
        )
    }

    async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    fn get(uri: &str) -> axum::http::Request<Body> {
        axum::http::Request::builder()
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    // TestTimeout "Times out" + the deferred goroutine (timeout_test.go:128-160,
    // timeout.go:125-142): the handler outlives the 504; when it finishes,
    // RecordRequestPostTimeout(timeout-handler, ok).
    #[tokio::test]
    async fn handler_outlives_504_and_records_post_timeout_ok() {
        let _g = POST_TIMEOUT.lock().await;
        let before = post_timeout("ok");
        let release = Arc::new(tokio::sync::Notify::new());
        let r = release.clone();
        let app = axum::Router::new()
            .fallback(move || {
                let r = r.clone();
                async move {
                    r.notified().await;
                    "late"
                }
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        let resp = app
            .oneshot(get("/api/v1/namespaces/default/postok?timeout=50ms"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 504);
        release.notify_one();
        eventually("post-timeout ok", || post_timeout("ok") >= before + 1.0).await;
    }

    // A handler that panics after the 504: PostTimeoutHandlerPanic.
    #[tokio::test]
    async fn panic_after_deadline_records_post_timeout_panic() {
        let _g = POST_TIMEOUT.lock().await;
        let before = post_timeout("panic");
        let app = axum::Router::new()
            .fallback(|| async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if true {
                    panic!("inner handler panics after the deadline");
                }
                "unreachable"
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        let resp = app
            .oneshot(get("/api/v1/namespaces/default/postpanic?timeout=50ms"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 504);
        eventually("post-timeout panic", || {
            post_timeout("panic") >= before + 1.0
        })
        .await;
    }

    // TestTimeout "Panics" (timeout_test.go:170-190): a panic before the
    // deadline is re-raised on the request task so WithPanicRecovery answers
    // 500; it is not a post-timeout event.
    #[tokio::test]
    async fn panic_before_deadline_reaches_panic_recovery() {
        let _g = POST_TIMEOUT.lock().await;
        let panics = post_timeout("panic");
        let app = axum::Router::new()
            .fallback(|| async {
                if true {
                    panic!("inner handler panics");
                }
                "unreachable"
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware))
            .layer(tower_http::catch_panic::CatchPanicLayer::custom(
                |_p: Box<dyn std::any::Any + Send>| {
                    (StatusCode::INTERNAL_SERVER_ERROR, "panic'd").into_response()
                },
            ));
        let resp = app
            .oneshot(get("/api/v1/namespaces/default/prepanic?timeout=5s"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 500);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(post_timeout("panic"), panics);
    }

    // The handler's request context (what etcd3 calls select on): once the
    // deadline passes, a StorageBackend call inside it fails with a timeout
    // instead of running to completion after the 504.
    #[tokio::test]
    async fn storage_call_after_deadline_fails_in_the_detached_handler() {
        use rusternetes_storage::{Storage, StorageBackend};
        let _g = POST_TIMEOUT.lock().await;
        let backend = Arc::new(StorageBackend::new_memory());
        let outcome = Arc::new(std::sync::Mutex::new(None::<bool>));
        let (b, o) = (backend.clone(), outcome.clone());
        let app = axum::Router::new()
            .fallback(move || {
                let (b, o) = (b.clone(), o.clone());
                async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    let r = Storage::current_revision(b.as_ref()).await;
                    *o.lock().unwrap() = Some(r.is_err());
                    "late"
                }
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        let resp = app
            .oneshot(get("/api/v1/namespaces/default/ctxstore?timeout=50ms"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 504);
        eventually("handler to finish", || outcome.lock().unwrap().is_some()).await;
        assert_eq!(*outcome.lock().unwrap(), Some(true));
        // And a call before the deadline is untouched.
        let ok = Arc::new(std::sync::Mutex::new(None::<bool>));
        let (b, o) = (backend.clone(), ok.clone());
        let app = axum::Router::new()
            .fallback(move || {
                let (b, o) = (b.clone(), o.clone());
                async move {
                    let r = Storage::current_revision(b.as_ref()).await;
                    *o.lock().unwrap() = Some(r.is_ok());
                    "fast"
                }
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        let resp = app
            .oneshot(get("/api/v1/namespaces/default/ctxstore?timeout=5s"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(*ok.lock().unwrap(), Some(true));
    }

    // Client disconnect: axum drops the middleware future. Go cancels
    // `req.Context()` (net/http connReader.backgroundRead), which also fires
    // timeoutCh: the handler sees context.Canceled, the request counts as a
    // 504 termination and the handler's end is recorded as post-timeout.
    #[tokio::test]
    async fn client_disconnect_cancels_the_handler_context() {
        use rusternetes_common::cancellation::current_request_cancellation;
        let _g = POST_TIMEOUT.lock().await;
        let before = post_timeout("ok");
        let l = [
            ("resource", "disconnectthings"),
            ("component", "apiserver"),
            ("code", "504"),
        ];
        let term_before = sample("apiserver_request_terminations_total", &l);
        let saw_cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = saw_cancel.clone();
        let app = axum::Router::new()
            .fallback(move || {
                let s = s.clone();
                async move {
                    let ctx = current_request_cancellation().expect("request context");
                    ctx.cancelled().await;
                    s.store(true, std::sync::atomic::Ordering::SeqCst);
                    "gone"
                }
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware));
        // The client gives up after 50ms: its future is dropped.
        let r = tokio::time::timeout(
            Duration::from_millis(50),
            app.oneshot(get(
                "/api/v1/namespaces/default/disconnectthings?timeout=30s",
            )),
        )
        .await;
        assert!(r.is_err(), "the request must still be in flight");
        eventually("handler to observe the cancel", || {
            saw_cancel.load(std::sync::atomic::Ordering::SeqCst)
        })
        .await;
        eventually("post-timeout ok", || post_timeout("ok") >= before + 1.0).await;
        assert_eq!(
            sample("apiserver_request_terminations_total", &l),
            term_before + 1.0
        );
    }

    // The handler runs on its own task: the task-locals installed by the
    // filters OUTSIDE this one (request-info, the payload scope) and the
    // request context itself must be re-scoped inside it.
    #[tokio::test]
    async fn outer_task_locals_reach_the_spawned_handler() {
        use rusternetes_common::cancellation::current_request_cancellation;
        let app = axum::Router::new()
            .fallback(|| async {
                let info = rusternetes_api_server::request_scope::current_request_info().is_some();
                let payload = rusternetes_common::dump::current_payload();
                let ctx = current_request_cancellation();
                format!(
                    "{info} {} {}",
                    payload.flatten().map(|b| b.len()).unwrap_or(0),
                    ctx.and_then(|c| c.deadline()).is_some()
                )
            })
            .layer(axum::middleware::from_fn(request_timeout_middleware))
            .layer(axum::middleware::from_fn(
                rusternetes_api_server::request_scope::request_info_middleware,
            ));
        let resp = rusternetes_common::dump::with_payload(
            axum::body::Bytes::from_static(b"abcd"),
            app.oneshot(get("/api/v1/namespaces/default/locals?timeout=5s")),
        )
        .await
        .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"true 4 true");
    }
}
