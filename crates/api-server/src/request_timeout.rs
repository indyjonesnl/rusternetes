//! Request deadline and timeout filters.
//!
//! Port of `endpoints/filters/request_deadline.go` (`WithRequestDeadline`,
//! `parseTimeout`) and `server/filters/timeout.go`
//! (`WithTimeoutForNonLongRunningRequests`, `timeoutHandler`,
//! `baseTimeoutWriter.timeout`) from `staging/src/k8s.io/apiserver/pkg`
//! (release-1.35), placed where `DefaultBuildHandlerChain` puts them
//! (`server/config.go:1065-1071`): inside request-info, outside everything
//! else.
//!
//! Like Go, the handler runs detached (`tokio::spawn`, timeout.go:101-116) and
//! outlives the 504: its request context (`rusternetes_common::cancellation`,
//! the task-local counterpart of `req.Context()`) is done once the deadline
//! passes or the client goes away, which ends the `StorageBackend` and webhook
//! calls it is blocked in, and `RecordRequestPostTimeout` records how it
//! finished (timeout.go:125-142).
//!
//! DEVIATIONS: (1) The request context is a task-local, not a parameter, and
//! only the storage dispatch point and the webhook dispatchers observe it; a
//! handler blocked elsewhere (e.g. proxying upstream) runs to its own timeouts,
//! as a Go handler that ignores `ctx` would. A client disconnect is detected
//! by axum dropping the middleware future (`DetachedHandler`'s `Drop`), not by
//! net/http's background read. (2) Go distinguishes "handler has written the header" via
//! `baseTimeoutWriter.wroteHeader`; here the handler has "started the
//! response" once it returns a `Response`, after which only the body can still
//! run past the deadline. That is the abort case: the body stream is failed
//! (hyper drops the connection, the analogue of `panic(http.ErrAbortHandler)`)
//! and `RecordRequestAbort` is called, as `WithPanicRecovery` does
//! (wrap.go:44-48). (3) A negative `?timeout=` parses to a zero duration,
//! which selects the default exactly as a negative one does upstream
//! (`userSpecifiedTimeout > 0`).

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rusternetes_common::cancellation::{
    request_cancellation_with_deadline, with_request_cancellation, RequestCanceller,
};
use rusternetes_common::types::{Status, StatusDetails};
use tracing::Instrument;

use crate::apiserver_request_metrics::{
    record_request_abort, record_request_post_timeout, record_request_termination, request_meta,
    RequestMeta, POST_TIMEOUT_HANDLER_OK, POST_TIMEOUT_HANDLER_PANIC,
    POST_TIMEOUT_SOURCE_TIMEOUT_HANDLER,
};
use crate::flow_control_filter::{is_long_running, DEFAULT_REQUEST_TIMEOUT};

/// `invalidTimeoutInURL` (request_deadline.go:41).
const INVALID_TIMEOUT_IN_URL: &str = "invalid timeout specified in the request URL";

/// `--request-timeout` (`ServerRunOptions.RequestTimeout`, config.go:445),
/// the `requestTimeoutMaximum` of `WithRequestDeadline`.
static REQUEST_TIMEOUT: OnceLock<Duration> = OnceLock::new();

/// Set `--request-timeout`; the first call wins.
pub fn set_request_timeout(d: Duration) {
    let _ = REQUEST_TIMEOUT.set(d);
}

fn request_timeout() -> Duration {
    REQUEST_TIMEOUT
        .get()
        .copied()
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT)
}

/// The request context's deadline and received timestamp, as
/// `withRequestDeadline` leaves them in `req.Context()`
/// (request_deadline.go:91-96): `ctx.Deadline()` and
/// `apirequest.ReceivedTimestampFrom(ctx)`. Absent on long-running requests,
/// which get no deadline (request_deadline.go:67-70).
#[derive(Clone, Copy, Debug)]
pub struct RequestDeadline {
    pub received_at: std::time::Instant,
    pub deadline: std::time::Instant,
}

/// `parseTimeout` (request_deadline.go:155-167): the user's `?timeout=`.
pub fn parse_timeout(query: Option<&str>) -> Result<Option<Duration>, String> {
    let value = query
        .and_then(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .find(|(k, _)| k == "timeout")
                .map(|(_, v)| v.into_owned())
        })
        .unwrap_or_default();
    if value.is_empty() {
        return Ok(None);
    }
    let ns = rusternetes_common::go_duration::parse_go_duration(&value)
        .map_err(|e| format!("{INVALID_TIMEOUT_IN_URL} - {e}"))?;
    Ok(Some(Duration::from_nanos(u64::try_from(ns).unwrap_or(0))))
}

/// The deadline `withRequestDeadline` sets (request_deadline.go:81-89): the
/// user's timeout when it is positive and below the maximum, else the maximum.
pub fn request_deadline(query: Option<&str>, max: Duration) -> Result<Duration, String> {
    Ok(match parse_timeout(query)? {
        Some(user) if user > Duration::ZERO && user < max => user,
        _ => max,
    })
}

fn status_response(code: StatusCode, status: Status) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&status).unwrap_or_default(),
    )
        .into_response()
}

/// `apierrors.NewTimeoutError("request did not complete within the allotted
/// timeout", 0)` (errors.go:401-411).
fn timeout_error() -> Response {
    let details = StatusDetails {
        name: None,
        group: None,
        kind: None,
        uid: None,
        causes: None,
        retry_after_seconds: Some(0),
    };
    status_response(
        StatusCode::GATEWAY_TIMEOUT,
        Status::failure_with_details(
            "Timeout: request did not complete within the allotted timeout",
            "Timeout",
            504,
            details,
        ),
    )
}

/// `WithRequestDeadline` (request_deadline.go:54-102) +
/// `WithTimeoutForNonLongRunningRequests` / `timeoutHandler.ServeHTTP`
/// (timeout.go:36-150).
pub async fn request_timeout_middleware(req: Request, next: Next) -> Response {
    let started = tokio::time::Instant::now();
    let meta = request_meta(&req);
    if is_long_running(
        &meta.verb,
        meta.is_resource_request,
        &meta.subresource,
        &meta.path,
    ) {
        return next.run(req).await;
    }
    let timeout = match request_deadline(req.uri().query(), request_timeout()) {
        Ok(t) => t,
        Err(msg) => {
            // `withFailedRequestAudit(failedErrorHandler(..), statusErr, ..)`
            // (request_deadline.go:75-77).
            let status = Status::failure(msg, "BadRequest", 400);
            let failed = status_response(StatusCode::BAD_REQUEST, status.clone());
            return crate::audit::audit_failed_request(
                req.method(),
                req.uri(),
                req.headers(),
                &status,
                failed,
            )
            .await;
        }
    };
    let deadline = started + timeout;
    // `context.WithDeadline(ctx, started.Add(timeout))` (request_deadline.go:91-96):
    // what `getRequestWaitContext` reads in APF.
    let mut req = req;
    req.extensions_mut().insert(RequestDeadline {
        received_at: started.into_std(),
        deadline: deadline.into_std(),
    });

    // `r.Context()` of the handler: the deadline, cancelled when the client
    // goes away (see [`DetachedHandler`]).
    let (canceller, ctx) = request_cancellation_with_deadline(Some(deadline));
    // `rCopy := r.Clone(r.Context()); go func() { t.handler.ServeHTTP(w, rCopy) }()`
    // (timeout.go:98-116). Task-locals set by the filters OUTSIDE this one do
    // not cross `tokio::spawn`, so re-scope them inside.
    let request_info = crate::request_scope::current_request_info();
    let payload = rusternetes_common::dump::current_payload();
    let handler = Box::pin(next.run(req)) as Pin<Box<dyn Future<Output = Response> + Send>>;
    let handler: Pin<Box<dyn Future<Output = Response> + Send>> = match request_info {
        Some(info) => Box::pin(crate::request_scope::with_request_info(info, handler)),
        None => handler,
    };
    let handler: Pin<Box<dyn Future<Output = Response> + Send>> = match payload {
        Some(p) => Box::pin(rusternetes_common::dump::scope_payload(p, handler)),
        None => handler,
    };
    let handler = crate::handlers::list_options::with_list_snapshot(handler);
    let handler = with_request_cancellation(ctx, handler).instrument(tracing::Span::current());
    let mut detached = DetachedHandler {
        handle: Some(tokio::spawn(handler)),
        canceller,
        meta: meta.clone(),
    };

    // `select { case err := <-resultCh: ...; case <-timeoutCh: ... }`
    // (timeout.go:117-148). `timeoutCh` is `r.Context().Done()`; the cancel
    // half of it is [`DetachedHandler`]'s `Drop`, the deadline is this arm.
    let result = {
        let handle = detached.handle.as_mut().expect("handle is set");
        tokio::select! {
            biased;
            res = handle => Some(res),
            _ = tokio::time::sleep_until(deadline) => None,
        }
    };
    match result {
        Some(res) => {
            detached.handle = None;
            match res {
                Ok(resp) => abort_body_at(resp, deadline, meta),
                // "panic if error occurs" (timeout.go:119-122): the
                // handler's panic payload continues on this task, to
                // `WithPanicRecovery`.
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => {
                    tracing::error!("request handler task failed: {e}");
                    let status = Status::failure("Internal error occurred", "InternalError", 500);
                    status_response(StatusCode::INTERNAL_SERVER_ERROR, status)
                }
            }
        }
        None => {
            // `defer postTimeoutFn()` (timeout.go:56-58, :146) after
            // `tw.timeout(err)`, then the deferred post-timeout goroutine
            // (:125-142).
            let resp = timeout_error();
            record_request_termination(&meta, "apiserver", 504);
            detached.finish_in_background();
            resp
        }
    }
}

/// The handler goroutine of `timeoutHandler.ServeHTTP`: it outlives the 504
/// (timeout.go:101-148), so it runs as its own task, and its context is
/// cancelled when this guard is dropped.
///
/// Go's `Request.Context()` is cancelled by net/http when the client's
/// connection goes away (`connReader.backgroundRead` -> `cancelCtx`), which
/// also fires `timeoutCh` and so takes the timeout branch. axum instead drops
/// the middleware future on a client disconnect; that drop IS the signal.
/// `Drop` therefore cancels the context (storage calls and webhook calls the
/// handler is blocked in end with `context.Canceled`) and counts the request as
/// timed out, exactly as the `<-timeoutCh` arm does. The handler task is not
/// aborted: like the Go goroutine it runs on to its next context check and
/// finishes, and [`record_post_timeout`] observes how.
struct DetachedHandler {
    handle: Option<tokio::task::JoinHandle<Response>>,
    canceller: RequestCanceller,
    meta: RequestMeta,
}

impl DetachedHandler {
    /// The `<-timeoutCh` arm's deferred goroutine: wait for the handler and
    /// record `RecordRequestPostTimeout`.
    fn finish_in_background(&mut self) {
        if let Some(handle) = self.handle.take() {
            record_post_timeout(handle, &self.meta);
        }
    }
}

impl Drop for DetachedHandler {
    fn drop(&mut self) {
        if self.handle.is_some() {
            // The client went away before the handler finished.
            self.canceller.cancel();
            record_request_termination(&self.meta, "apiserver", 504);
            self.finish_in_background();
        }
    }
}

/// timeout.go:129-141: after the request timed out, wait for the handler to
/// finish, then `RecordRequestPostTimeout(PostTimeoutSourceTimeoutHandler,
/// ok|panic)` and log "Post-timeout activity".
fn record_post_timeout(handle: tokio::task::JoinHandle<Response>, meta: &RequestMeta) {
    let method = meta.method.clone();
    let path = meta.path.clone();
    tokio::spawn(async move {
        let timed_out_at = tokio::time::Instant::now();
        let res = handle.await;
        let status = match &res {
            Ok(_) => POST_TIMEOUT_HANDLER_OK,
            // "a non nil res indicates that there was a panic."
            Err(_) => POST_TIMEOUT_HANDLER_PANIC,
        };
        record_request_post_timeout(POST_TIMEOUT_SOURCE_TIMEOUT_HANDLER, status);
        tracing::error!(
            time_elapsed = ?timed_out_at.elapsed(),
            method = %method,
            path = %path,
            result = status,
            "Post-timeout activity"
        );
    });
}

/// The handler already started its response, so a 504 can no longer be sent:
/// fail the body at the deadline (`panic(http.ErrAbortHandler)`,
/// timeout.go:255-275) and count the abort (wrap.go:44-48).
fn abort_body_at(resp: Response, deadline: tokio::time::Instant, meta: RequestMeta) -> Response {
    let (parts, body) = resp.into_parts();
    let state = (
        Box::pin(body.into_data_stream()),
        Box::pin(tokio::time::sleep_until(deadline)),
        meta,
        false,
    );
    let stream = futures::stream::unfold(state, |(mut data, mut sleep, meta, done)| async move {
        if done {
            return None;
        }
        // Ready data wins over an expired deadline: a handler that has already
        // written is not timed out until its next blocked write.
        tokio::select! {
            biased;
            item = data.next() => {
                let item = item?.map_err(std::io::Error::other);
                Some((item, (data, sleep, meta, false)))
            }
            _ = &mut sleep => {
                record_request_abort(&meta);
                let err = std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "request did not complete within the allotted timeout",
                );
                Some((Err(err), (data, sleep, meta, true)))
            }
        }
    });
    Response::from_parts(parts, Body::from_stream(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control::metric_test_util::sample;
    use axum::body::Body;
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
        crate::audit::install_audit(crate::audit::AuditConfig {
            policy: crate::audit::Policy::from_yaml(
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
                let info = crate::request_scope::current_request_info().is_some();
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
                crate::request_scope::request_info_middleware,
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
