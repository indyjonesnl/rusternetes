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
//! DEVIATIONS: (1) Go runs the handler in a goroutine and lets it finish after
//! the 504 is written; here the handler future is dropped at the deadline
//! (a cancelled future cannot write to the dead response anyway), so
//! `RecordRequestPostTimeout` / `PostTimeoutHandlerOK|Panic` have no
//! analogue. (2) Go distinguishes "handler has written the header" via
//! `baseTimeoutWriter.wroteHeader`; here the handler has "started the
//! response" once it returns a `Response`, after which only the body can still
//! run past the deadline. That is the abort case: the body stream is failed
//! (hyper drops the connection, the analogue of `panic(http.ErrAbortHandler)`)
//! and `RecordRequestAbort` is called, as `WithPanicRecovery` does
//! (wrap.go:44-48). (3) A negative `?timeout=` parses to a zero duration,
//! which selects the default exactly as a negative one does upstream
//! (`userSpecifiedTimeout > 0`).

use std::sync::OnceLock;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rusternetes_common::types::{Status, StatusDetails};

use crate::apiserver_request_metrics::{
    record_request_abort, record_request_termination, request_meta, RequestMeta,
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
    match tokio::time::timeout_at(deadline, next.run(req)).await {
        Err(_) => {
            // `postTimeoutFn` (timeout.go:56-58).
            record_request_termination(&meta, "apiserver", 504);
            timeout_error()
        }
        Ok(resp) => abort_body_at(resp, deadline, meta),
    }
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
}
