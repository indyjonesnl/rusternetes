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
