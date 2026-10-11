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
//! STUB: tests first (see the follow-up commit for the implementation).

use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// `parseTimeout` (request_deadline.go:155-167): the user's `?timeout=`.
pub fn parse_timeout(_query: Option<&str>) -> Result<Option<Duration>, String> {
    Ok(None)
}

/// The deadline `withRequestDeadline` sets (request_deadline.go:81-89).
pub fn request_deadline(_query: Option<&str>, max: Duration) -> Result<Duration, String> {
    Ok(max)
}

/// `WithRequestDeadline` + `WithTimeoutForNonLongRunningRequests`.
pub async fn request_timeout_middleware(req: Request, next: Next) -> Response {
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control::metric_test_util::sample;
    use axum::body::Body;
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
                        Some((Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"a")), 1))
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
}
