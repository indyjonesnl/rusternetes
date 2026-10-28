//! The kubelet's `/debug/pprof/` and `/debug/flags/v` handlers.
//!
//! Ported from `pkg/kubelet/server/server.go` in Kubernetes release-1.35:
//! `InstallProfilingHandler` (:705-735), `InstallDebugFlagsHandler` (:692-702),
//! `getHandlerForDisabledEndpoint` (:687-690), the `pprofBasePath` /
//! `debugFlagPath` constants (:108-109) and their call sites in `NewServer`
//! (:322-323, only under `EnableDebuggingHandlers`). The flags handler is
//! `routes.StringFlagPutHandler` (`apiserver/pkg/server/routes/flags.go:112-134`)
//! with `logs.GlogSetter` (`component-base/logs/logs.go:189-208`).
//!
//! Upstream tests ported: `TestDisablingLogAndProfilingHandler`
//! (`server_test.go:1835-1847`).
//!
//! Deviations (Go-runtime features with no Rust counterpart, flagged here):
//! * `net/http/pprof` serves CPU/heap/goroutine/... profiles. This module
//!   serves the parts that are process-level rather than runtime-level
//!   (`cmdline`, `symbol`, the index); `profile` and `trace` answer 501.
//! * `EnableContentionProfiling` calls `runtime.SetBlockProfileRate(1)`;
//!   there is no block profile in Rust, so it is logged and otherwise inert.
//! * klog verbosity maps onto the `tracing` filter, see
//!   `rusternetes_common::tracing::verbosity_directive`.

use axum::body::Bytes;
use axum::http::{header, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use std::sync::Arc;

use crate::server::disabled_endpoint;

/// `pprofBasePath` (server.go:108).
pub const PPROF_BASE_PATH: &str = "/debug/pprof/";
/// `debugFlagPath` (server.go:109).
pub const DEBUG_FLAG_PATH: &str = "/debug/flags/v";

/// `StringFlagSetterFunc` (`routes/flags.go`): `Ok(response)` or `Err(message)`.
pub type StringFlagSetter = Arc<dyn Fn(&str) -> Result<String, String> + Send + Sync>;

/// `writePlainText` (`routes/flags.go:137-142`).
fn write_plain_text(status: StatusCode, text: &str) -> Response {
    let mut resp = (status, format!("{text}\n")).into_response();
    let h = resp.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
}

/// `logs.GlogSetter` (`component-base/logs/logs.go:189-208`): parse the body
/// with `strconv.ParseUint(val, 10, 32)`, apply it, report it.
pub fn glog_setter(val: &str) -> Result<String, String> {
    let v = parse_uint32(val)?;
    rusternetes_common::tracing::set_verbosity(v).map_err(|e| e.to_string())?;
    Ok(format!("successfully set klog.logging.verbosity to {val}"))
}

/// `strconv.ParseUint(val, 10, 32)` including its error strings.
fn parse_uint32(val: &str) -> Result<u32, String> {
    let syntax = || format!("strconv.ParseUint: parsing {val:?}: invalid syntax");
    if val.is_empty() || !val.bytes().all(|b| b.is_ascii_digit()) {
        return Err(syntax());
    }
    val.parse::<u32>()
        .map_err(|_| format!("strconv.ParseUint: parsing {val:?}: value out of range"))
}

/// `routes.StringFlagPutHandler` (`routes/flags.go:112-134`).
async fn string_flag_put(setter: StringFlagSetter, method: Method, body: Bytes) -> Response {
    if method != Method::PUT {
        return write_plain_text(StatusCode::NOT_ACCEPTABLE, "unsupported http method");
    }
    match setter(&String::from_utf8_lossy(&body)) {
        Ok(response) => write_plain_text(StatusCode::OK, &response),
        Err(e) => write_plain_text(StatusCode::BAD_REQUEST, &e),
    }
}

/// `(*Server).InstallDebugFlagsHandler` (server.go:692-702).
pub fn install_debug_flags_handler(
    router: Router,
    enable_debug_flags_handler: bool,
    setter: StringFlagSetter,
) -> Router {
    if !enable_debug_flags_handler {
        return router.route(
            DEBUG_FLAG_PATH,
            any(|| async { disabled_endpoint("flags endpoint is disabled.") }),
        );
    }
    router.route(
        DEBUG_FLAG_PATH,
        any(move |method: Method, body: Bytes| string_flag_put(setter.clone(), method, body)),
    )
}

/// The pprof endpoints that exist without a Go runtime.
const PPROF_INDEX: &str = "<html>\n<head>\n<title>/debug/pprof/</title>\n</head>\n<body>\n\
/debug/pprof/<br>\n<br>\nTypes of profiles available:\n<table>\n\
<tr><td><a href=\"cmdline\">cmdline</a></td></tr>\n\
<tr><td><a href=\"symbol\">symbol</a></td></tr>\n\
</table>\n</body>\n</html>\n";

fn nosniff(mut resp: Response) -> Response {
    resp.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
}

/// `handlePprofEndpoint` (server.go:712-726): dispatch on the path under
/// `pprofBasePath`; anything else falls through to `pprof.Index`.
async fn handle_pprof_endpoint(method: Method, uri: Uri) -> Response {
    // restful registers `GET /{subpath:*}` only.
    if method != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let name = uri.path().strip_prefix(PPROF_BASE_PATH).unwrap_or("");
    match name {
        // pprof.Cmdline: NUL-joined os.Args.
        "cmdline" => nosniff(
            (
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                std::env::args().collect::<Vec<_>>().join("\0"),
            )
                .into_response(),
        ),
        // pprof.Symbol: `num_symbols: 0` -- no symbol table is exposed.
        "symbol" => nosniff(
            (
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "num_symbols: 0\n",
            )
                .into_response(),
        ),
        "profile" | "trace" => (
            StatusCode::NOT_IMPLEMENTED,
            format!("{name} profiling is not available in this build\n"),
        )
            .into_response(),
        // pprof.Index: the listing, or `Unknown profile` for any other name.
        "" => nosniff(
            (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                PPROF_INDEX,
            )
                .into_response(),
        ),
        _ => nosniff((StatusCode::NOT_FOUND, "Unknown profile\n").into_response()),
    }
}

/// `(*Server).InstallProfilingHandler` (server.go:705-735). The
/// `addMetricsBucketMatcher("debug")` half is
/// `MetricsBuckets::install_profiling_handler`.
pub fn install_profiling_handler(
    router: Router,
    enable_profiling_handler: bool,
    enable_contention_profiling: bool,
) -> Router {
    let subtree = format!("{PPROF_BASE_PATH}*subpath");
    if !enable_profiling_handler {
        let handler = any(|| async { disabled_endpoint("profiling endpoint is disabled.") });
        return router
            .route(PPROF_BASE_PATH, handler.clone())
            .route(&subtree, handler);
    }
    if enable_contention_profiling {
        // `goruntime.SetBlockProfileRate(1)` (server.go:733).
        tracing::warn!(
            "enableContentionProfiling is set but block/mutex profiling is not available in this build"
        );
    }
    router
        .route(PPROF_BASE_PATH, any(handle_pprof_endpoint))
        .route(&subtree, any(handle_pprof_endpoint))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    async fn call(app: &Router, method: &str, path: &str, body: &str) -> (StatusCode, String) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn echo_setter() -> StringFlagSetter {
        Arc::new(|v| {
            parse_uint32(v)?;
            Ok(format!("successfully set klog.logging.verbosity to {v}"))
        })
    }

    /// TestDisablingLogAndProfilingHandler (server_test.go:1835-1847).
    #[tokio::test]
    async fn disabled_profiling_and_flags_answer_405() {
        let app = install_debug_flags_handler(
            install_profiling_handler(Router::new(), false, false),
            false,
            echo_setter(),
        );
        for (m, p, want) in [
            (
                "GET",
                "/debug/pprof/profile?seconds=2",
                "profiling endpoint is disabled.\n",
            ),
            ("GET", "/debug/pprof/", "profiling endpoint is disabled.\n"),
            (
                "POST",
                "/debug/pprof/heap",
                "profiling endpoint is disabled.\n",
            ),
            ("GET", "/debug/flags/v", "flags endpoint is disabled.\n"),
            ("PUT", "/debug/flags/v", "flags endpoint is disabled.\n"),
        ] {
            let (status, body) = call(&app, m, p, "").await;
            assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{m} {p}");
            assert_eq!(body, want, "{m} {p}");
        }
    }

    #[tokio::test]
    async fn enabled_profiling_serves_pprof() {
        let app = install_profiling_handler(Router::new(), true, false);
        let (s, b) = call(&app, "GET", "/debug/pprof/", "").await;
        assert_eq!(s, StatusCode::OK);
        assert!(b.contains("Types of profiles available"));
        let (s, b) = call(&app, "GET", "/debug/pprof/cmdline", "").await;
        assert_eq!(s, StatusCode::OK);
        assert!(!b.is_empty());
        let (s, b) = call(&app, "GET", "/debug/pprof/symbol", "").await;
        assert_eq!((s, b.as_str()), (StatusCode::OK, "num_symbols: 0\n"));
        let (s, b) = call(&app, "GET", "/debug/pprof/nope", "").await;
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::NOT_FOUND, "Unknown profile\n")
        );
        let (s, _) = call(&app, "GET", "/debug/pprof/profile", "").await;
        assert_eq!(s, StatusCode::NOT_IMPLEMENTED);
        let (s, _) = call(&app, "POST", "/debug/pprof/cmdline", "").await;
        assert_eq!(s, StatusCode::METHOD_NOT_ALLOWED);
    }

    /// `StringFlagPutHandler`: PUT sets, other methods 406, bad value 400.
    #[tokio::test]
    async fn enabled_flags_handler_puts_verbosity() {
        let app = install_debug_flags_handler(Router::new(), true, echo_setter());
        let (s, b) = call(&app, "PUT", "/debug/flags/v", "4").await;
        assert_eq!(
            (s, b.as_str()),
            (
                StatusCode::OK,
                "successfully set klog.logging.verbosity to 4\n"
            )
        );
        let (s, b) = call(&app, "GET", "/debug/flags/v", "").await;
        assert_eq!(
            (s, b.as_str()),
            (StatusCode::NOT_ACCEPTABLE, "unsupported http method\n")
        );
        let (s, b) = call(&app, "PUT", "/debug/flags/v", "abc").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert_eq!(b, "strconv.ParseUint: parsing \"abc\": invalid syntax\n");
        let (s, b) = call(&app, "PUT", "/debug/flags/v", "99999999999").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(b.contains("value out of range"));
    }

    #[test]
    fn glog_setter_rejects_non_numeric() {
        assert!(glog_setter("-1").is_err());
        assert!(glog_setter("").is_err());
    }
}
