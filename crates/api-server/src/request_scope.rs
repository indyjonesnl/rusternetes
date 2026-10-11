//! The `RequestInfo` of the request being served, reachable from the
//! authorizers (`request.WithRequestInfo` / `RequestInfoFrom`,
//! `pkg/endpoints/request/requestinfo.go`).
//!
//! Upstream builds `authorizer.Attributes` once, from the request's
//! `RequestInfo` (`GetAuthorizerAttributes`,
//! `pkg/endpoints/filters/authorization.go:101-153`: `attribs.APIVersion =
//! requestInfo.APIVersion`). Here each handler builds its own
//! `RequestAttributes`, so the version is filled in where the attributes are
//! consumed instead of at ~200 call sites.

use crate::audit::{request_info, Attributes};
use rusternetes_common::authz::RequestAttributes;

tokio::task_local! {
    static REQUEST_INFO: Attributes;
}

/// Serve `fut` with `info` as the current request's `RequestInfo`.
pub async fn with_request_info<F: std::future::Future>(info: Attributes, fut: F) -> F::Output {
    REQUEST_INFO.scope(info, fut).await
}

/// Axum layer: install `NewRequestInfo` of each request.
pub async fn request_info_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let info = request_info(req.method(), req.uri().path(), req.uri().query());
    with_request_info(info, next.run(req)).await
}

/// `Attributes.GetAPIVersion` for `attr`.
pub fn api_version_for(attr: &RequestAttributes) -> String {
    attr.api_version.clone()
}
