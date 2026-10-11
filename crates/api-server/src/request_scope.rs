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

/// `Attributes.GetAPIVersion` for `attr`: the version the caller set, else the
/// current request's `RequestInfo.APIVersion` when `attr` is about that
/// request's own resource. A check on another resource (an escalation or
/// signer check inside the request) must not inherit the request's version.
pub fn api_version_for(attr: &RequestAttributes) -> String {
    if !attr.api_version.is_empty() {
        return attr.api_version.clone();
    }
    REQUEST_INFO
        .try_with(|info| {
            (info.is_resource_request
                && info.resource == attr.resource
                && info.api_group == attr.api_group)
                .then(|| info.api_version.clone())
        })
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// `attr` with the current request's selectors, parsed as
/// `GetAuthorizerAttributes` does (filters/authorization.go:126-150,
/// `AuthorizeWithSelectors`, GA and locked on since 1.34). As with
/// [`api_version_for`], only when `attr` is about the request's own
/// resource and verb, and the caller has not set selectors itself.
pub fn with_request_selectors(attr: &RequestAttributes) -> std::borrow::Cow<'_, RequestAttributes> {
    use std::borrow::Cow;
    if !attr.field_selector_requirements.is_empty()
        || attr.field_selector_parsing_err.is_some()
        || !attr.label_selector_requirements.is_empty()
        || attr.label_selector_parsing_err.is_some()
    {
        return Cow::Borrowed(attr);
    }
    let selectors = REQUEST_INFO
        .try_with(|info| {
            (info.is_resource_request
                && info.resource == attr.resource
                && info.api_group == attr.api_group
                && info.verb == attr.verb
                && (!info.field_selector.is_empty() || !info.label_selector.is_empty()))
            .then(|| (info.field_selector.clone(), info.label_selector.clone()))
        })
        .ok()
        .flatten();
    match selectors {
        Some((field, label)) => {
            let mut a = attr.clone();
            a.set_selectors(&field, &label);
            Cow::Owned(a)
        }
        None => Cow::Borrowed(attr),
    }
}
