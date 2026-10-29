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

/// `request.RequestInfo`. The api-server binary compiles its own copy of
/// `crate::audit`, so code compiled into both must name the type through
/// here (the lib's), which is what the task-local holds.
pub type RequestInfo = Attributes;
use rusternetes_common::authz::RequestAttributes;

tokio::task_local! {
    static REQUEST_INFO: Attributes;
}

/// Serve `fut` with `info` as the current request's `RequestInfo`.
pub async fn with_request_info<F: std::future::Future>(info: Attributes, fut: F) -> F::Output {
    REQUEST_INFO.scope(info, fut).await
}

/// `request.RequestInfoFrom(ctx)`: the current request's `RequestInfo`, if
/// [`request_info_middleware`] ran for it.
pub fn current_request_info() -> Option<Attributes> {
    REQUEST_INFO.try_with(|info| info.clone()).ok()
}

/// Axum layer: install `NewRequestInfo` of each request.
pub async fn request_info_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let info = request_info(req.method(), req.uri().path(), req.uri().query());
    with_request_info(info, next.run(req)).await
}

/// The query parameters that decide what a request is authorized as: `watch`
/// (list vs watch), `fieldSelector` (the name, and the selectors) and
/// `labelSelector` (requestinfo.go:225-275).
const AUTHZ_QUERY_KEYS: [&str; 3] = ["watch", "fieldSelector", "labelSelector"];

/// `query` with a repeated authorization parameter reduced to its first
/// occurrence, or `None` when nothing repeats. Upstream reads the first value
/// of a key everywhere (`DecodeParameters` / `req.URL.Query()[k][0]`), so
/// `RequestInfo` and the handler agree on which one counts. The handlers here
/// decode into a `HashMap`, where the last value wins; left alone, a request
/// authorized for `fieldSelector=metadata.name=a` could be served the
/// `=b` that follows it. Keys are compared decoded, as the handlers decode
/// them (`wat%63h` is `watch`); every other parameter (a repeatable one like
/// `command`, `dryRun`) is left byte for byte.
pub fn first_value_for_authz_keys(query: &str) -> Option<String> {
    let mut seen: Vec<String> = Vec::new();
    let mut kept: Vec<&str> = Vec::new();
    let mut dropped = false;
    for segment in query.split('&') {
        let key = url::form_urlencoded::parse(segment.as_bytes())
            .next()
            .map(|(k, _)| k.into_owned())
            .unwrap_or_default();
        if AUTHZ_QUERY_KEYS.contains(&key.as_str()) {
            if seen.contains(&key) {
                dropped = true;
                continue;
            }
            seen.push(key);
        }
        kept.push(segment);
    }
    dropped.then(|| kept.join("&"))
}

/// Axum layer: [`first_value_for_authz_keys`] on every request's query,
/// outside [`request_info_middleware`] so the filter and the handler read the
/// same one.
pub async fn first_value_query_middleware(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(query) = req.uri().query().and_then(first_value_for_authz_keys) {
        let mut parts = req.uri().clone().into_parts();
        let path_and_query = format!("{}?{}", req.uri().path(), query);
        if let Ok(pq) = path_and_query.parse() {
            parts.path_and_query = Some(pq);
            if let Ok(uri) = axum::http::Uri::from_parts(parts) {
                *req.uri_mut() = uri;
            }
        }
    }
    next.run(req).await
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

/// `attr` with `RequestInfo.Name` when the request's own list/watch names its
/// object through an exact `metadata.name` field selector
/// (requestinfo.go:247-253), which `GetAuthorizerAttributes` hands to the
/// authorizer as `Attributes.GetName()` (authorization.go:111-119).
///
/// Every list/watch handler builds its attributes without a name; this fills
/// it in once, where [`api_version_for`] and [`with_request_selectors`] fill
/// the version and selectors, instead of at ~100 call sites. Only for `attr`
/// about the request's own resource, verb and scope, so a check on another
/// object made inside the request is left alone. Dropped together with the
/// per-handler checks (#3515).
pub fn with_request_name(attr: &RequestAttributes) -> std::borrow::Cow<'_, RequestAttributes> {
    use std::borrow::Cow;
    if attr.name.as_deref().is_some_and(|n| !n.is_empty())
        || !matches!(attr.verb.as_str(), "list" | "watch")
    {
        return Cow::Borrowed(attr);
    }
    let name = REQUEST_INFO
        .try_with(|info| {
            (info.is_resource_request
                && !info.name.is_empty()
                && info.verb == attr.verb
                && info.resource == attr.resource
                && info.api_group == attr.api_group
                && info.subresource == attr.subresource.clone().unwrap_or_default()
                && info.namespace == attr.namespace.clone().unwrap_or_default())
            .then(|| info.name.clone())
        })
        .ok()
        .flatten();
    match name {
        Some(name) => {
            let mut a = attr.clone();
            a.name = Some(name);
            Cow::Owned(a)
        }
        None => Cow::Borrowed(attr),
    }
}

/// The verb of a list handler's own check: `watch` when `RequestInfo` derived
/// it (`?watch=true`, requestinfo.go:241-245) for a handler that serves the
/// request as a list, `list` otherwise.
pub fn list_or_watch_verb() -> &'static str {
    let watch = REQUEST_INFO
        .try_with(|info| info.is_resource_request && info.verb == "watch")
        .unwrap_or(false);
    if watch {
        "watch"
    } else {
        "list"
    }
}

/// The authorizer the handlers (and the `WithAuthorization` filter) use: the
/// configured one, with [`with_request_name`] applied to every check.
pub struct RequestInfoAuthorizer {
    inner: std::sync::Arc<dyn rusternetes_common::authz::Authorizer>,
}

impl RequestInfoAuthorizer {
    pub fn new(inner: std::sync::Arc<dyn rusternetes_common::authz::Authorizer>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl rusternetes_common::authz::Authorizer for RequestInfoAuthorizer {
    async fn authorize(
        &self,
        attrs: &RequestAttributes,
    ) -> rusternetes_common::Result<rusternetes_common::authz::Decision> {
        self.inner.authorize(&with_request_name(attrs)).await
    }
    async fn authorize_opinion(
        &self,
        attrs: &RequestAttributes,
    ) -> rusternetes_common::Result<rusternetes_common::authz::Opinion> {
        self.inner
            .authorize_opinion(&with_request_name(attrs))
            .await
    }
    async fn authorize_outcome(
        &self,
        attrs: &RequestAttributes,
    ) -> rusternetes_common::Result<rusternetes_common::authz::AuthorizationOutcome> {
        self.inner
            .authorize_outcome(&with_request_name(attrs))
            .await
    }
    async fn get_user_rules(
        &self,
        user: &rusternetes_common::auth::UserInfo,
        namespace: &str,
    ) -> rusternetes_common::Result<(
        Vec<rusternetes_common::resources::ResourceRule>,
        Vec<rusternetes_common::resources::NonResourceRule>,
    )> {
        self.inner.get_user_rules(user, namespace).await
    }
}
