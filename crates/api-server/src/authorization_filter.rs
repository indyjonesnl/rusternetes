//! `WithAuthorization` (#3444): authorizes each request once, from its
//! `RequestInfo`, and records the `authorization_attempts_total` /
//! `authorization_duration_seconds` metrics and the
//! `authorization.k8s.io/{decision,reason}` audit annotations for it.
//!
//! Ported from
//! `staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go`
//! (`withAuthorization` :57-99, `GetAuthorizerAttributes` :101-153) and
//! `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/responsewriters/errors.go`
//! (`Forbidden`, `ForbiddenStatusError`, `forbiddenMessage`, `InternalError`).
//!
//! In `DefaultBuildHandlerChain` (`server/config.go:1015-1043`) the filter is
//! innermost: after authentication, audit, impersonation and
//! priority-and-fairness, directly around the endpoint handler.
//!
//! The endpoint handlers still run their own authorization too; with this
//! filter in front it is redundant (and answers the same), so they use the
//! authorizer without recording metrics of their own and the filter's
//! attempt is the one that is counted.

use crate::audit::{add_audit_annotation, add_audit_annotations, Attributes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Opinion, RequestAttributes};
use rusternetes_common::authz_filter_metrics::record_authorization_metrics;
use rusternetes_common::types::{ListMeta, Status, StatusDetails};
use std::sync::Arc;
use std::time::Instant;

/// authorization.go:35-36, 39-42.
const DECISION_ANNOTATION_KEY: &str = "authorization.k8s.io/decision";
const REASON_ANNOTATION_KEY: &str = "authorization.k8s.io/reason";
const DECISION_ALLOW: &str = "allow";
const DECISION_FORBID: &str = "forbid";
const REASON_ERROR: &str = "internal error";

/// `GetAuthorizerAttributes` (authorization.go:101-153) for the request's
/// `RequestInfo`. The selectors are parsed as `AuthorizeWithSelectors` does
/// (GA and locked on since 1.34, kube_features.go:353).
pub fn get_authorizer_attributes(user: UserInfo, info: &Attributes) -> RequestAttributes {
    let mut attrs = if info.is_resource_request {
        RequestAttributes::new(user, info.verb.clone(), info.resource.clone())
    } else {
        RequestAttributes::new_non_resource(user, info.verb.clone(), info.path.clone())
    };
    attrs.api_group = info.api_group.clone();
    attrs.api_version = info.api_version.clone();
    attrs.namespace = Some(info.namespace.clone()).filter(|s| !s.is_empty());
    attrs.name = Some(info.name.clone()).filter(|s| !s.is_empty());
    attrs.subresource = Some(info.subresource.clone()).filter(|s| !s.is_empty());
    attrs.set_selectors(&info.field_selector, &info.label_selector);
    attrs
}

/// `sanitizer` (errors.go:40): avoid emitting errors that look like valid HTML.
fn sanitize(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// `forbiddenMessage` (errors.go).
pub fn forbidden_message(attrs: &RequestAttributes) -> String {
    let username = &attrs.user.username;
    if attrs.is_non_resource_request {
        return format!(
            "User {username:?} cannot {} path {:?}",
            attrs.verb,
            attrs.path.as_deref().unwrap_or("")
        );
    }
    let mut resource = attrs.resource.clone();
    if let Some(sub) = attrs.subresource.as_deref().filter(|s| !s.is_empty()) {
        resource = format!("{resource}/{sub}");
    }
    match attrs.namespace.as_deref().filter(|s| !s.is_empty()) {
        Some(ns) => format!(
            "User {username:?} cannot {} resource {resource:?} in API group {:?} in the namespace {ns:?}",
            attrs.verb, attrs.api_group
        ),
        None => format!(
            "User {username:?} cannot {} resource {resource:?} in API group {:?} at the cluster scope",
            attrs.verb, attrs.api_group
        ),
    }
}

/// `Forbidden` / `ForbiddenStatusError` (errors.go): `apierrors.NewForbidden`
/// for the attributes' resource, with `forbiddenMessage` (and the reason,
/// when there is one) as the cause.
pub fn forbidden(attrs: &RequestAttributes, reason: &str) -> Response {
    let msg = sanitize(&forbidden_message(attrs));
    let err = if reason.is_empty() {
        msg
    } else {
        format!("{msg}: {reason}")
    };
    let group = Some(attrs.api_group.clone()).filter(|s| !s.is_empty());
    let resource = Some(attrs.resource.clone()).filter(|s| !s.is_empty());
    let name = attrs.name.clone().filter(|s| !s.is_empty());
    // apierrors.NewForbidden (errors.go:209-229).
    let qualified = if resource.is_none() && group.is_none() {
        None
    } else if let Some(g) = &group {
        Some(format!("{}.{g}", attrs.resource))
    } else {
        Some(attrs.resource.clone())
    };
    let message = match (&qualified, &name) {
        (None, _) => format!("forbidden: {err}"),
        (Some(q), None) => format!("{q} is forbidden: {err}"),
        (Some(q), Some(n)) => format!("{q} {n:?} is forbidden: {err}"),
    };
    let mut status = Status::failure_with_details(
        message,
        "Forbidden",
        StatusCode::FORBIDDEN.as_u16(),
        StatusDetails {
            name,
            group,
            kind: resource,
            uid: None,
            causes: None,
            retry_after_seconds: None,
        },
    );
    // metav1.Status always carries an (empty) ListMeta.
    status.metadata = Some(ListMeta {
        resource_version: None,
        continue_token: None,
        remaining_item_count: None,
    });
    let audit_obj = serde_json::to_value(&status).ok();
    let mut response = (StatusCode::FORBIDDEN, axum::Json(status)).into_response();
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    if let Some(v) = audit_obj {
        response
            .extensions_mut()
            .insert(rusternetes_common::audit::AuditResponseObject(v));
    }
    response
}

/// `InternalError` (errors.go:83-87).
fn internal_error(request_uri: &str, err: &str) -> Response {
    tracing::error!("{err}");
    let body = sanitize(&format!("Internal Server Error: {request_uri:?}: {err}"));
    let mut response = (StatusCode::INTERNAL_SERVER_ERROR, format!("{body}\n")).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// `WithAuthorization` (authorization.go:51-99): passes all authorized
/// requests on to the handler, and returns a forbidden error otherwise.
pub async fn with_authorization(
    State(authorizer): State<Arc<dyn Authorizer>>,
    req: Request,
    next: Next,
) -> Response {
    let request_uri = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());
    let authorization_start = Instant::now();

    // `request.UserFrom(ctx)`; `GetAuthorizerAttributes` fails when there is
    // no RequestInfo.
    let user = req
        .extensions()
        .get::<rusternetes_middleware::AuthContext>()
        .map(|c| c.user.clone())
        .unwrap_or_else(UserInfo::anonymous);
    let Some(info) = crate::request_scope::current_request_info() else {
        return internal_error(&request_uri, "no RequestInfo found in the context");
    };
    let attributes = get_authorizer_attributes(user, &info);

    let result = authorizer.authorize_opinion(&attributes).await;
    // authorization.go:64-70: the metrics, whichever way the request goes.
    record_authorization_metrics(&result, authorization_start.elapsed());

    let (decision, reason, err) = match result {
        Ok(Opinion::Allow(reason)) => (Some(true), reason, None),
        Ok(Opinion::Deny(reason)) => (Some(false), reason, None),
        Ok(Opinion::NoOpinion { reason, error }) => (None, reason, error),
        Err(e) => (None, String::new(), Some(e.to_string())),
    };

    // An authorizer like RBAC could encounter evaluation errors and still
    // allow the request, so the decision is checked before the error.
    if decision == Some(true) {
        add_audit_annotations(&[
            (
                DECISION_ANNOTATION_KEY.to_string(),
                DECISION_ALLOW.to_string(),
            ),
            (REASON_ANNOTATION_KEY.to_string(), reason),
        ]);
        return next.run(req).await;
    }
    if let Some(err) = err {
        add_audit_annotation(REASON_ANNOTATION_KEY, REASON_ERROR);
        return internal_error(&request_uri, &err);
    }

    tracing::debug!(uri = %request_uri, %reason, "Forbidden");
    add_audit_annotations(&[
        (
            DECISION_ANNOTATION_KEY.to_string(),
            DECISION_FORBID.to_string(),
        ),
        (REASON_ANNOTATION_KEY.to_string(), reason.clone()),
    ]);
    forbidden(&attributes, &reason)
}
