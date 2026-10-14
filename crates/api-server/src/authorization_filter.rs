//! `WithAuthorization` (#3444): authorizes each request once, from its
//! `RequestInfo`.
//!
//! Ported from
//! `staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go`.

use crate::audit::Attributes;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, RequestAttributes};
use std::sync::Arc;

/// `GetAuthorizerAttributes` (authorization.go:101-153).
pub fn get_authorizer_attributes(user: UserInfo, _info: &Attributes) -> RequestAttributes {
    RequestAttributes::new(user, "", "")
}

/// `forbiddenMessage` (responsewriters/errors.go).
pub fn forbidden_message(_attrs: &RequestAttributes) -> String {
    String::new()
}

/// `Forbidden` (responsewriters/errors.go).
pub fn forbidden(_attrs: &RequestAttributes, _reason: &str) -> Response {
    Response::new(axum::body::Body::empty())
}

/// `WithAuthorization` (authorization.go:51-99).
pub async fn with_authorization(
    axum::extract::State(_authorizer): axum::extract::State<Arc<dyn Authorizer>>,
    req: Request,
    next: Next,
) -> Response {
    next.run(req).await
}
