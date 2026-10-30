//! The handler chain of the controller-manager / scheduler secure server.
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/controller-manager/app/serve.go:34-54`
//!   (`BuildHandlerChain`) and `cmd/kube-scheduler/app/server.go:340-352`
//!   (`buildHandlerChain`): panic recovery, HTTP logging, cache control,
//!   request info, authentication, authorization, innermost the mux.
//! * `staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go:61-141`
//!   (`WithAuthentication`, `Unauthorized`), `authorization.go:55-100`
//!   (`WithAuthorization`), `cachecontrol.go:25-35`
//! * `staging/src/k8s.io/apiserver/pkg/server/filters/wrap.go:30-75`
//!   (`WithPanicRecovery`)
//! * `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/responsewriters/errors.go:36-91`
//!   (`Forbidden`, `ForbiddenStatusError`, `forbiddenMessage`, `InternalError`)
//! * `staging/src/k8s.io/apimachinery/pkg/api/errors/errors.go:209-229` (`NewForbidden`)
//!
//! `RequestInfoFactory{}` has no API prefixes, so `NewRequestInfo`
//! (`apiserver/pkg/endpoints/request/requestinfo.go:130-146`) classifies EVERY
//! request as a non-resource request with `Path = URL.Path` and
//! `Verb = lower(Method)`; that is all [`authorizer_attributes`] needs.
//!
//! Deliberate reductions: responses are always JSON (the negotiated YAML and
//! protobuf encodings of `ErrorNegotiated` are not produced),
//! `WithHTTPLogging` (V(3) request logs) is not ported, and neither is the
//! `UnauthenticatedHTTP2DOSMitigation` `Connection: close` for HTTP/2.

use super::authn::{
    clear_authentication_headers, AuthRequest, RequestAuthenticator, ALL_UNAUTHENTICATED,
    ANONYMOUS_USER,
};
use crate::PeerCertificates;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::{from_fn, Next};
use axum::response::Response;
use axum::Router;
use futures::FutureExt;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_common::error::Error;
use std::collections::HashMap;
use std::sync::Arc;

/// The authenticated user of a request (`genericapirequest.WithUser`).
#[derive(Clone, Debug)]
pub struct AuthenticatedUser(pub UserInfo);

/// `sanitizer` (errors.go:33).
fn sanitize(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// `forbiddenMessage` (errors.go:60-80).
pub fn forbidden_message(attrs: &RequestAttributes) -> String {
    let username = &attrs.user.username;
    if attrs.is_non_resource_request {
        return format!(
            "User {:?} cannot {} path {:?}",
            username,
            attrs.verb,
            attrs.path.as_deref().unwrap_or("")
        );
    }
    let mut resource = attrs.resource.clone();
    if let Some(sub) = attrs.subresource.as_deref().filter(|s| !s.is_empty()) {
        resource = format!("{resource}/{sub}");
    }
    if let Some(ns) = attrs.namespace.as_deref().filter(|s| !s.is_empty()) {
        return format!(
            "User {:?} cannot {} resource {:?} in API group {:?} in the namespace {:?}",
            username, attrs.verb, resource, attrs.api_group, ns
        );
    }
    format!(
        "User {:?} cannot {} resource {:?} in API group {:?} at the cluster scope",
        username, attrs.verb, resource, attrs.api_group
    )
}

/// `NewForbidden(gr, name, err)` as `ForbiddenStatusError` builds it
/// (errors.go:45-58): the `metav1.Status` JSON.
pub fn forbidden_status(attrs: &RequestAttributes, reason: &str) -> serde_json::Value {
    let msg = sanitize(&forbidden_message(attrs));
    let err = if reason.is_empty() {
        msg
    } else {
        format!("{msg}: {reason}")
    };
    let group = &attrs.api_group;
    let resource = &attrs.resource;
    let name = attrs.name.as_deref().unwrap_or("");
    // NewForbidden (errors.go:209-229)
    let message = if group.is_empty() && resource.is_empty() {
        format!("forbidden: {err}")
    } else {
        let gr = if group.is_empty() {
            resource.clone()
        } else {
            format!("{resource}.{group}")
        };
        if name.is_empty() {
            format!("{gr} is forbidden: {err}")
        } else {
            format!("{gr} {name:?} is forbidden: {err}")
        }
    };
    let mut details = serde_json::Map::new();
    if !name.is_empty() {
        details.insert("name".into(), name.into());
    }
    if !group.is_empty() {
        details.insert("group".into(), group.as_str().into());
    }
    if !resource.is_empty() {
        details.insert("kind".into(), resource.as_str().into());
    }
    serde_json::json!({
        "kind": "Status", "apiVersion": "v1", "metadata": {},
        "status": "Failure",
        "message": message,
        "reason": "Forbidden",
        "details": details,
        "code": 403
    })
}

/// `apierrors.NewUnauthorized("Unauthorized")` rendered by `ErrorToAPIStatus`.
pub fn unauthorized_status() -> serde_json::Value {
    serde_json::json!({
        "kind": "Status", "apiVersion": "v1", "metadata": {},
        "status": "Failure", "message": "Unauthorized",
        "reason": "Unauthorized", "code": 401
    })
}

/// `GetAuthorizerAttributes` (authorization.go:101-153) for a request whose
/// `RequestInfo` came from an empty `RequestInfoFactory`.
pub fn authorizer_attributes(
    user: Option<&UserInfo>,
    method: &Method,
    path: &str,
) -> RequestAttributes {
    let user = user.cloned().unwrap_or(UserInfo {
        username: String::new(),
        uid: String::new(),
        groups: vec![],
        extra: HashMap::new(),
    });
    RequestAttributes::new_non_resource(user, method.as_str().to_lowercase(), path)
}

/// `isAnonymousUser` (authentication.go:131-141).
pub fn is_anonymous_user(u: &UserInfo) -> bool {
    u.username == ANONYMOUS_USER || u.groups.iter().any(|g| g == ALL_UNAUTHENTICATED)
}

fn json_response(code: u16, body: &serde_json::Value, extra: &[(&str, &str)]) -> Response {
    let mut b = Response::builder()
        .status(StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", "application/json");
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    b.body(Body::from(format!("{body}\n"))).unwrap()
}

/// `http.Error`.
fn http_error(code: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(code)
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-content-type-options", "nosniff")
        .body(Body::from(format!("{msg}\n")))
        .unwrap()
}

/// `responsewriters.InternalError` (errors.go:83-87).
pub fn internal_error_response(request_uri: &str, err: &str) -> Response {
    tracing::error!("{err}");
    http_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        &sanitize(&format!("Internal Server Error: {request_uri:?}: {err}")),
    )
}

/// `WithAuthentication` + `Unauthorized`: the middleware itself.
pub async fn authentication_middleware(
    authn: Arc<dyn RequestAuthenticator>,
    req: Request,
    next: Next,
) -> Response {
    let (mut parts, body) = req.into_parts();
    let path = parts.uri.path().to_string();
    let peer_certs: Vec<Vec<u8>> = parts
        .extensions
        .get::<PeerCertificates>()
        .map(|p| p.0.as_ref().clone())
        .unwrap_or_default();
    let result = authn
        .authenticate_request(&mut AuthRequest {
            headers: &mut parts.headers,
            path: &path,
            peer_certs: &peer_certs,
        })
        .await;
    let resp = match result {
        Ok(Some(r)) => r,
        other => {
            if let Err(e) = other {
                tracing::error!("Unable to authenticate the request: {e}");
            }
            return json_response(401, &unauthorized_status(), &[]);
        }
    };
    // authorization header is not required anymore in case of a successful authentication.
    parts.headers.remove("authorization");
    // delete standard front proxy headers
    clear_authentication_headers(
        &mut parts.headers,
        &["X-Remote-User".to_string()],
        &["X-Remote-Uid".to_string()],
        &["X-Remote-Group".to_string()],
        &["X-Remote-Extra-".to_string()],
    );
    parts.extensions.insert(AuthenticatedUser(resp.user));
    next.run(Request::from_parts(parts, body)).await
}

/// `WithAuthorization`: the middleware itself.
pub async fn authorization_middleware(
    authz: Arc<dyn Authorizer>,
    req: Request,
    next: Next,
) -> Response {
    let user = req.extensions().get::<AuthenticatedUser>().cloned();
    let attributes =
        authorizer_attributes(user.as_ref().map(|u| &u.0), req.method(), req.uri().path());
    match authz.authorize(&attributes).await {
        // an authorizer like RBAC could encounter evaluation errors and still allow the request,
        // so the decision is checked before the error.
        Ok(Decision::Allow) => next.run(req).await,
        Ok(Decision::Deny(reason)) => {
            tracing::debug!("Forbidden: URI {} reason {reason}", req.uri());
            json_response(
                403,
                &forbidden_status(&attributes, &reason),
                &[("x-content-type-options", "nosniff")],
            )
        }
        Err(e) => {
            let uri = req
                .uri()
                .path_and_query()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| req.uri().to_string());
            let msg = match e {
                Error::Authorization(m) => m,
                other => other.to_string(),
            };
            internal_error_response(&uri, &msg)
        }
    }
}

/// `WithCacheControl` (cachecontrol.go:25-35).
async fn cache_control_middleware(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    if !resp.headers().contains_key("cache-control") {
        resp.headers_mut().insert(
            "cache-control",
            axum::http::HeaderValue::from_static("no-cache, private"),
        );
    }
    resp
}

/// `WithPanicRecovery` (wrap.go:30-60).
async fn panic_recovery_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    match std::panic::AssertUnwindSafe(next.run(req))
        .catch_unwind()
        .await
    {
        Ok(r) => r,
        Err(_) => {
            tracing::error!("apiserver panic'd method={method} URI={uri}");
            http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "This request caused apiserver to panic. Look in the logs for details.",
            )
        }
    }
}

/// `BuildHandlerChain` / `buildHandlerChain`: wraps `handler` so every request
/// is authenticated, then authorized, before it is served. A `None` authenticator
/// or authorizer disables that filter (as upstream's `nil` does).
pub fn build_handler_chain(
    handler: Router,
    authz: Option<Arc<dyn Authorizer>>,
    authn: Option<Arc<dyn RequestAuthenticator>>,
) -> Router {
    let mut handler = handler;
    if let Some(authz) = authz {
        handler = handler.layer(from_fn(move |req, next| {
            authorization_middleware(authz.clone(), req, next)
        }));
    } else {
        tracing::warn!("Authorization is disabled");
    }
    if let Some(authn) = authn {
        handler = handler.layer(from_fn(move |req, next| {
            authentication_middleware(authn.clone(), req, next)
        }));
    } else {
        tracing::warn!("Authentication is disabled");
    }
    // WithRequestInfo is implicit: see the module docs.
    handler = handler.layer(from_fn(cache_control_middleware));
    // WithHTTPLogging is not ported.
    handler.layer(from_fn(panic_recovery_middleware))
}
