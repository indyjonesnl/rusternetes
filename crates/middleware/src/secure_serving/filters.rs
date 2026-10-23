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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secure_serving::authn::{
        AuthRequest, AuthResult, Response as AuthResponse, ALL_AUTHENTICATED,
    };
    use crate::PeerCertificates;
    use async_trait::async_trait;
    use axum::http::HeaderMap;
    use axum::routing::get;
    use http_body_util::BodyExt;
    use rusternetes_common::authz::Opinion;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    fn user(name: &str) -> UserInfo {
        UserInfo {
            username: name.to_string(),
            uid: String::new(),
            groups: vec![],
            extra: HashMap::new(),
        }
    }

    struct Authn<F>(F);
    #[async_trait]
    impl<F> RequestAuthenticator for Authn<F>
    where
        F: Fn(&mut AuthRequest<'_>) -> AuthResult + Send + Sync,
    {
        async fn authenticate_request(&self, req: &mut AuthRequest<'_>) -> AuthResult {
            (self.0)(req)
        }
    }

    struct Authz<F>(F);
    #[async_trait]
    impl<F> Authorizer for Authz<F>
    where
        F: Fn(&RequestAttributes) -> rusternetes_common::error::Result<Decision> + Send + Sync,
    {
        async fn authorize(
            &self,
            attrs: &RequestAttributes,
        ) -> rusternetes_common::error::Result<Decision> {
            (self.0)(attrs)
        }
        async fn authorize_opinion(
            &self,
            attrs: &RequestAttributes,
        ) -> rusternetes_common::error::Result<Opinion> {
            Ok(match (self.0)(attrs)? {
                Decision::Allow => Opinion::Allow(String::new()),
                Decision::Deny(r) => Opinion::NoOpinion {
                    reason: r,
                    error: None,
                },
            })
        }
        async fn get_user_rules(
            &self,
            _u: &UserInfo,
            _ns: &str,
        ) -> rusternetes_common::error::Result<(
            Vec<rusternetes_common::resources::ResourceRule>,
            Vec<rusternetes_common::resources::NonResourceRule>,
        )> {
            Ok((vec![], vec![]))
        }
    }

    fn app(hits: Arc<AtomicUsize>) -> Router {
        Router::new().route(
            "/metrics",
            get(move |req: Request| {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    // what the handler sees after authentication
                    let h = req.headers();
                    format!(
                        "auth={} remote-user={} remote-extra={} other={}",
                        h.contains_key("authorization"),
                        h.contains_key("x-remote-user"),
                        h.contains_key("x-remote-extra-foo"),
                        h.get("x-other").map(|v| v.to_str().unwrap()).unwrap_or("-")
                    )
                }
            }),
        )
    }

    async fn call(app: Router, req: Request) -> (StatusCode, HeaderMap, String) {
        let resp = app.oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&bytes).to_string(),
        )
    }

    fn get_req(path: &str) -> Request {
        Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())
            .unwrap()
    }

    // ---- errors.go forbiddenMessage

    #[test]
    fn forbidden_message_for_non_resource_and_resource_requests() {
        let a = RequestAttributes::new_non_resource(user("bob"), "get", "/metrics");
        assert_eq!(
            forbidden_message(&a),
            "User \"bob\" cannot get path \"/metrics\""
        );
        let a = RequestAttributes::new(user("bob"), "get", "pods")
            .with_namespace("ns")
            .with_api_group("apps")
            .with_subresource("status");
        assert_eq!(
            forbidden_message(&a),
            "User \"bob\" cannot get resource \"pods/status\" in API group \"apps\" in the namespace \"ns\""
        );
        let a = RequestAttributes::new(user("bob"), "list", "nodes");
        assert_eq!(
            forbidden_message(&a),
            "User \"bob\" cannot list resource \"nodes\" in API group \"\" at the cluster scope"
        );
    }

    // NewForbidden with an empty GroupResource: `forbidden: <msg>`, details `{}`.
    #[test]
    fn forbidden_status_for_a_non_resource_request() {
        let a = RequestAttributes::new_non_resource(user("system:anonymous"), "get", "/metrics");
        let s = forbidden_status(&a, "");
        assert_eq!(
            s,
            serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "metadata": {},
                "status": "Failure",
                "message": "forbidden: User \"system:anonymous\" cannot get path \"/metrics\"",
                "reason": "Forbidden", "details": {}, "code": 403
            })
        );
        let s = forbidden_status(&a, "no rule");
        assert_eq!(
            s["message"],
            "forbidden: User \"system:anonymous\" cannot get path \"/metrics\": no rule"
        );
    }

    // NewForbidden with a resource: `pods.apps "n" is forbidden: ...`, details filled.
    #[test]
    fn forbidden_status_for_a_resource_request() {
        let a = RequestAttributes::new(user("bob"), "get", "pods")
            .with_api_group("apps")
            .with_name("n");
        let s = forbidden_status(&a, "");
        assert_eq!(
            s["message"],
            "pods.apps \"n\" is forbidden: User \"bob\" cannot get resource \"pods\" in API group \"apps\" at the cluster scope"
        );
        assert_eq!(
            s["details"],
            serde_json::json!({"name": "n", "group": "apps", "kind": "pods"})
        );
    }

    // errors.go:46: the message is HTML-sanitised.
    #[test]
    fn forbidden_status_is_sanitised() {
        let a = RequestAttributes::new_non_resource(user("<b>&"), "get", "/x");
        let s = forbidden_status(&a, "");
        assert!(s["message"].as_str().unwrap().contains("&lt;b&gt;&amp;"));
    }

    #[test]
    fn unauthorized_status_shape() {
        assert_eq!(
            unauthorized_status(),
            serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "metadata": {},
                "status": "Failure", "message": "Unauthorized",
                "reason": "Unauthorized", "code": 401
            })
        );
    }

    // authorization.go:101-153 with an empty RequestInfoFactory.
    #[test]
    fn authorizer_attributes_are_always_non_resource() {
        let u = user("bob");
        let a = authorizer_attributes(Some(&u), &Method::GET, "/api/v1/pods");
        assert!(a.is_non_resource_request);
        assert_eq!(a.path.as_deref(), Some("/api/v1/pods"));
        assert_eq!(a.verb, "get");
        assert_eq!(a.user.username, "bob");
        let a = authorizer_attributes(None, &Method::POST, "/x");
        assert_eq!(a.verb, "post");
        assert_eq!(a.user.username, "");
    }

    // ---- authentication_test.go

    // TestAuthenticateRequestFailed / TestAuthenticateRequestError
    #[tokio::test]
    async fn failed_or_errored_authentication_is_a_401_and_never_reaches_the_handler() {
        for authn in [
            Arc::new(Authn(|_r: &mut AuthRequest<'_>| Ok(None))) as Arc<dyn RequestAuthenticator>,
            Arc::new(Authn(|_r: &mut AuthRequest<'_>| Err("failure".to_string()))),
        ] {
            let hits = Arc::new(AtomicUsize::new(0));
            let router = build_handler_chain(app(hits.clone()), None, Some(authn));
            let (status, headers, body) = call(router, get_req("/metrics")).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(headers["content-type"], "application/json");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v, unauthorized_status());
            assert_eq!(hits.load(Ordering::SeqCst), 0);
        }
    }

    // TestAuthenticateRequestClearHeaders + authentication.go:90-97: the
    // Authorization header and the standard front-proxy headers do not reach
    // the handler after a successful authentication; unrelated headers do.
    #[tokio::test]
    async fn successful_authentication_clears_credentials() {
        let hits = Arc::new(AtomicUsize::new(0));
        let authn: Arc<dyn RequestAuthenticator> = Arc::new(Authn(|_r: &mut AuthRequest<'_>| {
            Ok(Some(AuthResponse { user: user("bob") }))
        }));
        let router = build_handler_chain(app(hits.clone()), None, Some(authn));
        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", "Bearer secret")
            .header("x-remote-user", "evil")
            .header("x-remote-extra-foo", "evil")
            .header("x-other", "keep")
            .body(Body::empty())
            .unwrap();
        let (status, _h, body) = call(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            "auth=false remote-user=false remote-extra=false other=keep"
        );
    }

    // authentication.go:46-49: a nil authenticator disables the filter.
    #[tokio::test]
    async fn no_authenticator_serves_directly() {
        let hits = Arc::new(AtomicUsize::new(0));
        let router = build_handler_chain(app(hits.clone()), None, None);
        let (status, _h, _b) = call(router, get_req("/metrics")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    // The TLS peer chain reaches the authenticator.
    #[tokio::test]
    async fn peer_certificates_extension_reaches_the_authenticator() {
        let authn: Arc<dyn RequestAuthenticator> = Arc::new(Authn(|r: &mut AuthRequest<'_>| {
            assert_eq!(r.path, "/metrics");
            assert_eq!(r.peer_certs, &[vec![1u8, 2, 3]]);
            Ok(Some(AuthResponse {
                user: user("cert-user"),
            }))
        }));
        let router = build_handler_chain(app(Arc::new(AtomicUsize::new(0))), None, Some(authn));
        let mut req = get_req("/metrics");
        req.extensions_mut()
            .insert(PeerCertificates(Arc::new(vec![vec![1u8, 2, 3]])));
        let (status, _h, _b) = call(router, req).await;
        assert_eq!(status, StatusCode::OK);
    }

    // ---- authorization.go

    fn allow_authn() -> Arc<dyn RequestAuthenticator> {
        Arc::new(Authn(|_r: &mut AuthRequest<'_>| {
            let mut u = user("system:anonymous");
            u.groups = vec![ALL_UNAUTHENTICATED.to_string()];
            Ok(Some(AuthResponse { user: u }))
        }))
    }

    #[tokio::test]
    async fn authorization_allow_serves_the_handler() {
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(std::sync::Mutex::new(None));
        let seen2 = seen.clone();
        let authz: Arc<dyn Authorizer> = Arc::new(Authz(move |a: &RequestAttributes| {
            *seen2.lock().unwrap() =
                Some((a.user.username.clone(), a.verb.clone(), a.path.clone()));
            Ok(Decision::Allow)
        }));
        let router = build_handler_chain(app(hits.clone()), Some(authz), Some(allow_authn()));
        let (status, _h, _b) = call(router, get_req("/metrics")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            seen.lock().unwrap().clone().unwrap(),
            (
                "system:anonymous".to_string(),
                "get".to_string(),
                Some("/metrics".to_string())
            )
        );
    }

    // authorization.go:90-99: a denial is a 403 Status with nosniff.
    #[tokio::test]
    async fn authorization_denial_is_a_403_status() {
        let hits = Arc::new(AtomicUsize::new(0));
        let authz: Arc<dyn Authorizer> = Arc::new(Authz(|_a: &RequestAttributes| {
            Ok(Decision::Deny("no rule".into()))
        }));
        let router = build_handler_chain(app(hits.clone()), Some(authz), Some(allow_authn()));
        let (status, headers, body) = call(router, get_req("/metrics")).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(headers["x-content-type-options"], "nosniff");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            v["message"],
            "forbidden: User \"system:anonymous\" cannot get path \"/metrics\": no rule"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    // authorization.go:84-89: an error is a 500 `Internal Server Error: "<uri>": <err>`.
    #[tokio::test]
    async fn authorization_error_is_an_internal_error() {
        let authz: Arc<dyn Authorizer> = Arc::new(Authz(|_a: &RequestAttributes| {
            Err(rusternetes_common::error::Error::Authorization(
                "sar failed".into(),
            ))
        }));
        let router = build_handler_chain(
            app(Arc::new(AtomicUsize::new(0))),
            Some(authz),
            Some(allow_authn()),
        );
        let (status, _h, body) = call(router, get_req("/metrics?x=1")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.starts_with("Internal Server Error: \"/metrics?x=1\": "),
            "{body}"
        );
        assert!(body.contains("sar failed"), "{body}");
    }

    // authorization.go:61-64: a nil authorizer disables the filter.
    #[tokio::test]
    async fn no_authorizer_serves_authenticated_requests() {
        let router = build_handler_chain(
            app(Arc::new(AtomicUsize::new(0))),
            None,
            Some(allow_authn()),
        );
        let (status, _h, _b) = call(router, get_req("/metrics")).await;
        assert_eq!(status, StatusCode::OK);
    }

    // BuildHandlerChain order: authentication runs before authorization (a
    // failed authentication never consults the authorizer).
    #[tokio::test]
    async fn authentication_precedes_authorization() {
        let consulted = Arc::new(AtomicUsize::new(0));
        let c = consulted.clone();
        let authz: Arc<dyn Authorizer> = Arc::new(Authz(move |_a: &RequestAttributes| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(Decision::Allow)
        }));
        let authn: Arc<dyn RequestAuthenticator> =
            Arc::new(Authn(|_r: &mut AuthRequest<'_>| Ok(None)));
        let router =
            build_handler_chain(app(Arc::new(AtomicUsize::new(0))), Some(authz), Some(authn));
        let (status, _h, _b) = call(router, get_req("/metrics")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(consulted.load(Ordering::SeqCst), 0);
    }

    // ---- cachecontrol.go

    #[tokio::test]
    async fn cache_control_is_set_unless_the_handler_set_it() {
        let router = build_handler_chain(app(Arc::new(AtomicUsize::new(0))), None, None);
        let (_s, headers, _b) = call(router, get_req("/metrics")).await;
        assert_eq!(headers["cache-control"], "no-cache, private");

        let custom = Router::new().route(
            "/c",
            get(|| async { ([("cache-control", "max-age=5")], "x") }),
        );
        let router = build_handler_chain(custom, None, None);
        let (_s, headers, _b) = call(router, get_req("/c")).await;
        assert_eq!(headers["cache-control"], "max-age=5");
    }

    // ---- wrap.go WithPanicRecovery

    #[tokio::test]
    async fn a_panicking_handler_is_a_500() {
        let boom = Router::new().route(
            "/p",
            get(|| async {
                if true {
                    panic!("boom");
                }
                "x"
            }),
        );
        let router = build_handler_chain(boom, None, None);
        let (status, _h, body) = call(router, get_req("/p")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            body,
            "This request caused apiserver to panic. Look in the logs for details.\n"
        );
    }

    #[test]
    fn anonymous_user_detection() {
        assert!(is_anonymous_user(&user(ANONYMOUS_USER)));
        let mut u = user("x");
        u.groups = vec![ALL_UNAUTHENTICATED.into()];
        assert!(is_anonymous_user(&u));
        u.groups = vec![ALL_AUTHENTICATED.into()];
        assert!(!is_anonymous_user(&u));
    }
}
