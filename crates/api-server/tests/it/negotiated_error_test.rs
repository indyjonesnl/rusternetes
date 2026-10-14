//! The `WithAuthorization` forbidden body is negotiated (#3517).
//!
//! Ported from `endpoints/handlers/negotiation/negotiate_test.go`
//! (`TestNegotiate`) and `responsewriters/errors_test.go` (`TestForbidden`):
//! `Forbidden` -> `RespondWithError` -> `ErrorNegotiated` ->
//! `WriteObjectNegotiated` (responsewriters/writers.go:381-397, :323-361).

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::any;
use axum::Router;
use rusternetes_api_server::authorization_filter::with_authorization;
use rusternetes_api_server::request_scope::request_info_middleware;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use rusternetes_middleware::AuthContext;
use std::sync::Arc;
use tower::ServiceExt;

struct Deny;

#[async_trait]
impl Authorizer for Deny {
    async fn authorize(&self, _: &RequestAttributes) -> Result<Decision> {
        Ok(Decision::Deny("nope".into()))
    }
    async fn authorize_opinion(&self, _: &RequestAttributes) -> Result<Opinion> {
        Ok(Opinion::Deny("nope".into()))
    }
    async fn get_user_rules(
        &self,
        _: &UserInfo,
        _: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        Ok((vec![], vec![]))
    }
}

const MESSAGE: &str = "pods is forbidden: User \"alice\" cannot list resource \"pods\" in API group \"\" in the namespace \"default\": nope";

async fn forbidden_for(
    uri: &str,
    headers: &[(&str, &str)],
) -> (axum::http::response::Parts, Vec<u8>) {
    let authorizer: Arc<dyn Authorizer> = Arc::new(Deny);
    let app = Router::new()
        .fallback(any(|| async { "ok" }))
        .layer(axum::middleware::from_fn_with_state(
            authorizer,
            with_authorization,
        ))
        .layer(axum::middleware::from_fn(request_info_middleware));
    let mut b = Request::builder().uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    let mut req = b.body(Body::empty()).unwrap();
    req.extensions_mut().insert(AuthContext {
        user: UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        },
    });
    let resp = app.oneshot(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    (parts, bytes.to_vec())
}

const URI: &str = "/api/v1/namespaces/default/pods";

#[tokio::test]
async fn default_is_compact_json_with_trailing_newline() {
    let (p, b) = forbidden_for(URI, &[]).await;
    assert_eq!(p.status, StatusCode::FORBIDDEN);
    assert_eq!(p.headers["content-type"], "application/json");
    assert_eq!(p.headers["x-content-type-options"], "nosniff");
    let s = String::from_utf8(b).unwrap();
    assert!(
        s.starts_with(
            r#"{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"#
        ),
        "{s}"
    );
    assert!(s.ends_with("\"code\":403}\n"), "{s}");
}

#[tokio::test]
async fn json_escapes_html_like_go() {
    // encoding/json escapes <, > and & even in the sanitized message.
    let (_, b) = forbidden_for("/<script>", &[]).await;
    let s = String::from_utf8(b).unwrap();
    assert!(s.contains(r#"&lt;script&gt;"#), "{s}");
}

#[tokio::test]
async fn accept_yaml_is_yaml() {
    let (p, b) = forbidden_for(URI, &[("accept", "application/yaml")]).await;
    assert_eq!(p.status, StatusCode::FORBIDDEN);
    assert_eq!(p.headers["content-type"], "application/yaml");
    let v: serde_yaml::Value = serde_yaml::from_slice(&b).unwrap();
    assert_eq!(v["message"].as_str().unwrap(), MESSAGE);
    assert_eq!(v["code"].as_u64(), Some(403));
}

#[tokio::test]
async fn accept_protobuf_is_the_k8s_envelope() {
    let (p, b) = forbidden_for(URI, &[("accept", "application/vnd.kubernetes.protobuf")]).await;
    assert_eq!(p.status, StatusCode::FORBIDDEN);
    assert_eq!(
        p.headers["content-type"],
        "application/vnd.kubernetes.protobuf"
    );
    assert_eq!(&b[..4], b"k8s\0");
}

#[tokio::test]
async fn pretty_param_and_user_agent_indent() {
    for (uri, hdrs) in [
        (format!("{URI}?pretty=1"), vec![]),
        (
            URI.to_string(),
            vec![("accept", "application/json; pretty=1")],
        ),
        (URI.to_string(), vec![("user-agent", "curl/8.0")]),
    ] {
        let (p, b) = forbidden_for(&uri, &hdrs).await;
        assert_eq!(p.headers["content-type"], "application/json");
        let s = String::from_utf8(b).unwrap();
        assert!(s.starts_with("{\n  \"kind\": \"Status\",\n"), "{s}");
        assert!(s.ends_with("\n}"), "{s:?}");
    }
}

#[tokio::test]
async fn wildcards_and_unrecognized_clauses_fall_back_to_json() {
    for accept in [
        "*/*",
        "application/*",
        "unrecognized/stuff, application/json",
        "application/BOGUS, application/json",
    ] {
        let (p, _) = forbidden_for(URI, &[("accept", accept)]).await;
        assert_eq!(p.headers["content-type"], "application/json", "{accept}");
    }
}

/// `as`/`g`/`v` (a transform) are rejected by `DefaultEndpointRestrictions`
/// (negotiate.go:143-147), so the next clause wins.
#[tokio::test]
async fn restricted_params_skip_the_clause() {
    let (p, _) = forbidden_for(
        URI,
        &[(
            "accept",
            "application/yaml;as=Table;v=v1;g=meta.k8s.io, application/json",
        )],
    )
    .await;
    assert_eq!(p.headers["content-type"], "application/json");
}

/// A failed negotiation must not hide the original error: raw indented JSON
/// (writers.go:333-339 `WriteRawJSON`).
#[tokio::test]
async fn unnegotiable_accept_still_returns_the_403_as_raw_json() {
    let (p, b) = forbidden_for(URI, &[("accept", "text/html")]).await;
    assert_eq!(p.status, StatusCode::FORBIDDEN);
    assert_eq!(p.headers["content-type"], "application/json");
    let s = String::from_utf8(b).unwrap();
    assert!(s.starts_with("{\n  \"kind\": \"Status\","), "{s}");
    assert!(!s.ends_with('\n'), "{s:?}");
}

/// goautoneg orders by q: yaml (q=1) beats json (q=0.5).
#[tokio::test]
async fn q_values_order_the_clauses() {
    let (p, _) = forbidden_for(
        URI,
        &[("accept", "application/json;q=0.5, application/yaml")],
    )
    .await;
    assert_eq!(p.headers["content-type"], "application/yaml");
}
