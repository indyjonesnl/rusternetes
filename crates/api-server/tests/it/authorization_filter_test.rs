//! `WithAuthorization` filter (#3444).
//!
//! Ported from `endpoints/filters/authorization_test.go`
//! (`TestGetAuthorizerAttributes`, `TestAuditAnnotation`) and
//! `endpoints/handlers/responsewriters/errors_test.go` (`TestForbidden`),
//! plus the metrics the filter records once per request
//! (`filters/metrics.go:106-122`).

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::routing::any;
use axum::Router;
use rusternetes_api_server::audit::{request_info, with_audit, AuditConfig, Policy};
use rusternetes_api_server::authorization_filter::{
    forbidden_message, forbidden_negotiated, get_authorizer_attributes, with_authorization,
};
use rusternetes_api_server::request_scope::request_info_middleware;
use rusternetes_common::audit::{AuditBackend, AuditEvent};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use rusternetes_middleware::AuthContext;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tower::ServiceExt;

/// The attempt counters are process-global: serialize the tests that read them.
pub(crate) static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn user(name: &str) -> UserInfo {
    UserInfo {
        username: name.into(),
        uid: String::new(),
        groups: vec![],
        extra: Default::default(),
    }
}

/// `fakeAuthorizer` (authorization_test.go:229-236).
struct Fake {
    opinion: Opinion,
    error: Option<String>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Authorizer for Fake {
    async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
        match self.authorize_opinion(a).await? {
            Opinion::Allow(_) => Ok(Decision::Allow),
            Opinion::Deny(r) => Ok(Decision::Deny(r)),
            Opinion::NoOpinion { reason, .. } => Ok(Decision::Deny(reason)),
        }
    }
    async fn authorize_opinion(&self, _: &RequestAttributes) -> Result<Opinion> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(match (&self.opinion, &self.error) {
            (Opinion::NoOpinion { reason, .. }, Some(e)) => Opinion::NoOpinion {
                reason: reason.clone(),
                error: Some(e.clone()),
            },
            (o, _) => o.clone(),
        })
    }
    async fn get_user_rules(
        &self,
        _: &UserInfo,
        _: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        Ok((vec![], vec![]))
    }
}

struct Capture(tokio::sync::Mutex<Vec<AuditEvent>>);

#[async_trait]
impl AuditBackend for Capture {
    async fn log(&self, e: AuditEvent) -> std::result::Result<(), String> {
        self.0.lock().await.push(e);
        Ok(())
    }
    async fn flush(&self) -> std::result::Result<(), String> {
        Ok(())
    }
}

const POLICY: &str = "apiVersion: audit.k8s.io/v1\nkind: Policy\nrules:\n- level: Metadata\n";

struct Harness {
    app: Router,
    handler_calls: Arc<AtomicUsize>,
    authorizer_calls: Arc<AtomicUsize>,
    audit: Arc<Capture>,
}

/// request-info, then audit, then authorization, as in
/// `DefaultBuildHandlerChain` (server/config.go:1015-1043: authorization is
/// innermost, inside audit).
fn harness(opinion: Opinion, error: Option<&str>, with_request_info: bool) -> Harness {
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let authorizer_calls = Arc::new(AtomicUsize::new(0));
    let audit = Arc::new(Capture(Default::default()));
    let cfg = Arc::new(AuditConfig {
        policy: Policy::from_yaml(POLICY).unwrap(),
        sink: audit.clone(),
    });
    let authorizer: Arc<dyn Authorizer> = Arc::new(Fake {
        opinion,
        error: error.map(str::to_string),
        calls: authorizer_calls.clone(),
    });
    let hc = handler_calls.clone();
    let mut app = Router::new()
        .fallback(any(move || {
            let hc = hc.clone();
            async move {
                hc.fetch_add(1, Ordering::SeqCst);
                "ok"
            }
        }))
        .layer(axum::middleware::from_fn_with_state(
            authorizer,
            with_authorization,
        ))
        .layer(axum::middleware::from_fn(move |req, next| {
            let cfg = cfg.clone();
            async move { with_audit(cfg, req, next).await }
        }));
    if with_request_info {
        app = app.layer(axum::middleware::from_fn(request_info_middleware));
    }
    Harness {
        app,
        handler_calls,
        authorizer_calls,
        audit,
    }
}

fn request(method: Method, uri: &str) -> Request<Body> {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut().insert(AuthContext {
        user: user("alice"),
    });
    req
}

async fn body_string(resp: axum::response::Response) -> String {
    let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(b.to_vec()).unwrap()
}

fn attempts(label: &str) -> u64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.name() == "authorization_attempts_total")
        .flat_map(|f| f.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "result" && l.value() == label)
        })
        .map(|m| m.get_counter().value() as u64)
        .sum()
}

// ---- TestGetAuthorizerAttributes (authorization_test.go:41-141) ----

#[test]
fn get_authorizer_attributes_matches_upstream_cases() {
    // (verb, path, query) -> (verb, resource_request, group, version, ns,
    // resource, name, field reqs, label reqs, field err, label err)
    struct Case {
        method: Method,
        path: &'static str,
        query: Option<&'static str>,
        verb: &'static str,
        resource_request: bool,
        group: &'static str,
        version: &'static str,
        namespace: Option<&'static str>,
        resource: &'static str,
        name: Option<&'static str>,
        field_reqs: usize,
        label_reqs: usize,
        field_err: bool,
        label_err: bool,
    }
    let non_resource = |method: Method, path: &'static str, verb: &'static str| Case {
        method,
        path,
        query: None,
        verb,
        resource_request: false,
        group: "",
        version: "",
        namespace: None,
        resource: "",
        name: None,
        field_reqs: 0,
        label_reqs: 0,
        field_err: false,
        label_err: false,
    };
    let jobs = |query: Option<&'static str>, f: usize, l: usize, fe: bool, le: bool| Case {
        method: Method::GET,
        path: "/apis/batch/v1/namespaces/myns/jobs",
        query,
        verb: "list",
        resource_request: true,
        group: "batch",
        version: "v1",
        namespace: Some("myns"),
        resource: "jobs",
        name: None,
        field_reqs: f,
        label_reqs: l,
        field_err: fe,
        label_err: le,
    };
    let cases = vec![
        ("non-resource root", non_resource(Method::POST, "/", "post")),
        (
            "non-resource api prefix",
            non_resource(Method::GET, "/api/", "get"),
        ),
        (
            "non-resource group api prefix",
            non_resource(Method::GET, "/apis/extensions/", "get"),
        ),
        (
            "resource",
            Case {
                method: Method::POST,
                path: "/api/v1/nodes/mynode",
                query: None,
                verb: "create",
                resource_request: true,
                group: "",
                version: "v1",
                namespace: None,
                resource: "nodes",
                name: Some("mynode"),
                field_reqs: 0,
                label_reqs: 0,
                field_err: false,
                label_err: false,
            },
        ),
        (
            "namespaced resource",
            Case {
                method: Method::PUT,
                path: "/api/v1/namespaces/myns/pods/mypod",
                query: None,
                verb: "update",
                resource_request: true,
                group: "",
                version: "v1",
                namespace: Some("myns"),
                resource: "pods",
                name: Some("mypod"),
                field_reqs: 0,
                label_reqs: 0,
                field_err: false,
                label_err: false,
            },
        ),
        ("API group resource", jobs(None, 0, 0, false, false)),
        (
            "enabled, good field selector",
            jobs(Some("fieldSelector=foo%3D%3Dbar"), 1, 0, false, false),
        ),
        (
            "enabled, bad field selector",
            jobs(Some("fieldSelector=%2Abar"), 0, 0, true, false),
        ),
        (
            "enabled, good label selector",
            jobs(Some("labelSelector=foo%3D%3Dbar"), 0, 1, false, false),
        ),
        (
            "enabled, bad label selector",
            jobs(Some("labelSelector=%2Abar"), 0, 0, false, true),
        ),
    ];
    for (name, c) in cases {
        let info = request_info(&c.method, c.path, c.query);
        let a = get_authorizer_attributes(user("alice"), &info);
        assert_eq!(a.user.username, "alice", "{name}");
        assert_eq!(a.verb, c.verb, "{name}: verb");
        assert_eq!(a.is_non_resource_request, !c.resource_request, "{name}");
        if !c.resource_request {
            assert_eq!(a.path.as_deref(), Some(c.path), "{name}: path");
        }
        assert_eq!(a.api_group, c.group, "{name}: group");
        assert_eq!(a.api_version, c.version, "{name}: version");
        assert_eq!(a.namespace.as_deref(), c.namespace, "{name}: namespace");
        assert_eq!(a.resource, c.resource, "{name}: resource");
        assert_eq!(a.name.as_deref(), c.name, "{name}: name");
        assert_eq!(a.field_selector_requirements.len(), c.field_reqs, "{name}");
        assert_eq!(a.label_selector_requirements.len(), c.label_reqs, "{name}");
        assert_eq!(
            a.field_selector_parsing_err.is_some(),
            c.field_err,
            "{name}"
        );
        assert_eq!(
            a.label_selector_parsing_err.is_some(),
            c.label_err,
            "{name}"
        );
    }
}

// ---- TestForbidden (responsewriters/errors_test.go:58-92) ----

fn non_resource(verb: &str, path: &str) -> RequestAttributes {
    RequestAttributes::new_non_resource(user("NAME"), verb, path)
}

#[tokio::test]
async fn forbidden_body_matches_upstream_cases() {
    let resource =
        |group: &str, ns: Option<&str>, res: &str, sub: Option<&str>, name: Option<&str>| {
            let mut a = RequestAttributes::new(user("NAME"), "get", res).with_api_group(group);
            if let Some(ns) = ns {
                a = a.with_namespace(ns);
            }
            if let Some(sub) = sub {
                a = a.with_subresource(sub);
            }
            if let Some(name) = name {
                a = a.with_name(name);
            }
            a
        };
    let cases: Vec<(RequestAttributes, &str, serde_json::Value)> = vec![
        (
            non_resource("GET", "/whatever"),
            "",
            serde_json::json!({"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"forbidden: User \"NAME\" cannot GET path \"/whatever\"","reason":"Forbidden","details":{},"code":403}),
        ),
        (
            non_resource("GET", "/<script>"),
            "",
            serde_json::json!({"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"forbidden: User \"NAME\" cannot GET path \"/&lt;script&gt;\"","reason":"Forbidden","details":{},"code":403}),
        ),
        (
            resource("", None, "pod", None, None),
            "",
            serde_json::json!({"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"pod is forbidden: User \"NAME\" cannot get resource \"pod\" in API group \"\" at the cluster scope","reason":"Forbidden","details":{"kind":"pod"},"code":403}),
        ),
        (
            resource("", None, "pod", None, Some("mypod")),
            "",
            serde_json::json!({"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"pod \"mypod\" is forbidden: User \"NAME\" cannot get resource \"pod\" in API group \"\" at the cluster scope","reason":"Forbidden","details":{"name":"mypod","kind":"pod"},"code":403}),
        ),
        (
            resource("v2", Some("test"), "pod", Some("quota"), None),
            "",
            serde_json::json!({"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"pod.v2 is forbidden: User \"NAME\" cannot get resource \"pod/quota\" in API group \"v2\" in the namespace \"test\"","reason":"Forbidden","details":{"group":"v2","kind":"pod"},"code":403}),
        ),
        // ForbiddenStatusError appends a non-empty reason (errors.go).
        (
            resource("", None, "pod", None, None),
            "RBAC: no",
            serde_json::json!({"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"pod is forbidden: User \"NAME\" cannot get resource \"pod\" in API group \"\" at the cluster scope: RBAC: no","reason":"Forbidden","details":{"kind":"pod"},"code":403}),
        ),
    ];
    for (attrs, reason, expected) in cases {
        let resp = forbidden_negotiated(&attrs, reason, &axum::http::HeaderMap::new(), None);
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.headers()["content-type"], "application/json");
        let got: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
        assert_eq!(got, expected);
    }
    assert_eq!(
        forbidden_message(&non_resource("GET", "/whatever")),
        "User \"NAME\" cannot GET path \"/whatever\""
    );
}

// ---- TestAuditAnnotation (authorization_test.go:238-297) ----

async fn annotations(h: &Harness) -> std::collections::BTreeMap<String, String> {
    let ev = h.audit.0.lock().await;
    ev.last()
        .and_then(|e| e.annotations.clone())
        .map(|m| m.into_iter().collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn allow_passes_through_and_annotates() {
    let _g = LOCK.lock().await;
    let h = harness(
        Opinion::Allow("RBAC: allowed to patch pod".into()),
        None,
        true,
    );
    let before = attempts("allowed");
    let resp = h
        .app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/namespaces/default/pods"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(h.handler_calls.load(Ordering::SeqCst), 1);
    let a = annotations(&h).await;
    assert_eq!(a["authorization.k8s.io/decision"], "allow");
    assert_eq!(
        a["authorization.k8s.io/reason"],
        "RBAC: allowed to patch pod"
    );
    assert_eq!(attempts("allowed"), before + 1);
}

#[tokio::test]
async fn deny_is_a_403_status_and_annotates() {
    let _g = LOCK.lock().await;
    let h = harness(
        Opinion::Deny("RBAC: not allowed to patch pod".into()),
        None,
        true,
    );
    let before = attempts("denied");
    let resp = h
        .app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/namespaces/default/pods"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(h.handler_calls.load(Ordering::SeqCst), 0);
    let body: serde_json::Value = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(
        body["message"],
        "pods is forbidden: User \"alice\" cannot list resource \"pods\" in API group \"\" in the namespace \"default\": RBAC: not allowed to patch pod"
    );
    assert_eq!(body["details"]["kind"], "pods");
    let a = annotations(&h).await;
    assert_eq!(a["authorization.k8s.io/decision"], "forbid");
    assert_eq!(
        a["authorization.k8s.io/reason"],
        "RBAC: not allowed to patch pod"
    );
    assert_eq!(attempts("denied"), before + 1);
}

/// A NoOpinion without an error is forbidden too (authorization.go: only
/// `DecisionAllow` passes).
#[tokio::test]
async fn no_opinion_is_forbidden() {
    let _g = LOCK.lock().await;
    let h = harness(
        Opinion::NoOpinion {
            reason: "nobody decided".into(),
            error: None,
        },
        None,
        true,
    );
    let resp = h
        .app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/namespaces/default/pods"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(h.handler_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        annotations(&h).await["authorization.k8s.io/decision"],
        "forbid"
    );
}

#[tokio::test]
async fn authorizer_error_is_a_500_with_the_internal_error_reason() {
    let _g = LOCK.lock().await;
    let h = harness(
        Opinion::NoOpinion {
            reason: String::new(),
            error: None,
        },
        Some("can't parse user info"),
        true,
    );
    let resp = h
        .app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/namespaces/default/pods"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(h.handler_calls.load(Ordering::SeqCst), 0);
    // responsewriters.InternalError (errors.go:83-87).
    assert_eq!(
        body_string(resp).await.trim_end(),
        "Internal Server Error: \"/api/v1/namespaces/default/pods\": can't parse user info"
    );
    let a = annotations(&h).await;
    assert!(!a.contains_key("authorization.k8s.io/decision"));
    assert_eq!(a["authorization.k8s.io/reason"], "internal error");
}

/// authorization.go:60-63: no RequestInfo is an internal error and the
/// authorizer is never consulted.
#[tokio::test]
async fn missing_request_info_is_a_500_and_does_not_authorize() {
    let _g = LOCK.lock().await;
    let h = harness(Opinion::Allow(String::new()), None, false);
    let resp = h
        .app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/namespaces/default/pods"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(h.authorizer_calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.handler_calls.load(Ordering::SeqCst), 0);
    assert!(body_string(resp)
        .await
        .contains("no RequestInfo found in the context"));
}

/// The authorizer is asked once per request, with the RequestInfo's attributes.
#[tokio::test]
async fn authorizes_once_per_request() {
    let _g = LOCK.lock().await;
    let h = harness(Opinion::Allow(String::new()), None, true);
    h.app
        .clone()
        .oneshot(request(Method::GET, "/api/v1/namespaces/default/pods/p"))
        .await
        .unwrap();
    assert_eq!(h.authorizer_calls.load(Ordering::SeqCst), 1);
}

// ---- wiring: the filter is in the router's handler chain ----

/// Through `build_router`, a denied request is answered by the filter (the
/// `forbiddenMessage` body, one counted attempt, the handler's own check never
/// reached), not by the handler's own authorize.
#[tokio::test]
async fn router_authorizes_through_the_filter() {
    use rusternetes_test_support::harness::TestApiServer;
    let _g = LOCK.lock().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let api = TestApiServer::builder()
        .authorizer(Arc::new(Fake {
            opinion: Opinion::Deny("RBAC: nope".into()),
            error: None,
            calls: calls.clone(),
        }))
        .build();
    let before = attempts("denied");
    let (status, body) = api.get("/api/v1/namespaces/default/pods").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        body["message"],
        "pods is forbidden: User \"admin\" cannot list resource \"pods\" in API group \"\" in the namespace \"default\": RBAC: nope"
    );
    assert_eq!(body["details"]["kind"], "pods");
    assert_eq!(attempts("denied"), before + 1);
    // Once: the filter's; the handler never ran.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// An allowed request authorizes in the filter once, and once more in each
/// handler check that remains behind it, without counting again.
#[tokio::test]
async fn router_counts_one_attempt_for_an_allowed_request() {
    use rusternetes_test_support::harness::TestApiServer;
    let _g = LOCK.lock().await;
    let api = TestApiServer::new();
    let before = attempts("allowed");
    let (status, _) = api.get("/api/v1/namespaces/default/pods").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attempts("allowed"), before + 1);
}
