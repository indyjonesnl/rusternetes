//! Attribute-equality proof for #3515 ("option 3": make the WithAuthorization
//! filter the source of truth).
//!
//! Upstream authorizes once, from the request's `RequestInfo`
//! (`filters/authorization.go:57-99`, `GetAuthorizerAttributes` :101-153);
//! the registry never authorizes the request again. Rusternetes handlers still
//! build their own attributes. Before a handler's check can be dropped, the
//! attributes it builds must equal what the filter derived for the same
//! request. This test drives every route in `router.rs` (scraped from the
//! source, so a new route is covered automatically) through the real router
//! with a recording authorizer and asserts that every authorization the
//! handler makes equals the filter's (the first one) on
//! (verb, group, resource, subresource, name, namespace, path).
//!
//! `api_version` and the selectors are not compared: handlers leave them to
//! `api_version_for` / `with_request_selectors` (request_scope.rs).

use async_trait::async_trait;
use axum::http::StatusCode;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{
    AuthorizationOutcome, Authorizer, Decision, Opinion, RequestAttributes,
};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use rusternetes_test_support::harness::TestApiServer;
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Key = (String, String, String, String, String, String, String, bool);

fn key(a: &RequestAttributes) -> Key {
    (
        a.verb.clone(),
        a.api_group.clone(),
        a.resource.clone(),
        a.subresource.clone().unwrap_or_default(),
        a.name.clone().unwrap_or_default(),
        a.namespace.clone().unwrap_or_default(),
        a.path.clone().unwrap_or_default(),
        a.is_non_resource_request,
    )
}

fn show(k: &Key) -> String {
    format!(
        "verb={} group={:?} resource={} sub={:?} name={:?} ns={:?} path={:?} nonres={}",
        k.0, k.1, k.2, k.3, k.4, k.5, k.6, k.7
    )
}

struct Recorder {
    inner: Arc<dyn Authorizer>,
    log: Arc<Mutex<Vec<Key>>>,
}

#[async_trait]
impl Authorizer for Recorder {
    async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
        self.log.lock().unwrap().push(key(a));
        self.inner.authorize(a).await
    }
    async fn authorize_opinion(&self, a: &RequestAttributes) -> Result<Opinion> {
        self.log.lock().unwrap().push(key(a));
        self.inner.authorize_opinion(a).await
    }
    async fn authorize_outcome(&self, a: &RequestAttributes) -> Result<AuthorizationOutcome> {
        self.log.lock().unwrap().push(key(a));
        self.inner.authorize_outcome(a).await
    }
    async fn get_user_rules(
        &self,
        u: &UserInfo,
        ns: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        self.inner.get_user_rules(u, ns).await
    }
}

/// Every `.route("<path>"` literal in router.rs.
fn scraped_routes() -> Vec<String> {
    let src = include_str!("../../src/router.rs");
    let mut out = Vec::new();
    let mut rest = src;
    while let Some(i) = rest.find(".route(") {
        rest = &rest[i + ".route(".len()..];
        let t = rest.trim_start();
        if let Some(t) = t.strip_prefix('"') {
            if let Some(end) = t.find('"') {
                let p = &t[..end];
                if p.starts_with('/') && !out.contains(&p.to_string()) {
                    out.push(p.to_string());
                }
            }
        }
    }
    out
}

fn instantiate(route: &str) -> String {
    route
        .split('/')
        .map(|seg| {
            if let Some(p) = seg.strip_prefix(':') {
                match p {
                    "namespace" | "ns" => "ns1".to_string(),
                    _ => "obj1".to_string(),
                }
            } else if seg.starts_with('*') {
                "p/q".to_string()
            } else {
                seg.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Request variants for each route. (method, uri-suffix, content-type).
const VARIANTS: &[(&str, &str)] = &[
    ("GET", ""),
    ("GET", "?watch=true"),
    // requestinfo.go:247-253: an exact metadata.name field selector names the
    // object of a list or watch.
    ("GET", "?fieldSelector=metadata.name%3Dobj1"),
    ("GET", "?watch=true&fieldSelector=metadata.name%3Dobj1"),
    ("GET", "?watch=false"),
    ("HEAD", ""),
    ("OPTIONS", ""),
    ("POST", ""),
    ("PUT", ""),
    ("PATCH", ""),
    ("DELETE", ""),
];

async fn drive(server: &TestApiServer, method: &str, uri: &str) -> u16 {
    let body = serde_json::to_vec(&serde_json::json!({
        "apiVersion": "v1", "kind": "Object", "metadata": {"name": "obj1"}
    }))
    .unwrap();
    let ct = if method == "PATCH" {
        "application/merge-patch+json"
    } else {
        "application/json"
    };
    let has_body = matches!(method, "POST" | "PUT" | "PATCH");
    let fut = server.respond(
        method,
        uri,
        has_body.then_some(ct),
        has_body.then_some(body),
    );
    // Streaming responses (watch) are dropped unread: authorization happened
    // before the response head was produced.
    match tokio::time::timeout(Duration::from_secs(10), fut).await {
        Ok(resp) => resp.status().as_u16(),
        Err(_) => 0,
    }
}

/// In-handler checks upstream makes beyond the filter's, with the filter's
/// attributes otherwise unchanged. These stay (they are not the request's own
/// check) and are what the equality proof sets aside:
///
/// * create-on-update: PUT/PATCH of a missing object also needs `create`
///   (`handlers/update.go:192-215`, `handlers/patch.go:203-232`);
/// * pod `exec`/`attach`/`portforward` over websocket (GET) also need `create`
///   (`pkg/registry/core/pod/rest/authorize.go` `ensureAuthorizedForVerb`,
///   issue 133515 in kubernetes/kubernetes).
fn upstream_in_handler_check(filter: &Key, handler: &Key) -> bool {
    let rest = |k: &Key| {
        (
            k.1.clone(),
            k.2.clone(),
            k.3.clone(),
            k.4.clone(),
            k.5.clone(),
        )
    };
    if rest(filter) != rest(handler) || handler.0 != "create" {
        return false;
    }
    matches!(filter.0.as_str(), "update" | "patch")
        || (filter.0 == "get" && matches!(filter.3.as_str(), "exec" | "attach" | "portforward"))
}

async fn check_family(
    server: &TestApiServer,
    log: &Arc<Mutex<Vec<Key>>>,
    paths: &[String],
    diverged: &mut Vec<String>,
    stats: &mut (usize, usize),
) {
    for path in paths {
        for (method, suffix) in VARIANTS {
            log.lock().unwrap().clear();
            let uri = format!("{path}{suffix}");
            let status = drive(server, method, &uri).await;
            let calls: Vec<Key> = std::mem::take(&mut *log.lock().unwrap());
            let Some((filter, handler)) = calls.split_first() else {
                continue;
            };
            if handler.is_empty() {
                stats.1 += 1;
                eprintln!("FILTER-ONLY {status} {method} {uri} :: {}", show(filter));
                continue;
            }
            stats.0 += 1;
            for h in handler {
                if h != filter && !upstream_in_handler_check(filter, h) {
                    diverged.push(format!(
                        "{method} {uri}\n    filter : {}\n    handler: {}",
                        show(filter),
                        show(h)
                    ));
                }
            }
        }
    }
}

fn recording_server() -> (TestApiServer, Arc<Mutex<Vec<Key>>>) {
    let log: Arc<Mutex<Vec<Key>>> = Arc::default();
    let l2 = log.clone();
    let server = TestApiServer::builder()
        .wrap_authorizer(move |inner| Arc::new(Recorder { inner, log: l2 }))
        .build();
    (server, log)
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_attributes_equal_filter_attributes_on_every_route() {
    let (server, log) = recording_server();
    // Every resource route of router.rs (non-resource routes have no handler
    // check).
    let paths: Vec<String> = scraped_routes()
        .iter()
        .filter(|r| r.starts_with("/api"))
        .map(|r| instantiate(r))
        .collect();
    let (mut diverged, mut stats) = (Vec::new(), (0, 0));
    check_family(&server, &log, &paths, &mut diverged, &mut stats).await;
    eprintln!("covered={} filter_only={}", stats.0, stats.1);
    assert!(
        diverged.is_empty(),
        "{} handler authorizations differ from the filter's:\n{}",
        diverged.len(),
        diverged.join("\n")
    );
}

/// Custom resources are served by the dynamic routes, not `router.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn custom_resource_handler_attributes_equal_filter_attributes() {
    let (server, log) = recording_server();
    for (scope, plural, kind) in [
        ("Namespaced", "widgets", "Widget"),
        ("Cluster", "gadgets", "Gadget"),
    ] {
        let crd = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
            "metadata": {"name": format!("{plural}.example.com")},
            "spec": {"group": "example.com", "scope": scope,
                "names": {"plural": plural, "singular": plural.trim_end_matches('s'),
                          "kind": kind, "listKind": format!("{kind}List")},
                "versions": [{"name": "v1", "served": true, "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object",
                        "x-kubernetes-preserve-unknown-fields": true}},
                    "subresources": {"status": {}, "scale": {
                        "specReplicasPath": ".spec.replicas",
                        "statusReplicasPath": ".status.replicas"}}}]}
        });
        let (st, _) = server
            .post(
                "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
                &crd,
            )
            .await;
        assert!(st.is_success(), "CRD create: {st}");
        server
            .wait_crd_established(&format!("{plural}.example.com"))
            .await;
    }
    let paths: Vec<String> = [
        "/apis/example.com/v1/namespaces/ns1/widgets",
        "/apis/example.com/v1/namespaces/ns1/widgets/obj1",
        "/apis/example.com/v1/namespaces/ns1/widgets/obj1/status",
        "/apis/example.com/v1/namespaces/ns1/widgets/obj1/scale",
        "/apis/example.com/v1/widgets",
        "/apis/example.com/v1/gadgets",
        "/apis/example.com/v1/gadgets/obj1",
        "/apis/example.com/v1/gadgets/obj1/status",
        "/apis/example.com/v1/gadgets/obj1/scale",
    ]
    .map(String::from)
    .to_vec();
    let (mut diverged, mut stats) = (Vec::new(), (0, 0));
    check_family(&server, &log, &paths, &mut diverged, &mut stats).await;
    eprintln!("CR covered={} filter_only={}", stats.0, stats.1);
    assert!(diverged.is_empty(), "{}", diverged.join("\n"));
}

/// An authorizer that allows exactly the given attributes, as the filter
/// derives them, and nothing else: a handler that asks for anything else is
/// denied (403), so a request passes only if the handler's check equals the
/// filter's.
struct AllowOnly(Key);

#[async_trait]
impl Authorizer for AllowOnly {
    async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
        Ok(if key(a) == self.0 {
            Decision::Allow
        } else {
            Decision::Deny("not the filter's attributes".into())
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

/// Router-level denied-path tests for the divergences fixed for #3515: each
/// request's authorizer allows only what `RequestInfo` derives, and the
/// handler must ask for exactly that. Each of these was a 403 from the
/// handler before the fix.
#[tokio::test(flavor = "multi_thread")]
async fn fixed_divergences_are_not_denied_by_the_handler() {
    let cases: &[(&str, &str)] = &[
        // namespace requests carry their own name as the namespace
        ("GET", "/api/v1/namespaces/obj1"),
        ("PUT", "/api/v1/namespaces/obj1"),
        ("GET", "/api/v1/namespaces/obj1/status"),
        ("PUT", "/api/v1/namespaces/obj1/finalize"),
        // list/watch naming the object by an exact metadata.name selector
        (
            "GET",
            "/api/v1/namespaces/ns1/pods?fieldSelector=metadata.name%3Dobj1",
        ),
        (
            "GET",
            "/api/v1/namespaces/ns1/pods?watch=true&fieldSelector=metadata.name%3Dobj1",
        ),
        (
            "GET",
            "/apis/apps/v1/deployments?fieldSelector=metadata.name%3Dobj1",
        ),
        (
            "GET",
            "/api/v1/nodes?watch=true&fieldSelector=metadata.name%3Dobj1",
        ),
        // `proxy` is a subresource, and the name is the raw id
        ("GET", "/api/v1/nodes/obj1:10250/proxy/pods"),
        ("POST", "/api/v1/namespaces/ns1/pods/obj1:8080/proxy/x"),
        (
            "GET",
            "/api/v1/namespaces/ns1/services/http:obj1:80/proxy/x",
        ),
        ("OPTIONS", "/api/v1/namespaces/ns1/pods/obj1/proxy/x"),
        // token is a subresource
        ("POST", "/api/v1/namespaces/ns1/serviceaccounts/obj1/token"),
        // metrics.k8s.io has no `metrics` subresource; watch is its own verb
        ("GET", "/apis/metrics.k8s.io/v1beta1/nodes"),
        ("GET", "/apis/metrics.k8s.io/v1beta1/nodes?watch=true"),
        ("GET", "/apis/metrics.k8s.io/v1beta1/nodes/obj1"),
        ("GET", "/apis/metrics.k8s.io/v1beta1/pods?watch=true"),
        ("GET", "/apis/metrics.k8s.io/v1beta1/namespaces/ns1/pods"),
        (
            "GET",
            "/apis/metrics.k8s.io/v1beta1/namespaces/ns1/pods/obj1",
        ),
        ("GET", "/api/v1/componentstatuses?watch=true"),
        (
            "GET",
            "/apis/external.metrics.k8s.io/v1beta1/namespaces/ns1/obj1?watch=true",
        ),
        (
            "GET",
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns1/metrics/obj1",
        ),
        (
            "GET",
            "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns1/pods/obj1",
        ),
    ];
    for (method, uri) in cases {
        let (path, query) = match uri.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (*uri, None),
        };
        let m: axum::http::Method = method.parse().unwrap();
        let info = rusternetes_api_server::audit::request_info(&m, path, query);
        let expected = key(
            &rusternetes_api_server::authorization_filter::get_authorizer_attributes(
                UserInfo::anonymous(),
                &info,
            ),
        );
        let server = TestApiServer::builder()
            .authorizer(Arc::new(AllowOnly(expected.clone())))
            .build();
        let status = drive(&server, method, uri).await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN.as_u16(),
            "{method} {uri}: the handler was denied with the filter's attributes allowed ({})",
            show(&expected)
        );
    }
}
