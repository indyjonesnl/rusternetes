//! The query the `WithAuthorization` filter authorizes is the query the
//! handler serves (#3515).
//!
//! Upstream builds `RequestInfo` from `req.URL.Query()` and the handlers decode
//! the same query with `DecodeParameters`: the key is URL-decoded, a present
//! key with no value is the empty string (`?watch` is true,
//! `Convert_Slice_string_To_bool`, runtime/conversion.go:83), and the first
//! value of a repeated key is the one that counts (requestinfo.go:225-275).
//!
//! Before this, `request_info` read the raw key (`wat%63h=true` and `?watch`
//! were a list while the handler streamed a watch), and the handlers' `HashMap`
//! decode took the LAST of a repeated `fieldSelector` while `RequestInfo`
//! named the FIRST object: a user allowed to list pod `a` could be served pod
//! `b` once the handlers' own checks are gone.

use async_trait::async_trait;
use axum::http::{Method, StatusCode};
use rusternetes_api_server::audit::request_info;
use rusternetes_api_server::request_scope::first_value_for_authz_keys;
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use rusternetes_test_support::harness::TestApiServer;
use std::sync::Arc;

#[test]
fn request_info_watch_is_decoded_and_first_valued() {
    // (query, verb) for GET /api/v1/pods
    let cases = [
        ("", "list"),
        ("watch=true", "watch"),
        ("watch=1", "watch"),
        ("watch=t", "watch"),
        // present with no value is true
        ("watch", "watch"),
        ("watch=", "watch"),
        ("watch=0", "list"),
        ("watch=false", "list"),
        ("watch=False", "list"),
        ("watch=FALSE", "list"),
        // the key is URL-decoded
        ("wat%63h=true", "watch"),
        ("watch=%66alse", "list"),
        // the first value counts
        ("watch=true&watch=false", "watch"),
        ("watch=false&watch=true", "list"),
        ("limit=5&watch=true", "watch"),
        ("watchx=true", "list"),
    ];
    for (query, verb) in cases {
        let info = request_info(&Method::GET, "/api/v1/pods", Some(query));
        assert_eq!(info.verb, verb, "?{query}");
    }
}

#[test]
fn only_a_repeated_authorization_parameter_is_reduced_to_its_first_value() {
    for (query, want) in [
        ("watch=true&watch=false", Some("watch=true")),
        (
            "fieldSelector=metadata.name%3Da&fieldSelector=metadata.name%3Db",
            Some("fieldSelector=metadata.name%3Da"),
        ),
        (
            "labelSelector=a%3Db&x=1&labelSelector=c",
            Some("labelSelector=a%3Db&x=1"),
        ),
        // the decoded key repeats
        ("watch=true&wat%63h=false", Some("watch=true")),
        // repeatable parameters, and everything not repeated, are untouched
        ("command=a&command=b", None),
        ("dryRun=All&dryRun=All&watch=true", None),
        ("limit=1&watch=true&limit=2", None),
        ("", None),
        ("a=1&&b=2", None),
    ] {
        assert_eq!(
            first_value_for_authz_keys(query).as_deref(),
            want,
            "?{query}"
        );
    }
}

/// Allows exactly the requests `allow` accepts.
struct Pred(Box<dyn Fn(&RequestAttributes) -> bool + Send + Sync>);

#[async_trait]
impl Authorizer for Pred {
    async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
        Ok(if (self.0)(a) {
            Decision::Allow
        } else {
            Decision::Deny("denied".into())
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

fn server(allow: impl Fn(&RequestAttributes) -> bool + Send + Sync + 'static) -> TestApiServer {
    TestApiServer::builder()
        .authorizer(Arc::new(Pred(Box::new(allow))))
        .build()
}

/// A user who may only `list` pods cannot reach the watch by spelling `watch`
/// in a way the filter did not decode.
#[tokio::test]
async fn a_list_only_user_cannot_watch_through_an_unusual_spelling() {
    for query in [
        "watch",
        "watch=",
        "wat%63h=true",
        "watch=1&timeoutSeconds=1",
    ] {
        let s = server(|a| a.verb == "list");
        let (status, _) = s.get(&format!("/api/v1/namespaces/ns1/pods?{query}")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "?{query}");
    }
    // and an unambiguous list is still allowed
    let s = server(|a| a.verb == "list");
    let (status, _) = s.get("/api/v1/namespaces/ns1/pods?watch=false").await;
    assert_eq!(status, StatusCode::OK);
}

/// A user allowed to list pod `a` (by `metadata.name`) is served pod `a`, not
/// the object a later repeat of the selector names.
#[tokio::test]
async fn a_repeated_field_selector_serves_the_object_the_filter_authorized() {
    let s = server(|a| a.name.as_deref().is_none_or(|n| n == "a") || a.verb != "list");
    for n in ["a", "b"] {
        let pod = serde_json::json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": n}, "spec": {"containers": [{"name": "c", "image": "i"}]}});
        let (st, _) = s.post("/api/v1/namespaces/ns1/pods", &pod).await;
        assert!(st.is_success(), "create {n}: {st}");
    }
    let (status, body) = s
        .get(
            "/api/v1/namespaces/ns1/pods?fieldSelector=metadata.name%3Da\
             &fieldSelector=metadata.name%3Db",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["metadata"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["a"], "served {names:?}");
}
