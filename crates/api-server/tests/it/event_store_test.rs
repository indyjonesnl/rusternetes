//! Event on the generic Store (Epic #1990).
//!
//! Both `/api/v1` `events` and `/apis/events.k8s.io/v1` `events` reach one
//! upstream registry, `pkg/registry/core/event/storage/storage.go`, whose
//! strategy (`pkg/registry/core/event/strategy.go`) has
//! `AllowCreateOnUpdate() == true` and `AllowUnconditionalUpdate() == true`,
//! and validates with `ValidateEventCreate` / `ValidateEventUpdate`
//! (`pkg/apis/core/validation/events.go:41,74`), which branch on the request
//! group-version (`requestGroupVersion`, strategy.go:134-139): core `v1` gets
//! only the legacy rules, `events.k8s.io/v1` the strict ones.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn core(name: &str) -> String {
    format!("/api/v1/namespaces/{NS}/events/{name}")
}
fn core_coll() -> String {
    format!("/api/v1/namespaces/{NS}/events")
}
fn v1(name: &str) -> String {
    format!("/apis/events.k8s.io/v1/namespaces/{NS}/events/{name}")
}
fn v1_coll() -> String {
    format!("/apis/events.k8s.io/v1/namespaces/{NS}/events")
}

fn core_body(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": { "name": name },
        "involvedObject": { "kind": "Pod", "name": "p", "namespace": NS },
        "reason": "Probe",
        "message": "hello",
        "type": "Normal",
    })
}

fn v1_body(name: &str) -> Value {
    json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": { "name": name },
        "eventTime": "2026-09-08T10:05:00.123456Z",
        "reportingController": "probe",
        "reportingInstance": "probe-0",
        "action": "Probe",
        "reason": "Probe",
        "note": "hello",
        "type": "Normal",
        "regarding": { "kind": "Pod", "name": "p", "namespace": NS },
    })
}

async fn send(
    s: &TestApiServer,
    method: &str,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    s.send(method, uri, body.map(|_| "application/json"), body)
        .await
}

fn causes(status: &Value) -> Vec<String> {
    status["details"]["causes"]
        .as_array()
        .map(|c| {
            c.iter()
                .filter_map(|c| c["field"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// strategy.go:74 -- a PUT to a missing event creates it, on both versions.
#[tokio::test]
async fn a_put_to_a_missing_event_creates_it_on_both_versions() {
    let s = TestApiServer::new();
    let (code, out) = send(&s, "PUT", &core("cou-core"), Some(&core_body("cou-core"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert_eq!(out["apiVersion"], json!("v1"));
    assert!(!out["metadata"]["uid"].as_str().unwrap_or("").is_empty());

    let (code, out) = send(&s, "PUT", &v1("cou-v1"), Some(&v1_body("cou-v1"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    assert_eq!(out["apiVersion"], json!("events.k8s.io/v1"));
    assert_eq!(out["note"], json!("hello"));
}

/// strategy.go:98 -- no resourceVersion on a PUT is applied to the latest.
#[tokio::test]
async fn a_put_without_a_resource_version_updates_on_both_versions() {
    let s = TestApiServer::new();
    let (code, out) = send(&s, "POST", &core_coll(), Some(&core_body("unc-core"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let uid = out["metadata"]["uid"].clone();
    let mut b = core_body("unc-core");
    b["message"] = json!("changed");
    let (code, out) = send(&s, "PUT", &core("unc-core"), Some(&b)).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["message"], json!("changed"));
    assert_eq!(out["metadata"]["uid"], uid, "the uid is server-owned");

    let (code, out) = send(&s, "POST", &v1_coll(), Some(&v1_body("unc-v1"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let mut b = v1_body("unc-v1");
    b["series"] = json!({"count": 2, "lastObservedTime": "2026-09-08T10:06:00.000000Z"});
    let (code, out) = send(&s, "PUT", &v1("unc-v1"), Some(&b)).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["series"]["count"], json!(2));
}

/// ValidateEventUpdate: core/v1 is legacy-only, so `reason` may change there;
/// events.k8s.io/v1 makes it immutable.
#[tokio::test]
async fn update_validation_follows_the_served_version() {
    let s = TestApiServer::new();
    send(&s, "POST", &core_coll(), Some(&core_body("val-core"))).await;
    let mut b = core_body("val-core");
    b["reason"] = json!("Other");
    let (code, out) = send(&s, "PUT", &core("val-core"), Some(&b)).await;
    assert_eq!(code, StatusCode::OK, "core reason change: {out}");

    send(&s, "POST", &v1_coll(), Some(&v1_body("val-v1"))).await;
    let mut b = v1_body("val-v1");
    b["reason"] = json!("Other");
    let (code, out) = send(&s, "PUT", &v1("val-v1"), Some(&b)).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(causes(&out).contains(&"reason".to_string()), "{out}");
}

/// ValidateEventCreate: the strict branch applies only to events.k8s.io/v1.
#[tokio::test]
async fn create_validation_follows_the_served_version() {
    let s = TestApiServer::new();
    // No eventTime: legacy accepts it on core, strict demands it on v1.
    let (code, out) = send(&s, "POST", &core_coll(), Some(&core_body("cv-core"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let mut b = v1_body("cv-v1");
    b.as_object_mut().unwrap().remove("eventTime");
    let (code, out) = send(&s, "POST", &v1_coll(), Some(&b)).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(causes(&out).contains(&"eventTime".to_string()), "{out}");

    // legacyValidateEvent runs on both: involvedObject.namespace must match.
    let mut b = core_body("cv-ns");
    b["involvedObject"]["namespace"] = json!("other");
    let (code, out) = send(&s, "POST", &core_coll(), Some(&b)).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        causes(&out).contains(&"involvedObject.namespace".to_string()),
        "{out}"
    );

    // A new-style event on core names its field reportingComponent.
    let mut b = core_body("cv-rc");
    b["eventTime"] = json!("2026-09-08T10:05:00.123456Z");
    let (code, out) = send(&s, "POST", &core_coll(), Some(&b)).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(
        causes(&out).contains(&"reportingComponent".to_string()),
        "{out}"
    );
}

/// One stored object is readable through both APIs, in each one's schema.
#[tokio::test]
async fn each_api_reads_the_other_apis_objects() {
    let s = TestApiServer::new();
    let (code, out) = send(&s, "POST", &core_coll(), Some(&core_body("x-core"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, out) = send(&s, "GET", &v1("x-core"), None).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["apiVersion"], json!("events.k8s.io/v1"));
    assert_eq!(out["note"], json!("hello"));
    assert_eq!(out["regarding"]["name"], json!("p"));

    let (code, out) = send(&s, "POST", &v1_coll(), Some(&v1_body("x-v1"))).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, out) = send(&s, "GET", &core("x-v1"), None).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["apiVersion"], json!("v1"));
    assert_eq!(out["message"], json!("hello"));
    assert_eq!(out["involvedObject"]["name"], json!("p"));
}

/// A body naming the other API version is a 400 (rest.go decode).
#[tokio::test]
async fn a_body_for_the_other_version_is_a_bad_request() {
    let s = TestApiServer::new();
    let (code, out) = send(&s, "POST", &core_coll(), Some(&v1_body("wrong-a"))).await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{out}");
    let (code, out) = send(&s, "POST", &v1_coll(), Some(&core_body("wrong-b"))).await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{out}");
}

/// rest.BeforeCreate: `generateName` is honoured and a nameless event is
/// refused (ValidateObjectMeta: "name or generateName is required").
#[tokio::test]
async fn names_are_generated_or_required() {
    let s = TestApiServer::new();
    let mut b = core_body("");
    b["metadata"] = json!({ "generateName": "gen-" });
    let (code, out) = send(&s, "POST", &core_coll(), Some(&b)).await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let name = out["metadata"]["name"].as_str().unwrap_or("");
    assert!(name.starts_with("gen-") && name.len() > 4, "{name}");

    let mut b = core_body("");
    b["metadata"] = json!({});
    let (code, out) = send(&s, "POST", &core_coll(), Some(&b)).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(causes(&out).contains(&"metadata.name".to_string()), "{out}");
}

/// Name rules: core is path-segment only (legacy validation adds none), the
/// strict events.k8s.io/v1 path demands a DNS subdomain.
#[tokio::test]
async fn name_rules_follow_the_served_version() {
    let s = TestApiServer::new();
    let (code, out) = send(&s, "POST", &core_coll(), Some(&core_body("Upper_Case"))).await;
    assert_eq!(
        code,
        StatusCode::CREATED,
        "core takes any path segment: {out}"
    );

    let (code, out) = send(&s, "POST", &v1_coll(), Some(&v1_body("Upper_Case2"))).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
    assert!(causes(&out).contains(&"metadata.name".to_string()), "{out}");

    let (code, out) = send(&s, "POST", &core_coll(), Some(&core_body("a%25b"))).await;
    assert_eq!(code, StatusCode::UNPROCESSABLE_ENTITY, "{out}");
}

/// A PUT whose body names another object is a 400 (checkName).
#[tokio::test]
async fn a_put_with_a_mismatched_name_is_a_bad_request() {
    let s = TestApiServer::new();
    let (code, out) = send(&s, "PUT", &core("one"), Some(&core_body("two"))).await;
    assert_eq!(code, StatusCode::BAD_REQUEST, "{out}");
}

/// A PUT naming another uid fails the uid precondition (BeforeUpdate /
/// Store.Update), instead of silently adopting the stored one.
#[tokio::test]
async fn a_put_with_a_stale_uid_conflicts() {
    let s = TestApiServer::new();
    send(&s, "POST", &core_coll(), Some(&core_body("uid-probe"))).await;
    let mut b = core_body("uid-probe");
    b["metadata"]["uid"] = json!("00000000-0000-0000-0000-000000000000");
    let (code, out) = send(&s, "PUT", &core("uid-probe"), Some(&b)).await;
    assert_eq!(code, StatusCode::CONFLICT, "{out}");
}

/// Dry-run validates and answers without persisting.
#[tokio::test]
async fn a_dry_run_create_persists_nothing() {
    let s = TestApiServer::new();
    let (code, out) = send(
        &s,
        "POST",
        &format!("{}?dryRun=All", core_coll()),
        Some(&core_body("dry")),
    )
    .await;
    assert_eq!(code, StatusCode::CREATED, "{out}");
    let (code, _) = send(&s, "GET", &core("dry"), None).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}

/// The events.k8s.io/v1 DELETE and deletecollection are authorised against
/// that group and answer in that schema.
#[tokio::test]
async fn delete_and_delete_collection_work_on_both_versions() {
    let s = TestApiServer::new();
    send(&s, "POST", &v1_coll(), Some(&v1_body("del-v1"))).await;
    let (code, out) = send(&s, "DELETE", &v1("del-v1"), None).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["apiVersion"], json!("events.k8s.io/v1"));
    assert_eq!(out["note"], json!("hello"));

    send(&s, "POST", &core_coll(), Some(&core_body("del-core"))).await;
    let (code, out) = send(&s, "DELETE", &core("del-core"), None).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    assert_eq!(out["apiVersion"], json!("v1"));

    send(&s, "POST", &core_coll(), Some(&core_body("col-a"))).await;
    send(&s, "POST", &v1_coll(), Some(&v1_body("col-b"))).await;
    let (code, out) = send(&s, "DELETE", &v1_coll(), None).await;
    assert_eq!(code, StatusCode::OK, "{out}");
    let (code, _) = send(&s, "GET", &core("col-a"), None).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
    let (code, _) = send(&s, "GET", &core("col-b"), None).await;
    assert_eq!(code, StatusCode::NOT_FOUND);
}
