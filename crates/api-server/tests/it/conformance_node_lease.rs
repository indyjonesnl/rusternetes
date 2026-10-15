//! Mirror of the `Lease` conformance case (#3753).
//!
//! Upstream (`release-1.35`): `test/e2e/common/node/lease.go:75`
//! "lease API should be available".
//!
//! A pure API-operation case, so the mirror drives the api-server's router
//! step by step in the order the upstream body issues its client calls. The
//! e2e framework's per-test namespace (`f.Namespace`) is created up front with
//! `strict_namespaces()`, so nothing is auto-seeded.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

async fn send(
    api: &TestApiServer,
    method: &str,
    uri: &str,
    ct: &str,
    body: Option<&Value>,
) -> (u16, Value) {
    let (status, value) = api.send(method, uri, Some(ct), body).await;
    (status.as_u16(), value)
}

/// A `LeaseSpec` as upstream builds it. `AcquireTime`/`RenewTime` are
/// `metav1.MicroTime{Time: time.Time{}.Add(n * time.Second)}`, which marshal
/// as RFC3339 with microseconds (`metav1.RFC3339Micro`, micro_time.go).
fn lease_spec(
    holder: &str,
    duration: i32,
    acquire_s: u32,
    renew_s: u32,
    transitions: i32,
) -> Value {
    json!({
        "holderIdentity": holder,
        "leaseDurationSeconds": duration,
        "acquireTime": format!("0001-01-01T00:00:{acquire_s:02}.000000Z"),
        "renewTime": format!("0001-01-01T00:{:02}:{:02}.000000Z", renew_s / 60, renew_s % 60),
        "leaseTransitions": transitions,
    })
}

/// `apimachineryutils.HaveValidResourceVersion`
/// (test/e2e/common/apimachinery/resourceversion_matchers.go:39-54).
fn resource_version(obj: &Value) -> u128 {
    let rv = obj["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("object has no resourceVersion: {obj}"));
    rv.parse::<u128>()
        .unwrap_or_else(|_| panic!("resourceVersion {rv:?} is not a valid integer"))
}

/// `apiequality.Semantic.DeepEqual(expected.Spec, read.Spec)`.
fn assert_spec(expected: &Value, read: &Value) {
    assert_eq!(
        &read["spec"], expected,
        "Leases don't match (expected spec vs read object): {read}"
    );
}

fn items(list: &Value) -> usize {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
        .len()
}

/// lease.go:75 "lease API should be available".
#[tokio::test]
async fn conformance_lease_api_should_be_available() {
    let api = TestApiServer::builder().strict_namespaces().build();
    // framework.NewDefaultFramework("lease-test") (lease.go:59).
    let ns = "lease-test-mirror";
    let (s, body) = send(
        &api,
        "POST",
        "/api/v1/namespaces",
        "application/json",
        Some(&json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})),
    )
    .await;
    assert_eq!(s, 201, "create namespace: {body}");
    let collection = format!("/apis/coordination.k8s.io/v1/namespaces/{ns}/leases");
    let one = format!("{collection}/lease");

    // :76-93 create "lease"; resourceVersion MUST be valid.
    let spec1 = lease_spec("holder", 30, 2, 5, 0);
    let (s, created) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": "lease" },
            "spec": spec1,
        })),
    )
    .await;
    assert_eq!(s, 201, "creating Lease failed: {created}");
    let created_rv = resource_version(&created);

    // :95-99 get it; spec MUST match.
    let (s, read) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "couldn't read Lease: {read}");
    assert_spec(&spec1, &read);

    // :101-111 update createdLease with a new spec.
    let spec2 = lease_spec("holder2", 30, 20, 50, 1);
    let mut update = created.clone();
    update["spec"] = spec2.clone();
    let (s, body) = send(&api, "PUT", &one, "application/json", Some(&update)).await;
    assert_eq!(s, 200, "updating Lease failed: {body}");

    // :113-117 get it; spec MUST match the update.
    let (s, read) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "couldn't read Lease: {read}");
    assert_spec(&spec2, &read);

    // :119-130 strategic-merge patch built by getPatchBytes (:42-56,
    // CreateTwoWayMergePatch of readLease vs. a copy with only Spec changed).
    // LeaseSpec fields carry no patch strategy, so the patch is every
    // changed spec field — here all five.
    let spec3 = lease_spec("holder3", 60, 50, 70, 2);
    let (s, body) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "spec": spec3 })),
    )
    .await;
    assert_eq!(s, 200, "patching Lease failed: {body}");

    // :132-137 get it; spec MUST match the patch and the RV MUST have grown
    // past createdLease's.
    let (s, read) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "couldn't read Lease: {read}");
    assert_spec(&spec3, &read);
    assert!(
        created_rv < resource_version(&read),
        "patched object should have a larger resource version: {created_rv} vs {read}"
    );

    // :139-154 create "lease2" carrying the deletecollection label.
    let (s, body) = send(
        &api,
        "POST",
        &collection,
        "application/json",
        Some(&json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": "lease2", "labels": { "deletecollection": "true" } },
            "spec": lease_spec("holder", 30, 2, 5, 0),
        })),
    )
    .await;
    assert_eq!(s, 201, "creating Lease failed: {body}");

    // :156-158 list; MUST return both.
    let (s, list) = send(&api, "GET", &collection, "application/json", None).await;
    assert_eq!(s, 200, "couldn't list Leases: {list}");
    assert_eq!(items(&list), 2, "{list}");

    // :160-162 DeleteCollection by label; MUST delete only lease2.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{collection}?labelSelector=deletecollection%3Dtrue"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "couldn't delete collection: {body}");

    // :164-166 list; MUST return just the remaining lease.
    let (s, list) = send(&api, "GET", &collection, "application/json", None).await;
    assert_eq!(s, 200, "couldn't list Leases: {list}");
    assert_eq!(items(&list), 1, "{list}");
    assert_eq!(list["items"][0]["metadata"]["name"], "lease", "{list}");

    // :168-169 delete "lease".
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "deleting Lease failed: {body}");

    // :171-174 get MUST be NotFound.
    let (s, body) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "expected IsNotFound error, got {body}");
    assert_eq!(body["reason"], "NotFound", "{body}");

    // :176-182 list across all namespaces MUST succeed.
    let (s, body) = send(
        &api,
        "GET",
        "/apis/coordination.k8s.io/v1/leases",
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "couldn't list Leases from all namespace: {body}");
}
