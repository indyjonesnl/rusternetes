//! Mirror of the `VolumeAttributesClass` lifecycle conformance case (#3753).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/volumeattributesclass.go:51`
//! "[sig-storage] VolumeAttributesClass should run through the lifecycle of a
//! VolumeAttributesClass".
//!
//! A pure API-operation case on a cluster-scoped resource, so the mirror
//! drives the api-server's router step by step in the order the upstream
//! body issues its client calls. Names come from `generateName`, so each
//! step uses the name the server returned.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const VACS: &str = "/apis/storage.k8s.io/v1/volumeattributesclasses";

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

fn items(list: &Value) -> usize {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
        .len()
}

/// `labels.Set{k: v}.AsSelector().String()`, URL-encoded.
fn selector(k: &str, v: &str) -> String {
    format!("{VACS}?labelSelector={k}%3D{v}")
}

/// The VAC upstream builds at :56-67 / :114-125. client-go sends the
/// `TypeMeta` as set (Kind only; the typed client fills apiVersion).
fn vac(generate_name: &str) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttributesClass",
        "metadata": { "generateName": generate_name },
        "driverName": "e2e-fake-csi-driver",
        "parameters": { "foo": "bar" },
    })
}

/// Stands in for upstream's `vacprotection` controller
/// (pkg/controller/volume/vacprotection/vac_protection_controller.go
/// `processVAC`), which the api-server-only harness does not run. The
/// `StorageObjectInUseProtection` plugin adds `kubernetes.io/vac-protection`
/// on create (admission.go `admitVAC`), so a delete only sets
/// `deletionTimestamp`; the controller then drops the finalizer once no PV or
/// PVC references the class (none do here). Upstream's e2e body absorbs that
/// asynchrony by polling for 30s (volumeattributesclass.go:87-110, :153-173).
async fn reap_protection(api: &TestApiServer, uri: &str) {
    let (s, mut obj) = send(api, "GET", uri, "application/json", None).await;
    if s != 200 || obj["metadata"]["deletionTimestamp"].is_null() {
        return;
    }
    assert_eq!(
        obj["metadata"]["finalizers"],
        json!(["kubernetes.io/vac-protection"]),
        "a terminating VAC holds only the protection finalizer: {obj}"
    );
    obj["metadata"]["finalizers"] = json!([]);
    let (s, body) = send(api, "PUT", uri, "application/json", Some(&obj)).await;
    assert_eq!(s, 200, "failed to remove vac-protection: {body}");
}

fn name_of(obj: &Value) -> String {
    obj["metadata"]["name"]
        .as_str()
        .unwrap_or_else(|| panic!("object has no name: {obj}"))
        .to_string()
}

/// volumeattributesclass.go:51 "should run through the lifecycle of a
/// VolumeAttributesClass".
#[tokio::test]
async fn conformance_volumeattributesclass_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();

    // :69-71 create from generateName "e2e-".
    let (s, created) = send(&api, "POST", VACS, "application/json", Some(&vac("e2e-"))).await;
    assert_eq!(
        s, 201,
        "failed to create the requested VolumeAttributesClass: {created}"
    );
    let name = name_of(&created);
    assert!(name.starts_with("e2e-") && name.len() > 4, "{created}");
    let one = format!("{VACS}/{name}");

    // :73-75 get.
    let (s, retrieved) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to get VolumeAttributesClass {name}: {retrieved}"
    );

    // :77-81 strategic-merge patch a label keyed by the VAC's name; the
    // label MUST be present.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { name.as_str(): "patched" } } })),
    )
    .await;
    assert_eq!(
        s, 200,
        "failed to patch VolumeAttributesClass {name}: {patched}"
    );
    assert_eq!(
        patched["metadata"]["labels"][name.as_str()],
        "patched",
        "checking that patched label has been applied: {patched}"
    );

    // :83-85 delete.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to delete VolumeAttributesClass {name}: {body}"
    );

    // :87-110 confirm deletion: a list by the patched selector MUST be empty
    // (upstream polls for 30s while vacprotection removes the finalizer).
    reap_protection(&api, &one).await;
    let (s, list) = send(
        &api,
        "GET",
        &selector(&name, "patched"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list VolumeAttributesClass: {list}");
    assert_eq!(
        items(&list),
        0,
        "expected VolumeAttributesClass to be deleted: {list}"
    );

    // :112-128 create a replacement from generateName "e2e-v2-".
    let (s, replacement) = send(
        &api,
        "POST",
        VACS,
        "application/json",
        Some(&vac("e2e-v2-")),
    )
    .await;
    assert_eq!(
        s, 201,
        "failed to create replacement VolumeAttributesClass: {replacement}"
    );
    let name2 = name_of(&replacement);
    assert!(name2.starts_with("e2e-v2-"), "{replacement}");
    let two = format!("{VACS}/{name2}");

    // :130-142 RetryOnConflict(get, replace labels, update); the updated
    // label MUST be present.
    let (s, mut tmp) = send(&api, "GET", &two, "application/json", None).await;
    assert_eq!(s, 200, "unable to get VolumeAttributesClass {name2}: {tmp}");
    tmp["metadata"]["labels"] = json!({ name2.as_str(): "updated" });
    let (s, updated) = send(&api, "PUT", &two, "application/json", Some(&tmp)).await;
    assert_eq!(
        s, 200,
        "failed to update VolumeAttributesClass {name2}: {updated}"
    );
    assert_eq!(
        updated["metadata"]["labels"][name2.as_str()],
        "updated",
        "checking that updated label has been applied: {updated}"
    );

    // :143-147 list by the updated selector; MUST return exactly one.
    let updated_selector = selector(&name2, "updated");
    let (s, list) = send(&api, "GET", &updated_selector, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to list VolumeAttributesClasses with the labelSelector: {list}"
    );
    assert_eq!(items(&list), 1, "{list}");

    // :149-151 DeleteCollection by the updated selector.
    let (s, body) = send(&api, "DELETE", &updated_selector, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to delete VolumeAttributesClass {name2}: {body}"
    );

    // :153-173 confirm deletion: a list by the updated selector MUST be empty.
    reap_protection(&api, &two).await;
    let (s, list) = send(&api, "GET", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list VolumeAttributesClass: {list}");
    assert_eq!(
        items(&list),
        0,
        "expected VolumeAttributesClass to be deleted: {list}"
    );
}

/// Regression for #3692: `StorageObjectInUseProtection.admitVAC` adds the
/// finalizer on create, so delete is graceful until the controller reaps it.
#[tokio::test]
async fn volumeattributesclass_create_adds_protection_finalizer() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let (s, created) = send(&api, "POST", VACS, "application/json", Some(&vac("e2e-"))).await;
    assert_eq!(s, 201, "{created}");
    assert_eq!(
        created["metadata"]["finalizers"],
        json!(["kubernetes.io/vac-protection"]),
        "{created}"
    );
    let one = format!("{VACS}/{}", name_of(&created));
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "{body}");
    let (s, got) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "finalizer must hold the class: {got}");
    assert!(!got["metadata"]["deletionTimestamp"].is_null(), "{got}");
    reap_protection(&api, &one).await;
    let (s, got) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 404, "{got}");
}
