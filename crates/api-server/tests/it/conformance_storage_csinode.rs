//! Mirror of the `CSINode` lifecycle conformance case (#3753).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/csi_node.go:56`
//! "[sig-storage] CSINodes CSI Conformance should run through the lifecycle
//! of a csinode".
//!
//! A pure API-operation case on a cluster-scoped resource, so the mirror
//! drives the api-server's router step by step in the order the upstream
//! body issues its client calls. `utilrand.String(5)` name suffixes are fixed.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const CSINODES: &str = "/apis/storage.k8s.io/v1/csinodes";

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

/// `apimachineryutils.HaveValidResourceVersion`
/// (test/e2e/common/apimachinery/resourceversion_matchers.go:39-54).
fn resource_version(obj: &Value) -> u128 {
    let rv = obj["metadata"]["resourceVersion"]
        .as_str()
        .unwrap_or_else(|| panic!("object has no resourceVersion: {obj}"));
    rv.parse::<u128>()
        .unwrap_or_else(|_| panic!("resourceVersion {rv:?} is not a valid integer"))
}

fn items(list: &Value) -> usize {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
        .len()
}

/// `labels.Set{k: v}.AsSelector().String()`, URL-encoded.
fn selector(k: &str, v: &str) -> String {
    format!("{CSINODES}?labelSelector={k}%3D{v}")
}

/// csi_node.go:56 "should run through the lifecycle of a csinode".
#[tokio::test]
async fn conformance_csinode_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();

    // :60-64 initialCSINode "e2e-csinode-" + utilrand.String(5).
    let name = "e2e-csinode-abcde";
    let one = format!("{CSINODES}/{name}");

    // :66-69 create; resourceVersion MUST be valid.
    let (s, csi_node) = send(
        &api,
        "POST",
        CSINODES,
        "application/json",
        Some(&json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "CSINode",
            "metadata": { "name": name },
            // `Spec.Drivers` is a non-omitempty slice: Go marshals the
            // zero value as `null`.
            "spec": { "drivers": null },
        })),
    )
    .await;
    assert_eq!(s, 201, "failed to create csiNode {name}: {csi_node}");
    let created_rv = resource_version(&csi_node);

    // :71-74 get; name MUST match.
    let (s, retrieved) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to retrieve csiNode {name}: {retrieved}");
    assert_eq!(retrieved["metadata"]["name"], csi_node["metadata"]["name"]);

    // :76-82 strategic-merge patch a label keyed by the node's name; the
    // label MUST be present and the RV MUST have grown.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { name: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "Failed to patch csiNode {name}: {patched}");
    assert_eq!(
        patched["metadata"]["labels"][name], "patched",
        "Checking that patched label has been applied: {patched}"
    );
    assert!(
        created_rv < resource_version(&patched),
        "patched object should have a larger resource version: {created_rv} vs {patched}"
    );

    // :84-87 list by the patched selector; MUST return exactly one.
    let patched_selector = selector(name, "patched");
    let (s, list) = send(&api, "GET", &patched_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list csiNodes: {list}");
    assert_eq!(items(&list), 1, "{list}");

    // :89-91 delete.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete csiNode {name}: {body}");

    // :93-120 confirm deletion: a list by the patched selector MUST be empty
    // (upstream polls for 30s; the in-process store is synchronous).
    let (s, list) = send(&api, "GET", &patched_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list CSINode: {list}");
    assert_eq!(items(&list), 0, "Expected CSINode to be deleted: {list}");

    // :117-121 replacementCSINode with a fresh random name.
    let name2 = "e2e-csinode-fghij";
    let two = format!("{CSINODES}/{name2}");

    // :123-125 create the replacement.
    let (s, second) = send(
        &api,
        "POST",
        CSINODES,
        "application/json",
        Some(&json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "CSINode",
            "metadata": { "name": name2 },
            "spec": { "drivers": null },
        })),
    )
    .await;
    assert_eq!(s, 201, "failed to create csiNode {name2}: {second}");

    // :127-130 get; name MUST match.
    let (s, retrieved) = send(&api, "GET", &two, "application/json", None).await;
    assert_eq!(s, 200, "Failed to retrieve CSINode {name2}: {retrieved}");
    assert_eq!(retrieved["metadata"]["name"], second["metadata"]["name"]);

    // :132-145 RetryOnConflict(get, replace labels, update); the updated
    // label MUST be present.
    let (s, mut tmp) = send(&api, "GET", &two, "application/json", None).await;
    assert_eq!(s, 200, "Unable to get {name2}: {tmp}");
    tmp["metadata"]["labels"] = json!({ name2: "updated" });
    let (s, updated) = send(&api, "PUT", &two, "application/json", Some(&tmp)).await;
    assert_eq!(s, 200, "failed to update {name2}: {updated}");
    assert_eq!(
        updated["metadata"]["labels"][name2], "updated",
        "Checking that updated label has been applied: {updated}"
    );

    // :147-149 DeleteCollection by the updated selector.
    let updated_selector = selector(name2, "updated");
    let (s, body) = send(&api, "DELETE", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete csiNode Colllection: {body}");

    // :151-174 confirm deletion: a list by the updated selector MUST be empty.
    let (s, list) = send(&api, "GET", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list CSINode: {list}");
    assert_eq!(items(&list), 0, "Expected CSINode to be deleted: {list}");
}
