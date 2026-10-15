//! Mirror of the `StorageClass` lifecycle conformance case (#3758).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/storageclass.go:55`
//! "[sig-storage] CSI Conformance should run through the lifecycle of a
//! StorageClass". Cluster-scoped, so every step is an HTTP call in the order
//! the upstream body issues its client calls. `GenerateName` is exercised as
//! upstream does; the 30s `Eventually` polls collapse to one list because the
//! in-process store is synchronous.

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

const SCS: &str = "/apis/storage.k8s.io/v1/storageclasses";

fn selector(k: &str, v: &str) -> String {
    format!("{SCS}?labelSelector={k}%3D{v}")
}

/// storageclass.go:55 "should run through the lifecycle of a StorageClass".
#[tokio::test]
async fn conformance_storageclass_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();

    // :57-68 initialSC: GenerateName "e2e-", Provisioner "e2e-fake-provisioner".
    // :70-73 create; resourceVersion MUST be valid.
    let (s, created) = send(
        &api,
        "POST",
        SCS,
        "application/json",
        Some(&json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": { "generateName": "e2e-" },
            "provisioner": "e2e-fake-provisioner",
        })),
    )
    .await;
    assert_eq!(
        s, 201,
        "failed to create the requested StorageClass: {created}"
    );
    let name = created["metadata"]["name"]
        .as_str()
        .expect("generated name")
        .to_string();
    assert!(name.starts_with("e2e-"), "{name}");
    resource_version(&created);
    let one = format!("{SCS}/{name}");

    // :75-77 get.
    let (s, retrieved) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to get StorageClass {name}: {retrieved}");

    // :79-84 strategic-merge patch a label keyed by the name; label MUST be
    // present and the RV MUST have grown.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { &name: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch StorageClass {name}: {patched}");
    assert_eq!(patched["metadata"]["labels"][&name], "patched", "{patched}");
    assert!(
        resource_version(&retrieved) < resource_version(&patched),
        "patched object should have a larger resource version"
    );

    // :86-88 delete.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete StorageClass {name}: {body}");

    // :90-113 confirm deletion: list by the patched selector MUST be empty.
    let (s, list) = send(
        &api,
        "GET",
        &selector(&name, "patched"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "failed to list StorageClass: {list}");
    assert_eq!(
        items(&list),
        0,
        "expected StorageClass to be deleted: {list}"
    );

    // :115-128 replacement: GenerateName "e2e-v2-"; create.
    let (s, second) = send(
        &api,
        "POST",
        SCS,
        "application/json",
        Some(&json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": { "generateName": "e2e-v2-" },
            "provisioner": "e2e-fake-provisioner",
        })),
    )
    .await;
    assert_eq!(
        s, 201,
        "failed to create replacement StorageClass: {second}"
    );
    let name2 = second["metadata"]["name"]
        .as_str()
        .expect("generated name")
        .to_string();
    assert!(name2.starts_with("e2e-v2-"), "{name2}");
    let two = format!("{SCS}/{name2}");

    // :130-142 RetryOnConflict(get, replace labels, update); label MUST be
    // present.
    let (s, mut sc) = send(&api, "GET", &two, "application/json", None).await;
    assert_eq!(s, 200, "unable to get Storage {name2}: {sc}");
    sc["metadata"]["labels"] = json!({ &name2: "updated" });
    let (s, updated) = send(&api, "PUT", &two, "application/json", Some(&sc)).await;
    assert_eq!(s, 200, "failed to update StorageClass {name2}: {updated}");
    assert_eq!(
        updated["metadata"]["labels"][&name2], "updated",
        "{updated}"
    );

    // :145-148 list by the updated selector; MUST be exactly one.
    let updated_selector = selector(&name2, "updated");
    let (s, list) = send(&api, "GET", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list StorageClasses: {list}");
    assert_eq!(items(&list), 1, "{list}");

    // :150-152 DeleteCollection by the updated selector.
    let (s, body) = send(&api, "DELETE", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete StorageClass {name2}: {body}");

    // :154-170 confirm deletion: list by the updated selector MUST be empty.
    let (s, list) = send(&api, "GET", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list StorageClass: {list}");
    assert_eq!(
        items(&list),
        0,
        "expected StorageClass to be deleted: {list}"
    );
}
