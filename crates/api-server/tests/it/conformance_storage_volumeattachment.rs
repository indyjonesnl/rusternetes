//! Mirrors of the `VolumeAttachment` conformance cases (#3758).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/volume_attachment.go`
//! - :61  "should run through the lifecycle of a VolumeAttachment"
//! - :184 "should apply changes to a volumeattachment status"
//!
//! Pure API operations. Upstream lists the cluster's nodes and picks one at
//! random for `Spec.NodeName` (:298-310, :190-196); the in-process server has
//! no nodes, so a Node is created first as the stand-in.

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

const VAS: &str = "/apis/storage.k8s.io/v1/volumeattachments";

async fn create_node(api: &TestApiServer) -> String {
    let name = "e2e-node-1";
    let (s, body) = send(
        api,
        "POST",
        "/api/v1/nodes",
        "application/json",
        Some(&json!({"apiVersion": "v1", "kind": "Node", "metadata": {"name": name}})),
    )
    .await;
    assert_eq!(s, 201, "create node: {body}");
    name.to_string()
}

/// `NewVolumeAttachment` (volume_attachment.go:278-296).
fn new_va(va: &str, pv: &str, node: &str, attached: bool) -> Value {
    json!({
        "apiVersion": "storage.k8s.io/v1",
        "kind": "VolumeAttachment",
        "metadata": { "name": va, "uid": va },
        "spec": {
            "attacher": "e2e-test.storage.k8s.io",
            "nodeName": node,
            "source": { "persistentVolumeName": pv },
        },
        "status": { "attached": attached },
    })
}

/// `createVolumeAttachment` (volume_attachment.go:298-318); `rand` stands in
/// for `utilrand.String(5)`.
async fn create_va(api: &TestApiServer, node: &str, rand: &str) -> String {
    let uid = format!("e2e-{rand}");
    let va_name = format!("va-{uid}");
    let (s, created) = send(
        api,
        "POST",
        VAS,
        "application/json",
        Some(&new_va(&va_name, &format!("pv-{uid}"), node, false)),
    )
    .await;
    assert_eq!(
        s, 201,
        "failed to create VolumeAttachment {va_name}: {created}"
    );
    assert_eq!(created["metadata"]["name"], va_name.as_str());
    va_name
}

fn selector(k: &str, v: &str) -> String {
    format!("{VAS}?labelSelector={k}%3D{v}")
}

/// volume_attachment.go:61 "should run through the lifecycle of a
/// VolumeAttachment".
#[tokio::test]
async fn conformance_volumeattachment_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let node = create_node(&api).await;

    // :64 first VA.
    let first = create_va(&api, &node, "aaaaa").await;
    let one = format!("{VAS}/{first}");

    // :66-70 get; name matches, valid RV.
    let (s, retrieved) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to get VolumeAttachment {first}: {retrieved}"
    );
    assert_eq!(retrieved["metadata"]["name"], first.as_str());
    resource_version(&retrieved);

    // :72-78 MERGE patch a label keyed by name; label present, RV grew.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/merge-patch+json",
        Some(&json!({ "metadata": { "labels": { &first: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch {first}: {patched}");
    assert_eq!(
        patched["metadata"]["labels"][&first], "patched",
        "{patched}"
    );
    assert!(resource_version(&retrieved) < resource_version(&patched));

    // :80-83 list by the patched selector; MUST be exactly one.
    let patched_selector = selector(&first, "patched");
    let (s, list) = send(&api, "GET", &patched_selector, "application/json", None).await;
    assert_eq!(s, 200, "failed to list VolumeAttachments: {list}");
    assert_eq!(items(&list), 1, "{list}");

    // :85-87 delete.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "failed to delete VolumeAttachment {first}: {body}");

    // :89-114 confirm deletion: list by the patched selector MUST be empty.
    let (s, list) = send(&api, "GET", &patched_selector, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list),
        0,
        "Expected VolumeAttachment to be deleted: {list}"
    );

    // :116-129 second VA; RetryOnConflict(get, labels={va-e2e: updated},
    // update); label MUST be present. Likewise the third (:131-142).
    let updated_selector = selector("va-e2e", "updated");
    for rand in ["bbbbb", "ccccc"] {
        let va = create_va(&api, &node, rand).await;
        let uri = format!("{VAS}/{va}");
        let (s, mut cur) = send(&api, "GET", &uri, "application/json", None).await;
        assert_eq!(s, 200, "failed to get VolumeAttachment {va}: {cur}");
        cur["metadata"]["labels"] = json!({ "va-e2e": "updated" });
        let (s, updated) = send(&api, "PUT", &uri, "application/json", Some(&cur)).await;
        assert_eq!(s, 200, "failed to update VolumeAttachment {va}: {updated}");
        assert_eq!(
            updated["metadata"]["labels"]["va-e2e"], "updated",
            "{updated}"
        );
    }

    // :144-146 DeleteCollection by the updated selector.
    let (s, body) = send(&api, "DELETE", &updated_selector, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to delete VolumeAttachment collection: {body}"
    );

    // :148-179 confirm: list by the updated selector MUST be empty.
    let (s, list) = send(&api, "GET", &updated_selector, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list),
        0,
        "Expected VolumeAttachment(s) to be deleted: {list}"
    );
}

/// volume_attachment.go:184 "should apply changes to a volumeattachment
/// status".
#[tokio::test]
async fn conformance_volumeattachment_status() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let node = create_node(&api).await;

    // :188-191 names.
    let va_name = "va-e2e-abcde";
    let pv_name = "pv-e2e-abcde";
    let one = format!("{VAS}/{va_name}");
    let status = format!("{one}/status");

    // :198-203 create with Status.Attached=false; name matches.
    let (s, created) = send(
        &api,
        "POST",
        VAS,
        "application/json",
        Some(&new_va(va_name, pv_name, &node, false)),
    )
    .await;
    assert_eq!(
        s, 201,
        "failed to create VolumeAttachment {va_name}: {created}"
    );
    assert_eq!(created["metadata"]["name"], va_name);

    // :205-210 merge-patch a label; label present.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/merge-patch+json",
        Some(&json!({ "metadata": { "labels": { va_name: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch {va_name}: {patched}");
    assert_eq!(
        patched["metadata"]["labels"][va_name], "patched",
        "{patched}"
    );
    let patched_selector = selector(va_name, "patched");

    // :212-220 GET the /status subresource; Status.Attached MUST be false.
    let (s, read) = send(&api, "GET", &status, "application/json", None).await;
    assert_eq!(s, 200, "Failed to fetch the status of {va_name}: {read}");
    assert!(
        !read["status"]["attached"].as_bool().unwrap_or(false),
        "Checking that the VolumeAttachment status has been read: {read}"
    );

    // :222-228 merge-patch /status {"status":{"attached":true}}.
    let (s, vp) = send(
        &api,
        "PATCH",
        &status,
        "application/merge-patch+json",
        Some(&json!({"status": {"attached": true}})),
    )
    .await;
    assert_eq!(s, 200, "Failed to patch status: {vp}");
    assert_eq!(vp["status"]["attached"], true, "{vp}");

    // :230-244 RetryOnConflict(get, Attached=false, UpdateStatus).
    let (s, mut cur) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(
        s, 200,
        "Unable to retrieve VolumeAttachment {va_name}: {cur}"
    );
    cur["status"]["attached"] = json!(false);
    let (s, updated) = send(&api, "PUT", &status, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "Failed to update status: {updated}");
    assert!(
        !updated["status"]["attached"].as_bool().unwrap_or(false),
        "Checking that the VolumeAttachment status has been updated: {updated}"
    );

    // :246-248 delete.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(
        s, 200,
        "failed to delete VolumeAttachment {va_name}: {body}"
    );

    // :250-275 confirm: list by the patched selector MUST be empty.
    let (s, list) = send(&api, "GET", &patched_selector, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list),
        0,
        "Expected VolumeAttachment to be deleted: {list}"
    );
}
