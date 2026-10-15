//! Mirrors of the `CSIInlineVolumes` conformance cases (#3758).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/csi_inline.go`
//! - :52  "should support CSIVolumeSource in Pod API"
//! - :159 "should run through the lifecycle of a CSIDriver"
//!
//! Both are pure API operations. The per-test namespace is created up front
//! with `strict_namespaces()` (`framework.NewDefaultFramework`, :41); the
//! CSIDriver `f.UniqueName` is that namespace's name.

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

/// csi_inline.go:52 "should support CSIVolumeSource in Pod API".
#[tokio::test]
async fn conformance_csi_inline_volume_in_pod_api() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "csiinlinevolumes-mirror";
    let (s, body) = send(
        &api,
        "POST",
        "/api/v1/namespaces",
        "application/json",
        Some(&json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})),
    )
    .await;
    assert_eq!(s, 201, "create namespace: {body}");
    let pods = format!("/api/v1/namespaces/{ns}/pods");

    // :55-109 a pod with two CSI inline volumes (driver "e2e.example.com";
    // the second has fsType ext4 and readOnly).
    let pod_name = "pod-csi-inline-volumes";
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod_name },
        "spec": {
            "volumes": [
                { "name": "csi-inline-vol1", "csi": { "driver": "e2e.example.com" } },
                { "name": "csi-inline-vol2",
                  "csi": { "driver": "e2e.example.com", "fsType": "ext4", "readOnly": true } },
            ],
            "containers": [{
                "name": pod_name,
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "command": ["sh", "-c", "ls /mnt/*"],
                "volumeMounts": [
                    { "name": "csi-inline-vol1", "mountPath": "/mnt/vol1" },
                    { "name": "csi-inline-vol2", "mountPath": "/mnt/vol2" },
                ],
            }],
        },
    });

    // :111-117 create; a second create MUST be 409 AlreadyExists.
    let (s, created) = send(&api, "POST", &pods, "application/json", Some(&pod)).await;
    assert_eq!(s, 201, "{created}");
    let (s, again) = send(&api, "POST", &pods, "application/json", Some(&pod)).await;
    assert_eq!(s, 409, "expected 409, got {s}: {again}");
    assert_eq!(again["reason"], "AlreadyExists", "{again}");

    // :119-122 get; UID MUST match.
    let one = format!("{pods}/{pod_name}");
    let (s, retrieved) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{retrieved}");
    assert_eq!(retrieved["metadata"]["uid"], created["metadata"]["uid"]);
    // The CSI volume sources must round-trip.
    assert_eq!(
        retrieved["spec"]["volumes"][1]["csi"]["fsType"], "ext4",
        "{retrieved}"
    );
    assert_eq!(
        retrieved["spec"]["volumes"][1]["csi"]["readOnly"], true,
        "{retrieved}"
    );

    // :124-127 list in namespace; MUST have 1 item.
    let (s, list) = send(&api, "GET", &pods, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 1, "list should have 1 items, got: {list}");

    // :129-132 merge-patch an annotation.
    let (s, patched) = send(
        &api,
        "PATCH",
        &one,
        "application/merge-patch+json",
        Some(&json!({"metadata": {"annotations": {"patched": "true"}}})),
    )
    .await;
    assert_eq!(s, 200, "{patched}");
    assert_eq!(
        patched["metadata"]["annotations"]["patched"], "true",
        "{patched}"
    );

    // :134-148 delete; the pod MUST then be gone or carry a deletionTimestamp.
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "{body}");
    let (s, after) = send(&api, "GET", &one, "application/json", None).await;
    assert!(
        s == 404 || after["metadata"]["deletionTimestamp"].is_string(),
        "Pod should have been deleted or have DeletionTimestamp, but instead got: {s} {after}"
    );
}

/// csi_inline.go:159 "should run through the lifecycle of a CSIDriver".
#[tokio::test]
async fn conformance_csidriver_lifecycle() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let unique = "csiinlinevolumes-mirror";
    let drivers = "/apis/storage.k8s.io/v1/csidrivers";

    // :161-163 label e2e-test=f.UniqueName and its selector.
    let label_selector = format!("{drivers}?labelSelector=e2e-test%3D{unique}");
    let default_fs_group_policy = "ReadWriteOnceWithFSType";

    // :165-193 driver1 (Ephemeral only), driver2 (Persistent + Ephemeral);
    // names "inline-driver-" + uuid (fixed here).
    let name1 = "inline-driver-11111111-1111-4111-8111-111111111111";
    let name2 = "inline-driver-22222222-2222-4222-8222-222222222222";
    let driver = |name: &str, modes: Value| {
        json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "CSIDriver",
            "metadata": { "name": name, "labels": { "e2e-test": unique } },
            "spec": { "volumeLifecycleModes": modes, "fsGroupPolicy": default_fs_group_policy },
        })
    };
    let driver1 = driver(name1, json!(["Ephemeral"]));
    let driver2 = driver(name2, json!(["Persistent", "Ephemeral"]));

    // :195-205 create both (valid RVs); re-creating driver1 MUST be 409.
    let (s, created1) = send(&api, "POST", drivers, "application/json", Some(&driver1)).await;
    assert_eq!(s, 201, "Failed to create first CSIDriver: {created1}");
    resource_version(&created1);
    let (s, created2) = send(&api, "POST", drivers, "application/json", Some(&driver2)).await;
    assert_eq!(s, 201, "Failed to create second CSIDriver: {created2}");
    resource_version(&created2);
    let (s, again) = send(&api, "POST", drivers, "application/json", Some(&driver1)).await;
    assert_eq!(s, 409, "expected 409, got {s}: {again}");

    // :207-214 get both; UIDs MUST match.
    let one = format!("{drivers}/{name1}");
    let two = format!("{drivers}/{name2}");
    let (s, r1) = send(&api, "GET", &one, "application/json", None).await;
    assert_eq!(s, 200, "{r1}");
    assert_eq!(r1["metadata"]["uid"], created1["metadata"]["uid"]);
    let (s, r2) = send(&api, "GET", &two, "application/json", None).await;
    assert_eq!(s, 200, "{r2}");
    assert_eq!(r2["metadata"]["uid"], created2["metadata"]["uid"]);

    // :216-221 strategic-merge patch driver2's label; RV MUST have grown.
    let (s, patched) = send(
        &api,
        "PATCH",
        &two,
        "application/strategic-merge-patch+json",
        Some(&json!({ "metadata": { "labels": { name2: "patched" } } })),
    )
    .await;
    assert_eq!(s, 200, "failed to patch CSIDriver {name2}: {patched}");
    assert_eq!(patched["metadata"]["labels"][name2], "patched", "{patched}");
    assert!(resource_version(&created2) < resource_version(&patched));

    // :223-235 RetryOnConflict(get, set label, update); label MUST be present.
    let (s, mut cur) = send(&api, "GET", &two, "application/json", None).await;
    assert_eq!(s, 200, "Unable to get CSIDriver {name2}: {cur}");
    cur["metadata"]["labels"][name2] = json!("updated");
    let (s, updated) = send(&api, "PUT", &two, "application/json", Some(&cur)).await;
    assert_eq!(s, 200, "failed to update CSIDriver {name2}: {updated}");
    assert_eq!(updated["metadata"]["labels"][name2], "updated", "{updated}");

    // :237-240 list by the e2e-test label; MUST be 2.
    let (s, list) = send(&api, "GET", &label_selector, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(
        items(&list),
        2,
        "filtered list should have 2 items, got: {list}"
    );

    // :242-257 delete driver1; confirm gone (404 or deletionTimestamp).
    let (s, body) = send(&api, "DELETE", &one, "application/json", None).await;
    assert_eq!(s, 200, "Failed to delete CSIDriver {name1}: {body}");
    let (s, after) = send(&api, "GET", &one, "application/json", None).await;
    assert!(
        s == 404 || after["metadata"]["deletionTimestamp"].is_string(),
        "{s} {after}"
    );

    // :259-275 DeleteCollection with selector "<name2>=updated"; confirm gone.
    let (s, body) = send(
        &api,
        "DELETE",
        &format!("{drivers}?labelSelector={name2}%3Dupdated"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "Failed to delete CSIDriver {name2}: {body}");
    let (s, after) = send(&api, "GET", &two, "application/json", None).await;
    assert!(
        s == 404 || after["metadata"]["deletionTimestamp"].is_string(),
        "{s} {after}"
    );
}
