//! Mirror of the `PersistentVolumes` status conformance case (#3758).
//!
//! Upstream (`release-1.35`): `test/e2e/storage/persistent_volumes.go:693`
//! "[sig-storage] PersistentVolumes CSI Conformance should apply changes to a
//! pv/pvc status". The per-test namespace (`f.Namespace`, `ns`) is created up
//! front with `strict_namespaces()`. `CreatePVsPVCs` (framework/pv/pv.go:424)
//! creates an unbound PV then a PVC; no controller runs in-process, so the
//! phases read back are Available/Pending, both accepted by the case.

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

fn items(list: &Value) -> usize {
    list["items"]
        .as_array()
        .unwrap_or_else(|| panic!("list has no items array: {list}"))
        .len()
}

/// persistent_volumes.go:693 "should apply changes to a pv/pvc status".
#[tokio::test]
async fn conformance_pv_pvc_status() {
    let api = TestApiServer::builder().strict_namespaces().build();
    let ns = "pv-mirror";
    let (s, body) = send(
        &api,
        "POST",
        "/api/v1/namespaces",
        "application/json",
        Some(&json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})),
    )
    .await;
    assert_eq!(s, 201, "create namespace: {body}");
    let pvs = "/api/v1/persistentvolumes";
    let pvcs = format!("/api/v1/namespaces/{ns}/persistentvolumeclaims");

    // :698-718 initial PV (GenerateName "<ns>-", label
    // e2epv.VolumeSelectorKey=ns, StorageClassName ns, CSI source, 2Gi RWO,
    // Retain; pv.go:576-616) and PVC (StorageClassName ns, 2Gi RWO;
    // pv.go:631-660).
    let (s, pv) = send(
        &api,
        "POST",
        pvs,
        "application/json",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {
                "generateName": format!("{ns}-"),
                "labels": { "e2e-pv-pool": ns },
                "annotations": { "pv.beta.kubernetes.io/gid": "777" },
            },
            "spec": {
                "persistentVolumeReclaimPolicy": "Retain",
                "capacity": { "storage": "2Gi" },
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": ns,
                "csi": {
                    "driver": "e2e-driver-11111111-1111-4111-8111-111111111111",
                    "volumeHandle": "e2e-status-conformance",
                },
            },
        })),
    )
    .await;
    assert_eq!(s, 201, "Failed to create the requested PV: {pv}");
    let (s, pvc) = send(
        &api,
        "POST",
        &pvcs,
        "application/json",
        Some(&json!({
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": { "generateName": "pvc-" },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": ns,
                "resources": { "requests": { "storage": "2Gi" } },
            },
        })),
    )
    .await;
    assert_eq!(s, 201, "Failed to create the requested PVC: {pvc}");

    // :720-724 list PVs by the label selector; MUST be exactly one.
    let (s, list) = send(
        &api,
        "GET",
        &format!("{pvs}?labelSelector=e2e-pv-pool%3D{ns}"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 1, "{list}");
    let pv_name = list["items"][0]["metadata"]["name"]
        .as_str()
        .unwrap()
        .to_string();

    // :726-730 list PVCs in the namespace; MUST be exactly one.
    let (s, list) = send(&api, "GET", &pvcs, "application/json", None).await;
    assert_eq!(s, 200, "{list}");
    assert_eq!(items(&list), 1, "{list}");
    let pvc_name = list["items"][0]["metadata"]["name"]
        .as_str()
        .unwrap()
        .to_string();
    let pvc_uri = format!("{pvcs}/{pvc_name}");
    let pv_uri = format!("{pvs}/{pv_name}");

    // :732-739 GET the PVC /status; phase MUST be Pending or Bound.
    let (s, got) = send(
        &api,
        "GET",
        &format!("{pvc_uri}/status"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(
        s, 200,
        "Failed to fetch the status of PVC {pvc_name}: {got}"
    );
    let phase = got["status"]["phase"].as_str().unwrap_or("");
    assert!(
        ["Pending", "Bound"].contains(&phase),
        "PVC phase {phase:?}: {got}"
    );

    // :741-748 GET the PV /status; phase MUST be Available, Bound or Pending.
    let (s, got) = send(
        &api,
        "GET",
        &format!("{pv_uri}/status"),
        "application/json",
        None,
    )
    .await;
    assert_eq!(s, 200, "Failed to fetch the status of PV {pv_name}: {got}");
    let phase = got["status"]["phase"].as_str().unwrap_or("");
    assert!(
        ["Available", "Bound", "Pending"].contains(&phase),
        "PV phase {phase:?}: {got}"
    );

    // :750-761 merge-patch the PVC /status with a StatusPatched condition.
    let (s, patched_pvc) = send(
        &api,
        "PATCH",
        &format!("{pvc_uri}/status"),
        "application/merge-patch+json",
        Some(&json!({"status": {"conditions": [{
            "type": "StatusPatched", "status": "True",
            "reason": "E2E patchedStatus", "message": "Set from e2e test"
        }]}})),
    )
    .await;
    assert_eq!(s, 200, "Failed to patch status: {patched_pvc}");
    let cond = |obj: &Value, t: &str| {
        obj["status"]["conditions"]
            .as_array()
            .and_then(|c| c.iter().find(|c| c["type"] == t).cloned())
            .unwrap_or_else(|| panic!("no {t} condition: {obj}"))
    };
    let c = cond(&patched_pvc, "StatusPatched");
    assert!(
        c["message"].as_str().unwrap().contains("Set from e2e test"),
        "{c}"
    );
    assert!(
        c["reason"].as_str().unwrap().contains("E2E patchedStatus"),
        "{c}"
    );

    // :763-769 merge-patch the PV /status with reason/message.
    let (s, patched_pv) = send(
        &api,
        "PATCH",
        &format!("{pv_uri}/status"),
        "application/merge-patch+json",
        Some(&json!({"status": {"message": "StatusPatched", "reason": "E2E patchStatus"}})),
    )
    .await;
    assert_eq!(s, 200, "Failed to patch {pv_name} status: {patched_pv}");
    assert_eq!(
        patched_pv["status"]["reason"], "E2E patchStatus",
        "{patched_pv}"
    );
    assert_eq!(
        patched_pv["status"]["message"], "StatusPatched",
        "{patched_pv}"
    );

    // :771-794 RetryOnConflict(get, append StatusUpdated condition,
    // UpdateStatus on the PVC).
    let (s, mut cur) = send(&api, "GET", &pvc_uri, "application/json", None).await;
    assert_eq!(s, 200, "Unable to retrieve pvc {pvc_name}: {cur}");
    let mut conds = cur["status"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    conds.push(json!({
        "type": "StatusUpdated", "status": "True",
        "reason": "E2E updateStatus", "message": "Set from e2e test"
    }));
    cur["status"]["conditions"] = json!(conds);
    let (s, updated_pvc) = send(
        &api,
        "PUT",
        &format!("{pvc_uri}/status"),
        "application/json",
        Some(&cur),
    )
    .await;
    assert_eq!(s, 200, "Failed to update status: {updated_pvc}");
    let c = cond(&updated_pvc, "StatusUpdated");
    assert!(
        c["message"].as_str().unwrap().contains("Set from e2e test"),
        "{c}"
    );
    assert!(
        c["reason"].as_str().unwrap().contains("E2E updateStatus"),
        "{c}"
    );

    // :796-809 RetryOnConflict(get, set reason/message, UpdateStatus on the PV).
    let (s, mut cur) = send(&api, "GET", &pv_uri, "application/json", None).await;
    assert_eq!(s, 200, "Unable to retrieve pv {pv_name}: {cur}");
    cur["status"]["reason"] = json!("E2E updateStatus");
    cur["status"]["message"] = json!("StatusUpdated");
    let (s, updated_pv) = send(
        &api,
        "PUT",
        &format!("{pv_uri}/status"),
        "application/json",
        Some(&cur),
    )
    .await;
    assert_eq!(s, 200, "Failed to update status: {updated_pv}");
    assert_eq!(
        updated_pv["status"]["reason"], "E2E updateStatus",
        "{updated_pv}"
    );
    assert_eq!(
        updated_pv["status"]["message"], "StatusUpdated",
        "{updated_pv}"
    );
}
