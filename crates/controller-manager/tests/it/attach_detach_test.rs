//! AttachDetachController (#3048): creates / deletes `VolumeAttachment`s for
//! the CSI volumes of scheduled pods.
//!
//! Upstream: `pkg/volume/csi/csi_attacher.go` (`Attach`, `Detach`,
//! `getAttachmentName`), `pkg/volume/csi/csi_plugin.go` (`CanAttach`,
//! `skipAttach`), `pkg/controller/volume/attachdetach/reconciler/reconciler.go`
//! (`reconcile`, `attachDesiredVolumes`) and `.../util/util.go`
//! (`ProcessPodVolumes`, `getPVCFromCache`, `getPVSpecFromCache`,
//! `IsMultiAttachAllowed`).

use rusternetes_common::resources::csi::VolumeAttachment;
use rusternetes_controller_manager::controllers::attach_detach::{
    attachment_name, AttachDetachController,
};
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const DRIVER: &str = "test.csi.example.com";

async fn put(s: &MemoryStorage, resource: &str, ns: Option<&str>, name: &str, v: Value) {
    s.create(&build_key(resource, ns, name), &v).await.unwrap();
}

async fn vas(s: &MemoryStorage) -> Vec<VolumeAttachment> {
    s.list(&rusternetes_storage::build_prefix(
        "volumeattachments",
        None,
    ))
    .await
    .unwrap()
}

fn pod(name: &str, node: &str, claim: &str, phase: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "ns", "uid": format!("{name}-uid")},
        "spec": {
            "nodeName": node,
            "containers": [{"name": "c", "image": "i"}],
            "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": claim}}]
        },
        "status": {"phase": phase}
    })
}

async fn seed_volume(s: &MemoryStorage, modes: Value) {
    put(
        s,
        "persistentvolumeclaims",
        Some("ns"),
        "claim",
        json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "claim", "namespace": "ns", "uid": "claim-uid"},
            "spec": {"accessModes": modes, "resources": {}, "volumeName": "pv1"},
            "status": {"phase": "Bound"}
        }),
    )
    .await;
    put(
        s,
        "persistentvolumes",
        None,
        "pv1",
        json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": "pv1", "uid": "pv-uid"},
            "spec": {
                "accessModes": modes,
                "capacity": {"storage": "1Gi"},
                "claimRef": {"name": "claim", "namespace": "ns", "uid": "claim-uid"},
                "csi": {"driver": DRIVER, "volumeHandle": "vol-1"}
            },
            "status": {"phase": "Bound"}
        }),
    )
    .await;
}

async fn seed_node(s: &MemoryStorage, name: &str, in_use: Option<&str>) {
    let mut node = json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name}, "spec": {}, "status": {}
    });
    if let Some(v) = in_use {
        node["status"]["volumesInUse"] = json!([v]);
    }
    put(s, "nodes", None, name, node).await;
}

fn controller(s: &Arc<MemoryStorage>) -> AttachDetachController<MemoryStorage> {
    AttachDetachController::new(s.clone())
}

#[test]
fn attachment_name_matches_upstream_get_attachment_name() {
    // csi_attacher.go getAttachmentName: "csi-" + hex(sha256(vol+driver+node)).
    let want = format!(
        "csi-{:x}",
        Sha256::digest(format!("vol-1{DRIVER}node-1").as_bytes())
    );
    assert_eq!(attachment_name("vol-1", DRIVER, "node-1"), want);
}

#[tokio::test]
async fn creates_va_for_scheduled_pod_with_csi_pvc() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        pod("p", "node-1", "claim", "Running"),
    )
    .await;

    controller(&s).reconcile_all().await.unwrap();

    let v = vas(&s).await;
    assert_eq!(v.len(), 1);
    assert_eq!(
        v[0].metadata.name,
        attachment_name("vol-1", DRIVER, "node-1")
    );
    assert_eq!(v[0].spec.attacher, DRIVER);
    assert_eq!(v[0].spec.node_name, "node-1");
    assert_eq!(
        v[0].spec.source.persistent_volume_name.as_deref(),
        Some("pv1")
    );

    // Idempotent: AlreadyExists is tolerated (csi_attacher.go Attach).
    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);
}

#[tokio::test]
async fn skips_driver_with_attach_required_false() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    put(
        &s,
        "csidrivers",
        None,
        DRIVER,
        json!({"apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
               "metadata": {"name": DRIVER}, "spec": {"attachRequired": false}}),
    )
    .await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        pod("p", "node-1", "claim", "Running"),
    )
    .await;

    controller(&s).reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}

#[tokio::test]
async fn ignores_unscheduled_and_terminated_pods() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "u",
        pod("u", "", "claim", "Pending"),
    )
    .await;
    put(
        &s,
        "pods",
        Some("ns"),
        "d",
        pod("d", "node-1", "claim", "Succeeded"),
    )
    .await;

    controller(&s).reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}

#[tokio::test]
async fn deletes_va_when_no_pod_needs_it() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        pod("p", "node-1", "claim", "Running"),
    )
    .await;
    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);

    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();
    controller(&s).reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}

#[tokio::test]
async fn keeps_va_while_volume_is_in_use_by_node() {
    // reconciler.go: "Cannot detach volume because it is still mounted"
    // (attachedVolume.MountedByNode, fed by node.status.volumesInUse).
    let s = Arc::new(MemoryStorage::new());
    seed_node(
        &s,
        "node-1",
        Some(&format!("kubernetes.io/csi/{DRIVER}^vol-1")),
    )
    .await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        pod("p", "node-1", "claim", "Running"),
    )
    .await;
    controller(&s).reconcile_all().await.unwrap();
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();

    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);
}

#[tokio::test]
async fn rwo_volume_is_not_attached_to_a_second_node() {
    // reconciler.go attachDesiredVolumes: !IsMultiAttachAllowed and already
    // attached elsewhere -> reportMultiAttachError, no attach.
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_node(&s, "node-2", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "a",
        pod("a", "node-1", "claim", "Running"),
    )
    .await;
    controller(&s).reconcile_all().await.unwrap();
    put(
        &s,
        "pods",
        Some("ns"),
        "b",
        pod("b", "node-2", "claim", "Running"),
    )
    .await;
    controller(&s).reconcile_all().await.unwrap();

    let v = vas(&s).await;
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].spec.node_name, "node-1");
}

#[tokio::test]
async fn rwx_volume_attaches_to_every_node() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_node(&s, "node-2", None).await;
    seed_volume(&s, json!(["ReadWriteMany"])).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "a",
        pod("a", "node-1", "claim", "Running"),
    )
    .await;
    put(
        &s,
        "pods",
        Some("ns"),
        "b",
        pod("b", "node-2", "claim", "Running"),
    )
    .await;
    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 2);
}

// ---- node.status.volumesAttached (#3053) --------------------------------
// Upstream: `statusupdater/node_status_updater.go` `updateNodeStatus`
// (`node.Status.VolumesAttached = attachedVolumes`, patched via
// PatchNodeStatus) fed by `cache/actual_state_of_world.go`
// `GetVolumesToReportAttached`; `reconciler.go:245-265` removes the volume
// from the report and updates the node BEFORE detaching.

async fn node_attached(s: &MemoryStorage, name: &str) -> Vec<String> {
    let n: rusternetes_common::resources::Node =
        s.get(&build_key("nodes", None, name)).await.unwrap();
    n.status
        .and_then(|s| s.volumes_attached)
        .unwrap_or_default()
        .into_iter()
        .map(|v| v.name)
        .collect()
}

fn unique() -> String {
    format!("kubernetes.io/csi/{DRIVER}^vol-1")
}

async fn seed_pod_p(s: &MemoryStorage) {
    put(
        s,
        "pods",
        Some("ns"),
        "p",
        pod("p", "node-1", "claim", "Running"),
    )
    .await;
}

#[tokio::test]
async fn reports_attached_volume_on_node_status() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    seed_pod_p(&s).await;

    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);

    // Idempotent.
    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);
}

#[tokio::test]
async fn removes_volume_from_node_status_when_detached() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    seed_pod_p(&s).await;
    controller(&s).reconcile_all().await.unwrap();
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();

    controller(&s).reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
    assert!(node_attached(&s, "node-1").await.is_empty());
}

#[tokio::test]
async fn volume_still_mounted_stays_reported_attached() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    seed_pod_p(&s).await;
    controller(&s).reconcile_all().await.unwrap();
    // Kubelet reports the volume mounted, then the pod goes away.
    let key = build_key("nodes", None, "node-1");
    let mut n: rusternetes_common::resources::Node = s.get(&key).await.unwrap();
    n.status.as_mut().unwrap().volumes_in_use = Some(vec![unique()]);
    s.update(&key, &n).await.unwrap();
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();

    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);
}

// ---- force detach (reconciler.go:200-300) ----

const WAIT: std::time::Duration = std::time::Duration::from_millis(50);

/// A node whose volume is still mounted (volumesInUse) with the given Ready
/// status and optional out-of-service taint.
async fn seed_mounted_node(s: &MemoryStorage, ready: &str, taint: bool) {
    let mut node = json!({
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": "node-1"},
        "spec": {},
        "status": {
            "volumesInUse": [unique()],
            "conditions": [{"type": "Ready", "status": ready}]
        }
    });
    if taint {
        node["spec"]["taints"] =
            json!([{"key": "node.kubernetes.io/out-of-service", "effect": "NoExecute"}]);
    }
    put(s, "nodes", None, "node-1", node).await;
}

/// Attach, then delete the pod so the volume becomes undesired.
async fn attach_then_orphan(s: &Arc<MemoryStorage>, c: &AttachDetachController<MemoryStorage>) {
    seed_volume(s, json!(["ReadWriteOnce"])).await;
    seed_pod_p(s).await;
    c.reconcile_all().await.unwrap();
    assert_eq!(vas(s).await.len(), 1);
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();
}

#[tokio::test]
async fn force_detaches_mounted_volume_on_unhealthy_node_after_timeout() {
    // Test_Run_OneVolumeDetachOnUnhealthyNode
    let s = Arc::new(MemoryStorage::new());
    seed_mounted_node(&s, "False", false).await;
    let c = controller(&s).with_max_wait_for_unmount(WAIT);
    attach_then_orphan(&s, &c).await;

    c.reconcile_all().await.unwrap(); // sets detachRequestedTime
    assert_eq!(
        vas(&s).await.len(),
        1,
        "not before maxWaitForUnmountDuration"
    );
    tokio::time::sleep(WAIT * 2).await;
    c.reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}

#[tokio::test]
async fn does_not_force_detach_mounted_volume_on_healthy_node() {
    // "the volume will be detached even after the maxWaitForUnmountDuration
    // expires as volume is not unmounted and the node is healthy" (sic: kept)
    let s = Arc::new(MemoryStorage::new());
    seed_mounted_node(&s, "True", false).await;
    let c = controller(&s).with_max_wait_for_unmount(WAIT);
    attach_then_orphan(&s, &c).await;
    c.reconcile_all().await.unwrap();
    tokio::time::sleep(WAIT * 2).await;
    c.reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);
}

#[tokio::test]
async fn re_desired_volume_resets_detach_request_time() {
    // attachDesiredVolumes: AttachStateAttached => ResetDetachRequestTime.
    let s = Arc::new(MemoryStorage::new());
    seed_mounted_node(&s, "False", false).await;
    let c = controller(&s).with_max_wait_for_unmount(WAIT);
    attach_then_orphan(&s, &c).await;
    c.reconcile_all().await.unwrap();
    tokio::time::sleep(WAIT * 2).await;
    seed_pod_p(&s).await; // wanted again before the next pass
    c.reconcile_all().await.unwrap();
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();
    c.reconcile_all().await.unwrap(); // timer restarts here
    assert_eq!(vas(&s).await.len(), 1);
}

#[tokio::test]
async fn force_detach_timeout_can_be_disabled() {
    // Test_Run_OneVolumeDetachOnUnhealthyNodeWithForceDetachOnUnmountDisabled
    let s = Arc::new(MemoryStorage::new());
    seed_mounted_node(&s, "False", false).await;
    let c = controller(&s)
        .with_max_wait_for_unmount(WAIT)
        .with_disable_force_detach_on_timeout(true);
    attach_then_orphan(&s, &c).await;
    c.reconcile_all().await.unwrap();
    tokio::time::sleep(WAIT * 2).await;
    c.reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);
}

#[tokio::test]
async fn out_of_service_taint_detaches_mounted_volume_immediately() {
    // Test_Run_OneVolumeDetachOnOutOfServiceTaintedNode; the taint applies even
    // when the node is healthy and with force-detach disabled.
    let s = Arc::new(MemoryStorage::new());
    seed_mounted_node(&s, "True", true).await;
    let c = controller(&s).with_disable_force_detach_on_timeout(true);
    attach_then_orphan(&s, &c).await;
    c.reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}
