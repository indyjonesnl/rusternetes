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

// ---- reportMultiAttachError (reconciler.go:376, Test_ReportMultiAttachError
// reconciler_test.go:1279) ----

async fn pod_events(s: &MemoryStorage, pod: &str) -> Vec<(String, String, String)> {
    let events: Vec<rusternetes_common::resources::Event> = s
        .list(&rusternetes_storage::build_prefix("events", Some("ns")))
        .await
        .unwrap_or_default();
    events
        .into_iter()
        .filter(|e| e.involved_object.name.as_deref() == Some(pod))
        .map(|e| (format!("{:?}", e.event_type), e.reason, e.message))
        .collect()
}

/// pod `a` holds the RWO volume on node-1; `b` (node-2) is blocked.
async fn blocked_setup(s: &Arc<MemoryStorage>, keep_a: bool) {
    seed_node(s, "node-1", Some(&unique())).await;
    seed_node(s, "node-2", None).await;
    seed_volume(s, json!(["ReadWriteOnce"])).await;
    put(
        s,
        "pods",
        Some("ns"),
        "a",
        pod("a", "node-1", "claim", "Running"),
    )
    .await;
    controller(s).reconcile_all().await.unwrap();
    if !keep_a {
        // The pod is gone but the node still reports the volume in use, so the
        // attachment stays.
        s.delete(&build_key("pods", Some("ns"), "a")).await.unwrap();
    }
    put(
        s,
        "pods",
        Some("ns"),
        "b",
        pod("b", "node-2", "claim", "Running"),
    )
    .await;
}

#[tokio::test]
async fn multi_attach_event_names_the_pod_holding_the_volume() {
    let s = Arc::new(MemoryStorage::new());
    blocked_setup(&s, true).await;
    controller(&s).reconcile_all().await.unwrap();
    let ev = pod_events(&s, "b").await;
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0].1, "FailedAttachVolume");
    assert_eq!(
        ev[0].2,
        "Multi-Attach error for volume \"pv1\" Volume is already used by pod(s) a"
    );
}

#[tokio::test]
async fn multi_attach_event_without_holder_pods_is_the_simple_message() {
    let s = Arc::new(MemoryStorage::new());
    blocked_setup(&s, false).await;
    controller(&s).reconcile_all().await.unwrap();
    let ev = pod_events(&s, "b").await;
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(
        ev[0].2,
        "Multi-Attach error for volume \"pv1\" Volume is already exclusively attached to one node and can't be attached to another"
    );
}

#[tokio::test]
async fn multi_attach_error_is_reported_once_per_volume_and_node() {
    // `if !volumeToAttach.MultiAttachErrorReported` (reconciler.go:352).
    let s = Arc::new(MemoryStorage::new());
    blocked_setup(&s, true).await;
    let c = controller(&s);
    c.reconcile_all().await.unwrap();
    c.reconcile_all().await.unwrap();
    assert_eq!(pod_events(&s, "b").await.len(), 1);
}

// ---- CSI migration (#3801): TranslateInTreeSpecToCSI callers ----
//
// Upstream: `attachdetach/util/util.go` `CreateVolumeSpecWithNodeMigration`
// (`:108`) / `translateInTreeSpecToCSIOnNodeIfNeeded` (`:251`),
// `attach_detach_controller.go` `populateActualStateOfWorld` (`:722-735`),
// `csi_attacher.go` `Attach` (`:84-103`, inline source).

const GCE_DRIVER: &str = "pd.csi.storage.gke.io";

fn gce_pv_json() -> Value {
    json!({
        "apiVersion": "v1", "kind": "PersistentVolume",
        "metadata": {"name": "gce-pv", "uid": "gce-pv-uid"},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "capacity": {"storage": "1Gi"},
            "claimRef": {"name": "claim", "namespace": "ns", "uid": "claim-uid"},
            "gcePersistentDisk": {"pdName": "pd-1", "fsType": "ext4"}
        },
        "status": {"phase": "Bound"}
    })
}

/// The volume handle the shared translator produces for the GCE PV.
fn translated_gce_pv_handle() -> String {
    use rusternetes_common::csi_translation::CsiTranslator;
    let pv = serde_json::from_value(gce_pv_json()).unwrap();
    let csi = CsiTranslator::new()
        .translate_in_tree_pv_to_csi(&pv)
        .unwrap()
        .spec
        .csi
        .unwrap();
    assert_eq!(csi.driver, GCE_DRIVER);
    csi.volume_handle.unwrap()
}

async fn seed_claim(s: &MemoryStorage, volume: &str) {
    put(
        s,
        "persistentvolumeclaims",
        Some("ns"),
        "claim",
        json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "claim", "namespace": "ns", "uid": "claim-uid"},
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {}, "volumeName": volume},
            "status": {"phase": "Bound"}
        }),
    )
    .await;
}

#[tokio::test]
async fn migrated_in_tree_pv_gets_a_va_for_the_translated_csi_source() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_claim(&s, "gce-pv").await;
    put(&s, "persistentvolumes", None, "gce-pv", gce_pv_json()).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        pod("p", "node-1", "claim", "Running"),
    )
    .await;

    controller(&s).reconcile_all().await.unwrap();

    let handle = translated_gce_pv_handle();
    let v = vas(&s).await;
    assert_eq!(v.len(), 1, "a migrated GCE PD must be attached via CSI");
    assert_eq!(
        v[0].metadata.name,
        attachment_name(&handle, GCE_DRIVER, "node-1")
    );
    assert_eq!(v[0].spec.attacher, GCE_DRIVER);
    // regular PV scenario: the PV name, not an inline spec (csi_attacher.go:96-102).
    assert_eq!(
        v[0].spec.source.persistent_volume_name.as_deref(),
        Some("gce-pv")
    );
    assert!(v[0].spec.source.inline_volume_spec.is_none());

    // The attachment is recognised as owned on the next pass (no churn).
    controller(&s).reconcile_all().await.unwrap();
    assert_eq!(vas(&s).await.len(), 1);
}

#[tokio::test]
async fn migrated_in_tree_pv_va_is_detached_once_unused() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_claim(&s, "gce-pv").await;
    put(&s, "persistentvolumes", None, "gce-pv", gce_pv_json()).await;
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
    assert!(
        vas(&s).await.is_empty(),
        "populateActualStateOfWorld translates the PV so the VA is owned and detached"
    );
}

#[tokio::test]
async fn migrated_inline_volume_gets_a_va_with_inline_volume_spec() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "p-uid"},
            "spec": {
                "nodeName": "node-1",
                "containers": [{"name": "c", "image": "i"}],
                "volumes": [{"name": "data",
                    "gcePersistentDisk": {"pdName": "pd-1", "fsType": "ext4"}}]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await;

    controller(&s).reconcile_all().await.unwrap();

    let v = vas(&s).await;
    assert_eq!(
        v.len(),
        1,
        "inline in-tree volume migrates to an attachment"
    );
    let inline = v[0]
        .spec
        .source
        .inline_volume_spec
        .as_ref()
        .expect("csi_attacher.go:84-92: InlineVolumeSpecForCSIMigration");
    assert!(v[0].spec.source.persistent_volume_name.is_none());
    let csi = inline.csi.as_ref().unwrap();
    assert_eq!(csi.driver, GCE_DRIVER);
    assert_eq!(v[0].spec.attacher, GCE_DRIVER);
    assert_eq!(
        v[0].metadata.name,
        attachment_name(csi.volume_handle.as_deref().unwrap(), GCE_DRIVER, "node-1")
    );

    // Pod gone: the inline-source attachment is detached too.
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();
    controller(&s).reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}

#[tokio::test]
async fn inline_non_migratable_and_inline_csi_volumes_are_never_attached() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    put(
        &s,
        "pods",
        Some("ns"),
        "p",
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "p-uid"},
            "spec": {
                "nodeName": "node-1",
                "containers": [{"name": "c", "image": "i"}],
                "volumes": [
                    {"name": "e", "emptyDir": {}},
                    {"name": "c", "csi": {"driver": DRIVER}}
                ]
            },
            "status": {"phase": "Running"}
        }),
    )
    .await;
    controller(&s).reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}

// ---- verifyVolumeIsSafeToDetach (operation_generator.go:1489-1514) ----

#[tokio::test]
async fn verify_safe_to_detach_refuses_while_node_lists_volume_in_use() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", Some(&unique())).await;
    let e = controller(&s)
        .verify_volume_is_safe_to_detach("node-1", &unique())
        .await
        .unwrap_err();
    assert!(e.contains("still in use by node"), "{e}");
}

#[tokio::test]
async fn verify_safe_to_detach_passes_when_not_in_use_or_node_missing() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", Some("kubernetes.io/csi/other^x")).await;
    let c = controller(&s);
    assert!(c
        .verify_volume_is_safe_to_detach("node-1", &unique())
        .await
        .is_ok());
    // Node NotFound: "DetachVolume will skip safe to detach check".
    assert!(c
        .verify_volume_is_safe_to_detach("gone", &unique())
        .await
        .is_ok());
}

// ---- periodic VerifyVolumesAreAttached (reconciler.go:117-145) ----
// `GenerateVolumesAreAttachedFunc` (operation_generator.go:194-234) marks a
// volume whose attachment is not `status.attached` as detached, which drops it
// from `GetVolumesToReportAttached`; `attachDesiredVolumes` reports it again
// once `Attach` sees `status.attached`.

async fn set_va_attached(s: &MemoryStorage, attached: bool) {
    let key = build_key(
        "volumeattachments",
        None,
        &attachment_name("vol-1", DRIVER, "node-1"),
    );
    let mut va: VolumeAttachment = s.get(&key).await.unwrap();
    va.status = Some(serde_json::from_value(json!({"attached": attached})).unwrap());
    s.update(&key, &va).await.unwrap();
}

async fn attached_setup() -> (Arc<MemoryStorage>, AttachDetachController<MemoryStorage>) {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    seed_pod_p(&s).await;
    // Create the VA with a controller that does not sync, then hand over to
    // one whose first pass probes it (a restarted controller: zero timeOfLastSync).
    controller(&s)
        .with_disable_reconciliation_sync(true)
        .reconcile_all()
        .await
        .unwrap();
    (s.clone(), controller(&s))
}

#[tokio::test]
async fn sync_drops_unattached_volume_from_node_status_until_confirmed() {
    let (s, c) = attached_setup().await;
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);

    // First pass syncs (zero timeOfLastSync): the VA never became attached.
    c.reconcile_all().await.unwrap();
    // The mark takes effect on the next report.
    c.reconcile_all().await.unwrap();
    assert!(node_attached(&s, "node-1").await.is_empty());
    assert_eq!(vas(&s).await.len(), 1, "the attachment itself is kept");

    // The driver confirms: attachDesiredVolumes reports it again.
    set_va_attached(&s, true).await;
    c.reconcile_all().await.unwrap();
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);
}

#[tokio::test]
async fn sync_keeps_attached_volume_reported() {
    let (s, c) = attached_setup().await;
    set_va_attached(&s, true).await;
    c.reconcile_all().await.unwrap();
    c.reconcile_all().await.unwrap();
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);
}

#[tokio::test]
async fn sync_disabled_or_under_one_second_keeps_unattached_volume_reported() {
    for c in [
        |s: &Arc<MemoryStorage>| controller(s).with_disable_reconciliation_sync(true),
        |s: &Arc<MemoryStorage>| controller(s).with_reconciler_sync_period(WAIT),
    ] {
        let s = Arc::new(MemoryStorage::new());
        seed_node(&s, "node-1", None).await;
        seed_volume(&s, json!(["ReadWriteOnce"])).await;
        seed_pod_p(&s).await;
        let c = c(&s);
        for _ in 0..3 {
            c.reconcile_all().await.unwrap();
        }
        assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);
    }
}

#[tokio::test]
async fn sync_waits_for_the_period_between_probes() {
    let s = Arc::new(MemoryStorage::new());
    seed_node(&s, "node-1", None).await;
    seed_volume(&s, json!(["ReadWriteOnce"])).await;
    seed_pod_p(&s).await;
    controller(&s)
        .with_disable_reconciliation_sync(true)
        .reconcile_all()
        .await
        .unwrap();
    let c = controller(&s).with_reconciler_sync_period(std::time::Duration::from_secs(3600));
    c.reconcile_all().await.unwrap(); // first sync (zero time) marks the VA
    c.reconcile_all().await.unwrap();
    set_va_attached(&s, true).await;
    c.reconcile_all().await.unwrap(); // confirmed, reported again
    set_va_attached(&s, false).await;
    for _ in 0..2 {
        c.reconcile_all().await.unwrap(); // period not elapsed: no probe
    }
    assert_eq!(node_attached(&s, "node-1").await, vec![unique()]);
}

#[tokio::test]
async fn marked_detached_volume_is_still_deleted_when_no_longer_desired() {
    let (s, c) = attached_setup().await;
    c.reconcile_all().await.unwrap();
    c.reconcile_all().await.unwrap();
    assert!(node_attached(&s, "node-1").await.is_empty());
    s.delete(&build_key("pods", Some("ns"), "p")).await.unwrap();
    c.reconcile_all().await.unwrap();
    assert!(vas(&s).await.is_empty());
}
