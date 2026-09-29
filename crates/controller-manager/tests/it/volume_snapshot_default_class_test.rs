//! A VolumeSnapshot that names no class gets the default one for its driver.
//!
//! `volumeSnapshotClassName` is optional in the external-snapshotter CRD: nil
//! means "use the default VolumeSnapshotClass". A cluster may hold several —
//! one per CSI driver — so the driver has to be resolved from the snapshot's
//! source before a default can be picked.
//!
//! Ported from `kubernetes-csi/external-snapshotter`:
//! `SetDefaultSnapshotClass` (`pkg/common-controller/snapshot_controller.go:1466-1526`),
//! `pvDriverFromSnapshot` (`:1394-1408`), `getVolumeFromVolumeSnapshot`
//! (`:1350-1376`), `isVolumeBoundToClaim` (`:1381-1392`) and
//! `IsVolumeSnapshotClassDefaultAnnotation` (`pkg/utils/util.go:287-291`).
//! Before #2027 the controller logged "not implemented" and skipped.

use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::volume::*;
use rusternetes_common::resources::Event;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::volume_snapshot::VolumeSnapshotController;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

const DRIVER: &str = "rusternetes.io/hostpath-snapshotter";

async fn csi_pv(storage: &MemoryStorage, name: &str, driver: &str, claim: Option<(&str, &str)>) {
    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeSpec {
            capacity: HashMap::from([("storage".to_string(), "10Gi".to_string())]),
            host_path: None,
            nfs: None,
            iscsi: None,
            local: None,
            csi: Some(CSIVolumeSource {
                driver: driver.to_string(),
                volume_handle: Some(format!("handle-{name}")),
                ..Default::default()
            }),
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            storage_class_name: Some("fast".to_string()),
            mount_options: None,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            node_affinity: None,
            claim_ref: claim.map(|(ns, name)| ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: Some(ns.to_string()),
                name: Some(name.to_string()),
                ..Default::default()
            }),
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeStatus {
            phase: PersistentVolumePhase::Bound,
            message: None,
            reason: None,
            last_phase_transition_time: None,
        }),
    };
    storage
        .create(&build_key("persistentvolumes", None, name), &pv)
        .await
        .unwrap();
}

async fn pvc(
    storage: &MemoryStorage,
    name: &str,
    pv_name: &str,
    phase: PersistentVolumeClaimPhase,
) {
    let claim = PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: PersistentVolumeClaimSpec {
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            resources: ResourceRequirements {
                limits: None,
                requests: Some(HashMap::from([("storage".to_string(), "5Gi".to_string())])),
            },
            volume_name: Some(pv_name.to_string()),
            storage_class_name: Some("fast".to_string()),
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            selector: None,
            data_source: None,
            data_source_ref: None,
            volume_attributes_class_name: None,
        },
        status: Some(PersistentVolumeClaimStatus {
            allocated_resources: None,
            allocated_resource_statuses: None,
            resize_status: None,
            phase,
            access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
            capacity: None,
            conditions: None,
            current_volume_attributes_class_name: None,
            modify_volume_status: None,
        }),
    };
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("default"), name),
            &claim,
        )
        .await
        .unwrap();
}

async fn class(storage: &MemoryStorage, name: &str, driver: &str, is_default: bool) {
    let mut metadata = ObjectMeta::new(name);
    if is_default {
        metadata.annotations = Some(HashMap::from([(
            "snapshot.storage.kubernetes.io/is-default-class".to_string(),
            "true".to_string(),
        )]));
    }
    let vsc = VolumeSnapshotClass {
        type_meta: TypeMeta {
            kind: "VolumeSnapshotClass".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata,
        driver: driver.to_string(),
        parameters: None,
        deletion_policy: DeletionPolicy::Delete,
    };
    storage
        .create(&build_key("volumesnapshotclasses", None, name), &vsc)
        .await
        .unwrap();
}

async fn snapshot(storage: &MemoryStorage, name: &str, source: VolumeSnapshotSource) {
    let vs = VolumeSnapshot {
        type_meta: TypeMeta {
            kind: "VolumeSnapshot".to_string(),
            api_version: "snapshot.storage.k8s.io/v1".to_string(),
        },
        metadata: {
            let mut meta = ObjectMeta::new(name);
            meta.namespace = Some("default".to_string());
            meta.uid = uuid::Uuid::new_v4().to_string();
            meta
        },
        spec: VolumeSnapshotSpec {
            source,
            volume_snapshot_class_name: None,
        },
        status: None,
    };
    storage
        .create(&build_key("volumesnapshots", Some("default"), name), &vs)
        .await
        .unwrap();
}

fn from_pvc(name: &str) -> VolumeSnapshotSource {
    VolumeSnapshotSource {
        persistent_volume_claim_name: Some(name.to_string()),
        volume_snapshot_content_name: None,
    }
}

async fn stored(storage: &MemoryStorage, name: &str) -> VolumeSnapshot {
    storage
        .get(&build_key("volumesnapshots", Some("default"), name))
        .await
        .unwrap()
}

async fn warning_reasons(storage: &MemoryStorage) -> Vec<String> {
    let events: Vec<Event> = storage
        .list(&build_prefix("events", Some("default")))
        .await
        .unwrap_or_default();
    events.into_iter().map(|e| e.reason).collect()
}

/// One default class for the PV's driver: it is chosen, recorded on the
/// snapshot's spec, and the content is created from it.
#[tokio::test]
async fn the_single_default_class_for_the_driver_is_selected() {
    let storage = Arc::new(MemoryStorage::new());
    csi_pv(&storage, "pv-1", DRIVER, Some(("default", "pvc-1"))).await;
    pvc(&storage, "pvc-1", "pv-1", PersistentVolumeClaimPhase::Bound).await;
    class(&storage, "default-class", DRIVER, true).await;
    // A default class for a different driver must not be picked.
    class(
        &storage,
        "other-driver-default",
        "other.csi.example.com",
        true,
    )
    .await;
    // And a non-default class for the right driver must not be picked either.
    class(&storage, "not-default", DRIVER, false).await;
    snapshot(&storage, "snap-1", from_pvc("pvc-1")).await;

    VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();

    let vs = stored(&storage, "snap-1").await;
    assert_eq!(
        vs.spec.volume_snapshot_class_name.as_deref(),
        Some("default-class"),
        "upstream patches /spec/volumeSnapshotClassName with the chosen class"
    );

    let content: VolumeSnapshotContent = storage
        .get(&build_key(
            "volumesnapshotcontents",
            None,
            "snapcontent-default-snap-1",
        ))
        .await
        .expect("content must be created once a class is resolved");
    assert_eq!(content.spec.driver, DRIVER);
}

/// No default class for that driver: `cannot find default snapshot class`, and
/// the snapshot's spec is left alone.
#[tokio::test]
async fn no_default_class_leaves_the_snapshot_unbound_and_warns() {
    let storage = Arc::new(MemoryStorage::new());
    csi_pv(&storage, "pv-2", DRIVER, Some(("default", "pvc-2"))).await;
    pvc(&storage, "pvc-2", "pv-2", PersistentVolumeClaimPhase::Bound).await;
    class(&storage, "not-default", DRIVER, false).await;
    snapshot(&storage, "snap-2", from_pvc("pvc-2")).await;

    let _ = VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await;

    let vs = stored(&storage, "snap-2").await;
    assert_eq!(vs.spec.volume_snapshot_class_name, None);
    assert!(
        warning_reasons(&storage)
            .await
            .contains(&"SetDefaultSnapshotClassFailed".to_string()),
        "the failure is reported as an event, as upstream's checkAndUpdateSnapshotClass does"
    );
}

/// Two defaults for the same driver is upstream's
/// `%d default snapshot classes were found`.
#[tokio::test]
async fn two_default_classes_for_one_driver_is_a_failure() {
    let storage = Arc::new(MemoryStorage::new());
    csi_pv(&storage, "pv-3", DRIVER, Some(("default", "pvc-3"))).await;
    pvc(&storage, "pvc-3", "pv-3", PersistentVolumeClaimPhase::Bound).await;
    class(&storage, "default-a", DRIVER, true).await;
    class(&storage, "default-b", DRIVER, true).await;
    snapshot(&storage, "snap-3", from_pvc("pvc-3")).await;

    let _ = VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await;

    assert_eq!(
        stored(&storage, "snap-3")
            .await
            .spec
            .volume_snapshot_class_name,
        None
    );
    assert!(warning_reasons(&storage)
        .await
        .contains(&"SetDefaultSnapshotClassFailed".to_string()));
}

/// A PVC that is not yet Bound has no PV to read a driver from
/// (`getVolumeFromVolumeSnapshot`, `:1356-1358`).
#[tokio::test]
async fn an_unbound_pvc_is_not_snapshotted() {
    let storage = Arc::new(MemoryStorage::new());
    csi_pv(&storage, "pv-4", DRIVER, Some(("default", "pvc-4"))).await;
    pvc(
        &storage,
        "pvc-4",
        "pv-4",
        PersistentVolumeClaimPhase::Pending,
    )
    .await;
    class(&storage, "default-class", DRIVER, true).await;
    snapshot(&storage, "snap-4", from_pvc("pvc-4")).await;

    let _ = VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await;

    assert_eq!(
        stored(&storage, "snap-4")
            .await
            .spec
            .volume_snapshot_class_name,
        None
    );
}

/// A PV whose `claimRef` names someone else fails `isVolumeBoundToClaim`
/// (`:1381-1392`) — otherwise the snapshot would be taken of another claim's
/// volume.
#[tokio::test]
async fn a_broken_binding_is_refused() {
    let storage = Arc::new(MemoryStorage::new());
    csi_pv(&storage, "pv-5", DRIVER, Some(("default", "someone-else"))).await;
    pvc(&storage, "pvc-5", "pv-5", PersistentVolumeClaimPhase::Bound).await;
    class(&storage, "default-class", DRIVER, true).await;
    snapshot(&storage, "snap-5", from_pvc("pvc-5")).await;

    let _ = VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await;

    assert_eq!(
        stored(&storage, "snap-5")
            .await
            .spec
            .volume_snapshot_class_name,
        None
    );
}

/// "supports ONLY CSI volumes" (`:1403-1406`).
#[tokio::test]
async fn a_non_csi_volume_is_not_snapshotted() {
    let storage = Arc::new(MemoryStorage::new());
    let pv = PersistentVolume {
        type_meta: TypeMeta {
            kind: "PersistentVolume".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: ObjectMeta::new("pv-6"),
        spec: PersistentVolumeSpec {
            capacity: HashMap::from([("storage".to_string(), "10Gi".to_string())]),
            host_path: Some(HostPathVolumeSource {
                path: "/tmp/pv-6".to_string(),
                r#type: Some(HostPathType::DirectoryOrCreate),
            }),
            nfs: None,
            iscsi: None,
            local: None,
            csi: None,
            access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
            persistent_volume_reclaim_policy: Some(PersistentVolumeReclaimPolicy::Delete),
            storage_class_name: Some("fast".to_string()),
            mount_options: None,
            volume_mode: Some(PersistentVolumeMode::Filesystem),
            node_affinity: None,
            claim_ref: Some(ObjectReference {
                kind: Some("PersistentVolumeClaim".to_string()),
                namespace: Some("default".to_string()),
                name: Some("pvc-6".to_string()),
                ..Default::default()
            }),
            volume_attributes_class_name: None,
        },
        status: None,
    };
    storage
        .create(&build_key("persistentvolumes", None, "pv-6"), &pv)
        .await
        .unwrap();
    pvc(&storage, "pvc-6", "pv-6", PersistentVolumeClaimPhase::Bound).await;
    class(&storage, "default-class", DRIVER, true).await;
    snapshot(&storage, "snap-6", from_pvc("pvc-6")).await;

    let _ = VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await;

    assert_eq!(
        stored(&storage, "snap-6")
            .await
            .spec
            .volume_snapshot_class_name,
        None
    );
}

/// A pre-provisioned snapshot names its content, which carries its own driver:
/// upstream returns without an error and without a class (`:1469-1473`).
#[tokio::test]
async fn a_pre_provisioned_snapshot_needs_no_class() {
    let storage = Arc::new(MemoryStorage::new());
    snapshot(
        &storage,
        "snap-7",
        VolumeSnapshotSource {
            persistent_volume_claim_name: None,
            volume_snapshot_content_name: Some("pre-made-content".to_string()),
        },
    )
    .await;

    VolumeSnapshotController::new(storage.clone())
        .reconcile_all()
        .await
        .expect("a pre-provisioned snapshot is not an error");

    assert_eq!(
        stored(&storage, "snap-7")
            .await
            .spec
            .volume_snapshot_class_name,
        None
    );
    assert!(
        warning_reasons(&storage).await.is_empty(),
        "and it emits no warning"
    );
}
