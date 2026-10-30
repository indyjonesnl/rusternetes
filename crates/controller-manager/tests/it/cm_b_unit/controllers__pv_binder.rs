//! Unit tests moved from `src/controllers/pv_binder.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::{
        get_default_class, NodeSelectorTerm, PersistentVolumeClaimPhase, PersistentVolumeMode,
        PersistentVolumePhase, PersistentVolumeReclaimPolicy, StorageClass, VolumeBindingMode,
        VolumeNodeAffinity,
    };
    pub use rusternetes_common::resources::{
        EventSource, EventType, Node, PersistentVolume, PersistentVolumeClaim,
        PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus, Pod,
    };
    pub use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::service_account::ObjectReference;
    use rusternetes_common::resources::volume::{
        PersistentVolumeAccessMode, PersistentVolumeClaimPhase, PersistentVolumeClaimStatus,
        PersistentVolumeMode, ResourceRequirements,
    };
    use rusternetes_common::resources::{
        PersistentVolumeClaimSpec, PersistentVolumeSpec, PersistentVolumeStatus,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_controller_manager::controllers::pv_binder::*;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    fn ann_map(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// pv_controller_test.go TestAnnealMigrationAnnotations: every case, run
    /// through the one `updateMigrationAnnotations(..., claim)` helper
    /// (`pv_controller_base.go:445-496`). Migrated plugin = gce-pd
    /// (`IsMigrationEnabledForPlugin`, plugin_manager.go:85-107); non-migrated
    /// = rbd; `non-migrated-plugin` = unknown.
    #[test]
    fn anneal_migration_annotations_table() {
        const GCE: &str = "kubernetes.io/gce-pd";
        const GCE_DRIVER: &str = "pd.csi.storage.gke.io";
        const RBD: &str = "kubernetes.io/rbd";
        const RBD_DRIVER: &str = "rbd.csi.ceph.com";
        let vol = ANN_DYNAMICALLY_PROVISIONED;
        let claim = ANN_STORAGE_PROVISIONER;
        let beta = ANN_BETA_STORAGE_PROVISIONER;
        // (key, in, expected, is_claim)
        type Kv<'a> = Vec<(&'a str, &'a str)>;
        let cases: Vec<(&str, Kv, Kv, bool)> = vec![
            (
                "vol on",
                vec![(vol, GCE)],
                vec![(vol, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                false,
            ),
            (
                "claim on",
                vec![(claim, GCE)],
                vec![(claim, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                true,
            ),
            (
                "claim on beta",
                vec![(beta, GCE)],
                vec![(beta, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                true,
            ),
            ("vol off", vec![(vol, RBD)], vec![(vol, RBD)], false),
            ("claim off", vec![(claim, RBD)], vec![(claim, RBD)], true),
            (
                "vol rollback",
                vec![(vol, RBD), (ANN_MIGRATED_TO, RBD_DRIVER)],
                vec![(vol, RBD)],
                false,
            ),
            (
                "claim rollback",
                vec![(claim, RBD), (ANN_MIGRATED_TO, RBD_DRIVER)],
                vec![(claim, RBD)],
                true,
            ),
            (
                "claim rollback beta",
                vec![(beta, RBD), (ANN_MIGRATED_TO, RBD_DRIVER)],
                vec![(beta, RBD)],
                true,
            ),
            (
                "vol other",
                vec![(vol, "non-migrated-plugin")],
                vec![(vol, "non-migrated-plugin")],
                false,
            ),
            ("not provisioned", vec![], vec![], false),
            (
                "wrong key for kind",
                vec![(claim, GCE)],
                vec![(claim, GCE)],
                false,
            ),
            (
                "vol stale driver",
                vec![(vol, GCE), (ANN_MIGRATED_TO, "old")],
                vec![(vol, GCE), (ANN_MIGRATED_TO, GCE_DRIVER)],
                false,
            ),
        ];
        for (name, input, want, is_claim) in cases {
            let mut ann = ann_map(&input);
            let changed = update_migration_annotations(Some(&mut ann), is_claim);
            assert_eq!(ann, ann_map(&want), "{name}");
            assert_eq!(
                changed,
                input.iter().collect::<std::collections::BTreeSet<_>>() != want.iter().collect(),
                "{name}: changed flag"
            );
        }
        assert!(!update_migration_annotations(None, true));
    }

    /// `IsMigrationEnabledForPlugin` (plugin_manager.go:85-107) with the
    /// 1.35 GA-locked gates: all seven translator plugins are migrated.
    #[test]
    fn migration_enabled_plugins_match_upstream() {
        for (plugin, driver) in [
            ("kubernetes.io/aws-ebs", "ebs.csi.aws.com"),
            ("kubernetes.io/gce-pd", "pd.csi.storage.gke.io"),
            ("kubernetes.io/azure-file", "file.csi.azure.com"),
            ("kubernetes.io/azure-disk", "disk.csi.azure.com"),
            ("kubernetes.io/cinder", "cinder.csi.openstack.org"),
            ("kubernetes.io/vsphere-volume", "csi.vsphere.vmware.com"),
            ("kubernetes.io/portworx-volume", "pxd.portworx.com"),
        ] {
            assert!(is_migration_enabled_for_plugin(plugin), "{plugin}");
            assert_eq!(csi_name_from_in_tree_name(plugin), Some(driver));
        }
        assert!(!is_migration_enabled_for_plugin("kubernetes.io/rbd"));
        assert_eq!(csi_name_from_in_tree_name("kubernetes.io/rbd"), None);
    }

    /// TestModifyDeletionFinalizers migration-on branch
    /// (`pv_controller_base.go:411-420`): the in-tree delete finalizer is
    /// removed for a migrated plugin and nothing else is touched.
    #[test]
    fn modify_deletion_finalizers_migration_on_removes_in_tree_finalizer() {
        let mut pv = pv_with("m", None, Some(PersistentVolumePhase::Available));
        pv.metadata.annotations = Some(ann_map(&[(
            ANN_DYNAMICALLY_PROVISIONED,
            "kubernetes.io/gce-pd",
        )]));
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.metadata.finalizers = Some(vec![
            IN_TREE_PV_DELETION_PROTECTION_FINALIZER.into(),
            EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.into(),
        ]);
        let (out, modified) = modify_deletion_finalizers(&pv);
        assert!(modified);
        assert_eq!(
            out,
            Some(vec![EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.to_string()])
        );
        // migrated plugin without the in-tree finalizer: unchanged (no add).
        pv.metadata.finalizers = None;
        assert!(!modify_deletion_finalizers(&pv).1);
    }

    #[test]
    fn test_storage_comparison() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage);

        assert!(controller.storage_sufficient("10Gi", "5Gi"));
        assert!(controller.storage_sufficient("10Gi", "10Gi"));
        assert!(!controller.storage_sufficient("5Gi", "10Gi"));
        assert!(controller.storage_sufficient("100Mi", "50Mi"));
        assert!(!controller.storage_sufficient("50Mi", "100Mi"));
    }

    #[test]
    fn claim_ref_points_to_matches_name_namespace_uid() {
        let cr = ObjectReference {
            kind: Some("PersistentVolumeClaim".into()),
            namespace: Some("ns".into()),
            name: Some("pvc".into()),
            uid: Some("uid-1".into()),
            api_version: Some("v1".into()),
            resource_version: None,
            field_path: None,
        };
        assert!(claim_ref_points_to(&cr, "ns", "pvc", "uid-1"));
        assert!(!claim_ref_points_to(&cr, "ns", "other", "uid-1")); // wrong name
        assert!(!claim_ref_points_to(&cr, "other", "pvc", "uid-1")); // wrong ns
        assert!(!claim_ref_points_to(&cr, "ns", "pvc", "uid-2")); // stale uid
                                                                  // A pre-bind without a uid still counts.
        let cr_no_uid = ObjectReference {
            uid: None,
            ..cr.clone()
        };
        assert!(claim_ref_points_to(&cr_no_uid, "ns", "pvc", "uid-1"));
    }

    fn make_pvc(name: &str, uid: &str) -> PersistentVolumeClaim {
        let mut requests = HashMap::new();
        requests.insert("storage".to_string(), "1Gi".to_string());
        PersistentVolumeClaim {
            type_meta: TypeMeta {
                kind: "PersistentVolumeClaim".into(),
                api_version: "v1".into(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("sstest".into());
                m.uid = uid.into();
                m
            },
            spec: PersistentVolumeClaimSpec {
                access_modes: Some(vec![PersistentVolumeAccessMode::ReadWriteOnce]),
                resources: ResourceRequirements {
                    limits: None,
                    requests: Some(requests),
                },
                volume_name: None,
                storage_class_name: Some("standard".into()),
                volume_mode: None,
                selector: None,
                data_source: None,
                data_source_ref: None,
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeClaimStatus {
                phase: PersistentVolumeClaimPhase::Pending,
                access_modes: None,
                capacity: None,
                conditions: None,
                allocated_resources: None,
                allocated_resource_statuses: None,
                resize_status: None,
                current_volume_attributes_class_name: None,
                modify_volume_status: None,
            }),
        }
    }

    fn make_prebound_pv(pv_name: &str, pvc_name: &str, pvc_uid: &str) -> PersistentVolume {
        let mut capacity = HashMap::new();
        capacity.insert("storage".to_string(), "1Gi".to_string());
        PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".into(),
                api_version: "v1".into(),
            },
            metadata: ObjectMeta::new(pv_name),
            spec: PersistentVolumeSpec {
                capacity,
                host_path: None,
                nfs: None,
                iscsi: None,
                local: None,
                csi: None,
                gce_persistent_disk: None,
                aws_elastic_block_store: None,
                fc: None,
                more_sources: Default::default(),
                access_modes: vec![PersistentVolumeAccessMode::ReadWriteOnce],
                persistent_volume_reclaim_policy: None,
                storage_class_name: Some("standard".into()),
                mount_options: None,
                volume_mode: None,
                node_affinity: None,
                claim_ref: Some(ObjectReference {
                    kind: Some("PersistentVolumeClaim".into()),
                    namespace: Some("sstest".into()),
                    name: Some(pvc_name.into()),
                    uid: Some(pvc_uid.into()),
                    api_version: Some("v1".into()),
                    resource_version: None,
                    field_path: None,
                }),
                volume_attributes_class_name: None,
            },
            status: Some(PersistentVolumeStatus {
                phase: PersistentVolumePhase::Available,
                message: None,
                reason: None,
                last_phase_transition_time: None,
            }),
        }
    }

    fn pv_with(
        name: &str,
        claim_uid: Option<Option<&str>>,
        phase: Option<PersistentVolumePhase>,
    ) -> PersistentVolume {
        let mut pv = make_prebound_pv(name, "c", "u");
        pv.spec.claim_ref = claim_uid.map(|uid| ObjectReference {
            uid: uid.map(String::from),
            ..pv.spec.claim_ref.clone().unwrap()
        });
        pv.status = phase.map(|phase| PersistentVolumeStatus {
            phase,
            message: Some("stale".into()),
            reason: None,
            last_phase_transition_time: None,
        });
        pv
    }

    /// #2066: upstream syncVolume (pv_controller.go:576-596) moves a PV with no
    /// claimRef, or a claimRef with an empty UID, to Available; a claimRef with
    /// a UID is the bound branch and is untouched.
    #[tokio::test]
    async fn sync_unbound_volumes_moves_pending_to_available() {
        use PersistentVolumePhase::*;
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage.clone());
        let cases = [
            (
                "nil-claim",
                pv_with("nil-claim", None, Some(Pending)),
                Available,
            ),
            ("no-status", pv_with("no-status", None, None), Available),
            (
                "empty-uid",
                pv_with("empty-uid", Some(Some("")), Some(Pending)),
                Available,
            ),
            (
                "absent-uid",
                pv_with("absent-uid", Some(None), Some(Pending)),
                Available,
            ),
            (
                // claimRef with a UID and no such claim: the claim-not-found
                // branch (pv_controller.go:598+), covered in depth below.
                "bound-uid",
                pv_with("bound-uid", Some(Some("u")), Some(Pending)),
                Released,
            ),
        ];
        for (name, pv, _) in &cases {
            storage
                .create(&build_key("persistentvolumes", None, name), pv)
                .await
                .unwrap();
        }
        controller.sync_volumes(&WorkQueue::new()).await.unwrap();
        for (name, _, want) in &cases {
            let got: PersistentVolume = storage
                .get(&build_key("persistentvolumes", None, name))
                .await
                .unwrap();
            assert_eq!(got.status.as_ref().unwrap().phase, *want, "{name}");
        }
        let got: PersistentVolume = storage
            .get(&build_key("persistentvolumes", None, "nil-claim"))
            .await
            .unwrap();
        assert_eq!(got.status.unwrap().message, None, "message cleared");
    }

    /// #1095: two PVCs whose PVs were pre-bound (claimRef) by the dynamic
    /// provisioner must each bind to THEIR OWN PV, never cross. PVs are stored
    /// in the reverse of the bind order so a first-match binder would mis-pair.
    #[tokio::test]
    async fn prebound_pvs_do_not_cross_bind() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = PVBinderController::new(storage.clone());

        let pv_a = make_prebound_pv("pvc-sstest-explicit-pvc", "explicit-pvc", "uid-a");
        let pv_b = make_prebound_pv("pvc-sstest-default-pvc", "default-pvc", "uid-b");
        // Insert b first, a second — list order must not decide the pairing.
        storage
            .create(
                &build_key("persistentvolumes", None, &pv_b.metadata.name),
                &pv_b,
            )
            .await
            .unwrap();
        storage
            .create(
                &build_key("persistentvolumes", None, &pv_a.metadata.name),
                &pv_a,
            )
            .await
            .unwrap();

        let mut explicit = make_pvc("explicit-pvc", "uid-a");
        let mut default = make_pvc("default-pvc", "uid-b");
        // PVCs exist in storage (created by the user) before binding; the binder
        // persists the bound result via update().
        for pvc in [&explicit, &default] {
            storage
                .create(
                    &build_key(
                        "persistentvolumeclaims",
                        pvc.metadata.namespace.as_deref(),
                        &pvc.metadata.name,
                    ),
                    pvc,
                )
                .await
                .unwrap();
        }

        controller.bind_pvc(&mut explicit).await.unwrap();
        assert_eq!(
            explicit.spec.volume_name.as_deref(),
            Some("pvc-sstest-explicit-pvc"),
            "explicit-pvc must bind to its own pre-bound PV"
        );

        controller.bind_pvc(&mut default).await.unwrap();
        assert_eq!(
            default.spec.volume_name.as_deref(),
            Some("pvc-sstest-default-pvc"),
            "default-pvc must bind to its own pre-bound PV, not explicit-pvc's"
        );

        // Each PV's claimRef ends up pinned to the matching PVC.
        let bound_a: PersistentVolume = storage
            .get(&build_key(
                "persistentvolumes",
                None,
                "pvc-sstest-explicit-pvc",
            ))
            .await
            .unwrap();
        assert_eq!(
            bound_a.spec.claim_ref.unwrap().name.as_deref(),
            Some("explicit-pvc")
        );
    }

    // ---- syncVolume bound branches (pv_controller.go:598-770; binder_test.go 4-3..4-12) ----

    async fn put_pv(storage: &Arc<MemoryStorage>, pv: &PersistentVolume) {
        storage
            .create(&build_key("persistentvolumes", None, &pv.metadata.name), pv)
            .await
            .unwrap();
    }

    async fn put_pvc(storage: &Arc<MemoryStorage>, pvc: &PersistentVolumeClaim) {
        storage
            .create(
                &build_key(
                    "persistentvolumeclaims",
                    pvc.metadata.namespace.as_deref(),
                    &pvc.metadata.name,
                ),
                pvc,
            )
            .await
            .unwrap();
    }

    async fn get_pv(storage: &Arc<MemoryStorage>, name: &str) -> Option<PersistentVolume> {
        storage
            .get(&build_key("persistentvolumes", None, name))
            .await
            .ok()
    }

    async fn volume_mismatch_events(
        storage: &Arc<MemoryStorage>,
    ) -> Vec<rusternetes_common::resources::Event> {
        let all: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        all.into_iter()
            .filter(|e| e.reason == "VolumeMismatch")
            .collect()
    }

    fn bound_pv(name: &str) -> PersistentVolume {
        pv_with(name, Some(Some("u")), Some(PersistentVolumePhase::Bound))
    }

    /// binder_test.go "4-3": bound volume with missing claim -> Released, and
    /// Retain leaves it (and its claimRef) in place.
    #[tokio::test]
    async fn bound_pv_with_missing_claim_is_released_and_retained() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Retain);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.expect("retained");
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
        assert!(got.spec.claim_ref.is_some());
    }

    /// binder_test.go "4-4": claim recreated with a different UID counts as
    /// missing.
    #[tokio::test]
    async fn bound_pv_with_claim_of_different_uid_is_released() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        put_pvc(&storage, &make_pvc("c", "other-uid")).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
    }

    /// reclaimVolume Delete (pv_controller.go:1180+): released PV is deleted.
    #[tokio::test]
    async fn released_pv_with_delete_policy_is_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.spec.host_path = Some(
            rusternetes_common::resources::volume::HostPathVolumeSource {
                path: "/tmp/rusternetes-reclaim-test-nonexistent".into(),
                r#type: None,
            },
        );
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// A released `Delete` PV backed by a hostPath. All paths live under a
    /// unique `/tmp/hostpath.<uuid>` so the deleter's `/tmp/.+` guard passes
    /// and no test can touch anything outside its own scratch directory.
    fn host_path_delete_pv(name: &str, path: &str) -> PersistentVolume {
        use rusternetes_common::resources::volume::HostPathVolumeSource;
        let mut pv = bound_pv(name);
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.spec.host_path = Some(HostPathVolumeSource {
            path: path.to_string(),
            r#type: None,
        });
        pv
    }

    fn scratch_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(format!("/tmp/hostpath.{}", uuid::Uuid::new_v4()))
    }

    /// host_path_test.go `TestDeleter`: the deleter removes the directory,
    /// then the PV object is deleted (`deleteVolumeOperation`,
    /// pv_controller.go:1323-1393).
    #[tokio::test]
    async fn host_path_deleter_removes_directory_then_pv() {
        let dir = scratch_dir();
        std::fs::create_dir_all(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/data"), b"x").unwrap();
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &host_path_delete_pv("pv", dir.to_str().unwrap())).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let existed = dir.exists();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!existed, "hostPath directory must be wiped by the deleter");
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// host_path_test.go `TestDeleterTempDir` "just-tmp" / "not-tmp": the
    /// deleter refuses, so the PV goes Failed with a `VolumeFailedDelete`
    /// warning and is NOT deleted (`deleteVolumeOperation`, :1360-1366).
    #[tokio::test]
    async fn host_path_deleter_refuses_paths_outside_tmp() {
        for path in ["/tmp", "/tmp/", "/nottmp", "/nottmp/tmp/x"] {
            let storage = Arc::new(MemoryStorage::new());
            let c = PVBinderController::new(storage.clone());
            put_pv(&storage, &host_path_delete_pv("pv", path)).await;
            c.sync_volumes(&WorkQueue::new()).await.unwrap();
            let got = get_pv(&storage, "pv")
                .await
                .unwrap_or_else(|| panic!("{path}: PV must survive a refused delete"));
            let status = got.status.unwrap();
            assert_eq!(status.phase, PersistentVolumePhase::Failed, "{path}");
            let want =
                format!("host_path deleter only supports /tmp/.+ but received provided {path}");
            assert_eq!(status.message.as_deref(), Some(want.as_str()), "{path}");
            let events: Vec<rusternetes_common::resources::Event> =
                storage.list("/registry/events/").await.unwrap();
            assert!(
                events.iter().any(|e| e.reason == "VolumeFailedDelete"),
                "{path}: expected a VolumeFailedDelete event"
            );
        }
    }

    /// Hardening beyond upstream's unanchored `/tmp/.+` regex: a `..`
    /// component must not let a path escape /tmp/<x>/ and wipe a sibling.
    #[tokio::test]
    async fn host_path_deleter_refuses_parent_dir_traversal() {
        let victim = scratch_dir();
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::write(victim.join("keep"), b"x").unwrap();
        let decoy = scratch_dir();
        std::fs::create_dir_all(&decoy).unwrap();
        let sneaky = format!(
            "{}/../{}",
            decoy.display(),
            victim.file_name().unwrap().to_str().unwrap()
        );
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &host_path_delete_pv("pv", &sneaky)).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let survived = victim.join("keep").exists();
        let _ = std::fs::remove_dir_all(&victim);
        let _ = std::fs::remove_dir_all(&decoy);
        assert!(survived, "traversal path must not delete the target");
        let got = get_pv(&storage, "pv").await.expect("PV must survive");
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Failed);
    }

    /// `os.RemoveAll` returns nil for a missing path, so an already-gone
    /// directory still lets the PV be deleted.
    #[tokio::test]
    async fn host_path_deleter_missing_directory_is_ok() {
        let dir = scratch_dir();
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &host_path_delete_pv("pv", dir.to_str().unwrap())).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// recycle_test.go "6-3": a Recycle volume with no recycler plugin goes
    /// Failed with message "No recycler plugin found for the volume!" and a
    /// `Warning VolumeFailedRecycle` event (pv_controller.go:1279-1286).
    #[tokio::test]
    async fn recycle_policy_without_recycler_plugin_fails_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Recycle);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        let status = get_pv(&storage, "pv").await.unwrap().status.unwrap();
        assert_eq!(status.phase, PersistentVolumePhase::Failed);
        assert_eq!(
            status.message.as_deref(),
            Some("No recycler plugin found for the volume!")
        );
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        let ev: Vec<_> = events
            .iter()
            .filter(|e| e.reason == "VolumeFailedRecycle")
            .collect();
        assert_eq!(ev.len(), 1);
        // Already Failed: a further sync must not emit another event.
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.reason == "VolumeFailedRecycle")
                .count(),
            1
        );
    }

    /// Wait for every detached volume operation to finish (upstream's tests
    /// wait on `runningOperations` the same way).
    async fn settle(c: &PVBinderController<MemoryStorage>) {
        tokio::time::timeout(Duration::from_secs(20), async {
            while c.operations_running() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("volume operations must finish");
    }

    /// Stand-in for the kubelet: once the recycler pod `recycler-for-<pv>`
    /// exists, drive it to `phase` with `message`.
    fn fake_kubelet(
        storage: &Arc<MemoryStorage>,
        pv: &str,
        phase: rusternetes_common::types::Phase,
        message: Option<&str>,
    ) -> tokio::task::JoinHandle<()> {
        let storage = Arc::clone(storage);
        let key = build_key("pods", Some("default"), &format!("recycler-for-{pv}"));
        let message = message.map(String::from);
        tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if let Ok(mut pod) = storage.get::<Pod>(&key).await {
                        pod.status = Some(rusternetes_common::resources::pod::PodStatus {
                            phase: Some(phase),
                            message,
                            ..Default::default()
                        });
                        storage.update_status(&key, &pod).await.unwrap();
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("recycler pod was never created");
        })
    }

    fn recycle_pv(name: &str, by_controller: bool) -> PersistentVolume {
        let mut pv = bound_pv(name);
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Recycle);
        pv.spec.host_path = Some(
            rusternetes_common::resources::volume::HostPathVolumeSource {
                path: "/tmp/recycle-me".into(),
                r#type: None,
            },
        );
        if by_controller {
            pv.metadata.annotations = Some(
                [(ANN_BOUND_BY_CONTROLLER.to_string(), "yes".to_string())]
                    .into_iter()
                    .collect(),
            );
        }
        pv
    }

    /// recycle_test.go "6-1": a hostPath volume bound by the controller is
    /// recycled by a recycler pod, then unbound and Available again with a
    /// `Normal VolumeRecycled` event; the recycler pod is gone afterwards.
    #[tokio::test]
    async fn recycle_hostpath_volume_bound_by_controller_becomes_available() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
        assert!(got.spec.claim_ref.is_none());
        assert!(got.metadata.annotations.is_none());
        let ev = events_with_reason(&storage, "VolumeRecycled").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].message, "Volume recycled");
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("default"), "recycler-for-pv"))
            .await
            .is_err());
    }

    /// recycle_test.go "6-2": a volume pre-bound by the user keeps its
    /// claimRef (minus the UID) when recycled.
    #[tokio::test]
    async fn recycle_prebound_volume_keeps_claim_ref_name() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", false)).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
        let cr = got.spec.claim_ref.expect("claimRef kept");
        assert_eq!(cr.name.as_deref(), Some("c"));
        assert!(cr.uid.is_none());
    }

    /// recycle_test.go "6-5": the recycler failing marks the volume Failed
    /// with "Recycle failed: ..." and a `Warning VolumeFailedRecycle` event;
    /// the volume is not unbound.
    #[tokio::test]
    async fn recycle_failure_marks_volume_failed() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Failed,
            Some("Pod was active on the node longer than specified deadline"),
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        let status = got.status.unwrap();
        assert_eq!(status.phase, PersistentVolumePhase::Failed);
        assert_eq!(
            status.message.as_deref(),
            Some("Recycle failed: failed to recycle volume: Pod was active on the node longer than specified deadline")
        );
        assert!(got.spec.claim_ref.is_some());
        let ev = events_with_reason(&storage, "VolumeFailedRecycle").await;
        assert_eq!(ev.len(), 1);
        assert!(events_with_reason(&storage, "VolumeRecycled")
            .await
            .is_empty());
    }

    /// recycle_test.go "6-9": a claim with the same name but another UID
    /// exists (recreated); the old volume is still recycled.
    #[tokio::test]
    async fn recycle_prebound_volume_while_a_same_name_claim_exists() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", false)).await;
        put_pvc(&storage, &make_pvc("c", "uid-x")).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// `nfsPlugin.Recycle`: NFS volumes have a recycler too.
    #[tokio::test]
    async fn recycle_nfs_volume_becomes_available() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = recycle_pv("pv", true);
        pv.spec.host_path = None;
        pv.spec.nfs = Some(rusternetes_common::resources::volume::NFSVolumeSource {
            server: "srv".into(),
            path: "/exp".into(),
            read_only: None,
        });
        put_pv(&storage, &pv).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// `scheduleOperation`/`goroutinemap`: while a recycle operation for a
    /// volume runs, another sync must not start a second one (which would
    /// find the recycler pod "already exists" and delete it).
    #[tokio::test]
    async fn recycle_operation_is_not_scheduled_twice() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        // The first operation is waiting on the (never started) recycler pod.
        let key = build_key("pods", Some("default"), "recycler-for-pv");
        tokio::time::timeout(Duration::from_secs(10), async {
            while storage.get::<Pod>(&key).await.is_err() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("recycler pod created");
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            storage.get::<Pod>(&key).await.is_ok(),
            "second sync must not touch the running recycler pod"
        );
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// "Do not overwrite previous Failed state" (pv_controller.go:672-681).
    #[tokio::test]
    async fn failed_pv_with_missing_claim_stays_failed() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = pv_with("pv", Some(Some("u")), Some(PersistentVolumePhase::Failed));
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Failed);
    }

    fn dynamic_pv(
        name: &str,
        policy: PersistentVolumeReclaimPolicy,
        finalizers: &[&str],
    ) -> PersistentVolume {
        let mut pv = pv_with(name, None, Some(PersistentVolumePhase::Available));
        pv.spec.persistent_volume_reclaim_policy = Some(policy);
        pv.metadata.annotations = Some(
            [(
                ANN_DYNAMICALLY_PROVISIONED.to_string(),
                "kubernetes.io/rbd".to_string(),
            )]
            .into(),
        );
        pv.metadata.finalizers = if finalizers.is_empty() {
            None
        } else {
            Some(finalizers.iter().map(|s| s.to_string()).collect())
        };
        pv
    }

    /// pv_controller_test.go "5-9" + TestModifyDeletionFinalizers 13-3..13-6,
    /// 13-12..13-14 (CSI migration disabled): `syncVolume` first runs
    /// `updateVolumeMigrationAnnotationsAndFinalizers`
    /// (pv_controller.go:565, pv_controller_base.go:361-381).
    #[tokio::test]
    async fn sync_volume_adds_in_tree_finalizer_and_drops_external_one() {
        use PersistentVolumeReclaimPolicy::*;
        let cases: Vec<(&str, PersistentVolume, Option<Vec<&str>>)> = vec![
            // 13-4: Delete, no finalizers -> in-tree finalizer added.
            (
                "a",
                dynamic_pv("a", Delete, &[]),
                Some(vec![IN_TREE_PV_DELETION_PROTECTION_FINALIZER]),
            ),
            // 13-6: external finalizer removed, custom kept, in-tree added.
            (
                "b",
                dynamic_pv(
                    "b",
                    Delete,
                    &[EXTERNAL_PV_DELETION_PROTECTION_FINALIZER, "custom"],
                ),
                Some(vec!["custom", IN_TREE_PV_DELETION_PROTECTION_FINALIZER]),
            ),
            // 13-12: Retain, nothing to add.
            ("c", dynamic_pv("c", Retain, &[]), None),
            // 13-14: Retain removes the in-tree finalizer.
            (
                "d",
                dynamic_pv("d", Retain, &[IN_TREE_PV_DELETION_PROTECTION_FINALIZER]),
                None,
            ),
        ];
        for (name, pv, want) in cases {
            let storage = Arc::new(MemoryStorage::new());
            let c = PVBinderController::new(storage.clone());
            put_pv(&storage, &pv).await;
            c.sync_volumes(&WorkQueue::new()).await.unwrap();
            let got = get_pv(&storage, name).await.unwrap();
            let want = want.map(|v| v.into_iter().map(String::from).collect::<Vec<_>>());
            assert_eq!(got.metadata.finalizers, want, "case {name}");
        }
    }

    /// 13-11 / 13-15 / 13-10: statically provisioned or unannotated volumes
    /// are never touched, even when they carry the external finalizer.
    #[tokio::test]
    async fn sync_volume_leaves_static_volume_finalizers_alone() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_with("s", None, Some(PersistentVolumePhase::Available));
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.metadata.finalizers = Some(vec![EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.into()]);
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "s").await.unwrap();
        assert_eq!(
            got.metadata.finalizers,
            Some(vec![EXTERNAL_PV_DELETION_PROTECTION_FINALIZER.to_string()])
        );
    }

    /// TestControllerSync 5-9 / updateMigrationAnnotations: the migrated-to
    /// annotation is removed when migration is off for the plugin.
    #[tokio::test]
    async fn sync_volume_removes_stale_migrated_to_annotation() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = dynamic_pv("m", PersistentVolumeReclaimPolicy::Retain, &[]);
        pv.metadata
            .annotations
            .as_mut()
            .unwrap()
            .insert(ANN_MIGRATED_TO.into(), "pd.csi.storage.gke.io".into());
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "m").await.unwrap();
        assert!(!got
            .metadata
            .annotations
            .unwrap_or_default()
            .contains_key(ANN_MIGRATED_TO));
    }

    /// pv_controller.go:1217-1222: an unrecognised reclaim policy fails the
    /// volume with a `VolumeUnknownReclaimPolicy` Warning event.
    #[tokio::test]
    async fn unknown_reclaim_policy_fails_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut raw = serde_json::to_value(bound_pv("u")).unwrap();
        raw["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Bogus");
        let pv: PersistentVolume = serde_json::from_value(raw).expect("lenient decode");
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let status = get_pv(&storage, "u").await.unwrap().status.unwrap();
        assert_eq!(status.phase, PersistentVolumePhase::Failed);
        assert_eq!(
            status.message.as_deref(),
            Some("Volume has unrecognized PersistentVolumeReclaimPolicy")
        );
        let events: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.reason == "VolumeUnknownReclaimPolicy")
                .count(),
            1
        );
    }

    /// binder_test.go "4-6": volume and claim bound to each other -> Bound.
    #[tokio::test]
    async fn pv_bound_to_claim_that_points_back_is_bound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(
            &storage,
            &pv_with(
                "pv",
                Some(Some("u")),
                Some(PersistentVolumePhase::Available),
            ),
        )
        .await;
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_name = Some("pv".into());
        put_pvc(&storage, &pvc).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Bound);
    }

    /// binder_test.go "14-13": a PV pre-bound to a claim with an incompatible
    /// volumeMode emits VolumeMismatch on BOTH objects (pv_controller.go:684-692).
    #[tokio::test]
    async fn volume_mode_mismatch_emits_events_on_pv_and_claim() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_mode = Some(PersistentVolumeMode::Block);
        put_pvc(&storage, &pvc).await;
        let queue = WorkQueue::new();
        c.sync_volumes(&queue).await.unwrap();
        let events = volume_mismatch_events(&storage).await;
        let pv_ev = events
            .iter()
            .find(|e| e.involved_object.kind.as_deref() == Some("PersistentVolume"))
            .expect("event on the PV");
        assert_eq!(
            pv_ev.message,
            "Cannot bind PersistentVolume to requested PersistentVolumeClaim \"c\" due to incompatible volumeMode."
        );
        let claim_ev = events
            .iter()
            .find(|e| e.involved_object.kind.as_deref() == Some("PersistentVolumeClaim"))
            .expect("event on the claim");
        assert_eq!(
            claim_ev.message,
            "Cannot bind PersistentVolume \"pv\" to requested PersistentVolumeClaim due to incompatible volumeMode."
        );
        // "Skipping syncClaim": the claim is NOT enqueued.
        let got = tokio::time::timeout(Duration::from_millis(100), queue.get()).await;
        assert!(got.is_err(), "claim must not be enqueued on mismatch");
    }

    /// pv_controller.go:694-707: a bound PV whose claim has no volumeName
    /// enqueues the claim so syncClaim fixes it promptly.
    #[tokio::test]
    async fn pv_with_unbound_claim_enqueues_the_claim() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        put_pvc(&storage, &make_pvc("c", "u")).await;
        let queue = WorkQueue::new();
        c.sync_volumes(&queue).await.unwrap();
        let key = tokio::time::timeout(Duration::from_millis(500), queue.get())
            .await
            .expect("claim enqueued");
        assert_eq!(key.as_deref(), Some("persistentvolumeclaims/sstest/c"));
        assert!(volume_mismatch_events(&storage).await.is_empty());
    }

    fn claim_bound_elsewhere() -> PersistentVolumeClaim {
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_name = Some("other-pv".into());
        pvc
    }

    /// binder_test.go "4-7": bound by the controller to a claim bound
    /// elsewhere -> unbind fully (claimRef + annotation cleared), Available.
    #[tokio::test]
    async fn controller_bound_pv_of_claim_bound_elsewhere_is_unbound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.metadata.annotations = Some(HashMap::from([(
            ANN_BOUND_BY_CONTROLLER.to_string(),
            "yes".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        put_pvc(&storage, &claim_bound_elsewhere()).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert!(got.spec.claim_ref.is_none());
        assert!(got.metadata.annotations.is_none());
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// binder_test.go "4-8": bound by the user -> only the UID is cleared.
    #[tokio::test]
    async fn user_bound_pv_of_claim_bound_elsewhere_keeps_prebind() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &bound_pv("pv")).await;
        put_pvc(&storage, &claim_bound_elsewhere()).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        let cr = got.spec.claim_ref.expect("still pre-bound");
        assert_eq!(cr.name.as_deref(), Some("c"));
        assert!(cr.uid.as_deref().unwrap_or("").is_empty());
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
    }

    /// binder_test.go "4-9": dynamically provisioned + Delete, claim bound
    /// elsewhere -> deleted.
    #[tokio::test]
    async fn dynamically_provisioned_delete_pv_of_claim_bound_elsewhere_is_deleted() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(PersistentVolumeReclaimPolicy::Delete);
        pv.metadata.annotations = Some(HashMap::from([(
            ANN_DYNAMICALLY_PROVISIONED.to_string(),
            "kubernetes.io/host-path".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        put_pvc(&storage, &claim_bound_elsewhere()).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// GetBindVolumeToClaim (component-helpers pv_helpers.go:120-154): a PV the
    /// controller binds (no prior claimRef) gets bound-by-controller; one
    /// pre-bound by the user does not.
    #[tokio::test]
    async fn binding_sets_bound_by_controller_only_when_not_prebound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut free = make_prebound_pv("free", "x", "x");
        free.spec.claim_ref = None;
        put_pv(&storage, &free).await;
        let mut pvc = make_pvc("c", "u");
        put_pvc(&storage, &pvc).await;
        c.bind_pvc(&mut pvc).await.unwrap();
        let got = get_pv(&storage, "free").await.unwrap();
        assert_eq!(
            got.metadata
                .annotations
                .unwrap()
                .get(ANN_BOUND_BY_CONTROLLER)
                .map(String::as_str),
            Some("yes")
        );

        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &make_prebound_pv("pre", "c", "u")).await;
        let mut pvc = make_pvc("c", "u");
        put_pvc(&storage, &pvc).await;
        c.bind_pvc(&mut pvc).await.unwrap();
        let got = get_pv(&storage, "pre").await.unwrap();
        assert!(got.metadata.annotations.is_none());
    }

    /// binder_test.go claim expectations (newClaimArray with
    /// annBoundByController + annBindCompleted): a claim the controller binds
    /// carries both annotations, persisted via the main resource.
    #[tokio::test]
    async fn binding_sets_claim_bound_by_controller_and_bind_completed() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut free = make_prebound_pv("free", "x", "x");
        free.spec.claim_ref = None;
        put_pv(&storage, &free).await;
        let mut pvc = make_pvc("c", "u");
        put_pvc(&storage, &pvc).await;
        c.bind_pvc(&mut pvc).await.unwrap();
        let key = build_key("persistentvolumeclaims", Some("sstest"), "c");
        let got: PersistentVolumeClaim = storage.get(&key).await.unwrap();
        let ann = got.metadata.annotations.unwrap();
        assert_eq!(
            ann.get(ANN_BOUND_BY_CONTROLLER).map(String::as_str),
            Some("yes")
        );
        assert_eq!(ann.get(ANN_BIND_COMPLETED).map(String::as_str), Some("yes"));
    }

    /// bindClaimToVolume: a claim already pointing at the volume (user
    /// pre-bound) is not marked bound-by-controller, only bind-completed.
    #[test]
    fn bind_claim_to_volume_skips_bound_by_controller_when_prebound() {
        let mut pvc = make_pvc("c", "u");
        pvc.spec.volume_name = Some("pv".into());
        bind_claim_to_volume(&mut pvc, "pv");
        let ann = pvc.metadata.annotations.unwrap();
        assert!(!ann.contains_key(ANN_BOUND_BY_CONTROLLER));
        assert_eq!(ann.get(ANN_BIND_COMPLETED).map(String::as_str), Some("yes"));
    }

    // ---- syncClaim bound/pre-bound branches (pv_controller.go:251, :331-490,
    // :492-560; binder_test.go sets 2 and 3) ----

    fn claim_with(
        name: &str,
        uid: &str,
        volume: Option<&str>,
        phase: PersistentVolumeClaimPhase,
        annotations: &[&str],
    ) -> PersistentVolumeClaim {
        let mut pvc = make_pvc(name, uid);
        pvc.spec.volume_name = volume.map(String::from);
        pvc.status.as_mut().unwrap().phase = phase;
        if !annotations.is_empty() {
            pvc.metadata.annotations = Some(
                annotations
                    .iter()
                    .map(|a| (a.to_string(), "yes".to_string()))
                    .collect(),
            );
        }
        pvc
    }

    fn pv_for(name: &str, claim: Option<(&str, &str)>) -> PersistentVolume {
        let mut pv = make_prebound_pv(name, "x", "x");
        pv.spec.claim_ref = claim.map(|(n, uid)| ObjectReference {
            name: Some(n.into()),
            uid: if uid.is_empty() {
                None
            } else {
                Some(uid.into())
            },
            ..pv.spec.claim_ref.clone().unwrap()
        });
        pv.status.as_mut().unwrap().phase = PersistentVolumePhase::Available;
        pv
    }

    async fn sync_claim(
        storage: &Arc<MemoryStorage>,
        pvc: PersistentVolumeClaim,
    ) -> (Result<()>, PersistentVolumeClaim) {
        put_pvc(storage, &pvc).await;
        let c = PVBinderController::new(storage.clone());
        let mut pvc = pvc;
        let r = c.bind_pvc(&mut pvc).await;
        let key = build_key(
            "persistentvolumeclaims",
            pvc.metadata.namespace.as_deref(),
            &pvc.metadata.name,
        );
        (r, storage.get(&key).await.unwrap())
    }

    async fn events_with_reason(
        storage: &Arc<MemoryStorage>,
        reason: &str,
    ) -> Vec<rusternetes_common::resources::Event> {
        let all: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        all.into_iter().filter(|e| e.reason == reason).collect()
    }

    fn phase_of(pvc: &PersistentVolumeClaim) -> PersistentVolumeClaimPhase {
        pvc.status.as_ref().unwrap().phase.clone()
    }

    /// binder_test.go "3-1".
    #[tokio::test]
    async fn bound_claim_with_missing_volume_name_is_lost() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            None,
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Lost);
        let ev = events_with_reason(&storage, "ClaimLost").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Bound claim has lost reference to PersistentVolume. Data on the volume is lost!"
        );
    }

    /// binder_test.go "3-2".
    #[tokio::test]
    async fn bound_claim_with_missing_volume_is_lost() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            Some("gone"),
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Lost);
        let ev = events_with_reason(&storage, "ClaimLost").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Bound claim has lost its PersistentVolume. Data on the volume is lost!"
        );
    }

    /// binder_test.go "3-3": volume has come unbound -> bound again.
    #[tokio::test]
    async fn bound_claim_with_unbound_volume_rebinds() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", None)).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("u"));
    }

    /// binder_test.go "3-4": claimRef with a different UID -> Lost/ClaimMisbound.
    #[tokio::test]
    async fn bound_claim_with_prebound_volume_of_other_uid_is_misbound() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("c", "c-x")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[ANN_BOUND_BY_CONTROLLER, ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Lost);
        let ev = events_with_reason(&storage, "ClaimMisbound").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Two claims are bound to the same volume, this one is bound incorrectly"
        );
        // The volume is left alone.
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("c-x"));
    }

    /// binder_test.go "3-5": claim and volume agree -> claim Bound.
    #[tokio::test]
    async fn bound_claim_with_bound_volume_becomes_bound() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("c", "u")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[ANN_BIND_COMPLETED],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        assert!(events_with_reason(&storage, "ClaimMisbound")
            .await
            .is_empty());
    }

    /// updateClaimStatusWithEvent (pv_controller.go:889-909): the event is
    /// emitted only when the phase actually changes.
    #[tokio::test]
    async fn lost_claim_event_is_emitted_once() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            None,
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BIND_COMPLETED],
        );
        let (_, got) = sync_claim(&storage, pvc).await;
        let c = PVBinderController::new(storage.clone());
        let mut got = got;
        c.bind_pvc(&mut got).await.unwrap();
        assert_eq!(events_with_reason(&storage, "ClaimLost").await.len(), 1);
    }

    /// binder_test.go "2-1"/"2-2": claim pre-bound to a missing volume stays
    /// Pending, and a stale Bound phase is reset to Pending.
    #[tokio::test]
    async fn prebound_claim_to_missing_volume_is_reset_to_pending() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with(
            "c",
            "u",
            Some("gone"),
            PersistentVolumeClaimPhase::Bound,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        assert!(got.status.unwrap().capacity.is_none());
    }

    /// binder_test.go "2-3": claim pre-bound to an unbound volume binds it,
    /// the PV is marked bound-by-controller, the claim only bind-completed.
    #[tokio::test]
    async fn prebound_claim_to_unbound_volume_binds() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", None)).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        let ann = got.metadata.annotations.unwrap();
        assert_eq!(ann.get(ANN_BIND_COMPLETED).map(String::as_str), Some("yes"));
        assert!(!ann.contains_key(ANN_BOUND_BY_CONTROLLER));
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("u"));
        assert!(pv
            .metadata
            .annotations
            .unwrap()
            .contains_key(ANN_BOUND_BY_CONTROLLER));
    }

    /// binder_test.go "2-4": a PV pre-bound to the claim by name (no UID) is
    /// finished: UID set, no bound-by-controller on the PV.
    #[tokio::test]
    async fn prebound_claim_to_prebound_volume_by_name_binds() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("c", "")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("u"));
        assert!(pv.metadata.annotations.is_none());
    }

    /// binder_test.go "2-6": volume bound to another claim, claim not bound by
    /// the controller -> reset to Pending, volume untouched.
    #[tokio::test]
    async fn prebound_claim_to_volume_of_other_claim_is_pending() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("other", "other-uid")))).await;
        let pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let pv = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(pv.spec.claim_ref.unwrap().uid.as_deref(), Some("other-uid"));
        assert_eq!(events_with_reason(&storage, "FailedBinding").await.len(), 1);
    }

    /// binder_test.go "2-7": same but the claim is bound-by-controller -> error.
    #[tokio::test]
    async fn controller_bound_claim_to_volume_of_other_claim_errors() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", Some(("other", "other-uid")))).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BOUND_BY_CONTROLLER],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        assert!(r.is_err());
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Bound);
    }

    /// binder_test.go "2-9": volume smaller than requested -> VolumeMismatch,
    /// claim reset to Pending, volume not bound.
    #[tokio::test]
    async fn prebound_claim_to_too_small_volume_is_mismatch() {
        let storage = Arc::new(MemoryStorage::new());
        put_pv(&storage, &pv_for("pv", None)).await;
        let mut pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        pvc.spec
            .resources
            .requests
            .as_mut()
            .unwrap()
            .insert("storage".into(), "2Gi".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "VolumeMismatch").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "Cannot bind to requested volume \"pv\": requested PV is too small"
        );
        assert!(get_pv(&storage, "pv")
            .await
            .unwrap()
            .spec
            .claim_ref
            .is_none());
    }

    /// binder_test.go "2-10": storageClassName differs.
    #[tokio::test]
    async fn prebound_claim_to_volume_of_other_class_is_mismatch() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.storage_class_name = Some("gold".into());
        put_pv(&storage, &pv).await;
        let pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "VolumeMismatch").await;
        assert_eq!(ev.len(), 1);
        assert!(ev[0].message.ends_with("storageClassName does not match"));
    }

    #[test]
    fn quantity_eq_compares_by_value() {
        assert!(quantity_eq("1Gi", "1024Mi"));
        assert!(quantity_eq("1.5Gi", "1536Mi"));
        assert!(quantity_eq("1G", "1000M"));
        assert!(quantity_eq("1e3", "1k"));
        assert!(quantity_eq("500m", "0.5"));
        assert!(!quantity_eq("1Gi", "1G"));
        assert_eq!(parse_quantity_nano("bogus!"), None);
        assert_eq!(parse_quantity_nano(""), None);
    }

    fn storage_cap(v: &str) -> HashMap<String, String> {
        HashMap::from([("storage".to_string(), v.to_string())])
    }

    /// binder_test.go "2-12" (VolumeAttributesClass, GA in 1.35): binding a
    /// claim to a volume with the same VAC sets
    /// `status.currentVolumeAttributesClassName` from the volume
    /// (`updateClaimStatus`, pv_controller.go:841-857).
    #[tokio::test]
    async fn binding_sets_current_vac_name_and_capacity_from_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.volume_attributes_class_name = Some("gold".into());
        put_pv(&storage, &pv).await;
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        pvc.spec.volume_attributes_class_name = Some("gold".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        let st = got.status.unwrap();
        assert_eq!(st.phase, PersistentVolumeClaimPhase::Bound);
        assert_eq!(
            st.current_volume_attributes_class_name.as_deref(),
            Some("gold")
        );
        assert_eq!(st.capacity, Some(storage_cap("1Gi")));
        assert_eq!(st.access_modes, Some(pv.spec.access_modes.clone()));
    }

    /// pv_controller_test.go "5-2-3": a PV with the pre-resize-capacity
    /// annotation makes the claim's status capacity that value (:826-837).
    #[tokio::test]
    async fn binding_uses_pre_resize_capacity_annotation() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = storage_cap("2Gi");
        pv.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "1Gi".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.status.unwrap().capacity, Some(storage_cap("1Gi")));
    }

    /// An unparsable pre-resize annotation falls back to the volume capacity
    /// (:830-833).
    #[tokio::test]
    async fn unparsable_pre_resize_capacity_falls_back_to_volume_capacity() {
        let storage = Arc::new(MemoryStorage::new());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = storage_cap("2Gi");
        pv.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "bogus!".to_string(),
        )]));
        put_pv(&storage, &pv).await;
        let pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.status.unwrap().capacity, Some(storage_cap("2Gi")));
    }

    /// :817-819: capacity is only copied when the phase changes; an already
    /// Bound claim keeps a status capacity that differs from the volume's (a
    /// filesystem not yet resized), and nothing is written.
    #[tokio::test]
    async fn update_claim_status_does_not_clobber_capacity_when_already_bound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = storage_cap("2Gi");
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Bound,
            &[ANN_BIND_COMPLETED],
        );
        {
            let st = pvc.status.as_mut().unwrap();
            st.capacity = Some(storage_cap("1Gi"));
            st.access_modes = Some(pv.spec.access_modes.clone());
        }
        put_pvc(&storage, &pvc).await;
        c.update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap();
        assert_eq!(
            pvc.status.as_ref().unwrap().capacity,
            Some(storage_cap("1Gi"))
        );
        assert!(pvc.status.as_ref().unwrap().access_modes.is_some());
    }

    /// `volumeCap.Cmp(claimCap) != 0` (:835): quantities are compared by
    /// value, so an equal-valued status capacity is left as it was.
    #[tokio::test]
    async fn update_claim_status_compares_capacity_by_quantity_value() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = pv_for("pv", None);
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        {
            let st = pvc.status.as_mut().unwrap();
            st.capacity = Some(storage_cap("1024Mi"));
            st.access_modes = Some(pv.spec.access_modes.clone());
        }
        put_pvc(&storage, &pvc).await;
        c.update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap();
        let st = pvc.status.unwrap();
        assert_eq!(st.phase, PersistentVolumeClaimPhase::Bound);
        // The write goes through the server, which hands back the canonical
        // quantity ("1Gi"); equivalence by value is what `Cmp` asserts.
        let cap = st.capacity.unwrap();
        assert!(quantity_eq(&cap["storage"], "1024Mi"), "{cap:?}");
    }

    /// :821-824: a volume without storage capacity is an error on the phase
    /// transition.
    #[tokio::test]
    async fn update_claim_status_errors_on_volume_without_capacity() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_for("pv", None);
        pv.spec.capacity = HashMap::new();
        let mut pvc = claim_with(
            "c",
            "u",
            Some("pv"),
            PersistentVolumeClaimPhase::Pending,
            &[],
        );
        put_pvc(&storage, &pvc).await;
        let e = c
            .update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "PersistentVolume \"pv\" is without a storage capacity"
        );
    }

    /// :841-857: currentVolumeAttributesClassName is set only on the
    /// Pending -> Bound transition, never afterwards.
    #[tokio::test]
    async fn current_vac_name_is_only_set_on_pending_to_bound() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = pv_for("pv", None);
        pv.spec.volume_attributes_class_name = Some("gold".into());
        let mut pvc = claim_with("c", "u", Some("pv"), PersistentVolumeClaimPhase::Bound, &[]);
        {
            let st = pvc.status.as_mut().unwrap();
            st.capacity = Some(storage_cap("1Gi"));
            st.access_modes = Some(pv.spec.access_modes.clone());
        }
        put_pvc(&storage, &pvc).await;
        c.update_claim_status(&mut pvc, PersistentVolumeClaimPhase::Bound, Some(&pv))
            .await
            .unwrap();
        assert_eq!(
            pvc.status.unwrap().current_volume_attributes_class_name,
            None
        );
    }

    fn claim_with_annotations(pairs: &[(&str, &str)]) -> PersistentVolumeClaim {
        let mut pvc = claim_with("c", "u", None, PersistentVolumeClaimPhase::Pending, &[]);
        pvc.metadata.annotations = Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        pvc
    }

    /// pv_controller_test.go TestAnnealMigrationAnnotations "migration off
    /// removes migrated to (rollback)" (and the Beta provisioner variant) for
    /// a claim, via `syncClaim` (`updateClaimMigrationAnnotations`,
    /// pv_controller.go:240) with the claim written back.
    #[tokio::test]
    async fn sync_claim_removes_stale_migrated_to_annotation() {
        for provisioner_key in [
            "volume.kubernetes.io/storage-provisioner",
            "volume.beta.kubernetes.io/storage-provisioner",
        ] {
            let storage = Arc::new(MemoryStorage::new());
            let pvc = claim_with_annotations(&[
                (provisioner_key, "kubernetes.io/rbd"),
                (ANN_MIGRATED_TO, "rbd.csi.ceph.com"),
            ]);
            let (r, got) = sync_claim(&storage, pvc).await;
            r.unwrap();
            let ann = got.metadata.annotations.unwrap();
            assert!(!ann.contains_key(ANN_MIGRATED_TO), "{provisioner_key}");
            assert!(ann.contains_key(provisioner_key));
        }
    }

    /// TestAnnealMigrationAnnotations "not dynamically provisioned": a claim
    /// without a provisioner annotation keeps its annotations untouched.
    #[tokio::test]
    async fn sync_claim_leaves_unprovisioned_claim_annotations_alone() {
        let storage = Arc::new(MemoryStorage::new());
        let pvc = claim_with_annotations(&[(ANN_MIGRATED_TO, "x.csi")]);
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(
            got.metadata
                .annotations
                .unwrap()
                .get(ANN_MIGRATED_TO)
                .map(String::as_str),
            Some("x.csi")
        );
    }

    // ---- syncUnboundClaim volumeName == "" branch with no match
    // (pv_controller.go:331-409) ----

    async fn put_class(storage: &Arc<MemoryStorage>, name: &str, default: bool, mode: &str) {
        let mut sc: serde_json::Value = serde_json::json!({
            "apiVersion": "storage.k8s.io/v1",
            "kind": "StorageClass",
            "metadata": {"name": name},
            "provisioner": "example.com/none",
            "volumeBindingMode": mode,
        });
        if default {
            sc["metadata"]["annotations"] =
                serde_json::json!({"storageclass.kubernetes.io/is-default-class": "true"});
        }
        let sc: rusternetes_common::resources::StorageClass = serde_json::from_value(sc).unwrap();
        storage
            .create(&build_key("storageclasses", None, name), &sc)
            .await
            .unwrap();
    }

    async fn put_pod(storage: &Arc<MemoryStorage>, name: &str, node: Option<&str>, claim: &str) {
        let pod: rusternetes_common::resources::Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": "sstest"},
            "spec": {
                "nodeName": node,
                "containers": [{"name": "c", "image": "i"}],
                "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]
            }
        }))
        .unwrap();
        storage
            .create(&build_key("pods", Some("sstest"), name), &pod)
            .await
            .unwrap();
    }

    fn classless_claim() -> PersistentVolumeClaim {
        let mut pvc = make_pvc("c", "u");
        pvc.spec.storage_class_name = None;
        pvc
    }

    /// `assignDefaultStorageClass` (pv_controller.go:967-994): the claim gets
    /// the default class written back and the sync stops there.
    #[tokio::test]
    async fn unbound_claim_without_class_gets_the_default_class() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "old", true, "Immediate").await;
        let (r, got) = sync_claim(&storage, classless_claim()).await;
        r.unwrap();
        assert_eq!(got.spec.storage_class_name.as_deref(), Some("old"));
        assert!(events_with_reason(&storage, "FailedBinding")
            .await
            .is_empty());
    }

    /// A claim that asks for a class (even via the beta annotation) is left alone.
    #[tokio::test]
    async fn unbound_claim_with_beta_class_annotation_is_not_defaulted() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "dflt", true, "Immediate").await;
        let mut pvc = classless_claim();
        pvc.metadata.annotations = Some(
            [(
                "volume.beta.kubernetes.io/storage-class".to_string(),
                "gold".to_string(),
            )]
            .into(),
        );
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.spec.storage_class_name, None);
    }

    /// pv_controller.go:371-373: no class, no default, no volume.
    #[tokio::test]
    async fn unbound_claim_without_any_class_emits_failed_binding_and_is_pending() {
        let storage = Arc::new(MemoryStorage::new());
        let (r, got) = sync_claim(&storage, classless_claim()).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "FailedBinding").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "no persistent volumes available for this claim and no storage class is set"
        );
        assert_eq!(
            ev[0].event_type,
            rusternetes_common::resources::EventType::Normal
        );
    }

    /// emitEventForUnboundDelayBindingClaim (:306-326), no pod yet.
    #[tokio::test]
    async fn delay_binding_claim_without_pod_waits_for_first_consumer() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "WaitForFirstConsumer").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].message,
            "waiting for first consumer to be created before binding"
        );
    }

    /// One unscheduled pod uses the claim; scheduled and terminated pods are
    /// not counted (findNonScheduledPodsByPVC, :1477-1493).
    #[tokio::test]
    async fn delay_binding_claim_names_the_unscheduled_pods() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        put_pod(&storage, "p1", None, "c").await;
        put_pod(&storage, "p-sched", Some("node-1"), "c").await;
        put_pod(&storage, "p-other", None, "other-claim").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, _) = sync_claim(&storage, pvc.clone()).await;
        r.unwrap();
        let ev = events_with_reason(&storage, "WaitForPodScheduled").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].message, "waiting for pod p1 to be scheduled");
    }

    #[tokio::test]
    async fn delay_binding_claim_names_all_unscheduled_pods() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        put_pod(&storage, "p1", None, "c").await;
        put_pod(&storage, "p2", None, "c").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, _) = sync_claim(&storage, pvc).await;
        r.unwrap();
        let ev = events_with_reason(&storage, "WaitForPodScheduled").await;
        assert_eq!(ev.len(), 1);
        assert!(
            ev[0].message == "waiting for pods p1,p2 to be scheduled"
                || ev[0].message == "waiting for pods p2,p1 to be scheduled"
        );
    }

    /// `IsDelayBindingProvisioning`: the scheduler has chosen a node, so the
    /// claim is provisioned instead of waiting (:361-364 falls through).
    #[tokio::test]
    async fn delay_binding_claim_with_selected_node_does_not_wait() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        pvc.metadata.annotations = Some(
            [(
                "volume.kubernetes.io/selected-node".to_string(),
                "node-1".to_string(),
            )]
            .into(),
        );
        let (r, _) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert!(events_with_reason(&storage, "WaitForFirstConsumer")
            .await
            .is_empty());
        assert!(events_with_reason(&storage, "FailedBinding")
            .await
            .is_empty());
    }

    /// `FindMatchingVolume` (component-helpers `pv_helpers.go:279-283`):
    /// `node == nil && delayBinding` skips every PV that is not already bound
    /// to the claim, so the scheduler's VolumeBinding plugin binds it with a
    /// node chosen. The PV controller must leave the unclaimed PV alone.
    #[tokio::test]
    async fn delay_binding_claim_does_not_bind_unclaimed_pv() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        let mut pv = pv_with("pv-free", None, Some(PersistentVolumePhase::Available));
        pv.spec.storage_class_name = Some("wffc".into());
        put_pv(&storage, &pv).await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.spec.volume_name, None);
        assert_eq!(phase_of(&got), PersistentVolumeClaimPhase::Pending);
        let ev = events_with_reason(&storage, "WaitForFirstConsumer").await;
        assert_eq!(ev.len(), 1);
    }

    /// Control for the skip above: an Immediate class still binds.
    #[tokio::test]
    async fn immediate_class_claim_binds_unclaimed_pv() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "imm", false, "Immediate").await;
        let mut pv = pv_with("pv-free", None, Some(PersistentVolumePhase::Available));
        pv.spec.storage_class_name = Some("imm".into());
        put_pv(&storage, &pv).await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("imm".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.spec.volume_name.as_deref(), Some("pv-free"));
    }

    /// `IsVolumeBoundToClaim` is checked BEFORE the delayBinding skip
    /// (`pv_helpers.go:263-277`): a PV the scheduler pre-bound (claimRef set)
    /// is completed by the PV controller even for a delay-binding class.
    #[tokio::test]
    async fn delay_binding_claim_binds_prebound_pv() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "wffc", false, "WaitForFirstConsumer").await;
        let mut pv = make_prebound_pv("pv-pre", "c", "u");
        pv.spec.storage_class_name = Some("wffc".into());
        put_pv(&storage, &pv).await;
        let mut pvc = classless_claim();
        pvc.spec.storage_class_name = Some("wffc".into());
        let (r, got) = sync_claim(&storage, pvc).await;
        r.unwrap();
        assert_eq!(got.spec.volume_name.as_deref(), Some("pv-pre"));
    }

    /// An Immediate class with no volume is provisioned by the dynamic
    /// provisioner (provisionClaim, :366-374): no event from this branch.
    #[tokio::test]
    async fn immediate_class_claim_without_volume_emits_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        put_class(&storage, "standard", false, "Immediate").await;
        let (r, _) = sync_claim(&storage, classless_claim().with_class("standard")).await;
        r.unwrap();
        assert!(events_with_reason(&storage, "FailedBinding")
            .await
            .is_empty());
        assert!(events_with_reason(&storage, "WaitForFirstConsumer")
            .await
            .is_empty());
    }

    trait WithClass {
        fn with_class(self, c: &str) -> Self;
    }
    impl WithClass for PersistentVolumeClaim {
        fn with_class(mut self, c: &str) -> Self {
            self.spec.storage_class_name = Some(c.into());
            self
        }
    }

    // ---- reclaimVolume: delete/recycle operations (pv_controller.go:1180-1540;
    // delete_test.go 8-*, recycle_test.go 6-*) ----

    fn reclaim_pv(
        policy: PersistentVolumeReclaimPolicy,
        provisioned_by: Option<&str>,
        host_path: bool,
    ) -> PersistentVolume {
        let mut pv = bound_pv("pv");
        pv.spec.persistent_volume_reclaim_policy = Some(policy);
        if host_path {
            pv.spec.host_path = Some(
                rusternetes_common::resources::volume::HostPathVolumeSource {
                    path: "/tmp/x".into(),
                    r#type: None,
                },
            );
        }
        if let Some(p) = provisioned_by {
            pv.metadata.annotations = Some(HashMap::from([(
                ANN_DYNAMICALLY_PROVISIONED.to_string(),
                p.to_string(),
            )]));
        }
        pv
    }

    async fn reason_events(
        storage: &Arc<MemoryStorage>,
        reason: &str,
    ) -> Vec<rusternetes_common::resources::Event> {
        let all: Vec<rusternetes_common::resources::Event> =
            storage.list("/registry/events/").await.unwrap();
        all.into_iter().filter(|e| e.reason == reason).collect()
    }

    /// delete_test.go "8-3": no deleter plugin for the volume -> Failed and
    /// `Warning VolumeFailedDelete`; the PV is NOT deleted.
    #[tokio::test]
    async fn delete_without_deleter_plugin_fails_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(
            &storage,
            &reclaim_pv(PersistentVolumeReclaimPolicy::Delete, None, false),
        )
        .await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let pv = get_pv(&storage, "pv").await.expect("PV must survive");
        assert_eq!(pv.status.unwrap().phase, PersistentVolumePhase::Failed);
        let ev = reason_events(&storage, "VolumeFailedDelete").await;
        assert_eq!(ev.len(), 1);
        assert!(ev[0].message.contains("no deletable volume plugin matched"));
    }

    /// delete_test.go "8-10-1": a volume provisioned by an external
    /// provisioner is left for the external deleter (findDeletablePlugin,
    /// pv_controller.go:1957-1969): stays Released, no event.
    #[tokio::test]
    async fn delete_of_externally_provisioned_volume_is_left_alone() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = reclaim_pv(
            PersistentVolumeReclaimPolicy::Delete,
            Some("ebs.csi.aws.com"),
            false,
        );
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.expect("PV must survive");
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
        assert!(reason_events(&storage, "VolumeFailedDelete")
            .await
            .is_empty());
    }

    /// findDeletablePlugin (pv_controller.go:1977-1980): a CSI volume source
    /// (no provisioned-by annotation) is deleted by the external-provisioner,
    /// never by this controller: stays Released, no event, PV kept.
    #[tokio::test]
    async fn delete_of_csi_source_volume_is_left_to_external_deleter() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let mut pv = reclaim_pv(PersistentVolumeReclaimPolicy::Delete, None, false);
        pv.spec.csi = Some(rusternetes_common::resources::volume::CSIVolumeSource {
            driver: "ebs.csi.aws.com".into(),
            volume_handle: Some("vol-1".into()),
            ..Default::default()
        });
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.expect("PV must survive");
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
        assert!(reason_events(&storage, "VolumeFailedDelete")
            .await
            .is_empty());
    }

    /// findDeletablePlugin (pv_controller.go:1973-1976): a `migrated-to` PV
    /// is left to the external deleter too.
    #[test]
    fn find_deletable_plugin_migrated_to_is_external() {
        let mut pv = reclaim_pv(PersistentVolumeReclaimPolicy::Delete, None, true);
        pv.metadata
            .annotations
            .get_or_insert_with(Default::default)
            .insert(ANN_MIGRATED_TO.to_string(), "ebs.csi.aws.com".into());
        assert_eq!(find_deletable_plugin(&pv), Ok(false));
    }

    /// An unknown `kubernetes.io/` provisioner is an error, not external
    /// (pv_controller.go:1962-1966).
    #[tokio::test]
    async fn delete_with_unknown_in_tree_provisioner_fails_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = reclaim_pv(
            PersistentVolumeReclaimPolicy::Delete,
            Some("kubernetes.io/nope"),
            true,
        );
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Failed);
        assert_eq!(reason_events(&storage, "VolumeFailedDelete").await.len(), 1);
    }

    /// delete_test.go "8-1" + doDeleteVolume (:1532-1538): the in-tree
    /// deletion-protection finalizer is removed, then the PV is deleted.
    #[tokio::test]
    async fn delete_with_deleter_plugin_deletes_volume() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = reclaim_pv(
            PersistentVolumeReclaimPolicy::Delete,
            Some("kubernetes.io/host-path"),
            true,
        );
        put_pv(&storage, &pv).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_none());
    }

    /// delete_test.go "8-7": the volume is bound again before deleteVolumeOperation
    /// runs: isVolumeReleased is false, nothing is deleted.
    #[tokio::test]
    async fn delete_operation_skips_volume_that_is_no_longer_released() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let pv = reclaim_pv(PersistentVolumeReclaimPolicy::Delete, None, true);
        put_pv(&storage, &pv).await;
        let mut claim = make_pvc("c", "u");
        claim.spec.volume_name = Some("pv".into());
        put_pvc(&storage, &claim).await;
        c.reclaim_volume(&pv).await.unwrap();
        assert!(get_pv(&storage, "pv").await.is_some());
        // 8-6: volume deleted before deleting is not an error.
        storage
            .delete(&build_key("persistentvolumes", None, "pv"))
            .await
            .unwrap();
        c.reclaim_volume(&pv).await.unwrap();
    }

    async fn put_pod_json(storage: &Arc<MemoryStorage>, name: &str, phase: Option<&str>) {
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "namespace": "sstest"},
            "spec": {
                "nodeName": "n",
                "containers": [{"name": "c", "image": "i"}],
                "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": "c"}}]
            },
            "status": {"phase": phase}
        }))
        .unwrap();
        storage
            .create(&build_key("pods", Some("sstest"), name), &pod)
            .await
            .unwrap();
    }

    /// recycle_test.go "6-11": released volume still used by a running pod and
    /// its claim gone -> `Normal VolumeFailedRecycle` naming the pods, no
    /// phase change beyond Released (pv_controller.go:1265-1270).
    #[tokio::test]
    async fn recycle_of_volume_used_by_running_pod_emits_event_and_waits() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        put_pod_json(&storage, "p", Some("Running")).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
        let ev = reason_events(&storage, "VolumeFailedRecycle").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].message, "Volume is used by pods: sstest/p");
        assert_eq!(
            ev[0].event_type,
            rusternetes_common::resources::EventType::Normal
        );
        assert!(
            storage
                .get::<Pod>(&build_key("pods", Some("default"), "recycler-for-pv"))
                .await
                .is_err(),
            "no recycler pod may run while a pod uses the volume"
        );
    }

    /// recycle_test.go "6-12": a pending pod holds the volume too.
    #[tokio::test]
    async fn recycle_of_volume_used_by_pending_pod_emits_event_and_waits() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        put_pod_json(&storage, "p", Some("Pending")).await;
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Released);
        let ev = reason_events(&storage, "VolumeFailedRecycle").await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].message, "Volume is used by pods: sstest/p");
    }

    /// recycle_test.go "6-13": a completed pod does not hold the volume, so
    /// the recycler runs and the volume becomes Available.
    #[tokio::test]
    async fn recycle_ignores_completed_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        put_pod_json(&storage, "p", Some("Succeeded")).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
        assert!(got.spec.claim_ref.is_none());
        assert!(reason_events(&storage, "VolumeFailedRecycle")
            .await
            .is_empty());
    }

    /// recycle_test.go "6-14": a pod uses the claim name, but a (different)
    /// claim with that name exists, so the old PV is safe to recycle.
    #[tokio::test]
    async fn recycle_proceeds_when_a_same_name_claim_exists() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        put_pv(&storage, &recycle_pv("pv", true)).await;
        put_pod_json(&storage, "p", Some("Running")).await;
        put_pvc(&storage, &make_pvc("c", "other-uid")).await;
        let kubelet = fake_kubelet(
            &storage,
            "pv",
            rusternetes_common::types::Phase::Succeeded,
            None,
        );
        c.sync_volumes(&WorkQueue::new()).await.unwrap();
        settle(&c).await;
        kubelet.await.unwrap();
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
        assert!(reason_events(&storage, "VolumeFailedRecycle")
            .await
            .is_empty());
    }

    /// recycle_test.go "6-6": the volume is deleted before the operation
    /// starts: nothing happens and no recycler pod is created
    /// (pv_controller.go:1231-1238).
    #[tokio::test]
    async fn recycle_operation_skips_volume_deleted_before_it_starts() {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        c.recycle_volume_operation(recycle_pv("pv", true)).await;
        assert!(get_pv(&storage, "pv").await.is_none());
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("default"), "recycler-for-pv"))
            .await
            .is_err());
        assert!(reason_events(&storage, "VolumeFailedRecycle")
            .await
            .is_empty());
    }

    /// Stale volume passed to the operation, current copy already recycled by
    /// a previous operation (`isVolumeReleased` is false): skipped.
    async fn recycle_stale_volume_is_skipped(by_controller: bool) {
        let storage = Arc::new(MemoryStorage::new());
        let c = PVBinderController::new(storage.clone());
        let stale = recycle_pv("pv", by_controller);
        let mut current = stale.clone();
        // What `unbindVolume` leaves behind (`:1140-1178`).
        if by_controller {
            current.spec.claim_ref = None;
            current.metadata.annotations = None;
        } else if let Some(cr) = current.spec.claim_ref.as_mut() {
            cr.uid = None;
        }
        current.status = Some(rusternetes_common::resources::PersistentVolumeStatus {
            phase: PersistentVolumePhase::Available,
            ..Default::default()
        });
        put_pv(&storage, &current).await;
        c.recycle_volume_operation(stale).await;
        let got = get_pv(&storage, "pv").await.unwrap();
        assert_eq!(got.status.unwrap().phase, PersistentVolumePhase::Available);
        assert!(storage
            .get::<Pod>(&build_key("pods", Some("default"), "recycler-for-pv"))
            .await
            .is_err());
        assert!(reason_events(&storage, "VolumeFailedRecycle")
            .await
            .is_empty());
        assert!(reason_events(&storage, "VolumeRecycled").await.is_empty());
    }

    /// recycle_test.go "6-7": "volume no longer needs recycling, skipping"
    /// for a volume bound by the controller.
    #[tokio::test]
    async fn recycle_operation_skips_volume_recycled_by_previous_operation() {
        recycle_stale_volume_is_skipped(true).await;
    }

    /// recycle_test.go "6-8": the same for a volume bound by the user.
    #[tokio::test]
    async fn recycle_operation_skips_prebound_volume_recycled_by_previous_operation() {
        recycle_stale_volume_is_skipped(false).await;
    }
}
