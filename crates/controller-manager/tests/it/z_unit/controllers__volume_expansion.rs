//! Unit tests moved from `src/controllers/volume_expansion.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::PersistentVolumeClaimPhase;
    pub use rusternetes_common::resources::{EventSource, EventType};
    pub use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim};
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, extract_key, EventRecorder, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::Event;
    use rusternetes_controller_manager::controllers::volume_expansion::*;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    // Port of `TestSyncHandler` (pkg/controller/volume/expand/expand_controller_test.go:48-157)
    // and its fixtures `getFakePersistentVolume` / `getFakePersistentVolumeClaim` (:175-).

    fn pv(name: &str, driver: Option<&str>, size: &str, claim_uid: &str) -> PersistentVolume {
        let mut spec = serde_json::json!({
            "capacity": {"storage": size},
            "claimRef": {"namespace": "default", "uid": claim_uid},
        });
        match driver {
            Some(d) => spec["csi"] = serde_json::json!({"driver": d, "volumeHandle": "h"}),
            None => spec["hostPath"] = serde_json::json!({"path": "/tmp/x"}),
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name}, "spec": spec,
        }))
        .unwrap()
    }

    fn pvc(
        name: &str,
        volume: &str,
        status: &str,
        request: &str,
        uid: &str,
    ) -> PersistentVolumeClaim {
        let mut v = serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "default", "uid": uid},
            "spec": {"storageClassName": "sc", "resources": {"requests": {"storage": request}}},
            "status": {"phase": "Bound", "capacity": {"storage": status}},
        });
        if !volume.is_empty() {
            v["spec"]["volumeName"] = volume.into();
        }
        serde_json::from_value(v).unwrap()
    }

    async fn setup(
        pv_: Option<PersistentVolume>,
        pvc_: &PersistentVolumeClaim,
    ) -> (Arc<MemoryStorage>, VolumeExpansionController<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::new());
        if let Some(pv_) = pv_ {
            storage
                .create(
                    &build_key("persistentvolumes", None, &pv_.metadata.name),
                    &pv_,
                )
                .await
                .unwrap();
        }
        storage
            .create(
                &build_key(
                    "persistentvolumeclaims",
                    Some("default"),
                    &pvc_.metadata.name,
                ),
                pvc_,
            )
            .await
            .unwrap();
        let c = VolumeExpansionController::new(Arc::clone(&storage));
        (storage, c)
    }

    async fn events(storage: &Arc<MemoryStorage>) -> Vec<Event> {
        storage
            .list::<Event>("/registry/events/default/")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn pvc_with_no_pv_binding_is_an_error() {
        // "when pvc has no PV binding": hasError (expand_controller_test.go:58-63);
        // upstream looks the PV up BEFORE comparing sizes (expand_controller.go:215-220).
        let c1 = pvc("no-pv-pvc", "", "1Gi", "1Gi", "u1");
        let (_s, c) = setup(None, &c1).await;
        assert!(c.reconcile_pvc(&c1).await.is_err());
    }

    #[tokio::test]
    async fn pv_not_bound_to_this_pvc_is_an_error() {
        // `pv.Spec.ClaimRef == nil || pvc.Namespace != ... || pvc.UID != ...`
        // -> "persistent Volume is not bound to PVC being updated" (:222-226).
        let c1 = pvc("p", "vol", "1Gi", "2Gi", "uid-mine");
        let (_s, c) = setup(
            Some(pv("vol", Some("com.csi.ceph"), "1Gi", "uid-other")),
            &c1,
        )
        .await;
        assert!(c.reconcile_pvc(&c1).await.is_err());

        let mut unbound = pv("vol", Some("com.csi.ceph"), "1Gi", "uid-mine");
        unbound.spec.claim_ref = None;
        let (_s, c) = setup(Some(unbound), &c1).await;
        assert!(c.reconcile_pvc(&c1).await.is_err());
    }

    #[tokio::test]
    async fn csi_pv_is_not_resized_in_controller() {
        // "for csi plugin without migration path": expansionCalled false, no error
        // (:84-91). No expandable in-tree plugin -> ExternalExpanding event and the
        // external resizer does the work (expand_controller.go:262-273).
        let c1 = pvc("ceph-csi-pvc", "vol-5", "1Gi", "2Gi", "u5");
        let (s, c) = setup(Some(pv("vol-5", Some("com.csi.ceph"), "1Gi", "u5")), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();

        let stored_pv: PersistentVolume = s
            .get(&build_key("persistentvolumes", None, "vol-5"))
            .await
            .unwrap();
        assert_eq!(
            stored_pv.spec.capacity["storage"], "1Gi",
            "CSI PV must not be resized here"
        );
        let stored: PersistentVolumeClaim = s
            .get(&build_key(
                "persistentvolumeclaims",
                Some("default"),
                "ceph-csi-pvc",
            ))
            .await
            .unwrap();
        let st = stored.status.unwrap();
        assert_eq!(st.capacity.unwrap()["storage"], "1Gi");
        assert!(st.resize_status.is_none() && st.allocated_resources.is_none());
        let evs = events(&s).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "ExternalExpanding");
        assert_eq!(
            evs[0].message,
            "waiting for an external controller to expand this PVC"
        );
    }

    #[tokio::test]
    async fn pre_resize_annotation_triggers_sync_even_when_sizes_match() {
        // "if pv has pre-resize capacity annotation" (:72-83): the early return at
        // :228-236 is skipped, so the CSI PV still reaches ExternalExpanding.
        let c1 = pvc("p", "vol-4", "2Gi", "2Gi", "u4");
        let mut p = pv("vol-4", Some("com.csi.ceph"), "2Gi", "u4");
        p.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "1Gi".to_string(),
        )]));
        let (s, c) = setup(Some(p), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();
        assert_eq!(events(&s).await.len(), 1);
    }

    #[tokio::test]
    async fn equal_sizes_in_different_units_do_not_expand() {
        // `pvcRequestSize.Cmp(pvcStatusSize) <= 0` is resource.Quantity (:235): 1Gi == 1024Mi.
        let c1 = pvc("p", "vol", "1024Mi", "1Gi", "u");
        let (s, c) = setup(Some(pv("vol", Some("com.csi.ceph"), "1Gi", "u")), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();
        assert!(events(&s).await.is_empty());
    }

    #[tokio::test]
    async fn larger_request_in_different_units_expands() {
        // 2Gi > 1500Mi, 1Gi > 1G: the old string compare returned false on unit mismatch.
        for (status, request) in [("1500Mi", "2Gi"), ("1G", "1Gi")] {
            let c1 = pvc("p", "vol", status, request, "u");
            let (s, c) = setup(Some(pv("vol", Some("com.csi.ceph"), "1Gi", "u")), &c1).await;
            c.reconcile_pvc(&c1).await.unwrap();
            assert_eq!(events(&s).await.len(), 1, "{status} -> {request}");
        }
    }

    #[tokio::test]
    async fn hostpath_pv_is_not_resized_in_controller() {
        // No expandable in-tree plugin exists for hostPath, so
        // `FindExpandablePluginBySpec` returns nil and the controller only emits
        // ExternalExpanding and returns nil (expand_controller.go:271-283). It must
        // not write PV capacity or PVC status.
        let c1 = pvc("p", "vol-hp", "5Gi", "10Gi", "u-hp");
        let (s, c) = setup(Some(pv("vol-hp", None, "5Gi", "u-hp")), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();

        let stored_pv: PersistentVolume = s
            .get(&build_key("persistentvolumes", None, "vol-hp"))
            .await
            .unwrap();
        assert_eq!(stored_pv.spec.capacity["storage"], "5Gi");
        let stored: PersistentVolumeClaim = s
            .get(&build_key("persistentvolumeclaims", Some("default"), "p"))
            .await
            .unwrap();
        let st = stored.status.unwrap();
        assert_eq!(st.capacity.unwrap()["storage"], "5Gi");
        assert!(st.resize_status.is_none() && st.allocated_resources.is_none());
        let evs = events(&s).await;
        assert_eq!(evs.len(), 1, "{evs:?}");
        assert_eq!(evs[0].reason, "ExternalExpanding");
        assert_eq!(
            evs[0].message,
            "waiting for an external controller to expand this PVC"
        );
    }

    fn migratable_pv(name: &str, source: &str, claim_uid: &str) -> PersistentVolume {
        let mut spec = serde_json::json!({
            "capacity": {"storage": "1Gi"},
            "claimRef": {"namespace": "default", "uid": claim_uid},
        });
        match source {
            "kubernetes.io/aws-ebs" => {
                spec["awsElasticBlockStore"] = serde_json::json!({"volumeID": "v"})
            }
            "kubernetes.io/gce-pd" => {
                spec["gcePersistentDisk"] = serde_json::json!({"pdName": "d"})
            }
            "kubernetes.io/azure-disk" => {
                spec["azureDisk"] = serde_json::json!({"diskName": "d", "diskURI": "u"})
            }
            "kubernetes.io/azure-file" => {
                spec["azureFile"] = serde_json::json!({"secretName": "s", "shareName": "n"})
            }
            "kubernetes.io/cinder" => spec["cinder"] = serde_json::json!({"volumeID": "v"}),
            "kubernetes.io/vsphere-volume" => {
                spec["vsphereVolume"] = serde_json::json!({"volumePath": "[ds] vol.vmdk"})
            }
            "kubernetes.io/portworx-volume" => {
                spec["portworxVolume"] = serde_json::json!({"volumeID": "v"})
            }
            _ => unreachable!(),
        }
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name}, "spec": spec,
        }))
        .unwrap()
    }

    async fn stored_pvc(s: &Arc<MemoryStorage>, name: &str) -> PersistentVolumeClaim {
        s.get(&build_key("persistentvolumeclaims", Some("default"), name))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn csi_migrated_in_tree_pv_sets_resizer_annotation() {
        // TestSyncHandler "when pvc and pv has everything for in-tree plugin"
        // (expand_controller_test.go:65-71): expectedAnnotation
        // {VolumeResizerKey: AWSEBSDriverName}; GCE PD likewise.
        for (plugin, driver) in [
            ("kubernetes.io/aws-ebs", "ebs.csi.aws.com"),
            ("kubernetes.io/gce-pd", "pd.csi.storage.gke.io"),
            ("kubernetes.io/azure-disk", "disk.csi.azure.com"),
            ("kubernetes.io/azure-file", "file.csi.azure.com"),
            ("kubernetes.io/cinder", "cinder.csi.openstack.org"),
            ("kubernetes.io/vsphere-volume", "csi.vsphere.vmware.com"),
            ("kubernetes.io/portworx-volume", "pxd.portworx.com"),
        ] {
            let c1 = pvc("good-pvc", "vol-3", "1Gi", "2Gi", "u3");
            let (s, c) = setup(Some(migratable_pv("vol-3", plugin, "u3")), &c1).await;
            c.reconcile_pvc(&c1).await.unwrap();

            let ann = stored_pvc(&s, "good-pvc").await.metadata.annotations;
            assert_eq!(
                ann,
                Some(HashMap::from([(
                    "volume.kubernetes.io/storage-resizer".to_string(),
                    driver.to_string()
                )])),
                "{plugin}"
            );
            let evs = events(&s).await;
            assert_eq!(evs.len(), 1, "{evs:?}");
            assert_eq!(evs[0].reason, "ExternalExpanding");
            assert_eq!(
                evs[0].message,
                format!("CSI migration enabled for {plugin}; waiting for external resizer to expand the pvc")
            );
        }
    }

    #[tokio::test]
    async fn migrated_pv_with_pre_resize_annotation_still_sets_resizer() {
        // "if pv has pre-resize capacity annotation" (:73-83) with an AWS EBS PV:
        // the :228-236 early return is skipped, so the migratable branch runs.
        let c1 = pvc("p", "vol-4", "2Gi", "2Gi", "u4");
        let mut p = migratable_pv("vol-4", "kubernetes.io/aws-ebs", "u4");
        p.metadata.annotations = Some(HashMap::from([(
            "volume.alpha.kubernetes.io/pre-resize-capacity".to_string(),
            "1Gi".to_string(),
        )]));
        let (s, c) = setup(Some(p), &c1).await;
        c.reconcile_pvc(&c1).await.unwrap();
        assert!(stored_pvc(&s, "p")
            .await
            .metadata
            .annotations
            .is_some_and(|a| a.get("volume.kubernetes.io/storage-resizer")
                == Some(&"ebs.csi.aws.com".to_string())));
    }

    #[tokio::test]
    async fn migrated_pv_with_satisfied_request_is_left_alone() {
        // :228-236 returns before the migration branch.
        let c1 = pvc("p", "vol", "1Gi", "1Gi", "u");
        let (s, c) = setup(
            Some(migratable_pv("vol", "kubernetes.io/aws-ebs", "u")),
            &c1,
        )
        .await;
        c.reconcile_pvc(&c1).await.unwrap();
        assert!(stored_pvc(&s, "p").await.metadata.annotations.is_none());
        assert!(events(&s).await.is_empty());
    }

    #[tokio::test]
    async fn storage_class_is_not_consulted() {
        // upstream never reads the StorageClass in syncHandler; allowVolumeExpansion
        // is enforced by the PVC admission plugin. A missing class is not an error.
        let c1 = pvc("p", "vol", "1Gi", "2Gi", "u");
        let (s, c) = setup(Some(pv("vol", None, "1Gi", "u")), &c1).await;
        c.reconcile_all().await.unwrap();
        assert_eq!(events(&s).await.len(), 1);
    }
}
