//! Unit tests moved from `src/controllers/vac_protection.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::csi::VolumeAttributesClass;
    pub use rusternetes_common::resources::volume::VAC_PROTECTION_FINALIZER;
    pub use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim};
    pub use rusternetes_controller_manager::controllers::pvc_protection::{
        is_deletion_candidate, need_to_add_finalizer, update_or_reap,
    };
    pub use rusternetes_storage::{build_key, build_prefix, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::vac_protection::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::{json, Value};

    const FIN: &str = "kubernetes.io/vac-protection";

    fn vac(finalizer: bool, terminating: bool) -> VolumeAttributesClass {
        let mut meta = json!({"name": "vac1", "uid": "u"});
        if finalizer {
            meta["finalizers"] = json!([FIN]);
        }
        if terminating {
            meta["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        }
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttributesClass",
            "metadata": meta, "driverName": "d"
        }))
        .unwrap()
    }

    fn pv(name: &str, vac: &str) -> PersistentVolume {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name}, "spec": {"volumeAttributesClassName": vac}
        }))
        .unwrap()
    }

    fn pvc(name: &str, spec: &str, status: Value) -> PersistentVolumeClaim {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"volumeAttributesClassName": spec},
            "status": status
        }))
        .unwrap()
    }

    async fn setup(
        v: &VolumeAttributesClass,
    ) -> (Arc<MemoryStorage>, VacProtectionController<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(&build_key("volumeattributesclasses", None, "vac1"), v)
            .await
            .unwrap();
        let c = VacProtectionController::new(storage.clone());
        (storage, c)
    }

    async fn finalizers(s: &MemoryStorage) -> Option<Vec<String>> {
        s.get::<VolumeAttributesClass>(&build_key("volumeattributesclasses", None, "vac1"))
            .await
            .ok()
            .map(|v| v.metadata.finalizers.unwrap_or_default())
    }

    // "VAC without finalizer -> finalizer is added"
    #[tokio::test]
    async fn vac_without_finalizer_gets_it() {
        let (s, c) = setup(&vac(false, false)).await;
        c.process_vac("vac1").await.unwrap();
        assert_eq!(finalizers(&s).await.unwrap(), vec![FIN.to_string()]);
    }

    // "VAC with finalizer -> no action"
    #[tokio::test]
    async fn vac_with_finalizer_is_left_alone() {
        let (s, c) = setup(&vac(true, false)).await;
        c.process_vac("vac1").await.unwrap();
        assert_eq!(finalizers(&s).await.unwrap(), vec![FIN.to_string()]);
    }

    #[tokio::test]
    async fn missing_vac_is_ignored() {
        let c = VacProtectionController::new(Arc::new(MemoryStorage::new()));
        c.process_vac("gone").await.unwrap();
    }

    // "deleted VAC with finalizer -> finalizer is removed"
    #[tokio::test]
    async fn deleted_unused_vac_is_reaped() {
        let (s, c) = setup(&vac(true, true)).await;
        c.process_vac("vac1").await.unwrap();
        assert!(finalizers(&s).await.is_none());
    }

    // "deleted VAC with finalizer but it's referenced by a PV -> not removed"
    #[tokio::test]
    async fn deleted_vac_referenced_by_pv_is_kept() {
        let (s, c) = setup(&vac(true, true)).await;
        s.create(
            &build_key("persistentvolumes", None, "pv1"),
            &pv("pv1", "vac1"),
        )
        .await
        .unwrap();
        c.process_vac("vac1").await.unwrap();
        assert_eq!(finalizers(&s).await.unwrap(), vec![FIN.to_string()]);
    }

    // PV referencing a different class does not hold vac1 (pv1WithVAC2).
    #[tokio::test]
    async fn pv_with_other_vac_does_not_hold() {
        let (s, c) = setup(&vac(true, true)).await;
        s.create(
            &build_key("persistentvolumes", None, "pv1"),
            &pv("pv1", "vac2"),
        )
        .await
        .unwrap();
        c.process_vac("vac1").await.unwrap();
        assert!(finalizers(&s).await.is_none());
    }

    async fn pvc_case(spec: &str, status: Value) -> Option<Vec<String>> {
        let (s, c) = setup(&vac(true, true)).await;
        s.create(
            &build_key("persistentvolumeclaims", Some("default"), "pvc1"),
            &pvc("pvc1", spec, status),
        )
        .await
        .unwrap();
        c.process_vac("vac1").await.unwrap();
        finalizers(&s).await
    }

    // "...referenced by a PVC -> finalizer is not removed"
    #[tokio::test]
    async fn deleted_vac_referenced_by_pvc_spec_is_kept() {
        assert!(pvc_case("vac1", json!({})).await.is_some());
    }

    // "pvc changes vac but its status still holds terminating vac"
    #[tokio::test]
    async fn deleted_vac_referenced_by_pvc_current_status_is_kept() {
        let st = json!({"currentVolumeAttributesClassName": "vac1"});
        assert!(pvc_case("vac2", st).await.is_some());
    }

    // "pvc changes vac but its target status still holds terminating vac"
    #[tokio::test]
    async fn deleted_vac_referenced_by_pvc_target_status_is_kept() {
        let st = json!({"modifyVolumeStatus": {
            "targetVolumeAttributesClassName": "vac1", "status": "Pending"}});
        assert!(pvc_case("vac2", st).await.is_some());
    }

    // "pvc changes vac and deleted VAC with finalizer -> finalizer is removed"
    #[tokio::test]
    async fn deleted_vac_with_pvc_on_other_vac_is_reaped() {
        assert!(pvc_case("vac2", json!({})).await.is_none());
    }
}
