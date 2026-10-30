//! Unit tests moved from `src/controllers/pv_protection.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::volume::{
        PersistentVolumePhase, PV_PROTECTION_FINALIZER,
    };
    pub use rusternetes_common::resources::PersistentVolume;
    pub use rusternetes_controller_manager::controllers::pvc_protection::{
        is_deletion_candidate, need_to_add_finalizer, update_or_reap,
    };
    pub use rusternetes_storage::{build_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::pv_protection::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    const FIN: &str = "kubernetes.io/pv-protection";

    fn pv(finalizer: bool, deleting: bool, phase: &str) -> PersistentVolume {
        let mut meta = json!({"name": "pv1", "uid": "u"});
        if finalizer {
            meta["finalizers"] = json!([FIN]);
        }
        if deleting {
            meta["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": meta, "spec": {}, "status": {"phase": phase}
        }))
        .unwrap()
    }

    async fn setup(
        v: &PersistentVolume,
    ) -> (Arc<MemoryStorage>, PvProtectionController<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(&build_key("persistentvolumes", None, "pv1"), v)
            .await
            .unwrap();
        let c = PvProtectionController::new(storage.clone());
        (storage, c)
    }

    async fn finalizers(s: &MemoryStorage) -> Option<Vec<String>> {
        s.get::<PersistentVolume>(&build_key("persistentvolumes", None, "pv1"))
            .await
            .ok()
            .map(|v| v.metadata.finalizers.unwrap_or_default())
    }

    #[tokio::test]
    async fn pv_without_finalizer_gets_it() {
        let (s, c) = setup(&pv(false, false, "Available")).await;
        c.process_pv("pv1").await.unwrap();
        assert_eq!(finalizers(&s).await.unwrap(), vec![FIN.to_string()]);
    }

    #[tokio::test]
    async fn pv_with_finalizer_is_left_alone() {
        let (s, c) = setup(&pv(true, false, "Available")).await;
        c.process_pv("pv1").await.unwrap();
        assert_eq!(finalizers(&s).await.unwrap(), vec![FIN.to_string()]);
    }

    #[tokio::test]
    async fn missing_pv_is_ignored() {
        let c = PvProtectionController::new(Arc::new(MemoryStorage::new()));
        c.process_pv("gone").await.unwrap();
    }

    #[tokio::test]
    async fn deleted_unbound_pv_is_reaped() {
        let (s, c) = setup(&pv(true, true, "Released")).await;
        c.process_pv("pv1").await.unwrap();
        assert!(finalizers(&s).await.is_none());
    }

    #[tokio::test]
    async fn deleted_bound_pv_is_kept() {
        let (s, c) = setup(&pv(true, true, "Bound")).await;
        c.process_pv("pv1").await.unwrap();
        assert_eq!(finalizers(&s).await.unwrap(), vec![FIN.to_string()]);
    }
}
