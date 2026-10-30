//! Unit tests moved from `src/controllers/pvc_protection.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_common::resources::volume::PVC_PROTECTION_FINALIZER;
    pub use rusternetes_common::resources::{PersistentVolumeClaim, Pod};
    pub use rusternetes_common::types::ObjectMeta;
    pub use rusternetes_storage::{build_key, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::pvc_protection::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    const FIN: &str = "kubernetes.io/pvc-protection";

    fn pvc(name: &str, finalizer: bool, deleting: bool) -> PersistentVolumeClaim {
        let mut meta = json!({"name": name, "namespace": "ns", "uid": format!("uid-{name}")});
        if finalizer {
            meta["finalizers"] = json!([FIN]);
        }
        if deleting {
            meta["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": meta, "spec": {}
        }))
        .unwrap()
    }

    fn pod(name: &str, node: &str, claim: &str) -> Pod {
        let mut spec = json!({"containers": [],
            "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]});
        if !node.is_empty() {
            spec["nodeName"] = json!(node);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "ns", "uid": format!("uid-{name}")},
            "spec": spec
        }))
        .unwrap()
    }

    async fn setup(
        pvcs: &[PersistentVolumeClaim],
        pods: &[Pod],
    ) -> (Arc<MemoryStorage>, PvcProtectionController<MemoryStorage>) {
        let storage = Arc::new(MemoryStorage::new());
        for c in pvcs {
            let key = build_key("persistentvolumeclaims", Some("ns"), &c.metadata.name);
            storage.create(&key, c).await.unwrap();
        }
        for p in pods {
            let key = build_key("pods", Some("ns"), &p.metadata.name);
            storage.create(&key, p).await.unwrap();
        }
        let c = PvcProtectionController::new(storage.clone());
        (storage, c)
    }

    async fn finalizers(s: &MemoryStorage, name: &str) -> Option<Vec<String>> {
        let key = build_key("persistentvolumeclaims", Some("ns"), name);
        s.get::<PersistentVolumeClaim>(&key)
            .await
            .ok()
            .map(|c| c.metadata.finalizers.unwrap_or_default())
    }

    #[tokio::test]
    async fn pvc_without_finalizer_gets_it() {
        let (s, c) = setup(&[pvc("a", false, false)], &[]).await;
        c.process_pvc("ns", "a").await.unwrap();
        assert_eq!(finalizers(&s, "a").await.unwrap(), vec![FIN.to_string()]);
    }

    #[tokio::test]
    async fn pvc_with_finalizer_is_left_alone() {
        let (s, c) = setup(&[pvc("a", true, false)], &[]).await;
        c.process_pvc("ns", "a").await.unwrap();
        assert_eq!(finalizers(&s, "a").await.unwrap(), vec![FIN.to_string()]);
    }

    #[tokio::test]
    async fn missing_pvc_is_ignored() {
        let (_s, c) = setup(&[], &[]).await;
        c.process_pvc("ns", "gone").await.unwrap();
    }

    #[tokio::test]
    async fn deleted_unused_pvc_is_reaped() {
        let (s, c) = setup(&[pvc("a", true, true)], &[]).await;
        c.process_pvc("ns", "a").await.unwrap();
        assert!(finalizers(&s, "a").await.is_none());
    }

    #[tokio::test]
    async fn deleted_pvc_with_scheduled_pod_is_kept() {
        let (s, c) = setup(&[pvc("a", true, true)], &[pod("p", "node-1", "a")]).await;
        c.process_pvc("ns", "a").await.unwrap();
        assert_eq!(finalizers(&s, "a").await.unwrap(), vec![FIN.to_string()]);
    }

    #[tokio::test]
    async fn deleted_pvc_with_unscheduled_pod_is_reaped() {
        let (s, c) = setup(&[pvc("a", true, true)], &[pod("p", "", "a")]).await;
        c.process_pvc("ns", "a").await.unwrap();
        assert!(finalizers(&s, "a").await.is_none());
    }

    #[tokio::test]
    async fn deleted_pvc_with_unrelated_pod_is_reaped() {
        let (s, c) = setup(&[pvc("a", true, true)], &[pod("p", "node-1", "other")]).await;
        c.process_pvc("ns", "a").await.unwrap();
        assert!(finalizers(&s, "a").await.is_none());
    }

    #[tokio::test]
    async fn mixed_pvcs_only_unused_reaped() {
        let (s, c) = setup(
            &[pvc("used", true, true), pvc("free", true, true)],
            &[pod("p", "node-1", "used")],
        )
        .await;
        c.process_pvc("ns", "used").await.unwrap();
        c.process_pvc("ns", "free").await.unwrap();
        assert!(finalizers(&s, "used").await.is_some());
        assert!(finalizers(&s, "free").await.is_none());
    }

    #[test]
    fn finished_but_not_deleted_pod_still_uses_pvc() {
        let mut p = pod("p", "node-1", "a");
        p.status = serde_json::from_value(json!({"phase": "Succeeded"})).ok();
        assert!(pod_uses_pvc(&p, &pvc("a", true, true)));
    }

    #[test]
    fn force_deleted_pod_is_shut_down_and_ephemeral_unused() {
        let mut p: Pod = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "u",
                "deletionTimestamp": "2026-01-01T00:00:00Z",
                "deletionGracePeriodSeconds": 0},
            "spec": {"containers": [], "nodeName": "n",
                "volumes": [{"name": "v", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]}
        }))
        .unwrap();
        assert!(pod_is_shut_down(&p));
        let mut claim = pvc("p-v", true, true);
        claim.metadata.owner_references = serde_json::from_value(json!([{
            "apiVersion": "v1", "kind": "Pod", "name": "p", "uid": "u", "controller": true}]))
        .unwrap();
        assert!(!pod_uses_pvc(&p, &claim));
        // Not shut down: the generic ephemeral claim owned by the pod counts.
        p.metadata.deletion_grace_period_seconds = Some(30);
        assert!(pod_uses_pvc(&p, &claim));
        // Same name but not controlled by this pod: does not count.
        claim.metadata.owner_references = None;
        assert!(!pod_uses_pvc(&p, &claim));
    }
}
