//! Unit tests moved from `src/controllers/ephemeral_volume.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{anyhow, Result};
    pub use rusternetes_common::resources::service_account::ObjectReference;
    pub use rusternetes_common::resources::volume::PersistentVolumeClaim;
    pub use rusternetes_common::resources::{EventSource, EventType, Pod, Volume};
    pub use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
    pub use rusternetes_common::Error;
    pub use rusternetes_controller_manager::controllers::ephemeral_volume_metrics;
    pub use rusternetes_storage::{
        build_key, build_prefix, extract_key, EventRecorder, Storage, WorkQueue,
    };
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    //! Mirrors `pkg/controller/volume/ephemeral/controller_test.go`
    //! `TestSyncHandler`.
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::ephemeral_volume::*;
    use rusternetes_storage::memory::MemoryStorage;
    use serde_json::json;

    fn pod(name: &str, ns: &str, uid: &str, deleting: bool, with_vol: bool) -> Pod {
        let mut meta = json!({"name": name, "namespace": ns, "uid": uid});
        if deleting {
            meta["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        }
        let volumes = if with_vol {
            json!([{"name": "myvolume", "ephemeral": {"volumeClaimTemplate": {
                "metadata": {"labels": {"l": "v"}, "annotations": {"a": "b"}},
                "spec": {"storageClassName": "sc"}}}}])
        } else {
            json!([])
        };
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": meta,
            "spec": {"containers": [], "volumes": volumes}
        }))
        .unwrap()
    }

    fn claim(name: &str, ns: &str, owner_uid: Option<&str>) -> PersistentVolumeClaim {
        let mut meta = json!({"name": name, "namespace": ns, "uid": format!("c-{name}")});
        if let Some(u) = owner_uid {
            meta["ownerReferences"] = json!([{"apiVersion": "v1", "kind": "Pod",
                "name": "test-pod", "uid": u, "controller": true}]);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": meta, "spec": {}
        }))
        .unwrap()
    }

    async fn setup(
        pods: &[Pod],
        pvcs: &[PersistentVolumeClaim],
    ) -> (Arc<MemoryStorage>, EphemeralVolumeController<MemoryStorage>) {
        let s = Arc::new(MemoryStorage::new());
        for p in pods {
            let k = build_key("pods", p.metadata.namespace.as_deref(), &p.metadata.name);
            s.create(&k, p).await.unwrap();
        }
        for c in pvcs {
            let k = build_key(
                "persistentvolumeclaims",
                c.metadata.namespace.as_deref(),
                &c.metadata.name,
            );
            s.create(&k, c).await.unwrap();
        }
        let c = EphemeralVolumeController::new(s.clone());
        (s, c)
    }

    async fn pvcs(s: &MemoryStorage) -> Vec<PersistentVolumeClaim> {
        s.list(&build_prefix("persistentvolumeclaims", None))
            .await
            .unwrap()
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn creates_the_claim_owned_by_the_pod() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        let got = pvcs(&s).await;
        assert_eq!(got.len(), 1);
        let p = &got[0];
        assert_eq!(p.metadata.name, "test-pod-myvolume");
        assert_eq!(p.metadata.namespace.as_deref(), Some("ns"));
        let o = &p.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (o.kind.as_str(), o.name.as_str(), o.uid.as_str()),
            ("Pod", "test-pod", "u1")
        );
        assert_eq!(o.controller, Some(true));
        assert_eq!(o.block_owner_deletion, Some(true));
        assert_eq!(p.metadata.labels.as_ref().unwrap()["l"], "v");
        assert_eq!(p.metadata.annotations.as_ref().unwrap()["a"], "b");
        assert_eq!(p.spec.storage_class_name.as_deref(), Some("sc"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn no_such_pod_is_a_noop() {
        let (s, c) = setup(&[], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert!(pvcs(&s).await.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn deleted_pod_is_a_noop() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", true, true)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert!(pvcs(&s).await.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn pod_without_volumes_is_a_noop() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, false)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert!(pvcs(&s).await.is_empty());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_with_other_namespace_claim() {
        let other = claim("test-pod-myvolume", "other", None);
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[other]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(pvcs(&s).await.len(), 2);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn existing_owned_claim_is_left_alone() {
        let own = claim("test-pod-myvolume", "ns", Some("u1"));
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[own]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        let got = pvcs(&s).await;
        assert_eq!(got.len(), 1);
        assert!(got[0].metadata.labels.is_none());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn claim_owned_by_someone_else_is_an_error() {
        let foreign = claim("test-pod-myvolume", "ns", None);
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[foreign]).await;
        let err = c.sync_pod("ns", "test-pod").await.unwrap_err().to_string();
        assert!(
            err.contains("was not created for pod ns/test-pod (pod is not owner)"),
            "{err}"
        );
        assert_eq!(pvcs(&s).await.len(), 1);
        // upstream records a FailedBinding warning on the pod
        let events: Vec<serde_json::Value> =
            s.list(&build_prefix("events", Some("ns"))).await.unwrap();
        assert!(events.iter().any(|e| e["reason"] == FAILED_BINDING));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn a_claim_deleted_out_from_under_the_pod_is_recreated() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        c.sync_pod("ns", "test-pod").await.unwrap();
        s.delete(&build_key(
            "persistentvolumeclaims",
            Some("ns"),
            "test-pod-myvolume",
        ))
        .await
        .unwrap();
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(pvcs(&s).await.len(), 1);
    }

    // controller_test.go TestSyncHandler "create": one create attempt, no failure.
    #[tokio::test]
    #[serial_test::serial]
    async fn metrics_count_a_successful_create() {
        let (_s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        let (a, f) = (
            ephemeral_volume_metrics::create_attempts(),
            ephemeral_volume_metrics::create_failures(),
        );
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(ephemeral_volume_metrics::create_attempts(), a + 1);
        assert_eq!(ephemeral_volume_metrics::create_failures(), f);
        assert!(
            ephemeral_volume_metrics::gather().contains("ephemeral_volume_controller_create_total")
        );
    }

    // Existing claim: no create call, so no metric.
    #[tokio::test]
    #[serial_test::serial]
    async fn metrics_ignore_an_existing_claim() {
        let own = claim("test-pod-myvolume", "ns", Some("u1"));
        let (_s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[own]).await;
        let (a, f) = (
            ephemeral_volume_metrics::create_attempts(),
            ephemeral_volume_metrics::create_failures(),
        );
        c.sync_pod("ns", "test-pod").await.unwrap();
        assert_eq!(ephemeral_volume_metrics::create_attempts(), a);
        assert_eq!(ephemeral_volume_metrics::create_failures(), f);
    }

    // "create with failure": a Create that errors counts an attempt AND a
    // failure. The claim appears between the read and the create (the
    // lister-lag race), so the create is refused with AlreadyExists.
    #[tokio::test]
    #[serial_test::serial]
    async fn metrics_count_a_failed_create() {
        let (s, c) = setup(&[pod("test-pod", "ns", "u1", false, true)], &[]).await;
        let p = pod("test-pod", "ns", "u1", false, true);
        let vol = p.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let key = build_key("persistentvolumeclaims", Some("ns"), "test-pod-myvolume");
        s.create(&key, &claim("test-pod-myvolume", "ns", None))
            .await
            .unwrap();
        let (a, f) = (
            ephemeral_volume_metrics::create_attempts(),
            ephemeral_volume_metrics::create_failures(),
        );
        let err = c.create_claim(&p, &vol).await.unwrap_err().to_string();
        assert!(err.contains("create PVC test-pod-myvolume"), "{err}");
        assert_eq!(ephemeral_volume_metrics::create_attempts(), a + 1);
        assert_eq!(ephemeral_volume_metrics::create_failures(), f + 1);
    }
}
