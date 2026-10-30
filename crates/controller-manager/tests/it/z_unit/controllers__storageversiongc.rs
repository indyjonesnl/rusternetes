//! Unit tests moved from `src/controllers/storageversiongc.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::resources::storageversion::{
        set_common_encoding_version, ServerStorageVersion, StorageVersion,
    };
    pub use rusternetes_common::resources::Lease;
    pub use rusternetes_common::Error;
    pub use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent, WorkQueue};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    //! Ports of `pkg/controller/storageversiongc/gc_controller_test.go`.
    use super::__parent::*;
    use rusternetes_common::resources::storageversion::StorageVersionStatus;
    use rusternetes_controller_manager::controllers::storageversiongc::*;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    fn lease(name: &str) -> Lease {
        let mut l = Lease::new(name, NAMESPACE_SYSTEM);
        l.metadata.labels = Some(HashMap::from([(
            IDENTITY_LEASE_COMPONENT_LABEL_KEY.to_string(),
            KUBE_API_SERVER.to_string(),
        )]));
        l
    }

    fn ssv(id: &str, enc: &str) -> ServerStorageVersion {
        ServerStorageVersion {
            api_server_id: id.into(),
            encoding_version: enc.into(),
            decodable_versions: vec![enc.into()],
            served_versions: vec![],
        }
    }

    async fn setup(
        leases: &[&str],
        entries: Vec<ServerStorageVersion>,
        common: Option<&str>,
    ) -> Arc<MemoryStorage> {
        let storage = Arc::new(MemoryStorage::new());
        for l in leases {
            storage.create(&lease_key(l), &lease(l)).await.unwrap();
        }
        let mut sv = StorageVersion::default();
        sv.metadata.name = "k8s.test.resources".into();
        sv.status = StorageVersionStatus {
            storage_versions: entries,
            common_encoding_version: common.map(String::from),
            conditions: vec![],
        };
        storage
            .create(&sv_key("k8s.test.resources"), &sv)
            .await
            .unwrap();
        storage
    }

    async fn get_sv(s: &MemoryStorage) -> Result<StorageVersion, Error> {
        s.get(&sv_key("k8s.test.resources")).await
    }

    /// Run `run()` and delete `lease_name`; poll for `done`.
    async fn run_and_delete(
        storage: &Arc<MemoryStorage>,
        lease_name: &str,
        done: impl Fn(Option<StorageVersion>) -> bool,
    ) {
        let c = Arc::new(StorageVersionGCController::new(storage.clone()));
        let h = tokio::spawn(async move { c.run().await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        storage.delete(&lease_key(lease_name)).await.unwrap();
        for _ in 0..100 {
            if done(get_sv(storage).await.ok()) {
                h.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        h.abort();
        panic!("controller did not reconcile storage version in time");
    }

    /// Test_StorageVersionUpdatedWithAllEncodingVersionsEqualOnLeaseDeletion
    #[tokio::test]
    async fn updated_with_all_encoding_versions_equal_on_lease_deletion() {
        let s = setup(
            &["kube-apiserver-1", "kube-apiserver-2", "kube-apiserver-3"],
            vec![
                ssv("kube-apiserver-1", "v1"),
                ssv("kube-apiserver-2", "v2"),
                ssv("kube-apiserver-3", "v2"),
            ],
            Some("v1"),
        )
        .await;
        run_and_delete(&s, "kube-apiserver-1", |sv| {
            sv.is_some_and(|sv| {
                sv.status.storage_versions
                    == vec![ssv("kube-apiserver-2", "v2"), ssv("kube-apiserver-3", "v2")]
                    && sv.status.common_encoding_version.as_deref() == Some("v2")
            })
        })
        .await;
    }

    /// Test_StorageVersionUpdatedWithDifferentEncodingVersionsOnLeaseDeletion
    #[tokio::test]
    async fn updated_with_different_encoding_versions_on_lease_deletion() {
        let s = setup(
            &["kube-apiserver-1", "kube-apiserver-2", "kube-apiserver-3"],
            vec![ssv("kube-apiserver-1", "v1"), ssv("kube-apiserver-3", "v2")],
            Some("v1"),
        )
        .await;
        // kube-apiserver-2 has no entry: its deletion must change nothing.
        let c = StorageVersionGCController::new(s.clone());
        s.delete(&lease_key("kube-apiserver-2")).await.unwrap();
        c.process_deleted_lease("kube-apiserver-2").await.unwrap();
        let sv = get_sv(&s).await.unwrap();
        assert_eq!(
            sv.status.storage_versions,
            vec![ssv("kube-apiserver-1", "v1"), ssv("kube-apiserver-3", "v2")]
        );
        assert_eq!(sv.status.common_encoding_version.as_deref(), Some("v1"));
    }

    /// Test_StorageVersionContainsInvalidLeaseID
    #[tokio::test]
    async fn contains_invalid_lease_id() {
        let s = setup(
            &["kube-apiserver-1", "kube-apiserver-2", "kube-apiserver-3"],
            vec![
                ssv("kube-apiserver-1", "v1"),
                ssv("kube-apiserver-2", "v2"),
                ssv("kube-apiserver-3", "v2"),
                ssv("kube-apiserver-4", "v2"), // no lease
            ],
            Some("v1"),
        )
        .await;
        let c = Arc::new(StorageVersionGCController::new(s.clone()));
        let h = tokio::spawn(async move { c.run().await });
        for _ in 0..100 {
            if get_sv(&s)
                .await
                .is_ok_and(|sv| sv.status.storage_versions.len() == 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        h.abort();
        let sv = get_sv(&s).await.unwrap();
        assert_eq!(
            sv.status.storage_versions,
            vec![
                ssv("kube-apiserver-1", "v1"),
                ssv("kube-apiserver-2", "v2"),
                ssv("kube-apiserver-3", "v2")
            ]
        );
        assert_eq!(sv.status.conditions.len(), 1);
        assert_eq!(
            sv.status.conditions[0].condition_type,
            "AllEncodingVersionsEqual"
        );
        assert_eq!(sv.status.conditions[0].status, "False");
        assert_eq!(sv.status.common_encoding_version, None);
    }

    /// Test_StorageVersionDeletedOnLeaseDeletion
    #[tokio::test]
    async fn deleted_on_lease_deletion() {
        let s = setup(
            &["kube-apiserver-1"],
            vec![ssv("kube-apiserver-1", "v1")],
            None,
        )
        .await;
        run_and_delete(&s, "kube-apiserver-1", |sv| sv.is_none()).await;
    }

    /// A lease without the identity label is not a valid owner
    /// (`syncStorageVersion`'s label check).
    #[tokio::test]
    async fn unlabelled_lease_is_invalid() {
        let s = setup(&[], vec![ssv("x", "v1")], None).await;
        let mut l = Lease::new("x", NAMESPACE_SYSTEM);
        l.metadata.labels = None;
        s.create(&lease_key("x"), &l).await.unwrap();
        let c = StorageVersionGCController::new(s.clone());
        c.sync_storage_version("k8s.test.resources").await.unwrap();
        assert!(get_sv(&s).await.is_err());
    }

    #[tokio::test]
    async fn on_delete_lease_ignores_non_identity_leases() {
        let s = Arc::new(MemoryStorage::new());
        let c = StorageVersionGCController::new(s);
        let q = WorkQueue::new();
        let plain = Lease::new("other", NAMESPACE_SYSTEM);
        c.on_lease_event(
            &q,
            &WatchEvent::Deleted("k".into(), serde_json::to_string(&plain).unwrap()),
        )
        .await;
        assert!(q.is_empty().await);
        c.on_lease_event(
            &q,
            &WatchEvent::Deleted("k".into(), serde_json::to_string(&lease("a")).unwrap()),
        )
        .await;
        assert_eq!(q.len().await, 1);
    }
}
