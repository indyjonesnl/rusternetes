//! volume-data-source-validator: `UnrecognizedDataSourceKind` warning.
//!
//! Ported from kubernetes-csi/volume-data-source-validator
//! `pkg/data-source-validator/data_source_validator.go` (`syncPvcByKey`).

use rusternetes_common::resources::volume::*;
use rusternetes_common::resources::Event;
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::volume_data_source_validator::VolumeDataSourceValidator;
use rusternetes_storage::{build_key, build_prefix, memory::MemoryStorage, Storage};
use std::sync::Arc;

const POPULATORS: &str = "populator_storage_k8s_io_volumepopulators";

fn claim(name: &str, group: Option<&str>, kind: &str) -> PersistentVolumeClaim {
    let mut meta = ObjectMeta::new(name).with_namespace("default");
    meta.uid = format!("uid-{name}");
    PersistentVolumeClaim {
        type_meta: TypeMeta {
            kind: "PersistentVolumeClaim".into(),
            api_version: "v1".into(),
        },
        metadata: meta,
        spec: PersistentVolumeClaimSpec {
            data_source_ref: Some(TypedObjectReference {
                api_group: group.map(String::from),
                kind: kind.into(),
                name: "src".into(),
                namespace: None,
            }),
            ..Default::default()
        },
        status: None,
    }
}

async fn reasons(storage: &MemoryStorage) -> Vec<String> {
    let events: Vec<Event> = storage
        .list(&build_prefix("events", Some("default")))
        .await
        .unwrap_or_default();
    events.into_iter().map(|e| e.reason).collect()
}

async fn sync(storage: &Arc<MemoryStorage>, c: &PersistentVolumeClaim) {
    storage
        .create(
            &build_key("persistentvolumeclaims", Some("default"), &c.metadata.name),
            c,
        )
        .await
        .unwrap();
    VolumeDataSourceValidator::new(storage.clone())
        .sync_pvc_by_key(&format!(
            "persistentvolumeclaims/default/{}",
            c.metadata.name
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn unregistered_kind_gets_a_warning() {
    let storage = Arc::new(MemoryStorage::new());
    sync(&storage, &claim("a", Some("hello.example.com"), "Hello")).await;
    assert_eq!(reasons(&storage).await, vec!["UnrecognizedDataSourceKind"]);
}

#[tokio::test]
async fn registered_populator_kind_is_quiet() {
    let storage = Arc::new(MemoryStorage::new());
    storage
        .create(
            &build_key(POPULATORS, None, "hello"),
            &serde_json::json!({
                "apiVersion": "populator.storage.k8s.io/v1beta1",
                "kind": "VolumePopulator",
                "metadata": {"name": "hello"},
                "sourceKind": {"group": "hello.example.com", "kind": "Hello"}
            }),
        )
        .await
        .unwrap();
    sync(&storage, &claim("a", Some("hello.example.com"), "Hello")).await;
    assert!(reasons(&storage).await.is_empty());
}

#[tokio::test]
async fn clone_and_snapshot_sources_are_quiet() {
    let storage = Arc::new(MemoryStorage::new());
    sync(&storage, &claim("a", None, "PersistentVolumeClaim")).await;
    sync(
        &storage,
        &claim("b", Some("snapshot.storage.k8s.io"), "VolumeSnapshot"),
    )
    .await;
    assert!(reasons(&storage).await.is_empty());
}

#[tokio::test]
async fn missing_claim_is_not_an_error() {
    let storage = Arc::new(MemoryStorage::new());
    VolumeDataSourceValidator::new(storage)
        .sync_pvc_by_key("persistentvolumeclaims/default/gone")
        .await
        .unwrap();
}
