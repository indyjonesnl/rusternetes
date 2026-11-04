//! Regression for #4367: the storage-protection controllers must remove their
//! finalizer with a write and let the apiserver reap the object, as upstream
//! does (`pvc_protection_controller.go:processPVC` -> `Update`; the registry
//! reaps via `ShouldDeleteDuringUpdate`, `registry/generic/registry/store.go`
//! :565-586). `ApiStorage::delete` is a DELETE, which an object that still
//! holds its finalizer survives, so a delete-only reap never completed over
//! the API and the e2e "confirm deletion" polls timed out.
//!
//! `ApiLike` models just that registry behaviour on top of `MemoryStorage`.

use async_trait::async_trait;
use rusternetes_common::resources::csi::VolumeAttributesClass;
use rusternetes_common::resources::PersistentVolumeClaim;
use rusternetes_common::{Error, Result};
use rusternetes_controller_manager::controllers::pvc_protection::{
    update_or_reap, PvcProtectionController,
};
use rusternetes_controller_manager::controllers::vac_protection::VacProtectionController;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::{build_key, Storage, WatchStream};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

/// A `Storage` that behaves like the apiserver's registry: a write that
/// leaves a terminating object without finalizers reaps it, and a DELETE of an
/// object that still has finalizers keeps it.
struct ApiLike(MemoryStorage);

fn has_finalizers(v: &Value) -> bool {
    v["metadata"]["finalizers"]
        .as_array()
        .is_some_and(|f| !f.is_empty())
}

#[async_trait]
impl Storage for ApiLike {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.0.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        self.0.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let v = serde_json::to_value(value)?;
        if !v["metadata"]["deletionTimestamp"].is_null() && !has_finalizers(&v) {
            // ShouldDeleteDuringUpdate: the registry deletes instead.
            self.0.delete(key).await?;
            return Ok(serde_json::from_value(v)?);
        }
        self.0.update(key, value).await
    }

    async fn update_raw(&self, key: &str, value: &Value) -> Result<()> {
        self.0.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let cur: Value = self.0.get(key).await?;
        if has_finalizers(&cur) {
            return Ok(());
        }
        self.0.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.0.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        self.0.watch(prefix).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        self.0.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> Result<i64> {
        self.0.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        self.0.is_revision_compacted(revision).await
    }
}

fn terminating_pvc() -> PersistentVolumeClaim {
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "PersistentVolumeClaim",
        "metadata": {"name": "c", "namespace": "ns", "uid": "u",
            "finalizers": ["kubernetes.io/pvc-protection"],
            "deletionTimestamp": "2026-01-01T00:00:00Z"},
        "spec": {"accessModes": ["ReadWriteOnce"]}
    }))
    .unwrap()
}

fn terminating_vac() -> VolumeAttributesClass {
    serde_json::from_value(json!({
        "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttributesClass",
        "metadata": {"name": "v", "uid": "u",
            "finalizers": ["kubernetes.io/vac-protection"],
            "deletionTimestamp": "2026-01-01T00:00:00Z"},
        "driverName": "d"
    }))
    .unwrap()
}

#[tokio::test]
async fn pvc_protection_reaps_terminating_claim_over_api() {
    let s = Arc::new(ApiLike(MemoryStorage::new()));
    let key = build_key("persistentvolumeclaims", Some("ns"), "c");
    s.create(&key, &terminating_pvc()).await.unwrap();
    PvcProtectionController::new(s.clone())
        .process_pvc("ns", "c")
        .await
        .unwrap();
    assert!(matches!(
        s.get::<PersistentVolumeClaim>(&key).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn vac_protection_reaps_terminating_class_over_api() {
    let s = Arc::new(ApiLike(MemoryStorage::new()));
    let key = build_key("volumeattributesclasses", None, "v");
    s.create(&key, &terminating_vac()).await.unwrap();
    VacProtectionController::new(s.clone())
        .process_vac("v")
        .await
        .unwrap();
    assert!(matches!(
        s.get::<VolumeAttributesClass>(&key).await,
        Err(Error::NotFound(_))
    ));
}

/// A direct backend has no registry: the write leaves the emptied object and
/// the trailing delete reaps it.
#[tokio::test]
async fn update_or_reap_reaps_on_direct_storage() {
    let s = MemoryStorage::new();
    let key = build_key("persistentvolumeclaims", Some("ns"), "c");
    s.create(&key, &terminating_pvc()).await.unwrap();
    let mut pvc = terminating_pvc();
    pvc.metadata.finalizers = None;
    update_or_reap(&s, &key, &pvc).await.unwrap();
    assert!(matches!(
        s.get::<PersistentVolumeClaim>(&key).await,
        Err(Error::NotFound(_))
    ));
}
