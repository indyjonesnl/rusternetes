//! A storage double that behaves like the api-server's generic Store with
//! respect to **object identity on PUT**, for controller tests.
//!
//! The Store (`crates/api-server/src/registry/generic/store.rs`) turns a
//! non-empty `metadata.uid` on an update into a UID precondition, exactly as
//! `defaultUpdatedObjectInfo.Preconditions`
//! (staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:188-203) does, and
//! `BeforeCreate`/`BeforeUpdate` (rest/create.go, rest/update.go:107-167) fill
//! the system fields on create and restore them on update. A controller that
//! rebuilds an object with `X::new(..)` (random UID) and PUTs it therefore
//! 409s, invisibly on plain `MemoryStorage`: that is how every EndpointSlice
//! update broke after #2108 (#2718, audit #2719).
//!
//! Upstream controllers avoid it by updating a `DeepCopy()` of the stored
//! object (e.g. staging/src/k8s.io/endpointslice/reconciler.go:551,
//! pkg/controller/endpoint/endpoints_controller.go:470). Use this double in
//! any test that asserts a controller's update carries the stored identity.
//! Every PUT body is recorded in [`UidPreconditionStorage::puts`].

use rusternetes_storage::{memory::MemoryStorage, Storage, WatchStream};
use std::sync::Mutex;

pub struct UidPreconditionStorage {
    pub inner: MemoryStorage,
    /// Every body handed to `update`, as sent (before the Store's fills).
    pub puts: Mutex<Vec<serde_json::Value>>,
}

impl UidPreconditionStorage {
    pub fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            puts: Mutex::new(Vec::new()),
        }
    }
}

impl Default for UidPreconditionStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Storage for UidPreconditionStorage {
    /// `BeforeCreate`: the server assigns the UID and creationTimestamp.
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let mut doc = serde_json::to_value(value).unwrap();
        if let Some(meta) = doc.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            if meta
                .get("uid")
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .is_empty()
            {
                meta.insert(
                    "uid".to_string(),
                    serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
                );
            }
            meta.entry("creationTimestamp".to_string())
                .or_insert_with(|| serde_json::json!(chrono::Utc::now()));
        }
        let filled: T = serde_json::from_value(doc).unwrap();
        self.inner.create(key, &filled).await
    }

    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    /// `Preconditions()` + `BeforeUpdate`: a non-empty UID must match the
    /// stored one; an empty UID and a missing creationTimestamp are restored.
    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let sent = serde_json::to_value(value).unwrap();
        self.puts.lock().unwrap().push(sent.clone());
        let stored: serde_json::Value = self.inner.get(key).await?;
        let sent_uid = sent["metadata"]["uid"].as_str().unwrap_or("");
        let stored_uid = stored["metadata"]["uid"].as_str().unwrap_or("");
        if !sent_uid.is_empty() && sent_uid != stored_uid {
            return Err(rusternetes_common::Error::Conflict(format!(
                "Precondition failed: UID in precondition: {sent_uid}, UID in object meta: {stored_uid}"
            )));
        }
        let mut doc = sent;
        if let Some(meta) = doc.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.insert(
                "uid".to_string(),
                serde_json::Value::String(stored_uid.to_string()),
            );
            if let Some(created) = stored["metadata"].get("creationTimestamp") {
                meta.insert("creationTimestamp".to_string(), created.clone());
            }
        }
        let kept: T = serde_json::from_value(doc).unwrap();
        self.inner.update(key, &kept).await
    }

    async fn update_raw(
        &self,
        key: &str,
        value: &serde_json::Value,
    ) -> rusternetes_common::Result<()> {
        self.inner.update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete(key).await
    }

    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.list(prefix).await
    }

    async fn watch(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch(prefix).await
    }

    async fn watch_from_revision(
        &self,
        prefix: &str,
        revision: i64,
    ) -> rusternetes_common::Result<WatchStream> {
        self.inner.watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> rusternetes_common::Result<i64> {
        self.inner.current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
        self.inner.is_revision_compacted(revision).await
    }
}
