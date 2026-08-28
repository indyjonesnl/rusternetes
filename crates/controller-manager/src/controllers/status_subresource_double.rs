//! A storage double that behaves like a real api-server with respect to the
//! **status subresource**, for controller tests.
//!
//! An api-server that exposes `/status` for a type strips `.status` from an
//! ordinary object PUT — only the status subresource persists it. A controller
//! that writes status through `Storage::update` therefore looks correct on
//! `MemoryStorage` (whose `update_status` funnels through `update`, so both
//! paths persist) and silently loses every status write in a real cluster.
//!
//! That has already cost two conformance failure groups:
//!
//! * `[sig-apps] DisruptionController` — PDB `observedGeneration` never
//!   advanced, so upstream's `waitForPdbToBeProcessed` polled for its full
//!   10-minute budget (#1712).
//! * `[sig-apps] ReplicationController` — `status.readyReplicas` never
//!   appeared, so the lifecycle spec's watch never saw the RC become ready and
//!   the scale spec could not "confirm the quantity of replicas".
//!
//! Use this double in any test that asserts a controller's status write lands.

use rusternetes_storage::{memory::MemoryStorage, Storage, WatchStream};

pub struct StatusSubresourceStorage {
    pub inner: MemoryStorage,
}

impl StatusSubresourceStorage {
    pub fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
        }
    }
}

impl Default for StatusSubresourceStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Storage for StatusSubresourceStorage {
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    /// Full-object PUT: keep whatever status is already stored and ignore the
    /// caller's — exactly what an api-server with a status subresource does.
    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let mut doc = serde_json::to_value(value).unwrap();
        let stored: serde_json::Value =
            self.inner.get(key).await.unwrap_or(serde_json::Value::Null);
        if let Some(obj) = doc.as_object_mut() {
            match stored.get("status") {
                Some(prev) if !prev.is_null() => {
                    obj.insert("status".to_string(), prev.clone());
                }
                _ => {
                    obj.remove("status");
                }
            }
        }
        let kept: T = serde_json::from_value(doc).unwrap();
        self.inner.update(key, &kept).await
    }

    /// The status subresource: the ONLY path that persists `.status`.
    async fn update_status<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let incoming = serde_json::to_value(value).unwrap();
        let mut stored: serde_json::Value = self.inner.get(key).await?;
        if let Some(obj) = stored.as_object_mut() {
            if let Some(status) = incoming.get("status") {
                obj.insert("status".to_string(), status.clone());
            }
        }
        let merged: T = serde_json::from_value(stored).unwrap();
        self.inner.update(key, &merged).await
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
