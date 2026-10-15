//! A storage double that behaves like the api-server's ResourceQuota
//! admission plugin on a pod create, for controller tests.
//!
//! Upstream's replication/replicaset controllers have no client-side quota
//! check: they issue the create and the api-server's quota admission rejects
//! it with `403 Forbidden` (`admission.NewForbidden`,
//! staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/controller.go
//! `CheckRequest`: `exceeded quota: %s, requested: %s, used: %s, limited: %s`).
//! The controller then records that error as the `ReplicaFailure` condition
//! (pkg/controller/replicaset/replica_set_utils.go `calculateStatus`).
//!
//! `MemoryStorage` has no admission, so a test of that path needs a double
//! that denies the create server-side. [`QuotaAdmissionStorage`] denies a pod
//! create in a namespace whose ResourceQuota `spec.hard.pods` is already met
//! by the pods stored there, and counts the denials so a test can prove the
//! controller really reached the "api-server".

use rusternetes_storage::{memory::MemoryStorage, Storage, WatchStream};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct QuotaAdmissionStorage {
    pub inner: MemoryStorage,
    /// Pod creates the "api-server" rejected with Forbidden.
    pub denied_creates: AtomicUsize,
}

impl QuotaAdmissionStorage {
    pub fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            denied_creates: AtomicUsize::new(0),
        }
    }

    /// The Forbidden `CheckRequest` returns when a `pods` quota is full.
    async fn deny_pod_create(&self, key: &str) -> Option<rusternetes_common::Error> {
        let rest = key.strip_prefix("/registry/pods/")?;
        let (namespace, name) = rest.split_once('/')?;
        let quotas: Vec<serde_json::Value> = self
            .inner
            .list(&format!("/registry/resourcequotas/{namespace}/"))
            .await
            .ok()?;
        for q in &quotas {
            let Some(limit_str) = q.pointer("/spec/hard/pods").and_then(|v| v.as_str()) else {
                continue;
            };
            let limit: i64 = limit_str.parse().ok()?;
            let used = self
                .inner
                .list::<serde_json::Value>(&format!("/registry/pods/{namespace}/"))
                .await
                .ok()?
                .len() as i64;
            if used + 1 > limit {
                let quota_name = q.pointer("/metadata/name").and_then(|v| v.as_str())?;
                return Some(rusternetes_common::Error::Forbidden(format!(
                    "pods \"{name}\" is forbidden: exceeded quota: {quota_name}, \
                     requested: pods=1, used: pods={used}, limited: pods={limit}"
                )));
            }
        }
        None
    }
}

impl Default for QuotaAdmissionStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Storage for QuotaAdmissionStorage {
    async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        if let Some(err) = self.deny_pod_create(key).await {
            self.denied_creates.fetch_add(1, Ordering::SeqCst);
            return Err(err);
        }
        self.inner.create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update(key, value).await
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
