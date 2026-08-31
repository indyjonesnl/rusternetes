//! A storage double whose **list lags its store**, for controller tests.
//!
//! In direct-storage mode a controller's `list` and `get` read the same map, so
//! a reconcile always sees a perfectly consistent world. Against a real
//! api-server it does not: the object list a controller reconciles from is an
//! informer cache (or a LIST issued a moment ago), while a follow-up `get` — or
//! a later re-`list` in the same reconcile — reflects newer state. Every
//! controller loop that mixes the two has to be correct in that window.
//!
//! Two conformance failures came out of that window, both invisible in
//! direct-storage mode and both only reproducible against a vanilla api-server
//! (#1821):
//!
//! * `[sig-apps] StatefulSet ... Scaling should happen in predictable order and
//!   halt if any stateful pod is unhealthy` — the scale-down loop exited only on
//!   a *successful* delete, so when the fresh read revealed a pod already
//!   terminating it fell through to the next ordinal and deleted that one too.
//!   A 3→0 scale-down killed all three pods within a second instead of one at a
//!   time in reverse ordinal order.
//! * `[sig-apps] Job should execute all indexes despite some failing when using
//!   backoffLimitPerIndex` — the pod-creation gate took its active/succeeded
//!   counts from a fresh re-list but its per-index failure budget from the
//!   stale top-of-reconcile snapshot, so an index that had already burned its
//!   retries got one pod too many and `status.failed` came out 5 instead of 4.
//!
//! Two knobs, one per shape of lag. Both are opt-in so each test states exactly
//! what it models; everything else passes through to `MemoryStorage`.

#![allow(dead_code)]

use rusternetes_storage::{memory::MemoryStorage, Storage, WatchStream};
use std::collections::HashSet;
use std::sync::Mutex;

pub struct StaleListStorage {
    pub inner: MemoryStorage,
    /// Strip `metadata.deletionTimestamp` from every listed object: a delete
    /// issued this cycle is not yet visible to the next reconcile's list.
    hide_deletion_timestamps: bool,
    /// Object names omitted from the *next* `list` call only, then revealed —
    /// an informer that has not yet observed an object the store already has.
    hide_next_list: Mutex<HashSet<String>>,
}

impl StaleListStorage {
    pub fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            hide_deletion_timestamps: false,
            hide_next_list: Mutex::new(HashSet::new()),
        }
    }

    /// Model a list that has not yet caught up with in-flight deletions.
    pub fn hiding_deletion_timestamps(mut self) -> Self {
        self.hide_deletion_timestamps = true;
        self
    }

    /// Model a list that has not yet observed `names`. The *next* `list` call
    /// omits them; every later call returns them.
    pub fn hiding_next_list(self, names: &[&str]) -> Self {
        {
            let mut hidden = self.hide_next_list.lock().unwrap();
            for n in names {
                hidden.insert((*n).to_string());
            }
        }
        self
    }
}

impl Default for StaleListStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Storage for StaleListStorage {
    async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        let raw: Vec<serde_json::Value> = self.inner.list(prefix).await?;
        let mut out = Vec::with_capacity(raw.len());
        for mut item in raw {
            let name = item
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string();
            // Hide-once is consumed by the list that WOULD have contained the
            // object, not by any list at all: a reconcile lists several
            // collections (jobs, then pods), and draining on the first of them
            // would leave the pod list fresh and the lag unmodelled.
            if self.hide_next_list.lock().unwrap().remove(&name) {
                continue;
            }
            if self.hide_deletion_timestamps {
                if let Some(meta) = item.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                    meta.remove("deletionTimestamp");
                }
            }
            out.push(
                serde_json::from_value(item).map_err(rusternetes_common::Error::Serialization)?,
            );
        }
        Ok(out)
    }

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

    async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update(key, value).await
    }

    async fn update_subresource<T>(
        &self,
        key: &str,
        subresource: &str,
        value: &T,
    ) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update_subresource(key, subresource, value).await
    }

    async fn update_status<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        self.inner.update_status(key, value).await
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

    async fn delete_gracefully(&self, key: &str) -> rusternetes_common::Result<()> {
        self.inner.delete_gracefully(key).await
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
