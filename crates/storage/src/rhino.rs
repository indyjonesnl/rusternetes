//! Rhino-backed storage using rhino's `Backend` trait directly.
//!
//! This module implements the rusternetes [`Storage`] trait on top of rhino
//! backends (SQLite, Redis, etc.), bypassing the gRPC layer entirely.
//! The result is an embedded, zero-dependency storage backend suitable for
//! single-node / all-in-one deployments — no external etcd or rhino server
//! process needed.

use crate::busy::{map_backend_error, retry_busy, RetryPolicy};
use crate::concurrency;
use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use rhino::backend::Backend;
#[cfg(feature = "redis")]
use rhino::{RedisBackend, RedisConfig};
#[cfg(feature = "sqlite")]
use rhino::{SqliteBackend, SqliteConfig};
use rusternetes_common::{authz::AuthzStorage, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Storage implementation backed by a rhino backend.
///
/// Embeds a rhino backend in-process and translates rusternetes'
/// [`Storage`] trait calls directly into rhino backend operations.
/// No gRPC, no network — pure in-process Rust calls.
pub struct RhinoStorage<B: Backend> {
    backend: Arc<B>,
    /// Optional in-process event bus. `Some` only in the all-in-one binary,
    /// where this process is the sole writer (#1039). `None` everywhere else.
    bus: Option<crate::EventBus>,
    /// Per-key object sizes behind `Storage::stats` (stats.go
    /// `resourceSizeEstimator`), fed by the reads and creates that already
    /// hold the encoded value.
    sizes: Arc<crate::size_estimator::SizeEstimator>,
}

#[cfg(feature = "sqlite")]
impl RhinoStorage<SqliteBackend> {
    /// Create a new RhinoStorage with the given SQLite database path.
    ///
    /// This initialises the SQLite database (creating it if necessary),
    /// sets up the schema, and starts background compaction/poll loops.
    pub async fn new(db_path: &str) -> Result<Self> {
        let config = SqliteConfig {
            dsn: db_path.to_string(),
            compact_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let backend = SqliteBackend::new(config)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create SQLite backend: {}", e)))?;

        backend
            .start()
            .await
            .map_err(|e| Error::Storage(format!("Failed to start SQLite backend: {}", e)))?;

        info!("RhinoStorage (SQLite) initialized at {}", db_path);

        Ok(Self {
            backend: Arc::new(backend),
            bus: None,
            sizes: Default::default(),
        })
    }
}

#[cfg(feature = "redis")]
impl RhinoStorage<RedisBackend> {
    /// Create a new RhinoStorage backed by Redis.
    pub async fn new_redis(redis_url: &str) -> Result<Self> {
        let config = RedisConfig {
            dsn: redis_url.to_string(),
            compact_interval: Duration::from_secs(300),
            ..Default::default()
        };

        let backend = RedisBackend::new(config)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create Redis backend: {}", e)))?;

        backend
            .start()
            .await
            .map_err(|e| Error::Storage(format!("Failed to start Redis backend: {}", e)))?;

        info!("RhinoStorage (Redis) initialized at {}", redis_url);

        Ok(Self {
            backend: Arc::new(backend),
            bus: None,
            sizes: Default::default(),
        })
    }
}

/// A TTL in seconds as kine's `lease` column; `0` is no lease.
fn lease_seconds(ttl: u64) -> Result<i64> {
    i64::try_from(ttl).map_err(|_| Error::Storage(format!("TTL {ttl}s overflows a kine lease")))
}

impl<B: Backend> RhinoStorage<B> {
    /// Serialize a value to JSON.
    fn serialize<T: Serialize>(value: &T) -> Result<String> {
        serde_json::to_string(value).map_err(Error::Serialization)
    }

    /// Inject resourceVersion into JSON metadata from a rhino mod_revision.
    fn inject_resource_version(json: &str, mod_revision: i64) -> String {
        let rv_str = concurrency::mod_revision_to_resource_version(mod_revision);
        if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(json) {
            if let Some(metadata) = v.get_mut("metadata") {
                metadata["resourceVersion"] = serde_json::Value::String(rv_str);
            }
            serde_json::to_string(&v).unwrap_or_else(|_| json.to_string())
        } else {
            json.to_string()
        }
    }

    /// Attach an in-process event bus. After this, writes publish and `watch()`
    /// serves from the bus. Call only in single-writer (all-in-one) processes.
    pub fn set_event_bus(&mut self, bus: crate::EventBus) {
        self.bus = Some(bus);
    }

    /// Publish a watch event to the bus when one is attached; no-op otherwise.
    fn publish(&self, event: WatchEvent) {
        if let Some(bus) = &self.bus {
            bus.publish(event);
        }
    }

    /// The native backend watch (current revision onward), bypassing the bus.
    /// Used by `StorageBackend::watch_backend` to keep external HTTP clients on
    /// the globally-RV-ordered feed.
    pub async fn watch_native(&self, prefix: &str) -> Result<WatchStream> {
        let current_rev = retry_busy(RetryPolicy::default(), || self.backend.current_revision())
            .await
            .map_err(|e| map_backend_error(prefix, "watch", "get current revision", &e))?;
        self.watch_from_revision(prefix, current_rev + 1).await
    }

    /// List `prefix` at `revision`; 0 means the current revision.
    async fn list_inner<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let (_rev, kvs) = retry_busy(RetryPolicy::default(), || {
            self.backend.list(prefix, "", 0, revision, false)
        })
        .await
        .map_err(|e| map_backend_error(prefix, "list", "list resources", &e))?;

        let mut results = Vec::with_capacity(kvs.len());
        for kv in kvs {
            self.sizes
                .update_key(&kv.key, kv.value.len(), kv.mod_revision);
            let json = String::from_utf8(kv.value)
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
            let json_with_rv = Self::inject_resource_version(&json, kv.mod_revision);
            match serde_json::from_str::<T>(&json_with_rv) {
                Ok(value) => results.push(value),
                Err(e) => {
                    error!("Failed to deserialize value at {}: {}", kv.key, e);
                    continue;
                }
            }
        }

        debug!("Listed {} resources with prefix: {}", results.len(), prefix);
        Ok(results)
    }
}

#[async_trait]
impl<B: Backend + Send + Sync + 'static> Storage for RhinoStorage<B> {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.create_with_ttl(key, value, 0).await
    }

    /// kine takes a TTL as the key's `lease` column, in seconds, and its TTL
    /// loop deletes the key when that many seconds have passed.
    async fn create_with_ttl<T>(&self, key: &str, value: &T, ttl: u64) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let lease = lease_seconds(ttl)?;
        // Stamp system-managed creation metadata (uid, creationTimestamp,
        // generation) centrally, mirroring k8s registry.Store.Create.
        let json = {
            let mut raw = Self::serialize(value)?;
            if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(&raw) {
                crate::metadata::ensure_create_metadata(&mut v);
                raw = serde_json::to_string(&v).unwrap_or(raw);
            }
            raw
        };

        let mod_revision = retry_busy(RetryPolicy::default(), || {
            self.backend.create(key, json.as_bytes(), lease)
        })
        .await
        .map_err(|e| match e {
            rhino::backend::BackendError::KeyExists => Error::AlreadyExists(key.to_string()),
            other => map_backend_error(key, "create", "create resource", &other),
        })?;

        debug!("Created resource at key: {}", key);
        self.sizes.update_key(key, json.len(), mod_revision);

        let json_with_rv = Self::inject_resource_version(&json, mod_revision);
        self.publish(WatchEvent::Added(key.to_string(), json_with_rv.clone()));
        serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let (_rev, kv) = retry_busy(RetryPolicy::default(), || {
            self.backend.get(key, "", 1, 0, false)
        })
        .await
        .map_err(|e| map_backend_error(key, "get", "get resource", &e))?;

        match kv {
            Some(kv) => {
                self.sizes.update_key(key, kv.value.len(), kv.mod_revision);
                let json = String::from_utf8(kv.value)
                    .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
                let json_with_rv = Self::inject_resource_version(&json, kv.mod_revision);
                serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
            }
            None => Err(Error::NotFound(key.to_string())),
        }
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.update_with_ttl(key, value, 0).await
    }

    async fn update_with_ttl<T>(&self, key: &str, value: &T, ttl: u64) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let lease = lease_seconds(ttl)?;
        let json = Self::serialize(value)?;

        // Extract resourceVersion from the incoming resource for optimistic concurrency
        let incoming_resource: serde_json::Value =
            serde_json::from_str(&json).map_err(Error::Serialization)?;
        let incoming_rv = concurrency::extract_resource_version(
            incoming_resource
                .get("metadata")
                .unwrap_or(&serde_json::json!({})),
        );

        if let Some(incoming_rv) = incoming_rv.as_deref() {
            let expected_mod_revision = concurrency::resource_version_to_mod_revision(incoming_rv)?;

            let (rev, prev_kv, succeeded) = retry_busy(RetryPolicy::default(), || {
                self.backend
                    .update(key, json.as_bytes(), expected_mod_revision, lease)
            })
            .await
            .map_err(|e| map_backend_error(key, "update", "update resource", &e))?;

            if !succeeded {
                let current_rv = prev_kv
                    .as_ref()
                    .map(|kv| concurrency::mod_revision_to_resource_version(kv.mod_revision))
                    .unwrap_or_else(|| "unknown".to_string());

                if prev_kv.is_none() {
                    return Err(Error::NotFound(key.to_string()));
                }

                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: resource was modified (expected: {}, current: {})",
                    incoming_rv, current_rv
                )));
            }

            debug!("Updated resource at key: {}", key);
            self.sizes.update_key(key, json.len(), rev);
            let json_with_rv = Self::inject_resource_version(&json, rev);
            self.publish(WatchEvent::Modified(key.to_string(), json_with_rv.clone()));
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            // No resourceVersion provided — check key exists, then update
            // Use the current revision from a get to perform the update
            let (_rev, existing_kv) = retry_busy(RetryPolicy::default(), || {
                self.backend.get(key, "", 1, 0, false)
            })
            .await
            .map_err(|e| map_backend_error(key, "update", "check resource", &e))?;

            let existing_kv = existing_kv.ok_or_else(|| Error::NotFound(key.to_string()))?;

            let (new_rev, _prev_kv, succeeded) = retry_busy(RetryPolicy::default(), || {
                self.backend
                    .update(key, json.as_bytes(), existing_kv.mod_revision, lease)
            })
            .await
            .map_err(|e| map_backend_error(key, "update", "update resource", &e))?;

            if !succeeded {
                // Concurrent modification — retry once by re-reading
                let (_rev, latest_kv) = retry_busy(RetryPolicy::default(), || {
                    self.backend.get(key, "", 1, 0, false)
                })
                .await
                .map_err(|e| map_backend_error(key, "update", "re-read resource", &e))?;

                let latest_kv = latest_kv.ok_or_else(|| Error::NotFound(key.to_string()))?;

                let (new_rev, _prev_kv, succeeded) = retry_busy(RetryPolicy::default(), || {
                    self.backend
                        .update(key, json.as_bytes(), latest_kv.mod_revision, lease)
                })
                .await
                .map_err(|e| map_backend_error(key, "update", "update resource", &e))?;

                if !succeeded {
                    return Err(Error::Conflict(
                        "resourceVersion mismatch after retry".to_string(),
                    ));
                }

                let json_with_rv = Self::inject_resource_version(&json, new_rev);
                self.publish(WatchEvent::Modified(key.to_string(), json_with_rv.clone()));
                return serde_json::from_str(&json_with_rv).map_err(Error::Serialization);
            }

            debug!("Updated resource at key: {}", key);
            self.sizes.update_key(key, json.len(), new_rev);
            let json_with_rv = Self::inject_resource_version(&json, new_rev);
            self.publish(WatchEvent::Modified(key.to_string(), json_with_rv.clone()));
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let json = serde_json::to_string(value).map_err(Error::Serialization)?;

        // Get current revision to perform update
        let (_rev, existing_kv) = self
            .backend
            .get(key, "", 1, 0, false)
            .await
            .map_err(|e| Error::Storage(format!("Failed to check resource: {}", e)))?;

        let existing_kv = existing_kv.ok_or_else(|| Error::NotFound(key.to_string()))?;

        let (new_rev, _prev_kv, succeeded) = retry_busy(RetryPolicy::default(), || {
            self.backend
                .update(key, json.as_bytes(), existing_kv.mod_revision, 0)
        })
        .await
        .map_err(|e| map_backend_error(key, "update", "update resource", &e))?;

        if !succeeded {
            return Err(Error::Conflict(
                "resource was modified concurrently".to_string(),
            ));
        }

        debug!("Updated resource (raw) at key: {}", key);
        let json_with_rv = Self::inject_resource_version(&json, new_rev);
        self.publish(WatchEvent::Modified(key.to_string(), json_with_rv));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let (del_rev, prev_kv, succeeded) =
            retry_busy(RetryPolicy::default(), || self.backend.delete(key, 0))
                .await
                .map_err(|e| map_backend_error(key, "delete", "delete resource", &e))?;

        // kine's `Delete` reports a key that never existed as
        // `(rev, nil, true)` and one already deleted as `(rev, nil, false)`
        // (`pkg/logstructured/logstructured.go`). Both are NotFound upstream:
        // `conditionalDelete` reads the key via `getState(..., ignoreNotFound:
        // false)`, which returns `storage.NewKeyNotFoundError(key, 0)` when the
        // kv is nil (`staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go`).
        // A successful delete always carries the prior kv, so a missing one
        // means there was nothing to delete (#2041).
        if !succeeded || prev_kv.is_none() {
            return Err(Error::NotFound(key.to_string()));
        }

        debug!("Deleted resource at key: {}", key);
        self.sizes.delete_key(key, del_rev);
        // Guarded so the prev-value reconstruction (utf8 + RV inject) is skipped
        // entirely when no bus is attached — keep the guard, don't simplify to a
        // bare `self.publish(...)` which would do that work unconditionally.
        if self.bus.is_some() {
            let prev_value = prev_kv
                .as_ref()
                .map(|kv| {
                    let raw = String::from_utf8_lossy(&kv.value).to_string();
                    // Stamp the DELETE's revision, not the object's last
                    // write (etcd3/watcher.go parseEvent stamps `e.rev`), so
                    // a watch snapshot can dedupe DELETED by rv (#2223).
                    Self::inject_resource_version(&raw, del_rev)
                })
                .unwrap_or_default();
            self.publish(WatchEvent::Deleted(key.to_string(), prev_value));
        }
        Ok(())
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.list_inner(prefix, 0).await
    }

    /// `storage/etcd3/stats.go` `resourceSizeEstimator.Stats`: a keys-only
    /// read for the count, the average size from the per-key cache.
    async fn stats(&self, prefix: &str) -> Result<crate::ResourceStats> {
        let (_rev, kvs) = retry_busy(RetryPolicy::default(), || {
            self.backend.list(prefix, "", 0, 0, true)
        })
        .await
        .map_err(|e| map_backend_error(prefix, "stats", "list keys", &e))?;
        let keys: Vec<String> = kvs.into_iter().map(|kv| kv.key).collect();
        Ok(self.sizes.stats(prefix, &keys))
    }

    // Paged lists pin every continuation to the first page's revision
    // (upstream `GetList`, `withRev` in `etcd3/store.go`). Without this
    // override the trait default answered with a live `list`, so an object
    // created mid-walk leaked into a later page (#2042). The backend's `List`
    // already takes a revision, as kine's does, and returns `Compacted` below
    // the compaction floor; that error's text ("has been compacted") routes it
    // to the resumable 410 in `list_paginated`.
    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.list_inner(prefix, revision).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        // With a bus attached (all-in-one), internal consumers get the in-process
        // fast path. Without one, fall back to the native backend watch.
        match &self.bus {
            Some(bus) => Ok(bus.subscribe(prefix)),
            None => self.watch_native(prefix).await,
        }
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        let watch_result = self
            .backend
            .watch(prefix, revision)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create watch: {}", e)))?;

        info!(
            "Started watching prefix: {} from revision {}",
            prefix, revision
        );

        let mut events_rx = watch_result.events;
        let sizes = self.sizes.clone();

        let watch_stream = async_stream::stream! {
            while let Some(events) = events_rx.recv().await {
                for event in events {
                    let key = event.kv.key.clone();
                    let mod_revision = event.kv.mod_revision;

                    // Feed the size estimator like upstream's watch loop
                    // (storage/etcd3/watcher.go:408-415: UpdateKey on PUT,
                    // DeleteKey on DELETE) so a resource nobody reads still
                    // reports an average size (#2991).
                    if event.delete {
                        sizes.delete_key(&key, mod_revision);
                    } else {
                        sizes.update_key(&key, event.kv.value.len(), mod_revision);
                    }

                    if event.delete {
                        // For deletes, use prev_kv value if available, otherwise use kv value
                        let raw_prev = event
                            .prev_kv
                            .as_ref()
                            .map(|pk| String::from_utf8_lossy(&pk.value).to_string())
                            .unwrap_or_default();
                        let prev_value = Self::inject_resource_version(&raw_prev, mod_revision);
                        yield Ok(WatchEvent::Deleted(key, prev_value));
                    } else {
                        let raw_value = String::from_utf8_lossy(&event.kv.value).to_string();
                        let value = Self::inject_resource_version(&raw_value, mod_revision);
                        if event.create {
                            yield Ok(WatchEvent::Added(key, value));
                        } else {
                            yield Ok(WatchEvent::Modified(key, value));
                        }
                    }
                }
            }
        };

        Ok(Box::pin(watch_stream))
    }

    async fn current_revision(&self) -> Result<i64> {
        retry_busy(RetryPolicy::default(), || self.backend.current_revision())
            .await
            .map_err(|e| map_backend_error("", "get", "get current revision", &e))
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        // Try to list at the given revision — if compacted, rhino returns Compacted error
        match self.backend.list("/registry/", "", 1, revision, true).await {
            Ok(_) => Ok(false),
            Err(rhino::backend::BackendError::Compacted) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

// Implement AuthzStorage for RhinoStorage — same pattern as EtcdStorage
#[async_trait]
impl<B: Backend + Send + Sync + 'static> AuthzStorage for RhinoStorage<B> {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        // IMPORTANT: order matters — `type_name::<RoleBinding>()` also matches
        // `contains("Role")`, so the more-specific `RoleBinding`/`ClusterRoleBinding`
        // checks must come first or they get shadowed.
        let full_key = match namespace {
            Some(ns) => {
                if std::any::type_name::<T>().contains("RoleBinding")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else if std::any::type_name::<T>().contains("Role")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/roles/{}/{}", ns, key)
                } else {
                    format!("/registry/unknown/{}/{}", ns, key)
                }
            }
            None => {
                if std::any::type_name::<T>().contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else if std::any::type_name::<T>().contains("ClusterRole")
                    && !std::any::type_name::<T>().contains("Binding")
                {
                    format!("/registry/clusterroles/{}", key)
                } else {
                    format!("/registry/unknown/{}", key)
                }
            }
        };

        Storage::get(self, &full_key).await
    }

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // IMPORTANT: order matters — `type_name::<RoleBinding>()` also matches
        // `contains("Role")`, so the more-specific `RoleBinding`/`ClusterRoleBinding`
        // checks must come first or they get shadowed.
        let prefix = match namespace {
            Some(ns) => {
                if std::any::type_name::<T>().contains("RoleBinding")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/rolebindings/{}/", ns)
                } else if std::any::type_name::<T>().contains("Role")
                    && !std::any::type_name::<T>().contains("Cluster")
                {
                    format!("/registry/roles/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                if std::any::type_name::<T>().contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else if std::any::type_name::<T>().contains("ClusterRole")
                    && !std::any::type_name::<T>().contains("Binding")
                {
                    "/registry/clusterroles/".to_string()
                } else {
                    "/registry/unknown/".to_string()
                }
            }
        };

        Storage::list(self, &prefix).await
    }
}

#[cfg(test)]
mod busy_read_tests {
    //! Reads must ride out SQLite lock contention like writes do (#2344,
    //! follow-up to #1621). A backend whose reads fail `database is locked`
    //! a set number of times stands in for a writer holding BEGIN IMMEDIATE.
    use super::*;
    use rhino::backend::{Backend, BackendError, KeyValue, Result as BResult, WatchResult};
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Flaky {
        /// Reads fail with "database is locked" while this is > 0.
        locked_reads: AtomicU32,
        calls: AtomicU32,
    }

    impl Flaky {
        fn new(locked_reads: u32) -> Self {
            Self {
                locked_reads: AtomicU32::new(locked_reads),
                calls: AtomicU32::new(0),
            }
        }
        fn gate(&self) -> BResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let left = self.locked_reads.load(Ordering::SeqCst);
            if left > 0 {
                self.locked_reads.store(left - 1, Ordering::SeqCst);
                return Err(BackendError::Internal("database is locked".into()));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Backend for Flaky {
        async fn start(&self) -> BResult<()> {
            Ok(())
        }
        async fn get(
            &self,
            key: &str,
            _: &str,
            _: i64,
            _: i64,
            _: bool,
        ) -> BResult<(i64, Option<KeyValue>)> {
            self.gate()?;
            Ok((
                7,
                Some(KeyValue {
                    key: key.to_string(),
                    value: br#"{"a":1}"#.to_vec(),
                    version: 1,
                    create_revision: 7,
                    mod_revision: 7,
                    lease: 0,
                }),
            ))
        }
        async fn create(&self, _: &str, _: &[u8], _: i64) -> BResult<i64> {
            unimplemented!()
        }
        async fn delete(&self, _: &str, _: i64) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn delete_prefix(&self, _: &str) -> BResult<(i64, i64, Vec<KeyValue>)> {
            unimplemented!()
        }
        async fn list(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
            _: bool,
        ) -> BResult<(i64, Vec<KeyValue>)> {
            self.gate()?;
            Ok((7, vec![]))
        }
        async fn count(&self, _: &str, _: &str, _: i64) -> BResult<(i64, i64)> {
            unimplemented!()
        }
        async fn update(
            &self,
            _: &str,
            _: &[u8],
            _: i64,
            _: i64,
        ) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn watch(&self, _: &str, _: i64) -> BResult<WatchResult> {
            unimplemented!()
        }
        async fn db_size(&self) -> BResult<i64> {
            Ok(0)
        }
        async fn current_revision(&self) -> BResult<i64> {
            self.gate()?;
            Ok(7)
        }
        async fn wait_for_sync_to(&self, _: i64) {}
        async fn compact(&self, r: i64) -> BResult<i64> {
            Ok(r)
        }
    }

    fn store(locked: u32) -> RhinoStorage<Flaky> {
        RhinoStorage {
            backend: Arc::new(Flaky::new(locked)),
            bus: None,
            sizes: Default::default(),
        }
    }

    fn assert_server_timeout(e: Error) {
        let Error::Status(s) = e else {
            panic!("want ServerTimeout Status, got {e:?}")
        };
        assert_eq!(s.reason.as_deref(), Some("ServerTimeout"));
    }

    #[tokio::test]
    async fn get_retries_through_lock() {
        let s = store(2);
        let v: serde_json::Value = Storage::get(&s, "/registry/pods/ns/p").await.unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(s.backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn get_exhausted_lock_is_server_timeout() {
        let s = store(1000);
        let e = Storage::get::<serde_json::Value>(&s, "/registry/pods/ns/p")
            .await
            .unwrap_err();
        assert_server_timeout(e);
    }

    #[tokio::test]
    async fn list_retries_through_lock() {
        let s = store(2);
        let v: Vec<serde_json::Value> = Storage::list(&s, "/registry/pods/").await.unwrap();
        assert!(v.is_empty());
        assert_eq!(s.backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn list_exhausted_lock_is_server_timeout() {
        let s = store(1000);
        let e = Storage::list::<serde_json::Value>(&s, "/registry/pods/")
            .await
            .unwrap_err();
        assert_server_timeout(e);
    }

    #[tokio::test]
    async fn current_revision_retries_through_lock() {
        let s = store(2);
        assert_eq!(s.current_revision().await.unwrap(), 7);
    }
}

#[cfg(test)]
mod stats_tests {
    //! `Storage::stats` must be a keys-only read plus a per-key size cache
    //! (#2869; upstream `storage/etcd3/stats.go` `resourceSizeEstimator.Stats`),
    //! not a list that decodes and re-encodes every object once a minute.
    use super::*;
    use futures::StreamExt;
    use rhino::backend::{Backend, Event, KeyValue, Result as BResult, WatchResult};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recording {
        /// Events the backend watch replays, one batch.
        watch_events: Mutex<Vec<Event>>,
        saw_keys_only: AtomicBool,
        saw_value_read: AtomicBool,
    }

    fn kv(key: &str, value: &[u8], rev: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_vec(),
            version: 1,
            create_revision: rev,
            mod_revision: rev,
            lease: 0,
        }
    }

    #[async_trait]
    impl Backend for Recording {
        async fn start(&self) -> BResult<()> {
            Ok(())
        }
        async fn get(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
            _: bool,
        ) -> BResult<(i64, Option<KeyValue>)> {
            unimplemented!()
        }
        async fn create(&self, _: &str, _: &[u8], _: i64) -> BResult<i64> {
            unimplemented!()
        }
        async fn delete(&self, _: &str, _: i64) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn delete_prefix(&self, _: &str) -> BResult<(i64, i64, Vec<KeyValue>)> {
            unimplemented!()
        }
        async fn list(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
            keys_only: bool,
        ) -> BResult<(i64, Vec<KeyValue>)> {
            if keys_only {
                self.saw_keys_only.store(true, Ordering::SeqCst);
            } else {
                self.saw_value_read.store(true, Ordering::SeqCst);
            }
            let value = |v: &'static [u8]| if keys_only { Vec::new() } else { v.to_vec() };
            Ok((
                9,
                vec![
                    kv(
                        "/registry/pods/ns/a",
                        &value(br#"{"metadata":{"name":"a"}}"#),
                        8,
                    ),
                    kv(
                        "/registry/pods/ns/b",
                        &value(br#"{"metadata":{"name":"b"}}"#),
                        9,
                    ),
                ],
            ))
        }
        async fn count(&self, _: &str, _: &str, _: i64) -> BResult<(i64, i64)> {
            unimplemented!()
        }
        async fn update(
            &self,
            _: &str,
            _: &[u8],
            _: i64,
            _: i64,
        ) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn watch(&self, _: &str, _: i64) -> BResult<WatchResult> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let batch = std::mem::take(&mut *self.watch_events.lock().unwrap());
            tx.send(batch).await.unwrap();
            Ok(WatchResult {
                current_revision: 9,
                compact_revision: 0,
                events: rx,
            })
        }
        async fn db_size(&self) -> BResult<i64> {
            Ok(0)
        }
        async fn current_revision(&self) -> BResult<i64> {
            Ok(9)
        }
        async fn wait_for_sync_to(&self, _: i64) {}
        async fn compact(&self, r: i64) -> BResult<i64> {
            Ok(r)
        }
    }

    #[tokio::test]
    async fn stats_average_size_comes_from_what_a_list_already_read() {
        let s = RhinoStorage {
            backend: Arc::new(Recording::default()),
            bus: None,
            sizes: Default::default(),
        };
        let _: Vec<serde_json::Value> = Storage::list(&s, "/registry/pods/").await.unwrap();
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.object_count, 2);
        assert_eq!(
            st.estimated_average_object_size_bytes,
            br#"{"metadata":{"name":"a"}}"#.len() as i64
        );
    }

    #[tokio::test]
    async fn stats_reads_keys_only_and_never_the_values() {
        let s = RhinoStorage {
            backend: Arc::new(Recording::default()),
            bus: None,
            sizes: Default::default(),
        };
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.object_count, 2);
        assert!(s.backend.saw_keys_only.load(Ordering::SeqCst));
        assert!(
            !s.backend.saw_value_read.load(Ordering::SeqCst),
            "stats must not read object values"
        );
    }

    fn put(key: &str, value: &[u8], rev: i64, create: bool) -> Event {
        Event {
            delete: false,
            create,
            kv: kv(key, value, rev),
            prev_kv: None,
        }
    }

    async fn drain(s: &RhinoStorage<Recording>) {
        let mut w = Storage::watch_from_revision(s, "/registry/pods/", 1)
            .await
            .unwrap();
        while w.next().await.is_some() {}
    }

    /// Upstream feeds the estimator from watch events
    /// (`storage/etcd3/watcher.go:408-415`: `UpdateKey` on PUT, `DeleteKey`
    /// on DELETE), so a resource nobody lists/gets/writes still reports a
    /// size (#2991).
    #[tokio::test]
    async fn watch_put_event_feeds_the_size_estimator() {
        let value = br#"{"metadata":{"name":"a"},"spec":{}}"#;
        let b = Recording::default();
        b.watch_events
            .lock()
            .unwrap()
            .push(put("/registry/pods/ns/a", value, 10, true));
        let s = RhinoStorage {
            backend: Arc::new(b),
            bus: None,
            sizes: Default::default(),
        };
        drain(&s).await;
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.estimated_average_object_size_bytes, value.len() as i64);
    }

    #[tokio::test]
    async fn watch_delete_event_drops_the_key_from_the_size_estimator() {
        let value = br#"{"metadata":{"name":"a"},"spec":{}}"#;
        let b = Recording::default();
        b.watch_events.lock().unwrap().extend([
            put("/registry/pods/ns/a", value, 10, true),
            Event {
                delete: true,
                create: false,
                kv: kv("/registry/pods/ns/a", b"", 11),
                prev_kv: Some(kv("/registry/pods/ns/a", value, 10)),
            },
        ]);
        let s = RhinoStorage {
            backend: Arc::new(b),
            bus: None,
            sizes: Default::default(),
        };
        drain(&s).await;
        // Recording lists keys a and b; only b's size is unknown, and a's
        // delete must have evicted its cached size.
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.estimated_average_object_size_bytes, 0);
    }
}
