use async_trait::async_trait;
use rusternetes_common::authz::AuthzStorage;
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

#[cfg(feature = "api-client")]
pub mod api_storage;
pub mod concurrency;
pub mod etcd;
mod event_bus;
pub mod event_recorder;
pub mod memory;
pub mod metadata;
#[cfg(any(feature = "sqlite", feature = "redis"))]
pub mod rhino;
pub mod workqueue;

// Re-export MemoryStorage for convenient testing
pub use memory::MemoryStorage;

// Re-export the in-process watch event bus (#1039)
pub use event_bus::EventBus;

// Re-export the unified event recorder
pub use event_recorder::EventRecorder;

// Re-export work queue types
pub use workqueue::{extract_key, WorkQueue, WorkQueueConfig, RECONCILE_ALL_SENTINEL};

// Re-export RhinoStorage when sqlite or redis features are enabled
#[cfg(feature = "sqlite")]
pub type RhinoStorage = rhino::RhinoStorage<::rhino::SqliteBackend>;
#[cfg(feature = "redis")]
pub type RhinoRedisStorage = rhino::RhinoStorage<::rhino::RedisBackend>;

/// Storage trait for persisting Kubernetes resources
#[async_trait]
pub trait Storage: Send + Sync {
    /// Create a new resource
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    /// Get a resource by key
    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync;

    /// Update an existing resource
    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    /// Update a resource through a named API subresource.
    ///
    /// Direct storage backends have no HTTP subresources, so their default
    /// behavior is the same as [`Storage::update`]. API-backed implementations
    /// override this to target paths such as `/status` or `/finalize`.
    async fn update_subresource<T>(&self, key: &str, _subresource: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.update(key, value).await
    }

    /// Atomically update ONLY the `.status` subobject of the stored resource,
    /// preserving the currently-stored spec and metadata.
    ///
    /// Mirrors the Kubernetes `/status` subresource (registry status strategy
    /// forces `new.Spec = old.Spec`): a status write must never clobber a
    /// concurrently-updated spec. A background controller computing status from
    /// a stale snapshot would otherwise write its whole stale object back via
    /// [`Storage::update`], reverting a spec the client just changed (this is
    /// the ResourceQuota update+delete conformance flake, GitHub #268).
    ///
    /// Implemented as a compare-and-swap read-modify-write (upstream's
    /// `guaranteedUpdate`): re-read the current object, graft only the incoming
    /// `status` onto it, and write with the fresh resourceVersion so a racing
    /// spec update either wins the CAS (and we retry onto its value) or is
    /// preserved. Only `value`'s `status` field is ever applied.
    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Extract only the `status` subobject from the caller's value. The spec
        // (and everything else) comes from the freshly-read stored object, so a
        // stale caller can never overwrite spec.
        let incoming = serde_json::to_value(value).map_err(Error::Serialization)?;
        let new_status = incoming.get("status").cloned();

        // CAS-retry read-modify-write: graft status onto the current object and
        // write with its fresh resourceVersion. If a concurrent writer bumped
        // the version between our read and write, `update`'s optimistic
        // concurrency check fails with Conflict and we re-read onto its value.
        const MAX_ATTEMPTS: usize = 8;
        for attempt in 0..MAX_ATTEMPTS {
            let mut current: serde_json::Value = self.get(key).await?;
            if let Some(obj) = current.as_object_mut() {
                match &new_status {
                    Some(status) => {
                        obj.insert("status".to_string(), status.clone());
                    }
                    None => {
                        obj.remove("status");
                    }
                }
            }
            match self.update::<serde_json::Value>(key, &current).await {
                Ok(updated) => {
                    return serde_json::from_value(updated).map_err(Error::Serialization)
                }
                Err(Error::Conflict(_)) if attempt + 1 < MAX_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
        Err(Error::Conflict(format!(
            "update_status: exhausted {} CAS retries for {}",
            MAX_ATTEMPTS, key
        )))
    }

    /// Update a resource with raw JSON value (for GC operations)
    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()>;

    /// Delete a resource
    async fn delete(&self, key: &str) -> Result<()>;

    /// Delete a resource the way a CLIENT would: hand the deletion to the
    /// server and let it apply its own semantics — for pods, graceful
    /// termination (stamp `deletionTimestamp`, let the kubelet drain, remove
    /// the object afterwards).
    ///
    /// This is what upstream's controllers do. The StatefulSet controller
    /// scales down through `podControl.DeleteStatefulPod` →
    /// `client.CoreV1().Pods(ns).Delete(...)`
    /// (pkg/controller/statefulset/stateful_pod_control.go:97); the DaemonSet
    /// controller and the taint-eviction manager delete the same way. None of
    /// them writes `deletionTimestamp` themselves — it is immutable on update,
    /// and an api-server that enforces that (any upstream one) rejects the
    /// write with
    /// `metadata.deletionTimestamp: Invalid value: ...: field is immutable`,
    /// leaving the pod running forever.
    ///
    /// The default here is for the direct-store backends (etcd/rhino/memory),
    /// where there is no api-server in the path: emulate what the api-server
    /// would have done, which is what those controllers used to do inline.
    /// `ApiStorage` overrides it with a real DELETE.
    async fn delete_gracefully(&self, key: &str) -> Result<()> {
        let mut obj: serde_json::Value = match self.get(key).await {
            Ok(v) => v,
            // Already gone: deleting is idempotent, same as the api-server's
            // 404 being benign to a controller that wanted it deleted.
            Err(Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e),
        };
        let Some(metadata) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
            // No ObjectMeta to stamp — nothing graceful to emulate.
            return self.delete(key).await;
        };
        if metadata.contains_key("deletionTimestamp") {
            return Ok(()); // deletion already in progress
        }
        metadata.insert(
            "deletionTimestamp".to_string(),
            serde_json::Value::String(
                chrono::Utc::now()
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                    .to_string(),
            ),
        );
        let grace = obj
            .pointer("/spec/terminationGracePeriodSeconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(30);
        if let Some(metadata) = obj.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            metadata.insert(
                "deletionGracePeriodSeconds".to_string(),
                serde_json::Value::from(grace),
            );
        }
        self.update_raw(key, &obj).await
    }

    /// List resources with a given prefix
    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync;

    /// List a prefix as of `revision`.
    ///
    /// Paged lists must behave as a snapshot: upstream's `GetList` pins the
    /// revision of the first page and every continuation reads at that same
    /// revision (`etcd3/store.go`, `withRev`), so an object written midway
    /// through a walk cannot appear in a later page.
    ///
    /// The default implementation ignores `revision` and delegates to
    /// [`Storage::list`] — correct for backends with no historical reads, at
    /// the cost of snapshot semantics.
    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let _ = revision;
        self.list(prefix).await
    }

    /// Paginated list — return up to `limit` items in deterministic (sorted by
    /// storage key) order and a continue token for the next page, if any.
    ///
    /// `continue_token` is the opaque token returned from a previous call;
    /// `None` requests the first page. When the storage backend has compacted
    /// past the token's referenced revision, returns [`Error::Gone`] so the
    /// handler can surface `410 Gone` with reason `Expired`.
    ///
    /// The default implementation performs in-memory chunking on top of
    /// [`Storage::list`]: list the full prefix, sort by key, slice, and
    /// embed the next sort key in the token. Backends that can stream a
    /// partial range from native pagination (e.g. etcd `RangeRequest.limit`)
    /// may override for efficiency.
    ///
    /// This is a *storage-level* primitive that resumes by sort key. The
    /// handler-level offset-based helper in `rusternetes_common::pagination`
    /// (see `paginate`) operates on already-fetched `Vec<T>` and is used by
    /// resource handlers that need to filter/decorate items before paging.
    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        if limit == 0 {
            // limit=0 means "no chunking, return everything".
            return Ok((self.list(prefix).await?, None));
        }

        // Decode the continue token and settle compaction BEFORE reading any
        // data. Reading at a compacted revision fails at the backend, which
        // would bury the `GoneWithContinue` path below and turn a 410 Expired
        // (with a resumable rv = -1 token) into a 500.
        let decoded = match continue_token {
            Some(token) => Some(decode_default_token(token)?),
            None => None,
        };

        if let Some(d) = &decoded {
            // A token with rv == -1 is an "inconsistent" continue token issued
            // after a compaction: resume from the recorded key at the CURRENT
            // revision, skipping the compaction check. Mirrors upstream
            // `ValidateListOptions` (interfaces.go): continueRV < 0 means "read
            // at the latest resource version".
            if d.compacted_at != Some(INCONSISTENT_CONTINUE_RV) {
                if let Some(rv) = d.compacted_at {
                    if rv > 0 && self.is_revision_compacted(rv).await.unwrap_or(false) {
                        // A strict (rv-pinned) token whose revision has been
                        // compacted. Rather than a dead-end 410, return a fresh
                        // inconsistent continue token (same start key, rv = -1)
                        // so the client can resume from the last key at the
                        // current revision — the list is then inconsistent but
                        // completes. Mirrors upstream
                        // `handleCompactedErrorForPaging` (etcd3/errors.go),
                        // which returns a 410 whose `ListMeta.Continue` is a
                        // fresh rv=-1 token.
                        return Err(compacted_continue_error(&d.start_key, rv));
                    }
                }
            }
        }

        // Default path: list everything, sort by a stable per-item key, slice.
        // Backends with native pagination (e.g. etcd `RangeRequest.limit`) may
        // override this for efficiency.
        //
        // The page sequence is pinned to one revision so later pages cannot
        // observe writes made after the walk began (upstream pins `withRev` for
        // exactly this reason); the continue token carries that revision and
        // only the first page establishes it.
        //
        // On the first page the revision is read BEFORE the data, never after.
        // Upstream gets both atomically — it takes the pin from the read's own
        // response header (`getResp.Header.Revision`, `etcd3/store.go`) — and
        // our `list` surfaces no header, so the cheap equivalent is to order
        // the same two round trips the safe way round. Stamping the token from
        // a `current_revision()` read *after* the data would pin the sequence
        // to a revision NEWER than page 1 was read at, and an object created in
        // that window would then appear in page 2: the exact leak the pin
        // exists to prevent, just through a narrower window.
        let pinned_rv = decoded
            .as_ref()
            .and_then(|d| d.compacted_at)
            .filter(|rv| *rv > 0);
        let (all, page_rv): (Vec<serde_json::Value>, i64) = match pinned_rv {
            Some(rv) => match self.list_at_revision(prefix, rv).await {
                Ok(items) => (items, rv),
                // The explicit probe above swallows its own failures
                // (`unwrap_or(false)`) so a transient blip cannot turn every
                // list into a 410. That means a genuinely compacted revision
                // can still reach this read; when it does, the caller is owed
                // the same resumable 410 the probe would have produced, not the
                // backend's raw error as a 500.
                Err(e) if is_compaction_error(&e) => {
                    let start_key = decoded.as_ref().map(|d| d.start_key.as_str()).unwrap_or("");
                    return Err(compacted_continue_error(start_key, rv));
                }
                Err(e) => return Err(e),
            },
            None => match self.current_revision().await {
                Ok(rv) if rv > 0 => (self.list_at_revision(prefix, rv).await?, rv),
                // Backend has no usable revision (or the probe failed): read
                // live and leave the sequence unpinned. That is all such a
                // backend can offer anyway — its `list_at_revision` ignores the
                // revision.
                _ => (self.list(prefix).await?, 0),
            },
        };

        let mut indexed: Vec<(String, serde_json::Value)> =
            all.into_iter().map(|v| (default_sort_key(&v), v)).collect();
        indexed.sort_by(|a, b| a.0.cmp(&b.0));

        let start = match &decoded {
            Some(d) => indexed
                .iter()
                .position(|(k, _)| k.as_str() >= d.start_key.as_str())
                .unwrap_or(indexed.len()),
            None => 0,
        };

        let end = (start + limit).min(indexed.len());
        let next_token = if end < indexed.len() {
            let next_key = &indexed[end].0;
            // Carry `page_rv` forward unchanged — it is the revision this page
            // was actually read at. Re-reading `current_revision()` per page
            // would re-pin the sequence to "now" one page at a time, which is
            // the same live view the pin exists to prevent. Upstream threads
            // the first page's `withRev` through every continuation
            // (`etcd3/store.go`); only the first page establishes it.
            Some(encode_default_token(next_key, page_rv))
        } else {
            None
        };

        let mut out = Vec::with_capacity(end - start);
        for (_, v) in indexed.drain(start..end) {
            out.push(serde_json::from_value(v).map_err(Error::Serialization)?);
        }
        Ok((out, next_token))
    }

    /// Watch for changes to resources with a given prefix
    async fn watch(&self, prefix: &str) -> Result<WatchStream>;

    /// Watch for changes starting from a specific revision
    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream>;

    /// Get the current storage revision (etcd mod_revision)
    async fn current_revision(&self) -> Result<i64>;

    /// Check if a revision has been compacted (no longer available)
    async fn is_revision_compacted(&self, revision: i64) -> Result<bool>;
}

/// Default sort key for an opaque JSON resource — uses
/// `metadata.namespace/metadata.name` so iteration order matches
/// `/registry/<type>/<ns>/<name>` storage layout.
fn default_sort_key(v: &serde_json::Value) -> String {
    let ns = v
        .pointer("/metadata/namespace")
        .and_then(|n| n.as_str())
        .unwrap_or("");
    let name = v
        .pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .unwrap_or("");
    if ns.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", ns, name)
    }
}

/// Decoded continue token (opaque to callers).
#[derive(Debug, Clone)]
pub struct ContinueToken {
    /// Sort key of the next item to return.
    pub start_key: String,
    /// Revision this page sequence is pinned to — the revision the *first*
    /// page was read at, threaded through every continuation so the walk is a
    /// snapshot rather than a live view (upstream's `withRev`, `etcd3/store.go`).
    /// It doubles as the compaction check: a backend that has compacted past it
    /// can no longer serve the sequence, so the walk is cut short with a
    /// resumable 410. [`INCONSISTENT_CONTINUE_RV`] (`-1`) means "no pin, read
    /// at the latest revision".
    ///
    /// The name predates the pinning role; it is kept to avoid churning call
    /// sites for no functional gain.
    pub compacted_at: Option<i64>,
}

/// The resumable 410 owed to a client whose continue token outlived its pinned
/// revision: a `Gone` carrying a fresh *inconsistent* token (same start key,
/// `rv = -1`) so the walk can finish at the current revision. Mirrors upstream
/// `handleCompactedErrorForPaging` (etcd3/errors.go).
///
/// Shared by both detection paths — the explicit `is_revision_compacted` probe
/// and the compaction error surfacing from the pinned read — so a client sees
/// an identical response whichever one fires.
fn compacted_continue_error(start_key: &str, rv: i64) -> Error {
    Error::GoneWithContinue {
        message: format!(
            "continue token expired (resource version {} has been compacted)",
            rv
        ),
        continue_token: encode_default_token(start_key, INCONSISTENT_CONTINUE_RV),
    }
}

/// True when a backend error means "that revision is gone" rather than
/// "something went wrong".
///
/// etcd answers a read below its compaction point with gRPC `OutOfRange` and
/// the message `etcdserver: mvcc: required revision has been compacted`
/// (`etcd3/errors.go` matches the same way, via `rpctypes.ErrCompacted`); our
/// backends wrap that text into [`Error::Storage`]. Matching on the text is
/// deliberate: the `Storage` trait deliberately does not leak the backend's
/// error type.
fn is_compaction_error(err: &Error) -> bool {
    let msg = match err {
        Error::Storage(m) | Error::Gone(m) => m.as_str(),
        Error::GoneWithContinue { message, .. } => message.as_str(),
        _ => return false,
    };
    msg.contains("has been compacted") || msg.contains("OutOfRange")
}

/// Resource-version sentinel for an "inconsistent" continue token — one issued
/// after a compaction to let a client resume from the last key at the *current*
/// revision. Encoded as `c1:-1:<key>`. Mirrors upstream's use of `rv = -1` in
/// `handleCompactedErrorForPaging` / `ValidateListOptions` (a negative rv means
/// "read at the latest revision"; `0` is reserved for an invalid/empty rv).
pub const INCONSISTENT_CONTINUE_RV: i64 = -1;

/// Encode a continue token. Format: `c1:<rv>:<key>`. The version prefix lets
/// us evolve the format without breaking existing clients.
pub fn encode_default_token(start_key: &str, rv: i64) -> String {
    format!("c1:{}:{}", rv, start_key)
}

/// Decode a continue token produced by [`encode_default_token`].
pub fn decode_default_token(token: &str) -> Result<ContinueToken> {
    let rest = token.strip_prefix("c1:").ok_or_else(|| {
        Error::InvalidResource(format!(
            "malformed continue token (unknown version): {}",
            token
        ))
    })?;
    let (rv_str, start_key) = rest.split_once(':').ok_or_else(|| {
        Error::InvalidResource(format!("malformed continue token (missing key): {}", token))
    })?;
    let rv: i64 = rv_str.parse().map_err(|_| {
        Error::InvalidResource(format!(
            "malformed continue token (bad resource version): {}",
            token
        ))
    })?;
    Ok(ContinueToken {
        start_key: start_key.to_string(),
        compacted_at: Some(rv),
    })
}

/// Event types for watch operations
#[derive(Debug, Clone)]
pub enum WatchEvent {
    Added(String, String),    // key, value
    Modified(String, String), // key, value
    Deleted(String, String),  // key, previous value (for Kubernetes compliance)
}

/// Stream of watch events
pub type WatchStream = futures::stream::BoxStream<'static, Result<WatchEvent>>;

/// Blanket implementation so `Arc<S>` can be used wherever `S: Storage` is required.
#[async_trait]
impl<S: Storage> Storage for std::sync::Arc<S> {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).create(key, value).await
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        (**self).get(key).await
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).update(key, value).await
    }

    async fn update_subresource<T>(&self, key: &str, subresource: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).update_subresource(key, subresource, value).await
    }

    // Must forward (not inherit the trait default), or the inner type's
    // `update_status` override is bypassed. Notably `ApiStorage` routes status
    // to the api-server's `/status` subresource — without this, an
    // `Arc<ApiStorage>` would fall back to get+update (a full PUT), which the
    // api-server strips of `.status`, silently dropping every status write.
    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).update_status(key, value).await
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        (**self).update_raw(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key).await
    }

    async fn delete_gracefully(&self, key: &str) -> Result<()> {
        (**self).delete_gracefully(key).await
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).list(prefix).await
    }

    // Must forward, or the inner type's `list_at_revision` override is bypassed
    // and the trait default (a live `list`, ignoring the revision) answers
    // instead. That failure mode is silently wrong data — an unpinned paged
    // list — not a compile error, which is why it is spelled out here rather
    // than left to the default.
    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).list_at_revision(prefix, revision).await
    }

    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        (**self).list_paginated(prefix, limit, continue_token).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        (**self).watch(prefix).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        (**self).watch_from_revision(prefix, revision).await
    }

    async fn current_revision(&self) -> Result<i64> {
        (**self).current_revision().await
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        (**self).is_revision_compacted(revision).await
    }
}

/// Configuration for selecting a storage backend.
pub enum StorageConfig {
    /// Use an external etcd cluster.
    Etcd {
        /// Etcd endpoint URLs (e.g. `["http://localhost:2379"]`).
        endpoints: Vec<String>,
    },
    /// Use an embedded SQLite database via rhino (requires `sqlite` feature).
    #[cfg(feature = "sqlite")]
    Sqlite {
        /// Path to the SQLite database file.
        path: String,
    },
    /// Use Redis via rhino (requires `redis` feature).
    #[cfg(feature = "redis")]
    Redis {
        /// Redis connection URL (e.g. `"redis://localhost:6379"`).
        url: String,
    },
}

/// Unified storage backend that dispatches to etcd, SQLite, Redis, or in-memory at runtime.
///
/// This allows all components to remain generic over `S: Storage` while the
/// concrete backend is chosen once at startup via `StorageConfig`.
#[allow(clippy::large_enum_variant)]
pub enum StorageBackend {
    Etcd(etcd::EtcdStorage),
    #[cfg(feature = "sqlite")]
    Sqlite(RhinoStorage),
    #[cfg(feature = "redis")]
    Redis(RhinoRedisStorage),
    /// In-memory backend backed by `MemoryStorage`. Intended for unit/integration
    /// tests that need a full `ApiServerState` without an external store.
    Memory(Arc<MemoryStorage>),
    /// API-client backend: every `Storage` call is proxied to the api-server
    /// over REST via [`api_storage::ApiStorage`]. Lets a component run as an
    /// api-server client (in-cluster kubelet/controller-manager/kube-proxy)
    /// while keeping its `Arc<StorageBackend>` handle unchanged.
    #[cfg(feature = "api-client")]
    Api(api_storage::ApiStorage),
}

/// True when two stored objects are equal ignoring `metadata.resourceVersion`.
///
/// Used to detect no-op updates so the storage layer can short-circuit them
/// (upstream etcd3 `GuaranteedUpdate` parity — see [`StorageBackend::update`]).
/// `serde_json::Value` equality is structural (key order independent), so this
/// is a faithful "byte-identical modulo resourceVersion" check for our
/// deterministic struct serialization.
fn objects_equal_ignoring_resource_version(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    fn without_rv(v: &serde_json::Value) -> serde_json::Value {
        let mut v = v.clone();
        if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            meta.remove("resourceVersion");
        }
        v
    }
    without_rv(a) == without_rv(b)
}

impl StorageBackend {
    /// Create a new storage backend from the given configuration.
    pub async fn new(config: StorageConfig) -> Result<Self> {
        match config {
            StorageConfig::Etcd { endpoints } => {
                let storage = etcd::EtcdStorage::new(endpoints).await?;
                Ok(StorageBackend::Etcd(storage))
            }
            #[cfg(feature = "sqlite")]
            StorageConfig::Sqlite { path } => {
                let storage = RhinoStorage::new(&path).await?;
                Ok(StorageBackend::Sqlite(storage))
            }
            #[cfg(feature = "redis")]
            StorageConfig::Redis { url } => {
                let storage = RhinoRedisStorage::new_redis(&url).await?;
                Ok(StorageBackend::Redis(storage))
            }
        }
    }

    /// Construct an in-memory backend suitable for unit/integration tests.
    /// Wraps `MemoryStorage` in an `Arc` so the same handle can be cloned by
    /// the caller (e.g. for `inject_conflicts(...)`) while the enum owns one
    /// copy.
    pub fn new_memory() -> Self {
        StorageBackend::Memory(Arc::new(MemoryStorage::new()))
    }

    /// Construct an API-client backend that proxies all `Storage` calls to the
    /// api-server over REST. The component keeps an ordinary
    /// `Arc<StorageBackend>` and never touches a real store directly.
    #[cfg(feature = "api-client")]
    pub fn new_api(client: Arc<rusternetes_client::http::ApiClient>) -> Self {
        StorageBackend::Api(api_storage::ApiStorage::new(client))
    }

    /// Request a bound ServiceAccount token from the api-server (TokenRequest
    /// subresource) when running as an api-server client. Returns `Ok(Some(tok))`
    /// for the `Api` backend and `Ok(None)` for the storage-direct backends
    /// (all-in-one/native), where the caller self-mints with the in-process
    /// signing key the co-located api-server trusts.
    ///
    /// Mirrors the upstream kubelet, which always obtains projected SA tokens
    /// via `serviceaccounts/{name}/token` rather than signing them, so the
    /// tokens are accepted by the (possibly foreign/vanilla) api-server that
    /// issued them.
    ///
    /// `bound_pod` is the `(name, uid)` of the pod mounting the token; it binds
    /// the token to that pod via `spec.boundObjectRef` so the api-server stamps
    /// the pod/node claims a TokenReview reports as
    /// `authentication.kubernetes.io/pod-name` & friends (#1684).
    #[cfg_attr(not(feature = "api-client"), allow(unused_variables))]
    pub async fn create_sa_token(
        &self,
        namespace: &str,
        name: &str,
        audiences: &[String],
        expiration_seconds: i64,
        bound_pod: Option<(&str, &str)>,
    ) -> Result<Option<String>> {
        #[cfg(feature = "api-client")]
        if let StorageBackend::Api(s) = self {
            return s
                .create_sa_token(namespace, name, audiences, expiration_seconds, bound_pod)
                .await
                .map(Some);
        }
        Ok(None)
    }

    /// Gracefully evict a pod, in a mode-aware way (#1284). `mutated_pod` is the
    /// victim with `deletionTimestamp` + eviction status already applied.
    ///
    /// Storage-direct backends persist it whole (a single PUT) — the kubelet's
    /// own loop then observes `deletionTimestamp` and terminates it, exactly as
    /// before. The `Api` backend can't (the api-server owns `deletionTimestamp`
    /// on a PUT), so it stamps the status via `/status` then issues a real
    /// graceful `DELETE` with `grace_seconds`. Mirrors the scheduler's
    /// `DataPlane::evict_pod_for_preemption`.
    ///
    /// `grace_seconds` is only consumed by the `Api` backend's graceful
    /// `DELETE`; the storage-direct backends carry the grace period inside
    /// `mutated_pod`'s `deletionGracePeriodSeconds` and just persist it. When the
    /// `api-client` feature is off the `Api` arm is compiled out, so the
    /// parameter is legitimately unused — silence the lint only in that config.
    #[cfg_attr(not(feature = "api-client"), allow(unused_variables))]
    pub async fn evict_pod<T>(&self, key: &str, mutated_pod: &T, grace_seconds: i64) -> Result<()>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => s.evict_pod_graceful(key, mutated_pod, grace_seconds).await,
            _ => {
                Storage::update(self, key, mutated_pod).await?;
                Ok(())
            }
        }
    }

    /// Attach an in-process event bus to the backend so internal `watch()`
    /// consumers get the in-process fast path. Only valid in a single-writer
    /// (all-in-one) process. No-op for etcd/memory: memory already has an
    /// in-process bus, and etcd keeps its native watch.
    pub fn enable_event_bus(&mut self) {
        match self {
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => {
                s.set_event_bus(EventBus::new(event_bus::DEFAULT_CAPACITY))
            }
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.set_event_bus(EventBus::new(event_bus::DEFAULT_CAPACITY)),
            _ => {}
        }
    }

    /// Native backend watch, bypassing any attached event bus. The api-server
    /// watch cache uses this so external HTTP watch clients keep the
    /// globally-RV-ordered feed even when the bus is enabled for internal
    /// consumers.
    pub async fn watch_backend(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => s.watch_native(prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => s.watch_native(prefix).await,
            StorageBackend::Memory(s) => Storage::watch(s.as_ref(), prefix).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::watch(s, prefix).await,
        }
    }
}

#[async_trait]
impl Storage for StorageBackend {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::create(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::create(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::create(s, key, value).await,
            StorageBackend::Memory(s) => Storage::create(s.as_ref(), key, value).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::create(s, key, value).await,
        }
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::get(s, key).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::get(s, key).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::get(s, key).await,
            StorageBackend::Memory(s) => Storage::get(s.as_ref(), key).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::get(s, key).await,
        }
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // No-op update short-circuit (upstream parity). Kubernetes' etcd3 store
        // skips the write when the new object is byte-identical to what is
        // stored, modulo resourceVersion — no write, no resourceVersion bump,
        // and crucially no watch event:
        //   staging/.../apiserver/pkg/storage/etcd3/store.go:
        //     if !origState.stale && bytes.Equal(data, origState.data) { return }
        // Without this, an idempotent controller update — e.g. cert-manager's
        // cainjector re-setting the SAME caBundle on a webhook config every
        // reconcile — publishes a MODIFIED event each time, which re-triggers
        // the controller into a ~20/sec hot-loop (#1566). The Api backend
        // proxies to a real api-server that already applies this semantic, so
        // skip the extra network round-trip there.
        let is_api_backend = {
            #[cfg(feature = "api-client")]
            {
                matches!(self, StorageBackend::Api(_))
            }
            #[cfg(not(feature = "api-client"))]
            {
                false
            }
        };
        if !is_api_backend {
            if let Ok(incoming) = serde_json::to_value(value) {
                if let Ok(current) = Storage::get::<serde_json::Value>(self, key).await {
                    // Only short-circuit when the caller's resourceVersion is
                    // absent or matches the stored one. A stale RV with
                    // otherwise-identical content must still surface a Conflict
                    // (upstream checks the precondition before the no-op
                    // short-circuit), so let those fall through to the backend.
                    let cur_rv = current.pointer("/metadata/resourceVersion");
                    let incoming_rv = incoming.pointer("/metadata/resourceVersion");
                    let rv_compatible = incoming_rv.is_none() || incoming_rv == cur_rv;
                    if rv_compatible && objects_equal_ignoring_resource_version(&current, &incoming)
                    {
                        return serde_json::from_value(current).map_err(Error::Serialization);
                    }
                }
            }
        }
        match self {
            StorageBackend::Etcd(s) => Storage::update(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::update(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::update(s, key, value).await,
            StorageBackend::Memory(s) => Storage::update(s.as_ref(), key, value).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update(s, key, value).await,
        }
    }

    async fn update_subresource<T>(&self, key: &str, subresource: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => {
                Storage::update_subresource(s, key, subresource, value).await
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => {
                Storage::update_subresource(s, key, subresource, value).await
            }
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => {
                Storage::update_subresource(s, key, subresource, value).await
            }
            StorageBackend::Memory(s) => {
                Storage::update_subresource(s.as_ref(), key, subresource, value).await
            }
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update_subresource(s, key, subresource, value).await,
        }
    }

    /// Status writes must reach the inner backend's `update_status` — for the
    /// `Api` variant that routes to the `/status` subresource, which a plain
    /// `update` (full PUT) would otherwise strip. Other backends keep the
    /// default get+graft+update behavior.
    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update_status(s, key, value).await,
            // Non-API backends: replicate the trait default (graft only the
            // incoming status onto the freshly-read object, CAS-retry on
            // conflict) — they don't separate status, so update() persists it.
            _ => {
                let incoming = serde_json::to_value(value).map_err(Error::Serialization)?;
                let new_status = incoming.get("status").cloned();
                const MAX_ATTEMPTS: usize = 8;
                for attempt in 0..MAX_ATTEMPTS {
                    let mut current: serde_json::Value = Storage::get(self, key).await?;
                    if let Some(obj) = current.as_object_mut() {
                        match &new_status {
                            Some(status) => {
                                obj.insert("status".to_string(), status.clone());
                            }
                            None => {
                                obj.remove("status");
                            }
                        }
                    }
                    match Storage::update::<serde_json::Value>(self, key, &current).await {
                        Ok(updated) => {
                            return serde_json::from_value(updated).map_err(Error::Serialization)
                        }
                        Err(Error::Conflict(_)) if attempt + 1 < MAX_ATTEMPTS => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(Error::Conflict(format!(
                    "update_status: exhausted {} CAS retries for {}",
                    MAX_ATTEMPTS, key
                )))
            }
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::update_raw(s, key, value).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::update_raw(s, key, value).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::update_raw(s, key, value).await,
            StorageBackend::Memory(s) => Storage::update_raw(s.as_ref(), key, value).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::update_raw(s, key, value).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::delete(s, key).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::delete(s, key).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::delete(s, key).await,
            StorageBackend::Memory(s) => Storage::delete(s.as_ref(), key).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::delete(s, key).await,
        }
    }

    async fn delete_gracefully(&self, key: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(s) => Storage::delete_gracefully(s, key).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::delete_gracefully(s, key).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::delete_gracefully(s, key).await,
            StorageBackend::Memory(s) => Storage::delete_gracefully(s.as_ref(), key).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::delete_gracefully(s, key).await,
        }
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::list(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::list(s, prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::list(s, prefix).await,
            StorageBackend::Memory(s) => Storage::list(s.as_ref(), prefix).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::list(s, prefix).await,
        }
    }

    // Dispatch explicitly rather than inheriting the trait default: the default
    // ignores the revision and reads live, so an un-dispatched
    // `list_at_revision` would silently unpin every paged list served through
    // this enum (wrong data, not a compile error).
    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => Storage::list_at_revision(s, prefix, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::list_at_revision(s, prefix, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::list_at_revision(s, prefix, revision).await,
            StorageBackend::Memory(s) => {
                Storage::list_at_revision(s.as_ref(), prefix, revision).await
            }
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::list_at_revision(s, prefix, revision).await,
        }
    }

    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
            StorageBackend::Memory(s) => {
                Storage::list_paginated(s.as_ref(), prefix, limit, continue_token).await
            }
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => {
                Storage::list_paginated(s, prefix, limit, continue_token).await
            }
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::watch(s, prefix).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::watch(s, prefix).await,
            StorageBackend::Memory(s) => Storage::watch(s.as_ref(), prefix).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::watch(s, prefix).await,
        }
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(s) => Storage::watch_from_revision(s, prefix, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::watch_from_revision(s, prefix, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::watch_from_revision(s, prefix, revision).await,
            StorageBackend::Memory(s) => {
                Storage::watch_from_revision(s.as_ref(), prefix, revision).await
            }
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::watch_from_revision(s, prefix, revision).await,
        }
    }

    async fn current_revision(&self) -> Result<i64> {
        match self {
            StorageBackend::Etcd(s) => Storage::current_revision(s).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::current_revision(s).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::current_revision(s).await,
            StorageBackend::Memory(s) => Storage::current_revision(s.as_ref()).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::current_revision(s).await,
        }
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        match self {
            StorageBackend::Etcd(s) => Storage::is_revision_compacted(s, revision).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => Storage::is_revision_compacted(s, revision).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => Storage::is_revision_compacted(s, revision).await,
            StorageBackend::Memory(s) => Storage::is_revision_compacted(s.as_ref(), revision).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(s) => Storage::is_revision_compacted(s, revision).await,
        }
    }
}

// AuthzStorage for StorageBackend — delegates to the inner implementation.
#[async_trait]
impl rusternetes_common::authz::AuthzStorage for StorageBackend {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => AuthzStorage::get(s, key, namespace).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => AuthzStorage::get(s, key, namespace).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => AuthzStorage::get(s, key, namespace).await,
            StorageBackend::Memory(s) => AuthzStorage::get(s.as_ref(), key, namespace).await,
            // The API-client backend is used by node/controller components that
            // never run the authorizer (only the api-server does, with a real
            // backend), so AuthzStorage is unreachable here.
            #[cfg(feature = "api-client")]
            StorageBackend::Api(_) => Err(Error::Internal(
                "AuthzStorage is not supported on the API-client backend".into(),
            )),
        }
    }

    async fn list<T>(&self, namespace: Option<&str>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        match self {
            StorageBackend::Etcd(s) => AuthzStorage::list(s, namespace).await,
            #[cfg(feature = "sqlite")]
            StorageBackend::Sqlite(s) => AuthzStorage::list(s, namespace).await,
            #[cfg(feature = "redis")]
            StorageBackend::Redis(s) => AuthzStorage::list(s, namespace).await,
            StorageBackend::Memory(s) => AuthzStorage::list(s.as_ref(), namespace).await,
            #[cfg(feature = "api-client")]
            StorageBackend::Api(_) => Err(Error::Internal(
                "AuthzStorage is not supported on the API-client backend".into(),
            )),
        }
    }
}

/// Helper function to build resource keys
pub fn build_key(resource_type: &str, namespace: Option<&str>, name: &str) -> String {
    match namespace {
        Some(ns) => format!("/registry/{}/{}/{}", resource_type, ns, name),
        None => format!("/registry/{}/{}", resource_type, name),
    }
}

/// Helper function to build prefix for listing
pub fn build_prefix(resource_type: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) => format!("/registry/{}/{}/", resource_type, ns),
        None => format!("/registry/{}/", resource_type),
    }
}

#[cfg(test)]
mod evict_tests {
    use super::*;
    use serde_json::json;

    /// Storage-mode eviction must behaviour-preservingly persist the mutated pod
    /// (deletionTimestamp + Evicted status) via a whole-object write — i.e.
    /// identical to the prior `update` path. #1284.
    #[tokio::test]
    async fn evict_pod_storage_mode_persists_mutated_pod() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/victim";
        let pod = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": "victim", "namespace": "default" },
            "status": { "phase": "Running" }
        });
        let _: serde_json::Value = Storage::create(&backend, key, &pod).await.unwrap();

        let mut evicted = pod.clone();
        evicted["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        evicted["status"]["phase"] = json!("Failed");
        evicted["status"]["reason"] = json!("Evicted");

        backend.evict_pod(key, &evicted, 30).await.unwrap();

        let stored: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        assert_eq!(stored["status"]["phase"], "Failed");
        assert_eq!(stored["status"]["reason"], "Evicted");
        assert!(
            stored["metadata"]["deletionTimestamp"].is_string(),
            "deletionTimestamp must persist in storage mode: {stored}"
        );
    }
}

#[cfg(test)]
mod graceful_delete_tests {
    use super::*;
    use serde_json::json;

    fn running_pod(name: &str, grace: i64) -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": name, "namespace": "default" },
            "spec": { "terminationGracePeriodSeconds": grace },
            "status": { "phase": "Running" }
        })
    }

    /// Direct-store backends have no api-server in the path, so the graceful
    /// delete emulates one: stamp deletionTimestamp + the pod's own grace
    /// period and leave the object for the kubelet to drain. This is exactly
    /// what the StatefulSet/DaemonSet/taint-eviction controllers used to do
    /// inline, so their behaviour on the compose and all-in-one stacks is
    /// unchanged by moving them onto this call.
    #[tokio::test]
    async fn graceful_delete_stamps_deletion_timestamp_without_a_server() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/ss-2";
        let _: serde_json::Value = Storage::create(&backend, key, &running_pod("ss-2", 30))
            .await
            .unwrap();

        Storage::delete_gracefully(&backend, key).await.unwrap();

        let stored: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        assert!(
            stored["metadata"]["deletionTimestamp"].is_string(),
            "graceful delete must stamp deletionTimestamp: {stored}"
        );
        assert_eq!(
            stored["metadata"]["deletionGracePeriodSeconds"], 30,
            "grace must come from the pod's own terminationGracePeriodSeconds: {stored}"
        );
    }

    /// Re-issuing it is a no-op, not a second timestamp: controllers reconcile
    /// repeatedly and must not keep rewriting a pod that is already draining
    /// (rewriting it is what an upstream api-server rejects as immutable).
    #[tokio::test]
    async fn graceful_delete_is_idempotent() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/ss-1";
        let _: serde_json::Value = Storage::create(&backend, key, &running_pod("ss-1", 5))
            .await
            .unwrap();

        Storage::delete_gracefully(&backend, key).await.unwrap();
        let first: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        Storage::delete_gracefully(&backend, key).await.unwrap();
        let second: serde_json::Value = Storage::get(&backend, key).await.unwrap();

        assert_eq!(
            first["metadata"]["deletionTimestamp"], second["metadata"]["deletionTimestamp"],
            "a second graceful delete must not re-stamp the timestamp"
        );
    }

    /// And an object that is already gone is success, not an error — the
    /// caller wanted it deleted.
    #[tokio::test]
    async fn graceful_delete_of_a_missing_object_succeeds() {
        let backend = StorageBackend::new_memory();
        Storage::delete_gracefully(&backend, "/registry/pods/default/never-existed")
            .await
            .expect("deleting an absent object is not an error");
    }
}

#[cfg(test)]
mod continue_token_tests {
    use super::*;

    /// A backend that has compacted every revision below `compacted_below`.
    ///
    /// `list_at_revision` fails the way a real etcd does for a read below the
    /// compaction point, so a `list_paginated` that reads before it checks for
    /// compaction surfaces that error instead of the `GoneWithContinue` the
    /// caller is owed. Above the compaction point it returns the *historical*
    /// two-object view, while `list` returns a three-object live view — the
    /// difference is what proves a page actually read at its pin instead of
    /// falling through to a live read — `list_at_revision` answers with the
    /// historical view at *any* uncompacted revision, the current one included,
    /// precisely so the two calls are distinguishable. Everything the test does
    /// not exercise is
    /// `unimplemented!()` on purpose — reaching it is a test bug.
    struct CompactingStore {
        compacted_below: i64,
        /// When false, `is_revision_compacted` reports "not compacted" for
        /// everything — the swallow (`Err(_) => Ok(false)`) that a real etcd
        /// probe performs on a transient failure. The pinned read then has to
        /// produce the 410 by itself.
        probe_reports_compaction: bool,
    }

    impl CompactingStore {
        fn new(compacted_below: i64) -> Self {
            Self {
                compacted_below,
                probe_reports_compaction: true,
            }
        }

        /// The historical (pinned) view: two objects.
        fn pinned_items() -> Vec<serde_json::Value> {
            vec![
                serde_json::json!({"metadata": {"namespace": "default", "name": "aaa"}}),
                serde_json::json!({"metadata": {"namespace": "default", "name": "bbb"}}),
            ]
        }

        /// The live view: the same two, bracketed by objects written after the
        /// pin. One sorts before the historical view and one after, so a page
        /// taken anywhere in the sequence can tell the two reads apart.
        fn live_items() -> Vec<serde_json::Value> {
            let mut v =
                vec![serde_json::json!({"metadata": {"namespace": "default", "name": "000-live"}})];
            v.extend(Self::pinned_items());
            v.push(serde_json::json!({"metadata": {"namespace": "default", "name": "zzz-live"}}));
            v
        }
    }

    #[async_trait]
    impl Storage for CompactingStore {
        async fn create<T>(&self, _key: &str, _value: &T) -> Result<T>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            unimplemented!("not exercised by these tests")
        }

        async fn get<T>(&self, _key: &str) -> Result<T>
        where
            T: DeserializeOwned + Send + Sync,
        {
            unimplemented!("not exercised by these tests")
        }

        async fn update<T>(&self, _key: &str, _value: &T) -> Result<T>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            unimplemented!("not exercised by these tests")
        }

        async fn update_raw(&self, _key: &str, _value: &serde_json::Value) -> Result<()> {
            unimplemented!("not exercised by these tests")
        }

        async fn delete(&self, _key: &str) -> Result<()> {
            unimplemented!("not exercised by these tests")
        }

        async fn list<T>(&self, _prefix: &str) -> Result<Vec<T>>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            Ok(serde_json::from_value(serde_json::Value::Array(Self::live_items())).unwrap())
        }

        async fn list_at_revision<T>(&self, _prefix: &str, revision: i64) -> Result<Vec<T>>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            if revision < self.compacted_below {
                // Verbatim shape of what etcd returns below its compaction
                // point, wrapped the way `EtcdStorage::list_at_revision` does.
                return Err(Error::Storage(format!(
                    "Failed to list at revision {}: status: OutOfRange, message: \
                     \"etcdserver: mvcc: required revision has been compacted\"",
                    revision
                )));
            }
            Ok(serde_json::from_value(serde_json::Value::Array(Self::pinned_items())).unwrap())
        }

        async fn watch(&self, _prefix: &str) -> Result<WatchStream> {
            unimplemented!("not exercised by these tests")
        }

        async fn watch_from_revision(&self, _prefix: &str, _revision: i64) -> Result<WatchStream> {
            unimplemented!("not exercised by these tests")
        }

        async fn current_revision(&self) -> Result<i64> {
            Ok(1000)
        }

        async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
            Ok(self.probe_reports_compaction && revision < self.compacted_below)
        }
    }

    /// Regression: the compaction check must run BEFORE the pinned read.
    ///
    /// Pinning the page sequence to the first page's revision means a strict
    /// continue token can outlive its revision. When it does, the caller is
    /// owed a 410 Expired carrying a fresh `rv = -1` token so the walk can
    /// resume (inconsistently) at the current revision — upstream
    /// `handleCompactedErrorForPaging` (etcd3/errors.go). Reading at the
    /// compacted revision first turns that into a backend error, i.e. a 500.
    #[tokio::test]
    async fn a_compacted_token_returns_gone_with_a_resumable_token() {
        let store = CompactingStore::new(100);
        let token = encode_default_token("default/foo", 42);

        let err = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            2,
            Some(&token),
        )
        .await
        .expect_err("a compacted continue token must not succeed");

        match err {
            Error::GoneWithContinue { continue_token, .. } => assert_eq!(
                continue_token,
                encode_default_token("default/foo", INCONSISTENT_CONTINUE_RV),
                "the 410 must carry a fresh inconsistent token at the same start key"
            ),
            other => panic!(
                "expected GoneWithContinue, got {other:?} — the pinned read ran \
                 before the compaction check and buried the 410"
            ),
        }
    }

    /// Regression: a compacted revision that the *probe* misses must still
    /// yield the same resumable 410.
    ///
    /// `is_revision_compacted` swallows its own errors — `unwrap_or(false)` at
    /// the call site, `Err(_) => Ok(false)` in `EtcdStorage` — deliberately, so
    /// a transient blip does not turn every list into a 410. The cost is that a
    /// genuinely compacted revision can slip past the probe and reach the
    /// pinned read. Before the pin existed, that fell through to a live `list`
    /// that simply succeeded; now it hits `list_at_revision`, whose backend
    /// error would surface as a 500 unless the call site recognises it.
    #[tokio::test]
    async fn a_compaction_the_probe_missed_still_returns_gone() {
        let store = CompactingStore {
            compacted_below: 100,
            probe_reports_compaction: false,
        };
        let token = encode_default_token("default/foo", 42);

        let err = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            2,
            Some(&token),
        )
        .await
        .expect_err("reading at a compacted revision must not succeed");

        match err {
            Error::GoneWithContinue { continue_token, .. } => assert_eq!(
                continue_token,
                encode_default_token("default/foo", INCONSISTENT_CONTINUE_RV),
                "the probe and the read must produce an identical 410 + resume token"
            ),
            other => panic!(
                "expected GoneWithContinue, got {other:?} — a compacted pinned \
                 read leaked the backend error as a 500"
            ),
        }
    }

    /// The `rv = -1` token issued by that 410 skips the compaction check and
    /// reads at the current revision, so the resumed walk completes.
    #[tokio::test]
    async fn an_inconsistent_token_resumes_at_the_current_revision() {
        let store = CompactingStore::new(100);
        let token = encode_default_token("default/aaa", INCONSISTENT_CONTINUE_RV);

        let (items, next) = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            10,
            Some(&token),
        )
        .await
        .expect("an inconsistent token must read at the latest rv, not fail on compaction");

        // Read at `current_revision()` (1000), which is above the compaction
        // point, so the walk completes from the recorded start key.
        assert_eq!(items.len(), 2, "resumed walk should complete: {items:?}");
        assert!(next.is_none());
    }

    /// And a token whose revision is still served does take the pinned read —
    /// the reorder must not have quietly dropped the pin.
    ///
    /// The fixture's live view carries an extra `zzz-live` object that the
    /// historical view does not, so a page containing it proves the read fell
    /// through to `list` instead of `list_at_revision`.
    #[tokio::test]
    async fn a_live_token_still_reads_at_its_pinned_revision() {
        let store = CompactingStore::new(5);
        let token = encode_default_token("default/aaa", 42);

        let (items, next) = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            10,
            Some(&token),
        )
        .await
        .expect("an uncompacted pinned token must read at its own revision");

        let names: Vec<&str> = items
            .iter()
            .map(|v| v["metadata"]["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            ["aaa", "bbb"],
            "a pinned page must not see objects written after the pin"
        );
        assert!(next.is_none());
    }

    /// Regression: the outgoing token must carry the SAME revision the incoming
    /// one did.
    ///
    /// Re-deriving it from `current_revision()` per page re-pins the sequence to
    /// "now" one page at a time — a live view wearing a snapshot's clothes. The
    /// end-to-end proof of that lives in the contract suite's `list_paging`,
    /// which only runs where Docker does; this covers the carry-forward on
    /// every runner.
    #[tokio::test]
    async fn the_outgoing_token_carries_the_incoming_pin() {
        let store = CompactingStore::new(5);
        let token = encode_default_token("default/aaa", 42);

        let (items, next) = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            1,
            Some(&token),
        )
        .await
        .expect("pinned read failed");

        assert_eq!(items.len(), 1);
        let next = next.expect("two items and limit=1 must leave a second page");
        assert_eq!(
            next,
            encode_default_token("default/bbb", 42),
            "the pin must be threaded through, not re-read from current_revision() \
             (which this fixture reports as 1000)"
        );
    }

    /// And the first page establishes the pin from a revision read BEFORE the
    /// data, so the token can never name a revision newer than the page it
    /// describes. `current_revision()` is 1000 here and the data comes from
    /// `list_at_revision(1000)`, never the live `list`.
    #[tokio::test]
    async fn the_first_page_pins_the_revision_it_read_at() {
        let store = CompactingStore::new(5);

        let (items, next) =
            Storage::list_paginated::<serde_json::Value>(&store, "/registry/pods/", 1, None)
                .await
                .expect("first page failed");

        assert_eq!(items[0]["metadata"]["name"], "aaa");
        assert_eq!(
            next.expect("a second page is owed"),
            encode_default_token("default/bbb", 1000),
            "the first page must stamp the revision it was read at"
        );
    }
}
