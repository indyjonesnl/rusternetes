use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// In-memory storage implementation for testing
/// Default number of events retained for `watch_from_revision` replay.
///
/// Stands in for the watch cache's bounded ring
/// (`cacher/watch_cache.go`: `capacity`, `cache []*watchCacheEvent`).
pub const DEFAULT_HISTORY_CAPACITY: usize = 1000;

/// Bounded event history, ordered by write. Mirrors the watch cache's ring
/// (`cacher/watch_cache.go` `processEvent` + `updateCache`): the oldest event
/// is dropped once `capacity` is reached.
struct History {
    events: VecDeque<(i64, WatchEvent)>,
    capacity: usize,
    /// Highest revision evicted from `events`; events at or below it can no
    /// longer be replayed. Plays the role of upstream's `oldest` bound
    /// (`getAllEventsSinceLocked`, `watch_cache.go:885-896`).
    evicted_floor: i64,
    /// For each retained event, the value its key held BEFORE the write
    /// (`None` = the key did not exist), in the same order and with the same
    /// eviction as `events`. Lets `list_at_revision` rewind the live state to
    /// an earlier revision; stands in for etcd's MVCC revision history, which
    /// `WithRev` reads (`etcd3/store.go` `GetList`, `withRev`). Entries are
    /// `(revision, key, previous value)`.
    undo: VecDeque<(i64, String, Option<String>)>,
}

#[derive(Clone)]
pub struct MemoryStorage {
    /// Replay buffer for `watch_from_revision`. Guarded by a mutex that is
    /// also held across `bus.publish` and across subscribe+snapshot, so a
    /// watcher sees every event exactly once: either in the snapshot or live.
    /// (Upstream does the same under the watchCache lock: `watch_cache.go:339`
    /// `processEvent` and `cacher.go` `Watch` both take `w.Lock()`.)
    history: Arc<Mutex<History>>,
    data: Arc<RwLock<HashMap<String, String>>>,
    // In-process event bus for watch fan-out.
    bus: crate::EventBus,
    /// Number of remaining update() calls that will return Error::Conflict
    /// before delegating to the real update. Used by tests to inject CAS conflicts.
    conflict_update_count: Arc<AtomicUsize>,
    /// When set, `apply()` answers `Error::UnsupportedMediaType` (a server
    /// without Server Side Apply rejecting the apply PATCH, `415`). Test hook.
    reject_apply: Arc<std::sync::atomic::AtomicBool>,
    /// Number of `apply()` calls made (rejected or not). Test hook.
    apply_calls: Arc<AtomicUsize>,
    /// Highest compacted revision. Continue tokens issued before this RV are
    /// rejected with `Error::Gone`. Used by chunking tests to simulate the
    /// etcd compaction window.
    compacted_revision: Arc<std::sync::atomic::AtomicI64>,
    /// Monotonic write counter, standing in for etcd's cluster revision.
    ///
    /// Every write bumps it and stamps the result on the object's
    /// `metadata.resourceVersion`, mirroring what the etcd backend derives
    /// from `mod_revision` (`etcd.rs::inject_resource_version`). Without it a
    /// create answered with no resourceVersion at all, so an
    /// `Update()`-after-create -- ordinary read-modify-write, and what
    /// `client-go` does with the object it just got back -- had none to send,
    /// and every test running on this backend was blind to that whole class of
    /// bug (#1942).
    revision: Arc<std::sync::atomic::AtomicI64>,
    /// The TTL "lease" each expiring key currently holds, as a generation
    /// number. A key has at most one: a write that sets a new TTL, clears it,
    /// or deletes the key replaces or removes the entry, which orphans the
    /// timer armed under the old generation (it checks before it deletes).
    /// Stands in for the etcd lease a `clientv3.WithLease` put attaches.
    expiries: Arc<Mutex<HashMap<String, u64>>>,
    expiry_generation: Arc<std::sync::atomic::AtomicU64>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::with_history_capacity(DEFAULT_HISTORY_CAPACITY)
    }

    /// Like [`MemoryStorage::new`] with an explicit replay-buffer size.
    pub fn with_history_capacity(capacity: usize) -> Self {
        Self {
            history: Arc::new(Mutex::new(History {
                events: VecDeque::new(),
                capacity: capacity.max(1),
                evicted_floor: 0,
                undo: VecDeque::new(),
            })),
            data: Arc::new(RwLock::new(HashMap::new())),
            bus: crate::EventBus::new(crate::event_bus::DEFAULT_CAPACITY),
            conflict_update_count: Arc::new(AtomicUsize::new(0)),
            reject_apply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            apply_calls: Arc::new(AtomicUsize::new(0)),
            compacted_revision: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            // etcd's first revision is 1; 0 means "unset" on the wire.
            revision: Arc::new(std::sync::atomic::AtomicI64::new(1)),
            expiries: Arc::new(Mutex::new(HashMap::new())),
            expiry_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Replace `key`'s TTL: `0` detaches it (a write without a lease), a
    /// positive number arms a timer that deletes the key, and emits the
    /// `Deleted` watch event, when it fires -- what an expiring etcd lease
    /// does -- unless the TTL was replaced or the key deleted first.
    fn set_ttl(&self, key: &str, ttl: u64) {
        if ttl == 0 {
            self.expiries.lock().unwrap().remove(key);
            return;
        }
        let generation = self
            .expiry_generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.expiries
            .lock()
            .unwrap()
            .insert(key.to_string(), generation);
        let this = self.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(ttl)).await;
            let still_leased = {
                let mut expiries = this.expiries.lock().unwrap();
                if expiries.get(&key) == Some(&generation) {
                    expiries.remove(&key);
                    true
                } else {
                    false
                }
            };
            if still_leased {
                // Already gone is fine: the lease outlived nothing.
                let _ = Storage::delete(&this, &key).await;
            }
        });
    }

    /// Bump the write counter and stamp the new revision on
    /// `metadata.resourceVersion`, returning the serialized object.
    ///
    /// The etcd backend stores the blob without a resourceVersion and injects
    /// one on every read from the key's `mod_revision`
    /// (`etcd.rs:253-266`). Here the revision is stamped at write time
    /// instead, which reaches the same place -- reads, lists and watch frames
    /// all carry it -- and additionally overwrites a resourceVersion a caller
    /// put in a create body, which this backend used to persist verbatim so
    /// that a client could forge one.
    /// Record `event` (committed at `revision`) in the replay buffer and fan it
    /// out to live watchers, atomically with respect to `watch_from_revision`.
    ///
    /// `previous` is the value the key held before this write (`None` if it did
    /// not exist), kept so the state at an earlier revision can be rebuilt.
    fn emit(&self, revision: i64, event: WatchEvent, previous: Option<String>) {
        let mut h = self.history.lock().unwrap();
        if h.events.len() >= h.capacity {
            if let Some((rev, _)) = h.events.pop_front() {
                h.evicted_floor = h.evicted_floor.max(rev);
            }
            h.undo.pop_front();
        }
        let key = match &event {
            WatchEvent::Added(k, _) | WatchEvent::Modified(k, _) | WatchEvent::Deleted(k, _) => {
                k.clone()
            }
        };
        h.undo.push_back((revision, key, previous));
        h.events.push_back((revision, event.clone()));
        self.bus.publish(event);
    }

    fn stamp_revision(&self, value: &mut serde_json::Value) -> Result<String> {
        let rv = self
            .revision
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let rv = crate::concurrency::mod_revision_to_resource_version(rv);
        if let Some(metadata) = value.get_mut("metadata") {
            if let Some(obj) = metadata.as_object_mut() {
                obj.insert(
                    "resourceVersion".to_string(),
                    serde_json::Value::String(rv.clone()),
                );
            }
        }
        Ok(rv)
    }

    /// Simulate an etcd-style compaction: any continue token referencing a
    /// resource version `<= revision` will be rejected with `Error::Gone`.
    /// Used by the chunking conformance suite to drive the 410-after-compaction
    /// path without standing up a real etcd cluster.
    pub fn compact_to(&self, revision: i64) {
        self.compacted_revision
            .store(revision, std::sync::atomic::Ordering::SeqCst);
    }

    /// Clear all data (useful for test cleanup)
    pub fn clear(&self) {
        self.data.write().unwrap().clear();
    }

    /// Get the number of stored items
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Check if storage is empty
    pub fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }

    /// Arrange for the next `n` calls to `update()` to return `Error::Conflict`.
    /// Subsequent calls behave normally. Used by tests to verify retry logic.
    pub fn inject_conflicts(&self, n: usize) {
        self.conflict_update_count.store(n, Ordering::SeqCst);
    }

    /// Make every `apply()` fail with `Error::UnsupportedMediaType`, as an
    /// api-server without Server Side Apply does. Used by tests of callers
    /// that fall back to `Update` on `IsUnsupportedMediaType`.
    pub fn reject_apply_as_unsupported_media_type(&self, reject: bool) {
        self.reject_apply.store(reject, Ordering::SeqCst);
    }

    /// Number of `apply()` calls made so far.
    pub fn apply_calls(&self) -> usize {
        self.apply_calls.load(Ordering::SeqCst)
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    fn delete_inner(&self, key: &str, preconditions: Option<&crate::Preconditions>) -> Result<()> {
        let mut data = self.data.write().unwrap();
        if let Some(pre) = preconditions {
            let current = data
                .get(key)
                .ok_or_else(|| Error::NotFound(key.to_string()))?;
            let obj: serde_json::Value = serde_json::from_str(current)?;
            pre.check(key, &obj)?;
        }
        let previous_value = data
            .remove(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?;
        // A delete advances the cluster revision in etcd; a watcher replaying
        // from the revision the delete produced must not see it again.
        let rev = self
            .revision
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        drop(data); // Release lock before sending event
        self.expiries.lock().unwrap().remove(key);

        // Emit watch event with previous value
        // The deleted object carries the DELETE's revision, as etcd's watch
        // does (`etcd3/watcher.go` `parseEvent`: prevObj stamped with
        // `e.rev`): a watch snapshot cut dedupes DELETED by it (#2223).
        let undo_value = previous_value.clone();
        let stamped = match serde_json::from_str::<serde_json::Value>(&previous_value) {
            Ok(mut v) => match v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                Some(m) => {
                    m.insert(
                        "resourceVersion".to_string(),
                        serde_json::Value::String(
                            crate::concurrency::mod_revision_to_resource_version(rev),
                        ),
                    );
                    serde_json::to_string(&v).unwrap_or(previous_value)
                }
                None => previous_value,
            },
            Err(_) => previous_value,
        };
        self.emit(
            rev,
            WatchEvent::Deleted(key.to_string(), stamped),
            Some(undo_value),
        );

        Ok(())
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn apply<T>(
        &self,
        key: &str,
        apply_config: &serde_json::Value,
        field_manager: &str,
        force: bool,
        subresource: Option<&str>,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.apply_calls.fetch_add(1, Ordering::SeqCst);
        if self.reject_apply.load(Ordering::SeqCst) {
            return Err(Error::UnsupportedMediaType(
                "Apply not supported".to_string(),
            ));
        }
        crate::apply_via_update(self, key, apply_config, field_manager, force, subresource).await
    }

    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.create_with_ttl(key, value, 0).await
    }

    async fn create_with_ttl<T>(&self, key: &str, value: &T, ttl: u64) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut value_json: serde_json::Value = serde_json::to_value(value)?;

        // Generate UID if not already set or empty
        if let Some(metadata) = value_json.get_mut("metadata") {
            if let Some(metadata_obj) = metadata.as_object_mut() {
                // Check if uid field exists and is empty/null, or doesn't exist
                let should_generate_uid = match metadata_obj.get("uid") {
                    Some(serde_json::Value::String(s)) if !s.is_empty() => false,
                    _ => true, // None, null, empty string, or non-string
                };

                if should_generate_uid {
                    metadata_obj.insert(
                        "uid".to_string(),
                        serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
                    );
                }

                // Set creation timestamp if not set
                if metadata_obj.get("creationTimestamp").is_none()
                    || metadata_obj.get("creationTimestamp") == Some(&serde_json::Value::Null)
                {
                    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                    metadata_obj.insert(
                        "creationTimestamp".to_string(),
                        serde_json::Value::String(now),
                    );
                }
            }
        }

        // Check for the conflict before consuming a revision, so a rejected
        // create does not advance the counter (an etcd txn that fails its
        // compare does not bump the cluster revision either).
        {
            let data = self.data.read().unwrap();
            if data.contains_key(key) {
                return Err(Error::AlreadyExists(key.to_string()));
            }
        }

        let rev = self.stamp_revision(&mut value_json)?;
        let serialized = serde_json::to_string(&value_json)?;

        let mut data = self.data.write().unwrap();
        if data.contains_key(key) {
            return Err(Error::AlreadyExists(key.to_string()));
        }

        data.insert(key.to_string(), serialized.clone());
        drop(data); // Release lock before sending event
        self.set_ttl(key, ttl);

        // Emit watch event
        self.emit(
            rev.parse().unwrap_or_default(),
            WatchEvent::Added(key.to_string(), serialized.clone()),
            None,
        );

        Ok(serde_json::from_str(&serialized)?)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let data = self.data.read().unwrap();
        let value = data
            .get(key)
            .ok_or_else(|| Error::NotFound(key.to_string()))?;

        Ok(serde_json::from_str(value)?)
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
        // Inject a Conflict error if requested by a test (see inject_conflicts).
        if self.conflict_update_count.load(Ordering::SeqCst) > 0 {
            let prev =
                self.conflict_update_count
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                        if v > 0 {
                            Some(v - 1)
                        } else {
                            None
                        }
                    });
            if prev.is_ok() {
                return Err(Error::Conflict("injected conflict for test".to_string()));
            }
        }

        let mut value_json: serde_json::Value = serde_json::to_value(value)?;
        let incoming_rv = crate::concurrency::extract_resource_version(
            value_json
                .get("metadata")
                .unwrap_or(&serde_json::Value::Null),
        );

        // Optimistic concurrency, under ONE lock. Upstream's write is a single
        // etcd transaction guarded on the key's revision — `OptimisticPut`
        // (`vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go:83-107`):
        //
        // ```go
        // txn := k.KV.Txn(ctx).If(
        //     clientv3.Compare(clientv3.ModRevision(key), "=", expectedRevision),
        // ).Then(
        //     clientv3.OpPut(key, string(value), clientv3.WithLease(opts.LeaseID)),
        // )
        // ```
        //
        // and `GuaranteedUpdate` re-reads and retries when `!txnResp.Succeeded`
        // (`etcd3/store.go:597-616`). Comparing outside the lock that does the
        // write would let two writers both pass the compare and the later one
        // silently revert the earlier — which is the whole point of the check,
        // so the compare and the insert happen under the same write guard here.
        //
        // A value carrying no resourceVersion writes unconditionally, matching
        // `RhinoStorage::update` (`rhino.rs`) and upstream's unconditional
        // paths.
        let mut data = self.data.write().unwrap();
        let Some(stored) = data.get(key) else {
            return Err(Error::NotFound(key.to_string()));
        };
        if let Some(incoming_rv) = incoming_rv.as_deref() {
            let stored_rv = serde_json::from_str::<serde_json::Value>(stored)
                .ok()
                .and_then(|v| {
                    crate::concurrency::extract_resource_version(
                        v.get("metadata").unwrap_or(&serde_json::Value::Null),
                    )
                });
            if let Some(stored_rv) = stored_rv.as_deref() {
                if stored_rv != incoming_rv {
                    return Err(Error::Conflict(format!(
                        "resourceVersion mismatch: resource was modified (expected: {incoming_rv}, current: {stored_rv})"
                    )));
                }
            }
        }

        let rev = self.stamp_revision(&mut value_json)?;
        let serialized = serde_json::to_string(&value_json)?;
        let previous = data.insert(key.to_string(), serialized.clone());
        drop(data); // Release lock before sending event
        self.set_ttl(key, ttl);

        // Emit watch event
        self.emit(
            rev.parse().unwrap_or_default(),
            WatchEvent::Modified(key.to_string(), serialized.clone()),
            previous,
        );

        Ok(serde_json::from_str(&serialized)?)
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        {
            let data = self.data.read().unwrap();
            if !data.contains_key(key) {
                return Err(Error::NotFound(key.to_string()));
            }
        }

        let mut value = value.clone();
        let rev = self.stamp_revision(&mut value)?;
        let serialized = serde_json::to_string(&value)?;

        let mut data = self.data.write().unwrap();
        if !data.contains_key(key) {
            return Err(Error::NotFound(key.to_string()));
        }

        let previous = data.insert(key.to_string(), serialized.clone());
        drop(data); // Release lock before sending event

        // Emit watch event
        self.emit(
            rev.parse().unwrap_or_default(),
            WatchEvent::Modified(key.to_string(), serialized),
            previous,
        );

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.delete_inner(key, None)
    }

    /// `conditionalDelete` (`etcd3/store.go:361-453`): the precondition check
    /// and the removal happen under ONE write lock, which is this backend's
    /// equivalent of etcd's `OptimisticDelete` ModRevision guard -- no writer
    /// can slip between the check and the delete.
    async fn delete_with_preconditions(
        &self,
        key: &str,
        preconditions: &crate::Preconditions,
    ) -> Result<()> {
        self.delete_inner(key, Some(preconditions))
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let data = self.data.read().unwrap();
        // Return matches in key (lexicographic) order. A plain HashMap iterates
        // randomly, which makes order-dependent tests flaky AND lets a test pass
        // on MemoryStorage while behaving differently on the etcd/rhino backends,
        // whose range scans return key-sorted results (as does the real apiserver
        // List). Sorting by key here keeps MemoryStorage faithful to production.
        let mut matched: Vec<(&str, &str)> = data
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        matched.sort_by(|a, b| a.0.cmp(b.0));

        let mut results = Vec::with_capacity(matched.len());
        for (_, value) in matched {
            results.push(serde_json::from_str(value)?);
        }

        Ok(results)
    }

    /// The collection as it stood at `revision` — etcd's
    /// `Range(WithRev(revision))`, which `GetList` issues when
    /// `ValidateListOptions` yields `withRev` (`etcd3/store.go` GetList;
    /// `storage/interfaces.go:358-360,374-375`). The live state is rewound by
    /// undoing, newest first, every retained write newer than `revision`.
    ///
    /// A revision the retained history no longer covers (evicted from the
    /// ring) or at/below a compaction is `Gone`, upstream's
    /// `NewResourceExpired("The resourceVersion for the provided list is too
    /// old.")` (`etcd3/errors.go:53,68-72`). `revision <= 0` reads live, as
    /// `Range` with no revision does.
    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // The newest revision is never compacted: etcd keeps the latest
        // revision whatever the compaction point, and `list_paginated` reads
        // at `current_revision()` for a first page or an inconsistent (rv=-1)
        // continue (`ValidateListOptions`, `storage/interfaces.go:358-360`).
        if revision <= 0 || revision >= self.revision.load(std::sync::atomic::Ordering::SeqCst) {
            return self.list(prefix).await;
        }
        let compacted = self
            .compacted_revision
            .load(std::sync::atomic::Ordering::SeqCst);
        // Held across the data read so no write lands between the two: `emit`
        // takes this lock after the write, so a write either has its undo entry
        // here and its effect in `data`, or neither is visible... except the
        // sliver between a writer's `data` update and its `emit`; undoing only
        // entries newer than `revision` makes that harmless for a `revision`
        // at or below the revision the caller already observed.
        let h = self.history.lock().unwrap();
        if revision < h.evicted_floor || (compacted > 0 && revision <= compacted) {
            return Err(Error::Gone(
                "The resourceVersion for the provided list is too old.".to_string(),
            ));
        }
        let mut state: std::collections::BTreeMap<String, String> = self
            .data
            .read()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (rev, key, previous) in h.undo.iter().rev() {
            if *rev <= revision || !key.starts_with(prefix) {
                continue;
            }
            match previous {
                Some(v) => state.insert(key.clone(), v.clone()),
                None => state.remove(key),
            };
        }
        drop(h);
        state
            .values()
            .map(|v| serde_json::from_str(v).map_err(Error::from))
            .collect()
    }

    /// Replay every retained event with revision `>= revision` under `prefix`,
    /// then continue live -- etcd's `WithRev(start)` semantics
    /// (`etcd3/watcher.go:380`; callers pass `rv + 1`).
    ///
    /// A start revision the buffer no longer covers is `Gone`, upstream's
    /// `NewResourceExpired("too old resource version: ...")`
    /// (`cacher/watch_cache.go:915-916`), so the client relists rather than
    /// silently missing events. `revision <= 0` means "from now", as in etcd.
    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        if revision <= 0 {
            return self.watch(prefix).await;
        }
        let compacted = self
            .compacted_revision
            .load(std::sync::atomic::Ordering::SeqCst);
        let (replay, live) = {
            let h = self.history.lock().unwrap();
            if revision <= h.evicted_floor || (compacted > 0 && revision <= compacted) {
                return Err(Error::Gone(format!(
                    "too old resource version: {} ({})",
                    revision,
                    h.evicted_floor.max(compacted) + 1
                )));
            }
            // Subscribe and snapshot under the lock `emit` holds across
            // push+publish: no gap, no duplicate at the seam.
            let live = self.bus.subscribe(prefix);
            let replay: Vec<Result<WatchEvent>> = h
                .events
                .iter()
                .filter(|(rev, ev)| {
                    let key = match ev {
                        WatchEvent::Added(k, _)
                        | WatchEvent::Modified(k, _)
                        | WatchEvent::Deleted(k, _) => k,
                    };
                    *rev >= revision && key.starts_with(prefix)
                })
                .map(|(_, ev)| Ok(ev.clone()))
                .collect();
            (replay, live)
        };
        Ok(Box::pin(futures::StreamExt::chain(
            futures::stream::iter(replay),
            live,
        )))
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        Ok(self.bus.subscribe(prefix))
    }

    async fn current_revision(&self) -> Result<i64> {
        Ok(self.revision.load(std::sync::atomic::Ordering::SeqCst))
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        // Memory storage only compacts when a test explicitly calls
        // `compact_to(rv)` — see the chunking conformance suite.
        let compacted = self
            .compacted_revision
            .load(std::sync::atomic::Ordering::SeqCst);
        Ok(compacted > 0 && revision <= compacted)
    }
}

// AuthzStorage for MemoryStorage — used when StorageBackend::Memory is selected.
// The RBAC queries in tests use AlwaysAllowAuthorizer so these methods are
// typically not called; they're implemented for completeness.
#[async_trait::async_trait]
impl rusternetes_common::authz::AuthzStorage for MemoryStorage {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> rusternetes_common::Result<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync,
    {
        use crate::build_key;
        // Mirror the EtcdStorage pattern: derive a full /registry path from type name.
        // IMPORTANT: order matters — `type_name::<RoleBinding>()` also matches
        // `contains("Role")`, so the more-specific `RoleBinding`/`ClusterRoleBinding`
        // checks must come first or they get shadowed.
        let full_key = match namespace {
            Some(ns) => {
                let tn = std::any::type_name::<T>();
                if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/{}", ns, key)
                } else if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/{}", ns, key)
                } else {
                    build_key("unknown", Some(ns), key)
                }
            }
            None => {
                let tn = std::any::type_name::<T>();
                if tn.contains("ClusterRoleBinding") {
                    format!("/registry/clusterrolebindings/{}", key)
                } else if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    format!("/registry/clusterroles/{}", key)
                } else {
                    format!("/registry/unknown/{}", key)
                }
            }
        };
        Storage::get(self, &full_key).await
    }

    async fn list<T>(&self, namespace: Option<&str>) -> rusternetes_common::Result<Vec<T>>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    {
        // IMPORTANT: order matters — `type_name::<RoleBinding>()` also matches
        // `contains("Role")`, so the more-specific `RoleBinding`/`ClusterRoleBinding`
        // checks must come first or they get shadowed.
        let prefix = match namespace {
            Some(ns) => {
                let tn = std::any::type_name::<T>();
                if tn.contains("RoleBinding") && !tn.contains("Cluster") {
                    format!("/registry/rolebindings/{}/", ns)
                } else if tn.contains("Role") && !tn.contains("Cluster") {
                    format!("/registry/roles/{}/", ns)
                } else {
                    format!("/registry/unknown/{}/", ns)
                }
            }
            None => {
                let tn = std::any::type_name::<T>();
                if tn.contains("ClusterRoleBinding") {
                    "/registry/clusterrolebindings/".to_string()
                } else if tn.contains("ClusterRole") && !tn.contains("Binding") {
                    "/registry/clusterroles/".to_string()
                } else {
                    "/registry/unknown/".to_string()
                }
            }
        };
        Storage::list(self, &prefix).await
    }
}
