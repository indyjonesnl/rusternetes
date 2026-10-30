use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, GetOptions, PutOptions, TxnOp, WatchOptions};
use futures::StreamExt;
use rusternetes_common::{authz::AuthzStorage, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, error, info};

/// EtcdStorage implements the Storage trait using etcd as the backend.
///
/// The etcd `Client` is `Clone` and internally uses gRPC/tonic which
/// multiplexes requests over a single HTTP/2 connection. No mutex is needed —
/// cloning the client is cheap and allows fully concurrent access.
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub struct EtcdStorage {
    client: Client,
    /// Keeps the loopback bridges of any `unix:` endpoints alive (see
    /// `etcd_endpoint`); aborted when the last clone of the storage drops.
    _unix_bridges: Arc<Vec<crate::etcd_endpoint::UnixBridge>>,
    /// Highest revision the backend is known to have compacted away, or 0 when
    /// nothing is known yet.
    ///
    /// Upstream keeps exactly this — an in-process integer behind a lock,
    /// `compactor.compactRevision`, read via `CompactRevision()`
    /// (staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go:133-141) and
    /// exposed on the storage interface itself
    /// (`storage/interfaces.go:278`). It is answered from memory; upstream
    /// never asks the backend "is this revision compacted?".
    ///
    /// We used to ask, once per watch reconnect. Measured on a live kine
    /// cluster: 43 rejected `Range /registry/` calls per second, sustained,
    /// with kine pegged at ~1468% CPU (14.7 of 24 cores) — enough to push a
    /// 406-spec conformance phase from 27 minutes past 107, at which point
    /// specs fail on `context deadline exceeded` and read as product defects.
    compact_revision: Arc<AtomicI64>,
    /// Page size for prefix scans in [`Storage::list`]. Defaults to
    /// [`DEFAULT_LIST_PAGE_SIZE`]; overridable so tests can walk several pages
    /// without seeding hundreds of keys.
    page_size: i64,
    /// Grants the leases TTL'd writes attach (`etcd3.store.leaseManager`).
    leases: Arc<LeaseManager>,
    /// Per-key object sizes behind `Storage::stats` (stats.go
    /// `resourceSizeEstimator`), fed by the reads and writes that hold the
    /// encoded value.
    sizes: Arc<crate::size_estimator::SizeEstimator>,
}

/// `defaultLeaseReuseDurationSeconds` and `defaultLeaseMaxObjectCount`
/// (`staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go:29-30`).
const LEASE_REUSE_DURATION_SECONDS: i64 = 60;
const LEASE_MAX_OBJECT_COUNT: i64 = 1000;
/// `newDefaultLeaseManager` passes 0.05 as `leaseReuseDurationPercent`.
const LEASE_REUSE_DURATION_PERCENT: f64 = 0.05;

/// Port of `etcd3.leaseManager` (`lease_manager.go`). Lease operations are
/// expensive in etcd, so a write that needs a lease expiring about when the
/// previous one does reuses it instead of granting its own: every event has
/// the same TTL, and one lease per event would be one `LeaseGrant` per event.
/// Only the single previous lease is kept.
struct LeaseManager {
    state: std::sync::Mutex<LeaseState>,
}

#[derive(Default)]
struct LeaseState {
    prev_lease_id: i64,
    prev_expiration: Option<std::time::Instant>,
    attached_object_count: i64,
}

impl LeaseManager {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(LeaseState::default()),
        }
    }

    /// `getReuseDurationSecondsLocked`.
    fn reuse_duration_seconds(ttl: i64) -> i64 {
        ((LEASE_REUSE_DURATION_PERCENT * ttl as f64) as i64).min(LEASE_REUSE_DURATION_SECONDS)
    }

    /// The reusable previous lease for `ttl`, if there is one (the first half
    /// of `GetLease`): it must outlive the TTL (`valid`) by no more than the
    /// reuse window (`sufficient`), and carry fewer than the maximum object
    /// count. Every call counts as an attached object, success or failure.
    fn reusable(&self, ttl: i64, now: std::time::Instant) -> Option<i64> {
        let reuse = Self::reuse_duration_seconds(ttl);
        let mut st = self.state.lock().unwrap();
        st.attached_object_count += 1;
        let expiration = st.prev_expiration?;
        let valid = now + std::time::Duration::from_secs(ttl as u64) < expiration;
        let sufficient = now + std::time::Duration::from_secs((ttl + reuse) as u64) > expiration;
        (valid && sufficient && st.attached_object_count <= LEASE_MAX_OBJECT_COUNT)
            .then_some(st.prev_lease_id)
    }

    /// Cache a freshly granted lease (the second half of `GetLease`).
    fn granted(&self, id: i64, ttl: i64, now: std::time::Instant) {
        let mut st = self.state.lock().unwrap();
        st.prev_lease_id = id;
        st.prev_expiration = Some(now + std::time::Duration::from_secs(ttl as u64));
        st.attached_object_count = 1;
    }
}

/// Whether a cached compaction floor alone settles "is `revision` compacted?".
///
/// `Some(true)` means answer from memory; `None` means the floor cannot tell
/// and the backend must be asked. A floor of 0 means "nothing observed yet".
///
/// Compaction is monotonic — a revision below a floor already observed can
/// never become readable again — which is what makes the cached answer safe.
/// Upstream relies on the same monotonicity, keeping the floor with a `max`
/// (`UpdateCompactRevision`, etcd3/compact.go:139-146).
pub fn floor_settles_compaction(floor: i64, revision: i64) -> Option<bool> {
    if floor > 0 && revision <= floor {
        Some(true)
    } else {
        None
    }
}

/// Keys fetched per `RangeRequest` when listing a prefix, chosen to stay well
/// under the default 4MB gRPC message limit.
const DEFAULT_LIST_PAGE_SIZE: i64 = 500;

/// How many times an unversioned write re-reads and retries its guarded
/// transaction before giving up, mirroring upstream's `GuaranteedUpdate` retry
/// loop in `etcd3/store.go`.
const GUARANTEED_UPDATE_ATTEMPTS: usize = 5;

/// The exclusive upper bound of a prefix range, computed the way etcd's
/// `WithPrefix` does: the prefix with its last non-`0xff` byte incremented. An
/// empty or all-`0xff` prefix has no finite bound, which etcd spells as the
/// single zero byte "scan to the end of the keyspace".
fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

impl EtcdStorage {
    /// Create a new EtcdStorage instance
    pub async fn new(endpoints: Vec<String>) -> Result<Self> {
        let (endpoints, bridges) = crate::etcd_endpoint::resolve_endpoints(endpoints).await?;
        let options = Some(
            etcd_client::ConnectOptions::new()
                .with_keep_alive(
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(3),
                )
                .with_keep_alive_while_idle(true),
        );
        let client = Client::connect(endpoints, options)
            .await
            .map_err(|e| Error::Storage(format!("Failed to connect to etcd: {}", e)))?;

        info!("Connected to etcd successfully");

        Ok(Self {
            client,
            _unix_bridges: Arc::new(bridges),
            page_size: DEFAULT_LIST_PAGE_SIZE,
            compact_revision: Arc::new(AtomicI64::new(0)),
            leases: Arc::new(LeaseManager::new()),
            sizes: Default::default(),
        })
    }

    /// Override the number of keys fetched per page when listing a prefix.
    ///
    /// Production code uses the [`DEFAULT_LIST_PAGE_SIZE`]; tests lower it to
    /// exercise multi-page list behaviour cheaply.
    pub fn with_page_size(mut self, page_size: i64) -> Self {
        self.page_size = page_size;
        self
    }

    /// Helper to serialize a value to JSON
    fn serialize<T: Serialize>(value: &T) -> Result<String> {
        serde_json::to_string(value).map_err(Error::Serialization)
    }

    /// Read a key's current `mod_revision`, or [`Error::NotFound`].
    async fn read_mod_revision(client: &mut Client, key: &str) -> Result<i64> {
        let resp = crate::latency::tracked(client.get(key, None))
            .await
            .map_err(|e| Error::Storage(format!("Failed to read resource: {}", e)))?;
        resp.kvs()
            .first()
            .map(|kv| kv.mod_revision())
            .ok_or_else(|| Error::NotFound(key.to_string()))
    }

    /// The put a write carries: attached to a lease for `ttl` seconds, or
    /// bare for `ttl == 0` -- `ttlOpts` (`etcd3/store.go`) followed by
    /// `clientv3.OpPut(key, value, opts...)`. A put without a lease detaches
    /// the key from the one it held, which is how a TTL is removed.
    async fn put_op(&self, key: &str, json: &str, ttl: u64) -> Result<TxnOp> {
        if ttl == 0 {
            return Ok(TxnOp::put(key, json, None));
        }
        let ttl = i64::try_from(ttl)
            .map_err(|_| Error::Storage(format!("TTL {ttl}s overflows an etcd lease")))?;
        let now = std::time::Instant::now();
        let lease = match self.leases.reusable(ttl, now) {
            Some(id) => id,
            None => {
                // Grant a little extra, so the lease can be reused.
                let granted_ttl = ttl + LeaseManager::reuse_duration_seconds(ttl);
                let mut client = self.client.clone();
                let resp = client
                    .lease_grant(granted_ttl, None)
                    .await
                    .map_err(|e| Error::Storage(format!("Failed to grant lease: {}", e)))?;
                self.leases.granted(resp.id(), granted_ttl, now);
                resp.id()
            }
        };
        Ok(TxnOp::put(
            key,
            json,
            Some(PutOptions::new().with_lease(lease)),
        ))
    }

    /// Write an existing key without a caller-supplied resourceVersion,
    /// returning the new `mod_revision`.
    ///
    /// Upstream never issues a bare `Put`: a write with no expected revision
    /// goes through `GuaranteedUpdate` (`etcd3/store.go`), which reads the
    /// current revision, applies a `ModRevision`-guarded transaction, and
    /// retries from the top when a concurrent writer moves the key underneath
    /// it. Mirroring that keeps every mutation inside the upstream RPC subset
    /// *and* closes the lost-update race a bare `Put` leaves open.
    async fn put_guaranteed(&self, key: &str, json: &str, ttl: u64) -> Result<i64> {
        let mut client = self.client.clone();
        for _ in 0..GUARANTEED_UPDATE_ATTEMPTS {
            let expected = Self::read_mod_revision(&mut client, key).await?;
            // The failure branch's GET is upstream's `GetOnFailure: true`,
            // which `GuaranteedUpdate` always sets — it is part of the
            // recognised update shape, not an optional extra.
            let txn = etcd_client::Txn::new()
                .when(vec![Compare::mod_revision(key, CompareOp::Equal, expected)])
                .and_then(vec![self.put_op(key, json, ttl).await?])
                .or_else(vec![TxnOp::get(key, None)]);
            let resp = crate::latency::tracked(client.txn(txn))
                .await
                .map_err(|e| write_error("Failed to update resource", e))?;
            if resp.succeeded() {
                return Ok(resp.header().map(|h| h.revision()).unwrap_or(0));
            }
            // Lost the race — re-read and try again.
        }
        Err(Error::Conflict(format!(
            "failed to update {} after {} attempts: concurrent writers keep moving the revision",
            key, GUARANTEED_UPDATE_ATTEMPTS
        )))
    }

    /// The shared paging walk behind [`Storage::list`] and
    /// [`Storage::list_at_revision`] — the only difference between them is
    /// whether the range reads at a pinned `revision` or live.
    ///
    /// Paginate list calls to avoid hitting the default 4MB gRPC message size
    /// limit.
    ///
    /// Every page is an explicit range bounded by the prefix — `[start,
    /// prefix_range_end)` — continuing at `lastKey + "\x00"`, exactly as
    /// upstream does (`etcd3/store.go`: `continueKey = string(lastKey) +
    /// "\x00"`). The previous code combined `with_prefix()` with
    /// `with_from_key()`; the latter wins in `etcd-client`, so page 2 asked for
    /// the unbounded range `[lastKey, +inf)` and relied on a manual prefix
    /// re-check to trim the overshoot. That is outside the RPC subset and, on
    /// backends that do not implement open-ended ranges, silently returned
    /// nothing — truncating long lists to their first page.
    async fn list_inner<T>(&self, prefix: &str, revision: Option<i64>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();
        let page_size = self.page_size;
        let range_end = prefix_range_end(prefix.as_bytes());
        let mut results = Vec::new();
        let mut next_start: Vec<u8> = prefix.as_bytes().to_vec();

        loop {
            let mut get_options = GetOptions::new()
                .with_range(range_end.clone())
                .with_limit(page_size);
            if let Some(rev) = revision {
                get_options = get_options.with_revision(rev);
            }

            let resp = crate::latency::tracked(client.get(next_start.clone(), Some(get_options)))
                .await
                .map_err(|e| match revision {
                    Some(rev) => {
                        Error::Storage(format!("Failed to list at revision {}: {}", rev, e))
                    }
                    None => Error::Storage(format!("Failed to list resources: {}", e)),
                })?;

            let kvs = resp.kvs();
            for kv in kvs {
                let key_str = kv
                    .key_str()
                    .map_err(|e| Error::Storage(format!("Invalid UTF-8 in key: {}", e)))?;

                let json = kv
                    .value_str()
                    .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
                let mod_revision = kv.mod_revision();
                self.sizes.update_key(key_str, json.len(), mod_revision);

                // Inject resourceVersion and deserialize in one step.
                // `DecodeListItem` times exactly the codec decode
                // (etcd3/decoder.go:78-84).
                let decoded = crate::latency::tracked_decode(|| {
                    let json_with_rv = Self::inject_resource_version(json, mod_revision);
                    serde_json::from_str::<T>(&json_with_rv)
                });
                match decoded {
                    Ok(value) => {
                        results.push(value);
                    }
                    Err(e) => {
                        error!("Failed to deserialize value at {}: {}", key_str, e);
                        continue;
                    }
                }
            }

            // A short page means we reached the end of the range.
            if (kvs.len() as i64) < page_size {
                break;
            }

            // Continue immediately after the last key we saw.
            match kvs.last() {
                Some(last_kv) => {
                    next_start = last_kv.key().to_vec();
                    next_start.push(0);
                }
                None => break,
            }
        }

        Ok(results)
    }

    /// Inject resourceVersion into a JSON string by parsing, modifying, and re-serializing.
    ///
    /// This is a single parse→modify→reserialize pass (vs the old code which did
    /// parse→Value→modify→reserialize→from_value = two full cycles). Still faster
    /// than the original approach while being completely safe against edge cases.
    pub fn inject_resource_version(json: &str, mod_revision: i64) -> String {
        let rv_str = crate::concurrency::mod_revision_to_resource_version(mod_revision);
        if let Ok(mut v) = serde_json::from_str::<serde_json::Value>(json) {
            if let Some(metadata) = v.get_mut("metadata") {
                metadata["resourceVersion"] = serde_json::Value::String(rv_str);
            }
            // serde_json::to_string on a Value is infallible in practice
            serde_json::to_string(&v).unwrap_or_else(|_| json.to_string())
        } else {
            // Unparseable JSON — return as-is (should not happen with etcd data)
            json.to_string()
        }
    }
}

#[async_trait]
impl Storage for EtcdStorage {
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
        let mut client = self.client.clone();
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

        // Upstream's create is a single-op transaction guarded on the key not
        // existing — `OptimisticPut(key, data, 0)` in
        // `vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go`:
        //
        //     txn.If(clientv3.Compare(clientv3.ModRevision(key), "=", 0))
        //        .Then(clientv3.OpPut(key, value))
        //
        // Two details are deliberate: the guard compares `ModRevision` (not
        // `Version`), and the success branch holds exactly one operation. Any
        // other shape falls outside the subset etcd-API shims implement.
        let txn = etcd_client::Txn::new()
            .when(vec![Compare::mod_revision(key, CompareOp::Equal, 0)])
            .and_then(vec![self.put_op(key, &json, ttl).await?]);

        let txn_resp = crate::latency::tracked(client.txn(txn))
            .await
            .map_err(|e| write_error("Failed to create resource", e))?;

        if !txn_resp.succeeded() {
            return Err(Error::AlreadyExists(key.to_string()));
        }

        debug!("Created resource at key: {}", key);

        // The put is the only operation in the transaction, so the response
        // header's revision *is* the new mod_revision. Upstream reads it the
        // same way (`resp.Revision = txnResp.Header.Revision`), which is why
        // the read-back GET this used to carry was always redundant.
        let mod_revision = txn_resp.header().map(|h| h.revision()).unwrap_or(0);

        self.sizes.update_key(key, json.len(), mod_revision);

        // Inject resourceVersion and deserialize
        let json_with_rv = Self::inject_resource_version(&json, mod_revision);
        serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();

        let resp = crate::latency::tracked(client.get(key, None))
            .await
            .map_err(|e| Error::Storage(format!("Failed to get resource: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let json = kv
                .value_str()
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;

            let mod_revision = kv.mod_revision();
            self.sizes.update_key(key, json.len(), mod_revision);
            let json_with_rv = Self::inject_resource_version(json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            Err(Error::NotFound(key.to_string()))
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
        let mut client = self.client.clone();
        let json = Self::serialize(value)?;

        // Extract resourceVersion from the incoming resource
        let incoming_resource: serde_json::Value =
            serde_json::from_str(&json).map_err(Error::Serialization)?;
        let incoming_rv = crate::concurrency::extract_resource_version(
            incoming_resource
                .get("metadata")
                .unwrap_or(&serde_json::json!({})),
        );

        // Validate optimistic concurrency if resourceVersion is provided
        if let Some(incoming_rv) = incoming_rv.as_deref() {
            // Use a transaction to ensure atomic update with version check
            let expected_mod_revision =
                crate::concurrency::resource_version_to_mod_revision(incoming_rv)?;
            // Upstream `OptimisticPut(key, data, expectedRevision,
            // {GetOnFailure: true})`: one guarded PUT, and a GET only in the
            // failure branch so the conflict error can name the current
            // revision.
            let txn = etcd_client::Txn::new()
                .when(vec![Compare::mod_revision(
                    key,
                    CompareOp::Equal,
                    expected_mod_revision,
                )])
                .and_then(vec![self.put_op(key, &json, ttl).await?])
                .or_else(vec![TxnOp::get(key, None)]);

            let txn_resp = crate::latency::tracked(client.txn(txn))
                .await
                .map_err(|e| write_error("Failed to update resource", e))?;

            if !txn_resp.succeeded() {
                // Get the current resourceVersion from the failed txn's else branch
                let current_rv = txn_resp
                    .op_responses()
                    .first()
                    .and_then(|resp| {
                        if let etcd_client::TxnOpResponse::Get(get_resp) = resp {
                            get_resp.kvs().first().map(|kv| {
                                crate::concurrency::mod_revision_to_resource_version(
                                    kv.mod_revision(),
                                )
                            })
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| "unknown".to_string());
                return Err(Error::Conflict(format!(
                    "resourceVersion mismatch: resource was modified (expected: {}, current: {})",
                    incoming_rv, current_rv
                )));
            }

            debug!("Updated resource at key: {}", key);

            // Single-op success branch: the header revision is the new
            // mod_revision.
            let mod_revision = txn_resp.header().map(|h| h.revision()).unwrap_or(0);
            self.sizes.update_key(key, json.len(), mod_revision);

            let json_with_rv = Self::inject_resource_version(&json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            // No resourceVersion provided — read-modify-write under a guard
            // rather than a bare PUT, matching upstream `GuaranteedUpdate`.
            let mod_revision = self.put_guaranteed(key, &json, ttl).await?;

            debug!("Updated resource at key: {}", key);
            self.sizes.update_key(key, json.len(), mod_revision);

            let json_with_rv = Self::inject_resource_version(&json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let json = serde_json::to_string(value).map_err(Error::Serialization)?;

        self.put_guaranteed(key, &json, 0).await?;

        debug!("Updated resource (raw) at key: {}", key);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.client.clone();

        // Upstream `OptimisticDelete` (`client/v3/kubernetes/client.go`):
        //
        //     txn.If(clientv3.Compare(clientv3.ModRevision(key), "=", rev))
        //        .Then(clientv3.OpDelete(key))
        //
        // A bare `DeleteRange` would remove a revision we never observed, and
        // sits outside the RPC subset besides. Read the current revision, then
        // delete under a guard, retrying if a writer beats us to it.
        for _ in 0..GUARANTEED_UPDATE_ATTEMPTS {
            let expected = Self::read_mod_revision(&mut client, key).await?;
            let txn = etcd_client::Txn::new()
                .when(vec![Compare::mod_revision(key, CompareOp::Equal, expected)])
                .and_then(vec![TxnOp::delete(key, None)])
                .or_else(vec![TxnOp::get(key, None)]);
            let resp = crate::latency::tracked(client.txn(txn))
                .await
                .map_err(|e| Error::Storage(format!("Failed to delete resource: {}", e)))?;
            if resp.succeeded() {
                debug!("Deleted resource at key: {}", key);
                let rev = resp.header().map(|h| h.revision()).unwrap_or(0);
                self.sizes.delete_key(key, rev);
                return Ok(());
            }
        }

        Err(Error::Conflict(format!(
            "failed to delete {} after {} attempts: concurrent writers keep moving the revision",
            key, GUARANTEED_UPDATE_ATTEMPTS
        )))
    }

    /// `etcd3 store.conditionalDelete` (`etcd3/store.go:361-453`): read the
    /// object, `preconditions.Check` it, then `OptimisticDelete` under a
    /// `ModRevision == <revision read>` guard; if a writer moved the revision
    /// the guard fails, so re-read and re-check (a UID/RV that changed in
    /// between is then a precondition failure, never a delete).
    async fn delete_with_preconditions(
        &self,
        key: &str,
        preconditions: &crate::Preconditions,
    ) -> Result<()> {
        let mut client = self.client.clone();
        for _ in 0..GUARANTEED_UPDATE_ATTEMPTS {
            let resp = crate::latency::tracked(client.get(key, None))
                .await
                .map_err(|e| Error::Storage(format!("Failed to read resource: {}", e)))?;
            let kv = resp
                .kvs()
                .first()
                .ok_or_else(|| Error::NotFound(key.to_string()))?;
            let expected = kv.mod_revision();
            let json = kv
                .value_str()
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;
            let obj: serde_json::Value =
                serde_json::from_str(&Self::inject_resource_version(json, expected))
                    .map_err(Error::Serialization)?;
            preconditions.check(key, &obj)?;

            let txn = etcd_client::Txn::new()
                .when(vec![Compare::mod_revision(key, CompareOp::Equal, expected)])
                .and_then(vec![TxnOp::delete(key, None)])
                .or_else(vec![TxnOp::get(key, None)]);
            let resp = crate::latency::tracked(client.txn(txn))
                .await
                .map_err(|e| Error::Storage(format!("Failed to delete resource: {}", e)))?;
            if resp.succeeded() {
                debug!("Deleted resource at key: {} (preconditions held)", key);
                let rev = resp.header().map(|h| h.revision()).unwrap_or(0);
                self.sizes.delete_key(key, rev);
                return Ok(());
            }
        }

        Err(Error::Conflict(format!(
            "failed to delete {} after {} attempts: concurrent writers keep moving the revision",
            key, GUARANTEED_UPDATE_ATTEMPTS
        )))
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let results = self.list_inner(prefix, None).await?;
        debug!("Listed {} resources with prefix: {}", results.len(), prefix);
        Ok(results)
    }

    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.list_inner(prefix, Some(revision)).await
    }

    /// `storage/etcd3/stats.go` `resourceSizeEstimator.Stats`: a keys-only
    /// read of the prefix for the count, the average size from the per-key
    /// cache. Walks the range in pages like `list_inner`.
    async fn stats(&self, prefix: &str) -> Result<crate::ResourceStats> {
        let mut client = self.client.clone();
        let page_size = self.page_size;
        let range_end = prefix_range_end(prefix.as_bytes());
        let mut next_start: Vec<u8> = prefix.as_bytes().to_vec();
        let mut keys: Vec<String> = Vec::new();
        loop {
            let opts = GetOptions::new()
                .with_range(range_end.clone())
                .with_limit(page_size)
                .with_keys_only();
            let resp = crate::latency::tracked(client.get(next_start.clone(), Some(opts)))
                .await
                .map_err(|e| Error::Storage(format!("Failed to list keys: {}", e)))?;
            let kvs = resp.kvs();
            for kv in kvs {
                keys.push(
                    kv.key_str()
                        .map_err(|e| Error::Storage(format!("Invalid UTF-8 in key: {}", e)))?
                        .to_string(),
                );
            }
            if (kvs.len() as i64) < page_size {
                break;
            }
            match kvs.last() {
                Some(last) => {
                    next_start = last.key().to_vec();
                    next_start.push(0);
                }
                None => break,
            }
        }
        Ok(self.sizes.stats(prefix, &keys))
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        let mut client = self.client.clone();
        let watch_options = WatchOptions::new()
            .with_prefix()
            .with_prev_key()
            .with_start_revision(revision);
        let (watcher, stream) = client
            .watch(prefix, Some(watch_options))
            .await
            .map_err(|e| {
                Error::Storage(format!(
                    "Failed to create watch from revision {}: {}",
                    revision, e
                ))
            })?;
        info!(
            "Started watching prefix: {} from revision {}",
            prefix, revision
        );
        // Use flat_map to handle multiple events per etcd watch response.
        // etcd can batch multiple events into a single response, and we must
        // emit all of them — not just the first one.
        // IMPORTANT: Move `watcher` into the closure to keep the watch alive.
        // Dropping it closes the gRPC stream, which terminates the watch.
        let sizes = self.sizes.clone();
        let watch_stream = stream.flat_map(move |watch_resp| {
            let _ = &watcher;
            let events: Vec<Result<WatchEvent>> = match watch_resp {
                Ok(resp) => {
                    resp.events().iter().map(|event| {
                        let key = event
                            .kv()
                            .map(|kv| kv.key_str().unwrap_or("").to_string())
                            .unwrap_or_default();
                        // Feed the size estimator like upstream's watch loop
                        // (storage/etcd3/watcher.go:408-415) (#2991).
                        if let Some(kv) = event.kv() {
                            match event.event_type() {
                                etcd_client::EventType::Put => {
                                    sizes.update_key(&key, kv.value().len(), kv.mod_revision())
                                }
                                etcd_client::EventType::Delete => {
                                    sizes.delete_key(&key, kv.mod_revision())
                                }
                            }
                        }
                        match event.event_type() {
                            etcd_client::EventType::Put => {
                                let raw_value = event
                                    .kv()
                                    .map(|kv| String::from_utf8_lossy(kv.value()).to_string())
                                    .unwrap_or_default();
                                let mod_revision = event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                let value = Self::inject_resource_version(&raw_value, mod_revision);
                                // Distinguish create from update the way
                                // upstream does — `clientv3.Event.IsCreate()`
                                // is `Type == PUT && CreateRevision ==
                                // ModRevision` (`client/v3/watch.go`). Unlike
                                // `prev_kv` it survives compaction of the
                                // previous revision, and unlike the key
                                // `Version` field every etcd-API
                                // implementation populates it.
                                let is_create = event
                                    .kv()
                                    .map(|kv| kv.create_revision() == kv.mod_revision())
                                    .unwrap_or(false);
                                debug!("etcd watch_from_rev event: key={} mod_rev={} type={}",
                                    key, mod_revision,
                                    if is_create { "ADDED" } else { "MODIFIED" });
                                if is_create {
                                    Ok(WatchEvent::Added(key, value))
                                } else {
                                    Ok(WatchEvent::Modified(key, value))
                                }
                            }
                            etcd_client::EventType::Delete => {
                                let mod_revision = event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                // Use prev_kv for the deleted object's value.
                                // If prev_kv is missing (etcd compaction), construct a
                                // minimal JSON object with just metadata so watchers
                                // can still deliver the DELETE event.
                                let prev_value = if let Some(prev_kv) = event.prev_kv() {
                                    let raw = String::from_utf8_lossy(prev_kv.value()).to_string();
                                    Self::inject_resource_version(&raw, mod_revision)
                                } else {
                                    // Extract name from key: /registry/{type}/{ns}/{name}
                                    let parts: Vec<&str> = key.split('/').collect();
                                    let name = parts.last().unwrap_or(&"");
                                    let ns = if parts.len() >= 4 { parts[parts.len()-2] } else { "" };
                                    format!(
                                        r#"{{"metadata":{{"name":"{}","namespace":"{}","resourceVersion":"{}"}}}}"#,
                                        name, ns, mod_revision
                                    )
                                };
                                Ok(WatchEvent::Deleted(key, prev_value))
                            }
                        }
                    }).collect()
                }
                Err(e) => vec![Err(Error::Storage(format!("Watch error: {}", e)))],
            };
            futures::stream::iter(events)
        });
        Ok(Box::pin(watch_stream))
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let mut client = self.client.clone();

        // Enable prev_kv to get the previous value on DELETE events (required for Kubernetes)
        let watch_options = WatchOptions::new().with_prefix().with_prev_key();
        let (watcher, stream) = client
            .watch(prefix, Some(watch_options))
            .await
            .map_err(|e| Error::Storage(format!("Failed to create watch: {}", e)))?;

        info!("Started watching prefix: {}", prefix);

        // Convert etcd watch stream to our WatchStream.
        // Use flat_map to handle multiple events per etcd watch response.
        // IMPORTANT: Move `watcher` into the closure to keep the watch alive.
        // Dropping it closes the gRPC stream, which terminates the watch.
        let sizes = self.sizes.clone();
        let watch_stream = stream.flat_map(move |watch_resp| {
            let _ = &watcher;
            let events: Vec<Result<WatchEvent>> = match watch_resp {
                Ok(resp) => resp
                    .events()
                    .iter()
                    .map(|event| {
                        let key = event
                            .kv()
                            .map(|kv| kv.key_str().unwrap_or("").to_string())
                            .unwrap_or_default();

                        // Feed the size estimator like upstream's watch loop
                        // (storage/etcd3/watcher.go:408-415) (#2991).
                        if let Some(kv) = event.kv() {
                            match event.event_type() {
                                etcd_client::EventType::Put => {
                                    sizes.update_key(&key, kv.value().len(), kv.mod_revision())
                                }
                                etcd_client::EventType::Delete => {
                                    sizes.delete_key(&key, kv.mod_revision())
                                }
                            }
                        }
                        match event.event_type() {
                            etcd_client::EventType::Put => {
                                let raw_value = event
                                    .kv()
                                    .and_then(|kv| kv.value_str().ok())
                                    .unwrap_or("")
                                    .to_string();

                                let mod_revision =
                                    event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                let value = Self::inject_resource_version(&raw_value, mod_revision);

                                // See `watch_from_revision`: upstream's
                                // `IsCreate()` is `CreateRevision ==
                                // ModRevision`, not `Version == 1`.
                                let is_create = event
                                    .kv()
                                    .map(|kv| kv.create_revision() == kv.mod_revision())
                                    .unwrap_or(false);
                                if is_create {
                                    Ok(WatchEvent::Added(key, value))
                                } else {
                                    Ok(WatchEvent::Modified(key, value))
                                }
                            }
                            etcd_client::EventType::Delete => {
                                let raw_prev = event
                                    .prev_kv()
                                    .and_then(|kv| kv.value_str().ok())
                                    .unwrap_or("")
                                    .to_string();
                                let mod_revision =
                                    event.kv().map(|kv| kv.mod_revision()).unwrap_or(0);
                                let prev_value =
                                    Self::inject_resource_version(&raw_prev, mod_revision);
                                Ok(WatchEvent::Deleted(key, prev_value))
                            }
                        }
                    })
                    .collect(),
                Err(e) => vec![Err(Error::Storage(format!("Watch error: {}", e)))],
            };
            futures::stream::iter(events)
        });

        Ok(Box::pin(watch_stream))
    }

    async fn current_revision(&self) -> Result<i64> {
        let mut client = self.client.clone();
        // A single-key GET of a key that does not exist: the response carries
        // the current revision in its header and no payload. (`keys_only`
        // would be a hair cheaper but is outside the RPC subset — and
        // pointless here, since this range matches at most one key.)
        let resp = crate::latency::tracked(client.get("/", None))
            .await
            .map_err(|e| Error::Storage(format!("Failed to get current revision: {}", e)))?;
        Ok(resp.header().map(|h| h.revision()).unwrap_or(0))
    }

    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        let mut client = self.client.clone();
        // Read a single key at the requested revision. etcd answers a read
        // below its compaction point with `OutOfRange` / "required revision
        // has been compacted"; anything else means the revision is still
        // served.
        //
        // Caveat: an etcd-API shim need not report compaction at all. kine
        // prunes rows on `Compact` and then answers historical reads with
        // whatever survives instead of erroring, so there this always reports
        // "not compacted" and callers fall through to a merely inconsistent
        // list rather than a 410. That is a property of the backend, not a
        // call outside the subset.
        // Answer from the cached floor when we can. Compaction only ever moves
        // forward, so a revision at or below a floor we have already observed
        // is compacted for good and needs no round trip.
        let floor = self.compact_revision.load(Ordering::Relaxed);
        if let Some(answer) = floor_settles_compaction(floor, revision) {
            return Ok(answer);
        }

        let opts = GetOptions::new().with_revision(revision);
        match crate::latency::tracked(client.get("/registry/", Some(opts))).await {
            Ok(_) => Ok(false), // revision still available
            Err(etcd_client::Error::GRpcStatus(status)) => {
                let compacted = status.code() == tonic::Code::OutOfRange
                    || status.message().contains("has been compacted");
                if compacted {
                    // Learn the floor, so every later query at or below this
                    // revision is answered from memory. `fetch_max` mirrors
                    // upstream's `UpdateCompactRevision`, which is also a max:
                    //
                    // ```text
                    // c.compactRevision = max(c.compactRevision, rev)
                    // ```
                    //
                    // (compact.go:139-146) — the floor must never move
                    // backwards on a stale or out-of-order observation.
                    self.compact_revision.fetch_max(revision, Ordering::Relaxed);
                }
                Ok(compacted)
            }
            Err(_) => Ok(false),
        }
    }
}

/// Classify a failed write. Upstream hands the raw etcd error to the handlers,
/// whose `isTooLargeError` (endpoints/handlers/rest.go:457-471) matches either
///
/// ```text
/// etcdErr.Code() == grpccodes.InvalidArgument && etcdErr.Error() == "etcdserver: request is too large"
/// grpcErr.GRPCStatus().Code() == grpccodes.ResourceExhausted && strings.Contains(grpcErr.GRPCStatus().Message(), "trying to send message larger than max")
/// ```
///
/// so the same two are tagged [`Error::StorageTooLarge`] here.
pub fn write_error(what: &str, e: etcd_client::Error) -> Error {
    let too_large = match &e {
        etcd_client::Error::GRpcStatus(status) => status_is_too_large(status),
        _ => false,
    };
    let msg = format!("{what}: {e}");
    if too_large {
        Error::StorageTooLarge(msg)
    } else {
        Error::Storage(msg)
    }
}

pub fn status_is_too_large(status: &tonic::Status) -> bool {
    (status.code() == tonic::Code::InvalidArgument
        && status.message() == "etcdserver: request is too large")
        || (status.code() == tonic::Code::ResourceExhausted
            && status
                .message()
                .contains("trying to send message larger than max"))
}

// Implement AuthzStorage for EtcdStorage
#[async_trait]
impl AuthzStorage for EtcdStorage {
    async fn get<T>(&self, key: &str, namespace: Option<&str>) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        // Build the full key based on the resource type and namespace.
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
