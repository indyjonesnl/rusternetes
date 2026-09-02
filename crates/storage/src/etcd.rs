use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, GetOptions, TxnOp, WatchOptions};
use futures::StreamExt;
use rusternetes_common::{authz::AuthzStorage, Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, error, info};

/// EtcdStorage implements the Storage trait using etcd as the backend.
///
/// The etcd `Client` is `Clone` and internally uses gRPC/tonic which
/// multiplexes requests over a single HTTP/2 connection. No mutex is needed —
/// cloning the client is cheap and allows fully concurrent access.
pub struct EtcdStorage {
    client: Client,
    /// Page size for prefix scans in [`Storage::list`]. Defaults to
    /// [`DEFAULT_LIST_PAGE_SIZE`]; overridable so tests can walk several pages
    /// without seeding hundreds of keys.
    page_size: i64,
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
            page_size: DEFAULT_LIST_PAGE_SIZE,
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
        let resp = client
            .get(key, None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to read resource: {}", e)))?;
        resp.kvs()
            .first()
            .map(|kv| kv.mod_revision())
            .ok_or_else(|| Error::NotFound(key.to_string()))
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
    async fn put_guaranteed(&self, key: &str, json: &str) -> Result<i64> {
        let mut client = self.client.clone();
        for _ in 0..GUARANTEED_UPDATE_ATTEMPTS {
            let expected = Self::read_mod_revision(&mut client, key).await?;
            // The failure branch's GET is upstream's `GetOnFailure: true`,
            // which `GuaranteedUpdate` always sets — it is part of the
            // recognised update shape, not an optional extra.
            let txn = etcd_client::Txn::new()
                .when(vec![Compare::mod_revision(key, CompareOp::Equal, expected)])
                .and_then(vec![TxnOp::put(key, json, None)])
                .or_else(vec![TxnOp::get(key, None)]);
            let resp = client
                .txn(txn)
                .await
                .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;
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

            let resp = client
                .get(next_start.clone(), Some(get_options))
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

                // Inject resourceVersion and deserialize in one step
                let json_with_rv = Self::inject_resource_version(json, mod_revision);
                match serde_json::from_str::<T>(&json_with_rv) {
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
    fn inject_resource_version(json: &str, mod_revision: i64) -> String {
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
            .and_then(vec![TxnOp::put(key, json.clone(), None)]);

        let txn_resp = client
            .txn(txn)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create resource: {}", e)))?;

        if !txn_resp.succeeded() {
            return Err(Error::AlreadyExists(key.to_string()));
        }

        debug!("Created resource at key: {}", key);

        // The put is the only operation in the transaction, so the response
        // header's revision *is* the new mod_revision. Upstream reads it the
        // same way (`resp.Revision = txnResp.Header.Revision`), which is why
        // the read-back GET this used to carry was always redundant.
        let mod_revision = txn_resp.header().map(|h| h.revision()).unwrap_or(0);

        // Inject resourceVersion and deserialize
        let json_with_rv = Self::inject_resource_version(&json, mod_revision);
        serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let mut client = self.client.clone();

        let resp = client
            .get(key, None)
            .await
            .map_err(|e| Error::Storage(format!("Failed to get resource: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let json = kv
                .value_str()
                .map_err(|e| Error::Storage(format!("Invalid UTF-8 in value: {}", e)))?;

            let mod_revision = kv.mod_revision();
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
                .and_then(vec![TxnOp::put(key, json.clone(), None)])
                .or_else(vec![TxnOp::get(key, None)]);

            let txn_resp = client
                .txn(txn)
                .await
                .map_err(|e| Error::Storage(format!("Failed to update resource: {}", e)))?;

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

            let json_with_rv = Self::inject_resource_version(&json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        } else {
            // No resourceVersion provided — read-modify-write under a guard
            // rather than a bare PUT, matching upstream `GuaranteedUpdate`.
            let mod_revision = self.put_guaranteed(key, &json).await?;

            debug!("Updated resource at key: {}", key);

            let json_with_rv = Self::inject_resource_version(&json, mod_revision);
            serde_json::from_str(&json_with_rv).map_err(Error::Serialization)
        }
    }

    async fn update_raw(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        let json = serde_json::to_string(value).map_err(Error::Serialization)?;

        self.put_guaranteed(key, &json).await?;

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
            let resp = client
                .txn(txn)
                .await
                .map_err(|e| Error::Storage(format!("Failed to delete resource: {}", e)))?;
            if resp.succeeded() {
                debug!("Deleted resource at key: {}", key);
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
        let watch_stream = stream.flat_map(move |watch_resp| {
            let _ = &watcher;
            let events: Vec<Result<WatchEvent>> = match watch_resp {
                Ok(resp) => {
                    resp.events().iter().map(|event| {
                        let key = event
                            .kv()
                            .map(|kv| kv.key_str().unwrap_or("").to_string())
                            .unwrap_or_default();
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
        let resp = client
            .get("/", None)
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
        let opts = GetOptions::new().with_revision(revision);
        match client.get("/registry/", Some(opts)).await {
            Ok(_) => Ok(false), // revision still available
            Err(etcd_client::Error::GRpcStatus(status)) => Ok(status.code()
                == tonic::Code::OutOfRange
                || status.message().contains("has been compacted")),
            Err(_) => Ok(false),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_inject_resource_version_with_existing() {
        let json = r#"{"metadata":{"name":"test","resourceVersion":"100"},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 200);
        assert!(result.contains("\"200\""));
        assert!(!result.contains("\"100\""));
    }

    #[test]
    fn test_inject_resource_version_without_existing() {
        let json = r#"{"metadata":{"name":"test"},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 42);
        assert!(result.contains("\"resourceVersion\":\"42\""));
    }

    #[test]
    fn test_inject_resource_version_empty_metadata() {
        let json = r#"{"metadata":{},"spec":{}}"#;
        let result = EtcdStorage::inject_resource_version(json, 99);
        assert!(result.contains("\"resourceVersion\":\"99\""));
    }

    // These tests exercise the real etcd backend by spinning up a disposable
    // etcd container via `testcontainers`. They require Docker (or a
    // Docker-compatible socket) on the host. The container is torn down
    // automatically when the `ContainerAsync` handle is dropped at the end of
    // the test, so no manual cleanup is required.
    //
    // On runners without a Docker socket (e.g. the ARC self-hosted runners
    // that back this repo's `cargo nextest` workflow), `start_etcd` returns
    // `None` and the test prints a skip message and exits 0 instead of
    // panicking. Don't promote this back to `expect("...")` — that's exactly
    // the regression that turned PR #746's nextest job red.
    //
    // The image and CLI flags match what `compose.yml` runs in production
    // (`quay.io/coreos/etcd:v3.5.17`, single-node, insecure client listener).

    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        GenericImage, ImageExt, TestcontainersError,
    };

    /// Detects whether the host has a usable Docker (or Docker-compatible)
    /// socket. We treat any `Client(Init(...))` failure from testcontainers
    /// as "no Docker" — that variant wraps the bollard connect error, which
    /// fires both for a missing `/var/run/docker.sock` and for a refused
    /// connection to a custom `DOCKER_HOST`. Either way the test cannot run.
    fn is_docker_unavailable(err: &TestcontainersError) -> bool {
        matches!(
            err,
            TestcontainersError::Client(testcontainers::core::client::ClientError::Init(_))
        )
    }

    /// Boot a single-node etcd container and return an `EtcdStorage` pointing
    /// at it, alongside the container handle which must be kept alive for the
    /// duration of the test (drop = teardown).
    ///
    /// Returns `None` when Docker isn't reachable so callers can soft-skip
    /// rather than fail the whole nextest job on Docker-less runners.
    async fn start_etcd() -> Option<(testcontainers::ContainerAsync<GenericImage>, EtcdStorage)> {
        // `quay.io/coreos/etcd` listens on 2379 for client gRPC and prints
        // "ready to serve client requests" once the listener is up. We bind
        // 0.0.0.0 so the port mapping works from the host network.
        let result = GenericImage::new("quay.io/coreos/etcd", "v3.5.17")
            .with_exposed_port(2379.tcp())
            .with_wait_for(WaitFor::message_on_stderr("ready to serve client requests"))
            .with_cmd([
                "/usr/local/bin/etcd",
                "--name=etcd-test",
                "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://0.0.0.0:2379",
            ])
            .start()
            .await;

        let container = match result {
            Ok(c) => c,
            Err(e) if is_docker_unavailable(&e) => {
                eprintln!("skipping etcd integration test: Docker unavailable ({e})");
                return None;
            }
            Err(e) => panic!("failed to start etcd test container: {e}"),
        };

        let host = container
            .get_host()
            .await
            .expect("failed to resolve test container host");
        let port = container
            .get_host_port_ipv4(2379)
            .await
            .expect("failed to read mapped etcd client port");

        let endpoint = format!("http://{host}:{port}");
        let storage = EtcdStorage::new(vec![endpoint])
            .await
            .expect("failed to connect to test etcd");

        Some((container, storage))
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let Some((_etcd, storage)) = start_etcd().await else {
            return;
        };

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let created = Storage::create(&storage, "/test/key", &data).await.unwrap();
        assert_eq!(created, data);

        let retrieved: TestData = Storage::get(&storage, "/test/key").await.unwrap();
        assert_eq!(retrieved, data);

        storage.delete("/test/key").await.unwrap();
    }
}
