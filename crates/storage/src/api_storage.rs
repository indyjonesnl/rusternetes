//! [`ApiStorage`]: a [`Storage`] implementation backed by the api-server's
//! REST API instead of a storage backend directly.
//!
//! Every controller in this crate is generic over `S: Storage` and touches
//! cluster state only through the trait. `ApiStorage` lets the SAME controllers
//! run as an api-server *client* — the in-cluster path, and the all-in-one
//! binary over loopback — with no per-controller change: each trait call is
//! translated to the matching REST verb.
//!
//! - storage keys `/registry/{plural}/{ns}/{name}` ↔ REST paths
//!   (`/api/v1/...` or `/apis/{group}/{version}/...`), resolved through a
//!   built-in GVR table ([`static_resource_info`]) with a discovery fallback;
//! - `get`/`list` → GET, `create` → POST, `update` → PUT, `delete` → DELETE;
//! - `update_status` → PUT to the `/status` subresource (grafted onto a fresh
//!   GET so a stale caller cannot clobber spec, mirroring the storage impl);
//! - `watch` → the api-server's `?watch=true` JSON-lines stream, re-keyed to
//!   the `/registry/...` form [`rusternetes_storage::extract_key`] expects, and
//!   **shared**: all controllers watching the same resource type fan out from a
//!   single upstream connection via a broadcast (#1138).
//!
//! ## Known gaps (tracked follow-ups, not adapter bugs)
//!
//! - **Status via `update`.** Resources whose api-server handler enforces the
//!   status subresource (e.g. pods) strip `.status` on a full-object PUT — so a
//!   controller that persists status via [`Storage::update`] rather than
//!   [`Storage::update_status`] will not see it stick in API mode. That is
//!   faithful Kubernetes behavior; the fix is per-controller (use the status
//!   subresource), not here.
//! - **GVR resolution.** Built-in types resolve through a static table
//!   ([`static_resource_info`]) with no network. Types not in the table (CRDs,
//!   aggregated APIs, and the arbitrary types the garbage collector traverses)
//!   are resolved by loading the api-server's discovery once and caching it. A
//!   plural that is neither built-in nor present in discovery (e.g. a CRD whose
//!   controller runs but whose CRD is not registered) errors via [`unmapped`].
//! - **`current_revision`/`is_revision_compacted`** are best-effort (0 / false):
//!   no controller in this crate calls them, and `list_paginated` is a handler
//!   concern, not a controller one.

use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use futures::StreamExt;
use rusternetes_client::http::{ApiClient, GetError, KubernetesList};
use rusternetes_client::watch::{watch_stream, WatchEvent as ClientWatchEvent};
use rusternetes_common::{Error, Result};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};

/// Per-resource-type buffer for the shared watch broadcast. A subscriber that
/// falls more than this many events behind is told to relist (see `watch`).
///
/// tokio's broadcast eagerly allocates all `N` slots up front, so this is a
/// fixed per-watch cost (× ~one channel per resource type). 256 is ample lag
/// tolerance for promptly-draining controllers while keeping the idle-RAM
/// footprint small (#1138); a lagged subscriber just relists.
const SHARED_WATCH_BUFFER: usize = 256;

/// Registry of live shared upstream watches, keyed by resolved collection path.
type SharedWatches = Arc<Mutex<HashMap<String, broadcast::Sender<Arc<WatchEvent>>>>>;

/// A [`Storage`] implementation that proxies to the api-server over REST.
pub struct ApiStorage {
    client: Arc<ApiClient>,
    /// Discovery-resolved `plural -> (api_root, namespaced)` for types not in
    /// the built-in [`static_resource_info`] table (CRDs, aggregated APIs, and
    /// the arbitrary types the garbage collector traverses). `None` until the
    /// first miss triggers a one-shot discovery load; `Some` thereafter.
    dynamic: RwLock<Option<HashMap<String, (String, bool)>>>,
    /// One shared upstream `?watch=true` stream per resolved collection path,
    /// fanned out to every controller watching that type via a broadcast — so
    /// the ~50 controller watch() calls collapse to ~one HTTP connection per
    /// resource type (idle-RAM, #1138).
    shared_watches: SharedWatches,
}

impl ApiStorage {
    pub fn new(client: Arc<ApiClient>) -> Self {
        Self {
            client,
            dynamic: RwLock::new(None),
            shared_watches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Gracefully evict a pod through the api-server (#1284). The api-server
    /// owns `deletionTimestamp` on a PUT, so the storage-mode "set
    /// deletionTimestamp + update" path won't terminate the victim in API
    /// mode. Stamp the eviction status via the `/status` subresource
    /// (best-effort — a status hiccup must not abort the eviction), then issue
    /// a real DELETE with `gracePeriodSeconds` so the api-server marks the pod
    /// for deletion and the kubelet shuts it down. Mirrors the scheduler's
    /// `DataPlane::evict_pod_for_preemption`.
    pub async fn evict_pod_graceful<T: Serialize>(
        &self,
        key: &str,
        mutated_pod: &T,
        grace_seconds: i64,
    ) -> Result<()> {
        let path = self.object_path(key).await?;
        let status_path = format!("{path}/status");
        let _: std::result::Result<Value, _> = self.client.put(&status_path, mutated_pod).await;
        let query = vec![("gracePeriodSeconds".to_string(), grace_seconds.to_string())];
        let status = self
            .client
            .delete_with_options(&path, &query, None)
            .await
            .map_err(|e| Error::Storage(format!("{e:#}")))?;
        if status.is_success() || status.as_u16() == 404 {
            Ok(())
        } else {
            Err(Error::Storage(format!("evict {key} failed: HTTP {status}")))
        }
    }

    /// Request a bound ServiceAccount token from the api-server's TokenRequest
    /// subresource (`POST /api/v1/namespaces/{ns}/serviceaccounts/{name}/token`).
    ///
    /// This is how the upstream kubelet obtains projected SA tokens
    /// (`pkg/kubelet/token`: `CoreV1().ServiceAccounts(ns).CreateToken`) — it
    /// never self-signs. An api-server only trusts tokens IT signed, so a
    /// kubelet that self-mints is rejected (401) by a foreign/vanilla api-server
    /// (breaks in-cluster clients like kindnet). Returns the issued token.
    pub async fn create_sa_token(
        &self,
        namespace: &str,
        name: &str,
        audiences: &[String],
        expiration_seconds: i64,
    ) -> Result<String> {
        let path = format!("/api/v1/namespaces/{namespace}/serviceaccounts/{name}/token");
        let body = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "spec": { "audiences": audiences, "expirationSeconds": expiration_seconds },
        });
        let resp: Value = self
            .client
            .post(&path, &body)
            .await
            .map_err(map_write_err)?;
        resp.pointer("/status/token")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                Error::Internal(format!(
                    "TokenRequest for {namespace}/{name} returned no status.token"
                ))
            })
    }

    /// Resolve a plural to `(api_root, namespaced)`. Built-in types hit the
    /// static table with no network; on a miss, the api-server's discovery is
    /// loaded once and cached, then retried, so CRD/aggregated types map too.
    async fn resolve(&self, rt: &str) -> Result<(String, bool)> {
        if let Some((root, namespaced)) = static_resource_info(rt) {
            return Ok((root.to_string(), namespaced));
        }
        // Fast path: already-loaded discovery cache.
        if let Some(map) = self.dynamic.read().await.as_ref() {
            return map.get(rt).cloned().ok_or_else(|| unmapped(rt));
        }
        // Slow path: load discovery once, cache it, then look up. A concurrent
        // miss may load too — idempotent, last write wins.
        let map = self.load_discovery().await;
        let found = map.get(rt).cloned();
        *self.dynamic.write().await = Some(map);
        found.ok_or_else(|| unmapped(rt))
    }

    /// Like [`Self::resolve`] but yields `None` for a type the api-server does
    /// not serve, so `list`/`watch` can mirror storage mode's "empty
    /// collection" semantics instead of erroring (and log-spamming) on, e.g., a
    /// CRD whose controller runs but whose CRD is not registered.
    async fn try_resolve(&self, rt: &str) -> Option<(String, bool)> {
        self.resolve(rt).await.ok()
    }

    /// Build `plural -> (api_root, namespaced)` from the api-server's discovery
    /// documents: core `/api/v1`, then every group's preferred version. Best
    /// effort — discovery failures yield an empty map (callers then error with
    /// `unmapped`), never a panic.
    async fn load_discovery(&self) -> HashMap<String, (String, bool)> {
        let mut map = HashMap::new();
        // Core group.
        if let Ok(list) = self.client.get::<Value>("/api/v1").await {
            ingest_resource_list(&mut map, "/api/v1", &list);
        }
        // Named groups: /apis -> APIGroupList, then each preferred groupVersion.
        if let Ok(groups) = self.client.get::<Value>("/apis").await {
            if let Some(arr) = groups.get("groups").and_then(|g| g.as_array()) {
                for g in arr {
                    let gv = g
                        .get("preferredVersion")
                        .and_then(|pv| pv.get("groupVersion"))
                        .and_then(|v| v.as_str());
                    if let Some(gv) = gv {
                        let root = format!("/apis/{gv}");
                        if let Ok(list) = self.client.get::<Value>(&root).await {
                            ingest_resource_list(&mut map, &root, &list);
                        }
                    }
                }
            }
        }
        map
    }
}

/// `Error` for a plural that is neither built-in nor present in discovery.
fn unmapped(rt: &str) -> Error {
    Error::Internal(format!(
        "ApiStorage: resource type '{rt}' not served by the api-server (not built-in, absent from discovery)"
    ))
}

/// Query params that force an immediate (hard) delete on the api-server.
///
/// `Storage::delete` is unconditional removal in every backend. The api-server
/// only removes a pod immediately when `gracePeriodSeconds=0`; otherwise it
/// does a graceful (soft) delete that just stamps `deletionTimestamp`. Sending
/// grace=0 keeps the Api backend's `delete` consistent with the others — see
/// the call site in `ApiStorage::delete` for the full regression context.
fn force_delete_query() -> Vec<(String, String)> {
    vec![("gracePeriodSeconds".to_string(), "0".to_string())]
}

/// Fold an `APIResourceList` (`{resources: [{name, namespaced}, ...]}`) into the
/// dynamic map under `root`, skipping subresources (names containing `/`).
fn ingest_resource_list(map: &mut HashMap<String, (String, bool)>, root: &str, list: &Value) {
    let Some(resources) = list.get("resources").and_then(|r| r.as_array()) else {
        return;
    };
    for r in resources {
        let Some(name) = r.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        if name.contains('/') {
            continue; // subresource (e.g. "deployments/status")
        }
        let namespaced = r
            .get("namespaced")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        map.entry(name.to_string())
            .or_insert((root.to_string(), namespaced));
    }
}

/// Resolve a built-in resource-type plural to its REST API root and namespacing.
///
/// Covers the group/versions the api-server router serves natively. Returns
/// `(api_root, namespaced)` where `api_root` is e.g. `/api/v1` (core) or
/// `/apis/apps/v1`; `None` for non-built-in types (resolved via discovery).
fn static_resource_info(rt: &str) -> Option<(&'static str, bool)> {
    let info = match rt {
        // core /api/v1 — namespaced
        "pods"
        | "services"
        | "endpoints"
        | "configmaps"
        | "secrets"
        | "serviceaccounts"
        | "persistentvolumeclaims"
        | "events"
        | "replicationcontrollers"
        | "limitranges"
        | "resourcequotas"
        | "podtemplates" => ("/api/v1", true),
        // core /api/v1 — cluster-scoped
        "nodes" | "namespaces" | "persistentvolumes" | "componentstatuses" => ("/api/v1", false),
        // apps/v1 — namespaced
        "deployments" | "replicasets" | "statefulsets" | "daemonsets" | "controllerrevisions" => {
            ("/apis/apps/v1", true)
        }
        // batch/v1 — namespaced
        "jobs" | "cronjobs" => ("/apis/batch/v1", true),
        // autoscaling/v2 — namespaced
        "horizontalpodautoscalers" => ("/apis/autoscaling/v2", true),
        // networking.k8s.io/v1
        "ingresses" | "networkpolicies" => ("/apis/networking.k8s.io/v1", true),
        "ingressclasses" | "ipaddresses" | "servicecidrs" => ("/apis/networking.k8s.io/v1", false),
        // discovery.k8s.io/v1 — namespaced
        "endpointslices" => ("/apis/discovery.k8s.io/v1", true),
        // policy/v1 — namespaced
        "poddisruptionbudgets" => ("/apis/policy/v1", true),
        // coordination.k8s.io/v1 — namespaced
        "leases" => ("/apis/coordination.k8s.io/v1", true),
        // storage.k8s.io/v1 — cluster-scoped
        "storageclasses"
        | "volumeattachments"
        | "csinodes"
        | "csidrivers"
        | "volumeattributesclasses" => ("/apis/storage.k8s.io/v1", false),
        // scheduling.k8s.io/v1 — cluster-scoped
        "priorityclasses" => ("/apis/scheduling.k8s.io/v1", false),
        // rbac.authorization.k8s.io/v1
        "roles" | "rolebindings" => ("/apis/rbac.authorization.k8s.io/v1", true),
        "clusterroles" | "clusterrolebindings" => ("/apis/rbac.authorization.k8s.io/v1", false),
        // certificates.k8s.io/v1 — cluster-scoped
        "certificatesigningrequests" => ("/apis/certificates.k8s.io/v1", false),
        // apiextensions.k8s.io/v1 — cluster-scoped
        "customresourcedefinitions" => ("/apis/apiextensions.k8s.io/v1", false),
        // apiregistration.k8s.io/v1 — cluster-scoped
        "apiservices" => ("/apis/apiregistration.k8s.io/v1", false),
        // resource.k8s.io/v1 (DRA)
        "resourceclaims" | "resourceclaimtemplates" => ("/apis/resource.k8s.io/v1", true),
        "deviceclasses" | "resourceslices" => ("/apis/resource.k8s.io/v1", false),
        _ => return None,
    };
    Some(info)
}

/// Split a `/registry/{plural}/...` key into `(plural, remaining_segments)`.
fn parse_key(key: &str) -> Result<(String, Vec<String>)> {
    let stripped = key.strip_prefix("/registry/").ok_or_else(|| {
        Error::Internal(format!("ApiStorage: key missing /registry/ prefix: {key}"))
    })?;
    let parts: Vec<String> = stripped
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let rt = parts
        .first()
        .cloned()
        .ok_or_else(|| Error::Internal(format!("ApiStorage: empty key {key}")))?;
    Ok((rt, parts[1..].to_vec()))
}

/// Build the REST path for a single object key (`get`/`update`/`delete`) from
/// already-resolved `(root, namespaced)`.
fn build_object_path(root: &str, namespaced: bool, rt: &str, rest: &[String]) -> Result<String> {
    if namespaced {
        match rest {
            [ns, name] => Ok(format!("{root}/namespaces/{ns}/{rt}/{name}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected namespaced object key /registry/{rt}/<ns>/<name>"
            ))),
        }
    } else {
        match rest {
            [name] => Ok(format!("{root}/{rt}/{name}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected cluster object key /registry/{rt}/<name>"
            ))),
        }
    }
}

/// Build the REST collection path for an object key (drops the name) — `create`.
fn build_collection_for_object(
    root: &str,
    namespaced: bool,
    rt: &str,
    rest: &[String],
) -> Result<String> {
    if namespaced {
        match rest {
            [ns, _name] => Ok(format!("{root}/namespaces/{ns}/{rt}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: expected namespaced object key for create of {rt}"
            ))),
        }
    } else {
        Ok(format!("{root}/{rt}"))
    }
}

/// Build the REST collection path for a list/watch prefix.
fn build_collection_for_prefix(
    root: &str,
    namespaced: bool,
    rt: &str,
    rest: &[String],
) -> Result<String> {
    if namespaced {
        match rest {
            [] => Ok(format!("{root}/{rt}")),
            [ns] => Ok(format!("{root}/namespaces/{ns}/{rt}")),
            _ => Err(Error::Internal(format!(
                "ApiStorage: unexpected list prefix for {rt}"
            ))),
        }
    } else {
        Ok(format!("{root}/{rt}"))
    }
}

impl ApiStorage {
    /// Async REST path for a single object key (resolves the GVR first).
    async fn object_path(&self, key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = self.resolve(&rt).await?;
        build_object_path(&root, namespaced, &rt, &rest)
    }

    /// Async REST collection path for an object key (`create`).
    async fn collection_for_object(&self, key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = self.resolve(&rt).await?;
        build_collection_for_object(&root, namespaced, &rt, &rest)
    }
}

/// Reconstruct the `/registry/...` storage key for a watch-event object.
fn storage_key_for(rt: &str, namespaced: bool, obj: &Value) -> Result<String> {
    let md = obj.get("metadata");
    let name = md
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Internal("ApiStorage: watch object missing metadata.name".into()))?;
    if namespaced {
        let ns = md
            .and_then(|m| m.get("namespace"))
            .and_then(|v| v.as_str())
            .unwrap_or("default");
        Ok(format!("/registry/{rt}/{ns}/{name}"))
    } else {
        Ok(format!("/registry/{rt}/{name}"))
    }
}

/// Map a write error (`post`/`put`) to the storage [`Error`] vocabulary by the
/// api-server's `Error from server (<Reason>): ...` token. Needed so
/// optimistic-concurrency conflicts surface as [`Error::Conflict`].
fn map_write_err(e: anyhow::Error) -> Error {
    // `{:#}` renders the whole anyhow chain (top context + every `source()`),
    // so a transport failure keeps its actionable cause — e.g.
    // `POST https://… failed: error sending request …: dns error: failed to
    // lookup address` — instead of collapsing to just "Failed to send POST
    // request" (M2b worker-registration opaque-error hunt).
    let s = format!("{e:#}");
    if s.contains("(AlreadyExists)") {
        Error::AlreadyExists(s)
    } else if s.contains("(Conflict)") {
        Error::Conflict(s)
    } else if s.contains("(NotFound)") {
        Error::NotFound(s)
    } else if s.contains("(Invalid)") || s.contains("(BadRequest)") {
        Error::BadRequest(s)
    } else if s.contains("(Forbidden)") {
        Error::Forbidden(s)
    } else {
        Error::Storage(s)
    }
}

fn map_get_err(e: GetError) -> Error {
    match e {
        GetError::NotFound => Error::NotFound("resource not found".into()),
        // Preserve the full source chain (see `map_write_err`).
        GetError::Other(e) => Error::Storage(format!("{e:#}")),
    }
}

#[async_trait]
impl Storage for ApiStorage {
    async fn create<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = self.collection_for_object(key).await?;
        let body = serde_json::to_value(value).map_err(Error::Serialization)?;
        let created: Value = self
            .client
            .post(&path, &body)
            .await
            .map_err(map_write_err)?;
        serde_json::from_value(created).map_err(Error::Serialization)
    }

    async fn get<T>(&self, key: &str) -> Result<T>
    where
        T: DeserializeOwned + Send + Sync,
    {
        let path = self.object_path(key).await?;
        let v: Value = self.client.get(&path).await.map_err(map_get_err)?;
        serde_json::from_value(v).map_err(Error::Serialization)
    }

    async fn update<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = self.object_path(key).await?;
        let body = serde_json::to_value(value).map_err(Error::Serialization)?;
        let updated: Value = self.client.put(&path, &body).await.map_err(map_write_err)?;
        serde_json::from_value(updated).map_err(Error::Serialization)
    }

    async fn update_status<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        // Graft only the caller's `.status` onto a freshly-read object (so a
        // stale spec can't be written back), then PUT the /status subresource.
        //
        // Bounded optimistic-concurrency retry: a vanilla api-server enforces
        // resourceVersion/UID preconditions strictly, so if a concurrent writer
        // (e.g. the node-lifecycle controller taints/annotates a just-registered
        // node) bumps the version between our GET and the /status PUT, the PUT
        // 409s. Re-GET for the fresh version and retry, matching the default
        // `Storage::update_status` (lib.rs) and upstream kubelet's
        // `nodeStatusUpdateRetry` loop (pkg/kubelet/kubelet_node_status.go).
        // Without this the kubelet crashes on the first 409 immediately after
        // registering against a vanilla control plane (#1638).
        let incoming = serde_json::to_value(value).map_err(Error::Serialization)?;
        let new_status = incoming.get("status").cloned();

        let path = self.object_path(key).await?;
        let status_path = format!("{path}/status");

        const MAX_ATTEMPTS: usize = 8;
        for attempt in 0..MAX_ATTEMPTS {
            let mut current: Value = self.client.get(&path).await.map_err(map_get_err)?;
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
            let put: anyhow::Result<Value> = self.client.put(&status_path, &current).await;
            match put {
                Ok(updated) => {
                    return serde_json::from_value(updated).map_err(Error::Serialization);
                }
                Err(e) => {
                    let mapped = map_write_err(e);
                    if matches!(mapped, Error::Conflict(_)) && attempt + 1 < MAX_ATTEMPTS {
                        continue;
                    }
                    return Err(mapped);
                }
            }
        }
        Err(Error::Conflict(format!(
            "update_status: exhausted {MAX_ATTEMPTS} CAS retries for {key}"
        )))
    }

    async fn update_raw(&self, key: &str, value: &Value) -> Result<()> {
        let path = self.object_path(key).await?;
        let _: Value = self.client.put(&path, value).await.map_err(map_write_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.object_path(key).await?;
        // `Storage::delete` is a HARD delete in every backend (memory / rhino /
        // etcd remove the key outright). The api-server's pod DELETE, however,
        // defaults to GRACEFUL deletion: it merely stamps deletionTimestamp and
        // keeps the object until a force delete arrives — it only removes the
        // object when gracePeriodSeconds=0 (see api-server handlers/pod.rs).
        // The kubelet runs as an api-server client (StorageBackend::Api) and
        // calls this in `finalize_terminated_pod_storage` to complete deletion
        // of an already-terminated, already-deletionTimestamp'd pod. Without
        // gracePeriodSeconds=0 that final delete is a no-op: the pod lingers,
        // the kubelet worker re-terminates it every reconcile forever, and the
        // object is never removed (conformance "wait for pod to disappear"
        // 300s timeouts; pod-deletion regression from #1150). Force grace=0 so
        // the Api backend honours the same hard-delete contract as the others.
        let status = self
            .client
            .delete_with_options(&path, &force_delete_query(), None)
            .await
            .map_err(|e| Error::Storage(format!("{e:#}")))?;
        if status.is_success() {
            Ok(())
        } else if status.as_u16() == 404 {
            Err(Error::NotFound(format!("{key} not found")))
        } else {
            Err(Error::Storage(format!(
                "delete {key} failed: HTTP {status}"
            )))
        }
    }

    async fn list<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            // Type not served by the api-server — mirror storage mode, where an
            // unknown prefix simply lists empty (no error, no log spam).
            return Ok(Vec::new());
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;
        let list: KubernetesList<Value> = self.client.get(&path).await.map_err(map_get_err)?;
        let mut out = Vec::with_capacity(list.items.len());
        for item in list.items {
            out.push(serde_json::from_value(item).map_err(Error::Serialization)?);
        }
        Ok(out)
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        self.watch_inner(prefix, None).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        self.watch_inner(prefix, Some(revision.to_string())).await
    }

    async fn current_revision(&self) -> Result<i64> {
        // Unused by controllers; the api-server owns revisions.
        Ok(0)
    }

    async fn is_revision_compacted(&self, _revision: i64) -> Result<bool> {
        Ok(false)
    }
}

impl ApiStorage {
    /// Subscribe to a shared upstream `?watch=true` stream for `prefix`'s
    /// collection. The first subscriber for a path spawns one upstream task
    /// (re-keying events into `/registry/...` form and broadcasting them);
    /// later subscribers join its broadcast. So the ~50 controller watch()
    /// calls collapse to one HTTP connection per resource type (#1138).
    ///
    /// `rv` is intentionally ignored: a shared stream cannot honor a
    /// per-subscriber resourceVersion, and controllers relist on (re)connect.
    async fn watch_inner(&self, prefix: &str, _rv: Option<String>) -> Result<WatchStream> {
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            // Type not served — a stream that never yields, mirroring a watch on
            // an empty storage prefix (the controller idles; any periodic
            // relist also returns empty).
            return Ok(futures::stream::pending().boxed());
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;

        // Get-or-create the shared upstream for this collection path.
        let rx = {
            let mut guard = self.shared_watches.lock().await;
            match guard.get(&path) {
                Some(tx) => tx.subscribe(),
                None => {
                    let (tx, rx) = broadcast::channel::<Arc<WatchEvent>>(SHARED_WATCH_BUFFER);
                    guard.insert(path.clone(), tx.clone());
                    tokio::spawn(shared_upstream(
                        self.client.clone(),
                        path,
                        rt,
                        namespaced,
                        tx,
                        self.shared_watches.clone(),
                    ));
                    rx
                }
            }
        };

        // Adapt the broadcast receiver to a storage `WatchStream`. A lagged
        // subscriber (fell behind the buffer) yields an error so its controller
        // relists+reconnects; a closed channel (upstream ended) ends the stream
        // so the controller reconnects, recreating the upstream.
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(ev) => Some((Ok((*ev).clone()), rx)),
                Err(broadcast::error::RecvError::Lagged(n)) => Some((
                    Err(Error::Network(format!(
                        "shared watch lagged {n} events; relist"
                    ))),
                    rx,
                )),
                Err(broadcast::error::RecvError::Closed) => None,
            }
        });

        Ok(stream.boxed())
    }
}

/// The single upstream task behind a shared watch: read the api-server's
/// `?watch=true` stream for `path`, re-key each event into `/registry/...`
/// form, and broadcast it to all subscribers. Exits (and deregisters itself)
/// when the upstream closes/errors or when no subscribers remain — at which
/// point any surviving/late subscriber sees the channel close and reconnects,
/// recreating a fresh upstream.
async fn shared_upstream(
    client: Arc<ApiClient>,
    path: String,
    rt: String,
    namespaced: bool,
    tx: broadcast::Sender<Arc<WatchEvent>>,
    registry: SharedWatches,
) {
    let raw = match watch_stream::<Value>(&client, &path, None).await {
        Ok(s) => s,
        Err(_) => {
            // Connect failed; drop the registration so the next watch() retries.
            remove_shared(&registry, &path, &tx).await;
            return;
        }
    };
    futures::pin_mut!(raw);
    while let Some(ev) = raw.next().await {
        let mapped: Option<WatchEvent> = match ev {
            Ok(ClientWatchEvent::Added(o)) => storage_key_for(&rt, namespaced, &o)
                .ok()
                .map(|k| WatchEvent::Added(k, o.to_string())),
            Ok(ClientWatchEvent::Modified(o)) => storage_key_for(&rt, namespaced, &o)
                .ok()
                .map(|k| WatchEvent::Modified(k, o.to_string())),
            Ok(ClientWatchEvent::Deleted(o)) => storage_key_for(&rt, namespaced, &o)
                .ok()
                .map(|k| WatchEvent::Deleted(k, o.to_string())),
            // Bookmarks are watch-progress markers, not resource deltas.
            Ok(ClientWatchEvent::Bookmark(_)) => None,
            // Upstream error → end; subscribers see close and reconnect.
            Err(_) => break,
        };
        if let Some(ev) = mapped {
            // `send` errors only when there are zero receivers — every
            // controller has dropped this watch, so stop holding the connection.
            if tx.send(Arc::new(ev)).is_err() {
                break;
            }
        }
    }
    remove_shared(&registry, &path, &tx).await;
}

/// Deregister a shared upstream, but only if the registry still points at *this*
/// sender — a concurrent `watch()` may have already replaced it with a fresh one.
async fn remove_shared(
    registry: &SharedWatches,
    path: &str,
    tx: &broadcast::Sender<Arc<WatchEvent>>,
) {
    let mut guard = registry.lock().await;
    if let Some(current) = guard.get(path) {
        if current.same_channel(tx) {
            guard.remove(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Resolve a built-in object key through the static table + pure builder,
    /// mirroring the old free-fn `object_path` for test purposes.
    fn static_object_path(key: &str) -> Result<String> {
        let (rt, rest) = parse_key(key)?;
        let (root, namespaced) = static_resource_info(&rt).ok_or_else(|| unmapped(&rt))?;
        build_object_path(root, namespaced, &rt, &rest)
    }

    fn static_collection_prefix(prefix: &str) -> Result<String> {
        let (rt, rest) = parse_key(prefix)?;
        let (root, namespaced) = static_resource_info(&rt).ok_or_else(|| unmapped(&rt))?;
        build_collection_for_prefix(root, namespaced, &rt, &rest)
    }

    /// Regression: the Api backend's `delete` must force an immediate (hard)
    /// delete. Without `gracePeriodSeconds=0` the api-server only soft-deletes
    /// a pod (stamps deletionTimestamp), so the kubelet's finalize never
    /// removes an already-terminated pod and loops forever (#1150 pod-deletion
    /// regression — conformance "wait for pod to disappear" 300s timeouts).
    #[test]
    fn delete_forces_zero_grace_period() {
        let q = force_delete_query();
        assert!(
            q.iter().any(|(k, v)| k == "gracePeriodSeconds" && v == "0"),
            "ApiStorage::delete must send gracePeriodSeconds=0 for a hard delete, got {q:?}"
        );
    }

    #[test]
    fn object_path_namespaced_and_cluster() {
        assert_eq!(
            static_object_path("/registry/pods/default/nginx").unwrap(),
            "/api/v1/namespaces/default/pods/nginx"
        );
        assert_eq!(
            static_object_path("/registry/deployments/kube-system/coredns").unwrap(),
            "/apis/apps/v1/namespaces/kube-system/deployments/coredns"
        );
        assert_eq!(
            static_object_path("/registry/nodes/node-1").unwrap(),
            "/api/v1/nodes/node-1"
        );
        assert_eq!(
            static_object_path("/registry/priorityclasses/high").unwrap(),
            "/apis/scheduling.k8s.io/v1/priorityclasses/high"
        );
    }

    #[test]
    fn collection_paths() {
        assert_eq!(
            static_collection_prefix("/registry/replicasets/").unwrap(),
            "/apis/apps/v1/replicasets"
        );
        assert_eq!(
            static_collection_prefix("/registry/pods/default/").unwrap(),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            static_collection_prefix("/registry/nodes/").unwrap(),
            "/api/v1/nodes"
        );
        let (rt, rest) = parse_key("/registry/pods/default/nginx").unwrap();
        let (root, ns) = static_resource_info(&rt).unwrap();
        assert_eq!(
            build_collection_for_object(root, ns, &rt, &rest).unwrap(),
            "/api/v1/namespaces/default/pods"
        );
    }

    #[test]
    fn non_builtin_type_is_not_static() {
        // CRD types fall through the static table (resolved via discovery at
        // runtime); they are not hard-coded.
        assert!(static_resource_info("verticalpodautoscalers").is_none());
        assert!(static_object_path("/registry/verticalpodautoscalers/ns/v").is_err());
    }

    #[test]
    fn discovery_ingest_maps_plurals_and_skips_subresources() {
        let mut map = HashMap::new();
        let list = serde_json::json!({
            "resources": [
                {"name": "volumesnapshots", "namespaced": true},
                {"name": "volumesnapshots/status", "namespaced": true},
                {"name": "volumesnapshotcontents", "namespaced": false},
            ]
        });
        ingest_resource_list(&mut map, "/apis/snapshot.storage.k8s.io/v1", &list);
        assert_eq!(
            map.get("volumesnapshots"),
            Some(&("/apis/snapshot.storage.k8s.io/v1".to_string(), true))
        );
        assert_eq!(
            map.get("volumesnapshotcontents"),
            Some(&("/apis/snapshot.storage.k8s.io/v1".to_string(), false))
        );
        // Subresource entry must not become its own mapping.
        assert!(!map.contains_key("volumesnapshots/status"));
    }

    #[test]
    fn storage_key_roundtrip() {
        let obj = serde_json::json!({"metadata": {"name": "x", "namespace": "ns"}});
        assert_eq!(
            storage_key_for("pods", true, &obj).unwrap(),
            "/registry/pods/ns/x"
        );
        let cobj = serde_json::json!({"metadata": {"name": "node-1"}});
        assert_eq!(
            storage_key_for("nodes", false, &cobj).unwrap(),
            "/registry/nodes/node-1"
        );
    }

    // A transport failure (e.g. the kubelet POSTing a Node to an unresolvable
    // api-server host) arrives as an anyhow error whose *source* carries the
    // actionable detail (DNS / connect / TLS). `map_write_err` must preserve
    // that full chain, not just the top-level context — otherwise the kubelet
    // log reads only "Storage error: Failed to send POST request" and the real
    // cause (e.g. `dns error: failed to lookup address`) is lost. Regression
    // for the Mikronetes M2b worker-registration opaque-error hunt.
    #[test]
    fn map_write_err_preserves_source_chain() {
        let chained = anyhow::anyhow!("dns error: failed to lookup address information")
            .context("POST https://api-server:6443/api/v1/nodes failed");
        match map_write_err(chained) {
            Error::Storage(s) => {
                assert!(
                    s.contains("POST https://api-server:6443/api/v1/nodes failed"),
                    "top-level context must remain: {s}"
                );
                assert!(
                    s.contains("dns error: failed to lookup address information"),
                    "underlying source cause must survive into the Storage error: {s}"
                );
            }
            other => panic!("expected Error::Storage, got {other:?}"),
        }
    }

    // Same guarantee for the read path (`map_get_err`): a `GetError::Other`
    // carrying a chained anyhow must surface the full chain.
    #[test]
    fn map_get_err_preserves_source_chain() {
        let chained = anyhow::anyhow!("connection refused")
            .context("GET https://api-server:6443/api/v1/nodes/node-2 failed");
        match map_get_err(GetError::Other(chained)) {
            Error::Storage(s) => {
                assert!(
                    s.contains("GET https://api-server:6443"),
                    "context lost: {s}"
                );
                assert!(s.contains("connection refused"), "source lost: {s}");
            }
            other => panic!("expected Error::Storage, got {other:?}"),
        }
    }

    // ---- update_status optimistic-concurrency retry (#1638) ----------------
    //
    // Regression for #1638: running the rusternetes kubelet (API mode) against
    // a *vanilla* api-server, the node status write 409s when a concurrent
    // writer (node-lifecycle controller) bumps the node's resourceVersion
    // between our GET and the /status PUT. `ApiStorage::update_status` must
    // re-GET the fresh object and retry — mirroring the default
    // `Storage::update_status` and upstream kubelet's `nodeStatusUpdateRetry`
    // loop — instead of surfacing the first 409 as fatal (which crash-loops the
    // kubelet right after "Node registered successfully").

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// A minimal HTTP/1.1 server that fails the first `conflicts_before_ok`
    /// PUTs to `/status` with a 409 Conflict, then succeeds. Each response sets
    /// `Connection: close` so reqwest opens a fresh connection per request
    /// (one request per accepted socket — no keep-alive parsing needed).
    /// Returns the base URL and a shared counter of PUT-to-status attempts.
    async fn spawn_conflict_server(conflicts_before_ok: usize) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let put_attempts = Arc::new(AtomicUsize::new(0));
        let counter = put_attempts.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let req = read_request(&mut stream).await;
                let req_line = req.lines().next().unwrap_or("").to_string();
                let is_put_status = req_line.starts_with("PUT") && req_line.contains("/status");

                let node = r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"rusternetes-node","resourceVersion":"1"},"status":{}}"#;

                if is_put_status {
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    if n < conflicts_before_ok {
                        // Kubernetes Status object with reason=Conflict → maps
                        // to Error::Conflict via format_status_error/map_write_err.
                        let body = r#"{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","code":409,"message":"the object has been modified; please apply your changes to the latest version"}"#;
                        respond(&mut stream, "409 Conflict", body).await;
                    } else {
                        respond(&mut stream, "200 OK", node).await;
                    }
                } else {
                    // GET (object read) — hand back a fresh node each time.
                    respond(&mut stream, "200 OK", node).await;
                }
            }
        });

        (format!("http://{addr}"), put_attempts)
    }

    /// Read one HTTP request (headers + any Content-Length body) off the socket.
    async fn read_request(stream: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            let n = stream.read(&mut tmp).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(hdr_end) = find_subslice(&buf, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&buf[..hdr_end]).to_ascii_lowercase();
                let content_len = headers
                    .split("\r\n")
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if buf.len() >= hdr_end + 4 + content_len {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    async fn respond(stream: &mut TcpStream, status_line: &str, body: &str) {
        let resp = format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(resp.as_bytes()).await;
        let _ = stream.flush().await;
        let _ = stream.shutdown().await;
    }

    fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    #[tokio::test]
    async fn update_status_retries_on_conflict() {
        // Three 409s then success: within the retry budget → must succeed.
        let (base, attempts) = spawn_conflict_server(3).await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let node = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "rusternetes-node"},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]}
        });

        let result: Result<Value> = storage
            .update_status("/registry/nodes/rusternetes-node", &node)
            .await;

        assert!(
            result.is_ok(),
            "update_status must retry through 409 Conflicts, got {result:?}"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            4,
            "expected 3 conflicting PUTs + 1 success"
        );
    }

    #[tokio::test]
    async fn update_status_gives_up_after_max_retries() {
        // A permanently-conflicting api-server must eventually surface Conflict
        // (bounded retry), not spin forever.
        let (base, _attempts) = spawn_conflict_server(usize::MAX).await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let node = serde_json::json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "rusternetes-node"}, "status": {}
        });

        let result: Result<Value> = storage
            .update_status("/registry/nodes/rusternetes-node", &node)
            .await;

        assert!(
            matches!(result, Err(Error::Conflict(_))),
            "persistent conflict must surface as Error::Conflict, got {result:?}"
        );
    }

    /// A minimal server that captures the request and replies to any POST with a
    /// TokenRequest carrying `status.token`. Returns (base_url, shared capture).
    async fn spawn_tokenrequest_server(token: &'static str) -> (String, Arc<Mutex<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(String::new()));
        let cap = captured.clone();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let req = read_request(&mut stream).await;
                *cap.lock().await = req;
                let body = format!(
                    r#"{{"apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest","status":{{"token":"{token}","expirationTimestamp":"2026-01-01T00:00:00Z"}}}}"#
                );
                respond(&mut stream, "201 Created", &body).await;
            }
        });
        (format!("http://{addr}"), captured)
    }

    // Regression for #1648 chain: the kubelet must obtain projected SA tokens
    // from the api-server's TokenRequest subresource (a vanilla api-server only
    // trusts tokens it signed; self-minted ones 401 — breaking kindnet/CNI).
    // Assert create_sa_token POSTs serviceaccounts/<name>/token and returns the
    // api-server-issued token.
    #[tokio::test]
    async fn create_sa_token_posts_tokenrequest_and_extracts_token() {
        let (base, captured) = spawn_tokenrequest_server("ISSUED-BY-APISERVER").await;
        let client = Arc::new(ApiClient::new(&base, true, None).unwrap());
        let storage = ApiStorage::new(client);

        let tok = storage
            .create_sa_token("kube-system", "kindnet", &[], 3600)
            .await
            .expect("token issued");
        assert_eq!(tok, "ISSUED-BY-APISERVER");

        let req = captured.lock().await.clone();
        let req_line = req.lines().next().unwrap_or("");
        assert!(
            req_line.starts_with("POST")
                && req_line
                    .contains("/api/v1/namespaces/kube-system/serviceaccounts/kindnet/token"),
            "must POST the TokenRequest subresource, got: {req_line}"
        );
        assert!(
            req.contains("\"expirationSeconds\":3600"),
            "request body must carry expirationSeconds, got: {req}"
        );
    }
}
