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
//! - **`current_revision`/`is_revision_compacted`** are answered by the
//!   api-server (a one-item LIST's `metadata.resourceVersion`, and a
//!   `resourceVersionMatch=Exact` LIST's `Expired` status), the way a
//!   client-go reflector learns both. `watch_from_revision` opens its own
//!   upstream at the revision (see `watch_inner`); the shared stream tracks
//!   bookmark revisions and resumes from them.

use crate::{Storage, WatchEvent, WatchStream};
use async_trait::async_trait;
use futures::StreamExt;
use rusternetes_client::api_status::{
    is_already_exists, is_bad_request, is_conflict, is_forbidden, is_gone, is_invalid,
    is_not_found, is_resource_expired, is_unsupported_media_type,
};
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

/// Discovery cache: `plural -> (api_root, namespaced)`, shared between
/// per-controller views (see [`ApiStorage::with_client`]).
type DynamicTypes = Arc<RwLock<Option<HashMap<String, (String, bool)>>>>;

/// A [`Storage`] implementation that proxies to the api-server over REST.
pub struct ApiStorage {
    client: Arc<ApiClient>,
    /// Discovery-resolved `plural -> (api_root, namespaced)` for types not in
    /// the built-in [`static_resource_info`] table (CRDs, aggregated APIs, and
    /// the arbitrary types the garbage collector traverses). `None` until the
    /// first miss triggers a one-shot discovery load; `Some` thereafter.
    dynamic: DynamicTypes,
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
            dynamic: Arc::new(RwLock::new(None)),
            shared_watches: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// A view of this storage that sends its CRUD requests through `client`,
    /// sharing the discovery cache and the shared watch fan-out (#1138).
    ///
    /// The controller-manager gives each controller its own `ApiClient` (hence
    /// its own rate limiter, #1863) but must not give up the one-connection-
    /// per-resource-type watch sharing, so only the client differs.
    pub fn with_client(&self, client: Arc<ApiClient>) -> Self {
        Self {
            client,
            dynamic: Arc::clone(&self.dynamic),
            shared_watches: Arc::clone(&self.shared_watches),
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
    ///
    /// `bound_pod` is the `(name, uid)` of the pod that mounts the token. When
    /// set it becomes `spec.boundObjectRef`, matching upstream's projected
    /// volume plugin (`pkg/volume/projected/projected.go`:
    /// `BoundObjectRef{APIVersion: "v1", Kind: "Pod", Name, UID}`). The
    /// api-server derives the token's pod/node claims from that ref, which is
    /// what surfaces as the `authentication.kubernetes.io/pod-name`,
    /// `pod-uid` and `node-name` extras on a TokenReview (#1684). Callers with
    /// no owning pod pass `None`, which omits the ref.
    pub async fn create_sa_token(
        &self,
        namespace: &str,
        name: &str,
        audiences: &[String],
        expiration_seconds: i64,
        bound_pod: Option<(&str, &str)>,
    ) -> Result<String> {
        self.create_sa_token_status(namespace, name, audiences, expiration_seconds, bound_pod)
            .await
            .map(|(token, _)| token)
    }

    /// Same POST as [`Self::create_sa_token`], but returns the whole
    /// `TokenRequestStatus` as `(token, expirationTimestamp)`. The CSI mounter
    /// needs the expiry as well (`podServiceAccountTokenAttrs`,
    /// `pkg/volume/csi/csi_mounter.go:358-416` serialises the status).
    pub async fn create_sa_token_status(
        &self,
        namespace: &str,
        name: &str,
        audiences: &[String],
        expiration_seconds: i64,
        bound_pod: Option<(&str, &str)>,
    ) -> Result<(String, String)> {
        let path = format!("/api/v1/namespaces/{namespace}/serviceaccounts/{name}/token");
        let mut spec = serde_json::json!({
            "audiences": audiences,
            "expirationSeconds": expiration_seconds,
        });
        if let (Some((pod_name, pod_uid)), Some(spec_obj)) = (bound_pod, spec.as_object_mut()) {
            spec_obj.insert(
                "boundObjectRef".to_string(),
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": pod_name,
                    "uid": pod_uid,
                }),
            );
        }
        let body = serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "spec": spec,
        });
        let resp: Value = self
            .client
            .post(&path, &body)
            .await
            .map_err(map_write_err)?;
        let expiration = resp
            .pointer("/status/expirationTimestamp")
            .and_then(|t| t.as_str())
            .unwrap_or_default()
            .to_string();
        resp.pointer("/status/token")
            .and_then(|t| t.as_str())
            .map(|s| (s.to_string(), expiration))
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
pub fn unmapped(rt: &str) -> Error {
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
///
/// `propagationPolicy=Background` keeps the hard-delete contract for owners
/// too: every other backend removes the key and leaves the dependents to the
/// garbage collector. Without it, an owner whose strategy defaults to
/// `OrphanDependents` (a batch/v1 Job, pkg/registry/batch/job/strategy.go:62-74)
/// would orphan its pods. Upstream's controllers pass the policy explicitly
/// for the same reason (pkg/controller/cronjob/injection.go:108-109).
pub fn force_delete_query() -> Vec<(String, String)> {
    vec![
        ("gracePeriodSeconds".to_string(), "0".to_string()),
        ("propagationPolicy".to_string(), "Background".to_string()),
    ]
}

/// Query params for a collection delete: only the propagation policy, as in
/// upstream's namespace deleter
/// (`namespaced_resources_deleter.go:322-323`).
fn collection_delete_query() -> Vec<(String, String)> {
    vec![("propagationPolicy".to_string(), "Background".to_string())]
}

/// Query params for a GRACEFUL delete: none. The api-server then applies its
/// own defaults — for a pod, `spec.terminationGracePeriodSeconds` — exactly as
/// upstream's controllers do when they call `Pods(ns).Delete(ctx, name,
/// metav1.DeleteOptions{})` (pkg/controller/statefulset/stateful_pod_control.go:97).
pub fn graceful_delete_query() -> Vec<(String, String)> {
    Vec::new()
}

/// Fold an `APIResourceList` (`{resources: [{name, namespaced}, ...]}`) into the
/// dynamic map under `root`, skipping subresources (names containing `/`).
pub fn ingest_resource_list(map: &mut HashMap<String, (String, bool)>, root: &str, list: &Value) {
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
pub fn static_resource_info(rt: &str) -> Option<(&'static str, bool)> {
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
        // certificates.k8s.io/v1beta1 — cluster-scoped
        "clustertrustbundles" => ("/apis/certificates.k8s.io/v1beta1", false),
        // storagemigration.k8s.io/v1beta1 — cluster-scoped
        "storageversionmigrations" => ("/apis/storagemigration.k8s.io/v1beta1", false),
        // apiextensions.k8s.io/v1 — cluster-scoped
        "customresourcedefinitions" => ("/apis/apiextensions.k8s.io/v1", false),
        // apiregistration.k8s.io/v1 — cluster-scoped
        "apiservices" => ("/apis/apiregistration.k8s.io/v1", false),
        // resource.k8s.io/v1 (DRA)
        "resourceclaims" | "resourceclaimtemplates" => ("/apis/resource.k8s.io/v1", true),
        "deviceclasses" | "resourceslices" => ("/apis/resource.k8s.io/v1", false),
        // resource.k8s.io/v1alpha3 — cluster-scoped
        "devicetaintrules" => ("/apis/resource.k8s.io/v1alpha3", false),
        // internal.apiserver.k8s.io/v1alpha1 — cluster-scoped
        "storageversions" => ("/apis/internal.apiserver.k8s.io/v1alpha1", false),
        _ => return None,
    };
    Some(info)
}

/// Split a `/registry/{plural}/...` key into `(plural, remaining_segments)`.
pub fn parse_key(key: &str) -> Result<(String, Vec<String>)> {
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
pub fn build_object_path(
    root: &str,
    namespaced: bool,
    rt: &str,
    rest: &[String],
) -> Result<String> {
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
pub fn build_collection_for_object(
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
pub fn build_collection_for_prefix(
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
pub fn storage_key_for(rt: &str, namespaced: bool, obj: &Value) -> Result<String> {
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
pub fn map_write_err(e: anyhow::Error) -> Error {
    // `{:#}` renders the whole anyhow chain (top context + every `source()`),
    // so a transport failure keeps its actionable cause — e.g.
    // `POST https://… failed: error sending request …: dns error: failed to
    // lookup address` — instead of collapsing to just "Failed to send POST
    // request" (M2b worker-registration opaque-error hunt).
    let s = format!("{e:#}");
    // Typed mapping via the Status code + reason `ApiClient` now carries
    // (client-go `StatusError`); each predicate is the apimachinery
    // `errors.Is*` incl. its `knownReasons` code fallback (errors.go:529-684).
    if is_already_exists(&e) {
        Error::AlreadyExists(s)
    } else if is_conflict(&e) {
        Error::Conflict(s)
    } else if is_not_found(&e) {
        Error::NotFound(s)
    } else if is_invalid(&e) || is_bad_request(&e) {
        Error::BadRequest(s)
    } else if is_forbidden(&e) {
        Error::Forbidden(s)
    } else if is_unsupported_media_type(&e) {
        // `apierrors.IsUnsupportedMediaType` (apimachinery/pkg/api/errors/errors.go:607-615):
        // the client half of the apply -> Update fallback (#3351).
        Error::UnsupportedMediaType(s)
    } else {
        Error::Storage(s)
    }
}

pub fn map_get_err(e: GetError) -> Error {
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

    /// Over the API the server owns expiry: the Event registry applies the
    /// event TTL on every write (registry/core/event, `ttlFunc` in
    /// pkg/registry/core/event/storage/storage.go:42-44), so the client's
    /// `ttl` is not sent. Without this override the Storage default refuses
    /// ttl != 0 and EventRecorder (which always writes with a TTL) would lose
    /// every Event in API mode.
    async fn create_with_ttl<T>(&self, key: &str, value: &T, _ttl: u64) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.create(key, value).await
    }

    async fn update_with_ttl<T>(&self, key: &str, value: &T, _ttl: u64) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.update(key, value).await
    }

    async fn update_subresource<T>(&self, key: &str, subresource: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = format!("{}/{}", self.object_path(key).await?, subresource);
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

    /// PUT `/status` with the caller's object, resourceVersion included: the
    /// api-server's registry `Update` applies it as a precondition and answers
    /// 409 on mismatch, which `map_write_err` maps to `Error::Conflict`. No
    /// re-GET and no retry — that is what distinguishes it from
    /// [`Storage::update_status`] above.
    async fn update_status_cas<T>(&self, key: &str, value: &T) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        self.update_subresource(key, "status", value).await
    }

    async fn patch_strategic_merge<T>(&self, key: &str, patch: &Value) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = self.object_path(key).await?;
        let patched: Value = self
            .client
            .patch(&path, patch, "application/strategic-merge-patch+json")
            .await
            .map_err(map_write_err)?;
        serde_json::from_value(patched).map_err(Error::Serialization)
    }

    /// Preconditions travel as `metadata.uid` / `metadata.resourceVersion` in
    /// the patch body, exactly how upstream clients carry them
    /// (`applyv1.Secret(..).WithUID(uid)`,
    /// `legacy_serviceaccount_token_cleaner.go:190-198`); the api-server's
    /// registry `Update` turns them into `storage.Preconditions`
    /// (`registry/generic/registry/store.go:626-632`) and a mismatch is 409.
    async fn patch_strategic_merge_with_preconditions<T>(
        &self,
        key: &str,
        patch: &Value,
        preconditions: &crate::Preconditions,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let mut body = patch.clone();
        if let Some(root) = body.as_object_mut() {
            let meta = root
                .entry("metadata")
                .or_insert_with(|| Value::Object(Default::default()));
            if let Some(meta) = meta.as_object_mut() {
                if let Some(uid) = &preconditions.uid {
                    meta.insert("uid".into(), uid.clone().into());
                }
                if let Some(rv) = &preconditions.resource_version {
                    meta.insert("resourceVersion".into(), rv.clone().into());
                }
            }
        }
        self.patch_strategic_merge(key, &body).await
    }

    async fn apply<T>(
        &self,
        key: &str,
        apply_config: &Value,
        field_manager: &str,
        force: bool,
        subresource: Option<&str>,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let sub = subresource.map(|s| format!("/{s}")).unwrap_or_default();
        let path = format!(
            "{}{}?fieldManager={}&force={}",
            self.object_path(key).await?,
            sub,
            field_manager,
            force
        );
        let applied: Value = self
            .client
            .patch(&path, apply_config, "application/apply-patch+json")
            .await
            .map_err(map_write_err)?;
        serde_json::from_value(applied).map_err(Error::Serialization)
    }

    async fn patch_status_strategic_merge<T>(
        &self,
        key: &str,
        patch: &Value,
        _full: &T,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let path = format!("{}/status", self.object_path(key).await?);
        let patched: Value = self
            .client
            .patch(&path, patch, "application/strategic-merge-patch+json")
            .await
            .map_err(map_write_err)?;
        serde_json::from_value(patched).map_err(Error::Serialization)
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

    /// `metav1.DeleteOptions{Preconditions}` on the DELETE body, as upstream's
    /// clients send it (e.g. `pkg/controller/certificates/cleaner/pcrcleaner.go:93-99`);
    /// the api-server's registry `Store.Delete` enforces it atomically
    /// (`registry/generic/registry/store.go:1146-1149` -> `storage.Preconditions`)
    /// and answers 409 on a miss, which is `Error::Conflict` here. Same
    /// hard-delete query as `delete`.
    async fn delete_with_preconditions(
        &self,
        key: &str,
        preconditions: &crate::Preconditions,
    ) -> Result<()> {
        let path = self.object_path(key).await?;
        let body = preconditions.to_delete_options();
        let status = self
            .client
            .delete_with_options(&path, &force_delete_query(), Some(&body))
            .await
            .map_err(|e| Error::Storage(format!("{e:#}")))?;
        match status.as_u16() {
            s if (200..300).contains(&s) => Ok(()),
            404 => Err(Error::NotFound(format!("{key} not found"))),
            409 => Err(Error::Conflict(format!(
                "delete {key} failed its preconditions: HTTP 409"
            ))),
            _ => Err(Error::Storage(format!(
                "delete {key} failed: HTTP {status}"
            ))),
        }
    }

    /// Graceful delete: a plain DELETE with no `gracePeriodSeconds` override,
    /// so the api-server terminates the object on its own terms. Controllers
    /// that used to stamp `deletionTimestamp` themselves must come through
    /// here — that field is immutable on update and an upstream api-server
    /// rejects the write outright ("field is immutable"), which left
    /// StatefulSet scale-downs hanging forever against a vanilla control plane.
    async fn delete_gracefully(&self, key: &str) -> Result<()> {
        let path = self.object_path(key).await?;
        let status = self
            .client
            .delete_with_options(&path, &graceful_delete_query(), None)
            .await
            .map_err(|e| Error::Storage(format!("{e:#}")))?;
        if status.is_success() {
            Ok(())
        } else if status.as_u16() == 404 {
            // Already gone — the caller wanted it deleted, so this is success.
            Ok(())
        } else {
            Err(Error::Storage(format!(
                "graceful delete {key} failed: HTTP {status}"
            )))
        }
    }

    /// One `DELETE` on the collection URL with `propagationPolicy=Background`
    /// and nothing else — upstream's `deleteCollection`
    /// (`pkg/controller/namespace/deletion/namespaced_resources_deleter.go:322-326`:
    /// `DeleteOptions{PropagationPolicy: &background}`). 404 and 405 mean the
    /// type serves no `deletecollection` verb (`:340-348` maps NotFound and
    /// MethodNotSupported to "not supported"), so they return `Ok(false)` and
    /// the caller falls back to per-item deletes.
    async fn delete_collection(&self, prefix: &str) -> Result<bool> {
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            return Ok(false);
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;
        let status = self
            .client
            .delete_with_options(&path, &collection_delete_query(), None)
            .await
            .map_err(|e| Error::Storage(format!("{e:#}")))?;
        if status.is_success() {
            Ok(true)
        } else if matches!(status.as_u16(), 404 | 405) {
            Ok(false)
        } else {
            Err(Error::Storage(format!(
                "delete collection {prefix} failed: HTTP {status}"
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

    /// LIST at exactly `revision`: `resourceVersion=N&resourceVersionMatch=Exact`
    /// (`ResourceVersionMatchExact`, `apimachinery/pkg/apis/meta/v1/types.go`;
    /// the server pins the read, `apiserver/pkg/storage/etcd3/store.go`
    /// `withRev`). The trait default ignored the revision.
    async fn list_at_revision<T>(&self, prefix: &str, revision: i64) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        let query = format!("resourceVersion={revision}&resourceVersionMatch=Exact");
        Ok(self.list_page(prefix, &query).await?.0)
    }

    /// One server-side page: `limit` and `continue` go to the api-server and
    /// the server's own opaque continue token comes back, as a client-go
    /// `ListPager` does (`client-go/tools/pager/pager.go:88-140`) — rather than
    /// listing everything and slicing in memory. A 410 `Expired` surfaces as
    /// [`Error::Gone`] (`errors.IsResourceExpired`, `pager.go:107`), leaving the
    /// caller to decide on the full-list fallback (`FullListIfExpired`).
    async fn list_paginated<T>(
        &self,
        prefix: &str,
        limit: usize,
        continue_token: Option<&str>,
    ) -> Result<(Vec<T>, Option<String>)>
    where
        T: Serialize + DeserializeOwned + Send + Sync,
    {
        if limit == 0 && continue_token.is_none() {
            return Ok((self.list(prefix).await?, None));
        }
        let mut query = Vec::new();
        if limit > 0 {
            query.push(format!("limit={limit}"));
        }
        if let Some(t) = continue_token {
            query.push(format!("continue={}", percent_encode(t)));
        }
        self.list_page(prefix, &query.join("&")).await
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        self.watch_inner(prefix, None).await
    }

    async fn watch_from_revision(&self, prefix: &str, revision: i64) -> Result<WatchStream> {
        self.watch_inner(prefix, Some(revision.to_string())).await
    }

    /// The api-server's current revision, read the way a client-go reflector
    /// reads it: `resourceVersion = listMetaInterface.GetResourceVersion()` of
    /// a LIST response (`client-go/tools/cache/reflector.go:677`, release-1.35).
    /// `limit=1` keeps the page to one item; the list's `resourceVersion` is
    /// the store revision regardless of page size. The api-server owns
    /// revisions, so this asks it rather than answering a constant.
    async fn current_revision(&self) -> Result<i64> {
        let path = self.namespaces_collection().await?;
        let list: KubernetesList<Value> = self
            .client
            .get(&format!("{path}?limit=1"))
            .await
            .map_err(map_get_err)?;
        let rv = list
            .metadata
            .and_then(|m| m.resource_version)
            .ok_or_else(|| Error::Storage("list response carries no resourceVersion".into()))?;
        rv.parse::<i64>()
            .map_err(|e| Error::Storage(format!("list resourceVersion {rv:?} is not numeric: {e}")))
    }

    /// Whether the api-server can no longer serve `revision`. A client learns
    /// this from an `Expired` / `Gone` status, never by a flag, so ask for the
    /// revision exactly and classify the error with the reflector's
    /// `isExpiredError` (`reflector.go:1051-1058`: `IsResourceExpired(err) ||
    /// IsGone(err)`). Any other failure is not evidence of compaction and is
    /// surfaced rather than folded into `false`.
    async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
        let path = self.namespaces_collection().await?;
        let url = format!("{path}?limit=1&resourceVersion={revision}&resourceVersionMatch=Exact");
        match self.client.get::<KubernetesList<Value>>(&url).await {
            Ok(_) => Ok(false),
            Err(GetError::Other(e)) if is_expired_error(&e) => Ok(true),
            Err(e) => Err(map_get_err(e)),
        }
    }
}

/// `isExpiredError` (client-go `reflector.go:1051-1058`): the server said the
/// revision is gone: `IsResourceExpired(err) || IsGone(err)` on the typed
/// `ApiStatusError`.
pub fn is_expired_error(e: &anyhow::Error) -> bool {
    is_resource_expired(e) || is_gone(e)
}

/// Percent-encode a query value (RFC 3986 unreserved bytes pass through).
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl ApiStorage {
    /// GET one list page with `query` appended; returns the items and the
    /// server's `metadata.continue` (None when empty/absent).
    async fn list_page<T>(&self, prefix: &str, query: &str) -> Result<(Vec<T>, Option<String>)>
    where
        T: DeserializeOwned,
    {
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            return Ok((Vec::new(), None));
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;
        let sep = if path.contains('?') { '&' } else { '?' };
        let list: KubernetesList<Value> = self
            .client
            .get(&format!("{path}{sep}{query}"))
            .await
            .map_err(|e| match e {
                GetError::Other(ref err) if is_expired_error(err) => {
                    Error::Gone(format!("{err:#}"))
                }
                e => map_get_err(e),
            })?;
        let next = list
            .metadata
            .and_then(|m| m.continue_token)
            .filter(|t| !t.is_empty());
        let mut out = Vec::with_capacity(list.items.len());
        for item in list.items {
            out.push(serde_json::from_value(item).map_err(Error::Serialization)?);
        }
        Ok((out, next))
    }

    /// Collection path of `namespaces`: cluster-scoped and always served, so a
    /// revision probe has exactly one path and no namespace to choose.
    async fn namespaces_collection(&self) -> Result<String> {
        let (root, namespaced) = self.resolve("namespaces").await?;
        build_collection_for_prefix(&root, namespaced, "namespaces", &[])
    }

    /// Subscribe to a shared upstream `?watch=true` stream for `prefix`'s
    /// collection. The first subscriber for a path spawns one upstream task
    /// (re-keying events into `/registry/...` form and broadcasting them);
    /// later subscribers join its broadcast. So the ~50 controller watch()
    /// calls collapse to one HTTP connection per resource type (#1138).
    ///
    /// A shared stream cannot honor a per-subscriber resourceVersion, so a
    /// caller that names one (`watch_from_revision`) gets its own upstream
    /// opened at that revision ([`dedicated_watch`]) — the way a reflector's
    /// `Watch` carries `ResourceVersion: r.LastSyncResourceVersion()`
    /// (`client-go/tools/cache/reflector.go:526-527`). `watch()` (no revision)
    /// joins the shared stream.
    async fn watch_inner(&self, prefix: &str, rv: Option<String>) -> Result<WatchStream> {
        let (rt, rest) = parse_key(prefix)?;
        let Some((root, namespaced)) = self.try_resolve(&rt).await else {
            // Type not served — a stream that never yields, mirroring a watch on
            // an empty storage prefix (the controller idles; any periodic
            // relist also returns empty).
            return Ok(futures::stream::pending().boxed());
        };
        let path = build_collection_for_prefix(&root, namespaced, &rt, &rest)?;

        if let Some(rv) = rv {
            return Ok(dedicated_watch(
                self.client.clone(),
                path,
                rt,
                namespaced,
                rv,
            ));
        }

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

/// `AllowWatchBookmarks: true` on every watch, as the reflector does
/// (`reflector.go:534`): the server then sends BOOKMARK frames whose
/// resourceVersion lets a restarted watch resume instead of relisting.
const WATCH_BOOKMARKS_QUERY: &str = "?allowWatchBookmarks=true";

/// The resourceVersion an event's object carries (`meta.GetResourceVersion()`
/// in `watchHandler`, `reflector.go`). For a BOOKMARK this is the whole
/// payload.
fn event_resource_version(ev: &ClientWatchEvent<Value>) -> Option<String> {
    let (ClientWatchEvent::Added(o)
    | ClientWatchEvent::Modified(o)
    | ClientWatchEvent::Deleted(o)
    | ClientWatchEvent::Bookmark(o)) = ev;
    o.get("metadata")?
        .get("resourceVersion")?
        .as_str()
        .map(str::to_string)
}

/// Re-key a client watch event into storage form. Bookmarks are progress
/// markers, not resource deltas, so they map to nothing (their rv is consumed
/// via [`event_resource_version`]).
fn map_watch_event(rt: &str, namespaced: bool, ev: ClientWatchEvent<Value>) -> Option<WatchEvent> {
    match ev {
        ClientWatchEvent::Added(o) => storage_key_for(rt, namespaced, &o)
            .ok()
            .map(|k| WatchEvent::Added(k, o.to_string())),
        ClientWatchEvent::Modified(o) => storage_key_for(rt, namespaced, &o)
            .ok()
            .map(|k| WatchEvent::Modified(k, o.to_string())),
        ClientWatchEvent::Deleted(o) => storage_key_for(rt, namespaced, &o)
            .ok()
            .map(|k| WatchEvent::Deleted(k, o.to_string())),
        ClientWatchEvent::Bookmark(_) => None,
    }
}

/// Whether a watch error is the server's in-stream `ERROR` envelope carrying a
/// 410 `Status` — `isExpiredError` (`reflector.go:1051-1058`:
/// `IsResourceExpired(err) || IsGone(err)`). `parse_watch_line` renders the
/// envelope as `watch ERROR envelope: <Status json>`.
fn is_expired_watch_error(e: &anyhow::Error) -> bool {
    let m = e.to_string();
    let Some(body) = m.strip_prefix("watch ERROR envelope: ") else {
        return false;
    };
    let Ok(status) = serde_json::from_str::<Value>(body) else {
        return false;
    };
    status.get("code").and_then(Value::as_i64) == Some(410)
        || matches!(
            status.get("reason").and_then(Value::as_str),
            Some("Expired" | "Gone")
        )
}

/// A watch opened at `rv`, private to one caller (`watch_from_revision`).
/// Events arrive after `rv`; a compacted `rv` surfaces as `Err(Error::Gone)`
/// (the reflector's cue to relist), any other failure as `Err(Error::Network)`.
fn dedicated_watch(
    client: Arc<ApiClient>,
    path: String,
    rt: String,
    namespaced: bool,
    rv: String,
) -> WatchStream {
    let (out, rx) = tokio::sync::mpsc::channel::<Result<WatchEvent>>(SHARED_WATCH_BUFFER);
    tokio::spawn(async move {
        let url = format!("{path}{WATCH_BOOKMARKS_QUERY}");
        let raw = match watch_stream::<Value>(&client, &url, Some(&rv)).await {
            Ok(s) => s,
            Err(e) => {
                let _ = out.send(Err(Error::Network(e.to_string()))).await;
                return;
            }
        };
        futures::pin_mut!(raw);
        while let Some(ev) = raw.next().await {
            let item = match ev {
                Ok(ev) => match map_watch_event(&rt, namespaced, ev) {
                    Some(ev) => Ok(ev),
                    None => continue,
                },
                Err(e) if is_expired_watch_error(&e) => Err(Error::Gone(e.to_string())),
                Err(e) => Err(Error::Network(e.to_string())),
            };
            let failed = item.is_err();
            if out.send(item).await.is_err() || failed {
                return;
            }
        }
    });
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|i| (i, rx)) }).boxed()
}

/// The single upstream task behind a shared watch: read the api-server's
/// `?watch=true` stream for `path`, re-key each event into `/registry/...`
/// form, and broadcast it to all subscribers.
///
/// Resume mechanism ported from the reflector: every event (BOOKMARK included)
/// advances `lastSyncResourceVersion` (`watchHandler`, `reflector.go:962-970`:
/// `case watch.Bookmark` then `setLastSyncResourceVersion(resourceVersion)`),
/// and a watch that ends without an expiry is re-established from it
/// (`reflector.go:526-527`) with the subscribers none the wiser. A 410
/// (`isExpiredError`, `reflector.go:561`) ends the task so subscribers relist.
/// A connection that ends having made no progress also ends it (no hot loop).
///
/// Exits (and deregisters itself) when the upstream cannot be resumed or when
/// no subscribers remain — at which point any surviving/late subscriber sees
/// the channel close and reconnects, recreating a fresh upstream.
async fn shared_upstream(
    client: Arc<ApiClient>,
    path: String,
    rt: String,
    namespaced: bool,
    tx: broadcast::Sender<Arc<WatchEvent>>,
    registry: SharedWatches,
) {
    let url = format!("{path}{WATCH_BOOKMARKS_QUERY}");
    let mut last_rv: Option<String> = None;
    'connect: loop {
        let raw = match watch_stream::<Value>(&client, &url, last_rv.as_deref()).await {
            Ok(s) => s,
            // Connect failed; drop the registration so the next watch() retries.
            Err(_) => break,
        };
        let started_at = last_rv.clone();
        futures::pin_mut!(raw);
        while let Some(ev) = raw.next().await {
            let ev = match ev {
                Ok(ev) => ev,
                // Expired -> subscribers must relist. Any other error: resume
                // from the last observed rv if we have one.
                Err(e) if is_expired_watch_error(&e) => break 'connect,
                Err(_) => break,
            };
            if let Some(rv) = event_resource_version(&ev) {
                last_rv = Some(rv);
            }
            if let Some(ev) = map_watch_event(&rt, namespaced, ev) {
                // `send` errors only when there are zero receivers — every
                // controller has dropped this watch, so stop holding the
                // connection.
                if tx.send(Arc::new(ev)).is_err() {
                    break 'connect;
                }
            }
        }
        // Resume only if this connection advanced the rv.
        if last_rv.is_none() || last_rv == started_at {
            break;
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
