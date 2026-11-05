use crate::registry::rest::HasMetadata;
use crate::{middleware::AuthContext, state::ApiServerState};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use futures::future::BoxFuture;
use futures::StreamExt;
use rusternetes_common::{
    authz::{Decision, RequestAttributes},
    types::{ObjectMeta, Status},
    Error, Result,
};
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Kubernetes watch event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WatchEventType {
    Added,
    Modified,
    Deleted,
    Bookmark,
    Error,
}

/// Kubernetes watch event wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sWatchEvent<T> {
    #[serde(rename = "type")]
    pub event_type: WatchEventType,
    pub object: T,
}

/// Query parameters for watch requests
#[derive(Debug, Deserialize)]
pub struct WatchParams {
    /// Resource version to watch from
    #[serde(
        rename = "resourceVersion",
        deserialize_with = "rusternetes_common::serde_helpers::empty_as_none",
        default
    )]
    pub resource_version: Option<String>,

    /// Timeout in seconds
    #[serde(rename = "timeoutSeconds")]
    pub timeout_seconds: Option<u64>,

    /// Label selector
    #[serde(rename = "labelSelector")]
    pub label_selector: Option<String>,

    /// Field selector
    #[serde(rename = "fieldSelector")]
    pub field_selector: Option<String>,

    /// Watch for changes
    #[serde(deserialize_with = "deserialize_k8s_bool", default)]
    pub watch: Option<bool>,

    /// Allow watch bookmarks
    #[serde(
        rename = "allowWatchBookmarks",
        deserialize_with = "deserialize_k8s_bool",
        default
    )]
    pub allow_watch_bookmarks: Option<bool>,

    /// Send initial events (consistent reads from cache, K8s 1.30+)
    /// When true, send all existing resources as ADDED events followed by
    /// a BOOKMARK to signal initial list is complete.
    #[serde(
        rename = "sendInitialEvents",
        deserialize_with = "deserialize_k8s_bool",
        default
    )]
    pub send_initial_events: Option<bool>,
}

/// Normalize a resourceVersion value: treat empty string as None (= "start from current")
pub fn normalize_resource_version(rv: Option<String>) -> Option<String> {
    rv.filter(|s| !s.is_empty())
}

/// Parse a query-parameter boolean the way Kubernetes does.
///
/// NOT Go's `strconv.ParseBool`. For query parameters, meta/v1 ListOptions
/// conversion uses `runtime.Convert_Slice_string_To_bool`
/// (staging/src/k8s.io/apimachinery/pkg/runtime/conversion.go:79-95, wired for
/// `out.Watch` at apis/meta/v1/zz_generated.conversion.go:388), whose contract
/// is:
///
/// > Only the absence of a value (i.e. zero-length slice), a value of "false",
/// > or a value of "0" resolve to false. Any other value (including empty
/// > string) resolves to true.
///
/// So `?watch=1`, `?watch=t`, `?watch=yes` and even `?watch=` are all TRUE, and
/// an unrecognised spelling is never an error. Returns `None` only for an
/// ABSENT parameter, so `.unwrap_or(false)` at the call sites gives upstream's
/// absence-is-false behaviour.
///
/// Rust's `str::parse::<bool>()` accepts only `"true"`/`"false"`, so clients
/// sending `?watch=1` (Lens and other non-client-go informers) were treated as
/// plain LIST requests, making their reflectors relist-loop instead of watching.
pub fn parse_k8s_bool(v: &str) -> Option<bool> {
    if v == "0" || v.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        Some(true)
    }
}

/// serde adapter for the query bools on [`WatchParams`].
///
/// The typed `Query<WatchParams>` path (services, endpoints, endpointslices)
/// deserialized these as strict Rust bools, so `?watch=1` was rejected with
/// `Failed to deserialize query string: provided string was not `true` or
/// `false`` — a 400 on the whole request — while the HashMap-based pods path
/// accepted it. Measured against a live api-server: a pods watch succeeded
/// where the same request on services and endpointslices returned 400.
///
/// Accepts a string (the query case, via [`parse_k8s_bool`]) or a real bool
/// (JSON/other deserialization paths).
fn deserialize_k8s_bool<'de, D>(deserializer: D) -> std::result::Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Inner;
    impl<'de> serde::de::Visitor<'de> for Inner {
        type Value = bool;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a Kubernetes query boolean (anything but \"0\"/\"false\" is true)")
        }

        fn visit_bool<E: serde::de::Error>(self, v: bool) -> std::result::Result<bool, E> {
            Ok(v)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<bool, E> {
            Ok(parse_k8s_bool(v).unwrap_or(false))
        }
    }

    struct Outer;
    impl<'de> serde::de::Visitor<'de> for Outer {
        type Value = Option<bool>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an optional Kubernetes query boolean")
        }

        fn visit_none<E: serde::de::Error>(self) -> std::result::Result<Option<bool>, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Option<bool>, E> {
            Ok(None)
        }

        fn visit_some<D2>(self, d: D2) -> std::result::Result<Option<bool>, D2::Error>
        where
            D2: serde::Deserializer<'de>,
        {
            d.deserialize_any(Inner).map(Some)
        }
    }

    deserializer.deserialize_option(Outer)
}

/// Check if a query param map indicates a watch request.
///
/// Absence means false, matching upstream's zero-length-slice case; any present
/// value goes through [`parse_k8s_bool`].
pub fn is_watch_request(params: &std::collections::HashMap<String, String>) -> bool {
    params
        .get("watch")
        .and_then(|v| parse_k8s_bool(v))
        .unwrap_or(false)
}

/// Convert query parameters to WatchParams
pub fn watch_params_from_query(params: &std::collections::HashMap<String, String>) -> WatchParams {
    WatchParams {
        resource_version: normalize_resource_version(params.get("resourceVersion").cloned()),
        timeout_seconds: params
            .get("timeoutSeconds")
            .and_then(|v| v.parse::<u64>().ok()),
        label_selector: params.get("labelSelector").cloned(),
        field_selector: params.get("fieldSelector").cloned(),
        watch: Some(true),
        allow_watch_bookmarks: params
            .get("allowWatchBookmarks")
            .map(|v| rusternetes_common::query::k8s_query_bool(v)),
        send_initial_events: params
            .get("sendInitialEvents")
            .map(|v| rusternetes_common::query::k8s_query_bool(v)),
    }
}

/// Async per-object converter applied to each raw stored JSON object before it
/// is deserialized into the watched type `T`. Used by the custom-resource watch
/// path to convert every event's object into the API version the client
/// requested, mirroring the LIST conversion in `list_custom_resources`.
/// Best-effort: on any conversion error the converter returns its input
/// unchanged (LIST is likewise tolerant of objects that won't convert).
pub type WatchObjectConverter =
    Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, serde_json::Value> + Send + Sync>;

/// Does this watch open with a snapshot of the current state, delivered as one
/// synthetic `ADDED` per object before the live stream?
///
/// Ported from `watchCache.getAllEventsSinceLocked`
/// (`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:878-913`).
/// `ListOptions.SendInitialEvents` is a `*bool` upstream and all three states
/// mean different things:
///
/// * `true` — snapshot the store whatever the resourceVersion is
///   (`return w.getIntervalFromStoreLocked(key, matchesSingle)`, first branch).
/// * absent — snapshot only at `resourceVersion=0`/unset, "to keep backward
///   compatibility ... return the current state and only then start watching
///   from that point".
/// * `false` — never snapshot; at `resourceVersion=0` upstream rewrites the
///   revision to `w.resourceVersion`, i.e. "start watching from ~now". This is
///   the only way a client can ask for a watch that does not replay the world,
///   and `unwrap_or(false)` on the query parameter made it unaskable.
///
/// Divergence: upstream answers `resourceVersion=1` from the ring (`oldest` is
/// `listResourceVersion + 1`, so a fresh cache replays every create), and its
/// matrix expects initial events there even with `sendInitialEvents=false`.
/// This server excludes `rv=1` from replay — early revisions may be compacted —
/// so the store snapshot stands in for that replay and produces the same
/// envelopes.
fn opens_with_current_state(send_initial_events: Option<bool>, requested_rv: Option<&str>) -> bool {
    match send_initial_events {
        Some(true) => true,
        Some(false) => requested_rv == Some("1"),
        None => matches!(requested_rv, None | Some("") | Some("0") | Some("1")),
    }
}

/// Highest `metadata.resourceVersion` among the objects a snapshot lists: the
/// fallback cutoff when the backend cannot stamp the list with a revision.
/// It can only under-cut, never drop an event the client has not seen.
pub fn snapshot_resource_version(snapshot: &[serde_json::Value]) -> Option<u64> {
    snapshot
        .iter()
        .filter_map(|v| {
            v.pointer("/metadata/resourceVersion")
                .and_then(|rv| rv.as_str())
                .and_then(|rv| rv.parse::<u64>().ok())
        })
        .max()
}

/// `utilflowcontrol.WatchInitialized(ctx)`: the live stream is open and the
/// initial state is read, which is `cacheWatcher.process` starting
/// (`cacher/cache_watcher.go:527`). Tells API Priority and Fairness the watch
/// is initialized so it frees the seat the watch was admitted under.
fn signal_watch_initialized() {
    crate::flow_control_watch_tracker::watch_initialized();
}

/// Cutoff for a snapshot read at `rev`: never below an item the snapshot
/// returned. `current_revision()` can answer stale on a backend whose cached
/// revision regresses under concurrent writers (#3082, rhino#6) while its
/// `list_at_revision` is a live read, so `rev` alone could sit under the
/// listed items and replay already-delivered objects as ADDED. Same floor the
/// LIST path applies via [`crate::handlers::collection_resource_version`]
/// (#1824); upstream stamps a list with the revision of the very read that
/// produced its items (`etcd3/store.go` GetList, `getResp.Header.Revision`).
pub fn snapshot_cutoff(rev: i64, items: &[serde_json::Value]) -> u64 {
    crate::handlers::collection_resource_version(
        Some(rev),
        &snapshot_resource_version(items)
            .map(|v| v.to_string())
            .unwrap_or_default(),
    )
    .parse()
    .unwrap_or(rev as u64)
}

/// The resource a watch is authorized as. A custom resource's `resource_type`
/// is its storage key type, `{group with '_'}_{plural}`
/// (`handlers/custom_resource.rs`); `RequestInfo` (and so the filter) has the
/// plural. Built-in resource types carry no such prefix.
fn authorized_resource<'a>(resource_type: &'a str, api_group: &str) -> &'a str {
    if api_group.is_empty() {
        return resource_type;
    }
    let prefix = format!("{}_", api_group.replace('.', "_"));
    resource_type.strip_prefix(&prefix).unwrap_or(resource_type)
}

/// (Named `watch_snapshot`, not `list_*`: this is the WATCH path, whose start
/// revision legitimately IS the store revision -- see
/// `list_resource_version_floor_guard_test`.)
///
/// List `prefix` as a snapshot stamped with the revision it was read at, and
/// return that revision as the cutoff for the live stream (#2223).
///
/// Upstream stamps the whole snapshot with the cache's revision under its lock
/// and continues the interval from exactly there (`newCacheIntervalFromStore`,
/// `staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache_interval.go:
/// 139-179`). Same shape as `Storage::list_paginated`: the revision is read
/// BEFORE the data and the data is read AT it, so the list is exactly the
/// state at `rev` and every live event with `rv <= rev` is already in it.
/// Falls back to a plain list + highest listed rv when the backend has no
/// usable revision.
pub async fn watch_snapshot(
    state: &ApiServerState,
    prefix: &str,
) -> Result<(Vec<serde_json::Value>, Option<u64>)> {
    if let Ok(rev) = state.storage.current_revision().await {
        if rev > 0 {
            if let Ok(list) = state
                .storage
                .list_at_revision::<serde_json::Value>(prefix, rev)
                .await
            {
                signal_watch_initialized();
                let cutoff = snapshot_cutoff(rev, &list);
                return Ok((list, Some(cutoff)));
            }
        }
    }
    let list: Vec<serde_json::Value> = state.storage.list(prefix).await?;
    let cutoff = snapshot_resource_version(&list);
    signal_watch_initialized();
    Ok((list, cutoff))
}

/// Drop every live event (ADDED, MODIFIED and DELETED) at or below the
/// snapshot's resourceVersion.
///
/// The watch subscribes before it lists (so a write in the gap is not lost),
/// which means a write between the two is in the snapshot AND on the live
/// stream. Upstream's `cacheWatcher.process` forwards only events newer than
/// the interval's revision, whatever their type:
///
/// ```text
/// // only send events newer than resourceVersion
/// if event.ResourceVersion > resourceVersion || (event.Type == watch.Bookmark && ...
/// ```
///
/// (`staging/src/k8s.io/apiserver/pkg/storage/cacher/cache_watcher.go:535-539`;
/// `processInterval` raises `resourceVersion` the same way at `:459-462`).
/// The selector-transition bookkeeping is seeded from the snapshot, so an
/// event the snapshot already reflects needs no second pass. A DELETED event
/// carries the delete's revision (storage stamps it, as etcd3's
/// `parseEvent` does with `e.rev`), so it is ordered correctly here.
pub fn skip_events_covered_by_snapshot(
    stream: rusternetes_storage::WatchStream,
    cutoff: Option<u64>,
) -> rusternetes_storage::WatchStream {
    let Some(cutoff) = cutoff else {
        return stream;
    };
    stream
        .filter(move |event| {
            let covered = match event {
                Ok(WatchEvent::Added(_, value))
                | Ok(WatchEvent::Modified(_, value))
                | Ok(WatchEvent::Deleted(_, value)) => extract_rv_from_json(value)
                    .and_then(|rv| rv.parse::<u64>().ok())
                    .is_some_and(|rv| rv <= cutoff),
                _ => false,
            };
            futures::future::ready(!covered)
        })
        .boxed()
}

/// Generic watch handler for namespaced resources.
pub async fn watch_namespaced<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: String,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    watch_namespaced_inner::<T>(
        state,
        auth_ctx,
        namespace,
        resource_type,
        api_group,
        params,
        None,
        None,
    )
    .await
}

/// Like [`watch_namespaced`], but converts every streamed object through
/// `converter` (e.g. CRD version conversion) before filtering and emitting it.
/// `bookmark_gvk` overrides the (kind, apiVersion) stamped on watch bookmarks —
/// custom resources pass their real CRD kind here so typed clients can decode
/// the bookmark (the resource_type heuristic would mangle it).
#[allow(clippy::too_many_arguments)]
pub async fn watch_namespaced_converted<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: String,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
    converter: WatchObjectConverter,
    bookmark_gvk: Option<(String, String)>,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    watch_namespaced_inner::<T>(
        state,
        auth_ctx,
        namespace,
        resource_type,
        api_group,
        params,
        Some(converter),
        bookmark_gvk,
    )
    .await
}

/// An object a watch streams: parsed from the stored JSON, filtered through
/// the selectors and serialized back with its kind and apiVersion.
///
/// Upstream has ONE watch path for every resource: the generic registry's
/// `Watch` -> `cacher.Watch` -> `serveWatch`
/// (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go`), which
/// does not care what the object is. Here the object is either a typed
/// resource ([`TypedWatchObject`]) or raw JSON ([`JsonWatchObject`]).
trait WatchObject: Serialize + HasMetadata + Send + Sized + 'static {
    fn from_raw(raw: &str) -> Option<Self>;
    fn from_value(value: serde_json::Value) -> Option<Self>;
}

/// A typed resource as a [`WatchObject`].
struct TypedWatchObject<T>(T);

impl<T: Serialize> Serialize for TypedWatchObject<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<T: HasMetadata> HasMetadata for TypedWatchObject<T> {
    fn metadata(&self) -> &ObjectMeta {
        self.0.metadata()
    }
    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        self.0.metadata_mut()
    }
}

impl<T> WatchObject for TypedWatchObject<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + HasMetadata,
{
    fn from_raw(raw: &str) -> Option<Self> {
        serde_json::from_str::<T>(raw).ok().map(Self)
    }
    fn from_value(value: serde_json::Value) -> Option<Self> {
        serde_json::from_value::<T>(value).ok().map(Self)
    }
}

impl WatchObject for JsonWatchObject {
    fn from_raw(raw: &str) -> Option<Self> {
        Self::from_value(serde_json::from_str(raw).ok()?)
    }
    fn from_value(value: serde_json::Value) -> Option<Self> {
        let meta = value
            .get("metadata")
            .cloned()
            .and_then(|m| serde_json::from_value::<ObjectMeta>(m).ok())
            .unwrap_or_default();
        Some(Self { value, meta })
    }
}

/// Parse a stored object, converting it to the requested version first (CRD
/// version conversion) so selector filtering sees the requested layout.
async fn parse_watch_object<O: WatchObject>(
    raw: &str,
    converter: &Option<WatchObjectConverter>,
) -> Option<O> {
    match converter {
        Some(convert) => {
            let value = serde_json::from_str::<serde_json::Value>(raw).ok()?;
            O::from_value(convert(value).await)
        }
        None => O::from_raw(raw),
    }
}

/// `authorizer.Authorize` for the watch verb, on the resource (and namespace)
/// being watched.
async fn authorize_watch(
    state: &ApiServerState,
    auth_ctx: &AuthContext,
    resource_type: &str,
    api_group: &str,
    namespace: Option<&str>,
) -> Result<()> {
    let mut attrs = RequestAttributes::new(
        auth_ctx.user.clone(),
        "watch",
        authorized_resource(resource_type, api_group),
    )
    .with_api_group(api_group);
    if let Some(namespace) = namespace {
        attrs = attrs.with_namespace(namespace);
    }
    match state.authorizer.authorize(&attrs).await? {
        Decision::Allow => Ok(()),
        Decision::Deny(reason) => Err(Error::Forbidden(reason)),
    }
}

/// Everything a watch needs once the request is authorized.
struct WatchSpec {
    state: Arc<ApiServerState>,
    prefix: String,
    /// What the cache indexes the watcher by (`cacher.Watch`'s name / trigger
    /// value from the predicate).
    scope: crate::watch_cache::WatchScope,
    params: WatchParams,
    converter: Option<WatchObjectConverter>,
    kind: String,
    api_version: String,
    /// The longest a watch is held open, and the default when the client sets
    /// no `timeoutSeconds`. Client-go's conformance tests ask for 350-572s, so
    /// a cap below that closes the watch under the client.
    timeout_cap: u64,
}

/// A watcher registered with the cache, before its initial events are known.
struct OpenedWatch {
    /// Ring events newer than the requested resourceVersion.
    history: Vec<crate::watch_cache::CachedWatchEvent>,
    rx: crate::cache_watcher::WatchReceiver,
    /// The requested resourceVersion when one is replayed, else 0.
    since_revision: i64,
}

/// Selector filtering and event encoding for one watch: the part of
/// `cacheWatcher.convertToWatchEvent` (`cache_watcher.go:374-395`) that turns
/// a change moving an object into the selector into `ADDED` and out of it into
/// `DELETED`.
struct WatchEncoder {
    label_selector: Option<String>,
    field_selector: Option<String>,
    converter: Option<WatchObjectConverter>,
    kind: String,
    api_version: String,
    /// Names currently outside the selector, so a later MODIFIED that moves
    /// one in surfaces as ADDED.
    excluded: std::collections::HashSet<String>,
}

impl WatchEncoder {
    /// The line for one storage event; `None` when the selector drops it.
    async fn encode<O: WatchObject>(&mut self, event: WatchEvent) -> Option<String> {
        let (event_type, object) = match event {
            WatchEvent::Added(_, raw) => {
                let object = parse_watch_object::<O>(&raw, &self.converter).await?;
                if !watch_added_matches(
                    &object,
                    &self.label_selector,
                    &self.field_selector,
                    &mut self.excluded,
                ) {
                    return None;
                }
                (WatchEventType::Added, object)
            }
            WatchEvent::Modified(_, raw) => {
                let object = parse_watch_object::<O>(&raw, &self.converter).await?;
                let event_type = watch_modified_event_type(
                    &object,
                    &self.label_selector,
                    &self.field_selector,
                    &mut self.excluded,
                )?;
                (event_type, object)
            }
            WatchEvent::Deleted(key, prev) => {
                // A delete must reach the client with the full object. The
                // previous value can be empty after compaction or not fit the
                // typed struct; the delete is still delivered, best effort.
                let Some(object) = parse_watch_object::<O>(&prev, &self.converter).await else {
                    debug!(
                        "watch: object of DELETE {} does not parse, using the raw fallback",
                        key
                    );
                    let json = build_delete_fallback_json(
                        &key,
                        &prev,
                        Some((&self.kind, &self.api_version)),
                    )?;
                    return Some(format!("{}\n", json));
                };
                // A deletion reaches only watchers whose selector held the
                // object; the object is gone, so forget its exclusion.
                if !matches_label_selector(object.metadata(), &self.label_selector)
                    || !matches_field_selector(&object, &self.field_selector)
                {
                    return None;
                }
                self.excluded.remove(&object.metadata().name);
                (WatchEventType::Deleted, object)
            }
        };
        let event = K8sWatchEvent { event_type, object };
        typed_event_json(&event, &self.kind, &self.api_version)
            .ok()
            .map(|json| format!("{}\n", json))
    }

    /// A BOOKMARK line (`watch.Bookmark`): kind, apiVersion and a
    /// resourceVersion, plus the `k8s.io/initial-events-end` annotation on the
    /// one that ends a watch-list's initial events
    /// (`storage.AnnotateInitialEventsEndBookmark`).
    fn bookmark(&self, revision: i64, initial_events_end: bool) -> Option<String> {
        let annotations = initial_events_end.then(|| {
            std::collections::HashMap::from([(
                "k8s.io/initial-events-end".to_string(),
                "true".to_string(),
            )])
        });
        let event = K8sWatchEvent {
            event_type: WatchEventType::Bookmark,
            object: BookmarkObject {
                kind: Some(self.kind.clone()),
                api_version: Some(self.api_version.clone()),
                metadata: ObjectMeta {
                    resource_version: Some(revision.to_string()),
                    annotations,
                    ..Default::default()
                },
            },
        };
        serde_json::to_string(&event)
            .ok()
            .map(|json| format!("{}\n", json))
    }
}

/// Register the watcher with the cache and, for a resourceVersion, collect the
/// events to replay (`cacher.Watch` + `getAllEventsSinceLocked`).
///
/// A specific resourceVersion is answered from the event ring, and one below
/// the ring's floor is `errors.NewResourceExpired("too old resource version:
/// %d (%d)")`; that `Err` is already built as the streamed ERROR envelope the
/// handler returns as-is (see `build_watch_error_response`).
///
/// rv=0 and rv=1 mean "list current state" and are never replayed — early
/// revisions may have been compacted (the `rv=1` divergence is documented on
/// `opens_with_current_state`). Timestamp-like values far beyond the head are
/// also not replayed.
async fn open_watch(
    state: &ApiServerState,
    prefix: &str,
    requested_rv: Option<&str>,
    scope: &crate::watch_cache::WatchScope,
) -> Result<std::result::Result<OpenedWatch, Response>> {
    // `cacher.Watch` (storage/cacher/cacher.go:513-516) and `etcd3.store.Watch`
    // (storage/etcd3/store.go:967-970) return `ParseResourceVersion`'s
    // `InvalidError` untouched, and `Store.WatchPredicate` passes it on
    // (registry/generic/registry/store.go:1459-1462). Nothing in release-1.35
    // calls `storage/errors.InterpretWatchError`, so the endpoint renders a
    // plain error: a 500 with no reason (`Error::aggregate`), as `Get` does.
    if let Some(rv) = requested_rv {
        if let Err(errs) = crate::registry::generic::store::parse_resource_version_str(rv) {
            return Err(Error::aggregate(&errs));
        }
    }
    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let replay_revision = requested_rv
        .filter(|rv| !rv.is_empty() && *rv != "0" && *rv != "1")
        .and_then(|rv| rv.parse::<i64>().ok())
        .filter(|&rv| rv > 0 && rv <= current_rev + 1000);

    let Some(since_rev) = replay_revision else {
        let rx = state.watch_cache.subscribe_scoped(prefix, scope).await;
        return Ok(Ok(OpenedWatch {
            history: Vec::new(),
            rx,
            since_revision: 0,
        }));
    };

    // A compacted revision is an in-stream ERROR envelope (HTTP 200 +
    // `{type:"ERROR", object:Status{Code:410, Reason:"Expired"}}`), not an
    // HTTP 410: by the time `cacher.Watch` can report the failure,
    // `endpoints/handlers/watch.go::serveWatch` has already written the 200
    // and chunked headers. Pinned by `watch_event_envelope_test.rs::
    // watch_envelope_error_carries_status`.
    if state
        .storage
        .is_revision_compacted(since_rev)
        .await
        .unwrap_or(false)
    {
        warn!(
            "watch {}: resourceVersion {} is compacted (head {}) — answering 410 Expired",
            prefix, since_rev, current_rev
        );
        return build_watch_error_response(resource_expired_status(since_rev, current_rev))
            .map(Err);
    }

    // ALWAYS through the shared per-prefix watch cache, never a per-client
    // storage watch. Upstream serves every watch from the cacher (ONE storage
    // watch per resource, fanned out in memory). Per-client
    // `watch_from_revision` streams to the rhino/SQLite backend proved able to
    // stall silently under write bursts, blinding one informer at a time with
    // no error for either side to react to (#1165). The shared cache loop has
    // supervised reconnect-with-replay, so a backend hiccup heals for every
    // subscriber at once.
    match state
        .watch_cache
        .subscribe_from_checked_scoped(prefix, since_rev, scope)
        .await
    {
        Ok((history, rx)) => {
            debug!(
                "Serving watch from cache ring: {} history events since rev {} for prefix {}",
                history.len(),
                since_rev,
                prefix
            );
            Ok(Ok(OpenedWatch {
                history,
                rx,
                since_revision: since_rev,
            }))
        }
        Err(floor) => {
            // A client-go RetryWatcher stops for good after a 410 and the
            // caller sees only "watch closed" (#2383): record why we sent it.
            warn!(
                "watch {}: resourceVersion {} is below the replay floor {} (head {}) — answering 410 Expired",
                prefix, since_rev, floor, current_rev
            );
            build_watch_error_response(resource_expired_status(since_rev, floor)).map(Err)
        }
    }
}

/// The one watch path. Ported from `cacher.Watch`
/// (`storage/cacher/cacher.go:513-663`) and `serveWatch`
/// (`endpoints/handlers/watch.go`):
///
/// * the watcher registers with the cache (`allowWatchBookmarks`, deadline),
/// * the initial state is read after registering and handed to the cache
///   watcher as its replayed interval, cut at the snapshot's revision
///   (`cacheInterval`, `cacheWatcher.process`),
/// * a watch-list request gets the `k8s.io/initial-events-end` bookmark after
///   it (`setInitialEventsEndBookmarkIfRequested`),
/// * every later bookmark is produced by the cache (`timeBucketWatchers`,
///   `nextBookmarkTime`), not by the handler,
/// * the handler only encodes what the watcher delivers.
async fn serve_watch<O: WatchObject>(spec: WatchSpec) -> Result<Response> {
    let WatchSpec {
        state,
        prefix,
        mut scope,
        params,
        converter,
        kind,
        api_version,
        timeout_cap,
    } = spec;
    let allow_bookmarks = params.allow_watch_bookmarks.unwrap_or(false);
    // `sendInitialEvents` is three-way — see `opens_with_current_state`.
    // `watchlist` is upstream's `*opts.SendInitialEvents` being true, the
    // half of `isListWatchRequest` (`cacher.go:1241-1243`) that does not
    // depend on `allowWatchBookmarks`.
    let send_initial_events = params.send_initial_events;
    let watchlist = send_initial_events == Some(true);
    let timeout_duration = Duration::from_secs(
        params
            .timeout_seconds
            .unwrap_or(timeout_cap)
            .min(timeout_cap),
    );
    let requested_rv = params.resource_version.clone();

    scope.options = crate::cache_watcher::WatcherOptions {
        allow_watch_bookmarks: allow_bookmarks || watchlist,
        deadline: Some(std::time::Instant::now() + timeout_duration),
        bookmark_after_resource_version: 0,
    };
    let OpenedWatch {
        mut history,
        rx,
        mut since_revision,
    } = match open_watch(&state, &prefix, requested_rv.as_deref(), &scope).await? {
        Ok(opened) => opened,
        Err(expired) => return Ok(expired),
    };

    let current_rev = state.storage.current_revision().await.unwrap_or(1);
    let should_send_initial =
        opens_with_current_state(send_initial_events, requested_rv.as_deref());
    let mut first_bookmark = None;
    if should_send_initial {
        // Read after registering, so a write in the gap is on the live queue
        // as well as in the snapshot; the cutoff drops the second copy (#2038,
        // `skip_events_covered_by_snapshot`). Objects that do not parse are
        // skipped when encoded: upstream skips bad objects and keeps
        // streaming the valid ones.
        let (snapshot, cutoff) = watch_snapshot(&state, &prefix).await?;
        let cutoff = cutoff.map(|c| c as i64);
        let snapshot_empty = snapshot.is_empty();
        let mut initial: Vec<crate::watch_cache::CachedWatchEvent> =
            Vec::with_capacity(snapshot.len() + history.len());
        for object in &snapshot {
            let Ok(raw) = serde_json::to_string(object) else {
                continue;
            };
            let name = object
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or_default();
            let revision = json_resource_version(object)
                .and_then(|rv| rv.parse::<i64>().ok())
                .unwrap_or(0);
            initial.push(crate::watch_cache::CachedWatchEvent {
                event: crate::watch_cache::WatchEventData::Added(
                    format!("{}{}", prefix, name),
                    Arc::new(raw),
                ),
                revision,
                recorded: std::time::Instant::now(),
            });
        }
        let listed_max = initial.iter().map(|e| e.revision).max().unwrap_or(0);
        let cutoff = cutoff.unwrap_or(listed_max);
        // Ring events newer than the snapshot still follow it.
        history.retain(|e| e.revision > cutoff);
        initial.append(&mut history);
        history = initial;
        since_revision = cutoff;

        if watchlist && allow_bookmarks {
            first_bookmark = Some(crate::cache_watcher::FirstBookmark {
                revision: if cutoff > 0 { cutoff } else { current_rev },
                initial_events_end: true,
            });
        } else if allow_bookmarks && snapshot_empty {
            first_bookmark = Some(crate::cache_watcher::FirstBookmark {
                revision: if cutoff > 0 { cutoff } else { current_rev },
                initial_events_end: false,
            });
        }
    } else {
        signal_watch_initialized();
        // An immediate bookmark so the client sees data right away and does
        // not time out waiting for the first HTTP/2 DATA frame. ONLY when
        // allowWatchBookmarks is true — clients that did not ask treat a
        // bookmark as an unexpected event.
        if allow_bookmarks {
            let revision = requested_rv
                .as_deref()
                .and_then(|rv| rv.parse::<i64>().ok())
                .filter(|rv| *rv > 1)
                .unwrap_or(current_rev);
            first_bookmark = Some(crate::cache_watcher::FirstBookmark {
                revision,
                initial_events_end: false,
            });
        }
    }

    let mut items = crate::cache_watcher::process_with(history, first_bookmark, rx, since_revision);
    let mut encoder = WatchEncoder {
        label_selector: params.label_selector.clone(),
        field_selector: params.field_selector.clone(),
        converter,
        kind,
        api_version,
        excluded: std::collections::HashSet::new(),
    };
    // The selector bookkeeping starts from the initial events, which pass
    // through `encode` like any other.
    let deadline = tokio::time::Instant::now() + timeout_duration;
    let stream = async_stream::stream! {
        loop {
            let item = match tokio::time::timeout_at(deadline, items.recv()).await {
                Ok(item) => item,
                Err(_) => {
                    info!("Watch stream timeout after {:?}", timeout_duration);
                    break;
                }
            };
            match item {
                Some(crate::cache_watcher::WatchItem::Event(event)) => {
                    if let Some(line) = encoder.encode::<O>(event).await {
                        yield Ok::<String, std::io::Error>(line);
                    }
                }
                Some(crate::cache_watcher::WatchItem::Bookmark {
                    revision,
                    initial_events_end,
                }) => {
                    if let Some(line) = encoder.bookmark(revision, initial_events_end) {
                        yield Ok(line);
                    }
                }
                None => {
                    // The cache terminated this watcher (too slow). NEVER
                    // splice a fresh subscription in silently: every event
                    // committed in between would be dropped, leaving the
                    // client's informer permanently stale (KCM endpointslice
                    // tracker wedge, #1165). Upstream ends the watch; the
                    // client relists with a fresh resourceVersion. Send 410 so
                    // reflectors relist immediately.
                    warn!("watch stream ended for a subscriber — sending 410 so the client relists");
                    yield Ok(watch_lagged_error_line("watch stream ended; please relist"));
                    break;
                }
            }
        }
    };

    // Note: do NOT set a Connection header — it is prohibited in HTTP/2 and
    // can make client-go drop watch connections.
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-cache, private")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(stream))
        .map_err(|e| Error::Internal(format!("Failed to build response: {}", e)))
}

#[allow(clippy::too_many_arguments)]
async fn watch_namespaced_inner<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: String,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
    converter: Option<WatchObjectConverter>,
    bookmark_gvk: Option<(String, String)>,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    info!(
        "Starting watch for {} in namespace {} (timeout: {:?}s, bookmarks: {})",
        resource_type,
        namespace,
        params.timeout_seconds,
        params.allow_watch_bookmarks.unwrap_or(false)
    );
    authorize_watch(
        &state,
        &auth_ctx,
        resource_type,
        api_group,
        Some(&namespace),
    )
    .await?;
    let (kind, api_version) =
        bookmark_gvk.unwrap_or_else(|| resource_type_to_kind_and_version(resource_type, api_group));
    serve_watch::<TypedWatchObject<T>>(WatchSpec {
        prefix: build_prefix(resource_type, Some(&namespace)),
        scope: crate::watch_cache::WatchScope::new(
            api_group,
            resource_type,
            params.field_selector.as_deref(),
        ),
        state,
        params,
        converter,
        kind,
        api_version,
        timeout_cap: 600,
    })
    .await
}

/// Generic watch handler for cluster-scoped resources.
pub async fn watch_cluster_scoped<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    watch_cluster_scoped_inner::<T>(
        state,
        auth_ctx,
        resource_type,
        api_group,
        params,
        None,
        None,
    )
    .await
}

/// Like [`watch_cluster_scoped`], but converts every streamed object through
/// `converter` (e.g. CRD version conversion) before filtering and emitting it.
/// `bookmark_gvk` overrides the (kind, apiVersion) on watch bookmarks (custom
/// resources pass their real CRD kind so typed clients can decode bookmarks).
#[allow(clippy::too_many_arguments)]
pub async fn watch_cluster_scoped_converted<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
    converter: WatchObjectConverter,
    bookmark_gvk: Option<(String, String)>,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    watch_cluster_scoped_inner::<T>(
        state,
        auth_ctx,
        resource_type,
        api_group,
        params,
        Some(converter),
        bookmark_gvk,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn watch_cluster_scoped_inner<T>(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
    converter: Option<WatchObjectConverter>,
    bookmark_gvk: Option<(String, String)>,
) -> Result<Response>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static + Clone + HasMetadata,
{
    info!(
        "Starting watch for cluster-scoped {} (timeout: {:?}s, bookmarks: {})",
        resource_type,
        params.timeout_seconds,
        params.allow_watch_bookmarks.unwrap_or(false)
    );
    authorize_watch(&state, &auth_ctx, resource_type, api_group, None).await?;
    let (kind, api_version) =
        bookmark_gvk.unwrap_or_else(|| resource_type_to_kind_and_version(resource_type, api_group));
    serve_watch::<TypedWatchObject<T>>(WatchSpec {
        prefix: build_prefix(resource_type, None),
        scope: crate::watch_cache::WatchScope::new(
            api_group,
            resource_type,
            params.field_selector.as_deref(),
        ),
        state,
        params,
        converter,
        kind,
        api_version,
        timeout_cap: 600,
    })
    .await
}

/// Check if an object matches a label selector
fn matches_label_selector(metadata: &ObjectMeta, selector: &Option<String>) -> bool {
    let selector = match selector {
        Some(s) if !s.is_empty() => s,
        _ => return true, // No selector = match all
    };

    let labels = match &metadata.labels {
        Some(l) => l,
        None => return false, // No labels but selector exists = no match
    };

    // Parse selector: supports key=value, key!=value, key in (v1,v2), key notin (v1,v2), key, !key
    for requirement in split_label_requirements(selector) {
        let requirement = requirement.trim();
        if requirement.is_empty() {
            continue;
        }

        // Handle "key in (v1,v2,...)" — set-based
        if let Some(captures) = parse_set_requirement(requirement) {
            match captures {
                SetRequirement::In(key, values) => {
                    let label_val = labels.get(key);
                    if !values.iter().any(|v| label_val.is_some_and(|lv| lv == v)) {
                        return false;
                    }
                }
                SetRequirement::NotIn(key, values) => {
                    let label_val = labels.get(key);
                    if values.iter().any(|v| label_val.is_some_and(|lv| lv == v)) {
                        return false;
                    }
                }
            }
            continue;
        }

        if let Some((key, value)) = requirement.split_once('=') {
            // Handle != (key!=value)
            if key.ends_with('!') {
                let key = key.trim_end_matches('!');
                if labels.get(key).is_some_and(|v| v == value) {
                    return false; // Must NOT equal
                }
            } else {
                // key=value or key==value: must match
                let value = value.trim_start_matches('='); // handle ==
                if labels.get(key).is_none_or(|v| v != value) {
                    return false;
                }
            }
        } else if let Some(key) = requirement.strip_prefix('!') {
            // !key — key must not exist
            if labels.contains_key(key) {
                return false;
            }
        } else {
            // Just a key with no value — check existence
            if !labels.contains_key(requirement) {
                return false;
            }
        }
    }
    true
}

enum SetRequirement<'a> {
    In(&'a str, Vec<&'a str>),
    NotIn(&'a str, Vec<&'a str>),
}

fn parse_set_requirement(s: &str) -> Option<SetRequirement<'_>> {
    // "key in (v1,v2)" or "key notin (v1,v2)"
    if let Some(idx) = s.find(" in (") {
        let key = s[..idx].trim();
        let values_str = &s[idx + 5..];
        let values_str = values_str.trim_end_matches(')');
        let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();
        return Some(SetRequirement::In(key, values));
    }
    if let Some(idx) = s.find(" notin (") {
        let key = s[..idx].trim();
        let values_str = &s[idx + 8..];
        let values_str = values_str.trim_end_matches(')');
        let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();
        return Some(SetRequirement::NotIn(key, values));
    }
    None
}

/// Split label selector string into requirements, respecting parentheses in set-based expressions
fn split_label_requirements(selector: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in selector.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                results.push(&selector[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < selector.len() {
        results.push(&selector[start..]);
    }
    results
}

/// Check if an object matches a field selector.
///
/// Serializes the full object to JSON and evaluates the selector against every
/// field path (metadata.name, metadata.namespace AND arbitrary spec/status paths
/// such as a CRD's selectableFields like `spec.color`). The old metadata-only
/// version silently passed any non-metadata field, so watch streams ignored CR
/// field selectors entirely (conformance: CustomResourceFieldSelectors watch).
/// Mirrors the LIST path (`handlers::filtering::apply_field_selector`).
fn matches_field_selector<T: Serialize>(object: &T, selector: &Option<String>) -> bool {
    let selector = match selector {
        Some(s) if !s.is_empty() => s,
        _ => return true,
    };
    let parsed = match rusternetes_common::field_selector::FieldSelector::parse(selector) {
        Ok(p) => p,
        Err(_) => return true, // unparseable selector: don't drop events
    };
    match serde_json::to_value(object) {
        Ok(value) => parsed.matches(&value),
        Err(_) => true,
    }
}

/// Decide whether an ADDED/initial object passes the combined label+field
/// selector, maintaining `excluded` — the set of object names currently NOT
/// matching the selector. Tracking exclusions lets a later MODIFIED that
/// transitions a field INTO the selector emit a synthetic ADDED (K8s watch
/// semantics), instead of a MODIFIED the client's accumulator ignores.
pub fn watch_added_matches<T: Serialize + HasMetadata>(
    object: &T,
    label_selector: &Option<String>,
    field_selector: &Option<String>,
    excluded: &mut std::collections::HashSet<String>,
) -> bool {
    let matches = matches_label_selector(object.metadata(), label_selector)
        && matches_field_selector(object, field_selector);
    let key = object.metadata().name.clone();
    if matches {
        excluded.remove(&key);
        true
    } else {
        if label_selector.is_some() || field_selector.is_some() {
            excluded.insert(key);
        }
        false
    }
}

/// Decide the watch event type for a MODIFIED storage event under the combined
/// label+field selector. Applies the same transition semantics to BOTH selector
/// kinds: into-match → ADDED, out-of-match → DELETED (once), match→match →
/// MODIFIED. Returns `None` when the event must be suppressed (object still
/// doesn't match and was already excluded).
/// Decide the event type a MODIFIED storage event should surface as, under the
/// watch's combined label+field selector.
///
/// `None` means suppress: the object did not match before and still does not.
/// An object transitioning INTO the selector surfaces as `Added`; one
/// transitioning OUT surfaces as `Deleted`, which is the contract upstream's
/// "should observe an object deletion if it stops meeting the requirements of
/// the selector" case asserts
/// (k8s.io/kubernetes/test/e2e/apimachinery/watch.go:257).
///
/// Exposed for the conformance mirror; the sibling helpers in this module are
/// public for the same reason.
pub fn watch_modified_event_type<T: Serialize + HasMetadata>(
    object: &T,
    label_selector: &Option<String>,
    field_selector: &Option<String>,
    excluded: &mut std::collections::HashSet<String>,
) -> Option<WatchEventType> {
    let has_selector = label_selector.is_some() || field_selector.is_some();
    let matches = matches_label_selector(object.metadata(), label_selector)
        && matches_field_selector(object, field_selector);
    let key = object.metadata().name.clone();
    if !matches {
        if !has_selector {
            return Some(WatchEventType::Modified);
        }
        // No longer matches: emit DELETED the first time, then suppress.
        if excluded.insert(key) {
            Some(WatchEventType::Deleted)
        } else {
            None
        }
    } else if has_selector && excluded.remove(&key) {
        Some(WatchEventType::Added)
    } else {
        Some(WatchEventType::Modified)
    }
}

/// A raw JSON object seen through [`HasMetadata`], so the JSON watch paths
/// share the typed paths' selector transitions ([`watch_added_matches`],
/// [`watch_modified_event_type`]) instead of carrying a third copy.
struct JsonWatchObject {
    value: serde_json::Value,
    meta: ObjectMeta,
}

impl Serialize for JsonWatchObject {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.value.serialize(serializer)
    }
}

impl HasMetadata for JsonWatchObject {
    fn metadata(&self) -> &ObjectMeta {
        &self.meta
    }
    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.meta
    }
}

/// Serialize a typed watch event whose object always names its `kind` and
/// `apiVersion`.
///
/// Upstream's watch encoder is `EncoderForVersion(..., scope.Kind.GroupVersion())`
/// (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:81-83`); the
/// versioning codec stamps the GVK on every encode
/// (`apimachinery/pkg/runtime/serializer/versioning/versioning.go`
/// `doEncode`: `objectKind.SetGroupVersionKind(gvk)`). A typed or dynamic
/// client's write body carries no TypeMeta, so the stored object has none, and
/// client-go cannot decode an event object without a kind: the stream
/// watcher reports a 500 ERROR that `RetryWatcher` retries silently, forever.
fn typed_event_json<T: Serialize>(
    event: &K8sWatchEvent<T>,
    kind: &str,
    api_version: &str,
) -> serde_json::Result<String> {
    let mut value = serde_json::to_value(event)?;
    if let Some(object) = value.get_mut("object") {
        stamp_gvk(object, kind, api_version);
    }
    serde_json::to_string(&value)
}

/// Construct a fallback DELETE event JSON when typed deserialization fails.
///
/// When etcd's prev_kv is absent (compaction) or the stored JSON doesn't match
/// the typed struct, the DELETE event must still be delivered. K8s always
/// delivers DELETE events; the object payload is best-effort.
///
/// Returns `Some(json_string)` if a valid event was constructed, `None` otherwise.
pub fn build_delete_fallback_json(
    key: &str,
    prev_value: &str,
    kind_hint: Option<(&str, &str)>,
) -> Option<String> {
    // Try to parse prev_value as raw JSON
    if let Ok(raw_obj) = serde_json::from_str::<serde_json::Value>(prev_value) {
        let mut raw_obj = raw_obj;
        if let Some((kind, api_version)) = kind_hint {
            stamp_gvk(&mut raw_obj, kind, api_version);
        }
        let k8s_event = serde_json::json!({
            "type": "DELETED",
            "object": raw_obj
        });
        return serde_json::to_string(&k8s_event).ok();
    }

    // prev_value is not valid JSON — construct minimal DELETE event from the key path.
    // Key format: /registry/{type}/{ns}/{name} or /registry/{type}/{name}
    let parts: Vec<&str> = key.split('/').collect();
    let name = parts.last().unwrap_or(&"");
    let ns = if parts.len() >= 5 {
        parts[parts.len() - 2]
    } else {
        ""
    };
    let k8s_event = serde_json::json!({
        "type": "DELETED",
        "object": {
            "metadata": {
                "name": name,
                "namespace": ns,
            }
        }
    });
    serde_json::to_string(&k8s_event).ok()
}

/// Stamp `kind` / `apiVersion` onto a JSON watch-event object that lacks them.
///
/// Same upstream mechanism as the typed path: the watch encoder is
/// `EncoderForVersion(..., scope.Kind.GroupVersion())`
/// (`staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:81-83`) and
/// the versioning codec's `doEncode` calls `objectKind.SetGroupVersionKind(gvk)`
/// (`apimachinery/pkg/runtime/serializer/versioning/versioning.go`). A
/// kind-less write body is stored kind-less (#2772).
pub(crate) fn stamp_gvk(object: &mut serde_json::Value, kind: &str, api_version: &str) {
    if let Some(map) = object.as_object_mut() {
        for (key, want) in [("apiVersion", api_version), ("kind", kind)] {
            let missing = map
                .get(key)
                .and_then(|v| v.as_str())
                .is_none_or(str::is_empty);
            if missing {
                map.insert(key.to_string(), serde_json::Value::String(want.into()));
            }
        }
    }
}

/// Extract resourceVersion from a raw JSON string.
pub fn extract_rv_from_json(json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| {
            v.pointer("/metadata/resourceVersion")
                .and_then(|rv| rv.as_str())
                .map(|s| s.to_string())
        })
}

/// Derive the Kind and apiVersion from resource_type and api_group
pub fn resource_type_to_kind_and_version(resource_type: &str, api_group: &str) -> (String, String) {
    // Served only as certificates.k8s.io/v1beta1 (storage_certificates.go:91-104).
    if resource_type == "clustertrustbundles" && api_group == "certificates.k8s.io" {
        return (
            "ClusterTrustBundle".to_string(),
            "certificates.k8s.io/v1beta1".to_string(),
        );
    }
    // Served only as admissionregistration.k8s.io/v1beta1
    // (storage_apiserver.go:187-205).
    if api_group == "admissionregistration.k8s.io" {
        let mutating = match resource_type {
            "mutatingadmissionpolicies" => Some("MutatingAdmissionPolicy"),
            "mutatingadmissionpolicybindings" => Some("MutatingAdmissionPolicyBinding"),
            _ => None,
        };
        if let Some(kind) = mutating {
            return (
                kind.to_string(),
                "admissionregistration.k8s.io/v1beta1".to_string(),
            );
        }
    }
    let kind = match resource_type {
        "pods" => "Pod",
        "services" => "Service",
        "deployments" => "Deployment",
        "replicasets" => "ReplicaSet",
        "statefulsets" => "StatefulSet",
        "daemonsets" => "DaemonSet",
        "jobs" => "Job",
        "cronjobs" => "CronJob",
        "configmaps" => "ConfigMap",
        "secrets" => "Secret",
        "serviceaccounts" => "ServiceAccount",
        "namespaces" => "Namespace",
        "nodes" => "Node",
        "persistentvolumes" => "PersistentVolume",
        "persistentvolumeclaims" => "PersistentVolumeClaim",
        "endpoints" => "Endpoints",
        "endpointslices" => "EndpointSlice",
        "events" => "Event",
        "ingresses" => "Ingress",
        "networkpolicies" => "NetworkPolicy",
        "leases" => "Lease",
        "clusterroles" => "ClusterRole",
        "clusterrolebindings" => "ClusterRoleBinding",
        "roles" => "Role",
        "rolebindings" => "RoleBinding",
        "storageclasses" => "StorageClass",
        "customresourcedefinitions" => "CustomResourceDefinition",
        "poddisruptionbudgets" => "PodDisruptionBudget",
        "ipaddresses" => "IPAddress",
        "limitranges" => "LimitRange",
        "resourcequotas" => "ResourceQuota",
        "runtimeclasses" => "RuntimeClass",
        "ingressclasses" => "IngressClass",
        "priorityclasses" => "PriorityClass",
        "validatingwebhookconfigurations" => "ValidatingWebhookConfiguration",
        "mutatingwebhookconfigurations" => "MutatingWebhookConfiguration",
        "validatingadmissionpolicies" => "ValidatingAdmissionPolicy",
        "validatingadmissionpolicybindings" => "ValidatingAdmissionPolicyBinding",
        "certificatesigningrequests" => "CertificateSigningRequest",
        "flowschemas" => "FlowSchema",
        "prioritylevelconfigurations" => "PriorityLevelConfiguration",
        "servicecidrs" => "ServiceCIDR",
        "replicationcontrollers" => "ReplicationController",
        "horizontalpodautoscalers" => "HorizontalPodAutoscaler",
        "controllerrevisions" => "ControllerRevision",
        "csistoragecapacities" => "CSIStorageCapacity",
        "csidrivers" => "CSIDriver",
        "csinodes" => "CSINode",
        "apiservices" => "APIService",
        "podcertificaterequests" => "PodCertificateRequest",
        "storageversionmigrations" => "StorageVersionMigration",
        // Multi-word / irregular-plural kinds the CamelCase fallback below
        // mangles (e.g. "podtemplates" -> "Podtemplate", "deviceclasses" ->
        // "Deviceclasse"). A wrong Kind on a watch event/bookmark makes the
        // client-go informer fail to decode ("no kind X is registered"), so the
        // informer never syncs. In particular the ResourceQuota QuotaMonitor
        // watches `podtemplates`; its mangled Kind wedged the whole quota
        // controller and every ResourceQuota conformance spec timed out (#1670).
        "podtemplates" => "PodTemplate",
        "volumeattachments" => "VolumeAttachment",
        "volumeattributesclasses" => "VolumeAttributesClass",
        "resourceslices" => "ResourceSlice",
        "resourceclaims" => "ResourceClaim",
        "resourceclaimtemplates" => "ResourceClaimTemplate",
        "deviceclasses" => "DeviceClass",
        "devicetaintrules" => "DeviceTaintRule",
        "storageversions" => "StorageVersion",
        other => {
            // CamelCase heuristic: capitalize first letter, remove trailing 's'
            let s = other.strip_suffix('s').unwrap_or(other);
            return (
                format!("{}{}", s[..1].to_uppercase(), &s[1..]),
                if api_group.is_empty() {
                    "v1".to_string()
                } else {
                    format!("{}/v1", api_group)
                },
            );
        }
    };
    // Served only as certificates.k8s.io/v1beta1 (storage_certificates.go).
    let api_version = if resource_type == "podcertificaterequests"
        || resource_type == "storageversionmigrations"
    {
        // storagemigration.k8s.io is served only as v1beta1.
        format!("{}/v1beta1", api_group)
    } else if resource_type == "devicetaintrules" {
        // Served only as resource.k8s.io/v1alpha3 (storage_resource.go:117-130).
        format!("{}/v1alpha3", api_group)
    } else if resource_type == "storageversions" {
        // Served only as internal.apiserver.k8s.io/v1alpha1
        // (apiserverinternal/rest/storage.go v1alpha1Storage).
        format!("{}/v1alpha1", api_group)
    } else if api_group.is_empty() {
        "v1".to_string()
    } else {
        format!("{}/v1", api_group)
    };
    (kind.to_string(), api_version)
}

/// Bookmark object containing only metadata with resourceVersion
/// Note: Bookmarks in Kubernetes watch streams don't need apiVersion/kind
/// as they are just checkpoint markers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookmarkObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    pub metadata: ObjectMeta,
}

/// Build a streamed-ERROR watch response: HTTP 200 + a single
/// `{type:"ERROR", object:<metav1.Status>}` frame, then close the stream.
///
/// Upstream parity: `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/
/// watch.go::serveWatch`. The watch handler writes a 200 status + headers
/// *before* the watch backend reports anything, so once the stream is open
/// the only way to deliver a fatal condition (compacted RV, cache
/// invalidation, etc.) is an in-stream `watch.Event{Type: Error,
/// Object: NewResourceExpired(...).Status()}` frame. Upstream
/// `cacher.cacheWatcher.process` emits exactly this when the requested
/// `resourceVersion` is below the cacher's earliest available revision.
///
/// We mirror that contract: even though the compacted-RV check could
/// produce an HTTP 410 (and used to), upstream clients treat
/// `?watch=true` as "always 200 + stream of events" — so we must surface
/// the failure as a streamed ERROR envelope.
///
/// Framing is left to the HTTP server, as Go's net/http does for `serveWatch`:
/// the body here is a fixed-size `Body::from(String)`, so hyper sends a
/// `Content-Length`. Setting `Transfer-Encoding: chunked` by hand on it makes
/// hyper write the raw bytes unchunked and the client sees
/// `IncompleteMessage` instead of the ERROR event (#3761).
fn build_watch_error_response(status_obj: Status) -> Result<Response> {
    let envelope = K8sWatchEvent {
        event_type: WatchEventType::Error,
        object: status_obj,
    };
    let json = serde_json::to_string(&envelope)
        .map_err(|e| Error::Internal(format!("Failed to serialize ERROR envelope: {}", e)))?;
    let body = format!("{}\n", json);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-cache, private")
        .body(Body::from(body))
        .map_err(|e| Error::Internal(format!("Failed to build ERROR response: {}", e)))
}

/// Build the `metav1.Status` carried in an ERROR envelope for a compacted
/// resourceVersion. Upstream emits `apierrors.NewResourceExpired(...)`
/// which produces `Status{Code: 410, Reason: "Expired",
/// Message: "too old resource version: X (Y)"}`. We mirror the wording so
/// client-go's `errors.IsResourceExpired` reflect-based check still works.
fn resource_expired_status(since_rev: i64, current_rev: i64) -> Status {
    Status::failure(
        format!("too old resource version: {} ({})", since_rev, current_rev),
        "Expired",
        410,
    )
}

/// In-stream `{"type":"ERROR","object":<410 Status>}` line for a watch that
/// fell behind the event ring (see `watch_cache::broadcast_to_stream` lag
/// termination). client-go treats an ERROR event whose Status is
/// `Expired`/410 as `ResourceExpired` and immediately re-lists + re-watches —
/// upstream cacher semantics for a too-slow watcher. Handlers MUST send this
/// and then terminate the HTTP stream (never internally resubscribe: the
/// dropped events would stay silently lost and the client's informer would be
/// permanently stale — the #1165 wedge).
fn watch_lagged_error_line(message: &str) -> String {
    let status = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": message,
        "reason": "Expired",
        "code": 410,
    });
    format!(
        "{}\n",
        serde_json::json!({"type": "ERROR", "object": status})
    )
}

// Concrete handler functions for specific resources

/// Watch pods in a namespace
pub async fn watch_pods(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::Pod>(
        state, auth_ctx, namespace, "pods", "", params,
    )
    .await
}

/// Watch services in a namespace
pub async fn watch_services(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    let converter = crate::handlers::service::default_on_read_watch_converter(
        state.cluster_ip_allocators.primary_family(),
    );
    watch_namespaced_converted::<rusternetes_common::resources::Service>(
        state, auth_ctx, namespace, "services", "", params, converter, None,
    )
    .await
}

/// Watch services across all namespaces via the legacy `/watch/` prefix
/// (installer.go registers `<root>/watch/<resource>` for the all-namespaces
/// scope too). Same decorator as `list_all_services?watch=true`.
pub async fn watch_all_services(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    let converter = crate::handlers::service::default_on_read_watch_converter(
        state.cluster_ip_allocators.primary_family(),
    );
    watch_cluster_scoped_converted::<rusternetes_common::resources::Service>(
        state, auth_ctx, "services", "", params, converter, None,
    )
    .await
}

/// Watch deployments in a namespace
pub async fn watch_deployments(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::Deployment>(
        state,
        auth_ctx,
        namespace,
        "deployments",
        "apps",
        params,
    )
    .await
}

/// Watch configmaps in a namespace
pub async fn watch_configmaps(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::ConfigMap>(
        state,
        auth_ctx,
        namespace,
        "configmaps",
        "",
        params,
    )
    .await
}

/// Watch secrets in a namespace
pub async fn watch_secrets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_namespaced::<rusternetes_common::resources::Secret>(
        state, auth_ctx, namespace, "secrets", "", params,
    )
    .await
}

/// Watch nodes (cluster-scoped)
pub async fn watch_nodes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<impl IntoResponse> {
    watch_cluster_scoped::<rusternetes_common::resources::Node>(
        state, auth_ctx, "nodes", "", params,
    )
    .await
}

/// Watch namespaces (cluster-scoped)
pub async fn watch_namespaces(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::Namespace>(
        state,
        auth_ctx,
        "namespaces",
        "",
        params,
    )
    .await
}

/// Watch endpoints in a namespace
pub async fn watch_endpoints(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Endpoints>(
        state,
        auth_ctx,
        namespace,
        "endpoints",
        "",
        params,
    )
    .await
}

/// Watch endpointslices in a namespace
pub async fn watch_endpointslices(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::EndpointSlice>(
        state,
        auth_ctx,
        namespace,
        "endpointslices",
        "discovery.k8s.io",
        params,
    )
    .await
}

/// Watch statefulsets in a namespace
pub async fn watch_statefulsets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::StatefulSet>(
        state,
        auth_ctx,
        namespace,
        "statefulsets",
        "apps",
        params,
    )
    .await
}

/// Watch replicasets in a namespace
pub async fn watch_replicasets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ReplicaSet>(
        state,
        auth_ctx,
        namespace,
        "replicasets",
        "apps",
        params,
    )
    .await
}

/// Watch daemonsets in a namespace
pub async fn watch_daemonsets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::DaemonSet>(
        state,
        auth_ctx,
        namespace,
        "daemonsets",
        "apps",
        params,
    )
    .await
}

/// Watch jobs in a namespace
pub async fn watch_jobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Job>(
        state, auth_ctx, namespace, "jobs", "batch", params,
    )
    .await
}

/// Watch cronjobs in a namespace
pub async fn watch_cronjobs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::CronJob>(
        state, auth_ctx, namespace, "cronjobs", "batch", params,
    )
    .await
}

/// Watch events in a namespace
pub async fn watch_events(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Event>(
        state, auth_ctx, namespace, "events", "", params,
    )
    .await
}

/// Watch serviceaccounts in a namespace
pub async fn watch_serviceaccounts(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ServiceAccount>(
        state,
        auth_ctx,
        namespace,
        "serviceaccounts",
        "",
        params,
    )
    .await
}

/// Watch persistentvolumes (cluster-scoped)
pub async fn watch_persistentvolumes(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PersistentVolume>(
        state,
        auth_ctx,
        "persistentvolumes",
        "",
        params,
    )
    .await
}

/// Watch persistentvolumeclaims in a namespace
pub async fn watch_persistentvolumeclaims(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::PersistentVolumeClaim>(
        state,
        auth_ctx,
        namespace,
        "persistentvolumeclaims",
        "",
        params,
    )
    .await
}

/// Watch runtimeclasses (cluster-scoped)
pub async fn watch_runtimeclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::RuntimeClass>(
        state,
        auth_ctx,
        "runtimeclasses",
        "node.k8s.io",
        params,
    )
    .await
}

/// Watch resourcequotas in a namespace
pub async fn watch_resourcequotas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ResourceQuota>(
        state,
        auth_ctx,
        namespace,
        "resourcequotas",
        "",
        params,
    )
    .await
}

/// Watch resourcequotas across all namespaces
pub async fn watch_resourcequotas_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ResourceQuota>(
        state,
        auth_ctx,
        "resourcequotas",
        "",
        params,
    )
    .await
}

/// Watch servicecidrs (cluster-scoped)
pub async fn watch_servicecidrs(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ServiceCIDR>(
        state,
        auth_ctx,
        "servicecidrs",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch ipaddresses (cluster-scoped)
pub async fn watch_ipaddresses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::IPAddress>(
        state,
        auth_ctx,
        "ipaddresses",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch customresourcedefinitions (cluster-scoped)
pub async fn watch_crds(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::CustomResourceDefinition>(
        state,
        auth_ctx,
        "customresourcedefinitions",
        "apiextensions.k8s.io",
        params,
    )
    .await
}

/// Watch validatingwebhookconfigurations (cluster-scoped)
pub async fn watch_validatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ValidatingWebhookConfiguration>(
        state,
        auth_ctx,
        "validatingwebhookconfigurations",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch mutatingwebhookconfigurations (cluster-scoped)
pub async fn watch_mutatingwebhookconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::MutatingWebhookConfiguration>(
        state,
        auth_ctx,
        "mutatingwebhookconfigurations",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch validatingadmissionpolicies (cluster-scoped)
pub async fn watch_validatingadmissionpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ValidatingAdmissionPolicy>(
        state,
        auth_ctx,
        "validatingadmissionpolicies",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch validatingadmissionpolicybindings (cluster-scoped)
pub async fn watch_validatingadmissionpolicybindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ValidatingAdmissionPolicyBinding>(
        state,
        auth_ctx,
        "validatingadmissionpolicybindings",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch poddisruptionbudgets in a namespace
pub async fn watch_poddisruptionbudgets(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::PodDisruptionBudget>(
        state,
        auth_ctx,
        namespace,
        "poddisruptionbudgets",
        "policy",
        params,
    )
    .await
}

/// Watch poddisruptionbudgets across all namespaces
pub async fn watch_poddisruptionbudgets_all(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PodDisruptionBudget>(
        state,
        auth_ctx,
        "poddisruptionbudgets",
        "policy",
        params,
    )
    .await
}

/// Watch limitranges in a namespace
pub async fn watch_limitranges(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::LimitRange>(
        state,
        auth_ctx,
        namespace,
        "limitranges",
        "",
        params,
    )
    .await
}

/// Watch replicationcontrollers in a namespace
pub async fn watch_replicationcontrollers(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ReplicationController>(
        state,
        auth_ctx,
        namespace,
        "replicationcontrollers",
        "",
        params,
    )
    .await
}

/// Watch priorityclasses (cluster-scoped)
pub async fn watch_priorityclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PriorityClass>(
        state,
        auth_ctx,
        "priorityclasses",
        "scheduling.k8s.io",
        params,
    )
    .await
}

/// Watch storageclasses (cluster-scoped)
pub async fn watch_storageclasses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::StorageClass>(
        state,
        auth_ctx,
        "storageclasses",
        "storage.k8s.io",
        params,
    )
    .await
}

/// Watch clusterroles (cluster-scoped)
pub async fn watch_clusterroles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ClusterRole>(
        state,
        auth_ctx,
        "clusterroles",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch clusterrolebindings (cluster-scoped)
pub async fn watch_clusterrolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::ClusterRoleBinding>(
        state,
        auth_ctx,
        "clusterrolebindings",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch roles in a namespace
pub async fn watch_roles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Role>(
        state,
        auth_ctx,
        namespace,
        "roles",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch rolebindings in a namespace
pub async fn watch_rolebindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::RoleBinding>(
        state,
        auth_ctx,
        namespace,
        "rolebindings",
        "rbac.authorization.k8s.io",
        params,
    )
    .await
}

/// Watch leases in a namespace
pub async fn watch_leases(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Lease>(
        state,
        auth_ctx,
        namespace,
        "leases",
        "coordination.k8s.io",
        params,
    )
    .await
}

/// Watch ingresses in a namespace
pub async fn watch_ingresses(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::Ingress>(
        state,
        auth_ctx,
        namespace,
        "ingresses",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch networkpolicies in a namespace
pub async fn watch_networkpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::NetworkPolicy>(
        state,
        auth_ctx,
        namespace,
        "networkpolicies",
        "networking.k8s.io",
        params,
    )
    .await
}

/// Watch certificatesigningrequests (cluster-scoped)
pub async fn watch_certificatesigningrequests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::CertificateSigningRequest>(
        state,
        auth_ctx,
        "certificatesigningrequests",
        "certificates.k8s.io",
        params,
    )
    .await
}

/// Watch mutatingadmissionpolicies (cluster-scoped,
/// admissionregistration.k8s.io/v1beta1, 404 unless the gate is on)
pub async fn watch_mutatingadmissionpolicies(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    crate::handlers::mutating_admission_policy::gate()?;
    watch_cluster_scoped::<rusternetes_common::resources::MutatingAdmissionPolicy>(
        state,
        auth_ctx,
        "mutatingadmissionpolicies",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Watch mutatingadmissionpolicybindings (cluster-scoped,
/// admissionregistration.k8s.io/v1beta1, 404 unless the gate is on)
pub async fn watch_mutatingadmissionpolicybindings(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    crate::handlers::mutating_admission_policy::gate()?;
    watch_cluster_scoped::<rusternetes_common::resources::MutatingAdmissionPolicyBinding>(
        state,
        auth_ctx,
        "mutatingadmissionpolicybindings",
        "admissionregistration.k8s.io",
        params,
    )
    .await
}

/// Legacy `GET /apis/storagemigration.k8s.io/v1beta1/watch/storageversionmigrations`
/// (the deprecated `/watch/` path prefix upstream still serves for every
/// resource; same stream as `?watch=true` on the list route).
pub async fn watch_storageversionmigrations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    crate::handlers::storageversionmigration::gate()?;
    watch_cluster_scoped::<
        rusternetes_common::resources::storageversionmigration::StorageVersionMigration,
    >(
        state,
        auth_ctx,
        "storageversionmigrations",
        "storagemigration.k8s.io",
        params,
    )
    .await
}

/// Watch clustertrustbundles (cluster-scoped, certificates.k8s.io/v1beta1)
pub async fn watch_clustertrustbundles(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    crate::handlers::clustertrustbundle::gate()?;
    watch_cluster_scoped::<rusternetes_common::resources::ClusterTrustBundle>(
        state,
        auth_ctx,
        "clustertrustbundles",
        "certificates.k8s.io",
        params,
    )
    .await
}

/// Watch podcertificaterequests in a namespace (certificates.k8s.io/v1beta1,
/// 404 unless the PodCertificateRequest gate is on). The deprecated `/watch/`
/// route installer.go registers for the namespaced scope.
pub async fn watch_podcertificaterequests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    crate::handlers::podcertificaterequest::gate()?;
    watch_namespaced::<rusternetes_common::resources::podcertificaterequest::PodCertificateRequest>(
        state,
        auth_ctx,
        namespace,
        "podcertificaterequests",
        "certificates.k8s.io",
        params,
    )
    .await
}

/// Watch podcertificaterequests across all namespaces via the `/watch/`
/// prefix (installer.go registers it for the all-namespaces scope too).
pub async fn watch_all_podcertificaterequests(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    crate::handlers::podcertificaterequest::gate()?;
    watch_cluster_scoped::<
        rusternetes_common::resources::podcertificaterequest::PodCertificateRequest,
    >(
        state,
        auth_ctx,
        "podcertificaterequests",
        "certificates.k8s.io",
        params,
    )
    .await
}

/// Watch flowschemas (cluster-scoped)
pub async fn watch_flowschemas(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::FlowSchema>(
        state,
        auth_ctx,
        "flowschemas",
        "flowcontrol.apiserver.k8s.io",
        params,
    )
    .await
}

/// Watch prioritylevelconfigurations (cluster-scoped)
pub async fn watch_prioritylevelconfigurations(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_cluster_scoped::<rusternetes_common::resources::PriorityLevelConfiguration>(
        state,
        auth_ctx,
        "prioritylevelconfigurations",
        "flowcontrol.apiserver.k8s.io",
        params,
    )
    .await
}

/// Watch podtemplates in a namespace
pub async fn watch_podtemplates(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::PodTemplate>(
        state,
        auth_ctx,
        namespace,
        "podtemplates",
        "",
        params,
    )
    .await
}

/// Watch controllerrevisions in a namespace
pub async fn watch_controllerrevisions(
    State(state): State<Arc<ApiServerState>>,
    Extension(auth_ctx): Extension<AuthContext>,
    Path(namespace): Path<String>,
    Query(params): Query<WatchParams>,
) -> Result<Response> {
    watch_namespaced::<rusternetes_common::resources::ControllerRevision>(
        state,
        auth_ctx,
        namespace,
        "controllerrevisions",
        "apps",
        params,
    )
    .await
}

/// Helper to extract metadata fields from a serde_json::Value
fn json_resource_version(val: &serde_json::Value) -> Option<String> {
    val.get("metadata")?
        .get("resourceVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Watch cluster-scoped resources using serde_json::Value (for DRA types without HasMetadata)
pub async fn watch_cluster_scoped_json(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response> {
    info!("Starting JSON watch for cluster-scoped {}", resource_type);
    authorize_watch(&state, &auth_ctx, resource_type, api_group, None).await?;
    let (kind, api_version) = resource_type_to_kind_and_version(resource_type, api_group);
    serve_watch::<JsonWatchObject>(WatchSpec {
        prefix: build_prefix(resource_type, None),
        scope: crate::watch_cache::WatchScope::new(
            api_group,
            resource_type,
            params.field_selector.as_deref(),
        ),
        state,
        params,
        converter: None,
        kind,
        api_version,
        timeout_cap: 300,
    })
    .await
}

/// Watch namespaced resources using serde_json::Value (for DRA types without HasMetadata)
pub async fn watch_namespaced_json(
    state: Arc<ApiServerState>,
    auth_ctx: AuthContext,
    namespace: String,
    resource_type: &str,
    api_group: &str,
    params: WatchParams,
) -> Result<Response> {
    info!(
        "Starting JSON watch for namespaced {}/{}",
        namespace, resource_type
    );
    authorize_watch(
        &state,
        &auth_ctx,
        resource_type,
        api_group,
        Some(&namespace),
    )
    .await?;
    let (kind, api_version) = resource_type_to_kind_and_version(resource_type, api_group);
    serve_watch::<JsonWatchObject>(WatchSpec {
        prefix: build_prefix(resource_type, Some(&namespace)),
        scope: crate::watch_cache::WatchScope::new(
            api_group,
            resource_type,
            params.field_selector.as_deref(),
        ),
        state,
        params,
        converter: None,
        kind,
        api_version,
        timeout_cap: 300,
    })
    .await
}
