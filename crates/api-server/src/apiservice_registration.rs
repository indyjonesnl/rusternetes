//! The kube-aggregator `apiservice-registration-controller` and
//! `apiservice-discovery-controller` post-start hooks, with the controllers
//! behind them.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/apiserver/`:
//! - `apiserver.go:368-420` -- the two hooks and the
//!   `apiServiceRegistrationControllerInitiated` signal between them;
//! - `apiservice_controller.go` -- `APIServiceRegistrationController`
//!   (`sync`, `Run`, the rate-limited worker, the informer handlers);
//! - `apiserver.go:AddAPIService/RemoveAPIService` -- the `APIHandlerManager`
//!   ([`AggregatorHandlerManager`]);
//! - `handler_discovery.go` -- `discoveryManager` (`AddAPIService`,
//!   `RemoveAPIService`, `syncAPIService`, `Run`, `removeUnusedServices`,
//!   `fetchFreshDiscoveryForService`). The HTTP half (etag/304, v2/v2beta1,
//!   legacy `/apis/{g}/{v}` fallback, 5s timeout) is in
//!   [`crate::aggregated_discovery`]. Tests are ported from
//!   `handler_discovery_test.go` (`TestInitialRunHasAllAPIServices`,
//!   `TestWaitForSync`, `TestRemoveAPIService`, `TestBasic`, `TestServiceGC`,
//!   `TestV2Beta1Skew`, `TestDirty`, `TestLegacyFallback*`,
//!   `TestAPIServiceStale`, `TestNotModified`).
//!
//! Deviations (each tracked as a follow-up issue):
//! - The APIService informer is a storage watch ([`spawn_apiservice_event_handlers`])
//!   feeding the same Add/Update/Delete handlers; the lister reads storage
//!   directly. The `Run` cache-sync wait is therefore "storage is readable".
//! - There is no per-APIService `proxyHandler` to build or mux to mount it on:
//!   the aggregator proxy (`handlers::aggregator`) resolves the APIService from
//!   storage per request. [`AggregatorHandlerManager`] keeps upstream's
//!   `proxyHandlers` / `handledGroupVersions` bookkeeping and forwards to the
//!   discovery manager, which is what the hooks' ordering depends on.
//! - The `http.Handler` of an APIService is an [`AggregatorHandler`]
//!   (production: [`crate::aggregated_discovery::ProxyDiscoveryHandler`]).
//! - The merged document ([`MergedDiscovery`], process-global via
//!   [`merged_discovery`]) is read by the `/apis` handler in `router.rs` to fill
//!   in aggregated groups' resources and freshness.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use rusternetes_common::resources::APIService;
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use tokio::sync::watch;

use crate::aggregated_discovery::{
    is_aggregated_discovery_content_type, legacy_path, parse_discovery_list,
    parse_legacy_resource_list, serve_with_timeout, AggregatorHandler, HandlerRequest,
    ProxyDiscoveryHandler, ACCEPT_V2, ACCEPT_V2_BETA1, AGGREGATOR_GROUP, AGGREGATOR_USER,
};
use crate::autoregister::{APIServiceClient, StorageAPIServiceClient};
use crate::crdregistration::WorkQueue;
use crate::post_start_hooks::{spawn_hook_with, PostStartHooks};
use crate::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;

/// `apiserver.go:372`.
pub const APISERVICE_REGISTRATION_HOOK: &str = "apiservice-registration-controller";
/// `apiserver.go:383`.
pub const APISERVICE_DISCOVERY_HOOK: &str = "apiservice-discovery-controller";

/// `legacyAPIServiceName` (apiserver.go): the `v1.` APIService for `/api`.
const LEGACY_API_SERVICE_NAME: &str = "v1.";
/// `APIRegistrationGroupPriority` (handler_discovery.go:54).
pub const API_REGISTRATION_GROUP_PRIORITY: i32 = 20001;

/// `v1helper.APIServiceNameToGroupVersion`: `<version>.<group>`.
fn api_service_name_to_group_version(name: &str) -> (String, String) {
    match name.split_once('.') {
        Some((version, group)) => (group.to_string(), version.to_string()),
        None => (String::new(), name.to_string()),
    }
}

/// `APIHandlerManager` (apiservice_controller.go:41-44).
pub trait APIHandlerManager: Send + Sync {
    fn add_api_service(&self, api_service: &APIService) -> Result<(), String>;
    fn remove_api_service(&self, api_service_name: &str);
}

// ---------------------------------------------------------------------------
// APIServiceRegistrationController
// ---------------------------------------------------------------------------

/// `APIServiceRegistrationController` (apiservice_controller.go:47-58).
pub struct APIServiceRegistrationController {
    api_handler_manager: Arc<dyn APIHandlerManager>,
    api_service_lister: Arc<dyn APIServiceClient>,
    queue: WorkQueue<String>,
    rate_limiter: Mutex<ControllerRateLimiter>,
}

impl APIServiceRegistrationController {
    /// `NewAPIServiceRegistrationController` (the informer handlers are
    /// [`Self::on_add`] / [`Self::on_update`] / [`Self::on_delete`]).
    pub fn new(
        api_service_lister: Arc<dyn APIServiceClient>,
        api_handler_manager: Arc<dyn APIHandlerManager>,
    ) -> Self {
        Self {
            api_handler_manager,
            api_service_lister,
            queue: WorkQueue::default(),
            rate_limiter: Mutex::new(ControllerRateLimiter::new()),
        }
    }

    /// `addAPIService`.
    pub fn on_add(&self, api_service: &APIService) {
        self.queue.add(api_service.metadata.name.clone());
    }
    /// `updateAPIService`.
    pub fn on_update(&self, api_service: &APIService) {
        self.queue.add(api_service.metadata.name.clone());
    }
    /// `deleteAPIService` (the tombstone is unrepresentable: a storage watch
    /// delivers the previous object).
    pub fn on_delete(&self, api_service: &APIService) {
        self.queue.add(api_service.metadata.name.clone());
    }

    /// `Enqueue`: queue all apiservices to be rehandled.
    pub async fn enqueue(&self) {
        match self.api_service_lister.list_names().await {
            Ok(names) => names.into_iter().for_each(|n| self.queue.add(n)),
            Err(e) => tracing::error!("apiservice-registration: {e:?}"),
        }
    }

    /// `sync`.
    pub async fn sync(&self, key: &str) -> Result<(), String> {
        match self.api_service_lister.get(key).await {
            Ok(None) => {
                self.api_handler_manager.remove_api_service(key);
                Ok(())
            }
            Ok(Some(api_service)) => self.api_handler_manager.add_api_service(&api_service),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// The "initially sync all APIServices to make sure the proxy handler is
    /// complete" poll (apiservice_controller.go:104-119): one attempt.
    async fn initial_sync_once(&self) -> Result<(), String> {
        let names = self
            .api_service_lister
            .list_names()
            .await
            .map_err(|e| format!("failed to initially list APIServices: {e:?}"))?;
        for name in names {
            // Listing and getting are two calls here (the lister is a cache
            // upstream); a name deleted in between is simply not registered.
            let Some(s) = self
                .api_service_lister
                .get(&name)
                .await
                .map_err(|e| format!("failed to initially list APIServices: {e:?}"))?
            else {
                continue;
            };
            self.api_handler_manager
                .add_api_service(&s)
                .map_err(|e| format!("failed to initially sync APIService {name}: {e}"))?;
        }
        Ok(())
    }

    /// `Run`: install every known APIService (retrying each second), close
    /// `handler_synced` (`apiServiceRegistrationControllerInitiated`), then
    /// process queued changes with ONE worker until `stop` fires.
    pub async fn run(
        self: Arc<Self>,
        mut stop: watch::Receiver<bool>,
        handler_synced: watch::Sender<bool>,
    ) {
        tracing::info!("Starting APIServiceRegistrationController");
        loop {
            match self.initial_sync_once().await {
                Ok(()) => break,
                Err(e) => tracing::error!("apiservice-registration: {e}"),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = stop.changed() => return,
            }
        }
        handler_synced.send_replace(true);

        // "only start one worker thread since its a slow moving API and the
        // aggregation server adding bits aren't threadsafe"
        let c = self.clone();
        let worker = tokio::spawn(async move { while c.process_next_work_item().await {} });
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
        self.queue.shut_down();
        let _ = worker.await;
        tracing::info!("Shutting down APIServiceRegistrationController");
    }

    /// `processNextWorkItem`: false when it's time to quit.
    async fn process_next_work_item(self: &Arc<Self>) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        let result = self.sync(&key).await;
        self.queue.done(&key);
        match result {
            Ok(()) => self.rate_limiter.lock().unwrap().forget(&key),
            Err(e) => {
                tracing::error!("apiservice-registration: {key} failed with : {e}");
                // `AddRateLimited`
                let delay = self
                    .rate_limiter
                    .lock()
                    .unwrap()
                    .when_at(&key, tokio::time::Instant::now());
                let c = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    c.queue.add(key);
                });
            }
        }
        true
    }
}

/// The APIService informer's event handlers, fed by a storage watch. Start it
/// BEFORE `run` so no event falls between the initial list and the watch.
pub async fn spawn_apiservice_event_handlers<S: Storage + 'static>(
    storage: Arc<S>,
    controller: Arc<APIServiceRegistrationController>,
) {
    let mut stream = match storage.watch(&build_prefix("apiservices", None)).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("apiservice-registration: cannot watch APIServices: {e}");
            return;
        }
    };
    tokio::spawn(async move {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        while let Some(ev) = stream.next().await {
            let Ok(ev) = ev else { continue };
            match ev {
                WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                    let Ok(s) = serde_json::from_str::<APIService>(&v) else {
                        continue;
                    };
                    if seen.insert(s.metadata.name.clone()) {
                        controller.on_add(&s);
                    } else {
                        controller.on_update(&s);
                    }
                }
                WatchEvent::Deleted(_, v) => {
                    let Ok(s) = serde_json::from_str::<APIService>(&v) else {
                        continue;
                    };
                    seen.remove(&s.metadata.name);
                    controller.on_delete(&s);
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Merged aggregated discovery + discoveryManager
// ---------------------------------------------------------------------------

/// `apidiscoveryv2.DiscoveryFreshness`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryFreshness {
    Current,
    Stale,
}

/// `apidiscoveryv2.APIVersionDiscovery`, as far as the manager touches it.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionDiscovery {
    pub version: String,
    pub resources: Vec<serde_json::Value>,
    pub freshness: DiscoveryFreshness,
}

/// The `discoveryendpoint.ResourceManager` the manager merges into
/// (`AddGroupVersion`, `RemoveGroupVersion`, `SetGroupVersionPriority`).
#[derive(Default)]
pub struct MergedDiscovery {
    state: Mutex<MergedState>,
}

#[derive(Default)]
struct MergedState {
    groups: BTreeMap<String, BTreeMap<String, VersionDiscovery>>,
    priorities: BTreeMap<(String, String), (i32, i32)>,
}

impl MergedDiscovery {
    pub fn add_group_version(&self, group: &str, entry: VersionDiscovery) {
        let mut st = self.state.lock().unwrap();
        st.groups
            .entry(group.to_string())
            .or_default()
            .insert(entry.version.clone(), entry);
    }
    /// Removes the version, and the group when no version is left.
    pub fn remove_group_version(&self, group: &str, version: &str) {
        let mut st = self.state.lock().unwrap();
        if let Some(g) = st.groups.get_mut(group) {
            g.remove(version);
            if g.is_empty() {
                st.groups.remove(group);
            }
        }
        st.priorities
            .remove(&(group.to_string(), version.to_string()));
    }
    pub fn set_group_version_priority(
        &self,
        group: &str,
        version: &str,
        group_priority: i32,
        version_priority: i32,
    ) {
        self.state.lock().unwrap().priorities.insert(
            (group.to_string(), version.to_string()),
            (group_priority, version_priority),
        );
    }
    /// group -> version -> entry.
    pub fn groups(&self) -> BTreeMap<String, BTreeMap<String, VersionDiscovery>> {
        self.state.lock().unwrap().groups.clone()
    }
    pub fn priority(&self, group: &str, version: &str) -> Option<(i32, i32)> {
        self.state
            .lock()
            .unwrap()
            .priorities
            .get(&(group.to_string(), version.to_string()))
            .copied()
    }
}

/// `serviceKey` (handler_discovery.go:117-143): `spec.service` with the port
/// defaulted to 443.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceKey {
    pub namespace: String,
    pub name: String,
    pub port: i32,
}

fn new_service_key(s: &rusternetes_common::resources::APIServiceReference) -> ServiceKey {
    ServiceKey {
        namespace: s.namespace.clone(),
        name: s.name.clone(),
        port: s.port.unwrap_or(443),
    }
}

/// The process-wide merged aggregated-discovery document the discovery manager
/// writes and the `/apis` handler reads (upstream: the aggregator's
/// `discoveryendpoint.ResourceManager`).
pub fn merged_discovery() -> &'static Arc<MergedDiscovery> {
    static MERGED: std::sync::OnceLock<Arc<MergedDiscovery>> = std::sync::OnceLock::new();
    MERGED.get_or_init(|| Arc::new(MergedDiscovery::default()))
}

#[derive(Clone)]
struct GroupVersionInfo {
    /// "Guaranteed to be a time greater than the most recent time the
    /// APIService was known to be modified."
    last_marked_dirty: Instant,
    service: ServiceKey,
    group_priority: i32,
    version_priority: i32,
}

#[derive(Clone)]
struct CachedResult {
    discovery: BTreeMap<(String, String), VersionDiscovery>,
    /// ETag of the cached discovery document.
    etag: Option<String>,
    /// "Guaranteed to be a time less than the time the server responded".
    last_updated: Instant,
}

/// `discoveryManager` (handler_discovery.go:67-110).
pub struct DiscoveryManager {
    merged: Arc<MergedDiscovery>,
    handler: Arc<dyn AggregatorHandler>,
    api_services: Mutex<HashMap<String, GroupVersionInfo>>,
    cached_results: Mutex<HashMap<ServiceKey, CachedResult>>,
    dirty_api_service_queue: WorkQueue<String>,
    rate_limiter: Mutex<ControllerRateLimiter>,
}

impl DiscoveryManager {
    /// `NewDiscoveryManager`.
    pub fn new(merged: Arc<MergedDiscovery>, handler: Arc<dyn AggregatorHandler>) -> Arc<Self> {
        Arc::new(Self {
            merged,
            handler,
            api_services: Mutex::new(HashMap::new()),
            cached_results: Mutex::new(HashMap::new()),
            dirty_api_service_queue: WorkQueue::default(),
            rate_limiter: Mutex::new(ControllerRateLimiter::new()),
        })
    }

    /// `AddAPIService`.
    pub fn add_api_service(&self, api_service: &APIService) {
        // "If service is nil then its information is contained by a local
        // APIService which is has already been added to the manager."
        let Some(service) = api_service.spec.service.as_ref() else {
            return;
        };
        self.api_services.lock().unwrap().insert(
            api_service.metadata.name.clone(),
            GroupVersionInfo {
                group_priority: api_service.spec.group_priority_minimum,
                version_priority: api_service.spec.version_priority,
                last_marked_dirty: Instant::now(),
                service: new_service_key(service),
            },
        );
        self.remove_unused_services();
        self.dirty_api_service_queue
            .add(api_service.metadata.name.clone());
    }

    /// `RemoveAPIService`.
    pub fn remove_api_service(&self, api_service_name: &str) {
        let removed = self
            .api_services
            .lock()
            .unwrap()
            .remove(api_service_name)
            .is_some();
        if removed {
            // "mark dirty if there was actually something deleted"
            self.remove_unused_services();
            self.dirty_api_service_queue
                .add(api_service_name.to_string());
        }
    }

    /// `removeUnusedServices`: purge cache entries no known APIService uses.
    fn remove_unused_services(&self) {
        let used: std::collections::HashSet<ServiceKey> = self
            .api_services
            .lock()
            .unwrap()
            .values()
            .map(|i| i.service.clone())
            .collect();
        self.cached_results
            .lock()
            .unwrap()
            .retain(|k, _| used.contains(k));
    }

    /// `fetchFreshDiscoveryForService` (handler_discovery.go:216-379).
    ///
    /// "Returns the cached result if it is retrieved after the apiservice was
    /// last marked dirty." On a fetch error upstream returns a nil result and
    /// the error (its doc comment promises the stale cache entry, its code does
    /// not; the code is what is ported), so the caller marks the GV `Stale`
    /// with no resources.
    async fn fetch_fresh_discovery_for_service(
        &self,
        group: &str,
        version: &str,
        info: &GroupVersionInfo,
    ) -> (Option<CachedResult>, Option<String>) {
        // "Lookup last cached result for this apiservice's service."
        let cached = self
            .cached_results
            .lock()
            .unwrap()
            .get(&info.service)
            .cloned();
        // "If entry exists and was updated after the given time, just stop now"
        if let Some(c) = &cached {
            if c.last_updated > info.last_marked_dirty {
                return (cached, None);
            }
        }
        let etag = cached.as_ref().and_then(|c| c.etag.clone());

        let req = HandlerRequest {
            path: "/apis".to_string(),
            accept: format!("{ACCEPT_V2},{ACCEPT_V2_BETA1}"),
            if_none_match: etag.clone(),
            user: AGGREGATOR_USER.to_string(),
            groups: vec![AGGREGATOR_GROUP.to_string()],
        };
        // "Important that the time recorded in the data's "lastUpdated" is
        // conservatively from BEFORE the request is dispatched so that
        // lastUpdated can be used to de-duplicate requests."
        let now = Instant::now();
        let resp = serve_with_timeout(self.handler.as_ref(), group, version, req).await;
        let is_aggregated = resp
            .content_type
            .as_deref()
            .is_some_and(is_aggregated_discovery_content_type);

        match resp.status {
            304 => {
                // "Keep old entry, update timestamp"
                let c = CachedResult {
                    discovery: cached.map(|c| c.discovery).unwrap_or_default(),
                    etag,
                    last_updated: now,
                };
                self.cached_results
                    .lock()
                    .unwrap()
                    .insert(info.service.clone(), c.clone());
                (Some(c), None)
            }
            503 => (
                None,
                Some(format!(
                    "service {}/{}:{} returned non-success response code: 503",
                    info.service.namespace, info.service.name, info.service.port
                )),
            ),
            200 if is_aggregated => match parse_discovery_list(&resp.body) {
                Err(e) => (None, Some(e)),
                Ok(discovery) => {
                    let c = CachedResult {
                        discovery,
                        etag: resp.etag,
                        last_updated: now,
                    };
                    self.cached_results
                        .lock()
                        .unwrap()
                        .insert(info.service.clone(), c.clone());
                    (Some(c), None)
                }
            },
            _ => {
                // "Could not get acceptable response for Aggregated Discovery.
                // Fall back to legacy discovery information"
                if version.is_empty() {
                    return (None, Some("not found".to_string()));
                }
                let path = legacy_path(group, version);
                let req = HandlerRequest {
                    path: path.clone(),
                    accept: "application/json".to_string(),
                    if_none_match: etag,
                    user: AGGREGATOR_USER.to_string(),
                    groups: Vec::new(),
                };
                let resp = serve_with_timeout(self.handler.as_ref(), group, version, req).await;
                if resp.status != 200 {
                    return (
                        None,
                        Some(format!(
                            "failed to download legacy discovery for {path}: {}",
                            String::from_utf8_lossy(&resp.body)
                        )),
                    );
                }
                match parse_legacy_resource_list(group, version, &resp.body) {
                    Err(e) => (None, Some(e)),
                    // "Do not save the resolve as the legacy fallback only
                    // fetches one group version and an API Service may serve
                    // multiple group versions."
                    Ok(discovery) => (
                        Some(CachedResult {
                            discovery,
                            etag: None,
                            last_updated: now,
                        }),
                        None,
                    ),
                }
            }
        }
    }

    /// `syncAPIService`: try to sync a single APIService.
    pub async fn sync_api_service(&self, api_service_name: &str) -> Result<(), String> {
        let info = self
            .api_services
            .lock()
            .unwrap()
            .get(api_service_name)
            .cloned();
        let (group, version) = api_service_name_to_group_version(api_service_name);
        let Some(info) = info else {
            // apiservice was removed. remove it from merged discovery
            self.merged.remove_group_version(&group, &version);
            return Ok(());
        };

        let (cached, err) = self
            .fetch_fresh_discovery_for_service(&group, &version, &info)
            .await;
        // "Just use empty GV to mark that GV exists, but no resources" when
        // nothing was fetched, or the server did not include this GV.
        let mut entry = cached
            .and_then(|c| c.discovery.get(&(group.clone(), version.clone())).cloned())
            .unwrap_or(VersionDiscovery {
                version: version.clone(),
                resources: Vec::new(),
                freshness: DiscoveryFreshness::Current,
            });
        entry.freshness = if err.is_none() {
            DiscoveryFreshness::Current
        } else {
            DiscoveryFreshness::Stale
        };
        self.merged.add_group_version(&group, entry);
        self.merged.set_group_version_priority(
            &group,
            &version,
            info.group_priority,
            info.version_priority,
        );
        Ok(())
    }

    /// `Run`: make every known APIService present (the unreachable ones
    /// `Stale`), close `discovery_synced`, then work the dirty queue (2
    /// workers) and re-dirty everything each minute until `stop` fires.
    pub async fn run(
        self: Arc<Self>,
        mut stop: watch::Receiver<bool>,
        discovery_synced: Option<watch::Sender<bool>>,
    ) {
        tracing::info!("Starting ResourceDiscoveryManager");
        // "Ensure that apiregistration.k8s.io is the first group in the
        // discovery group."
        self.merged.set_group_version_priority(
            "apiregistration.k8s.io",
            "v1",
            API_REGISTRATION_GROUP_PRIORITY,
            0,
        );

        // "Ensure that all APIServices are present before readiness check
        // succeeds": sync a copy of the keys, concurrently.
        let keys: Vec<String> = self.api_services.lock().unwrap().keys().cloned().collect();
        futures::future::join_all(keys.iter().map(|k| async {
            // "If an error was returned, the APIService will still have been
            // added but marked as stale. Ignore the return value here"
            let _ = self.sync_api_service(k).await;
        }))
        .await;
        if let Some(tx) = discovery_synced {
            tx.send_replace(true);
        }

        let mut workers = Vec::new();
        for _ in 0..2 {
            let dm = self.clone();
            workers.push(tokio::spawn(async move {
                while let Some(next) = dm.dirty_api_service_queue.get().await {
                    let result = dm.sync_api_service(&next).await;
                    dm.dirty_api_service_queue.done(&next);
                    match result {
                        Ok(()) => dm.rate_limiter.lock().unwrap().forget(&next),
                        Err(_) => {
                            let delay = dm
                                .rate_limiter
                                .lock()
                                .unwrap()
                                .when_at(&next, tokio::time::Instant::now());
                            let dm2 = dm.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                dm2.dirty_api_service_queue.add(next);
                            });
                        }
                    }
                }
            }));
        }

        // `wait.PollUntil(1*time.Minute, ...)`: "Mark all non-local
        // APIServices as dirty".
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                _ = stop.changed() => {}
            }
            if *stop.borrow() {
                break;
            }
            let now = Instant::now();
            let mut services = self.api_services.lock().unwrap();
            for (key, info) in services.iter_mut() {
                info.last_marked_dirty = now;
                self.dirty_api_service_queue.add(key.clone());
            }
        }
        self.dirty_api_service_queue.shut_down();
        for w in workers {
            let _ = w.await;
        }
    }

    /// Whether the dirty queue has drained (test hook; upstream's
    /// `waitForQueueComplete`).
    #[cfg(test)]
    async fn wait_for_queue_complete(&self) {
        for _ in 0..500 {
            if self.dirty_api_service_queue.is_idle() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("queue never drained");
    }
}

// ---------------------------------------------------------------------------
// AggregatorHandlerManager (APIAggregator.AddAPIService / RemoveAPIService)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct AggregatorState {
    /// `proxyHandlers`: the APIServices that have a handler installed.
    proxy_handlers: HashMap<String, APIService>,
    /// `handledGroupVersions`.
    handled_group_versions: HashMap<String, BTreeSet<String>>,
}

/// The `APIAggregator`'s `APIHandlerManager` implementation
/// (apiserver.go:`AddAPIService`/`RemoveAPIService`). "It is not thread-safe,
/// so only call it on one thread at a time please" -- the registration
/// controller's single worker is that thread; the mutex only guards readers.
pub struct AggregatorHandlerManager {
    state: Mutex<AggregatorState>,
    discovery_aggregation_controller: Option<Arc<DiscoveryManager>>,
}

impl AggregatorHandlerManager {
    pub fn new(discovery_aggregation_controller: Option<Arc<DiscoveryManager>>) -> Self {
        Self {
            state: Mutex::new(AggregatorState::default()),
            discovery_aggregation_controller,
        }
    }

    pub fn registered(&self) -> BTreeSet<String> {
        self.state
            .lock()
            .unwrap()
            .proxy_handlers
            .keys()
            .cloned()
            .collect()
    }

    pub fn handled_group_versions(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.state
            .lock()
            .unwrap()
            .handled_group_versions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl APIHandlerManager for AggregatorHandlerManager {
    fn add_api_service(&self, api_service: &APIService) -> Result<(), String> {
        let mut st = self.state.lock().unwrap();
        let exists = st.proxy_handlers.contains_key(&api_service.metadata.name);
        // "Forward calls to discovery manager to update discovery document"
        if let Some(dm) = &self.discovery_aggregation_controller {
            dm.add_api_service(api_service);
        }
        st.proxy_handlers
            .insert(api_service.metadata.name.clone(), api_service.clone());
        if exists {
            return Ok(());
        }
        // "if we're dealing with the legacy group, we're done here"
        if api_service.metadata.name == LEGACY_API_SERVICE_NAME {
            return Ok(());
        }
        // Register the group version; the first one for a group is "time to
        // register the group aggregation endpoint".
        st.handled_group_versions
            .entry(api_service.spec.group.clone())
            .or_default()
            .insert(api_service.spec.version.clone());
        Ok(())
    }

    fn remove_api_service(&self, api_service_name: &str) {
        // "Forward calls to discovery manager to update discovery document"
        if let Some(dm) = &self.discovery_aggregation_controller {
            dm.remove_api_service(api_service_name);
        }
        let (group, version) = api_service_name_to_group_version(api_service_name);
        let mut st = self.state.lock().unwrap();
        st.proxy_handlers.remove(api_service_name);
        let Some(versions) = st.handled_group_versions.get_mut(&group) else {
            return;
        };
        versions.remove(&version);
        if versions.is_empty() {
            st.handled_group_versions.remove(&group);
        }
    }
}

// ---------------------------------------------------------------------------
// The hooks
// ---------------------------------------------------------------------------

/// Register `apiservice-registration-controller` and
/// `apiservice-discovery-controller` on `registry` (apiserver.go:368-420):
///
/// - the registration hook starts the controller, then returns once it has
///   "installed" all known APIServices (`apiServiceRegistrationControllerInitiated`);
/// - the discovery hook "depends on the apiservice registration controller
///   having the full list of APIServices already synced": it blocks on that
///   signal, runs the discovery manager, and returns once every APIService is
///   present in the merged document.
///
/// `stop` is `context.Done()`.
pub fn register_apiservice_hooks_with(
    registry: &PostStartHooks,
    controller: Arc<APIServiceRegistrationController>,
    discovery: Arc<DiscoveryManager>,
    stop: watch::Receiver<bool>,
) {
    // `apiServiceRegistrationControllerInitiated`
    let (initiated_tx, initiated_rx) = watch::channel(false);

    let stop_reg = stop.clone();
    let mut initiated = initiated_rx.clone();
    spawn_hook_with(
        registry,
        APISERVICE_REGISTRATION_HOOK,
        async move {
            // `go apiserviceRegistrationController.Run(context.Done(), initiated)`
            tokio::spawn(controller.run(stop_reg.clone(), initiated_tx));
            let mut stop = stop_reg;
            tokio::select! {
                _ = stop.wait_for(|s| *s) => {}
                _ = initiated.wait_for(|i| *i) => {}
            }
            Ok::<(), std::convert::Infallible>(())
        },
        |_, e| match *e {},
    );

    let mut initiated = initiated_rx;
    let mut stop_disc = stop;
    spawn_hook_with(
        registry,
        APISERVICE_DISCOVERY_HOOK,
        async move {
            tokio::select! {
                _ = stop_disc.wait_for(|s| *s) => return Ok(()),
                _ = initiated.wait_for(|i| *i) => {}
            }
            // `go s.discoveryAggregationController.Run(context.Done(), discoverySyncedCh)`
            let (synced_tx, mut synced_rx) = watch::channel(false);
            tokio::spawn(discovery.run(stop_disc.clone(), Some(synced_tx)));
            tokio::select! {
                _ = stop_disc.wait_for(|s| *s) => {}
                _ = synced_rx.wait_for(|s| *s) => {}
            }
            Ok::<(), std::convert::Infallible>(())
        },
        |_, e| match *e {},
    );
}

/// The production wiring over `storage`, on the global hook registry. The
/// discovery manager is created first and handed to the handler manager, as
/// upstream does before `NewAPIServiceRegistrationController` runs.
pub fn spawn_apiservice_registration_hooks<S: Storage + 'static>(storage: Arc<S>) {
    let discovery = DiscoveryManager::new(
        merged_discovery().clone(),
        Arc::new(ProxyDiscoveryHandler::new(storage.clone())),
    );
    let manager = Arc::new(AggregatorHandlerManager::new(Some(discovery.clone())));
    let controller = Arc::new(APIServiceRegistrationController::new(
        Arc::new(StorageAPIServiceClient(storage.clone())),
        manager,
    ));
    // `context.Done()` never fires for the process lifetime.
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _keep = stop_tx;
        std::future::pending::<()>().await
    });
    let c = controller.clone();
    tokio::spawn(async move { spawn_apiservice_event_handlers(storage, c).await });
    register_apiservice_hooks_with(
        crate::post_start_hooks::global(),
        controller,
        discovery,
        stop_rx,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregated_discovery::HandlerResponse;
    use crate::autoregister::ClientError;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn svc(name: &str, with_service: bool) -> APIService {
        let (group, version) = api_service_name_to_group_version(name);
        let mut s = APIService::default();
        s.metadata.name = name.to_string();
        s.spec.group = group;
        s.spec.version = version;
        if with_service {
            s.spec.service = Some(rusternetes_common::resources::APIServiceReference {
                namespace: "ns".into(),
                name: "backend".into(),
                port: None,
            });
        }
        s
    }

    #[derive(Default)]
    struct FakeLister {
        present: Mutex<BTreeMap<String, APIService>>,
        failing: AtomicBool,
    }
    #[async_trait]
    impl APIServiceClient for FakeLister {
        async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError> {
            if self.failing.load(Ordering::SeqCst) {
                return Err(ClientError::Other("down".into()));
            }
            Ok(self.present.lock().unwrap().get(name).cloned())
        }
        async fn list_names(&self) -> Result<Vec<String>, ClientError> {
            if self.failing.load(Ordering::SeqCst) {
                return Err(ClientError::Other("down".into()));
            }
            Ok(self.present.lock().unwrap().keys().cloned().collect())
        }
        async fn create(&self, _: &APIService) -> Result<(), ClientError> {
            unreachable!()
        }
        async fn update(&self, _: &APIService) -> Result<(), ClientError> {
            unreachable!()
        }
        async fn delete(&self, _: &str, _: &str) -> Result<(), ClientError> {
            unreachable!()
        }
    }

    fn lister_with(names: &[&str]) -> Arc<FakeLister> {
        let l = Arc::new(FakeLister::default());
        for n in names {
            l.present
                .lock()
                .unwrap()
                .insert(n.to_string(), svc(n, true));
        }
        l
    }

    fn controller_over(
        lister: Arc<FakeLister>,
    ) -> (
        Arc<APIServiceRegistrationController>,
        Arc<AggregatorHandlerManager>,
        Arc<DiscoveryManager>,
        Arc<MergedDiscovery>,
    ) {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(merged.clone(), FnHandler::status(503));
        let mgr = Arc::new(AggregatorHandlerManager::new(Some(dm.clone())));
        let c = Arc::new(APIServiceRegistrationController::new(lister, mgr.clone()));
        (c, mgr, dm, merged)
    }

    /// `sync`: a present APIService is added, an absent one removed.
    #[tokio::test]
    async fn sync_adds_present_and_removes_absent() {
        let lister = lister_with(&["v1.apps.example.com"]);
        let (c, mgr, _, _) = controller_over(lister.clone());
        c.sync("v1.apps.example.com").await.unwrap();
        assert_eq!(
            mgr.registered(),
            BTreeSet::from(["v1.apps.example.com".to_string()])
        );
        lister.present.lock().unwrap().clear();
        c.sync("v1.apps.example.com").await.unwrap();
        assert!(mgr.registered().is_empty());
        assert!(mgr.handled_group_versions().is_empty());
    }

    /// `Run`: nothing is signalled until the initial sync succeeds; it retries.
    #[tokio::test(start_paused = true)]
    async fn run_signals_only_after_every_apiservice_is_installed() {
        let lister = lister_with(&["v1.a.example.com", "v1beta1.a.example.com"]);
        lister.failing.store(true, Ordering::SeqCst);
        let (c, mgr, _, _) = controller_over(lister.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (synced_tx, synced_rx) = watch::channel(false);
        tokio::spawn(c.run(stop_rx, synced_tx));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!*synced_rx.borrow(), "must not signal while listing fails");
        assert!(mgr.registered().is_empty());
        lister.failing.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(*synced_rx.borrow());
        assert_eq!(mgr.registered().len(), 2);
        assert_eq!(
            mgr.handled_group_versions()["a.example.com"],
            BTreeSet::from(["v1".to_string(), "v1beta1".to_string()])
        );
    }

    /// An informer event after the initial sync is processed by the worker.
    #[tokio::test]
    async fn worker_applies_informer_events() {
        let lister = lister_with(&[]);
        let (c, mgr, _, _) = controller_over(lister.clone());
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (synced_tx, mut synced_rx) = watch::channel(false);
        tokio::spawn(c.clone().run(stop_rx, synced_tx));
        synced_rx.wait_for(|s| *s).await.unwrap();
        let s = svc("v1.late.example.com", true);
        lister
            .present
            .lock()
            .unwrap()
            .insert(s.metadata.name.clone(), s.clone());
        c.on_add(&s);
        for _ in 0..200 {
            if !mgr.registered().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(mgr.registered().len(), 1);
        lister.present.lock().unwrap().clear();
        c.on_delete(&s);
        for _ in 0..200 {
            if mgr.registered().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(mgr.registered().is_empty());
    }

    /// `TestInitialRunHasAllAPIServices`: when discovery is ready, every
    /// APIService is present, and one that could not sync is `Stale`.
    #[tokio::test]
    async fn discovery_initial_run_has_all_apiservices_stale() {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(merged.clone(), FnHandler::status(503));
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (tx, mut rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, Some(tx)));
        rx.wait_for(|s| *s).await.unwrap();
        let groups = merged.groups();
        assert_eq!(groups.len(), 1);
        let v = &groups["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Stale);
        // apiregistration.k8s.io sorts first.
        assert_eq!(
            merged.priority("apiregistration.k8s.io", "v1"),
            Some((API_REGISTRATION_GROUP_PRIORITY, 0))
        );
    }

    fn v2_doc(groups: &[(&str, &[&str])]) -> serde_json::Value {
        serde_json::json!({
            "kind": "APIGroupDiscoveryList",
            "apiVersion": "apidiscovery.k8s.io/v2",
            "items": groups.iter().map(|(g, vs)| serde_json::json!({
                "metadata": {"name": g},
                "versions": vs.iter().map(|v| serde_json::json!({
                    "version": v,
                    "freshness": "Current",
                    "resources": [{
                        "resource": "foos",
                        "responseKind": {"group": g, "version": v, "kind": "Foo"},
                        "scope": "Cluster",
                        "verbs": ["get"],
                    }],
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
    }

    fn ok(content_type: &str, body: &serde_json::Value, etag: Option<&str>) -> HandlerResponse {
        HandlerResponse {
            status: 200,
            content_type: Some(content_type.to_string()),
            etag: etag.map(str::to_string),
            body: body.to_string().into_bytes(),
        }
    }

    /// A closure-backed [`AggregatorHandler`] that records every request.
    struct FnHandler {
        f: Box<dyn Fn(&HandlerRequest) -> HandlerResponse + Send + Sync>,
        delay: Duration,
        seen: Mutex<Vec<HandlerRequest>>,
    }
    impl FnHandler {
        fn new(
            f: impl Fn(&HandlerRequest) -> HandlerResponse + Send + Sync + 'static,
        ) -> Arc<Self> {
            Self::slow(Duration::ZERO, f)
        }
        fn slow(
            delay: Duration,
            f: impl Fn(&HandlerRequest) -> HandlerResponse + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                f: Box::new(f),
                delay,
                seen: Mutex::new(Vec::new()),
            })
        }
        fn status(code: u16) -> Arc<Self> {
            Self::new(move |_| HandlerResponse {
                status: code,
                ..Default::default()
            })
        }
        fn paths(&self) -> Vec<String> {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.path.clone())
                .collect()
        }
    }
    #[async_trait]
    impl AggregatorHandler for FnHandler {
        async fn serve(&self, _: &str, _: &str, req: HandlerRequest) -> HandlerResponse {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let r = (self.f)(&req);
            self.seen.lock().unwrap().push(req);
            r
        }
    }

    const V2_CT: &str = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList";
    const V2BETA1_CT: &str =
        "application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList";

    async fn run_to_idle(dm: &Arc<DiscoveryManager>) -> watch::Sender<bool> {
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, None));
        dm.wait_for_queue_complete().await;
        stop_tx
    }

    /// `TestWaitForSync`: `Run` blocks on the slow backend before signalling,
    /// and a reachable one is `Current` with its resources.
    #[tokio::test]
    async fn discovery_run_waits_for_slow_backend() {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(
            merged.clone(),
            FnHandler::slow(Duration::from_millis(300), |_| {
                ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None)
            }),
        );
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (tx, rx) = watch::channel(false);
        let mut rx2 = rx.clone();
        tokio::spawn(dm.clone().run(stop_rx, Some(tx)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!*rx.borrow(), "signalled before the backend answered");
        rx2.wait_for(|s| *s).await.unwrap();
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Current);
        assert_eq!(v.resources.len(), 1);
    }

    /// `TestRemoveAPIService`: removed APIServices leave no group behind.
    #[tokio::test]
    async fn discovery_remove_api_service_drops_group() {
        let merged = Arc::new(MergedDiscovery::default());
        let dm = DiscoveryManager::new(merged.clone(), FnHandler::status(503));
        let a = svc("v1.stable.example.com", true);
        let b = svc("v2.stable.example.com", true);
        dm.add_api_service(&a);
        dm.add_api_service(&b);
        let (_stop_tx, stop_rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, None));
        dm.wait_for_queue_complete().await;
        assert_eq!(merged.groups()["stable.example.com"].len(), 2);
        dm.remove_api_service("v1.stable.example.com");
        dm.remove_api_service("v2.stable.example.com");
        dm.wait_for_queue_complete().await;
        assert!(merged.groups().is_empty(), "{:?}", merged.groups());
    }

    /// A local APIService (no `spec.service`) is not tracked by the manager.
    #[test]
    fn local_apiservice_is_not_tracked_by_discovery() {
        let dm =
            DiscoveryManager::new(Arc::new(MergedDiscovery::default()), FnHandler::status(503));
        dm.add_api_service(&svc("v1.apps", false));
        assert!(dm.api_services.lock().unwrap().is_empty());
    }

    /// apiserver.go:381-419: the discovery hook blocks on
    /// `apiServiceRegistrationControllerInitiated`; neither hook finishes until
    /// the registration controller has installed every APIService, then both do.
    #[tokio::test(start_paused = true)]
    async fn hooks_finish_only_after_registration_is_initiated() {
        let lister = lister_with(&["v1.a.example.com"]);
        lister.failing.store(true, Ordering::SeqCst);
        let (c, mgr, dm, merged) = controller_over(lister.clone());
        let reg = PostStartHooks::new();
        let (_stop_tx, stop_rx) = watch::channel(false);
        register_apiservice_hooks_with(&reg, c, dm, stop_rx);
        assert_eq!(
            reg.checks()
                .iter()
                .map(|c| c.0.clone())
                .collect::<Vec<_>>()
                .len(),
            2
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            reg.check(APISERVICE_REGISTRATION_HOOK),
            Some(Err("not finished"))
        );
        assert_eq!(
            reg.check(APISERVICE_DISCOVERY_HOOK),
            Some(Err("not finished"))
        );
        assert!(merged.groups().is_empty());

        lister.failing.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(reg.check(APISERVICE_REGISTRATION_HOOK), Some(Ok(())));
        assert_eq!(reg.check(APISERVICE_DISCOVERY_HOOK), Some(Ok(())));
        assert_eq!(mgr.registered().len(), 1);
        // The discovery hook returns only once every APIService is in the
        // merged document.
        assert!(merged.groups().contains_key("a.example.com"));
    }

    /// `TestBasic`: one backend serving several groups feeds every APIService
    /// that points at it; each GV gets only its own entry, `Current`.
    #[tokio::test]
    async fn discovery_basic_merges_groups_from_backend() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|r| {
            assert_eq!(r.path, "/apis");
            assert_eq!(r.user, AGGREGATOR_USER);
            assert_eq!(r.groups, vec![AGGREGATOR_GROUP.to_string()]);
            assert_eq!(r.accept, format!("{ACCEPT_V2},{ACCEPT_V2_BETA1}"));
            ok(
                V2_CT,
                &v2_doc(&[("a.example.com", &["v1", "v2"]), ("b.example.com", &["v1"])]),
                Some("\"e1\""),
            )
        });
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        for n in ["v1.a.example.com", "v2.a.example.com", "v1.b.example.com"] {
            dm.add_api_service(&svc(n, true));
        }
        let _stop = run_to_idle(&dm).await;
        let groups = merged.groups();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups["a.example.com"].len(), 2);
        for vs in groups.values() {
            for v in vs.values() {
                assert_eq!(v.freshness, DiscoveryFreshness::Current);
                assert_eq!(v.resources.len(), 1);
            }
        }
    }

    /// `TestServiceGC`: re-pointing an APIService at another service purges the
    /// cache entry of the old one.
    #[tokio::test]
    async fn discovery_service_gc_purges_unused_cache_entries() {
        let h = FnHandler::new(|_| ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None));
        let dm = DiscoveryManager::new(Arc::new(MergedDiscovery::default()), h);
        let _stop = run_to_idle(&dm).await;
        dm.add_api_service(&svc("v1.stable.example.com", true));
        dm.wait_for_queue_complete().await;
        assert_eq!(dm.cached_results.lock().unwrap().len(), 1);
        let mut changed = svc("v1.stable.example.com", true);
        changed.spec.service.as_mut().unwrap().name = "backend-changed".into();
        dm.add_api_service(&changed);
        dm.wait_for_queue_complete().await;
        let cache = dm.cached_results.lock().unwrap();
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.keys().next().unwrap().name, "backend-changed");
    }

    /// `TestV2Beta1Skew`: a backend that only serves v2beta1 is still read,
    /// and nil `responseKind`s are defaulted.
    #[tokio::test]
    async fn discovery_accepts_v2beta1_backend() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|_| {
            let mut d = v2_doc(&[("stable.example.com", &["v1"])]);
            d["apiVersion"] = "apidiscovery.k8s.io/v2beta1".into();
            d["items"][0]["versions"][0]["resources"][0]["responseKind"] = serde_json::Value::Null;
            ok(V2BETA1_CT, &d, None)
        });
        let dm = DiscoveryManager::new(merged.clone(), h);
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Current);
        assert_eq!(v.resources[0]["responseKind"], serde_json::json!({}));
    }

    /// `TestDirty`: the handler is pinged for a newly added APIService.
    #[tokio::test]
    async fn discovery_pings_handler_for_dirty_apiservice() {
        let h = FnHandler::new(|_| ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None));
        let dm = DiscoveryManager::new(Arc::new(MergedDiscovery::default()), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        assert_eq!(h.paths(), vec!["/apis".to_string()]);
    }

    fn legacy_handler(root: HandlerResponse) -> Arc<FnHandler> {
        FnHandler::new(move |r| match r.path.as_str() {
            "/apis" => root.clone(),
            "/apis/stable.example.com/v1" => HandlerResponse {
                status: 200,
                content_type: Some("application/json".into()),
                etag: None,
                body: serde_json::json!({
                    "kind": "APIResourceList",
                    "groupVersion": "stable.example.com/v1",
                    "resources": [{
                        "name": "foos", "singularName": "foo", "namespaced": false,
                        "group": "stable.example.com", "version": "v1", "kind": "Foo",
                        "verbs": ["get", "list"], "categories": ["all"],
                    }],
                })
                .to_string()
                .into_bytes(),
            },
            p => panic!("unexpected request sent to {p}"),
        })
    }

    /// `TestLegacyFallback`: every kind of unacceptable root response falls
    /// back to `/apis/{group}/{version}`; the result is `Current`, converted,
    /// and NOT cached (`TestLegacyFallbackNoCache`).
    #[tokio::test]
    async fn discovery_legacy_fallback_for_unacceptable_root() {
        let roots = [
            HandlerResponse {
                status: 406,
                ..Default::default()
            },
            HandlerResponse {
                status: 404,
                ..Default::default()
            },
            HandlerResponse {
                status: 200,
                content_type: Some("application/json".into()),
                ..Default::default()
            },
            HandlerResponse {
                status: 200,
                content_type: Some(
                    "application/json;g=apidiscovery.k8s.io;v=v1alpha1;as=APIGroupDiscoveryList"
                        .into(),
                ),
                ..Default::default()
            },
        ];
        for root in roots {
            let merged = Arc::new(MergedDiscovery::default());
            let dm = DiscoveryManager::new(merged.clone(), legacy_handler(root));
            dm.add_api_service(&svc("v1.stable.example.com", true));
            let _stop = run_to_idle(&dm).await;
            let v = &merged.groups()["stable.example.com"]["v1"];
            assert_eq!(v.freshness, DiscoveryFreshness::Current);
            assert_eq!(v.resources.len(), 1);
            assert_eq!(v.resources[0]["resource"], "foos");
            assert_eq!(v.resources[0]["scope"], "Cluster");
            assert_eq!(v.resources[0]["singularResource"], "foo");
            assert_eq!(v.resources[0]["categories"], serde_json::json!(["all"]));
            assert_eq!(v.resources[0]["responseKind"]["kind"], "Foo");
            assert!(dm.cached_results.lock().unwrap().is_empty());
        }
    }

    /// A legacy failure marks the GV `Stale`.
    #[tokio::test]
    async fn discovery_legacy_failure_is_stale() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|_| HandlerResponse {
            status: 404,
            ..Default::default()
        });
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Stale);
        assert!(v.resources.is_empty());
        assert_eq!(
            h.paths()[..2],
            [
                "/apis".to_string(),
                "/apis/stable.example.com/v1".to_string()
            ]
        );
    }

    /// `TestAPIServiceStale`: a 503 backend is listed `Stale` with no resources.
    #[tokio::test]
    async fn discovery_503_backend_is_stale_and_empty() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::status(503);
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Stale);
        assert!(v.resources.is_empty());
        // 503 is a hard error: no legacy fallback request.
        // (Run's initial sync and the dirty queue each try once.)
        assert!(h.paths().iter().all(|p| p == "/apis"), "{:?}", h.paths());
    }

    /// `http.TimeoutHandler(..., 5s)`: a backend that never answers is `Stale`.
    #[tokio::test(start_paused = true)]
    async fn discovery_timeout_is_stale() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::slow(Duration::from_secs(60), |_| {
            ok(V2_CT, &v2_doc(&[("stable.example.com", &["v1"])]), None)
        });
        let dm = DiscoveryManager::new(merged.clone(), h);
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let (_stop_tx, stop_rx) = watch::channel(false);
        let (tx, mut rx) = watch::channel(false);
        tokio::spawn(dm.clone().run(stop_rx, Some(tx)));
        rx.wait_for(|s| *s).await.unwrap();
        assert_eq!(
            merged.groups()["stable.example.com"]["v1"].freshness,
            DiscoveryFreshness::Stale
        );
    }

    /// `TestNotModified`: once an etag is cached, a re-sync after the APIService
    /// is marked dirty sends `If-None-Match`; a 304 keeps the cached resources.
    #[tokio::test]
    async fn discovery_not_modified_keeps_cached_document() {
        let merged = Arc::new(MergedDiscovery::default());
        let h = FnHandler::new(|r| match &r.if_none_match {
            Some(e) if e == "\"e1\"" => HandlerResponse {
                status: 304,
                ..Default::default()
            },
            Some(e) => panic!("unexpected etag {e}"),
            None => ok(
                V2_CT,
                &v2_doc(&[("stable.example.com", &["v1"])]),
                Some("\"e1\""),
            ),
        });
        let dm = DiscoveryManager::new(merged.clone(), h.clone());
        dm.add_api_service(&svc("v1.stable.example.com", true));
        let _stop = run_to_idle(&dm).await;
        assert_eq!(h.seen.lock().unwrap().len(), 1);
        // Mark dirty again (as the minute poll / an APIService update does).
        dm.add_api_service(&svc("v1.stable.example.com", true));
        dm.wait_for_queue_complete().await;
        {
            let seen = h.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[1].if_none_match.as_deref(), Some("\"e1\""));
        }
        let v = &merged.groups()["stable.example.com"]["v1"];
        assert_eq!(v.freshness, DiscoveryFreshness::Current);
        assert_eq!(v.resources.len(), 1);
    }

    /// The cache rule: a result fetched after the APIService was marked dirty is
    /// reused, so syncing again does not contact the backend.
    #[tokio::test]
    async fn discovery_fresh_cache_is_not_refetched() {
        let h = FnHandler::new(|_| {
            ok(
                V2_CT,
                &v2_doc(&[("a.example.com", &["v1"]), ("b.example.com", &["v1"])]),
                None,
            )
        });
        let dm = DiscoveryManager::new(Arc::new(MergedDiscovery::default()), h.clone());
        dm.add_api_service(&svc("v1.a.example.com", true));
        let _stop = run_to_idle(&dm).await;
        let n = h.seen.lock().unwrap().len();
        dm.sync_api_service("v1.a.example.com").await.unwrap();
        assert_eq!(h.seen.lock().unwrap().len(), n);
    }
}
