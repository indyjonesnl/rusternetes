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
//! - There is no `NonGoRestfulMux` to mount handlers on: `/apis/<group>/...`
//!   is dispatched by `router::custom_resource_fallback`.
//!   [`AggregatorHandlerManager`] owns the per-APIService
//!   [`ProxyHandler`]s (`proxyHandlers`) and the `handledGroupVersions`
//!   bookkeeping that gates the per-group `apiGroupHandler`, and that
//!   fallback consults it ([`crate::aggregator_mux::active_handler_manager`])
//!   instead of reading the APIService from storage. "Mounting" is insertion,
//!   `Unregister` is removal. The per-group handler's lister is the set of
//!   APIServices the registration controller has processed (the informer
//!   cache's equivalent here). Still open: the openapi / openapiv3
//!   aggregation controllers' `AddAPIService`/`UpdateAPIService`/
//!   `RemoveAPIService` calls, and the `/api` proxy path of the legacy `v1.`
//!   APIService (the built-in `/api` routes serve it).
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
use crate::aggregator_mux::{
    install_handler_manager, serve_api_group, ApiGroupResponse, ProxyHandler,
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
pub fn api_service_name_to_group_version(name: &str) -> (String, String) {
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
pub struct GroupVersionInfo {
    /// "Guaranteed to be a time greater than the most recent time the
    /// APIService was known to be modified."
    last_marked_dirty: Instant,
    service: ServiceKey,
    group_priority: i32,
    version_priority: i32,
}

#[derive(Clone)]
pub struct CachedResult {
    discovery: BTreeMap<(String, String), VersionDiscovery>,
    /// ETag of the cached discovery document.
    etag: Option<String>,
    /// "Guaranteed to be a time less than the time the server responded".
    last_updated: Instant,
}

/// `discoveryManager` (handler_discovery.go:67-110).
pub struct DiscoveryManager {
    pub merged: Arc<MergedDiscovery>,
    pub handler: Arc<dyn AggregatorHandler>,
    pub api_services: Mutex<HashMap<String, GroupVersionInfo>>,
    pub cached_results: Mutex<HashMap<ServiceKey, CachedResult>>,
    pub dirty_api_service_queue: WorkQueue<String>,
    pub rate_limiter: Mutex<ControllerRateLimiter>,
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
    pub async fn wait_for_queue_complete(&self) {
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
    /// `proxyHandlers`: the APIServices that have a handler installed, with
    /// the latest object the handler was told about (the group handler's
    /// lister view).
    proxy_handlers: HashMap<String, (Arc<ProxyHandler>, APIService)>,
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

    /// The `proxyHandler` mounted at `/apis/<group>/<version>` (the mux
    /// lookup), or `None` when nothing is mounted there.
    pub fn proxy_handler(&self, group: &str, version: &str) -> Option<Arc<ProxyHandler>> {
        self.state
            .lock()
            .unwrap()
            .proxy_handlers
            .get(&format!("{version}.{group}"))
            .map(|(h, _)| h.clone())
    }

    /// `apiGroupHandler.ServeHTTP` for `/apis/<group>`, or `None` when no
    /// group handler is mounted for `group` (`handledGroupVersions`).
    pub fn serve_api_group(&self, group: &str) -> Option<ApiGroupResponse> {
        let st = self.state.lock().unwrap();
        if !st.handled_group_versions.contains_key(group) {
            return None;
        }
        let services: Vec<APIService> =
            st.proxy_handlers.values().map(|(_, a)| a.clone()).collect();
        Some(serve_api_group(group, &services))
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
        // "if the proxyHandler already exists, it needs to be updated. The
        // aggregation bits do not since they are wired against listers"
        if let Some((handler, stored)) = st.proxy_handlers.get_mut(&api_service.metadata.name) {
            handler.update_api_service(api_service);
            *stored = api_service.clone();
            // "Forward calls to discovery manager to update discovery document"
            if let Some(dm) = &self.discovery_aggregation_controller {
                dm.add_api_service(api_service);
            }
            return Ok(());
        }
        // "register the proxy handler"
        let handler = Arc::new(ProxyHandler::new());
        handler.update_api_service(api_service);
        if let Some(dm) = &self.discovery_aggregation_controller {
            dm.add_api_service(api_service);
        }
        st.proxy_handlers.insert(
            api_service.metadata.name.clone(),
            (handler, api_service.clone()),
        );
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
    // The request path's "mux": from here on aggregated requests are routed by
    // the handlers this controller installs.
    install_handler_manager(manager.clone());
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
