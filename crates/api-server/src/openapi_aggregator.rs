//! The kube-aggregator `apiservice-openapi-controller`: it downloads the
//! OpenAPI v2 document of every aggregated APIService and merges them into the
//! document `/openapi/v2` serves.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/controllers/openapi/`:
//! - `controller.go`: `AggregationController` (`NewAggregationController`,
//!   `Run`, `processNextWorkItem`, `sync`, `AddAPIService`, `UpdateAPIService`,
//!   `RemoveAPIService`) -> [`AggregationController`];
//! - `aggregator/downloader.go`: `Downloader.Download`, `cacheableDownloader`
//!   -> [`Downloader`], [`CacheableDownloader`];
//! - `aggregator/aggregator.go`: `specAggregator` (`AddUpdateAPIService`,
//!   `UpdateAPIServiceSpec`, `RemoveAPIService`, `updateServiceLocked`,
//!   `buildMergeSpecLocked`) -> [`SpecAggregator`];
//! - `aggregator/priority.go`: `sortByPriority` -> [`sort_by_priority`];
//! - `apiserver.go:469-474,:486-498,:528-530,:559-561,:617-619`: the
//!   `apiservice-openapi-controller` post-start hook and the
//!   Add/Update/RemoveAPIService calls
//!   ([`crate::apiservice_registration::AggregatorHandlerManager`]).
//!
//! Deviations (each a deliberate scope decision, not an equivalent):
//! - The built-in documents (`buildAndRegisterSpecAggregatorForLocalServices`'s
//!   aggregator spec and delegation chain) are built per request by
//!   `handlers::openapi::get_swagger_spec`; [`SpecAggregator::merged_spec`]
//!   takes that document as the destination of the merge (upstream merges its
//!   local specs into an empty one, and `mergeSpecs` skips a source without
//!   paths together with all its definitions; the built-in document's
//!   definitions are not derived from its paths, so merging it that way would
//!   drop them).
//! - `cached.MergeList`'s etag-keyed cache is not kept: the merge runs per
//!   request, over the caches captured at the last Add/Remove
//!   (`buildMergeSpecLocked`). The result is the same document.
//! - `UpdateAPIService` downloads synchronously upstream; the handler manager
//!   here is not async, so the update runs on a spawned task.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use rusternetes_common::resources::APIService;
use serde_json::Value;
use sha2::{Digest, Sha512};
use tokio::sync::watch;

use crate::aggregated_discovery::{HandlerRequest, HandlerResponse};
use crate::aggregator_mux::ProxyHandler;
use crate::crdregistration::WorkQueue;
use crate::kube_openapi::{
    filter_spec_by_paths_without_side_effects,
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters,
};
use crate::openapiv3_aggregator::{
    SpecServer, UpdateError, AGGREGATOR_USER, FAILED_UPDATE_MAX_EXP_DELAY, SPEC_DOWNLOAD_TIMEOUT,
    SUCCESSFUL_UPDATE_DELAY,
};
use crate::registry::apiextensions::customresourcedefinition::ItemExponentialFailureRateLimiter;

/// `apiserver.go:470`.
pub const APISERVICE_OPENAPI_HOOK: &str = "apiservice-openapi-controller";
/// `locallyGeneratedEtagPrefix` (aggregator.go:61).
pub const LOCALLY_GENERATED_ETAG_PREFIX: &str = "\"6E8F849B434D4B98A569B9D7718876E9-";
/// `localDelegateChainNamePattern` with index 0.
pub const LOCAL_SPEC_NAME: &str = "k8s_internal_local_delegation_chain_0000000000";

/// `etagFor`.
pub fn etag_for(data: &[u8]) -> String {
    let sum = Sha512::digest(data);
    let hex: String = sum.iter().map(|b| format!("{b:02X}")).collect();
    format!("{LOCALLY_GENERATED_ETAG_PREFIX}{hex}\"")
}

/// `Downloader` (downloader.go).
#[derive(Debug, Clone, Copy, Default)]
pub struct Downloader;

impl Downloader {
    /// `NewDownloader`.
    pub fn new() -> Self {
        Self
    }

    /// `Download`: `(spec, new etag, status)`. `spec` is `None` on 304 and
    /// 404. `handlerWithUser` -> the `system:aggregator` user;
    /// `http.TimeoutHandler(handler, specDownloadTimeout, "request timed out")`.
    pub async fn download(
        &self,
        server: &dyn SpecServer,
        handler: &ProxyHandler,
        group: &str,
        version: &str,
        etag: &str,
    ) -> Result<(Option<Value>, String, u16), String> {
        let req = HandlerRequest {
            path: "/openapi/v2".to_string(),
            accept: "application/json".to_string(),
            // "len(etag) > 0 && !strings.HasPrefix(etag, locallyGeneratedEtagPrefix)"
            if_none_match: (!etag.is_empty() && !etag.starts_with(LOCALLY_GENERATED_ETAG_PREFIX))
                .then(|| etag.to_string()),
            user: AGGREGATOR_USER.to_string(),
            groups: Vec::new(),
        };
        let resp = match tokio::time::timeout(
            SPEC_DOWNLOAD_TIMEOUT,
            server.serve(handler, group, version, req),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => HandlerResponse {
                status: 503,
                body: b"request timed out".to_vec(),
                ..Default::default()
            },
        };
        match resp.status {
            304 => {
                if etag.is_empty() {
                    return Err("http.StatusNotModified is not allowed in absence of etag".into());
                }
                Ok((None, etag.to_string(), 304))
            }
            404 => Ok((None, String::new(), 404)),
            200 => {
                let spec: Value = serde_json::from_slice(&resp.body).map_err(|e| e.to_string())?;
                let mut new_etag = resp.etag.clone().unwrap_or_default();
                if new_etag.is_empty() {
                    new_etag = etag_for(&resp.body);
                    if !etag.is_empty()
                        && etag.starts_with(LOCALLY_GENERATED_ETAG_PREFIX)
                        && etag == new_etag
                    {
                        return Ok((None, etag.to_string(), 304));
                    }
                }
                Ok((Some(spec), new_etag, 200))
            }
            code => Err(format!(
                "failed to retrieve openAPI spec, http error: ResponseCode: {code}, Body: {}, Header: map[]",
                String::from_utf8_lossy(&resp.body)
            )),
        }
    }
}

/// `cacheableDownloader`: keeps the last downloaded spec and its etag so the
/// next download is conditional.
pub struct CacheableDownloader {
    name: String,
    downloader: Downloader,
    handler: Mutex<Arc<ProxyHandler>>,
    cached: Mutex<(String, Option<Value>)>,
}

impl CacheableDownloader {
    /// `NewCacheableDownloader`.
    pub fn new(api_service_name: &str, downloader: Downloader, handler: Arc<ProxyHandler>) -> Self {
        Self {
            name: api_service_name.to_string(),
            downloader,
            handler: Mutex::new(handler),
            cached: Mutex::new((String::new(), None)),
        }
    }

    /// `UpdateHandler`.
    pub fn update_handler(&self, handler: Arc<ProxyHandler>) {
        *self.handler.lock().unwrap() = handler;
    }

    /// `Get`: the failure of `get` is wrapped as `failed to download <name>`.
    pub async fn get(
        &self,
        server: &dyn SpecServer,
        group: &str,
        version: &str,
    ) -> Result<(Value, String), String> {
        self.get_inner(server, group, version)
            .await
            .map_err(|e| format!("failed to download {}: {e}", self.name))
    }

    async fn get_inner(
        &self,
        server: &dyn SpecServer,
        group: &str,
        version: &str,
    ) -> Result<(Value, String), String> {
        let h = self.handler.lock().unwrap().clone();
        let etag = self.cached.lock().unwrap().0.clone();
        let (swagger, new_etag, status) = self
            .downloader
            .download(server, &h, group, version, &etag)
            .await?;
        let mut cached = self.cached.lock().unwrap();
        match status {
            304 => {}
            200 => match swagger {
                Some(s) => *cached = (new_etag, Some(s)),
                // "fallthrough"
                None => return Err(UpdateError::NotFound.to_string()),
            },
            404 => return Err(UpdateError::NotFound.to_string()),
            other => return Err(format!("invalid status code: {other}")),
        }
        match &cached.1 {
            Some(s) => Ok((s.clone(), cached.0.clone())),
            None => Err("no spec cached for a 304 response".to_string()),
        }
    }
}

/// `cached.LastSuccess[*spec.Swagger]`: the latest stored result, hiding an
/// error behind the last success when there has been one.
struct LastSuccess {
    current: Result<(Arc<Value>, String), String>,
    success: Option<(Arc<Value>, String)>,
}

impl LastSuccess {
    /// `Get`.
    fn get(&mut self) -> Result<(Arc<Value>, String), String> {
        match &self.current {
            Ok((v, e)) => {
                if self.success.is_none() {
                    self.success = Some((v.clone(), e.clone()));
                }
                Ok((v.clone(), e.clone()))
            }
            Err(err) => match &self.success {
                Some((v, e)) => Ok((v.clone(), e.clone())),
                None => Err(err.clone()),
            },
        }
    }
}

/// `openAPISpecInfo`.
pub struct SpecInfo {
    api_service: Mutex<APIService>,
    spec: Mutex<LastSuccess>,
    /// `None` for a local spec (`addLocalSpec`).
    downloader: Option<CacheableDownloader>,
}

impl SpecInfo {
    fn api_service(&self) -> APIService {
        self.api_service.lock().unwrap().clone()
    }

    fn get(&self) -> Result<(Arc<Value>, String), String> {
        self.spec.lock().unwrap().get()
    }
}

/// `byPriority.Less` (priority.go).
fn priority_less(
    a: &APIService,
    b: &APIService,
    group_priorities: &HashMap<String, i32>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Greater, Less};
    let (a_svc, b_svc) = (a.spec.service.is_some(), b.spec.service.is_some());
    if !a_svc && b_svc {
        return Less;
    }
    if a_svc && !b_svc {
        return Greater;
    }
    if !a_svc {
        return a.metadata.name.cmp(&b.metadata.name);
    }
    let (i_priority, j_priority) = if a.spec.group == b.spec.group {
        // Upstream compares `apiServices[i]`'s VersionPriority with itself
        // (`jPriority = a.apiServices[i].Spec.VersionPriority`).
        (a.spec.version_priority, a.spec.version_priority)
    } else {
        (
            group_priorities.get(&a.spec.group).copied().unwrap_or(0),
            group_priorities.get(&b.spec.group).copied().unwrap_or(0),
        )
    };
    if i_priority != j_priority {
        return if i_priority > j_priority {
            Less
        } else {
            Greater
        };
    }
    a.metadata.name.cmp(&b.metadata.name)
}

/// `sortByPriority`: local services first (by name), then aggregated ones by
/// descending group priority, then name.
pub fn sort_by_priority(api_services: &mut [APIService]) {
    let mut group_priorities: HashMap<String, i32> = HashMap::new();
    for a in api_services.iter() {
        if a.spec.service.is_none() {
            continue;
        }
        let pr = group_priorities.get(&a.spec.group);
        if pr.is_none_or(|pr| a.spec.group_priority_minimum > *pr) {
            group_priorities.insert(a.spec.group.clone(), a.spec.group_priority_minimum);
        }
    }
    api_services.sort_by(|a, b| priority_less(a, b, &group_priorities));
}

/// `specAggregator`.
pub struct SpecAggregator {
    /// `specsByAPIServiceName` (guarded by `mutex` upstream).
    specs_by_api_service_name: Mutex<HashMap<String, Arc<SpecInfo>>>,
    /// Serializes downloads: upstream's `UpdateAPIServiceSpec` holds `mutex`
    /// across one, which also stalls Add/Remove; here Add/Remove take only
    /// the map's lock so the (synchronous) handler manager never waits on an
    /// aggregated server.
    update_lock: tokio::sync::Mutex<()>,
    /// The caches `buildMergeSpecLocked` captured, in priority order. Read
    /// without `mutex` by the merge (`cached.MergeList`).
    merge_list: RwLock<Vec<Arc<SpecInfo>>>,
    downloader: Downloader,
    server: Arc<dyn SpecServer>,
}

impl SpecAggregator {
    pub fn new(downloader: Downloader, server: Arc<dyn SpecServer>) -> Self {
        Self {
            specs_by_api_service_name: Mutex::new(HashMap::new()),
            update_lock: tokio::sync::Mutex::new(()),
            merge_list: RwLock::new(Vec::new()),
            downloader,
            server,
        }
    }

    /// `addLocalSpec` with a static spec (`cached.Static(spec, "never-changes")`).
    pub fn add_local_spec(&self, name: &str, spec: Value) {
        let mut api_service = APIService::default();
        api_service.metadata.name = name.to_string();
        let info = Arc::new(SpecInfo {
            api_service: Mutex::new(api_service),
            spec: Mutex::new(LastSuccess {
                current: Ok((Arc::new(spec), "never-changes".to_string())),
                success: None,
            }),
            downloader: None,
        });
        let mut map = self.specs_by_api_service_name.lock().unwrap();
        map.insert(name.to_string(), info);
        self.build_merge_spec_locked(&map);
    }

    /// `buildMergeSpecLocked`: sort the specs by priority and capture them.
    fn build_merge_spec_locked(&self, map: &HashMap<String, Arc<SpecInfo>>) {
        let mut services: Vec<APIService> = map.values().map(|i| i.api_service()).collect();
        sort_by_priority(&mut services);
        let list = services
            .iter()
            .map(|a| map[&a.metadata.name].clone())
            .collect();
        *self.merge_list.write().unwrap() = list;
    }

    /// The merged document `/openapi/v2` serves: the `cached.MergeList`
    /// function. `local` is the built-in document, the destination (module docs). `Ok(None)`
    /// is Go's nil `*spec.Swagger` (nothing to merge).
    pub fn merged_spec(&self, local: Option<&Value>) -> Result<Option<Value>, String> {
        let infos: Vec<Arc<SpecInfo>> = self.merge_list.read().unwrap().clone();
        let mut results: Vec<Arc<Value>> = Vec::new();
        for info in &infos {
            match info.get() {
                Ok((v, _)) => results.push(v),
                Err(e) => tracing::warn!("{e}"),
            }
        }
        // The built-in document is the destination as it is (module docs);
        // without one the first result is, minus its paths, definitions and
        // parameters, as `cached.MergeList`'s function builds `merged`.
        let mut merged: Option<Value> = local.cloned();
        for result in results {
            let m = merged.get_or_insert_with(|| {
                let mut m = (*result).clone();
                if let Some(o) = m.as_object_mut() {
                    o.remove("paths");
                    o.remove("definitions");
                    o.remove("parameters");
                }
                m
            });
            merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(m, &result)
                .map_err(|e| format!("failed to build merge specs: {e}"))?;
        }
        Ok(merged)
    }

    /// `UpdateAPIServiceSpec` / `updateServiceLocked`.
    pub async fn update_api_service_spec(&self, name: &str) -> Result<(), UpdateError> {
        let _serialized = self.update_lock.lock().await;
        let info = {
            let map = self.specs_by_api_service_name.lock().unwrap();
            map.get(name).cloned()
        };
        let Some(info) = info else {
            return Err(UpdateError::NotFound);
        };
        let api_service = info.api_service();
        let (group, version) = (api_service.spec.group, api_service.spec.version);
        let Some(downloader) = &info.downloader else {
            return Err(UpdateError::Other(format!("{name} has no downloader")));
        };
        let result = downloader.get(self.server.as_ref(), &group, &version).await;
        let filtered = match &result {
            Ok((spec, etag)) => Ok((
                Arc::new(filter_spec_by_paths_without_side_effects(
                    spec,
                    &[format!("/apis/{group}/{version}/")],
                )),
                etag.clone(),
            )),
            Err(e) => Err(e.clone()),
        };
        info.spec.lock().unwrap().current = filtered;
        result.map(|_| ()).map_err(UpdateError::Other)
    }

    /// `AddUpdateAPIService`.
    pub fn add_update_api_service(&self, api_service: &APIService, handler: Arc<ProxyHandler>) {
        if api_service.spec.service.is_none() {
            return;
        }
        let mut map = self.specs_by_api_service_name.lock().unwrap();
        let name = &api_service.metadata.name;
        match map.get(name) {
            None => {
                let info = Arc::new(SpecInfo {
                    api_service: Mutex::new(api_service.clone()),
                    spec: Mutex::new(LastSuccess {
                        current: Err(format!("spec for apiservice {name} is not yet available")),
                        success: None,
                    }),
                    downloader: Some(CacheableDownloader::new(name, self.downloader, handler)),
                });
                map.insert(name.clone(), info);
                self.build_merge_spec_locked(&map);
            }
            Some(existing) => {
                *existing.api_service.lock().unwrap() = api_service.clone();
                if let Some(d) = &existing.downloader {
                    d.update_handler(handler);
                }
            }
        }
    }

    /// `RemoveAPIService`.
    pub fn remove_api_service(&self, api_service_name: &str) {
        let mut map = self.specs_by_api_service_name.lock().unwrap();
        if map.remove(api_service_name).is_some() {
            self.build_merge_spec_locked(&map);
        }
    }
}

/// `syncAction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAction {
    Requeue,
    RequeueRateLimited,
    Nothing,
}

/// `AggregationController` (controller.go).
pub struct AggregationController {
    pub spec_aggregator: Arc<SpecAggregator>,
    queue: Arc<WorkQueue<String>>,
    /// `NewTypedItemExponentialFailureRateLimiter(successfulUpdateDelay,
    /// failedUpdateMaxExpDelay)`.
    rate_limiter: Arc<Mutex<ItemExponentialFailureRateLimiter>>,
}

impl AggregationController {
    /// `NewAggregationController`.
    pub fn new(spec_aggregator: Arc<SpecAggregator>) -> Arc<Self> {
        Arc::new(Self {
            spec_aggregator,
            queue: Arc::new(WorkQueue::default()),
            rate_limiter: Arc::new(Mutex::new(ItemExponentialFailureRateLimiter::new(
                SUCCESSFUL_UPDATE_DELAY,
                FAILED_UPDATE_MAX_EXP_DELAY,
            ))),
        })
    }

    /// `queue.AddAfter`.
    fn add_after(&self, key: String, delay: Duration) {
        let q = self.queue.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            q.add(key);
        });
    }

    /// `Run`: one worker until `stop` fires.
    pub async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        tracing::info!("Starting OpenAPI AggregationController");
        let worker = {
            let c = self.clone();
            tokio::spawn(async move { while c.process_next_work_item().await {} })
        };
        let _ = stop.wait_for(|s| *s).await;
        self.queue.shut_down();
        let _ = worker.await;
        tracing::info!("Shutting down OpenAPI AggregationController");
    }

    /// `processNextWorkItem`: false when it is time to quit.
    pub async fn process_next_work_item(&self) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        tracing::debug!("OpenAPI AggregationController: Processing item {key}");
        let (action, err) = self.sync(&key).await;
        if let Some(e) = err {
            tracing::error!("loading OpenAPI spec for {key:?} failed with: {e}");
        }
        self.queue.done(&key);
        match action {
            SyncAction::Requeue => self.add_after(key, SUCCESSFUL_UPDATE_DELAY),
            SyncAction::RequeueRateLimited => {
                tracing::info!(
                    "OpenAPI AggregationController: action for item {key}: Rate Limited Requeue."
                );
                let delay = self.rate_limiter.lock().unwrap().when(&key);
                self.add_after(key, delay);
            }
            SyncAction::Nothing => self.rate_limiter.lock().unwrap().forget(&key),
        }
        true
    }

    /// `sync`.
    pub async fn sync(&self, key: &str) -> (SyncAction, Option<UpdateError>) {
        match self.spec_aggregator.update_api_service_spec(key).await {
            Ok(()) => (SyncAction::Requeue, None),
            Err(UpdateError::NotFound) => (SyncAction::Nothing, None),
            Err(e) => (SyncAction::RequeueRateLimited, Some(e)),
        }
    }

    /// `AddAPIService`.
    pub fn add_api_service(self: &Arc<Self>, handler: Arc<ProxyHandler>, api_service: &APIService) {
        if api_service.spec.service.is_none() {
            return;
        }
        self.spec_aggregator
            .add_update_api_service(api_service, handler);
        self.add_after(api_service.metadata.name.clone(), Duration::from_secs(1));
    }

    /// `UpdateAPIService`: refreshes the spec, then moves a failing item
    /// forward. The refresh runs on a spawned task (module docs).
    pub fn update_api_service(self: &Arc<Self>, api_service: &APIService) {
        if api_service.spec.service.is_none() {
            return;
        }
        let c = self.clone();
        let key = api_service.metadata.name.clone();
        tokio::spawn(async move {
            if let Err(e) = c.spec_aggregator.update_api_service_spec(&key).await {
                tracing::error!("Error updating APIService {key:?} with err: {e}");
            }
            let mut rl = c.rate_limiter.lock().unwrap();
            if rl.num_requeues(&key) > 0 {
                rl.forget(&key);
                drop(rl);
                c.add_after(key, Duration::from_secs(1));
            }
        });
    }

    /// `RemoveAPIService`.
    pub fn remove_api_service(&self, api_service_name: &str) {
        self.spec_aggregator.remove_api_service(api_service_name);
        self.rate_limiter.lock().unwrap().forget(api_service_name);
    }
}

/// Register the `apiservice-openapi-controller` post-start hook
/// (apiserver.go:469-474): `go s.openAPIAggregationController.Run(...)`, then
/// return.
pub fn register_openapi_hook_with(
    registry: &crate::post_start_hooks::PostStartHooks,
    controller: Arc<AggregationController>,
    stop: watch::Receiver<bool>,
) {
    crate::post_start_hooks::spawn_hook_with(
        registry,
        APISERVICE_OPENAPI_HOOK,
        async move {
            tokio::spawn(controller.run(stop));
            Ok::<(), std::convert::Infallible>(())
        },
        |_, e| match *e {},
    );
}
