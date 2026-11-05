//! The kube-aggregator `apiservice-openapiv3-controller`: it downloads the
//! OpenAPI v3 discovery document of every aggregated APIService and keeps the
//! per-APIService record the `/openapi/v3` endpoints merge and proxy.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/controllers/openapiv3/`:
//! - `controller.go`: `AggregationController` (`NewAggregationController`,
//!   `Run`, `processNextWorkItem`, `sync`, `AddAPIService`, `UpdateAPIService`,
//!   `RemoveAPIService`) -> [`AggregationController`];
//! - `aggregator/downloader.go`: `Downloader.OpenAPIV3Root` -> [`Downloader`];
//! - `aggregator/aggregator.go`: `specProxier` (`AddUpdateAPIService`,
//!   `UpdateAPIServiceSpec`, `RemoveAPIServiceSpec`, `GetAPIServiceNames`,
//!   `getOpenAPIV3Root`, the lookup half of `handleGroupVersion`) ->
//!   [`SpecProxier`];
//! - `apiserver.go:476-480,:500-512,:528-533,:559-564,:617-622`: the
//!   `apiservice-openapiv3-controller` post-start hook and the
//!   Add/Update/RemoveAPIService calls
//!   ([`crate::apiservice_registration::AggregatorHandlerManager`]).
//!
//! - `aggregator/aggregator.go` `updateAPIServiceSpecLocked`'s OpenAPI v2
//!   fallback: an APIService answering 404 on `/openapi/v3` is marked
//!   `isLegacyAPIService`, its `/openapi/v2` is downloaded
//!   ([`crate::openapi_aggregator::Downloader`]), converted with
//!   `openapiconv.ConvertV2ToV3` ([`crate::openapiconv`]) and published by the
//!   `openapiv2converter` service.
//!
//! Deviations (each a deliberate scope decision, not an equivalent):
//! - The `openapiv2converter` service (a `handler3.OpenAPIService` behind a
//!   `PathRecorderMux`, registered as an APIService whose discovery the
//!   controller downloads) is the in-process [`SpecProxier::converted`] map:
//!   its discovery is computed on read and a group-version it lists is served
//!   from the map, so no download of its own discovery happens.
//! - The local services (`BuildAndRegisterAggregator`'s aggregator types and
//!   delegation chain) are not entries of the store: the built-in documents
//!   are served by `handlers::openapi`, which merges this store's discovery
//!   into its root and lets its own paths win a collision (upstream iterates
//!   a Go map, so a collision is unspecified there).
//! - `updateAPIServiceSpecLocked` holds the write lock across the download;
//!   here the download runs outside the lock and its result is committed
//!   under it, so a slow aggregated server cannot stall `/openapi/v3` reads.
//!   One worker drives the store, as upstream.
//! - `metrics.InstrumentHandlerFunc` on the two `register`ed handlers is not
//!   applied.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use rusternetes_common::resources::APIService;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;

use crate::aggregated_discovery::{HandlerRequest, HandlerResponse};
use crate::aggregator_mux::ProxyHandler;
use crate::crdregistration::WorkQueue;
use crate::registry::apiextensions::customresourcedefinition::ItemExponentialFailureRateLimiter;

/// `apiserver.go:477`.
pub const APISERVICE_OPENAPIV3_HOOK: &str = "apiservice-openapiv3-controller";

/// `successfulUpdateDelay` (controller.go:34).
pub const SUCCESSFUL_UPDATE_DELAY: Duration = Duration::from_secs(60);
/// `failedUpdateMaxExpDelay` (controller.go:36).
pub const FAILED_UPDATE_MAX_EXP_DELAY: Duration = Duration::from_secs(3600);
/// `aggregatorUser` (aggregator.go:55).
pub const AGGREGATOR_USER: &str = "system:aggregator";
/// `specDownloadTimeout` (aggregator.go:56).
pub const SPEC_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(60);

/// `handler3.OpenAPIV3DiscoveryGroupVersion`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpenAPIV3DiscoveryGroupVersion {
    /// Relative URL of the group-version's document, with the content hash.
    #[serde(rename = "serverRelativeURL", default)]
    pub server_relative_url: String,
}

/// `handler3.OpenAPIV3Discovery`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpenAPIV3Discovery {
    #[serde(default)]
    pub paths: BTreeMap<String, OpenAPIV3DiscoveryGroupVersion>,
}

/// An APIService's `proxyHandler` as the downloader drives it: the request is
/// served by the aggregated server of `<version>.<group>` through the
/// handler's decision ladder (`proxyHandler.ServeHTTP`).
#[async_trait]
pub trait SpecServer: Send + Sync {
    async fn serve(
        &self,
        handler: &ProxyHandler,
        group: &str,
        version: &str,
        req: HandlerRequest,
    ) -> HandlerResponse;
}

/// `Downloader` (downloader.go): downloads the OpenAPI v3 root document.
#[derive(Debug, Clone, Copy, Default)]
pub struct Downloader;

impl Downloader {
    pub fn new() -> Self {
        Self
    }

    /// `OpenAPIV3Root`: `(groups, status)` where `groups` is `None` on a 404,
    /// and an error for any other non-200 answer or an undecodable body.
    /// `handlerWithUser` -> the `system:aggregator` user;
    /// `http.TimeoutHandler(handler, specDownloadTimeout, "request timed out")`.
    pub async fn openapi_v3_root(
        &self,
        server: &dyn SpecServer,
        handler: &ProxyHandler,
        group: &str,
        version: &str,
    ) -> Result<(Option<OpenAPIV3Discovery>, u16), String> {
        let req = HandlerRequest {
            path: "/openapi/v3".to_string(),
            accept: String::new(),
            if_none_match: None,
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
            404 => Ok((None, resp.status)),
            200 => serde_json::from_slice::<OpenAPIV3Discovery>(&resp.body)
                .map(|g| (Some(g), resp.status))
                .map_err(|e| e.to_string()),
            _ => Err("Error, could not get list of group versions for APIService".to_string()),
        }
    }
}

/// `ErrAPIServiceNotFound` / any other failure of `UpdateAPIServiceSpec`.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateError {
    /// `ErrAPIServiceNotFound`: "resource not found".
    NotFound,
    Other(String),
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateError::NotFound => write!(f, "resource not found"),
            UpdateError::Other(e) => write!(f, "{e}"),
        }
    }
}

/// `openAPIV3APIServiceInfo`.
struct ServiceInfo {
    api_service: APIService,
    handler: Arc<ProxyHandler>,
    discovery: Option<OpenAPIV3Discovery>,
    /// `isLegacyAPIService`: the APIService does not serve `/openapi/v3`.
    is_legacy: bool,
    /// The etag of the last `/openapi/v2` download (legacy APIServices only).
    etag: String,
}

/// `specProxier`: the OpenAPI v3 specs by APIService name.
pub struct SpecProxier {
    /// `rwMutex` over `apiServiceInfo`.
    info: RwLock<HashMap<String, ServiceInfo>>,
    downloader: Downloader,
    server: Arc<dyn SpecServer>,
    /// The `openapiv2converter` service's documents: the converted OpenAPI v3
    /// document of each legacy APIService, by `apis/<group>/<version>`.
    converted: RwLock<BTreeMap<String, Value>>,
}

/// `getGroupVersionStringFromAPIService`.
fn group_version_string(api_service: &APIService) -> String {
    if api_service.spec.group.is_empty() && api_service.spec.version.is_empty() {
        return String::new();
    }
    format!(
        "apis/{}/{}",
        api_service.spec.group, api_service.spec.version
    )
}

/// `handler3.computeETag`.
pub fn compute_etag(data: &[u8]) -> String {
    use sha2::{Digest, Sha512};
    Sha512::digest(data)
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect()
}

impl SpecProxier {
    pub fn new(downloader: Downloader, server: Arc<dyn SpecServer>) -> Self {
        Self {
            info: RwLock::new(HashMap::new()),
            downloader,
            server,
            converted: RwLock::new(BTreeMap::new()),
        }
    }

    /// The converted OpenAPI v3 document of the legacy APIService serving
    /// `target_gv` (`openAPIV2ConverterHandler.HandleGroupVersion`).
    pub fn converted_group_version(&self, target_gv: &str) -> Option<Value> {
        self.converted.read().unwrap().get(target_gv).cloned()
    }

    /// `AddUpdateAPIService`: "If the APIService is being updated, use the
    /// existing struct."
    pub fn add_update_api_service(&self, handler: Arc<ProxyHandler>, api_service: &APIService) {
        let mut info = self.info.write().unwrap();
        if let Some(existing) = info.get_mut(&api_service.metadata.name) {
            existing.api_service = api_service.clone();
            existing.handler = handler;
            return;
        }
        info.insert(
            api_service.metadata.name.clone(),
            ServiceInfo {
                api_service: api_service.clone(),
                handler,
                discovery: None,
                is_legacy: false,
                etag: String::new(),
            },
        );
    }

    /// `UpdateAPIServiceSpec` / `updateAPIServiceSpecLocked`: refresh the
    /// APIService's discovery document, or (legacy) its converted OpenAPI v2.
    pub async fn update_api_service_spec(&self, name: &str) -> Result<(), UpdateError> {
        let (handler, api_service, is_legacy, etag) = {
            let info = self.info.read().unwrap();
            let Some(i) = info.get(name) else {
                return Err(UpdateError::NotFound);
            };
            (
                i.handler.clone(),
                i.api_service.clone(),
                i.is_legacy,
                i.etag.clone(),
            )
        };
        let (group, version) = (&api_service.spec.group, &api_service.spec.version);
        if !is_legacy {
            let (gv, status) = self
                .downloader
                .openapi_v3_root(self.server.as_ref(), &handler, group, version)
                .await
                .map_err(UpdateError::Other)?;
            let mut info = self.info.write().unwrap();
            let Some(i) = info.get_mut(name) else {
                // Removed while downloading.
                return Err(UpdateError::NotFound);
            };
            if status != 404 {
                i.discovery = gv;
                return Ok(());
            }
            i.is_legacy = true;
        }

        // The OpenAPI v2 fallback: `newDownloader.Download(handler, etag)`.
        let (v2_spec, new_etag, status) = crate::openapi_aggregator::Downloader::new()
            .download(self.server.as_ref(), &handler, group, version, &etag)
            .await
            .map_err(UpdateError::Other)?;
        {
            let mut info = self.info.write().unwrap();
            let Some(i) = info.get_mut(name) else {
                return Err(UpdateError::NotFound);
            };
            i.etag = new_etag;
        }
        if status == 200 {
            if let Some(v2) = v2_spec {
                let v3 = crate::openapiconv::convert_v2_to_v3(&v2);
                self.converted
                    .write()
                    .unwrap()
                    .insert(group_version_string(&api_service), v3);
            }
        }
        Ok(())
    }

    /// `RemoveAPIServiceSpec`: "If it does not exist, no error is returned."
    pub fn remove_api_service_spec(&self, name: &str) {
        if let Some(removed) = self.info.write().unwrap().remove(name) {
            self.converted
                .write()
                .unwrap()
                .remove(&group_version_string(&removed.api_service));
        }
    }

    /// `GetAPIServiceNames`.
    pub fn get_api_service_names(&self) -> Vec<String> {
        self.info.read().unwrap().keys().cloned().collect()
    }

    /// `getOpenAPIV3Root`: the merge of every APIService's discovery paths.
    pub fn get_open_api_v3_root(&self) -> OpenAPIV3Discovery {
        let info = self.info.read().unwrap();
        let mut names: Vec<&String> = info.keys().collect();
        names.sort();
        let mut merged = OpenAPIV3Discovery::default();
        for name in names {
            let Some(d) = &info[name].discovery else {
                continue;
            };
            for (k, v) in &d.paths {
                merged.paths.insert(k.clone(), v.clone());
            }
        }
        // The `openapiv2converter` service's discovery
        // (`handler3.OpenAPIService.HandleDiscovery`).
        for (gv, doc) in self.converted.read().unwrap().iter() {
            let etag = compute_etag(&serde_json::to_vec(doc).unwrap_or_default());
            merged
                .paths
                .entry(gv.clone())
                .or_insert_with(|| OpenAPIV3DiscoveryGroupVersion {
                    server_relative_url: format!("/openapi/v3/{gv}?hash={etag}"),
                });
        }
        merged
    }

    /// The lookup half of `handleGroupVersion`: the APIService whose
    /// discovery lists `target_gv` (`apis/<group>/<version>`), as its
    /// handler and `(group, version)`. `None` is the 404 branch.
    pub fn group_version_handler(
        &self,
        target_gv: &str,
    ) -> Option<(Arc<ProxyHandler>, String, String)> {
        let info = self.info.read().unwrap();
        let mut names: Vec<&String> = info.keys().collect();
        names.sort();
        for name in names {
            let i = &info[name];
            if i.discovery
                .as_ref()
                .is_some_and(|d| d.paths.contains_key(target_gv))
            {
                return Some((
                    i.handler.clone(),
                    i.api_service.spec.group.clone(),
                    i.api_service.spec.version.clone(),
                ));
            }
        }
        None
    }
}

/// `syncAction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncAction {
    Requeue,
    RequeueRateLimited,
    Nothing,
}

/// `AggregationController`.
pub struct AggregationController {
    pub spec_proxier: Arc<SpecProxier>,
    queue: Arc<WorkQueue<String>>,
    /// `NewTypedItemExponentialFailureRateLimiter(successfulUpdateDelay,
    /// failedUpdateMaxExpDelay)`.
    rate_limiter: Mutex<ItemExponentialFailureRateLimiter>,
}

impl AggregationController {
    /// `NewAggregationController`.
    pub fn new(spec_proxier: Arc<SpecProxier>) -> Arc<Self> {
        let c = Arc::new(Self {
            spec_proxier,
            queue: Arc::new(WorkQueue::default()),
            rate_limiter: Mutex::new(ItemExponentialFailureRateLimiter::new(
                SUCCESSFUL_UPDATE_DELAY,
                FAILED_UPDATE_MAX_EXP_DELAY,
            )),
        });
        // "update each service at least once, also those which are not coming
        // from APIServices, namely local services"
        for name in c.spec_proxier.get_api_service_names() {
            c.add_after(name, Duration::from_secs(1));
        }
        c
    }

    /// `queue.AddAfter`.
    fn add_after(self: &Arc<Self>, key: String, delay: Duration) {
        let q = self.queue.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            q.add(key);
        });
    }

    /// `Run`: one worker (`wait.Until(c.runWorker, time.Second, stopCh)`)
    /// until `stop` fires.
    pub async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        tracing::info!("Starting OpenAPI V3 AggregationController");
        let worker = {
            let c = self.clone();
            tokio::spawn(async move { while c.process_next_work_item().await {} })
        };
        let _ = stop.wait_for(|s| *s).await;
        self.queue.shut_down();
        let _ = worker.await;
        tracing::info!("Shutting down OpenAPI V3 AggregationController");
    }

    /// `processNextWorkItem`: false when it is time to quit.
    pub async fn process_next_work_item(self: &Arc<Self>) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        tracing::debug!("OpenAPI AggregationController: Processing item {key}");
        let (action, err) = self.sync(&key).await;
        match err {
            None => self.rate_limiter.lock().unwrap().forget(&key),
            Some(e) => tracing::error!("loading OpenAPI spec for {key:?} failed with: {e}"),
        }
        self.queue.done(&key);
        match action {
            SyncAction::Requeue => self.add_after(key, SUCCESSFUL_UPDATE_DELAY),
            SyncAction::RequeueRateLimited => {
                let delay = self.rate_limiter.lock().unwrap().when(&key);
                self.add_after(key, delay);
            }
            SyncAction::Nothing => {
                tracing::info!(
                    "OpenAPI AggregationController: action for item {key}: Nothing (removed from the queue)."
                );
            }
        }
        true
    }

    /// `sync`.
    pub async fn sync(&self, key: &str) -> (SyncAction, Option<UpdateError>) {
        match self.spec_proxier.update_api_service_spec(key).await {
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
        self.spec_proxier
            .add_update_api_service(handler, api_service);
        self.add_after(api_service.metadata.name.clone(), Duration::from_secs(1));
    }

    /// `UpdateAPIService`.
    pub fn update_api_service(
        self: &Arc<Self>,
        handler: Arc<ProxyHandler>,
        api_service: &APIService,
    ) {
        if api_service.spec.service.is_none() {
            return;
        }
        self.spec_proxier
            .add_update_api_service(handler, api_service);
        let key = api_service.metadata.name.clone();
        let mut rl = self.rate_limiter.lock().unwrap();
        if rl.num_requeues(&key) > 0 {
            // "The item has failed before. Remove it from failure queue and
            // update it in a second"
            rl.forget(&key);
            drop(rl);
            self.add_after(key, Duration::from_secs(1));
        }
    }

    /// `RemoveAPIService`.
    pub fn remove_api_service(&self, api_service_name: &str) {
        self.spec_proxier.remove_api_service_spec(api_service_name);
        // "This will only remove it if it was failing before. If it was
        // successful, processNextWorkItem will figure it out and will not add
        // it again to the queue."
        self.rate_limiter.lock().unwrap().forget(api_service_name);
    }
}

// ---------------------------------------------------------------------------
// Production SpecServer
// ---------------------------------------------------------------------------

/// Serves a request through an APIService's `proxyHandler` the way the
/// aggregator proxy does: the decision ladder and endpoint resolution of
/// [`crate::handlers::aggregator::resolve_aggregator_target_for_handler`],
/// then [`crate::handlers::aggregator::forward_to_aggregator`] (which dials
/// through the egress selector's `Cluster` dialer, as `proxyTransportDial`).
pub struct ProxySpecServer<S: rusternetes_storage::Storage + Send + Sync + 'static> {
    storage: Arc<S>,
}

impl<S: rusternetes_storage::Storage + Send + Sync + 'static> ProxySpecServer<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

pub(crate) async fn response_to_handler_response(
    resp: axum::response::Response,
) -> HandlerResponse {
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .map(|b| b.to_vec())
        .unwrap_or_default();
    HandlerResponse {
        status,
        content_type,
        etag,
        body,
    }
}

#[async_trait]
impl<S: rusternetes_storage::Storage + Send + Sync + 'static> SpecServer for ProxySpecServer<S> {
    async fn serve(
        &self,
        handler: &ProxyHandler,
        group: &str,
        version: &str,
        req: HandlerRequest,
    ) -> HandlerResponse {
        let target = match crate::handlers::aggregator::resolve_aggregator_target_for_handler(
            self.storage.as_ref(),
            group,
            version,
            handler,
        )
        .await
        {
            Ok(Some(t)) => t,
            Ok(None) => {
                return HandlerResponse {
                    status: 503,
                    body: b"service unavailable".to_vec(),
                    ..Default::default()
                }
            }
            Err(resp) => return response_to_handler_response(resp).await,
        };
        forward_handler_request(&target, &req, None).await
    }
}

/// Forwards `req` (a `GET` as `req.user`/`req.groups`) to the resolved
/// aggregated server through [`crate::handlers::aggregator::forward_to_aggregator`]:
/// the egress selector's `Cluster` dialer (`proxyTransportDial`), the
/// front-proxy client identity and the `X-Remote-*` identity headers, as the
/// APIService's `proxyHandler` does for any request. `dialer` is the proxy
/// transport's dial fixed at construction (`None` looks the selector up per
/// request, `Some(None)` is the default dial).
pub async fn forward_handler_request(
    target: &crate::handlers::aggregator::AggregatorTarget,
    req: &HandlerRequest,
    dialer: Option<Option<crate::egress_selector::Dialer>>,
) -> HandlerResponse {
    let auth_ctx = crate::middleware::AuthContext {
        user: rusternetes_common::auth::UserInfo {
            username: req.user.clone(),
            uid: String::new(),
            groups: req.groups.clone(),
            extra: Default::default(),
        },
    };
    let mut headers = axum::http::HeaderMap::new();
    if !req.accept.is_empty() {
        if let Ok(v) = axum::http::HeaderValue::from_str(&req.accept) {
            headers.insert(axum::http::header::ACCEPT, v);
        }
    }
    if let Some(etag) = &req.if_none_match {
        if let Ok(v) = axum::http::HeaderValue::from_str(etag) {
            headers.insert(axum::http::header::IF_NONE_MATCH, v);
        }
    }
    let resp = match dialer {
        None => {
            crate::handlers::aggregator::forward_to_aggregator(
                target,
                &auth_ctx,
                axum::http::Method::GET,
                &req.path,
                &headers,
                Vec::new(),
            )
            .await
        }
        Some(dialer) => {
            crate::handlers::aggregator::forward_to_aggregator_with_dialer(
                target,
                &auth_ctx,
                axum::http::Method::GET,
                &req.path,
                &headers,
                Vec::new(),
                dialer,
            )
            .await
        }
    };
    response_to_handler_response(resp).await
}

/// Register the `apiservice-openapiv3-controller` post-start hook
/// (apiserver.go:476-480): `go s.openAPIV3AggregationController.Run(...)`,
/// then return.
pub fn register_openapiv3_hook_with(
    registry: &crate::post_start_hooks::PostStartHooks,
    controller: Arc<AggregationController>,
    stop: watch::Receiver<bool>,
) {
    crate::post_start_hooks::spawn_hook_with(
        registry,
        APISERVICE_OPENAPIV3_HOOK,
        async move {
            tokio::spawn(controller.run(stop));
            Ok::<(), std::convert::Infallible>(())
        },
        |_, e| match *e {},
    );
}
