//! The kube-aggregator `autoregister` controller: keeps a set of built-in
//! APIServices present in the API.
//!
//! Ported from `staging/src/k8s.io/kube-aggregator/pkg/controllers/autoregister/
//! autoregister_controller.go` (`checkAPIService`, `Add/RemoveAPIServiceToSync`)
//! and `pkg/controlplane/apiserver/aggregator.go` (`makeAPIService`,
//! `apiServicesToRegister`, `DefaultGenericAPIServicePriorities`). Tests are
//! ported from `autoregister_controller_test.go` (`TestSync`).
//!
//! Like upstream, `checkAPIService` is driven from APIService add/update/delete
//! events (a storage watch, [`spawn_api_service_event_handlers`]) and from
//! `Add/RemoveAPIServiceToSync` through a de-duplicating workqueue with
//! `AddRateLimited` retries on error ([`AutoRegisterController::run`]).
//! Wired into `startup.rs` via [`spawn_autoregistration`], together with the
//! `crdregistration` controller (`crate::crdregistration`).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use rusternetes_common::resources::APIService;
use rusternetes_storage::{build_key, build_prefix, Storage, WatchEvent};

use crate::crdregistration::WorkQueue;
use crate::registry::apiextensions::customresourcedefinition::ControllerRateLimiter;

/// `AutoRegisterManagedLabel`.
pub const AUTO_REGISTER_MANAGED_LABEL: &str = "kube-aggregator.kubernetes.io/automanaged";
/// `manageOnStart`.
pub const MANAGE_ON_START: &str = "onstart";
/// `manageContinuously`.
pub const MANAGE_CONTINUOUSLY: &str = "true";

/// The API errors `checkAPIService` distinguishes (`apierrors.IsNotFound`, ...).
#[derive(Debug, Clone, PartialEq)]
pub enum ClientError {
    NotFound,
    AlreadyExists,
    Conflict,
    Other(String),
}

/// `apiServiceLister` + `apiServiceClient`.
#[async_trait]
pub trait APIServiceClient: Send + Sync {
    /// Lister `Get`: `Ok(None)` is NotFound.
    async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError>;
    async fn list_names(&self) -> Result<Vec<String>, ClientError>;
    async fn create(&self, apiservice: &APIService) -> Result<(), ClientError>;
    async fn update(&self, apiservice: &APIService) -> Result<(), ClientError>;
    /// Delete with `Preconditions: NewUIDPreconditions(uid)`.
    async fn delete(&self, name: &str, uid: &str) -> Result<(), ClientError>;
}

/// `autoRegisterController` state.
pub struct AutoRegisterController {
    pub api_services_to_sync: Mutex<HashMap<String, APIService>>,
    pub synced_successfully: Mutex<HashSet<String>>,
    pub api_services_at_start: Mutex<HashSet<String>>,
    /// "where incoming work is placed to de-dup and to allow easy rate limited
    /// requeues on errors"
    pub queue: WorkQueue<String>,
    pub rate_limiter: Mutex<ControllerRateLimiter>,
}

impl Default for AutoRegisterController {
    fn default() -> Self {
        Self {
            api_services_to_sync: Mutex::default(),
            synced_successfully: Mutex::default(),
            api_services_at_start: Mutex::default(),
            queue: WorkQueue::default(),
            rate_limiter: Mutex::new(ControllerRateLimiter::new()),
        }
    }
}

pub fn automanaged_type(service: Option<&APIService>) -> &str {
    service
        .and_then(|s| s.metadata.labels.as_ref())
        .and_then(|l| l.get(AUTO_REGISTER_MANAGED_LABEL))
        .map(String::as_str)
        .unwrap_or("")
}

fn is_automanaged_on_start(service: Option<&APIService>) -> bool {
    automanaged_type(service) == MANAGE_ON_START
}

fn is_automanaged(service: Option<&APIService>) -> bool {
    let t = automanaged_type(service);
    t == MANAGE_ON_START || t == MANAGE_CONTINUOUSLY
}

impl AutoRegisterController {
    pub fn new() -> Self {
        Self::default()
    }

    /// `AddAPIServiceToSyncOnStart`.
    pub fn add_api_service_to_sync_on_start(&self, in_: &APIService) {
        self.add_api_service_to_sync_typed(in_, MANAGE_ON_START);
    }

    /// `AddAPIServiceToSync`.
    pub fn add_api_service_to_sync(&self, in_: &APIService) {
        self.add_api_service_to_sync_typed(in_, MANAGE_CONTINUOUSLY);
    }

    /// `addAPIServiceToSync`: label the copy and enqueue its name.
    fn add_api_service_to_sync_typed(&self, in_: &APIService, sync_type: &str) {
        let mut api_service = in_.clone();
        api_service
            .metadata
            .labels
            .get_or_insert_with(HashMap::new)
            .insert(
                AUTO_REGISTER_MANAGED_LABEL.to_string(),
                sync_type.to_string(),
            );
        let name = api_service.metadata.name.clone();
        self.api_services_to_sync
            .lock()
            .unwrap()
            .insert(name.clone(), api_service);
        self.queue.add(name);
    }

    /// `RemoveAPIServiceToSync`.
    pub fn remove_api_service_to_sync(&self, name: &str) {
        self.api_services_to_sync.lock().unwrap().remove(name);
        self.queue.add(name.to_string());
    }

    /// `GetAPIServiceToSync`.
    pub fn get_api_service_to_sync(&self, name: &str) -> Option<APIService> {
        self.api_services_to_sync.lock().unwrap().get(name).cloned()
    }

    /// Test/`Run` helper: names present when the controller started.
    pub fn record_present_at_start(&self, names: impl IntoIterator<Item = String>) {
        self.api_services_at_start.lock().unwrap().extend(names);
    }

    /// Test helper: mark a name as already synced successfully.
    pub fn mark_synced(&self, name: &str) {
        self.synced_successfully
            .lock()
            .unwrap()
            .insert(name.to_string());
    }

    /// `checkAPIService` (autoregister_controller.go:~207-288): the decision
    /// table of current vs desired, with the synced-once and present-at-start
    /// enforcement.
    pub async fn check_api_service(
        &self,
        client: &dyn APIServiceClient,
        name: &str,
    ) -> Result<(), ClientError> {
        let desired = self.get_api_service_to_sync(name);
        let curr = client.get(name).await;

        // if we've never synced this service successfully, record a successful sync.
        let has_synced = self.synced_successfully.lock().unwrap().contains(name);
        let result = self
            .decide_and_apply(client, name, desired.as_ref(), curr, has_synced)
            .await;
        if !has_synced && result.is_ok() {
            self.synced_successfully
                .lock()
                .unwrap()
                .insert(name.to_string());
        }
        result
    }

    async fn decide_and_apply(
        &self,
        client: &dyn APIServiceClient,
        name: &str,
        desired: Option<&APIService>,
        curr: Result<Option<APIService>, ClientError>,
        has_synced: bool,
    ) -> Result<(), ClientError> {
        // we had a real error, just return it (1A,1B,1C)
        let curr = curr?;
        // we don't have an entry and we don't want one (2A)
        if curr.is_none() && desired.is_none() {
            return Ok(());
        }
        // the local object only wants to sync on start and has already synced
        // (2B,5B,6B "once" enforcement)
        if is_automanaged_on_start(desired) && has_synced {
            return Ok(());
        }
        // we don't have an entry and we do want one (2B,2C)
        let Some(curr) = curr else {
            return match client.create(desired.expect("checked above")).await {
                // created in the meantime, we'll get called again
                Err(ClientError::AlreadyExists) => Ok(()),
                other => other,
            };
        };
        // we aren't trying to manage this APIService (3A,3B,3C)
        if !is_automanaged(Some(&curr)) {
            return Ok(());
        }
        // the remote object only wants to sync on start, but was added after
        // we started (4A,4B,4C)
        if is_automanaged_on_start(Some(&curr))
            && !self.api_services_at_start.lock().unwrap().contains(name)
        {
            return Ok(());
        }
        // the remote object only wants to sync on start and has already
        // synced (5A,5B,5C "once" enforcement)
        if is_automanaged_on_start(Some(&curr)) && has_synced {
            return Ok(());
        }
        let Some(desired) = desired else {
            // we have a spurious APIService that we're managing, delete it (5A,6A)
            return match client.delete(&curr.metadata.name, &curr.metadata.uid).await {
                // deleted or changed in the meantime, we'll get called again
                Err(ClientError::NotFound | ClientError::Conflict) => Ok(()),
                other => other,
            };
        };
        // if the specs already match, nothing for us to do
        if curr.spec == desired.spec {
            return Ok(());
        }
        // we have an entry and we have a desired, now we deconflict. Only a
        // few fields matter. (5B,5C,6B,6C)
        let mut api_service = curr;
        api_service.spec = desired.spec.clone();
        match client.update(&api_service).await {
            Err(ClientError::NotFound | ClientError::Conflict) => Ok(()),
            other => other,
        }
    }

    /// Informer `AddFunc`.
    pub fn on_add(&self, api_service: &APIService) {
        self.queue.add(api_service.metadata.name.clone());
    }

    /// Informer `UpdateFunc`.
    pub fn on_update(&self, api_service: &APIService) {
        self.queue.add(api_service.metadata.name.clone());
    }

    /// Informer `DeleteFunc` (the `DeletedFinalStateUnknown` tombstone is
    /// unrepresentable: a storage watch delivers the previous object).
    pub fn on_delete(&self, api_service: &APIService) {
        self.queue.add(api_service.metadata.name.clone());
    }

    /// `Run`: wait for the APIService cache, record the APIServices that
    /// existed when we started, then run `workers` workers until `stop` fires.
    pub async fn run(
        self: Arc<Self>,
        client: Arc<dyn APIServiceClient>,
        workers: usize,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        tracing::info!("Starting autoregister controller");
        // `WaitForCacheSync("autoregister", stopCh, c.apiServiceSynced)`
        let names = loop {
            match client.list_names().await {
                Ok(names) => break names,
                Err(e) => tracing::debug!("autoregister: waiting for cache sync: {e:?}"),
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = stop.changed() => return,
            }
        };
        // record APIService objects that existed when we started
        self.record_present_at_start(names);

        let mut tasks = Vec::new();
        for _ in 0..workers {
            let c = self.clone();
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                while c.process_next_work_item(&client).await {}
            }));
        }
        // wait until we're told to stop
        while !*stop.borrow() {
            if stop.changed().await.is_err() {
                break;
            }
        }
        self.queue.shut_down();
        for t in tasks {
            let _ = t.await;
        }
        tracing::info!("Shutting down autoregister controller");
    }

    /// `processNextWorkItem`: false when it's time to quit.
    async fn process_next_work_item(self: &Arc<Self>, client: &Arc<dyn APIServiceClient>) -> bool {
        let Some(key) = self.queue.get().await else {
            return false;
        };
        let result = self.check_api_service(client.as_ref(), &key).await;
        self.queue.done(&key);
        match result {
            // `Forget`: reset the per-item failure count
            Ok(()) => self.rate_limiter.lock().unwrap().forget(&key),
            Err(e) => {
                tracing::error!("autoregister: {key} failed with : {e:?}");
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

/// The APIService informer's event handlers, fed by a storage watch: the
/// controller's `AddFunc`/`UpdateFunc`/`DeleteFunc` (aggregator.go
/// `NewAutoRegisterController`) and, for Add/Update, the
/// `autoregister-completion` check's `handleAPIServiceChange`
/// (aggregator.go:~275-284). The informer's initial list is replayed as Adds
/// after the watch starts, so no event falls between the two.
pub async fn spawn_api_service_event_handlers<S: Storage + 'static>(
    storage: Arc<S>,
    controller: Arc<AutoRegisterController>,
    check: Arc<APIServiceAvailableCheck>,
) {
    let mut stream = match storage.watch(&build_prefix("apiservices", None)).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("autoregister: cannot watch APIServices: {e}");
            return;
        }
    };
    match storage
        .list::<APIService>(&build_prefix("apiservices", None))
        .await
    {
        Ok(all) => {
            for a in &all {
                controller.on_add(a);
                check.handle_api_service_change(a);
            }
        }
        Err(e) => tracing::error!("autoregister: cannot list APIServices: {e}"),
    }
    tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            let Ok(ev) = ev else { continue };
            match ev {
                WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                    let Ok(a) = serde_json::from_str::<APIService>(&v) else {
                        continue;
                    };
                    controller.on_update(&a);
                    check.handle_api_service_change(&a);
                }
                WatchEvent::Deleted(_, v) => {
                    let Ok(a) = serde_json::from_str::<APIService>(&v) else {
                        continue;
                    };
                    controller.on_delete(&a);
                }
            }
        }
    });
}

impl crate::crdregistration::AutoAPIServiceRegistration for AutoRegisterController {
    fn add_api_service_to_sync(&self, in_: &APIService) {
        AutoRegisterController::add_api_service_to_sync(self, in_)
    }
    fn remove_api_service_to_sync(&self, name: &str) {
        AutoRegisterController::remove_api_service_to_sync(self, name)
    }
}

/// An `APIServiceClient` over the API server's own storage (upstream uses the
/// loopback client; creates stamp the system fields as `FillObjectMetaSystemFields`).
pub struct StorageAPIServiceClient<S: Storage>(pub std::sync::Arc<S>);

fn map_err(e: rusternetes_common::Error) -> ClientError {
    match e {
        rusternetes_common::Error::NotFound(_) => ClientError::NotFound,
        rusternetes_common::Error::AlreadyExists(_) => ClientError::AlreadyExists,
        rusternetes_common::Error::Conflict(_) => ClientError::Conflict,
        other => ClientError::Other(other.to_string()),
    }
}

#[async_trait]
impl<S: Storage + 'static> APIServiceClient for StorageAPIServiceClient<S> {
    async fn get(&self, name: &str) -> Result<Option<APIService>, ClientError> {
        match self
            .0
            .get::<APIService>(&build_key("apiservices", None, name))
            .await
        {
            Ok(a) => Ok(Some(a)),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }
    async fn list_names(&self) -> Result<Vec<String>, ClientError> {
        let all = self
            .0
            .list::<APIService>(&build_prefix("apiservices", None))
            .await
            .map_err(map_err)?;
        Ok(all.into_iter().map(|a| a.metadata.name).collect())
    }
    async fn create(&self, apiservice: &APIService) -> Result<(), ClientError> {
        let mut a = apiservice.clone();
        if a.api_version.is_empty() {
            a.api_version = "apiregistration.k8s.io/v1".to_string();
        }
        if a.kind.is_empty() {
            a.kind = "APIService".to_string();
        }
        crate::registry::rest::fill_object_meta_system_fields(&mut a.metadata);
        self.0
            .create(&build_key("apiservices", None, &a.metadata.name), &a)
            .await
            .map(|_| ())
            .map_err(map_err)
    }
    async fn update(&self, apiservice: &APIService) -> Result<(), ClientError> {
        self.0
            .update(
                &build_key("apiservices", None, &apiservice.metadata.name),
                apiservice,
            )
            .await
            .map(|_| ())
            .map_err(map_err)
    }
    async fn delete(&self, name: &str, uid: &str) -> Result<(), ClientError> {
        let key = build_key("apiservices", None, name);
        let current = self.0.get::<APIService>(&key).await.map_err(map_err)?;
        // `Preconditions: NewUIDPreconditions(uid)`
        if current.metadata.uid != uid {
            return Err(ClientError::Conflict);
        }
        self.0.delete(&key).await.map_err(map_err)
    }
}

/// `APIServicePriority`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct APIServicePriority {
    pub group: i32,
    pub version: i32,
}

/// `DefaultGenericAPIServicePriorities` (aggregator.go:317-352), as
/// `(group, version, group priority, version priority)`.
pub const DEFAULT_GENERIC_API_SERVICE_PRIORITIES: &[(&str, &str, i32, i32)] = &[
    ("", "v1", 18000, 1),
    ("events.k8s.io", "v1", 17750, 15),
    ("events.k8s.io", "v1beta1", 17750, 5),
    ("authentication.k8s.io", "v1", 17700, 15),
    ("authentication.k8s.io", "v1beta1", 17700, 9),
    ("authentication.k8s.io", "v1alpha1", 17700, 1),
    ("authorization.k8s.io", "v1", 17600, 15),
    ("certificates.k8s.io", "v1", 17300, 15),
    ("certificates.k8s.io", "v1beta1", 17300, 9),
    ("certificates.k8s.io", "v1alpha1", 17300, 1),
    ("rbac.authorization.k8s.io", "v1", 17000, 15),
    ("apiextensions.k8s.io", "v1", 16700, 15),
    ("admissionregistration.k8s.io", "v1", 16700, 15),
    ("admissionregistration.k8s.io", "v1beta1", 16700, 12),
    ("admissionregistration.k8s.io", "v1alpha1", 16700, 9),
    ("coordination.k8s.io", "v1", 16500, 15),
    ("coordination.k8s.io", "v1beta1", 16500, 13),
    ("coordination.k8s.io", "v1alpha2", 16500, 12),
    ("discovery.k8s.io", "v1", 16200, 15),
    ("discovery.k8s.io", "v1beta1", 16200, 12),
    ("flowcontrol.apiserver.k8s.io", "v1", 16100, 21),
    ("flowcontrol.apiserver.k8s.io", "v1beta3", 16100, 18),
    ("flowcontrol.apiserver.k8s.io", "v1beta2", 16100, 15),
    ("flowcontrol.apiserver.k8s.io", "v1beta1", 16100, 12),
    ("flowcontrol.apiserver.k8s.io", "v1alpha1", 16100, 9),
    ("internal.apiserver.k8s.io", "v1alpha1", 16000, 9),
    ("resource.k8s.io", "v1alpha3", 15900, 9),
    ("storagemigration.k8s.io", "v1beta1", 15800, 9),
];

/// The kube-apiserver's own `apiVersionPriorities` overlay
/// (`cmd/kube-apiserver/app/aggregator.go:31-60`, `merge(DefaultGeneric...,
/// {...})`): the groups served by the kube-apiserver rather than the generic
/// control plane. Entries here win over [`DEFAULT_GENERIC_API_SERVICE_PRIORITIES`].
pub const KUBE_APISERVER_API_VERSION_PRIORITIES: &[(&str, &str, i32, i32)] = &[
    ("", "v1", 18000, 1),
    ("apps", "v1", 17800, 15),
    ("autoscaling", "v1", 17500, 15),
    ("autoscaling", "v2", 17500, 30),
    ("autoscaling", "v2beta1", 17500, 9),
    ("autoscaling", "v2beta2", 17500, 1),
    ("batch", "v1", 17400, 15),
    ("batch", "v1beta1", 17400, 9),
    ("batch", "v2alpha1", 17400, 9),
    ("networking.k8s.io", "v1", 17200, 15),
    ("networking.k8s.io", "v1beta1", 17200, 9),
    ("policy", "v1", 17100, 15),
    ("policy", "v1beta1", 17100, 9),
    ("storage.k8s.io", "v1", 16800, 15),
    ("storage.k8s.io", "v1beta1", 16800, 9),
    ("storage.k8s.io", "v1alpha1", 16800, 1),
    ("scheduling.k8s.io", "v1", 16600, 15),
    ("scheduling.k8s.io", "v1alpha1", 16600, 1),
    ("node.k8s.io", "v1", 16300, 15),
    ("node.k8s.io", "v1alpha1", 16300, 1),
    ("node.k8s.io", "v1beta1", 16300, 9),
    ("resource.k8s.io", "v1", 16200, 21),
    ("resource.k8s.io", "v1beta2", 16200, 15),
    ("resource.k8s.io", "v1beta1", 16200, 9),
    ("resource.k8s.io", "v1alpha3", 16200, 1),
];

/// `makeAPIService` (aggregator.go:~262-281): `None` for a group-version
/// without a priority, so a CRD's group-version is never pinned in the list.
pub fn make_api_service(group: &str, version: &str) -> Option<APIService> {
    let (_, _, group_priority, version_priority) = KUBE_APISERVER_API_VERSION_PRIORITIES
        .iter()
        .chain(DEFAULT_GENERIC_API_SERVICE_PRIORITIES.iter())
        .find(|(g, v, _, _)| *g == group && *v == version)?;
    let mut s = APIService::default();
    s.metadata.name = format!("{version}.{group}");
    s.spec.group = group.to_string();
    s.spec.version = version.to_string();
    s.spec.group_priority_minimum = *group_priority;
    s.spec.version_priority = *version_priority;
    Some(s)
}

/// `apiServicesToRegister` (aggregator.go:354-387): registers on start one
/// APIService per delegate path `/api/v1` or `/apis/<group>/<version>`.
pub fn api_services_to_register(
    listed_paths: &[String],
    registration: &AutoRegisterController,
) -> Vec<APIService> {
    let mut api_services = vec![];
    for curr in listed_paths {
        if curr == "/api/v1" {
            if let Some(s) = make_api_service("", "v1") {
                registration.add_api_service_to_sync_on_start(&s);
                api_services.push(s);
            }
            continue;
        }
        if !curr.starts_with("/apis/") {
            continue;
        }
        let tokens: Vec<&str> = curr.split('/').collect();
        if tokens.len() != 4 {
            continue;
        }
        let Some(s) = make_api_service(tokens[2], tokens[3]) else {
            continue;
        };
        registration.add_api_service_to_sync_on_start(&s);
        api_services.push(s);
    }
    api_services
}

/// `makeAPIServiceAvailableHealthCheck` (aggregator.go:266-301): the
/// `autoregister-completion` boot-sequence check, failing with
/// `missing APIService: [..]` until every registered APIService was seen Available.
pub struct APIServiceAvailableCheck {
    pending: std::sync::Arc<Mutex<BTreeSet<String>>>,
}

impl APIServiceAvailableCheck {
    pub fn new(api_services: &[APIService]) -> Self {
        Self {
            pending: std::sync::Arc::new(Mutex::new(
                api_services
                    .iter()
                    .map(|s| s.metadata.name.clone())
                    .collect(),
            )),
        }
    }
    /// `handleAPIServiceChange` (aggregator.go:275-284): a pending APIService
    /// seen `Available=True` leaves the pending list.
    pub fn handle_api_service_change(&self, service: &APIService) {
        let mut pending = self.pending.lock().unwrap();
        if !pending.contains(&service.metadata.name) {
            return;
        }
        if service.status.conditions.iter().any(|c| {
            c.type_ == rusternetes_common::resources::apiregistration::AVAILABLE
                && c.status == "True"
        }) {
            pending.remove(&service.metadata.name);
        }
    }
    /// The named check (aggregator.go:293-300); the message formats the sorted
    /// names like Go's `fmt` of `[]string` (`[a b]`).
    pub fn check(&self) -> Result<(), String> {
        let pending = self.pending.lock().unwrap();
        if pending.is_empty() {
            return Ok(());
        }
        Err(format!(
            "missing APIService: [{}]",
            pending.iter().cloned().collect::<Vec<_>>().join(" ")
        ))
    }
}

/// Name of the boot-sequence check (aggregator.go:~196).
pub const AUTOREGISTER_COMPLETION_CHECK: &str = "autoregister-completion";
/// Name of the post-start hook (aggregator.go:~170).
pub const AUTOREGISTRATION_HOOK: &str = "kube-apiserver-autoregistration";

/// The `kube-apiserver-autoregistration` PostStartHook (aggregator.go:150-181)
/// plus its `autoregister-completion` boot-sequence check (:183-193), over
/// `storage`. `listed_paths` is the delegate's `ListedPaths()`.
///
/// This server always serves CRDs, so it takes upstream's `crdAPIEnabled ==
/// true` branch: `go crdRegistrationController.Run(5, ...)` (:164-166), and the
/// autoregistration controller starts only after `WaitForInitialSync()`
/// (:172-176) so its initial sync cannot delete APIServices of existing CRDs.
///
/// Both are event-driven like upstream: [`spawn_api_service_event_handlers`]
/// feeds the workqueue and the completion check from an APIService watch.
///
/// Deviation (tracked in #2916): the client is the API server's own storage
/// rather than the loopback registry-store client.
pub fn spawn_autoregistration<S: Storage + 'static>(
    storage: std::sync::Arc<S>,
    listed_paths: Vec<String>,
) -> tokio::task::JoinHandle<()> {
    let controller = std::sync::Arc::new(AutoRegisterController::new());
    let api_services = api_services_to_register(&listed_paths, &controller);
    let check = std::sync::Arc::new(APIServiceAvailableCheck::new(&api_services));
    crate::post_start_hooks::global().register_boot_check(AUTOREGISTER_COMPLETION_CHECK, {
        let check = check.clone();
        move || check.check()
    });
    crate::post_start_hooks::spawn_starting_hook(AUTOREGISTRATION_HOOK, move || {
        let storage_for_crds = storage.clone();
        let storage_for_events = storage.clone();
        let client: Arc<dyn APIServiceClient> = Arc::new(StorageAPIServiceClient(storage));
        // `Run(5, context.Done())`: the stop channel never fires for the
        // process lifetime.
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let crd_controller =
            std::sync::Arc::new(crate::crdregistration::CrdRegistrationController::new(
                crate::crdregistration::StorageCrdLister(storage_for_crds.clone()),
                controller.clone(),
            ));
        let c = controller.clone();
        tokio::spawn(async move {
            let _keep = stop_tx;
            // Event handlers first, so no CRD event falls between the initial
            // list and the watch.
            crate::crdregistration::spawn_crd_event_handlers(
                storage_for_crds,
                crd_controller.clone(),
            )
            .await;
            // The APIService informer's handlers (also drive the completion
            // check's Available observation).
            spawn_api_service_event_handlers(storage_for_events, c.clone(), check).await;
            // `go crdRegistrationController.Run(5, context.Done())`
            tokio::spawn(crd_controller.clone().run(5, stop_rx.clone()));
            // "let the CRD controller process the initial set of CRDs before
            // starting the autoregistration controller."
            tracing::info!("waiting for initial CRD sync...");
            crd_controller.wait_for_initial_sync().await;
            tracing::info!("initial CRD sync complete...");
            c.run(client, 5, stop_rx).await;
        });
    })
}
