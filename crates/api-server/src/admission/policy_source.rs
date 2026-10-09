//! The policy/binding source of the generic policy plugin (#2908, part of
//! #2886 / #2731): the informer-equivalent over storage that turns
//! MutatingAdmissionPolicy and MutatingAdmissionPolicyBinding objects into
//! `PolicyHook`s for the dispatcher.
//!
//! Ported from, in `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/generic/`:
//! - `policy_source.go` (`policySource`, `NewPolicySource`, `Run`,
//!   `refreshPolicies`, `calculatePolicyData`, `ensureParamsForPolicyLocked`,
//!   `compilePolicyLocked`, `Hooks`, `HasSynced`)
//! - `plugin.go` / `interfaces.go` (`Source`, `PolicyHook`)
//!
//! Tests are ported from `policy_source_test.go`
//! (`TestPolicySourceHasSyncedEmpty`, `TestPolicySourceHasSyncedInitialList`,
//! `TestPolicySourceBindsToPolicies`); the extra tests pin the branches of
//! `calculatePolicyData` and `compilePolicyLocked` that upstream exercises only
//! through the integration suite.
//!
//! Deviations, all Rust-shaped rather than behavioural:
//! - There is no client-go. [`StorageInformer`] is the `SharedIndexInformer`
//!   equivalent (list + watch into a cache, `HasSynced`, event handlers) and
//!   [`StorageParamInformerFactory`] stands for the `informerFactory` /
//!   `dynamicinformer` pair of `ensureParamsForPolicyLocked`; both sit behind
//!   traits so the source does not know about storage.
//! - `meta.RESTMapper` is the [`RestMapper`] trait; no implementation backed by
//!   the API registry exists yet.
//! - Upstream iterates a Go map (random order); the hooks here are sorted by
//!   policy key so the result is deterministic.
//! - `Run` stops when its future is dropped (upstream: `ctx.Done()`); param
//!   informers stop when their [`ParamInformer`] handle is dropped (upstream:
//!   `cancelFunc`).

// Not called from the request path yet; the bin target compiles `admission`
// separately and would flag every item.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use rusternetes_common::admission::GroupVersionResource;
use rusternetes_common::resources::validating_admission_policy::{
    FailurePolicy, MatchResources, ParamKind, ParamRef,
};
use rusternetes_common::resources::{MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding};
use rusternetes_common::types::Selector;
use rusternetes_common::Error;
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::policy_dispatch::{
    BindingAccessor, NamespacedName, ParamScope, ParamStore, PolicyAccessor, PolicyHook,
};

/// policy_source.go:47 `policyRefreshIntervalDefault`.
const POLICY_REFRESH_INTERVAL_DEFAULT: Duration = Duration::from_secs(1);
/// client-go `syncedPollPeriod` (`tools/cache/shared_informer.go`).
const SYNCED_POLL_PERIOD: Duration = Duration::from_millis(100);
/// Delay before a [`StorageInformer`] relists after its watch ended or failed
/// (client-go's reflector backs off 800ms initially).
const RELIST_BACKOFF: Duration = Duration::from_millis(800);

// ---------------------------------------------------------------------------
// Objects the source can hold.
// ---------------------------------------------------------------------------

/// `meta.Accessor(policySpec).GetResourceVersion()`, which `compilePolicyLocked`
/// compares to decide whether a cached evaluator is still current.
pub trait HasResourceVersion {
    fn resource_version(&self) -> &str;
}

impl PolicyAccessor for MutatingAdmissionPolicy {
    fn name(&self) -> &str {
        &self.metadata.name
    }
    fn namespace(&self) -> &str {
        self.metadata.namespace.as_deref().unwrap_or("")
    }
    fn param_kind(&self) -> Option<&ParamKind> {
        self.spec.as_ref()?.param_kind.as_ref()
    }
    fn match_constraints(&self) -> Option<&MatchResources> {
        self.spec.as_ref()?.match_constraints.as_ref()
    }
    fn failure_policy(&self) -> Option<&FailurePolicy> {
        self.spec.as_ref()?.failure_policy.as_ref()
    }
}

impl HasResourceVersion for MutatingAdmissionPolicy {
    fn resource_version(&self) -> &str {
        self.metadata.resource_version.as_deref().unwrap_or("")
    }
}

impl BindingAccessor for MutatingAdmissionPolicyBinding {
    fn name(&self) -> &str {
        &self.metadata.name
    }
    fn namespace(&self) -> &str {
        self.metadata.namespace.as_deref().unwrap_or("")
    }
    /// mutating/accessor.go `mutatingAdmissionPolicyBindingAccessor.GetPolicyName`:
    /// the policy is cluster-scoped, so only the name.
    fn policy_name(&self) -> NamespacedName {
        NamespacedName {
            name: self
                .spec
                .as_ref()
                .and_then(|s| s.policy_name.clone())
                .unwrap_or_default(),
            namespace: String::new(),
        }
    }
    fn param_ref(&self) -> Option<&ParamRef> {
        self.spec.as_ref()?.param_ref.as_ref()
    }
    fn match_resources(&self) -> Option<&MatchResources> {
        self.spec.as_ref()?.match_resources.as_ref()
    }
}

// ---------------------------------------------------------------------------
// Informer.
// ---------------------------------------------------------------------------

/// `generic.Informer[T]` (internal/generic/interfaces.go): the cached, typed
/// view the source reads, with the `AddEventHandler` / `HasSynced` half of
/// `cache.SharedIndexInformer`.
pub trait Informer<T>: Send + Sync {
    fn has_synced(&self) -> bool;
    /// `Lister().List(labels.Everything())`.
    fn list(&self) -> Vec<Arc<T>>;
    /// `Get` / `Namespaced(ns).Get`; `namespace` is `""` for a cluster-scoped
    /// object. `None` is upstream's `IsNotFound`.
    fn get(&self, namespace: &str, name: &str) -> Option<Arc<T>>;
    /// `AddEventHandler`: `handler` runs after every add, update and delete.
    /// The returned id is the handle for [`Informer::remove_event_handler`].
    fn add_event_handler(&self, handler: Arc<dyn Fn() + Send + Sync>) -> usize;
    fn remove_event_handler(&self, id: usize);
}

/// An informer event handler.
type Handler = Arc<dyn Fn() + Send + Sync>;

/// A list+watch cache over one storage prefix: the `SharedIndexInformer` of
/// this crate. [`StorageInformer::run`] is the reflector loop.
pub struct StorageInformer<T> {
    prefix: String,
    cache: RwLock<HashMap<String, Arc<T>>>,
    synced: AtomicBool,
    handlers: Mutex<(usize, Vec<(usize, Handler)>)>,
}

impl<T: DeserializeOwned + Send + Sync + 'static> StorageInformer<T> {
    /// An informer over `resource_type` (`/registry/{resource_type}/`).
    pub fn new(resource_type: &str) -> Arc<Self> {
        Arc::new(Self {
            prefix: build_prefix(resource_type, None),
            cache: RwLock::new(HashMap::new()),
            synced: AtomicBool::new(false),
            handlers: Mutex::new((0, Vec::new())),
        })
    }

    fn key(&self, namespace: &str, name: &str) -> String {
        if namespace.is_empty() {
            format!("{}{}", self.prefix, name)
        } else {
            format!("{}{}/{}", self.prefix, namespace, name)
        }
    }

    fn notify(&self) {
        let handlers: Vec<_> = self
            .handlers
            .lock()
            .unwrap()
            .1
            .iter()
            .map(|(_, h)| h.clone())
            .collect();
        for h in handlers {
            h();
        }
    }

    fn decode(&self, key: &str, value: &str) -> Option<Arc<T>> {
        match serde_json::from_str::<T>(value) {
            Ok(v) => Some(Arc::new(v)),
            Err(e) => {
                tracing::warn!("policy informer: cannot decode {key}: {e}");
                None
            }
        }
    }

    /// The reflector: watch, list, replace the cache, mark synced, then apply
    /// events. Watching first means an object written between the list and the
    /// watch cannot be missed; an event older than the list is re-applied
    /// idempotently and superseded by the newer event behind it. Runs until the
    /// future is dropped; a failed list or ended watch relists after a backoff.
    pub async fn run<S: Storage + 'static>(self: Arc<Self>, storage: Arc<S>) {
        loop {
            let mut stream = match storage.watch(&self.prefix).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("policy informer {}: watch failed: {e}", self.prefix);
                    tokio::time::sleep(RELIST_BACKOFF).await;
                    continue;
                }
            };
            match storage.list::<Value>(&self.prefix).await {
                Ok(items) => {
                    let mut fresh = HashMap::new();
                    for item in items {
                        let Ok(text) = serde_json::to_string(&item) else {
                            continue;
                        };
                        let key = self.key(
                            item["metadata"]["namespace"].as_str().unwrap_or(""),
                            item["metadata"]["name"].as_str().unwrap_or(""),
                        );
                        if let Some(obj) = self.decode(&key, &text) {
                            fresh.insert(key, obj);
                        }
                    }
                    *self.cache.write().unwrap() = fresh;
                    self.synced.store(true, Ordering::SeqCst);
                    self.notify();
                }
                Err(e) => {
                    tracing::warn!("policy informer {}: list failed: {e}", self.prefix);
                    tokio::time::sleep(RELIST_BACKOFF).await;
                    continue;
                }
            }
            while let Some(event) = stream.next().await {
                match event {
                    Ok(WatchEvent::Added(k, v)) | Ok(WatchEvent::Modified(k, v)) => {
                        if let Some(obj) = self.decode(&k, &v) {
                            self.cache.write().unwrap().insert(k, obj);
                            self.notify();
                        }
                    }
                    Ok(WatchEvent::Deleted(k, _)) => {
                        self.cache.write().unwrap().remove(&k);
                        self.notify();
                    }
                    Err(e) => {
                        tracing::warn!("policy informer {}: watch error: {e}", self.prefix);
                        break;
                    }
                }
            }
            tokio::time::sleep(RELIST_BACKOFF).await;
        }
    }
}

impl<T: DeserializeOwned + Send + Sync + 'static> Informer<T> for StorageInformer<T> {
    fn has_synced(&self) -> bool {
        self.synced.load(Ordering::SeqCst)
    }
    fn list(&self) -> Vec<Arc<T>> {
        self.cache.read().unwrap().values().cloned().collect()
    }
    fn get(&self, namespace: &str, name: &str) -> Option<Arc<T>> {
        self.cache
            .read()
            .unwrap()
            .get(&self.key(namespace, name))
            .cloned()
    }
    fn add_event_handler(&self, handler: Arc<dyn Fn() + Send + Sync>) -> usize {
        let mut guard = self.handlers.lock().unwrap();
        guard.0 += 1;
        let id = guard.0;
        guard.1.push((id, handler));
        id
    }
    fn remove_event_handler(&self, id: usize) {
        self.handlers.lock().unwrap().1.retain(|(i, _)| *i != id);
    }
}

// ---------------------------------------------------------------------------
// Param informers.
// ---------------------------------------------------------------------------

/// `meta.RESTMapping`: the resource a kind is served as and its scope.
#[derive(Debug, Clone, PartialEq)]
pub struct RestMapping {
    pub resource: GroupVersionResource,
    pub scope: ParamScope,
}

/// `meta.RESTMapper.RESTMapping(GroupKind, version)`. An `Err` is any failure
/// to resolve (`NoKindMatchError`, ...), which the source reports as a
/// configuration error.
pub trait RestMapper: Send + Sync {
    fn rest_mapping(&self, group: &str, kind: &str, version: &str) -> Result<RestMapping, String>;
}

/// A running param informer. Dropping it is upstream's `cancelFunc()`.
pub struct ParamInformer {
    pub store: Arc<dyn ParamStore>,
    cancel: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl ParamInformer {
    pub fn new(store: Arc<dyn ParamStore>, cancel: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            store,
            cancel: Some(Box::new(cancel)),
        }
    }
}

impl Drop for ParamInformer {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }
}

/// The `informerFactory.ForResource` / `dynamicinformer` fallback of
/// `ensureParamsForPolicyLocked` (policy_source.go:414-441): start an informer
/// for a param resource.
pub trait ParamInformerFactory: Send + Sync {
    fn start(&self, mapping: &RestMapping) -> ParamInformer;
}

/// A [`ParamStore`] over an `Informer<Value>`: the `GenericInformer.Lister()`.
pub struct InformerParamStore {
    informer: Arc<dyn Informer<Value>>,
}

impl InformerParamStore {
    pub fn new(informer: Arc<dyn Informer<Value>>) -> Self {
        Self { informer }
    }
}

#[async_trait]
impl ParamStore for InformerParamStore {
    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, Error> {
        self.informer
            .get(namespace.unwrap_or(""), name)
            .map(|v| (*v).clone())
            .ok_or_else(|| Error::NotFound(format!("{name:?} not found")))
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        selector: &Selector,
    ) -> Result<Vec<Value>, Error> {
        Ok(self
            .informer
            .list()
            .into_iter()
            .filter(|o| match namespace {
                Some(ns) => o["metadata"]["namespace"].as_str().unwrap_or("") == ns,
                None => true,
            })
            .filter(|o| {
                let labels: Option<HashMap<String, String>> =
                    o["metadata"]["labels"].as_object().map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
                            .collect()
                    });
                selector.matches(labels.as_ref())
            })
            .map(|o| (*o).clone())
            .collect())
    }

    fn has_synced(&self) -> bool {
        self.informer.has_synced()
    }
}

/// Starts a [`StorageInformer`] per param resource, on its own task, stopped
/// when the [`ParamInformer`] is dropped.
pub struct StorageParamInformerFactory<S> {
    storage: Arc<S>,
    /// The storage resource type (`/registry/{type}/`) a param resource lives
    /// under; custom resources are not keyed like built-ins.
    resource_type_for: Arc<dyn Fn(&GroupVersionResource) -> String + Send + Sync>,
}

impl<S: Storage + 'static> StorageParamInformerFactory<S> {
    pub fn new(
        storage: Arc<S>,
        resource_type_for: impl Fn(&GroupVersionResource) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            storage,
            resource_type_for: Arc::new(resource_type_for),
        }
    }
}

impl<S: Storage + 'static> ParamInformerFactory for StorageParamInformerFactory<S> {
    fn start(&self, mapping: &RestMapping) -> ParamInformer {
        let informer = StorageInformer::<Value>::new(&(self.resource_type_for)(&mapping.resource));
        let task = tokio::spawn(informer.clone().run(self.storage.clone()));
        ParamInformer::new(Arc::new(InformerParamStore::new(informer)), move || {
            task.abort()
        })
    }
}

// ---------------------------------------------------------------------------
// The source.
// ---------------------------------------------------------------------------

/// `schema.ParseGroupVersion` (apimachinery/pkg/runtime/schema/group_version.go).
fn parse_group_version(gv: &str) -> Result<(String, String), String> {
    if gv.is_empty() || gv == "/" {
        return Ok((String::new(), String::new()));
    }
    match gv.matches('/').count() {
        0 => Ok((String::new(), gv.to_string())),
        1 => {
            let (g, v) = gv.split_once('/').expect("one slash");
            Ok((g.to_string(), v.to_string()))
        }
        _ => Err(format!("unexpected GroupVersion string: {gv}")),
    }
}

/// The compiled hook list, shared with readers.
pub type Hooks<P, B, E> = Arc<Vec<PolicyHook<P, B, E>>>;

struct CompiledPolicyEntry<E> {
    policy_version: String,
    evaluator: E,
}

struct ParamInfo {
    mapping: RestMapping,
    informer: ParamInformer,
}

/// The mutable half of `policySource`, guarded by its `lock`.
struct SourceState<E> {
    compiled_policies: HashMap<NamespacedName, CompiledPolicyEntry<E>>,
    params_controllers: HashMap<(String, String, String), ParamInfo>,
}

/// `policySource[P, B, E]`.
pub struct PolicySource<P, B, E> {
    policy_informer: Arc<dyn Informer<P>>,
    binding_informer: Arc<dyn Informer<B>>,
    rest_mapper: Arc<dyn RestMapper>,
    param_factory: Arc<dyn ParamInformerFactory>,
    compiler: Arc<dyn Fn(&P) -> E + Send + Sync>,
    refresh_interval: Duration,
    /// Currently compiled list of valid/active policy-binding pairs. As an
    /// invariant `None` is only the not-yet-compiled state (policy_source.go:218-221).
    policies: RwLock<Option<Hooks<P, B, E>>>,
    /// Whether the cache of policies is dirty and needs to be recompiled.
    policies_dirty: Arc<AtomicBool>,
    running: AtomicBool,
    state: Mutex<SourceState<E>>,
}

impl<P, B, E> PolicySource<P, B, E>
where
    P: PolicyAccessor + HasResourceVersion + Clone + Send + Sync + 'static,
    B: BindingAccessor + Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    /// `NewPolicySource` (policy_source.go:108-131).
    pub fn new(
        policy_informer: Arc<dyn Informer<P>>,
        binding_informer: Arc<dyn Informer<B>>,
        compiler: impl Fn(&P) -> E + Send + Sync + 'static,
        param_factory: Arc<dyn ParamInformerFactory>,
        rest_mapper: Arc<dyn RestMapper>,
    ) -> Arc<Self> {
        Arc::new(Self {
            policy_informer,
            binding_informer,
            rest_mapper,
            param_factory,
            compiler: Arc::new(compiler),
            refresh_interval: POLICY_REFRESH_INTERVAL_DEFAULT,
            policies: RwLock::new(None),
            policies_dirty: Arc::new(AtomicBool::new(false)),
            running: AtomicBool::new(false),
            state: Mutex::new(SourceState {
                compiled_policies: HashMap::new(),
                params_controllers: HashMap::new(),
            }),
        })
    }

    /// `SetPolicyRefreshIntervalForTests`, per source rather than global.
    pub fn with_refresh_interval(self: Arc<Self>, interval: Duration) -> Arc<Self> {
        match Arc::try_unwrap(self) {
            Ok(mut s) => {
                s.refresh_interval = interval;
                Arc::new(s)
            }
            Err(_) => panic!("with_refresh_interval must be called before the source is shared"),
        }
    }

    /// `Run` (policy_source.go:146-210). Returns an error if already running;
    /// otherwise runs until the future is dropped.
    pub async fn run(self: Arc<Self>) -> Result<(), String> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err("policy source already running".into());
        }

        // Wait for initial cache sync of policies and informers before
        // reconciling any.
        while !self.upstream_has_synced() {
            tokio::time::sleep(SYNCED_POLL_PERIOD).await;
        }

        // Perform initial policy compilation after initial list has finished.
        self.notify();
        self.refresh_policies();

        let flag = self.policies_dirty.clone();
        let notify: Arc<dyn Fn() + Send + Sync> =
            Arc::new(move || flag.store(true, Ordering::SeqCst));
        let policy_handle = self.policy_informer.add_event_handler(notify.clone());
        let binding_handle = self.binding_informer.add_event_handler(notify);
        let _guard = HandlerGuard {
            source: self.clone(),
            policy_handle,
            binding_handle,
        };

        // Start a worker that checks every interval to see if policy data is
        // dirty and needs to be recompiled.
        let mut ticker = tokio::time::interval(self.refresh_interval);
        loop {
            ticker.tick().await;
            self.refresh_policies();
        }
    }

    /// policy_source.go:212-214.
    pub fn upstream_has_synced(&self) -> bool {
        self.policy_informer.has_synced() && self.binding_informer.has_synced()
    }

    /// `HasSynced` (policy_source.go:217-222).
    pub fn has_synced(&self) -> bool {
        self.hooks().is_some()
    }

    /// `Hooks` (policy_source.go:225-236): `None` until the first compilation.
    pub fn hooks(&self) -> Option<Hooks<P, B, E>> {
        self.policies.read().unwrap().clone()
    }

    /// `refreshPolicies` (policy_source.go:238-264).
    pub fn refresh_policies(&self) {
        // `||` short-circuits: the dirty flag is only cleared once synced.
        if !self.upstream_has_synced() || !self.policies_dirty.swap(false, Ordering::SeqCst) {
            return;
        }

        // It is ok the cache gets marked dirty again between us clearing the
        // flag and us calculating the policies. The dirty flag would be marked
        // again, and we'd have a no-op after comparing resource versions on the
        // next sync.
        tracing::info!("refreshing policies");
        let (policies, err) = self.calculate_policy_data();

        // Intentionally store policy list regardless of error: a policy with a
        // configuration error must still be evaluated (to fail on failurePolicy),
        // and an error listing everything wipes the list.
        *self.policies.write().unwrap() = Some(Arc::new(policies));

        if let Some(err) = err {
            // An error was generated while syncing policies. Mark it as dirty
            // again so we can retry later.
            tracing::error!("encountered error syncing policies: {err}. Rescheduling policy sync");
            self.notify();
        }
    }

    fn notify(&self) {
        self.policies_dirty.store(true, Ordering::SeqCst);
    }

    /// `calculatePolicyData` (policy_source.go:277-387): the hooks plus the
    /// joined errors (`errors.Join`, newline separated). A policy with an error
    /// still appears, with `configuration_error` set.
    fn calculate_policy_data(&self) -> (Vec<PolicyHook<P, B, E>>, Option<String>) {
        if !self.upstream_has_synced() {
            return (
                Vec::new(),
                Some("cannot calculate policy data until upstream has synced".into()),
            );
        }

        // Fat-fingered lock that can be made more fine-tuned if required.
        let mut state = self.state.lock().unwrap();

        // Create a local copy of all policies and bindings.
        let mut policies_to_bindings: HashMap<NamespacedName, Vec<B>> = HashMap::new();
        let binding_list = self.binding_informer.list();
        let mut order: Vec<NamespacedName> = Vec::new();

        // Gather a list of all active policy bindings.
        for binding in &binding_list {
            let policy_key = binding.policy_name();
            let entry = policies_to_bindings.entry(policy_key.clone());
            if matches!(entry, std::collections::hash_map::Entry::Vacant(_)) {
                order.push(policy_key);
            }
            // Bindings are held by Arc in the informer; the hook owns a copy.
            entry.or_default().push((**binding).clone());
        }
        order.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

        let mut result = Vec::with_capacity(binding_list.len());
        let mut used_params: HashSet<(String, String, String)> = HashSet::new();
        let mut errs: Vec<String> = Vec::new();
        for policy_key in &order {
            let binding_specs = policies_to_bindings
                .remove(policy_key)
                .expect("keyed by order");
            let Some(policy_spec) = self
                .policy_informer
                .get(&policy_key.namespace, &policy_key.name)
            else {
                // Policy for bindings doesn't exist. This can happen if the
                // policy was deleted before the binding, or the binding was
                // created first. Just skip bindings that refer to non-existent
                // policies; if the policy is recreated, the cache will be
                // marked dirty and this function will run again.
                continue;
            };

            let mut parsed_param_kind: Option<(String, String, String)> = None;
            if let Some(param_kind) = policy_spec.param_kind() {
                match parse_group_version(param_kind.api_version.as_deref().unwrap_or("")) {
                    Ok((group, version)) => {
                        parsed_param_kind = Some((group, version, param_kind.kind.clone()));
                        // TEMPORARY UNTIL WE HAVE SHARED PARAM INFORMERS
                        used_params.insert(parsed_param_kind.clone().expect("just set"));
                    }
                    Err(e) => {
                        errs.push(format!("failed to parse paramKind APIVersion: {e}"));
                        continue;
                    }
                }
            }

            let (param_store, param_scope, configuration_error) =
                self.ensure_params_for_policy_locked(&mut state, parsed_param_kind.as_ref());
            let evaluator = self.compile_policy_locked(&mut state, &policy_spec);
            result.push(PolicyHook {
                policy: (*policy_spec).clone(),
                bindings: binding_specs,
                param_store,
                param_scope,
                evaluator,
                configuration_error: configuration_error.clone(),
            });

            // Should queue a re-sync for policy sync error. If our shared param
            // informer can notify us when CRD discovery changes we can remove
            // this and just rely on the informer to notify us when the CRDs
            // change.
            if let Some(e) = configuration_error {
                errs.push(e);
            }
        }

        // Clean up orphaned policies by replacing the old cache of compiled
        // policies (the map of used policies is updated by `compile_policy`).
        let seen: HashSet<&NamespacedName> = order.iter().collect();
        state.compiled_policies.retain(|k, _| seen.contains(k));

        // Clean up orphaned param informers.
        state
            .params_controllers
            .retain(|k, _| used_params.contains(k));

        let err = if errs.is_empty() {
            None
        } else {
            Some(errs.join("\n"))
        };
        (result, err)
    }

    /// `ensureParamsForPolicyLocked` (policy_source.go:393-451): start the
    /// informer for the paramKind, if not yet started, and return its store and
    /// scope.
    fn ensure_params_for_policy_locked(
        &self,
        state: &mut SourceState<E>,
        param_source: Option<&(String, String, String)>,
    ) -> (Option<Arc<dyn ParamStore>>, ParamScope, Option<String>) {
        let Some(source) = param_source else {
            return (None, ParamScope::Namespace, None);
        };
        if let Some(info) = state.params_controllers.get(source) {
            return (Some(info.informer.store.clone()), info.mapping.scope, None);
        }

        let (group, version, kind) = source;
        let mapping = match self.rest_mapper.rest_mapping(group, kind, version) {
            Ok(m) => m,
            Err(_) => {
                // Failed to resolve. Return error so we retry again (rate
                // limited).
                return (
                    None,
                    ParamScope::Namespace,
                    Some(format!(
                        "failed to find resource referenced by paramKind: '{{{group} {version} {kind}}}'"
                    )),
                );
            }
        };

        // We are not watching this param. Start an informer for it.
        let informer = self.param_factory.start(&mapping);
        let store = informer.store.clone();
        let scope = mapping.scope;
        state
            .params_controllers
            .insert(source.clone(), ParamInfo { mapping, informer });
        tracing::info!("informer started for {{{group} {version} {kind}}}");
        (Some(store), scope, None)
    }

    /// `compilePolicyLocked` (policy_source.go:470-501): the cached evaluator
    /// unless the policy's resource version changed.
    fn compile_policy_locked(&self, state: &mut SourceState<E>, policy: &Arc<P>) -> E {
        let key = NamespacedName {
            namespace: policy.namespace().to_string(),
            name: policy.name().to_string(),
        };
        let version = policy.resource_version();
        if let Some(entry) = state.compiled_policies.get(&key) {
            if entry.policy_version == version {
                return entry.evaluator.clone();
            }
        }
        let evaluator = (self.compiler)(policy);
        state.compiled_policies.insert(
            key,
            CompiledPolicyEntry {
                policy_version: version.to_string(),
                evaluator: evaluator.clone(),
            },
        );
        evaluator
    }
}

/// `defer RemoveEventHandler` (policy_source.go:182-196).
struct HandlerGuard<P, B, E> {
    source: Arc<PolicySource<P, B, E>>,
    policy_handle: usize,
    binding_handle: usize,
}

impl<P, B, E> Drop for HandlerGuard<P, B, E> {
    fn drop(&mut self) {
        self.source
            .policy_informer
            .remove_event_handler(self.policy_handle);
        self.source
            .binding_informer
            .remove_event_handler(self.binding_handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    // policy_source_test.go FakePolicy / FakeBinding.
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FakePolicy {
        metadata: ObjectMeta,
        #[serde(default)]
        param_kind: Option<ParamKind>,
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FakeBinding {
        metadata: ObjectMeta,
        #[serde(default)]
        policy_name: String,
    }

    impl PolicyAccessor for FakePolicy {
        fn name(&self) -> &str {
            &self.metadata.name
        }
        fn namespace(&self) -> &str {
            self.metadata.namespace.as_deref().unwrap_or("")
        }
        fn param_kind(&self) -> Option<&ParamKind> {
            self.param_kind.as_ref()
        }
        fn match_constraints(&self) -> Option<&MatchResources> {
            None
        }
        fn failure_policy(&self) -> Option<&FailurePolicy> {
            None
        }
    }
    impl HasResourceVersion for FakePolicy {
        fn resource_version(&self) -> &str {
            self.metadata.resource_version.as_deref().unwrap_or("")
        }
    }
    impl BindingAccessor for FakeBinding {
        fn name(&self) -> &str {
            &self.metadata.name
        }
        fn namespace(&self) -> &str {
            ""
        }
        fn policy_name(&self) -> NamespacedName {
            NamespacedName {
                name: self.policy_name.clone(),
                namespace: String::new(),
            }
        }
        fn param_ref(&self) -> Option<&ParamRef> {
            None
        }
        fn match_resources(&self) -> Option<&MatchResources> {
            None
        }
    }

    /// Maps only `policy.example.com/v1 Known`; everything else is an unknown
    /// kind, as upstream's fake RESTMapper does for an unregistered CRD.
    struct FakeMapper;
    impl RestMapper for FakeMapper {
        fn rest_mapping(&self, g: &str, k: &str, v: &str) -> Result<RestMapping, String> {
            if (g, k, v) == ("policy.example.com", "Known", "v1") {
                Ok(RestMapping {
                    resource: GroupVersionResource {
                        group: g.into(),
                        version: v.into(),
                        resource: "knowns".into(),
                    },
                    scope: ParamScope::Root,
                })
            } else {
                Err("no match".into())
            }
        }
    }

    #[derive(Default)]
    struct CountingFactory {
        started: Arc<AtomicUsize>,
        stopped: Arc<AtomicUsize>,
    }
    struct NoStore;
    #[async_trait]
    impl ParamStore for NoStore {
        async fn get(&self, _: Option<&str>, _: &str) -> Result<Value, Error> {
            Err(Error::NotFound("x".into()))
        }
        async fn list(&self, _: Option<&str>, _: &Selector) -> Result<Vec<Value>, Error> {
            Ok(vec![])
        }
        fn has_synced(&self) -> bool {
            true
        }
    }
    impl ParamInformerFactory for CountingFactory {
        fn start(&self, _: &RestMapping) -> ParamInformer {
            self.started.fetch_add(1, Ordering::SeqCst);
            let stopped = self.stopped.clone();
            ParamInformer::new(Arc::new(NoStore), move || {
                stopped.fetch_add(1, Ordering::SeqCst);
            })
        }
    }

    struct Ctx {
        storage: Arc<MemoryStorage>,
        source: Arc<PolicySource<FakePolicy, FakeBinding, usize>>,
        compiles: Arc<AtomicUsize>,
        factory_started: Arc<AtomicUsize>,
        factory_stopped: Arc<AtomicUsize>,
        tasks: Vec<tokio::task::JoinHandle<()>>,
    }

    impl Drop for Ctx {
        fn drop(&mut self) {
            for t in &self.tasks {
                t.abort();
            }
        }
    }

    fn put(kind: &str, name: &str, extra: Value) -> (String, Value) {
        let mut v = json!({"metadata": {"name": name}});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        (format!("/registry/{kind}/{name}"), v)
    }

    /// `NewPolicyTestContext` + `Start`: informers running over a storage seeded
    /// with `initial`, the source running with a 10ms refresh, and `Start`
    /// returning once the source has synced.
    async fn start(initial: Vec<(String, Value)>) -> Ctx {
        let storage = Arc::new(MemoryStorage::new());
        for (k, v) in initial {
            storage.create(&k, &v).await.unwrap();
        }
        let policies = StorageInformer::<FakePolicy>::new("fakepolicies");
        let bindings = StorageInformer::<FakeBinding>::new("fakebindings");
        let compiles = Arc::new(AtomicUsize::new(0));
        let c = compiles.clone();
        let factory = CountingFactory::default();
        let (started, stopped) = (factory.started.clone(), factory.stopped.clone());
        let source = PolicySource::new(
            policies.clone(),
            bindings.clone(),
            move |_p: &FakePolicy| c.fetch_add(1, Ordering::SeqCst) + 1,
            Arc::new(factory),
            Arc::new(FakeMapper),
        )
        .with_refresh_interval(Duration::from_millis(10));
        let tasks = vec![
            tokio::spawn(policies.run(storage.clone())),
            tokio::spawn(bindings.run(storage.clone())),
            tokio::spawn({
                let s = source.clone();
                async move {
                    let _ = s.run().await;
                }
            }),
        ];
        let ctx = Ctx {
            storage,
            source,
            compiles,
            factory_started: started,
            factory_stopped: stopped,
            tasks,
        };
        wait_for(|| ctx.source.has_synced()).await;
        ctx
    }

    async fn wait_for(cond: impl Fn() -> bool) {
        for _ in 0..500 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within 5s");
    }

    impl Ctx {
        /// `UpdateAndWait`'s write half: create or replace each object. Callers
        /// then wait for the condition they expect.
        async fn write(&self, objs: Vec<(String, Value)>) {
            for (k, v) in objs {
                if self.storage.update_raw(&k, &v).await.is_err() {
                    self.storage.create(&k, &v).await.unwrap();
                }
            }
        }
        fn hook_names(&self) -> Vec<String> {
            self.source
                .hooks()
                .map(|h| h.iter().map(|h| h.policy.metadata.name.clone()).collect())
                .unwrap_or_default()
        }
        async fn wait_hooks(&self, n: usize) {
            wait_for(|| self.source.hooks().is_some_and(|h| h.len() == n)).await;
        }
    }

    fn pol(name: &str) -> (String, Value) {
        put("fakepolicies", name, json!({}))
    }
    fn bind(name: &str, policy: &str) -> (String, Value) {
        put("fakebindings", name, json!({"policyName": policy}))
    }

    // policy_source_test.go TestPolicySourceHasSyncedEmpty
    #[tokio::test]
    async fn has_synced_empty() {
        let ctx = start(vec![]).await;
        assert!(ctx.source.has_synced());
        assert_eq!(ctx.source.hooks().unwrap().len(), 0);
    }

    // policy_source_test.go TestPolicySourceHasSyncedInitialList
    #[tokio::test]
    async fn has_synced_initial_list() {
        let ctx = start(vec![pol("policy1"), bind("binding1", "policy1")]).await;
        assert_eq!(
            ctx.source.hooks().unwrap().len(),
            1,
            "should have one policy"
        );

        ctx.write(vec![pol("policy2"), bind("binding2", "policy2")])
            .await;
        ctx.wait_hooks(2).await;

        let mut p2 = pol("policy2");
        p2.1["paramKind"] = json!({"apiVersion": "policy.example.com/v1", "kind": "FakeParam"});
        ctx.write(vec![pol("policy3"), bind("binding3", "policy3"), p2])
            .await;
        ctx.wait_hooks(3).await;
        // policy2's paramKind is unknown to the mapper: still a hook, with a
        // configuration error.
        wait_for(|| {
            ctx.source.hooks().is_some_and(|h| {
                h.iter()
                    .any(|h| h.policy.metadata.name == "policy2" && h.configuration_error.is_some())
            })
        })
        .await;
    }

    // policy_source_test.go TestPolicySourceBindsToPolicies
    #[tokio::test]
    async fn binds_to_policies() {
        let ctx = start(vec![pol("policy1"), bind("binding1", "policy1")]).await;
        let hooks = ctx.source.hooks().unwrap();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].bindings.len(), 1);
        assert_eq!(hooks[0].bindings[0].metadata.name, "binding1");

        // Change the binding to another policy (policies without bindings are
        // ignored, so it removes the first).
        ctx.write(vec![pol("policy2"), bind("binding1", "policy2")])
            .await;
        wait_for(|| ctx.hook_names() == vec!["policy2".to_string()]).await;
        let hooks = ctx.source.hooks().unwrap();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].bindings.len(), 1);
        assert_eq!(hooks[0].bindings[0].metadata.name, "binding1");
    }

    // policy_source.go:312-320: a binding whose policy does not exist is
    // skipped; the policy arriving later makes the hook appear.
    #[tokio::test]
    async fn binding_without_policy_is_skipped_until_policy_exists() {
        let ctx = start(vec![bind("b", "late")]).await;
        assert_eq!(ctx.source.hooks().unwrap().len(), 0);
        ctx.write(vec![pol("late")]).await;
        ctx.wait_hooks(1).await;
    }

    // policy_source.go:490-498: the evaluator is recompiled only when the
    // policy's resource version changes.
    #[tokio::test]
    async fn evaluator_is_cached_by_resource_version() {
        let ctx = start(vec![pol("p"), bind("b1", "p")]).await;
        assert_eq!(ctx.compiles.load(Ordering::SeqCst), 1);

        // A binding change refreshes the hooks but must not recompile `p`.
        ctx.write(vec![bind("b2", "p")]).await;
        wait_for(|| ctx.source.hooks().is_some_and(|h| h[0].bindings.len() == 2)).await;
        assert_eq!(ctx.compiles.load(Ordering::SeqCst), 1);

        // A policy update bumps its resource version: recompiled.
        let mut p = pol("p");
        p.1["paramKind"] = json!({"apiVersion": "v1", "kind": "Whatever"});
        ctx.write(vec![p]).await;
        wait_for(|| ctx.compiles.load(Ordering::SeqCst) == 2).await;
    }

    // policy_source.go:332-348 + 389-451: a known paramKind starts one
    // informer (shared by policies using it) and reports its scope.
    #[tokio::test]
    async fn param_informer_started_once_with_scope() {
        let known = json!({"apiVersion": "policy.example.com/v1", "kind": "Known"});
        let mut p1 = pol("p1");
        p1.1["paramKind"] = known.clone();
        let mut p2 = pol("p2");
        p2.1["paramKind"] = known;
        let ctx = start(vec![p1, p2, bind("b1", "p1"), bind("b2", "p2")]).await;
        let hooks = ctx.source.hooks().unwrap();
        assert_eq!(hooks.len(), 2);
        for h in hooks.iter() {
            assert!(h.param_store.is_some());
            assert_eq!(h.param_scope, ParamScope::Root);
            assert!(h.configuration_error.is_none());
        }
        assert_eq!(ctx.factory_started.load(Ordering::SeqCst), 1);
    }

    // policy_source.go:374-380: an informer nobody uses any more is stopped.
    #[tokio::test]
    async fn orphaned_param_informer_is_stopped() {
        let mut p = pol("p");
        p.1["paramKind"] = json!({"apiVersion": "policy.example.com/v1", "kind": "Known"});
        let ctx = start(vec![p, bind("b", "p")]).await;
        assert_eq!(ctx.factory_started.load(Ordering::SeqCst), 1);
        assert_eq!(ctx.factory_stopped.load(Ordering::SeqCst), 0);

        // Drop the paramKind.
        ctx.write(vec![pol("p")]).await;
        wait_for(|| ctx.factory_stopped.load(Ordering::SeqCst) == 1).await;
    }

    // policy_source.go:332-337: a paramKind with an unparseable apiVersion
    // drops the policy for this pass (no hook), unlike an unmapped kind.
    #[tokio::test]
    async fn unparseable_param_kind_api_version_yields_no_hook() {
        let mut p = pol("p");
        p.1["paramKind"] = json!({"apiVersion": "a/b/c", "kind": "K"});
        let ctx = start(vec![p, bind("b", "p")]).await;
        assert_eq!(ctx.source.hooks().unwrap().len(), 0);
    }

    // policy_source.go:148-150
    #[tokio::test]
    async fn run_twice_is_an_error() {
        let ctx = start(vec![]).await;
        assert_eq!(
            ctx.source.clone().run().await,
            Err("policy source already running".to_string())
        );
    }

    #[test]
    fn parse_group_version_cases() {
        assert_eq!(parse_group_version("v1"), Ok((String::new(), "v1".into())));
        assert_eq!(
            parse_group_version("apps/v1"),
            Ok(("apps".into(), "v1".into()))
        );
        assert!(parse_group_version("a/b/c").is_err());
    }

    #[test]
    fn mutating_admission_policy_binding_policy_name_is_cluster_scoped() {
        use rusternetes_common::resources::mutating_admission_policy::MutatingAdmissionPolicyBindingSpec;
        let mut b = MutatingAdmissionPolicyBinding::new("b");
        b.spec = Some(MutatingAdmissionPolicyBindingSpec {
            policy_name: Some("p".into()),
            param_ref: None,
            match_resources: None,
        });
        assert_eq!(
            b.policy_name(),
            NamespacedName {
                name: "p".into(),
                namespace: String::new()
            }
        );
    }
}
