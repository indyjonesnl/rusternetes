//! The server-side collaborators the MutatingAdmissionPolicy source and plugin
//! need at startup (#3013, part of #2886): a `meta.RESTMapper` over the
//! served resources, a storage-backed `NamespaceObjects`, registry-backed
//! `TypeConverters`, and the `HookSource` adapter over a `PolicySource`.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/`:
//! - `generic/plugin.go` `ValidateInitialization` (:137-198): the plugin is
//!   handed the REST mapper (`SetRESTMapper`), the namespace informer
//!   (`SetExternalKubeInformerFactory`) and the object-interfaces
//!   (`SetObjectInterfaces`), then `source.Run` is started.
//! - `mutating/dispatcher.go` `dispatchOne` (:229-279, the nil type converter check at :244-245): a kind with no type
//!   converter is a 503 `Resource kind ... not found`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use futures::StreamExt;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::resources::Namespace;
use rusternetes_common::Error;
use rusternetes_discovery::restmapper::{
    new_discovery_rest_mapper, served_group_resources, Gk, PriorityRestMapper, Scope,
};
use rusternetes_storage::{build_prefix, Storage, WatchEvent};
use serde_json::Value;

use super::policy_dispatch::ParamScope;
use super::policy_matching::EquivalentResourceMapper;
use super::policy_mutating::{
    compile_policy, MutatingEvaluator, NamespaceObjects, ObjectDefaulter, TypeConverters,
};
use super::policy_plugin::{HookSource, MutatingPolicyPlugin};
use super::policy_registry::{EquivalentResourceRegistry, StorageNamespaceLister};
use super::policy_source::{
    Informer, ParamInformerFactory, PolicySource, RestMapper, RestMapping, StorageInformer,
    StorageParamInformerFactory,
};
use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::mutating_admission_policy::{
    MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding,
};

/// The mutating plugin's policy source, compiling every policy with
/// [`compile_policy`].
///
/// Upstream, staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
/// mutating/plugin.go:98-103 (`NewPlugin`):
/// ```text
/// return generic.NewPolicySource(
///     ...
///     compilePolicy,
/// ```
/// into generic/policy_source.go:108-117 `NewPolicySource(..., compiler func(P) E, ...)`.
/// `compilePolicyLocked` (generic/policy_source.go:485-500) caches the compiled
/// evaluator per policy key and returns it by value on every sync:
/// ```text
/// compiledPolicy, wasCompiled := s.compiledPolicies[key]
/// if !wasCompiled ||
///     compiledPolicy.policyVersion != policyMeta.GetResourceVersion() {
///     compiledPolicy = compiledPolicyEntry[E]{
///         policyVersion: policyMeta.GetResourceVersion(),
///         evaluator:     s.compiler(policySpec),
///     }
///     s.compiledPolicies[key] = compiledPolicy
/// }
/// return compiledPolicy.evaluator
/// ```
/// `E` is the `PolicyEvaluator` interface/pointer, so the "copy" shares the
/// compiled matcher and patchers. `MutatingEvaluator: Clone` (Arc fields)
/// reproduces exactly that sharing; `PolicySource::compile_policy_locked`
/// requires `E: Clone` for the same cache-and-return step.
pub fn new_mutating_policy_source(
    policy_informer: Arc<dyn Informer<MutatingAdmissionPolicy>>,
    binding_informer: Arc<dyn Informer<MutatingAdmissionPolicyBinding>>,
    param_factory: Arc<dyn ParamInformerFactory>,
    rest_mapper: Arc<dyn RestMapper>,
) -> Arc<PolicySource<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>> {
    PolicySource::new(
        policy_informer,
        binding_informer,
        compile_policy,
        param_factory,
        rest_mapper,
    )
}

/// A synchronous snapshot of the CustomResourceDefinitions, kept current by a
/// storage watch ([`CrdSnapshot::spawn`]); the `RestMapper` trait is sync.
///
/// Deviation (stated): upstream's mapper is
/// `restmapper.NewDeferredDiscoveryRESTMapper(cacheddiscovery.NewMemCacheClient(
/// kubeClient.Discovery()))` (staging/src/k8s.io/apiserver/pkg/server/options/
/// admission.go:156-157), which invalidates its cache and retries once on a
/// `NoMatch` (client-go restmapper/discovery.go `DeferredDiscoveryRESTMapper`).
/// Here the CRD set is local, so the cache is invalidated by CRD change
/// (`generation`) instead of by a miss.
#[derive(Default)]
pub struct CrdSnapshot {
    crds: RwLock<BTreeMap<String, Value>>,
    generation: AtomicU64,
}

impl CrdSnapshot {
    /// Insert or replace one CRD and invalidate dependent mappers.
    pub fn apply(&self, name: &str, crd: Value) {
        self.crds.write().unwrap().insert(name.to_string(), crd);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Remove one CRD and invalidate dependent mappers.
    pub fn remove(&self, name: &str) {
        self.crds.write().unwrap().remove(name);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    fn values(&self) -> Vec<Value> {
        self.crds.read().unwrap().values().cloned().collect()
    }

    fn name_of(crd: &Value) -> Option<String> {
        crd.pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .map(str::to_string)
    }

    /// Watch first, then list, so no event falls between the two (the same
    /// order as `crdregistration::spawn_crd_event_handlers`); later watch
    /// events are applied by a background task.
    pub async fn spawn<S: Storage + 'static>(storage: Arc<S>) -> Arc<Self> {
        let snapshot = Arc::new(Self::default());
        let prefix = build_prefix("customresourcedefinitions", None);
        let mut stream = match storage.watch(&prefix).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("CRD snapshot: cannot watch CRDs: {e}");
                return snapshot;
            }
        };
        match storage.list::<Value>(&prefix).await {
            Ok(list) => {
                for crd in list {
                    if let Some(name) = Self::name_of(&crd) {
                        snapshot.apply(&name, crd);
                    }
                }
            }
            Err(e) => tracing::error!("CRD snapshot: cannot list CRDs: {e}"),
        }
        let task = snapshot.clone();
        tokio::spawn(async move {
            while let Some(ev) = stream.next().await {
                let Ok(ev) = ev else { continue };
                match ev {
                    WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => {
                        let Ok(crd) = serde_json::from_str::<Value>(&v) else {
                            continue;
                        };
                        if let Some(name) = Self::name_of(&crd) {
                            task.apply(&name, crd);
                        }
                    }
                    WatchEvent::Deleted(_, v) => {
                        let Ok(crd) = serde_json::from_str::<Value>(&v) else {
                            continue;
                        };
                        if let Some(name) = Self::name_of(&crd) {
                            task.remove(&name);
                        }
                    }
                }
            }
        });
        snapshot
    }
}

/// The `EquivalentResourceMapper` the matcher reads: the built-in registry
/// ([`EquivalentResourceRegistry::from_discovery`], installer.go:1127) plus
/// every established CustomResourceDefinition.
///
/// Ported from staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/
/// customresource_handler.go:762-768 (`getOrCreateServingInfoFor`): for every
/// version in `crd.Spec.Versions` (served or not), `RegisterKindFor(resource,
/// "", kind)`, plus `"status"` with the same kind when the version has a status
/// subresource and `"scale"` with `autoscaling/v1 Scale` when it has a scale
/// subresource. The resource and kind are `crd.Status.AcceptedNames` (:716-736,
/// which `getOrCreateServingInfoFor` requires non-empty; here `spec.names` is
/// the fallback until status is populated).
///
/// Deviation (stated): upstream builds a fresh registry per CRD serving-info
/// (:651), dropped with the CRD's storage on update/delete. Here one registry
/// is rebuilt from the [`CrdSnapshot`] whenever the CRD set changed, which has
/// the same lifetime semantics (a deleted or renamed CRD leaves no entries).
/// Only Established CRDs are registered, as only those get a serving-info
/// (customresource_handler.go `ServeHTTP` :300-313).
pub struct CrdEquivalentRegistry {
    builtin: EquivalentResourceRegistry,
    crds: Arc<CrdSnapshot>,
    cached: Mutex<Option<(u64, Arc<EquivalentResourceRegistry>)>>,
}

impl CrdEquivalentRegistry {
    pub fn new(builtin: EquivalentResourceRegistry, crds: Arc<CrdSnapshot>) -> Self {
        Self {
            builtin,
            crds,
            cached: Mutex::new(None),
        }
    }

    fn crd_registry(&self) -> Arc<EquivalentResourceRegistry> {
        let generation = self.crds.generation();
        let mut cached = self.cached.lock().unwrap();
        if let Some((g, r)) = cached.as_ref() {
            if *g == generation {
                return r.clone();
            }
        }
        let reg = Arc::new(EquivalentResourceRegistry::new());
        for crd in self
            .crds
            .values()
            .iter()
            .filter(|c| rusternetes_discovery::crd_established(c))
        {
            register_crd(&reg, crd);
        }
        *cached = Some((generation, reg.clone()));
        reg
    }
}

/// customresource_handler.go:754-769 for one CRD document.
fn register_crd(reg: &EquivalentResourceRegistry, crd: &Value) {
    let text = |ptr: &str| crd.pointer(ptr).and_then(|v| v.as_str()).unwrap_or("");
    let group = text("/spec/group");
    let accepted = |field: &str| match text(&format!("/status/acceptedNames/{field}")) {
        "" => text(&format!("/spec/names/{field}")),
        s => s,
    };
    let (plural, kind) = (accepted("plural"), accepted("kind"));
    if plural.is_empty() || kind.is_empty() {
        return;
    }
    for ver in crd
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        let Some(name) = ver.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let resource = GroupVersionResource {
            group: group.to_string(),
            version: name.to_string(),
            resource: plural.to_string(),
        };
        let own_kind = GroupVersionKind {
            group: group.to_string(),
            version: name.to_string(),
            kind: kind.to_string(),
        };
        // `GetSubresourcesForVersion`: the per-version field, or the
        // top-level (v1beta1-era) one.
        let has = |sub: &str| {
            ver.pointer(&format!("/subresources/{sub}"))
                .is_some_and(|v| !v.is_null())
                || crd
                    .pointer(&format!("/spec/subresources/{sub}"))
                    .is_some_and(|v| !v.is_null())
        };
        reg.register_kind_for(resource.clone(), "", own_kind.clone());
        if has("status") {
            reg.register_kind_for(resource.clone(), "status", own_kind);
        }
        if has("scale") {
            reg.register_kind_for(
                resource,
                "scale",
                GroupVersionKind {
                    group: "autoscaling".into(),
                    version: "v1".into(),
                    kind: "Scale".into(),
                },
            );
        }
    }
}

impl EquivalentResourceMapper for CrdEquivalentRegistry {
    fn equivalent_resources_for(
        &self,
        resource: &GroupVersionResource,
        subresource: &str,
    ) -> Vec<GroupVersionResource> {
        let builtin = self.builtin.equivalent_resources_for(resource, subresource);
        if !builtin.is_empty() {
            return builtin;
        }
        self.crd_registry()
            .equivalent_resources_for(resource, subresource)
    }

    fn kind_for(&self, resource: &GroupVersionResource, subresource: &str) -> GroupVersionKind {
        let builtin = self.builtin.kind_for(resource, subresource);
        if !builtin.kind.is_empty() {
            return builtin;
        }
        self.crd_registry().kind_for(resource, subresource)
    }
}

/// `meta.RESTMapper` over the resources this server serves: the built-in
/// groups plus the groups of the established CustomResourceDefinitions
/// (#3597), so a MAP matching/param kind defined by a CRD resolves.
///
/// The mapper is rebuilt only when the CRD set changed.
#[derive(Default)]
pub struct DiscoveryRestMapper {
    crds: Option<Arc<CrdSnapshot>>,
    cached: Mutex<Option<(u64, Arc<PriorityRestMapper>)>>,
}

impl DiscoveryRestMapper {
    pub fn new(crds: Arc<CrdSnapshot>) -> Self {
        Self {
            crds: Some(crds),
            cached: Mutex::new(None),
        }
    }

    fn mapper(&self) -> Arc<PriorityRestMapper> {
        let generation = self.crds.as_ref().map_or(0, |c| c.generation());
        let mut cached = self.cached.lock().unwrap();
        if let Some((g, m)) = cached.as_ref() {
            if *g == generation {
                return m.clone();
            }
        }
        let crds = self.crds.as_ref().map(|c| c.values()).unwrap_or_default();
        let m = Arc::new(new_discovery_rest_mapper(&served_group_resources(&crds)));
        *cached = Some((generation, m.clone()));
        m
    }
}

impl RestMapper for DiscoveryRestMapper {
    /// `RESTMapping(GroupKind, version)`: a kind that is not served is
    /// upstream's `NoKindMatchError`.
    ///
    /// Delegates to the ported `PriorityRESTMapper` over the served discovery
    /// (`restmapper.NewDiscoveryRESTMapper`, #3519), so this and the
    /// custom.metrics handler share one kind -> resource path.
    fn rest_mapping(&self, group: &str, kind: &str, version: &str) -> Result<RestMapping, String> {
        let mapper = self.mapper();
        let mapping = mapper
            .rest_mapping(&Gk::new(group, kind), &[version])
            .map_err(|e| e.to_string())?;
        Ok(RestMapping {
            resource: GroupVersionResource {
                group: mapping.resource.group,
                version: mapping.resource.version,
                resource: mapping.resource.resource,
            },
            scope: match mapping.scope {
                Scope::Namespace => ParamScope::Namespace,
                Scope::Root => ParamScope::Root,
            },
        })
    }
}

/// `matcher.GetNamespace` over storage.
pub struct StorageNamespaceObjects<S: Storage> {
    storage: Arc<S>,
}

impl<S: Storage> StorageNamespaceObjects<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: Storage> NamespaceObjects for StorageNamespaceObjects<S> {
    async fn get_namespace(&self, name: &str) -> Result<Value, Error> {
        let ns: Namespace = self
            .storage
            .get(&rusternetes_storage::build_key("namespaces", None, name))
            .await?;
        serde_json::to_value(ns).map_err(|e| Error::Internal(e.to_string()))
    }
}

/// `TypeConverterManager.GetTypeConverter` for every served kind.
pub struct RegistryTypeConverters;

impl TypeConverters for RegistryTypeConverters {
    fn has_type_converter(&self, kind: &GroupVersionKind) -> bool {
        rusternetes_discovery::resolve_kind_to_resource(&kind.group, &kind.version, &kind.kind)
            .is_some()
    }
}

/// `ObjectInterfaces.GetObjectDefaulter()` over the registry's per-kind
/// defaulting (`handlers::defaults`, the ports of the `SetDefaults_*`
/// functions in `pkg/apis/*/v*/defaults.go`).
///
/// Upstream, staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/mutating/
/// dispatcher.go:274 (`dispatchOne`, after `patcher.Patch` and the typed
/// conversion at :268-272):
/// `o.GetObjectDefaulter().Default(newVersionedObject)`
/// and staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:355-359:
/// `if fn, ok := s.defaulterFuncs[reflect.TypeOf(src)]; ok { fn(src) }`
/// i.e. defaulting is looked up by the object's type and is a no-op when none
/// is registered (so an `*unstructured.Unstructured` custom resource is not
/// defaulted here either). Here the kind comes from the object's own
/// `apiVersion`/`kind`. Defaulting funcs only set unset fields, so the
/// defaulted typed form is merged back fill-missing-only; this is equivalent
/// to in-place defaulting and keeps fields our structs do not model. Upstream's
/// dispatcher_test.go:627-630 registers a fake `AddTypeDefaultingFunc` for
/// `appsv1.Deployment` (strategy type) to observe defaulting after a patch,
/// which `defaulter_defaults_a_deployment_after_patch` mirrors. The covered
/// kinds are the `SetDefaults_*` ports that exist in `handlers::defaults`;
/// upstream registers defaulters for every built-in kind.
pub struct SchemeObjectDefaulter;

fn default_typed<T>(object: &mut Value, f: impl FnOnce(&mut T))
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let Ok(mut typed) = serde_json::from_value::<T>(object.clone()) else {
        return;
    };
    f(&mut typed);
    if let Ok(defaulted) = serde_json::to_value(&typed) {
        fill_missing(object, &defaulted);
    }
}

/// Copy into `target` every value `source` has and `target` lacks.
fn fill_missing(target: &mut Value, source: &Value) {
    match (target, source) {
        (Value::Object(t), Value::Object(s)) => {
            for (k, sv) in s {
                match t.get_mut(k) {
                    Some(tv) => fill_missing(tv, sv),
                    None => {
                        t.insert(k.clone(), sv.clone());
                    }
                }
            }
        }
        (Value::Array(t), Value::Array(s)) if t.len() == s.len() => {
            for (tv, sv) in t.iter_mut().zip(s) {
                fill_missing(tv, sv);
            }
        }
        _ => {}
    }
}

impl ObjectDefaulter for SchemeObjectDefaulter {
    fn default_object(&self, object: &mut Value) {
        use crate::handlers::defaults as d;
        use rusternetes_common::resources as r;
        let api_version = object["apiVersion"].as_str().unwrap_or_default();
        let group = api_version.split_once('/').map_or("", |(g, _)| g);
        let kind = object["kind"].as_str().unwrap_or_default().to_string();
        match (group, kind.as_str()) {
            ("", "Pod") => default_typed::<r::Pod>(object, |p| {
                if let Some(spec) = p.spec.as_mut() {
                    d::apply_pod_spec_defaults(spec);
                    d::apply_pod_defaults(spec);
                }
            }),
            ("", "ReplicationController") => {
                default_typed(object, d::apply_replicationcontroller_defaults)
            }
            ("", "Service") => default_typed(object, d::apply_service_defaults),
            ("", "LimitRange") => default_typed(object, d::apply_limit_range_defaults),
            ("", "PersistentVolumeClaim") => {
                default_typed::<r::volume::PersistentVolumeClaim>(object, |c| {
                    d::apply_pvc_spec_defaults(&mut c.spec)
                })
            }
            ("apps", "Deployment") => default_typed(object, d::apply_deployment_defaults),
            ("apps", "DaemonSet") => default_typed(object, d::apply_daemonset_defaults),
            ("apps", "StatefulSet") => default_typed(object, d::apply_statefulset_defaults),
            ("apps", "ReplicaSet") => default_typed(object, d::apply_replicaset_defaults),
            ("batch", "Job") => default_typed(object, d::apply_job_defaults),
            ("batch", "CronJob") => default_typed(object, d::apply_cronjob_defaults),
            // No registered defaulting func: Scheme.Default is a no-op.
            _ => {}
        }
    }
}

type MutatingSource =
    PolicySource<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>;

/// The generic `Source[H]` the plugin reads, over the compiled policy source.
impl HookSource for MutatingSource {
    fn has_synced(&self) -> bool {
        PolicySource::has_synced(self)
    }

    fn hooks(
        &self,
    ) -> Option<
        super::policy_source::Hooks<
            MutatingAdmissionPolicy,
            MutatingAdmissionPolicyBinding,
            MutatingEvaluator,
        >,
    > {
        PolicySource::hooks(self)
    }
}

/// The storage resource type (`/registry/{type}/`) a param resource lives
/// under: built-in groups are keyed by resource name, custom resources by
/// `{group}_{plural}` (`registry::apiextensions::customresource::storage_prefix`).
pub fn param_storage_type(gvr: &GroupVersionResource) -> String {
    if rusternetes_discovery::restmapper::is_builtin_group(&gvr.group) {
        gvr.resource.clone()
    } else {
        crate::registry::apiextensions::customresource::storage_prefix(&gvr.group, &gvr.resource)
    }
}

/// `generic.Plugin.ValidateInitialization` (generic/plugin.go:137-198) for the
/// mutating plugin: when the `MutatingAdmissionPolicy` feature is enabled
/// (`if !c.enabled { return nil }`, :139-143), build the source over the
/// policy and binding informers (`SetExternalKubeInformerFactory`), the REST
/// mapper (`SetRESTMapper`) and the authorizer (`SetAuthorizer`), and start it
/// (`go c.source.Run(pluginContext)`, :178-183), together with the informers
/// the informer factory starts. Returns `None` while the gate is off, so
/// nothing watches storage and `Admission::admit` skips the plugin, exactly
/// as upstream constructs no source for a disabled plugin.
///
/// The gate is read once, at startup, as upstream reads it in
/// `InspectFeatureGates`. The plugin's per-request check
/// ([`MutatingPolicyPlugin::enabled`]) still applies.
pub async fn new_mutating_policy_plugin<S: Storage + 'static>(
    storage: Arc<S>,
    authorizer: Arc<dyn Authorizer>,
) -> Option<Arc<MutatingPolicyPlugin>> {
    if !MutatingPolicyPlugin::enabled() {
        return None;
    }
    let policies = StorageInformer::<MutatingAdmissionPolicy>::new("mutatingadmissionpolicies");
    let bindings =
        StorageInformer::<MutatingAdmissionPolicyBinding>::new("mutatingadmissionpolicybindings");
    let crds = CrdSnapshot::spawn(storage.clone()).await;
    let source = new_mutating_policy_source(
        policies.clone(),
        bindings.clone(),
        Arc::new(StorageParamInformerFactory::new(
            storage.clone(),
            |gvr: &GroupVersionResource| param_storage_type(gvr),
        )),
        Arc::new(DiscoveryRestMapper::new(crds.clone())),
    );
    tokio::spawn(policies.run(storage.clone()));
    tokio::spawn(bindings.run(storage.clone()));
    tokio::spawn({
        let source = source.clone();
        async move {
            // `policy source context unexpectedly closed` (plugin.go:179-182).
            if let Err(e) = source.run().await {
                tracing::error!("policy source context unexpectedly closed: {e}");
            }
        }
    });
    Some(Arc::new(MutatingPolicyPlugin {
        source,
        namespaces: Arc::new(StorageNamespaceLister::new(storage.clone())),
        mapper: Arc::new(CrdEquivalentRegistry::new(
            EquivalentResourceRegistry::from_discovery(),
            crds,
        )),
        namespace_objects: Arc::new(StorageNamespaceObjects::new(storage)),
        type_converters: Arc::new(RegistryTypeConverters),
        defaulter: Arc::new(SchemeObjectDefaulter),
        authorizer,
    }))
}
