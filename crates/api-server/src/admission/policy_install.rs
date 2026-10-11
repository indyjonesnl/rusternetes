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

#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::resources::Namespace;
use rusternetes_common::Error;
use rusternetes_storage::Storage;
use serde_json::Value;

use super::policy_dispatch::ParamScope;
use super::policy_mutating::{
    compile_policy, MutatingEvaluator, NamespaceObjects, ObjectDefaulter, TypeConverters,
};
use super::policy_source::{Informer, ParamInformerFactory, PolicySource, RestMapper, RestMapping};
use rusternetes_common::resources::mutating_admission_policy::{
    MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding,
};

/// The mutating plugin's policy source, compiling every policy with
/// [`compile_policy`].
///
/// Upstream, staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
/// mutating/plugin.go `NewPlugin` hands `compilePolicy` (compilation.go:34) to
/// `generic.NewPolicySource` as the compiler, so every
/// `compilePolicyLocked` (policy_source.go:470-501) yields a
/// `PolicyEvaluator` with the real patchers and the matchConditions matcher.
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

/// `meta.RESTMapper` over the resources this server serves.
pub struct DiscoveryRestMapper;

impl RestMapper for DiscoveryRestMapper {
    /// `RESTMapping(GroupKind, version)`: a kind that is not served is
    /// upstream's `NoKindMatchError`.
    fn rest_mapping(&self, group: &str, kind: &str, version: &str) -> Result<RestMapping, String> {
        let (resource, namespaced) =
            rusternetes_discovery::resolve_kind_to_resource_and_scope(group, version, kind)
                .ok_or_else(|| {
                    format!(
                        "no matches for kind \"{kind}\" in version \"{}\"",
                        if group.is_empty() {
                            version.to_string()
                        } else {
                            format!("{group}/{version}")
                        }
                    )
                })?;
        Ok(RestMapping {
            resource: GroupVersionResource {
                group: group.to_string(),
                version: version.to_string(),
                resource,
            },
            scope: if namespaced {
                ParamScope::Namespace
            } else {
                ParamScope::Root
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::Namespace;
    use rusternetes_storage::MemoryStorage;

    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }

    #[tokio::test]
    async fn mutating_source_compiles_policies_with_compile_policy() {
        use super::super::policy_source::{StorageInformer, StorageParamInformerFactory};
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create(
                "/registry/mutatingadmissionpolicies/p",
                &serde_json::json!({
                    "apiVersion": "admissionregistration.k8s.io/v1beta1",
                    "kind": "MutatingAdmissionPolicy",
                    "metadata": {"name": "p"},
                    "spec": {
                        "matchConditions": [{"name": "c", "expression": "true"}],
                        "mutations": [{
                            "patchType": "JSONPatch",
                            "jsonPatch": {"expression": "[]"}
                        }]
                    }
                }),
            )
            .await
            .unwrap();
        storage
            .create(
                "/registry/mutatingadmissionpolicybindings/b",
                &serde_json::json!({
                    "apiVersion": "admissionregistration.k8s.io/v1beta1",
                    "kind": "MutatingAdmissionPolicyBinding",
                    "metadata": {"name": "b"},
                    "spec": {"policyName": "p"}
                }),
            )
            .await
            .unwrap();
        let policies = StorageInformer::<MutatingAdmissionPolicy>::new("mutatingadmissionpolicies");
        let bindings = StorageInformer::<MutatingAdmissionPolicyBinding>::new(
            "mutatingadmissionpolicybindings",
        );
        let source = new_mutating_policy_source(
            policies.clone(),
            bindings.clone(),
            Arc::new(StorageParamInformerFactory::new(storage.clone(), |_| {
                String::new()
            })),
            Arc::new(DiscoveryRestMapper),
        );
        let tasks = [
            tokio::spawn(policies.run(storage.clone())),
            tokio::spawn(bindings.run(storage.clone())),
            tokio::spawn({
                let s = source.clone();
                async move {
                    let _ = s.run().await;
                }
            }),
        ];
        let mut hooks = None;
        for _ in 0..500 {
            if source.has_synced() {
                hooks = source.hooks().filter(|h| !h.is_empty());
                if hooks.is_some() {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        for t in &tasks {
            t.abort();
        }
        let hooks = hooks.expect("hooks compiled within 5s");
        // compile_policy built the patcher and the matchConditions matcher.
        assert_eq!(hooks[0].evaluator.mutators.len(), 1);
        assert!(hooks[0].evaluator.matcher.is_some());
    }

    #[test]
    fn rest_mapper_resolves_namespaced_and_cluster_kinds() {
        let m = DiscoveryRestMapper;
        let cm = m.rest_mapping("", "ConfigMap", "v1").unwrap();
        assert_eq!(cm.resource.resource, "configmaps");
        assert_eq!(cm.resource.version, "v1");
        assert_eq!(cm.scope, ParamScope::Namespace);
        let ns = m.rest_mapping("", "Namespace", "v1").unwrap();
        assert_eq!(ns.resource.resource, "namespaces");
        assert_eq!(ns.scope, ParamScope::Root);
        let d = m.rest_mapping("apps", "Deployment", "v1").unwrap();
        assert_eq!(d.resource.group, "apps");
        assert_eq!(d.resource.resource, "deployments");
    }

    #[test]
    fn rest_mapper_unknown_kind_is_an_error() {
        // NoKindMatchError.
        assert!(DiscoveryRestMapper
            .rest_mapping("example.com", "Widget", "v1")
            .is_err());
        assert!(DiscoveryRestMapper.rest_mapping("", "Nope", "v1").is_err());
    }

    #[tokio::test]
    async fn namespace_objects_returns_the_full_namespace() {
        let storage = Arc::new(MemoryStorage::new());
        let mut ns = Namespace::new("team-a");
        ns.metadata.labels = Some([("env".to_string(), "dev".to_string())].into());
        storage
            .create(
                &rusternetes_storage::build_key("namespaces", None, "team-a"),
                &ns,
            )
            .await
            .unwrap();
        let got = StorageNamespaceObjects::new(storage)
            .get_namespace("team-a")
            .await
            .unwrap();
        assert_eq!(got["metadata"]["name"], "team-a");
        assert_eq!(got["metadata"]["labels"]["env"], "dev");
    }

    #[tokio::test]
    async fn namespace_objects_missing_is_not_found() {
        let storage = Arc::new(MemoryStorage::new());
        let err = StorageNamespaceObjects::new(storage)
            .get_namespace("absent")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }

    #[test]
    fn defaulter_defaults_a_deployment_after_patch() {
        // Scheme.Default runs SetDefaults_Deployment and the pod template's.
        let mut o = serde_json::json!({
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": "d"},
            "spec": {"selector": {"matchLabels": {"a": "b"}},
                "template": {"metadata": {"labels": {"a": "b"}},
                    "spec": {"containers": [{"name": "c", "image": "i", "customField": true}]}}}
        });
        SchemeObjectDefaulter.default_object(&mut o);
        assert_eq!(o["spec"]["strategy"]["type"], "RollingUpdate");
        assert_eq!(o["spec"]["template"]["spec"]["restartPolicy"], "Always");
        // Fields the typed form does not know survive.
        assert_eq!(
            o["spec"]["template"]["spec"]["containers"][0]["customField"],
            true
        );
    }

    #[test]
    fn defaulter_leaves_set_values_and_unknown_kinds_alone() {
        let mut o = serde_json::json!({
            "apiVersion": "example.com/v1", "kind": "Widget", "spec": {"a": 1}
        });
        let before = o.clone();
        SchemeObjectDefaulter.default_object(&mut o);
        assert_eq!(o, before);
        let mut p = serde_json::json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "i"}]}
        });
        SchemeObjectDefaulter.default_object(&mut p);
        assert_eq!(p["spec"]["restartPolicy"], "Never");
        assert_eq!(p["spec"]["enableServiceLinks"], true);
    }

    #[test]
    fn type_converters_cover_served_kinds_only() {
        let c = RegistryTypeConverters;
        assert!(c.has_type_converter(&gvk("", "v1", "ConfigMap")));
        assert!(c.has_type_converter(&gvk("apps", "v1", "Deployment")));
        assert!(!c.has_type_converter(&gvk("example.com", "v1", "Widget")));
    }
}
