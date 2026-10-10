//! The MutatingAdmissionPolicy admission plugin (#2910, part of #2886 and
//! #2731): `generic.Plugin.Dispatch` over the policy source, the matcher and
//! the mutating dispatcher delegate.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/`:
//! - `generic/plugin.go` `Plugin.Dispatch` (:200-215), `shouldIgnoreResource`
//!   (:217-222) and the `admissionResources` exclusion list (:46-52)
//! - `mutating/plugin.go` `NewPlugin` (`admission.NewHandler(Create, Update,
//!   Connect)`), `Admit` and `InspectFeatureGates`
//!
//! The plugin sits at the head of the mutating chain
//! (`pkg/kubeapiserver/options/plugins.go:106-110`), see
//! `endpoints/handlers/admission.rs`.

#![allow(dead_code)]

use std::sync::Arc;

use rusternetes_common::admission::Operation;
use rusternetes_common::resources::{MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding};
use rusternetes_common::Error;

use super::policy_dispatch::{dispatch, PolicyHook};
use super::policy_matching::{Attributes, EquivalentResourceMapper, Matcher, NamespaceLister};
use super::policy_mutating::{
    MutatingDispatcher, MutatingEvaluator, NamespaceObjects, ObjectDefaulter, ReinvocationContext,
    TypeConverters,
};
use super::policy_source::Hooks;
use crate::registry::rest::GroupResource;
use rusternetes_common::feature_gates::Feature;

pub type MutatingHook =
    PolicyHook<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>;

/// generic `Source[H]`, reduced to what `Plugin.Dispatch` calls.
pub trait HookSource: Send + Sync {
    fn has_synced(&self) -> bool;
    fn hooks(
        &self,
    ) -> Option<Hooks<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>>;
}

pub struct MutatingPolicyPlugin {
    pub source: Arc<dyn HookSource>,
    pub namespaces: Arc<dyn NamespaceLister>,
    pub mapper: Arc<dyn EquivalentResourceMapper>,
    pub namespace_objects: Arc<dyn NamespaceObjects>,
    pub type_converters: Arc<dyn TypeConverters>,
    pub defaulter: Arc<dyn ObjectDefaulter>,
}

/// generic/plugin.go:46-52 `admissionResources`: the CEL-based admission
/// configuration resources are always excluded. The decision ignores the
/// version (`gvr.GroupResource()`).
const EXCLUDED_RESOURCES: [(&str, &str); 4] = [
    (
        "admissionregistration.k8s.io",
        "validatingadmissionpolicies",
    ),
    (
        "admissionregistration.k8s.io",
        "validatingadmissionpolicybindings",
    ),
    ("admissionregistration.k8s.io", "mutatingadmissionpolicies"),
    (
        "admissionregistration.k8s.io",
        "mutatingadmissionpolicybindings",
    ),
];

impl MutatingPolicyPlugin {
    /// `Handler.Handles`: `NewHandler(Create, Update, Connect)`.
    pub fn handles(op: &Operation) -> bool {
        matches!(
            op,
            Operation::Create | Operation::Update | Operation::Connect
        )
    }

    /// `InspectFeatureGates` (mutating/plugin.go): `SetEnabled(featureGates.
    /// Enabled(features.MutatingAdmissionPolicy))`, read per request here
    /// because the gate is process-wide and settable.
    pub fn enabled() -> bool {
        rusternetes_common::feature_gates::enabled(Feature::MutatingAdmissionPolicy)
    }

    /// `shouldIgnoreResource` (generic/plugin.go:217-222).
    fn should_ignore_resource(attr: &Attributes) -> bool {
        EXCLUDED_RESOURCES
            .iter()
            .any(|(g, r)| attr.resource.group == *g && attr.resource.resource == *r)
    }

    /// `Plugin.Dispatch` (generic/plugin.go:200-215), behind `Handles`.
    pub async fn admit(
        &self,
        attr: &mut Attributes,
        reinvocation: Arc<ReinvocationContext>,
    ) -> Result<(), Error> {
        if !Self::handles(&attr.operation) || !Self::enabled() {
            return Ok(());
        }
        if Self::should_ignore_resource(attr) {
            return Ok(());
        }
        // `c.WaitForReady()`: the handler's ready func is the namespace
        // informer and the source having synced (plugin.go:191-193). The
        // namespace lister reads storage, so only the source can be unready.
        let hooks = match self.source.hooks() {
            Some(h) if self.source.has_synced() => h,
            _ => {
                return Err(Error::Forbidden(format!(
                    "{} \"{}\" is forbidden: not yet ready to handle request",
                    GroupResource::new(&attr.resource.group, &attr.resource.resource),
                    attr.name
                )))
            }
        };
        let matcher = Matcher {
            namespaces: self.namespaces.as_ref(),
            mapper: self.mapper.as_ref(),
        };
        let delegate = MutatingDispatcher {
            namespaces: self.namespace_objects.clone(),
            type_converters: self.type_converters.clone(),
            defaulter: self.defaulter.clone(),
            reinvocation,
        };
        dispatch(&matcher, attr, &hooks, &delegate).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use async_trait::async_trait;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    use rusternetes_common::feature_gates::{with_feature, Feature};
    use rusternetes_common::resources::mutating_admission_policy::{
        MutatingAdmissionPolicyBindingSpec, MutatingAdmissionPolicySpec, Mutation, PatchType,
    };
    use rusternetes_common::resources::validating_admission_policy::{
        MatchPolicyType, NamedRuleWithOperations, OperationType, RuleWithOperations,
    };
    use rusternetes_common::resources::{LabelSelector, MatchResources};
    use serde_json::{json, Value};
    use serial_test::serial;

    use crate::admission::policy_dispatch::ParamScope;
    use crate::admission::policy_mutating::{
        PatchError, PatchRequest, Patcher, VersionedAttributes,
    };

    struct Source {
        synced: bool,
        hooks: Option<
            Hooks<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>,
        >,
        /// Set when a test requires the plugin to never touch the source.
        forbid_calls: bool,
    }
    impl HookSource for Source {
        fn has_synced(&self) -> bool {
            assert!(!self.forbid_calls, "source consulted");
            self.synced
        }
        fn hooks(
            &self,
        ) -> Option<Hooks<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>>
        {
            assert!(!self.forbid_calls, "source consulted");
            self.hooks.clone()
        }
    }

    struct Namespaces;
    #[async_trait]
    impl NamespaceLister for Namespaces {
        async fn namespace_labels(
            &self,
            _: &str,
        ) -> rusternetes_common::Result<HashMap<String, String>> {
            Ok(HashMap::new())
        }
    }
    #[async_trait]
    impl NamespaceObjects for Namespaces {
        async fn get_namespace(&self, name: &str) -> Result<Value, Error> {
            Ok(json!({"kind": "Namespace", "metadata": {"name": name}}))
        }
    }
    struct Mapper;
    impl EquivalentResourceMapper for Mapper {
        fn equivalent_resources_for(
            &self,
            r: &GroupVersionResource,
            _: &str,
        ) -> Vec<GroupVersionResource> {
            vec![r.clone()]
        }
        fn kind_for(&self, _: &GroupVersionResource, _: &str) -> GroupVersionKind {
            gvk("", "", "")
        }
    }
    struct Converters;
    impl TypeConverters for Converters {
        fn has_type_converter(&self, _: &GroupVersionKind) -> bool {
            true
        }
    }
    struct NoDefaults;
    impl ObjectDefaulter for NoDefaults {
        fn default_object(&self, _: &mut Value) {}
    }
    struct SetLabel;
    #[async_trait]
    impl Patcher for SetLabel {
        async fn patch(&self, r: &PatchRequest<'_>) -> Result<Value, PatchError> {
            let mut o = r.versioned_attributes.versioned_object.clone().unwrap();
            o["metadata"]["labels"] = json!({"mutated": "yes"});
            Ok(o)
        }
    }

    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }
    fn gvr(g: &str, v: &str, r: &str) -> GroupVersionResource {
        GroupVersionResource {
            group: g.into(),
            version: v.into(),
            resource: r.into(),
        }
    }
    fn configmap_attr(op: Operation) -> Attributes {
        Attributes {
            kind: gvk("", "v1", "ConfigMap"),
            resource: gvr("", "v1", "configmaps"),
            subresource: "".into(),
            namespace: "default".into(),
            name: "cm".into(),
            operation: op,
            object: Some(json!({
                "kind": "ConfigMap", "apiVersion": "v1",
                "metadata": {"name": "cm", "namespace": "default"},
            })),
            old_object: None,
        }
    }
    fn match_resources(group: &str, resource: &str) -> MatchResources {
        MatchResources {
            namespace_selector: Some(LabelSelector::default()),
            object_selector: Some(LabelSelector::default()),
            resource_rules: Some(vec![NamedRuleWithOperations {
                resource_names: None,
                rule: RuleWithOperations {
                    operations: Some(vec![OperationType::All]),
                    api_groups: Some(vec![group.into()]),
                    api_versions: Some(vec!["*".into()]),
                    resources: Some(vec![resource.into()]),
                    scope: None,
                },
            }]),
            exclude_resource_rules: None,
            match_policy: Some(MatchPolicyType::Exact),
        }
    }
    fn hook(group: &str, resource: &str) -> MutatingHook {
        let mut p = MutatingAdmissionPolicy::new("p");
        p.spec = Some(MutatingAdmissionPolicySpec {
            match_constraints: Some(match_resources(group, resource)),
            mutations: Some(vec![Mutation {
                patch_type: Some(PatchType::ApplyConfiguration),
                apply_configuration: None,
                json_patch: None,
            }]),
            ..Default::default()
        });
        let mut b = MutatingAdmissionPolicyBinding::new("b");
        b.spec = Some(MutatingAdmissionPolicyBindingSpec {
            policy_name: Some("p".into()),
            ..Default::default()
        });
        PolicyHook {
            policy: p,
            bindings: vec![b],
            param_store: None,
            param_scope: ParamScope::Root,
            evaluator: MutatingEvaluator {
                matcher: None,
                mutators: vec![Some(Arc::new(SetLabel))],
            },
            configuration_error: None,
        }
    }
    fn plugin(synced: bool, hooks: Vec<MutatingHook>, forbid_calls: bool) -> MutatingPolicyPlugin {
        MutatingPolicyPlugin {
            source: Arc::new(Source {
                synced,
                hooks: Some(Arc::new(hooks)),
                forbid_calls,
            }),
            namespaces: Arc::new(Namespaces),
            mapper: Arc::new(Mapper),
            namespace_objects: Arc::new(Namespaces),
            type_converters: Arc::new(Converters),
            defaulter: Arc::new(NoDefaults),
        }
    }
    fn ctx() -> Arc<ReinvocationContext> {
        Arc::new(ReinvocationContext::new(false))
    }

    /// mutating/plugin.go `NewHandler(Create, Update, Connect)`: no request
    /// body to mutate for DELETE.
    #[test]
    fn handles_create_update_connect_not_delete() {
        assert!(MutatingPolicyPlugin::handles(&Operation::Create));
        assert!(MutatingPolicyPlugin::handles(&Operation::Update));
        assert!(MutatingPolicyPlugin::handles(&Operation::Connect));
        assert!(!MutatingPolicyPlugin::handles(&Operation::Delete));
    }

    /// generic/plugin.go:206-207: `!c.enabled` returns before the source is
    /// consulted. The gate is off by default.
    #[tokio::test]
    #[serial]
    async fn gate_off_does_nothing() {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, false);
        let p = plugin(true, vec![hook("", "configmaps")], true);
        let mut a = configmap_attr(Operation::Create);
        let before = a.object.clone();
        p.admit(&mut a, ctx()).await.unwrap();
        assert_eq!(a.object, before);
    }

    /// generic/plugin.go:208: `shouldIgnoreResource`, the four admission
    /// registration resources of `admissionResources`, version-blind.
    #[tokio::test]
    #[serial]
    async fn excluded_resources_are_ignored() {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
        let p = plugin(true, vec![hook("*", "*")], true);
        for r in [
            "validatingadmissionpolicies",
            "validatingadmissionpolicybindings",
            "mutatingadmissionpolicies",
            "mutatingadmissionpolicybindings",
        ] {
            for v in ["v1", "v1beta1"] {
                let mut a = configmap_attr(Operation::Create);
                a.resource = gvr("admissionregistration.k8s.io", v, r);
                p.admit(&mut a, ctx()).await.unwrap();
            }
        }
    }

    /// generic/plugin.go:209-210: not yet synced is a Forbidden.
    #[tokio::test]
    #[serial]
    async fn not_ready_is_forbidden() {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
        let p = plugin(false, vec![], false);
        let mut a = configmap_attr(Operation::Create);
        let err = p.admit(&mut a, ctx()).await.unwrap_err();
        let Error::Forbidden(m) = err else {
            panic!("expected Forbidden, got {err:?}")
        };
        assert_eq!(
            m,
            "configmaps \"cm\" is forbidden: not yet ready to handle request"
        );
    }

    /// generic/plugin.go:213: a matching policy's patcher runs and its
    /// output replaces the request object.
    #[tokio::test]
    #[serial]
    async fn matching_policy_mutates_the_object() {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
        let p = plugin(true, vec![hook("", "configmaps")], false);
        let mut a = configmap_attr(Operation::Create);
        p.admit(&mut a, ctx()).await.unwrap();
        assert_eq!(
            a.object.unwrap()["metadata"]["labels"],
            json!({"mutated": "yes"})
        );
    }

    /// A policy for another resource leaves the object alone.
    #[tokio::test]
    #[serial]
    async fn non_matching_policy_leaves_the_object() {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
        let p = plugin(true, vec![hook("", "secrets")], false);
        let mut a = configmap_attr(Operation::Create);
        let before = a.object.clone();
        p.admit(&mut a, ctx()).await.unwrap();
        assert_eq!(a.object, before);
    }

    /// `Handles` gates `Admit`: DELETE never reaches the dispatcher.
    #[tokio::test]
    #[serial]
    async fn delete_is_not_dispatched() {
        let _g = with_feature(Feature::MutatingAdmissionPolicy, true);
        let p = plugin(true, vec![hook("", "configmaps")], true);
        let mut a = configmap_attr(Operation::Delete);
        p.admit(&mut a, ctx()).await.unwrap();
    }

    #[allow(unused)]
    fn _uses(_: VersionedAttributes) {}
}
