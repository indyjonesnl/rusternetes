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

use std::sync::Arc;

use rusternetes_common::admission::Operation;
use rusternetes_common::authz::Authorizer;
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
    /// The request authorizer handed to the dispatcher (`d.authz`).
    pub authorizer: Arc<dyn Authorizer>,
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
            authz: self.authorizer.clone(),
        };
        dispatch(&matcher, attr, &hooks, &delegate).await
    }
}
