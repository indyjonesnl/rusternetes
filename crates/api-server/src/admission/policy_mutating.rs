//! The MutatingAdmissionPolicy dispatcher delegate (#2907, part of #2886 and
//! #2731): `dispatchInvocations` and `dispatchOne`, plugged into the generic
//! policy dispatcher's `DispatchDelegate` seam (`policy_dispatch.rs`).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/mutating/`:
//! - `dispatcher.go` `dispatchInvocations` (:70-221), `dispatchOne`
//!   (:223-269) and `keyFor` (:271-300)
//! - `patch/interface.go` (`Patcher`, `Request`)
//! - `compilation.go` `PolicyEvaluator` (`Matcher` + `Mutators`)
//! - `reinvocationcontext.go` (in `policy_dispatch.rs`)
//!
//! Tests are ported from `mutating/dispatcher_test.go` (`TestDispatcher`),
//! with fake `Patcher`s and a fake match-condition matcher: that test drives
//! the real `applyConfiguration` patcher through CEL, and the `patch/`
//! package is a separate port (follow-up of #2886).
//!
//! Deviations from upstream, each deliberate:
//! - Objects are JSON `Value`s throughout, i.e. every object is
//!   `*unstructured.Unstructured`. Upstream's versioned-attribute conversion
//!   to a typed object, and the typed `ConvertToVersion` after a patch
//!   (dispatcher.go:253-260), are identities here; so is the final
//!   `Convert(versioned, attributes.GetObject())` (:211).
//! - The `celmetrics` observations (`ObserveRejection` / `ObserveAdmission`,
//!   dispatcher.go:196, :205) are ported (#3229, `policy_mutating_metrics.rs`).
//!   `NewCachingAuthorizer` is (#3228, `caching_authorizer.rs`) and reaches
//!   the matcher and, as `OptionalVariables.Authorizer`, the `Patcher`; the CEL
//!   `authorizer` library it feeds is `rusternetes_common::cel_authz_library` (#3300).
//! - `CompositionEnv.CreateContext` (:131-133) belongs to the CEL compiler and
//!   is the evaluator's business, not the dispatcher's.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel_cost::RUNTIME_CEL_COST_BUDGET;
use rusternetes_common::resources::mutating_admission_policy::ReinvocationPolicyType;
use rusternetes_common::resources::{MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding};
use rusternetes_common::Error;
use serde_json::Value;

use super::caching_authorizer::new_caching_authorizer;
use super::policy_dispatch::{
    BindingAccessor, DispatchDelegate, NamespacedName, PolicyAccessor, PolicyError,
    PolicyInvocation, PolicyKey, PolicyReinvokeContext, VersionedAttributeAccessor,
};
use super::policy_matching::Attributes;
use super::policy_mutating_metrics::{observe_admission, observe_rejection, MutationErrorType};

/// mutating/plugin.go `PluginName`.
pub const PLUGIN_NAME: &str = "MutatingAdmissionPolicy";

/// `admission.ReinvocationContext`: shared by every plugin of one request.
/// Upstream's `Value`/`SetValue` store is keyed by plugin name.
#[derive(Debug, Default)]
pub struct ReinvocationContext {
    pub is_reinvoke: AtomicBool,
    pub should_reinvoke: AtomicBool,
    pub values: Mutex<HashMap<String, PolicyReinvokeContext>>,
    /// The `MutatingAdmissionWebhook` plugin's value
    /// (`reinvokeCtx.SetValue(PluginName, webhookReinvokeCtx)`,
    /// webhook/mutating/dispatcher.go:108-113). Typed rather than keyed.
    pub webhook_value: Mutex<rusternetes_admission_webhook::WebhookReinvokeContext>,
}

impl ReinvocationContext {
    pub fn new(is_reinvoke: bool) -> Self {
        Self {
            is_reinvoke: AtomicBool::new(is_reinvoke),
            ..Default::default()
        }
    }
    pub fn is_reinvoke(&self) -> bool {
        self.is_reinvoke.load(Ordering::SeqCst)
    }
    /// `SetIsReinvoke`: called by the reinvoker before the second pass.
    pub fn set_is_reinvoke(&self) {
        self.is_reinvoke.store(true, Ordering::SeqCst);
    }
    pub fn set_should_reinvoke(&self) {
        self.should_reinvoke.store(true, Ordering::SeqCst);
    }
    pub fn should_reinvoke(&self) -> bool {
        self.should_reinvoke.load(Ordering::SeqCst)
    }
    /// Takes the webhook plugin's context out so it can be held across an
    /// `.await`; hand it back with [`Self::restore_webhook_context`]. The
    /// chain of one request is sequential, so nothing reads it in between.
    pub fn take_webhook_context(&self) -> rusternetes_admission_webhook::WebhookReinvokeContext {
        std::mem::take(&mut *self.webhook_value.lock().unwrap())
    }
    pub fn restore_webhook_context(
        &self,
        ctx: rusternetes_admission_webhook::WebhookReinvokeContext,
    ) {
        *self.webhook_value.lock().unwrap() = ctx;
    }
    /// Runs `f` on this plugin's context, creating it on first use
    /// (dispatcher.go:81-87).
    pub fn with_policy_context<R>(&self, f: impl FnOnce(&mut PolicyReinvokeContext) -> R) -> R {
        let mut values = self.values.lock().unwrap();
        f(values.entry(PLUGIN_NAME.to_string()).or_default())
    }
}

/// `admission.VersionedAttributes`, reduced to what the mutating dispatcher
/// reads and writes.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedAttributes {
    pub versioned_kind: GroupVersionKind,
    pub versioned_object: Option<Value>,
    pub versioned_old_object: Option<Value>,
    /// Set once a mutation replaced `versioned_object`.
    pub dirty: bool,
}

/// An error from a `Patcher` (or `dispatchOne`): a `StatusError` fails the
/// request as is, anything else becomes a `PolicyError`
/// (dispatcher.go:165-172).
#[derive(Debug)]
pub enum PatchError {
    Status(Error),
    Other(String),
}

/// `cel.OptionalVariableBindings` (cel/interface.go:73-83), as passed to
/// `dispatchOne` (mutating/dispatcher.go:190).
pub struct OptionalVariableBindings<'a> {
    pub versioned_params: Option<&'a Value>,
    pub authorizer: Arc<dyn Authorizer>,
}

/// `patch.Request` (patch/interface.go:35-44), minus the object interfaces
/// and type converter the JSON representation does not need.
pub struct PatchRequest<'a> {
    pub matched_resource: &'a GroupVersionResource,
    pub versioned_attributes: &'a VersionedAttributes,
    /// The request's `admission.Attributes`, which upstream threads to the
    /// patcher via `Patch`'s caller (`CreateAdmissionRequest(
    /// versionedAttributes.Attributes, ...)`, json_patch.go:76-82).
    pub attributes: &'a Attributes,
    /// `OptionalVariables.VersionedParams`.
    pub param: Option<&'a Value>,
    /// The request's `v1.Namespace`; `None` for a cluster-scoped request.
    pub namespace: Option<&'a Value>,
    /// `OptionalVariables.Authorizer` (cel/interface.go:80-83): the
    /// dispatcher's caching authorizer; binds the CEL `authorizer` and
    /// `authorizer.requestResource` variables when set (activation.go:52-55).
    pub authorizer: Option<Arc<dyn Authorizer>>,
}

/// `patch.Patcher` (patch/interface.go:29-33). `patch` returns a modified
/// copy and must not change the request's object in place.
#[async_trait]
pub trait Patcher: Send + Sync {
    ///
    /// `runtime_cel_cost_budget` is `Patch`'s `runtimeCELCostBudget`: callers
    /// pass `RUNTIME_CEL_COST_BUDGET` (dispatcher.go:259); it is a parameter
    /// for testing only upstream too.
    async fn patch(
        &self,
        request: &PatchRequest<'_>,
        runtime_cel_cost_budget: i64,
    ) -> Result<Value, PatchError>;
}

/// `matchconditions.MatchResult`.
#[derive(Debug, Default)]
pub struct MatchResult {
    pub matches: bool,
    pub error: Option<String>,
    /// `FailedConditionName`: the first condition that evaluated to false.
    pub failed_condition_name: Option<String>,
}

/// `matchconditions.Matcher.Match` over the versioned attributes.
#[async_trait]
pub trait ConditionMatcher: Send + Sync {
    async fn matches(
        &self,
        attributes: &Attributes,
        versioned: &VersionedAttributes,
        param: Option<&Value>,
        authz: Arc<dyn Authorizer>,
    ) -> MatchResult;
}

/// compilation.go `PolicyEvaluator`: the compiled match conditions and one
/// patcher per mutation (`None` is a nil patcher).
#[derive(Default, Clone)]
pub struct MutatingEvaluator {
    pub matcher: Option<Arc<dyn ConditionMatcher>>,
    pub mutators: Vec<Option<Arc<dyn Patcher>>>,
}

/// compilation.go `compilePolicy` (:34-81).
///
/// Builds one patcher per mutation, in order. A mutation whose payload is
/// absent (or whose `patchType` is neither supported value) is skipped, as
/// upstream's `switch` does (:57-72); the dispatcher then reports the count
/// mismatch. `spec.variables` are composited into every patcher
/// (`CompileAndStoreVariables`, :44).
///
/// `spec.matchConditions` compile to a `matchconditions.NewMatcher`
/// (:47-54) with matcherKind "policy", matcherType "validate".
pub fn compile_policy(policy: &MutatingAdmissionPolicy) -> MutatingEvaluator {
    use super::policy_apply_config::ApplyConfigPatcher;
    use super::policy_json_patch::JsonPatcher;
    use rusternetes_common::resources::mutating_admission_policy::PatchType;

    let Some(spec) = policy.spec.as_ref() else {
        return MutatingEvaluator::default();
    };
    let variables: Vec<(String, String)> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| (v.name.clone(), v.expression.clone()))
        .collect();
    let mut mutators: Vec<Option<Arc<dyn Patcher>>> = Vec::new();
    for m in spec.mutations.iter().flatten() {
        match m.patch_type {
            Some(PatchType::JsonPatch) => {
                if let Some(j) = &m.json_patch {
                    mutators.push(Some(JsonPatcher::new_patcher_with_variables(
                        &j.expression,
                        &variables,
                    )));
                }
            }
            Some(PatchType::ApplyConfiguration) => {
                if let Some(a) = &m.apply_configuration {
                    mutators.push(Some(ApplyConfigPatcher::new_patcher_with_variables(
                        &a.expression,
                        &variables,
                    )));
                }
            }
            _ => {}
        }
    }
    // Compile matchers (compilation.go:47-54).
    let matcher: Option<Arc<dyn ConditionMatcher>> = match spec.match_conditions.as_deref() {
        Some(conds) if !conds.is_empty() => Some(super::policy_match_conditions::new_matcher(
            conds,
            &variables,
            spec.failure_policy.as_ref(),
            PolicyAccessor::name(policy),
        )),
        _ => None,
    };
    MutatingEvaluator { matcher, mutators }
}

/// errors.go `ErrorType`.
pub fn error_type(err: Option<&str>) -> MutationErrorType {
    match err {
        None => MutationErrorType::NoError,
        Some(e) if e.starts_with("compilation") => MutationErrorType::CompileError,
        Some(e) if e.starts_with("validation failed due to running out of cost budget") => {
            MutationErrorType::OutOfBudget
        }
        Some(_) => MutationErrorType::InvalidError,
    }
}

/// `patch.TypeConverterManager.GetTypeConverter != nil`.
pub trait TypeConverters: Send + Sync {
    fn has_type_converter(&self, kind: &GroupVersionKind) -> bool;
}

/// `ObjectInterfaces.GetObjectDefaulter().Default`.
pub trait ObjectDefaulter: Send + Sync {
    fn default_object(&self, object: &mut Value);
}

/// `matcher.GetNamespace`: the full `v1.Namespace`.
#[async_trait]
pub trait NamespaceObjects: Send + Sync {
    async fn get_namespace(&self, name: &str) -> Result<Value, Error>;
}

/// The mutating half of the generic dispatcher (`dispatcher` in
/// mutating/dispatcher.go:59-64).
pub struct MutatingDispatcher {
    pub namespaces: Arc<dyn NamespaceObjects>,
    pub type_converters: Arc<dyn TypeConverters>,
    pub defaulter: Arc<dyn ObjectDefaulter>,
    /// `a.GetReinvocationContext()`.
    pub reinvocation: Arc<ReinvocationContext>,
    /// `d.authz`: the request authorizer, wrapped per `dispatchInvocations`
    /// call in `NewCachingAuthorizer` (dispatcher.go:135).
    pub authz: Arc<dyn Authorizer>,
}

/// dispatcher.go `keyFor` (:271-300).
pub fn key_for(
    policy: &MutatingAdmissionPolicy,
    binding: &MutatingAdmissionPolicyBinding,
    param: Option<&Value>,
) -> Result<PolicyKey, Error> {
    let param_uid = match param {
        None => NamespacedName::default(),
        Some(p) => {
            // meta.Accessor fails for anything that is not an object.
            let obj = p.as_object().ok_or_else(|| {
                Error::Internal("object does not implement the Object interfaces".into())
            })?;
            let meta = obj.get("metadata");
            let get = |k: &str| {
                meta.and_then(|m| m.get(k))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            };
            NamespacedName {
                name: get("name"),
                namespace: get("namespace"),
            }
        }
    };
    Ok(PolicyKey {
        policy: NamespacedName {
            name: PolicyAccessor::name(policy).to_string(),
            namespace: PolicyAccessor::namespace(policy).to_string(),
        },
        binding: NamespacedName {
            name: BindingAccessor::name(binding).to_string(),
            namespace: BindingAccessor::namespace(binding).to_string(),
        },
        param: param_uid,
        mutation_index: 0,
    })
}

fn gvk_string(k: &GroupVersionKind) -> String {
    // schema.GroupVersionKind.String(): "apps/v1, Kind=Deployment".
    let gv = if k.group.is_empty() {
        k.version.clone()
    } else {
        format!("{}/{}", k.group, k.version)
    };
    format!("{gv}, Kind={}", k.kind)
}

impl MutatingDispatcher {
    /// dispatcher.go:223-269 (`dispatchOne`).
    async fn dispatch_one(
        &self,
        patcher: Option<&Arc<dyn Patcher>>,
        attributes: &Attributes,
        versioned: &mut VersionedAttributes,
        namespace: Option<&Value>,
        resource: &GroupVersionResource,
        optional_variables: OptionalVariableBindings<'_>,
    ) -> Result<(), PatchError> {
        let OptionalVariableBindings {
            versioned_params: param,
            authorizer: authz,
        } = optional_variables;
        let Some(patcher) = patcher else {
            // internal error. this should not happen
            return Err(PatchError::Status(Error::Internal(
                "policy evaluator is nil".into(),
            )));
        };

        // Find type converter for the invoked Group-Version.
        if !self
            .type_converters
            .has_type_converter(&versioned.versioned_kind)
        {
            // This can happen if the request is for a resource whose schema
            // has not been registered with the type converter manager.
            return Err(PatchError::Status(Error::Network(format!(
                "Resource kind {} not found. There can be a delay between when CustomResourceDefinitions are created and when they are available.",
                gvk_string(&versioned.versioned_kind)
            ))));
        }

        // dispatcher.go:259: `patcher.Patch(ctx, patchRequest,
        // celconfig.RuntimeCELCostBudget)`.
        let mut new_object = patcher
            .patch(
                &PatchRequest {
                    matched_resource: resource,
                    versioned_attributes: versioned,
                    attributes,
                    param,
                    namespace,
                    authorizer: Some(authz),
                },
                RUNTIME_CEL_COST_BUDGET,
            )
            .await?;
        self.defaulter.default_object(&mut new_object);

        versioned.dirty = true;
        versioned.versioned_object = Some(new_object);
        Ok(())
    }

    /// dispatcher.go:70-221 (`dispatchInvocations`), after `Dispatch` has
    /// collected the invocations. `versioned` is upstream's
    /// `versionedAttributes` accessor, one entry per kind.
    async fn dispatch_invocations(
        &self,
        attr: &mut Attributes,
        versioned: &mut VersionedAttributeAccessor<'_>,
        invocations: &[PolicyInvocation<
            '_,
            MutatingAdmissionPolicy,
            MutatingAdmissionPolicyBinding,
            MutatingEvaluator,
        >],
    ) -> Result<Vec<PolicyError>, Error> {
        let reinvoke = &self.reinvocation;
        // dispatcher.go:135.
        let authz: Arc<dyn Authorizer> = Arc::new(new_caching_authorizer(self.authz.clone()));
        let mut last_kind: Option<GroupVersionKind> = None;

        if reinvoke.is_reinvoke()
            && reinvoke.with_policy_context(|c| {
                c.is_output_changed_since_last_policy_invocation(attr.object.as_ref())
            })
        {
            // If the object has changed, we know the in-tree plugin re-invocations have mutated the object,
            // and we need to reinvoke all eligible policies.
            reinvoke.with_policy_context(|c| c.require_reinvoking_previously_invoked_plugins());
        }

        let mut policy_errors: Vec<PolicyError> = Vec::new();
        let config_error = |err: String, inv: &PolicyInvocation<'_, _, _, _>| {
            PolicyError::new(inv.policy, Some(inv.binding), err, Some("Invalid"))
        };

        // There is at least one invocation to invoke. Make sure we have a namespace
        // object if the incoming object is not cluster scoped to pass into the evaluator.
        let mut namespace_name = attr.namespace.clone();

        // Special case, the namespace object has the namespace of itself (maybe a bug).
        // unset it if the incoming object is a namespace
        let gvk = &attr.kind;
        if gvk.kind == "Namespace" && gvk.version == "v1" && gvk.group.is_empty() {
            namespace_name.clear();
        }

        // if it is cluster scoped, namespaceName will be empty
        // Otherwise, get the Namespace resource.
        let namespace = if namespace_name.is_empty() {
            None
        } else {
            match self.namespaces.get_namespace(&namespace_name).await {
                Ok(ns) => Some(ns),
                Err(e @ Error::Status(_)) => return Err(e),
                Err(_) => {
                    return Err(Error::NotFound(format!(
                        "namespaces \"{namespace_name}\" not found"
                    )))
                }
            }
        };

        // Should loop through invocations, handling possible error and invoking
        // evaluator to apply patch, also should handle re-invocations
        for invocation in invocations {
            let mutations = invocation
                .policy
                .spec
                .as_ref()
                .and_then(|s| s.mutations.as_ref())
                .map_or(0, Vec::len);
            if invocation.evaluator.mutators.len() != mutations {
                // This would be a bug. The compiler should always return exactly as
                // many evaluators as there are mutations
                return Err(Error::Internal(format!(
                    "expected {} compiled evaluators for policy {}, got {}",
                    mutations,
                    PolicyAccessor::name(invocation.policy),
                    invocation.evaluator.mutators.len()
                )));
            }

            // `versionedAttributes.VersionedAttribute(invocation.Kind)`
            // (cached; `Dispatch` has already warmed it).
            let current = versioned
                .versioned_attribute(attr, &invocation.kind)
                .map_err(Error::Internal)?;

            if let Some(matcher) = &invocation.evaluator.matcher {
                let result = matcher
                    .matches(attr, current, invocation.param.as_ref(), authz.clone())
                    .await;
                if let Some(err) = result.error {
                    policy_errors.push(config_error(err, invocation));
                    continue;
                }

                // if preconditions are not met, then skip mutations
                if !result.matches {
                    continue;
                }
            }

            // This should never fail: it occurs if there is a programming
            // error causing the Param not to be a valid object.
            let invocation_key = key_for(
                invocation.policy,
                invocation.binding,
                invocation.param.as_ref(),
            )?;
            if reinvoke.is_reinvoke()
                && !reinvoke.with_policy_context(|c| c.should_reinvoke(&invocation_key))
            {
                continue;
            }

            let object_before_mutations = current.versioned_object.clone();
            // Mutations for a single invocation of a MutatingAdmissionPolicy are evaluated
            // in order.
            for mutation_index in 0..mutations {
                last_kind = Some(invocation.kind.clone());
                let current = versioned
                    .versioned_attribute(attr, &invocation.kind)
                    .map_err(Error::Internal)?;
                if current.versioned_object.is_none() {
                    // Do not call patchers if there is no object to patch.
                    continue;
                }

                let patcher = invocation.evaluator.mutators[mutation_index].as_ref();
                let start_time = Instant::now();
                let result = self
                    .dispatch_one(
                        patcher,
                        attr,
                        current,
                        namespace.as_ref(),
                        &invocation.resource,
                        OptionalVariableBindings {
                            versioned_params: invocation.param.as_ref(),
                            authorizer: authz.clone(),
                        },
                    )
                    .await;
                let elapsed = start_time.elapsed();
                if let Err(err) = result {
                    // dispatcher.go:196: ObserveRejection precedes the
                    // StatusError / config-error split.
                    let message = match &err {
                        PatchError::Status(e) => e.to_string(),
                        PatchError::Other(m) => m.clone(),
                    };
                    observe_rejection(
                        elapsed,
                        PolicyAccessor::name(invocation.policy),
                        BindingAccessor::name(invocation.binding),
                        error_type(Some(&message)),
                    );
                    match err {
                        PatchError::Status(e) => return Err(e),
                        PatchError::Other(m) => {
                            policy_errors.push(config_error(m, invocation));
                            continue;
                        }
                    }
                } else {
                    // dispatcher.go:205.
                    observe_admission(
                        elapsed,
                        PolicyAccessor::name(invocation.policy),
                        BindingAccessor::name(invocation.binding),
                        MutationErrorType::NoError,
                    );
                }
            }
            let current = versioned
                .versioned_attribute(attr, &invocation.kind)
                .map_err(Error::Internal)?;
            if object_before_mutations != current.versioned_object {
                // The mutation has changed the object. Prepare to reinvoke all previous mutations that are eligible for re-invocation.
                reinvoke.with_policy_context(|c| c.require_reinvoking_previously_invoked_plugins());
                reinvoke.set_should_reinvoke();
            }
            if invocation
                .policy
                .spec
                .as_ref()
                .and_then(|s| s.reinvocation_policy.as_ref())
                == Some(&ReinvocationPolicyType::IfNeeded)
            {
                reinvoke.with_policy_context(|c| {
                    c.add_reinvocable_policy_to_previously_invoked(invocation_key)
                });
            }
        }

        if let Some(kind) = last_kind {
            if let Some(last) = versioned.cached(&kind) {
                if last.versioned_object.is_some() && last.dirty {
                    reinvoke
                        .with_policy_context(|c| c.require_reinvoking_previously_invoked_plugins());
                    reinvoke.set_should_reinvoke();
                    // `Convert(VersionedObject, Attributes.GetObject())`.
                    attr.object = last.versioned_object.clone();
                }
            }
        }

        Ok(policy_errors)
    }
}

#[async_trait]
impl DispatchDelegate<MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding, MutatingEvaluator>
    for MutatingDispatcher
{
    async fn dispatch(
        &self,
        attr: &mut Attributes,
        versioned: &mut VersionedAttributeAccessor<'_>,
        invocations: &[PolicyInvocation<
            '_,
            MutatingAdmissionPolicy,
            MutatingAdmissionPolicyBinding,
            MutatingEvaluator,
        >],
    ) -> Result<Vec<PolicyError>, Error> {
        let result = self
            .dispatch_invocations(attr, versioned, invocations)
            .await;
        // The deferred `SetLastPolicyInvocationOutput` (dispatcher.go:96-98)
        // runs on every return path.
        self.reinvocation
            .with_policy_context(|c| c.set_last_policy_invocation_output(attr.object.as_ref()));
        result
    }
}
