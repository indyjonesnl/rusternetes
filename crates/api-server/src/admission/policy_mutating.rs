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
//! - `NewCachingAuthorizer` and the `celmetrics` observations
//!   (`ObserveRejection` / `ObserveAdmission`) are not ported yet; the
//!   authorizer reaches CEL through the `Patcher` once the `patch/` package
//!   exists.
//! - `CompositionEnv.CreateContext` (:131-133) belongs to the CEL compiler and
//!   is the evaluator's business, not the dispatcher's.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::resources::mutating_admission_policy::ReinvocationPolicyType;
use rusternetes_common::resources::{MutatingAdmissionPolicy, MutatingAdmissionPolicyBinding};
use rusternetes_common::Error;
use serde_json::Value;

use super::policy_dispatch::{
    BindingAccessor, DispatchDelegate, NamespacedName, PolicyAccessor, PolicyError,
    PolicyInvocation, PolicyKey, PolicyReinvokeContext,
};
use super::policy_matching::Attributes;

/// mutating/plugin.go `PluginName`.
pub const PLUGIN_NAME: &str = "MutatingAdmissionPolicy";

/// `admission.ReinvocationContext`: shared by every plugin of one request.
/// Upstream's `Value`/`SetValue` store is keyed by plugin name.
#[derive(Debug, Default)]
pub struct ReinvocationContext {
    is_reinvoke: AtomicBool,
    should_reinvoke: AtomicBool,
    values: Mutex<HashMap<String, PolicyReinvokeContext>>,
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

/// `patch.Request` (patch/interface.go:35-44), minus the object interfaces
/// and type converter the JSON representation does not need.
pub struct PatchRequest<'a> {
    pub matched_resource: &'a GroupVersionResource,
    pub versioned_attributes: &'a VersionedAttributes,
    /// `OptionalVariables.VersionedParams`.
    pub param: Option<&'a Value>,
    /// The request's `v1.Namespace`; `None` for a cluster-scoped request.
    pub namespace: Option<&'a Value>,
}

/// `patch.Patcher` (patch/interface.go:29-33). `patch` returns a modified
/// copy and must not change the request's object in place.
#[async_trait]
pub trait Patcher: Send + Sync {
    async fn patch(&self, request: &PatchRequest<'_>) -> Result<Value, PatchError>;
}

/// `matchconditions.MatchResult`.
#[derive(Debug, Default)]
pub struct MatchResult {
    pub matches: bool,
    pub error: Option<String>,
}

/// `matchconditions.Matcher.Match` over the versioned attributes.
#[async_trait]
pub trait ConditionMatcher: Send + Sync {
    async fn matches(&self, versioned: &VersionedAttributes, param: Option<&Value>) -> MatchResult;
}

/// compilation.go `PolicyEvaluator`: the compiled match conditions and one
/// patcher per mutation (`None` is a nil patcher).
#[derive(Default)]
pub struct MutatingEvaluator {
    pub matcher: Option<Arc<dyn ConditionMatcher>>,
    pub mutators: Vec<Option<Arc<dyn Patcher>>>,
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
        versioned: &mut VersionedAttributes,
        namespace: Option<&Value>,
        resource: &GroupVersionResource,
        param: Option<&Value>,
    ) -> Result<(), PatchError> {
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

        let mut new_object = patcher
            .patch(&PatchRequest {
                matched_resource: resource,
                versioned_attributes: versioned,
                param,
                namespace,
            })
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
        invocations: &[PolicyInvocation<
            '_,
            MutatingAdmissionPolicy,
            MutatingAdmissionPolicyBinding,
            MutatingEvaluator,
        >],
    ) -> Result<Vec<PolicyError>, Error> {
        let reinvoke = &self.reinvocation;
        let mut versioned: Vec<(GroupVersionKind, VersionedAttributes)> = Vec::new();
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

            // `versionedAttributes.VersionedAttribute(invocation.Kind)`: the
            // conversion to the invoked version is the identity on JSON.
            let idx = match versioned.iter().position(|(k, _)| *k == invocation.kind) {
                Some(i) => i,
                None => {
                    versioned.push((
                        invocation.kind.clone(),
                        VersionedAttributes {
                            versioned_kind: invocation.kind.clone(),
                            versioned_object: attr.object.clone(),
                            versioned_old_object: attr.old_object.clone(),
                            dirty: false,
                        },
                    ));
                    versioned.len() - 1
                }
            };

            if let Some(matcher) = &invocation.evaluator.matcher {
                let result = matcher
                    .matches(&versioned[idx].1, invocation.param.as_ref())
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

            let object_before_mutations = versioned[idx].1.versioned_object.clone();
            // Mutations for a single invocation of a MutatingAdmissionPolicy are evaluated
            // in order.
            for mutation_index in 0..mutations {
                last_kind = Some(invocation.kind.clone());
                if versioned[idx].1.versioned_object.is_none() {
                    // Do not call patchers if there is no object to patch.
                    continue;
                }

                let patcher = invocation.evaluator.mutators[mutation_index].as_ref();
                if let Err(err) = self
                    .dispatch_one(
                        patcher,
                        &mut versioned[idx].1,
                        namespace.as_ref(),
                        &invocation.resource,
                        invocation.param.as_ref(),
                    )
                    .await
                {
                    match err {
                        PatchError::Status(e) => return Err(e),
                        PatchError::Other(m) => {
                            policy_errors.push(config_error(m, invocation));
                            continue;
                        }
                    }
                }
            }
            if object_before_mutations != versioned[idx].1.versioned_object {
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
            if let Some((_, last)) = versioned.iter().find(|(k, _)| *k == kind) {
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
        invocations: &[PolicyInvocation<
            '_,
            MutatingAdmissionPolicy,
            MutatingAdmissionPolicyBinding,
            MutatingEvaluator,
        >],
    ) -> Result<Vec<PolicyError>, Error> {
        let result = self.dispatch_invocations(attr, invocations).await;
        // The deferred `SetLastPolicyInvocationOutput` (dispatcher.go:96-98)
        // runs on every return path.
        self.reinvocation
            .with_policy_context(|c| c.set_last_policy_invocation_output(attr.object.as_ref()));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::admission::Operation;
    use rusternetes_common::resources::mutating_admission_policy::{
        MutatingAdmissionPolicyBindingSpec, MutatingAdmissionPolicySpec, Mutation, PatchType,
    };
    use rusternetes_common::resources::validating_admission_policy::FailurePolicy;
    use serde_json::json;

    fn deployment_gvk() -> GroupVersionKind {
        GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        }
    }
    fn deployment_gvr() -> GroupVersionResource {
        GroupVersionResource {
            group: "apps".into(),
            version: "v1".into(),
            resource: "deployments".into(),
        }
    }
    fn deployment(replicas: Option<i64>) -> Value {
        let mut d = json!({
            "kind": "Deployment", "apiVersion": "apps/v1",
            "metadata": {"name": "d1", "namespace": "default"},
            "spec": {"template": {"spec": {"volumes": [{"name": "x"}]}}},
        });
        if let Some(r) = replicas {
            d["spec"]["replicas"] = json!(r);
        }
        d
    }
    fn attr(object: Value) -> Attributes {
        Attributes {
            kind: deployment_gvk(),
            resource: deployment_gvr(),
            subresource: "".into(),
            namespace: "default".into(),
            name: "d1".into(),
            operation: Operation::Create,
            object: Some(object),
            old_object: None,
        }
    }

    /// `fakeSetDefaultForDeployment` (dispatcher_test.go:709-715).
    struct DefaultStrategy;
    impl ObjectDefaulter for DefaultStrategy {
        fn default_object(&self, o: &mut Value) {
            if o["spec"]["strategy"]["type"].is_null() {
                o["spec"]["strategy"] = json!({"type": "RollingUpdate"});
            }
        }
    }
    struct AllConverters;
    impl TypeConverters for AllConverters {
        fn has_type_converter(&self, _: &GroupVersionKind) -> bool {
            true
        }
    }
    struct NoConverters;
    impl TypeConverters for NoConverters {
        fn has_type_converter(&self, _: &GroupVersionKind) -> bool {
            false
        }
    }
    struct Namespaces;
    #[async_trait]
    impl NamespaceObjects for Namespaces {
        async fn get_namespace(&self, name: &str) -> Result<Value, Error> {
            if name == "default" {
                Ok(json!({"kind": "Namespace", "metadata": {"name": "default"}}))
            } else {
                Err(Error::NotFound(format!("namespaces \"{name}\" not found")))
            }
        }
    }

    type PatchFn = dyn Fn(&Value) -> Result<Value, PatchError> + Send + Sync;
    struct FnPatcher(Box<PatchFn>);
    #[async_trait]
    impl Patcher for FnPatcher {
        async fn patch(&self, r: &PatchRequest<'_>) -> Result<Value, PatchError> {
            (self.0)(r.versioned_attributes.versioned_object.as_ref().unwrap())
        }
    }
    fn patcher(
        f: impl Fn(&Value) -> Result<Value, PatchError> + Send + Sync + 'static,
    ) -> Option<Arc<dyn Patcher>> {
        Some(Arc::new(FnPatcher(Box::new(f))))
    }
    fn set_label(key: &'static str, val: &'static str) -> Option<Arc<dyn Patcher>> {
        patcher(move |o| {
            let mut o = o.clone();
            o["metadata"]["labels"][key] = json!(val);
            Ok(o)
        })
    }

    struct Cond(Box<dyn Fn(&Value) -> MatchResult + Send + Sync>);
    #[async_trait]
    impl ConditionMatcher for Cond {
        async fn matches(&self, v: &VersionedAttributes, _: Option<&Value>) -> MatchResult {
            (self.0)(v.versioned_object.as_ref().unwrap())
        }
    }
    fn cond(
        f: impl Fn(&Value) -> MatchResult + Send + Sync + 'static,
    ) -> Arc<dyn ConditionMatcher> {
        Arc::new(Cond(Box::new(f)))
    }

    fn policy(
        name: &str,
        mutations: usize,
        reinvocation: Option<ReinvocationPolicyType>,
    ) -> MutatingAdmissionPolicy {
        let mut p = MutatingAdmissionPolicy::new(name);
        p.spec = Some(MutatingAdmissionPolicySpec {
            mutations: Some(
                (0..mutations)
                    .map(|_| Mutation {
                        patch_type: Some(PatchType::ApplyConfiguration),
                        apply_configuration: None,
                        json_patch: None,
                    })
                    .collect(),
            ),
            reinvocation_policy: reinvocation,
            ..Default::default()
        });
        p
    }
    fn binding(name: &str, policy: &str) -> MutatingAdmissionPolicyBinding {
        let mut b = MutatingAdmissionPolicyBinding::new(name);
        b.spec = Some(MutatingAdmissionPolicyBindingSpec {
            policy_name: Some(policy.into()),
            ..Default::default()
        });
        b
    }

    struct Case {
        policy: MutatingAdmissionPolicy,
        binding: MutatingAdmissionPolicyBinding,
        evaluator: MutatingEvaluator,
        param: Option<Value>,
    }
    fn case(name: &str, evaluator: MutatingEvaluator) -> Case {
        Case {
            policy: policy(name, evaluator.mutators.len(), None),
            binding: binding("binding", name),
            evaluator,
            param: None,
        }
    }
    fn if_needed(mut c: Case) -> Case {
        let n = c.evaluator.mutators.len();
        c.policy = policy(
            PolicyAccessor::name(&c.policy),
            n,
            Some(ReinvocationPolicyType::IfNeeded),
        );
        c
    }
    fn mutators(m: Vec<Option<Arc<dyn Patcher>>>) -> MutatingEvaluator {
        MutatingEvaluator {
            matcher: None,
            mutators: m,
        }
    }
    fn key_of(policy: &str) -> PolicyKey {
        PolicyKey {
            policy: NamespacedName {
                name: policy.into(),
                namespace: "".into(),
            },
            binding: NamespacedName {
                name: "binding".into(),
                namespace: "".into(),
            },
            ..Default::default()
        }
    }

    fn dispatcher(reinvocation: Arc<ReinvocationContext>) -> MutatingDispatcher {
        MutatingDispatcher {
            namespaces: Arc::new(Namespaces),
            type_converters: Arc::new(AllConverters),
            defaulter: Arc::new(DefaultStrategy),
            reinvocation,
        }
    }
    async fn run(
        d: &MutatingDispatcher,
        attr: &mut Attributes,
        cases: &[Case],
    ) -> Result<Vec<PolicyError>, Error> {
        let invocations: Vec<_> = cases
            .iter()
            .map(|c| PolicyInvocation {
                policy: &c.policy,
                kind: deployment_gvk(),
                resource: deployment_gvr(),
                binding: &c.binding,
                evaluator: &c.evaluator,
                param: c.param.clone(),
            })
            .collect();
        d.dispatch(attr, &invocations).await
    }
    fn ctx() -> Arc<ReinvocationContext> {
        Arc::new(ReinvocationContext::new(false))
    }

    /// TestDispatcher "simple patch": the patched object is defaulted and
    /// replaces the request's object.
    #[tokio::test]
    async fn simple_patch_is_defaulted_and_applied() {
        let mut a = attr(deployment(Some(1)));
        let c = case(
            "policy1",
            mutators(vec![patcher(|o| {
                let mut o = o.clone();
                o["spec"]["replicas"] = json!(o["spec"]["replicas"].as_i64().unwrap() + 100);
                Ok(o)
            })]),
        );
        let errs = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap();
        assert!(errs.is_empty());
        let o = a.object.unwrap();
        assert_eq!(o["spec"]["replicas"], json!(101));
        assert_eq!(o["spec"]["strategy"]["type"], json!("RollingUpdate"));
    }

    /// TestDispatcher "both policies reinvoked": both labels land, and the
    /// second policy saw the first one's output.
    #[tokio::test]
    async fn later_policy_sees_earlier_policys_mutation() {
        let mut a = attr(deployment(None));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s2 = seen.clone();
        let c1 = case("policy1", mutators(vec![set_label("policy1", "2")]));
        let c2 = case(
            "policy2",
            mutators(vec![patcher(move |o| {
                s2.lock().unwrap().push(o["metadata"]["labels"].clone());
                let mut o = o.clone();
                o["metadata"]["labels"]["policy2"] = json!("2");
                Ok(o)
            })]),
        );
        run(&dispatcher(ctx()), &mut a, &[c1, c2]).await.unwrap();
        let o = a.object.unwrap();
        assert_eq!(
            o["metadata"]["labels"],
            json!({"policy1": "2", "policy2": "2"})
        );
        assert_eq!(*seen.lock().unwrap(), vec![json!({"policy1": "2"})]);
    }

    /// TestDispatcher "1st policy sets match condition that 2nd policy
    /// matches": the condition is evaluated on the already-mutated object.
    #[tokio::test]
    async fn match_conditions_see_earlier_mutations() {
        let mut a = attr(deployment(None));
        let c1 = case(
            "policy1",
            mutators(vec![set_label("environment", "production")]),
        );
        let mut ev = mutators(vec![set_label("policy1invoked", "true")]);
        ev.matcher = Some(cond(|o| MatchResult {
            matches: o["metadata"]["labels"]["environment"] == json!("production"),
            error: None,
        }));
        run(&dispatcher(ctx()), &mut a, &[c1, case("policy2", ev)])
            .await
            .unwrap();
        assert_eq!(
            a.object.unwrap()["metadata"]["labels"],
            json!({"environment": "production", "policy1invoked": "true"})
        );
    }

    /// TestDispatcher "1st policy still does not match": an unmet condition
    /// skips every mutation of the invocation.
    #[tokio::test]
    async fn unmet_match_condition_skips_the_mutations() {
        let mut a = attr(deployment(None));
        let mut ev = mutators(vec![set_label("p1", "x")]);
        ev.matcher = Some(cond(|_| MatchResult {
            matches: false,
            error: None,
        }));
        let before = a.object.clone();
        let errs = run(&dispatcher(ctx()), &mut a, &[case("policy1", ev)])
            .await
            .unwrap();
        assert!(errs.is_empty());
        assert_eq!(a.object, before);
    }

    #[tokio::test]
    async fn match_condition_error_is_an_invalid_policy_error_and_continues() {
        let mut a = attr(deployment(None));
        let mut ev = mutators(vec![set_label("p1", "x")]);
        ev.matcher = Some(cond(|_| MatchResult {
            matches: false,
            error: Some("boom".into()),
        }));
        let c2 = case("policy2", mutators(vec![set_label("p2", "y")]));
        let errs = run(&dispatcher(ctx()), &mut a, &[case("policy1", ev), c2])
            .await
            .unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].policy_name, "policy1");
        assert_eq!(errs[0].binding_name.as_deref(), Some("binding"));
        assert_eq!(errs[0].message, "boom");
        assert_eq!(errs[0].reason.as_deref(), Some("Invalid"));
        assert_eq!(a.object.unwrap()["metadata"]["labels"], json!({"p2": "y"}));
    }

    #[tokio::test]
    async fn non_status_patch_error_is_a_policy_error_and_status_error_fails_the_request() {
        let mut a = attr(deployment(None));
        let c = case(
            "policy1",
            mutators(vec![patcher(|_| Err(PatchError::Other("cel: bad".into())))]),
        );
        let errs = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].message, "cel: bad");
        assert_eq!(errs[0].reason.as_deref(), Some("Invalid"));

        let c = case(
            "policy1",
            mutators(vec![patcher(|_| {
                Err(PatchError::Status(Error::Forbidden("no".into())))
            })]),
        );
        let e = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap_err();
        assert!(matches!(e, Error::Forbidden(_)), "{e:?}");
    }

    #[tokio::test]
    async fn mutations_run_in_order_within_one_invocation() {
        let mut a = attr(deployment(None));
        let c = case(
            "policy1",
            mutators(vec![set_label("a", "1"), set_label("b", "2")]),
        );
        run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap();
        assert_eq!(
            a.object.unwrap()["metadata"]["labels"],
            json!({"a": "1", "b": "2"})
        );
    }

    #[tokio::test]
    async fn evaluator_with_the_wrong_number_of_mutators_is_an_internal_error() {
        let mut a = attr(deployment(None));
        let mut c = case("policy1", mutators(vec![set_label("a", "1")]));
        c.policy = policy("policy1", 2, None);
        let e = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap_err();
        assert!(
            matches!(&e, Error::Internal(m) if m == "expected 2 compiled evaluators for policy policy1, got 1"),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn nil_patcher_is_an_internal_error() {
        let mut a = attr(deployment(None));
        let c = case("policy1", mutators(vec![None]));
        let e = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap_err();
        assert!(
            matches!(&e, Error::Internal(m) if m == "policy evaluator is nil"),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn kind_without_a_type_converter_is_service_unavailable() {
        let mut a = attr(deployment(None));
        let mut d = dispatcher(ctx());
        d.type_converters = Arc::new(NoConverters);
        let c = case("policy1", mutators(vec![set_label("a", "1")]));
        let e = run(&d, &mut a, &[c]).await.unwrap_err();
        assert!(
            matches!(&e, Error::Network(m) if m.starts_with("Resource kind apps/v1, Kind=Deployment not found.")),
            "{e:?}"
        );
    }

    #[tokio::test]
    async fn missing_namespace_is_not_found_and_a_namespace_request_skips_the_lookup() {
        let mut a = attr(deployment(None));
        a.namespace = "gone".into();
        let c = case("policy1", mutators(vec![set_label("a", "1")]));
        let e = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap_err();
        assert!(matches!(e, Error::NotFound(_)), "{e:?}");

        // Special case: a Namespace's attributes carry its own name as the
        // namespace; it is unset so no lookup happens (dispatcher.go:108-112).
        let mut a = attr(json!({"kind": "Namespace", "metadata": {"name": "gone"}}));
        a.kind = GroupVersionKind {
            group: "".into(),
            version: "v1".into(),
            kind: "Namespace".into(),
        };
        a.namespace = "gone".into();
        let c = case("policy1", mutators(vec![set_label("a", "1")]));
        run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap();
        assert_eq!(a.object.unwrap()["metadata"]["labels"], json!({"a": "1"}));
    }

    #[tokio::test]
    async fn no_object_means_no_patchers_are_called() {
        let mut a = attr(Value::Null);
        a.object = None;
        a.operation = Operation::Delete;
        let c = case(
            "policy1",
            mutators(vec![patcher(|_| panic!("must not be called"))]),
        );
        let errs = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap();
        assert!(errs.is_empty());
        assert!(a.object.is_none());
    }

    /// A mutation that changed the object asks for reinvocation, and the
    /// IfNeeded policy that made it becomes eligible for it.
    #[tokio::test]
    async fn a_mutation_requests_reinvocation_of_if_needed_policies() {
        let rc = ctx();
        let mut a = attr(deployment(None));
        let c = if_needed(case("policy1", mutators(vec![set_label("a", "1")])));
        run(&dispatcher(rc.clone()), &mut a, &[c]).await.unwrap();
        assert!(rc.should_reinvoke());
        assert!(rc.with_policy_context(|c| c.should_reinvoke(&key_of("policy1"))));
    }

    /// reinvocationPolicy Never (or unset) is not remembered, so a later
    /// reinvocation does not run it.
    #[tokio::test]
    async fn never_policies_are_not_eligible_for_reinvocation() {
        let rc = ctx();
        let mut a = attr(deployment(None));
        let c = case("policy1", mutators(vec![set_label("a", "1")]));
        run(&dispatcher(rc.clone()), &mut a, &[c]).await.unwrap();
        assert!(rc.should_reinvoke());
        assert!(!rc.with_policy_context(|c| c.should_reinvoke(&key_of("policy1"))));
    }

    #[tokio::test]
    async fn reinvocation_reruns_eligible_policies() {
        let rc = ctx();
        let mut a = attr(deployment(None));
        let calls = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let mk = |name: &'static str| {
            let calls = calls.clone();
            if_needed(case(
                name,
                mutators(vec![patcher(move |o| {
                    calls.lock().unwrap().push(name);
                    Ok(o.clone())
                })]),
            ))
        };
        let cases = [mk("p1"), mk("p2")];
        run(&dispatcher(rc.clone()), &mut a, &cases).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec!["p1", "p2"]);
        assert!(rc.should_reinvoke());

        // The reinvocation pass shares the per-plugin state.
        let rc2 = Arc::new(ReinvocationContext::new(true));
        let carried = std::mem::take(&mut *rc.values.lock().unwrap());
        *rc2.values.lock().unwrap() = carried;
        calls.lock().unwrap().clear();
        run(&dispatcher(rc2), &mut a, &cases).await.unwrap();
        assert_eq!(*calls.lock().unwrap(), vec!["p1", "p2"]);
    }

    #[tokio::test]
    async fn on_reinvocation_a_policy_not_marked_is_skipped() {
        let rc = Arc::new(ReinvocationContext::new(true));
        let mut a = attr(deployment(None));
        let called = Arc::new(AtomicBool::new(false));
        let c2 = called.clone();
        let c = case(
            "policy1",
            mutators(vec![patcher(move |o| {
                c2.store(true, Ordering::SeqCst);
                Ok(o.clone())
            })]),
        );
        run(&dispatcher(rc), &mut a, &[c]).await.unwrap();
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn param_must_be_an_object_for_the_invocation_key() {
        let mut a = attr(deployment(None));
        let mut c = case("policy1", mutators(vec![set_label("a", "1")]));
        c.param = Some(json!("not an object"));
        let e = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap_err();
        assert!(matches!(e, Error::Internal(_)), "{e:?}");
    }

    #[test]
    fn key_for_names_policy_binding_and_param() {
        let p = policy("p", 1, None);
        let b = binding("b", "p");
        let param = json!({"metadata": {"name": "cm", "namespace": "ns"}});
        let k = key_for(&p, &b, Some(&param)).unwrap();
        assert_eq!(k.policy.name, "p");
        assert_eq!(k.binding.name, "b");
        assert_eq!(
            k.param,
            NamespacedName {
                name: "cm".into(),
                namespace: "ns".into()
            }
        );
        assert_eq!(
            key_for(&p, &b, None).unwrap().param,
            NamespacedName::default()
        );
    }

    #[tokio::test]
    async fn failure_policy_is_carried_on_policy_errors() {
        let mut a = attr(deployment(None));
        let mut c = case(
            "policy1",
            mutators(vec![patcher(|_| Err(PatchError::Other("x".into())))]),
        );
        c.policy.spec.as_mut().unwrap().failure_policy = Some(FailurePolicy::Ignore);
        let errs = run(&dispatcher(ctx()), &mut a, &[c]).await.unwrap();
        assert_eq!(errs[0].failure_policy, Some(FailurePolicy::Ignore));
    }

    /// dispatcher.go:96-98: the object at return is the next pass's baseline.
    #[tokio::test]
    async fn the_output_is_recorded_for_the_next_pass() {
        let rc = ctx();
        let mut a = attr(deployment(None));
        let c = case("policy1", mutators(vec![set_label("a", "1")]));
        run(&dispatcher(rc.clone()), &mut a, &[c]).await.unwrap();
        assert!(!rc.with_policy_context(|c| {
            c.is_output_changed_since_last_policy_invocation(a.object.as_ref())
        }));
    }
}
