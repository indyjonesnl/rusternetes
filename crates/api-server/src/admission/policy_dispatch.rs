//! The generic policy dispatcher and the MutatingAdmissionPolicy reinvocation
//! context (#2886, part of #2731).
//!
//! Ported from, in `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/`:
//! - `generic/policy_dispatcher.go` (`policyDispatcher.Dispatch`,
//!   `CollectParams`, `PolicyError`)
//! - `generic/accessor.go` (`PolicyAccessor`, `BindingAccessor`)
//! - `generic/plugin.go` / `interfaces.go` (`PolicyHook`, `Dispatcher`)
//! - `mutating/reinvocationcontext.go` (`policyReinvokeContext`, `key`)
//! - `mutating/dispatcher.go` `keyFor` (:249-276)
//!
//! Tests are ported from `mutating/reinvocationcontext_test.go`; upstream has
//! no unit test for `CollectParams` or `Dispatch` on their own, so those are
//! pinned branch by branch to the Go source.
//!
//! Not here yet (follow-ups of #2886): the mutating delegate
//! (`dispatchInvocations`, needs the CEL struct-literal evaluator #2885 and the
//! typed env #2834), versioned attributes / the type converter, the
//! policy/binding source, the equivalent-resource mapper, and the wiring at the
//! head of `Admission::admit`.

// Not called from the request path yet; the bin target compiles `admission`
// separately and would flag every item.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
use rusternetes_common::resources::validating_admission_policy::{
    FailurePolicy, MatchResources, ParamKind, ParamRef, ParameterNotFoundAction,
};
use rusternetes_common::types::{Selector, Status, StatusCause, StatusDetails};
use rusternetes_common::Error;
use serde_json::Value;

use super::policy_matching::{selector_of, Attributes, Matcher};

/// `types.NamespacedName`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct NamespacedName {
    pub name: String,
    pub namespace: String,
}

/// mutating/reinvocationcontext.go:28-33 (`key`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct PolicyKey {
    pub policy: NamespacedName,
    pub binding: NamespacedName,
    pub param: NamespacedName,
    pub mutation_index: usize,
}

/// mutating/reinvocationcontext.go:35-43 (`policyReinvokeContext`).
#[derive(Debug, Default)]
pub struct PolicyReinvokeContext {
    /// The result of the last policy admission plugin call.
    last_policy_output: Option<Value>,
    /// Policies that were invoked and should be reinvoked if a later mutation
    /// occurs.
    previously_invoked_reinvocable_policies: HashSet<PolicyKey>,
    /// Policies that should be reinvoked.
    reinvoke_policies: HashSet<PolicyKey>,
}

impl PolicyReinvokeContext {
    /// reinvocationcontext.go:45-47.
    pub fn should_reinvoke(&self, policy: &PolicyKey) -> bool {
        self.reinvoke_policies.contains(policy)
    }

    /// reinvocationcontext.go:49-51; `nil` against an object is a change.
    pub fn is_output_changed_since_last_policy_invocation(&self, object: Option<&Value>) -> bool {
        self.last_policy_output.as_ref() != object
    }

    /// reinvocationcontext.go:53-59 (a deep copy, as upstream).
    pub fn set_last_policy_invocation_output(&mut self, object: Option<&Value>) {
        self.last_policy_output = object.cloned();
    }

    /// reinvocationcontext.go:61-66.
    pub fn add_reinvocable_policy_to_previously_invoked(&mut self, policy: PolicyKey) {
        self.previously_invoked_reinvocable_policies.insert(policy);
    }

    /// reinvocationcontext.go:68-78: everything invoked so far becomes due for
    /// reinvocation, and the invoked set starts over.
    pub fn require_reinvoking_previously_invoked_plugins(&mut self) {
        self.reinvoke_policies
            .extend(self.previously_invoked_reinvocable_policies.drain());
    }
}

/// generic/accessor.go:25-31 (`PolicyAccessor`).
pub trait PolicyAccessor {
    fn name(&self) -> &str;
    fn namespace(&self) -> &str;
    fn param_kind(&self) -> Option<&ParamKind>;
    fn match_constraints(&self) -> Option<&MatchResources>;
    fn failure_policy(&self) -> Option<&FailurePolicy>;
}

/// generic/accessor.go:33-43 (`BindingAccessor`).
pub trait BindingAccessor {
    fn name(&self) -> &str;
    fn namespace(&self) -> &str;
    fn policy_name(&self) -> NamespacedName;
    fn param_ref(&self) -> Option<&ParamRef>;
    fn match_resources(&self) -> Option<&MatchResources>;
}

/// `meta.RESTScope` of a param kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamScope {
    Namespace,
    Root,
}

/// The `GenericInformer` a `PolicyHook` carries for its param kind, reduced
/// to what `CollectParams` calls: `Lister()` / `ByNamespace`, and
/// `Informer().HasSynced`.
#[async_trait]
pub trait ParamStore: Send + Sync {
    /// `GenericNamespaceLister.Get`; `namespace` is `None` for a cluster-scoped
    /// param kind. A miss is `Error::NotFound`.
    async fn get(&self, namespace: Option<&str>, name: &str) -> Result<Value, Error>;
    /// `GenericNamespaceLister.List`.
    async fn list(&self, namespace: Option<&str>, selector: &Selector)
        -> Result<Vec<Value>, Error>;
    /// `Informer().HasSynced`.
    fn has_synced(&self) -> bool;
}

/// `cache.WaitForCacheSync` polls every 100ms (client-go
/// `tools/cache/shared_informer.go` `syncedPollPeriod`).
const SYNCED_POLL_PERIOD: Duration = Duration::from_millis(100);
/// policy_dispatcher.go:261 `context.WithTimeout(..., 1*time.Second)`.
const PARAM_SYNC_TIMEOUT: Duration = Duration::from_secs(1);

async fn wait_for_cache_sync(store: &dyn ParamStore, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if store.has_synced() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(SYNCED_POLL_PERIOD).await;
    }
}

/// `ParamKind.String()` (the gogo-generated form, `generated.pb.go:2302`).
fn param_kind_string(k: &ParamKind) -> String {
    format!(
        "&ParamKind{{APIVersion:{},Kind:{},}}",
        k.api_version.as_deref().unwrap_or(""),
        k.kind
    )
}

/// `CollectParams` (policy_dispatcher.go:232-345). Returns the params to
/// evaluate a policy-binding with; no param configuration is a single `None`.
/// An empty list (not an error) means `parameterNotFoundAction: Allow`. The
/// error is the message upstream's `addConfigError` wraps.
pub async fn collect_params(
    param_kind: Option<&ParamKind>,
    param_store: Option<&dyn ParamStore>,
    param_scope: ParamScope,
    param_ref: Option<&ParamRef>,
    namespace: &str,
) -> Result<Vec<Option<Value>>, String> {
    // Where the param kind is ready to use, set up cluster-scoped or
    // namespaced access to the params.
    let mut store_namespace: Option<String> = None;
    if let (Some(kind), Some(pref)) = (param_kind, param_ref) {
        let Some(store) = param_store else {
            return Err(format!(
                "paramKind kind `{}` not known",
                param_kind_string(kind)
            ));
        };
        if param_scope == ParamScope::Namespace {
            // "default" to the request's namespace if not provided.
            let ns = match pref.namespace.as_deref() {
                Some(n) if !n.is_empty() => n,
                _ if namespace.is_empty() => {
                    // You must supply a namespace if your matcher can possibly
                    // match a cluster-scoped resource.
                    return Err("cannot use namespaced paramRef in policy binding that matches cluster-scoped resources".into());
                }
                _ => namespace,
            };
            store_namespace = Some(ns.to_string());
        }
        // If the param informer has not yet had time to perform an initial
        // listing, don't attempt to use it.
        if !wait_for_cache_sync(store, PARAM_SYNC_TIMEOUT).await {
            return Err(format!(
                "paramKind kind `{}` not yet synced to use for admission",
                param_kind_string(kind)
            ));
        }
    }

    // Find params to use with the policy.
    let Some(_) = param_kind else {
        // ParamKind is unset. Ignore any paramRef.
        return Ok(vec![None]);
    };
    let Some(pref) = param_ref else {
        // Policy ParamKind is set, but the binding does not use it. Validate
        // with nil params.
        return Ok(vec![None]);
    };
    let namespace_set = pref.namespace.as_deref().is_some_and(|n| !n.is_empty());
    if namespace_set && param_scope == ParamScope::Root {
        // Not allowed to set a namespace for a cluster-scoped param.
        return Err(
            "paramRef.namespace must not be provided for a cluster-scoped `paramKind`".into(),
        );
    }
    // `param_store` is Some: checked above.
    let store = param_store.expect("checked above");

    let params: Vec<Value> = match pref.name.as_deref() {
        Some(name) if !name.is_empty() => {
            if pref.selector.is_some() {
                // This should be validated, but just in case.
                return Err("paramRef.name and paramRef.selector are mutually exclusive".into());
            }
            match store.get(store_namespace.as_deref(), name).await {
                Ok(param) => vec![param],
                // Param not yet available. The user may need to wait a bit
                // before being able to use it; fall through to the not-found
                // action.
                Err(Error::NotFound(_)) => Vec::new(),
                // Param mis-configured (namespace set for a cluster-scoped
                // kind or unset for a namespaced one), or an internal error.
                Err(e) => return Err(e.to_string()),
            }
        }
        _ => match pref.selector.as_ref() {
            Some(_) => {
                // Cannot parse the label selector: configuration error.
                let selector = selector_of(pref.selector.as_ref())?;
                store
                    .list(store_namespace.as_deref(), &selector)
                    .await
                    .map_err(|e| e.to_string())?
            }
            // Should be unreachable due to validation.
            None => return Err("one of name or selector must be provided".into()),
        },
    };

    // Apply the fail action for the params-not-found case.
    if params.is_empty()
        && matches!(
            pref.parameter_not_found_action,
            Some(ParameterNotFoundAction::Deny)
        )
    {
        return Err(
            "no params found for policy binding with `Deny` parameterNotFoundAction".into(),
        );
    }
    Ok(params.into_iter().map(Some).collect())
}

/// policy_dispatcher.go:347-355 (`PolicyError`), with the policy's failure
/// policy copied in so the dispatcher need not hold the accessor.
#[derive(Debug, Clone, PartialEq)]
pub struct PolicyError {
    pub policy_name: String,
    pub failure_policy: Option<FailurePolicy>,
    pub binding_name: Option<String>,
    pub message: String,
    pub reason: Option<String>,
}

impl PolicyError {
    pub fn new(
        policy: &dyn PolicyAccessor,
        binding: Option<&dyn BindingAccessor>,
        message: impl Into<String>,
        reason: Option<&str>,
    ) -> Self {
        Self {
            policy_name: policy.name().to_string(),
            failure_policy: policy.failure_policy().cloned(),
            binding_name: binding.map(|b| b.name().to_string()),
            message: message.into(),
            reason: reason.map(str::to_string),
        }
    }

    /// `PolicyError.Error` (policy_dispatcher.go:357-363).
    pub fn error(&self) -> String {
        match &self.binding_name {
            Some(b) => format!(
                "policy '{}' with binding '{}' denied request: {}",
                self.policy_name, b, self.message
            ),
            None => format!(
                "policy {:?} denied request: {}",
                self.policy_name, self.message
            ),
        }
    }
}

/// generic/plugin.go `PolicyHook`: one policy with its bindings, its compiled
/// evaluator and the store for its param kind.
pub struct PolicyHook<P, B, E> {
    pub policy: P,
    pub bindings: Vec<B>,
    pub param_store: Option<Arc<dyn ParamStore>>,
    pub param_scope: ParamScope,
    pub evaluator: E,
    /// A policy that failed to compile or whose param kind could not be
    /// resolved (`ConfigurationError`).
    pub configuration_error: Option<String>,
}

/// policy_dispatcher.go:42-62 (`PolicyInvocation`): one policy-binding-param
/// tuple of a request.
pub struct PolicyInvocation<'a, P, B, E> {
    pub policy: &'a P,
    pub kind: GroupVersionKind,
    pub resource: GroupVersionResource,
    pub binding: &'a B,
    pub evaluator: &'a E,
    pub param: Option<Value>,
}

/// `dispatcherDelegate` (policy_dispatcher.go:64-71): the "mutation" or
/// "validation" half. An `Err` is a `StatusError` that fails the request.
#[async_trait]
pub trait DispatchDelegate<P: Sync, B: Sync, E: Sync>: Send + Sync {
    async fn dispatch(
        &self,
        attr: &mut Attributes,
        invocations: &[PolicyInvocation<'_, P, B, E>],
    ) -> Result<Vec<PolicyError>, Error>;
}

/// `admission.NewForbidden(a, "admission request denied by policy")` with the
/// message and reason the first denial supplies and every denial as a cause
/// (policy_dispatcher.go:181-215).
fn denied(attr: &Attributes, errors: &[PolicyError]) -> Error {
    let mut status = Status {
        kind: "Status".into(),
        api_version: "v1".into(),
        metadata: None,
        status: Some("Failure".into()),
        message: None,
        reason: Some("Forbidden".into()),
        details: Some(StatusDetails {
            name: Some(attr.name.clone()),
            group: Some(attr.resource.group.clone()),
            kind: Some(attr.resource.resource.clone()),
            uid: None,
            causes: Some(Vec::new()),
            retry_after_seconds: None,
        }),
        code: Some(403),
    };
    for e in errors {
        let message = e.error();
        if status.message.is_none() {
            status.message = Some(message.clone());
            if let Some(reason) = e.reason.as_deref().filter(|r| !r.is_empty()) {
                status.reason = Some(reason.to_string());
            }
        }
        if let Some(causes) = status.details.as_mut().and_then(|d| d.causes.as_mut()) {
            causes.push(StatusCause {
                reason: None,
                message: Some(message),
                field: None,
            });
        }
    }
    Error::Status(Box::new(status))
}

/// `policyDispatcher.Dispatch` (policy_dispatcher.go:99-229): select the
/// policy-binding pairs that match the request, resolve their params, hand the
/// tuples to the delegate, and fail the request with every error whose policy
/// has `failurePolicy: Fail`.
///
/// Deviation: upstream also pre-warms `VersionedAttribute(matchGVK)` per
/// binding; that is the delegate's concern until versioned attributes exist
/// (follow-up on #2886).
pub async fn dispatch<P, B, E>(
    matcher: &Matcher<'_>,
    attr: &mut Attributes,
    hooks: &[PolicyHook<P, B, E>],
    delegate: &dyn DispatchDelegate<P, B, E>,
) -> Result<(), Error>
where
    P: PolicyAccessor + Sync,
    B: BindingAccessor + Sync,
    E: Sync,
{
    let mut relevant: Vec<PolicyInvocation<'_, P, B, E>> = Vec::new();
    let mut policy_errors: Vec<PolicyError> = Vec::new();
    let config_error = |policy: &P, binding: Option<&B>, err: String| PolicyError {
        policy_name: policy.name().to_string(),
        failure_policy: policy.failure_policy().cloned(),
        binding_name: binding.map(|b| b.name().to_string()),
        message: match binding {
            None => format!("failed to configure policy: {err}"),
            Some(_) => format!("failed to configure binding: {err}"),
        },
        reason: None,
    };

    for hook in hooks {
        let matched = match matcher
            .definition_matches(attr, hook.policy.match_constraints())
            .await
        {
            // An error evaluating whether this policy matches anything.
            Err(e) => {
                policy_errors.push(config_error(&hook.policy, None, error_text(&e)));
                continue;
            }
            Ok(None) => continue,
            Ok(Some(m)) => m,
        };
        if let Some(err) = &hook.configuration_error {
            policy_errors.push(config_error(&hook.policy, None, err.clone()));
            continue;
        }

        for binding in &hook.bindings {
            match matcher
                .binding_matches(attr, binding.match_resources())
                .await
            {
                Err(e) => {
                    policy_errors.push(config_error(&hook.policy, Some(binding), error_text(&e)));
                    continue;
                }
                Ok(false) => continue,
                Ok(true) => {}
            }

            // Collect params for this binding.
            let params = match collect_params(
                hook.policy.param_kind(),
                hook.param_store.as_deref(),
                hook.param_scope,
                binding.param_ref(),
                &attr.namespace,
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    policy_errors.push(config_error(&hook.policy, Some(binding), e));
                    continue;
                }
            };

            // Empty params without an error is parameterNotFoundAction
            // Allow: nothing to add.
            for param in params {
                relevant.push(PolicyInvocation {
                    policy: &hook.policy,
                    kind: matched.kind.clone(),
                    resource: matched.resource.clone(),
                    binding,
                    evaluator: &hook.evaluator,
                    param,
                });
            }
        }
    }

    if !relevant.is_empty() {
        policy_errors.extend(delegate.dispatch(&mut *attr, &relevant).await?);
    }

    // The failure policy defaults to Fail (and is validated at the API level).
    let filtered: Vec<PolicyError> = policy_errors
        .into_iter()
        .filter(|e| !matches!(e.failure_policy, Some(FailurePolicy::Ignore)))
        .collect();
    if filtered.is_empty() {
        Ok(())
    } else {
        Err(denied(attr, &filtered))
    }
}

/// `err.Error()` for the errors the matcher returns, without the variant
/// prefix `thiserror` adds (`Error::Internal` renders `Internal error: ..`).
fn error_text(e: &Error) -> String {
    match e {
        Error::Internal(m) | Error::NotFound(m) => m.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::admission::Operation;
    use rusternetes_common::resources::validating_admission_policy::{
        MatchPolicyType, NamedRuleWithOperations, OperationType, RuleWithOperations,
    };
    use rusternetes_common::resources::LabelSelector;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn key(n: &str) -> PolicyKey {
        PolicyKey {
            policy: NamespacedName {
                name: format!("p{n}"),
                ..Default::default()
            },
            binding: NamespacedName {
                name: format!("b{n}"),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    // ---- reinvocationcontext_test.go ------------------------------------

    #[test]
    fn full_reinvocation() {
        let (key1, key2, key3) = (key("1"), key("2"), key("3"));
        let v1 = json!({"data": {"v": "1"}});
        let v2 = json!({"data": {"v": "2"}});
        let mut rc = PolicyReinvokeContext::default();

        // key1 is invoked and it updates the configmap
        rc.set_last_policy_invocation_output(Some(&v1));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key1.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        // key2 is invoked and it updates the configmap
        rc.set_last_policy_invocation_output(Some(&v2));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key2.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v1)));

        // key3 is invoked but it does not change anything
        rc.add_reinvocable_policy_to_previously_invoked(key3.clone());
        assert!(!rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        // key1 is reinvoked
        assert!(rc.should_reinvoke(&key1));
        rc.add_reinvocable_policy_to_previously_invoked(key1);
        rc.set_last_policy_invocation_output(Some(&v1));
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v2)));
        rc.require_reinvoking_previously_invoked_plugins();

        // key2 is reinvoked
        assert!(rc.should_reinvoke(&key2));
        rc.add_reinvocable_policy_to_previously_invoked(key2);
        rc.set_last_policy_invocation_output(Some(&v2));
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v1)));
        rc.require_reinvoking_previously_invoked_plugins();

        // key3 is reinvoked, because the reinvocations changed the resource
        assert!(rc.should_reinvoke(&key3));
    }

    #[test]
    fn partial_reinvocation() {
        let (key1, key2, key3) = (key("1"), key("2"), key("3"));
        let v1 = json!({"data": {"v": "1"}});
        let v2 = json!({"data": {"v": "2"}});
        let mut rc = PolicyReinvokeContext::default();

        rc.set_last_policy_invocation_output(Some(&v1));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key1.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        rc.set_last_policy_invocation_output(Some(&v2));
        rc.require_reinvoking_previously_invoked_plugins();
        rc.add_reinvocable_policy_to_previously_invoked(key2.clone());
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v1)));

        rc.add_reinvocable_policy_to_previously_invoked(key3.clone());
        assert!(!rc.is_output_changed_since_last_policy_invocation(Some(&v2)));

        // key1 is reinvoked but does not change anything
        assert!(rc.should_reinvoke(&key1));
        // key2 and key3 are not reinvoked: nothing changed since
        assert!(!rc.should_reinvoke(&key2));
        assert!(!rc.should_reinvoke(&key3));
    }

    #[test]
    fn no_reinvocation() {
        let (key1, key2, key3) = (key("1"), key("2"), key("3"));
        let v1 = json!({"data": {"v": "1"}});
        let mut rc = PolicyReinvokeContext::default();

        for k in [&key1, &key2, &key3] {
            rc.add_reinvocable_policy_to_previously_invoked(k.clone());
            rc.set_last_policy_invocation_output(Some(&v1));
            assert!(!rc.is_output_changed_since_last_policy_invocation(Some(&v1)));
        }
        assert!(!rc.should_reinvoke(&key1));
        assert!(!rc.should_reinvoke(&key2));
        assert!(!rc.should_reinvoke(&key3));
    }

    #[test]
    fn nil_output_is_a_change_from_an_object() {
        // SetLastPolicyInvocationOutput(nil) stores nil (reinvocationcontext.go:54-57)
        let mut rc = PolicyReinvokeContext::default();
        assert!(!rc.is_output_changed_since_last_policy_invocation(None));
        let v = json!({"a": 1});
        assert!(rc.is_output_changed_since_last_policy_invocation(Some(&v)));
        rc.set_last_policy_invocation_output(Some(&v));
        assert!(rc.is_output_changed_since_last_policy_invocation(None));
        rc.set_last_policy_invocation_output(None);
        assert!(!rc.is_output_changed_since_last_policy_invocation(None));
    }

    #[test]
    fn requiring_reinvocation_drains_the_invoked_set() {
        let k = key("1");
        let mut rc = PolicyReinvokeContext::default();
        rc.add_reinvocable_policy_to_previously_invoked(k.clone());
        rc.require_reinvoking_previously_invoked_plugins();
        assert!(rc.should_reinvoke(&k));
        // a second call has nothing new to add, and keeps what is due
        rc.require_reinvoking_previously_invoked_plugins();
        assert!(rc.should_reinvoke(&k));
        assert!(rc.previously_invoked_reinvocable_policies.is_empty());
    }

    // ---- CollectParams --------------------------------------------------

    #[derive(Default)]
    struct Store {
        /// (namespace, name) -> object
        objects: Vec<(Option<String>, Value)>,
        synced: bool,
        get_error: Option<fn() -> Error>,
    }
    #[async_trait]
    impl ParamStore for Store {
        async fn get(&self, ns: Option<&str>, name: &str) -> Result<Value, Error> {
            if let Some(f) = self.get_error {
                return Err(f());
            }
            self.objects
                .iter()
                .find(|(n, o)| n.as_deref() == ns && o["metadata"]["name"] == name)
                .map(|(_, o)| o.clone())
                .ok_or_else(|| Error::NotFound(format!("{name} not found")))
        }
        async fn list(&self, ns: Option<&str>, selector: &Selector) -> Result<Vec<Value>, Error> {
            Ok(self
                .objects
                .iter()
                .filter(|(n, _)| ns.is_none() || n.as_deref() == ns)
                .filter(|(_, o)| {
                    let labels: HashMap<String, String> = o["metadata"]["labels"]
                        .as_object()
                        .map(|m| {
                            m.iter()
                                .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    selector.matches(Some(&labels))
                })
                .map(|(_, o)| o.clone())
                .collect())
        }
        fn has_synced(&self) -> bool {
            self.synced
        }
    }

    fn cm(name: &str, labels: Value) -> Value {
        json!({"metadata": {"name": name, "labels": labels}})
    }
    fn kind() -> ParamKind {
        ParamKind {
            api_version: Some("v1".into()),
            kind: "ConfigMap".into(),
        }
    }
    fn pref(name: Option<&str>) -> ParamRef {
        ParamRef {
            name: name.map(str::to_string),
            namespace: None,
            selector: None,
            parameter_not_found_action: None,
        }
    }
    fn synced(objects: Vec<(Option<String>, Value)>) -> Store {
        Store {
            objects,
            synced: true,
            get_error: None,
        }
    }

    #[tokio::test]
    async fn params_without_a_param_kind_or_ref_are_a_single_nil() {
        let r = pref(Some("x"));
        // paramKind unset: any paramRef is ignored
        assert_eq!(
            collect_params(None, None, ParamScope::Root, Some(&r), "ns").await,
            Ok(vec![None])
        );
        // paramKind set, binding has no paramRef
        let s = synced(vec![]);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, None, "ns").await,
            Ok(vec![None])
        );
    }

    #[tokio::test]
    async fn params_by_name_namespaced_default_to_the_request_namespace() {
        let obj = cm("p", json!({}));
        let s = synced(vec![(Some("ns1".into()), obj.clone())]);
        let r = pref(Some("p"));
        assert_eq!(
            collect_params(
                Some(&kind()),
                Some(&s),
                ParamScope::Namespace,
                Some(&r),
                "ns1"
            )
            .await,
            Ok(vec![Some(obj.clone())])
        );
        // paramRef.namespace wins over the request's
        let mut r2 = pref(Some("p"));
        r2.namespace = Some("ns1".into());
        assert_eq!(
            collect_params(
                Some(&kind()),
                Some(&s),
                ParamScope::Namespace,
                Some(&r2),
                "other"
            )
            .await,
            Ok(vec![Some(obj)])
        );
    }

    #[tokio::test]
    async fn namespaced_param_ref_cannot_serve_a_cluster_scoped_request() {
        let s = synced(vec![]);
        let r = pref(Some("p"));
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Namespace, Some(&r), "").await,
            Err("cannot use namespaced paramRef in policy binding that matches cluster-scoped resources".into())
        );
    }

    #[tokio::test]
    async fn namespace_on_a_cluster_scoped_param_kind_is_rejected() {
        let s = synced(vec![]);
        let mut r = pref(Some("p"));
        r.namespace = Some("ns".into());
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "ns").await,
            Err("paramRef.namespace must not be provided for a cluster-scoped `paramKind`".into())
        );
    }

    #[tokio::test]
    async fn unknown_or_unsynced_param_kind_is_a_config_error() {
        let r = pref(Some("p"));
        assert_eq!(
            collect_params(Some(&kind()), None, ParamScope::Root, Some(&r), "").await,
            Err("paramKind kind `&ParamKind{APIVersion:v1,Kind:ConfigMap,}` not known".into())
        );
        let s = Store::default();
        tokio::time::pause();
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("paramKind kind `&ParamKind{APIVersion:v1,Kind:ConfigMap,}` not yet synced to use for admission".into())
        );
    }

    #[tokio::test]
    async fn name_and_selector_are_mutually_exclusive() {
        let s = synced(vec![]);
        let mut r = pref(Some("p"));
        r.selector = Some(LabelSelector::default());
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("paramRef.name and paramRef.selector are mutually exclusive".into())
        );
        // neither
        let r = pref(None);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("one of name or selector must be provided".into())
        );
    }

    #[tokio::test]
    async fn selector_lists_every_match() {
        let a = cm("a", json!({"k": "v"}));
        let b = cm("b", json!({"k": "w"}));
        let c = cm("c", json!({"k": "v"}));
        let s = synced(vec![
            (None, a.clone()),
            (None, b.clone()),
            (None, c.clone()),
        ]);
        let mut r = pref(None);
        r.selector = Some(LabelSelector {
            match_labels: Some([("k".to_string(), "v".to_string())].into()),
            match_expressions: None,
        });
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Ok(vec![Some(a), Some(c)])
        );
    }

    #[tokio::test]
    async fn missing_param_follows_parameter_not_found_action() {
        let s = synced(vec![]);
        let mut r = pref(Some("absent"));
        // unset and Allow: no params, no error
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Ok(vec![])
        );
        r.parameter_not_found_action = Some(ParameterNotFoundAction::Allow);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Ok(vec![])
        );
        r.parameter_not_found_action = Some(ParameterNotFoundAction::Deny);
        assert_eq!(
            collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await,
            Err("no params found for policy binding with `Deny` parameterNotFoundAction".into())
        );
    }

    #[tokio::test]
    async fn store_errors_other_than_not_found_are_config_errors() {
        let mut s = synced(vec![]);
        s.get_error = Some(|| Error::Internal("boom".into()));
        let r = pref(Some("p"));
        let got = collect_params(Some(&kind()), Some(&s), ParamScope::Root, Some(&r), "").await;
        assert!(got.unwrap_err().contains("boom"));
    }

    // ---- Dispatch -------------------------------------------------------

    use crate::admission::policy_matching::{EquivalentResourceMapper, NamespaceLister};

    struct NoMapper;
    impl EquivalentResourceMapper for NoMapper {
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
    struct NoNamespaces;
    #[async_trait]
    impl NamespaceLister for NoNamespaces {
        async fn namespace_labels(
            &self,
            name: &str,
        ) -> rusternetes_common::Result<HashMap<String, String>> {
            Err(Error::NotFound(format!("namespace {name} not found")))
        }
    }

    fn gvk(g: &str, v: &str, k: &str) -> GroupVersionKind {
        GroupVersionKind {
            group: g.into(),
            version: v.into(),
            kind: k.into(),
        }
    }
    fn pod_attr() -> Attributes {
        Attributes {
            kind: gvk("", "v1", "Pod"),
            resource: GroupVersionResource {
                group: "".into(),
                version: "v1".into(),
                resource: "pods".into(),
            },
            subresource: "".into(),
            namespace: "".into(),
            name: "p".into(),
            operation: Operation::Create,
            object: Some(json!({"metadata": {"name": "p"}})),
            old_object: None,
        }
    }
    fn match_pods() -> MatchResources {
        MatchResources {
            namespace_selector: Some(LabelSelector::default()),
            object_selector: Some(LabelSelector::default()),
            resource_rules: Some(vec![NamedRuleWithOperations {
                resource_names: None,
                rule: RuleWithOperations {
                    operations: Some(vec![OperationType::All]),
                    api_groups: Some(vec!["*".into()]),
                    api_versions: Some(vec!["*".into()]),
                    resources: Some(vec!["pods".into()]),
                    scope: None,
                },
            }]),
            exclude_resource_rules: None,
            match_policy: Some(MatchPolicyType::Exact),
        }
    }

    struct Pol {
        name: &'static str,
        constraints: Option<MatchResources>,
        kind: Option<ParamKind>,
        failure: Option<FailurePolicy>,
    }
    impl PolicyAccessor for Pol {
        fn name(&self) -> &str {
            self.name
        }
        fn namespace(&self) -> &str {
            ""
        }
        fn param_kind(&self) -> Option<&ParamKind> {
            self.kind.as_ref()
        }
        fn match_constraints(&self) -> Option<&MatchResources> {
            self.constraints.as_ref()
        }
        fn failure_policy(&self) -> Option<&FailurePolicy> {
            self.failure.as_ref()
        }
    }
    struct Bind {
        name: &'static str,
        resources: Option<MatchResources>,
        param_ref: Option<ParamRef>,
    }
    impl BindingAccessor for Bind {
        fn name(&self) -> &str {
            self.name
        }
        fn namespace(&self) -> &str {
            ""
        }
        fn policy_name(&self) -> NamespacedName {
            NamespacedName::default()
        }
        fn param_ref(&self) -> Option<&ParamRef> {
            self.param_ref.as_ref()
        }
        fn match_resources(&self) -> Option<&MatchResources> {
            self.resources.as_ref()
        }
    }
    fn pol(name: &'static str) -> Pol {
        Pol {
            name,
            constraints: Some(match_pods()),
            kind: None,
            failure: None,
        }
    }
    fn bind(name: &'static str) -> Bind {
        Bind {
            name,
            resources: None,
            param_ref: None,
        }
    }
    fn hook(p: Pol, bindings: Vec<Bind>) -> PolicyHook<Pol, Bind, ()> {
        PolicyHook {
            policy: p,
            bindings,
            param_store: None,
            param_scope: ParamScope::Root,
            evaluator: (),
            configuration_error: None,
        }
    }

    /// Records what it was called with and returns canned errors.
    #[derive(Default)]
    struct Delegate {
        seen: Mutex<Vec<(String, String, Option<Value>)>>,
        errors: Mutex<Vec<PolicyError>>,
        status_error: Option<fn() -> Error>,
    }
    #[async_trait]
    impl DispatchDelegate<Pol, Bind, ()> for Delegate {
        async fn dispatch(
            &self,
            _: &mut Attributes,
            invocations: &[PolicyInvocation<'_, Pol, Bind, ()>],
        ) -> Result<Vec<PolicyError>, Error> {
            for i in invocations {
                self.seen.lock().unwrap().push((
                    i.policy.name.to_string(),
                    i.binding.name.to_string(),
                    i.param.clone(),
                ));
            }
            if let Some(f) = self.status_error {
                return Err(f());
            }
            Ok(self.errors.lock().unwrap().clone())
        }
    }

    async fn run(hooks: &[PolicyHook<Pol, Bind, ()>], delegate: &Delegate) -> Result<(), Error> {
        let m = Matcher {
            namespaces: &NoNamespaces,
            mapper: &NoMapper,
        };
        dispatch(&m, &mut pod_attr(), hooks, delegate).await
    }
    fn status_of(e: Error) -> Status {
        match e {
            Error::Status(s) => *s,
            other => panic!("not a status: {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_matching_policy_is_not_dispatched() {
        let mut p = pol("p");
        let mut mr = match_pods();
        mr.resource_rules.as_mut().unwrap()[0].rule.resources = Some(vec!["deployments".into()]);
        p.constraints = Some(mr);
        let d = Delegate::default();
        assert!(run(&[hook(p, vec![bind("b")])], &d).await.is_ok());
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn matching_pairs_reach_the_delegate_once_per_binding() {
        let d = Delegate::default();
        run(&[hook(pol("p"), vec![bind("b1"), bind("b2")])], &d)
            .await
            .unwrap();
        assert_eq!(
            *d.seen.lock().unwrap(),
            vec![
                ("p".into(), "b1".into(), None),
                ("p".into(), "b2".into(), None)
            ]
        );
    }

    #[tokio::test]
    async fn non_matching_binding_is_skipped() {
        let mut b = bind("skip");
        let mut mr = match_pods();
        mr.resource_rules.as_mut().unwrap()[0].rule.resources = Some(vec!["nodes".into()]);
        b.resources = Some(mr);
        let d = Delegate::default();
        run(&[hook(pol("p"), vec![b, bind("keep")])], &d)
            .await
            .unwrap();
        assert_eq!(d.seen.lock().unwrap().len(), 1);
        assert_eq!(d.seen.lock().unwrap()[0].1, "keep");
    }

    #[tokio::test]
    async fn a_policy_without_match_constraints_is_denied_as_a_config_error() {
        let mut p = pol("p");
        p.constraints = None;
        let d = Delegate::default();
        let s = status_of(run(&[hook(p, vec![bind("b")])], &d).await.unwrap_err());
        assert_eq!(s.reason.as_deref(), Some("Forbidden"));
        assert_eq!(
            s.message.as_deref(),
            Some("policy \"p\" denied request: failed to configure policy: policy contained no match constraints, a required field")
        );
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failure_policy_ignore_swallows_config_errors() {
        let mut p = pol("p");
        p.constraints = None;
        p.failure = Some(FailurePolicy::Ignore);
        assert!(run(&[hook(p, vec![bind("b")])], &Delegate::default())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn hook_configuration_error_is_a_policy_error() {
        let mut h = hook(pol("p"), vec![bind("b")]);
        h.configuration_error = Some("compile failed".into());
        let d = Delegate::default();
        let s = status_of(run(&[h], &d).await.unwrap_err());
        assert_eq!(
            s.message.as_deref(),
            Some("policy \"p\" denied request: failed to configure policy: compile failed")
        );
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn param_errors_name_the_binding_and_skip_it() {
        let mut p = pol("p");
        p.kind = Some(kind());
        let mut b = bind("b");
        b.param_ref = Some(pref(Some("x")));
        let d = Delegate::default();
        // no param store: "not known"
        let s = status_of(run(&[hook(p, vec![b])], &d).await.unwrap_err());
        assert_eq!(
            s.message.as_deref(),
            Some("policy 'p' with binding 'b' denied request: failed to configure binding: paramKind kind `&ParamKind{APIVersion:v1,Kind:ConfigMap,}` not known")
        );
        assert!(d.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resolved_params_each_become_an_invocation() {
        let a = cm("a", json!({"k": "v"}));
        let c = cm("c", json!({"k": "v"}));
        let mut p = pol("p");
        p.kind = Some(kind());
        let mut b = bind("b");
        let mut r = pref(None);
        r.selector = Some(LabelSelector {
            match_labels: Some([("k".to_string(), "v".to_string())].into()),
            match_expressions: None,
        });
        b.param_ref = Some(r);
        let mut h = hook(p, vec![b]);
        h.param_store = Some(Arc::new(synced(vec![(None, a.clone()), (None, c.clone())])));
        let d = Delegate::default();
        run(&[h], &d).await.unwrap();
        let seen = d.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].2, Some(a));
        assert_eq!(seen[1].2, Some(c));
    }

    #[tokio::test]
    async fn delegate_status_error_fails_the_request_as_is() {
        let d = Delegate {
            status_error: Some(|| Error::BadRequest("delegate says no".into())),
            ..Default::default()
        };
        let e = run(&[hook(pol("p"), vec![bind("b")])], &d)
            .await
            .unwrap_err();
        assert!(matches!(e, Error::BadRequest(m) if m == "delegate says no"));
    }

    #[tokio::test]
    async fn delegate_errors_are_filtered_by_failure_policy_and_denial_lists_each() {
        let ignored = Pol {
            failure: Some(FailurePolicy::Ignore),
            ..pol("ignored")
        };
        let failing = pol("failing");
        let d = Delegate::default();
        *d.errors.lock().unwrap() = vec![
            PolicyError::new(&ignored, Some(&bind("b")), "dropped", None),
            PolicyError::new(&failing, Some(&bind("b1")), "first", Some("Invalid")),
            PolicyError::new(&failing, Some(&bind("b2")), "second", None),
        ];
        let s = status_of(
            run(
                &[
                    hook(ignored, vec![bind("b")]),
                    hook(failing, vec![bind("b1")]),
                ],
                &d,
            )
            .await
            .unwrap_err(),
        );
        // the first denial supplies the message and reason; code stays 403
        assert_eq!(
            s.message.as_deref(),
            Some("policy 'failing' with binding 'b1' denied request: first")
        );
        assert_eq!(s.reason.as_deref(), Some("Invalid"));
        assert_eq!(s.code, Some(403));
        let causes = s.details.unwrap().causes.unwrap();
        assert_eq!(causes.len(), 2);
        assert_eq!(
            causes[1].message.as_deref(),
            Some("policy 'failing' with binding 'b2' denied request: second")
        );
    }

    #[test]
    fn policy_error_text() {
        let p = pol("p");
        assert_eq!(
            PolicyError::new(&p, Some(&bind("b")), "m", None).error(),
            "policy 'p' with binding 'b' denied request: m"
        );
        assert_eq!(
            PolicyError::new(&p, None, "m", None).error(),
            "policy \"p\" denied request: m"
        );
    }
}
