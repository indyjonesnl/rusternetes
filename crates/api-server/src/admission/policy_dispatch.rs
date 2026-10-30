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
//! The mutating delegate (`dispatchInvocations`) is in `policy_mutating.rs`;
//! the equivalent-resource mapper and namespace lister are in
//! `policy_registry.rs` (#2909). Not here yet (follow-ups of #2886): the
//! `patch/` package (applyConfiguration, jsonPatch) and the type converter,
//! and the wiring at the head of `Admission::admit`.

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
use super::policy_mutating::VersionedAttributes;

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
    pub last_policy_output: Option<Value>,
    /// Policies that were invoked and should be reinvoked if a later mutation
    /// occurs.
    pub previously_invoked_reinvocable_policies: HashSet<PolicyKey>,
    /// Policies that should be reinvoked.
    pub reinvoke_policies: HashSet<PolicyKey>,
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
        versioned: &mut VersionedAttributeAccessor<'_>,
        invocations: &[PolicyInvocation<'_, P, B, E>],
    ) -> Result<Vec<PolicyError>, Error>;
}

/// `admission.NewVersionedAttributes` (apiserver `pkg/admission/attributes.go`):
/// converts the request's objects to `gvk`. The error is what a failed
/// conversion returns.
pub trait VersionConverter: Send + Sync {
    fn new_versioned_attributes(
        &self,
        attr: &Attributes,
        gvk: &GroupVersionKind,
    ) -> Result<VersionedAttributes, String>;
}

/// Objects are JSON in every version here, so converting to `gvk` is the
/// identity on the request's objects.
pub struct IdentityConverter;

impl VersionConverter for IdentityConverter {
    fn new_versioned_attributes(
        &self,
        attr: &Attributes,
        gvk: &GroupVersionKind,
    ) -> Result<VersionedAttributes, String> {
        Ok(VersionedAttributes {
            versioned_kind: gvk.clone(),
            versioned_object: attr.object.clone(),
            versioned_old_object: attr.old_object.clone(),
            dirty: false,
        })
    }
}

/// policy_dispatcher.go `versionedAttributeAccessor` (`webhookgeneric.
/// VersionedAttributeAccessor`): one `VersionedAttributes` per kind, built on
/// first use and cached for every later hook call.
pub struct VersionedAttributeAccessor<'a> {
    versioned_attrs: Vec<(GroupVersionKind, VersionedAttributes)>,
    converter: &'a dyn VersionConverter,
}

impl<'a> VersionedAttributeAccessor<'a> {
    pub fn new(converter: &'a dyn VersionConverter) -> Self {
        Self {
            versioned_attrs: Vec::new(),
            converter,
        }
    }

    /// `VersionedAttribute(gvk)` (policy_dispatcher.go:378-389).
    pub fn versioned_attribute(
        &mut self,
        attr: &Attributes,
        gvk: &GroupVersionKind,
    ) -> Result<&mut VersionedAttributes, String> {
        let idx = match self.versioned_attrs.iter().position(|(k, _)| k == gvk) {
            Some(i) => i,
            None => {
                let v = self.converter.new_versioned_attributes(attr, gvk)?;
                self.versioned_attrs.push((gvk.clone(), v));
                self.versioned_attrs.len() - 1
            }
        };
        Ok(&mut self.versioned_attrs[idx].1)
    }

    /// The cached entry for `gvk`, without building one.
    pub fn cached(&self, gvk: &GroupVersionKind) -> Option<&VersionedAttributes> {
        self.versioned_attrs
            .iter()
            .find(|(k, _)| k == gvk)
            .map(|(_, v)| v)
    }
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
    dispatch_with_converter(matcher, attr, hooks, delegate, &IdentityConverter).await
}

/// `dispatch` with the converter `NewVersionedAttributes` uses.
pub async fn dispatch_with_converter<P, B, E>(
    matcher: &Matcher<'_>,
    attr: &mut Attributes,
    hooks: &[PolicyHook<P, B, E>],
    delegate: &dyn DispatchDelegate<P, B, E>,
    converter: &dyn VersionConverter,
) -> Result<(), Error>
where
    P: PolicyAccessor + Sync,
    B: BindingAccessor + Sync,
    E: Sync,
{
    let mut relevant: Vec<PolicyInvocation<'_, P, B, E>> = Vec::new();
    // Construct all the versions we need to call our webhooks.
    let mut versioned = VersionedAttributeAccessor::new(converter);
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

            // Here the binding matches. The versioned attributes are built now
            // and cached, to be reused during the delegate's hook calls
            // (policy_dispatcher.go:152-160). A failure is a policy-level
            // config error: the binding is not named.
            if let Err(e) = versioned.versioned_attribute(attr, &matched.kind) {
                policy_errors.push(config_error(&hook.policy, None, e));
                continue;
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
        policy_errors.extend(
            delegate
                .dispatch(&mut *attr, &mut versioned, &relevant)
                .await?,
        );
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
