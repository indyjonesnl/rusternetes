//! MutatingAdmissionPolicy strategy and storage — port of
//! `pkg/registry/admissionregistration/mutatingadmissionpolicy/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.
//!
//! Unlike its validating sibling the policy has no `status` (the type has
//! none, `v1beta1/types.go:1202`), so there is no status strategy or
//! subresource, and `PrepareForCreate` only starts the generation.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::MutatingAdmissionPolicy;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::mutating_admission_policy::{
    set_defaults_mutating_admission_policy, validate_mutating_admission_policy,
};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use super::authz::authorize_mutating_param_kind;
use crate::registry::generic::store::{
    BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
use crate::registry::generic::Store;
use crate::registry::rbac::policybased::Noop;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `SetObjectDefaults_MutatingAdmissionPolicy`
/// (`pkg/apis/admissionregistration/v1beta1/zz_generated.defaults.go`).
pub fn convert_to_internal(policy: &mut MutatingAdmissionPolicy) {
    set_defaults_mutating_admission_policy(policy);
}

fn spec_json(obj: &MutatingAdmissionPolicy) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// `validateMutatingAdmissionPolicy` (validation.go:1366-1370): ObjectMeta with
/// `NameIsDNSSubdomain`, then the spec.
fn validate(obj: &MutatingAdmissionPolicy) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_mutating_admission_policy(obj));
    errs
}

/// `mutatingAdmissionPolicyStrategy` (strategy.go:34-39).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:52-54.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<MutatingAdmissionPolicy> for Strategy {
    /// `PrepareForCreate` (strategy.go:57-60): the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut MutatingAdmissionPolicy) {
        obj.metadata.generation = Some(1);
    }

    /// `Validate` (strategy.go:76-85) minus the `paramKind` authorization,
    /// which is [`ParamKindAuthz`].
    fn validate(&self, _ctx: &RequestContext, obj: &MutatingAdmissionPolicy) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<MutatingAdmissionPolicy> for Strategy {
    /// strategy.go:97-99.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:63-73): a spec change increments the
    /// generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicy,
        old: &MutatingAdmissionPolicy,
    ) {
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateMutatingAdmissionPolicyUpdate` (validation.go:1349-1354). The
    /// `ignoreMatchConditions` / `preexistingExpressions` options only matter
    /// to the CEL compile, which is not modelled (see [`super`]).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &MutatingAdmissionPolicy,
        _old: &MutatingAdmissionPolicy,
    ) -> ErrorList {
        validate(obj)
    }

    /// strategy.go:120-122.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<MutatingAdmissionPolicy> for Strategy {}

/// `Validate` / `ValidateUpdate` (strategy.go:76-85, :102-111): the `paramKind`
/// read-access check, run once the object is well-formed. See [`super::authz`]
/// for why it is a Store hook.
pub struct ParamKindAuthz {
    authorizer: Arc<dyn Authorizer>,
    storage: Arc<StorageBackend>,
}

#[async_trait]
impl BeginCreate<MutatingAdmissionPolicy> for ParamKindAuthz {
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicy,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        if validate(obj).is_empty() {
            let errs = authorize_mutating_param_kind(
                ctx,
                self.authorizer.as_ref(),
                &self.storage,
                obj,
                None,
            )
            .await;
            if !errs.is_empty() {
                return Err(Error::Invalid(errs));
            }
        }
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl BeginUpdate<MutatingAdmissionPolicy> for ParamKindAuthz {
    async fn begin_update(
        &self,
        ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicy,
        old: &mut MutatingAdmissionPolicy,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        if validate(obj).is_empty() {
            let errs = authorize_mutating_param_kind(
                ctx,
                self.authorizer.as_ref(),
                &self.storage,
                obj,
                Some(old),
            )
            .await;
            if !errs.is_empty() {
                return Err(Error::Invalid(errs));
            }
        }
        Ok(Box::new(Noop))
    }
}

/// `NewREST` (storage/storage.go:38-62). `Categories()` (`api-extensions`,
/// :65-68) is served by discovery.
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<MutatingAdmissionPolicy, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("admissionregistration.k8s.io", "mutatingadmissionpolicies"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let hook = Arc::new(ParamKindAuthz {
        authorizer,
        storage,
    });
    store.begin_create = Some(hook.clone());
    store.begin_update = Some(hook);
    store
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// `validMutatingAdmissionPolicy` (strategy_test.go:60-102).
    fn valid() -> MutatingAdmissionPolicy {
        serde_json::from_value(json!({
            "metadata": {"name": "foo"},
            "spec": {
                "paramKind": {"kind": "ReplicaLimit", "apiVersion": "rules.example.com/v1"},
                "reinvocationPolicy": "IfNeeded",
                "mutations": [{
                    "patchType": "ApplyConfiguration",
                    "applyConfiguration": {"expression":
                        "Object{ spec: Object.spec{ replicas: object.spec.replicas % 2 == 0?object.spec.replicas + 1:object.spec.replicas } }"}
                }],
                "matchConstraints": {
                    "matchPolicy": "Exact",
                    "objectSelector": {"matchLabels": {"a": "b"}},
                    "namespaceSelector": {"matchLabels": {"a": "b"}},
                    "resourceRules": [{
                        "operations": ["CREATE"],
                        "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]
                    }]
                },
                "failurePolicy": "Ignore"
            }
        }))
        .unwrap()
    }

    /// `TestMutatingAdmissionPolicyStrategy` (strategy_test.go:29-58).
    #[test]
    fn mutating_admission_policy_strategy() {
        let strategy = Strategy;
        let ctx = RequestContext::new(None);
        assert!(!strategy.namespace_scoped(), "must be cluster scoped");
        assert!(
            !RestUpdateStrategy::<MutatingAdmissionPolicy>::allow_create_on_update(&strategy),
            "should not allow create on update"
        );
        assert!(
            !RestUpdateStrategy::<MutatingAdmissionPolicy>::allow_unconditional_update(&strategy)
        );

        let mut configuration = valid();
        strategy.prepare_for_create(&ctx, &mut configuration);
        assert_eq!(configuration.metadata.generation, Some(1));
        let errs = RestCreateStrategy::validate(&strategy, &ctx, &configuration);
        assert!(errs.is_empty(), "Unexpected error mutating {errs:?}");

        let mut invalid = MutatingAdmissionPolicy::new("");
        invalid.metadata.name = String::new();
        strategy.prepare_for_update(&ctx, &mut invalid, &configuration);
        let errs = strategy.validate_update(&ctx, &invalid, &configuration);
        assert!(!errs.is_empty(), "Expected a validation error");
    }

    /// strategy.go:63-73: only a spec change bumps the generation.
    #[test]
    fn generation_bumps_on_a_spec_change_only() {
        let strategy = Strategy;
        let ctx = RequestContext::new(None);
        let mut old = valid();
        old.metadata.generation = Some(3);

        let mut same = old.clone();
        same.metadata.labels = Some([("a".to_string(), "b".to_string())].into());
        strategy.prepare_for_update(&ctx, &mut same, &old);
        assert_eq!(same.metadata.generation, Some(3));

        let mut changed = old.clone();
        changed.spec.as_mut().unwrap().failure_policy =
            Some(rusternetes_common::resources::validating_admission_policy::FailurePolicy::Fail);
        strategy.prepare_for_update(&ctx, &mut changed, &old);
        assert_eq!(changed.metadata.generation, Some(4));
    }
}
