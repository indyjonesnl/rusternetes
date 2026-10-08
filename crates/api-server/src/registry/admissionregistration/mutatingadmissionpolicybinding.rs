//! MutatingAdmissionPolicyBinding strategy and storage — port of
//! `pkg/registry/admissionregistration/mutatingadmissionpolicybinding/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::MutatingAdmissionPolicyBinding;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::mutating_admission_policy::{
    set_defaults_mutating_admission_policy_binding, validate_mutating_admission_policy_binding,
};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::{Error, Result};
use rusternetes_storage::StorageBackend;

use super::authz::authorize_mutating_param_ref;
use crate::registry::generic::store::{
    BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
use crate::registry::generic::Store;
use crate::registry::rbac::policybased::Noop;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `SetObjectDefaults_MutatingAdmissionPolicyBinding`
/// (`pkg/apis/admissionregistration/v1beta1/zz_generated.defaults.go`).
pub fn convert_to_internal(binding: &mut MutatingAdmissionPolicyBinding) {
    set_defaults_mutating_admission_policy_binding(binding);
}

fn spec_json(obj: &MutatingAdmissionPolicyBinding) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// `validateMutatingAdmissionPolicyBinding` (validation.go:1506-1511).
fn validate(obj: &MutatingAdmissionPolicyBinding) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_mutating_admission_policy_binding(obj));
    errs
}

/// `mutatingAdmissionPolicyBindingStrategy` (strategy.go:34-40).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:59-62.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<MutatingAdmissionPolicyBinding> for Strategy {
    /// `PrepareForCreate` (strategy.go:64-68): the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut MutatingAdmissionPolicyBinding) {
        obj.metadata.generation = Some(1);
    }

    /// `Validate` (strategy.go:83-93) minus the `paramRef` authorization,
    /// which is [`ParamRefAuthz`].
    fn validate(&self, _ctx: &RequestContext, obj: &MutatingAdmissionPolicyBinding) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<MutatingAdmissionPolicyBinding> for Strategy {
    /// strategy.go:104-107.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:70-81): a spec change increments the
    /// generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicyBinding,
        old: &MutatingAdmissionPolicyBinding,
    ) {
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateMutatingAdmissionPolicyBindingUpdate` (validation.go:1357-1359).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &MutatingAdmissionPolicyBinding,
        _old: &MutatingAdmissionPolicyBinding,
    ) -> ErrorList {
        validate(obj)
    }

    /// strategy.go:126-129.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<MutatingAdmissionPolicyBinding> for Strategy {}

/// `Validate` / `ValidateUpdate` (strategy.go:83-93, :110-119): the `paramRef`
/// read-access check, run once the object is well-formed. See [`super::authz`]
/// for why it is a Store hook.
pub struct ParamRefAuthz {
    authorizer: Arc<dyn Authorizer>,
    storage: Arc<StorageBackend>,
}

#[async_trait]
impl BeginCreate<MutatingAdmissionPolicyBinding> for ParamRefAuthz {
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicyBinding,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        if validate(obj).is_empty() {
            let errs = authorize_mutating_param_ref(
                ctx,
                self.authorizer.as_ref(),
                &self.storage,
                obj,
                None,
            )
            .await?;
            if !errs.is_empty() {
                return Err(Error::Invalid(errs));
            }
        }
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl BeginUpdate<MutatingAdmissionPolicyBinding> for ParamRefAuthz {
    async fn begin_update(
        &self,
        ctx: &RequestContext,
        obj: &mut MutatingAdmissionPolicyBinding,
        old: &mut MutatingAdmissionPolicyBinding,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        if validate(obj).is_empty() {
            let errs = authorize_mutating_param_ref(
                ctx,
                self.authorizer.as_ref(),
                &self.storage,
                obj,
                Some(old),
            )
            .await?;
            if !errs.is_empty() {
                return Err(Error::Invalid(errs));
            }
        }
        Ok(Box::new(Noop))
    }
}

/// `NewREST` (storage/storage.go:36-60). The `DefaultPolicyGetter`
/// (:82-96) is the policy read in [`super::authz`].
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<MutatingAdmissionPolicyBinding, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new(
            "admissionregistration.k8s.io",
            "mutatingadmissionpolicybindings",
        ),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let hook = Arc::new(ParamRefAuthz {
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

    /// `validPolicyBindings` (strategy_test.go:60-).
    fn valid_bindings() -> Vec<MutatingAdmissionPolicyBinding> {
        let v = json!([
            {"metadata": {"name": "foo"}, "spec": {
                "policyName": "replicalimit-policy.example.com",
                "paramRef": {"name": "replica-limit-test.example.com",
                             "parameterNotFoundAction": "Deny"}}},
        ]);
        serde_json::from_value(v).unwrap()
    }

    /// `TestPolicyBindingStrategy` (strategy_test.go:30-58).
    #[test]
    fn policy_binding_strategy() {
        let strategy = Strategy;
        let ctx = RequestContext::new(None);
        assert!(!strategy.namespace_scoped(), "must be cluster scoped");
        assert!(
            !RestUpdateStrategy::<MutatingAdmissionPolicyBinding>::allow_create_on_update(
                &strategy
            ),
            "should not allow create on update"
        );
        for mut configuration in valid_bindings() {
            strategy.prepare_for_create(&ctx, &mut configuration);
            assert_eq!(configuration.metadata.generation, Some(1));
            let errs = RestCreateStrategy::validate(&strategy, &ctx, &configuration);
            assert!(errs.is_empty(), "Unexpected error validating {errs:?}");

            let mut invalid = MutatingAdmissionPolicyBinding::new("");
            invalid.metadata.name = String::new();
            strategy.prepare_for_update(&ctx, &mut invalid, &configuration);
            let errs = strategy.validate_update(&ctx, &invalid, &configuration);
            assert!(!errs.is_empty(), "Expected a validation error");
        }
    }

    /// strategy.go:70-81: only a spec change bumps the generation.
    #[test]
    fn generation_bumps_on_a_spec_change_only() {
        let strategy = Strategy;
        let ctx = RequestContext::new(None);
        let mut old = valid_bindings().remove(0);
        old.metadata.generation = Some(2);

        let mut same = old.clone();
        strategy.prepare_for_update(&ctx, &mut same, &old);
        assert_eq!(same.metadata.generation, Some(2));

        let mut changed = old.clone();
        changed.spec.as_mut().unwrap().policy_name = Some("other".to_string());
        strategy.prepare_for_update(&ctx, &mut changed, &old);
        assert_eq!(changed.metadata.generation, Some(3));
    }
}
