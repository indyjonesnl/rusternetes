//! MutatingAdmissionPolicyBinding strategy and storage — port of
//! `pkg/registry/admissionregistration/mutatingadmissionpolicybinding/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::MutatingAdmissionPolicyBinding;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::mutating_admission_policy::{
    set_defaults_mutating_admission_policy_binding, validate_mutating_admission_policy_binding,
};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_storage::StorageBackend;

use super::authz::{authorize_mutating_param_ref, ParamAuthz};
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy, ValidateFuture,
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
#[derive(Default)]
pub struct Strategy {
    /// `authorizer` / `resourceResolver`; `None` skips the authorization.
    pub authz: Option<ParamAuthz>,
}

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

    /// `Validate` (strategy.go:84-92) minus the `paramRef` authorization,
    /// which is `validate_async`.
    fn validate(&self, _ctx: &RequestContext, obj: &MutatingAdmissionPolicyBinding) -> ErrorList {
        validate(obj)
    }

    /// The `paramRef` authorization of `Validate`, run only when the rest of
    /// `Validate` found nothing (`len(errs) == 0`).
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a MutatingAdmissionPolicyBinding,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestCreateStrategy::validate(self, ctx, obj).is_empty() {
                return Vec::new();
            }
            match authorize_mutating_param_ref(
                ctx,
                authz.authorizer.as_ref(),
                &authz.storage,
                obj,
                None,
            )
            .await
            {
                Ok(errs) => errs,
                Err(e) => vec![FieldError::forbidden(
                    &Path::new("spec").child("paramRef"),
                    e.to_string(),
                )],
            }
        })
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

    /// The `paramRef` authorization of `ValidateUpdate`.
    fn validate_update_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a MutatingAdmissionPolicyBinding,
        old: &'a MutatingAdmissionPolicyBinding,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestUpdateStrategy::validate_update(self, ctx, obj, old).is_empty() {
                return Vec::new();
            }
            match authorize_mutating_param_ref(
                ctx,
                authz.authorizer.as_ref(),
                &authz.storage,
                obj,
                Some(old),
            )
            .await
            {
                Ok(errs) => errs,
                Err(e) => vec![FieldError::forbidden(
                    &Path::new("spec").child("paramRef"),
                    e.to_string(),
                )],
            }
        })
    }

    /// strategy.go:126-129.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<MutatingAdmissionPolicyBinding> for Strategy {}

/// `NewREST` (storage/storage.go:36-60). The `DefaultPolicyGetter`
/// (:82-96) is the policy read in [`super::authz`].
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<MutatingAdmissionPolicyBinding, StorageBackend> {
    Store::new(
        storage.clone(),
        GroupResource::new(
            "admissionregistration.k8s.io",
            "mutatingadmissionpolicybindings",
        ),
        Arc::new(Strategy {
            authz: Some(ParamAuthz {
                authorizer,
                storage,
            }),
        }),
    )
    .with_decode_defaulter(convert_to_internal)
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
        let strategy = Strategy::default();
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
        let strategy = Strategy::default();
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

#[cfg(test)]
mod invalid_status_tests {
    use super::*;
    use crate::registry::generic::store::{CreateOptions, UpdateOptions};
    use crate::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;

    /// `MutatingAdmissionPolicyBinding` strategy `Validate` appends the authz error as a
    /// `field.Forbidden`, and `BeforeCreate` / `BeforeUpdate` wrap the list in
    /// `errors.NewInvalid(kind.GroupKind(), name, errs)`
    /// (rest/create.go:122-131, rest/update.go:149-158; errors.go:284-312), so
    /// the status is `<Kind>.<group> "<name>" is invalid: ...` with details.
    #[tokio::test]
    async fn authz_denial_is_a_new_invalid_status() {
        let obj: MutatingAdmissionPolicyBinding = serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "MutatingAdmissionPolicyBinding", "metadata": {"name": "foo"}, "spec": {
            "policyName": "p.example.com",
            "paramRef": {"name": "x", "parameterNotFoundAction": "Deny"}}})).unwrap();
        let storage = Arc::new(StorageBackend::Memory(Arc::new(
            rusternetes_storage::MemoryStorage::new(),
        )));
        let store = new_store(storage, Arc::new(AlwaysDenyAuthorizer));
        let user = UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        };
        let ctx = RequestContext::new(None).with_user(&user);
        let Err(Error::Status(st)) = store
            .create(&ctx, obj.clone(), None, &CreateOptions::default())
            .await
        else {
            panic!("expected a Status error from create")
        };
        assert_eq!(st.code, Some(422));
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
        let msg = st.message.clone().unwrap_or_default();
        assert!(
            msg.starts_with(r#"MutatingAdmissionPolicyBinding.admissionregistration.k8s.io "foo" is invalid: spec.paramRef: Forbidden: "#),
            "{msg}"
        );
        let d = st.details.unwrap();
        assert_eq!(d.group.as_deref(), Some("admissionregistration.k8s.io"));
        assert_eq!(d.kind.as_deref(), Some("MutatingAdmissionPolicyBinding"));
        assert_eq!(d.name.as_deref(), Some("foo"));

        let mut plain = obj.clone();
        plain.spec.as_mut().unwrap().param_ref = None;
        let created = store
            .create(&ctx, plain, None, &CreateOptions::default())
            .await
            .unwrap();
        let mut update = obj.clone();
        update.metadata.resource_version = created.metadata.resource_version.clone();
        let info = DefaultUpdatedObjectInfo::new(Some(update), Vec::new());
        let Err(Error::Status(st)) = store
            .update(
                &ctx,
                "foo",
                &info,
                None,
                None,
                false,
                &UpdateOptions::default(),
            )
            .await
        else {
            panic!("expected a Status error from update")
        };
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
    }
}
