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
