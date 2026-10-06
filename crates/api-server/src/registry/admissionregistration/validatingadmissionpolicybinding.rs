//! ValidatingAdmissionPolicyBinding strategy and storage — port of
//! `pkg/registry/admissionregistration/validatingadmissionpolicybinding/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::{Error, Result};

use rusternetes_common::resources::ValidatingAdmissionPolicyBinding;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::validating_admission_policy::{
    set_defaults_validating_admission_policy_binding, validate_validating_admission_policy_binding,
};
use rusternetes_storage::StorageBackend;

use super::authz::authorize_param_ref;
use crate::registry::generic::store::{
    BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
use crate::registry::generic::Store;
use crate::registry::rbac::policybased::Noop;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `SetObjectDefaults_ValidatingAdmissionPolicyBinding`
/// (`pkg/apis/admissionregistration/v1/zz_generated.defaults.go`).
pub fn convert_to_internal(binding: &mut ValidatingAdmissionPolicyBinding) {
    set_defaults_validating_admission_policy_binding(binding);
}

fn spec_json(obj: &ValidatingAdmissionPolicyBinding) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// `validateValidatingAdmissionPolicyBinding` (validation.go:1175-1179).
fn validate(obj: &ValidatingAdmissionPolicyBinding) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_validating_admission_policy_binding(obj));
    errs
}

/// `validatingAdmissionPolicyBindingStrategy` (strategy.go:36-44).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ValidatingAdmissionPolicyBinding> for Strategy {
    /// `PrepareForCreate` (strategy.go:60-63): the generation starts at 1.
    fn prepare_for_create(
        &self,
        _ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicyBinding,
    ) {
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ValidatingAdmissionPolicyBinding) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<ValidatingAdmissionPolicyBinding> for Strategy {
    /// strategy.go:104-106.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:66-77): a spec change increments the
    /// generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicyBinding,
        old: &ValidatingAdmissionPolicyBinding,
    ) {
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateValidatingAdmissionPolicyBindingUpdate` (validation.go:1252).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ValidatingAdmissionPolicyBinding,
        _old: &ValidatingAdmissionPolicyBinding,
    ) -> ErrorList {
        validate(obj)
    }

    /// strategy.go:124-126.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<ValidatingAdmissionPolicyBinding> for Strategy {}

/// `Validate` / `ValidateUpdate` (strategy.go:80-88, :109-117): the `paramRef`
/// read-access check, run once the object is well-formed. See [`super::authz`]
/// for why it is a Store hook.
pub struct ParamRefAuthz {
    authorizer: Arc<dyn Authorizer>,
    storage: Arc<StorageBackend>,
}

#[async_trait]
impl BeginCreate<ValidatingAdmissionPolicyBinding> for ParamRefAuthz {
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicyBinding,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        if validate(obj).is_empty() {
            let errs = authorize_param_ref(ctx, self.authorizer.as_ref(), &self.storage, obj, None)
                .await?;
            if !errs.is_empty() {
                return Err(Error::Invalid(errs));
            }
        }
        Ok(Box::new(Noop))
    }
}

#[async_trait]
impl BeginUpdate<ValidatingAdmissionPolicyBinding> for ParamRefAuthz {
    async fn begin_update(
        &self,
        ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicyBinding,
        old: &mut ValidatingAdmissionPolicyBinding,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
        if validate(obj).is_empty() {
            let errs =
                authorize_param_ref(ctx, self.authorizer.as_ref(), &self.storage, obj, Some(old))
                    .await?;
            if !errs.is_empty() {
                return Err(Error::Invalid(errs));
            }
        }
        Ok(Box::new(Noop))
    }
}

/// `NewREST` (storage/storage.go:46-69).
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<ValidatingAdmissionPolicyBinding, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new(
            "admissionregistration.k8s.io",
            "validatingadmissionpolicybindings",
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
