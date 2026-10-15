//! ValidatingAdmissionPolicyBinding strategy and storage — port of
//! `pkg/registry/admissionregistration/validatingadmissionpolicybinding/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;

use rusternetes_common::resources::ValidatingAdmissionPolicyBinding;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::validating_admission_policy::{
    set_defaults_validating_admission_policy_binding, validate_validating_admission_policy_binding,
};
use rusternetes_storage::StorageBackend;

use super::authz::{authorize_param_ref, ParamAuthz};
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy, ValidateFuture,
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
#[derive(Default)]
pub struct Strategy {
    /// `authorizer` / `resourceResolver`; `None` skips the authorization.
    pub authz: Option<ParamAuthz>,
}

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

    /// The `paramRef` authorization of `Validate`, run only when the rest of
    /// `Validate` found nothing (`len(errs) == 0`).
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ValidatingAdmissionPolicyBinding,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestCreateStrategy::validate(self, ctx, obj).is_empty() {
                return Vec::new();
            }
            match authorize_param_ref(ctx, authz.authorizer.as_ref(), &authz.storage, obj, None)
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

    /// The `paramRef` authorization of `ValidateUpdate`.
    fn validate_update_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ValidatingAdmissionPolicyBinding,
        old: &'a ValidatingAdmissionPolicyBinding,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestUpdateStrategy::validate_update(self, ctx, obj, old).is_empty() {
                return Vec::new();
            }
            match authorize_param_ref(
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

    /// strategy.go:124-126.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<ValidatingAdmissionPolicyBinding> for Strategy {}

/// `NewREST` (storage/storage.go:46-69).
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<ValidatingAdmissionPolicyBinding, StorageBackend> {
    Store::new(
        storage.clone(),
        GroupResource::new(
            "admissionregistration.k8s.io",
            "validatingadmissionpolicybindings",
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
mod invalid_status_tests {
    use super::*;
    use crate::registry::generic::store::{CreateOptions, UpdateOptions};
    use crate::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;

    /// `ValidatingAdmissionPolicyBinding` strategy `Validate` appends the authz error as a
    /// `field.Forbidden`, and `BeforeCreate` / `BeforeUpdate` wrap the list in
    /// `errors.NewInvalid(kind.GroupKind(), name, errs)`
    /// (rest/create.go:122-131, rest/update.go:149-158; errors.go:284-312), so
    /// the status is `<Kind>.<group> "<name>" is invalid: ...` with details.
    #[tokio::test]
    async fn authz_denial_is_a_new_invalid_status() {
        let obj: ValidatingAdmissionPolicyBinding = serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding", "metadata": {"name": "foo"}, "spec": {
            "policyName": "p.example.com", "validationActions": ["Deny"],
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
            msg.starts_with(r#"ValidatingAdmissionPolicyBinding.admissionregistration.k8s.io "foo" is invalid: spec.paramRef: Forbidden: "#),
            "{msg}"
        );
        let d = st.details.unwrap();
        assert_eq!(d.group.as_deref(), Some("admissionregistration.k8s.io"));
        assert_eq!(d.kind.as_deref(), Some("ValidatingAdmissionPolicyBinding"));
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
