//! Unit tests moved from `src/registry/admissionregistration/mutatingadmissionpolicybinding.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::admissionregistration::authz::{
        authorize_mutating_param_ref, ParamAuthz,
    };
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
    };
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::resources::MutatingAdmissionPolicyBinding;
    pub use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
    pub use rusternetes_common::validation::mutating_admission_policy::{
        set_defaults_mutating_admission_policy_binding, validate_mutating_admission_policy_binding,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::mutatingadmissionpolicybinding::*;
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

mod invalid_status_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::mutatingadmissionpolicybinding::*;
    use rusternetes_api_server::registry::generic::store::{CreateOptions, UpdateOptions};
    use rusternetes_api_server::registry::rest::DefaultUpdatedObjectInfo;
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
