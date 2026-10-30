//! Unit tests moved from `src/registry/admissionregistration/validatingadmissionpolicybinding.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::admissionregistration::authz::{
        authorize_param_ref, ParamAuthz,
    };
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
    };
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::resources::ValidatingAdmissionPolicyBinding;
    pub use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_common::validation::validating_admission_policy::{
        set_defaults_validating_admission_policy_binding,
        validate_validating_admission_policy_binding,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod invalid_status_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::validatingadmissionpolicybinding::*;
    use rusternetes_api_server::registry::generic::store::{CreateOptions, UpdateOptions};
    use rusternetes_api_server::registry::rest::DefaultUpdatedObjectInfo;
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
