//! Unit tests moved from `src/registry/admissionregistration/validatingadmissionpolicy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::admissionregistration::authz::{
        authorize_param_kind, ParamAuthz,
    };
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
    };
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
    pub use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
    pub use rusternetes_common::resources::validating_admission_policy::ValidatingAdmissionPolicyStatus;
    pub use rusternetes_common::resources::ValidatingAdmissionPolicy;
    pub use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_common::validation::validating_admission_policy::{
        find_validating_policy_preexisting_expressions,
        ignore_validating_admission_policy_match_conditions,
        set_defaults_validating_admission_policy, validate_validating_admission_policy,
        validate_validating_admission_policy_status_update, PreexistingExpressions,
        MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod invalid_status_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::validatingadmissionpolicy::*;
    use rusternetes_api_server::registry::generic::store::{CreateOptions, UpdateOptions};
    use rusternetes_api_server::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;

    /// `ValidatingAdmissionPolicy` strategy `Validate` appends the authz error as a
    /// `field.Forbidden`, and `BeforeCreate` / `BeforeUpdate` wrap the list in
    /// `errors.NewInvalid(kind.GroupKind(), name, errs)`
    /// (rest/create.go:122-131, rest/update.go:149-158; errors.go:284-312), so
    /// the status is `<Kind>.<group> "<name>" is invalid: ...` with details.
    #[tokio::test]
    async fn authz_denial_is_a_new_invalid_status() {
        let obj: ValidatingAdmissionPolicy = serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "paramKind": {"kind": "ReplicaLimit", "apiVersion": "rules.example.com/v1"},
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "validations": [{"expression": "true"}],
            "failurePolicy": "Fail"}})).unwrap();
        assert!(
            validate(&obj, None).is_empty(),
            "{:?}",
            validate(&obj, None)
        );
        let storage = Arc::new(StorageBackend::Memory(Arc::new(
            rusternetes_storage::MemoryStorage::new(),
        )));
        let (store, _status) = new_stores(storage, Arc::new(AlwaysDenyAuthorizer));
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
            msg.starts_with(r#"ValidatingAdmissionPolicy.admissionregistration.k8s.io "foo" is invalid: spec.paramKind: Forbidden: "#),
            "{msg}"
        );
        let d = st.details.unwrap();
        assert_eq!(d.group.as_deref(), Some("admissionregistration.k8s.io"));
        assert_eq!(d.kind.as_deref(), Some("ValidatingAdmissionPolicy"));
        assert_eq!(d.name.as_deref(), Some("foo"));

        let mut plain = obj.clone();
        plain.spec.as_mut().unwrap().param_kind = None;
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

mod param_kind_authz_ordering_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::validatingadmissionpolicy::*;
    use rusternetes_api_server::registry::generic::store::{CreateOptions, UpdateOptions};
    use rusternetes_api_server::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;
    use rusternetes_storage::Storage;

    fn policy(param_kind: bool) -> ValidatingAdmissionPolicy {
        let mut spec = serde_json::json!({
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "validations": [{"expression": "true"}],
            "failurePolicy": "Fail"});
        if param_kind {
            spec["paramKind"] =
                serde_json::json!({"kind": "ReplicaLimit", "apiVersion": "rules.example.com/v1"});
        }
        serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": spec})).unwrap()
    }

    /// Upstream authorizes `paramKind` INSIDE `Validate` / `ValidateUpdate`
    /// (validatingadmissionpolicy/strategy.go:83-92, :109-119), which
    /// `BeforeUpdate` (rest/update.go:149-158) calls after
    /// `validateCommonFields`; both lists are returned in ONE `Invalid`. An
    /// update that adds a finalizer to an object being deleted
    /// (`ValidateObjectMetaUpdate`) and an unauthorized paramKind therefore
    /// reports both causes.
    #[tokio::test]
    async fn an_unauthorized_param_kind_is_reported_with_the_metadata_errors() {
        let storage = Arc::new(StorageBackend::Memory(Arc::new(
            rusternetes_storage::MemoryStorage::new(),
        )));
        let (store, _status) = new_stores(storage.clone(), Arc::new(AlwaysDenyAuthorizer));
        let user = UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        };
        let ctx = RequestContext::new(None).with_user(&user);
        let mut plain = policy(false);
        plain.metadata.finalizers = Some(vec!["a".to_string()]);
        store
            .create(&ctx, plain, None, &CreateOptions::default())
            .await
            .unwrap();
        // The object is now being deleted.
        let key = rusternetes_storage::build_key("validatingadmissionpolicies", None, "foo");
        let mut stored: ValidatingAdmissionPolicy = storage.get(&key).await.unwrap();
        stored.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let stored = storage.update(&key, &stored).await.unwrap();

        let mut next = policy(true);
        next.metadata.finalizers = Some(vec!["a".to_string(), "b".to_string()]);
        next.metadata.resource_version = stored.metadata.resource_version.clone();
        let info = DefaultUpdatedObjectInfo::new(Some(next), Vec::new());
        let err = store
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
            .unwrap_err();
        let Error::Status(st) = err else {
            panic!("expected Status, got {err:?}")
        };
        let fields: Vec<_> = st
            .details
            .unwrap()
            .causes
            .unwrap()
            .into_iter()
            .filter_map(|c| c.field)
            .collect();
        assert!(fields.contains(&"spec.paramKind".to_string()), "{fields:?}");
        assert!(
            fields.contains(&"metadata.finalizers".to_string()),
            "{fields:?}"
        );
    }
}

mod match_condition_compile_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::validatingadmissionpolicy::*;
    use serde_json::json;

    fn policy(match_conditions: serde_json::Value) -> ValidatingAdmissionPolicy {
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "validations": [{"expression": "true"}],
            "matchConditions": match_conditions,
            "failurePolicy": "Fail"}})).unwrap()
    }

    fn messages(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// `validateMatchConditionsExpression` (validation.go:1100-1112) compiles
    /// with `MatchCondition.ReturnTypes = [bool]` (matcher.go:49-51), and
    /// `CompileCELExpression` rejects another output type (compile.go:189-203)
    /// (#3265).
    #[test]
    fn create_rejects_a_non_bool_match_condition() {
        let p = policy(json!([{"name": "c", "expression": "6"}]));
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(
            messages(&errs).contains(
                r#"spec.matchConditions[0].expression: Invalid value: "6": must evaluate to bool but got int"#
            ),
            "{}",
            messages(&errs)
        );
    }

    /// `validateMatchCondition` (validation.go:984-997).
    #[test]
    fn create_rejects_a_match_condition_that_does_not_compile() {
        let p = policy(json!([{"name": "c", "expression": "///"}]));
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(
            messages(&errs).contains(
                r#"spec.matchConditions[0].expression: Invalid value: "///": compilation failed"#
            ),
            "{}",
            messages(&errs)
        );
    }

    #[test]
    fn create_accepts_a_bool_match_condition() {
        let p = policy(json!([{"name": "c", "expression": "object.metadata.name == 'x'"}]));
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(errs.is_empty(), "{}", messages(&errs));
    }

    /// `ignoreValidatingAdmissionPolicyMatchConditions` (validation.go:619-628):
    /// unchanged `matchConditions` / `paramKind` are not compiled again; a
    /// change is.
    #[test]
    fn update_skips_unchanged_match_conditions_only() {
        let old = policy(json!([{"name": "c", "expression": "6"}]));
        let unchanged = old.clone();
        let ctx = RequestContext::new(None);
        let errs =
            RestUpdateStrategy::validate_update(&Strategy::default(), &ctx, &unchanged, &old);
        assert!(errs.is_empty(), "{}", messages(&errs));
        let changed = policy(json!([{"name": "c", "expression": "7"}]));
        let errs = RestUpdateStrategy::validate_update(&Strategy::default(), &ctx, &changed, &old);
        assert!(
            messages(&errs).contains("spec.matchConditions[0].expression"),
            "{}",
            messages(&errs)
        );
    }
}

mod expression_compile_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::validatingadmissionpolicy::*;
    use serde_json::json;

    fn policy(spec_extra: serde_json::Value) -> ValidatingAdmissionPolicy {
        let mut spec = json!({
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "failurePolicy": "Fail"});
        for (k, v) in spec_extra.as_object().unwrap() {
            spec[k] = v.clone();
        }
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": spec})).unwrap()
    }

    fn msgs(p: &ValidatingAdmissionPolicy) -> String {
        RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), p)
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// validation_test.go:2618 "validation expression that does not compile".
    #[test]
    fn rejects_a_validation_expression_that_does_not_compile() {
        let m = msgs(&policy(
            json!({"validations": [{"expression": "object.x in [1, 2, "}]}),
        ));
        assert!(
            m.contains(r#"spec.validations[0].expression: Invalid value: "object.x in [1, 2, ": compilation failed"#),
            "{m}"
        );
    }

    /// `ValidationCondition.ReturnTypes` is `[bool]` (interface.go:46-48).
    #[test]
    fn rejects_a_non_bool_validation_expression() {
        let m = msgs(&policy(json!({"validations": [{"expression": "6"}]})));
        assert!(
            m.contains(r#"spec.validations[0].expression: Invalid value: "6": must evaluate to bool but got int"#),
            "{m}"
        );
    }

    /// validation_test.go:2954: `variables` has no field `replicas`.
    #[test]
    fn rejects_an_undefined_variable_in_a_validation() {
        let m = msgs(&policy(json!({
            "variables": [{"name": "foo", "expression": "1 + 1"}],
            "validations": [{"expression": "true"}, {"expression": "variables.replicas == 2"}]})));
        assert!(
            m.contains(r#"spec.validations[1].expression: Invalid value: "variables.replicas == 2": compilation failed"#) && m.contains("undefined field 'replicas'"),
            "{m}"
        );
    }

    /// validation_test.go:2983: a variable only sees the ones before it.
    #[test]
    fn variables_only_see_earlier_variables() {
        let m = msgs(&policy(json!({
            "variables": [
                {"name": "correct", "expression": "object"},
                {"name": "bar", "expression": "variables.foo + 1"},
                {"name": "foo", "expression": "1 + 1"}],
            "validations": [{"expression": "variables.foo > 1"}]})));
        assert!(
            m.contains(r#"spec.variables[1].expression: Invalid value: "variables.foo + 1": compilation failed"#) && m.contains("undefined field 'foo'"),
            "{m}"
        );
        assert!(!m.contains("spec.validations"), "{m}");
    }

    /// validation_test.go:4415.
    #[test]
    fn rejects_a_variable_expression_that_does_not_compile() {
        let m = msgs(&policy(json!({
            "variables": [{"name": "a", "expression": "///"}],
            "validations": [{"expression": "true"}]})));
        assert!(
            m.contains(r#"spec.variables[0].expression: Invalid value: "///": compilation failed"#),
            "{m}"
        );
    }

    /// validation_test.go:2731-2757.
    #[test]
    fn audit_annotation_value_expression_is_compiled() {
        let m = msgs(&policy(json!({
            "validations": [{"expression": "true"}],
            "auditAnnotations": [{"key": "a", "valueExpression": "object.x in [1, 2, "}]})));
        assert!(
            m.contains(r#"spec.auditAnnotations[0].valueExpression: Invalid value: "object.x in [1, 2, ": compilation failed"#),
            "{m}"
        );
        let m = msgs(&policy(json!({
            "validations": [{"expression": "true"}],
            "auditAnnotations": [{"key": "a", "valueExpression": "true"}]})));
        assert!(
            m.contains(r#"spec.auditAnnotations[0].valueExpression: Invalid value: "true": must evaluate to one of [string null_type]"#),
            "{m}"
        );
    }

    #[test]
    fn accepts_valid_expressions() {
        let m = msgs(&policy(json!({
            "variables": [{"name": "foo", "expression": "object.metadata.name"}],
            "validations": [{"expression": "variables.foo == 'x' && authorizer.group('a').check('get').allowed()"}],
            "auditAnnotations": [{"key": "a", "valueExpression": "variables.foo"}]})));
        assert!(m.is_empty(), "{m}");
    }
}

mod preexisting_expression_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::validatingadmissionpolicy::*;
    use serde_json::json;

    const SEMVER: &str = "semver('1.2.3').major() > 0";

    fn env_1_32() -> EnvSet {
        EnvSet::must_base_env_set(rusternetes_common::feature_gates::Version::major_minor(
            1, 32,
        ))
    }

    fn policy(
        match_condition: &str,
        variable: &str,
        validation: &str,
        message: &str,
        audit: &str,
    ) -> ValidatingAdmissionPolicy {
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "matchConditions": [{"name": "c", "expression": match_condition}],
            "variables": [{"name": "v", "expression": variable}],
            "validations": [{"expression": validation, "messageExpression": message}],
            "auditAnnotations": [{"key": "k", "valueExpression": audit}],
            "failurePolicy": "Fail"}})).unwrap()
    }

    fn all_errors(
        new: &ValidatingAdmissionPolicy,
        old: Option<&ValidatingAdmissionPolicy>,
    ) -> String {
        let preexisting = old
            .map(find_validating_policy_preexisting_expressions)
            .unwrap_or_default();
        let env = env_1_32();
        let mut errs = validate_in(new, &preexisting, &env);
        errs.extend(compile_errors_in(new, false, &preexisting, &env));
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Each of the five expression sites compiles in `NewExpressions` when
    /// the old policy does not store it (validateMatchConditionsExpression
    /// :1100, validateVariable :1012, validateValidationExpression :1088,
    /// validateMessageExpression :1114, validateAuditAnnotation :1143).
    #[test]
    fn new_expressions_are_version_gated_at_every_site() {
        let s = format!("{SEMVER} ? 'a' : 'b'");
        let p = policy(SEMVER, SEMVER, SEMVER, &s, &s);
        let got = all_errors(&p, None);
        for path in [
            "spec.matchConditions[0].expression",
            "spec.variables[0].expression",
            "spec.validations[0].expression",
            "spec.validations[0].messageExpression",
            "spec.auditAnnotations[0].valueExpression",
        ] {
            assert!(
                got.contains(path) && got.contains("undeclared reference to 'semver'"),
                "{path}: {got}"
            );
        }
    }

    /// An old policy that stores the expressions compiles them in
    /// `StoredExpressions`, where every library is enabled.
    #[test]
    fn stored_expressions_are_not_version_gated() {
        let s = format!("{SEMVER} ? 'a' : 'b'");
        let p = policy(SEMVER, SEMVER, SEMVER, &s, &s);
        assert_eq!(all_errors(&p, Some(&p)), "");
        // A different expression is new again.
        let other = policy("semver('1.2.3').minor() > 0", SEMVER, SEMVER, &s, &s);
        assert!(all_errors(&other, Some(&p)).contains("spec.matchConditions[0].expression"));
    }

    /// Upstream replicated: `validateVariable` consults
    /// `validationExpressions` (validation.go:1012), so a variable is stored
    /// only when it equals a stored `validations[].expression`.
    #[test]
    fn variable_consults_the_validation_expression_set() {
        let ok = "true";
        let old = policy(ok, ok, SEMVER, "'m'", "'a'");
        let new = policy(ok, SEMVER, ok, "'m'", "'a'");
        assert_eq!(all_errors(&new, Some(&old)), "");
        assert!(all_errors(&new, None).contains("spec.variables[0].expression"));
    }

    /// `findValidatingPolicyPreexistingExpressions` (:308-322): a blank
    /// `messageExpression` is not recorded.
    #[test]
    fn blank_message_expression_is_not_preexisting() {
        let p = policy("true", "true", "true", "", "true");
        let pre = find_validating_policy_preexisting_expressions(&p);
        assert!(pre.validation_message_expressions.is_empty());
        assert!(pre.validation_expressions.contains("true"));
    }
}
