//! Unit tests moved from `src/registry/admissionregistration/mutatingadmissionpolicy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::admissionregistration::authz::{
        authorize_mutating_param_kind, ParamAuthz,
    };
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
    };
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
    pub use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
    pub use rusternetes_common::resources::mutating_admission_policy::PatchType;
    pub use rusternetes_common::resources::MutatingAdmissionPolicy;
    pub use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
    pub use rusternetes_common::validation::mutating_admission_policy::{
        find_mutating_policy_preexisting_expressions,
        ignore_mutating_admission_policy_match_conditions, set_defaults_mutating_admission_policy,
        validate_mutating_admission_policy, PreexistingExpressions,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::BTreeSet;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::mutatingadmissionpolicy::*;
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
        let strategy = Strategy::default();
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
        let strategy = Strategy::default();
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

    fn messages(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn strategy_update(
        ctx: &RequestContext,
        new: &MutatingAdmissionPolicy,
        old: &MutatingAdmissionPolicy,
    ) -> ErrorList {
        Strategy::default().validate_update(ctx, new, old)
    }

    const SEMVER: &str = "semver('1.2.3').major() > 0";

    fn env_1_32() -> EnvSet {
        EnvSet::must_base_env_set(rusternetes_common::feature_gates::Version::major_minor(
            1, 32,
        ))
    }

    fn with_expressions(
        match_condition: Option<&str>,
        variable: Option<&str>,
        apply: Option<&str>,
        json_patch: Option<&str>,
    ) -> MutatingAdmissionPolicy {
        let mut p = valid();
        let spec = p.spec.as_mut().unwrap();
        if let Some(e) = match_condition {
            spec.match_conditions =
                Some(serde_json::from_value(json!([{"name": "mc", "expression": e}])).unwrap());
        }
        if let Some(e) = variable {
            spec.variables =
                Some(serde_json::from_value(json!([{"name": "x", "expression": e}])).unwrap());
        }
        if let Some(e) = apply {
            spec.mutations = Some(
                serde_json::from_value(json!([{"patchType": "ApplyConfiguration",
                    "applyConfiguration": {"expression": e}}]))
                .unwrap(),
            );
        }
        if let Some(e) = json_patch {
            spec.mutations = Some(
                serde_json::from_value(
                    json!([{"patchType": "JSONPatch", "jsonPatch": {"expression": e}}]),
                )
                .unwrap(),
            );
        }
        p
    }

    fn errs_in(new: &MutatingAdmissionPolicy, old: Option<&MutatingAdmissionPolicy>) -> String {
        let preexisting = old
            .map(find_mutating_policy_preexisting_expressions)
            .unwrap_or_default();
        messages(&compile_errors_in(new, false, &preexisting, &env_1_32()))
    }

    /// `validateMatchConditionsExpression` (validation.go:1100-1112): a new
    /// expression compiles in `NewExpressions`, where a library introduced
    /// above the min-compatibility version is undeclared; one the old policy
    /// stores compiles in `StoredExpressions`.
    #[test]
    fn match_condition_new_expression_is_version_gated() {
        let new = with_expressions(Some(SEMVER), None, None, None);
        let got = errs_in(&new, None);
        assert!(
            got.contains("spec.matchConditions[0].expression")
                && got.contains("undeclared reference to 'semver'"),
            "{got}"
        );
        assert_eq!(errs_in(&new, Some(&new)), "");
        let other = with_expressions(Some("semver('1.2.3').minor() > 0"), None, None, None);
        assert!(errs_in(&other, Some(&new)).contains("undeclared reference to 'semver'"));
    }

    /// `validateVariable` consults `validationExpressions` (validation.go:1012),
    /// which `findMutatingPolicyPreexistingExpressions` never fills, so a
    /// variable is always a new expression.
    #[test]
    fn variable_is_always_a_new_expression() {
        let p = with_expressions(None, Some(SEMVER), None, None);
        for old in [None, Some(&p)] {
            let got = errs_in(&p, old);
            assert!(
                got.contains("spec.variables[0].expression")
                    && got.contains("undeclared reference to 'semver'"),
                "{got}"
            );
        }
    }

    /// `validateApplyConfiguration` (validation.go:1461) uses
    /// `applyConfigurationExpressions`.
    #[test]
    fn apply_configuration_preexisting_is_stored() {
        let e = "Object{spec: Object.spec{replicas: semver('1.2.3').major()}}";
        let p = with_expressions(None, None, Some(e), None);
        assert!(errs_in(&p, None).contains("undeclared reference to 'semver'"));
        assert_eq!(errs_in(&p, Some(&p)), "");
    }

    /// Upstream replicated: `validateJSONPatch` checks
    /// `applyConfigurationExpressions` (validation.go:1485), not
    /// `jsonPatchExpressions`, so a stored jsonPatch is still compiled as new.
    #[test]
    fn json_patch_consults_the_apply_configuration_set() {
        let e = "[JSONPatch{op: 'add', path: '/x', value: semver('1.2.3').major()}]";
        let p = with_expressions(None, None, None, Some(e));
        assert!(errs_in(&p, Some(&p)).contains("undeclared reference to 'semver'"));
        // Equal to a stored applyConfiguration expression: the stored env.
        let apply = with_expressions(None, None, Some(e), None);
        assert_eq!(errs_in(&p, Some(&apply)), "");
    }

    /// `TestValidateMutatingAdmissionPolicy` "variable compile error"
    /// (validation_test.go:4395-4415): a `variables` expression that does not
    /// compile.
    #[test]
    fn create_rejects_a_variable_that_does_not_compile() {
        let mut p = valid();
        p.spec.as_mut().unwrap().variables =
            Some(serde_json::from_value(json!([{"name": "x", "expression": "///"}])).unwrap());
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(
            messages(&errs).contains(
                r#"spec.variables[0].expression: Invalid value: "///": compilation failed"#
            ),
            "{}",
            messages(&errs)
        );
    }

    /// `MatchCondition.ReturnTypes` is `[bool]` (matcher.go:49-51), checked by
    /// `CompileCELExpression` (compile.go:189-203) (#3265).
    #[test]
    fn create_rejects_a_non_bool_match_condition() {
        let mut p = valid();
        p.spec.as_mut().unwrap().match_conditions =
            Some(serde_json::from_value(json!([{"name": "c", "expression": "6"}])).unwrap());
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

    /// `validateMatchCondition` (validation.go:984-997) under the mutating
    /// policy's `spec.matchConditions`.
    #[test]
    fn create_rejects_a_match_condition_that_does_not_compile() {
        let mut p = valid();
        p.spec.as_mut().unwrap().match_conditions =
            Some(serde_json::from_value(json!([{"name": "c", "expression": "///"}])).unwrap());
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

    /// `validateApplyConfiguration` / `validateJSONPatch`
    /// (validation.go:1458-1498).
    #[test]
    fn create_rejects_a_patch_expression_that_does_not_compile() {
        let mut p = valid();
        p.spec.as_mut().unwrap().mutations = Some(
            serde_json::from_value(json!([
                {"patchType": "ApplyConfiguration", "applyConfiguration": {"expression": "///"}},
                {"patchType": "JSONPatch", "jsonPatch": {"expression": "///"}}
            ]))
            .unwrap(),
        );
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        let m = messages(&errs);
        assert!(
            m.contains(r#"spec.mutations[0].applyConfiguration.expression: Invalid value: "///": compilation failed"#),
            "{m}"
        );
        assert!(
            m.contains(r#"spec.mutations[1].jsonPatch.expression: Invalid value: "///": compilation failed"#),
            "{m}"
        );
    }

    /// The `Object{...}` / `JSONPatch{...}` initializers are valid.
    #[test]
    fn create_accepts_object_and_jsonpatch_initializers() {
        let mut p = valid();
        p.spec.as_mut().unwrap().mutations = Some(
            serde_json::from_value(json!([
                {"patchType": "ApplyConfiguration", "applyConfiguration":
                    {"expression": "Object{ spec: Object.spec{ replicas: 1 } }"}},
                {"patchType": "JSONPatch", "jsonPatch": {"expression":
                    "[JSONPatch{op: \"add\", path: \"/spec/replicas\", value: 1}]"}}
            ]))
            .unwrap(),
        );
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(errs.is_empty(), "{}", messages(&errs));
    }

    fn create_errors(p: &MutatingAdmissionPolicy) -> String {
        messages(&RestCreateStrategy::validate(
            &Strategy::default(),
            &RequestContext::new(None),
            p,
        ))
    }

    fn with_mutations_on(
        mut p: MutatingAdmissionPolicy,
        mutations: serde_json::Value,
    ) -> MutatingAdmissionPolicy {
        p.spec.as_mut().unwrap().mutations = Some(serde_json::from_value(mutations).unwrap());
        p
    }

    fn with_mutations(mutations: serde_json::Value) -> MutatingAdmissionPolicy {
        with_mutations_on(valid(), mutations)
    }

    /// `TestValidateMutatingAdmissionPolicy` "applyConfiguration must
    /// evaluate to Object" (validation_test.go:4270-4286); the message is
    /// `compile.go:199`.
    #[test]
    fn create_rejects_an_apply_configuration_that_is_not_an_object() {
        let p = with_mutations(json!([
            {"patchType": "ApplyConfiguration", "applyConfiguration": {"expression": "1 < 2"}}
        ]));
        let m = create_errors(&p);
        assert!(
            m.contains(r#"spec.mutations[0].applyConfiguration.expression: Invalid value: "1 < 2": must evaluate to Object"#),
            "{m}"
        );
    }

    /// "Reference to missing variable" (validation_test.go:4415-4442).
    #[test]
    fn create_rejects_a_reference_to_a_missing_variable() {
        let mut p = with_mutations(
            json!([{"patchType": "JSONPatch", "jsonPatch": {"expression":
            "[JSONPatch{op: \"add\", path: \"/spec/repliacs\", value: variables.x + variables.y}]"}}]),
        );
        p.spec.as_mut().unwrap().variables =
            Some(serde_json::from_value(json!([{"name": "x", "expression": "10 + 10"}])).unwrap());
        let m = create_errors(&p);
        assert!(m.contains("undefined field 'y'"), "{m}");
        // the declared one is fine
        p.spec.as_mut().unwrap().mutations = Some(
            serde_json::from_value(
                json!([{"patchType": "JSONPatch", "jsonPatch": {"expression":
            "[JSONPatch{op: \"add\", path: \"/spec/replicas\", value: variables.x}]"}}]),
            )
            .unwrap(),
        );
        assert_eq!(create_errors(&p), "");
    }

    /// matchConditions with `params` and no `paramKind`
    /// (validation_test.go:5170-5190), and the same for a mutation.
    #[test]
    fn create_rejects_params_without_a_param_kind() {
        let mut p = valid();
        let spec = p.spec.as_mut().unwrap();
        spec.param_kind = None;
        spec.match_conditions = Some(
            serde_json::from_value(
                json!([{"name": "hasParams", "expression": "params.foo == \"okay\""}]),
            )
            .unwrap(),
        );
        let m = create_errors(&p);
        assert!(
            m.contains("spec.matchConditions[0].expression")
                && m.contains("undeclared reference to 'params'"),
            "{m}"
        );
        let mut p = valid();
        p.spec.as_mut().unwrap().param_kind = None;
        let m = create_errors(&with_mutations_on(
            p,
            json!([{"patchType": "ApplyConfiguration", "applyConfiguration":
                {"expression": "Object{ spec: Object.spec{ replicas: params.n } }"}}]),
        ));
        assert!(m.contains("undeclared reference to 'params'"), "{m}");
    }

    /// The checker declares only the environment's variables
    /// (`createEnvForOpts`, compile.go:251-291); comprehension variables are
    /// bound by their macro.
    #[test]
    fn create_checks_declarations_and_scopes_comprehension_variables() {
        let p = with_mutations(
            json!([{"patchType": "JSONPatch", "jsonPatch": {"expression":
            "[JSONPatch{op: \"add\", path: \"/a\", value: nope}]"}}]),
        );
        assert!(create_errors(&p).contains("undeclared reference to 'nope'"));
        let p = with_mutations(
            json!([{"patchType": "JSONPatch", "jsonPatch": {"expression":
            "object.items.map(i, JSONPatch{op: \"add\", path: \"/a\", value: i})"}}]),
        );
        assert_eq!(create_errors(&p), "");
        let p = with_mutations(
            json!([{"patchType": "JSONPatch", "jsonPatch": {"expression":
            "[JSONPatch{op: \"add\", path: \"/a\", bogus: 1}]"}}]),
        );
        assert!(create_errors(&p).contains("undefined field 'bogus'"));
    }

    /// `ValidateMutatingAdmissionPolicyUpdate` (validation.go:1349-1354) with
    /// `ignoreMutatingAdmissionPolicyMatchConditions` (:630-638): unchanged
    /// `matchConditions` and `paramKind` are not compiled again; a change is.
    #[test]
    fn update_skips_unchanged_match_conditions_only() {
        let ctx = RequestContext::new(None);
        let mut old = valid();
        old.spec.as_mut().unwrap().match_conditions =
            Some(serde_json::from_value(json!([{"name": "c", "expression": "///"}])).unwrap());
        let unchanged = old.clone();
        let errs = strategy_update(&ctx, &unchanged, &old);
        assert!(errs.is_empty(), "{}", messages(&errs));

        let mut changed = old.clone();
        changed.spec.as_mut().unwrap().match_conditions =
            Some(serde_json::from_value(json!([{"name": "c", "expression": "///  "}])).unwrap());
        let errs = strategy_update(&ctx, &changed, &old);
        assert!(
            messages(&errs).contains("spec.matchConditions[0].expression"),
            "{}",
            messages(&errs)
        );

        let mut param_changed = old.clone();
        param_changed.spec.as_mut().unwrap().param_kind = None;
        let errs = strategy_update(&ctx, &param_changed, &old);
        assert!(
            messages(&errs).contains("spec.matchConditions[0].expression"),
            "{}",
            messages(&errs)
        );
    }
}

mod invalid_status_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::mutatingadmissionpolicy::*;
    use rusternetes_api_server::registry::generic::store::{CreateOptions, UpdateOptions};
    use rusternetes_api_server::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;

    /// `MutatingAdmissionPolicy` strategy `Validate` appends the authz error as a
    /// `field.Forbidden`, and `BeforeCreate` / `BeforeUpdate` wrap the list in
    /// `errors.NewInvalid(kind.GroupKind(), name, errs)`
    /// (rest/create.go:122-131, rest/update.go:149-158; errors.go:284-312), so
    /// the status is `<Kind>.<group> "<name>" is invalid: ...` with details.
    #[tokio::test]
    async fn authz_denial_is_a_new_invalid_status() {
        let obj: MutatingAdmissionPolicy = serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "MutatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "paramKind": {"kind": "ReplicaLimit", "apiVersion": "rules.example.com/v1"},
            "reinvocationPolicy": "IfNeeded",
            "mutations": [{"patchType": "ApplyConfiguration", "applyConfiguration": {"expression": "Object{ spec: Object.spec{ replicas: 1 } }"}}],
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "failurePolicy": "Ignore"}})).unwrap();
        assert!(validate(&obj).is_empty(), "{:?}", validate(&obj));
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
            msg.starts_with(r#"MutatingAdmissionPolicy.admissionregistration.k8s.io "foo" is invalid: spec.paramKind: Forbidden: "#),
            "{msg}"
        );
        let d = st.details.unwrap();
        assert_eq!(d.group.as_deref(), Some("admissionregistration.k8s.io"));
        assert_eq!(d.kind.as_deref(), Some("MutatingAdmissionPolicy"));
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
