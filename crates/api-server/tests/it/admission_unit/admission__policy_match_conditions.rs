//! Unit tests moved from `src/admission/policy_match_conditions.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::admission::admission_metrics::{
        observe_match_condition_eval_error, observe_match_condition_evaluation_time, KIND_POLICY,
        STEP_VALIDATE,
    };
    pub use rusternetes_api_server::admission::policy_json_patch::{
        evaluate_expression_costed, CostReport,
    };
    pub use rusternetes_api_server::admission::policy_matching::Attributes;
    pub use rusternetes_api_server::admission::policy_mutating::{
        ConditionMatcher, MatchResult, PatchError, PatchRequest, VersionedAttributes,
    };
    pub use rusternetes_common::admission::Operation;
    pub use rusternetes_common::authz::Authorizer;
    pub use rusternetes_common::cel::CELEvaluator;
    pub use rusternetes_common::cel_cost::{
        charge_budget, RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS,
    };
    pub use rusternetes_common::cel_struct::TypeCheck;
    pub use rusternetes_common::resources::admission_webhook::MatchCondition;
    pub use rusternetes_common::resources::validating_admission_policy::FailurePolicy;
    pub use serde_json::Value;
    pub use std::collections::HashSet;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_match_conditions::*;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    use serde_json::json;

    struct Fake {
        evaluations: Vec<EvaluationResult>,
        throw_error: bool,
    }
    #[async_trait]
    impl ConditionEvaluator for Fake {
        async fn for_input(
            &self,
            _: &Attributes,
            _: &VersionedAttributes,
            _: Option<&Value>,
            _: Arc<dyn Authorizer>,
        ) -> Result<Vec<EvaluationResult>, String> {
            if self.throw_error {
                return Err("test error".into());
            }
            Ok(self.evaluations.clone())
        }
    }

    fn attrs(object: Option<Value>) -> (Attributes, VersionedAttributes) {
        let gvk = GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        };
        let a = Attributes {
            kind: gvk.clone(),
            resource: GroupVersionResource {
                group: "apps".into(),
                version: "v1".into(),
                resource: "deployments".into(),
            },
            subresource: String::new(),
            namespace: "default".into(),
            name: "foo".into(),
            operation: Operation::Create,
            object: object.clone(),
            old_object: None,
            user_info: None,
            dry_run: false,
            operation_options: None,
        };
        let v = VersionedAttributes {
            versioned_kind: gvk,
            versioned_object: object,
            versioned_old_object: None,
            dirty: false,
        };
        (a, v)
    }

    fn t(name: &str) -> EvaluationResult {
        EvaluationResult {
            name: name.into(),
            result: Ok(true),
        }
    }
    fn f(name: &str) -> EvaluationResult {
        EvaluationResult {
            name: name.into(),
            result: Ok(false),
        }
    }
    fn e() -> EvaluationResult {
        EvaluationResult {
            name: String::new(),
            result: Err("test error".into()),
        }
    }

    /// matcher_test.go `TestMatch`.
    #[tokio::test]
    async fn upstream_test_match() {
        use FailurePolicy::{Fail, Ignore};
        // (name, evaluations, throw, matches, returned name, policy, error)
        type Case = (
            &'static str,
            Vec<EvaluationResult>,
            bool,
            bool,
            &'static str,
            Option<FailurePolicy>,
            &'static str,
        );
        let cases: Vec<Case> = vec![
            ("single matches", vec![t("")], false, true, "", None, ""),
            (
                "multiple match",
                vec![t(""), t("")],
                false,
                true,
                "",
                None,
                "",
            ),
            ("empty evals", vec![], false, true, "", None, ""),
            (
                "single no match",
                vec![f("test1")],
                false,
                false,
                "test1",
                None,
                "",
            ),
            (
                "multiple no match",
                vec![f("test1"), f("test2")],
                false,
                false,
                "test1",
                None,
                "",
            ),
            (
                "mixed, no match first",
                vec![f("test1"), t("test2")],
                false,
                false,
                "test1",
                None,
                "",
            ),
            (
                "mixed, no match last",
                vec![t("test2"), f("test1")],
                false,
                false,
                "test1",
                None,
                "",
            ),
            (
                "mixed, no match middle",
                vec![t("test2"), f("test1"), t("test2")],
                false,
                false,
                "test1",
                None,
                "",
            ),
            (
                "error, no fail policy",
                vec![t("")],
                true,
                false,
                "",
                None,
                "test error",
            ),
            (
                "error, fail",
                vec![t("")],
                true,
                false,
                "",
                Some(Fail),
                "test error",
            ),
            (
                "error, ignore",
                vec![t("")],
                true,
                false,
                "",
                Some(Ignore),
                "",
            ),
            (
                "mix of true, errors and false",
                vec![t(""), e(), f(""), t(""), e()],
                false,
                false,
                "",
                None,
                "",
            ),
            (
                "mix of true, errors, policy not set",
                vec![t(""), e(), t(""), e()],
                false,
                false,
                "",
                None,
                "[test error, test error]",
            ),
            (
                "mix of true, errors, fail",
                vec![t(""), e(), t(""), e()],
                false,
                false,
                "",
                Some(Fail),
                "[test error, test error]",
            ),
            (
                "mix of true, errors, ignore",
                vec![t(""), e(), t(""), e()],
                false,
                false,
                "",
                Some(Ignore),
                "",
            ),
        ];
        let (a, v) = attrs(None);
        for (name, evals, throw, matches, returned, policy, expect_error) in cases {
            let m = new_matcher_with(
                Arc::new(Fake {
                    evaluations: evals,
                    throw_error: throw,
                }),
                policy.as_ref(),
                KIND_POLICY,
                STEP_VALIDATE,
                "test",
            );
            let r = m
                .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
                .await;
            // A failed fake-evaluator call matches nothing, so `matches` is
            // false there (the Go case's `shouldMatch` is not asserted for
            // errors).
            assert_eq!(r.matches, matches, "{name}");
            assert_eq!(r.error.as_deref().unwrap_or(""), expect_error, "{name}");
            assert_eq!(
                r.failed_condition_name.as_deref().unwrap_or(""),
                returned,
                "{name}"
            );
        }
    }

    fn cond(name: &str, expr: &str) -> MatchCondition {
        MatchCondition {
            name: name.into(),
            expression: expr.into(),
        }
    }

    /// Real CEL: `object`, `request` and `variables` are bound.
    #[tokio::test]
    async fn cel_conditions_bind_object_request_and_variables() {
        let (a, v) = attrs(Some(
            json!({"metadata": {"name": "x", "labels": {"a": "b"}}}),
        ));
        let m = new_matcher(
            &[
                cond("name", "object.metadata.name == 'x'"),
                cond("op", "request.operation == 'CREATE'"),
                cond("var", "variables.lbl == 'b'"),
            ],
            &[("lbl".into(), "object.metadata.labels.a".into())],
            None,
            "test",
        );
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(r.matches && r.error.is_none(), "{r:?}");
    }

    #[tokio::test]
    async fn cel_false_condition_short_circuits_with_its_name() {
        let (a, v) = attrs(Some(json!({"metadata": {"name": "y"}})));
        let m = new_matcher(
            &[
                cond("ok", "true"),
                cond("name", "object.metadata.name == 'x'"),
                cond("late", "false"),
            ],
            &[],
            None,
            "test",
        );
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(!r.matches);
        assert_eq!(r.failed_condition_name.as_deref(), Some("name"));
    }

    #[tokio::test]
    async fn cel_errors_follow_failure_policy() {
        let (a, v) = attrs(Some(json!({"metadata": {"name": "y"}})));
        let conds = [cond("bad", "object.nope.deeper == 1"), cond("ok", "true")];
        let r = new_matcher(&conds, &[], None, "test")
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(r.error.is_some() && !r.matches, "{r:?}");
        let r = new_matcher(&conds, &[], Some(&FailurePolicy::Ignore), "test")
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(r.error.is_none() && !r.matches, "{r:?}");
        // A non-bool result is an evaluation error too.
        let r = new_matcher(&[cond("n", "1 + 1")], &[], None, "test")
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(r.error.is_some(), "{r:?}");
    }

    #[tokio::test]
    async fn cel_params_are_bound() {
        let (a, v) = attrs(Some(json!({"metadata": {"name": "x"}})));
        let m = new_matcher(
            &[cond("p", "params.spec.name == object.metadata.name")],
            &[],
            None,
            "test",
        );
        let p = json!({"spec": {"name": "x"}});
        assert!(
            m.matches(&a, &v, Some(&p), Arc::new(AlwaysAllowAuthorizer))
                .await
                .matches
        );
        let p2 = json!({"spec": {"name": "z"}});
        assert!(
            !m.matches(&a, &v, Some(&p2), Arc::new(AlwaysAllowAuthorizer))
                .await
                .matches
        );
    }

    /// `(eval errors, evaluation-time samples)` for `name` / CREATE.
    fn observed(name: &str) -> (u64, u64) {
        use rusternetes_api_server::admission::admission_metrics::{
            MATCH_CONDITION_EVALUATION_SECONDS, MATCH_CONDITION_EVAL_ERRORS,
        };
        let l = [name, KIND_POLICY, STEP_VALIDATE, "CREATE"];
        (
            MATCH_CONDITION_EVAL_ERRORS.with_label_values(&l).get(),
            MATCH_CONDITION_EVALUATION_SECONDS
                .with_label_values(&l)
                .get_sample_count(),
        )
    }

    async fn match_named(
        name: &str,
        evals: Vec<EvaluationResult>,
        throw: bool,
        p: Option<FailurePolicy>,
    ) {
        let (a, v) = attrs(None);
        let m = new_matcher_with(
            Arc::new(Fake {
                evaluations: evals,
                throw_error: throw,
            }),
            p.as_ref(),
            KIND_POLICY,
            STEP_VALIDATE,
            name,
        );
        m.matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
    }

    /// matcher.go:87-89: a filter error observes the evaluation time but is
    /// "not an evaluation error so not incrementing metric here".
    #[tokio::test]
    async fn filter_error_observes_time_not_eval_error() {
        match_named("m-filter-err", vec![], true, None).await;
        assert_eq!(observed("m-filter-err"), (0, 1));
    }

    /// matcher.go:111-119, :124-126: each eval error counts, and a false
    /// condition observes the time once.
    #[tokio::test]
    async fn eval_errors_count_each_and_false_observes_time() {
        match_named("m-mixed", vec![e(), e(), f("c")], false, None).await;
        assert_eq!(observed("m-mixed"), (2, 1));
    }

    /// matcher.go:124-126: errors with no false condition observe the time
    /// once, whatever the failure policy.
    #[tokio::test]
    async fn eval_errors_without_false_observe_time_once() {
        match_named("m-errs-fail", vec![t(""), e()], false, None).await;
        assert_eq!(observed("m-errs-fail"), (1, 1));
        match_named(
            "m-errs-ignore",
            vec![t(""), e()],
            false,
            Some(FailurePolicy::Ignore),
        )
        .await;
        assert_eq!(observed("m-errs-ignore"), (1, 1));
    }

    /// matcher.go:136-142: all conditions true observes nothing.
    #[tokio::test]
    async fn all_true_observes_nothing() {
        match_named("m-true", vec![t(""), t("")], false, None).await;
        assert_eq!(observed("m-true"), (0, 0));
    }

    /// The `authorizer` library in `matchConditions`: ported from
    /// `admission/plugin/cel/condition_test.go` `TestCondition`
    /// ("test authorizer ..." cases, :478-785), evaluated through the matcher
    /// that hands `Match`'s `authz` to the activation (matcher.go:80-86).
    mod authorizer_library {
        use super::*;
        use rusternetes_common::auth::UserInfo;
        use rusternetes_common::authz::{Decision, Opinion, RequestAttributes};
        use rusternetes_common::Error;

        type Outcome = Result<Opinion, String>;

        /// `fakeAuthorizer`: the `expect` attributes get `match_result`, any
        /// other request gets `default_result`.
        struct Fake {
            expect: Option<RequestAttributes>,
            match_result: Outcome,
            default_result: Outcome,
        }

        impl Fake {
            fn allow_match(expect: RequestAttributes) -> Self {
                Self {
                    expect: Some(expect),
                    match_result: Ok(Opinion::Allow(String::new())),
                    default_result: Ok(Opinion::Deny(String::new())),
                }
            }
            /// `denyAll`.
            fn deny_all() -> Self {
                Self {
                    expect: None,
                    match_result: Ok(Opinion::Allow(String::new())),
                    default_result: Ok(Opinion::Deny("fake reason".into())),
                }
            }
            /// `errorAll`.
            fn error_all() -> Self {
                Self {
                    expect: None,
                    match_result: Ok(Opinion::Allow(String::new())),
                    default_result: Err("fake authz error".into()),
                }
            }
        }

        #[async_trait]
        impl Authorizer for Fake {
            async fn authorize(
                &self,
                attrs: &RequestAttributes,
            ) -> rusternetes_common::Result<Decision> {
                match self.authorize_opinion(attrs).await? {
                    Opinion::Allow(_) => Ok(Decision::Allow),
                    Opinion::Deny(r) => Ok(Decision::Deny(r)),
                    Opinion::NoOpinion { reason, .. } => Ok(Decision::Deny(reason)),
                }
            }
            async fn authorize_opinion(
                &self,
                attrs: &RequestAttributes,
            ) -> rusternetes_common::Result<Opinion> {
                let matched = self
                    .expect
                    .as_ref()
                    .is_some_and(|e| format!("{e:?}") == format!("{attrs:?}"));
                let r = if matched {
                    &self.match_result
                } else {
                    &self.default_result
                };
                r.clone().map_err(Error::Authorization)
            }
            async fn get_user_rules(
                &self,
                _: &UserInfo,
                _: &str,
            ) -> rusternetes_common::Result<(
                Vec<rusternetes_common::resources::ResourceRule>,
                Vec<rusternetes_common::resources::NonResourceRule>,
            )> {
                Ok((vec![], vec![]))
            }
        }

        fn no_user() -> UserInfo {
            UserInfo {
                username: String::new(),
                uid: String::new(),
                groups: vec![],
                extra: Default::default(),
            }
        }

        fn endpoints_attrs(subresource: &str) -> (Attributes, VersionedAttributes) {
            let (mut a, v) = attrs(Some(json!({"metadata": {"name": "endpoints1"}})));
            a.resource = GroupVersionResource {
                group: String::new(),
                version: "v1".into(),
                resource: "endpoints".into(),
            };
            a.subresource = subresource.into();
            a.name = "endpoints1".into();
            (a, v)
        }

        fn resource(verb: &str) -> RequestAttributes {
            let mut r = RequestAttributes::new(no_user(), verb, "endpoints");
            r.api_version = "*".into();
            r
        }

        fn deployments(verb: &str) -> RequestAttributes {
            let mut r = resource(verb);
            r.api_group = "apps".into();
            r.resource = "deployments".into();
            r.subresource = Some("status".into());
            r.namespace = Some("test".into());
            r.name = Some("backend".into());
            r
        }

        /// Each expression is matched as its own single condition.
        async fn eval(
            exprs: &[&str],
            attrs: (Attributes, VersionedAttributes),
            authz: Fake,
        ) -> Vec<Result<bool, String>> {
            let authz: Arc<dyn Authorizer> = Arc::new(authz);
            let mut out = Vec::new();
            for e in exprs {
                let m = new_matcher(&[cond("c", e)], &[], None, "test");
                let r = m.matches(&attrs.0, &attrs.1, None, authz.clone()).await;
                out.push(match (r.error, r.matches) {
                    (Some(e), _) => Err(e),
                    (None, m) => Ok(m),
                });
            }
            out
        }

        #[tokio::test]
        async fn allow_resource_check() {
            let r = eval(
                &[
                    "authorizer.group('').resource('endpoints').check('create').allowed()",
                    "!authorizer.group('').resource('endpoints').check('create').errored()",
                ],
                endpoints_attrs(""),
                Fake::allow_match(resource("create")),
            )
            .await;
            assert_eq!(r, vec![Ok(true), Ok(true)]);
        }

        #[tokio::test]
        async fn allow_resource_check_with_all_fields() {
            let mut want = deployments("create");
            want.set_selectors("foo=bar", "apple=banana");
            assert!(!want.field_selector_requirements.is_empty());
            assert!(!want.label_selector_requirements.is_empty());
            let r = eval(
                &["authorizer.group('apps').resource('deployments').fieldSelector('foo=bar').labelSelector('apple=banana').subresource('status').namespace('test').name('backend').check('create').allowed()"],
                endpoints_attrs(""),
                Fake::allow_match(want),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        #[tokio::test]
        async fn allow_resource_check_with_selector_parse_failures() {
            let mut want = deployments("create");
            want.set_selectors("foo badoperator bar", "apple badoperator banana");
            assert!(want.field_selector_parsing_err.is_some());
            assert!(want.label_selector_parsing_err.is_some());
            let r = eval(
                &["authorizer.group('apps').resource('deployments').fieldSelector('foo badoperator bar').labelSelector('apple badoperator banana').subresource('status').namespace('test').name('backend').check('create').allowed()"],
                endpoints_attrs(""),
                Fake::allow_match(want),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        #[tokio::test]
        async fn not_allowed_when_one_field_differs() {
            let mut want = deployments("create");
            want.resource = "deployments-xxxx".into();
            let r = eval(
                &["authorizer.group('apps').resource('deployments').subresource('status').namespace('test').name('backend').check('create').allowed() == false"],
                endpoints_attrs(""),
                Fake::allow_match(want),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        #[tokio::test]
        async fn reason() {
            let r = eval(
                &["authorizer.group('').resource('endpoints').check('create').reason() == 'fake reason'"],
                endpoints_attrs(""),
                Fake::deny_all(),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        #[tokio::test]
        async fn error() {
            let r = eval(
                &[
                    "authorizer.group('').resource('endpoints').check('create').errored()",
                    "authorizer.group('').resource('endpoints').check('create').error() == 'fake authz error'",
                    "!authorizer.group('').resource('endpoints').check('create').allowed()",
                ],
                endpoints_attrs(""),
                Fake::error_all(),
            )
            .await;
            assert_eq!(r, vec![Ok(true), Ok(true), Ok(true)]);
        }

        #[tokio::test]
        async fn path_check() {
            let want = RequestAttributes::new_non_resource(no_user(), "get", "/healthz");
            let r = eval(
                &[
                    "authorizer.path('/healthz').check('get').allowed()",
                    "authorizer.path('/other').check('get').allowed() == false",
                ],
                endpoints_attrs(""),
                Fake::allow_match(want),
            )
            .await;
            assert_eq!(r, vec![Ok(true), Ok(true)]);
            let r = eval(
                &["authorizer.path('/healthz').check('get').allowed() == false"],
                endpoints_attrs(""),
                Fake::deny_all(),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        #[tokio::test]
        async fn request_resource() {
            let mut want = resource("custom-verb");
            want.namespace = Some("default".into());
            want.name = Some("endpoints1".into());
            let r = eval(
                &["authorizer.requestResource.check('custom-verb').allowed()"],
                endpoints_attrs(""),
                Fake::allow_match(want.clone()),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
            want.subresource = Some("status".into());
            let r = eval(
                &["authorizer.requestResource.check('custom-verb').allowed()"],
                endpoints_attrs("status"),
                Fake::allow_match(want),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        #[tokio::test]
        async fn service_account() {
            let mut want = resource("custom-verb");
            want.user = UserInfo {
                username: "system:serviceaccount:default:test-serviceaccount".into(),
                uid: String::new(),
                groups: vec![
                    "system:serviceaccounts".into(),
                    "system:serviceaccounts:default".into(),
                ],
                extra: Default::default(),
            };
            want.namespace = Some("default".into());
            want.name = Some("endpoints1".into());
            let r = eval(
                &["authorizer.serviceAccount('default', 'test-serviceaccount').group('').resource('endpoints').namespace('default').name('endpoints1').check('custom-verb').allowed()"],
                endpoints_attrs(""),
                Fake::allow_match(want),
            )
            .await;
            assert_eq!(r, vec![Ok(true)]);
        }

        /// authz.go:383-385, :404-432, :445-447: argument validation errors.
        #[tokio::test]
        async fn argument_validation_errors() {
            for e in [
                "authorizer.path('  ').check('get').allowed()",
                "authorizer.group('').resource('  ').check('get').allowed()",
                "authorizer.serviceAccount('not@a#valid!namespace', 'ok').group('').resource('pods').check('get').allowed()",
                "authorizer.serviceAccount('ns', 'invalid@*name').group('').resource('pods').check('get').allowed()",
            ] {
                let r = eval(&[e], endpoints_attrs(""), Fake::deny_all()).await;
                assert!(r[0].is_err(), "{e}: {r:?}");
            }
        }
    }
}

#[path = "policy_match_conditions_budget_tests.rs"]
mod budget_tests {
    //! `RuntimeCELCostBudgetMatchConditions` enforcement of the matchConditions
    //! matcher (#3313): `matcher.go:82-85` passes
    //! `celconfig.RuntimeCELCostBudgetMatchConditions` to `ForInput`, and
    //! `condition.go:103-109` threads the remaining budget through every
    //! `activation.Evaluate` (`activation.go:132-189`).

    use super::__parent::*;
    use rusternetes_api_server::admission::policy_match_conditions::*;
    use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    use rusternetes_common::authz::AlwaysAllowAuthorizer;
    use serde_json::json;

    fn attrs(object: Value) -> (Attributes, VersionedAttributes) {
        let gvk = GroupVersionKind {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
        };
        let a = Attributes {
            kind: gvk.clone(),
            resource: GroupVersionResource {
                group: "apps".into(),
                version: "v1".into(),
                resource: "deployments".into(),
            },
            subresource: String::new(),
            namespace: "default".into(),
            name: "foo".into(),
            operation: Operation::Create,
            object: Some(object.clone()),
            old_object: None,
            user_info: None,
            dry_run: false,
            operation_options: None,
        };
        let v = VersionedAttributes {
            versioned_kind: gvk,
            versioned_object: Some(object),
            versioned_old_object: None,
            dirty: false,
        };
        (a, v)
    }

    fn cond(name: &str, expr: &str) -> MatchCondition {
        MatchCondition {
            name: name.into(),
            expression: expr.into(),
        }
    }

    /// An object whose `xs` makes `object.xs.all(i, i >= 0)` cost more than the
    /// per-call limit (1,000,000), so each such condition spends ~1M of the 2.5M
    /// match-conditions budget while only failing as an evaluation error.
    fn heavy_object() -> Value {
        let xs: Vec<u32> = (0..250_000).collect();
        json!({"xs": xs})
    }

    const HEAVY: &str = "object.xs.all(i, i >= 0)";

    /// matcher.go:86-95 with `Fail`: the budget error fails the request.
    #[tokio::test]
    async fn exhausting_the_budget_fails_the_request_with_fail_policy() {
        let (a, v) = attrs(heavy_object());
        let m = new_matcher(
            &[cond("a", HEAVY), cond("b", HEAVY), cond("c", HEAVY)],
            &[],
            None,
            "test",
        );
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(!r.matches, "{r:?}");
        assert_eq!(
            r.error.as_deref(),
            Some(
                "validation failed due to running out of cost budget, no further validation rules will be run"
            )
        );
    }

    /// matcher.go:86-95 with `Ignore`: the policy is skipped.
    #[tokio::test]
    async fn exhausting_the_budget_skips_the_policy_with_ignore() {
        let (a, v) = attrs(heavy_object());
        let m = new_matcher(
            &[cond("a", HEAVY), cond("b", HEAVY), cond("c", HEAVY)],
            &[],
            Some(&FailurePolicy::Ignore),
            "test",
        );
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(!r.matches && r.error.is_none(), "{r:?}");
    }

    /// Two over-limit conditions spend ~2M; they still fit the budget and are
    /// ordinary per-expression evaluation errors (activation.go:176-180).
    #[tokio::test]
    async fn per_call_limit_is_an_evaluation_error_within_the_budget() {
        let (a, v) = attrs(heavy_object());
        let m = new_matcher(&[cond("a", HEAVY), cond("b", HEAVY)], &[], None, "test");
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        let err = r
            .error
            .expect("evaluation errors resort to the fail policy");
        assert!(
            err.contains("operation cancelled: actual cost limit exceeded"),
            "{err}"
        );
        assert!(!err.contains("running out of cost budget"), "{err}");
    }

    /// composition.go:191-198, :217-225: a composited variable is evaluated (and
    /// its cost reported) once, when first read, however many conditions read it.
    #[tokio::test]
    async fn variable_cost_is_charged_once() {
        let (a, v) = attrs(heavy_object());
        let m = new_matcher(
            &[
                cond("a", "variables.v"),
                cond("b", "variables.v"),
                cond("c", "variables.v"),
            ],
            &[("v".into(), HEAVY.into())],
            None,
            "test",
        );
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        let err = r.error.expect("the variable fails to evaluate");
        assert!(!err.contains("running out of cost budget"), "{err}");
    }

    /// A cheap condition set is unaffected.
    #[tokio::test]
    async fn cheap_conditions_stay_within_the_budget() {
        let (a, v) = attrs(json!({"xs": [1, 2, 3]}));
        let m = new_matcher(
            &[cond("a", "object.xs.all(i, i > 0)"), cond("b", "true")],
            &[],
            None,
            "test",
        );
        let r = m
            .matches(&a, &v, None, Arc::new(AlwaysAllowAuthorizer))
            .await;
        assert!(r.matches && r.error.is_none(), "{r:?}");
    }
}
