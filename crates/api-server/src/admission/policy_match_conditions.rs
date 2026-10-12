//! The `matchConditions` matcher of the policy plugins (#3196, rest of
//! #3113).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/
//! matchconditions/matcher.go` (`NewMatcher`, `matcher.Match`, :44-143) and
//! its tests (`matcher_test.go` `TestMatch`). The CEL half is
//! `cel/condition.go` `condition.ForInput` (:90-111): every condition is
//! evaluated against `object`/`oldObject`/`params`/`request`/`variables`
//! (`namespaceObject` is nil, :98), and `Match` then reads the results in
//! order.
//!
//! The `admissionmetrics` observations (matcher.go:88, :113, :116, :125) are
//! ported (#3229, `admission_metrics.rs`).
//!
//! Deviations: the CEL cost budget (`RuntimeCELCostBudgetMatchConditions`)
//! is not ported. The request authorizer
//! (`Match`'s `authz` argument, a `NewCachingAuthorizer` from the dispatcher,
//! #3228) is accepted but unused until the CEL `authorizer` library is bound. A non-`bool` result is an
//! evaluation error here; the compile-time rejection (`ReturnTypes`) happens at
//! policy / configuration validation (`cel_env::output_type_failure`).

use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use rusternetes_common::admission::Operation;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel::CELEvaluator;
use rusternetes_common::cel_struct::TypeCheck;
use rusternetes_common::resources::admission_webhook::MatchCondition;
use rusternetes_common::resources::validating_admission_policy::FailurePolicy;
use serde_json::Value;

use super::admission_metrics::{
    observe_match_condition_eval_error, observe_match_condition_evaluation_time, KIND_POLICY,
    STEP_VALIDATE,
};
use super::policy_json_patch::evaluate_expression;
use super::policy_matching::Attributes;
use super::policy_mutating::{
    ConditionMatcher, MatchResult, PatchError, PatchRequest, VersionedAttributes,
};

/// `cel.EvaluationResult`: one condition's outcome.
#[derive(Debug, Clone)]
pub struct EvaluationResult {
    /// `MatchCondition.Name` of the `ExpressionAccessor`.
    pub name: String,
    /// `EvalResult == True/False`, or `Error`.
    pub result: Result<bool, String>,
}

/// `cel.ConditionEvaluator.ForInput`: `Err` is an error that is not an
/// evaluation error (the activation could not be built).
#[async_trait]
pub trait ConditionEvaluator: Send + Sync {
    async fn for_input(
        &self,
        attributes: &Attributes,
        versioned: &VersionedAttributes,
        param: Option<&Value>,
    ) -> Result<Vec<EvaluationResult>, String>;
}

/// matcher.go `matcher`.
pub struct Matcher {
    filter: Arc<dyn ConditionEvaluator>,
    /// `Fail` unless `Ignore` (a nil policy is `Fail`, :46-51).
    ignore: bool,
    matcher_kind: String,
    matcher_type: String,
    object_name: String,
}

/// matcher.go `NewMatcher` (:44-58) over an arbitrary evaluator.
pub fn new_matcher_with(
    filter: Arc<dyn ConditionEvaluator>,
    fail_policy: Option<&FailurePolicy>,
    matcher_kind: &str,
    matcher_type: &str,
    object_name: &str,
) -> Arc<dyn ConditionMatcher> {
    Arc::new(Matcher {
        filter,
        ignore: matches!(fail_policy, Some(FailurePolicy::Ignore)),
        matcher_kind: matcher_kind.to_string(),
        matcher_type: matcher_type.to_string(),
        object_name: object_name.to_string(),
    })
}

/// `NewMatcher` over compiled CEL `matchConditions` (compilation.go:47-54),
/// with matcherKind "policy" and matcherType "validate" as there;
/// `object_name` is `policy.Name`.
pub fn new_matcher(
    conditions: &[MatchCondition],
    variables: &[(String, String)],
    fail_policy: Option<&FailurePolicy>,
    object_name: &str,
) -> Arc<dyn ConditionMatcher> {
    new_matcher_with(
        Arc::new(CelConditions {
            conditions: conditions
                .iter()
                .map(|c| (c.name.clone(), c.expression.clone()))
                .collect(),
            variables: variables.to_vec(),
            evaluator: Mutex::new(CELEvaluator::new()),
        }),
        fail_policy,
        KIND_POLICY,
        STEP_VALIDATE,
        object_name,
    )
}

/// `string(versionedAttr.GetOperation())`: `admission.Operation` values.
fn operation_str(op: &Operation) -> &'static str {
    match op {
        Operation::Create => "CREATE",
        Operation::Update => "UPDATE",
        Operation::Delete => "DELETE",
        Operation::Connect => "CONNECT",
    }
}

/// `utilerrors.NewAggregate(...).Error()`.
fn aggregate(errors: &[String]) -> String {
    match errors {
        [one] => one.clone(),
        many => format!("[{}]", many.join(", ")),
    }
}

#[async_trait]
impl ConditionMatcher for Matcher {
    /// matcher.go `Match` (:60-143).
    async fn matches(
        &self,
        attributes: &Attributes,
        versioned: &VersionedAttributes,
        param: Option<&Value>,
        _authz: &dyn Authorizer,
    ) -> MatchResult {
        let t = Instant::now();
        let operation = operation_str(&attributes.operation);
        let observe_time = |t: Instant| {
            observe_match_condition_evaluation_time(
                t.elapsed(),
                &self.object_name,
                &self.matcher_kind,
                &self.matcher_type,
                operation,
            )
        };
        let results = match self.filter.for_input(attributes, versioned, param).await {
            Ok(r) => r,
            Err(err) => {
                observe_time(t);
                // filter returning error is unexpected and not an evaluation
                // error. Fail fails the request; Ignore skips the policy.
                return if self.ignore {
                    MatchResult::default()
                } else {
                    MatchResult {
                        error: Some(err),
                        ..Default::default()
                    }
                };
            }
        };

        let mut errors: Vec<String> = Vec::new();
        for r in &results {
            match &r.result {
                Err(e) => {
                    errors.push(e.clone());
                    observe_match_condition_eval_error(
                        &self.object_name,
                        &self.matcher_kind,
                        &self.matcher_type,
                        operation,
                    );
                }
                // If any condition false, skip calling webhook always
                Ok(false) => {
                    observe_time(t);
                    return MatchResult {
                        matches: false,
                        error: None,
                        failed_condition_name: Some(r.name.clone()),
                    };
                }
                Ok(true) => {}
            }
        }
        if !errors.is_empty() {
            observe_time(t);
            // If mix of true and eval errors then resort to fail policy.
            return if self.ignore {
                MatchResult::default()
            } else {
                MatchResult {
                    error: Some(aggregate(&errors)),
                    ..Default::default()
                }
            };
        }
        MatchResult {
            matches: true,
            ..Default::default()
        }
    }
}

/// The compiled CEL conditions: `condition.ForInput`.
struct CelConditions {
    /// `(name, expression)` in declaration order.
    conditions: Vec<(String, String)>,
    variables: Vec<(String, String)>,
    evaluator: Mutex<CELEvaluator>,
}

#[async_trait]
impl ConditionEvaluator for CelConditions {
    async fn for_input(
        &self,
        attributes: &Attributes,
        versioned: &VersionedAttributes,
        param: Option<&Value>,
    ) -> Result<Vec<EvaluationResult>, String> {
        // `CreateAdmissionRequest(versionedAttr.Attributes,
        // versionedAttr.GetResource(), versionedAttr.VersionedKind)`: the
        // matcher passes the request's own resource (matcher.go:62).
        let request = PatchRequest {
            matched_resource: &attributes.resource,
            versioned_attributes: versioned,
            attributes,
            param,
            namespace: None,
        };
        Ok(self
            .conditions
            .iter()
            .map(|(name, expr)| EvaluationResult {
                name: name.clone(),
                result: evaluate_expression(
                    &self.evaluator,
                    expr,
                    &self.variables,
                    &request,
                    TypeCheck::Root,
                )
                .map_err(|e| match e {
                    PatchError::Status(e) => e.to_string(),
                    PatchError::Other(m) => m,
                })
                .and_then(|v| match v {
                    Value::Bool(b) => Ok(b),
                    other => Err(format!(
                        "expression '{expr}' resulted in non-bool value: {other}"
                    )),
                }),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let r = m.matches(&a, &v, None, &AlwaysAllowAuthorizer).await;
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
        let r = m.matches(&a, &v, None, &AlwaysAllowAuthorizer).await;
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
        let r = m.matches(&a, &v, None, &AlwaysAllowAuthorizer).await;
        assert!(!r.matches);
        assert_eq!(r.failed_condition_name.as_deref(), Some("name"));
    }

    #[tokio::test]
    async fn cel_errors_follow_failure_policy() {
        let (a, v) = attrs(Some(json!({"metadata": {"name": "y"}})));
        let conds = [cond("bad", "object.nope.deeper == 1"), cond("ok", "true")];
        let r = new_matcher(&conds, &[], None, "test")
            .matches(&a, &v, None, &AlwaysAllowAuthorizer)
            .await;
        assert!(r.error.is_some() && !r.matches, "{r:?}");
        let r = new_matcher(&conds, &[], Some(&FailurePolicy::Ignore), "test")
            .matches(&a, &v, None, &AlwaysAllowAuthorizer)
            .await;
        assert!(r.error.is_none() && !r.matches, "{r:?}");
        // A non-bool result is an evaluation error too.
        let r = new_matcher(&[cond("n", "1 + 1")], &[], None, "test")
            .matches(&a, &v, None, &AlwaysAllowAuthorizer)
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
            m.matches(&a, &v, Some(&p), &AlwaysAllowAuthorizer)
                .await
                .matches
        );
        let p2 = json!({"spec": {"name": "z"}});
        assert!(
            !m.matches(&a, &v, Some(&p2), &AlwaysAllowAuthorizer)
                .await
                .matches
        );
    }

    /// `(eval errors, evaluation-time samples)` for `name` / CREATE.
    fn observed(name: &str) -> (u64, u64) {
        use crate::admission::admission_metrics::{
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
        m.matches(&a, &v, None, &AlwaysAllowAuthorizer).await;
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
}
