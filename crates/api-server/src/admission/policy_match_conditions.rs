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
//! Deviations: the request authorizer
//! (`Match`'s `authz` argument, a `NewCachingAuthorizer` from the dispatcher,
//! #3228) is bound to the CEL `authorizer` / `authorizer.requestResource`
//! variables (`OptionalVariableBindings.Authorizer`, matcher.go:80-86;
//! `rusternetes_common::cel_authz_library`, #3300). A non-`bool` result is an
//! evaluation error here; the compile-time rejection (`ReturnTypes`) happens at
//! policy / configuration validation (`cel_env::output_type_failure`).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use rusternetes_common::admission::Operation;
use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel::CELEvaluator;
use rusternetes_common::cel_cost::{charge_budget, RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS};
use rusternetes_common::cel_struct::TypeCheck;
use rusternetes_common::resources::admission_webhook::MatchCondition;
use rusternetes_common::resources::validating_admission_policy::FailurePolicy;
use serde_json::Value;

use super::admission_metrics::{
    observe_match_condition_eval_error, observe_match_condition_evaluation_time, KIND_POLICY,
    STEP_VALIDATE,
};
use super::policy_json_patch::{evaluate_expression_costed, CostReport};
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
        authz: Arc<dyn Authorizer>,
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
        authz: Arc<dyn Authorizer>,
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
        let results = match self
            .filter
            .for_input(attributes, versioned, param, authz)
            .await
        {
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
        authz: Arc<dyn Authorizer>,
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
            authorizer: Some(authz),
        };
        // condition.go:103-109: `remainingBudget` starts at the caller's
        // budget (matcher.go:85 `RuntimeCELCostBudgetMatchConditions`) and is
        // threaded through every `activation.Evaluate`; running out of it
        // returns the error instead of any evaluation (`return nil, -1, err`).
        let mut remaining = RUNTIME_CEL_COST_BUDGET_MATCH_CONDITIONS;
        // A composited variable is evaluated, and its cost reported, once
        // (composition.go:191-225).
        let mut charged: HashSet<String> = HashSet::new();
        let mut evaluations = Vec::with_capacity(self.conditions.len());
        for (name, expr) in &self.conditions {
            let mut report = CostReport::default();
            let result = evaluate_expression_costed(
                &self.evaluator,
                expr,
                &self.variables,
                &request,
                TypeCheck::Root,
                Some(&mut report),
            );
            let composition: u64 = report
                .variable_costs
                .iter()
                .filter(|(n, _)| charged.insert(n.clone()))
                .map(|(_, c)| *c)
                .sum();
            remaining = charge_budget(
                Some(i64::try_from(composition).unwrap_or(i64::MAX)),
                report.cost,
                remaining,
            )
            .map_err(|e| e.detail)?;
            evaluations.push(EvaluationResult {
                name: name.clone(),
                result: result
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
            });
        }
        Ok(evaluations)
    }
}
