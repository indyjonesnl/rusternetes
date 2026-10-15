//! Webhook `matchConditions` evaluation with upstream's metrics and
//! failurePolicy handling.
//!
//! Port of `Match` in
//! `staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions/matcher.go:84-143`
//! (release-1.35), with its caller in
//! `.../plugin/webhook/generic/webhook.go:224-240` (`ShouldCallHook`):
//!
//! * every condition is evaluated in order; a condition error is counted
//!   (`ObserveMatchConditionEvalError`) and evaluation CONTINUES;
//! * the first `false` stops evaluation, observes the evaluation time and
//!   skips the webhook whatever the failurePolicy;
//! * otherwise, with errors collected: observe the time, then
//!   `failurePolicy: Fail` fails the request closed (`Forbidden`) and
//!   `Ignore` skips the webhook;
//! * a skipped webhook observes `ObserveMatchConditionExclusion`; the
//!   all-`true` path observes nothing.

use std::time::Instant;

use rusternetes_common::admission::AdmissionRequest;
use rusternetes_common::cel::CELEvaluator;
use rusternetes_common::resources::admission_webhook::MatchCondition;
use rusternetes_common::resources::FailurePolicy;

use crate::admission_metrics::{
    observe_match_condition_eval_error, observe_match_condition_evaluation_time,
    observe_match_condition_exclusion, operation_label, KIND_WEBHOOK,
};
use crate::cel_evaluators::build_context;

/// What `ShouldCallHook` does with the webhook after its match conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookMatch {
    /// Call the webhook.
    Matches,
    /// Skip the webhook (`return nil, nil`, webhook.go:235-238).
    Excluded,
    /// Fail the request closed with a `Forbidden` carrying this message
    /// (webhook.go:231-234).
    Forbidden(String),
}

/// `matcher.Match` + the `ShouldCallHook` branch on its result. `step_type`
/// is the accessor's `GetType()`: `"admit"` for mutating, `"validate"` for
/// validating webhooks (webhook/accessors.go:129, :296).
pub fn match_webhook_conditions(
    name: &str,
    step_type: &str,
    conditions: &[MatchCondition],
    failure_policy: Option<&FailurePolicy>,
    request: &AdmissionRequest,
) -> WebhookMatch {
    if conditions.is_empty() {
        return WebhookMatch::Matches;
    }
    // NewMatcher: a nil failurePolicy is Fail (matcher.go:65-69).
    let policy = failure_policy.unwrap_or(&FailurePolicy::Fail);
    let op = operation_label(&request.operation);
    let observe_time = |t: Instant| {
        observe_match_condition_evaluation_time(t.elapsed(), name, KIND_WEBHOOK, step_type, op)
    };
    let t = Instant::now();

    let result = match_inner(
        name,
        step_type,
        conditions,
        policy,
        request,
        &observe_time,
        t,
    );
    match result {
        Inner::Matches => WebhookMatch::Matches,
        Inner::Excluded => {
            observe_match_condition_exclusion(name, KIND_WEBHOOK, step_type, op);
            WebhookMatch::Excluded
        }
        Inner::Error(msg) => WebhookMatch::Forbidden(msg),
    }
}

enum Inner {
    Matches,
    Excluded,
    Error(String),
}

fn match_inner(
    name: &str,
    step_type: &str,
    conditions: &[MatchCondition],
    policy: &FailurePolicy,
    request: &AdmissionRequest,
    observe_time: &dyn Fn(Instant),
    t: Instant,
) -> Inner {
    let op = operation_label(&request.operation);
    let context = match build_context(request, None) {
        Ok(c) => c,
        Err(e) => {
            // matcher.go:88-103: the filter itself failing is not an
            // evaluation error, so no error metric.
            observe_time(t);
            return match policy {
                FailurePolicy::Fail => Inner::Error(e.to_string()),
                FailurePolicy::Ignore => Inner::Excluded,
                // No default branch upstream: falls through with no results.
                FailurePolicy::Unknown(_) => Inner::Matches,
            };
        }
    };

    let mut evaluator = CELEvaluator::new();
    let mut errors: Vec<String> = Vec::new();
    for cond in conditions {
        if cond.expression.trim().is_empty() {
            continue;
        }
        match evaluator.evaluate(&cond.expression, &context) {
            Ok(true) => {}
            Ok(false) => {
                // matcher.go:111-117: any false skips the webhook always.
                observe_time(t);
                return Inner::Excluded;
            }
            Err(e) => {
                errors.push(format!(
                    "matchCondition '{}' (expression: {}) failed: {}",
                    cond.name, cond.expression, e
                ));
                observe_match_condition_eval_error(name, KIND_WEBHOOK, step_type, op);
            }
        }
    }
    if !errors.is_empty() {
        // matcher.go:118-135: a mix of true and errors resorts to failurePolicy.
        observe_time(t);
        return match policy {
            FailurePolicy::Fail => Inner::Error(errors.join(", ")),
            FailurePolicy::Ignore => Inner::Excluded,
            FailurePolicy::Unknown(_) => Inner::Matches,
        };
    }
    Inner::Matches
}
