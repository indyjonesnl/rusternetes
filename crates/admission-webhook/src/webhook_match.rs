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

/// `apierrors.NewForbidden(qualifiedResource, name, err)` message
/// (staging/src/k8s.io/apimachinery/pkg/api/errors/errors.go:209-217), as
/// `ShouldCallHook` builds it from `attr.GetResource().GroupResource()` and
/// `attr.GetName()` (webhook.go:231-234).
pub fn forbidden_message(group: &str, resource: &str, name: &str, err: &str) -> String {
    // GroupResource.String() (runtime/schema/group_version.go:62).
    let qualified = if group.is_empty() {
        resource.to_string()
    } else {
        format!("{resource}.{group}")
    };
    if qualified.is_empty() {
        format!("forbidden: {err}")
    } else if name.is_empty() {
        format!("{qualified} is forbidden: {err}")
    } else {
        format!("{qualified} \"{name}\" is forbidden: {err}")
    }
}

/// `utilerrors.NewAggregate(errs).Error()`
/// (apimachinery/pkg/util/errors/errors.go:70-96): one error is its message;
/// several are their distinct messages joined by ", " inside brackets.
fn aggregate(errors: &[String]) -> String {
    if let [one] = errors {
        return one.clone();
    }
    let mut seen: Vec<&str> = Vec::new();
    for e in errors {
        if !seen.contains(&e.as_str()) {
            seen.push(e);
        }
    }
    if seen.len() == 1 {
        seen[0].to_string()
    } else {
        format!("[{}]", seen.join(", "))
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
            FailurePolicy::Fail => Inner::Error(aggregate(&errors)),
            FailurePolicy::Ignore => Inner::Excluded,
            FailurePolicy::Unknown(_) => Inner::Matches,
        };
    }
    Inner::Matches
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::admission::{GroupVersionKind, UserInfo};

    #[test]
    fn forbidden_message_matches_new_forbidden() {
        // errors.go:216: `<group-resource> "<name>" is forbidden: <err>`,
        // GroupResource.String() being `resource.group` outside core.
        assert_eq!(
            forbidden_message("apps", "deployments", "d", "boom"),
            "deployments.apps \"d\" is forbidden: boom"
        );
        assert_eq!(
            forbidden_message("", "configmaps", "c", "boom"),
            "configmaps \"c\" is forbidden: boom"
        );
        // errors.go:213-214: no name, no quoted name.
        assert_eq!(
            forbidden_message("apps", "deployments", "", "boom"),
            "deployments.apps is forbidden: boom"
        );
    }

    fn request() -> AdmissionRequest {
        crate::build_admission_request_for_match(
            &rusternetes_common::admission::Operation::Create,
            &GroupVersionKind {
                group: String::new(),
                version: "v1".into(),
                kind: "ConfigMap".into(),
            },
            Some("default"),
            "c",
            Some(&serde_json::json!({"metadata": {"name": "c"}})),
            None,
            Some(&UserInfo {
                username: "u".into(),
                uid: "1".into(),
                groups: vec![],
            }),
        )
    }

    fn cond(name: &str, expr: &str) -> MatchCondition {
        MatchCondition {
            name: name.into(),
            expression: expr.into(),
        }
    }

    /// `utilerrors.NewAggregate(...).Error()` (util/errors/errors.go:70-96):
    /// several distinct errors are `[a, b]`; a single one is bare.
    #[test]
    fn several_errors_are_an_aggregate_in_brackets() {
        let conds = [
            cond("a", "object.metadata.nosuch1 == 'x'"),
            cond("b", "object.metadata.nosuch2 == 'x'"),
        ];
        let r = match_webhook_conditions(
            "agg-test",
            "validate",
            &conds,
            Some(&FailurePolicy::Fail),
            &request(),
        );
        let WebhookMatch::Forbidden(msg) = r else {
            panic!("expected Forbidden, got {r:?}")
        };
        assert!(msg.starts_with('[') && msg.ends_with(']'), "got {msg}");
        let one = match_webhook_conditions(
            "agg-test-one",
            "validate",
            &conds[..1],
            Some(&FailurePolicy::Fail),
            &request(),
        );
        let WebhookMatch::Forbidden(msg) = one else {
            panic!("expected Forbidden")
        };
        assert!(!msg.starts_with('['), "got {msg}");
    }
}
