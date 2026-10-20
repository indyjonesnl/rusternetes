//! `RuntimeCELCostBudgetMatchConditions` per webhook (#3587, #3507 item 1).
//!
//! Upstream: `webhook/matchconditions/matcher.go:82-85` passes
//! `celconfig.RuntimeCELCostBudgetMatchConditions` (2,500,000) to
//! `ForInput`; `cel/condition.go:103-109` threads the remaining budget through
//! every `activation.Evaluate` and returns the out-of-budget error instead of
//! any evaluation (`activation.go:146-177`); `matcher.go:86-95` turns that
//! error into `MatchResult{Error}` on `Fail` and a skip on `Ignore`;
//! `generic/webhook.go:232-234` fails the request closed with `Forbidden`.

use crate::cel_evaluators::{MatchConditionEvaluator, MatchOutcome};
use crate::AdmissionWebhookManager;
use rusternetes_common::admission::{
    AdmissionReview, AdmissionReviewResponse, GroupVersionKind, GroupVersionResource, Operation,
    UserInfo,
};
use rusternetes_common::resources::{
    FailurePolicy, MatchCondition, MutatingWebhook, MutatingWebhookConfiguration, OperationType,
    Rule, RuleWithOperations, SideEffectClass, ValidatingWebhook, ValidatingWebhookConfiguration,
    WebhookClientConfig,
};
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_storage::Storage;
use serde_json::{json, Value};
use std::sync::Arc;

const HEAVY: &str = "object.xs.all(i, i >= 0)";

fn cond(name: &str, expr: &str) -> MatchCondition {
    MatchCondition {
        name: name.into(),
        expression: expr.into(),
    }
}

/// An object whose `xs` makes `object.xs.all(i, i >= 0)` run into the 1M
/// per-call limit (an evaluation error that still spends ~1M), so three such
/// conditions exhaust the 2.5M budget.
fn heavy_object() -> Value {
    let xs: Vec<u32> = (0..250_000).collect();
    json!({"metadata": {"name": "c"}, "xs": xs})
}

/// A webhook server that denies everything: reaching it means the webhook
/// was called.
async fn deny_server() -> String {
    use axum::{routing::post, Json, Router};
    let app = Router::new().route(
        "/admit",
        post(|Json(review): Json<AdmissionReview>| async move {
            let uid = review.request.map(|r| r.uid).unwrap_or_default();
            Json(AdmissionReview::new_response(AdmissionReviewResponse {
                uid,
                allowed: false,
                status: None,
                patch: None,
                patch_type: None,
                audit_annotations: None,
                warnings: None,
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    format!("http://{addr}/admit")
}

fn rule() -> RuleWithOperations {
    RuleWithOperations {
        operations: vec![OperationType::Create],
        rule: Rule {
            api_groups: vec!["".into()],
            api_versions: vec!["v1".into()],
            resources: vec!["configmaps".into()],
            scope: None,
        },
    }
}

fn gvk() -> GroupVersionKind {
    GroupVersionKind {
        group: "".into(),
        version: "v1".into(),
        kind: "ConfigMap".into(),
    }
}

fn gvr() -> GroupVersionResource {
    GroupVersionResource {
        group: "".into(),
        version: "v1".into(),
        resource: "configmaps".into(),
    }
}

fn user() -> UserInfo {
    UserInfo {
        username: "u".into(),
        uid: "1".into(),
        groups: vec![],
    }
}

async fn validating(
    url: &str,
    policy: FailurePolicy,
    conds: Vec<MatchCondition>,
) -> rusternetes_common::Result<rusternetes_common::admission::AdmissionResponse> {
    let storage = Arc::new(MemoryStorage::new());
    let cfg = ValidatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "ValidatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("v"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "v.example.com".into(),
            client_config: WebhookClientConfig {
                url: Some(url.into()),
                service: None,
                ca_bundle: None,
            },
            rules: vec![rule()],
            failure_policy: Some(policy),
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: Some(2),
            admission_review_versions: vec!["v1".into()],
            match_conditions: Some(conds),
        }]),
    };
    storage
        .create("/registry/validatingwebhookconfigurations/v", &cfg)
        .await
        .unwrap();
    AdmissionWebhookManager::new(storage)
        .run_validating_webhooks(
            &Operation::Create,
            &gvk(),
            &gvr(),
            Some("default"),
            "c",
            Some(heavy_object()),
            None,
            &user(),
        )
        .await
}

async fn mutating(
    url: &str,
    policy: FailurePolicy,
    conds: Vec<MatchCondition>,
) -> rusternetes_common::Result<rusternetes_common::admission::AdmissionResponse> {
    let storage = Arc::new(MemoryStorage::new());
    let cfg = MutatingWebhookConfiguration {
        api_version: "admissionregistration.k8s.io/v1".into(),
        kind: "MutatingWebhookConfiguration".into(),
        metadata: rusternetes_common::types::ObjectMeta::new("m"),
        webhooks: Some(vec![MutatingWebhook {
            name: "m.example.com".into(),
            client_config: WebhookClientConfig {
                url: Some(url.into()),
                service: None,
                ca_bundle: None,
            },
            rules: vec![rule()],
            failure_policy: Some(policy),
            match_policy: None,
            namespace_selector: None,
            object_selector: None,
            side_effects: SideEffectClass::None,
            timeout_seconds: Some(2),
            admission_review_versions: vec!["v1".into()],
            match_conditions: Some(conds),
            reinvocation_policy: None,
        }]),
    };
    storage
        .create("/registry/mutatingwebhookconfigurations/m", &cfg)
        .await
        .unwrap();
    AdmissionWebhookManager::new(storage)
        .run_mutating_webhooks(
            &Operation::Create,
            &gvk(),
            &gvr(),
            Some("default"),
            "c",
            Some(heavy_object()),
            None,
            &user(),
        )
        .await
        .map(|(r, _)| r)
}

fn heavy3() -> Vec<MatchCondition> {
    vec![cond("a", HEAVY), cond("b", HEAVY), cond("c", HEAVY)]
}

fn request() -> rusternetes_common::admission::AdmissionRequest {
    crate::build_admission_request_for_match(
        &Operation::Create,
        &gvk(),
        Some("default"),
        "c",
        Some(&heavy_object()),
        None,
        Some(&user()),
    )
}

/// matcher.go:86-95 + webhook.go:232-234: the budget error with `Fail`
/// fails the request closed, without calling the webhook.
#[tokio::test]
async fn validating_budget_exhausted_fail_forbids_the_request() {
    let url = deny_server().await;
    let r = validating(&url, FailurePolicy::Fail, heavy3()).await;
    let err = r.expect_err("a spent budget must fail the request");
    let msg = err.to_string();
    assert!(
        msg.contains("running out of cost budget"),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("forbidden"), "unexpected error: {msg}");
}

/// matcher.go:90-93: with `Ignore` the webhook is skipped.
#[tokio::test]
async fn validating_budget_exhausted_ignore_skips_the_webhook() {
    let url = deny_server().await;
    let r = validating(&url, FailurePolicy::Ignore, heavy3()).await;
    assert!(
        matches!(
            r,
            Ok(rusternetes_common::admission::AdmissionResponse::Allow)
        ),
        "{r:?}"
    );
}

#[tokio::test]
async fn mutating_budget_exhausted_fail_forbids_the_request() {
    let url = deny_server().await;
    let r = mutating(&url, FailurePolicy::Fail, heavy3()).await;
    let err = r.expect_err("a spent budget must fail the request");
    assert!(err.to_string().contains("running out of cost budget"));
}

#[tokio::test]
async fn mutating_budget_exhausted_ignore_skips_the_webhook() {
    let url = deny_server().await;
    let r = mutating(&url, FailurePolicy::Ignore, heavy3()).await;
    assert!(r.is_ok(), "{r:?}");
}

/// condition.go:98-110 evaluates EVERY condition before `Match` reads the
/// results, so a false first condition does not save the later ones from the
/// budget: the budget error wins over the false result.
#[test]
fn budget_is_charged_for_conditions_after_a_false_one() {
    let mut e = MatchConditionEvaluator::new();
    let conds = vec![
        cond("never", "false"),
        cond("b", HEAVY),
        cond("c", HEAVY),
        cond("d", HEAVY),
    ];
    match e.evaluate(&conds, &request(), None) {
        MatchOutcome::Error(m) => assert!(m.contains("running out of cost budget"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// Under the budget the result is unchanged.
#[test]
fn within_the_budget_conditions_match() {
    let mut e = MatchConditionEvaluator::new();
    let conds = vec![cond("a", "object.xs.size() > 0"), cond("b", "true")];
    assert_eq!(e.evaluate(&conds, &request(), None), MatchOutcome::Matched);
}

/// matcher.go:112-120: a false condition wins over errors that precede it.
#[test]
fn false_condition_wins_over_a_preceding_error() {
    let mut e = MatchConditionEvaluator::new();
    let conds = vec![cond("bad", "object.nope.x == 1"), cond("f", "false")];
    assert_eq!(
        e.evaluate(&conds, &request(), None),
        MatchOutcome::NotMatched
    );
}
