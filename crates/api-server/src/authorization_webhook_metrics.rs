//! Authorization webhook and matchCondition metrics (#3165).
//!
//! Ported from
//! - `staging/src/k8s.io/apiserver/plugin/pkg/authorizer/webhook/metrics/metrics.go`
//!   (`webhookEvaluations` / `webhookDuration` / `webhookFailOpen`, :113-150),
//! - `staging/src/k8s.io/apiserver/pkg/authorization/cel/metrics.go`
//!   (`match_condition_*`, :60-95).
//!
//! DEVIATIONS: kube-apiserver passes `NoopRequestMetrics` for the webhook
//! request total/latency (`kubeapiserverWebhookMetrics`, reload.go:182-189),
//! so those are not ported either. The `canceled` result has no request
//! context to inspect; a future dropped mid-call records it (#3217).

use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};
use std::sync::LazyLock;
use std::time::Duration;

/// `compbasemetrics.DefBuckets` (prometheus `DefBuckets`).
const DEF_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

fn counter(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(
        Opts::new(name, help)
            .namespace("apiserver")
            .subsystem("authorization"),
        labels,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
}

fn histogram(name: &str, help: &str, labels: &[&str], buckets: &[f64]) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace("apiserver")
            .subsystem("authorization")
            .buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
}

static WEBHOOK_EVALUATIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "webhook_evaluations_total",
        "Round-trips to authorization webhooks.",
        &["name", "result"],
    )
});
static WEBHOOK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram(
        "webhook_duration_seconds",
        "Request latency in seconds.",
        &["name", "result"],
        DEF_BUCKETS,
    )
});
static WEBHOOK_FAIL_OPEN: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "webhook_evaluations_fail_open_total",
        "NoOpinion results due to webhook timeout or error.",
        &["name", "result"],
    )
});
static MATCH_CONDITION_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "match_condition_evaluation_errors_total",
        "Total number of errors when an authorization webhook encounters a match condition error split by authorizer type and name.",
        &["type", "name"],
    )
});
static MATCH_CONDITION_EXCLUSIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "match_condition_exclusions_total",
        "Total number of exclusions when an authorization webhook is skipped because match conditions exclude it.",
        &["type", "name"],
    )
});
static MATCH_CONDITION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram(
        "match_condition_evaluation_seconds",
        "Authorization match condition evaluation time in seconds, split by authorizer type and name.",
        &["type", "name"],
        &[0.001, 0.005, 0.01, 0.025, 0.1, 0.2, 0.25],
    )
});

/// `RecordWebhookEvaluation` + `RecordWebhookDuration` (metrics.go:140-146).
/// `result` is one of success / canceled / timeout / error.
pub fn record_webhook_round_trip(name: &str, result: &str, elapsed: Duration) {
    WEBHOOK_EVALUATIONS.with_label_values(&[name, result]).inc();
    WEBHOOK_DURATION
        .with_label_values(&[name, result])
        .observe(elapsed.as_secs_f64());
}

/// `RecordWebhookFailOpen` (metrics.go:147-149).
pub fn record_webhook_fail_open(name: &str, result: &str) {
    WEBHOOK_FAIL_OPEN.with_label_values(&[name, result]).inc();
}

/// `RecordAuthorizationMatchConditionEvaluation` (cel/metrics.go:91-95).
pub fn record_match_condition_evaluation(
    authorizer_type: &str,
    authorizer_name: &str,
    elapsed: Duration,
) {
    MATCH_CONDITION_SECONDS
        .with_label_values(&[authorizer_type, authorizer_name])
        .observe(elapsed.as_secs_f64());
}

/// `RecordAuthorizationMatchConditionEvaluationFailure` (cel/metrics.go:83-85).
pub fn record_match_condition_failure(authorizer_type: &str, authorizer_name: &str) {
    MATCH_CONDITION_ERRORS
        .with_label_values(&[authorizer_type, authorizer_name])
        .inc();
}

/// `RecordAuthorizationMatchConditionExclusion` (cel/metrics.go:87-89).
pub fn record_match_condition_exclusion(authorizer_type: &str, authorizer_name: &str) {
    MATCH_CONDITION_EXCLUSIONS
        .with_label_values(&[authorizer_type, authorizer_name])
        .inc();
}
