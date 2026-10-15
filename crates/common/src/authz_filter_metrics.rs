//! `WithAuthorization` filter metrics (#3386).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/endpoints/filters/metrics.go`
//! (`authorizationAttemptsCounter` :78-85, `authorizationLatency` :87-95,
//! `recordAuthorizationMetrics` :106-122) as called by `withAuthorization`
//! (`filters/authorization.go:57-99`).
//!
//! The api-server's `WithAuthorization` filter (`authorization_filter.rs`)
//! calls [`record_authorization_outcome_metrics`] once per request.

use crate::authz::{AuthorizationOutcome, Opinion};
use crate::Result;
use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};
use std::sync::LazyLock;
use std::time::Duration;

/// `allowedLabel` / `deniedLabel` / `noOpinionLabel` / `errorLabel`
/// (metrics.go:42-46).
const ALLOWED: &str = "allowed";
const DENIED: &str = "denied";
const NO_OPINION: &str = "no-opinion";
const ERROR: &str = "error";

static ATTEMPTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "authorization_attempts_total",
            "[ALPHA] Counter of authorization attempts broken down by result. It can be either 'allowed', 'denied', 'no-opinion' or 'error'.",
        ),
        &["result"],
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

/// `metrics.ExponentialBuckets(0.001, 2, 15)`.
fn exponential_buckets() -> Vec<f64> {
    (0..15).map(|i| 0.001 * 2f64.powi(i)).collect()
}

static LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "authorization_duration_seconds",
            "[ALPHA] Authorization duration in seconds broken out by result.",
        )
        .buckets(exponential_buckets()),
        &["result"],
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
});

/// `recordAuthorizationMetrics` (metrics.go:106-122): `allowed` wins even with
/// an error ("an authorizer like RBAC could encounter evaluation errors and
/// still allow the request", authorization.go:79), then `error`, then the
/// decision.
pub fn record_authorization_metrics(result: &Result<Opinion>, elapsed: Duration) {
    let label = match result {
        Ok(Opinion::Allow(_)) => ALLOWED,
        Err(_) | Ok(Opinion::NoOpinion { error: Some(_), .. }) => ERROR,
        Ok(Opinion::Deny(_)) => DENIED,
        Ok(Opinion::NoOpinion { error: None, .. }) => NO_OPINION,
    };
    record_label(label, elapsed);
}

/// filters/metrics.go:106-122 with an independent evaluation-error slot.
/// Allow takes precedence over error; an error overrides an explicit denial.
pub fn record_authorization_outcome_metrics(
    result: &Result<AuthorizationOutcome>,
    elapsed: Duration,
) {
    let label = match result {
        Ok(outcome) if matches!(outcome.opinion, Opinion::Allow(_)) => ALLOWED,
        Err(_) => ERROR,
        Ok(outcome)
            if outcome.error.is_some()
                || matches!(&outcome.opinion, Opinion::NoOpinion { error: Some(_), .. }) =>
        {
            ERROR
        }
        Ok(outcome) if matches!(outcome.opinion, Opinion::Deny(_)) => DENIED,
        Ok(_) => NO_OPINION,
    };
    record_label(label, elapsed);
}

fn record_label(label: &str, elapsed: Duration) {
    ATTEMPTS.with_label_values(&[label]).inc();
    LATENCY
        .with_label_values(&[label])
        .observe(elapsed.as_secs_f64());
}

/// Register the metrics, so they are exposed before the first request.
pub fn register_metrics() {
    LazyLock::force(&ATTEMPTS);
    LazyLock::force(&LATENCY);
}
