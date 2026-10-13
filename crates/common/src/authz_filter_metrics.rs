//! `WithAuthorization` filter metrics (#3386).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/endpoints/filters/metrics.go`
//! (`authorizationAttemptsCounter` :78-85, `authorizationLatency` :87-95,
//! `recordAuthorizationMetrics` :106-122) as called by `withAuthorization`
//! (`filters/authorization.go:57-99`).
//!
//! Unlike upstream, which authorizes once in the `WithAuthorization` filter,
//! each Rusternetes handler authorizes its own request through
//! `ApiServerState::authorizer`. That field holds the authorizer wrapped by
//! [`filter_instrumented`]; callers that reach the authorizer outside the
//! upstream filter use `ApiServerState::unfiltered_authorizer`.

use crate::auth::UserInfo;
use crate::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use crate::resources::{NonResourceRule, ResourceRule};
use crate::Result;
use async_trait::async_trait;
use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

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
        Ok(Opinion::Allow) => ALLOWED,
        Err(_) | Ok(Opinion::NoOpinion { error: Some(_), .. }) => ERROR,
        Ok(Opinion::Deny(_)) => DENIED,
        Ok(Opinion::NoOpinion { error: None, .. }) => NO_OPINION,
    };
    ATTEMPTS.with_label_values(&[label]).inc();
    LATENCY
        .with_label_values(&[label])
        .observe(elapsed.as_secs_f64());
}

/// Wraps the authorizer the request handlers call, recording the filter's
/// attempt counter and duration histogram.
pub fn filter_instrumented(delegate: Arc<dyn Authorizer>) -> Arc<dyn Authorizer> {
    LazyLock::force(&ATTEMPTS);
    LazyLock::force(&LATENCY);
    Arc::new(FilterInstrumentedAuthorizer { delegate })
}

struct FilterInstrumentedAuthorizer {
    delegate: Arc<dyn Authorizer>,
}

#[async_trait]
impl Authorizer for FilterInstrumentedAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        match self.authorize_opinion(attrs).await? {
            Opinion::Allow => Ok(Decision::Allow),
            Opinion::Deny(reason) => Ok(Decision::Deny(reason)),
            Opinion::NoOpinion { reason, error } => match error {
                Some(e) => Err(crate::error::Error::Authorization(e)),
                None => Ok(Decision::Deny(reason)),
            },
        }
    }

    /// authorization.go:64-77: time the call, record, then act on it.
    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        let start = Instant::now();
        let result = self.delegate.authorize_opinion(attrs).await;
        record_authorization_metrics(&result, start.elapsed());
        result
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        self.delegate.get_user_rules(user, namespace).await
    }
}
