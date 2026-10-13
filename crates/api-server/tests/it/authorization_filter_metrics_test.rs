//! `WithAuthorization` filter metrics (#3386).
//!
//! Ported from `endpoints/filters/metrics_test.go`
//! `TestRecordAuthorizationMetricsMetrics`: result is `allowed` when the
//! decision is allow (even with an error), else `error` when there is an
//! error, else `denied` / `no-opinion`.

use async_trait::async_trait;
use rusternetes_api_server::state::ApiServerState;
use rusternetes_common::auth::{TokenManager, UserInfo};
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::observability::MetricsRegistry;
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use rusternetes_storage::{MemoryStorage, StorageBackend};
use std::sync::Arc;

/// The metrics are process-global: serialize the tests that read them.
static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Fake(Result<Opinion>);

#[async_trait]
impl Authorizer for Fake {
    async fn authorize(&self, a: &RequestAttributes) -> Result<Decision> {
        match self.authorize_opinion(a).await? {
            Opinion::Allow => Ok(Decision::Allow),
            Opinion::Deny(r) => Ok(Decision::Deny(r)),
            Opinion::NoOpinion {
                reason,
                error: None,
            } => Ok(Decision::Deny(reason)),
            Opinion::NoOpinion { error: Some(e), .. } => {
                Err(rusternetes_common::Error::Authorization(e))
            }
        }
    }
    async fn authorize_opinion(&self, _: &RequestAttributes) -> Result<Opinion> {
        match &self.0 {
            Ok(o) => Ok(o.clone()),
            Err(e) => Err(rusternetes_common::Error::Authorization(e.to_string())),
        }
    }
    async fn get_user_rules(
        &self,
        _: &UserInfo,
        _: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        Ok((vec![], vec![]))
    }
}

fn state(op: Result<Opinion>) -> ApiServerState {
    ApiServerState::new(
        Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new()))),
        Arc::new(TokenManager::new(b"secret")),
        Arc::new(Fake(op)),
        Arc::new(MetricsRegistry::new()),
        false,
    )
}

fn attrs() -> RequestAttributes {
    RequestAttributes::new(
        UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        },
        "get",
        "pods",
    )
}

/// The value of `authorization_attempts_total{result=<r>}`.
fn attempts(r: &str) -> u64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.name() == "authorization_attempts_total")
        .flat_map(|f| f.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "result" && l.value() == r)
        })
        .map(|m| m.get_counter().value() as u64)
        .sum()
}

fn duration_count(r: &str) -> u64 {
    prometheus::gather()
        .iter()
        .filter(|f| f.name() == "authorization_duration_seconds")
        .flat_map(|f| f.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "result" && l.value() == r)
        })
        .map(|m| m.get_histogram().get_sample_count())
        .sum()
}

#[tokio::test]
async fn filter_records_every_result_label() {
    let _g = LOCK.lock().await;
    let cases: Vec<(&str, Result<Opinion>)> = vec![
        ("allowed", Ok(Opinion::Allow)),
        ("denied", Ok(Opinion::Deny("no".into()))),
        (
            "error",
            Ok(Opinion::NoOpinion {
                reason: String::new(),
                error: Some("can't parse user info".into()),
            }),
        ),
        (
            "no-opinion",
            Ok(Opinion::NoOpinion {
                reason: String::new(),
                error: None,
            }),
        ),
    ];
    for (label, op) in cases {
        let s = state(op);
        let (a0, d0) = (attempts(label), duration_count(label));
        let _ = s.authorizer.authorize(&attrs()).await;
        // Other tests authorize through states in this process: at least one.
        assert!(attempts(label) > a0, "{label}");
        assert!(duration_count(label) > d0, "{label}");
    }
}

// An authorizer that errors outright is recorded as `error`.
#[tokio::test]
async fn authorizer_error_is_the_error_label() {
    let _g = LOCK.lock().await;
    let s = state(Err(rusternetes_common::Error::Authorization("boom".into())));
    let a0 = attempts("error");
    let _ = s.authorizer.authorize(&attrs()).await;
    assert!(attempts("error") > a0);
}

// SubjectAccessReview subjects, impersonation and admission do not pass the
// filter upstream, so they must not be counted. Uses the `error` label, which
// no other test in this process produces.
#[tokio::test]
async fn unfiltered_authorizer_is_not_recorded() {
    let _g = LOCK.lock().await;
    let s = state(Err(rusternetes_common::Error::Authorization("boom".into())));
    let a0 = attempts("error");
    let _ = s.unfiltered_authorizer.authorize(&attrs()).await;
    assert_eq!(attempts("error"), a0);
}
