//! Authorizer decision metrics (#3315).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/authorization/metrics/metrics.go`:
//! `authorizationDecisionsTotal` (:36-45), `RecordAuthorizationDecision`
//! (:62-64) and `InstrumentedAuthorizer` / `instrumentedAuthorizer.Authorize`
//! (:66-95).

use async_trait::async_trait;
use prometheus::{register, IntCounterVec, Opts};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use std::sync::{Arc, LazyLock, Once};

static DECISIONS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "decisions_total",
            "[ALPHA] Total number of terminal decisions made by an authorizer split by authorizer type, name, and decision.",
        )
        .namespace("apiserver")
        .subsystem("authorization"),
        &["type", "name", "decision"],
    )
    .expect("valid metric")
});

static REGISTER: Once = Once::new();

/// `RegisterMetrics` (metrics.go:47-51).
pub fn register_metrics() {
    REGISTER.call_once(|| {
        let _ = register(Box::new(DECISIONS_TOTAL.clone()));
    });
}

/// `RecordAuthorizationDecision` (metrics.go:57-59).
pub fn record_authorization_decision(authorizer_type: &str, authorizer_name: &str, decision: &str) {
    DECISIONS_TOTAL
        .with_label_values(&[authorizer_type, authorizer_name, decision])
        .inc();
}

/// `ResetMetricsForTest` (metrics.go:53-55).
pub fn reset_metrics_for_test() {
    DECISIONS_TOTAL.reset();
}

/// `InstrumentedAuthorizer` (metrics.go:61-68): wraps `delegate`, counting
/// every terminal decision.
pub fn instrumented_authorizer(
    authorizer_type: &str,
    authorizer_name: &str,
    delegate: Arc<dyn Authorizer>,
) -> Arc<dyn Authorizer> {
    register_metrics();
    Arc::new(InstrumentedAuthorizer {
        authorizer_type: authorizer_type.to_string(),
        authorizer_name: authorizer_name.to_string(),
        delegate,
    })
}

struct InstrumentedAuthorizer {
    authorizer_type: String,
    authorizer_name: String,
    delegate: Arc<dyn Authorizer>,
}

impl InstrumentedAuthorizer {
    /// The `switch decision` of `Authorize` (metrics.go:77-91).
    fn record(&self, opinion: &Opinion) {
        match opinion {
            // non-terminal, not reported
            Opinion::NoOpinion { .. } => {}
            // matches SubjectAccessReview status.allowed field name
            Opinion::Allow(_) => record_authorization_decision(
                &self.authorizer_type,
                &self.authorizer_name,
                "allowed",
            ),
            // matches SubjectAccessReview status.denied field name
            Opinion::Deny(_) => record_authorization_decision(
                &self.authorizer_type,
                &self.authorizer_name,
                "denied",
            ),
        }
    }
}

#[async_trait]
impl Authorizer for InstrumentedAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        self.authorize_outcome(attrs).await?.into_decision()
    }

    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(self.authorize_outcome(attrs).await?.into_opinion())
    }

    // metrics/metrics.go:77-94 forwards decision, reason and error together.
    async fn authorize_outcome(
        &self,
        attrs: &RequestAttributes,
    ) -> Result<rusternetes_common::authz::AuthorizationOutcome> {
        let outcome = self.delegate.authorize_outcome(attrs).await?;
        self.record(&outcome.opinion);
        Ok(outcome)
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        self.delegate.get_user_rules(user, namespace).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::authz::{AlwaysAllowAuthorizer, AlwaysDenyAuthorizer};

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

    fn count(t: &str, n: &str, d: &str) -> u64 {
        DECISIONS_TOTAL.with_label_values(&[t, n, d]).get()
    }

    // TestInstrumentedAuthorizer-style: allowed is counted, NoOpinion is not.
    #[tokio::test]
    async fn counts_terminal_decisions_only() {
        let allow =
            instrumented_authorizer("AlwaysAllow", "t-allow", Arc::new(AlwaysAllowAuthorizer));
        assert!(matches!(
            allow.authorize(&attrs()).await.unwrap(),
            Decision::Allow
        ));
        assert_eq!(count("AlwaysAllow", "t-allow", "allowed"), 1);

        // AlwaysDeny returns the binary Deny, i.e. NoOpinion: not reported.
        let deny = instrumented_authorizer("AlwaysDeny", "t-deny", Arc::new(AlwaysDenyAuthorizer));
        assert!(matches!(
            deny.authorize(&attrs()).await.unwrap(),
            Decision::Deny(_)
        ));
        assert_eq!(count("AlwaysDeny", "t-deny", "denied"), 0);
    }

    #[test]
    fn registered_with_upstream_name_and_help() {
        register_metrics();
        record_authorization_decision("T", "n-help", "allowed");
        let fam = prometheus::gather()
            .into_iter()
            .find(|f| f.name() == "apiserver_authorization_decisions_total")
            .expect("registered");
        assert_eq!(
            fam.help(),
            "[ALPHA] Total number of terminal decisions made by an authorizer split by authorizer type, name, and decision."
        );
    }
}
