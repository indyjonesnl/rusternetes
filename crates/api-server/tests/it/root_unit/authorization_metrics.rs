//! Unit tests moved from `src/authorization_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use prometheus::{register, IntCounterVec, Opts};
    pub use rusternetes_common::auth::UserInfo;
    pub use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
    pub use rusternetes_common::resources::{NonResourceRule, ResourceRule};
    pub use rusternetes_common::Result;
    pub use std::sync::{Arc, LazyLock, Once};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::authorization_metrics::*;
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
