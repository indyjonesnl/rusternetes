//! Unit tests moved from `src/authorization_config_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::core::{Collector, Desc};
    pub use prometheus::proto::MetricFamily;
    pub use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};
    pub use sha2::{Digest, Sha256};
    pub use std::sync::{Arc, LazyLock, Once, RwLock};
    pub use std::time::{SystemTime, UNIX_EPOCH};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::authorization_config_metrics::*;

    // metrics_test.go:28-29.
    const TEST_API_SERVER_ID: &str = "testAPIServerID";
    const TEST_API_SERVER_ID_HASH: &str =
        "sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37";
    // metrics_test.go's testConfigData (:30-50) and the `hash` label pinned
    // for it (:79).
    const TEST_CONFIG_DATA: &str = "\napiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n  name: error.example.com\n  webhook:\n    timeout: 5s\n    failurePolicy: Deny\n    subjectAccessReviewVersion: v1\n    matchConditionSubjectAccessReviewVersion: v1\n    authorizedTTL: 1ms\n    unauthorizedTTL: 1ms\n    connectionInfo:\n      type: KubeConfigFile\n      kubeConfigFile: /path/to/kubeconfig\n    matchConditions:\n    - expression: has(request.resourceAttributes)\n    - expression: 'request.resourceAttributes.namespace == \"fail\"'\n    - expression: 'request.resourceAttributes.name == \"error\"'\n";
    const CONFIG_HASH: &str =
        "sha256:2a66c95e5593fc74e6c3dbbb708741361f0690ed45d17e4d4ac0b9c282b6538f";

    /// Private metrics + registry per test (no shared process state, so no
    /// serialisation needed; see [`Metrics`]).
    fn fixture() -> (Metrics, Registry) {
        let m = Metrics::new();
        let r = Registry::new();
        m.register_with(&r);
        (m, r)
    }

    fn family(r: &Registry, name: &str) -> Option<prometheus::proto::MetricFamily> {
        r.gather().into_iter().find(|f| f.name() == name)
    }

    fn label<'a>(m: &'a prometheus::proto::Metric, name: &str) -> &'a str {
        m.get_label()
            .iter()
            .find(|l| l.name() == name)
            .map(|l| l.value())
            .unwrap_or("")
    }

    #[test]
    fn get_hash_matches_upstream_vectors() {
        assert_eq!(get_hash(TEST_API_SERVER_ID), TEST_API_SERVER_ID_HASH);
        assert_eq!(get_hash(""), "");
        assert_eq!(get_hash(TEST_CONFIG_DATA), CONFIG_HASH);
    }

    // TestRecordAuthorizationConfigAutomaticReloadFailure (metrics_test.go:53-70).
    #[test]
    fn failure_is_counted_by_status_and_id_hash() {
        let (m, r) = fixture();
        m.record_failure(TEST_API_SERVER_ID);
        let f = family(
            &r,
            "apiserver_authorization_config_controller_automatic_reloads_total",
        )
        .unwrap();
        assert_eq!(
            f.help(),
            "[BETA] Total number of automatic reloads of authorization configuration split by status and apiserver identity."
        );
        let m = &f.get_metric()[0];
        assert_eq!(label(m, "status"), "failure");
        assert_eq!(label(m, "apiserver_id_hash"), TEST_API_SERVER_ID_HASH);
        assert_eq!(m.get_counter().value(), 1.0);
    }

    // TestRecordAuthorizationConfigAutomaticReloadSuccess (:72-93) and
    // _StaleMetricCleanup (:121-): only the latest config hash is exposed.
    #[test]
    fn success_exposes_only_the_latest_config_info() {
        let (m, r) = fixture();
        m.record_success(TEST_API_SERVER_ID, "config1");
        m.record_success(TEST_API_SERVER_ID, "config2");
        let f = family(
            &r,
            "apiserver_authorization_config_controller_automatic_reloads_total",
        )
        .unwrap();
        assert_eq!(label(&f.get_metric()[0], "status"), "success");
        assert_eq!(f.get_metric()[0].get_counter().value(), 2.0);
        let info = family(
            &r,
            "apiserver_authorization_config_controller_last_config_info",
        )
        .unwrap();
        assert_eq!(
            info.help(),
            "[ALPHA] Information about the last applied authorization configuration with hash as label, split by apiserver identity."
        );
        assert_eq!(info.get_metric().len(), 1);
        assert_eq!(label(&info.get_metric()[0], "hash"), get_hash("config2"));
        assert_eq!(info.get_metric()[0].get_gauge().value(), 1.0);
    }

    // Regression for the flake: `ReloadableAuthorizer::spawn_reload` (run by
    // authorizer.rs tests in the same process, without LOCK) writes the global
    // hashes at any moment. A foreign writer landing mid-test must not change
    // what this test observes.
    #[test]
    fn foreign_global_writer_does_not_leak_into_isolated_metrics() {
        let (m, r) = fixture();
        m.record_success(TEST_API_SERVER_ID, "config1");
        record_authorization_config_last_config_info("interloper", "other"); // global, foreign
        m.record_success(TEST_API_SERVER_ID, "config2");
        record_authorization_config_last_config_info("interloper", "other"); // global, foreign
        let info = family(
            &r,
            "apiserver_authorization_config_controller_last_config_info",
        )
        .unwrap();
        assert_eq!(label(&info.get_metric()[0], "hash"), get_hash("config2"));
    }

    #[test]
    fn last_config_info_hash_label_is_sha256_of_the_data() {
        let (m, r) = fixture();
        m.record_last_config_info(TEST_API_SERVER_ID, TEST_CONFIG_DATA);
        let info = family(
            &r,
            "apiserver_authorization_config_controller_last_config_info",
        )
        .unwrap();
        assert_eq!(
            label(&info.get_metric()[0], "apiserver_id_hash"),
            TEST_API_SERVER_ID_HASH
        );
        assert_eq!(label(&info.get_metric()[0], "hash"), CONFIG_HASH);
    }

    // TestAuthorizationConfigAutomaticReloadLastTimestampSeconds (:95-).
    #[test]
    fn last_timestamp_gauge_is_named_and_labelled_like_upstream() {
        let (m, r) = fixture();
        m.record_failure(TEST_API_SERVER_ID);
        let f = family(
            &r,
            "apiserver_authorization_config_controller_automatic_reload_last_timestamp_seconds",
        )
        .unwrap();
        assert_eq!(
            f.help(),
            "[BETA] Timestamp of the last automatic reload of authorization configuration split by status and apiserver identity."
        );
        assert!(f.get_metric()[0].get_gauge().value() > 1.6e9);
    }
}
