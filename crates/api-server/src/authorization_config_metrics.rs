//! `--authorization-config` reload metrics (#3315).
//!
//! Ported from
//! `staging/src/k8s.io/apiserver/pkg/server/options/authorizationconfig/metrics/metrics.go`
//! (and `pkg/util/configmetrics` for the `last_config_info` collector, which
//! reads the current hashes on demand so a new config replaces the old
//! series instead of accumulating).

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{register, IntCounterVec, IntGaugeVec, Opts};
use sha2::{Digest, Sha256};
use std::sync::{LazyLock, Once, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const NAMESPACE: &str = "apiserver";
const SUBSYSTEM: &str = "authorization_config_controller";

static AUTOMATIC_RELOADS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "automatic_reloads_total",
            "[BETA] Total number of automatic reloads of authorization configuration split by status and apiserver identity.",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        &["status", "apiserver_id_hash"],
    )
    .expect("valid metric")
});

static AUTOMATIC_RELOAD_LAST_TIMESTAMP_SECONDS: LazyLock<prometheus::GaugeVec> = LazyLock::new(
    || {
        prometheus::GaugeVec::new(
            Opts::new(
                "automatic_reload_last_timestamp_seconds",
                "[BETA] Timestamp of the last automatic reload of authorization configuration split by status and apiserver identity.",
            )
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
            &["status", "apiserver_id_hash"],
        )
        .expect("valid metric")
    },
);

fn last_config_info_vec() -> IntGaugeVec {
    IntGaugeVec::new(
        Opts::new(
            "last_config_info",
            "[ALPHA] Information about the last applied authorization configuration with hash as label, split by apiserver identity.",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        &["apiserver_id_hash", "hash"],
    )
    .expect("valid metric")
}

/// `configmetrics.AtomicHashProvider` (`SetHashes` / `GetCurrentHashes`).
static CONFIG_HASHES: RwLock<Vec<String>> = RwLock::new(Vec::new());

/// `configmetrics.NewConfigInfoCustomCollector`: emits one gauge of value 1
/// carrying the current hashes, or nothing while none are set.
struct ConfigInfoCollector {
    template: IntGaugeVec,
}

impl Collector for ConfigInfoCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.template.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let hashes = CONFIG_HASHES.read().unwrap().clone();
        if hashes.is_empty() {
            return vec![];
        }
        let v = last_config_info_vec();
        let labels: Vec<&str> = hashes.iter().map(String::as_str).collect();
        v.with_label_values(&labels).set(1);
        v.collect()
    }
}

static REGISTER: Once = Once::new();

/// `RegisterMetrics` (metrics.go:80-91).
pub fn register_metrics() {
    REGISTER.call_once(|| {
        let _ = register(Box::new(AUTOMATIC_RELOADS_TOTAL.clone()));
        let _ = register(Box::new(AUTOMATIC_RELOAD_LAST_TIMESTAMP_SECONDS.clone()));
        let _ = register(Box::new(ConfigInfoCollector {
            template: last_config_info_vec(),
        }));
    });
}

/// `ResetMetricsForTest` (metrics.go:93-96).
pub fn reset_metrics_for_test() {
    AUTOMATIC_RELOADS_TOTAL.reset();
    AUTOMATIC_RELOAD_LAST_TIMESTAMP_SECONDS.reset();
}

fn set_to_current_time(status: &str, id_hash: &str) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    AUTOMATIC_RELOAD_LAST_TIMESTAMP_SECONDS
        .with_label_values(&[status, id_hash])
        .set(now);
}

/// `RecordAuthorizationConfigAutomaticReloadFailure` (metrics.go:98-102).
pub fn record_authorization_config_automatic_reload_failure(api_server_id: &str) {
    let id_hash = get_hash(api_server_id);
    AUTOMATIC_RELOADS_TOTAL
        .with_label_values(&["failure", &id_hash])
        .inc();
    set_to_current_time("failure", &id_hash);
}

/// `RecordAuthorizationConfigAutomaticReloadSuccess` (metrics.go:104-111).
pub fn record_authorization_config_automatic_reload_success(
    api_server_id: &str,
    authz_config_data: &str,
) {
    let id_hash = get_hash(api_server_id);
    AUTOMATIC_RELOADS_TOTAL
        .with_label_values(&["success", &id_hash])
        .inc();
    set_to_current_time("success", &id_hash);
    record_authorization_config_last_config_info(api_server_id, authz_config_data);
}

/// `RecordAuthorizationConfigLastConfigInfo` (metrics.go:113-115).
pub fn record_authorization_config_last_config_info(api_server_id: &str, authz_config_data: &str) {
    *CONFIG_HASHES.write().unwrap() = vec![get_hash(api_server_id), get_hash(authz_config_data)];
}

/// `getHash` (metrics.go:117-127): empty data hashes to the empty string.
pub fn get_hash(data: &str) -> String {
    if data.is_empty() {
        return String::new();
    }
    format!("sha256:{:x}", Sha256::digest(data.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // metrics_test.go:28-29.
    const TEST_API_SERVER_ID: &str = "testAPIServerID";
    const TEST_API_SERVER_ID_HASH: &str =
        "sha256:14f9d63e669337ac6bfda2e2162915ee6a6067743eddd4e5c374b572f951ff37";
    // metrics_test.go's testConfigData (:30-50) and the `hash` label pinned
    // for it (:79).
    const TEST_CONFIG_DATA: &str = "\napiVersion: apiserver.config.k8s.io/v1\nkind: AuthorizationConfiguration\nauthorizers:\n- type: Webhook\n  name: error.example.com\n  webhook:\n    timeout: 5s\n    failurePolicy: Deny\n    subjectAccessReviewVersion: v1\n    matchConditionSubjectAccessReviewVersion: v1\n    authorizedTTL: 1ms\n    unauthorizedTTL: 1ms\n    connectionInfo:\n      type: KubeConfigFile\n      kubeConfigFile: /path/to/kubeconfig\n    matchConditions:\n    - expression: has(request.resourceAttributes)\n    - expression: 'request.resourceAttributes.namespace == \"fail\"'\n    - expression: 'request.resourceAttributes.name == \"error\"'\n";
    const CONFIG_HASH: &str =
        "sha256:2a66c95e5593fc74e6c3dbbb708741361f0690ed45d17e4d4ac0b9c282b6538f";

    /// The tests share process-global metrics; serialise them.
    static LOCK: Mutex<()> = Mutex::new(());

    fn family(name: &str) -> Option<prometheus::proto::MetricFamily> {
        prometheus::gather().into_iter().find(|f| f.name() == name)
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
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        register_metrics();
        reset_metrics_for_test();
        record_authorization_config_automatic_reload_failure(TEST_API_SERVER_ID);
        let f =
            family("apiserver_authorization_config_controller_automatic_reloads_total").unwrap();
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
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        register_metrics();
        reset_metrics_for_test();
        record_authorization_config_automatic_reload_success(TEST_API_SERVER_ID, "config1");
        record_authorization_config_automatic_reload_success(TEST_API_SERVER_ID, "config2");
        let f =
            family("apiserver_authorization_config_controller_automatic_reloads_total").unwrap();
        assert_eq!(label(&f.get_metric()[0], "status"), "success");
        assert_eq!(f.get_metric()[0].get_counter().value(), 2.0);
        let info = family("apiserver_authorization_config_controller_last_config_info").unwrap();
        assert_eq!(
            info.help(),
            "[ALPHA] Information about the last applied authorization configuration with hash as label, split by apiserver identity."
        );
        assert_eq!(info.get_metric().len(), 1);
        assert_eq!(label(&info.get_metric()[0], "hash"), get_hash("config2"));
        assert_eq!(info.get_metric()[0].get_gauge().value(), 1.0);
    }

    #[test]
    fn last_config_info_hash_label_is_sha256_of_the_data() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        register_metrics();
        record_authorization_config_last_config_info(TEST_API_SERVER_ID, TEST_CONFIG_DATA);
        let info = family("apiserver_authorization_config_controller_last_config_info").unwrap();
        assert_eq!(
            label(&info.get_metric()[0], "apiserver_id_hash"),
            TEST_API_SERVER_ID_HASH
        );
        assert_eq!(label(&info.get_metric()[0], "hash"), CONFIG_HASH);
    }

    // TestAuthorizationConfigAutomaticReloadLastTimestampSeconds (:95-).
    #[test]
    fn last_timestamp_gauge_is_named_and_labelled_like_upstream() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        register_metrics();
        reset_metrics_for_test();
        record_authorization_config_automatic_reload_failure(TEST_API_SERVER_ID);
        let f = family(
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
