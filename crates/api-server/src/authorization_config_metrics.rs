//! `--authorization-config` reload metrics (#3315).
//!
//! Ported from
//! `staging/src/k8s.io/apiserver/pkg/server/options/authorizationconfig/metrics/metrics.go`
//! (and `pkg/util/configmetrics` for the `last_config_info` collector, which
//! reads the current hashes on demand so a new config replaces the old
//! series instead of accumulating).

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry};
use sha2::{Digest, Sha256};
use std::sync::{Arc, LazyLock, Once, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const NAMESPACE: &str = "apiserver";
const SUBSYSTEM: &str = "authorization_config_controller";

fn reloads_total_vec() -> IntCounterVec {
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
}

fn last_timestamp_vec() -> prometheus::GaugeVec {
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
}

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

/// `configmetrics.NewConfigInfoCustomCollector`: emits one gauge of value 1
/// carrying the current hashes, or nothing while none are set. `hashes` is
/// the `AtomicHashProvider` (`SetHashes` / `GetCurrentHashes`).
struct ConfigInfoCollector {
    template: IntGaugeVec,
    hashes: Arc<RwLock<Vec<String>>>,
}

impl Collector for ConfigInfoCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.template.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let hashes = self.hashes.read().unwrap().clone();
        if hashes.is_empty() {
            return vec![];
        }
        let v = last_config_info_vec();
        let labels: Vec<&str> = hashes.iter().map(String::as_str).collect();
        v.with_label_values(&labels).set(1);
        v.collect()
    }
}

/// The metric set of metrics.go as an instance. Upstream keeps these as
/// package vars on `legacyregistry`, which is safe there because `go test`
/// runs each package in its own process; `cargo test` shares one process
/// across every module, so unit tests build a private instance + registry
/// ([`Metrics::new`]) while production uses the process-global one.
pub struct Metrics {
    automatic_reloads_total: IntCounterVec,
    automatic_reload_last_timestamp_seconds: prometheus::GaugeVec,
    hashes: Arc<RwLock<Vec<String>>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            automatic_reloads_total: reloads_total_vec(),
            automatic_reload_last_timestamp_seconds: last_timestamp_vec(),
            hashes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// `RegisterMetrics` (metrics.go:80-91) against `registry`.
    pub fn register_with(&self, registry: &Registry) {
        let _ = registry.register(Box::new(self.automatic_reloads_total.clone()));
        let _ = registry.register(Box::new(
            self.automatic_reload_last_timestamp_seconds.clone(),
        ));
        let _ = registry.register(Box::new(ConfigInfoCollector {
            template: last_config_info_vec(),
            hashes: self.hashes.clone(),
        }));
    }

    /// `ResetMetricsForTest` (metrics.go:93-96).
    pub fn reset(&self) {
        self.automatic_reloads_total.reset();
        self.automatic_reload_last_timestamp_seconds.reset();
    }

    fn set_to_current_time(&self, status: &str, id_hash: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        self.automatic_reload_last_timestamp_seconds
            .with_label_values(&[status, id_hash])
            .set(now);
    }

    /// `RecordAuthorizationConfigAutomaticReloadFailure` (metrics.go:98-102).
    pub fn record_failure(&self, api_server_id: &str) {
        let id_hash = get_hash(api_server_id);
        self.automatic_reloads_total
            .with_label_values(&["failure", &id_hash])
            .inc();
        self.set_to_current_time("failure", &id_hash);
    }

    /// `RecordAuthorizationConfigAutomaticReloadSuccess` (metrics.go:104-111).
    pub fn record_success(&self, api_server_id: &str, authz_config_data: &str) {
        let id_hash = get_hash(api_server_id);
        self.automatic_reloads_total
            .with_label_values(&["success", &id_hash])
            .inc();
        self.set_to_current_time("success", &id_hash);
        self.record_last_config_info(api_server_id, authz_config_data);
    }

    /// `RecordAuthorizationConfigLastConfigInfo` (metrics.go:113-115).
    pub fn record_last_config_info(&self, api_server_id: &str, authz_config_data: &str) {
        *self.hashes.write().unwrap() = vec![get_hash(api_server_id), get_hash(authz_config_data)];
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL: LazyLock<Metrics> = LazyLock::new(Metrics::new);

static REGISTER: Once = Once::new();

/// `RegisterMetrics` (metrics.go:80-91) on the default registry.
pub fn register_metrics() {
    REGISTER.call_once(|| GLOBAL.register_with(prometheus::default_registry()));
}

/// `ResetMetricsForTest` (metrics.go:93-96).
pub fn reset_metrics_for_test() {
    GLOBAL.reset();
}

pub fn record_authorization_config_automatic_reload_failure(api_server_id: &str) {
    GLOBAL.record_failure(api_server_id);
}

pub fn record_authorization_config_automatic_reload_success(
    api_server_id: &str,
    authz_config_data: &str,
) {
    GLOBAL.record_success(api_server_id, authz_config_data);
}

pub fn record_authorization_config_last_config_info(api_server_id: &str, authz_config_data: &str) {
    GLOBAL.record_last_config_info(api_server_id, authz_config_data);
}

/// `getHash` (metrics.go:117-127): empty data hashes to the empty string.
pub fn get_hash(data: &str) -> String {
    if data.is_empty() {
        return String::new();
    }
    format!("sha256:{:x}", Sha256::digest(data.as_bytes()))
}
