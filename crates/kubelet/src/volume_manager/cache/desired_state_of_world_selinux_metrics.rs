//! Port of `pkg/kubelet/volumemanager/cache/desired_state_of_wold_selinux_metrics.go`
//! (upstream's filename has the `wold` typo; the module is spelled correctly
//! here).
//!
//! **Deviation, flagged.** Upstream builds `compbasemetrics.GaugeVec`s and
//! registers them with `legacyregistry`. This crate has no Prometheus
//! registry, so the gauges are ported as in-process label-keyed counters with
//! upstream's names, help text and label sets preserved. The *mechanism* that
//! matters to `DesiredStateOfWorld` — which of the warning/error pair gets
//! bumped, and whether the error is consumed or propagated — is in
//! `handle_selinux_metric_error`, and is ported exactly. Wiring these into a
//! real `/metrics` endpoint is a follow-up, not a behaviour change.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// A gauge with a fixed label set, keyed by label values. The stand-in for
/// `compbasemetrics.GaugeVec`.
pub struct GaugeVec {
    /// Upstream's `GaugeOpts.Name`.
    pub name: &'static str,
    /// Upstream's label names, in `WithLabelValues` order.
    pub labels: &'static [&'static str],
    values: OnceLock<Mutex<HashMap<Vec<String>, f64>>>,
}

impl GaugeVec {
    const fn new(name: &'static str, labels: &'static [&'static str]) -> Self {
        Self {
            name,
            labels,
            values: OnceLock::new(),
        }
    }

    fn map(&self) -> &Mutex<HashMap<Vec<String>, f64>> {
        self.values.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// `WithLabelValues(...).Add(v)`.
    pub fn add(&self, label_values: &[&str], v: f64) {
        let key: Vec<String> = label_values.iter().map(|s| (*s).to_string()).collect();
        *self
            .map()
            .lock()
            .expect("gauge mutex")
            .entry(key)
            .or_insert(0.0) += v;
    }

    /// Current value for a label set. Not something upstream's `GaugeVec`
    /// exposes — Prometheus scrapes instead — but the tests that pin which
    /// metric a path bumps need to read it.
    pub fn value(&self, label_values: &[&str]) -> f64 {
        let key: Vec<String> = label_values.iter().map(|s| (*s).to_string()).collect();
        self.map()
            .lock()
            .expect("gauge mutex")
            .get(&key)
            .copied()
            .unwrap_or(0.0)
    }
}

/// Number of errors when kubelet cannot compute SELinux context for a
/// container. Kubelet can't start such a Pod then and it will retry, therefore
/// value of this metric may not represent the actual nr. of containers.
pub static SELINUX_CONTAINER_CONTEXT_ERRORS: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_container_errors_total",
    &["access_mode"],
);

/// Number of errors when kubelet cannot compute SELinux context for a
/// container that are ignored. They will become real errors when
/// SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.
pub static SELINUX_CONTAINER_CONTEXT_WARNINGS: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_container_warnings_total",
    &["access_mode"],
);

/// Number of errors when a Pod defines different SELinux contexts for its
/// containers that use the same volume.
pub static SELINUX_POD_CONTEXT_MISMATCH_ERRORS: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_pod_context_mismatch_errors_total",
    &["access_mode"],
);

/// As above, but not errors yet.
pub static SELINUX_POD_CONTEXT_MISMATCH_WARNINGS: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_pod_context_mismatch_warnings_total",
    &["access_mode"],
);

/// Number of errors when a Pod uses a volume that is already mounted with a
/// different SELinux context than the Pod needs.
pub static SELINUX_VOLUME_CONTEXT_MISMATCH_ERRORS: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_volume_context_mismatch_errors_total",
    &["volume_plugin", "access_mode"],
);

/// As above, but not errors yet.
pub static SELINUX_VOLUME_CONTEXT_MISMATCH_WARNINGS: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_volume_context_mismatch_warnings_total",
    &["volume_plugin", "access_mode"],
);

/// Number of volumes whose SELinux context was fine and will be mounted with
/// mount -o context option.
pub static SELINUX_VOLUMES_ADMITTED: GaugeVec = GaugeVec::new(
    "volume_manager_selinux_volumes_admitted_total",
    &["volume_plugin", "access_mode"],
);

/// Port of `registerSELinuxMetrics` (`:88-98`), whose `sync.Once` registers the
/// seven gauges with `legacyregistry`. With no registry to register into this
/// is a no-op, kept so `DesiredStateOfWorld::new` still has upstream's
/// gate-conditional call at the point upstream has it.
pub fn register_selinux_metrics() {}
