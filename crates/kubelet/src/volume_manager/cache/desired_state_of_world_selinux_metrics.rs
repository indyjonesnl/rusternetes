//! Port of `pkg/kubelet/volumemanager/cache/desired_state_of_wold_selinux_metrics.go`
//! (upstream's filename has the `wold` typo; the module is spelled correctly
//! here).
//!
//! Upstream builds `compbasemetrics.GaugeVec`s (`:25-82`) and
//! `registerSELinuxMetrics` (`:88-98`) registers them once, via `sync.Once`,
//! with the process-global `legacyregistry`. Here the gauges are
//! `prometheus::GaugeVec`s and `legacyregistry` is
//! `prometheus::default_registry()`, which `MetricsRegistry::gather` (served
//! by the kubelet's `/metrics`) also gathers. Names, help text and label sets
//! are upstream's verbatim.

use prometheus::{GaugeVec as PromGaugeVec, Opts};
use std::sync::{LazyLock, Once};

/// Stand-in for `compbasemetrics.GaugeVec`, backed by a `prometheus::GaugeVec`.
pub struct GaugeVec {
    /// Upstream's `GaugeOpts.Name`.
    pub name: &'static str,
    /// Upstream's label names, in `WithLabelValues` order.
    pub labels: &'static [&'static str],
    inner: LazyLock<PromGaugeVec>,
}

fn build(name: &str, help: &str, labels: &[&str]) -> PromGaugeVec {
    PromGaugeVec::new(Opts::new(name, help), labels).expect("valid selinux gauge definition")
}

macro_rules! gauge_vec {
    ($name:expr, $help:expr, $labels:expr) => {
        GaugeVec {
            name: $name,
            labels: $labels,
            inner: LazyLock::new(|| build($name, $help, $labels)),
        }
    };
}

impl GaugeVec {
    /// `WithLabelValues(...).Add(v)`.
    pub fn add(&self, label_values: &[&str], v: f64) {
        self.inner.with_label_values(label_values).add(v);
    }

    /// Current value for a label set. Not something upstream's `GaugeVec`
    /// exposes — Prometheus scrapes instead — but the tests that pin which
    /// metric a path bumps need to read it.
    pub fn value(&self, label_values: &[&str]) -> f64 {
        self.inner.with_label_values(label_values).get()
    }
}

/// Number of errors when kubelet cannot compute SELinux context for a
/// container. Kubelet can't start such a Pod then and it will retry, therefore
/// value of this metric may not represent the actual nr. of containers.
pub static SELINUX_CONTAINER_CONTEXT_ERRORS: GaugeVec = gauge_vec!(
    "volume_manager_selinux_container_errors_total",
    "Number of errors when kubelet cannot compute SELinux context for a container. Kubelet can't start such a Pod then and it will retry, therefore value of this metric may not represent the actual nr. of containers.",
    &["access_mode"]
);

/// Number of errors when kubelet cannot compute SELinux context for a
/// container that are ignored. They will become real errors when
/// SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.
pub static SELINUX_CONTAINER_CONTEXT_WARNINGS: GaugeVec = gauge_vec!(
    "volume_manager_selinux_container_warnings_total",
    "Number of errors when kubelet cannot compute SELinux context for a container that are ignored. They will become real errors when SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.",
    &["access_mode"]
);

/// Number of errors when a Pod defines different SELinux contexts for its
/// containers that use the same volume.
pub static SELINUX_POD_CONTEXT_MISMATCH_ERRORS: GaugeVec = gauge_vec!(
    "volume_manager_selinux_pod_context_mismatch_errors_total",
    "Number of errors when a Pod defines different SELinux contexts for its containers that use the same volume. Kubelet can't start such a Pod then and it will retry, therefore value of this metric may not represent the actual nr. of Pods.",
    &["access_mode"]
);

/// As above, but not errors yet.
pub static SELINUX_POD_CONTEXT_MISMATCH_WARNINGS: GaugeVec = gauge_vec!(
    "volume_manager_selinux_pod_context_mismatch_warnings_total",
    "Number of errors when a Pod defines different SELinux contexts for its containers that use the same volume. They are not errors yet, but they will become real errors when SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.",
    &["access_mode"]
);

/// Number of errors when a Pod uses a volume that is already mounted with a
/// different SELinux context than the Pod needs.
pub static SELINUX_VOLUME_CONTEXT_MISMATCH_ERRORS: GaugeVec = gauge_vec!(
    "volume_manager_selinux_volume_context_mismatch_errors_total",
    "Number of errors when a Pod uses a volume that is already mounted with a different SELinux context than the Pod needs. Kubelet can't start such a Pod then and it will retry, therefore value of this metric may not represent the actual nr. of Pods.",
    &["volume_plugin", "access_mode"]
);

/// As above, but not errors yet.
pub static SELINUX_VOLUME_CONTEXT_MISMATCH_WARNINGS: GaugeVec = gauge_vec!(
    "volume_manager_selinux_volume_context_mismatch_warnings_total",
    "Number of errors when a Pod uses a volume that is already mounted with a different SELinux context than the Pod needs. They are not errors yet, but they will become real errors when SELinuxMountReadWriteOncePod feature is expanded to all volume access modes.",
    &["volume_plugin", "access_mode"]
);

/// Number of volumes whose SELinux context was fine and will be mounted with
/// mount -o context option.
pub static SELINUX_VOLUMES_ADMITTED: GaugeVec = gauge_vec!(
    "volume_manager_selinux_volumes_admitted_total",
    "Number of volumes whose SELinux context was fine and will be mounted with mount -o context option.",
    &["volume_plugin", "access_mode"]
);

static REGISTER_METRICS: Once = Once::new();

/// Port of `registerSELinuxMetrics` (`:88-98`): `registerMetrics.Do(...)`
/// registering the seven gauges with the global registry
/// (`legacyregistry.MustRegister`).
pub fn register_selinux_metrics() {
    REGISTER_METRICS.call_once(|| {
        let registry = prometheus::default_registry();
        for g in [
            &SELINUX_CONTAINER_CONTEXT_ERRORS,
            &SELINUX_CONTAINER_CONTEXT_WARNINGS,
            &SELINUX_POD_CONTEXT_MISMATCH_ERRORS,
            &SELINUX_POD_CONTEXT_MISMATCH_WARNINGS,
            &SELINUX_VOLUME_CONTEXT_MISMATCH_ERRORS,
            &SELINUX_VOLUME_CONTEXT_MISMATCH_WARNINGS,
            &SELINUX_VOLUMES_ADMITTED,
        ] {
            registry
                .register(Box::new(g.inner.clone()))
                .expect("selinux metric registered once");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registered gauges appear on the kubelet's `/metrics` body, with
    /// upstream's name, help and labels.
    #[test]
    fn registered_gauges_are_served_by_kubelet_metrics() {
        register_selinux_metrics();
        register_selinux_metrics(); // sync.Once: second call is a no-op
        SELINUX_VOLUMES_ADMITTED.add(&["kubernetes.io/csi", "ReadWriteOncePod"], 1.0);
        let body = rusternetes_common::observability::MetricsRegistry::new().gather();
        assert!(body.contains("# HELP volume_manager_selinux_volumes_admitted_total Number of volumes whose SELinux context was fine and will be mounted with mount -o context option."));
        assert!(body.contains("volume_manager_selinux_volumes_admitted_total{access_mode=\"ReadWriteOncePod\",volume_plugin=\"kubernetes.io/csi\"}"));
    }
}
