//! Unit tests moved from `src/volume_manager/cache/desired_state_of_world_selinux_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{GaugeVec as PromGaugeVec, Opts};
    pub use std::sync::{LazyLock, Once};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_manager::cache::desired_state_of_world_selinux_metrics::*;

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
