//! Port of `pkg/controller/volume/selinuxwarning/metrics.go` (release-1.35):
//! the `selinux_warning_controller_selinux_volume_conflict` gauge.
//!
//! Upstream registers a custom collector once (`registerMetrics sync.Once`)
//! whose `Collect` reads the volume cache at scrape time and emits one `1.0`
//! gauge per active conflict. Here [`register_metrics`] stores the cache in a
//! process-global slot (first caller wins, like the `sync.Once`) and
//! [`gather`] builds the series from it on every scrape.

use super::cache::VolumeCache;
use prometheus::{Encoder, GaugeVec, Opts, Registry, TextEncoder};
use std::sync::{Arc, OnceLock};

const NAME: &str = "selinux_volume_conflict";
const SUBSYSTEM: &str = "selinux_warning_controller";
const HELP: &str = "Conflict between two Pods using the same volume";
const LABELS: [&str; 7] = [
    "property",
    "pod1_namespace",
    "pod1_name",
    "pod1_value",
    "pod2_namespace",
    "pod2_name",
    "pod2_value",
];

static CACHE: OnceLock<Arc<dyn VolumeCache>> = OnceLock::new();

/// `RegisterMetrics`: expose `cache` on the controller-manager `/metrics`.
pub fn register_metrics(cache: Arc<dyn VolumeCache>) {
    let _ = CACHE.set(cache);
}

/// `collector.CollectWithStability` rendered in Prometheus text exposition
/// format. Empty until a controller registered its cache.
pub fn gather() -> String {
    match CACHE.get() {
        Some(cache) => render(cache.as_ref()),
        None => String::new(),
    }
}

/// One gauge series per conflict returned by `cache.get_conflicts()`.
pub(crate) fn render(cache: &dyn VolumeCache) -> String {
    let registry = Registry::new();
    let gauge =
        GaugeVec::new(Opts::new(NAME, HELP).subsystem(SUBSYSTEM), &LABELS).expect("valid opts");
    registry
        .register(Box::new(gauge.clone()))
        .expect("register once");
    for c in cache.get_conflicts() {
        gauge
            .with_label_values(&[
                &c.property_name,
                &c.pod.namespace,
                &c.pod.name,
                &c.property_value,
                &c.other_pod.namespace,
                &c.other_pod.name,
                &c.other_property_value,
            ])
            .set(1.0);
    }
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}

#[cfg(test)]
mod tests {
    use super::super::cache::{
        ObjectName, VolumeLabelCache, SELINUX_CHANGE_POLICY_MOUNT_OPTION as MOUNT,
    };
    use super::super::translator::ControllerSELinuxTranslator;
    use super::*;

    #[test]
    fn conflicts_are_exposed_as_gauges() {
        let cache = VolumeLabelCache::new(ControllerSELinuxTranslator);
        assert_eq!(render(&cache), "");
        cache.add_volume("v", &ObjectName::new("ns", "p1"), ":::s0:c1", MOUNT, "d");
        cache.add_volume("v", &ObjectName::new("ns", "p2"), ":::s0:c2", MOUNT, "d");
        let out = render(&cache);
        assert!(out.contains("# TYPE selinux_warning_controller_selinux_volume_conflict gauge"));
        assert!(out.contains(
            "selinux_warning_controller_selinux_volume_conflict{pod1_name=\"p1\",pod1_namespace=\"ns\",pod1_value=\":::s0:c1\",pod2_name=\"p2\",pod2_namespace=\"ns\",pod2_value=\":::s0:c2\",property=\"SELinuxLabel\"} 1"
        ));
        assert!(out.contains("pod1_name=\"p2\""), "both directions: {out}");
        cache.delete_pod(&ObjectName::new("ns", "p1"));
        assert_eq!(render(&cache), "");
    }
}
