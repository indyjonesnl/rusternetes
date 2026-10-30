//! Unit tests moved from `src/controllers/selinux_warning/metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{Encoder, GaugeVec, Opts, Registry, TextEncoder};
    pub use rusternetes_controller_manager::controllers::selinux_warning::cache::VolumeCache;
    pub use std::sync::{Arc, OnceLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::selinux_warning::cache::{
        ObjectName, VolumeLabelCache, SELINUX_CHANGE_POLICY_MOUNT_OPTION as MOUNT,
    };
    use rusternetes_controller_manager::controllers::selinux_warning::metrics::*;
    use rusternetes_controller_manager::controllers::selinux_warning::translator::ControllerSELinuxTranslator;

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
