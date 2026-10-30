//! Unit tests moved from `src/pluginmanager/metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::core::{Collector, Desc};
    pub use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
    pub use rusternetes_kubelet::pluginmanager::cache::{ActualStateOfWorld, DesiredStateOfWorld};
    pub use std::collections::BTreeMap;
    pub use std::sync::{Arc, Once};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::pluginmanager::cache::PluginInfo;
    use rusternetes_kubelet::pluginmanager::metrics::*;
    use std::time::SystemTime;

    fn fake_plugin() -> PluginInfo {
        PluginInfo {
            socket_path: "fake/path/plugin.sock".to_string(),
            timestamp: SystemTime::now(),
            uuid: String::new(),
            handler: None,
            name: String::new(),
            endpoint: String::new(),
        }
    }

    fn states() -> (Arc<ActualStateOfWorld>, Arc<DesiredStateOfWorld>) {
        let dsw = Arc::new(DesiredStateOfWorld::new());
        let asw = Arc::new(ActualStateOfWorld::new());
        let p = fake_plugin();
        dsw.add_or_update_plugin(&p.socket_path).unwrap();
        asw.add_plugin(p).unwrap();
        (asw, dsw)
    }

    /// `TestMetricCollection` (`metrics_test.go:30-83`).
    #[test]
    fn metric_collection() {
        let (asw, dsw) = states();
        let collector = TotalPluginsCollector::new(asw, dsw);
        let count = collector.get_plugin_count();
        assert_eq!(count.len(), 2);
        assert_eq!(count["desired_state_of_world"]["fake/path/plugin.sock"], 1);
        assert_eq!(count["actual_state_of_world"]["fake/path/plugin.sock"], 1);
    }

    /// The collector exposes `plugin_manager_total_plugins{socket_path,state}`
    /// through a real registry, and `register` is once-only (`metrics.go:59`).
    #[test]
    fn register_exposes_gauge_once() {
        let (asw, dsw) = states();
        let registry = prometheus::Registry::new();
        register(&registry, asw.clone(), dsw.clone());
        // The second call is a no-op rather than an AlreadyReg panic.
        register(&registry, asw, dsw);
        let out = {
            use prometheus::Encoder;
            let mut buf = Vec::new();
            prometheus::TextEncoder::new()
                .encode(&registry.gather(), &mut buf)
                .unwrap();
            String::from_utf8(buf).unwrap()
        };
        for state in ["actual_state_of_world", "desired_state_of_world"] {
            let line = format!(
                "plugin_manager_total_plugins{{socket_path=\"fake/path/plugin.sock\",state=\"{state}\"}} 1"
            );
            assert!(out.contains(&line), "missing {line} in:\n{out}");
        }
        assert!(
            out.contains("# HELP plugin_manager_total_plugins Number of plugins in Plugin Manager")
        );
    }
}
