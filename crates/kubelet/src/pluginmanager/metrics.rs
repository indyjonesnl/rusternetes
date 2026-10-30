//! Port of `pkg/kubelet/pluginmanager/metrics/metrics.go`: the
//! `plugin_manager_total_plugins` gauge, one series per
//! (`socket_path`, `state`), computed from the two states of world at scrape
//! time by a custom collector (`metrics.go:64-103`).
//!
//! Mechanism note: upstream registers a `metrics.StableCollector` with
//! `legacyregistry.CustomMustRegister`; the kubelet's `/metrics` endpoint here
//! serves `rusternetes_common::observability::MetricsRegistry`, a
//! `prometheus::Registry`, so the collector implements
//! `prometheus::core::Collector` and is registered there.

use super::cache::{ActualStateOfWorld, DesiredStateOfWorld};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
use std::collections::BTreeMap;
use std::sync::{Arc, Once};

/// `pluginManagerTotalPlugins` (`metrics.go:29`).
const PLUGIN_MANAGER_TOTAL_PLUGINS: &str = "plugin_manager_total_plugins";

/// `pluginCount` (`metrics.go:46`): state -> socket path -> count. A
/// `BTreeMap` so exposition order is deterministic.
pub type PluginCount = BTreeMap<String, BTreeMap<String, i64>>;

fn add(pc: &mut PluginCount, state: &str, plugin_name: &str) {
    *pc.entry(state.to_string())
        .or_default()
        .entry(plugin_name.to_string())
        .or_insert(0) += 1;
}

/// `totalPluginsCollector` (`metrics.go:64-69`).
pub struct TotalPluginsCollector {
    asw: Arc<ActualStateOfWorld>,
    dsw: Arc<DesiredStateOfWorld>,
    /// `totalPluginsDesc` (`metrics.go:35-42`).
    desc: Desc,
}

impl TotalPluginsCollector {
    pub fn new(asw: Arc<ActualStateOfWorld>, dsw: Arc<DesiredStateOfWorld>) -> Self {
        let desc = Desc::new(
            PLUGIN_MANAGER_TOTAL_PLUGINS.to_string(),
            "Number of plugins in Plugin Manager".to_string(),
            vec!["socket_path".to_string(), "state".to_string()],
            Default::default(),
        )
        .expect("static metric description is valid");
        Self { asw, dsw, desc }
    }

    /// `getPluginCount` (`metrics.go:91-103`).
    pub fn get_plugin_count(&self) -> PluginCount {
        let mut counter = PluginCount::new();
        for registered_plugin in self.asw.get_registered_plugins() {
            add(
                &mut counter,
                "actual_state_of_world",
                &registered_plugin.socket_path,
            );
        }
        for plugin_to_register in self.dsw.get_plugins_to_register() {
            add(
                &mut counter,
                "desired_state_of_world",
                &plugin_to_register.socket_path,
            );
        }
        counter
    }
}

impl Collector for TotalPluginsCollector {
    /// `DescribeWithStability` (`metrics.go:74-76`).
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    /// `CollectWithStability` (`metrics.go:79-89`): one gauge per
    /// (state, socket path).
    fn collect(&self) -> Vec<MetricFamily> {
        let mut family = MetricFamily::default();
        family.set_name(PLUGIN_MANAGER_TOTAL_PLUGINS.to_string());
        family.set_help("Number of plugins in Plugin Manager".to_string());
        family.set_field_type(MetricType::GAUGE);
        let mut metrics = Vec::new();
        for (state_name, plugin_count) in self.get_plugin_count() {
            for (socket_path, count) in plugin_count {
                let mut metric = Metric::default();
                // Label pairs sorted by name, as the exposition format expects.
                let labels = [("socket_path", &socket_path), ("state", &state_name)]
                    .into_iter()
                    .map(|(name, value)| {
                        let mut lp = LabelPair::default();
                        lp.set_name(name.to_string());
                        lp.set_value(value.clone());
                        lp
                    })
                    .collect::<Vec<_>>();
                metric.set_label(labels);
                let mut gauge = Gauge::default();
                gauge.set_value(count as f64);
                metric.set_gauge(gauge);
                metrics.push(metric);
            }
        }
        if metrics.is_empty() {
            return Vec::new();
        }
        family.set_metric(metrics);
        vec![family]
    }
}

static REGISTER_METRICS: Once = Once::new();

/// `Register` (`metrics.go:58-62`): `registerMetrics sync.Once` guards
/// `CustomMustRegister`, so only the first call in the process registers.
pub fn register(
    registry: &prometheus::Registry,
    asw: Arc<ActualStateOfWorld>,
    dsw: Arc<DesiredStateOfWorld>,
) {
    REGISTER_METRICS.call_once(|| {
        registry
            .register(Box::new(TotalPluginsCollector::new(asw, dsw)))
            .expect("plugin_manager_total_plugins registers once");
    });
}
