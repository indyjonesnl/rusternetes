//! Prometheus series for the node lifecycle controller.
//!
//! Port of `pkg/controller/nodelifecycle/metrics.go` (release-1.35):
//! subsystem `node_collector`, series keyed by the `zone` label. Upstream
//! registers them in `legacyregistry` once (`registerMetrics sync.Once`);
//! here a process-global [`Registry`] plays that role (same pattern as
//! [`super::cidrset_metrics`]) and is served by `gather_metrics`.
//!
//! Only the four series the controller sets today are ported:
//! `zone_health`, `zone_size`, `unhealthy_nodes_in_zone`, `evictions_total`.
//! The two `update_*_health_duration_seconds` histograms belong to
//! `tryUpdateNodeHealth`, which this controller does not have.

use prometheus::{Encoder, GaugeVec, IntCounterVec, Opts, Registry, TextEncoder};
use std::sync::LazyLock;

const SUBSYSTEM: &str = "node_collector";
const LABEL: &[&str] = &["zone"];

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).subsystem(SUBSYSTEM)
}

struct Series {
    registry: Registry,
    zone_health: GaugeVec,
    zone_size: GaugeVec,
    unhealthy_nodes: GaugeVec,
    evictions_total: IntCounterVec,
}

static SERIES: LazyLock<Series> = LazyLock::new(|| {
    let registry = Registry::new();
    let zone_health = GaugeVec::new(
        opts(
            "zone_health",
            "Gauge measuring percentage of healthy nodes per zone.",
        ),
        LABEL,
    )
    .expect("valid opts");
    let zone_size = GaugeVec::new(
        opts(
            "zone_size",
            "Gauge measuring number of registered Nodes per zones.",
        ),
        LABEL,
    )
    .expect("valid opts");
    let unhealthy_nodes = GaugeVec::new(
        opts(
            "unhealthy_nodes_in_zone",
            "Gauge measuring number of not Ready Nodes per zones.",
        ),
        LABEL,
    )
    .expect("valid opts");
    let evictions_total = IntCounterVec::new(
        opts(
            "evictions_total",
            "Number of Node evictions that happened since current instance of NodeController started.",
        ),
        LABEL,
    )
    .expect("valid opts");
    registry
        .register(Box::new(zone_health.clone()))
        .expect("register once");
    registry
        .register(Box::new(zone_size.clone()))
        .expect("register once");
    registry
        .register(Box::new(unhealthy_nodes.clone()))
        .expect("register once");
    registry
        .register(Box::new(evictions_total.clone()))
        .expect("register once");
    Series {
        registry,
        zone_health,
        zone_size,
        unhealthy_nodes,
        evictions_total,
    }
});

/// `zoneSize/zoneHealth/unhealthyNodes.WithLabelValues(zone).Set(..)`
/// (`node_lifecycle_controller.go:1001-1004`).
pub fn set_zone_stats(zone: &str, size: usize, unhealthy: usize) {
    let s = &*SERIES;
    s.zone_size.with_label_values(&[zone]).set(size as f64);
    s.zone_health
        .with_label_values(&[zone])
        .set(100.0 * (size - unhealthy) as f64 / size as f64);
    s.unhealthy_nodes
        .with_label_values(&[zone])
        .set(unhealthy as f64);
}

/// A zone that no longer has nodes (`:1018-1020`).
pub fn clear_zone(zone: &str) {
    let s = &*SERIES;
    s.zone_size.with_label_values(&[zone]).set(0.0);
    s.zone_health.with_label_values(&[zone]).set(100.0);
    s.unhealthy_nodes.with_label_values(&[zone]).set(0.0);
}

/// `evictionsTotal.WithLabelValues(zone).Inc()` (`:657`).
pub fn inc_evictions(zone: &str) {
    SERIES.evictions_total.with_label_values(&[zone]).inc();
}

/// `evictionsTotal.WithLabelValues(zone).Add(0)` (`:1235`): initialise the
/// series for a new zone.
pub fn init_evictions(zone: &str) {
    SERIES.evictions_total.with_label_values(&[zone]).inc_by(0);
}

#[cfg(test)]
pub fn zone_size(zone: &str) -> Option<f64> {
    SERIES
        .zone_size
        .get_metric_with_label_values(&[zone])
        .ok()
        .map(|g| g.get())
}

#[cfg(test)]
pub fn zone_health(zone: &str) -> Option<f64> {
    SERIES
        .zone_health
        .get_metric_with_label_values(&[zone])
        .ok()
        .map(|g| g.get())
}

#[cfg(test)]
pub fn unhealthy_nodes(zone: &str) -> Option<f64> {
    SERIES
        .unhealthy_nodes
        .get_metric_with_label_values(&[zone])
        .ok()
        .map(|g| g.get())
}

#[cfg(test)]
pub fn evictions_total(zone: &str) -> u64 {
    SERIES.evictions_total.with_label_values(&[zone]).get()
}

/// The registry in Prometheus text exposition format.
pub fn gather() -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&SERIES.registry.gather(), &mut buf)
        .expect("text encode");
    String::from_utf8(buf).expect("utf8")
}
