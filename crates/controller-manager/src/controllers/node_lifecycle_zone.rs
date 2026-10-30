//! Zone state and eviction-rate policy of the node lifecycle controller.
//!
//! Ports, from `pkg/controller/nodelifecycle/node_lifecycle_controller.go`
//! (release-1.35): `ZoneState` (:114-123), the limiter knobs on `Controller`
//! (:296-299), `HealthyQPSFunc` / `ReducedQPSFunc` (:1210-1222),
//! `ComputeZoneState` (:1281-1298), and
//! `nodetopology.GetZoneKey`
//! (staging/src/k8s.io/component-helpers/node/topology/helpers.go:31-58).
//!
//! Upstream's own `node_lifecycle_controller_test.go` carries no direct
//! `ComputeZoneState` test in release-1.35 (the zone cases of
//! `TestMonitorNodeHealth` were dropped); the tests below pin the documented
//! behaviour of the functions as written at the cited lines.

use rusternetes_common::resources::Node;

/// `ZoneState` (:114-123).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZoneState {
    Initial,
    Normal,
    FullDisruption,
    PartialDisruption,
}

/// Eviction rate-limiting knobs (`evictionLimiterQPS`,
/// `secondaryEvictionLimiterQPS`, `largeClusterThreshold`,
/// `unhealthyZoneThreshold`, :296-299). Defaults come from
/// `pkg/controller/apis/config/v1alpha1/defaults.go`
/// (`--node-eviction-rate` 0.1, `--secondary-node-eviction-rate` 0.01,
/// `--large-cluster-size-threshold` 50, `--unhealthy-zone-threshold` 0.55).
#[derive(Clone, Copy, Debug)]
pub struct EvictionConfig {
    pub eviction_limiter_qps: f32,
    pub secondary_eviction_limiter_qps: f32,
    pub large_cluster_threshold: i32,
    pub unhealthy_zone_threshold: f32,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            eviction_limiter_qps: 0.1,
            secondary_eviction_limiter_qps: 0.01,
            large_cluster_threshold: 50,
            unhealthy_zone_threshold: 0.55,
        }
    }
}

impl EvictionConfig {
    /// `ReducedQPSFunc` (:1216): when the cluster is large make evictions
    /// slower, if it is small stop evictions altogether. This is
    /// `enterPartialDisruptionFunc` (:357); `enterFullDisruptionFunc` is
    /// `HealthyQPSFunc` (:358), i.e. `eviction_limiter_qps`.
    pub fn reduced_qps(&self, node_num: usize) -> f32 {
        if node_num as i64 > i64::from(self.large_cluster_threshold) {
            self.secondary_eviction_limiter_qps
        } else {
            0.0
        }
    }

    /// `ComputeZoneState` (:1281). `ready[i]` is whether node i's Ready
    /// condition is present and `True`. Returns the not-ready node count and
    /// the zone state:
    /// - fully disrupted if there are no Ready nodes,
    /// - partially disrupted if more than 2 nodes and at least
    ///   `unhealthy_zone_threshold` of them are not Ready,
    /// - normal otherwise.
    pub fn compute_zone_state(&self, ready: &[bool]) -> (usize, ZoneState) {
        let ready_nodes = ready.iter().filter(|r| **r).count();
        let not_ready = ready.len() - ready_nodes;
        if ready_nodes == 0 && not_ready > 0 {
            (not_ready, ZoneState::FullDisruption)
        } else if not_ready > 2
            && not_ready as f32 / (not_ready + ready_nodes) as f32 >= self.unhealthy_zone_threshold
        {
            (not_ready, ZoneState::PartialDisruption)
        } else {
            (not_ready, ZoneState::Normal)
        }
    }
}

/// `nodetopology.GetZoneKey`.
pub fn get_zone_key(node: &Node) -> String {
    let Some(labels) = node.metadata.labels.as_ref() else {
        return String::new();
    };
    // "failure-domain.beta..." names are deprecated but still win over the GA
    // ones (helpers.go:37-48).
    let zone = labels
        .get("failure-domain.beta.kubernetes.io/zone")
        .or_else(|| labels.get("topology.kubernetes.io/zone"))
        .cloned()
        .unwrap_or_default();
    let region = labels
        .get("failure-domain.beta.kubernetes.io/region")
        .or_else(|| labels.get("topology.kubernetes.io/region"))
        .cloned()
        .unwrap_or_default();
    if region.is_empty() && zone.is_empty() {
        return String::new();
    }
    // The null character is included in case region or zone has a colon.
    format!("{region}:\0:{zone}")
}
