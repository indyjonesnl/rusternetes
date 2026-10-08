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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> EvictionConfig {
        EvictionConfig::default()
    }

    #[test]
    fn defaults_match_upstream_flag_defaults() {
        let c = cfg();
        assert_eq!(c.eviction_limiter_qps, 0.1);
        assert_eq!(c.secondary_eviction_limiter_qps, 0.01);
        assert_eq!(c.large_cluster_threshold, 50);
        assert_eq!(c.unhealthy_zone_threshold, 0.55);
    }

    #[test]
    fn zone_with_no_ready_nodes_is_fully_disrupted() {
        assert_eq!(
            cfg().compute_zone_state(&[false, false]),
            (2, ZoneState::FullDisruption)
        );
        // An empty zone is Normal: `notReadyNodes > 0` is required.
        assert_eq!(cfg().compute_zone_state(&[]), (0, ZoneState::Normal));
    }

    #[test]
    fn partial_disruption_needs_more_than_two_and_the_threshold() {
        // 2 of 3 not ready: ratio 0.66 but not > 2 not-ready nodes.
        assert_eq!(
            cfg().compute_zone_state(&[false, false, true]),
            (2, ZoneState::Normal)
        );
        // 3 of 6 = 0.5 < 0.55.
        assert_eq!(
            cfg().compute_zone_state(&[false, false, false, true, true, true]),
            (3, ZoneState::Normal)
        );
        // 3 of 5 = 0.6 >= 0.55.
        assert_eq!(
            cfg().compute_zone_state(&[false, false, false, true, true]),
            (3, ZoneState::PartialDisruption)
        );
        // Exactly at the threshold counts (`>=`): 11 of 20 = 0.55.
        let mut v = vec![false; 11];
        v.extend(vec![true; 9]);
        assert_eq!(
            cfg().compute_zone_state(&v),
            (11, ZoneState::PartialDisruption)
        );
    }

    #[test]
    fn reduced_qps_stops_small_clusters_and_slows_large_ones() {
        let c = cfg();
        assert_eq!(c.reduced_qps(10), 0.0);
        assert_eq!(c.reduced_qps(50), 0.0);
        assert_eq!(c.reduced_qps(51), 0.01);
    }

    #[test]
    fn zone_key_prefers_beta_labels_and_is_empty_without_topology() {
        use rusternetes_common::types::{ObjectMeta, TypeMeta};
        let mut node = Node {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("n"),
            spec: None,
            status: None,
        };
        assert_eq!(get_zone_key(&node), "");
        let mut labels = std::collections::HashMap::new();
        labels.insert("topology.kubernetes.io/zone".to_string(), "z".to_string());
        labels.insert("topology.kubernetes.io/region".to_string(), "r".to_string());
        node.metadata.labels = Some(labels.clone());
        assert_eq!(get_zone_key(&node), "r:\0:z");
        labels.insert(
            "failure-domain.beta.kubernetes.io/zone".to_string(),
            "bz".to_string(),
        );
        node.metadata.labels = Some(labels);
        assert_eq!(get_zone_key(&node), "r:\0:bz");
    }
}
