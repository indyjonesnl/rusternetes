//! Unit tests moved from `src/controllers/node_lifecycle_zone.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::Node;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::node_lifecycle_zone::*;

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
