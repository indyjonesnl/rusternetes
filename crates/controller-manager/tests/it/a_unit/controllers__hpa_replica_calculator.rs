//! Unit tests moved from `src/controllers/hpa_replica_calculator.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_controller_manager::controllers::hpa_metrics_client::PodMetricsInfo;
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::hpa_metrics_client::FakeMetricsClient;
    use rusternetes_controller_manager::controllers::hpa_replica_calculator::*;

    #[test]
    fn pods_metric_doubles_replicas() {
        // 2 pods @ 200 each, target 100/pod -> ratio 2.0 -> 2*2=4
        let info = FakeMetricsClient::pods_info(&[("p1", 200, None), ("p2", 200, None)]);
        let (replicas, avg) = get_metric_replicas(&info, 2, 100).unwrap();
        assert_eq!(replicas, 4);
        assert_eq!(avg, 200);
    }

    #[test]
    fn within_tolerance_keeps_current() {
        // avg 105 vs target 100 -> ratio 1.05 (<=0.1 band) -> unchanged
        let info = FakeMetricsClient::pods_info(&[("p1", 105, None)]);
        let (replicas, _) = get_metric_replicas(&info, 3, 100).unwrap();
        assert_eq!(replicas, 3);
    }

    #[test]
    fn empty_pods_errors() {
        let info = PodMetricsInfo::new();
        assert!(get_metric_replicas(&info, 2, 100).is_err());
        assert!(get_resource_replicas(&info, 2, 80).is_err());
    }

    #[test]
    fn external_sum_over_per_pod_target() {
        // queue depth 90 total, target 30/pod -> ceil(90/30)=3
        let (replicas, sum) = get_external_metric_replicas(&[90], 30).unwrap();
        assert_eq!(replicas, 3);
        assert_eq!(sum, 90);
    }

    #[test]
    fn object_ratio() {
        // value 200, target 100, current 2 -> 4
        let (replicas, _) = get_object_metric_replicas(200, 2, 100).unwrap();
        assert_eq!(replicas, 4);
    }
}
