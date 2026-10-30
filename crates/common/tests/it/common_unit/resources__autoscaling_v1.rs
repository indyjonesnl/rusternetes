//! Unit tests moved from `src/resources/autoscaling_v1.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::autoscaling::CrossVersionObjectReference;
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::{json, Map, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::autoscaling_v1::*;

    /// conversion_test.go:32-: the CPU utilization metric is the percentage.
    #[test]
    fn the_cpu_metric_is_the_percentage() {
        let v2 = json!({"spec": {"maxReplicas": 3, "metrics": [
            {"type": "Resource", "resource": {"name": "cpu",
             "target": {"type": "Utilization", "averageUtilization": 55}}}]}});
        let v1 = hpa_v2_to_v1(&v2);
        assert_eq!(v1["spec"]["targetCPUUtilizationPercentage"], 55);
        assert!(v1["spec"].get("metrics").is_none());
        assert!(v1["metadata"].get("annotations").is_none());
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["metrics"], v2["spec"]["metrics"]);
    }

    #[test]
    fn a_memory_utilization_metric_goes_to_the_annotation_and_back() {
        let v2 = json!({"spec": {"maxReplicas": 3, "metrics": [
            {"type": "Resource", "resource": {"name": "memory",
             "target": {"type": "Utilization", "averageUtilization": 55}}}]}});
        let v1 = hpa_v2_to_v1(&v2);
        assert!(v1["spec"].get("targetCPUUtilizationPercentage").is_none());
        assert!(v1["metadata"]["annotations"][METRIC_SPECS_ANNOTATION].is_string());
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["metrics"], v2["spec"]["metrics"]);
        assert!(back["metadata"].get("annotations").is_none());
    }

    #[test]
    fn object_and_external_metrics_round_trip() {
        let v2 = json!({"spec": {"maxReplicas": 3, "metrics": [
            {"type": "Object", "object": {
                "describedObject": {"kind": "Ingress", "name": "i", "apiVersion": "networking.k8s.io/v1"},
                "metric": {"name": "rps"},
                "target": {"type": "Value", "value": "10"}}},
            {"type": "External", "external": {
                "metric": {"name": "q", "selector": {"matchLabels": {"a": "b"}}},
                "target": {"type": "AverageValue", "averageValue": "5"}}}]}});
        let back = hpa_v1_to_v2(&hpa_v2_to_v1(&v2));
        assert_eq!(back["spec"]["metrics"], v2["spec"]["metrics"]);
    }

    #[test]
    fn behavior_and_conditions_round_trip() {
        let v2 = json!({"spec": {"maxReplicas": 3,
            "behavior": {"scaleUp": {"stabilizationWindowSeconds": 5, "selectPolicy": "Max",
                "policies": [{"type": "Pods", "value": 4, "periodSeconds": 15}], "tolerance": "100m"}}},
          "status": {"currentReplicas": 1, "desiredReplicas": 2,
            "conditions": [{"type": "AbleToScale", "status": "True"}]}});
        let v1 = hpa_v2_to_v1(&v2);
        assert_eq!(
            serde_json::from_str::<Value>(
                v1["metadata"]["annotations"][BEHAVIOR_SPECS_ANNOTATION]
                    .as_str()
                    .unwrap()
            )
            .unwrap()["ScaleUp"]["Policies"][0]["PeriodSeconds"],
            15
        );
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["behavior"], v2["spec"]["behavior"]);
        assert_eq!(back["status"]["conditions"], v2["status"]["conditions"]);
    }

    /// conversion.go:420-433.
    #[test]
    fn an_empty_v1_spec_gets_the_default_cpu_metric() {
        let back = hpa_v1_to_v2(&json!({"spec": {"maxReplicas": 3}}));
        assert_eq!(
            back["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"],
            80
        );
        assert_eq!(back["spec"]["minReplicas"], 1);
    }

    /// An undecodable annotation is ignored (`err == nil` guards).
    #[test]
    fn a_bad_annotation_is_ignored() {
        let v1 = json!({"metadata": {"annotations": {METRIC_SPECS_ANNOTATION: "nope"}},
                        "spec": {"maxReplicas": 3, "targetCPUUtilizationPercentage": 40}});
        let back = hpa_v1_to_v2(&v1);
        assert_eq!(back["spec"]["metrics"].as_array().unwrap().len(), 1);
        assert!(back["metadata"].get("annotations").is_none());
    }
}
