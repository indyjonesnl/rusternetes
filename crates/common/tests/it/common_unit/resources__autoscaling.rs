//! Unit tests moved from `src/resources/autoscaling.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::autoscaling::*;

    #[test]
    fn test_hpa_creation() {
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(2),
            max_replicas: 10,
            metrics: Some(vec![MetricSpec {
                metric_type: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        target_type: "Utilization".to_string(),
                        value: None,
                        average_value: None,
                        average_utilization: Some(80),
                    },
                }),
                pods: None,
                object: None,
                external: None,
                container_resource: None,
            }]),
            behavior: None,
        };

        let hpa = HorizontalPodAutoscaler::new("web-hpa", "default", spec);

        assert_eq!(hpa.metadata.name, "web-hpa");
        assert_eq!(hpa.type_meta.api_version, "autoscaling/v2");
        assert_eq!(hpa.spec.max_replicas, 10);
    }

    #[test]
    fn test_vpa_creation() {
        let spec = VerticalPodAutoscalerSpec {
            target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "api-server".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            update_policy: Some(PodUpdatePolicy {
                update_mode: Some("Auto".to_string()),
            }),
            resource_policy: None,
            recommenders: None,
        };

        let vpa = VerticalPodAutoscaler::new("api-vpa", "default", spec);

        assert_eq!(vpa.metadata.name, "api-vpa");
        assert_eq!(vpa.type_meta.api_version, "autoscaling.k8s.io/v1");
        assert_eq!(vpa.spec.target_ref.kind, "Deployment");
    }

    #[test]
    fn test_hpa_serialization() {
        let json = r#"{
  "apiVersion": "autoscaling/v2",
  "kind": "HorizontalPodAutoscaler",
  "metadata": {
    "name": "test-hpa",
    "namespace": "default"
  },
  "spec": {
    "scaleTargetRef": {
      "kind": "Deployment",
      "name": "test-app"
    },
    "minReplicas": 1,
    "maxReplicas": 5,
    "metrics": [
      {
        "type": "Resource",
        "resource": {
          "name": "cpu",
          "target": {
            "type": "Utilization",
            "averageUtilization": 70
          }
        }
      }
    ]
  }
}"#;

        let hpa: HorizontalPodAutoscaler = serde_json::from_str(json).unwrap();
        assert_eq!(hpa.metadata.name, "test-hpa");
        assert_eq!(hpa.spec.max_replicas, 5);

        let serialized = serde_json::to_string(&hpa).unwrap();
        let _: HorizontalPodAutoscaler = serde_json::from_str(&serialized).unwrap();
    }
}
