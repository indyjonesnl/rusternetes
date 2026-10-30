//! Unit tests moved from `src/resources/policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::policy::*;

    #[test]
    fn test_resource_quota_creation() {
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        hard.insert("requests.cpu".to_string(), "4".to_string());
        hard.insert("requests.memory".to_string(), "8Gi".to_string());

        let spec = ResourceQuotaSpec {
            hard: Some(hard),
            scopes: None,
            scope_selector: None,
        };

        let quota = ResourceQuota::new("test-quota", "default", spec);

        assert_eq!(quota.metadata.name, "test-quota");
        assert_eq!(quota.metadata.namespace, Some("default".to_string()));
        assert_eq!(quota.type_meta.api_version, "v1");
        assert!(quota.spec.hard.is_some());
        assert_eq!(quota.spec.hard.as_ref().unwrap().get("pods").unwrap(), "10");
    }

    #[test]
    fn test_limit_range_creation() {
        let mut max = HashMap::new();
        max.insert("memory".to_string(), "1Gi".to_string());
        max.insert("cpu".to_string(), "2".to_string());

        let mut default = HashMap::new();
        default.insert("memory".to_string(), "512Mi".to_string());
        default.insert("cpu".to_string(), "500m".to_string());

        let limit_item = LimitRangeItem {
            item_type: "Container".to_string(),
            max: Some(max),
            min: None,
            default: Some(default),
            default_request: None,
            max_limit_request_ratio: None,
        };

        let spec = LimitRangeSpec {
            limits: vec![limit_item],
        };

        let limit_range = LimitRange::new("test-limits", "default", spec);

        assert_eq!(limit_range.metadata.name, "test-limits");
        assert_eq!(limit_range.type_meta.api_version, "v1");
        assert_eq!(limit_range.spec.limits.len(), 1);
        assert_eq!(limit_range.spec.limits[0].item_type, "Container");
    }

    #[test]
    fn test_priority_class_creation() {
        let pc = PriorityClass::new("high-priority", 1000)
            .with_description("High priority workloads")
            .with_preemption_policy("PreemptLowerPriority");

        assert_eq!(pc.metadata.name, "high-priority");
        assert_eq!(pc.value, 1000);
        assert_eq!(pc.type_meta.api_version, "scheduling.k8s.io/v1");
        assert_eq!(pc.description.as_ref().unwrap(), "High priority workloads");
        assert_eq!(
            pc.preemption_policy.as_ref().unwrap(),
            "PreemptLowerPriority"
        );
    }

    #[test]
    fn test_priority_class_global_default() {
        let pc = PriorityClass::new("default-priority", 0).with_global_default(true);

        assert_eq!(pc.global_default, Some(true));
    }

    #[test]
    fn test_priority_class_serialization() {
        let pc = PriorityClass::new("test", 500);
        let json = serde_json::to_string(&pc).unwrap();

        let deserialized: PriorityClass = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.metadata.name, "test");
        assert_eq!(deserialized.value, 500);
    }

    #[test]
    fn test_resource_quota_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "ResourceQuota",
  "metadata": {
    "name": "compute-quota",
    "namespace": "default"
  },
  "spec": {
    "hard": {
      "pods": "10",
      "requests.cpu": "4",
      "requests.memory": "8Gi"
    }
  }
}"#;

        let quota: ResourceQuota = serde_json::from_str(json).unwrap();
        assert_eq!(quota.metadata.name, "compute-quota");
        assert!(quota.spec.hard.is_some());

        let serialized = serde_json::to_string(&quota).unwrap();
        let _: ResourceQuota = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn test_limit_range_serialization() {
        let json = r#"{
  "apiVersion": "v1",
  "kind": "LimitRange",
  "metadata": {
    "name": "mem-limit",
    "namespace": "default"
  },
  "spec": {
    "limits": [
      {
        "type": "Container",
        "max": {
          "memory": "2Gi"
        },
        "default": {
          "memory": "1Gi"
        }
      }
    ]
  }
}"#;

        let limit_range: LimitRange = serde_json::from_str(json).unwrap();
        assert_eq!(limit_range.metadata.name, "mem-limit");
        assert_eq!(limit_range.spec.limits.len(), 1);

        let serialized = serde_json::to_string(&limit_range).unwrap();
        let _: LimitRange = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn test_pod_disruption_budget_creation() {
        let spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(2)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("test-pdb", "default", spec);

        assert_eq!(pdb.metadata.name, "test-pdb");
        assert_eq!(pdb.metadata.namespace, Some("default".to_string()));
        assert_eq!(pdb.type_meta.api_version, "policy/v1");
        assert!(pdb.spec.min_available.is_some());
    }

    #[test]
    fn test_pod_disruption_budget_int_or_string() {
        // Test integer value
        let int_spec = PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(3)),
            max_unavailable: None,
            selector: Some(LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };

        let pdb = PodDisruptionBudget::new("int-pdb", "default", int_spec);
        match &pdb.spec.min_available {
            Some(IntOrString::Int(n)) => assert_eq!(*n, 3),
            _ => panic!("Expected IntOrString::Int"),
        }

        // Test percentage string value
        let percent_spec = PodDisruptionBudgetSpec {
            min_available: None,
            max_unavailable: Some(IntOrString::String("20%".to_string())),
            selector: Some(LabelSelector {
                match_labels: Some(HashMap::new()),
                match_expressions: None,
            }),
            unhealthy_pod_eviction_policy: None,
        };

        let pdb2 = PodDisruptionBudget::new("percent-pdb", "default", percent_spec);
        match &pdb2.spec.max_unavailable {
            Some(IntOrString::String(s)) => assert_eq!(s, "20%"),
            _ => panic!("Expected IntOrString::String"),
        }
    }

    #[test]
    fn test_pod_disruption_budget_serialization() {
        let json = r#"{
  "apiVersion": "policy/v1",
  "kind": "PodDisruptionBudget",
  "metadata": {
    "name": "web-pdb",
    "namespace": "default"
  },
  "spec": {
    "minAvailable": 2,
    "selector": {
      "matchLabels": {
        "app": "web"
      }
    }
  }
}"#;

        let pdb: PodDisruptionBudget = serde_json::from_str(json).unwrap();
        assert_eq!(pdb.metadata.name, "web-pdb");
        assert!(pdb.spec.min_available.is_some());

        match &pdb.spec.min_available {
            Some(IntOrString::Int(n)) => assert_eq!(*n, 2),
            _ => panic!("Expected IntOrString::Int"),
        }

        let serialized = serde_json::to_string(&pdb).unwrap();
        let _: PodDisruptionBudget = serde_json::from_str(&serialized).unwrap();
    }

    #[test]
    fn test_pod_disruption_budget_with_percentage() {
        let json = r#"{
  "apiVersion": "policy/v1",
  "kind": "PodDisruptionBudget",
  "metadata": {
    "name": "db-pdb",
    "namespace": "production"
  },
  "spec": {
    "maxUnavailable": "25%",
    "selector": {
      "matchLabels": {
        "app": "database"
      }
    }
  }
}"#;

        let pdb: PodDisruptionBudget = serde_json::from_str(json).unwrap();
        assert_eq!(pdb.metadata.name, "db-pdb");

        match &pdb.spec.max_unavailable {
            Some(IntOrString::String(s)) => assert_eq!(s, "25%"),
            _ => panic!("Expected IntOrString::String"),
        }
    }

    // The `should update/patch PodDisruptionBudget status` conformance test sends
    // a `disruptedPods` whose values are metav1.Time. Different clients encode a
    // zero/empty Time differently (string, empty object, or null). The status
    // update path must accept all of them instead of returning a 422.
    #[test]
    fn test_pdb_status_disrupted_pods_string_form() {
        let json = r#"{
            "currentHealthy": 1,
            "desiredHealthy": 1,
            "disruptionsAllowed": 0,
            "expectedPods": 1,
            "disruptedPods": {"pod-0": "2026-05-31T21:33:36Z"}
        }"#;
        let status: PodDisruptionBudgetStatus = serde_json::from_str(json).unwrap();
        let dp = status.disrupted_pods.expect("disruptedPods present");
        assert!(dp.contains_key("pod-0"));
        assert_eq!(
            dp.get("pod-0")
                .unwrap()
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string(),
            "2026-05-31T21:33:36Z"
        );
    }

    #[test]
    fn test_pdb_status_disrupted_pods_object_form() {
        // A zero metav1.Time emitted by an apply configuration serializes as `{}`.
        let json = r#"{
            "currentHealthy": 1,
            "disruptedPods": {"pod-0": {}}
        }"#;
        let status: PodDisruptionBudgetStatus = serde_json::from_str(json).unwrap();
        let dp = status.disrupted_pods.expect("disruptedPods present");
        assert!(dp.contains_key("pod-0"));
    }

    #[test]
    fn test_pdb_status_disrupted_pods_roundtrip_serializes_as_string() {
        let json = r#"{"disruptedPods": {"pod-0": "2026-05-31T21:33:36Z"}}"#;
        let status: PodDisruptionBudgetStatus = serde_json::from_str(json).unwrap();
        let out = serde_json::to_string(&status).unwrap();
        // Values must serialize back as RFC3339 strings, never objects.
        assert!(
            out.contains(r#""pod-0":"2026-05-31T21:33:36Z""#),
            "got: {out}"
        );
    }

    #[test]
    fn test_pdb_status_no_disrupted_pods_omitted() {
        let status = PodDisruptionBudgetStatus {
            current_healthy: 1,
            desired_healthy: 1,
            disruptions_allowed: 0,
            expected_pods: 1,
            observed_generation: None,
            conditions: None,
            disrupted_pods: None,
        };
        let out = serde_json::to_string(&status).unwrap();
        assert!(!out.contains("disruptedPods"), "got: {out}");
    }
}
