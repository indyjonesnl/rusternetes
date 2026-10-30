//! Unit tests moved from `src/commands/autoscale.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::{json, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::autoscale::*;

    #[test]
    fn test_build_hpa_v1() {
        let hpa = build_hpa_v1(
            "nginx",
            "default",
            "apps/v1",
            "Deployment",
            "nginx",
            Some(2),
            5,
            Some(80),
        );
        assert_eq!(hpa["spec"]["maxReplicas"], 5);
        assert_eq!(hpa["spec"]["minReplicas"], 2);
        assert_eq!(hpa["spec"]["targetCPUUtilizationPercentage"], 80);
        assert_eq!(hpa["spec"]["scaleTargetRef"]["kind"], "Deployment");
        assert_eq!(hpa["spec"]["scaleTargetRef"]["name"], "nginx");
        assert_eq!(hpa["apiVersion"], "autoscaling/v1");
    }

    #[test]
    fn test_build_hpa_v1_no_min() {
        let hpa = build_hpa_v1(
            "nginx",
            "default",
            "apps/v1",
            "Deployment",
            "nginx",
            None,
            10,
            None,
        );
        assert_eq!(hpa["spec"]["maxReplicas"], 10);
        assert!(hpa["spec"]["minReplicas"].is_null());
        assert!(hpa["spec"]["targetCPUUtilizationPercentage"].is_null());
    }

    #[test]
    fn test_hpa_v2_construction() {
        // Build an HPA v2 spec the same way the execute function does
        let name = "web";
        let namespace = "production";
        let api_version = "apps/v1";
        let kind = "Deployment";
        let min = Some(3);
        let max = 10;
        let cpu_percent = Some(75);

        let mut hpa = json!({
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {
                "name": name,
                "namespace": namespace,
            },
            "spec": {
                "scaleTargetRef": {
                    "apiVersion": api_version,
                    "kind": kind,
                    "name": name,
                },
                "maxReplicas": max,
            }
        });

        if let Some(min_val) = min {
            if min_val > 0 {
                hpa["spec"]["minReplicas"] = json!(min_val);
            }
        }

        if let Some(cpu) = cpu_percent {
            if cpu > 0 {
                hpa["spec"]["metrics"] = json!([{
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "target": {
                            "type": "Utilization",
                            "averageUtilization": cpu,
                        }
                    }
                }]);
            }
        }

        assert_eq!(hpa["apiVersion"], "autoscaling/v2");
        assert_eq!(hpa["kind"], "HorizontalPodAutoscaler");
        assert_eq!(hpa["metadata"]["name"], "web");
        assert_eq!(hpa["metadata"]["namespace"], "production");
        assert_eq!(hpa["spec"]["scaleTargetRef"]["kind"], "Deployment");
        assert_eq!(hpa["spec"]["scaleTargetRef"]["apiVersion"], "apps/v1");
        assert_eq!(hpa["spec"]["maxReplicas"], 10);
        assert_eq!(hpa["spec"]["minReplicas"], 3);
        assert_eq!(hpa["spec"]["metrics"][0]["type"], "Resource");
        assert_eq!(hpa["spec"]["metrics"][0]["resource"]["name"], "cpu");
        assert_eq!(
            hpa["spec"]["metrics"][0]["resource"]["target"]["averageUtilization"],
            75
        );
    }

    #[test]
    fn test_hpa_resource_type_mapping() {
        // Verify the resource type -> (apiVersion, kind) mapping
        let cases = vec![
            ("deployment", "apps/v1", "Deployment"),
            ("deploy", "apps/v1", "Deployment"),
            ("rs", "apps/v1", "ReplicaSet"),
            ("sts", "apps/v1", "StatefulSet"),
            ("rc", "v1", "ReplicationController"),
        ];
        for (input, expected_api, expected_kind) in cases {
            let (api_version, kind) = match input {
                "deployment" | "deployments" | "deploy" => ("apps/v1", "Deployment"),
                "replicaset" | "replicasets" | "rs" => ("apps/v1", "ReplicaSet"),
                "statefulset" | "statefulsets" | "sts" => ("apps/v1", "StatefulSet"),
                "replicationcontroller" | "replicationcontrollers" | "rc" => {
                    ("v1", "ReplicationController")
                }
                _ => panic!("unexpected"),
            };
            assert_eq!(api_version, expected_api, "for input '{}'", input);
            assert_eq!(kind, expected_kind, "for input '{}'", input);
        }
    }

    #[test]
    fn test_hpa_v1_zero_min_not_set() {
        // min of 0 should not set minReplicas
        let hpa = build_hpa_v1(
            "app",
            "default",
            "apps/v1",
            "Deployment",
            "app",
            Some(0),
            5,
            None,
        );
        assert!(hpa["spec"]["minReplicas"].is_null());
    }

    #[test]
    fn test_hpa_v1_zero_cpu_not_set() {
        let hpa = build_hpa_v1(
            "app",
            "default",
            "apps/v1",
            "Deployment",
            "app",
            None,
            5,
            Some(0),
        );
        assert!(hpa["spec"]["targetCPUUtilizationPercentage"].is_null());
    }

    #[test]
    fn test_hpa_unsupported_resource_type() {
        // Verify that unsupported types like "configmap" don't match the autoscale mapping
        let resource_type = "configmap";
        let result = match resource_type {
            "deployment" | "deployments" | "deploy" => Ok(("apps/v1", "Deployment")),
            "replicaset" | "replicasets" | "rs" => Ok(("apps/v1", "ReplicaSet")),
            "statefulset" | "statefulsets" | "sts" => Ok(("apps/v1", "StatefulSet")),
            "replicationcontroller" | "replicationcontrollers" | "rc" => {
                Ok(("v1", "ReplicationController"))
            }
            _ => Err(format!("cannot autoscale a {}", resource_type)),
        };
        assert!(result.is_err());
    }
}
