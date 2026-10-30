//! Unit tests moved from `src/commands/expose.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::{json, Value};
    pub use std::collections::BTreeMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::expose::*;
    use serde_json::json;

    #[test]
    fn test_extract_selector_deployment() {
        let deployment = json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "nginx",
                "labels": {"app": "nginx"}
            },
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "nginx"
                    }
                },
                "template": {
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx", "ports": [{"containerPort": 80}]}]
                    }
                }
            }
        });

        let selector = extract_selector("deployment", &deployment).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("app".to_string(), "nginx".to_string());
        assert_eq!(selector, expected);
    }

    #[test]
    fn test_extract_selector_pod() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "nginx",
                "labels": {
                    "app": "nginx",
                    "tier": "frontend"
                }
            },
            "spec": {
                "containers": [{"name": "nginx", "image": "nginx"}]
            }
        });

        let selector = extract_selector("pod", &pod).unwrap();
        assert_eq!(selector.get("app").unwrap(), "nginx");
        assert_eq!(selector.get("tier").unwrap(), "frontend");
        assert_eq!(selector.len(), 2);
    }

    #[test]
    fn test_extract_selector_replication_controller() {
        let rc = json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "nginx"},
            "spec": {
                "selector": {
                    "app": "nginx"
                },
                "template": {
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx"}]
                    }
                }
            }
        });

        let selector = extract_selector("rc", &rc).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("app".to_string(), "nginx".to_string());
        assert_eq!(selector, expected);
    }

    #[test]
    fn test_extract_selector_replicaset() {
        let rs = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {"name": "nginx"},
            "spec": {
                "selector": {
                    "matchLabels": {
                        "app": "nginx",
                        "version": "v1"
                    }
                }
            }
        });

        let selector = extract_selector("rs", &rs).unwrap();
        assert_eq!(selector.get("app").unwrap(), "nginx");
        assert_eq!(selector.get("version").unwrap(), "v1");
    }

    #[test]
    fn test_extract_selector_service() {
        let svc = json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "nginx"},
            "spec": {
                "selector": {
                    "app": "nginx"
                },
                "ports": [{"port": 80}]
            }
        });

        let selector = extract_selector("svc", &svc).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert("app".to_string(), "nginx".to_string());
        assert_eq!(selector, expected);
    }

    #[test]
    fn test_extract_selector_missing_labels_fails() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "nginx"},
            "spec": {}
        });

        let result = extract_selector("pod", &pod);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_service_basic() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "nginx".to_string());

        let svc = build_service("nginx", "default", &selector, 80, None, "TCP", None);

        assert_eq!(svc["metadata"]["name"], "nginx");
        assert_eq!(svc["metadata"]["namespace"], "default");
        assert_eq!(svc["spec"]["selector"]["app"], "nginx");
        assert_eq!(svc["spec"]["ports"][0]["port"], 80);
        assert_eq!(svc["spec"]["ports"][0]["targetPort"], 80);
        assert_eq!(svc["spec"]["ports"][0]["protocol"], "TCP");
        assert!(svc["spec"]["type"].is_null());
    }

    #[test]
    fn test_build_service_with_target_port() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "web".to_string());

        let svc = build_service(
            "web-svc",
            "production",
            &selector,
            80,
            Some(8080),
            "TCP",
            None,
        );

        assert_eq!(svc["spec"]["ports"][0]["port"], 80);
        assert_eq!(svc["spec"]["ports"][0]["targetPort"], 8080);
    }

    #[test]
    fn test_build_service_with_type() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "frontend".to_string());

        let svc = build_service(
            "frontend",
            "default",
            &selector,
            443,
            None,
            "TCP",
            Some("NodePort"),
        );

        assert_eq!(svc["spec"]["type"], "NodePort");
    }

    #[test]
    fn test_build_service_udp_protocol() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "dns".to_string());

        let svc = build_service("dns-svc", "kube-system", &selector, 53, None, "UDP", None);

        assert_eq!(svc["spec"]["ports"][0]["protocol"], "UDP");
    }

    #[test]
    fn test_build_service_with_custom_name() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "nginx".to_string());

        let svc = build_service(
            "my-custom-name",
            "default",
            &selector,
            80,
            None,
            "TCP",
            Some("LoadBalancer"),
        );

        assert_eq!(svc["metadata"]["name"], "my-custom-name");
        assert_eq!(svc["spec"]["type"], "LoadBalancer");
    }

    #[test]
    fn test_extract_ports_from_pod() {
        let pod = json!({
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "ports": [{"containerPort": 80}, {"containerPort": 443}]
                }]
            }
        });

        let ports = extract_ports(&pod);
        assert_eq!(ports, vec![80, 443]);
    }

    #[test]
    fn test_extract_ports_from_deployment_template() {
        let deploy = json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "app",
                            "ports": [{"containerPort": 8080}]
                        }]
                    }
                }
            }
        });

        let ports = extract_ports(&deploy);
        assert_eq!(ports, vec![8080]);
    }

    #[test]
    fn test_extract_ports_no_ports() {
        let resource = json!({
            "spec": {
                "containers": [{
                    "name": "app"
                }]
            }
        });

        let ports = extract_ports(&resource);
        assert!(ports.is_empty());
    }

    #[test]
    fn test_resource_path_valid_types() {
        assert_eq!(
            resource_path("pod", "default", "nginx").unwrap(),
            "/api/v1/namespaces/default/pods/nginx"
        );
        assert_eq!(
            resource_path("deploy", "prod", "web").unwrap(),
            "/apis/apps/v1/namespaces/prod/deployments/web"
        );
        assert_eq!(
            resource_path("rc", "default", "app").unwrap(),
            "/api/v1/namespaces/default/replicationcontrollers/app"
        );
        assert_eq!(
            resource_path("rs", "default", "app").unwrap(),
            "/apis/apps/v1/namespaces/default/replicasets/app"
        );
        assert_eq!(
            resource_path("svc", "default", "app").unwrap(),
            "/api/v1/namespaces/default/services/app"
        );
    }

    #[test]
    fn test_resource_path_invalid_type() {
        assert!(resource_path("configmap", "default", "foo").is_err());
    }

    #[test]
    fn test_extract_selector_unsupported_type() {
        let resource = json!({"spec": {}});
        let result = extract_selector("configmap", &resource);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_selector_empty_labels_fails() {
        let pod = json!({
            "metadata": {
                "labels": {}
            }
        });
        let result = extract_selector("pod", &pod);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("selectors"));
    }

    #[test]
    fn test_extract_ports_multiple_containers() {
        let resource = json!({
            "spec": {
                "containers": [
                    {"name": "web", "ports": [{"containerPort": 80}]},
                    {"name": "api", "ports": [{"containerPort": 8080}]}
                ]
            }
        });
        let ports = extract_ports(&resource);
        assert_eq!(ports, vec![80, 8080]);
    }

    #[test]
    fn test_extract_ports_no_containers() {
        let resource = json!({"spec": {}});
        let ports = extract_ports(&resource);
        assert!(ports.is_empty());
    }

    #[test]
    fn test_build_service_multiple_selectors() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "web".to_string());
        selector.insert("version".to_string(), "v2".to_string());

        let svc = build_service(
            "web",
            "staging",
            &selector,
            443,
            Some(8443),
            "TCP",
            Some("ClusterIP"),
        );
        assert_eq!(svc["spec"]["selector"]["app"], "web");
        assert_eq!(svc["spec"]["selector"]["version"], "v2");
        assert_eq!(svc["spec"]["type"], "ClusterIP");
        assert_eq!(svc["metadata"]["namespace"], "staging");
    }

    #[test]
    fn test_resource_path_all_aliases() {
        // Test all aliases resolve correctly
        assert_eq!(
            resource_path("po", "default", "x").unwrap(),
            "/api/v1/namespaces/default/pods/x"
        );
        assert_eq!(
            resource_path("pods", "default", "x").unwrap(),
            "/api/v1/namespaces/default/pods/x"
        );
        assert_eq!(
            resource_path("services", "default", "x").unwrap(),
            "/api/v1/namespaces/default/services/x"
        );
        assert_eq!(
            resource_path("replicationcontrollers", "default", "x").unwrap(),
            "/api/v1/namespaces/default/replicationcontrollers/x"
        );
        assert_eq!(
            resource_path("deployments", "default", "x").unwrap(),
            "/apis/apps/v1/namespaces/default/deployments/x"
        );
        assert_eq!(
            resource_path("replicasets", "default", "x").unwrap(),
            "/apis/apps/v1/namespaces/default/replicasets/x"
        );
    }

    #[test]
    fn test_extract_selector_deployment_missing_match_labels() {
        let deploy = json!({
            "spec": {
                "selector": {
                    "matchExpressions": [{"key": "app", "operator": "In", "values": ["nginx"]}]
                }
            }
        });
        let result = extract_selector("deployment", &deploy);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_ports_empty_ports_array() {
        let resource = json!({
            "spec": {
                "containers": [{
                    "name": "app",
                    "ports": []
                }]
            }
        });
        let ports = extract_ports(&resource);
        assert!(ports.is_empty());
    }

    #[test]
    fn test_build_service_apiversion_and_kind() {
        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), "test".to_string());

        let svc = build_service("test", "default", &selector, 80, None, "TCP", None);
        assert_eq!(svc["apiVersion"], "v1");
        assert_eq!(svc["kind"], "Service");
    }

    #[test]
    fn test_extract_selector_service_missing_selector() {
        let svc = json!({
            "spec": {
                "ports": [{"port": 80}]
            }
        });
        let result = extract_selector("svc", &svc);
        assert!(result.is_err());
    }

    #[test]
    fn test_resource_path_rc_aliases() {
        // Both "replicationcontroller" and "rc" must resolve to the core v1 API path
        assert_eq!(
            resource_path("replicationcontroller", "default", "my-rc").unwrap(),
            "/api/v1/namespaces/default/replicationcontrollers/my-rc"
        );
        assert_eq!(
            resource_path("rc", "my-ns", "web-rc").unwrap(),
            "/api/v1/namespaces/my-ns/replicationcontrollers/web-rc"
        );
        assert_eq!(
            resource_path("replicationcontrollers", "prod", "app").unwrap(),
            "/api/v1/namespaces/prod/replicationcontrollers/app"
        );
    }

    #[test]
    fn test_extract_selector_rc_uses_spec_selector() {
        // RC selectors live at .spec.selector (flat map), not .spec.selector.matchLabels
        let rc = json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {"name": "web-rc"},
            "spec": {
                "selector": {
                    "app": "web",
                    "tier": "frontend"
                },
                "template": {
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx"}]
                    }
                }
            }
        });

        // All three aliases should work
        for alias in &["replicationcontroller", "replicationcontrollers", "rc"] {
            let selector = extract_selector(alias, &rc).unwrap();
            assert_eq!(selector.get("app").unwrap(), "web");
            assert_eq!(selector.get("tier").unwrap(), "frontend");
            assert_eq!(selector.len(), 2);
        }
    }
}
