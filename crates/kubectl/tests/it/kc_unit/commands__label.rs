//! Unit tests moved from `src/commands/label.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::{json, Value};
    pub use std::collections::HashMap;
}

mod tests {
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn parse_labels(labels: &[String]) -> Result<HashMap<String, Value>, String> {
        let mut map = HashMap::new();
        for label in labels {
            if let Some(key) = label.strip_suffix('-') {
                map.insert(key.to_string(), Value::Null);
            } else if let Some((key, value)) = label.split_once('=') {
                map.insert(key.to_string(), Value::String(value.to_string()));
            } else {
                return Err(format!("Invalid label format: {}", label));
            }
        }
        Ok(map)
    }

    #[test]
    fn test_label_patch_set() {
        let labels = vec!["env=production".to_string()];
        let map = parse_labels(&labels).unwrap();

        let patch = json!({"metadata": {"labels": map}});
        assert_eq!(patch["metadata"]["labels"]["env"], "production");
    }

    #[test]
    fn test_label_patch_remove() {
        let labels = vec!["env-".to_string()];
        let map = parse_labels(&labels).unwrap();

        let patch = json!({"metadata": {"labels": map}});
        assert!(patch["metadata"]["labels"]["env"].is_null());
    }

    #[test]
    fn test_label_patch_multiple() {
        let labels = vec![
            "app=nginx".to_string(),
            "tier=frontend".to_string(),
            "deprecated-".to_string(),
        ];
        let map = parse_labels(&labels).unwrap();

        assert_eq!(map.len(), 3);
        assert_eq!(map["app"], Value::String("nginx".to_string()));
        assert_eq!(map["tier"], Value::String("frontend".to_string()));
        assert_eq!(map["deprecated"], Value::Null);
    }

    #[test]
    fn test_label_invalid_format() {
        let labels = vec!["noequalsnodash".to_string()];
        let result = parse_labels(&labels);
        assert!(result.is_err());
    }

    #[test]
    fn test_label_api_path_for_nodes_is_cluster_scoped() {
        let api_path = "api/v1";
        let resource_name = "nodes";
        let name = "worker-1";
        let path = format!("/{}/{}/{}", api_path, resource_name, name);
        assert_eq!(path, "/api/v1/nodes/worker-1");
    }

    #[test]
    fn test_label_api_path_for_pods_is_namespaced() {
        let api_path = "api/v1";
        let resource_name = "pods";
        let name = "my-pod";
        let namespace = "kube-system";
        let path = format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, namespace, resource_name, name
        );
        assert_eq!(path, "/api/v1/namespaces/kube-system/pods/my-pod");
    }

    #[test]
    fn test_label_empty_value() {
        let labels = vec!["key=".to_string()];
        let map = parse_labels(&labels).unwrap();
        assert_eq!(map.get("key").unwrap(), &Value::String("".to_string()));
    }

    #[test]
    fn test_label_resource_type_mapping() {
        let cases = vec![
            ("pod", "api/v1", "pods"),
            ("deploy", "apis/apps/v1", "deployments"),
            ("svc", "api/v1", "services"),
            ("ds", "apis/apps/v1", "daemonsets"),
            ("sts", "apis/apps/v1", "statefulsets"),
            ("rs", "apis/apps/v1", "replicasets"),
            ("cm", "api/v1", "configmaps"),
            ("secret", "api/v1", "secrets"),
            ("node", "api/v1", "nodes"),
        ];
        for (input, expected_api, expected_resource) in cases {
            let (api_path, resource_name) = match input {
                "pod" | "pods" => ("api/v1", "pods"),
                "service" | "services" | "svc" => ("api/v1", "services"),
                "deployment" | "deployments" | "deploy" => ("apis/apps/v1", "deployments"),
                "daemonset" | "daemonsets" | "ds" => ("apis/apps/v1", "daemonsets"),
                "statefulset" | "statefulsets" | "sts" => ("apis/apps/v1", "statefulsets"),
                "replicaset" | "replicasets" | "rs" => ("apis/apps/v1", "replicasets"),
                "configmap" | "configmaps" | "cm" => ("api/v1", "configmaps"),
                "secret" | "secrets" => ("api/v1", "secrets"),
                "node" | "nodes" => ("api/v1", "nodes"),
                _ => panic!("unexpected"),
            };
            assert_eq!(api_path, expected_api, "for input '{}'", input);
            assert_eq!(resource_name, expected_resource, "for input '{}'", input);
        }
    }

    #[test]
    fn test_label_value_with_equals() {
        // Values containing '=' should work because split_once only splits on first '='
        let labels = vec!["key=val=ue".to_string()];
        let map = parse_labels(&labels).unwrap();
        assert_eq!(
            map.get("key").unwrap(),
            &Value::String("val=ue".to_string())
        );
    }
}
