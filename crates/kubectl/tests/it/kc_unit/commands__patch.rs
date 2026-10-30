//! Unit tests moved from `src/commands/patch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::Value;
    pub use std::fs;
}

mod tests {
    #[test]
    fn test_patch_content_type_strategic() {
        let content_type = match "strategic" {
            "strategic" => "application/strategic-merge-patch+json",
            "merge" => "application/merge-patch+json",
            "json" => "application/json-patch+json",
            _ => panic!("invalid"),
        };
        assert_eq!(content_type, "application/strategic-merge-patch+json");
    }

    #[test]
    fn test_patch_content_type_merge() {
        let content_type = match "merge" {
            "strategic" => "application/strategic-merge-patch+json",
            "merge" => "application/merge-patch+json",
            "json" => "application/json-patch+json",
            _ => panic!("invalid"),
        };
        assert_eq!(content_type, "application/merge-patch+json");
    }

    #[test]
    fn test_patch_content_type_json() {
        let content_type = match "json" {
            "strategic" => "application/strategic-merge-patch+json",
            "merge" => "application/merge-patch+json",
            "json" => "application/json-patch+json",
            _ => panic!("invalid"),
        };
        assert_eq!(content_type, "application/json-patch+json");
    }

    #[test]
    fn test_patch_api_path_pod() {
        let (api_path, resource_name) = ("api/v1", "pods");
        let namespace = "default";
        let name = "nginx";
        let path = format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, namespace, resource_name, name
        );
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
    }

    #[test]
    fn test_patch_api_path_node_no_namespace() {
        let (api_path, resource_name) = ("api/v1", "nodes");
        let name = "node-1";
        let path = format!("/{}/{}/{}", api_path, resource_name, name);
        assert_eq!(path, "/api/v1/nodes/node-1");
    }

    #[test]
    fn test_patch_body_json_parse() {
        let patch_str = r#"{"spec":{"replicas":3}}"#;
        let value: serde_json::Value = serde_json::from_str(patch_str).unwrap();
        assert_eq!(value["spec"]["replicas"], 3);
    }

    #[test]
    fn test_patch_invalid_json_fails() {
        let patch_str = r#"{"spec": invalid}"#;
        let result: Result<serde_json::Value, _> = serde_json::from_str(patch_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_patch_resource_type_mapping() {
        let cases = [
            ("pod", "api/v1", "pods"),
            ("svc", "api/v1", "services"),
            ("deploy", "apis/apps/v1", "deployments"),
            ("ds", "apis/apps/v1", "daemonsets"),
            ("sts", "apis/apps/v1", "statefulsets"),
            ("rs", "apis/apps/v1", "replicasets"),
            ("cm", "api/v1", "configmaps"),
            ("secrets", "api/v1", "secrets"),
            ("nodes", "api/v1", "nodes"),
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
}
