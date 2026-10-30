//! Unit tests moved from `src/commands/top.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::types::TopCommands;
    pub use serde::Deserialize;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::top::*;

    #[test]
    fn test_node_metrics_deserialization() {
        let json = r#"{
            "metadata": {"name": "node-1"},
            "usage": {"cpu": "250m", "memory": "1024Mi"}
        }"#;
        let metrics: NodeMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.metadata.name, "node-1");
        assert_eq!(metrics.usage.cpu, "250m");
        assert_eq!(metrics.usage.memory, "1024Mi");
    }

    #[test]
    fn test_pod_metrics_deserialization() {
        let json = r#"{
            "metadata": {"name": "web-pod"},
            "containers": [
                {"name": "nginx", "usage": {"cpu": "100m", "memory": "64Mi"}},
                {"name": "sidecar", "usage": {"cpu": "50m", "memory": "32Mi"}}
            ]
        }"#;
        let metrics: PodMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.metadata.name, "web-pod");
        assert_eq!(metrics.containers.len(), 2);
        assert_eq!(metrics.containers[0].name, "nginx");
        assert_eq!(metrics.containers[1].usage.cpu, "50m");
    }

    #[test]
    fn test_metrics_api_path_node() {
        let path = "/apis/metrics.k8s.io/v1beta1/nodes";
        assert!(path.contains("metrics.k8s.io"));
    }

    #[test]
    fn test_metrics_api_path_specific_node() {
        let name = "node-1";
        let path = format!("/apis/metrics.k8s.io/v1beta1/nodes/{}", name);
        assert_eq!(path, "/apis/metrics.k8s.io/v1beta1/nodes/node-1");
    }

    #[test]
    fn test_metrics_api_path_pod_in_namespace() {
        let ns = "default";
        let path = format!("/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods", ns);
        assert_eq!(path, "/apis/metrics.k8s.io/v1beta1/namespaces/default/pods");
    }

    #[test]
    fn test_container_metrics_deserialization() {
        let json = r#"{"name": "sidecar", "usage": {"cpu": "10m", "memory": "16Mi"}}"#;
        let metrics: ContainerMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.name, "sidecar");
        assert_eq!(metrics.usage.cpu, "10m");
        assert_eq!(metrics.usage.memory, "16Mi");
    }

    #[test]
    fn test_pod_metrics_empty_containers() {
        let json = r#"{
            "metadata": {"name": "empty-pod"},
            "containers": []
        }"#;
        let metrics: PodMetrics = serde_json::from_str(json).unwrap();
        assert_eq!(metrics.metadata.name, "empty-pod");
        assert!(metrics.containers.is_empty());
    }
}
