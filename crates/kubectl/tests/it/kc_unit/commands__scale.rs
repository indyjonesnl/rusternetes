//! Unit tests moved from `src/commands/scale.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use serde_json::{json, Value};
}

mod tests {
    use serde_json::json;

    #[test]
    fn test_scale_path_deployment() {
        let path = format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}/scale",
            "default", "web"
        );
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/default/deployments/web/scale"
        );
    }

    #[test]
    fn test_scale_path_statefulset() {
        let path = format!(
            "/apis/apps/v1/namespaces/{}/statefulsets/{}/scale",
            "prod", "db"
        );
        assert_eq!(path, "/apis/apps/v1/namespaces/prod/statefulsets/db/scale");
    }

    #[test]
    fn test_scale_body_construction() {
        let replicas = 5;
        let scale_body = json!({
            "spec": {
                "replicas": replicas
            }
        });
        assert_eq!(scale_body["spec"]["replicas"], 5);
    }

    #[test]
    fn test_scale_body_zero_replicas() {
        let scale_body = json!({
            "spec": {
                "replicas": 0
            }
        });
        assert_eq!(scale_body["spec"]["replicas"], 0);
    }

    #[test]
    fn test_scale_path_replicaset() {
        let path = format!(
            "/apis/apps/v1/namespaces/{}/replicasets/{}/scale",
            "default", "web-rs"
        );
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/default/replicasets/web-rs/scale"
        );
    }

    #[test]
    fn test_scale_path_replication_controller() {
        let path = format!(
            "/api/v1/namespaces/{}/replicationcontrollers/{}/scale",
            "default", "my-rc"
        );
        assert_eq!(
            path,
            "/api/v1/namespaces/default/replicationcontrollers/my-rc/scale"
        );
    }
}
