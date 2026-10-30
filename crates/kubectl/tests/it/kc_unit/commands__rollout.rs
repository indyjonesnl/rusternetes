//! Unit tests moved from `src/commands/rollout.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::types::RolloutCommands;
    pub use serde_json::{json, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::rollout::*;

    #[test]
    fn test_rollout_api_path_deployment() {
        let (path, version) = get_resource_api_path("deployment", "default", "nginx").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/deployments/nginx");
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_deploy_alias() {
        let (path, _) = get_resource_api_path("deploy", "prod", "web").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/prod/deployments/web");
    }

    #[test]
    fn test_rollout_api_path_statefulset() {
        let (path, _) = get_resource_api_path("sts", "default", "db").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/db");
    }

    #[test]
    fn test_rollout_api_path_daemonset() {
        let (path, _) = get_resource_api_path("ds", "kube-system", "proxy").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/kube-system/daemonsets/proxy"
        );
    }

    #[test]
    fn test_rollout_api_path_unsupported() {
        let result = get_resource_api_path("pod", "default", "x");
        assert!(result.is_err());
    }

    #[test]
    fn test_rollout_api_path_deployments_plural() {
        let (path, version) = get_resource_api_path("deployments", "staging", "api").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/staging/deployments/api");
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_statefulset_full_name() {
        let (path, version) = get_resource_api_path("statefulset", "db-ns", "postgres").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/db-ns/statefulsets/postgres");
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_statefulsets_plural() {
        let (path, _) = get_resource_api_path("statefulsets", "default", "redis").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/redis");
    }

    #[test]
    fn test_rollout_api_path_daemonset_full_name() {
        let (path, version) = get_resource_api_path("daemonset", "monitoring", "fluentd").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/monitoring/daemonsets/fluentd"
        );
        assert_eq!(version, "apps/v1");
    }

    #[test]
    fn test_rollout_api_path_daemonsets_plural() {
        let (path, _) = get_resource_api_path("daemonsets", "kube-system", "kube-proxy").unwrap();
        assert_eq!(
            path,
            "/apis/apps/v1/namespaces/kube-system/daemonsets/kube-proxy"
        );
    }

    #[test]
    fn test_rollout_api_path_unsupported_service() {
        let result = get_resource_api_path("service", "default", "x");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported resource type for rollout"));
    }

    #[test]
    fn test_rollout_api_path_unsupported_replicaset() {
        let result = get_resource_api_path("replicaset", "default", "x");
        assert!(result.is_err());
    }

    #[test]
    fn test_rollout_restart_patch_construction() {
        let now = "2024-01-01T00:00:00Z";
        let patch = json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now
                        }
                    }
                }
            }
        });
        assert_eq!(
            patch["spec"]["template"]["metadata"]["annotations"]
                ["kubectl.kubernetes.io/restartedAt"],
            "2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn test_rollout_pause_patch_construction() {
        let patch = json!({
            "spec": {
                "paused": true
            }
        });
        assert_eq!(patch["spec"]["paused"], true);
    }

    #[test]
    fn test_rollout_resume_patch_construction() {
        let patch = json!({
            "spec": {
                "paused": false
            }
        });
        assert_eq!(patch["spec"]["paused"], false);
    }

    // ===== 8 additional tests for untested async functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_rollout_status_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_status(&client, "deployment", "nginx", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_history_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_history(&client, "deployment", "nginx", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_undo_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_undo(&client, "deployment", "nginx", "default", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_undo_non_deployment_returns_err() {
        let client = make_test_client();
        let result = rollout_undo(&client, "statefulset", "db", "default", None).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only supported for deployments"));
    }

    #[tokio::test]
    async fn test_rollout_restart_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_restart(&client, "deployment", "nginx", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_pause_returns_err_on_unreachable() {
        let client = make_test_client();
        let result = rollout_pause(&client, "deployment", "nginx", "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rollout_pause_non_deployment_returns_err() {
        let client = make_test_client();
        let result = rollout_pause(&client, "statefulset", "db", "default").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only supported for deployments"));
    }

    #[tokio::test]
    async fn test_rollout_resume_non_deployment_returns_err() {
        let client = make_test_client();
        let result = rollout_resume(&client, "statefulset", "db", "default").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only supported for deployments"));
    }
}
