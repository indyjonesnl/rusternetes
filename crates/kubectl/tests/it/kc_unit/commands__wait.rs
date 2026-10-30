//! Unit tests moved from `src/commands/wait.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::{ApiClient, GetError};
    pub use serde_json::Value;
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::wait::*;

    #[test]
    fn test_parse_condition_bare() {
        let (typ, status) = parse_condition("Ready");
        assert_eq!(typ, "Ready");
        assert_eq!(status, "True");
    }

    #[test]
    fn test_parse_condition_with_prefix() {
        let (typ, status) = parse_condition("condition=Ready");
        assert_eq!(typ, "Ready");
        assert_eq!(status, "True");
    }

    #[test]
    fn test_parse_condition_with_status() {
        let (typ, status) = parse_condition("condition=Available=False");
        assert_eq!(typ, "Available");
        assert_eq!(status, "False");
    }

    #[test]
    fn test_parse_duration_seconds() {
        let d = parse_duration("30s").unwrap();
        assert_eq!(d, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_minutes() {
        let d = parse_duration("5m").unwrap();
        assert_eq!(d, Duration::from_secs(300));
    }

    #[test]
    fn test_parse_duration_hours() {
        let d = parse_duration("1h").unwrap();
        assert_eq!(d, Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_empty_defaults() {
        let d = parse_duration("").unwrap();
        assert_eq!(d, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_raw_number() {
        let d = parse_duration("60").unwrap();
        assert_eq!(d, Duration::from_secs(60));
    }

    #[test]
    fn test_check_condition_found() {
        let resource = serde_json::json!({
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "Available", "status": "False"}
                ]
            }
        });
        assert!(check_condition(&resource, "Ready", "True").unwrap());
        assert!(!check_condition(&resource, "Available", "True").unwrap());
        assert!(check_condition(&resource, "Available", "False").unwrap());
    }

    #[test]
    fn test_check_condition_not_found() {
        let resource = serde_json::json!({"status": {"conditions": []}});
        assert!(!check_condition(&resource, "Ready", "True").unwrap());
    }

    #[test]
    fn test_parse_resource_type_pods() {
        let (api, name) = parse_resource_type("pods").unwrap();
        assert_eq!(api, "api/v1");
        assert_eq!(name, "pods");
    }

    #[test]
    fn test_parse_resource_type_deploy() {
        let (api, name) = parse_resource_type("deploy").unwrap();
        assert_eq!(api, "apis/apps/v1");
        assert_eq!(name, "deployments");
    }

    #[test]
    fn test_parse_resource_type_all_aliases() {
        let (api, name) = parse_resource_type("pod").unwrap();
        assert_eq!(api, "api/v1");
        assert_eq!(name, "pods");

        let (api, name) = parse_resource_type("svc").unwrap();
        assert_eq!(api, "api/v1");
        assert_eq!(name, "services");

        let (api, name) = parse_resource_type("services").unwrap();
        assert_eq!(api, "api/v1");
        assert_eq!(name, "services");

        let (api, name) = parse_resource_type("ds").unwrap();
        assert_eq!(api, "apis/apps/v1");
        assert_eq!(name, "daemonsets");

        let (api, name) = parse_resource_type("daemonsets").unwrap();
        assert_eq!(api, "apis/apps/v1");
        assert_eq!(name, "daemonsets");

        let (api, name) = parse_resource_type("sts").unwrap();
        assert_eq!(api, "apis/apps/v1");
        assert_eq!(name, "statefulsets");

        let (api, name) = parse_resource_type("rs").unwrap();
        assert_eq!(api, "apis/apps/v1");
        assert_eq!(name, "replicasets");

        let (api, name) = parse_resource_type("jobs").unwrap();
        assert_eq!(api, "apis/batch/v1");
        assert_eq!(name, "jobs");
    }

    #[test]
    fn test_parse_resource_type_unsupported() {
        let result = parse_resource_type("configmap");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported resource type"));
    }

    #[test]
    fn test_check_condition_no_status_field() {
        let resource = serde_json::json!({"metadata": {"name": "test"}});
        assert!(!check_condition(&resource, "Ready", "True").unwrap());
    }

    #[test]
    fn test_check_condition_no_conditions_field() {
        let resource = serde_json::json!({"status": {"phase": "Running"}});
        assert!(!check_condition(&resource, "Ready", "True").unwrap());
    }

    #[test]
    fn test_check_condition_multiple_conditions() {
        let resource = serde_json::json!({
            "status": {
                "conditions": [
                    {"type": "Initialized", "status": "True"},
                    {"type": "Ready", "status": "False"},
                    {"type": "ContainersReady", "status": "False"},
                    {"type": "PodScheduled", "status": "True"}
                ]
            }
        });
        assert!(check_condition(&resource, "Initialized", "True").unwrap());
        assert!(!check_condition(&resource, "Ready", "True").unwrap());
        assert!(check_condition(&resource, "Ready", "False").unwrap());
        assert!(check_condition(&resource, "PodScheduled", "True").unwrap());
        assert!(!check_condition(&resource, "ContainersReady", "True").unwrap());
    }

    #[test]
    fn test_parse_duration_invalid() {
        let result = parse_duration("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_duration_zero_seconds() {
        let d = parse_duration("0s").unwrap();
        assert_eq!(d, Duration::from_secs(0));
    }

    #[test]
    fn test_parse_duration_zero_minutes() {
        let d = parse_duration("0m").unwrap();
        assert_eq!(d, Duration::from_secs(0));
    }

    #[test]
    fn test_parse_duration_large_value() {
        let d = parse_duration("3600s").unwrap();
        assert_eq!(d, Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_condition_bare_complex_type() {
        let (typ, status) = parse_condition("PodScheduled");
        assert_eq!(typ, "PodScheduled");
        assert_eq!(status, "True");
    }

    #[test]
    fn test_check_condition_missing_status_in_condition_entry() {
        let resource = serde_json::json!({
            "status": {
                "conditions": [
                    {"type": "Ready"}
                ]
            }
        });
        // Condition entry has type but no status field
        assert!(!check_condition(&resource, "Ready", "True").unwrap());
    }

    #[test]
    fn test_check_condition_null_resource() {
        let resource = serde_json::json!(null);
        assert!(!check_condition(&resource, "Ready", "True").unwrap());
    }

    #[test]
    fn test_parse_resource_type_singular_and_plural_consistency() {
        // Ensure singular and plural map to the same result
        let singular = parse_resource_type("deployment").unwrap();
        let plural = parse_resource_type("deployments").unwrap();
        assert_eq!(singular, plural);

        let singular = parse_resource_type("statefulset").unwrap();
        let plural = parse_resource_type("statefulsets").unwrap();
        assert_eq!(singular, plural);

        let singular = parse_resource_type("replicaset").unwrap();
        let plural = parse_resource_type("replicasets").unwrap();
        assert_eq!(singular, plural);

        let singular = parse_resource_type("service").unwrap();
        let plural = parse_resource_type("services").unwrap();
        assert_eq!(singular, plural);

        let singular = parse_resource_type("job").unwrap();
        let plural = parse_resource_type("jobs").unwrap();
        assert_eq!(singular, plural);
    }

    #[test]
    fn test_polling_url_construction() {
        let (api_path, resource_name) = parse_resource_type("pod").unwrap();
        let path_with_name = format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, "default", resource_name, "nginx"
        );
        assert_eq!(path_with_name, "/api/v1/namespaces/default/pods/nginx");

        let path_without_name = format!("/{}/namespaces/{}/{}", api_path, "default", resource_name);
        assert_eq!(path_without_name, "/api/v1/namespaces/default/pods");

        let (api_path, resource_name) = parse_resource_type("deployment").unwrap();
        let path = format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, "prod", resource_name, "web"
        );
        assert_eq!(path, "/apis/apps/v1/namespaces/prod/deployments/web");

        let (api_path, resource_name) = parse_resource_type("job").unwrap();
        let path = format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, "batch-ns", resource_name, "my-job"
        );
        assert_eq!(path, "/apis/batch/v1/namespaces/batch-ns/jobs/my-job");
    }
}
