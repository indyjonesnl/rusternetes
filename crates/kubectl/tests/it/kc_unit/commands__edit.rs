//! Unit tests moved from `src/commands/edit.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use std::env;
    pub use std::fs;
    pub use std::process::Command;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::edit::*;

    #[test]
    fn test_get_resource_api_path_pod_aliases() {
        for alias in &["pod", "pods", "po"] {
            let path = get_resource_api_path(alias, "default", "nginx").unwrap();
            assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
        }
    }

    #[test]
    fn test_get_resource_api_path_deployment_alias() {
        let path = get_resource_api_path("deploy", "ns1", "web").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/ns1/deployments/web");
    }

    #[test]
    fn test_get_resource_api_path_cluster_scoped_node() {
        let path = get_resource_api_path("node", "ignored", "node-1").unwrap();
        assert_eq!(path, "/api/v1/nodes/node-1");
    }

    #[test]
    fn test_get_resource_api_path_unsupported() {
        let result = get_resource_api_path("foobar", "default", "x");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_resource_api_path_all_aliases() {
        // Test that short aliases map to the same path
        let pairs = vec![
            ("svc", "service"),
            ("deploy", "deployment"),
            ("sts", "statefulset"),
            ("ds", "daemonset"),
            ("rs", "replicaset"),
            ("cj", "cronjob"),
            ("cm", "configmap"),
            ("sa", "serviceaccount"),
            ("ing", "ingress"),
            ("pvc", "persistentvolumeclaim"),
            ("pv", "persistentvolume"),
            ("sc", "storageclass"),
            ("ns", "namespace"),
            ("no", "node"),
        ];
        for (short, long) in pairs {
            let short_path = get_resource_api_path(short, "default", "test").unwrap();
            let long_path = get_resource_api_path(long, "default", "test").unwrap();
            assert_eq!(
                short_path, long_path,
                "Alias mismatch: {} vs {}",
                short, long
            );
        }
    }

    #[test]
    fn test_get_resource_api_path_rbac_types() {
        let path = get_resource_api_path("role", "ns1", "reader").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/ns1/roles/reader"
        );

        let path = get_resource_api_path("clusterrole", "ignored", "admin").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );
    }

    #[test]
    fn test_get_resource_api_path_job_and_cronjob() {
        let path = get_resource_api_path("job", "default", "my-job").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/jobs/my-job");

        let path = get_resource_api_path("cronjob", "default", "my-cj").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/cronjobs/my-cj");
    }
}
