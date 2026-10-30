//! Unit tests moved from `src/commands/create.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use base64::Engine;
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::discovery::RestMapper;
    pub use rusternetes_kubectl::types::{CreateCommands, SecretCommands, ServiceCommands};
    pub use serde_json::{json, Value};
    pub use std::collections::BTreeMap;
    pub use std::fs;
    pub use std::path::Path;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::create::*;

    // 1. build_cluster_role — basic with verbs/resources
    #[test]
    fn test_build_cluster_role_basic() {
        let verbs = vec!["get".to_string(), "list".to_string()];
        let resources = vec!["pods".to_string()];
        let result = build_cluster_role("test-cr", &verbs, &resources, &[], &[], &[]).unwrap();
        assert_eq!(result["kind"], "ClusterRole");
        assert_eq!(result["metadata"]["name"], "test-cr");
        assert!(result["rules"].is_array());
    }

    // 2. build_cluster_role — with aggregation rule
    #[test]
    fn test_build_cluster_role_aggregation() {
        let agg = vec!["rbac.example.com/aggregate-to-admin=true".to_string()];
        let result = build_cluster_role("agg-cr", &[], &[], &[], &[], &agg).unwrap();
        assert!(result["aggregationRule"]["clusterRoleSelectors"].is_array());
        assert!(result.get("rules").is_none() || result["rules"].is_null());
    }

    // 3. build_cluster_role_binding
    #[test]
    fn test_build_cluster_role_binding() {
        let users = vec!["admin-user".to_string()];
        let result =
            build_cluster_role_binding("test-crb", "cluster-admin", &users, &[], &[]).unwrap();
        assert_eq!(result["kind"], "ClusterRoleBinding");
        assert_eq!(result["roleRef"]["name"], "cluster-admin");
        assert_eq!(result["subjects"][0]["name"], "admin-user");
    }

    // 4. build_configmap — from literals
    #[test]
    fn test_build_configmap_from_literal() {
        let literals = vec!["key1=value1".to_string(), "key2=value2".to_string()];
        let result = build_configmap("test-cm", "default", &literals, &[]).unwrap();
        assert_eq!(result["kind"], "ConfigMap");
        assert_eq!(result["data"]["key1"], "value1");
        assert_eq!(result["data"]["key2"], "value2");
    }

    // 5. build_configmap — from file
    #[test]
    fn test_build_configmap_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let file_path = dir.path().join("config.txt");
        std::fs::write(&file_path, "file-content").unwrap();
        let from_file = vec![file_path.to_string_lossy().to_string()];
        let result = build_configmap("file-cm", "default", &[], &from_file).unwrap();
        assert_eq!(result["data"]["config.txt"], "file-content");
    }

    // 6. build_cronjob — basic
    #[test]
    fn test_build_cronjob_basic() {
        let result = build_cronjob("my-cj", "default", "busybox", "*/5 * * * *", "Never", &[]);
        assert_eq!(result["kind"], "CronJob");
        assert_eq!(result["spec"]["schedule"], "*/5 * * * *");
    }

    // 7. build_cronjob — with command
    #[test]
    fn test_build_cronjob_with_command() {
        let cmd = vec!["echo".to_string(), "hello".to_string()];
        let result = build_cronjob("cj2", "default", "busybox", "0 * * * *", "Never", &cmd);
        let container = &result["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["command"][0], "echo");
    }

    // 8. build_ingress — basic rule
    #[test]
    fn test_build_ingress_basic() {
        let rules = vec!["example.com/path*=my-svc:80".to_string()];
        let result =
            build_ingress("test-ing", "default", Some("nginx"), &rules, None, &[]).unwrap();
        assert_eq!(result["kind"], "Ingress");
        assert_eq!(result["spec"]["ingressClassName"], "nginx");
        assert!(result["spec"]["rules"].is_array());
    }

    // 9. build_ingress — with default backend
    #[test]
    fn test_build_ingress_default_backend() {
        let result = build_ingress(
            "test-ing2",
            "default",
            None,
            &[],
            Some("default-svc:80"),
            &[],
        )
        .unwrap();
        assert_eq!(
            result["spec"]["defaultBackend"]["service"]["name"],
            "default-svc"
        );
    }

    // 10. build_ingress — with annotations
    #[test]
    fn test_build_ingress_annotations() {
        let annotations = vec!["nginx.ingress.kubernetes.io/rewrite-target=/".to_string()];
        let result = build_ingress("ann-ing", "default", None, &[], None, &annotations).unwrap();
        assert!(result["metadata"]["annotations"]
            .as_object()
            .unwrap()
            .contains_key("nginx.ingress.kubernetes.io/rewrite-target"));
    }

    // 11. build_job — with image
    #[test]
    fn test_build_job_with_image() {
        let result = build_job("test-job", "default", Some("busybox"), None, &[]).unwrap();
        assert_eq!(result["kind"], "Job");
        assert_eq!(
            result["spec"]["template"]["spec"]["containers"][0]["image"],
            "busybox"
        );
    }

    // 12. build_job — with from cronjob
    #[test]
    fn test_build_job_from_cronjob() {
        let result = build_job("manual-job", "default", None, Some("cronjob/my-cj"), &[]).unwrap();
        assert_eq!(
            result["metadata"]["annotations"]["cronjob.kubernetes.io/instantiate"],
            "manual"
        );
    }

    // 13. build_job — error: neither image nor from
    #[test]
    fn test_build_job_no_image_no_from() {
        let result = build_job("bad-job", "default", None, None, &[]);
        assert!(result.is_err());
    }

    // 14. build_job — error: both image and from
    #[test]
    fn test_build_job_both_image_and_from() {
        let result = build_job("bad-job", "default", Some("img"), Some("cronjob/cj"), &[]);
        assert!(result.is_err());
    }

    // 15. build_pdb — with min_available
    #[test]
    fn test_build_pdb_min_available() {
        let result = build_pdb("test-pdb", "default", "app=nginx", Some("2"), None).unwrap();
        assert_eq!(result["kind"], "PodDisruptionBudget");
        assert_eq!(result["spec"]["minAvailable"], 2);
    }

    // 16. build_pdb — error: neither min nor max
    #[test]
    fn test_build_pdb_no_min_no_max() {
        let result = build_pdb("bad-pdb", "default", "app=nginx", None, None);
        assert!(result.is_err());
    }

    // 17. build_role — basic
    #[test]
    fn test_build_role_basic() {
        let verbs = vec!["get".to_string()];
        let resources = vec!["pods".to_string()];
        let result = build_role("test-role", "default", &verbs, &resources, &[]).unwrap();
        assert_eq!(result["kind"], "Role");
        assert_eq!(result["metadata"]["namespace"], "default");
    }

    // 18. build_role — error: no verbs
    #[test]
    fn test_build_role_no_verbs() {
        let result = build_role("bad-role", "default", &[], &["pods".to_string()], &[]);
        assert!(result.is_err());
    }

    // 19. build_role_binding — with clusterrole ref
    #[test]
    fn test_build_role_binding_clusterrole() {
        let users = vec!["user1".to_string()];
        let result =
            build_role_binding("test-rb", "default", Some("admin"), None, &users, &[], &[])
                .unwrap();
        assert_eq!(result["kind"], "RoleBinding");
        assert_eq!(result["roleRef"]["kind"], "ClusterRole");
    }

    // 20. build_role_binding — with role ref
    #[test]
    fn test_build_role_binding_role() {
        let groups = vec!["devs".to_string()];
        let result = build_role_binding(
            "test-rb2",
            "default",
            None,
            Some("my-role"),
            &[],
            &groups,
            &[],
        )
        .unwrap();
        assert_eq!(result["roleRef"]["kind"], "Role");
        assert_eq!(result["roleRef"]["name"], "my-role");
    }

    // 21. build_secret_generic — from literals
    #[test]
    fn test_build_secret_generic() {
        let literals = vec!["password=s3cr3t".to_string()];
        let result = build_secret_generic("test-secret", "default", &literals, &[], None).unwrap();
        assert_eq!(result["kind"], "Secret");
        assert_eq!(result["type"], "Opaque");
        // data should be base64-encoded
        let encoded = result["data"]["password"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "s3cr3t");
    }

    // 22. build_secret_docker_registry — valid
    #[test]
    fn test_build_secret_docker_registry_valid() {
        let result = build_secret_docker_registry(
            "my-reg",
            "default",
            "https://index.docker.io/v1/",
            Some("user"),
            Some("pass"),
            Some("user@example.com"),
        )
        .unwrap();
        assert_eq!(result["type"], "kubernetes.io/dockerconfigjson");
        assert!(result["data"][".dockerconfigjson"].is_string());
    }

    // 23. build_secret_docker_registry — missing credentials
    #[test]
    fn test_build_secret_docker_registry_missing_creds() {
        let result = build_secret_docker_registry("bad-reg", "default", "server", None, None, None);
        assert!(result.is_err());
    }

    // 24. build_service_account
    #[test]
    fn test_build_service_account() {
        let result = build_service_account("test-sa", "kube-system");
        assert_eq!(result["kind"], "ServiceAccount");
        assert_eq!(result["metadata"]["name"], "test-sa");
        assert_eq!(result["metadata"]["namespace"], "kube-system");
    }

    // 25. build_token_request — basic with audiences
    #[test]
    fn test_build_token_request_basic() {
        let audiences = vec!["api".to_string()];
        let result = build_token_request(&audiences, None, None, None, None).unwrap();
        assert_eq!(result["kind"], "TokenRequest");
        assert_eq!(result["spec"]["audiences"][0], "api");
    }

    // 26. build_token_request — with duration
    #[test]
    fn test_build_token_request_duration() {
        let result = build_token_request(&[], Some("1h"), None, None, None).unwrap();
        assert_eq!(result["spec"]["expirationSeconds"], 3600);
    }

    // 27. parse_resource_group — with group
    #[test]
    fn test_parse_resource_group_with_group() {
        let (resource, group) = parse_resource_group("deployments.apps");
        assert_eq!(resource, "deployments");
        assert_eq!(group, "apps");
    }

    // 28. parse_resource_group — without group
    #[test]
    fn test_parse_resource_group_without_group() {
        let (resource, group) = parse_resource_group("pods");
        assert_eq!(resource, "pods");
        assert_eq!(group, "");
    }

    // Additional helpers that are private but callable from tests module
    #[test]
    fn test_parse_key_value_valid() {
        let (k, v) = parse_key_value("foo=bar").unwrap();
        assert_eq!(k, "foo");
        assert_eq!(v, "bar");
    }

    #[test]
    fn test_parse_key_value_invalid() {
        assert!(parse_key_value("noequalssign").is_err());
    }

    #[test]
    fn test_parse_file_source_with_key() {
        let (k, p) = parse_file_source("mykey=/path/to/file").unwrap();
        assert_eq!(k, "mykey");
        assert_eq!(p, "/path/to/file");
    }

    #[test]
    fn test_parse_file_source_without_key() {
        let (k, p) = parse_file_source("/path/to/config.txt").unwrap();
        assert_eq!(k, "config.txt");
        assert_eq!(p, "/path/to/config.txt");
    }

    #[test]
    fn test_parse_label_selector() {
        let result = parse_label_selector("app=nginx,env=prod");
        let labels = result["matchLabels"].as_object().unwrap();
        assert_eq!(labels["app"], "nginx");
        assert_eq!(labels["env"], "prod");
    }

    #[test]
    fn test_parse_int_or_string_integer() {
        assert_eq!(parse_int_or_string("5"), serde_json::json!(5));
    }

    #[test]
    fn test_parse_int_or_string_percentage() {
        assert_eq!(parse_int_or_string("50%"), serde_json::json!("50%"));
    }

    #[test]
    fn test_parse_duration_to_seconds_seconds() {
        assert_eq!(parse_duration_to_seconds("300s").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_to_seconds_minutes() {
        assert_eq!(parse_duration_to_seconds("5m").unwrap(), 300);
    }

    #[test]
    fn test_parse_duration_to_seconds_hours() {
        assert_eq!(parse_duration_to_seconds("2h").unwrap(), 7200);
    }

    #[test]
    fn test_parse_duration_to_seconds_bare_number() {
        assert_eq!(parse_duration_to_seconds("120").unwrap(), 120);
    }

    #[test]
    fn test_build_ingress_backend_numeric_port() {
        let backend = build_ingress_backend("my-svc", "8080");
        assert_eq!(backend["service"]["name"], "my-svc");
        assert_eq!(backend["service"]["port"]["number"], 8080);
    }

    #[test]
    fn test_build_ingress_backend_named_port() {
        let backend = build_ingress_backend("my-svc", "http");
        assert_eq!(backend["service"]["port"]["name"], "http");
    }

    #[test]
    fn test_build_policy_rules_basic() {
        let verbs = vec!["get".to_string(), "list".to_string()];
        let resources = vec!["pods".to_string()];
        let result = build_policy_rules(&verbs, &resources, &[], &[]);
        assert!(result.is_array());
        let rules = result.as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["verbs"][0], "get");
    }

    #[test]
    fn test_build_policy_rules_non_resource_urls() {
        let verbs = vec!["get".to_string()];
        let urls = vec!["/healthz".to_string()];
        let result = build_policy_rules(&verbs, &[], &[], &urls);
        let rules = result.as_array().unwrap();
        assert_eq!(rules[0]["nonResourceURLs"][0], "/healthz");
    }

    #[test]
    fn test_build_subjects_user() {
        let users = vec!["alice".to_string()];
        let result = build_subjects(&users, &[], &[]).unwrap();
        assert_eq!(result[0]["kind"], "User");
        assert_eq!(result[0]["name"], "alice");
    }

    #[test]
    fn test_build_subjects_group() {
        let groups = vec!["developers".to_string()];
        let result = build_subjects(&[], &groups, &[]).unwrap();
        assert_eq!(result[0]["kind"], "Group");
    }

    #[test]
    fn test_build_subjects_service_account() {
        let sas = vec!["kube-system:default".to_string()];
        let result = build_subjects(&[], &[], &sas).unwrap();
        assert_eq!(result[0]["kind"], "ServiceAccount");
        assert_eq!(result[0]["namespace"], "kube-system");
        assert_eq!(result[0]["name"], "default");
    }

    #[test]
    fn test_build_subjects_invalid_sa() {
        let sas = vec!["invalid-format".to_string()];
        assert!(build_subjects(&[], &[], &sas).is_err());
    }

    #[test]
    fn test_inline_namespace_maps_to_subcommand() {
        for kind in ["namespace", "ns"] {
            let args = vec![kind.to_string(), "test-ns".to_string()];
            match inline_to_subcommand(&args).unwrap() {
                CreateCommands::Namespace { name } => assert_eq!(name, "test-ns"),
                _ => panic!("expected Namespace subcommand"),
            }
        }
    }

    #[test]
    fn test_inline_namespace_requires_exactly_one_name() {
        let none = vec!["namespace".to_string()];
        assert_eq!(
            inline_to_subcommand(&none).err().unwrap().to_string(),
            "NAME is required"
        );
        let two = vec!["ns".to_string(), "a".to_string(), "b".to_string()];
        assert_eq!(
            inline_to_subcommand(&two).err().unwrap().to_string(),
            "exactly one NAME is required, got 2"
        );
    }

    #[tokio::test]
    async fn test_execute_inline_empty_args() {
        let client = ApiClient::new("https://127.0.0.1:1", false, None).unwrap();
        let result = execute_inline(&client, &[], "default").await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "must specify one of -f and -k"
        );
    }

    #[tokio::test]
    async fn test_execute_inline_unknown_resource() {
        let client = ApiClient::new("https://127.0.0.1:1", false, None).unwrap();
        let args = vec!["widget".to_string(), "x".to_string()];
        let result = execute_inline(&client, &args, "default").await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "must specify one of -f and -k"
        );
    }

    #[test]
    fn test_build_secret_tls_missing_cert() {
        let result = build_secret_tls(
            "tls-secret",
            "default",
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_build_pdb_max_unavailable_percentage() {
        let result = build_pdb("pdb-pct", "default", "app=web", None, Some("25%")).unwrap();
        assert_eq!(result["spec"]["maxUnavailable"], "25%");
    }

    #[test]
    fn test_parse_resource_group_with_subresource() {
        let (resource, group) = parse_resource_group("pods/status");
        assert_eq!(resource, "pods/status");
        assert_eq!(group, "");
    }

    #[test]
    fn test_build_token_request_bound_object() {
        let result =
            build_token_request(&[], None, Some("Pod"), Some("my-pod"), Some("abc-123")).unwrap();
        let bound = &result["spec"]["boundObjectRef"];
        assert_eq!(bound["kind"], "Pod");
        assert_eq!(bound["name"], "my-pod");
        assert_eq!(bound["uid"], "abc-123");
    }

    // ── Tests for new subcommands ───────────────────────────────────────

    #[test]
    fn test_build_namespace() {
        let result = build_namespace("test-ns");
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "Namespace");
        assert_eq!(result["metadata"]["name"], "test-ns");
    }

    #[test]
    fn test_build_deployment_basic() {
        let result = build_deployment("nginx", "default", "nginx:1.25", 3, None);
        assert_eq!(result["apiVersion"], "apps/v1");
        assert_eq!(result["kind"], "Deployment");
        assert_eq!(result["metadata"]["name"], "nginx");
        assert_eq!(result["metadata"]["namespace"], "default");
        assert_eq!(result["spec"]["replicas"], 3);
        let container = &result["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["name"], "nginx");
        assert_eq!(container["image"], "nginx:1.25");
        assert!(container.get("ports").is_none() || container["ports"].is_null());
    }

    #[test]
    fn test_build_deployment_with_port() {
        let result = build_deployment("web", "prod", "nginx:latest", 2, Some(80));
        let container = &result["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(container["ports"][0]["containerPort"], 80);
        assert_eq!(result["spec"]["selector"]["matchLabels"]["app"], "web");
        assert_eq!(
            result["spec"]["template"]["metadata"]["labels"]["app"],
            "web"
        );
    }

    #[test]
    fn test_build_service_clusterip() {
        let tcp = vec!["80:8080".to_string()];
        let result = build_service("my-svc", "default", "ClusterIP", &tcp, None).unwrap();
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "Service");
        assert_eq!(result["spec"]["type"], "ClusterIP");
        assert_eq!(result["spec"]["ports"][0]["port"], 80);
        assert_eq!(result["spec"]["ports"][0]["targetPort"], 8080);
        assert_eq!(result["spec"]["ports"][0]["protocol"], "TCP");
    }

    #[test]
    fn test_build_service_nodeport() {
        let tcp = vec!["80:8080".to_string()];
        let result = build_service("my-svc", "default", "NodePort", &tcp, None).unwrap();
        assert_eq!(result["spec"]["type"], "NodePort");
    }

    #[test]
    fn test_build_service_loadbalancer() {
        let tcp = vec!["443:8443".to_string()];
        let result = build_service("lb-svc", "prod", "LoadBalancer", &tcp, None).unwrap();
        assert_eq!(result["spec"]["type"], "LoadBalancer");
        assert_eq!(result["spec"]["ports"][0]["port"], 443);
        assert_eq!(result["spec"]["ports"][0]["targetPort"], 8443);
    }

    #[test]
    fn test_build_service_multiple_ports() {
        let tcp = vec!["80:8080".to_string(), "443:8443".to_string()];
        let result = build_service("multi-svc", "default", "ClusterIP", &tcp, None).unwrap();
        let ports = result["spec"]["ports"].as_array().unwrap();
        assert_eq!(ports.len(), 2);
    }

    #[test]
    fn test_build_service_invalid_tcp() {
        let tcp = vec!["invalid".to_string()];
        let result = build_service("bad-svc", "default", "ClusterIP", &tcp, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_service_external_name() {
        let result = build_service_external_name("ext-svc", "default", "my.database.example.com");
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "Service");
        assert_eq!(result["spec"]["type"], "ExternalName");
        assert_eq!(result["spec"]["externalName"], "my.database.example.com");
    }

    #[test]
    fn test_build_priority_class_basic() {
        let result = build_priority_class(
            "high-priority",
            1000000,
            false,
            "PreemptLowerPriority",
            None,
        );
        assert_eq!(result["apiVersion"], "scheduling.k8s.io/v1");
        assert_eq!(result["kind"], "PriorityClass");
        assert_eq!(result["metadata"]["name"], "high-priority");
        assert_eq!(result["value"], 1000000);
        assert_eq!(result["globalDefault"], false);
        assert_eq!(result["preemptionPolicy"], "PreemptLowerPriority");
        assert!(result.get("description").is_none() || result["description"].is_null());
    }

    #[test]
    fn test_build_priority_class_with_description() {
        let result = build_priority_class(
            "critical",
            2000000,
            true,
            "Never",
            Some("Critical priority class"),
        );
        assert_eq!(result["globalDefault"], true);
        assert_eq!(result["preemptionPolicy"], "Never");
        assert_eq!(result["description"], "Critical priority class");
    }

    #[test]
    fn test_build_quota_basic() {
        let result = build_quota("my-quota", "default", Some("pods=10,services=5")).unwrap();
        assert_eq!(result["apiVersion"], "v1");
        assert_eq!(result["kind"], "ResourceQuota");
        assert_eq!(result["metadata"]["name"], "my-quota");
        assert_eq!(result["metadata"]["namespace"], "default");
        assert_eq!(result["spec"]["hard"]["pods"], "10");
        assert_eq!(result["spec"]["hard"]["services"], "5");
    }

    #[test]
    fn test_build_quota_empty_hard() {
        let result = build_quota("empty-quota", "default", None).unwrap();
        assert_eq!(result["kind"], "ResourceQuota");
        assert!(result["spec"]["hard"].as_object().unwrap().is_empty());
    }

    #[test]
    fn test_build_quota_invalid_hard() {
        let result = build_quota("bad-quota", "default", Some("invalid-format"));
        assert!(result.is_err());
    }
}
