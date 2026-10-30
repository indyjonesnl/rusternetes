//! Unit tests moved from `src/commands/delete.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::delete::*;

    #[test]
    fn test_urlencoding() {
        assert_eq!(urlencoding::encode("app=web"), "app=web");
        assert_eq!(urlencoding::encode("a b"), "a+b");
    }

    // === New tests for the 7 fixed issues ===

    #[test]
    fn test_grace_period_query_param() {
        let opts = DeleteOptions {
            grace_period: Some(30),
            ..Default::default()
        };
        let params = opts.query_params();
        assert!(params.contains(&("gracePeriodSeconds".to_string(), "30".to_string())));
    }

    #[test]
    fn test_grace_period_zero_query_param() {
        let opts = DeleteOptions {
            grace_period: Some(0),
            ..Default::default()
        };
        let params = opts.query_params();
        assert!(params.contains(&("gracePeriodSeconds".to_string(), "0".to_string())));
    }

    #[test]
    fn test_force_sets_grace_period_zero_and_background() {
        let mut opts = DeleteOptions {
            force: true,
            ..Default::default()
        };
        opts.resolve();
        assert_eq!(opts.grace_period, Some(0));
        assert_eq!(opts.cascade, CascadeStrategy::Background);

        let params = opts.query_params();
        assert!(params.contains(&("gracePeriodSeconds".to_string(), "0".to_string())));

        let body = opts.delete_body().unwrap();
        assert_eq!(body["propagationPolicy"], "Background");
        assert_eq!(body["gracePeriodSeconds"], 0);
    }

    #[test]
    fn test_cascade_foreground_body() {
        let opts = DeleteOptions {
            cascade: CascadeStrategy::Foreground,
            ..Default::default()
        };
        let body = opts.delete_body().unwrap();
        assert_eq!(body["propagationPolicy"], "Foreground");
    }

    #[test]
    fn test_cascade_orphan_body() {
        let opts = DeleteOptions {
            cascade: CascadeStrategy::Orphan,
            ..Default::default()
        };
        let body = opts.delete_body().unwrap();
        assert_eq!(body["propagationPolicy"], "Orphan");
    }

    #[test]
    fn test_cascade_background_body() {
        let opts = DeleteOptions {
            cascade: CascadeStrategy::Background,
            ..Default::default()
        };
        let body = opts.delete_body().unwrap();
        assert_eq!(body["propagationPolicy"], "Background");
    }

    #[test]
    fn test_cascade_from_str() {
        assert_eq!(
            CascadeStrategy::from_str_value("foreground").unwrap(),
            CascadeStrategy::Foreground
        );
        assert_eq!(
            CascadeStrategy::from_str_value("background").unwrap(),
            CascadeStrategy::Background
        );
        assert_eq!(
            CascadeStrategy::from_str_value("orphan").unwrap(),
            CascadeStrategy::Orphan
        );
        assert!(CascadeStrategy::from_str_value("invalid").is_err());
    }

    #[test]
    fn test_dry_run_query_param() {
        let opts = DeleteOptions {
            dry_run: true,
            ..Default::default()
        };
        let params = opts.query_params();
        assert!(params.contains(&("dryRun".to_string(), "All".to_string())));
    }

    #[test]
    fn test_dry_run_not_set_by_default() {
        let opts = DeleteOptions::default();
        let params = opts.query_params();
        assert!(!params.iter().any(|(k, _)| k == "dryRun"));
    }

    #[test]
    fn test_output_name_format() {
        let opts = DeleteOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        let output = opts.format_output("pod", "nginx");
        assert_eq!(output, "pod/nginx");
    }

    #[test]
    fn test_output_name_format_deployment() {
        let opts = DeleteOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        let output = opts.format_output("deployment", "web");
        assert_eq!(output, "deployment.apps/web");
    }

    #[test]
    fn test_output_default_format() {
        let opts = DeleteOptions::default();
        let output = opts.format_output("pod", "nginx");
        assert_eq!(output, "pod \"nginx\" deleted");
    }

    #[test]
    fn test_output_force_format() {
        let mut opts = DeleteOptions {
            force: true,
            ..Default::default()
        };
        opts.resolve();
        let output = opts.format_output("pod", "nginx");
        assert_eq!(output, "pod \"nginx\" force deleted");
    }

    #[test]
    fn test_output_dry_run_format() {
        let opts = DeleteOptions {
            dry_run: true,
            ..Default::default()
        };
        let output = opts.format_output("pod", "nginx");
        assert_eq!(output, "pod \"nginx\" deleted (server dry run)");
    }

    #[test]
    fn test_combined_force_and_dry_run_query_params() {
        let mut opts = DeleteOptions {
            force: true,
            dry_run: true,
            ..Default::default()
        };
        opts.resolve();
        let params = opts.query_params();
        assert!(params.contains(&("gracePeriodSeconds".to_string(), "0".to_string())));
        assert!(params.contains(&("dryRun".to_string(), "All".to_string())));
    }

    #[test]
    fn test_delete_body_includes_kind_and_api_version() {
        let opts = DeleteOptions::default();
        let body = opts.delete_body().unwrap();
        assert_eq!(body["kind"], "DeleteOptions");
        assert_eq!(body["apiVersion"], "v1");
    }

    #[test]
    fn test_delete_body_grace_period_in_body() {
        let opts = DeleteOptions {
            grace_period: Some(60),
            ..Default::default()
        };
        let body = opts.delete_body().unwrap();
        assert_eq!(body["gracePeriodSeconds"], 60);
    }

    #[test]
    fn test_delete_body_no_grace_period_when_none() {
        let opts = DeleteOptions::default();
        let body = opts.delete_body().unwrap();
        assert!(body.get("gracePeriodSeconds").is_none());
    }

    #[test]
    fn test_resource_type_to_kind_core() {
        assert_eq!(resource_type_to_kind("pod"), "pod");
        assert_eq!(resource_type_to_kind("pods"), "pod");
        assert_eq!(resource_type_to_kind("service"), "service");
        assert_eq!(resource_type_to_kind("svc"), "service");
    }

    #[test]
    fn test_resource_type_to_kind_with_group() {
        assert_eq!(resource_type_to_kind("deployment"), "deployment.apps");
        assert_eq!(resource_type_to_kind("deploy"), "deployment.apps");
        assert_eq!(resource_type_to_kind("job"), "job.batch");
        assert_eq!(
            resource_type_to_kind("clusterrole"),
            "clusterrole.rbac.authorization.k8s.io"
        );
    }

    // ===== Additional tests for untested functions =====

    #[test]
    fn test_cascade_propagation_policy_values() {
        assert_eq!(
            CascadeStrategy::Background.propagation_policy(),
            "Background"
        );
        assert_eq!(
            CascadeStrategy::Foreground.propagation_policy(),
            "Foreground"
        );
        assert_eq!(CascadeStrategy::Orphan.propagation_policy(), "Orphan");
    }

    #[test]
    fn test_delete_options_default_fields() {
        let opts = DeleteOptions::default();
        assert_eq!(opts.grace_period, None);
        assert!(!opts.force);
        assert_eq!(opts.cascade, CascadeStrategy::Background);
        assert!(!opts.dry_run);
        assert!(!opts.wait);
        assert!(opts.output.is_none());
    }

    #[test]
    fn test_resolve_no_op_when_not_force() {
        let mut opts = DeleteOptions::default();
        opts.resolve();
        assert_eq!(opts.grace_period, None);
        assert_eq!(opts.cascade, CascadeStrategy::Background);
    }

    #[test]
    fn test_resolve_preserves_cascade_when_not_force() {
        let mut opts = DeleteOptions {
            cascade: CascadeStrategy::Orphan,
            ..Default::default()
        };
        opts.resolve();
        assert_eq!(opts.cascade, CascadeStrategy::Orphan);
    }

    #[test]
    fn test_query_params_empty_by_default() {
        let opts = DeleteOptions::default();
        let params = opts.query_params();
        assert!(params.is_empty());
    }

    #[test]
    fn test_delete_body_foreground_with_grace_period() {
        let opts = DeleteOptions {
            cascade: CascadeStrategy::Foreground,
            grace_period: Some(30),
            ..Default::default()
        };
        let body = opts.delete_body().unwrap();
        assert_eq!(body["propagationPolicy"], "Foreground");
        assert_eq!(body["gracePeriodSeconds"], 30);
        assert_eq!(body["kind"], "DeleteOptions");
        assert_eq!(body["apiVersion"], "v1");
    }

    #[test]
    fn test_format_output_force_and_dry_run_combined() {
        let opts = DeleteOptions {
            force: true,
            dry_run: true,
            ..Default::default()
        };
        let output = opts.format_output("pod", "nginx");
        assert_eq!(output, "pod \"nginx\" force deleted (server dry run)");
    }

    #[test]
    fn test_format_output_name_mode_for_all_resource_types() {
        let opts = DeleteOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        assert_eq!(opts.format_output("service", "my-svc"), "service/my-svc");
        assert_eq!(opts.format_output("svc", "my-svc"), "service/my-svc");
        assert_eq!(
            opts.format_output("statefulset", "web"),
            "statefulset.apps/web"
        );
        assert_eq!(opts.format_output("sts", "web"), "statefulset.apps/web");
        assert_eq!(
            opts.format_output("daemonset", "agent"),
            "daemonset.apps/agent"
        );
        assert_eq!(opts.format_output("ds", "agent"), "daemonset.apps/agent");
        assert_eq!(
            opts.format_output("replicaset", "web-abc"),
            "replicaset.apps/web-abc"
        );
        assert_eq!(
            opts.format_output("rs", "web-abc"),
            "replicaset.apps/web-abc"
        );
        assert_eq!(opts.format_output("job", "myjob"), "job.batch/myjob");
        assert_eq!(opts.format_output("cronjob", "cj1"), "cronjob.batch/cj1");
        assert_eq!(opts.format_output("cj", "cj1"), "cronjob.batch/cj1");
    }

    #[test]
    fn test_resource_type_to_kind_remaining_aliases() {
        assert_eq!(resource_type_to_kind("configmap"), "configmap");
        assert_eq!(resource_type_to_kind("configmaps"), "configmap");
        assert_eq!(resource_type_to_kind("cm"), "configmap");
        assert_eq!(resource_type_to_kind("secret"), "secret");
        assert_eq!(resource_type_to_kind("secrets"), "secret");
        assert_eq!(resource_type_to_kind("serviceaccount"), "serviceaccount");
        assert_eq!(resource_type_to_kind("serviceaccounts"), "serviceaccount");
        assert_eq!(resource_type_to_kind("sa"), "serviceaccount");
        assert_eq!(
            resource_type_to_kind("ingress"),
            "ingress.networking.k8s.io"
        );
        assert_eq!(
            resource_type_to_kind("ingresses"),
            "ingress.networking.k8s.io"
        );
        assert_eq!(resource_type_to_kind("ing"), "ingress.networking.k8s.io");
        assert_eq!(
            resource_type_to_kind("persistentvolumeclaim"),
            "persistentvolumeclaim"
        );
        assert_eq!(resource_type_to_kind("pvc"), "persistentvolumeclaim");
        assert_eq!(
            resource_type_to_kind("persistentvolume"),
            "persistentvolume"
        );
        assert_eq!(resource_type_to_kind("pv"), "persistentvolume");
        assert_eq!(
            resource_type_to_kind("storageclass"),
            "storageclass.storage.k8s.io"
        );
        assert_eq!(resource_type_to_kind("sc"), "storageclass.storage.k8s.io");
        assert_eq!(resource_type_to_kind("namespace"), "namespace");
        assert_eq!(resource_type_to_kind("ns"), "namespace");
        assert_eq!(resource_type_to_kind("node"), "node");
        assert_eq!(
            resource_type_to_kind("role"),
            "role.rbac.authorization.k8s.io"
        );
        assert_eq!(
            resource_type_to_kind("rolebinding"),
            "rolebinding.rbac.authorization.k8s.io"
        );
        assert_eq!(
            resource_type_to_kind("rolebindings"),
            "rolebinding.rbac.authorization.k8s.io"
        );
        assert_eq!(
            resource_type_to_kind("clusterrolebinding"),
            "clusterrolebinding.rbac.authorization.k8s.io"
        );
        assert_eq!(
            resource_type_to_kind("clusterrolebindings"),
            "clusterrolebinding.rbac.authorization.k8s.io"
        );
    }

    #[test]
    fn test_resource_type_to_kind_unknown_passthrough() {
        assert_eq!(resource_type_to_kind("customresource"), "customresource");
        assert_eq!(resource_type_to_kind("foobar"), "foobar");
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("key/value"), "key%2Fvalue");
        assert_eq!(urlencoding::encode("a&b"), "a%26b");
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
        assert_eq!(
            urlencoding::encode("safe-chars_here.ok~"),
            "safe-chars_here.ok~"
        );
        assert_eq!(urlencoding::encode("a=b,c=d"), "a=b,c=d");
        assert_eq!(urlencoding::encode("!bang"), "!bang");
        assert_eq!(urlencoding::encode(""), "");
    }

    #[test]
    fn test_format_output_default_for_various_types() {
        let opts = DeleteOptions::default();
        assert_eq!(
            opts.format_output("configmap", "cfg1"),
            "configmap \"cfg1\" deleted"
        );
        assert_eq!(
            opts.format_output("secret", "sec1"),
            "secret \"sec1\" deleted"
        );
        assert_eq!(
            opts.format_output("sa", "default"),
            "serviceaccount \"default\" deleted"
        );
        assert_eq!(
            opts.format_output("ingress", "ing1"),
            "ingress.networking.k8s.io \"ing1\" deleted"
        );
        assert_eq!(
            opts.format_output("pvc", "data"),
            "persistentvolumeclaim \"data\" deleted"
        );
        assert_eq!(
            opts.format_output("sc", "fast"),
            "storageclass.storage.k8s.io \"fast\" deleted"
        );
        assert_eq!(
            opts.format_output("ns", "test"),
            "namespace \"test\" deleted"
        );
    }

    #[test]
    fn test_format_output_non_name_output_option() {
        let opts = DeleteOptions {
            output: Some("json".to_string()),
            ..Default::default()
        };
        let output = opts.format_output("pod", "nginx");
        assert_eq!(output, "pod \"nginx\" deleted");
    }

    #[test]
    fn test_delete_body_orphan_no_grace_period() {
        let opts = DeleteOptions {
            cascade: CascadeStrategy::Orphan,
            ..Default::default()
        };
        let body = opts.delete_body().unwrap();
        assert_eq!(body["propagationPolicy"], "Orphan");
        assert!(body.get("gracePeriodSeconds").is_none());
    }

    #[test]
    fn test_cascade_from_str_case_sensitive() {
        assert!(CascadeStrategy::from_str_value("Background").is_err());
        assert!(CascadeStrategy::from_str_value("FOREGROUND").is_err());
        assert!(CascadeStrategy::from_str_value("Orphan").is_err());
    }

    // ===== 20 additional tests for untested functions =====

    fn make_test_client() -> ApiClient {
        ApiClient::new("http://127.0.0.1:1", true, None).unwrap()
    }

    #[tokio::test]
    async fn test_execute_enhanced_returns_err_on_unreachable() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = execute_enhanced(&client, "pod", "nginx", "default", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_selector_returns_err_on_unreachable() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = execute_with_selector(&client, "pod", "app=nginx", "default", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_delete_all_returns_err_on_unreachable() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = execute_delete_all(&client, "pod", "default", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_single_resource_returns_err_on_unreachable() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = delete_single_resource(&client, "pod", "nginx", Some("default"), &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_from_file_nonexistent_returns_err() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = execute_from_file(&client, "/nonexistent/file.yaml", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_resource_missing_kind_returns_err() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let value = serde_yaml::from_str::<serde_yaml::Value>("metadata:\n  name: test").unwrap();
        let result = delete_resource(&client, &value, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("kind"));
    }

    #[tokio::test]
    async fn test_delete_resource_missing_metadata_returns_err() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let value = serde_yaml::from_str::<serde_yaml::Value>("kind: Pod").unwrap();
        let result = delete_resource(&client, &value, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("metadata"));
    }

    #[tokio::test]
    async fn test_delete_resource_missing_name_returns_err() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let value =
            serde_yaml::from_str::<serde_yaml::Value>("kind: Pod\nmetadata:\n  namespace: default")
                .unwrap();
        let result = delete_resource(&client, &value, &opts).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name"));
    }

    #[tokio::test]
    async fn test_delete_resource_threads_opts_unreachable() {
        // With a valid kind/name/namespace but an unreachable server, the file
        // path now routes through the shared opts-aware core (which performs
        // discovery), so it must return an error rather than succeed.
        let client = make_test_client();
        let mut opts = DeleteOptions {
            force: true,
            dry_run: true,
            ..Default::default()
        };
        opts.resolve();
        let value = serde_yaml::from_str::<serde_yaml::Value>(
            "kind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: default",
        )
        .unwrap();
        let result = delete_resource(&client, &value, &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_enhanced_with_force_option() {
        let client = make_test_client();
        let mut opts = DeleteOptions {
            force: true,
            ..Default::default()
        };
        opts.resolve();
        assert_eq!(opts.grace_period, Some(0));
        let result = execute_enhanced(&client, "deployment", "web", "default", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_enhanced_with_dry_run_option() {
        let client = make_test_client();
        let opts = DeleteOptions {
            dry_run: true,
            ..Default::default()
        };
        let result = execute_enhanced(&client, "pod", "test", "default", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_single_resource_unsupported_type() {
        // With RestMapper, an unsupported type fails either at discovery (no server)
        // or at resolve (unknown kind). Either way it must return an error.
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result =
            delete_single_resource(&client, "foobar", "name", Some("default"), &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_selector_unsupported_type() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = execute_with_selector(&client, "foobar", "app=x", "default", &opts).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_delete_all_unsupported_type() {
        let client = make_test_client();
        let opts = DeleteOptions::default();
        let result = execute_delete_all(&client, "foobar", "default", &opts).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_options_wait_field() {
        let opts = DeleteOptions {
            wait: true,
            ..Default::default()
        };
        assert!(opts.wait);
        assert!(!opts.dry_run);
        assert!(!opts.force);
    }

    #[tokio::test]
    async fn test_execute_enhanced_with_wait_option() {
        let client = make_test_client();
        let opts = DeleteOptions {
            wait: true,
            ..Default::default()
        };
        let result = execute_enhanced(&client, "service", "my-svc", "default", &opts).await;
        assert!(result.is_err());
    }
}
