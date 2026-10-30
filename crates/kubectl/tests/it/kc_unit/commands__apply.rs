//! Unit tests moved from `src/commands/apply.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use rusternetes_client::http::ApiClient;
    pub use rusternetes_kubectl::discovery::RestMapper;
    pub use rusternetes_kubectl::types::ApplyCommands;
    pub use serde_json::{json, Value};
    pub use std::fs;
    pub use std::path::Path;
    pub use walkdir::WalkDir;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::apply::*;
    use std::fs;
    use tempfile::TempDir;

    // ---- Test 1: Multiple files ----
    #[test]
    fn test_collect_files_multiple() {
        let dir = TempDir::new().unwrap();
        let f1 = dir.path().join("a.yaml");
        let f2 = dir.path().join("b.yaml");
        fs::write(&f1, "kind: Pod").unwrap();
        fs::write(&f2, "kind: Service").unwrap();

        let inputs = vec![
            f1.to_string_lossy().to_string(),
            f2.to_string_lossy().to_string(),
        ];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files[0].ends_with("a.yaml"));
        assert!(files[1].ends_with("b.yaml"));
    }

    // ---- Test 2: Directory apply (non-recursive) ----
    #[test]
    fn test_collect_files_directory() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.yaml"), "kind: Pod").unwrap();
        fs::write(dir.path().join("b.yml"), "kind: Service").unwrap();
        fs::write(dir.path().join("c.json"), "{}").unwrap();
        fs::write(dir.path().join("d.txt"), "ignore").unwrap();

        let inputs = vec![dir.path().to_string_lossy().to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(
            files.len(),
            3,
            "should include .yaml, .yml, .json but not .txt"
        );
        // Verify .txt was excluded
        assert!(!files.iter().any(|f| f.ends_with(".txt")));
    }

    // ---- Test 3: Recursive directory ----
    #[test]
    fn test_collect_files_recursive() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("sub");
        fs::create_dir(&subdir).unwrap();
        fs::write(dir.path().join("top.yaml"), "kind: Pod").unwrap();
        fs::write(subdir.join("nested.yaml"), "kind: Service").unwrap();
        fs::write(subdir.join("skip.txt"), "not a manifest").unwrap();

        // Non-recursive should only find top-level files.
        let files_no_recurse =
            collect_files(&[dir.path().to_string_lossy().to_string()], false).unwrap();
        assert_eq!(files_no_recurse.len(), 1);

        // Recursive should find both.
        let files_recurse =
            collect_files(&[dir.path().to_string_lossy().to_string()], true).unwrap();
        assert_eq!(files_recurse.len(), 2);
    }

    // ---- Test 4: Field manager query string ----
    #[test]
    fn test_build_query_string_field_manager() {
        let options = ApplyOptions {
            server_side: true,
            field_manager: "my-manager".to_string(),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldManager=my-manager"), "qs = {}", qs);
    }

    #[test]
    fn test_build_query_string_field_manager_with_force() {
        let options = ApplyOptions {
            server_side: true,
            force: true,
            field_manager: "my-manager".to_string(),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldManager=my-manager"));
        assert!(qs.contains("force=true"));
    }

    // ---- Test 5: Output format label ----
    #[test]
    fn test_apply_result_resource_label_core() {
        let r = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "nginx".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "pod/nginx");
    }

    #[test]
    fn test_apply_result_resource_label_apps() {
        let r = ApplyResult {
            kind: "Deployment".to_string(),
            api_group: "apps".to_string(),
            name: "web".to_string(),
            action: ApplyAction::Configured,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "deployment.apps/web");
    }

    // ---- Test 6: Validate flag query string ----
    #[test]
    fn test_build_query_string_validate_strict() {
        let options = ApplyOptions {
            validate: Some("strict".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Strict"), "qs = {}", qs);
    }

    #[test]
    fn test_build_query_string_validate_warn() {
        let options = ApplyOptions {
            validate: Some("warn".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Warn"));
    }

    #[test]
    fn test_build_query_string_validate_true() {
        let options = ApplyOptions {
            validate: Some("true".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Strict"));
    }

    #[test]
    fn test_build_query_string_validate_false() {
        let options = ApplyOptions {
            validate: Some("false".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Ignore"));
    }

    // ---- Test 7: last-applied-configuration annotation ----
    #[test]
    fn test_set_last_applied_annotation() {
        let mut val = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-cm"
            },
            "data": {
                "key": "value"
            }
        });

        set_last_applied_annotation(&mut val);

        let ann = val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION]
            .as_str()
            .expect("annotation should be set");
        // The annotation should be valid JSON.
        let parsed: Value = serde_json::from_str(ann).expect("annotation should be valid JSON");
        // It should contain the resource data.
        assert_eq!(parsed["kind"], "ConfigMap");
        assert_eq!(parsed["data"]["key"], "value");
        // The annotation value itself should NOT contain the annotation (no recursion).
        assert!(
            parsed
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(LAST_APPLIED_ANNOTATION))
                .is_none(),
            "annotation should not recursively contain itself"
        );
    }

    #[test]
    fn test_set_last_applied_annotation_idempotent() {
        let mut val = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-cm",
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old-value"
                }
            },
            "data": { "key": "value" }
        });

        set_last_applied_annotation(&mut val);

        let ann = val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION]
            .as_str()
            .unwrap();
        let parsed: Value = serde_json::from_str(ann).unwrap();
        // Should reflect the current config, not the old annotation value.
        assert_eq!(parsed["data"]["key"], "value");
    }

    // ---- Existing tests ----

    #[test]
    fn test_resolve_api_path_pods() {
        let path = resolve_api_path("pod", "nginx", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/nginx");
    }

    #[test]
    fn test_resolve_api_path_deployments() {
        let path = resolve_api_path("deployment", "web", "production").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/production/deployments/web");
    }

    #[test]
    fn test_resolve_api_path_clusterrole() {
        let path = resolve_api_path("clusterrole", "admin", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/admin"
        );
    }

    #[test]
    fn test_resolve_api_path_unsupported() {
        let result = resolve_api_path("foobar", "test", "default");
        assert!(result.is_err());
    }

    #[test]
    fn test_last_applied_annotation_key() {
        assert_eq!(
            LAST_APPLIED_ANNOTATION,
            "kubectl.kubernetes.io/last-applied-configuration"
        );
    }

    // ---- Test: is_manifest_file ----
    #[test]
    fn test_is_manifest_file() {
        assert!(is_manifest_file(Path::new("foo.yaml")));
        assert!(is_manifest_file(Path::new("bar.yml")));
        assert!(is_manifest_file(Path::new("baz.json")));
        assert!(!is_manifest_file(Path::new("readme.txt")));
        assert!(!is_manifest_file(Path::new("Makefile")));
    }

    // ---- Test: default ApplyOptions ----
    #[test]
    fn test_apply_options_default() {
        let opts = ApplyOptions::default();
        assert_eq!(opts.field_manager, "kubectl-client-side-apply");
        assert!(!opts.server_side);
        assert!(!opts.recursive);
        assert!(opts.output.is_none());
        assert!(opts.validate.is_none());
    }

    // ---- Test: empty query string when no options ----
    #[test]
    fn test_build_query_string_empty() {
        let options = ApplyOptions::default();
        let qs = build_query_string(&options);
        assert_eq!(qs, "");
    }

    // ---- Test: stdin token ----
    #[test]
    fn test_collect_files_stdin() {
        let inputs = vec!["-".to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files, vec!["-"]);
    }

    // ---- Test: nonexistent path ----
    #[test]
    fn test_collect_files_nonexistent() {
        let result = collect_files(&["/nonexistent/path/xyz".to_string()], false);
        assert!(result.is_err());
    }

    // ---- Test: dry run server query ----
    #[test]
    fn test_build_query_string_dry_run_server() {
        let options = ApplyOptions {
            dry_run: Some("server".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("dryRun=All"));
    }

    #[test]
    fn test_build_query_string_dry_run_client() {
        let options = ApplyOptions {
            dry_run: Some("client".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(!qs.contains("dryRun"));
    }

    #[test]
    fn test_build_query_string_dry_run_none() {
        let options = ApplyOptions {
            dry_run: Some("none".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(!qs.contains("dryRun"));
    }

    #[test]
    fn test_build_query_string_combined() {
        let options = ApplyOptions {
            server_side: true,
            force: true,
            field_manager: "test-mgr".to_string(),
            dry_run: Some("server".to_string()),
            validate: Some("strict".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.starts_with("?"));
        assert!(qs.contains("dryRun=All"));
        assert!(qs.contains("fieldManager=test-mgr"));
        assert!(qs.contains("force=true"));
        assert!(qs.contains("fieldValidation=Strict"));
    }

    #[test]
    fn test_build_query_string_validate_ignore() {
        let options = ApplyOptions {
            validate: Some("ignore".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Ignore"));
    }

    #[test]
    fn test_build_query_string_validate_warn_capitalized() {
        let options = ApplyOptions {
            validate: Some("Warn".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Warn"));
    }

    #[test]
    fn test_build_query_string_validate_strict_capitalized() {
        let options = ApplyOptions {
            validate: Some("Strict".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=Strict"));
    }

    #[test]
    fn test_build_query_string_validate_unknown() {
        let options = ApplyOptions {
            validate: Some("custom".to_string()),
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldValidation=custom"));
    }

    #[test]
    fn test_strip_last_applied_removes_annotation() {
        let val = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old-config",
                    "other": "keep"
                }
            }
        });
        let clean = strip_last_applied(&val);
        let annotations = clean["metadata"]["annotations"].as_object().unwrap();
        assert!(!annotations.contains_key(LAST_APPLIED_ANNOTATION));
        assert_eq!(annotations["other"], "keep");
    }

    #[test]
    fn test_strip_last_applied_removes_empty_annotations() {
        let val = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old-config"
                }
            }
        });
        let clean = strip_last_applied(&val);
        assert!(clean["metadata"].get("annotations").is_none());
    }

    #[test]
    fn test_strip_last_applied_no_annotations() {
        let val = json!({
            "metadata": {
                "name": "test"
            }
        });
        let clean = strip_last_applied(&val);
        assert_eq!(clean["metadata"]["name"], "test");
    }

    #[test]
    fn test_format_output_default_created() {
        let result = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "nginx".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        let options = ApplyOptions::default();
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_default_configured() {
        let result = ApplyResult {
            kind: "Deployment".to_string(),
            api_group: "apps".to_string(),
            name: "web".to_string(),
            action: ApplyAction::Configured,
            response: json!({}),
        };
        let options = ApplyOptions::default();
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_json() {
        let result = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "test".to_string(),
            action: ApplyAction::Created,
            response: json!({"kind": "Pod", "metadata": {"name": "test"}}),
        };
        let options = ApplyOptions {
            output: Some("json".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_yaml() {
        let result = ApplyResult {
            kind: "Pod".to_string(),
            api_group: "".to_string(),
            name: "test".to_string(),
            action: ApplyAction::Created,
            response: json!({"kind": "Pod"}),
        };
        let options = ApplyOptions {
            output: Some("yaml".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_name() {
        let result = ApplyResult {
            kind: "Service".to_string(),
            api_group: "".to_string(),
            name: "frontend".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        let options = ApplyOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_apply_action_eq() {
        assert_eq!(ApplyAction::Created, ApplyAction::Created);
        assert_eq!(ApplyAction::Configured, ApplyAction::Configured);
        assert_ne!(ApplyAction::Created, ApplyAction::Configured);
    }

    #[test]
    fn test_resource_label_rbac() {
        let r = ApplyResult {
            kind: "ClusterRole".to_string(),
            api_group: "rbac.authorization.k8s.io".to_string(),
            name: "admin".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(
            r.resource_label(),
            "clusterrole.rbac.authorization.k8s.io/admin"
        );
    }

    #[test]
    fn test_resource_label_namespace() {
        let r = ApplyResult {
            kind: "Namespace".to_string(),
            api_group: "".to_string(),
            name: "test-ns".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "namespace/test-ns");
    }

    #[test]
    fn test_resolve_api_path_services() {
        let path = resolve_api_path("svc", "my-svc", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/services/my-svc");
    }

    #[test]
    fn test_resolve_api_path_statefulsets() {
        let path = resolve_api_path("sts", "my-sts", "default").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/statefulsets/my-sts");
    }

    #[test]
    fn test_resolve_api_path_daemonsets() {
        let path = resolve_api_path("ds", "my-ds", "default").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/daemonsets/my-ds");
    }

    #[test]
    fn test_resolve_api_path_configmaps() {
        let path = resolve_api_path("cm", "my-cm", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/configmaps/my-cm");
    }

    #[test]
    fn test_resolve_api_path_secrets() {
        let path = resolve_api_path("secrets", "my-secret", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/secrets/my-secret");
    }

    #[test]
    fn test_resolve_api_path_serviceaccounts() {
        let path = resolve_api_path("sa", "my-sa", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/serviceaccounts/my-sa");
    }

    #[test]
    fn test_resolve_api_path_jobs() {
        let path = resolve_api_path("jobs", "my-job", "default").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/jobs/my-job");
    }

    #[test]
    fn test_resolve_api_path_cronjobs() {
        let path = resolve_api_path("cj", "my-cj", "default").unwrap();
        assert_eq!(path, "/apis/batch/v1/namespaces/default/cronjobs/my-cj");
    }

    #[test]
    fn test_resolve_api_path_roles() {
        let path = resolve_api_path("roles", "my-role", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/roles/my-role"
        );
    }

    #[test]
    fn test_resolve_api_path_rolebindings() {
        let path = resolve_api_path("rolebindings", "my-rb", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings/my-rb"
        );
    }

    #[test]
    fn test_resolve_api_path_clusterrolebindings() {
        let path = resolve_api_path("clusterrolebindings", "my-crb", "default").unwrap();
        assert_eq!(
            path,
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/my-crb"
        );
    }

    #[test]
    fn test_resolve_api_path_namespaces() {
        let path = resolve_api_path("ns", "test-ns", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/test-ns");
    }

    #[test]
    fn test_resolve_api_path_nodes() {
        let path = resolve_api_path("nodes", "node-1", "default").unwrap();
        assert_eq!(path, "/api/v1/nodes/node-1");
    }

    #[test]
    fn test_is_manifest_file_no_extension() {
        assert!(!is_manifest_file(Path::new("Dockerfile")));
    }

    #[test]
    fn test_is_manifest_file_hidden_yaml() {
        assert!(is_manifest_file(Path::new(".hidden.yaml")));
    }

    #[test]
    fn test_collect_files_empty_input() {
        let result = collect_files(&[], false).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_set_last_applied_annotation_creates_metadata() {
        let mut val = json!({
            "kind": "ConfigMap",
            "metadata": {}
        });
        set_last_applied_annotation(&mut val);
        assert!(val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION].is_string());
    }

    #[test]
    fn test_build_query_string_no_server_side() {
        let options = ApplyOptions {
            server_side: false,
            force: true,
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(!qs.contains("fieldManager"));
        assert!(!qs.contains("force"));
    }

    #[test]
    fn test_apply_options_custom() {
        let opts = ApplyOptions {
            files: vec!["test.yaml".to_string()],
            namespace: Some("prod".to_string()),
            dry_run: Some("server".to_string()),
            server_side: true,
            force: true,
            recursive: true,
            field_manager: "custom-manager".to_string(),
            output: Some("json".to_string()),
            validate: Some("strict".to_string()),
        };
        assert_eq!(opts.files.len(), 1);
        assert_eq!(opts.namespace, Some("prod".to_string()));
        assert!(opts.server_side);
        assert!(opts.force);
        assert!(opts.recursive);
        assert_eq!(opts.field_manager, "custom-manager");
    }

    // --- 21 additional tests below ---

    #[test]
    fn test_resolve_api_path_pod_alias_po() {
        let path = resolve_api_path("po", "my-pod", "default").unwrap();
        assert_eq!(path, "/api/v1/namespaces/default/pods/my-pod");
    }

    #[test]
    fn test_resolve_api_path_deploy_alias() {
        let path = resolve_api_path("deploy", "web", "default").unwrap();
        assert_eq!(path, "/apis/apps/v1/namespaces/default/deployments/web");
    }

    #[test]
    fn test_resolve_api_path_ingress() {
        // ingress is not supported in resolve_api_path
        let result = resolve_api_path("ingress", "my-ing", "default");
        assert!(result.is_err());
    }

    #[test]
    fn test_is_manifest_file_uppercase() {
        // The function only matches lowercase extensions
        assert!(!is_manifest_file(Path::new("FILE.YAML")));
    }

    #[test]
    fn test_is_manifest_file_json() {
        assert!(is_manifest_file(Path::new("config.json")));
    }

    #[test]
    fn test_is_manifest_file_yml() {
        assert!(is_manifest_file(Path::new("deploy.yml")));
    }

    #[test]
    fn test_strip_last_applied_preserves_other_fields() {
        let val = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test",
                "labels": {"app": "web"},
                "annotations": {
                    LAST_APPLIED_ANNOTATION: "old",
                    "other-ann": "keep-me"
                }
            },
            "spec": {"containers": []}
        });
        let clean = strip_last_applied(&val);
        assert_eq!(clean["apiVersion"], "v1");
        assert_eq!(clean["kind"], "Pod");
        assert_eq!(clean["metadata"]["labels"]["app"], "web");
        assert_eq!(clean["metadata"]["annotations"]["other-ann"], "keep-me");
    }

    #[test]
    fn test_strip_last_applied_no_metadata() {
        let val = json!({"kind": "Pod"});
        let clean = strip_last_applied(&val);
        assert_eq!(clean["kind"], "Pod");
    }

    #[test]
    fn test_set_last_applied_annotation_preserves_existing_annotations() {
        let mut val = json!({
            "kind": "ConfigMap",
            "metadata": {
                "name": "test",
                "annotations": {
                    "custom/annotation": "keep"
                }
            }
        });
        set_last_applied_annotation(&mut val);
        assert_eq!(val["metadata"]["annotations"]["custom/annotation"], "keep");
        assert!(val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION].is_string());
    }

    #[test]
    fn test_format_output_configured_action() {
        let result = ApplyResult {
            kind: "ConfigMap".to_string(),
            api_group: "".to_string(),
            name: "my-cm".to_string(),
            action: ApplyAction::Configured,
            response: json!({}),
        };
        let options = ApplyOptions::default();
        // Should not panic
        format_output(&result, &options);
    }

    #[test]
    fn test_format_output_name_output() {
        let result = ApplyResult {
            kind: "Deployment".to_string(),
            api_group: "apps".to_string(),
            name: "web".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        let options = ApplyOptions {
            output: Some("name".to_string()),
            ..Default::default()
        };
        format_output(&result, &options);
    }

    #[test]
    fn test_resource_label_batch() {
        let r = ApplyResult {
            kind: "Job".to_string(),
            api_group: "batch".to_string(),
            name: "my-job".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "job.batch/my-job");
    }

    #[test]
    fn test_resource_label_core_service() {
        let r = ApplyResult {
            kind: "Service".to_string(),
            api_group: "".to_string(),
            name: "frontend".to_string(),
            action: ApplyAction::Created,
            response: json!({}),
        };
        assert_eq!(r.resource_label(), "service/frontend");
    }

    #[test]
    fn test_apply_options_default_field_manager() {
        let opts = ApplyOptions::default();
        assert_eq!(opts.field_manager, "kubectl-client-side-apply");
    }

    #[test]
    fn test_apply_options_default_force_false() {
        let opts = ApplyOptions::default();
        assert!(!opts.force);
    }

    #[test]
    fn test_apply_options_default_dry_run_none() {
        let opts = ApplyOptions::default();
        assert!(opts.dry_run.is_none());
    }

    #[test]
    fn test_collect_files_single_file() {
        let dir = TempDir::new().unwrap();
        let f = dir.path().join("pod.yaml");
        fs::write(&f, "kind: Pod").unwrap();
        let inputs = vec![f.to_string_lossy().to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_collect_files_directory_excludes_subdirs_nonrecursive() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();
        fs::write(dir.path().join("top.yaml"), "kind: Pod").unwrap();
        fs::write(subdir.join("sub.yaml"), "kind: Service").unwrap();
        let inputs = vec![dir.path().to_string_lossy().to_string()];
        let files = collect_files(&inputs, false).unwrap();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn test_build_query_string_server_side_default_manager() {
        let options = ApplyOptions {
            server_side: true,
            ..Default::default()
        };
        let qs = build_query_string(&options);
        assert!(qs.contains("fieldManager=kubectl-client-side-apply"));
    }

    #[test]
    fn test_resolve_api_path_secret_alias() {
        let path = resolve_api_path("secret", "my-secret", "ns1").unwrap();
        assert_eq!(path, "/api/v1/namespaces/ns1/secrets/my-secret");
    }

    /// Replaced test_has_metadata_trait_pod: the typed HasMetadata machinery is
    /// gone. This test covers the equivalent behavior in the new Value-based
    /// apply path: set_last_applied_annotation operates on a pod-shaped JSON
    /// Value and preserves all metadata fields while injecting the annotation.
    #[test]
    fn test_set_last_applied_annotation_on_pod_shaped_value() {
        let mut val = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test",
                "namespace": "default"
            },
            "spec": {"containers": []}
        });
        set_last_applied_annotation(&mut val);

        // The annotation must be present and valid JSON.
        let ann = val["metadata"]["annotations"][LAST_APPLIED_ANNOTATION]
            .as_str()
            .expect("annotation must be set on pod-shaped value");
        let parsed: Value = serde_json::from_str(ann).expect("annotation must be valid JSON");

        // The stored config must reflect the original resource.
        assert_eq!(parsed["kind"], "Pod");
        assert_eq!(parsed["metadata"]["name"], "test");
        // The annotation must NOT recursively contain itself.
        assert!(
            parsed
                .pointer("/metadata/annotations/kubectl.kubernetes.io~1last-applied-configuration")
                .is_none(),
            "annotation must not self-reference"
        );
    }
}
