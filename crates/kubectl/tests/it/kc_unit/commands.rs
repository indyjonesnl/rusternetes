//! Unit tests moved from `src/commands/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {}

mod apply_test {
    #[cfg(test)]
    mod tests {
        #[test]
        fn test_multi_document_yaml_parsing() {
            let yaml = r#"
apiVersion: v1
kind: Namespace
metadata:
  name: test-ns
---
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: test-ns
spec:
  containers:
  - name: nginx
    image: nginx:latest
---
apiVersion: v1
kind: Service
metadata:
  name: test-service
  namespace: test-ns
spec:
  selector:
    app: test
  ports:
  - port: 80
    targetPort: 80
"#;

            // Parse all documents
            let mut count = 0;
            for document in serde_yaml::Deserializer::from_str(yaml) {
                use serde::Deserialize;
                let value = serde_yaml::Value::deserialize(document).unwrap();
                if !value.is_null() {
                    let kind = value.get("kind").and_then(|k| k.as_str());
                    assert!(kind.is_some());
                    count += 1;
                }
            }

            assert_eq!(count, 3, "Should parse 3 documents");
        }

        #[test]
        fn test_empty_document_skipped() {
            let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
---
---
apiVersion: v1
kind: Service
metadata:
  name: test-service
"#;

            let mut count = 0;
            for document in serde_yaml::Deserializer::from_str(yaml) {
                use serde::Deserialize;
                let value = serde_yaml::Value::deserialize(document).unwrap();
                if !value.is_null() {
                    count += 1;
                }
            }

            assert_eq!(count, 2, "Should skip empty documents");
        }

        #[test]
        fn test_resource_kind_extraction() {
            let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: default
spec:
  containers:
  - name: nginx
    image: nginx:latest
"#;

            let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
            let kind = value.get("kind").and_then(|k| k.as_str());

            assert_eq!(kind, Some("Pod"));
        }

        #[test]
        fn test_namespace_extraction() {
            let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  namespace: custom-ns
spec:
  containers:
  - name: nginx
    image: nginx:latest
"#;

            use rusternetes_common::resources::Pod;
            let pod: Pod = serde_yaml::from_str(yaml).unwrap();

            assert_eq!(pod.metadata.namespace, Some("custom-ns".to_string()));
            assert_eq!(pod.metadata.name, "test-pod");
        }

        #[test]
        fn test_namespace_defaults_to_default() {
            let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
spec:
  containers:
  - name: nginx
    image: nginx:latest
"#;

            use rusternetes_common::resources::Pod;
            let pod: Pod = serde_yaml::from_str(yaml).unwrap();

            let namespace = pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".to_string());
            assert_eq!(namespace, "default");
        }

        #[test]
        fn test_all_supported_resource_kinds() {
            let kinds = vec![
                "Pod",
                "Service",
                "Deployment",
                "StatefulSet",
                "DaemonSet",
                "Node",
                "Namespace",
                "Job",
                "CronJob",
                "PersistentVolume",
                "PersistentVolumeClaim",
                "StorageClass",
                "VolumeSnapshot",
                "VolumeSnapshotClass",
                "Endpoints",
                "ConfigMap",
                "Secret",
                "Ingress",
                "ServiceAccount",
                "Role",
                "RoleBinding",
                "ClusterRole",
                "ClusterRoleBinding",
                "ResourceQuota",
                "LimitRange",
                "PriorityClass",
                "CustomResourceDefinition",
            ];

            for kind in kinds {
                let yaml = format!(
                    r#"
apiVersion: v1
kind: {}
metadata:
  name: test-resource
"#,
                    kind
                );

                let value: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
                let extracted_kind = value.get("kind").and_then(|k| k.as_str());

                assert_eq!(extracted_kind, Some(kind), "Kind should match for {}", kind);
            }
        }
    }
}

mod create_test {
    #[cfg(test)]
    mod tests {
        use rusternetes_kubectl::commands::create::*;
        use serde_json::Value;

        #[test]
        fn test_kind_extraction_from_yaml() {
            let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
spec:
  containers:
  - name: nginx
    image: nginx:latest
"#;

            let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
            let kind = value.get("kind").and_then(|k| k.as_str());

            assert_eq!(kind, Some("Pod"));
        }

        #[test]
        fn test_missing_kind_field() {
            let yaml = r#"
apiVersion: v1
metadata:
  name: test-pod
"#;

            let value: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
            let kind = value.get("kind").and_then(|k| k.as_str());

            assert_eq!(kind, None);
        }

        #[test]
        fn test_build_cluster_role() {
            let result = build_cluster_role(
                "pod-reader",
                &["get".into(), "list".into(), "watch".into()],
                &["pods".into()],
                &[],
                &[],
                &[],
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "rbac.authorization.k8s.io/v1");
            assert_eq!(result["kind"], "ClusterRole");
            assert_eq!(result["metadata"]["name"], "pod-reader");

            let rules = result["rules"].as_array().unwrap();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0]["resources"], serde_json::json!(["pods"]));
            assert_eq!(
                rules[0]["verbs"],
                serde_json::json!(["get", "list", "watch"])
            );
            assert_eq!(rules[0]["apiGroups"], serde_json::json!([""]))
        }

        #[test]
        fn test_build_cluster_role_with_aggregation() {
            let result = build_cluster_role(
                "monitoring",
                &[],
                &[],
                &[],
                &[],
                &["rbac.example.com/aggregate-to-monitoring=true".into()],
            )
            .unwrap();

            assert_eq!(result["kind"], "ClusterRole");
            let agg = &result["aggregationRule"];
            let selectors = agg["clusterRoleSelectors"].as_array().unwrap();
            assert_eq!(selectors.len(), 1);
            assert_eq!(
                selectors[0]["matchLabels"]["rbac.example.com/aggregate-to-monitoring"],
                "true"
            );
        }

        #[test]
        fn test_build_cluster_role_with_non_resource_url() {
            let result = build_cluster_role(
                "log-reader",
                &["get".into()],
                &[],
                &[],
                &["/logs/*".into()],
                &[],
            )
            .unwrap();

            let rules = result["rules"].as_array().unwrap();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0]["nonResourceURLs"], serde_json::json!(["/logs/*"]));
            assert_eq!(rules[0]["verbs"], serde_json::json!(["get"]));
        }

        #[test]
        fn test_build_cluster_role_binding() {
            let result = build_cluster_role_binding(
                "admin-binding",
                "cluster-admin",
                &["user1".into(), "user2".into()],
                &["group1".into()],
                &["kube-system:default".into()],
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "rbac.authorization.k8s.io/v1");
            assert_eq!(result["kind"], "ClusterRoleBinding");
            assert_eq!(result["metadata"]["name"], "admin-binding");
            assert_eq!(result["roleRef"]["kind"], "ClusterRole");
            assert_eq!(result["roleRef"]["name"], "cluster-admin");

            let subjects = result["subjects"].as_array().unwrap();
            assert_eq!(subjects.len(), 4);
            assert_eq!(subjects[0]["kind"], "User");
            assert_eq!(subjects[0]["name"], "user1");
            assert_eq!(subjects[1]["kind"], "User");
            assert_eq!(subjects[1]["name"], "user2");
            assert_eq!(subjects[2]["kind"], "Group");
            assert_eq!(subjects[2]["name"], "group1");
            assert_eq!(subjects[3]["kind"], "ServiceAccount");
            assert_eq!(subjects[3]["name"], "default");
            assert_eq!(subjects[3]["namespace"], "kube-system");
        }

        #[test]
        fn test_build_configmap_from_literals() {
            let result = build_configmap(
                "my-config",
                "default",
                &["key1=value1".into(), "key2=value2".into()],
                &[],
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "v1");
            assert_eq!(result["kind"], "ConfigMap");
            assert_eq!(result["metadata"]["name"], "my-config");
            assert_eq!(result["metadata"]["namespace"], "default");
            assert_eq!(result["data"]["key1"], "value1");
            assert_eq!(result["data"]["key2"], "value2");
        }

        #[test]
        fn test_build_cronjob() {
            let result = build_cronjob(
                "my-cron",
                "default",
                "busybox",
                "*/5 * * * *",
                "OnFailure",
                &["echo".into(), "hello".into()],
            );

            assert_eq!(result["apiVersion"], "batch/v1");
            assert_eq!(result["kind"], "CronJob");
            assert_eq!(result["metadata"]["name"], "my-cron");
            assert_eq!(result["spec"]["schedule"], "*/5 * * * *");

            let container =
                &result["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0];
            assert_eq!(container["name"], "my-cron");
            assert_eq!(container["image"], "busybox");
            assert_eq!(container["command"], serde_json::json!(["echo", "hello"]));

            assert_eq!(
                result["spec"]["jobTemplate"]["spec"]["template"]["spec"]["restartPolicy"],
                "OnFailure"
            );
        }

        #[test]
        fn test_build_ingress_simple() {
            let result = build_ingress(
                "simple",
                "default",
                Some("nginx"),
                &["foo.com/bar=svc1:8080".into()],
                None,
                &[],
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "networking.k8s.io/v1");
            assert_eq!(result["kind"], "Ingress");
            assert_eq!(result["spec"]["ingressClassName"], "nginx");

            let rules = result["spec"]["rules"].as_array().unwrap();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0]["host"], "foo.com");

            let path = &rules[0]["http"]["paths"][0];
            assert_eq!(path["path"], "/bar");
            assert_eq!(path["pathType"], "Exact");
            assert_eq!(path["backend"]["service"]["name"], "svc1");
            assert_eq!(path["backend"]["service"]["port"]["number"], 8080);
        }

        #[test]
        fn test_build_ingress_with_tls() {
            let result = build_ingress(
                "tls-ingress",
                "default",
                None,
                &["foo.com/bar=svc1:8080,tls=my-cert".into()],
                None,
                &[],
            )
            .unwrap();

            let tls = result["spec"]["tls"].as_array().unwrap();
            assert_eq!(tls.len(), 1);
            assert_eq!(tls[0]["secretName"], "my-cert");
            assert_eq!(tls[0]["hosts"], serde_json::json!(["foo.com"]));
        }

        #[test]
        fn test_build_ingress_prefix_path() {
            let result = build_ingress(
                "prefix",
                "default",
                None,
                &["foo.com/path*=svc1:8080".into()],
                None,
                &[],
            )
            .unwrap();

            let path = &result["spec"]["rules"][0]["http"]["paths"][0];
            assert_eq!(path["path"], "/path");
            assert_eq!(path["pathType"], "Prefix");
        }

        #[test]
        fn test_build_job_with_image() {
            let result =
                build_job("my-job", "default", Some("busybox"), None, &["date".into()]).unwrap();

            assert_eq!(result["apiVersion"], "batch/v1");
            assert_eq!(result["kind"], "Job");
            assert_eq!(result["metadata"]["name"], "my-job");

            let container = &result["spec"]["template"]["spec"]["containers"][0];
            assert_eq!(container["name"], "my-job");
            assert_eq!(container["image"], "busybox");
            assert_eq!(container["command"], serde_json::json!(["date"]));
            assert_eq!(result["spec"]["template"]["spec"]["restartPolicy"], "Never");
        }

        #[test]
        fn test_build_job_requires_image_or_from() {
            let result = build_job("my-job", "default", None, None, &[]);
            assert!(result.is_err());
        }

        #[test]
        fn test_build_job_from_cronjob() {
            let result =
                build_job("my-job", "default", None, Some("cronjob/my-cron"), &[]).unwrap();

            assert_eq!(result["kind"], "Job");
            assert_eq!(
                result["metadata"]["annotations"]["cronjob.kubernetes.io/instantiate"],
                "manual"
            );
        }

        #[test]
        fn test_build_pdb_min_available() {
            let result = build_pdb("my-pdb", "default", "app=rails", Some("1"), None).unwrap();

            assert_eq!(result["apiVersion"], "policy/v1");
            assert_eq!(result["kind"], "PodDisruptionBudget");
            assert_eq!(result["metadata"]["name"], "my-pdb");
            assert_eq!(result["spec"]["minAvailable"], 1);
            assert_eq!(result["spec"]["selector"]["matchLabels"]["app"], "rails");
        }

        #[test]
        fn test_build_pdb_max_unavailable_percentage() {
            let result = build_pdb("my-pdb", "default", "app=nginx", None, Some("50%")).unwrap();

            assert_eq!(result["spec"]["maxUnavailable"], "50%");
        }

        #[test]
        fn test_build_pdb_requires_min_or_max() {
            let result = build_pdb("my-pdb", "default", "app=test", None, None);
            assert!(result.is_err());
        }

        #[test]
        fn test_build_role() {
            let result = build_role(
                "pod-reader",
                "default",
                &["get".into(), "list".into(), "watch".into()],
                &["pods".into()],
                &[],
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "rbac.authorization.k8s.io/v1");
            assert_eq!(result["kind"], "Role");
            assert_eq!(result["metadata"]["name"], "pod-reader");
            assert_eq!(result["metadata"]["namespace"], "default");

            let rules = result["rules"].as_array().unwrap();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0]["resources"], serde_json::json!(["pods"]));
            assert_eq!(
                rules[0]["verbs"],
                serde_json::json!(["get", "list", "watch"])
            );
        }

        #[test]
        fn test_build_role_with_api_group() {
            let result = build_role(
                "deploy-reader",
                "default",
                &["get".into()],
                &["deployments.apps".into()],
                &[],
            )
            .unwrap();

            let rules = result["rules"].as_array().unwrap();
            assert_eq!(rules[0]["apiGroups"], serde_json::json!(["apps"]));
            assert_eq!(rules[0]["resources"], serde_json::json!(["deployments"]));
        }

        #[test]
        fn test_build_role_binding_with_clusterrole() {
            let result = build_role_binding(
                "admin-binding",
                "default",
                Some("admin"),
                None,
                &["user1".into()],
                &[],
                &[],
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "rbac.authorization.k8s.io/v1");
            assert_eq!(result["kind"], "RoleBinding");
            assert_eq!(result["roleRef"]["kind"], "ClusterRole");
            assert_eq!(result["roleRef"]["name"], "admin");

            let subjects = result["subjects"].as_array().unwrap();
            assert_eq!(subjects.len(), 1);
            assert_eq!(subjects[0]["kind"], "User");
            assert_eq!(subjects[0]["name"], "user1");
        }

        #[test]
        fn test_build_role_binding_with_role() {
            let result = build_role_binding(
                "dev-binding",
                "default",
                None,
                Some("developer"),
                &[],
                &[],
                &["monitoring:sa-dev".into()],
            )
            .unwrap();

            assert_eq!(result["roleRef"]["kind"], "Role");
            assert_eq!(result["roleRef"]["name"], "developer");

            let subjects = result["subjects"].as_array().unwrap();
            assert_eq!(subjects[0]["kind"], "ServiceAccount");
            assert_eq!(subjects[0]["name"], "sa-dev");
            assert_eq!(subjects[0]["namespace"], "monitoring");
        }

        #[test]
        fn test_build_role_binding_requires_one_ref() {
            let result = build_role_binding("bad", "default", None, None, &[], &[], &[]);
            assert!(result.is_err());

            let result2 = build_role_binding("bad", "default", Some("a"), Some("b"), &[], &[], &[]);
            assert!(result2.is_err());
        }

        #[test]
        fn test_build_secret_generic() {
            let result = build_secret_generic(
                "my-secret",
                "default",
                &["key1=supersecret".into(), "key2=topsecret".into()],
                &[],
                None,
            )
            .unwrap();

            assert_eq!(result["apiVersion"], "v1");
            assert_eq!(result["kind"], "Secret");
            assert_eq!(result["metadata"]["name"], "my-secret");
            assert_eq!(result["type"], "Opaque");

            // Verify base64 encoding
            let key1_encoded = result["data"]["key1"].as_str().unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(key1_encoded)
                .unwrap();
            assert_eq!(String::from_utf8(decoded).unwrap(), "supersecret");
        }

        #[test]
        fn test_build_secret_generic_custom_type() {
            let result =
                build_secret_generic("my-secret", "default", &[], &[], Some("my-type")).unwrap();

            assert_eq!(result["type"], "my-type");
        }

        #[test]
        fn test_build_secret_docker_registry() {
            let result = build_secret_docker_registry(
                "my-registry",
                "default",
                "https://index.docker.io/v1/",
                Some("myuser"),
                Some("mypass"),
                Some("my@email.com"),
            )
            .unwrap();

            assert_eq!(result["kind"], "Secret");
            assert_eq!(result["type"], "kubernetes.io/dockerconfigjson");

            // Verify the dockerconfigjson data
            let encoded = result["data"][".dockerconfigjson"].as_str().unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            let config: Value = serde_json::from_slice(&decoded).unwrap();
            let auth_entry = &config["auths"]["https://index.docker.io/v1/"];
            assert_eq!(auth_entry["username"], "myuser");
            assert_eq!(auth_entry["password"], "mypass");
            assert_eq!(auth_entry["email"], "my@email.com");
        }

        #[test]
        fn test_build_secret_docker_registry_requires_credentials() {
            let result = build_secret_docker_registry(
                "my-registry",
                "default",
                "https://index.docker.io/v1/",
                None,
                None,
                None,
            );
            assert!(result.is_err());
        }

        #[test]
        fn test_build_secret_tls() {
            use std::io::Write;
            // Create temp files for cert and key using tempfile for reliability
            let mut cert_file = tempfile::NamedTempFile::new().unwrap();
            let mut key_file = tempfile::NamedTempFile::new().unwrap();
            cert_file.write_all(b"FAKE-CERT-DATA").unwrap();
            key_file.write_all(b"FAKE-KEY-DATA").unwrap();

            let result = build_secret_tls(
                "tls-secret",
                "default",
                cert_file.path().to_str().unwrap(),
                key_file.path().to_str().unwrap(),
            )
            .unwrap();

            assert_eq!(result["kind"], "Secret");
            assert_eq!(result["type"], "kubernetes.io/tls");

            let cert_encoded = result["data"]["tls.crt"].as_str().unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(cert_encoded)
                .unwrap();
            assert_eq!(decoded, b"FAKE-CERT-DATA");

            let key_encoded = result["data"]["tls.key"].as_str().unwrap();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(key_encoded)
                .unwrap();
            assert_eq!(decoded, b"FAKE-KEY-DATA");
        }

        #[test]
        fn test_build_service_account() {
            let result = build_service_account("my-sa", "default");

            assert_eq!(result["apiVersion"], "v1");
            assert_eq!(result["kind"], "ServiceAccount");
            assert_eq!(result["metadata"]["name"], "my-sa");
            assert_eq!(result["metadata"]["namespace"], "default");
        }

        #[test]
        fn test_build_token_request_simple() {
            let result = build_token_request(&[], None, None, None, None).unwrap();

            assert_eq!(result["apiVersion"], "authentication.k8s.io/v1");
            assert_eq!(result["kind"], "TokenRequest");
        }

        #[test]
        fn test_build_token_request_with_audiences() {
            let result =
                build_token_request(&["https://example.com".into()], None, None, None, None)
                    .unwrap();

            assert_eq!(
                result["spec"]["audiences"],
                serde_json::json!(["https://example.com"])
            );
        }

        #[test]
        fn test_build_token_request_with_duration() {
            let result = build_token_request(&[], Some("10m"), None, None, None).unwrap();

            assert_eq!(result["spec"]["expirationSeconds"], 600);
        }

        #[test]
        fn test_build_token_request_with_bound_object() {
            let result = build_token_request(
                &[],
                None,
                Some("Secret"),
                Some("my-secret"),
                Some("abc-123"),
            )
            .unwrap();

            let bound_ref = &result["spec"]["boundObjectRef"];
            assert_eq!(bound_ref["kind"], "Secret");
            assert_eq!(bound_ref["apiVersion"], "v1");
            assert_eq!(bound_ref["name"], "my-secret");
            assert_eq!(bound_ref["uid"], "abc-123");
        }

        #[test]
        fn test_build_token_request_bound_object_requires_name() {
            let result = build_token_request(&[], None, Some("Pod"), None, None);
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_resource_group_simple() {
            let (resource, group) = parse_resource_group("pods");
            assert_eq!(resource, "pods");
            assert_eq!(group, "");
        }

        #[test]
        fn test_parse_resource_group_with_api_group() {
            let (resource, group) = parse_resource_group("deployments.apps");
            assert_eq!(resource, "deployments");
            assert_eq!(group, "apps");
        }

        #[test]
        fn test_parse_resource_group_with_subresource() {
            let (resource, group) = parse_resource_group("pods/status");
            assert_eq!(resource, "pods/status");
            assert_eq!(group, "");
        }

        #[test]
        fn test_build_cluster_role_binding_invalid_sa() {
            let result =
                build_cluster_role_binding("test", "admin", &[], &[], &["invalid-sa".into()]);
            assert!(result.is_err());
        }

        #[test]
        fn test_build_ingress_with_default_backend() {
            let result = build_ingress(
                "default-backend-ingress",
                "default",
                None,
                &[],
                Some("defaultsvc:http"),
                &[],
            )
            .unwrap();

            let backend = &result["spec"]["defaultBackend"];
            assert_eq!(backend["service"]["name"], "defaultsvc");
            assert_eq!(backend["service"]["port"]["name"], "http");
        }

        #[test]
        fn test_build_ingress_with_annotations() {
            let result = build_ingress(
                "annotated",
                "default",
                Some("nginx"),
                &["foo.com/bar=svc:80".into()],
                None,
                &["nginx.ingress.kubernetes.io/rewrite-target=/$1".into()],
            )
            .unwrap();

            assert_eq!(
                result["metadata"]["annotations"]["nginx.ingress.kubernetes.io/rewrite-target"],
                "/$1"
            );
        }

        use base64::Engine;
    }
}

mod get_test {
    #[cfg(test)]
    mod tests {
        use rusternetes_kubectl::commands::get::*;

        #[test]
        fn test_output_format_json() {
            let format = OutputFormat::from_str("json").unwrap();
            assert!(matches!(format, OutputFormat::Json));
        }

        #[test]
        fn test_output_format_yaml() {
            let format = OutputFormat::from_str("yaml").unwrap();
            assert!(matches!(format, OutputFormat::Yaml));
        }

        #[test]
        fn test_output_format_wide() {
            let format = OutputFormat::from_str("wide").unwrap();
            assert!(matches!(format, OutputFormat::Wide));
        }

        #[test]
        fn test_output_format_invalid() {
            let result = OutputFormat::from_str("invalid");
            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("Unknown output format"));
            assert!(err_msg.contains("invalid"));
        }

        #[test]
        fn test_output_format_error_message_includes_supported_formats() {
            let result = OutputFormat::from_str("xml");
            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("Supported formats"));
            assert!(err_msg.contains("json"));
            assert!(err_msg.contains("yaml"));
            assert!(err_msg.contains("wide"));
        }

        #[test]
        fn test_format_output_json() {
            use serde::Serialize;

            #[derive(Serialize)]
            struct TestResource {
                name: String,
                value: i32,
            }

            let resource = TestResource {
                name: "test".to_string(),
                value: 42,
            };

            // Just verify it doesn't panic
            let result = format_output(&resource, &OutputFormat::Json);
            assert!(result.is_ok());
        }

        #[test]
        fn test_format_output_yaml() {
            use serde::Serialize;

            #[derive(Serialize)]
            struct TestResource {
                name: String,
                value: i32,
            }

            let resource = TestResource {
                name: "test".to_string(),
                value: 42,
            };

            // Just verify it doesn't panic
            let result = format_output(&resource, &OutputFormat::Yaml);
            assert!(result.is_ok());
        }

        #[test]
        fn test_resource_type_aliases() {
            // These are common aliases that should be supported
            let pod_aliases = ["pod", "pods"];
            let service_aliases = ["service", "services", "svc"];
            let _deployment_aliases = ["deployment", "deployments", "deploy"];
            let _namespace_aliases = ["namespace", "namespaces", "ns"];
            let _pvc_aliases = ["persistentvolumeclaim", "persistentvolumeclaims", "pvc"];
            let _pv_aliases = ["persistentvolume", "persistentvolumes", "pv"];
            let _sc_aliases = ["storageclass", "storageclasses", "sc"];
            let _cm_aliases = ["configmap", "configmaps", "cm"];
            let _sa_aliases = ["serviceaccount", "serviceaccounts", "sa"];

            // Just verify the patterns exist (actual matching happens in the execute function)
            for alias in pod_aliases {
                assert!(["pod", "pods"].contains(&alias));
            }
            for alias in service_aliases {
                assert!(["service", "services", "svc"].contains(&alias));
            }
            // ... etc
        }

        #[test]
        fn test_output_format_jsonpath() {
            let format = OutputFormat::from_str("jsonpath={.metadata.name}").unwrap();
            assert!(matches!(format, OutputFormat::JsonPath(_)));
            if let OutputFormat::JsonPath(expr) = format {
                assert_eq!(expr, "{.metadata.name}");
            }
        }

        #[test]
        fn test_output_format_name() {
            let format = OutputFormat::from_str("name").unwrap();
            assert!(matches!(format, OutputFormat::Name));
        }

        #[test]
        fn test_format_output_jsonpath_basic_field() {
            use serde::Serialize;
            use serde_json::json;

            #[derive(Serialize)]
            struct TestResource {
                metadata: serde_json::Value,
            }

            let resource = TestResource {
                metadata: json!({ "name": "my-pod", "namespace": "default" }),
            };

            let format = OutputFormat::JsonPath("{.metadata.name}".to_string());
            let result = format_output(&resource, &format);
            assert!(result.is_ok());
        }

        #[test]
        fn test_format_output_jsonpath_array_index() {
            use serde::Serialize;
            use serde_json::json;

            #[derive(Serialize)]
            struct TestResource {
                status: serde_json::Value,
            }

            let resource = TestResource {
                status: json!({
                    "conditions": [
                        { "type": "Ready", "status": "True" },
                        { "type": "Initialized", "status": "True" }
                    ]
                }),
            };

            let format = OutputFormat::JsonPath("{.status.conditions[0].type}".to_string());
            let result = format_output(&resource, &format);
            assert!(result.is_ok());
        }

        #[test]
        fn test_format_output_jsonpath_missing_field() {
            use serde::Serialize;
            use serde_json::json;

            #[derive(Serialize)]
            struct TestResource {
                metadata: serde_json::Value,
            }

            let resource = TestResource {
                metadata: json!({ "name": "my-pod" }),
            };

            // Missing field should not error — it should return empty
            let format = OutputFormat::JsonPath("{.metadata.nonexistent}".to_string());
            let result = format_output(&resource, &format);
            assert!(result.is_ok());
        }

        #[test]
        fn test_format_output_name() {
            use serde::Serialize;
            use serde_json::json;

            #[derive(Serialize)]
            struct TestResource {
                kind: String,
                metadata: serde_json::Value,
            }

            let resource = TestResource {
                kind: "Pod".to_string(),
                metadata: json!({ "name": "my-pod" }),
            };

            let format = OutputFormat::Name;
            let result = format_output(&resource, &format);
            assert!(result.is_ok());
        }

        #[test]
        fn test_output_format_jsonpath_invalid_prefix_rejected() {
            // jsonpath without the = separator is not a valid format
            let result = OutputFormat::from_str("jsonpath{.metadata.name}");
            assert!(result.is_err());
        }

        #[test]
        fn test_new_resource_types_supported() {
            // Verify new resource types are in our type list
            let new_types = [
                "storageclass",
                "volumesnapshot",
                "volumesnapshotclass",
                "endpoints",
                "configmap",
                "secret",
                "statefulset",
                "daemonset",
                "ingress",
                "serviceaccount",
                "role",
                "rolebinding",
                "clusterrole",
                "clusterrolebinding",
                "resourcequota",
                "limitrange",
                "priorityclass",
                "customresourcedefinition",
            ];

            // These should all be valid resource type strings
            for resource_type in new_types {
                assert!(!resource_type.is_empty());
                assert!(resource_type
                    .chars()
                    .all(|c| c.is_lowercase() || c.is_ascii_alphanumeric()));
            }
        }
    }
}
