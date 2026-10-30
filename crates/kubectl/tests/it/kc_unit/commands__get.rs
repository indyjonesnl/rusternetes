//! Unit tests moved from `src/commands/get.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::{Context, Result};
    pub use chrono::Utc;
    pub use futures::StreamExt;
    pub use rusternetes_client::http::{ApiClient, GetError};
    pub use rusternetes_common::resources::{
        CronJob, DaemonSet, Deployment, Job, Namespace, Node, PersistentVolume,
        PersistentVolumeClaim, Pod, Service, StatefulSet,
    };
    pub use serde::Serialize;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::get::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_resource_slash_with_name() {
        let (rtype, name) = parse_resource_slash("pod/nginx");
        assert_eq!(rtype, "pod");
        assert_eq!(name, Some("nginx"));
    }

    #[test]
    fn test_parse_resource_slash_without_name() {
        let (rtype, name) = parse_resource_slash("pods");
        assert_eq!(rtype, "pods");
        assert_eq!(name, None);
    }

    #[test]
    fn test_parse_resource_slash_empty_name() {
        let (rtype, name) = parse_resource_slash("pod/");
        assert_eq!(rtype, "pod");
        assert_eq!(name, None);
    }

    #[test]
    fn test_parse_resource_slash_service() {
        let (rtype, name) = parse_resource_slash("service/frontend");
        assert_eq!(rtype, "service");
        assert_eq!(name, Some("frontend"));
    }

    #[test]
    fn test_comma_separated_resource_types() {
        let input = "pods,services,deployments";
        let types: Vec<&str> = input.split(',').collect();
        assert_eq!(types, vec!["pods", "services", "deployments"]);
    }

    #[test]
    fn test_comma_separated_single_type() {
        let input = "pods";
        let types: Vec<&str> = input.split(',').collect();
        assert_eq!(types, vec!["pods"]);
    }

    #[test]
    fn test_sort_by_jsonpath_metadata_name() {
        use rusternetes_common::types::ObjectMeta;

        let mut pods = vec![
            Pod {
                metadata: ObjectMeta {
                    name: "charlie".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
            Pod {
                metadata: ObjectMeta {
                    name: "alpha".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
            Pod {
                metadata: ObjectMeta {
                    name: "bravo".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
        ];

        sort_by_jsonpath(&mut pods, ".metadata.name");

        assert_eq!(pods[0].metadata.name, "alpha");
        assert_eq!(pods[1].metadata.name, "bravo");
        assert_eq!(pods[2].metadata.name, "charlie");
    }

    #[test]
    fn test_sort_by_jsonpath_with_braces() {
        use rusternetes_common::types::ObjectMeta;

        let mut pods = vec![
            Pod {
                metadata: ObjectMeta {
                    name: "zulu".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
            Pod {
                metadata: ObjectMeta {
                    name: "alpha".to_string(),
                    ..Default::default()
                },
                spec: None,
                status: None,
                type_meta: Default::default(),
            },
        ];

        sort_by_jsonpath(&mut pods, "{.metadata.name}");

        assert_eq!(pods[0].metadata.name, "alpha");
        assert_eq!(pods[1].metadata.name, "zulu");
    }

    #[test]
    fn test_resolve_sort_key_nested() {
        let value = serde_json::json!({
            "metadata": {
                "name": "test-pod",
                "namespace": "default"
            },
            "status": {
                "phase": "Running"
            }
        });

        assert_eq!(resolve_sort_key(&value, ".metadata.name"), "test-pod");
        assert_eq!(resolve_sort_key(&value, ".status.phase"), "Running");
        assert_eq!(resolve_sort_key(&value, ".metadata.namespace"), "default");
        assert_eq!(resolve_sort_key(&value, ".nonexistent.field"), "");
    }

    #[test]
    fn test_format_labels_with_labels() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "nginx".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        let result = format_labels(&Some(labels));
        // Labels are sorted alphabetically
        assert_eq!(result, "app=nginx,env=prod");
    }

    #[test]
    fn test_format_labels_empty() {
        let result = format_labels(&None);
        assert_eq!(result, "<none>");

        let result = format_labels(&Some(HashMap::new()));
        assert_eq!(result, "<none>");
    }

    #[test]
    fn test_format_labels_single() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        let result = format_labels(&Some(labels));
        assert_eq!(result, "app=web");
    }

    #[test]
    fn test_mapper_build_path_pods() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let pod_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Pod".into(),
            plural: "pods".into(),
            singular: "pod".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(
            build_path(&pod_mapping, Some("default"), None),
            "/api/v1/namespaces/default/pods"
        );
        assert_eq!(
            build_path(&pod_mapping, Some("kube-system"), None),
            "/api/v1/namespaces/kube-system/pods"
        );
    }

    #[test]
    fn test_mapper_build_path_deployments() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let dep_mapping = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "Deployment".into(),
            plural: "deployments".into(),
            singular: "deployment".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(
            build_path(&dep_mapping, Some("default"), None),
            "/apis/apps/v1/namespaces/default/deployments"
        );
    }

    #[test]
    fn test_mapper_build_path_nodes() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let node_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Node".into(),
            plural: "nodes".into(),
            singular: "node".into(),
            namespaced: false,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(build_path(&node_mapping, None, None), "/api/v1/nodes");
    }

    #[test]
    fn test_mapper_build_path_namespaces() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let ns_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Namespace".into(),
            plural: "namespaces".into(),
            singular: "namespace".into(),
            namespaced: false,
            verbs: vec![],
            short_names: vec!["ns".into()],
        };
        assert_eq!(build_path(&ns_mapping, None, None), "/api/v1/namespaces");
    }

    #[test]
    fn test_no_headers_suppresses_header() {
        // Verify print_pods with no_headers=true doesn't print header
        // Just verify the function signature accepts no_headers=true
        // (actual stdout capture would need more infrastructure)
        let pods: Vec<Pod> = vec![];
        // This should not panic
        print_pods(&pods, true, false);
        print_pods(&pods, false, false);
    }

    #[test]
    fn test_show_labels_with_pods() {
        // Verify print_pods with show_labels=true doesn't panic
        let pods: Vec<Pod> = vec![];
        print_pods(&pods, false, true);
        print_pods(&pods, true, true);
    }

    #[test]
    fn test_output_format_json() {
        let fmt = OutputFormat::from_str("json").unwrap();
        assert_eq!(fmt, OutputFormat::Json);
    }

    #[test]
    fn test_output_format_yaml() {
        let fmt = OutputFormat::from_str("yaml").unwrap();
        assert_eq!(fmt, OutputFormat::Yaml);
    }

    #[test]
    fn test_output_format_wide() {
        let fmt = OutputFormat::from_str("wide").unwrap();
        assert_eq!(fmt, OutputFormat::Wide);
    }

    #[test]
    fn test_output_format_name() {
        let fmt = OutputFormat::from_str("name").unwrap();
        assert_eq!(fmt, OutputFormat::Name);
    }

    #[test]
    fn test_output_format_jsonpath() {
        let fmt = OutputFormat::from_str("jsonpath={.metadata.name}").unwrap();
        assert_eq!(fmt, OutputFormat::JsonPath("{.metadata.name}".to_string()));
    }

    #[test]
    fn test_output_format_unknown() {
        let result = OutputFormat::from_str("csv");
        assert!(result.is_err());
    }

    #[test]
    fn test_evaluate_jsonpath_simple_field() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = evaluate_jsonpath(&val, "{.metadata.name}").unwrap();
        assert_eq!(result, "test");
    }

    #[test]
    fn test_evaluate_jsonpath_no_braces() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = evaluate_jsonpath(&val, ".metadata.name").unwrap();
        assert_eq!(result, "test");
    }

    #[test]
    fn test_evaluate_jsonpath_missing_field() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = evaluate_jsonpath(&val, "{.metadata.labels}").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_path_string_literal() {
        let val = serde_json::json!({});
        let result = resolve_path(&val, "\"hello\\nworld\"").unwrap();
        assert_eq!(result, "hello\nworld");
    }

    #[test]
    fn test_resolve_path_empty() {
        let val = serde_json::json!("root_value");
        let result = resolve_path(&val, ".").unwrap();
        assert_eq!(result, "root_value");
    }

    #[test]
    fn test_resolve_path_array_index() {
        let val = serde_json::json!({"items": ["a", "b", "c"]});
        let result = resolve_path(&val, ".items[1]").unwrap();
        assert_eq!(result, "b");
    }

    #[test]
    fn test_resolve_path_array_index_out_of_bounds() {
        let val = serde_json::json!({"items": ["a"]});
        let result = resolve_path(&val, ".items[5]").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_format_value_string() {
        let val = serde_json::Value::String("hello".to_string());
        assert_eq!(format_value(&val).unwrap(), "hello");
    }

    #[test]
    fn test_format_value_null() {
        let val = serde_json::Value::Null;
        assert_eq!(format_value(&val).unwrap(), "");
    }

    #[test]
    fn test_format_value_bool() {
        let val = serde_json::Value::Bool(true);
        assert_eq!(format_value(&val).unwrap(), "true");
    }

    #[test]
    fn test_format_value_number() {
        let val = serde_json::json!(42);
        assert_eq!(format_value(&val).unwrap(), "42");
    }

    #[test]
    fn test_format_value_array() {
        let val = serde_json::json!(["a", "b", "c"]);
        assert_eq!(format_value(&val).unwrap(), "a b c");
    }

    #[test]
    fn test_format_value_object() {
        let val = serde_json::json!({"key": "val"});
        let result = format_value(&val).unwrap();
        assert!(result.contains("key"));
        assert!(result.contains("val"));
    }

    #[test]
    fn test_urlencoding_encode_passthrough() {
        assert_eq!(urlencoding::encode("app=nginx"), "app=nginx");
    }

    #[test]
    fn test_urlencoding_encode_spaces() {
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
    }

    #[test]
    fn test_urlencoding_encode_special_chars() {
        let encoded = urlencoding::encode("key=val&other");
        assert!(encoded.contains("%26"));
    }

    #[test]
    fn test_format_duration_days() {
        let d = chrono::Duration::days(5);
        assert_eq!(format_duration(d), "5d");
    }

    #[test]
    fn test_format_duration_hours() {
        let d = chrono::Duration::hours(3);
        assert_eq!(format_duration(d), "3h");
    }

    #[test]
    fn test_format_duration_minutes() {
        let d = chrono::Duration::minutes(45);
        assert_eq!(format_duration(d), "45m");
    }

    #[test]
    fn test_format_duration_seconds() {
        let d = chrono::Duration::seconds(30);
        assert_eq!(format_duration(d), "30s");
    }

    #[test]
    fn test_format_duration_zero() {
        let d = chrono::Duration::seconds(0);
        assert_eq!(format_duration(d), "0s");
    }

    #[test]
    fn test_print_services_no_panic() {
        let services: Vec<Service> = vec![];
        print_services(&services, false, false);
        print_services(&services, true, false);
        print_services(&services, false, true);
    }

    #[test]
    fn test_print_deployments_no_panic() {
        let deployments: Vec<Deployment> = vec![];
        print_deployments(&deployments, false, false);
        print_deployments(&deployments, true, false);
        print_deployments(&deployments, false, true);
    }

    #[test]
    fn test_print_nodes_no_panic() {
        let nodes: Vec<Node> = vec![];
        print_nodes(&nodes, false, false);
        print_nodes(&nodes, true, false);
        print_nodes(&nodes, false, true);
    }

    #[test]
    fn test_print_namespaces_no_panic() {
        let namespaces: Vec<Namespace> = vec![];
        print_namespaces(&namespaces, false, false);
        print_namespaces(&namespaces, true, false);
        print_namespaces(&namespaces, false, true);
    }

    #[test]
    fn test_print_pvs_no_panic() {
        let pvs: Vec<PersistentVolume> = vec![];
        print_pvs(&pvs, false, false);
        print_pvs(&pvs, true, false);
    }

    #[test]
    fn test_print_pvcs_no_panic() {
        let pvcs: Vec<PersistentVolumeClaim> = vec![];
        print_pvcs(&pvcs, false, false);
        print_pvcs(&pvcs, true, false);
    }

    #[test]
    fn test_print_jobs_no_panic() {
        let jobs: Vec<Job> = vec![];
        print_jobs(&jobs, false, false);
        print_jobs(&jobs, true, false);
    }

    #[test]
    fn test_print_statefulsets_no_panic() {
        let statefulsets: Vec<StatefulSet> = vec![];
        print_statefulsets(&statefulsets, false, false);
        print_statefulsets(&statefulsets, true, false);
        print_statefulsets(&statefulsets, false, true);
    }

    #[test]
    fn test_print_statefulsets_ready_column() {
        // A StatefulSet with no status should render 0/<replicas> without panic.
        let sts: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": { "name": "web", "namespace": "default" },
            "spec": {
                "serviceName": "web",
                "replicas": 3,
                "selector": { "matchLabels": { "app": "web" } },
                "template": {
                    "metadata": { "labels": { "app": "web" } },
                    "spec": { "containers": [ { "name": "c", "image": "busybox" } ] }
                }
            }
        }))
        .expect("statefulset should deserialize");
        assert_eq!(sts.spec.replicas, Some(3));
        assert!(sts.status.is_none());
        // ready defaults to 0 when status is absent
        let ready = sts
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);
        assert_eq!(ready, 0);
        print_statefulsets(std::slice::from_ref(&sts), false, false);
    }

    #[test]
    fn test_print_daemonsets_no_panic() {
        let daemonsets: Vec<DaemonSet> = vec![];
        print_daemonsets(&daemonsets, false, false);
        print_daemonsets(&daemonsets, true, false);
        print_daemonsets(&daemonsets, false, true);
    }

    #[test]
    fn test_print_daemonsets_missing_status() {
        // A DaemonSet with no status should render zeros and <none> node
        // selector without panicking (the server may not populate status).
        let ds: DaemonSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": { "name": "fluentd", "namespace": "default" },
            "spec": {
                "selector": { "matchLabels": { "app": "fluentd" } },
                "template": {
                    "metadata": { "labels": { "app": "fluentd" } },
                    "spec": { "containers": [ { "name": "c", "image": "busybox" } ] }
                }
            }
        }))
        .expect("daemonset should deserialize");
        assert!(ds.status.is_none());
        assert!(ds.spec.template.spec.node_selector.is_none());
        print_daemonsets(std::slice::from_ref(&ds), false, false);
    }

    #[test]
    fn test_print_cronjobs_no_panic() {
        let cronjobs: Vec<CronJob> = vec![];
        print_cronjobs(&cronjobs, false, false);
        print_cronjobs(&cronjobs, true, false);
    }

    #[test]
    fn test_mapper_build_path_services() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let svc_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "Service".into(),
            plural: "services".into(),
            singular: "service".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["svc".into()],
        };
        assert_eq!(
            build_path(&svc_mapping, Some("default"), None),
            "/api/v1/namespaces/default/services"
        );
        assert_eq!(
            build_path(&svc_mapping, Some("test-ns"), None),
            "/api/v1/namespaces/test-ns/services"
        );
    }

    #[test]
    fn test_mapper_build_path_configmaps() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let cm_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "ConfigMap".into(),
            plural: "configmaps".into(),
            singular: "configmap".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["cm".into()],
        };
        assert_eq!(
            build_path(&cm_mapping, Some("default"), None),
            "/api/v1/namespaces/default/configmaps"
        );
    }

    #[test]
    fn test_mapper_build_path_jobs() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let job_mapping = ResourceMapping {
            group: "batch".into(),
            version: "v1".into(),
            kind: "Job".into(),
            plural: "jobs".into(),
            singular: "job".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec![],
        };
        assert_eq!(
            build_path(&job_mapping, Some("default"), None),
            "/apis/batch/v1/namespaces/default/jobs"
        );
    }

    #[test]
    fn test_mapper_build_path_cronjobs() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let cj_mapping = ResourceMapping {
            group: "batch".into(),
            version: "v1".into(),
            kind: "CronJob".into(),
            plural: "cronjobs".into(),
            singular: "cronjob".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["cj".into()],
        };
        assert_eq!(
            build_path(&cj_mapping, Some("default"), None),
            "/apis/batch/v1/namespaces/default/cronjobs"
        );
    }

    #[test]
    fn test_mapper_build_path_statefulsets() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let sts_mapping = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "StatefulSet".into(),
            plural: "statefulsets".into(),
            singular: "statefulset".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["sts".into()],
        };
        assert_eq!(
            build_path(&sts_mapping, Some("default"), None),
            "/apis/apps/v1/namespaces/default/statefulsets"
        );
    }

    #[test]
    fn test_mapper_build_path_daemonsets() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let ds_mapping = ResourceMapping {
            group: "apps".into(),
            version: "v1".into(),
            kind: "DaemonSet".into(),
            plural: "daemonsets".into(),
            singular: "daemonset".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["ds".into()],
        };
        assert_eq!(
            build_path(&ds_mapping, Some("default"), None),
            "/apis/apps/v1/namespaces/default/daemonsets"
        );
    }

    #[test]
    fn test_resolve_sort_key_number() {
        let val = serde_json::json!({"spec": {"replicas": 3}});
        let key = resolve_sort_key(&val, ".spec.replicas");
        assert!(key.contains("3"));
    }

    #[test]
    fn test_resolve_sort_key_bool() {
        let val = serde_json::json!({"spec": {"active": true}});
        let key = resolve_sort_key(&val, ".spec.active");
        assert_eq!(key, "true");
    }

    #[test]
    fn test_resolve_sort_key_null() {
        let val = serde_json::json!({"spec": {"field": null}});
        let key = resolve_sort_key(&val, ".spec.field");
        assert_eq!(key, "");
    }

    #[test]
    fn test_evaluate_jsonpath_filter_returns_empty() {
        let val = serde_json::json!({"items": [{"type": "Ready"}]});
        let result = evaluate_jsonpath(&val, "{.items[?(@.type==\"Ready\")]}").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_path_tab_literal() {
        let val = serde_json::json!({});
        let result = resolve_path(&val, "\"col1\\tcol2\"").unwrap();
        assert_eq!(result, "col1\tcol2");
    }

    // --- 26 additional tests below ---

    #[test]
    fn test_format_output_json_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
        let result = format_output(&resource, &OutputFormat::Json);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_yaml_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "test"}});
        let result = format_output(&resource, &OutputFormat::Yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_name_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "nginx"}});
        let result = format_output(&resource, &OutputFormat::Name);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_jsonpath_mode() {
        let resource = serde_json::json!({"kind": "Pod", "metadata": {"name": "test-pod"}});
        let result = format_output(
            &resource,
            &OutputFormat::JsonPath("{.metadata.name}".to_string()),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_table_mode() {
        let resource = serde_json::json!({"kind": "Pod"});
        let result = format_output(&resource, &OutputFormat::Table);
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_output_wide_mode() {
        let resource = serde_json::json!({"kind": "Pod"});
        let result = format_output(&resource, &OutputFormat::Wide);
        assert!(result.is_ok());
    }

    #[test]
    fn test_resolve_path_nested_array() {
        let val = serde_json::json!({"spec": {"containers": [{"name": "c1"}, {"name": "c2"}]}});
        let result = resolve_path(&val, ".spec.containers[0].name").unwrap();
        assert_eq!(result, "c1");
    }

    #[test]
    fn test_resolve_path_missing_field() {
        let val = serde_json::json!({"metadata": {"name": "test"}});
        let result = resolve_path(&val, ".spec.nodeName").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_evaluate_jsonpath_nested_object() {
        let val = serde_json::json!({"status": {"phase": "Running"}});
        let result = evaluate_jsonpath(&val, "{.status.phase}").unwrap();
        assert_eq!(result, "Running");
    }

    #[test]
    fn test_evaluate_jsonpath_array() {
        let val = serde_json::json!({"items": ["a", "b", "c"]});
        let result = evaluate_jsonpath(&val, "{.items[2]}").unwrap();
        assert_eq!(result, "c");
    }

    #[test]
    fn test_resolve_sort_key_object() {
        let val = serde_json::json!({"metadata": {"labels": {"app": "nginx"}}});
        let key = resolve_sort_key(&val, ".metadata.labels");
        // Object should be serialized as JSON string
        assert!(key.contains("app"));
    }

    #[test]
    fn test_sort_by_jsonpath_empty_vec() {
        let mut items: Vec<serde_json::Value> = vec![];
        sort_by_jsonpath(&mut items, ".metadata.name");
        assert!(items.is_empty());
    }

    #[test]
    fn test_sort_by_jsonpath_single_item() {
        let mut items = vec![serde_json::json!({"metadata": {"name": "only"}})];
        sort_by_jsonpath(&mut items, ".metadata.name");
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn test_mapper_build_path_serviceaccounts() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let sa_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "ServiceAccount".into(),
            plural: "serviceaccounts".into(),
            singular: "serviceaccount".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["sa".into()],
        };
        assert_eq!(
            build_path(&sa_mapping, Some("default"), None),
            "/api/v1/namespaces/default/serviceaccounts"
        );
    }

    #[test]
    fn test_mapper_build_path_persistentvolumes() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let pv_mapping = ResourceMapping {
            group: "".into(),
            version: "v1".into(),
            kind: "PersistentVolume".into(),
            plural: "persistentvolumes".into(),
            singular: "persistentvolume".into(),
            namespaced: false,
            verbs: vec![],
            short_names: vec!["pv".into()],
        };
        // Cluster-scoped: no namespace segment
        assert_eq!(
            build_path(&pv_mapping, None, None),
            "/api/v1/persistentvolumes"
        );
    }

    #[test]
    fn test_mapper_build_path_ingresses() {
        use rusternetes_kubectl::discovery::ResourceMapping;
        use rusternetes_kubectl::ops::build_path;
        let ing_mapping = ResourceMapping {
            group: "networking.k8s.io".into(),
            version: "v1".into(),
            kind: "Ingress".into(),
            plural: "ingresses".into(),
            singular: "ingress".into(),
            namespaced: true,
            verbs: vec![],
            short_names: vec!["ing".into()],
        };
        assert_eq!(
            build_path(&ing_mapping, Some("default"), None),
            "/apis/networking.k8s.io/v1/namespaces/default/ingresses"
        );
    }

    #[test]
    fn test_urlencoding_encode_hash() {
        let encoded = urlencoding::encode("key#value");
        assert!(encoded.contains("%23"));
    }

    #[test]
    fn test_urlencoding_encode_slash() {
        let encoded = urlencoding::encode("path/to");
        assert!(encoded.contains("%2F"));
    }

    #[test]
    fn test_format_duration_large_days() {
        let d = chrono::Duration::days(365);
        assert_eq!(format_duration(d), "365d");
    }

    #[test]
    fn test_format_duration_negative() {
        let d = chrono::Duration::seconds(-10);
        // Should still produce some output without panic
        let result = format_duration(d);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_parse_resource_slash_multiple_slashes() {
        let (rtype, name) = parse_resource_slash("pod/my/pod");
        assert_eq!(rtype, "pod");
        assert_eq!(name, Some("my/pod"));
    }

    #[test]
    fn test_format_labels_many_labels() {
        let mut labels = HashMap::new();
        labels.insert("z".to_string(), "last".to_string());
        labels.insert("a".to_string(), "first".to_string());
        labels.insert("m".to_string(), "middle".to_string());
        let result = format_labels(&Some(labels));
        assert!(result.starts_with("a=first"));
        assert!(result.ends_with("z=last"));
    }

    #[test]
    fn test_output_format_jsonpath_file_nonexistent() {
        let result = OutputFormat::from_str("jsonpath-file=/nonexistent/path");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_path_filter_expression() {
        let val = serde_json::json!({"items": [{"type": "Ready"}, {"type": "NotReady"}]});
        let result = resolve_path(&val, ".items[?(@.type==\"Ready\")]").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_resolve_sort_key_empty_path() {
        let val = serde_json::json!({"name": "test"});
        let key = resolve_sort_key(&val, "");
        // empty path with no keys should serialize the whole value
        assert!(!key.is_empty());
    }

    #[test]
    fn test_format_value_nested_array() {
        let val = serde_json::json!([1, 2, 3]);
        let result = format_value(&val).unwrap();
        assert_eq!(result, "1 2 3");
    }
}
