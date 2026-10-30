//! Unit tests moved from `src/controllers/hpa.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::Utc;
    pub use rusternetes_common::resources::autoscaling::ResourceMetricStatus;
    pub use rusternetes_common::resources::{
        Deployment, HorizontalPodAutoscaler, HorizontalPodAutoscalerCondition,
        HorizontalPodAutoscalerStatus, MetricSpec, MetricStatus, MetricValueStatus, ReplicaSet,
        StatefulSet,
    };
    pub use rusternetes_controller_manager::controllers::hpa_metrics_client::{
        format_milli_quantity, FakeMetricsClient, HttpMetricsClient, HttpMetricsConfig,
        MetricsClient, PodMetricsInfo,
    };
    pub use rusternetes_controller_manager::controllers::hpa_replica_calculator as calc;
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{
        CrossVersionObjectReference, DeploymentSpec, HorizontalPodAutoscalerSpec, MetricSpec,
        MetricTarget, ResourceMetricSource,
    };
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::hpa::*;
    use rusternetes_controller_manager::controllers::hpa_metrics_client::FakeMetricsClient;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    fn controller_with_cpu_util(
        storage: std::sync::Arc<MemoryStorage>,
        utilization: i32,
    ) -> HorizontalPodAutoscalerController<MemoryStorage> {
        let mut fake = FakeMetricsClient::new();
        fake.resource.insert(
            "cpu".to_string(),
            FakeMetricsClient::pods_info(&[("pod-a", 0, Some(utilization))]),
        );
        HorizontalPodAutoscalerController::with_metrics_client(storage, std::sync::Arc::new(fake))
    }

    #[tokio::test]
    async fn test_get_current_replicas_deployment() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = HorizontalPodAutoscalerController::new(storage.clone());

        // Create a deployment
        let mut deployment = Deployment {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("web-app").with_namespace("default"),
            spec: DeploymentSpec {
                replicas: Some(3),
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("web-pod")),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![],
                        init_containers: None,
                        restart_policy: None,
                        node_selector: None,
                        node_name: None,
                        volumes: None,
                        affinity: None,
                        tolerations: None,
                        service_account_name: None,
                        service_account: None,
                        priority: None,
                        priority_class_name: None,
                        hostname: None,
                        subdomain: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        automount_service_account_token: None,
                        ephemeral_containers: None,
                        overhead: None,
                        scheduler_name: None,
                        topology_spread_constraints: None,
                        resource_claims: None,
                        active_deadline_seconds: None,
                        dns_policy: None,
                        dns_config: None,
                        security_context: None,
                        image_pull_secrets: None,
                        share_process_namespace: None,
                        readiness_gates: None,
                        runtime_class_name: None,
                        enable_service_links: None,
                        preemption_policy: None,
                        host_users: None,
                        set_hostname_as_fqdn: None,
                        termination_grace_period_seconds: None,
                        host_aliases: None,
                        os: None,
                        scheduling_gates: None,
                        resources: None,
                        ..Default::default()
                    },
                },
                strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: None,
                progress_deadline_seconds: None,
            },
            status: None,
        };
        deployment.metadata.ensure_uid();
        deployment.metadata.ensure_creation_timestamp();

        let key = build_key("deployments", Some("default"), "web-app");
        storage.create(&key, &deployment).await.unwrap();

        let target_ref = CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "web-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        };

        let replicas = controller
            .get_current_replicas("default", &target_ref)
            .await
            .unwrap();
        assert_eq!(replicas, 3);
    }

    #[tokio::test]
    async fn test_calculate_desired_replicas_with_bounds() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = controller_with_cpu_util(storage, 85);

        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(2),
            max_replicas: 10,
            metrics: Some(vec![MetricSpec {
                metric_type: "Resource".to_string(),
                resource: Some(ResourceMetricSource {
                    name: "cpu".to_string(),
                    target: MetricTarget {
                        target_type: "Utilization".to_string(),
                        value: None,
                        average_value: None,
                        average_utilization: Some(80),
                    },
                }),
                pods: None,
                object: None,
                external: None,
                container_resource: None,
            }]),
            behavior: None,
        };

        let hpa = HorizontalPodAutoscaler::new("test-hpa", "default", spec);

        // Test with current replicas = 1 (below min)
        let (desired, _) = controller
            .calculate_desired_replicas(&hpa, 1, "default")
            .await
            .unwrap();
        assert!(
            desired >= 2,
            "Desired replicas should be at least min_replicas (2), got {}",
            desired
        );

        // Test with current replicas = 20 (above max)
        let (desired, _) = controller
            .calculate_desired_replicas(&hpa, 20, "default")
            .await
            .unwrap();
        assert!(
            desired <= 10,
            "Desired replicas should be at most max_replicas (10), got {}",
            desired
        );
    }

    fn hpa_with_metric(metric: MetricSpec) -> HorizontalPodAutoscaler {
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(1),
            max_replicas: 100,
            metrics: Some(vec![metric]),
            behavior: None,
        };
        HorizontalPodAutoscaler::new("test-hpa", "default", spec)
    }

    #[tokio::test]
    async fn container_resource_metric_scales_on_container_utilization() {
        use rusternetes_common::resources::ContainerResourceMetricSource;
        let storage = Arc::new(MemoryStorage::new());
        let mut fake = FakeMetricsClient::new();
        // Keyed "{resource}/{container}" — 100% util vs 50% target -> ratio 2.
        fake.resource.insert(
            "cpu/app".to_string(),
            FakeMetricsClient::pods_info(&[("pod-a", 0, Some(100))]),
        );
        let controller =
            HorizontalPodAutoscalerController::with_metrics_client(storage, Arc::new(fake));

        let metric = MetricSpec {
            metric_type: "ContainerResource".to_string(),
            resource: None,
            pods: None,
            object: None,
            external: None,
            container_resource: Some(ContainerResourceMetricSource {
                name: "cpu".to_string(),
                container: "app".to_string(),
                target: MetricTarget {
                    target_type: "Utilization".to_string(),
                    value: None,
                    average_value: None,
                    average_utilization: Some(50),
                },
            }),
        };
        let hpa = hpa_with_metric(metric.clone());
        let (replicas, status) = controller
            .calculate_replicas_for_metric(&metric, 2, "default", &hpa)
            .await
            .unwrap();
        assert_eq!(replicas, 4, "100% vs 50% target on 2 replicas -> 4");
        let cr = status
            .container_resource
            .expect("container_resource status");
        assert_eq!(cr.container, "app");
        assert_eq!(cr.current.average_utilization, Some(100));
    }

    /// `%+v` of a `*metav1.LabelSelector` is the generated `String()`
    /// (generated.pb.go:3157-3182), so the external-metric error text matches.
    #[test]
    fn go_label_selector_string_matches_generated_string() {
        assert_eq!(go_label_selector_string(None), "nil");
        let sel = rusternetes_common::types::LabelSelector {
            match_labels: Some(
                [
                    ("b".to_string(), "2".to_string()),
                    ("a".to_string(), "1".to_string()),
                ]
                .into(),
            ),
            match_expressions: Some(vec![rusternetes_common::types::LabelSelectorRequirement {
                key: "k".into(),
                operator: "In".into(),
                values: Some(vec!["x".into(), "y".into()]),
            }]),
        };
        assert_eq!(
            go_label_selector_string(Some(&sel)),
            "&LabelSelector{MatchLabels:map[string]string{a: 1,b: 2,},MatchExpressions:[]LabelSelectorRequirement{LabelSelectorRequirement{Key:k,Operator:In,Values:[x y],},},}"
        );
    }

    #[tokio::test]
    async fn external_status_reports_sum_over_replicas() {
        use rusternetes_common::resources::{ExternalMetricSource, MetricIdentifier};
        let storage = Arc::new(MemoryStorage::new());
        let mut fake = FakeMetricsClient::new();
        fake.external.insert("queue".to_string(), vec![90_000]);
        let controller =
            HorizontalPodAutoscalerController::with_metrics_client(storage, Arc::new(fake));

        let metric = MetricSpec {
            metric_type: "External".to_string(),
            resource: None,
            pods: None,
            object: None,
            external: Some(ExternalMetricSource {
                metric: MetricIdentifier {
                    name: "queue".to_string(),
                    selector: None,
                },
                target: MetricTarget {
                    target_type: "AverageValue".to_string(),
                    value: None,
                    average_value: Some("30".to_string()),
                    average_utilization: None,
                },
            }),
            container_resource: None,
        };
        let hpa = hpa_with_metric(metric.clone());
        let (_replicas, status) = controller
            .calculate_replicas_for_metric(&metric, 3, "default", &hpa)
            .await
            .unwrap();
        let ext = status.external.expect("external status");
        // sum 90 / 3 replicas = 30, not the raw sum 90.
        assert_eq!(ext.current.average_value.as_deref(), Some("30"));
    }

    #[tokio::test]
    async fn object_average_value_target_reports_average_value() {
        use rusternetes_common::resources::{MetricIdentifier, ObjectMetricSource};
        let storage = Arc::new(MemoryStorage::new());
        let mut fake = FakeMetricsClient::new();
        fake.object.insert("hits".to_string(), 200_000);
        let controller =
            HorizontalPodAutoscalerController::with_metrics_client(storage, Arc::new(fake));

        let metric = MetricSpec {
            metric_type: "Object".to_string(),
            resource: None,
            pods: None,
            object: Some(ObjectMetricSource {
                described_object: CrossVersionObjectReference {
                    kind: "Service".to_string(),
                    name: "web".to_string(),
                    api_version: Some("v1".to_string()),
                },
                metric: MetricIdentifier {
                    name: "hits".to_string(),
                    selector: None,
                },
                target: MetricTarget {
                    target_type: "AverageValue".to_string(),
                    value: None,
                    average_value: Some("100".to_string()),
                    average_utilization: None,
                },
            }),
            external: None,
            container_resource: None,
        };
        let hpa = hpa_with_metric(metric.clone());
        let (_replicas, status) = controller
            .calculate_replicas_for_metric(&metric, 2, "default", &hpa)
            .await
            .unwrap();
        let obj = status.object.expect("object status");
        // AverageValue target -> averageValue = value/replicas = 200/2 = 100;
        // the `value` field stays unset.
        assert_eq!(obj.current.average_value.as_deref(), Some("100"));
        assert!(obj.current.value.is_none());
    }

    #[tokio::test]
    async fn test_scale_target_deployment() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = HorizontalPodAutoscalerController::new(storage.clone());

        // Create a deployment
        let mut deployment = Deployment {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("web-app").with_namespace("default"),
            spec: DeploymentSpec {
                replicas: Some(2),
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("web-pod")),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![],
                        init_containers: None,
                        restart_policy: None,
                        node_selector: None,
                        node_name: None,
                        volumes: None,
                        affinity: None,
                        tolerations: None,
                        service_account_name: None,
                        service_account: None,
                        priority: None,
                        priority_class_name: None,
                        hostname: None,
                        subdomain: None,
                        host_network: None,
                        host_pid: None,
                        host_ipc: None,
                        automount_service_account_token: None,
                        ephemeral_containers: None,
                        overhead: None,
                        scheduler_name: None,
                        topology_spread_constraints: None,
                        resource_claims: None,
                        active_deadline_seconds: None,
                        dns_policy: None,
                        dns_config: None,
                        security_context: None,
                        image_pull_secrets: None,
                        share_process_namespace: None,
                        readiness_gates: None,
                        runtime_class_name: None,
                        enable_service_links: None,
                        preemption_policy: None,
                        host_users: None,
                        set_hostname_as_fqdn: None,
                        termination_grace_period_seconds: None,
                        host_aliases: None,
                        os: None,
                        scheduling_gates: None,
                        resources: None,
                        ..Default::default()
                    },
                },
                strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: None,
                progress_deadline_seconds: None,
            },
            status: None,
        };
        deployment.metadata.ensure_uid();
        deployment.metadata.ensure_creation_timestamp();

        let key = build_key("deployments", Some("default"), "web-app");
        storage.create(&key, &deployment).await.unwrap();

        let target_ref = CrossVersionObjectReference {
            kind: "Deployment".to_string(),
            name: "web-app".to_string(),
            api_version: Some("apps/v1".to_string()),
        };

        // Scale to 5 replicas
        controller
            .scale_target("default", &target_ref, 5)
            .await
            .unwrap();

        // Verify the deployment was scaled
        let updated_deployment: Deployment = storage.get(&key).await.unwrap();
        assert_eq!(updated_deployment.spec.replicas, Some(5));
    }

    /// Regression test: `update_hpa_status_success` must not rewrite the HPA on
    /// successive reconciles when nothing has changed. Without preservation of
    /// each condition's `last_transition_time`, every reconcile cycle stamped
    /// `Utc::now()` into the conditions and re-wrote the HPA — emitting a
    /// MODIFIED watch event per interval and a downstream churn loop.
    #[tokio::test]
    async fn test_update_hpa_status_idempotent_when_nothing_changes() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = controller_with_cpu_util(storage.clone(), 85);

        // Minimal Deployment with replicas matching what the HPA will compute.
        let mut deployment_spec = DeploymentSpec {
            replicas: Some(3),
            selector: rusternetes_common::types::LabelSelector {
                match_labels: Some(HashMap::from([("app".to_string(), "web".to_string())])),
                match_expressions: None,
            },
            template: rusternetes_common::resources::PodTemplateSpec {
                metadata: Some(ObjectMeta::new("web-pod")),
                spec: rusternetes_common::resources::PodSpec::default(),
            },
            strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
            paused: None,
            progress_deadline_seconds: None,
        };
        deployment_spec.replicas = Some(3);

        let mut deployment = Deployment::new("web-app", deployment_spec);
        deployment.metadata = ObjectMeta::new("web-app").with_namespace("default");
        deployment.metadata.ensure_uid();
        deployment.metadata.ensure_creation_timestamp();
        storage
            .create(
                &build_key("deployments", Some("default"), "web-app"),
                &deployment,
            )
            .await
            .unwrap();

        // HPA targeting that Deployment, with min/max wide enough to leave
        // ScalingLimited=False so we exercise the within-range branch.
        let spec = HorizontalPodAutoscalerSpec {
            scale_target_ref: CrossVersionObjectReference {
                kind: "Deployment".to_string(),
                name: "web-app".to_string(),
                api_version: Some("apps/v1".to_string()),
            },
            min_replicas: Some(1),
            max_replicas: 10,
            metrics: None,
            behavior: None,
        };
        let hpa = HorizontalPodAutoscaler::new("test-hpa", "default", spec);
        let hpa_key = build_key("horizontalpodautoscalers", Some("default"), "test-hpa");
        storage.create(&hpa_key, &hpa).await.unwrap();

        // First reconcile establishes the HPA status + conditions.
        controller.reconcile_all().await.unwrap();
        let after_first: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
        let first_v: serde_json::Value = serde_json::to_value(after_first.status.as_ref()).unwrap();

        // Sleep enough that any Utc::now() on a re-write would differ from the
        // timestamps captured above (chrono::Utc has sub-millisecond resolution
        // but in tight CI the two calls could otherwise coincide).
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Second reconcile over the unchanged cluster must NOT mutate the HPA.
        controller.reconcile_all().await.unwrap();
        let after_second: HorizontalPodAutoscaler = storage.get(&hpa_key).await.unwrap();
        let second_v: serde_json::Value =
            serde_json::to_value(after_second.status.as_ref()).unwrap();

        assert_eq!(
            first_v, second_v,
            "HPA status (incl. condition.last_transition_time fields) must be \
             semantically equal after a no-op reconcile — otherwise the controller \
             produces a MODIFIED event every interval (controller hot-loop)."
        );
    }
}
