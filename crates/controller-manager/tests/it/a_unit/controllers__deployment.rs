//! Unit tests moved from `src/controllers/deployment.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use futures::StreamExt;
    pub use rusternetes_common::{
        resources::{
            Deployment, DeploymentCondition, DeploymentStatus, Pod, ReplicaSet, ReplicaSetSpec,
        },
        types::{ObjectMeta, TypeMeta},
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::{sync::Arc, time::Duration};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::deployment::*;

    #[test]
    fn test_parse_int_or_percent_percentage() {
        assert_eq!(parse_int_or_percent("25%", 10), 3); // ceil(2.5) = 3
        assert_eq!(parse_int_or_percent("50%", 10), 5);
        assert_eq!(parse_int_or_percent("100%", 10), 10);
        assert_eq!(parse_int_or_percent("25%", 4), 1); // ceil(1.0) = 1
        assert_eq!(parse_int_or_percent("25%", 1), 1); // ceil(0.25) = 1
    }

    #[test]
    fn test_parse_int_or_percent_absolute() {
        assert_eq!(parse_int_or_percent("1", 10), 1);
        assert_eq!(parse_int_or_percent("3", 10), 3);
        assert_eq!(parse_int_or_percent("0", 10), 0);
    }

    #[test]
    fn test_parse_int_or_percent_invalid() {
        // Invalid strings default to 1
        assert_eq!(parse_int_or_percent("abc", 10), 1);
        assert_eq!(parse_int_or_percent("", 10), 1);
    }

    #[test]
    fn test_parse_int_or_percent_invalid_percentage() {
        // Invalid percentage defaults to 25%
        assert_eq!(parse_int_or_percent("abc%", 10), 3); // ceil(25% of 10) = 3
    }

    #[test]
    fn test_compute_rolling_update_counts_defaults() {
        let (surge, unavailable) = compute_rolling_update_counts(10, "25%", "25%");
        assert_eq!(surge, 3); // ceil(2.5) = 3
        assert_eq!(unavailable, 3);
    }

    #[test]
    fn test_compute_rolling_update_counts_absolute() {
        let (surge, unavailable) = compute_rolling_update_counts(10, "2", "1");
        assert_eq!(surge, 2);
        assert_eq!(unavailable, 1);
    }

    #[test]
    fn test_compute_rolling_update_counts_mixed() {
        let (surge, unavailable) = compute_rolling_update_counts(10, "30%", "1");
        assert_eq!(surge, 3); // ceil(3.0) = 3
        assert_eq!(unavailable, 1);
    }

    #[test]
    fn test_compute_rolling_update_counts_small_deployment() {
        // For a deployment with 1 replica, 25% rounds up to 1
        let (surge, unavailable) = compute_rolling_update_counts(1, "25%", "25%");
        assert_eq!(surge, 1);
        assert_eq!(unavailable, 1);
    }

    /// Helper to create a minimal Container for tests
    fn test_container(name: &str, image: &str) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
            name: name.to_string(),
            image: image.to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: None,
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        }
    }

    /// Upstream `scale()` short-circuits before proportional scaling whenever
    /// exactly one ReplicaSet is active, and scales it to the deployment's
    /// replica count verbatim:
    ///
    /// ```text
    /// // pkg/controller/deployment/sync.go:308-316
    /// // If there is only one active replica set then we should scale that up to the full count of the
    /// // deployment. If there is no active replica set, then we should scale up the newest replica set.
    /// if activeOrLatest := deploymentutil.FindActiveOrLatest(newRS, oldRSs); activeOrLatest != nil {
    ///     if *(activeOrLatest.Spec.Replicas) == *(deployment.Spec.Replicas) {
    ///         return nil
    ///     }
    ///     _, _, err := dc.scaleReplicaSetAndRecordEvent(ctx, activeOrLatest, *(deployment.Spec.Replicas), deployment)
    ///     return err
    /// }
    /// ```
    ///
    /// Falling through to proportional scaling instead computes
    /// `allowedSize = desired + maxSurge = 3` and hands the whole delta to the
    /// single RS, overshooting the desired count. Exercised by conformance
    /// "[sig-apps] Deployment should run the lifecycle of a Deployment", whose
    /// final step changes the image and `spec.replicas` 1 -> 2 in one PUT.
    #[tokio::test]
    async fn a_scaling_event_with_one_active_replicaset_scales_it_to_desired_not_allowed_size() {
        use rusternetes_common::resources::{PodTemplateSpec, ReplicaSetStatus};
        use rusternetes_common::types::LabelSelector;
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        // Deployment now wants 2 replicas of the NEW image.
        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(2),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("test", "agnhost:2.59")],
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
        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // The only active ReplicaSet still runs the OLD image at 1 replica,
        // and its annotations record the previous desired count of 1.
        let mut rs_labels = labels.clone();
        rs_labels.insert("pod-template-hash".to_string(), "old".to_string());
        let mut annotations = HashMap::new();
        annotations.insert(
            "deployment.kubernetes.io/desired-replicas".to_string(),
            "1".to_string(),
        );
        annotations.insert(
            "deployment.kubernetes.io/max-replicas".to_string(),
            "2".to_string(),
        );

        let mut old_template = deployment.spec.template.clone();
        old_template.spec.containers = vec![test_container("test", "pause:3.10")];

        let rs = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-deploy-old".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(rs_labels.clone()),
                annotations: Some(annotations),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: "test-deploy".to_string(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: 1,
                selector: LabelSelector {
                    match_labels: Some(rs_labels.clone()),
                    match_expressions: None,
                },
                template: old_template,
                min_ready_seconds: None,
            },
            status: Some(ReplicaSetStatus {
                replicas: 1,
                ready_replicas: 1,
                available_replicas: 1,
                fully_labeled_replicas: Some(1),
                observed_generation: None,
                conditions: None,
                terminating_replicas: None,
            }),
        };
        let rs_key = build_key("replicasets", Some("default"), "test-deploy-old");
        storage.create(&rs_key, &rs).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let scaled: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert_eq!(
            scaled.spec.replicas, 2,
            "the single active ReplicaSet must be scaled to spec.replicas (2), \
             not to desired + maxSurge (3)"
        );
    }

    #[tokio::test]
    async fn test_deployment_status_aggregates_from_replicaset_status() {
        use rusternetes_common::resources::{PodTemplateSpec, ReplicaSetStatus};
        use rusternetes_common::types::LabelSelector;
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        // Create a deployment
        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(3),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("test", "nginx:latest")],
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

        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // Create an owned ReplicaSet with populated status
        let mut rs_labels = labels.clone();
        rs_labels.insert("pod-template-hash".to_string(), "abc123".to_string());

        let rs = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-deploy-abc123".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(rs_labels.clone()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: "test-deploy".to_string(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: 3,
                selector: LabelSelector {
                    match_labels: Some(rs_labels.clone()),
                    match_expressions: None,
                },
                template: deployment.spec.template.clone(),
                min_ready_seconds: None,
            },
            status: Some(ReplicaSetStatus {
                replicas: 3,
                ready_replicas: 3,
                available_replicas: 3,
                fully_labeled_replicas: Some(3),
                observed_generation: None,
                conditions: None,
                terminating_replicas: None,
            }),
        };

        let rs_key = build_key("replicasets", Some("default"), "test-deploy-abc123");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run status update
        controller
            .update_deployment_status(&deployment)
            .await
            .unwrap();

        // Read back the deployment and verify status
        let updated: Deployment = storage.get(&dep_key).await.unwrap();
        let status = updated.status.unwrap();
        assert_eq!(status.replicas, Some(3));
        assert_eq!(status.ready_replicas, Some(3));
        assert_eq!(status.available_replicas, Some(3));
        assert_eq!(status.updated_replicas, Some(3));

        // Verify Available condition is True
        let conditions = status.conditions.unwrap();
        let available_cond = conditions
            .iter()
            .find(|c| c.condition_type == "Available")
            .unwrap();
        assert_eq!(available_cond.status, "True");
    }

    #[tokio::test]
    async fn test_deployment_status_fallback_to_pod_count_when_rs_has_no_status() {
        use rusternetes_common::resources::{PodCondition, PodStatus, PodTemplateSpec};
        use rusternetes_common::types::{LabelSelector, Phase};
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(2),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("test", "nginx:latest")],
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

        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // Create an RS without status (simulates RS controller not having run yet)
        let mut rs_labels = labels.clone();
        rs_labels.insert("pod-template-hash".to_string(), "abc123".to_string());

        let rs = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-deploy-abc123".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(rs_labels.clone()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: "test-deploy".to_string(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: 2,
                selector: LabelSelector {
                    match_labels: Some(rs_labels.clone()),
                    match_expressions: None,
                },
                template: deployment.spec.template.clone(),
                min_ready_seconds: None,
            },
            status: None, // No status yet!
        };

        let rs_key = build_key("replicasets", Some("default"), "test-deploy-abc123");
        storage.create(&rs_key, &rs).await.unwrap();

        // Create 2 ready pods owned by the RS
        for i in 0..2 {
            let pod_name = format!("test-deploy-abc123-pod-{}", i);
            let pod = Pod {
                type_meta: TypeMeta {
                    kind: "Pod".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta {
                    name: pod_name.clone(),
                    namespace: Some("default".to_string()),
                    labels: Some(rs_labels.clone()),
                    owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                        api_version: "apps/v1".to_string(),
                        kind: "ReplicaSet".to_string(),
                        name: "test-deploy-abc123".to_string(),
                        uid: rs.metadata.uid.clone(),
                        controller: Some(true),
                        block_owner_deletion: Some(true),
                    }]),
                    creation_timestamp: Some(chrono::Utc::now() - chrono::Duration::minutes(5)),
                    ..Default::default()
                },
                spec: None,
                status: Some(PodStatus {
                    phase: Some(Phase::Running),
                    conditions: Some(vec![PodCondition {
                        condition_type: "Ready".to_string(),
                        status: "True".to_string(),
                        last_probe_time: None,
                        last_transition_time: None,
                        reason: None,
                        message: None,
                        observed_generation: None,
                    }]),
                    ..Default::default()
                }),
            };
            let pod_key = build_key("pods", Some("default"), &pod_name);
            storage.create(&pod_key, &pod).await.unwrap();
        }

        // Run status update — should pick up pod counts via fallback
        controller
            .update_deployment_status(&deployment)
            .await
            .unwrap();

        let updated: Deployment = storage.get(&dep_key).await.unwrap();
        let status = updated.status.unwrap();

        // Should have counted the pods directly
        assert_eq!(status.replicas, Some(2));
        assert_eq!(status.ready_replicas, Some(2));
        assert_eq!(status.available_replicas, Some(2));
    }

    #[tokio::test]
    async fn test_recreate_deployment_waits_for_old_pods() {
        use rusternetes_common::resources::PodTemplateSpec;
        use rusternetes_common::types::LabelSelector;
        use rusternetes_storage::memory::MemoryStorage;
        use std::collections::HashMap;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "recreate-test".to_string());

        // Create a Recreate deployment with 1 replica
        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("recreate-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(1),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("main", "nginx:1.0")],
                        ..Default::default()
                    },
                },
                strategy: Some(
                    rusternetes_common::resources::deployment::DeploymentStrategy {
                        strategy_type: "Recreate".to_string(),
                        rolling_update: None,
                    },
                ),
                min_ready_seconds: None,
                revision_history_limit: None,
                paused: None,
                progress_deadline_seconds: None,
            },
            status: None,
        };

        let dep_key = build_key("deployments", Some("default"), "recreate-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // First reconcile — creates initial RS and pods
        controller.reconcile_all().await.unwrap();

        // Should have created a ReplicaSet
        let rs_prefix = build_prefix("replicasets", Some("default"));
        let replicasets: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
        assert!(
            !replicasets.is_empty(),
            "Should have created at least one ReplicaSet"
        );

        // Now simulate an image update (template change) by updating the deployment
        let mut dep: Deployment = storage.get(&dep_key).await.unwrap();
        dep.spec.template.spec.containers[0].image = "nginx:2.0".to_string();
        dep.metadata.generation = Some(dep.metadata.generation.unwrap_or(1) + 1);
        storage.update(&dep_key, &dep).await.unwrap();

        // Create a pod owned by the old RS that is still "running" (not terminated)
        let old_rs = &replicasets[0];
        let old_pod = Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "old-pod-0".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(labels.clone()),
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "ReplicaSet".to_string(),
                    name: old_rs.metadata.name.clone(),
                    uid: old_rs.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![test_container("main", "nginx:1.0")],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "old-pod-0"), &old_pod)
            .await
            .unwrap();

        // Reconcile — should scale down old RS but NOT create new RS yet
        // because old pod is still running
        controller.reconcile_all().await.unwrap();

        // Count ReplicaSets — should still be 1 (old one), no new RS created
        let replicasets_after: Vec<ReplicaSet> = storage.list(&rs_prefix).await.unwrap();
        let new_rs_count = replicasets_after
            .iter()
            .filter(|rs| {
                rs.spec
                    .template
                    .spec
                    .containers
                    .first()
                    .map(|c| c.image == "nginx:2.0")
                    .unwrap_or(false)
            })
            .count();

        // Old pod is still running, so new RS should NOT be created
        assert_eq!(
            new_rs_count, 0,
            "Recreate should not create new RS while old pods are still running"
        );
    }

    /// Deployment revision annotation should be computed from existing ReplicaSets,
    /// not hardcoded to "1". When a deployment owns pre-existing ReplicaSets,
    /// its revision should be the max revision from those ReplicaSets.
    #[tokio::test]
    async fn test_deployment_revision_from_existing_replicasets() {
        use rusternetes_storage::MemoryStorage;
        use std::collections::HashMap;
        use std::sync::Arc;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 2);
        let ns = "default";

        let mut rs_labels = HashMap::new();
        rs_labels.insert("app".to_string(), "web".to_string());
        let mut rs_annotations = HashMap::new();
        rs_annotations.insert(
            "deployment.kubernetes.io/revision".to_string(),
            "5".to_string(),
        );

        // Create a pre-existing ReplicaSet with revision 5 as raw JSON
        let rs_json = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "web-rs-1",
                "namespace": ns,
                "uid": "rs-uid-1",
                "annotations": { "deployment.kubernetes.io/revision": "5" },
                "labels": { "app": "web" },
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "web",
                    "uid": "deploy-uid-1",
                    "controller": true
                }]
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": { "app": "web" } },
                "template": {
                    "metadata": { "labels": { "app": "web" } },
                    "spec": { "containers": [{ "name": "nginx", "image": "nginx:1.19" }] }
                }
            }
        });
        let rs: ReplicaSet = serde_json::from_value(rs_json).unwrap();
        let rs_key = format!("/registry/replicasets/{}/web-rs-1", ns);
        storage.create(&rs_key, &rs).await.unwrap();

        // Create a Deployment (no revision annotation yet) as raw JSON
        let deploy_json = serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "web",
                "namespace": ns,
                "uid": "deploy-uid-1"
            },
            "spec": {
                "replicas": 1,
                "selector": { "matchLabels": { "app": "web" } },
                "template": {
                    "metadata": { "labels": { "app": "web" } },
                    "spec": { "containers": [{ "name": "nginx", "image": "nginx:1.19" }] }
                }
            }
        });
        let deployment: Deployment = serde_json::from_value(deploy_json).unwrap();
        let deploy_key = format!("/registry/deployments/{}/web", ns);
        storage.create(&deploy_key, &deployment).await.unwrap();

        // Reconcile
        let d: Deployment = storage.get(&deploy_key).await.unwrap();
        controller.reconcile_deployment(&d).await.unwrap();

        // The deployment's revision should be based on the pre-existing ReplicaSet (5),
        // NOT hardcoded to "1"
        let d: Deployment = storage.get(&deploy_key).await.unwrap();
        let revision = d
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("deployment.kubernetes.io/revision"))
            .expect("deployment should have revision annotation");
        assert!(
            revision.parse::<i64>().unwrap() >= 5,
            "deployment revision ({}) should be >= 5 (max from owned ReplicaSets)",
            revision
        );
    }
    /// Build a Deployment plus one owned, template-matching ReplicaSet in storage.
    ///
    /// This is the state a Deployment sits in for its whole steady life: exactly
    /// one active ReplicaSet and no old ones, so `old_rs_total` is 0 — which is
    /// what made every scale branch unreachable (#1605).
    async fn seed_single_rs_deployment(
        storage: &Arc<rusternetes_storage::memory::MemoryStorage>,
        desired: i32,
        rs_replicas: i32,
        annotated_desired: Option<&str>,
    ) -> (Deployment, String) {
        use rusternetes_common::resources::{PodTemplateSpec, ReplicaSetStatus};
        use rusternetes_common::types::LabelSelector;
        use std::collections::HashMap;

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "test".to_string());

        let deployment = Deployment {
            type_meta: TypeMeta {
                kind: "Deployment".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new("test-deploy").with_namespace("default"),
            spec: rusternetes_common::resources::DeploymentSpec {
                replicas: Some(desired),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![test_container("test", "agnhost:2.59")],
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
        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.create(&dep_key, &deployment).await.unwrap();

        // Same template as the Deployment with only the pod-template-hash label
        // added, so EqualIgnoreHash treats this as the ACTIVE ReplicaSet. The
        // template metadata is cloned rather than rebuilt: ObjectMeta::new()
        // stamps a fresh uid and creationTimestamp, which the whole-template
        // JSON comparison would see as a mismatch.
        let mut rs_labels = labels.clone();
        rs_labels.insert("pod-template-hash".to_string(), "abc123".to_string());
        let mut rs_template = deployment.spec.template.clone();
        if let Some(meta) = rs_template.metadata.as_mut() {
            meta.labels = Some(rs_labels.clone());
        }

        let annotations = annotated_desired.map(|d| {
            let mut a = HashMap::new();
            a.insert(
                "deployment.kubernetes.io/desired-replicas".to_string(),
                d.to_string(),
            );
            a.insert(
                "deployment.kubernetes.io/max-replicas".to_string(),
                d.to_string(),
            );
            a
        });

        let rs = ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta {
                name: "test-deploy-abc123".to_string(),
                namespace: Some("default".to_string()),
                labels: Some(rs_labels.clone()),
                annotations,
                owner_references: Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    name: "test-deploy".to_string(),
                    uid: deployment.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: ReplicaSetSpec {
                replicas: rs_replicas,
                selector: LabelSelector {
                    match_labels: Some(rs_labels.clone()),
                    match_expressions: None,
                },
                template: rs_template,
                min_ready_seconds: None,
            },
            status: Some(ReplicaSetStatus {
                replicas: rs_replicas,
                ready_replicas: rs_replicas,
                available_replicas: rs_replicas,
                fully_labeled_replicas: Some(rs_replicas),
                observed_generation: None,
                conditions: None,
                terminating_replicas: None,
            }),
        };
        let rs_key = build_key("replicasets", Some("default"), "test-deploy-abc123");
        storage.create(&rs_key, &rs).await.unwrap();

        (deployment, rs_key)
    }

    /// #1605: a plain scale-up of a Deployment whose only ReplicaSet is the
    /// active one must reach the requested count.
    ///
    /// Before the fix every scale branch was gated on `old_rs_total > 0`
    /// (`is_scaling_event`, the rolling scale-up, the recreate and rollover
    /// paths), so this state entered none of them: the ReplicaSet stayed at 1
    /// and "[sig-apps] Deployment should run the lifecycle of a Deployment"
    /// waited out its 5-minute timeout observing `ReadyReplicas 1`.
    ///
    /// Upstream runs the FindActiveOrLatest short-circuit unconditionally, at
    /// the top of scale():
    ///   pkg/controller/deployment/sync.go — scale()
    ///   pkg/controller/deployment/util/deployment_util.go:357-377 — FindActiveOrLatest
    #[tokio::test]
    async fn a_single_active_replicaset_scales_up_to_spec_replicas() {
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let (deployment, rs_key) = seed_single_rs_deployment(&storage, 2, 1, Some("1")).await;

        controller.reconcile_deployment(&deployment).await.unwrap();

        let scaled: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert_eq!(
            scaled.spec.replicas, 2,
            "the sole active ReplicaSet must be scaled to spec.replicas (2); it is at {}",
            scaled.spec.replicas
        );
    }

    /// #1605 in the other direction: a scale-DOWN of a single-ReplicaSet
    /// Deployment must also converge, and must not overshoot to 0.
    #[tokio::test]
    async fn a_single_active_replicaset_scales_down_to_spec_replicas() {
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let (deployment, rs_key) = seed_single_rs_deployment(&storage, 1, 3, Some("3")).await;

        controller.reconcile_deployment(&deployment).await.unwrap();

        let scaled: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert_eq!(
            scaled.spec.replicas, 1,
            "the sole active ReplicaSet must be scaled down to spec.replicas (1), not to 0"
        );
    }

    /// #1605: scaling must work when the ReplicaSet carries no
    /// desired-replicas annotation — that annotation is rolling-update
    /// bookkeeping, not a precondition for plain scaling.
    #[tokio::test]
    async fn a_single_active_replicaset_scales_without_a_desired_replicas_annotation() {
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let (deployment, rs_key) = seed_single_rs_deployment(&storage, 3, 1, None).await;

        controller.reconcile_deployment(&deployment).await.unwrap();

        let scaled: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert_eq!(
            scaled.spec.replicas, 3,
            "scaling must not depend on the deployment.kubernetes.io/desired-replicas annotation"
        );
    }

    /// Guard for the #1605 fix: a PAUSED Deployment is still never scaled.
    #[tokio::test]
    async fn a_paused_deployment_is_not_scaled_even_with_one_active_replicaset() {
        use rusternetes_storage::memory::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = DeploymentController::new(storage.clone(), 5);

        let (mut deployment, rs_key) = seed_single_rs_deployment(&storage, 4, 1, Some("1")).await;
        deployment.spec.paused = Some(true);
        let dep_key = build_key("deployments", Some("default"), "test-deploy");
        storage.update(&dep_key, &deployment).await.unwrap();

        controller.reconcile_deployment(&deployment).await.unwrap();

        let untouched: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert_eq!(
            untouched.spec.replicas, 1,
            "a paused Deployment must not be scaled"
        );
    }
}
