//! Unit tests moved from `src/controllers/daemonset.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::node::Taint;
    pub use rusternetes_common::resources::pod::Toleration;
    pub use rusternetes_common::resources::policy::IntOrString;
    pub use rusternetes_common::resources::workloads::DEPRECATED_TEMPLATE_GENERATION;
    pub use rusternetes_common::resources::{
        ControllerRevision, DaemonSet, DaemonSetStatus, Node, Pod, PodStatus,
    };
    pub use rusternetes_common::types::{
        label_selector_as_selector, OwnerReference, Phase, Selector,
    };
    pub use rusternetes_controller_manager::controllers::expectations::ControllerExpectations;
    pub use rusternetes_controller_manager::controllers::pod_informer::{
        ControllerRef, PodEventTarget, ResolvedController, SharedPodInformer,
    };
    pub use rusternetes_controller_manager::controllers::replicationcontroller::slow_start_batches_capped;
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tokio::time;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_controller_manager::controllers::daemonset::*;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_node_selector_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage);

        let mut node_labels = HashMap::new();
        node_labels.insert("disktype".to_string(), "ssd".to_string());
        node_labels.insert("region".to_string(), "us-west".to_string());

        let node = Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "node-1".to_string(),
                namespace: None,
                labels: Some(node_labels),
                annotations: None,
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: None,
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            }),
            status: None,
        };

        // Test: no selector = all nodes match
        let ds_no_selector = DaemonSet {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "DaemonSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: rusternetes_common::types::ObjectMeta {
                name: "test-ds".to_string(),
                namespace: Some("default".to_string()),
                labels: None,
                annotations: None,
                uid: uuid::Uuid::new_v4().to_string(),
                creation_timestamp: None,
                deletion_timestamp: None,
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                generate_name: None,
                generation: None,
                managed_fields: None,
            },
            spec: rusternetes_common::resources::DaemonSetSpec {
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: None,
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(rusternetes_common::types::ObjectMeta {
                        name: "".to_string(),
                        namespace: None,
                        labels: None,
                        annotations: None,
                        uid: uuid::Uuid::new_v4().to_string(),
                        creation_timestamp: None,
                        deletion_timestamp: None,
                        resource_version: None,
                        deletion_grace_period_seconds: None,
                        finalizers: None,
                        owner_references: None,
                        generate_name: None,
                        generation: None,
                        managed_fields: None,
                    }),
                    spec: PodSpec {
                        init_containers: None,
                        containers: vec![],
                        node_name: None,
                        node_selector: None,
                        restart_policy: None,
                        service_account_name: None,
                        service_account: None,
                        volumes: None,
                        affinity: None,
                        tolerations: None,
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
                update_strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
            },
            status: None,
        };

        assert!(controller.matches_node_selector(&node, &ds_no_selector));
    }

    #[test]
    fn test_pod_tolerates_no_taints() {
        // No taints = always tolerated
        let tolerations: Vec<Toleration> = vec![];
        let taints: Vec<Taint> = vec![];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_does_not_tolerate_noschedule() {
        let tolerations: Vec<Toleration> = vec![];
        let taints = vec![Taint {
            key: "node-role.kubernetes.io/control-plane".to_string(),
            value: None,
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(!pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_with_exists_operator() {
        let tolerations = vec![Toleration {
            key: Some("node-role.kubernetes.io/control-plane".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "node-role.kubernetes.io/control-plane".to_string(),
            value: None,
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_with_equal_operator() {
        let tolerations = vec![Toleration {
            key: Some("dedicated".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("gpu".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_does_not_tolerate_wrong_value() {
        let tolerations = vec![Toleration {
            key: Some("dedicated".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("cpu".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "dedicated".to_string(),
            value: Some("gpu".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        }];
        assert!(!pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_all_with_empty_key_exists() {
        // Empty key with Exists operator matches all taints
        let tolerations = vec![Toleration {
            key: None,
            operator: Some("Exists".to_string()),
            value: None,
            effect: None,
            toleration_seconds: None,
        }];
        let taints = vec![
            Taint {
                key: "key1".to_string(),
                value: Some("val1".to_string()),
                effect: "NoSchedule".to_string(),
                time_added: None,
            },
            Taint {
                key: "key2".to_string(),
                value: None,
                effect: "NoExecute".to_string(),
                time_added: None,
            },
        ];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_prefer_noschedule_always() {
        // PreferNoSchedule taints are not blocking
        let tolerations: Vec<Toleration> = vec![];
        let taints = vec![Taint {
            key: "preference".to_string(),
            value: None,
            effect: "PreferNoSchedule".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_with_no_effect_matches_all() {
        // A toleration with no effect matches all effects for the same key
        let tolerations = vec![Toleration {
            key: Some("key1".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: None, // matches all effects
            toleration_seconds: None,
        }];
        let taints = vec![Taint {
            key: "key1".to_string(),
            value: None,
            effect: "NoExecute".to_string(),
            time_added: None,
        }];
        assert!(pod_tolerates_node_taints(&tolerations, &taints));
    }

    #[test]
    fn test_pod_tolerates_multiple_taints_partial() {
        // Pod tolerates one taint but not the other
        let tolerations = vec![Toleration {
            key: Some("key1".to_string()),
            operator: Some("Exists".to_string()),
            value: None,
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        }];
        let taints = vec![
            Taint {
                key: "key1".to_string(),
                value: None,
                effect: "NoSchedule".to_string(),
                time_added: None,
            },
            Taint {
                key: "key2".to_string(),
                value: None,
                effect: "NoSchedule".to_string(),
                time_added: None,
            },
        ];
        assert!(!pod_tolerates_node_taints(&tolerations, &taints));
    }

    /// Helper to create a minimal DaemonSet for testing
    #[tokio::test]
    async fn test_ds_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        let mut sa = rusternetes_common::resources::ServiceAccount::new("default", "default");
        sa.image_pull_secrets = Some(vec![
            rusternetes_common::resources::service_account::LocalObjectReference {
                name: "regcred".to_string(),
            },
        ]);
        storage
            .create("/registry/serviceaccounts/default/default", &sa)
            .await
            .unwrap();

        let node = make_test_node("test-node-1");
        storage
            .create("/registry/nodes/test-node-1", &node)
            .await
            .unwrap();

        let mut ds = make_test_daemonset("pullsecrets-ds", "default");
        storage
            .create("/registry/daemonsets/default/pullsecrets-ds", &ds)
            .await
            .unwrap();

        controller.reconcile(&mut ds).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1, "DS should create one pod on the node");
        let secrets = pods[0]
            .spec
            .as_ref()
            .unwrap()
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
    }

    fn make_test_daemonset(name: &str, namespace: &str) -> DaemonSet {
        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_string(), name.to_string());

        DaemonSet {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "DaemonSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: {
                let mut m = rusternetes_common::types::ObjectMeta::new(name.to_string())
                    .with_namespace(namespace.to_string());
                m.ensure_uid();
                m.generation = Some(1);
                m
            },
            spec: rusternetes_common::resources::DaemonSetSpec {
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(match_labels.clone()),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(rusternetes_common::types::ObjectMeta {
                        name: "".to_string(),
                        namespace: None,
                        labels: Some(match_labels),
                        annotations: None,
                        uid: String::new(),
                        creation_timestamp: None,
                        deletion_timestamp: None,
                        resource_version: None,
                        deletion_grace_period_seconds: None,
                        finalizers: None,
                        owner_references: None,
                        generate_name: None,
                        generation: None,
                        managed_fields: None,
                    }),
                    spec: PodSpec {
                        init_containers: None,
                        containers: vec![rusternetes_common::resources::pod::Container {
                            name: "test".to_string(),
                            image: "busybox:latest".to_string(),
                            command: None,
                            args: None,
                            working_dir: None,
                            ports: None,
                            env: None,
                            env_from: None,
                            resources: None,
                            volume_mounts: None,
                            volume_devices: None,
                            liveness_probe: None,
                            readiness_probe: None,
                            startup_probe: None,
                            lifecycle: None,
                            termination_message_path: None,
                            termination_message_policy: None,
                            image_pull_policy: None,
                            security_context: None,
                            stdin: None,
                            stdin_once: None,
                            tty: None,
                            resize_policy: None,
                            restart_policy: None,
                            ..Default::default()
                        }],
                        node_name: None,
                        node_selector: None,
                        restart_policy: None,
                        service_account_name: None,
                        service_account: None,
                        volumes: None,
                        affinity: None,
                        tolerations: None,
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
                update_strategy: None,
                min_ready_seconds: None,
                revision_history_limit: None,
            },
            status: None,
        }
    }

    /// Helper to create a minimal Node for testing
    fn make_test_node(name: &str) -> Node {
        Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = rusternetes_common::types::ObjectMeta::new(name.to_string());
                m.ensure_uid();
                m
            },
            spec: Some(rusternetes_common::resources::NodeSpec {
                pod_cidr: None,
                pod_cidrs: None,
                provider_id: None,
                unschedulable: None,
                taints: None,
            }),
            status: None,
        }
    }

    #[tokio::test]
    async fn test_reconcile_creates_controller_revision() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        // Create a node
        let node = make_test_node("test-node-1");
        storage
            .create("/registry/nodes/test-node-1", &node)
            .await
            .unwrap();

        // Create a DaemonSet
        let mut ds = make_test_daemonset("my-ds", "default");
        storage
            .create("/registry/daemonsets/default/my-ds", &ds)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile(&mut ds).await.unwrap();

        // Verify a ControllerRevision was created
        let cr_prefix = "/registry/controllerrevisions/default/";
        let revisions: Vec<ControllerRevision> = storage.list(cr_prefix).await.unwrap();
        assert!(
            !revisions.is_empty(),
            "ControllerRevision should be created"
        );

        let cr = &revisions[0];
        assert_eq!(cr.type_meta.kind, "ControllerRevision");
        assert_eq!(cr.type_meta.api_version, "apps/v1");
        assert_eq!(cr.revision, 1);
        assert!(
            !cr.metadata.uid.is_empty(),
            "ControllerRevision should have a UID"
        );
        assert!(
            cr.metadata.creation_timestamp.is_some(),
            "Should have creation timestamp"
        );

        // Verify owner reference
        let owner_refs = cr.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "DaemonSet");
        assert_eq!(owner_refs[0].name, "my-ds");
        assert_eq!(owner_refs[0].uid, ds.metadata.uid);
        assert_eq!(owner_refs[0].controller, Some(true));

        // Verify labels include controller-revision-hash
        let labels = cr.metadata.labels.as_ref().unwrap();
        assert!(labels.contains_key("controller-revision-hash"));

        // Verify ControllerRevision data format matches K8s getPatch().
        // K8s Match() does bytes.Equal(getPatch(ds), history.Data.Raw).
        // The data MUST have: {"spec":{"template":{...,"$patch":"replace"}}}
        // with alphabetically sorted keys.
        let data = cr
            .data
            .as_ref()
            .expect("ControllerRevision should have data");
        let data_obj = data.as_object().expect("data should be an object");
        assert!(data_obj.contains_key("spec"), "data should have 'spec'");
        let spec = data_obj.get("spec").unwrap().as_object().unwrap();
        assert!(spec.contains_key("template"), "spec should have 'template'");
        let template = spec.get("template").unwrap().as_object().unwrap();
        assert_eq!(
            template.get("$patch"),
            Some(&serde_json::json!("replace")),
            "template should have $patch: replace"
        );

        // Verify keys are alphabetically sorted (Match() does byte comparison)
        let data_json = serde_json::to_string(&data).unwrap();
        // Re-parse and re-serialize to verify sorting is stable
        let reparsed: serde_json::Value = serde_json::from_str(&data_json).unwrap();
        let reserialized = serde_json::to_string(&reparsed).unwrap();
        assert_eq!(
            data_json, reserialized,
            "ControllerRevision data JSON should be deterministic (sorted keys)"
        );

        // Verify Match() equivalent: build_patch_data should produce same bytes
        let fresh_patch = DaemonSetController::<MemoryStorage>::build_patch_data(&ds.spec.template)
            .expect("build_patch_data should succeed");
        let fresh_json = serde_json::to_string(&fresh_patch).unwrap();
        assert_eq!(
            data_json, fresh_json,
            "build_patch_data should produce identical bytes for same template (K8s Match)"
        );
    }

    #[tokio::test]
    async fn test_reconcile_deletes_terminal_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        // Create a node
        let node = make_test_node("test-node-2");
        storage
            .create("/registry/nodes/test-node-2", &node)
            .await
            .unwrap();

        // Create a DaemonSet
        let mut ds = make_test_daemonset("fail-ds", "default");
        storage
            .create("/registry/daemonsets/default/fail-ds", &ds)
            .await
            .unwrap();

        // Reconcile once to create pods
        controller.reconcile(&mut ds).await.unwrap();

        // Verify a pod was created
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let ds_pods: Vec<&Pod> = pods
            .iter()
            .filter(|p| {
                p.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|r| r.name == "fail-ds"))
            })
            .collect();
        assert_eq!(ds_pods.len(), 1, "Should have 1 DS pod");
        let pod_name = ds_pods[0].metadata.name.clone();

        // Mark the pod as Failed
        let pod_key = format!("/registry/pods/default/{}", pod_name);
        let mut failed_pod: Pod = storage.get(&pod_key).await.unwrap();
        if let Some(status) = failed_pod.status.as_mut() {
            status.phase = Some(Phase::Failed);
        }
        storage.update(&pod_key, &failed_pod).await.unwrap();

        // Re-read DaemonSet (status was updated)
        let mut ds: DaemonSet = storage
            .get("/registry/daemonsets/default/fail-ds")
            .await
            .unwrap();

        // Reconcile again — should delete the failed pod AND recreate in the same cycle.
        // K8s DS controller deletes failed pods and creates replacements immediately.
        // K8s ref: pkg/controller/daemon/daemon_controller.go — podsShouldBeOnNode
        controller.reconcile(&mut ds).await.unwrap();

        // The failed pod is being deleted gracefully (#2465: DeletePod stamps
        // a deletionTimestamp; the kubelet removes it later).
        let failed: Pod = storage.get(&pod_key).await.unwrap();
        assert!(
            failed.metadata.deletion_timestamp.is_some(),
            "Failed pod should have been deleted"
        );

        // A replacement pod should already exist (created in the same cycle)
        let pods_after: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let ds_pods_after: Vec<&Pod> = pods_after
            .iter()
            .filter(|p| {
                p.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|r| r.name == "fail-ds"))
                    && p.metadata.deletion_timestamp.is_none()
            })
            .collect();
        assert_eq!(
            ds_pods_after.len(),
            1,
            "A replacement DS pod should be created in the same cycle as deletion"
        );
    }

    #[tokio::test]
    async fn test_reconcile_sets_number_available() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());

        // Create a node
        let node = make_test_node("test-node-3");
        storage
            .create("/registry/nodes/test-node-3", &node)
            .await
            .unwrap();

        // Create a DaemonSet
        let mut ds = make_test_daemonset("avail-ds", "default");
        storage
            .create("/registry/daemonsets/default/avail-ds", &ds)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile(&mut ds).await.unwrap();

        // Read back the updated DS
        let updated_ds: DaemonSet = storage
            .get("/registry/daemonsets/default/avail-ds")
            .await
            .unwrap();
        let status = updated_ds.status.as_ref().unwrap();

        assert_eq!(status.desired_number_scheduled, 1);
        assert_eq!(status.current_number_scheduled, 1);
        // Pod is Pending, not Running, so number_ready should be 0
        assert_eq!(status.number_ready, 0);
        assert!(
            status.number_available.is_some(),
            "number_available should be set"
        );
        assert!(
            status.updated_number_scheduled.is_some(),
            "updated_number_scheduled should be set"
        );
        assert_eq!(status.updated_number_scheduled, Some(1));
    }

    #[test]
    fn test_resolve_max_unavailable_absolute() {
        // None defaults to 1
        assert_eq!(resolve_max_unavailable(None, 3), 1);
        // Absolute integer passes through
        assert_eq!(resolve_max_unavailable(Some(&IntOrString::Int(2)), 5), 2);
        // Absolute clamped to at least 1
        assert_eq!(resolve_max_unavailable(Some(&IntOrString::Int(0)), 5), 1);
        // Unparseable falls back to 1
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("notanumber".to_string())), 5),
            1
        );
    }

    #[test]
    fn test_resolve_max_unavailable_percentage() {
        // 25% of 4 nodes = 1 (rounded up from 1.0)
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("25%".to_string())), 4),
            1
        );
        // 50% of 4 nodes = 2
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("50%".to_string())), 4),
            2
        );
        // 25% of 5 nodes = 2 (rounded up from 1.25)
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("25%".to_string())), 5),
            2
        );
        // 100% of 3 nodes = 3
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("100%".to_string())), 3),
            3
        );
        // Tiny percentage on tiny cluster still allows at least 1
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("1%".to_string())), 1),
            1
        );
        // Regression guard: "25%" must NOT be parsed as 25 absolute on a
        // small cluster — that previously allowed all pods to be deleted.
        assert_eq!(
            resolve_max_unavailable(Some(&IntOrString::String("25%".to_string())), 3),
            1
        );
    }

    // --- Ports of pkg/controller/daemon/util/daemonset_util_test.go ---

    fn pod_with_labels(labels: &[(&str, &str)]) -> Pod {
        let mut pod = Pod::new("p", rusternetes_common::resources::PodSpec::default());
        pod.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        pod
    }

    /// TestIsPodUpdated (daemonset_util_test.go:53-143).
    #[test]
    fn test_is_pod_updated() {
        let gen = Some(12345);
        let bad = Some(12350);
        let hash = "55555";
        let both = [(TEMPLATE_GENERATION_LABEL, "12345"), (UNIQUE_LABEL, hash)];
        let no_hash = [(TEMPLATE_GENERATION_LABEL, "12345")];
        let longer = format!("{}123", hash);
        let cases: Vec<(&str, Option<i64>, Pod, &str, bool)> = vec![
            ("both match", gen, pod_with_labels(&both), hash, true),
            (
                "gen matches, hash doesn't",
                gen,
                pod_with_labels(&both),
                &longer,
                true,
            ),
            (
                "gen matches, no hash label, has hash",
                gen,
                pod_with_labels(&no_hash),
                hash,
                true,
            ),
            (
                "gen matches, no hash label, no hash",
                gen,
                pod_with_labels(&no_hash),
                "",
                true,
            ),
            (
                "gen matches, has hash label, no hash",
                gen,
                pod_with_labels(&both),
                "",
                true,
            ),
            (
                "gen doesn't match, hash does",
                bad,
                pod_with_labels(&both),
                hash,
                true,
            ),
            ("neither match", bad, pod_with_labels(&both), &longer, false),
            (
                "empty labels, no hash",
                gen,
                pod_with_labels(&[]),
                "",
                false,
            ),
            ("empty labels", gen, pod_with_labels(&[]), hash, false),
            (
                "nil generation, hash matches",
                None,
                pod_with_labels(&both),
                hash,
                true,
            ),
            (
                "nil generation, hash doesn't",
                None,
                pod_with_labels(&both),
                &longer,
                false,
            ),
        ];
        for (name, g, pod, h, want) in cases {
            assert_eq!(is_pod_updated(&pod, h, g), want, "{}", name);
        }
    }

    /// GetTemplateGeneration (daemonset_util.go:35-46).
    #[test]
    fn test_get_template_generation() {
        let mut ds = make_test_daemonset("g", "default");
        assert_eq!(get_template_generation(&ds), None);
        let mut ann = HashMap::new();
        ann.insert(DEPRECATED_TEMPLATE_GENERATION.to_string(), "7".to_string());
        ds.metadata.annotations = Some(ann.clone());
        assert_eq!(get_template_generation(&ds), Some(7));
        ann.insert(DEPRECATED_TEMPLATE_GENERATION.to_string(), "x".to_string());
        ds.metadata.annotations = Some(ann);
        assert_eq!(get_template_generation(&ds), None, "parse error => nil");
    }

    /// TestCreatePodTemplate (daemonset_util_test.go:145-169), via the
    /// labels create_pod applies.
    #[tokio::test]
    async fn test_create_pod_sets_template_generation_label() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        let node = make_test_node("n1");
        storage.create("/registry/nodes/n1", &node).await.unwrap();
        let mut ds = make_test_daemonset("tg", "default");
        let mut ann = HashMap::new();
        ann.insert(DEPRECATED_TEMPLATE_GENERATION.to_string(), "3".to_string());
        ds.metadata.annotations = Some(ann);
        storage
            .create("/registry/daemonsets/default/tg", &ds)
            .await
            .unwrap();
        controller.reconcile(&mut ds).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1);
        let labels = pods[0].metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels.get(TEMPLATE_GENERATION_LABEL).map(String::as_str),
            Some("3")
        );
        assert!(labels.contains_key(UNIQUE_LABEL));

        // No annotation => no label (generation == nil in CreatePodTemplate).
        let mut ds2 = make_test_daemonset("tg2", "default");
        storage
            .create("/registry/daemonsets/default/tg2", &ds2)
            .await
            .unwrap();
        controller.reconcile(&mut ds2).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let p2 = pods
            .iter()
            .find(|p| p.metadata.name.starts_with("tg2-"))
            .unwrap();
        assert!(!p2
            .metadata
            .labels
            .as_ref()
            .unwrap()
            .contains_key(TEMPLATE_GENERATION_LABEL));
    }

    // ---- #2465: graceful pod deletion (RealPodControl.DeletePod) ----

    fn ds_pods(pods: &[Pod], ds_name: &str) -> Vec<Pod> {
        pods.iter()
            .filter(|p| {
                p.metadata
                    .owner_references
                    .as_ref()
                    .is_some_and(|refs| refs.iter().any(|r| r.name == ds_name))
            })
            .cloned()
            .collect()
    }

    /// One node + one DS pod created by a first reconcile; returns the
    /// controller, storage, the DS (re-read) and the pod's name.
    async fn one_pod_fixture(
        ds_name: &str,
    ) -> (
        DaemonSetController<MemoryStorage>,
        Arc<MemoryStorage>,
        DaemonSet,
        String,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        let node = make_test_node("n1");
        storage.create("/registry/nodes/n1", &node).await.unwrap();
        let mut ds = make_test_daemonset(ds_name, "default");
        storage
            .create(&format!("/registry/daemonsets/default/{ds_name}"), &ds)
            .await
            .unwrap();
        controller.reconcile(&mut ds).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let pod_name = ds_pods(&pods, ds_name)[0].metadata.name.clone();
        let ds: DaemonSet = storage
            .get(&format!("/registry/daemonsets/default/{ds_name}"))
            .await
            .unwrap();
        (controller, storage, ds, pod_name)
    }

    /// `RealPodControl.DeletePod` is a graceful `Pods(ns).Delete(ctx, podID,
    /// DeleteOptions{})` (pkg/controller/controller_utils.go:618): a failed
    /// pod gets a deletionTimestamp, it does not vanish.
    #[tokio::test]
    async fn test_failed_pod_is_deleted_gracefully() {
        let (controller, storage, mut ds, pod_name) = one_pod_fixture("gf").await;
        let pod_key = format!("/registry/pods/default/{pod_name}");
        let mut pod: Pod = storage.get(&pod_key).await.unwrap();
        pod.status.as_mut().unwrap().phase = Some(Phase::Failed);
        storage.update(&pod_key, &pod).await.unwrap();

        controller.reconcile(&mut ds).await.unwrap();

        let after: Pod = storage
            .get(&pod_key)
            .await
            .expect("failed pod must still exist: delete is graceful");
        assert!(
            after.metadata.deletion_timestamp.is_some(),
            "failed pod must carry a deletionTimestamp"
        );
    }

    /// Rolling update replaces old pods through `podControl.DeletePod`
    /// (update.go rollingUpdate -> syncNodes), i.e. gracefully.
    #[tokio::test]
    async fn test_rolling_update_old_pod_is_deleted_gracefully() {
        let (controller, storage, mut ds, pod_name) = one_pod_fixture("gr").await;
        ds.spec.template.spec.containers[0].image = "busybox:new".to_string();
        controller.reconcile(&mut ds).await.unwrap();

        let old: Pod = storage
            .get(&format!("/registry/pods/default/{pod_name}"))
            .await
            .expect("old pod must still exist: delete is graceful");
        assert!(old.metadata.deletion_timestamp.is_some());
    }

    /// Pods on a node that no longer qualifies are deleted via `syncNodes`
    /// (daemon_controller.go podsShouldBeOnNode `!shouldContinueRunning`).
    #[tokio::test]
    async fn test_pod_on_ineligible_node_is_deleted_gracefully() {
        let (controller, storage, mut ds, pod_name) = one_pod_fixture("gi").await;
        let mut sel = HashMap::new();
        sel.insert("disk".to_string(), "ssd".to_string());
        ds.spec.template.spec.node_selector = Some(sel);
        controller.reconcile(&mut ds).await.unwrap();

        let pod: Pod = storage
            .get(&format!("/registry/pods/default/{pod_name}"))
            .await
            .expect("pod must still exist: delete is graceful");
        assert!(pod.metadata.deletion_timestamp.is_some());
    }

    /// Port of the `ExpectDeletions` / `deletePod` pair
    /// (daemon_controller.go:1000, :668; UID-tracked per
    /// controller_utils.go:380-390): a delete issued by a sync is expected;
    /// the MODIFIED event carrying the deletionTimestamp observes it exactly
    /// once; the later DELETED event of the same pod must not count again.
    /// While it is unmet the next sync only updates status.
    #[tokio::test]
    async fn test_deletion_expectations_observed_once_and_gate_sync() {
        let (controller, storage, mut ds, pod_name) = one_pod_fixture("ge").await;
        ds.spec.template.spec.containers[0].image = "busybox:new".to_string();
        controller.reconcile(&mut ds).await.unwrap();
        assert_eq!(
            controller.expectations.get_expectations("default/ge"),
            Some((0, 1)),
            "the graceful delete must be expected"
        );
        assert!(!controller.expectations.satisfied("default/ge"));

        // MODIFIED with deletionTimestamp observes the deletion...
        let pod_key = format!("/registry/pods/default/{pod_name}");
        let pod: Pod = storage.get(&pod_key).await.unwrap();
        let json = serde_json::to_string(&pod).unwrap();
        let queue = WorkQueue::new();
        controller
            .deliver_pod_event(
                &rusternetes_storage::WatchEvent::Modified(pod_key.clone(), json.clone()),
                &queue,
            )
            .await;
        assert!(controller.expectations.satisfied("default/ge"));
        assert_eq!(
            controller.expectations.get_expectations("default/ge"),
            Some((0, 0))
        );
        // ...and the DELETED event of the same pod is not counted again.
        controller
            .deliver_pod_event(
                &rusternetes_storage::WatchEvent::Deleted(pod_key, json),
                &queue,
            )
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/ge"),
            Some((0, 0)),
            "DELETED after MODIFIED must not drive the counter negative"
        );
    }

    /// `syncDaemonSet` (daemon_controller.go:1263-1266): while an expected
    /// deletion is unobserved (the pod is still live in the listing) the sync
    /// only updates status - no rolling-update deletion, no creation.
    #[tokio::test]
    async fn test_unmet_deletion_expectation_gates_sync() {
        let (controller, storage, mut ds, pod_name) = one_pod_fixture("gg").await;
        controller
            .expectations
            .expect_deletions_of("default/gg", &[format!("default/{pod_name}")]);
        ds.spec.template.spec.containers[0].image = "busybox:new".to_string();

        controller.reconcile(&mut ds).await.unwrap();

        let pod: Pod = storage
            .get(&format!("/registry/pods/default/{pod_name}"))
            .await
            .unwrap();
        assert!(
            pod.metadata.deletion_timestamp.is_none(),
            "gated sync must not delete"
        );
        assert!(!controller.expectations.satisfied("default/gg"));
    }

    /// A pod the fresh listing shows terminating counts as observed: the
    /// next sync releases the gate without waiting for a watch event.
    #[tokio::test]
    async fn test_listing_settles_deletion_expectation() {
        let (controller, _storage, mut ds, _pod) = one_pod_fixture("gs").await;
        ds.spec.template.spec.containers[0].image = "busybox:new".to_string();
        controller.reconcile(&mut ds).await.unwrap();
        assert_eq!(
            controller.expectations.get_expectations("default/gs"),
            Some((0, 1))
        );
        controller.reconcile(&mut ds).await.unwrap();
        assert!(controller.expectations.satisfied("default/gs"));
    }

    /// `findUpdatedPodsOnNode` (update.go:268) and `podsShouldBeOnNode`
    /// (daemon_controller.go:811,903) skip pods with a deletionTimestamp: a
    /// terminating pod is not re-deleted and does not get a second pod
    /// created next to it (TestDaemonSetRespectsTermination).
    #[tokio::test]
    async fn test_terminating_pod_is_left_alone() {
        let (controller, storage, mut ds, pod_name) = one_pod_fixture("gt").await;
        let pod_key = format!("/registry/pods/default/{pod_name}");
        let mut pod: Pod = storage.get(&pod_key).await.unwrap();
        let ts = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp() - 5, 0).unwrap();
        pod.metadata.deletion_timestamp = Some(ts);
        storage.update(&pod_key, &pod).await.unwrap();
        // A template change would otherwise make this pod an old-pod candidate.
        ds.spec.template.spec.containers[0].image = "busybox:new".to_string();

        controller.reconcile(&mut ds).await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(ds_pods(&pods, "gt").len(), 1, "no second pod next to it");
        let after: Pod = storage.get(&pod_key).await.unwrap();
        assert_eq!(
            after.metadata.deletion_timestamp,
            Some(ts),
            "deletionTimestamp untouched"
        );
    }

    /// `syncNodes` (daemon_controller.go:1000) sets creation expectations
    /// together with deletion ones; `addPod` (:551) observes each created pod
    /// once. While unmet the next sync only updates status.
    #[tokio::test]
    async fn test_creation_expectations_set_observed_once_and_gate_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        for n in ["n1", "n2"] {
            storage
                .create(&format!("/registry/nodes/{n}"), &make_test_node(n))
                .await
                .unwrap();
        }
        let mut ds = make_test_daemonset("cx", "default");
        storage
            .create("/registry/daemonsets/default/cx", &ds)
            .await
            .unwrap();
        controller.reconcile(&mut ds).await.unwrap();
        assert_eq!(
            controller.expectations.get_expectations("default/cx"),
            Some((2, 0))
        );
        assert!(!controller.expectations.satisfied("default/cx"));

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        let queue = WorkQueue::new();
        for pod in &pods {
            let key = format!("/registry/pods/default/{}", pod.metadata.name);
            let json = serde_json::to_string(pod).unwrap();
            let ev = rusternetes_storage::WatchEvent::Added(key, json);
            controller.deliver_pod_event(&ev, &queue).await;
            // a replayed event must not count twice
            controller.deliver_pod_event(&ev, &queue).await;
        }
        assert_eq!(
            controller.expectations.get_expectations("default/cx"),
            Some((0, 0))
        );
    }

    /// Rusternetes settle step: a created pod the fresh listing shows counts
    /// as observed, so direct `reconcile` calls do not wedge on a missing
    /// event.
    #[tokio::test]
    async fn test_listing_settles_creation_expectation() {
        let (controller, _storage, mut ds, _pod) = one_pod_fixture("cs").await;
        assert_eq!(
            controller.expectations.get_expectations("default/cs"),
            Some((1, 0))
        );
        controller.reconcile(&mut ds).await.unwrap();
        assert!(controller.expectations.satisfied("default/cs"));
    }

    /// `syncNodes` caps one call at `burstReplicas` (:987-989) and sets the
    /// capped count as the expectation.
    /// A generateName collision is retried with a fresh name
    /// (`createWithGenerateNameRetry`, store.go:460-470) rather than being
    /// swallowed as "already exists" and leaving the node without a pod.
    #[tokio::test]
    async fn test_create_pod_retries_generate_name_collision() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        let ds = make_test_daemonset("gn", "default");
        let taken = make_test_daemonset("gn", "default");
        let squatter = controller
            .create_pod(&taken, "n0", "default")
            .await
            .unwrap();
        let mut names = vec!["gn-fresh".to_string(), squatter.clone(), squatter.clone()];
        let created = controller
            .create_pod_with_namer(&ds, "n1", "default", &mut || names.pop().unwrap())
            .await
            .unwrap();
        assert_eq!(created, "gn-fresh");
    }

    #[tokio::test]
    async fn test_burst_replicas_caps_creates() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        for i in 0..(BURST_REPLICAS + 10) {
            let n = format!("n{i}");
            storage
                .create(&format!("/registry/nodes/{n}"), &make_test_node(&n))
                .await
                .unwrap();
        }
        let mut ds = make_test_daemonset("bc", "default");
        storage
            .create("/registry/daemonsets/default/bc", &ds)
            .await
            .unwrap();
        controller.reconcile(&mut ds).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(ds_pods(&pods, "bc").len(), BURST_REPLICAS);
        assert_eq!(
            controller.expectations.get_expectations("default/bc"),
            Some((BURST_REPLICAS as i64, 0))
        );
    }

    /// A failed create lowers its own expectation (`CreationObserved`,
    /// :1043): no pod will ever arrive for it. Forced with an OnDelete
    /// DaemonSet whose deterministic pod name is already taken.
    #[tokio::test]
    async fn test_failed_create_lowers_expectation() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        storage
            .create("/registry/nodes/n1", &make_test_node("n1"))
            .await
            .unwrap();
        let mut ds = make_test_daemonset("fc", "default");
        ds.spec.update_strategy = Some(
            rusternetes_common::resources::workloads::DaemonSetUpdateStrategy {
                strategy_type: Some("OnDelete".to_string()),
                rolling_update: None,
            },
        );
        storage
            .create("/registry/daemonsets/default/fc", &ds)
            .await
            .unwrap();
        // An unowned pod already holds the name `fc-n1`.
        let mut squatter = DaemonSetController::<MemoryStorage>::candidate_pod_from_template(&ds);
        squatter.metadata.name = "fc-n1".to_string();
        squatter.metadata.namespace = Some("default".to_string());
        squatter.metadata.owner_references = None;
        squatter.metadata.labels = None;
        squatter.spec.as_mut().unwrap().node_name = None;
        storage
            .create("/registry/pods/default/fc-n1", &squatter)
            .await
            .unwrap();
        controller.reconcile(&mut ds).await.unwrap();
        assert_eq!(
            controller.expectations.get_expectations("default/fc"),
            Some((0, 0)),
            "the failed create must not be waited on"
        );
    }

    // --- pod event handlers on the shared pod informer (#3868) -------------
    // daemon_controller_test.go TestAddPod :2951, TestAddPodOrphan :3000,
    // TestUpdatePod, TestUpdatePodOrphanSameLabels, TestUpdatePodOrphanWithNewLabels,
    // TestUpdatePodChangeControllerRef, TestUpdatePodControllerRefRemoved,
    // TestDeletePod, TestDeletePodOrphan.

    use rusternetes_storage::WatchEvent;

    /// Drive one pod event through the informer's classification and this
    /// controller's handlers (what `SharedPodInformer::on_event` does, for a
    /// controller that is not behind an `Arc`).
    trait DeliverPodEvent {
        async fn deliver_pod_event(&self, event: &WatchEvent, queue: &WorkQueue);
    }

    impl DeliverPodEvent for DaemonSetController<MemoryStorage> {
        async fn deliver_pod_event(&self, event: &WatchEvent, queue: &WorkQueue) {
            if let Some(observation) = self.pod_informer.cache().on_event(event) {
                rusternetes_controller_manager::controllers::pod_informer::apply_pod_observation(
                    self,
                    queue,
                    &observation,
                )
                .await;
            }
        }
    }

    fn pod_json(
        name: &str,
        owner: Option<(&str, &str)>,
        labels: &[(&str, &str)],
        rv: &str,
    ) -> String {
        let labels: HashMap<&str, &str> = labels.iter().copied().collect();
        let mut v = serde_json::json!({
            "metadata": {
                "name": name, "namespace": "default", "labels": labels,
                "resourceVersion": rv
            },
            "spec": {"containers": []},
        });
        if let Some((owner, uid)) = owner {
            v["metadata"]["ownerReferences"] = serde_json::json!([{
                "apiVersion": "apps/v1", "kind": "DaemonSet", "name": owner,
                "uid": uid, "controller": true
            }]);
        }
        v.to_string()
    }

    fn pod_key(name: &str) -> String {
        format!("/registry/pods/default/{name}")
    }

    /// Two DaemonSets "ds1" (selects app=ds1) and "ds2" (selects app=ds2), as
    /// upstream's `newDaemonSet("foo1")`/`("foo2")`; returns their UIDs.
    async fn two_daemonsets() -> (
        DaemonSetController<MemoryStorage>,
        (String, String),
        WorkQueue,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let controller = DaemonSetController::new(storage.clone());
        let mut uids = Vec::new();
        for n in ["ds1", "ds2"] {
            let ds = make_test_daemonset(n, "default");
            uids.push(ds.metadata.uid.clone());
            storage
                .create(&format!("/registry/daemonsets/default/{n}"), &ds)
                .await
                .unwrap();
        }
        (
            controller,
            (uids[0].clone(), uids[1].clone()),
            WorkQueue::new(),
        )
    }

    async fn drain(queue: &WorkQueue) -> Vec<String> {
        let mut keys = Vec::new();
        while queue.len().await > 0 {
            let k = queue.get().await.unwrap();
            queue.done(&k).await;
            keys.push(k);
        }
        keys.sort();
        keys
    }

    /// TestAddPod: a pod owned by a DaemonSet enqueues exactly that one.
    #[tokio::test]
    async fn an_owned_pod_enqueues_only_its_daemonset() {
        let (c, (u1, u2), q) = two_daemonsets().await;
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("p1"),
                pod_json("p1", Some(("ds1", &u1)), &[("app", "ds1")], "1"),
            ),
            &q,
        )
        .await;
        assert_eq!(drain(&q).await, vec!["daemonsets/default/ds1"]);
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("p2"),
                pod_json("p2", Some(("ds2", &u2)), &[("app", "ds2")], "1"),
            ),
            &q,
        )
        .await;
        assert_eq!(drain(&q).await, vec!["daemonsets/default/ds2"]);
    }

    /// `resolveControllerRef`: a UID that does not match the DaemonSet found by
    /// name, or a different kind, resolves to nothing (daemon_controller.go:769).
    #[tokio::test]
    async fn a_pod_owned_by_another_uid_or_kind_enqueues_nothing() {
        let (c, _, q) = two_daemonsets().await;
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("p1"),
                pod_json("p1", Some(("ds1", "stale-uid")), &[("app", "ds1")], "1"),
            ),
            &q,
        )
        .await;
        let wrong_kind = pod_json("p2", Some(("ds1", "x")), &[("app", "ds1")], "1")
            .replace("\"DaemonSet\"", "\"ReplicaSet\"");
        c.deliver_pod_event(&WatchEvent::Added(pod_key("p2"), wrong_kind), &q)
            .await;
        assert!(drain(&q).await.is_empty());
    }

    /// TestAddPodOrphan: an orphan wakes every DaemonSet whose selector
    /// matches its labels (`GetPodDaemonSets`) and none that do not.
    #[tokio::test]
    async fn an_orphan_wakes_the_daemonsets_whose_selector_matches() {
        let (c, _, q) = two_daemonsets().await;
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("stray"),
                pod_json("stray", None, &[("app", "nope")], "1"),
            ),
            &q,
        )
        .await;
        // `len(pod.Labels) == 0` matches nothing either.
        c.deliver_pod_event(
            &WatchEvent::Added(pod_key("bare"), pod_json("bare", None, &[], "1")),
            &q,
        )
        .await;
        assert!(drain(&q).await.is_empty());
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("adoptable"),
                pod_json("adoptable", None, &[("app", "ds2")], "1"),
            ),
            &q,
        )
        .await;
        assert_eq!(drain(&q).await, vec!["daemonsets/default/ds2"]);
    }

    /// TestUpdatePod + the "periodic resync" guard: an update with the same
    /// resourceVersion is ignored, a new one wakes the owner.
    #[tokio::test]
    async fn an_updated_owned_pod_enqueues_its_daemonset_unless_resync() {
        let (c, (u1, _), q) = two_daemonsets().await;
        let p = |rv: &str| pod_json("p1", Some(("ds1", &u1)), &[("app", "ds1")], rv);
        c.deliver_pod_event(&WatchEvent::Added(pod_key("p1"), p("1")), &q)
            .await;
        drain(&q).await;
        c.deliver_pod_event(&WatchEvent::Modified(pod_key("p1"), p("1")), &q)
            .await;
        assert!(drain(&q).await.is_empty(), "same resourceVersion");
        c.deliver_pod_event(&WatchEvent::Modified(pod_key("p1"), p("2")), &q)
            .await;
        assert_eq!(drain(&q).await, vec!["daemonsets/default/ds1"]);
    }

    /// TestUpdatePodOrphanSameLabels / TestUpdatePodOrphanWithNewLabels.
    #[tokio::test]
    async fn an_orphan_update_wakes_matching_sets_only_when_labels_change() {
        let (c, _, q) = two_daemonsets().await;
        let p = |rv: &str, app: &str| pod_json("o", None, &[("app", app)], rv);
        c.deliver_pod_event(&WatchEvent::Added(pod_key("o"), p("1", "nope")), &q)
            .await;
        drain(&q).await;
        c.deliver_pod_event(&WatchEvent::Modified(pod_key("o"), p("2", "nope")), &q)
            .await;
        assert!(drain(&q).await.is_empty(), "same labels");
        c.deliver_pod_event(&WatchEvent::Modified(pod_key("o"), p("3", "ds1")), &q)
            .await;
        assert_eq!(drain(&q).await, vec!["daemonsets/default/ds1"]);
    }

    /// TestUpdatePodChangeControllerRef: both the old and the new owner wake.
    #[tokio::test]
    async fn changing_the_controller_ref_wakes_old_and_new_owner() {
        let (c, (u1, u2), q) = two_daemonsets().await;
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("p"),
                pod_json("p", Some(("ds1", &u1)), &[("app", "ds1")], "1"),
            ),
            &q,
        )
        .await;
        drain(&q).await;
        c.deliver_pod_event(
            &WatchEvent::Modified(
                pod_key("p"),
                pod_json("p", Some(("ds2", &u2)), &[("app", "ds1")], "2"),
            ),
            &q,
        )
        .await;
        assert_eq!(
            drain(&q).await,
            vec!["daemonsets/default/ds1", "daemonsets/default/ds2"]
        );
    }

    /// TestUpdatePodControllerRefRemoved: the old owner wakes, and so do the
    /// sets that could now adopt the orphan.
    #[tokio::test]
    async fn removing_the_controller_ref_wakes_the_old_owner_and_adopters() {
        let (c, (u1, _), q) = two_daemonsets().await;
        c.deliver_pod_event(
            &WatchEvent::Added(
                pod_key("p"),
                pod_json("p", Some(("ds1", &u1)), &[("app", "ds2")], "1"),
            ),
            &q,
        )
        .await;
        drain(&q).await;
        c.deliver_pod_event(
            &WatchEvent::Modified(pod_key("p"), pod_json("p", None, &[("app", "ds2")], "2")),
            &q,
        )
        .await;
        assert_eq!(
            drain(&q).await,
            vec!["daemonsets/default/ds1", "daemonsets/default/ds2"]
        );
    }

    /// TestDeletePod / TestDeletePodOrphan: deleting an owned pod wakes its
    /// owner only; deleting an orphan wakes nobody ("No controller should care
    /// about orphans being deleted").
    #[tokio::test]
    async fn deleting_an_owned_pod_wakes_its_owner_and_an_orphan_nobody() {
        let (c, (u1, _), q) = two_daemonsets().await;
        let owned = pod_json("p", Some(("ds1", &u1)), &[("app", "ds1")], "1");
        c.deliver_pod_event(&WatchEvent::Added(pod_key("p"), owned.clone()), &q)
            .await;
        drain(&q).await;
        c.deliver_pod_event(&WatchEvent::Deleted(pod_key("p"), owned), &q)
            .await;
        assert_eq!(drain(&q).await, vec!["daemonsets/default/ds1"]);
        let orphan = pod_json("o", None, &[("app", "ds1")], "1");
        c.deliver_pod_event(&WatchEvent::Added(pod_key("o"), orphan.clone()), &q)
            .await;
        drain(&q).await;
        c.deliver_pod_event(&WatchEvent::Deleted(pod_key("o"), orphan), &q)
            .await;
        assert!(drain(&q).await.is_empty());
    }

    /// `updatePod` (daemon_controller.go:573-632): a pod turning Ready requeues
    /// the DaemonSet after `minReadySeconds` PLUS ONE second ("Add a second to
    /// avoid milliseconds skew in AddAfter").
    #[tokio::test]
    async fn a_pod_turning_ready_requeues_after_min_ready_seconds_plus_one() {
        let (c, _, _) = two_daemonsets().await;
        assert_eq!(c.min_ready_requeue_delay(10), Duration::from_secs(11));
    }
}
