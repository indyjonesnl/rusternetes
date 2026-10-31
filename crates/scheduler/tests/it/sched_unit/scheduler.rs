//! Unit tests moved from `src/scheduler.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use rusternetes_common::{
        resources::{EventSource, EventType, Node, ObjectReference, Pod, PriorityClass},
        types::Phase,
    };
    pub use rusternetes_scheduler::advanced::{
        check_host_port_conflicts, check_node_affinity, check_pod_affinity,
        check_pod_anti_affinity, check_taints_tolerations, check_topology_spread_constraints,
        parse_resource_quantity,
    };
    pub use rusternetes_scheduler::data_plane::{ApiBackend, DataPlane};
    pub use rusternetes_scheduler::diagnosis::{fit_error_message, Diagnosis};
    pub use rusternetes_scheduler::framework_runtime::{names, ExtensionPoint};
    pub use rusternetes_scheduler::profile::DEFAULT_SCHEDULER_NAME;
    pub use rusternetes_scheduler::queue::{
        hints, node_scheduling_properties_change, pod_scheduling_properties_change,
        priority_sort_less, ActionType, ClusterEvent, EventObject, EventResource, NominatingInfo,
        PriorityQueue, PriorityQueueOptions, QueuedPodInfo, EVENT_ASSIGNED_POD_ADD,
        EVENT_ASSIGNED_POD_DELETE,
    };
    pub use rusternetes_scheduler::volume_binding::{self, PreFilterOutcome};
    pub use rusternetes_storage::{
        build_key, build_prefix, EventRecorder, Storage, StorageBackend, WatchEvent,
    };
    pub use std::sync::atomic::{AtomicUsize, Ordering};
    pub use std::{collections::HashMap, sync::Arc, time::Duration};
    pub use tracing::{debug, error, info, warn};
}

// Unit tests for the DisruptionTarget condition are verified inline:
mod tests {
    fn test_queue() -> Arc<PriorityQueue> {
        PriorityQueue::new(priority_sort_less(), PriorityQueueOptions::default())
    }

    use super::__parent::*;
    use rusternetes_common::resources::{Container, PodSpec, PodStatus};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_scheduler::scheduler::*;
    use rusternetes_storage::{MemoryStorage, WatchStream};

    fn make_node(name: &str) -> Node {
        Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.labels = Some({
                    let mut labels = HashMap::new();
                    labels.insert("kubernetes.io/os".to_string(), "linux".to_string());
                    labels.insert("kubernetes.io/arch".to_string(), "amd64".to_string());
                    labels.insert("kubernetes.io/hostname".to_string(), name.to_string());
                    labels
                });
                m
            },
            spec: None,
            status: Some(rusternetes_common::resources::NodeStatus {
                conditions: Some(vec![rusternetes_common::resources::NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: Some("KubeletReady".to_string()),
                    message: Some("kubelet is posting ready status".to_string()),
                    last_heartbeat_time: Some(Utc::now()),
                    last_transition_time: Some(Utc::now()),
                }]),
                capacity: Some({
                    let mut m = HashMap::new();
                    m.insert("cpu".to_string(), "4".to_string());
                    m.insert("memory".to_string(), "8Gi".to_string());
                    m.insert("pods".to_string(), "110".to_string());
                    m
                }),
                allocatable: Some({
                    let mut m = HashMap::new();
                    m.insert("cpu".to_string(), "4".to_string());
                    m.insert("memory".to_string(), "8Gi".to_string());
                    m.insert("pods".to_string(), "110".to_string());
                    m
                }),
                addresses: None,
                daemon_endpoints: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                config: None,
                runtime_handlers: None,
                features: None,
                declared_features: None,
            }),
        }
    }

    fn make_pending_pod(name: &str, ns: &str) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some(ns.to_string());
                m
            },
            spec: Some(PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "main".to_string(),
                    image: "busybox".to_string(),
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
                scheduler_name: Some("default-scheduler".to_string()),
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: None,
                termination_grace_period_seconds: None,
                dns_policy: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                automount_service_account_token: None,
                node_name: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                security_context: None,
                image_pull_secrets: None,
                hostname: None,
                subdomain: None,
                affinity: None,
                tolerations: None,
                host_aliases: None,
                priority_class_name: None,
                priority: None,
                preemption_policy: None,
                overhead: None,
                topology_spread_constraints: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_config: None,
                enable_service_links: None,
                readiness_gates: None,
                runtime_class_name: None,
                os: None,
                set_hostname_as_fqdn: None,
                share_process_namespace: None,
                scheduling_gates: None,
                resource_claims: None,
                host_users: None,
                resources: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                start_time: None,
                qos_class: None,
                nominated_node_name: None,
                host_i_ps: None,
                pod_i_ps: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        }
    }

    #[tokio::test]
    async fn test_scheduler_assigns_pod_to_node() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        // Create two nodes
        let node1 = make_node("node-1");
        let node2 = make_node("node-2");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();
        storage
            .create("/registry/nodes/node-2", &node2)
            .await
            .unwrap();

        // Create a pending pod
        let pod = make_pending_pod("test-pod", "default");
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        // Run one scheduling cycle
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should now have a node name assigned
        let scheduled_pod: Pod = storage
            .get("/registry/pods/default/test-pod")
            .await
            .unwrap();
        let node_name = scheduled_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref());
        assert!(
            node_name.is_some(),
            "Pod should be assigned to a node after scheduling"
        );
        let node_name = node_name.unwrap();
        assert!(
            node_name == "node-1" || node_name == "node-2",
            "Pod should be on node-1 or node-2, got: {}",
            node_name
        );

        // Pod should have PodScheduled condition
        let conditions = scheduled_pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        assert!(conditions.is_some(), "Pod should have conditions");
        let has_scheduled = conditions
            .unwrap()
            .iter()
            .any(|c| c.condition_type == "PodScheduled" && c.status == "True");
        assert!(has_scheduled, "Pod should have PodScheduled=True condition");
    }

    /// VolumeBinding wiring (#3017): a pod whose PVC does not exist is
    /// UnschedulableAndUnresolvable; once a bound PVC whose PV fits only
    /// `node-2` exists, the pod lands on `node-2`.
    #[tokio::test]
    async fn test_volume_binding_filters_nodes_by_pv_affinity() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        for n in ["node-1", "node-2"] {
            let mut node = make_node(n);
            node.metadata.labels =
                Some(HashMap::from([("kubernetes.io/hostname".into(), n.into())]));
            storage
                .create(&format!("/registry/nodes/{n}"), &node)
                .await
                .unwrap();
        }
        let mut pod = make_pending_pod("vol-pod", "default");
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "v", "persistentVolumeClaim": {"claimName": "claim"}}),
        )
        .unwrap()]);
        storage
            .create("/registry/pods/default/vol-pod", &pod)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();
        let p: Pod = storage.get("/registry/pods/default/vol-pod").await.unwrap();
        assert!(
            p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none(),
            "pod with a missing PVC must not be scheduled"
        );

        let pvc: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default",
                    "annotations": {"pv.kubernetes.io/bind-completed": "yes"}},
                "spec": {"volumeName": "pv1", "storageClassName": "std",
                    "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Bound"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/claim", &pvc)
            .await
            .unwrap();
        let pv: rusternetes_common::resources::PersistentVolume =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolume",
                "metadata": {"name": "pv1"},
                "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "std", "hostPath": {"path": "/x"},
                    "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                        {"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-2"]}]}]}}},
                "status": {"phase": "Bound"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumes/pv1", &pv)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();
        let p: Pod = storage.get("/registry/pods/default/vol-pod").await.unwrap();
        assert_eq!(
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-2")
        );
    }

    /// VolumeBinding Score wiring (#3036), StorageCapacityScoring on: with the
    /// default shape (utilization 0 -> 100, 100 -> 0) the node whose static
    /// PV leaves more room wins even though `node-1` would win the tie-break.
    // `serial`: it flips a process-global feature gate that
    // `scheduler_config::tests::wired_into_validate_profiles` (also serial)
    // resets; unserialised, plain `cargo test` raced them.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_volume_binding_score_prefers_node_with_more_free_capacity() {
        let _gate = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
            true,
        );
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        for n in ["node-1", "node-2"] {
            let mut node = make_node(n);
            node.metadata.labels =
                Some(HashMap::from([("kubernetes.io/hostname".into(), n.into())]));
            storage
                .create(&format!("/registry/nodes/{n}"), &node)
                .await
                .unwrap();
        }
        let class: rusternetes_common::resources::StorageClass =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": "wait"},
                "provisioner": "kubernetes.io/no-provisioner",
                "volumeBindingMode": "WaitForFirstConsumer"
            }))
            .unwrap();
        storage
            .create("/registry/storageclasses/wait", &class)
            .await
            .unwrap();
        let pvc: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default"},
                "spec": {"storageClassName": "wait",
                    "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/claim", &pvc)
            .await
            .unwrap();
        for (node, size) in [("node-1", "1Gi"), ("node-2", "10Gi")] {
            let pv: rusternetes_common::resources::PersistentVolume =
                serde_json::from_value(serde_json::json!({
                    "apiVersion": "v1", "kind": "PersistentVolume",
                    "metadata": {"name": format!("pv-{node}")},
                    "spec": {"capacity": {"storage": size}, "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "wait", "hostPath": {"path": "/x"},
                        "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                            {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}}},
                    "status": {"phase": "Available"}
                }))
                .unwrap();
            storage
                .create(&format!("/registry/persistentvolumes/pv-{node}"), &pv)
                .await
                .unwrap();
        }
        let mut pod = make_pending_pod("score-pod", "default");
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "v", "persistentVolumeClaim": {"claimName": "claim"}}),
        )
        .unwrap()]);
        storage
            .create("/registry/pods/default/score-pod", &pod)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();
        let p: Pod = storage
            .get("/registry/pods/default/score-pod")
            .await
            .unwrap();
        assert_eq!(
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-2")
        );
    }

    /// Fixture: two nodes, a WaitForFirstConsumer class, an unbound claim and
    /// one static PV on node-2, plus a pod using the claim.
    async fn wffc_fixture(storage: &Arc<MemoryStorage>) {
        for n in ["node-1", "node-2"] {
            let mut node = make_node(n);
            node.metadata.labels =
                Some(HashMap::from([("kubernetes.io/hostname".into(), n.into())]));
            storage
                .create(&format!("/registry/nodes/{n}"), &node)
                .await
                .unwrap();
        }
        let class: rusternetes_common::resources::StorageClass =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": "wait"},
                "provisioner": "kubernetes.io/no-provisioner",
                "volumeBindingMode": "WaitForFirstConsumer"
            }))
            .unwrap();
        storage
            .create("/registry/storageclasses/wait", &class)
            .await
            .unwrap();
        let pvc: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default", "uid": "claim-uid"},
                "spec": {"storageClassName": "wait",
                    "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/claim", &pvc)
            .await
            .unwrap();
        let pv: rusternetes_common::resources::PersistentVolume =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolume",
                "metadata": {"name": "pv-node-2"},
                "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "wait", "hostPath": {"path": "/x"},
                    "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                        {"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-2"]}]}]}}},
                "status": {"phase": "Available"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumes/pv-node-2", &pv)
            .await
            .unwrap();
        let mut pod = make_pending_pod("wffc-pod", "default");
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "v", "persistentVolumeClaim": {"claimName": "claim"}}),
        )
        .unwrap()]);
        storage
            .create("/registry/pods/default/wffc-pod", &pod)
            .await
            .unwrap();
    }

    /// Reserve -> PreBind -> Bind (schedule_one.go:211, :327, :332): the pod is
    /// only bound once PreBind has written the PV claimRef and the PV
    /// controller (simulated here) has completed the PVC binding.
    #[tokio::test]
    async fn test_try_schedule_pod_prebinds_volumes_before_binding_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let mut scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        scheduler
            .profiles
            .get_mut("default-scheduler")
            .unwrap()
            .volume_bind_timeout = Duration::from_secs(10);
        scheduler.volume_bind_poll = Duration::from_millis(20);
        wffc_fixture(&storage).await;

        // PV controller: once the scheduler has set claimRef, bind the PVC.
        let st = storage.clone();
        let controller = tokio::spawn(async move {
            for _ in 0..250 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let pv: rusternetes_common::resources::PersistentVolume = st
                    .get("/registry/persistentvolumes/pv-node-2")
                    .await
                    .unwrap();
                if pv.spec.claim_ref.is_some() {
                    let mut pvc: rusternetes_common::resources::PersistentVolumeClaim = st
                        .get("/registry/persistentvolumeclaims/default/claim")
                        .await
                        .unwrap();
                    pvc.spec.volume_name = Some("pv-node-2".into());
                    pvc.metadata
                        .annotations
                        .get_or_insert_with(HashMap::new)
                        .insert("pv.kubernetes.io/bind-completed".into(), "yes".into());
                    st.update("/registry/persistentvolumeclaims/default/claim", &pvc)
                        .await
                        .unwrap();
                    return;
                }
            }
        });

        scheduler
            .try_schedule_pod(&test_queue(), "pods/default/wffc-pod")
            .await
            .unwrap();
        controller.await.unwrap();

        let pv: rusternetes_common::resources::PersistentVolume = storage
            .get("/registry/persistentvolumes/pv-node-2")
            .await
            .unwrap();
        let cr = pv.spec.claim_ref.expect("PreBind must set the PV claimRef");
        assert_eq!(cr.name.as_deref(), Some("claim"));
        let p: Pod = storage
            .get("/registry/pods/default/wffc-pod")
            .await
            .unwrap();
        assert_eq!(
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-2")
        );
    }

    /// A PreBind failure (BindTimeoutSeconds elapsed without the PV controller
    /// binding) must leave the pod unscheduled and return an error so the
    /// work queue retries (handleBindingCycleError, schedule_one.go:356-368).
    #[tokio::test]
    async fn test_try_schedule_pod_prebind_timeout_leaves_pod_unbound() {
        let storage = Arc::new(MemoryStorage::new());
        let mut scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        scheduler
            .profiles
            .get_mut("default-scheduler")
            .unwrap()
            .volume_bind_timeout = Duration::from_millis(200);
        scheduler.volume_bind_poll = Duration::from_millis(20);
        wffc_fixture(&storage).await;

        let r = scheduler
            .try_schedule_pod(&test_queue(), "pods/default/wffc-pod")
            .await;
        assert!(r.is_err(), "PreBind timeout must surface as an error");
        let p: Pod = storage
            .get("/registry/pods/default/wffc-pod")
            .await
            .unwrap();
        assert!(p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none());
    }

    #[tokio::test]
    async fn test_scheduler_emits_event_for_unschedulable_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        // Create a node with a label
        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        // Create a pod with a nodeSelector that doesn't match any node
        let mut pod = make_pending_pod("unsched-pod", "default");
        if let Some(ref mut spec) = pod.spec {
            spec.node_selector = Some({
                let mut m = HashMap::new();
                m.insert("disktype".to_string(), "ssd".to_string());
                m
            });
        }
        storage
            .create("/registry/pods/default/unsched-pod", &pod)
            .await
            .unwrap();

        // Run scheduling
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should NOT have a node name
        let unsched_pod: Pod = storage
            .get("/registry/pods/default/unsched-pod")
            .await
            .unwrap();
        assert!(
            unsched_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .is_none(),
            "Unschedulable pod should not be assigned to a node"
        );

        // Pod should have PodScheduled=False condition
        let conditions = unsched_pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        assert!(conditions.is_some(), "Pod should have conditions");
        let has_unschedulable = conditions.unwrap().iter().any(|c| {
            c.condition_type == "PodScheduled"
                && c.status == "False"
                && c.reason.as_deref() == Some("Unschedulable")
        });
        assert!(
            has_unschedulable,
            "Pod should have PodScheduled=False with Unschedulable reason"
        );

        // Should have a FailedScheduling event
        let events: Vec<serde_json::Value> = storage
            .list("/registry/events/default/")
            .await
            .unwrap_or_default();
        let has_failed_event = events.iter().any(|e| {
            e.get("reason")
                .and_then(|r| r.as_str())
                .map(|r| r == "FailedScheduling")
                .unwrap_or(false)
        });
        assert!(
            has_failed_event,
            "Should have a FailedScheduling event for unschedulable pod"
        );
    }

    #[tokio::test]
    async fn test_scheduler_emits_scheduled_event_via_recorder() {
        use rusternetes_common::resources::{Event, EventType, ObjectReference};

        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        let mut pod = make_pending_pod("sched-pod", "default");
        pod.metadata.uid = "sched-pod-uid-1234".to_string();
        storage
            .create("/registry/pods/default/sched-pod", &pod)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();

        // Pod must be bound.
        let bound: Pod = storage
            .get("/registry/pods/default/sched-pod")
            .await
            .unwrap();
        assert_eq!(
            bound.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-1"),
            "pod should be bound to node-1"
        );

        // The Scheduled event must be retrievable at the recorder's STABLE key
        // (object.reason.uid) — proving it was routed through EventRecorder, not
        // the old ad-hoc `{pod}.sched.{timestamp}` write that never deduplicates.
        let involved = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("sched-pod".to_string()),
            uid: Some("sched-pod-uid-1234".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };
        let name = Event::generate_name(&involved, "Scheduled");
        let key = format!("/registry/events/default/{}", name);
        let ev: Event = storage
            .get(&key)
            .await
            .expect("Scheduled event must be stored at the recorder's stable key");

        assert_eq!(ev.reason, "Scheduled");
        assert!(matches!(ev.event_type, EventType::Normal));
        assert_eq!(ev.source.component, "default-scheduler");
        assert_eq!(
            ev.message,
            "Successfully assigned default/sched-pod to node-1"
        );
    }

    /// #3346: with several profiles the scheduler schedules every pod whose
    /// `spec.schedulerName` names a profile (`responsibleForPod`,
    /// eventhandlers.go:474-476), reports each Scheduled event under that
    /// profile's name (`NewRecorderFactory`, profile.go:85-90), and leaves pods
    /// of unknown scheduler names alone.
    #[tokio::test]
    async fn test_multi_profile_routes_pods_by_scheduler_name() {
        use rusternetes_common::resources::{Event, ObjectReference};
        let storage = Arc::new(MemoryStorage::new());
        let profiles = rusternetes_scheduler::profile::Map::new_map(vec![
            rusternetes_scheduler::profile::Profile::named("default-scheduler"),
            rusternetes_scheduler::profile::Profile::named("my-sched"),
        ])
        .unwrap();
        let scheduler = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
        storage
            .create("/registry/nodes/node-1", &make_node("node-1"))
            .await
            .unwrap();
        for (name, sn) in [
            ("p-default", "default-scheduler"),
            ("p-mine", "my-sched"),
            ("p-other", "other-sched"),
        ] {
            let mut pod = make_pending_pod(name, "default");
            pod.metadata.uid = format!("{name}-uid");
            pod.spec.as_mut().unwrap().scheduler_name = Some(sn.to_string());
            storage
                .create(&format!("/registry/pods/default/{name}"), &pod)
                .await
                .unwrap();
        }
        scheduler.schedule_pending_pods().await.unwrap();
        let node_of = |name: &'static str| {
            let storage = storage.clone();
            async move {
                storage
                    .get::<Pod>(&format!("/registry/pods/default/{name}"))
                    .await
                    .unwrap()
                    .spec
                    .and_then(|s| s.node_name)
            }
        };
        assert_eq!(node_of("p-default").await.as_deref(), Some("node-1"));
        assert_eq!(node_of("p-mine").await.as_deref(), Some("node-1"));
        assert_eq!(node_of("p-other").await, None);
        for (name, comp) in [("p-default", "default-scheduler"), ("p-mine", "my-sched")] {
            let involved = ObjectReference {
                kind: Some("Pod".to_string()),
                namespace: Some("default".to_string()),
                name: Some(name.to_string()),
                uid: Some(format!("{name}-uid")),
                api_version: Some("v1".to_string()),
                resource_version: None,
                field_path: None,
            };
            let key = format!(
                "/registry/events/default/{}",
                Event::generate_name(&involved, "Scheduled")
            );
            let ev: Event = storage.get(&key).await.unwrap();
            assert_eq!(ev.source.component, comp, "{name}");
        }
    }

    /// #3346: the `VolumeBinding` bind timeout is read from the pod's own
    /// profile (each profile has its own framework + plugin args).
    #[tokio::test]
    async fn test_profile_for_pod_resolves_per_profile_args() {
        let mut slow = rusternetes_scheduler::profile::Profile::named("slow");
        slow.volume_bind_timeout = Duration::from_secs(7);
        let profiles = rusternetes_scheduler::profile::Map::new_map(vec![
            rusternetes_scheduler::profile::Profile::named("default-scheduler"),
            slow,
        ])
        .unwrap();
        let scheduler = Scheduler::new_with_profiles(Arc::new(MemoryStorage::new()), 1, profiles);
        let mut pod = make_pending_pod("x", "default");
        pod.spec.as_mut().unwrap().scheduler_name = Some("slow".to_string());
        assert_eq!(
            scheduler.profile_for(&pod).unwrap().volume_bind_timeout,
            Duration::from_secs(7)
        );
        pod.spec.as_mut().unwrap().scheduler_name = Some("nope".to_string());
        assert_eq!(
            scheduler.profile_for(&pod).unwrap_err(),
            "profile not found for scheduler name \"nope\""
        );
    }

    // schedule_one_test.go TestNumFeasibleNodesToFind.
    #[test]
    fn test_num_feasible_nodes_to_find() {
        // (global, profile, numAllNodes) -> want
        for (global, profile, all, want) in [
            (0, None, 10, 10),
            (0, Some(40), 10, 10),
            (0, None, 1000, 420),
            (0, Some(40), 1000, 400),
            (100, Some(40), 1000, 400),
            (40, None, 1000, 400),
            (0, None, 6000, 300),
            (0, Some(40), 6000, 2400),
        ] {
            assert_eq!(
                num_feasible_nodes_to_find(profile, global, all),
                want,
                "global={global} profile={profile:?} nodes={all}"
            );
        }
    }

    /// schedule_one_test.go TestFairEvaluationForNodes: with 500 nodes and
    /// `percentageOfNodesToScore: 30` every cycle finds 150 feasible nodes and
    /// `nextStartNodeIndex` advances by exactly that, wrapping around.
    #[tokio::test]
    async fn test_fair_evaluation_for_nodes() {
        let num_all_nodes = 500usize;
        let mut profiles = rusternetes_scheduler::profile::Map::new_map(vec![
            rusternetes_scheduler::profile::Profile::named(DEFAULT_SCHEDULER_NAME),
        ])
        .unwrap();
        profiles.percentage_of_nodes_to_score = 30;
        let scheduler = Scheduler::new_with_profiles(Arc::new(MemoryStorage::new()), 1, profiles);
        let nodes: Vec<Node> = (0..num_all_nodes)
            .map(|i| make_node(&format!("n-{i:03}")))
            .collect();
        let pod = make_pending_pod("p", "default");
        let nodes_to_find = num_feasible_nodes_to_find(None, 30, num_all_nodes as i32) as usize;
        assert_eq!(nodes_to_find, 150);
        for i in 0..2 * (num_all_nodes / nodes_to_find + 1) {
            let mut diag = Diagnosis::default();
            let picked = scheduler
                .select_node_inner(&pod, &nodes, &[], &HashMap::new(), &mut diag)
                .await;
            assert!(picked.is_some());
            assert_eq!(
                scheduler.next_start_node_index.load(Ordering::Relaxed),
                (i + 1) * nodes_to_find % num_all_nodes,
                "cycle {i}"
            );
        }
    }

    /// schedule_one_test.go TestPreferNominatedNodeFilterCallCounts, observed
    /// through `nextStartNodeIndex`: a nominated node that passes every filter
    /// is chosen without the node search (schedule_one.go:518-530), so the
    /// search cursor does not advance; one that fails falls through to the
    /// full search, which does advance it.
    #[tokio::test]
    async fn test_prefer_nominated_node() {
        // 300 nodes so the search stops early (141 feasible) and the cursor
        // visibly moves.
        let nodes: Vec<Node> = (0..300).map(|i| make_node(&format!("node{i}"))).collect();
        for (nominated, unschedulable, want_node, want_cursor_advanced) in [
            (Some("node250"), None, Some("node250"), false),
            (None, None, None, true),
            // nominated node cannot pass the filters: full search.
            (Some("node1"), Some("node1"), None, true),
            // nominated node is not in the snapshot: logged, full search.
            (Some("gone"), None, None, true),
        ] {
            let mut nodes = nodes.clone();
            if let Some(u) = unschedulable {
                for n in nodes.iter_mut().filter(|n| n.metadata.name == u) {
                    n.spec = Some(
                        serde_json::from_value(serde_json::json!({"unschedulable": true})).unwrap(),
                    );
                }
            }
            let scheduler = Scheduler::new_with_name(
                Arc::new(MemoryStorage::new()),
                1,
                DEFAULT_SCHEDULER_NAME.to_string(),
            );
            let mut pod = make_pending_pod("p", "default");
            if let Some(n) = nominated {
                pod.status
                    .get_or_insert_with(Default::default)
                    .nominated_node_name = Some(n.to_string());
            }
            let mut diag = Diagnosis::default();
            let picked = scheduler
                .select_node_inner(&pod, &nodes, &[], &HashMap::new(), &mut diag)
                .await
                .expect("a node fits");
            if let Some(w) = want_node {
                assert_eq!(picked.0.metadata.name, w, "nominated={nominated:?}");
            }
            if nominated == Some("node1") {
                assert_ne!(picked.0.metadata.name, "node1");
            }
            let cursor = scheduler.next_start_node_index.load(Ordering::Relaxed);
            assert_eq!(
                cursor != 0,
                want_cursor_advanced,
                "nominated={nominated:?} cursor={cursor}"
            );
        }
    }

    fn pod_pinned_to(names: &[&str]) -> Pod {
        let mut pod = make_pending_pod("p", "default");
        pod.spec.as_mut().unwrap().affinity = Some(
            serde_json::from_value(serde_json::json!({"nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                    {"matchFields": [{"key": "metadata.name", "operator": "In", "values": names}]}
                ]}
            }}))
            .unwrap(),
        );
        pod
    }

    /// `findNodesThatFitPod` (schedule_one.go:549-562): the NodeAffinity
    /// PreFilterResult restricts the search to the named node, and the
    /// restricting plugin is an unschedulable plugin of the cycle.
    #[tokio::test]
    async fn pre_filter_result_restricts_the_search() {
        let nodes: Vec<Node> = (0..5).map(|i| make_node(&format!("node{i}"))).collect();
        let scheduler = Scheduler::new_with_name(
            Arc::new(MemoryStorage::new()),
            1,
            DEFAULT_SCHEDULER_NAME.to_string(),
        );
        let mut diag = Diagnosis::default();
        let picked = scheduler
            .select_node_inner(
                &pod_pinned_to(&["node3"]),
                &nodes,
                &[],
                &HashMap::new(),
                &mut diag,
            )
            .await
            .expect("node3 fits");
        assert_eq!(picked.0.metadata.name, "node3");
        assert!(diag.unschedulable_plugins.contains("NodeAffinity"));
    }

    /// A PreFilterResult naming no node of the snapshot leaves every node
    /// absent: "node(s) didn't satisfy plugin(s) [NodeAffinity]".
    #[tokio::test]
    async fn pre_filter_result_with_no_existing_node_is_unschedulable() {
        let nodes: Vec<Node> = (0..3).map(|i| make_node(&format!("node{i}"))).collect();
        let scheduler = Scheduler::new_with_name(
            Arc::new(MemoryStorage::new()),
            1,
            DEFAULT_SCHEDULER_NAME.to_string(),
        );
        let mut diag = Diagnosis::default();
        let picked = scheduler
            .select_node_inner(
                &pod_pinned_to(&["gone"]),
                &nodes,
                &[],
                &HashMap::new(),
                &mut diag,
            )
            .await;
        assert!(picked.is_none());
        assert_eq!(
            rusternetes_scheduler::diagnosis::fit_error_message(nodes.len(), &diag),
            "0/3 nodes are available: 3 node(s) didn't satisfy plugin(s) [NodeAffinity]."
        );
        assert!(diag.unschedulable_plugins.contains("NodeAffinity"));
    }

    /// Conflicting `metadata.name` requirements are rejected by PreFilter
    /// itself, with the plugin recorded and no per-node reasons.
    #[tokio::test]
    async fn conflicting_pre_filter_rejects_the_pod() {
        let nodes: Vec<Node> = (0..3).map(|i| make_node(&format!("node{i}"))).collect();
        let scheduler = Scheduler::new_with_name(
            Arc::new(MemoryStorage::new()),
            1,
            DEFAULT_SCHEDULER_NAME.to_string(),
        );
        let mut pod = pod_pinned_to(&["node1"]);
        pod.spec
            .as_mut()
            .unwrap()
            .affinity
            .as_mut()
            .unwrap()
            .node_affinity
            .as_mut()
            .unwrap()
            .required_during_scheduling_ignored_during_execution
            .as_mut()
            .unwrap()
            .node_selector_terms[0]
            .match_fields
            .as_mut()
            .unwrap()
            .push(
                serde_json::from_value(serde_json::json!(
                    {"key": "metadata.name", "operator": "In", "values": ["node2"]}
                ))
                .unwrap(),
            );
        let mut diag = Diagnosis::default();
        assert!(scheduler
            .select_node_inner(&pod, &nodes, &[], &HashMap::new(), &mut diag)
            .await
            .is_none());
        assert_eq!(diag.pre_filter_msg, "pod affinity terms conflict");
        assert!(diag.unschedulable_plugins.contains("NodeAffinity"));
        assert!(diag.node_reasons.is_empty());
    }

    fn running_pod(name: &str, node: &str, cpu: &str, priority: i32) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "default", "uid": format!("{name}-uid")},
            "spec": {
                "nodeName": node,
                "priority": priority,
                "containers": [{
                    "name": "c", "image": "busybox",
                    "resources": {"requests": {"cpu": cpu}}
                }]
            },
            "status": {"phase": "Running"}
        }))
        .unwrap()
    }

    /// default_preemption_test.go TestCalculateNumCandidates /
    /// TestDryRunPreemption shape: with `minCandidateNodesPercentage: 0` and
    /// `minCandidateNodesAbsolute: 1` the dry run stops at the first candidate
    /// found from the offset (here n1, two victims); the default args
    /// (10% / 100) examine every node and pick the node with the fewest victims.
    #[tokio::test]
    async fn test_preemption_stops_after_min_candidate_nodes() {
        assert_eq!(calculate_num_candidates(5, 10, 100), 5);
        assert_eq!(calculate_num_candidates(1000, 10, 100), 100);
        assert_eq!(calculate_num_candidates(1000, 50, 100), 500);
        assert_eq!(calculate_num_candidates(1000, 0, 1), 1);

        let nodes = vec![make_node("n1"), make_node("n2"), make_node("n3")];
        let all_pods = vec![
            running_pod("a", "n1", "2", 0),
            running_pod("b", "n1", "2", 0),
            running_pod("c", "n2", "4", 0),
            running_pod("d", "n3", "4", 0),
        ];
        let mut preemptor = make_pending_pod("hi", "default");
        {
            let spec = preemptor.spec.as_mut().unwrap();
            spec.priority = Some(1000);
            spec.containers[0].resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(HashMap::from([("cpu".to_string(), "4".to_string())])),
                limits: None,
                claims: None,
            });
        }
        let run = |doc: &'static str| {
            let nodes = nodes.clone();
            let all_pods = all_pods.clone();
            let preemptor = preemptor.clone();
            async move {
                let profiles =
                    rusternetes_scheduler::scheduler_config::profiles_from_config(doc, false)
                        .unwrap();
                let scheduler =
                    Scheduler::new_with_profiles(Arc::new(MemoryStorage::new()), 1, profiles);
                scheduler
                    .preemption_offset_override
                    .store(0, Ordering::Relaxed);
                scheduler
                    .try_preempt(&preemptor, &nodes, &all_pods, &HashMap::new(), &[])
                    .await
                    .map(|(node, victims)| (node, victims.len()))
            }
        };
        assert_eq!(run("").await, Some(("n2".to_string(), 1)));
        let one_candidate = "profiles:\n- schedulerName: default-scheduler\n  pluginConfig:\n  - name: DefaultPreemption\n    args:\n      minCandidateNodesPercentage: 0\n      minCandidateNodesAbsolute: 1\n";
        assert_eq!(run(one_candidate).await, Some(("n1".to_string(), 2)));
    }

    #[tokio::test]
    async fn test_scheduler_does_not_reschedule_already_scheduled_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        // Create a pod that is already scheduled
        let mut pod = make_pending_pod("scheduled-pod", "default");
        if let Some(ref mut spec) = pod.spec {
            spec.node_name = Some("node-1".to_string());
        }
        storage
            .create("/registry/pods/default/scheduled-pod", &pod)
            .await
            .unwrap();

        // Run scheduling — should not touch already-scheduled pod
        scheduler.schedule_pending_pods().await.unwrap();

        let result_pod: Pod = storage
            .get("/registry/pods/default/scheduled-pod")
            .await
            .unwrap();
        assert_eq!(
            result_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref()),
            Some("node-1"),
            "Already-scheduled pod should remain on its node"
        );
    }

    /// Preemption should set deletionTimestamp and DisruptionTarget condition
    /// on evicted pods, but NOT delete them from storage. The kubelet handles
    /// actual cleanup. The conformance test needs to observe the condition.
    #[tokio::test]
    async fn test_preemption_sets_disruption_target_condition() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 2, "default-scheduler".to_string());

        // Create a node
        let node = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node)
            .await
            .unwrap();

        // Create a low-priority pod already scheduled on node-1
        let mut low_pod = make_pending_pod("low-pod", "default");
        low_pod.spec.as_mut().unwrap().node_name = Some("node-1".to_string());
        low_pod.spec.as_mut().unwrap().priority = Some(0);
        low_pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            pod_ip: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            start_time: None,
            qos_class: None,
            nominated_node_name: None,
            host_i_ps: None,
            pod_i_ps: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });
        storage
            .create("/registry/pods/default/low-pod", &low_pod)
            .await
            .unwrap();

        // Evict the pod (private method, accessible from within the module)
        let mut preemptor = make_pending_pod("high-pod", "default");
        preemptor.spec.as_mut().unwrap().scheduler_name = Some("my-sched".to_string());
        scheduler.evict_pod(&preemptor, &low_pod).await.unwrap();

        // The pod should still exist in storage with:
        // 1. deletionTimestamp set
        // 2. DisruptionTarget condition
        // 3. Phase = Failed
        let evicted: Pod = storage
            .get("/registry/pods/default/low-pod")
            .await
            .expect("Evicted pod should still exist in storage (not hard-deleted)");

        assert!(
            evicted.metadata.deletion_timestamp.is_some(),
            "Evicted pod should have deletionTimestamp set"
        );

        let status = evicted.status.as_ref().expect("Should have status");
        assert_eq!(
            status.phase,
            Some(Phase::Failed),
            "Evicted pod phase should be Failed"
        );
        assert_eq!(
            status.reason.as_deref(),
            Some("Preempted"),
            "Evicted pod reason should be Preempted"
        );

        let conditions = status.conditions.as_ref().expect("Should have conditions");
        let disruption = conditions
            .iter()
            .find(|c| c.condition_type == "DisruptionTarget");
        assert!(
            disruption.is_some(),
            "Evicted pod must have DisruptionTarget condition"
        );
        let dt = disruption.unwrap();
        assert_eq!(dt.status, "True");
        // preemption.go:191: the PREEMPTOR's scheduler name, not a global one.
        assert_eq!(
            dt.message.as_deref(),
            Some("my-sched: preempting to accommodate a higher priority pod")
        );
        assert_eq!(dt.reason.as_deref(), Some("PreemptionByScheduler"));
    }

    /// #2477: evicting a victim deletes that namespace's pod only. Upstream
    /// `util.DeletePod` deletes `Pods(pod.Namespace)` / `pod.Name`
    /// (pkg/scheduler/util/utils.go:137-138); a name-only scan evicted the
    /// same-named pod in every namespace.
    #[tokio::test]
    async fn test_evict_pod_leaves_same_named_pod_in_other_namespace() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 2, "default-scheduler".to_string());
        for ns in ["ns-a", "ns-b"] {
            let mut p = make_pending_pod("w", ns);
            p.spec.as_mut().unwrap().node_name = Some("node-1".to_string());
            storage
                .create(&format!("/registry/pods/{ns}/w"), &p)
                .await
                .unwrap();
        }
        let victim: Pod = storage.get("/registry/pods/ns-a/w").await.unwrap();

        let preemptor = make_pending_pod("high-pod", "ns-c");
        scheduler.evict_pod(&preemptor, &victim).await.unwrap();

        let a: Pod = storage.get("/registry/pods/ns-a/w").await.unwrap();
        let b: Pod = storage.get("/registry/pods/ns-b/w").await.unwrap();
        assert!(a.metadata.deletion_timestamp.is_some(), "victim evicted");
        assert!(
            b.metadata.deletion_timestamp.is_none(),
            "same-named pod in another namespace must be untouched"
        );
    }

    /// K8s treats nodeName="" the same as nodeName=nil (unscheduled).
    /// Some Go JSON serialization produces "nodeName":"" in pod templates.
    /// The scheduler MUST consider these pods as pending and schedule them.
    /// K8s ref: pkg/scheduler/schedule_one.go — pod is unscheduled if
    /// len(pod.Spec.NodeName) == 0
    #[tokio::test]
    async fn test_empty_node_name_treated_as_unscheduled() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 5, "default-scheduler".to_string());

        // Create a node
        let node = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node)
            .await
            .unwrap();

        // Create a pod with nodeName="" (empty string, not None)
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                // Key: nodeName is Some("") — empty string, not None
                node_name: Some(String::new()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        // Schedule
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should be scheduled to node-1
        let scheduled: Pod = storage
            .get("/registry/pods/default/test-pod")
            .await
            .unwrap();
        assert!(
            scheduled
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .is_some_and(|n| !n.is_empty()),
            "Pod with nodeName='' must be treated as unscheduled and get assigned a node"
        );
    }
    fn cpu_pod(name: &str, uid: &str, cpu: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "default", "uid": uid},
            "spec": {"containers": [{"name": "main", "image": "busybox",
                "resources": {"requests": {"cpu": cpu}}}]},
            "status": {"phase": "Pending"}
        }))
        .unwrap()
    }

    fn cpu_node(name: &str, cpu: &str) -> Node {
        let mut node = make_node(name);
        let st = node.status.as_mut().unwrap();
        st.capacity
            .as_mut()
            .unwrap()
            .insert("cpu".into(), cpu.into());
        st.allocatable
            .as_mut()
            .unwrap()
            .insert("cpu".into(), cpu.into());
        node
    }

    /// `handleSchedulingFailure` copies `fitError.Diagnosis.UnschedulablePlugins`
    /// onto the queued pod (schedule_one.go:1064-1065); the cycle must therefore
    /// report which plugin rejected the pod.
    #[tokio::test]
    async fn try_schedule_pod_reports_rejecting_plugin() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/nodes/small", &cpu_node("small", "1"))
            .await
            .unwrap();
        storage
            .create(
                "/registry/pods/default/big",
                &cpu_pod("big", "big-uid", "2"),
            )
            .await
            .unwrap();
        let scheduler =
            Scheduler::new_with_name(Arc::clone(&storage), 60, DEFAULT_SCHEDULER_NAME.to_string());
        match scheduler
            .try_schedule_pod(&test_queue(), "pods/default/big")
            .await
            .unwrap()
        {
            ScheduleOutcome::Unschedulable(plugins) => assert_eq!(
                plugins,
                std::collections::HashSet::from([names::NODE_RESOURCES_FIT.to_string()])
            ),
            other => panic!("expected an unschedulable outcome, got {other:?}"),
        }
    }

    /// `findNodesThatPassFilters` (schedule_one.go:664-668) + `ScheduleOne`
    /// (:161-164): a Filter `Error` status (csi_test.go "ephemral volume not
    /// owned") aborts the cycle as a scheduling error - not a FitError - so
    /// the pod is neither reported unschedulable nor preempted for.
    #[tokio::test]
    async fn node_volume_limits_filter_error_aborts_cycle() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/nodes/n1", &cpu_node("n1", "4"))
            .await
            .unwrap();
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "abc", "namespace": "default", "uid": "12345"},
            "spec": {"containers": [{"name": "main", "image": "busybox"}],
                "volumes": [{"name": "xyz", "ephemeral": {}}]},
            "status": {"phase": "Pending"}
        }))
        .unwrap();
        let claim: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "abc-xyz", "namespace": "default"},
                "spec": {}
            }))
            .unwrap();
        storage
            .create("/registry/pods/default/abc", &pod)
            .await
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/abc-xyz", &claim)
            .await
            .unwrap();
        // Take VolumeBinding's PreFilter/Filter out so that NodeVolumeLimits
        // is the plugin that sees the un-owned claim (csi_test.go drives the
        // plugin alone).
        let mut profile = rusternetes_scheduler::profile::Profile::named(DEFAULT_SCHEDULER_NAME);
        let mut plugins = profile.plugins.take().unwrap();
        plugins.pre_filter.disabled = vec![rusternetes_scheduler::framework_runtime::Plugin::new(
            names::VOLUME_BINDING,
        )];
        plugins.filter.disabled = vec![rusternetes_scheduler::framework_runtime::Plugin::new(
            names::VOLUME_BINDING,
        )];
        profile.plugins = Some(plugins);
        let profiles = rusternetes_scheduler::profile::Map::new_map(vec![profile]).unwrap();
        let scheduler = Scheduler::new_with_profiles(Arc::clone(&storage), 60, profiles);
        let err = scheduler
            .try_schedule_pod(&test_queue(), "pods/default/abc")
            .await
            .expect_err("a filter Error must abort the cycle");
        let msg = err.to_string();
        assert!(
            msg.contains("running \"NodeVolumeLimits\" filter plugin")
                && msg.contains("pod is not owner"),
            "{msg}"
        );
    }

    /// End-to-end through `run()`: a pod rejected by NodeResourcesFit waits in
    /// unschedulablePods; a Node/Add whose node fits it is the only thing that
    /// moves it (the resync is far away), via `isSchedulableAfterNodeChange`
    /// → backoffQ → activeQ → bound.
    #[tokio::test]
    async fn run_requeues_unschedulable_pod_on_node_add_hint() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/nodes/small", &cpu_node("small", "1"))
            .await
            .unwrap();
        storage
            .create(
                "/registry/pods/default/big",
                &cpu_pod("big", "big-uid", "2"),
            )
            .await
            .unwrap();
        let scheduler = Arc::new(Scheduler::new_with_name(
            Arc::clone(&storage),
            3600, // resync must not be what requeues the pod
            DEFAULT_SCHEDULER_NAME.to_string(),
        ));
        let runner = tokio::spawn(Arc::clone(&scheduler).run());

        // The first cycle rejects the pod; nothing binds it.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let p: Pod = storage.get("/registry/pods/default/big").await.unwrap();
        assert!(
            p.spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .is_none(),
            "pod must stay unscheduled while no node fits"
        );

        storage
            .create("/registry/nodes/large", &cpu_node("large", "4"))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let p: Pod = storage.get("/registry/pods/default/big").await.unwrap();
            if p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some("large") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Node/Add should have requeued and bound the pod"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        runner.abort();
    }
    /// A `Storage` that delegates to `MemoryStorage` but fails the n-th `get`
    /// of one key with a transient error (api-server restart, 429, backend
    /// timeout) — the failure mode upstream's informer-cache read cannot have.
    struct FlakyStorage {
        inner: Arc<MemoryStorage>,
        fail_key: String,
        fail_on_nth_get: usize,
        gets: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Storage for FlakyStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            if key == self.fail_key {
                let n = self.gets.fetch_add(1, Ordering::SeqCst) + 1;
                if n == self.fail_on_nth_get {
                    return Err(rusternetes_common::Error::Internal(
                        "transient: connection reset".to_string(),
                    ));
                }
            }
            self.inner.get(key).await
        }

        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update(key, value).await
        }

        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }

        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }

        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }

        async fn list_at_revision<T>(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list_at_revision(prefix, revision).await
        }

        async fn watch(&self, prefix: &str) -> rusternetes_common::Result<WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// `handleSchedulingFailure` re-reads the pod from the informer cache
    /// (schedule_one.go:1071-1072), which can only fail NotFound. Our re-read
    /// can fail transiently; that must NOT be taken as "pod gone" (which
    /// `Done`s the pod out of every queue for good): the pod is requeued with
    /// the in-flight copy and retried.
    #[tokio::test]
    async fn run_requeues_pod_when_failure_reread_fails_transiently() {
        let inner = Arc::new(MemoryStorage::new());
        inner
            .create("/registry/nodes/small", &cpu_node("small", "1"))
            .await
            .unwrap();
        inner
            .create(
                "/registry/pods/default/big",
                &cpu_pod("big", "big-uid", "2"),
            )
            .await
            .unwrap();
        // get #1: the scheduling cycle; get #2: handleSchedulingFailure's
        // re-read → transient error.
        let storage = Arc::new(FlakyStorage {
            inner,
            fail_key: "/registry/pods/default/big".to_string(),
            fail_on_nth_get: 2,
            gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let scheduler = Arc::new(Scheduler::new_with_name(
            Arc::clone(&storage),
            3600, // resync must not be what rescues the pod
            DEFAULT_SCHEDULER_NAME.to_string(),
        ));
        let runner = tokio::spawn(Arc::clone(&scheduler).run());

        // error backoff is 1s; the retry must come on its own.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while storage.gets.load(Ordering::SeqCst) < 3 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "pod must be retried after the transient re-read error (gets = {})",
                storage.gets.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // …and it is back in the queue's bookkeeping: a fitting node binds it.
        storage
            .create("/registry/nodes/large", &cpu_node("large", "4"))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let p: Pod = storage.get("/registry/pods/default/big").await.unwrap();
            if p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some("large") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "pod must be bound once a node fits"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        runner.abort();
    }

    /// Upstream never calls `updatePod` for an unchanged object (informer
    /// resync is 0, `options.go:336`). Our periodic relist must not replay
    /// identical assigned pods through `updateAssignedPodInCache`: the
    /// generic `AssignedPodUpdate` that produces would requeue every pod
    /// rejected by a plugin registered for Pod/Update* on every tick.
    #[tokio::test]
    async fn resync_relist_does_not_fire_update_events_for_unchanged_pods() {
        let storage = Arc::new(MemoryStorage::new());
        storage
            .create("/registry/nodes/n", &cpu_node("n", "4"))
            .await
            .unwrap();
        let mut assigned = cpu_pod("a", "a-uid", "1");
        assigned.spec.as_mut().unwrap().node_name = Some("n".to_string());
        storage
            .create("/registry/pods/default/a", &assigned)
            .await
            .unwrap();
        let scheduler = Scheduler::new_with_name(
            Arc::clone(&storage),
            3600,
            DEFAULT_SCHEDULER_NAME.to_string(),
        );
        // A plugin registered for the general Pod/Update event (the only
        // kind the generic `AssignedPodUpdate` reaches), hint-less.
        let queue = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                queueing_hint_map: HashMap::from([(
                    DEFAULT_SCHEDULER_NAME.to_string(),
                    HashMap::from([(
                        ClusterEvent::new(EventResource::Pod, ActionType::UPDATE),
                        vec![rusternetes_scheduler::queue::QueueingHintFunction {
                            plugin_name: "fooPlugin".to_string(),
                            queueing_hint_fn: hints::default_queueing_hint_fn(),
                        }],
                    )]),
                )]),
                ..Default::default()
            },
        );
        let mut seen = SeenObjects::default();
        scheduler.resync(&queue, &mut seen).await;

        // A pod rejected by that plugin: any Pod/Update event requeues it.
        queue.add(cpu_pod("p", "p-uid", "1"));
        let mut popped = queue.pop().await.unwrap();
        popped.unschedulable_plugins = std::collections::HashSet::from(["fooPlugin".to_string()]);
        queue
            .add_unschedulable_if_not_present(popped, queue.scheduling_cycle())
            .unwrap();
        assert_eq!(queue.unschedulable_pods().len(), 1);

        // Relist with nothing changed.
        scheduler.resync(&queue, &mut seen).await;
        assert_eq!(
            queue.unschedulable_pods().len(),
            1,
            "an unchanged assigned pod must not fire AssignedPodUpdate"
        );
        assert!(queue.pods_in_backoff_q().is_empty());
        assert!(queue.pods_in_active_q().is_empty());
    }
}
