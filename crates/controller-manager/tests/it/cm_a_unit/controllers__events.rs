//! Unit tests moved from `src/controllers/events.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::{Event, EventSource, EventType, ObjectReference, Pod};
    pub use rusternetes_storage::{
        build_prefix, EventRecorder, Storage, WorkQueue, RECONCILE_ALL_SENTINEL,
    };
    pub use std::sync::Arc;
    pub use tokio::time::{sleep, Duration};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{Container, PodSpec, PodStatus};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::events::*;

    fn create_test_pod(name: &str, namespace: &str, phase: &str, node_name: Option<String>) -> Pod {
        use rusternetes_common::types::{Phase, TypeMeta};

        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: Some(PodSpec {
                init_containers: None,
                containers: vec![Container {
                    name: "test-container".to_string(),
                    image: "nginx:latest".to_string(),
                    image_pull_policy: None,
                    ports: None,
                    env: None,
                    volume_mounts: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    resources: None,
                    working_dir: None,
                    command: None,
                    args: None,
                    security_context: None,
                    restart_policy: None,
                    resize_policy: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    env_from: None,
                    volume_devices: None,
                    ..Default::default()
                }],
                restart_policy: None,
                node_selector: None,
                node_name,
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
            }),
            status: Some(PodStatus {
                phase: Some(match phase {
                    "Pending" => Phase::Pending,
                    "Running" => Phase::Running,
                    "Succeeded" => Phase::Succeeded,
                    "Failed" => Phase::Failed,
                    _ => Phase::Unknown,
                }),
                message: None,
                reason: None,
                pod_ip: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                host_ip: None,
                host_i_ps: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_event_creation() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "Started".to_string(),
            "Pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.reason, "Started");
        assert_eq!(event.message, "Pod started successfully");
        assert_eq!(event.count, 1);
    }

    #[tokio::test]
    async fn test_recurring_event_bumps_count_and_series() {
        use rusternetes_storage::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EventsController::new(storage.clone(), 5);

        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("recurring-pod".to_string()),
            uid: Some("uid-recur-1".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        // First emission creates the event at count 1.
        controller
            .create_event_if_new(
                "default",
                &obj_ref,
                "BackOff",
                "Back-off restarting failed container",
                EventType::Warning,
                Some("kubelet".to_string()),
            )
            .await
            .unwrap();

        let event_name = Event::generate_name(&obj_ref, "BackOff");
        let key = format!("/registry/events/default/{}", event_name);
        let after_first: Event = storage.get(&key).await.unwrap();
        assert_eq!(after_first.count, 1, "first emission should be count 1");
        assert!(
            after_first.series.is_none(),
            "no series before a recurrence"
        );

        // Second emission with a changed message is a genuine recurrence and
        // must bump the count (not stay stuck at 1) and populate the series.
        controller
            .create_event_if_new(
                "default",
                &obj_ref,
                "BackOff",
                "Back-off restarting failed container (attempt 2)",
                EventType::Warning,
                Some("kubelet".to_string()),
            )
            .await
            .unwrap();

        let after_second: Event = storage.get(&key).await.unwrap();
        assert!(
            after_second.count >= 2,
            "recurring event must reach count >= 2, got {}",
            after_second.count
        );
        let series = after_second
            .series
            .as_ref()
            .expect("series must be populated on a recurrence");
        assert_eq!(
            series.count, after_second.count,
            "series.count must track the event count"
        );
        assert!(
            after_second.last_timestamp.is_some(),
            "lastTimestamp must be advanced on a recurrence"
        );
    }

    #[tokio::test]
    async fn test_succeeded_pod_emits_completed_not_started() {
        use rusternetes_common::resources::{ContainerState, ContainerStateTerminated};
        use rusternetes_common::types::Phase;
        use rusternetes_storage::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EventsController::new(storage.clone(), 5);

        let mut pod = create_test_pod(
            "quick-pod",
            "default",
            "Succeeded",
            Some("node-1".to_string()),
        );
        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Succeeded);
            status.container_statuses =
                Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "main".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            signal: None,
                            reason: Some("Completed".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("busybox".to_string()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]);
        }
        storage
            .create("/registry/pods/default/quick-pod", &pod)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let events: Vec<Event> = storage.list("/registry/events/default/").await.unwrap();

        let has_started = events.iter().any(|e| e.reason == "Started");
        let has_completed = events.iter().any(|e| e.reason == "Completed");

        // Container "Started" is now kubelet-owned — this controller must no
        // longer synthesise it. It still summarises the pod-level Completed.
        assert!(
            !has_started,
            "controller must NOT infer Started (kubelet owns it). Events: {:?}",
            events.iter().map(|e| &e.reason).collect::<Vec<_>>()
        );
        assert!(has_completed, "Succeeded pod should have Completed event");
    }

    #[tokio::test]
    async fn test_failed_pod_emits_failed_not_started() {
        use rusternetes_common::resources::{ContainerState, ContainerStateTerminated};
        use rusternetes_common::types::Phase;
        use rusternetes_storage::MemoryStorage;

        let storage = Arc::new(MemoryStorage::new());
        let controller = EventsController::new(storage.clone(), 5);

        let mut pod = create_test_pod("fail-pod", "default", "Failed", Some("node-1".to_string()));
        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Failed);
            status.reason = Some("Error".to_string());
            status.message = Some("Container exited with error".to_string());
            status.container_statuses =
                Some(vec![rusternetes_common::resources::ContainerStatus {
                    name: "main".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            signal: None,
                            reason: Some("Error".to_string()),
                            message: None,
                            started_at: None,
                            finished_at: None,
                            container_id: None,
                        }),
                        ..Default::default()
                    }),
                    last_state: None,
                    image: Some("busybox".to_string()),
                    image_id: None,
                    container_id: None,
                    started: Some(false),
                    allocated_resources: None,
                    allocated_resources_status: None,
                    resources: None,
                    user: None,
                    volume_mounts: None,
                    stop_signal: None,
                }]);
        }
        storage
            .create("/registry/pods/default/fail-pod", &pod)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let events: Vec<Event> = storage.list("/registry/events/default/").await.unwrap();

        let has_started = events.iter().any(|e| e.reason == "Started");
        let has_failed = events.iter().any(|e| e.reason == "Failed");

        // Container lifecycle events are kubelet-owned; the controller keeps
        // only the pod-level Failed summary.
        assert!(
            !has_started,
            "controller must NOT infer Started (kubelet owns it). Events: {:?}",
            events.iter().map(|e| &e.reason).collect::<Vec<_>>()
        );
        assert!(has_failed, "Failed pod should have Failed event");
    }
}
