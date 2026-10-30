//! Unit tests moved from `src/controllers/replicationcontroller.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::Utc;
    pub use futures::StreamExt;
    pub use rusternetes_common::{
        resources::{Pod, PodStatus, ReplicationController, ReplicationControllerCondition},
        types::{label_selector_as_selector, ObjectMeta, OwnerReference, Phase, Selector},
    };
    pub use rusternetes_controller_manager::controllers::controller_utils::{
        find_min_next_pod_availability_check, is_pod_active, pod_key,
    };
    pub use rusternetes_controller_manager::controllers::expectations::ControllerExpectations;
    pub use rusternetes_controller_manager::controllers::pod_informer::{
        apply_pod_observation, ControllerRef, PodEventTarget, PodObservation, ResolvedController,
        SharedPodInformer,
    };
    pub use rusternetes_controller_manager::controllers::replicaset::{
        count_pods, get_pods_to_delete, indirectly_related_pods, PodCounts, RelatedSet,
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::{sync::Arc, time::Duration};
    pub use tracing::{debug, error, info, warn};
}

pub(crate) mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::workloads::{PodTemplateSpec, ReplicationControllerSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_controller_manager::controllers::replicationcontroller::*;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    const ZERO_COUNTS: PodCounts = PodCounts {
        replicas: 0,
        fully_labeled: 0,
        ready: 0,
        available: 0,
    };

    fn make_rc(
        name: &str,
        ns: &str,
        replicas: i32,
        selector: HashMap<String, String>,
        template_labels: Option<HashMap<String, String>>,
    ) -> ReplicationController {
        ReplicationController {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "ReplicationController".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some(ns.to_string());
                m
            },
            spec: ReplicationControllerSpec {
                replicas: Some(replicas),
                selector: Some(selector),
                template: PodTemplateSpec {
                    metadata: template_labels.map(|labels| {
                        let mut m = ObjectMeta::new("");
                        m.labels = Some(labels);
                        m
                    }),
                    spec: rusternetes_common::resources::PodSpec {
                        containers: vec![rusternetes_common::resources::Container {
                            name: "test".to_string(),
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
                        init_containers: None,
                        ephemeral_containers: None,
                        restart_policy: Some("Always".to_string()),
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
                        scheduler_name: None,
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
                    },
                },
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    fn make_orphan_pod(name: &str, labels: HashMap<String, String>) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some("default".to_string());
                m.labels = Some(labels);
                m
            },
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "test".to_string(),
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
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: Some("Always".to_string()),
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
                scheduler_name: None,
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
                phase: Some(Phase::Running),
                message: None,
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                host_i_ps: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                ..Default::default()
            }),
        }
    }

    /// RC status must be written through the STATUS SUBRESOURCE.
    ///
    /// `Storage::update` is a full-object PUT, and an api-server that exposes
    /// `/status` for a type strips `.status` from those. Driving a vanilla
    /// control plane, every RC status write vanished — a Running single-replica
    /// RC reported `status={"replicas":0}` forever:
    ///
    /// * `[sig-apps] ReplicationController should test the lifecycle of a
    ///   ReplicationController` watches for `status.replicas == 1 &&
    ///   status.readyReplicas == 1` (test/e2e/apps/rc.go:190) and timed out.
    /// * `... should get and update a ReplicationController scale` could not
    ///   "confirm the quantity of ReplicationController replicas" (rc.go:442).
    /// * `... should surface a failure condition on a common issue like
    ///   exceeded quota` never saw the ReplicaFailure condition the controller
    ///   does build (rc.go:594) — conditions live in `.status` too.
    ///
    /// MemoryStorage cannot catch this (its `update_status` funnels through
    /// `update`), hence `StatusSubresourceStorage`.
    #[tokio::test]
    async fn rc_status_survives_an_apiserver_that_strips_status_on_put() {
        use rusternetes_controller_manager::controllers::status_subresource_double::StatusSubresourceStorage;

        let storage = Arc::new(StatusSubresourceStorage::new());
        let mut selector = HashMap::new();
        selector.insert("name".to_string(), "test-rc".to_string());
        let rc = make_rc("test-rc", "default", 1, selector.clone(), None);
        let key = build_key("replicationcontrollers", Some("default"), "test-rc");
        storage.create(&key, &rc).await.unwrap();

        let controller = ReplicationControllerController::new(storage.clone(), 1);
        // One reconcile creates the pod; mark it Ready, then reconcile again so
        // the controller observes a ready replica and reports it.
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&rusternetes_storage::build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 1, "reconcile must create the replica");
        let pod_key = build_key("pods", Some("default"), &pods[0].metadata.name);
        let mut pod = pods[0].clone();
        let mut status = pod.status.clone().unwrap_or_default();
        status.phase = Some(Phase::Running);
        status.conditions = Some(vec![rusternetes_common::resources::PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: None,
            observed_generation: None,
        }]);
        pod.status = Some(status);
        storage.update_status(&pod_key, &pod).await.unwrap();

        controller.reconcile_all().await.unwrap();

        let stored: ReplicationController = storage.get(&key).await.unwrap();
        let st = stored
            .status
            .expect("RC status must persist through the status subresource");
        assert_eq!(
            st.replicas, 1,
            "status.replicas must report the live replica"
        );
        assert_eq!(
            st.ready_replicas,
            Some(1),
            "status.readyReplicas must report the ready replica; the lifecycle \
             spec watches for exactly this"
        );
    }

    #[tokio::test]
    async fn test_rc_creates_pods_with_selector_labels_when_template_has_none() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "test".to_string());

        // Template has NO labels (metadata is None)
        let rc = make_rc("test-rc", "default", 1, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/test-rc", &rc)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        // The pod should exist and have the selector labels
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(!pods.is_empty(), "RC should have created a pod");

        let pod = &pods[0];
        let pod_labels = pod
            .metadata
            .labels
            .as_ref()
            .expect("Pod should have labels");
        assert_eq!(
            pod_labels.get("app"),
            Some(&"test".to_string()),
            "Pod should inherit selector labels when template has no labels"
        );
    }

    #[tokio::test]
    async fn test_rc_creates_pods_with_template_labels_when_present() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "test".to_string());

        let mut template_labels = HashMap::new();
        template_labels.insert("app".to_string(), "test".to_string());
        template_labels.insert("version".to_string(), "v1".to_string());

        let rc = make_rc("test-rc", "default", 1, selector, Some(template_labels));
        storage
            .create("/registry/replicationcontrollers/default/test-rc", &rc)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(!pods.is_empty(), "RC should have created a pod");

        let pod_labels = pods[0].metadata.labels.as_ref().unwrap();
        assert_eq!(pod_labels.get("app"), Some(&"test".to_string()));
        assert_eq!(
            pod_labels.get("version"),
            Some(&"v1".to_string()),
            "Pod should use template labels when present"
        );
    }

    #[tokio::test]
    async fn test_rc_created_pods_get_default_tolerations() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "tol-test".to_string());

        let rc = make_rc("tol-rc", "default", 1, selector, None);
        storage
            .create("/registry/replicationcontrollers/default/tol-rc", &rc)
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(!pods.is_empty(), "RC should have created a pod");

        // DefaultTolerationSeconds admission parity (#442): RC-created pods
        // bypass the api-server, so the controller must add the defaults.
        let tolerations = pods[0]
            .spec
            .as_ref()
            .and_then(|s| s.tolerations.as_ref())
            .expect("RC-created pod must carry default tolerations");
        for key in [
            "node.kubernetes.io/not-ready",
            "node.kubernetes.io/unreachable",
        ] {
            let tol = tolerations
                .iter()
                .find(|t| t.key.as_deref() == Some(key))
                .unwrap_or_else(|| panic!("missing default toleration for {key}"));
            assert_eq!(tol.operator.as_deref(), Some("Exists"));
            assert_eq!(tol.effect.as_deref(), Some("NoExecute"));
            assert_eq!(tol.toleration_seconds, Some(300));
        }
    }

    #[tokio::test]
    async fn test_rc_pods_inherit_sa_image_pull_secrets_and_token_volume() {
        // #1118: SA imagePullSecrets and the SA token volume must reach
        // RC-created pods, which bypass the api-server admission path —
        // mirrors the ReplicaSet wiring (upstream Go's RC controller IS the
        // ReplicaSet controller via adapters).
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

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

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "pullsecrets".to_string());
        let rc = make_rc("pullsecrets-rc", "default", 1, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/pullsecrets-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 1);
        let spec = pods[0].spec.as_ref().unwrap();
        let secrets = spec
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
        assert!(
            spec.volumes
                .as_ref()
                .is_some_and(|vols| vols.iter().any(|v| v.name.starts_with("kube-api-access"))),
            "pod must get the SA token volume injected"
        );
    }

    /// `calculateStatus` (replica_set_utils.go:124-136; the RC rides the RS
    /// controller upstream) sets ReplicaFailure only when absent, so a failure
    /// that persists keeps its lastTransitionTime and message; the reason is
    /// FailedDelete when over-replicated.
    #[tokio::test]
    async fn rc_replica_failure_is_not_rewritten_while_it_persists() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "stable".to_string());
        let rc = make_rc("stable", "default", 1, selector, None);
        let key = "/registry/replicationcontrollers/default/stable";
        storage.create(key, &rc).await.unwrap();

        controller
            .update_status(
                &rc,
                ZERO_COUNTS,
                Some("first failure"),
                Some("FailedDelete"),
            )
            .await
            .unwrap();
        let cond = |rc: ReplicationController| {
            rc.status
                .unwrap()
                .conditions
                .unwrap()
                .into_iter()
                .find(|c| c.condition_type == "ReplicaFailure")
                .unwrap()
        };
        let first = cond(storage.get(key).await.unwrap());
        assert_eq!(first.reason.as_deref(), Some("FailedDelete"));

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let rc: ReplicationController = storage.get(key).await.unwrap();
        controller
            .update_status(
                &rc,
                ZERO_COUNTS,
                Some("second failure"),
                Some("FailedCreate"),
            )
            .await
            .unwrap();
        let second = cond(storage.get(key).await.unwrap());
        assert_eq!(second.message, first.message);
        assert_eq!(second.last_transition_time, first.last_transition_time);
    }

    #[tokio::test]
    async fn test_rc_sets_replica_failure_on_quota_exceeded() {
        // The api-server's quota admission denies the create; the controller has
        // no client-side quota check (#3618).
        let storage =
            Arc::new(rusternetes_controller_manager::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "quota-test".to_string());

        // Create RC wanting 2 replicas
        let rc = make_rc("quota-rc", "default", 2, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/quota-rc", &rc)
            .await
            .unwrap();

        // Create a ResourceQuota that limits pods to 0
        let quota: serde_json::Value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {
                "name": "test-quota",
                "namespace": "default",
                "uid": "quota-uid-1"
            },
            "spec": {
                "hard": {
                    "pods": "0"
                }
            }
        });
        storage
            .create("/registry/resourcequotas/default/test-quota", &quota)
            .await
            .unwrap();

        // Reconcile — pod creation should fail due to quota
        controller.reconcile_all().await.unwrap();

        // Verify no pods were created
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            0,
            "No pods should be created when quota is exceeded"
        );
        assert!(
            storage
                .denied_creates
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0,
            "the controller must reach the api-server and be denied there, \
             not pre-check quota client-side"
        );

        // Verify the RC has a ReplicaFailure condition
        let updated_rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/quota-rc")
            .await
            .unwrap();
        let status = updated_rc.status.expect("RC should have status");
        let conditions = status.conditions.expect("RC should have conditions");
        let failure_condition = conditions
            .iter()
            .find(|c| c.condition_type == "ReplicaFailure")
            .expect("RC should have a ReplicaFailure condition");
        assert_eq!(failure_condition.status, "True");
        assert_eq!(failure_condition.reason.as_deref(), Some("FailedCreate"));
        assert!(
            failure_condition
                .message
                .as_ref()
                .unwrap()
                .contains("exceeded quota"),
            "Message should mention exceeded quota, got: {}",
            failure_condition.message.as_ref().unwrap()
        );
    }

    #[tokio::test]
    async fn test_rc_clears_replica_failure_when_pods_created_successfully() {
        // The api-server's quota admission denies the create; the controller has
        // no client-side quota check (#3618).
        let storage =
            Arc::new(rusternetes_controller_manager::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "clear-test".to_string());

        // Create RC wanting 1 replica
        let rc = make_rc("clear-rc", "default", 1, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/clear-rc", &rc)
            .await
            .unwrap();

        // Create a ResourceQuota that limits pods to 0 — forces failure
        let quota: serde_json::Value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {
                "name": "test-quota2",
                "namespace": "default",
                "uid": "quota-uid-2"
            },
            "spec": {
                "hard": {
                    "pods": "0"
                }
            }
        });
        storage
            .create("/registry/resourcequotas/default/test-quota2", &quota)
            .await
            .unwrap();

        // First reconcile — should fail and set ReplicaFailure
        controller.reconcile_all().await.unwrap();

        let rc_after_fail: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/clear-rc")
            .await
            .unwrap();
        assert!(
            rc_after_fail
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|c| c.iter().find(|c| c.condition_type == "ReplicaFailure"))
                .map(|c| c.status == "True")
                .unwrap_or(false),
            "Should have ReplicaFailure=True after quota exceeded"
        );

        // Remove the quota so pods can be created
        storage
            .delete("/registry/resourcequotas/default/test-quota2")
            .await
            .unwrap();

        // Second reconcile — should succeed and clear the condition
        controller.reconcile_all().await.unwrap();

        let rc_after_success: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/clear-rc")
            .await
            .unwrap();
        let conditions = rc_after_success
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        // Conditions should be None (cleared) when no failure
        assert!(
            conditions.is_none(),
            "ReplicaFailure condition should be cleared after successful pod creation, got: {:?}",
            conditions
        );

        // Verify pod was actually created
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(
            !pods.is_empty(),
            "Pod should be created after quota removed"
        );
    }

    #[tokio::test]
    async fn test_rc_adopt_orphan_pods_and_release_non_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "web".to_string());

        // Create RC with selector app=web, wanting 1 replica
        let rc = make_rc("adopt-rc", "default", 1, selector.clone(), None);
        storage
            .create("/registry/replicationcontrollers/default/adopt-rc", &rc)
            .await
            .unwrap();

        // Create an orphan pod with matching labels (no ownerReference)
        let mut orphan_labels = HashMap::new();
        orphan_labels.insert("app".to_string(), "web".to_string());
        let orphan_pod = make_orphan_pod("orphan-pod", orphan_labels);
        storage
            .create("/registry/pods/default/orphan-pod", &orphan_pod)
            .await
            .unwrap();

        // Reconcile — should adopt the orphan pod (1 desired, 1 orphan matches)
        controller.reconcile_all().await.unwrap();

        // Verify the orphan pod was adopted (now has ownerReference pointing to the RC)
        let adopted_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod")
            .await
            .unwrap();
        let owner_refs = adopted_pod.metadata.owner_references.as_ref().unwrap();
        assert!(
            owner_refs.iter().any(|r| r.kind == "ReplicationController"
                && r.name == "adopt-rc"
                && r.controller == Some(true)),
            "Pod should have ownerReference to RC after adoption"
        );

        // Verify no extra pods were created (orphan counts toward desired replicas)
        let all_pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            all_pods.len(),
            1,
            "Should still have only 1 pod — orphan was adopted, not a new pod created"
        );

        // Now change the pod's labels so it no longer matches the selector
        let mut changed_pod = adopted_pod.clone();
        let labels = changed_pod.metadata.labels.as_mut().unwrap();
        labels.clear();
        labels.insert("app".to_string(), "backend".to_string());
        storage
            .update("/registry/pods/default/orphan-pod", &changed_pod)
            .await
            .unwrap();

        // Reconcile — should release the pod (labels no longer match)
        controller.reconcile_all().await.unwrap();

        // Verify the pod's ownerReference was removed (released)
        let released_pod: Pod = storage
            .get("/registry/pods/default/orphan-pod")
            .await
            .unwrap();
        let has_rc_owner = released_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.kind == "ReplicationController" && r.name == "adopt-rc")
            })
            .unwrap_or(false);
        assert!(
            !has_rc_owner,
            "Pod should no longer have ownerReference to RC after label change"
        );
    }

    #[tokio::test]
    async fn test_rc_matches_created_pods_on_next_reconcile() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "test".to_string());

        let rc = make_rc("test-rc", "default", 2, selector, None);
        storage
            .create("/registry/replicationcontrollers/default/test-rc", &rc)
            .await
            .unwrap();

        // First reconcile: creates pods
        controller.reconcile_all().await.unwrap();

        let pods_after_first: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods_after_first.len(), 2, "Should create 2 pods");

        // Second reconcile: should match existing pods, not create more
        controller.reconcile_all().await.unwrap();

        let pods_after_second: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods_after_second.len(),
            2,
            "Should still have 2 pods — RC must match its own pods on second reconcile"
        );
    }

    /// Pod creates must go out in upstream's slow-start batches, not one at a
    /// time.
    ///
    /// Upstream `slowStartBatch`
    /// (pkg/controller/replicaset/replica_set.go:820-844) starts at
    /// `SlowStartInitialBatchSize` and doubles each round, capping each batch
    /// at what remains, and creates every pod WITHIN a batch concurrently.
    /// The doubling is what bounds the damage when creates are being rejected
    /// (a quota denial costs one create, not N) while still reaching a large
    /// replica count in a logarithmic number of rounds.
    ///
    /// This matters because our creates are not cheap. Measured on the kine
    /// leg (run 33750828466), `create_pod` takes ~4s — a ServiceAccount GET, a
    /// ResourceQuota LIST and the pod CREATE, at ~1.3s per api call — so a
    /// serial loop needs ~400s for 100 replicas. The conformance specs give
    /// `replicaSyncTimeout` 120s (garbage_collector.go:128), which is why
    /// three `[sig-api-machinery] Garbage collector` specs fail in setup on
    /// `rc.Status.Replicas (0)` never reaching `Spec.Replicas (100)` (#1847).
    /// Batched, the same 100 creates take 7 rounds instead of 100.
    #[test]
    fn slow_start_batches_capped_respects_max_batch_size() {
        // statefulset slowStartBatch: min(min(2*batch, remaining), MaxBatchSize)
        assert_eq!(slow_start_batches_capped(10, 1, 4), vec![1, 2, 4, 3]);
        assert_eq!(
            slow_start_batches_capped(10, 1, 4).iter().sum::<usize>(),
            10
        );
    }

    #[test]
    fn slow_start_batches_double_and_cap_at_remaining() {
        // Upstream's worked example: 100 pods from an initial batch of 1.
        assert_eq!(
            slow_start_batches(100, 1),
            vec![1, 2, 4, 8, 16, 32, 37],
            "batches double until the tail is capped by what remains"
        );
        assert_eq!(slow_start_batches(100, 1).iter().sum::<usize>(), 100);

        // Round counts follow upstream's 1+floor(log2(ceil(N/initial)))
        // (controller_utils.go:84-86): 100 pods is 7 rounds, not 100.
        assert_eq!(slow_start_batches(100, 1).len(), 7);

        assert_eq!(slow_start_batches(5, 1), vec![1, 2, 2]);
        assert_eq!(slow_start_batches(1, 1), vec![1]);
        assert!(slow_start_batches(0, 1).is_empty(), "nothing to create");

        // A larger initial batch is still capped by the count.
        assert_eq!(slow_start_batches(3, 8), vec![3]);
    }

    /// Wraps `MemoryStorage` and records how many pod creates are in flight at
    /// once, so a test can tell a batched implementation from a serial one.
    /// Every other method delegates straight through.
    pub(crate) struct ConcurrencyProbeStorage {
        inner: Arc<MemoryStorage>,
        in_flight: std::sync::atomic::AtomicUsize,
        peak_in_flight: std::sync::atomic::AtomicUsize,
    }

    impl ConcurrencyProbeStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak_in_flight: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        pub(crate) fn peak(&self) -> usize {
            self.peak_in_flight
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for ConcurrencyProbeStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if !key.starts_with("/registry/pods/") {
                return self.inner.create(key, value).await;
            }
            use std::sync::atomic::Ordering;
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
            // Hold the slot open across an await so overlapping creates are
            // actually observable; a serial loop can never exceed 1.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let out = self.inner.create(key, value).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            out
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
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

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// One reconcile must bring a 100-replica RC all the way to 100 pods AND
    /// issue those creates concurrently.
    ///
    /// This is the behavioural guard for the slow-start port; the sizes are
    /// pinned separately by
    /// [`slow_start_batches_double_and_cap_at_remaining`]. A future change that
    /// reverts to `for i in 0..to_create { create_pod(..).await }` still
    /// converges and would pass an outcome-only test, but it drops the peak
    /// in-flight count to exactly 1 — which is what actually broke the
    /// conformance specs, because serial creates at ~1.3s per api call need
    /// ~400s for 100 pods against a 120s `replicaSyncTimeout` (#1847).
    #[tokio::test]
    async fn rc_creates_all_replicas_in_one_reconcile_and_concurrently() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(ConcurrencyProbeStorage::new(inner));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "batched".to_string());
        let rc = make_rc("simpletest-rc", "default", 100, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            100,
            "one reconcile must create every missing replica, not one per pass"
        );

        // The load-bearing assertion: a serial loop peaks at exactly 1.
        assert!(
            storage.peak() > 1,
            "pod creates must overlap; peak in-flight was {} (serial implementation?)",
            storage.peak()
        );

        // Deliberately not asserting peak == the batch size. `create_pod`
        // awaits a ServiceAccount GET and a ResourceQuota LIST before the
        // create, so futures in a batch interleave at those earlier awaits and
        // the peak *create* overlap is a lower bound on the batch width.

        // `calculateStatus` counts the pods listed BEFORE `manageReplicas`
        // (replica_set.go:743), so this sync reported 0. The pod watch's Added
        // events (replica_set.go:436 `CreationObserved`) lower the
        // expectations and enqueue the RC, and that sync reports the pods.
        for _ in 0..100 {
            controller
                .expectations
                .creation_observed("default/simpletest-rc");
        }
        controller.reconcile_all().await.unwrap();

        let observed: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/simpletest-rc")
            .await
            .unwrap();
        assert_eq!(
            observed.status.as_ref().map(|st| st.replicas),
            Some(100),
            "status.replicas must reach spec.replicas — this is what \
             garbage_collector.go:1174 waits on"
        );
    }

    /// A create rejected because the namespace is terminating is not a replica
    /// failure.
    ///
    /// Upstream treats the admission rejection and the controller's reaction as
    /// one design. `manageReplicas` inspects the create error and swallows this
    /// specific cause:
    ///
    /// ```text
    /// if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
    ///     // if the namespace is being terminated, we don't have to do
    ///     // anything because any creation will fail
    ///     return nil
    /// }
    /// ```
    ///
    /// (pkg/controller/replicaset/replica_set.go:632-637)
    ///
    /// We shipped the api-server half of that pair in #1849 — creates into a
    /// Terminating namespace now return 403 with the `NamespaceTerminating`
    /// cause — without the controller half. So every namespace teardown put the
    /// RC into a retry loop against creates that can only ever fail, and
    /// stamped a `ReplicaFailure` condition upstream deliberately does not set.
    #[test]
    fn namespace_terminating_rejection_is_recognised() {
        use rusternetes_common::Error;

        // The message our api-server produces (#1849), as it reaches the
        // controller through ApiStorage's stringified error chain.
        assert!(is_namespace_terminating_rejection(&Error::Forbidden(
            "POST https://api-server:6443/api/v1/namespaces/ns-1/pods failed: \
             (Forbidden) pods is forbidden: unable to create new content in \
             namespace ns-1 because it is being terminated"
                .to_string()
        )));

        // The structured cause reason, if it ever reaches us intact — this is
        // what upstream actually matches on.
        assert!(is_namespace_terminating_rejection(&Error::Forbidden(
            "(Forbidden) NamespaceTerminating".to_string()
        )));

        // Any other Forbidden is a real failure and must still surface: a quota
        // denial is the case that ReplicaFailure exists for.
        assert!(!is_namespace_terminating_rejection(&Error::Forbidden(
            "exceeded quota: pods, requested: 1, used: 10, limited: 10".to_string()
        )));

        // Non-Forbidden errors are never this case.
        assert!(!is_namespace_terminating_rejection(&Error::Storage(
            "connection refused".to_string()
        )));
        assert!(!is_namespace_terminating_rejection(&Error::Conflict(
            "resourceVersion mismatch".to_string()
        )));
    }

    /// Rejects every pod create the way our api-server does for a Terminating
    /// namespace (#1849), and counts the attempts. Everything else delegates.
    pub(crate) struct TerminatingNamespaceStorage {
        inner: Arc<MemoryStorage>,
        pod_create_attempts: std::sync::atomic::AtomicUsize,
    }

    impl TerminatingNamespaceStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>) -> Self {
            Self {
                inner,
                pod_create_attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        pub(crate) fn attempts(&self) -> usize {
            self.pod_create_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for TerminatingNamespaceStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if key.starts_with("/registry/pods/") {
                self.pod_create_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(rusternetes_common::Error::Forbidden(format!(
                    "POST {key} failed: (Forbidden) pods is forbidden: unable to create new \
                     content in namespace ns-term because it is being terminated"
                )));
            }
            self.inner.create(key, value).await
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
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

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// `HasStatusCause(err, v1.NamespaceTerminatingCause)` -> `return nil`
    /// inside the `slowStartBatch` callback (replica_set.go:632-637), so the
    /// rejected create counts as a SUCCESS: no ReplicaFailure, no requeue, and
    /// the batches carry on (:820-844 only stops on an error). A 20-replica RC
    /// therefore attempts all 20 creates (batches 1, 2, 4, 8, 5) rather than
    /// abandoning after the first rejection.
    ///
    /// Upstream also does NOT decrement the expectation for these creates
    /// (`successfulCreations` includes them, so the skipped-pod loop at
    /// :644-650 has nothing to undo); replicated, not fixed.
    ///
    /// There is no replication_controller_test.go upstream: the RC runs through
    /// the ReplicaSet controller (pkg/controller/replication/conversion.go), so
    /// the equivalents are `TestSlowStartBatch` (replica_set_test.go:1872) and
    /// `TestSyncReplicaSetCreateFailures` (replica_set_test.go:281).
    #[tokio::test]
    async fn rc_continues_slow_start_past_namespace_terminating_rejection() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(TerminatingNamespaceStorage::new(inner));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "doomed".to_string());
        let rc = make_rc("simpletest-rc", "default", 20, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        // Must not surface as a reconcile error: upstream's callback returns nil.
        controller
            .reconcile_rc(&rc)
            .await
            .expect("a terminating namespace must not fail the reconcile");

        let observed: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/simpletest-rc")
            .await
            .unwrap();
        let has_replica_failure = observed
            .status
            .as_ref()
            .and_then(|st| st.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure"));
        assert!(
            !has_replica_failure,
            "a terminating namespace must not raise ReplicaFailure"
        );

        assert_eq!(
            storage.attempts(),
            20,
            "slowStartBatch keeps going: the rejection is a nil return, not a failure"
        );
        assert_eq!(
            controller
                .expectations
                .get_expectations("default/simpletest-rc"),
            Some((20, 0)),
            "upstream counts the rejected creates as successes, so none is decremented"
        );
    }

    /// `syncReplicaSet` returns `manageReplicasErr` AFTER updating status
    /// (replica_set.go:772-774) so the workqueue requeues the key with backoff:
    /// the error must reach the worker, and ReplicaFailure must already be on
    /// the object. Mirrors `TestSyncReplicaSetCreateFailures`
    /// (replica_set_test.go:281).
    #[tokio::test]
    async fn rc_returns_create_failure_after_updating_status_for_requeue() {
        let storage =
            Arc::new(rusternetes_controller_manager::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "cf".to_string());
        let rc = make_rc("cf", "default", 3, selector, None);
        let rc_key = "/registry/replicationcontrollers/default/cf";
        storage.create(rc_key, &rc).await.unwrap();
        storage
            .create(
                "/registry/resourcequotas/default/pq",
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "ResourceQuota",
                    "metadata": {"name": "pq", "namespace": "default", "uid": "q"},
                    "spec": {"hard": {"pods": "0"}}
                }),
            )
            .await
            .unwrap();

        let result = controller.reconcile_rc(&rc).await;
        assert!(result.is_err(), "a create failure must requeue the key");

        let after: ReplicationController = storage.get(rc_key).await.unwrap();
        let failure = after
            .status
            .and_then(|s| s.conditions)
            .and_then(|cs| {
                cs.into_iter()
                    .find(|c| c.condition_type == "ReplicaFailure")
            })
            .expect("status is written before the error is returned");
        assert_eq!(failure.reason.as_deref(), Some("FailedCreate"));
    }

    /// Fails `delete_gracefully` for every pod once armed, and counts the
    /// attempts. Everything else delegates.
    pub(crate) struct DeleteFailingStorage {
        inner: Arc<MemoryStorage>,
        fail_with_not_found: bool,
        armed: std::sync::atomic::AtomicBool,
        delete_attempts: std::sync::atomic::AtomicUsize,
    }

    impl DeleteFailingStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>, fail_with_not_found: bool) -> Self {
            Self {
                inner,
                fail_with_not_found,
                armed: std::sync::atomic::AtomicBool::new(false),
                delete_attempts: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn attempts(&self) -> usize {
            self.delete_attempts
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for DeleteFailingStorage {
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

        async fn delete_gracefully(&self, key: &str) -> rusternetes_common::Result<()> {
            if key.starts_with("/registry/pods/")
                && self.armed.load(std::sync::atomic::Ordering::SeqCst)
            {
                self.delete_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(if self.fail_with_not_found {
                    rusternetes_common::Error::NotFound(format!("pod {key} not found"))
                } else {
                    rusternetes_common::Error::Internal(format!("delete {key} failed"))
                });
            }
            self.inner.delete_gracefully(key).await
        }

        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.list(prefix).await
        }

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// Scale an RC from 3 to 1 against `storage`, with every pod delete failing
    /// once the storage is armed.
    async fn scale_down_with_failing_deletes(
        storage: &Arc<DeleteFailingStorage>,
    ) -> (
        ReplicationControllerController<DeleteFailingStorage>,
        rusternetes_common::Result<()>,
    ) {
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "df".to_string());
        let rc = make_rc("df", "default", 3, selector, None);
        let rc_key = "/registry/replicationcontrollers/default/df";
        storage.create(rc_key, &rc).await.unwrap();
        controller.reconcile_rc(&rc).await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 3);
        // The creates are observed (pod watch Added events); simulate them.
        for _ in 0..3 {
            controller.expectations.creation_observed("default/df");
        }

        let mut rc: ReplicationController = storage.get(rc_key).await.unwrap();
        rc.spec.replicas = Some(1);
        storage.update(rc_key, &rc).await.unwrap();
        // Reconcile the object as the worker would read it: the update bumped
        // its resourceVersion, which the status write is conditional on.
        let rc: ReplicationController = storage.get(rc_key).await.unwrap();
        storage.arm();
        let result = controller.reconcile_rc(&rc).await;
        (controller, result.map(|_| ()))
    }

    /// The deletes run concurrently, every one is attempted, and only the
    /// first error is reported AFTER the status update
    /// (replica_set.go:672-698, :772-774): no early return on the first failed
    /// delete, ReplicaFailure=FailedDelete recorded, error returned to requeue.
    #[tokio::test]
    async fn rc_attempts_every_delete_and_updates_status_when_a_delete_fails() {
        let storage = Arc::new(DeleteFailingStorage::new(
            Arc::new(MemoryStorage::new()),
            false,
        ));
        let (controller, result) = scale_down_with_failing_deletes(&storage).await;

        assert!(result.is_err(), "a delete failure must requeue the key");
        assert_eq!(storage.attempts(), 2, "every delete is attempted");

        let after: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/df")
            .await
            .unwrap();
        let status = after.status.expect("status is updated despite the error");
        let failure = status
            .conditions
            .and_then(|cs| {
                cs.into_iter()
                    .find(|c| c.condition_type == "ReplicaFailure")
            })
            .expect("ReplicaFailure recorded for the failed delete");
        assert_eq!(failure.reason.as_deref(), Some("FailedDelete"));

        // The informer will not observe the failed deletions, so each lowers
        // the expectation back (replica_set.go:679-686).
        assert_eq!(
            controller.expectations.get_expectations("default/df"),
            Some((0, 0))
        );
    }

    /// `!apierrors.IsNotFound(err)` guards the error report
    /// (replica_set.go:682-685): a pod already gone is no failure.
    #[tokio::test]
    async fn rc_delete_not_found_is_not_a_failure() {
        let storage = Arc::new(DeleteFailingStorage::new(
            Arc::new(MemoryStorage::new()),
            true,
        ));
        let (_controller, result) = scale_down_with_failing_deletes(&storage).await;

        result.expect("NotFound on delete must not fail the sync");
        let after: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/df")
            .await
            .unwrap();
        assert!(
            !after
                .status
                .and_then(|s| s.conditions)
                .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure")),
            "NotFound is not a ReplicaFailure"
        );
    }

    /// While creates are in flight and unobserved, a re-entrant reconcile must
    /// not issue them again.
    ///
    /// This is the whole purpose of expectations, and the failure it prevents is
    /// concrete: a watch-driven controller wakes on its OWN pod-create events,
    /// lists pods from a backend that has not caught up, sees too few, and
    /// creates a second batch. Upstream gates `manageReplicas` on
    /// `SatisfiedExpectations` for exactly this
    /// (pkg/controller/replicaset/replica_set.go:728, 756).
    ///
    /// Simulated by leaving an outstanding expectation and reconciling against
    /// a storage that reports no pods at all — the worst case of a stale list.
    /// Without the gate the controller creates the full replica count again.
    #[tokio::test]
    async fn rc_defers_creates_while_expectations_are_outstanding() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(ConcurrencyProbeStorage::new(inner));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "gated".to_string());
        let rc = make_rc("simpletest-rc", "default", 10, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        // Stand in for a batch this controller has just issued and not yet
        // seen land.
        controller
            .expectations
            .expect_creations("default/simpletest-rc", 10);

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert!(
            pods.is_empty(),
            "no pods may be created while 10 creates are outstanding, got {}",
            pods.len()
        );

        // Once those creates are observed the gate opens and the controller
        // does its job — the brake must not become a wedge.
        for _ in 0..10 {
            controller
                .expectations
                .creation_observed("default/simpletest-rc");
        }
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            10,
            "once expectations are satisfied the controller must create the replicas"
        );
    }

    /// A single sync creates at most `BURST_REPLICAS` pods, and the next sync
    /// picks up the rest.
    ///
    /// Upstream caps the diff in both directions before doing anything with it:
    ///
    /// ```text
    /// if diff > rsc.burstReplicas {
    ///     diff = rsc.burstReplicas
    /// }
    /// ```
    ///
    /// (pkg/controller/replicaset/replica_set.go:611-612 for creates, :653-654
    /// for deletes; `BurstReplicas = 500` at :72.)
    ///
    /// The cap bounds what one object can do to the api-server in one sync.
    /// Slow-start batching alone does not: its batches double without limit, so
    /// a single large RC can otherwise issue an unbounded burst — and this
    /// controller shares an api-server with every other client, including the
    /// conformance suite whose own rate limiter starved during run
    /// 33762395978.
    #[tokio::test]
    async fn rc_caps_creates_at_burst_replicas_and_finishes_next_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "burst".to_string());
        let desired = BURST_REPLICAS as i32 + 100;
        let rc = make_rc("simpletest-rc", "default", desired, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            BURST_REPLICAS,
            "one sync must not exceed the burst cap"
        );

        // The cap is a throttle, not a ceiling: once the creates are observed
        // (the pod watch's Added events), the next sync finishes the job.
        for _ in 0..BURST_REPLICAS {
            controller
                .expectations
                .creation_observed("default/simpletest-rc");
        }
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            desired as usize,
            "the following sync must create the remainder"
        );
    }

    /// Fails the first `n` pod creates with AlreadyExists, then behaves. Stands
    /// in for a generated pod name colliding with one already in the namespace.
    pub(crate) struct NameCollisionStorage {
        inner: Arc<MemoryStorage>,
        remaining_collisions: std::sync::atomic::AtomicUsize,
    }

    impl NameCollisionStorage {
        pub(crate) fn new(inner: Arc<MemoryStorage>, collisions: usize) -> Self {
            Self {
                inner,
                remaining_collisions: std::sync::atomic::AtomicUsize::new(collisions),
            }
        }
    }

    #[async_trait::async_trait]
    impl rusternetes_storage::Storage for NameCollisionStorage {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            use std::sync::atomic::Ordering;
            if key.starts_with("/registry/pods/")
                && self
                    .remaining_collisions
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
            {
                return Err(rusternetes_common::Error::AlreadyExists(format!(
                    "pods {key} already exists"
                )));
            }
            self.inner.create(key, value).await
        }

        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
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

        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }

        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }

        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }

        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    /// A generated pod name that collides must be retried with a new name, not
    /// reported as a failure.
    ///
    /// Upstream retries the create up to `maxNameGenerationCreateAttempts` (8),
    /// generating a fresh name each time
    /// (staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go:431-470).
    /// Its comment gives the arithmetic that makes this mandatory rather than
    /// defensive:
    ///
    /// ```text
    /// Without retry, a 0.1% probability occurs at ~500
    /// generated names and a 50% probability occurs at ~4500
    /// generated names.
    /// ```
    ///
    /// This is not hypothetical here. `rc_caps_creates_at_burst_replicas_and_
    /// finishes_next_sync` creates 500 pods in one namespace and failed in CI
    /// with 254 of them — a collision killed the batch — while passing locally.
    #[tokio::test]
    async fn rc_retries_pod_create_when_the_generated_name_collides() {
        let inner = Arc::new(MemoryStorage::new());
        // Three collisions in a row: within upstream's budget of 8.
        let storage = Arc::new(NameCollisionStorage::new(inner, 3));
        let controller = ReplicationControllerController::new(storage.clone(), 5);

        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "collide".to_string());
        let rc = make_rc("simpletest-rc", "default", 1, selector, None);
        storage
            .create(
                "/registry/replicationcontrollers/default/simpletest-rc",
                &rc,
            )
            .await
            .unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.len(),
            1,
            "the create must be retried under a new name, not abandoned"
        );

        // And it must not be reported as a replica failure: nothing is wrong
        // with this RC.
        let observed: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/simpletest-rc")
            .await
            .unwrap();
        let has_replica_failure = observed
            .status
            .as_ref()
            .and_then(|st| st.conditions.as_ref())
            .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure"));
        assert!(
            !has_replica_failure,
            "a retried name collision is not a ReplicaFailure"
        );
    }

    /// #2458: upstream `DeletePod` is a graceful delete
    /// (`pkg/controller/controller_utils.go:618`, `DeleteOptions{}`): the pod
    /// keeps living, stamped with a deletionTimestamp, for its grace period.
    /// Terminating pods are not replicas, so the follow-up sync must not take
    /// another pod down.
    #[tokio::test]
    async fn rc_scale_down_deletes_pods_gracefully() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), "g".to_string());
        let rc = make_rc("g", "default", 3, selector.clone(), None);
        let rc_key = "/registry/replicationcontrollers/default/g";
        storage.create(rc_key, &rc).await.unwrap();
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 3);
        // The creates are observed (pod watch Added events); simulate them.
        for _ in 0..3 {
            controller.expectations.creation_observed("default/g");
        }

        let mut rc: ReplicationController = storage.get(rc_key).await.unwrap();
        rc.spec.replicas = Some(1);
        storage.update(rc_key, &rc).await.unwrap();
        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(pods.len(), 3, "graceful delete keeps the pods in storage");
        assert_eq!(
            pods.iter()
                .filter(|p| p.metadata.deletion_timestamp.is_some())
                .count(),
            2
        );

        // Terminating pods are not replicas: nothing further is deleted.
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list("/registry/pods/default/").await.unwrap();
        assert_eq!(
            pods.iter()
                .filter(|p| p.metadata.deletion_timestamp.is_some())
                .count(),
            2
        );
    }

    /// #2458: the deletion is observed on the MODIFIED event that sets the
    /// deletionTimestamp (`replica_set.go:468-480`), once per pod
    /// (`controller_utils.go:380-390`).
    #[tokio::test]
    async fn rc_observes_deletion_on_deletion_timestamp_once() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        // `resolveControllerRef` needs the RC the pod's reference names.
        let mut rc = make_rc("g", "default", 1, rc_selector("g"), None);
        rc.metadata.uid = "u".to_string();
        put_rc(&storage, &rc).await;
        controller.expectations.expect_deletions_of(
            "default/g",
            &["default/p1".to_string(), "default/p2".to_string()],
        );
        let pod = serde_json::json!({
            "metadata": {
                "name": "p1",
                "namespace": "default",
                "deletionTimestamp": "2026-10-07T00:00:00Z",
                "ownerReferences": [{"apiVersion": "v1", "kind": "ReplicationController",
                    "name": "g", "uid": "u", "controller": true}]
            }
        })
        .to_string();
        let key = "/registry/pods/default/p1".to_string();

        controller
            .enqueue_owner_rc(
                &queue,
                &rusternetes_storage::WatchEvent::Modified(key.clone(), pod.clone()),
            )
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1)),
            "the deletionTimestamp update is the observation"
        );
        controller
            .enqueue_owner_rc(&queue, &rusternetes_storage::WatchEvent::Deleted(key, pod))
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1))
        );
    }

    // ---------------------------------------------------------------------
    // syncReplicaSet / calculateStatus / getPodsToDelete parity (#3627)
    //
    // The RC rides the ReplicaSet controller upstream
    // (pkg/controller/replication/replication_controller.go:57-79,
    // `NewBaseController` with `EnableStatusTerminatingReplicas: false`), so
    // each behaviour below is the RS one (#3624): replica_set.go
    // `syncReplicaSet` :713-790, `getPodsToDelete` :874,
    // replica_set_utils.go `calculateStatus` :96-141.
    // ---------------------------------------------------------------------

    fn rc_selector(app: &str) -> HashMap<String, String> {
        let mut selector = HashMap::new();
        selector.insert("app".to_string(), app.to_string());
        selector
    }

    async fn seed_rc(
        storage: &Arc<MemoryStorage>,
        controller: &ReplicationControllerController<MemoryStorage>,
        rc: &ReplicationController,
    ) {
        storage
            .create(
                &format!(
                    "/registry/replicationcontrollers/default/{}",
                    rc.metadata.name
                ),
                rc,
            )
            .await
            .unwrap();
        let stored: ReplicationController = storage
            .get(&format!(
                "/registry/replicationcontrollers/default/{}",
                rc.metadata.name
            ))
            .await
            .unwrap();
        controller.reconcile_rc(&stored).await.unwrap();
    }

    async fn rc_pods(storage: &Arc<MemoryStorage>) -> Vec<Pod> {
        storage.list("/registry/pods/default/").await.unwrap()
    }

    async fn write_pod(storage: &Arc<MemoryStorage>, pod: &Pod) {
        storage
            .update(
                &format!("/registry/pods/default/{}", pod.metadata.name),
                pod,
            )
            .await
            .unwrap();
    }

    fn make_ready(pod: &mut Pod, since: chrono::DateTime<chrono::Utc>) {
        let status = pod.status.get_or_insert_with(Default::default);
        status.phase = Some(Phase::Running);
        status.conditions = Some(vec![rusternetes_common::resources::pod::PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(since),
            observed_generation: None,
        }]);
    }

    async fn rc_status(storage: &Arc<MemoryStorage>, name: &str) -> ReplicationControllerStatus {
        let rc: ReplicationController = storage
            .get(&format!("/registry/replicationcontrollers/default/{name}"))
            .await
            .unwrap();
        rc.status.expect("status written")
    }

    use rusternetes_common::resources::ReplicationControllerStatus;

    /// `calculateStatus(rs, activePods, ...)` over the pods listed before
    /// `manageReplicas` (replica_set.go:743, :597 "Does NOT modify
    /// <activePods>").
    #[tokio::test]
    async fn rc_status_counts_the_pods_listed_before_manage_replicas() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let rc = make_rc("pre", "default", 3, rc_selector("pre"), None);
        seed_rc(&storage, &controller, &rc).await;

        assert_eq!(rc_pods(&storage).await.len(), 3);
        assert_eq!(
            rc_status(&storage, "pre").await.replicas,
            0,
            "status is the pre-manage count, not a re-list"
        );
    }

    /// `CreationObserved` is the informer's `addPod` (replica_set.go:436);
    /// the sync never lowers its own expectations.
    #[tokio::test]
    async fn rc_sync_does_not_lower_its_own_create_expectations() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let rc = make_rc("exp", "default", 3, rc_selector("exp"), None);
        seed_rc(&storage, &controller, &rc).await;

        assert_eq!(
            controller.expectations.get_expectations("default/exp"),
            Some((3, 0))
        );
    }

    /// `getPodsToDelete` (replica_set.go:874) ranks victims: not-ready before
    /// ready (controller_utils.go:849).
    #[tokio::test]
    async fn rc_scale_down_deletes_the_not_ready_pod_first() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let rc = make_rc("rank", "default", 2, rc_selector("rank"), None);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        make_ready(&mut pods[0], ago);
        for pod in &mut pods {
            pod.spec.as_mut().unwrap().node_name = Some("n1".to_string());
            write_pod(&storage, pod).await;
        }
        let keep = pods[0].metadata.name.clone();
        controller.expectations.delete_expectations("default/rank");

        let mut rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/rank")
            .await
            .unwrap();
        rc.spec.replicas = Some(1);
        controller.reconcile_rc(&rc).await.unwrap();

        let dying: Vec<String> = rc_pods(&storage)
            .await
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .map(|p| p.metadata.name)
            .collect();
        assert_eq!(dying.len(), 1);
        assert_ne!(dying[0], keep, "the Ready pod must survive");
    }

    /// `fullyLabeledReplicas` matches the TEMPLATE labels
    /// (replica_set_utils.go:103-110).
    #[tokio::test]
    async fn rc_fully_labeled_replicas_matches_template_labels() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut template = rc_selector("fl");
        template.insert("tier".to_string(), "web".to_string());
        let rc = make_rc("fl", "default", 2, rc_selector("fl"), Some(template));
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        pods.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        pods[0].metadata.labels = Some(rc_selector("fl"));
        write_pod(&storage, &pods[0]).await;
        controller.expectations.delete_expectations("default/fl");

        let rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/fl")
            .await
            .unwrap();
        controller.reconcile_rc(&rc).await.unwrap();

        let status = rc_status(&storage, "fl").await;
        assert_eq!(status.replicas, 2);
        assert_eq!(status.fully_labeled_replicas, Some(1));
    }

    /// `IsPodAvailable` measures minReadySeconds from the Ready condition's
    /// lastTransitionTime (pkg/api/v1/pod/util.go:283-294); the RC's
    /// `availableReplicas` is that count, not `readyReplicas`.
    #[tokio::test]
    async fn rc_availability_honours_min_ready_seconds() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut rc = make_rc("avail", "default", 2, rc_selector("avail"), None);
        rc.spec.min_ready_seconds = Some(60);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        let now = chrono::Utc::now();
        make_ready(&mut pods[0], now - chrono::Duration::seconds(10));
        make_ready(&mut pods[1], now - chrono::Duration::seconds(120));
        for pod in &pods {
            write_pod(&storage, pod).await;
        }
        controller.expectations.delete_expectations("default/avail");

        let rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/avail")
            .await
            .unwrap();
        controller.reconcile_rc(&rc).await.unwrap();

        let status = rc_status(&storage, "avail").await;
        assert_eq!(status.ready_replicas, Some(2));
        assert_eq!(status.available_replicas, Some(1));
    }

    /// `syncReplicaSet` plans the next availability check
    /// (replica_set.go:775-788).
    #[tokio::test]
    async fn rc_requeues_when_a_ready_pod_is_not_yet_available() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let mut rc = make_rc("next", "default", 1, rc_selector("next"), None);
        rc.spec.min_ready_seconds = Some(60);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        make_ready(
            &mut pods[0],
            chrono::Utc::now() - chrono::Duration::seconds(10),
        );
        write_pod(&storage, &pods[0]).await;
        controller.expectations.delete_expectations("default/next");

        let rc: ReplicationController = storage
            .get("/registry/replicationcontrollers/default/next")
            .await
            .unwrap();
        let next = controller
            .reconcile_rc(&rc)
            .await
            .unwrap()
            .expect("an AddAfter must be planned");

        assert!(
            next > Duration::from_secs(48) && next <= Duration::from_secs(50),
            "~50s left until the pod is available, got {next:?}"
        );
    }

    /// The informer's re-list on (re)connect (`DeltaFIFO.Replace`): pods
    /// created while the watch was down are observed as adds, once, so a lost
    /// Added event does not wedge the sync until the expectations TTL.
    #[tokio::test]
    async fn rc_relist_observes_creations_the_watch_missed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc = make_rc("gap", "default", 2, rc_selector("gap"), None);
        seed_rc(&storage, &controller, &rc).await;
        assert_eq!(rc_pods(&storage).await.len(), 2);
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((2, 0)),
            "the sync itself does not observe its creates"
        );

        controller.relist_pods(&queue).await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((0, 0))
        );

        // Nothing new on a second re-list.
        controller.expectations.expect_creations("default/gap", 1);
        controller.relist_pods(&queue).await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((1, 0))
        );
    }

    // ---------------------------------------------------------------------
    // #3633 / #3634: selector semantics and informer event handlers. The RC
    // rides the RS controller (replication_controller.go:57-79); its lister
    // is `GetPodControllers` (client-go listers/core/v1/
    // replicationcontroller_expansion.go:36-66) and its selector is
    // `Convert_v1_ReplicationControllerSpec_To_apps_ReplicaSetSpec`
    // (pkg/apis/core/v1/conversion.go:117-130).
    // ---------------------------------------------------------------------

    async fn queued(queue: &WorkQueue) -> Vec<String> {
        let mut keys = Vec::new();
        while queue.len().await > 0 {
            let key = queue.get().await.unwrap();
            queue.done(&key).await;
            keys.push(key);
        }
        keys.sort();
        keys
    }

    /// A pod event for `p` in `default`. `owner` is (kind, name, uid, controller).
    fn rc_pod_event(
        make: fn(String, String) -> rusternetes_storage::WatchEvent,
        name: &str,
        labels: &[(&str, &str)],
        owner: Option<(&str, &str, &str, bool)>,
        rv: &str,
    ) -> rusternetes_storage::WatchEvent {
        let labels: HashMap<&str, &str> = labels.iter().copied().collect();
        let mut v = serde_json::json!({
            "metadata": {"name": name, "namespace": "default", "labels": labels,
                "resourceVersion": rv},
            "spec": {"containers": []},
        });
        if let Some((kind, owner_name, uid, controller)) = owner {
            v["metadata"]["ownerReferences"] = serde_json::json!([{
                "apiVersion": "v1", "kind": kind, "name": owner_name, "uid": uid,
                "controller": controller
            }]);
        }
        make(format!("/registry/pods/default/{name}"), v.to_string())
    }

    async fn put_rc(storage: &Arc<MemoryStorage>, rc: &ReplicationController) {
        storage
            .create(
                &format!(
                    "/registry/replicationcontrollers/default/{}",
                    rc.metadata.name
                ),
                rc,
            )
            .await
            .unwrap();
    }

    /// `selector: {}` converts to `LabelSelector{MatchLabels: {}}` and
    /// `LabelSelectorAsSelector` turns that into `labels.Everything()`, which
    /// claims even an unlabelled orphan.
    #[tokio::test]
    async fn rc_empty_selector_selects_everything_as_upstream() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        // `selector: {}` converts to `LabelSelector{MatchLabels: {}}`, which
        // `LabelSelectorAsSelector` turns into `labels.Everything()`.
        let rc = make_rc("all", "default", 1, HashMap::new(), Some(HashMap::new()));
        put_rc(&storage, &rc).await;
        let orphan = pod_from_event_json("o");
        storage
            .create("/registry/pods/default/o", &orphan)
            .await
            .unwrap();

        controller.reconcile_rc(&rc).await.unwrap();

        let pods = rc_pods(&storage).await;
        assert_eq!(
            pods.len(),
            1,
            "the unlabelled orphan is claimed, none created"
        );
        assert!(pods[0].metadata.owner_references.is_some());
    }

    fn pod_from_event_json(name: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"containers": []},
            "status": {"phase": "Running"},
        }))
        .unwrap()
    }

    /// `addPod`: only a `controller: true` reference counts
    /// (`GetControllerOf`, replica_set.go:426-440).
    #[tokio::test]
    async fn rc_pod_event_ignores_a_non_controller_owner_reference() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc = make_rc("g", "default", 1, rc_selector("g"), None);
        put_rc(&storage, &rc).await;
        controller.expectations.set_expectations("default/g", 1, 0);

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    rusternetes_storage::WatchEvent::Added,
                    "p",
                    &[("other", "x")],
                    Some(("ReplicationController", "g", &rc.metadata.uid, false)),
                    "1",
                ),
            )
            .await;

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0))
        );
    }

    /// `resolveControllerRef` (replica_set.go:302-321): wrong kind, missing
    /// controller and UID mismatch resolve to nil.
    #[tokio::test]
    async fn rc_pod_event_unresolvable_controller_ref_does_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc = make_rc("g", "default", 1, rc_selector("g"), None);
        put_rc(&storage, &rc).await;
        controller.expectations.set_expectations("default/g", 1, 0);

        for (i, (kind, name, uid)) in [
            ("ReplicaSet", "g", rc.metadata.uid.as_str()),
            ("ReplicationController", "missing", rc.metadata.uid.as_str()),
            ("ReplicationController", "g", "recreated-uid"),
        ]
        .into_iter()
        .enumerate()
        {
            controller
                .enqueue_owner_rc(
                    &queue,
                    &rc_pod_event(
                        rusternetes_storage::WatchEvent::Added,
                        &format!("p{i}"),
                        &[("app", "g")],
                        Some((kind, name, uid, true)),
                        "1",
                    ),
                )
                .await;
        }

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0))
        );
    }

    /// `addPod` of an orphan syncs the RCs `GetPodControllers` returns: those
    /// whose non-empty selector the pod's labels satisfy. A pod without
    /// labels matches none, and no creation is observed.
    #[tokio::test]
    async fn rc_orphan_pod_added_enqueues_only_matching_controllers() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        put_rc(
            &storage,
            &make_rc("hit", "default", 1, rc_selector("a"), None),
        )
        .await;
        put_rc(
            &storage,
            &make_rc("miss", "default", 1, rc_selector("z"), None),
        )
        .await;
        put_rc(
            &storage,
            &make_rc("empty", "default", 1, HashMap::new(), None),
        )
        .await;
        controller
            .expectations
            .set_expectations("default/hit", 1, 0);

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    rusternetes_storage::WatchEvent::Added,
                    "orphan",
                    &[("app", "a")],
                    None,
                    "1",
                ),
            )
            .await;
        assert_eq!(
            queued(&queue).await,
            vec!["replicationcontrollers/default/hit"]
        );
        assert_eq!(
            controller.expectations.get_expectations("default/hit"),
            Some((1, 0))
        );

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    rusternetes_storage::WatchEvent::Added,
                    "unlabeled",
                    &[],
                    None,
                    "1",
                ),
            )
            .await;
        assert!(queued(&queue).await.is_empty());
    }

    /// `updatePod`: dropping the ControllerRef wakes the former owner and the
    /// label-matching controllers; an unchanged orphan wakes nobody.
    #[tokio::test]
    async fn rc_update_pod_follows_the_controller_ref() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicationControllerController::new(storage.clone(), 5);
        let queue = WorkQueue::new();
        let rc1 = make_rc("foo", "default", 1, rc_selector("foo"), None);
        let rc2 = make_rc("barfoo", "default", 1, rc_selector("bar"), None);
        put_rc(&storage, &rc1).await;
        put_rc(&storage, &rc2).await;
        let added = rusternetes_storage::WatchEvent::Added;
        let modified = rusternetes_storage::WatchEvent::Modified;

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(
                    added,
                    "p",
                    &[("app", "foo")],
                    Some(("ReplicationController", "foo", &rc1.metadata.uid, true)),
                    "1",
                ),
            )
            .await;
        queued(&queue).await;

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(modified, "p", &[("app", "bar")], None, "2"),
            )
            .await;
        assert_eq!(
            queued(&queue).await,
            vec![
                "replicationcontrollers/default/barfoo",
                "replicationcontrollers/default/foo"
            ]
        );

        controller
            .enqueue_owner_rc(
                &queue,
                &rc_pod_event(modified, "p", &[("app", "bar")], None, "3"),
            )
            .await;
        assert!(queued(&queue).await.is_empty());
    }

    /// The worker wires `nextSyncDuration` to `queue.AddAfter` after `Forget`
    /// (replica_set.go:786-788; the RC rides this code): a ready but not yet
    /// available pod is re-checked by a second sync after `minReadySeconds`
    /// with no watch event to wake the key. (`WorkQueue` times on
    /// `std::time::Instant`, so a paused tokio clock cannot drive it; the delay
    /// here is real and 1s.)
    #[tokio::test]
    async fn rc_worker_requeues_the_key_after_the_next_sync_duration() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = Arc::new(ReplicationControllerController::new(storage.clone(), 5));
        let mut rc = make_rc("later", "default", 1, rc_selector("later"), None);
        rc.spec.min_ready_seconds = Some(1);
        seed_rc(&storage, &controller, &rc).await;
        let mut pods = rc_pods(&storage).await;
        make_ready(&mut pods[0], chrono::Utc::now());
        write_pod(&storage, &pods[0]).await;
        controller.expectations.delete_expectations("default/later");

        let queue = WorkQueue::new();
        let worker = tokio::spawn({
            let controller = Arc::clone(&controller);
            let queue = queue.clone();
            async move { controller.worker(queue).await }
        });
        queue
            .add("replicationcontrollers/default/later".to_string())
            .await;

        let available = async {
            loop {
                let rc: ReplicationController = storage
                    .get("/registry/replicationcontrollers/default/later")
                    .await
                    .unwrap();
                if rc.status.is_some_and(|s| s.available_replicas == Some(1)) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), available)
            .await
            .expect("the AddAfter must drive a second sync that sees the pod available");
        queue.shutdown().await;
        worker.await.unwrap();
    }
}
