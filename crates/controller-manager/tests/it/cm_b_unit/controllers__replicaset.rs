//! Unit tests moved from `src/controllers/replicaset.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures::StreamExt;
    pub use rusternetes_common::{
        resources::{Pod, PodStatus, ReplicaSet, ReplicaSetStatus},
        types::{label_selector_as_selector, ObjectMeta, Phase, Selector},
    };
    pub use rusternetes_controller_manager::controllers::controller_utils::{
        find_min_next_pod_availability_check, is_pod_available, is_pod_ready, is_pod_terminating,
        node_of, pod_key, pods_on_node_counts, sort_active_pods_with_ranks, RankGates,
    };
    pub use rusternetes_controller_manager::controllers::expectations::ControllerExpectations;
    pub use rusternetes_controller_manager::controllers::pod_informer::{
        apply_pod_observation, ControllerRef, PodEventTarget, PodObservation, ResolvedController,
        SharedPodInformer,
    };
    pub use rusternetes_controller_manager::controllers::replicationcontroller::{
        generate_pod_name, is_namespace_terminating_rejection, slow_start_batches, BURST_REPLICAS,
        MAX_NAME_GENERATION_CREATE_ATTEMPTS, SLOW_START_INITIAL_BATCH_SIZE,
    };
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::WatchEvent;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::{sync::Arc, time::Duration};
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::PodSpec;
    use rusternetes_common::types::{LabelSelector, OwnerReference, TypeMeta};
    use rusternetes_controller_manager::controllers::replicaset::*;
    use rusternetes_storage::memory::MemoryStorage;
    use rusternetes_storage::Storage;
    use std::collections::HashMap;

    fn make_labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn make_replicaset(name: &str, labels: HashMap<String, String>, replicas: i32) -> ReplicaSet {
        ReplicaSet {
            type_meta: TypeMeta {
                kind: "ReplicaSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("default"),
            spec: rusternetes_common::resources::ReplicaSetSpec {
                replicas,
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                template: rusternetes_common::resources::PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels)),
                    spec: PodSpec {
                        containers: vec![],
                        ..Default::default()
                    },
                },
                min_ready_seconds: None,
            },
            status: None,
        }
    }

    fn make_pod(name: &str, labels: HashMap<String, String>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace("default")
                .with_labels(labels),
            spec: Some(PodSpec {
                containers: vec![],
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Running),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn test_matches_selector() {
        let labels = make_labels(&[("app", "test")]);

        let pod_with_labels = make_pod("test-pod", labels);
        assert_eq!(pod_with_labels.metadata.name, "test-pod");
    }

    /// Regression for `[sig-apps] ReplicaSet should surface a failure condition
    /// on a common issue like exceeded quota`: a pod-creation failure must be
    /// surfaced as a `ReplicaFailure`/`FailedCreate` status condition, and
    /// cleared once creates succeed. (MemoryStorage can't deny on quota, so we
    /// exercise the status path directly.)
    #[tokio::test]
    async fn test_update_status_sets_and_clears_replica_failure() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);
        let labels = make_labels(&[("app", "rf")]);
        let rs = make_replicaset("rf-rs", labels, 1);
        let rs_key = build_key("replicasets", Some("default"), "rf-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Failure present → ReplicaFailure=True / FailedCreate.
        controller
            .update_status(
                &rs,
                calculate_status(
                    &rs,
                    &[],
                    &[],
                    Some("pods \"x\" is forbidden: exceeded quota"),
                    chrono::Utc::now(),
                ),
            )
            .await
            .unwrap();
        let after: ReplicaSet = storage.get(&rs_key).await.unwrap();
        let cond = after
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "ReplicaFailure"))
            .expect("ReplicaFailure condition must be set on a pod-create failure");
        assert_eq!(cond.status, "True");
        assert_eq!(cond.reason.as_deref(), Some("FailedCreate"));
        assert!(cond.message.as_deref().unwrap_or("").contains("quota"));

        // Failure resolved → ReplicaFailure cleared.
        // A status write is conditional on the resourceVersion of the object it
        // was handed (#2153), so the second sync re-reads like a worker would.
        let rs: ReplicaSet = storage.get(&rs_key).await.unwrap();
        controller
            .update_status(
                &rs,
                calculate_status(&rs, &[], &[], None, chrono::Utc::now()),
            )
            .await
            .unwrap();
        let after2: ReplicaSet = storage.get(&rs_key).await.unwrap();
        let has_rf = after2
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure"))
            .unwrap_or(false);
        assert!(!has_rf, "ReplicaFailure must clear once creates succeed");
    }

    /// `[sig-apps] ReplicaSet should surface a failure condition on a common
    /// issue like exceeded quota`: the rejection is the api-server's quota
    /// admission 403, not a client-side pre-check (#3618; upstream
    /// `manageReplicas` -> `calculateStatus` records the create error as
    /// `ReplicaFailure`). The controller must issue the create and surface
    /// the Forbidden it gets back, then clear the condition once it clears.
    #[tokio::test]
    async fn test_rs_replica_failure_from_apiserver_quota_forbidden() {
        let storage =
            Arc::new(rusternetes_controller_manager::controllers::quota_admission_double::QuotaAdmissionStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);
        let labels = make_labels(&[("app", "q")]);
        let rs = make_replicaset("q-rs", labels, 2);
        let rs_key = build_key("replicasets", Some("default"), "q-rs");
        storage.create(&rs_key, &rs).await.unwrap();
        let quota = serde_json::json!({
            "apiVersion": "v1", "kind": "ResourceQuota",
            "metadata": {"name": "pq", "namespace": "default", "uid": "q-uid"},
            "spec": {"hard": {"pods": "0"}}
        });
        let quota_key = "/registry/resourcequotas/default/pq";
        storage.create(quota_key, &quota).await.unwrap();

        controller.reconcile_all().await.unwrap();

        assert!(
            storage
                .denied_creates
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0,
            "the controller must reach the api-server and be denied there"
        );
        let after: ReplicaSet = storage.get(&rs_key).await.unwrap();
        let cond = after
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == "ReplicaFailure"))
            .expect("ReplicaFailure must be set from the api-server's Forbidden");
        assert_eq!(cond.reason.as_deref(), Some("FailedCreate"));
        assert!(cond
            .message
            .as_deref()
            .unwrap_or("")
            .contains("exceeded quota"));

        storage.delete(quota_key).await.unwrap();
        controller.reconcile_all().await.unwrap();
        let after: ReplicaSet = storage.get(&rs_key).await.unwrap();
        assert!(
            !after
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .is_some_and(|cs| cs.iter().any(|c| c.condition_type == "ReplicaFailure")),
            "ReplicaFailure must clear once creates succeed"
        );
    }

    #[tokio::test]
    async fn test_adopt_orphan_pods_and_release_non_matching() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);

        let labels = make_labels(&[("app", "myapp")]);

        // Create an orphan pod with matching labels but no ownerReference
        let orphan_pod = make_pod("orphan-pod", labels.clone());
        let pod_key = build_key("pods", Some("default"), "orphan-pod");
        storage.create(&pod_key, &orphan_pod).await.unwrap();

        // Create a ReplicaSet with matching selector and replicas=1
        let rs = make_replicaset("my-rs", labels.clone(), 1);
        let rs_key = build_key("replicasets", Some("default"), "my-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run reconciliation
        controller.reconcile_all().await.unwrap();

        // Verify the orphan pod was adopted (now has ownerReference pointing to the RS)
        let adopted_pod: Pod = storage.get(&pod_key).await.unwrap();
        let owner_refs = adopted_pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "ReplicaSet");
        assert_eq!(owner_refs[0].name, "my-rs");
        assert_eq!(owner_refs[0].controller, Some(true));
        assert_eq!(owner_refs[0].block_owner_deletion, Some(true));

        // Since the RS wants 1 replica and has adopted 1, no extra pods should be created
        let pods_prefix = build_prefix("pods", Some("default"));
        let all_pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(
            all_pods.len(),
            1,
            "Should have exactly 1 pod (the adopted one), got {}",
            all_pods.len()
        );

        // Now change the pod's labels so they no longer match the RS selector
        let mut modified_pod: Pod = storage.get(&pod_key).await.unwrap();
        modified_pod.metadata.labels = Some(make_labels(&[("app", "different")]));
        storage.update(&pod_key, &modified_pod).await.unwrap();

        // Run reconciliation again
        controller.reconcile_all().await.unwrap();

        // Verify the pod's ownerReference was removed (released)
        let released_pod: Pod = storage.get(&pod_key).await.unwrap();
        let has_rs_owner = released_pod
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.kind == "ReplicaSet" && r.name == "my-rs")
            })
            .unwrap_or(false);
        assert!(
            !has_rs_owner,
            "Pod should no longer have ownerReference to RS after label change"
        );
    }

    #[tokio::test]
    async fn test_adopt_does_not_steal_owned_pods() {
        // Pods that already have a controller owner should NOT be adopted
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);

        let labels = make_labels(&[("app", "myapp")]);

        // Create a pod owned by a different controller
        let mut owned_pod = make_pod("owned-pod", labels.clone());
        owned_pod.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: "other-rs".to_string(),
            uid: "other-uid".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        let pod_key = build_key("pods", Some("default"), "owned-pod");
        storage.create(&pod_key, &owned_pod).await.unwrap();

        // Create a RS with matching selector
        let rs = make_replicaset("my-rs", labels.clone(), 1);
        let rs_key = build_key("replicasets", Some("default"), "my-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        // Run reconciliation
        controller.reconcile_all().await.unwrap();

        // Verify the owned pod was NOT adopted (still owned by other-rs)
        let pod: Pod = storage.get(&pod_key).await.unwrap();
        let refs = pod.metadata.owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "other-rs");
    }

    #[tokio::test]
    async fn test_rs_creates_pods_with_selector_labels_when_template_has_none() {
        // Regression: a ReplicaSet whose template has no labels would create
        // pods with no labels, so the controller's matches_selector check
        // failed for its own pods, causing infinite pod creation. Fall back
        // to selector matchLabels when template labels are absent.
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);

        let labels = make_labels(&[("app", "myapp")]);

        // Build an RS where the pod template has no labels at all.
        let mut rs = make_replicaset("no-template-labels-rs", labels.clone(), 2);
        rs.spec.template.metadata = None;
        let rs_key = build_key("replicasets", Some("default"), "no-template-labels-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        controller.reconcile_all().await.unwrap();

        // Two pods should be created with selector labels applied so the
        // controller matches them on the next reconcile.
        let pods_prefix = build_prefix("pods", Some("default"));
        let pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(pods.len(), 2, "RS should create exactly 2 pods");
        for pod in &pods {
            let pod_labels = pod
                .metadata
                .labels
                .as_ref()
                .expect("Pod must inherit selector labels");
            assert_eq!(pod_labels.get("app"), Some(&"myapp".to_string()));
        }

        // Second reconcile must not create more pods (matching works).
        controller.reconcile_all().await.unwrap();
        let pods: Vec<Pod> = storage.list(&pods_prefix).await.unwrap();
        assert_eq!(
            pods.len(),
            2,
            "second reconcile must match existing pods, not create more"
        );
    }

    #[tokio::test]
    async fn test_rs_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);

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

        let labels = make_labels(&[("app", "pullsecrets")]);
        let rs = make_replicaset("pullsecrets-rs", labels, 1);
        let rs_key = build_key("replicasets", Some("default"), "pullsecrets-rs");
        storage.create(&rs_key, &rs).await.unwrap();

        controller.reconcile_all().await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 1);
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

    #[test]
    fn test_expectations_primitives() {
        let c = ReplicaSetController::new(Arc::new(MemoryStorage::new()), 30);
        // No entry → satisfied.
        assert!(c.expectations_satisfied("default/rs"));
        // Outstanding create → not satisfied until observed.
        c.set_expectations("default/rs", 2, 0);
        assert!(!c.expectations_satisfied("default/rs"));
        c.observe_creation("default/rs");
        assert!(!c.expectations_satisfied("default/rs"));
        c.observe_creation("default/rs");
        assert!(c.expectations_satisfied("default/rs"));
        // Outstanding delete → not satisfied until observed.
        c.expectations
            .expect_deletions_of("default/rs", &["default/p".to_string()]);
        assert!(!c.expectations_satisfied("default/rs"));
        c.observe_deletion("default/rs", "default/p");
        assert!(c.expectations_satisfied("default/rs"));
        // Clearing removes the entry (treated as satisfied).
        c.set_expectations("default/rs", 5, 0);
        c.clear_expectations("default/rs");
        assert!(c.expectations_satisfied("default/rs"));
    }

    /// The core #542 guard: while a ReplicaSet has unmet create expectations
    /// (pods created but not yet observed via the watch), a re-reconcile that
    /// momentarily sees too few pods MUST NOT create duplicates.
    #[tokio::test]
    async fn test_reconcile_defers_create_while_expectations_unmet() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let labels = make_labels(&[("name", "pod1")]);
        let rs = make_replicaset("rs1", labels.clone(), 2);
        let rs_key = build_key("replicasets", Some("default"), "rs1");
        storage.create(&rs_key, &rs).await.unwrap();

        // Simulate "we already issued 2 creates that haven't been observed yet"
        // — i.e. the storage list does not yet reflect them.
        controller.set_expectations("default/rs1", 2, 0);

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.len(),
            0,
            "must not create pods while create expectations are unmet (got {})",
            pods.len()
        );

        // Once both creations are observed, the next reconcile may act.
        controller.observe_creation("default/rs1");
        controller.observe_creation("default/rs1");
        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.len(),
            2,
            "after expectations satisfied, reconcile creates the desired pods"
        );
    }

    fn owned_pod(name: &str, labels: HashMap<String, String>, rs: &str, uid: &str) -> Pod {
        let mut pod = make_pod(name, labels);
        pod.metadata.owner_references = Some(vec![rusternetes_common::types::OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: "ReplicaSet".to_string(),
            name: rs.to_string(),
            uid: uid.to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        pod
    }

    /// #2458: upstream `DeletePod` issues `Delete(ctx, podID, DeleteOptions{})`
    /// (`pkg/controller/controller_utils.go:618`), a graceful delete: the pod
    /// is stamped with a deletionTimestamp and survives for its grace period.
    /// The terminating pod must not count as a replica
    /// (`FilterActivePods`, `controller_utils.go:1004`), or the next sync would
    /// delete another pod.
    #[tokio::test]
    async fn test_scale_down_deletes_pods_gracefully() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let labels = make_labels(&[("app", "g")]);
        let rs = make_replicaset("g", labels.clone(), 1);
        storage
            .create(&build_key("replicasets", Some("default"), "g"), &rs)
            .await
            .unwrap();
        for n in ["p1", "p2", "p3"] {
            storage
                .create(
                    &build_key("pods", Some("default"), n),
                    &owned_pod(n, labels.clone(), "g", &rs.metadata.uid),
                )
                .await
                .unwrap();
        }

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 3, "graceful delete keeps the pods in storage");
        let terminating = pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .count();
        assert_eq!(terminating, 2);

        // A second sync sees 1 active pod == desired: nothing more to delete.
        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.iter()
                .filter(|p| p.metadata.deletion_timestamp.is_some())
                .count(),
            2
        );
        let got: ReplicaSet = storage
            .get(&build_key("replicasets", Some("default"), "g"))
            .await
            .unwrap();
        assert_eq!(
            got.status.unwrap().replicas,
            1,
            "terminating pods are not replicas"
        );
    }

    /// #2458: a graceful delete first MODIFIES the pod (deletionTimestamp),
    /// and upstream `updatePod` observes the deletion there
    /// (`replica_set.go:468-480`) so the RS can create replacements without
    /// waiting for the kubelet's final DELETE. The later DELETED event for the
    /// same pod must not be counted twice (`UIDTrackingControllerExpectations`,
    /// `controller_utils.go:380-390`).
    #[tokio::test]
    async fn test_modified_with_deletion_timestamp_observes_deletion_once() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let labels = make_labels(&[("app", "g")]);
        // `resolveControllerRef` needs the RS the pod's reference names.
        let mut rs = make_replicaset("g", labels.clone(), 1);
        rs.metadata.uid = "u".to_string();
        put_rs(&storage, &rs).await;
        controller.expectations.expect_deletions_of(
            "default/g",
            &["default/p1".to_string(), "default/p2".to_string()],
        );

        let mut pod = owned_pod("p1", labels, "g", "u");
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let json = serde_json::to_string(&pod).unwrap();
        let key = "/registry/pods/default/p1".to_string();

        controller
            .enqueue_owner_replicaset(&queue, &WatchEvent::Modified(key.clone(), json.clone()))
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1))
        );

        controller
            .enqueue_owner_replicaset(&queue, &WatchEvent::Deleted(key, json))
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((0, 1)),
            "the DELETED event of an already-observed pod must not count again"
        );
    }

    // ---------------------------------------------------------------------
    // manageReplicas / syncReplicaSet parity (#3619)
    //
    // Ported from upstream pkg/controller/replicaset/replica_set_test.go:
    // TestSlowStartBatch, TestSyncReplicaSetCreateFailures,
    // TestControllerBurstReplicas, TestRSSyncExpectations,
    // TestControllerUpdateStatusWithFailure and, from
    // replica_set_utils_test.go, TestCalculateStatusConditions.
    // ---------------------------------------------------------------------

    use crate::cm_b_unit::controllers__replicationcontroller::tests::{
        ConcurrencyProbeStorage, TerminatingNamespaceStorage,
    };
    use rusternetes_controller_manager::controllers::quota_admission_double::QuotaAdmissionStorage;

    fn rs_condition(
        rs: &ReplicaSet,
        ty: &str,
    ) -> Option<rusternetes_common::resources::ReplicaSetCondition> {
        rs.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|cs| cs.iter().find(|c| c.condition_type == ty))
            .cloned()
    }

    /// `slowStartBatch` (replica_set.go:820-844) issues each batch
    /// concurrently, and one sync must reach the full replica count. Serial
    /// creates could not create 100 pods inside the conformance spec's 120s
    /// budget (#1847).
    #[tokio::test]
    async fn rs_creates_pods_in_concurrent_slow_start_batches() {
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(ConcurrencyProbeStorage::new(inner));
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let labels = make_labels(&[("app", "ss")]);
        let rs = make_replicaset("ss", labels, 100);
        storage
            .create(&build_key("replicasets", Some("default"), "ss"), &rs)
            .await
            .unwrap();

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 100);
        assert!(
            storage.peak() > 1,
            "creates within a slow-start batch must overlap (peak {})",
            storage.peak()
        );
    }

    /// `TestControllerBurstReplicas`: `diff > rsc.burstReplicas` is capped
    /// (replica_set.go:611-613); the next sync creates the remainder.
    #[tokio::test]
    async fn rs_caps_creates_at_burst_replicas_and_finishes_next_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let desired =
            rusternetes_controller_manager::controllers::replicationcontroller::BURST_REPLICAS
                as i32
                + 100;
        let rs = make_replicaset("burst", make_labels(&[("app", "burst")]), desired);
        storage
            .create(&build_key("replicasets", Some("default"), "burst"), &rs)
            .await
            .unwrap();

        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(
            pods.len(),
            rusternetes_controller_manager::controllers::replicationcontroller::BURST_REPLICAS,
            "one sync must not exceed burstReplicas"
        );

        // The creates were observed (pod watch Added events); simulate them.
        for _ in
            0..rusternetes_controller_manager::controllers::replicationcontroller::BURST_REPLICAS
        {
            controller.observe_creation("default/burst");
        }
        let rs: ReplicaSet = storage
            .get(&build_key("replicasets", Some("default"), "burst"))
            .await
            .unwrap();
        controller.reconcile_replicaset(&rs).await.unwrap();
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), desired as usize);
    }

    /// `TestSyncReplicaSetCreateFailures`: 100 replicas, a limit of 10 (quota);
    /// the controller must not make 100 create calls in one pass. Slow start
    /// stops after the first failing batch (replica_set.go:838-840).
    #[tokio::test]
    async fn rs_stops_creating_after_the_first_failing_batch() {
        let storage = Arc::new(QuotaAdmissionStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let rs = make_replicaset("cf", make_labels(&[("app", "cf")]), 100);
        storage
            .create(&build_key("replicasets", Some("default"), "cf"), &rs)
            .await
            .unwrap();
        storage
            .create(
                "/registry/resourcequotas/default/pq",
                &serde_json::json!({
                    "apiVersion": "v1", "kind": "ResourceQuota",
                    "metadata": {"name": "pq", "namespace": "default", "uid": "q"},
                    "spec": {"hard": {"pods": "10"}}
                }),
            )
            .await
            .unwrap();

        // syncReplicaSet returns manageReplicasErr after updating status, so
        // the workqueue requeues with backoff (replica_set.go:772-774).
        let result = controller.reconcile_replicaset(&rs).await;
        assert!(result.is_err(), "a create failure must requeue the key");

        let denied = storage
            .denied_creates
            .load(std::sync::atomic::Ordering::SeqCst);
        // Batches 1,2,4,8: the 8-batch is the first to fail, so <= 8 denials.
        assert!(
            denied > 0 && denied <= 8,
            "expected the sync to stop after the first failing batch, saw {denied} denials"
        );
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert!(pods.len() >= 10 && pods.len() < 100, "got {}", pods.len());
    }

    /// `HasStatusCause(err, v1.NamespaceTerminatingCause)` -> `return nil`
    /// (replica_set.go:632-637): no ReplicaFailure, no requeue.
    #[tokio::test]
    async fn rs_ignores_namespace_terminating_create_rejection() {
        let storage = Arc::new(TerminatingNamespaceStorage::new(Arc::new(
            MemoryStorage::new(),
        )));
        let controller = ReplicaSetController::new(storage.clone(), 5);
        let rs = make_replicaset("doomed", make_labels(&[("app", "doomed")]), 5);
        let key = build_key("replicasets", Some("default"), "doomed");
        storage.create(&key, &rs).await.unwrap();

        controller
            .reconcile_replicaset(&rs)
            .await
            .expect("a terminating namespace must not fail the sync");

        let after: ReplicaSet = storage.get(&key).await.unwrap();
        assert!(
            rs_condition(&after, "ReplicaFailure").is_none(),
            "a terminating namespace must not raise ReplicaFailure"
        );
    }

    /// Storage whose first pod `list` lets one "late" pod arrive and lowers
    /// the RS's outstanding create expectation AFTER the snapshot was taken —
    /// upstream's `FakeRSExpectations` hook in `TestRSSyncExpectations`.
    struct LatePodStorage {
        inner: Arc<MemoryStorage>,
        late_pod: std::sync::Mutex<Option<Pod>>,
        expectations: std::sync::OnceLock<Arc<ControllerExpectations>>,
    }

    #[async_trait::async_trait]
    impl Storage for LatePodStorage {
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
        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            let snapshot = self.inner.list(prefix).await;
            if prefix.starts_with("/registry/pods/") {
                let late = self.late_pod.lock().unwrap().take();
                if let Some(pod) = late {
                    // The pod arrives, and its Added event lowers the
                    // expectation, strictly after the snapshot above.
                    let key = build_key("pods", Some("default"), &pod.metadata.name);
                    self.inner.create(&key, &pod).await.unwrap();
                    self.expectations
                        .get()
                        .unwrap()
                        .creation_observed("default/race");
                }
            }
            snapshot
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

    /// `TestRSSyncExpectations`: a pod cannot sneak in between listing pods and
    /// checking expectations. Expectations must be read BEFORE the list
    /// (replica_set.go:728 precedes :736): checked after, the late pod makes the
    /// record look fulfilled while the list still lacks it, and the controller
    /// creates a duplicate.
    #[tokio::test]
    async fn rs_sync_reads_expectations_before_listing_pods() {
        let labels = make_labels(&[("app", "race")]);
        let rs = make_replicaset("race", labels.clone(), 2);
        let storage = Arc::new(LatePodStorage {
            inner: Arc::new(MemoryStorage::new()),
            late_pod: std::sync::Mutex::new(Some(owned_pod(
                "late",
                labels.clone(),
                "race",
                &rs.metadata.uid,
            ))),
            expectations: std::sync::OnceLock::new(),
        });
        let controller = ReplicaSetController::new(storage.clone(), 5);
        storage
            .expectations
            .set(controller.expectations.clone())
            .ok()
            .unwrap();
        storage
            .create(&build_key("replicasets", Some("default"), "race"), &rs)
            .await
            .unwrap();
        storage
            .create(
                &build_key("pods", Some("default"), "first"),
                &owned_pod("first", labels, "race", &rs.metadata.uid),
            )
            .await
            .unwrap();
        // One create is in flight: the "late" pod.
        controller.expectations.expect_creations("default/race", 1);

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 2, "no duplicate create: got {}", pods.len());
    }

    /// `SetCondition` (replica_set_utils.go:169-176) is a no-op when the
    /// condition already exists with the same status and reason, and
    /// `calculateStatus` only sets ReplicaFailure when none exists
    /// (:124-136): a failure that persists keeps its lastTransitionTime and
    /// message instead of being rewritten every sync.
    #[tokio::test]
    async fn rs_replica_failure_is_not_rewritten_while_it_persists() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(storage.clone(), 10);
        let rs = make_replicaset("stable", make_labels(&[("app", "s")]), 1);
        let key = build_key("replicasets", Some("default"), "stable");
        storage.create(&key, &rs).await.unwrap();

        controller
            .update_status(
                &rs,
                calculate_status(&rs, &[], &[], Some("first failure"), chrono::Utc::now()),
            )
            .await
            .unwrap();
        let first = rs_condition(&storage.get(&key).await.unwrap(), "ReplicaFailure").unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        let rs: ReplicaSet = storage.get(&key).await.unwrap();
        controller
            .update_status(
                &rs,
                calculate_status(&rs, &[], &[], Some("second failure"), chrono::Utc::now()),
            )
            .await
            .unwrap();
        let second = rs_condition(&storage.get(&key).await.unwrap(), "ReplicaFailure").unwrap();

        assert_eq!(second.message, first.message);
        assert_eq!(second.last_transition_time, first.last_transition_time);
    }

    /// `TestCalculateStatusConditions`: reason is FailedCreate for diff < 0,
    /// FailedDelete for diff > 0 (replica_set_utils.go:127-132).
    #[test]
    fn rs_replica_failure_reason_follows_the_diff() {
        assert_eq!(replica_failure_reason(1, 2), Some("FailedCreate"));
        assert_eq!(replica_failure_reason(3, 2), Some("FailedDelete"));
        // diff == 0 leaves `reason` unset (a zero-value string upstream).
        assert_eq!(replica_failure_reason(2, 2), None);
    }

    // ---------------------------------------------------------------------
    // syncReplicaSet / calculateStatus / getPodsToDelete parity (#3624)
    //
    // Ported from upstream pkg/controller/replicaset/replica_set.go
    // (`syncReplicaSet` :713-790, `getPodsToDelete` :874) and
    // replica_set_utils.go (`calculateStatus` :96-141), with
    // pkg/controller/controller_utils.go `ActivePodsWithRanks` (:778-870).
    // ---------------------------------------------------------------------

    use rusternetes_common::resources::pod::PodCondition;

    fn ready_condition(since: chrono::DateTime<chrono::Utc>) -> PodCondition {
        PodCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            reason: None,
            message: None,
            last_probe_time: None,
            last_transition_time: Some(since),
            observed_generation: None,
        }
    }

    /// A pod owned by `rs`, on `node` ("" = unscheduled), in `phase`, Ready
    /// since `ready_since` (None = not ready).
    fn ranked_pod(
        name: &str,
        rs: &ReplicaSet,
        node: &str,
        phase: Phase,
        ready_since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Pod {
        let labels = rs.spec.selector.match_labels.clone().unwrap_or_default();
        let mut pod = owned_pod(name, labels, &rs.metadata.name, &rs.metadata.uid);
        if !node.is_empty() {
            pod.spec.as_mut().unwrap().node_name = Some(node.to_string());
        }
        let status = pod.status.as_mut().unwrap();
        status.phase = Some(phase);
        status.conditions = ready_since.map(|t| vec![ready_condition(t)]);
        pod
    }

    async fn seed_rs(storage: &Arc<MemoryStorage>, rs: &ReplicaSet, pods: &[Pod]) {
        storage
            .create(
                &build_key("replicasets", Some("default"), &rs.metadata.name),
                rs,
            )
            .await
            .unwrap();
        for pod in pods {
            storage
                .create(&build_key("pods", Some("default"), &pod.metadata.name), pod)
                .await
                .unwrap();
        }
    }

    async fn terminating_pod_names(storage: &Arc<MemoryStorage>) -> Vec<String> {
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        let mut names: Vec<String> = pods
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_some())
            .map(|p| p.metadata.name)
            .collect();
        names.sort();
        names
    }

    async fn status_of(storage: &Arc<MemoryStorage>, name: &str) -> ReplicaSetStatus {
        let rs: ReplicaSet = storage
            .get(&build_key("replicasets", Some("default"), name))
            .await
            .unwrap();
        rs.status.expect("status written")
    }

    /// `syncReplicaSet` computes status from the `activePods` it listed BEFORE
    /// `manageReplicas` (replica_set.go:743 `calculateStatus(rs, activePods,
    /// ...)`; `manageReplicas` is documented "Does NOT modify <activePods>",
    /// :597). Pods created this sync are counted by the sync their Added
    /// events trigger, not by a second list.
    #[tokio::test]
    async fn rs_status_counts_the_pods_listed_before_manage_replicas() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("pre", make_labels(&[("app", "pre")]), 3);
        seed_rs(&storage, &rs, &[]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 3);
        assert_eq!(
            status_of(&storage, "pre").await.replicas,
            0,
            "status is the pre-manage count, not a re-list"
        );
    }

    /// Creations are observed by the informer's `addPod` handler
    /// (replica_set.go:436 `CreationObserved`), never by the sync itself: a
    /// sync that lowers its own expectations from a re-list defeats the brake.
    #[tokio::test]
    async fn rs_sync_does_not_lower_its_own_create_expectations() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("exp", make_labels(&[("app", "exp")]), 3);
        seed_rs(&storage, &rs, &[]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(
            controller.expectations.get_expectations("default/exp"),
            Some((3, 0)),
            "only pod-watch Added events lower the creation expectation"
        );
    }

    /// `getPodsToDelete` (replica_set.go:874) deletes the ranked victims,
    /// not the first `diff` pods: not-ready before ready
    /// (`ActivePodsWithRanks.Less` rule 3, controller_utils.go:849).
    #[tokio::test]
    async fn rs_scale_down_deletes_the_not_ready_pod_first() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("rank", make_labels(&[("app", "rank")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let pods = [
            ranked_pod("a-ready", &rs, "n1", Phase::Running, Some(ago)),
            ranked_pod("z-notready", &rs, "n1", Phase::Running, None),
        ];
        seed_rs(&storage, &rs, &pods).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(terminating_pod_names(&storage).await, vec!["z-notready"]);
    }

    /// Rule 5 of `ActivePodsWithRanks.Less` (controller_utils.go:861): a pod
    /// colocated with more related pods is deleted first
    /// (`getPodsRankedByRelatedPodsOnSameNode`, replica_set.go:942-957).
    #[tokio::test]
    async fn rs_scale_down_prefers_the_pod_sharing_a_node() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("colo", make_labels(&[("app", "colo")]), 2);
        // Related pods come from the ReplicaSets sharing this one's controller
        // (`getReplicaSetsWithSameController`, replica_set.go:264-286); a bare
        // ReplicaSet has none, and every rank is 0.
        rs.metadata.owner_references = Some(vec![OwnerReference::new(
            "apps/v1",
            "Deployment",
            "colo-deploy",
            "deploy-uid",
        )
        .with_controller(true)]);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let pods = [
            ranked_pod("a-alone", &rs, "n2", Phase::Running, Some(ago)),
            ranked_pod("b-shared", &rs, "n1", Phase::Running, Some(ago)),
            ranked_pod("c-shared", &rs, "n1", Phase::Running, Some(ago)),
        ];
        seed_rs(&storage, &rs, &pods).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let victims = terminating_pod_names(&storage).await;
        assert_eq!(victims.len(), 1);
        assert_ne!(victims[0], "a-alone", "the pod alone on its node survives");
    }

    /// Unscheduled before scheduled, pending before running
    /// (controller_utils.go:835-846; TestGetPodsToDelete "various pod phases").
    #[tokio::test]
    async fn rs_scale_down_prefers_unscheduled_then_pending_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("phase", make_labels(&[("app", "phase")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let pods = [
            ranked_pod("a-running", &rs, "n1", Phase::Running, Some(ago)),
            ranked_pod("b-pending", &rs, "n1", Phase::Pending, None),
            ranked_pod("c-unscheduled", &rs, "", Phase::Pending, None),
        ];
        seed_rs(&storage, &rs, &pods).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(
            terminating_pod_names(&storage).await,
            vec!["b-pending", "c-unscheduled"]
        );
    }

    /// `calculateStatus` counts a pod fully labeled when the TEMPLATE labels
    /// match it (replica_set_utils.go:103-110), not every active pod.
    #[tokio::test]
    async fn rs_fully_labeled_replicas_matches_template_labels() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("fl", make_labels(&[("app", "fl")]), 2);
        rs.spec.template.metadata =
            Some(ObjectMeta::new("").with_labels(make_labels(&[("app", "fl"), ("tier", "web")])));
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let mut full = ranked_pod("full", &rs, "n1", Phase::Running, Some(ago));
        full.metadata.labels = Some(make_labels(&[("app", "fl"), ("tier", "web")]));
        let partial = ranked_pod("partial", &rs, "n1", Phase::Running, Some(ago));
        seed_rs(&storage, &rs, &[full, partial]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let status = status_of(&storage, "fl").await;
        assert_eq!(status.replicas, 2);
        assert_eq!(status.fully_labeled_replicas, Some(1));
    }

    /// `IsPodAvailable` (pkg/api/v1/pod/util.go:283-294) measures
    /// minReadySeconds from the Ready condition's lastTransitionTime, not the
    /// pod's creation time.
    #[tokio::test]
    async fn rs_availability_uses_the_ready_transition_time() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("avail", make_labels(&[("app", "avail")]), 2);
        rs.spec.min_ready_seconds = Some(60);
        let now = chrono::Utc::now();
        // Created an hour ago but only just became Ready: not available.
        let mut just_ready = ranked_pod(
            "just-ready",
            &rs,
            "n1",
            Phase::Running,
            Some(now - chrono::Duration::seconds(10)),
        );
        just_ready.metadata.creation_timestamp = Some(now - chrono::Duration::hours(1));
        // Also created an hour ago, Ready for two minutes: available.
        let mut long_ready = ranked_pod(
            "long-ready",
            &rs,
            "n1",
            Phase::Running,
            Some(now - chrono::Duration::seconds(120)),
        );
        long_ready.metadata.creation_timestamp = Some(now - chrono::Duration::hours(1));
        seed_rs(&storage, &rs, &[just_ready, long_ready]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let status = status_of(&storage, "avail").await;
        assert_eq!(status.ready_replicas, 2);
        assert_eq!(status.available_replicas, 1);
    }

    /// `syncReplicaSet` plans the next availability check
    /// (replica_set.go:775-788): `FindMinNextPodAvailabilityCheck` gives the
    /// time until the earliest ready pod turns available.
    #[tokio::test]
    async fn rs_requeues_when_a_ready_pod_is_not_yet_available() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("next", make_labels(&[("app", "next")]), 1);
        rs.spec.min_ready_seconds = Some(60);
        let pod = ranked_pod(
            "p",
            &rs,
            "n1",
            Phase::Running,
            Some(chrono::Utc::now() - chrono::Duration::seconds(10)),
        );
        seed_rs(&storage, &rs, &[pod]).await;

        let next = controller
            .reconcile_replicaset(&rs)
            .await
            .unwrap()
            .expect("an AddAfter must be planned");

        assert!(
            next > Duration::from_secs(48) && next <= Duration::from_secs(50),
            "~50s left until the pod is available, got {next:?}"
        );
    }

    /// With `DeploymentReplicaSetTerminatingReplicas` on, `calculateStatus`
    /// reports the claimed terminating pods (replica_set_utils.go:119-122,
    /// replica_set.go:748-753).
    #[tokio::test]
    #[serial_test::serial]
    async fn rs_reports_terminating_replicas_when_the_gate_is_on() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _gate = with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, true);
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("term", make_labels(&[("app", "term")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let live = ranked_pod("live", &rs, "n1", Phase::Running, Some(ago));
        let mut dying = ranked_pod("dying", &rs, "n1", Phase::Running, Some(ago));
        dying.metadata.deletion_timestamp = Some(chrono::Utc::now());
        seed_rs(&storage, &rs, &[live, dying]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let status = status_of(&storage, "term").await;
        assert_eq!(status.replicas, 1);
        assert_eq!(status.terminating_replicas, Some(1));
    }

    /// Gate off: the field stays unset (replica_set_utils.go:119).
    #[tokio::test]
    #[serial_test::serial]
    async fn rs_leaves_terminating_replicas_unset_when_the_gate_is_off() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let _gate = with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, false);
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let rs = make_replicaset("termoff", make_labels(&[("app", "termoff")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let live = ranked_pod("live", &rs, "n1", Phase::Running, Some(ago));
        let mut dying = ranked_pod("dying", &rs, "n1", Phase::Running, Some(ago));
        dying.metadata.deletion_timestamp = Some(chrono::Utc::now());
        seed_rs(&storage, &rs, &[live, dying]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(
            status_of(&storage, "termoff").await.terminating_replicas,
            None
        );
    }

    // ---- ports of the pure helpers' upstream tests (#3624) ----

    fn names(pods: &[&Pod]) -> Vec<String> {
        pods.iter().map(|p| p.metadata.name.clone()).collect()
    }

    /// `TestGetPodsToDelete` (replica_set_test.go:1942-2140).
    #[test]
    fn get_pods_to_delete_follows_upstream_table() {
        let rs = make_replicaset("foo", make_labels(&[("name", "foo")]), 1);
        let ago = chrono::Utc::now() - chrono::Duration::seconds(300);
        let unscheduled_pending =
            ranked_pod("unscheduled-pending-pod", &rs, "", Phase::Pending, None);
        let scheduled_pending = ranked_pod(
            "scheduled-pending-pod",
            &rs,
            "fake-node",
            Phase::Pending,
            None,
        );
        let running_not_ready = ranked_pod(
            "scheduled-running-not-ready-pod",
            &rs,
            "fake-node",
            Phase::Running,
            None,
        );
        let ready_node1 = ranked_pod(
            "scheduled-running-ready-pod-on-node-1",
            &rs,
            "fake-node-1",
            Phase::Running,
            Some(ago),
        );
        let ready_node2 = ranked_pod(
            "scheduled-running-ready-pod-on-node-2",
            &rs,
            "fake-node-2",
            Phase::Running,
            Some(ago),
        );

        struct Case<'a> {
            name: &'a str,
            pods: Vec<&'a Pod>,
            // defaults to `pods` when None
            related: Option<Vec<&'a Pod>>,
            diff: usize,
            expected: Vec<&'a Pod>,
        }
        let cases = vec![
            Case {
                name: "len(pods) = 0 (i.e., diff = 0 too)",
                pods: vec![],
                related: None,
                diff: 0,
                expected: vec![],
            },
            Case {
                name: "diff = len(pods)",
                pods: vec![&running_not_ready, &ready_node1],
                related: None,
                diff: 2,
                expected: vec![&running_not_ready, &ready_node1],
            },
            Case {
                name: "diff < len(pods)",
                pods: vec![&ready_node1, &running_not_ready],
                related: None,
                diff: 1,
                expected: vec![&running_not_ready],
            },
            Case {
                name: "various pod phases and conditions, diff = len(pods)",
                pods: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
                related: None,
                diff: 6,
                expected: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
            },
            Case {
                name: "various pod phases and conditions, diff = len(pods), relatedPods empty",
                pods: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
                related: Some(vec![]),
                diff: 6,
                expected: vec![
                    &ready_node1,
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
            },
            Case {
                name: "scheduled vs unscheduled, diff < len(pods)",
                pods: vec![&scheduled_pending, &unscheduled_pending],
                related: None,
                diff: 1,
                expected: vec![&unscheduled_pending],
            },
            Case {
                name: "ready vs not-ready, diff < len(pods)",
                pods: vec![&ready_node1, &running_not_ready, &running_not_ready],
                related: None,
                diff: 2,
                expected: vec![&running_not_ready, &running_not_ready],
            },
            Case {
                name:
                    "ready and colocated with another ready pod vs not colocated, diff < len(pods)",
                pods: vec![&ready_node1, &ready_node2],
                related: Some(vec![&ready_node1, &ready_node2, &ready_node2]),
                diff: 1,
                expected: vec![&ready_node2],
            },
            Case {
                name: "pending vs running, diff < len(pods)",
                pods: vec![&scheduled_pending, &running_not_ready],
                related: None,
                diff: 1,
                expected: vec![&scheduled_pending],
            },
            Case {
                name: "various pod phases and conditions, diff < len(pods)",
                pods: vec![
                    &ready_node1,
                    &ready_node2,
                    &running_not_ready,
                    &scheduled_pending,
                    &unscheduled_pending,
                ],
                related: None,
                diff: 3,
                expected: vec![&unscheduled_pending, &scheduled_pending, &running_not_ready],
            },
        ];
        for case in cases {
            let related = case.related.clone().unwrap_or_else(|| case.pods.clone());
            let got = get_pods_to_delete(&case.pods, &related, case.diff);
            assert_eq!(
                names(&got),
                names(&case.expected),
                "{}: unexpected pods to delete",
                case.name
            );
        }
    }

    /// `TestRelatedPodsLookup` (replica_set_test.go:552-625).
    #[test]
    fn related_pods_lookup_follows_upstream_table() {
        let owned_by = |mut rs: ReplicaSet, uid: &str| {
            rs.metadata.owner_references = Some(vec![OwnerReference::new(
                "apps/v1",
                "Deployment",
                "owner",
                uid,
            )
            .with_controller(true)]);
            rs
        };
        let some_rs = owned_by(
            make_replicaset("foo1", make_labels(&[("foo", "bar")]), 1),
            "123",
        );
        let related_rs = owned_by(
            make_replicaset("foo2", make_labels(&[("foo", "baz")]), 1),
            "123",
        );
        let unrelated_rs = owned_by(
            make_replicaset("bar1", make_labels(&[("foo", "quux")]), 1),
            "456",
        );
        let mut pending_deletion_rs = owned_by(
            make_replicaset("foo3", make_labels(&[("foo", "xyzzy")]), 1),
            "789",
        );
        pending_deletion_rs.metadata.deletion_timestamp = Some(chrono::Utc::now());

        let pod1 = ranked_pod("pod1", &some_rs, "", Phase::Running, None);
        let pod2 = ranked_pod("pod2", &some_rs, "", Phase::Running, None);
        let pod3 = ranked_pod("pod3", &related_rs, "", Phase::Running, None);
        let pod4 = ranked_pod("pod4", &unrelated_rs, "", Phase::Running, None);
        let pending_pod = ranked_pod("pod", &pending_deletion_rs, "", Phase::Running, None);

        let related_names = |rs: &ReplicaSet, all: &[&ReplicaSet], pods: &[Pod]| {
            let sets: Vec<RelatedSet<'_>> = all
                .iter()
                .map(|r| RelatedSet {
                    metadata: &r.metadata,
                    selector: r.spec.selector.clone(),
                })
                .collect();
            let mut got: Vec<String> = indirectly_related_pods(&rs.metadata, &sets, pods)
                .iter()
                .map(|p| p.metadata.name.clone())
                .collect();
            got.sort();
            got
        };

        assert_eq!(
            related_names(
                &pending_deletion_rs,
                &[&pending_deletion_rs, &unrelated_rs],
                std::slice::from_ref(&pending_pod)
            ),
            vec!["pod"],
            "a pod is related even if its owning ReplicaSet is pending deletion"
        );
        assert_eq!(
            related_names(
                &some_rs,
                &[&some_rs, &unrelated_rs],
                &[pod1.clone(), pod2.clone(), pod4.clone()]
            ),
            vec!["pod1", "pod2"],
            "only the ReplicaSet's own pods when there is no related ReplicaSet"
        );
        assert_eq!(
            related_names(
                &some_rs,
                &[&some_rs, &related_rs, &unrelated_rs],
                &[pod1, pod2, pod3, pod4]
            ),
            vec!["pod1", "pod2", "pod3"],
            "own pods plus any related ReplicaSet's, never an unrelated one's"
        );

        // `getReplicaSetsWithSameController`: no controller, no related sets.
        let orphan = make_replicaset("lonely", make_labels(&[("foo", "bar")]), 1);
        assert!(related_names(
            &orphan,
            &[&orphan],
            &[ranked_pod("p", &orphan, "", Phase::Running, None)]
        )
        .is_empty());
    }

    /// `TestCalculateStatus` (replica_set_utils_test.go:38-330).
    #[test]
    #[serial_test::serial]
    fn calculate_status_follows_upstream_table() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        // Template labels are always the full map (`newReplicaSet`); the
        // pods carry the selector's labels (`newPod`).
        let rs_with = |replicas: i32, selector: &[(&str, &str)], min_ready: i32| {
            let mut rs = make_replicaset("foobar", make_labels(selector), replicas);
            rs.spec.template.metadata = Some(
                ObjectMeta::new("")
                    .with_labels(make_labels(&[("name", "foo"), ("type", "production")])),
            );
            rs.spec.min_ready_seconds = Some(min_ready);
            rs
        };
        let not_fully = rs_with(1, &[("name", "foo")], 0);
        let fully = rs_with(2, &[("name", "foo"), ("type", "production")], 0);
        let long_min_ready = rs_with(1, &[("name", "foo"), ("type", "production")], 3600);
        let now = chrono::Utc::now();
        let ready = |name: &str, rs: &ReplicaSet, since: Option<chrono::DateTime<chrono::Utc>>| {
            ranked_pod(name, rs, "", Phase::Running, Some(since.unwrap_or(now)))
        };
        let terminating = |mut p: Pod| {
            p.metadata.deletion_timestamp = Some(now);
            p
        };

        struct Case {
            name: &'static str,
            gate: bool,
            rs: ReplicaSet,
            active: Vec<Pod>,
            terminating: Vec<Pod>,
            // replicas, fully labeled, ready, available, terminating
            expected: (i32, i32, i32, i32, Option<i32>),
        }
        let cases = vec![
            Case {
                name: "1 fully labelled pod",
                gate: false,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None)],
                terminating: vec![],
                expected: (1, 1, 1, 1, None),
            },
            Case {
                name: "1 not fully labelled pod",
                gate: false,
                rs: not_fully.clone(),
                active: vec![ready("pod1", &not_fully, None)],
                terminating: vec![],
                expected: (1, 0, 1, 1, None),
            },
            Case {
                name: "2 fully labelled pods with DeploymentReplicaSetTerminatingReplicas",
                gate: true,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None), ready("pod2", &fully, None)],
                terminating: vec![],
                expected: (2, 2, 2, 2, Some(0)),
            },
            Case {
                name: "1 fully labelled pod, 1 not fully labelled pod",
                gate: false,
                rs: not_fully.clone(),
                active: vec![ready("pod1", &not_fully, None), ready("pod2", &fully, None)],
                terminating: vec![],
                expected: (2, 1, 2, 2, None),
            },
            Case {
                name: "1 non-ready pod",
                gate: false,
                rs: fully.clone(),
                active: vec![ranked_pod("pod1", &fully, "", Phase::Pending, None)],
                terminating: vec![],
                expected: (1, 1, 0, 0, None),
            },
            Case {
                name: "1 ready but non-available pod",
                gate: false,
                rs: long_min_ready.clone(),
                active: vec![ready("pod1", &long_min_ready, Some(now))],
                terminating: vec![],
                expected: (1, 1, 1, 0, None),
            },
            Case {
                name: "1 available pod with minReadySeconds",
                gate: false,
                rs: long_min_ready.clone(),
                active: vec![ready(
                    "pod1",
                    &long_min_ready,
                    Some(now - chrono::Duration::seconds(3600)),
                )],
                terminating: vec![],
                expected: (1, 1, 1, 1, None),
            },
            Case {
                name: "1 fully labelled pod and 1 terminating without the gate",
                gate: false,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None)],
                terminating: vec![terminating(ready("pod2", &fully, None))],
                expected: (1, 1, 1, 1, None),
            },
            Case {
                name: "1 fully labelled pod and 2 terminating with the gate",
                gate: true,
                rs: fully.clone(),
                active: vec![ready("pod1", &fully, None)],
                terminating: vec![
                    terminating(ready("pod2", &fully, None)),
                    terminating(ready("pod3", &fully, None)),
                ],
                expected: (1, 1, 1, 1, Some(2)),
            },
        ];
        for case in cases {
            let _gate = with_feature(Feature::DeploymentReplicaSetTerminatingReplicas, case.gate);
            let active: Vec<&Pod> = case.active.iter().collect();
            let terminating: Vec<&Pod> = case.terminating.iter().collect();
            let status = calculate_status(&case.rs, &active, &terminating, None, now);
            assert_eq!(
                (
                    status.replicas,
                    status.fully_labeled_replicas,
                    status.ready_replicas,
                    status.available_replicas,
                    status.terminating_replicas
                ),
                (
                    case.expected.0,
                    Some(case.expected.1),
                    case.expected.2,
                    case.expected.3,
                    case.expected.4
                ),
                "{}",
                case.name
            );
        }
    }

    /// `TestCalculateStatusConditions` (replica_set_utils_test.go:332-440):
    /// set once with the diff's reason, kept while it persists, removed when
    /// the error clears.
    #[test]
    fn calculate_status_conditions_follow_upstream_table() {
        let now = chrono::Utc::now();
        let rs = make_replicaset("foobar", make_labels(&[("name", "foo")]), 2);
        let mut failing_rs = make_replicaset("foobar", make_labels(&[("name", "foo")]), 10);
        failing_rs.status = Some(ReplicaSetStatus {
            conditions: Some(vec![rusternetes_common::resources::ReplicaSetCondition {
                condition_type: "ReplicaFailure".to_string(),
                status: "True".to_string(),
                last_transition_time: None,
                reason: None,
                message: None,
            }]),
            ..Default::default()
        });
        let pods = |rs: &ReplicaSet, n: usize| -> Vec<Pod> {
            (0..n)
                .map(|i| ranked_pod(&format!("pod{i}"), rs, "", Phase::Running, Some(now)))
                .collect()
        };
        let conds = |rs: &ReplicaSet, n: usize, err: Option<&str>| {
            let owned = pods(rs, n);
            let refs: Vec<&Pod> = owned.iter().collect();
            calculate_status(rs, &refs, &[], err, now)
                .conditions
                .unwrap_or_default()
        };

        let c = conds(&rs, 1, Some("fake manageReplicasErr"));
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].reason.as_deref(), Some("FailedCreate"), "diff < 0");
        assert_eq!(c[0].message.as_deref(), Some("fake manageReplicasErr"));

        let c = conds(&rs, 3, Some("fake manageReplicasErr"));
        assert_eq!(c[0].reason.as_deref(), Some("FailedDelete"), "diff > 0");

        assert!(
            conds(&failing_rs, 1, None).is_empty(),
            "manageReplicasErr == nil && failureCond != nil removes it"
        );

        let c = conds(&failing_rs, 1, Some("fake manageReplicasErr"));
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].reason, None, "the existing condition is left alone");
        assert_eq!(c[0].message, None);

        assert!(conds(&rs, 1, None).is_empty());
    }

    /// The informer's re-list on (re)connect (`DeltaFIFO.Replace`): a pod
    /// created while the watch was down is observed as an add, exactly once,
    /// so a creation whose Added event was lost does not wedge the sync until
    /// the expectations TTL.
    #[tokio::test]
    async fn rs_relist_observes_creations_the_watch_missed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("gap", make_labels(&[("app", "gap")]), 2);
        let pods = [
            ranked_pod("p1", &rs, "n1", Phase::Pending, None),
            ranked_pod("p2", &rs, "n1", Phase::Pending, None),
        ];
        seed_rs(&storage, &rs, &pods).await;
        // The sync issued two creates; their Added events never arrived.
        controller.set_expectations("default/gap", 2, 0);

        controller.relist_pods(&queue).await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((0, 0))
        );

        // A second re-list, and a late Added event for a pod already seen,
        // are updates: they must not lower the count again.
        controller.set_expectations("default/gap", 1, 0);
        controller.relist_pods(&queue).await;
        controller
            .enqueue_owner_replicaset(
                &queue,
                &WatchEvent::Added(
                    "/registry/pods/default/p1".to_string(),
                    serde_json::to_string(&pods[0]).unwrap(),
                ),
            )
            .await;
        assert_eq!(
            controller.expectations.get_expectations("default/gap"),
            Some((1, 0)),
            "a pod the informer already holds is not a creation observation"
        );
    }

    /// Pods that vanished while the watch was down are observed as deletes
    /// (`DeletedFinalStateUnknown`), so the delete expectation clears.
    #[tokio::test]
    async fn rs_relist_observes_deletions_the_watch_missed() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("gone", make_labels(&[("app", "gone")]), 1);
        let pods = [
            ranked_pod("keep", &rs, "n1", Phase::Pending, None),
            ranked_pod("doomed", &rs, "n1", Phase::Pending, None),
        ];
        seed_rs(&storage, &rs, &pods).await;
        controller.relist_pods(&queue).await;

        controller
            .expectations
            .expect_deletions_of("default/gone", &["default/doomed".to_string()]);
        storage
            .delete(&build_key("pods", Some("default"), "doomed"))
            .await
            .unwrap();
        controller.relist_pods(&queue).await;

        assert_eq!(
            controller.expectations.get_expectations("default/gone"),
            Some((0, 0))
        );
    }

    // ---------------------------------------------------------------------
    // #3633 / #3634: selector semantics and informer event handlers.
    //
    // Ported from upstream pkg/controller/replicaset/replica_set_test.go
    // (TestPodControllerLookup :486, TestUpdatePods :731) and
    // replica_set.go addPod/updatePod/deletePod :415-570 +
    // `metav1.LabelSelectorAsSelector`.
    // ---------------------------------------------------------------------

    pub(crate) fn expr(
        key: &str,
        op: &str,
        values: &[&str],
    ) -> rusternetes_common::types::LabelSelector {
        rusternetes_common::types::LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![rusternetes_common::types::LabelSelectorRequirement {
                key: key.to_string(),
                operator: op.to_string(),
                values: if values.is_empty() {
                    None
                } else {
                    Some(values.iter().map(|v| v.to_string()).collect())
                },
            }]),
        }
    }

    /// Everything currently queued, sorted.
    pub(crate) async fn queued(queue: &WorkQueue) -> Vec<String> {
        let mut keys = Vec::new();
        while queue.len().await > 0 {
            let key = queue.get().await.unwrap();
            queue.done(&key).await;
            keys.push(key);
        }
        keys.sort();
        keys
    }

    pub(crate) fn controlled(
        mut pod: Pod,
        kind: &str,
        name: &str,
        uid: &str,
        controller: bool,
    ) -> Pod {
        pod.metadata.owner_references = Some(vec![OwnerReference {
            api_version: "apps/v1".to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
            controller: Some(controller),
            block_owner_deletion: None,
        }]);
        pod
    }

    pub(crate) fn pod_event(
        make: fn(String, String) -> WatchEvent,
        pod: &Pod,
        rv: &str,
    ) -> WatchEvent {
        let mut pod = pod.clone();
        pod.metadata.resource_version = Some(rv.to_string());
        make(
            format!("/registry/pods/default/{}", pod.metadata.name),
            serde_json::to_string(&pod).unwrap(),
        )
    }

    async fn put_rs(storage: &Arc<MemoryStorage>, rs: &ReplicaSet) {
        storage
            .create(
                &build_key("replicasets", Some("default"), &rs.metadata.name),
                rs,
            )
            .await
            .unwrap();
    }

    /// `claimPods` reads the selector through `LabelSelectorAsSelector`, so a
    /// matchExpressions selector adopts a matching orphan.
    #[tokio::test]
    async fn rs_match_expressions_selector_adopts_orphan() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("expr", make_labels(&[("app", "a")]), 1);
        rs.spec.selector = expr("app", "In", &["a", "b"]);
        let orphan = make_pod("orphan", make_labels(&[("app", "a")]));
        seed_rs(&storage, &rs, &[orphan]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert_eq!(pods.len(), 1, "the orphan fills the one replica");
        let adopted = &pods[0].metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(adopted.uid, rs.metadata.uid);
    }

    /// A pod this RS controls whose labels satisfy a matchExpressions selector
    /// stays claimed, and one that does not is released
    /// (`ControllerRefManager.ClaimObject`).
    #[tokio::test]
    async fn rs_match_expressions_selector_keeps_matching_and_releases_unmatched() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("expr", make_labels(&[("app", "a")]), 1);
        rs.spec.selector = expr("tier", "In", &["db"]);
        let uid = rs.metadata.uid.clone();
        let kept = owned_pod("kept", make_labels(&[("tier", "db")]), "expr", &uid);
        let dropped = owned_pod("dropped", make_labels(&[("tier", "web")]), "expr", &uid);
        seed_rs(&storage, &rs, &[kept, dropped]).await;

        controller.reconcile_replicaset(&rs).await.unwrap();

        let owned_by_rs = |p: &Pod| {
            p.metadata
                .owner_references
                .as_ref()
                .is_some_and(|refs| refs.iter().any(|r| r.uid == uid))
        };
        let kept: Pod = storage
            .get(&build_key("pods", Some("default"), "kept"))
            .await
            .unwrap();
        let dropped: Pod = storage
            .get(&build_key("pods", Some("default"), "dropped"))
            .await
            .unwrap();
        assert!(owned_by_rs(&kept), "tier In [db] still matches");
        assert!(!owned_by_rs(&dropped), "tier=web no longer matches");
    }

    /// `syncReplicaSet`: "error converting pod selector to selector" is
    /// logged and the sync returns nil (replica_set.go:729-733). No pod is
    /// created or claimed.
    #[tokio::test]
    async fn rs_invalid_selector_skips_the_sync() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let mut rs = make_replicaset("bad", make_labels(&[("app", "a")]), 2);
        rs.spec.selector = expr("app", "Bogus", &["a"]);
        seed_rs(&storage, &rs, &[]).await;

        let next = controller.reconcile_replicaset(&rs).await.unwrap();

        assert_eq!(next, None);
        let pods: Vec<Pod> = storage
            .list(&build_prefix("pods", Some("default")))
            .await
            .unwrap();
        assert!(pods.is_empty(), "an invalid selector syncs nothing");
    }

    /// `addPod`: "If it has a ControllerRef, that's all that matters"
    /// (replica_set.go:426-440). `GetControllerOf` returns only the
    /// `controller: true` ref, so a plain owner reference is an orphan.
    #[tokio::test]
    async fn rs_pod_event_ignores_a_non_controller_owner_reference() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("g", make_labels(&[("app", "g")]), 1);
        put_rs(&storage, &rs).await;
        controller.set_expectations("default/g", 1, 0);

        // A pod with labels the RS does not select and a non-controller ref.
        let pod = controlled(
            make_pod("p", make_labels(&[("other", "x")])),
            "ReplicaSet",
            "g",
            &rs.metadata.uid,
            false,
        );
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &pod, "1"))
            .await;

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0)),
            "a creation is observed only for the controller reference"
        );
    }

    /// `resolveControllerRef` (replica_set.go:302-321): wrong kind, a missing
    /// RS and a UID mismatch all resolve to nil, and nothing is enqueued or
    /// observed.
    #[tokio::test]
    async fn rs_pod_event_unresolvable_controller_ref_does_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs = make_replicaset("g", make_labels(&[("app", "g")]), 1);
        put_rs(&storage, &rs).await;
        controller.set_expectations("default/g", 1, 0);
        let labels = make_labels(&[("app", "g")]);

        for (i, (kind, name, uid)) in [
            ("Deployment", "g", rs.metadata.uid.as_str()),
            ("ReplicaSet", "missing", rs.metadata.uid.as_str()),
            ("ReplicaSet", "g", "recreated-uid"),
        ]
        .into_iter()
        .enumerate()
        {
            let pod = controlled(
                make_pod(&format!("p{i}"), labels.clone()),
                kind,
                name,
                uid,
                true,
            );
            controller
                .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &pod, "1"))
                .await;
        }

        assert!(queued(&queue).await.is_empty());
        assert_eq!(
            controller.expectations.get_expectations("default/g"),
            Some((1, 0))
        );
    }

    /// `addPod` of an orphan syncs every RS whose selector matches it
    /// (`getPodReplicaSets`, replica_set.go:289-300; the lister skips a pod
    /// without labels and an empty selector) — not every RS in the namespace —
    /// and observes no creation (:441-453). `TestPodControllerLookup`.
    #[tokio::test]
    async fn rs_orphan_pod_added_enqueues_only_matching_replicasets() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let by_labels = make_replicaset("by-labels", make_labels(&[("app", "a")]), 1);
        let mut by_expr = make_replicaset("by-expr", make_labels(&[("app", "a")]), 1);
        by_expr.spec.selector = expr("app", "In", &["a"]);
        let other = make_replicaset("other", make_labels(&[("app", "z")]), 1);
        let mut invalid = make_replicaset("invalid", make_labels(&[("app", "a")]), 1);
        invalid.spec.selector = expr("app", "Bogus", &["a"]);
        let mut empty = make_replicaset("empty", make_labels(&[("app", "a")]), 1);
        empty.spec.selector = rusternetes_common::types::LabelSelector {
            match_labels: None,
            match_expressions: None,
        };
        for rs in [&by_labels, &by_expr, &other, &invalid, &empty] {
            put_rs(&storage, rs).await;
        }
        controller.set_expectations("default/by-labels", 1, 0);

        let orphan = make_pod("orphan", make_labels(&[("app", "a")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &orphan, "1"))
            .await;

        assert_eq!(
            queued(&queue).await,
            vec![
                "replicasets/default/by-expr",
                "replicasets/default/by-labels"
            ]
        );
        assert_eq!(
            controller
                .expectations
                .get_expectations("default/by-labels"),
            Some((1, 0)),
            "no controller waits for an orphan"
        );

        // "pods without labels don't match any ReplicaSets".
        let unlabeled = make_pod("unlabeled", HashMap::new());
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &unlabeled, "1"))
            .await;
        assert!(queued(&queue).await.is_empty());
    }

    /// `deletePod`: "No controller should care about orphans being deleted"
    /// (replica_set.go:553-557).
    #[tokio::test]
    async fn rs_orphan_pod_deleted_enqueues_nothing() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        put_rs(
            &storage,
            &make_replicaset("a", make_labels(&[("app", "a")]), 1),
        )
        .await;
        let orphan = make_pod("orphan", make_labels(&[("app", "a")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &orphan, "1"))
            .await;
        queued(&queue).await;

        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Deleted, &orphan, "2"))
            .await;

        assert!(queued(&queue).await.is_empty());
    }

    /// `TestUpdatePods` (replica_set_test.go:731): case 1, a label change on a
    /// controlled pod syncs the owner only; case 3, dropping the ControllerRef
    /// syncs the former owner and any label-matching RS; an update that changes
    /// nothing about an orphan syncs nobody (:517-528).
    #[tokio::test]
    async fn rs_update_pod_follows_the_controller_ref() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ReplicaSetController::new(Arc::clone(&storage), 30);
        let queue = WorkQueue::new();
        let rs1 = make_replicaset("foo", make_labels(&[("foo", "bar")]), 1);
        let rs2 = make_replicaset("barfoo", make_labels(&[("bar", "foo")]), 1);
        put_rs(&storage, &rs1).await;
        put_rs(&storage, &rs2).await;

        // Case 1: owned by rs1, labels change to rs2's: only the owner wakes.
        let owned = controlled(
            make_pod("p", make_labels(&[("foo", "bar")])),
            "ReplicaSet",
            "foo",
            &rs1.metadata.uid,
            true,
        );
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Added, &owned, "1"))
            .await;
        queued(&queue).await;
        let mut relabeled = owned.clone();
        relabeled.metadata.labels = Some(make_labels(&[("bar", "foo")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &relabeled, "2"))
            .await;
        assert_eq!(queued(&queue).await, vec!["replicasets/default/foo"]);

        // Case 3: the ControllerRef is removed: former owner + matching RS.
        let mut orphaned = relabeled.clone();
        orphaned.metadata.owner_references = None;
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &orphaned, "3"))
            .await;
        assert_eq!(
            queued(&queue).await,
            vec!["replicasets/default/barfoo", "replicasets/default/foo"]
        );

        // An orphan whose labels and ref did not change wakes nobody.
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &orphaned, "4"))
            .await;
        assert!(queued(&queue).await.is_empty());

        // An orphan that changes labels wakes the RSs matching the new ones.
        let mut moved = orphaned.clone();
        moved.metadata.labels = Some(make_labels(&[("foo", "bar")]));
        controller
            .enqueue_owner_replicaset(&queue, &pod_event(WatchEvent::Modified, &moved, "5"))
            .await;
        assert_eq!(queued(&queue).await, vec!["replicasets/default/foo"]);
    }

    /// The worker wires `nextSyncDuration` to `queue.AddAfter` after `Forget`
    /// (replica_set.go:786-788): a pod that is ready but not yet available is
    /// re-checked by a second sync when `minReadySeconds` has passed, with no
    /// watch event to wake the key. (`WorkQueue` times on `std::time::Instant`,
    /// so a paused tokio clock cannot drive it; the delay here is real and 1s.)
    #[tokio::test]
    async fn rs_worker_requeues_the_key_after_the_next_sync_duration() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = Arc::new(ReplicaSetController::new(Arc::clone(&storage), 30));
        let mut rs = make_replicaset("avail-later", make_labels(&[("app", "al")]), 1);
        rs.spec.min_ready_seconds = Some(1);
        let pod = ranked_pod("p", &rs, "n1", Phase::Running, Some(chrono::Utc::now()));
        seed_rs(&storage, &rs, &[pod]).await;

        let queue = WorkQueue::new();
        let worker = tokio::spawn({
            let controller = Arc::clone(&controller);
            let queue = queue.clone();
            async move { controller.worker(queue).await }
        });
        queue
            .add("replicasets/default/avail-later".to_string())
            .await;

        let available = async {
            loop {
                let rs: ReplicaSet = storage
                    .get(&build_key("replicasets", Some("default"), "avail-later"))
                    .await
                    .unwrap();
                if rs.status.is_some_and(|s| s.available_replicas == 1) {
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
