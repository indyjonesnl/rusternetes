//! Unit tests moved from `src/controllers/statefulset.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::{
        PersistentVolumeClaim, Pod, PodStatus, StatefulSet, StatefulSetStatus,
    };
    pub use rusternetes_common::types::{
        label_selector_as_selector, ObjectMeta, OwnerReference, Phase, Selector, TypeMeta,
    };
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
    use rusternetes_common::resources::workloads::{
        RollingUpdateStatefulSetStrategy, StatefulSetUpdateStrategy,
    };
    use rusternetes_common::resources::{
        Container, PodCondition, PodSpec, PodTemplateSpec, StatefulSetSpec,
    };
    use rusternetes_common::types::LabelSelector;
    use rusternetes_controller_manager::controllers::statefulset::*;
    use rusternetes_storage::MemoryStorage;
    use std::collections::HashMap;

    #[test]
    fn test_pod_name_generation() {
        let statefulset_name = "web";
        let ordinal = 2;
        let pod_name = format!("{}-{}", statefulset_name, ordinal);
        assert_eq!(pod_name, "web-2");
    }

    #[test]
    fn test_pod_ordinal_parsing() {
        let pod_name = "web-5";
        let ordinal: i32 = pod_name
            .rsplit_once('-')
            .and_then(|(_, idx)| idx.parse().ok())
            .unwrap();
        assert_eq!(ordinal, 5);
    }

    /// Verify compute_revision is deterministic even with HashMap-backed labels
    #[test]
    fn test_compute_revision_deterministic() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());
        labels.insert("version".to_string(), "v1".to_string());
        labels.insert("tier".to_string(), "frontend".to_string());

        let template = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
            spec: PodSpec {
                containers: vec![test_container("nginx", "nginx:1.19")],
                ..Default::default()
            },
        };

        // Compute multiple times — must always produce the same result
        let rev1 = StatefulSetController::<MemoryStorage>::compute_revision(&template);
        let rev2 = StatefulSetController::<MemoryStorage>::compute_revision(&template);
        let rev3 = StatefulSetController::<MemoryStorage>::compute_revision(&template);
        assert_eq!(rev1, rev2, "Revision should be deterministic across calls");
        assert_eq!(rev2, rev3, "Revision should be deterministic across calls");
    }

    /// Verify compute_revision changes when image changes
    #[test]
    fn test_compute_revision_changes_on_image_change() {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "web".to_string());

        let template_v1 = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
            spec: PodSpec {
                containers: vec![test_container("nginx", "nginx:1.19")],
                ..Default::default()
            },
        };

        let template_v2 = PodTemplateSpec {
            metadata: Some(ObjectMeta::new("").with_labels(labels.clone())),
            spec: PodSpec {
                containers: vec![test_container("nginx", "nginx:1.20")],
                ..Default::default()
            },
        };

        let rev1 = StatefulSetController::<MemoryStorage>::compute_revision(&template_v1);
        let rev2 = StatefulSetController::<MemoryStorage>::compute_revision(&template_v2);
        assert_ne!(rev1, rev2, "Revision should change when image changes");
    }

    fn test_container(name: &str, image: &str) -> Container {
        Container {
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

    fn make_statefulset(name: &str, namespace: &str, replicas: i32, image: &str) -> StatefulSet {
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), name.to_string());

        StatefulSet {
            type_meta: TypeMeta {
                kind: "StatefulSet".to_string(),
                api_version: "apps/v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace.to_string()),
            spec: StatefulSetSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    match_expressions: None,
                },
                service_name: format!("{}-svc", name),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta::new("").with_labels(labels)),
                    spec: PodSpec {
                        containers: vec![test_container("main", image)],
                        ..Default::default()
                    },
                },
                update_strategy: None,
                pod_management_policy: Some("Parallel".to_string()),
                min_ready_seconds: None,
                revision_history_limit: None,
                volume_claim_templates: None,
                persistent_volume_claim_retention_policy: None,
                ordinals: None,
            },
            status: None,
        }
    }

    /// A volumeClaimTemplate PVC without an explicit storageClassName must
    /// inherit the cluster's default StorageClass; an explicit one is preserved.
    /// Regression guard: the StatefulSet controller writes these PVCs straight
    /// to storage, bypassing the api-server's DefaultStorageClass admission, so
    /// without in-controller defaulting the PVC never binds and the pod mounts
    /// nothing (pods never become Ready).
    #[tokio::test]
    async fn test_volumeclaimtemplate_pvc_inherits_default_storage_class() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";

        // Default StorageClass present in the cluster.
        let sc: rusternetes_common::resources::StorageClass =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "standard",
                    "annotations": {"storageclass.kubernetes.io/is-default-class": "true"}
                },
                "provisioner": "rusternetes.io/hostpath"
            }))
            .unwrap();
        storage
            .create("/registry/storageclasses/standard", &sc)
            .await
            .unwrap();

        // Two templates: one without a storageClassName, one with an explicit class.
        let mut ss = make_statefulset("web", ns, 1, "nginx:1.19");
        let tmpl = |name: &str, sc: Option<&str>| -> PersistentVolumeClaim {
            let mut spec = serde_json::json!({
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": "1Mi"}}
            });
            if let Some(s) = sc {
                spec["storageClassName"] = serde_json::json!(s);
            }
            serde_json::from_value(serde_json::json!({
                "metadata": {"name": name},
                "spec": spec
            }))
            .unwrap()
        };
        ss.spec.volume_claim_templates = Some(vec![tmpl("data", None), tmpl("fast", Some("ssd"))]);
        let key = format!("/registry/statefulsets/{}/web", ns);
        storage.create(&key, &ss).await.unwrap();

        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        let data_pvc: PersistentVolumeClaim = storage
            .get(&format!(
                "/registry/persistentvolumeclaims/{}/data-web-0",
                ns
            ))
            .await
            .expect("PVC data-web-0 created");
        assert_eq!(
            data_pvc.spec.storage_class_name.as_deref(),
            Some("standard"),
            "unset storageClassName must inherit the default StorageClass"
        );

        let fast_pvc: PersistentVolumeClaim = storage
            .get(&format!(
                "/registry/persistentvolumeclaims/{}/fast-web-0",
                ns
            ))
            .await
            .expect("PVC fast-web-0 created");
        assert_eq!(
            fast_pvc.spec.storage_class_name.as_deref(),
            Some("ssd"),
            "explicit storageClassName must be preserved, not overwritten"
        );

        // The pod must carry a persistentVolumeClaim volume per template, else
        // the container's volumeMount has no backing volume and nothing mounts.
        let pod: Pod = storage
            .get(&format!("/registry/pods/{}/web-0", ns))
            .await
            .expect("pod web-0 created");
        let volumes = pod.spec.unwrap().volumes.unwrap_or_default();
        let data_vol = volumes
            .iter()
            .find(|v| v.name == "data")
            .expect("pod must have a 'data' volume for the volumeClaimTemplate");
        assert_eq!(
            data_vol
                .persistent_volume_claim
                .as_ref()
                .map(|p| p.claim_name.as_str()),
            Some("data-web-0"),
            "'data' volume must reference PVC data-web-0"
        );
    }

    #[tokio::test]
    async fn test_sts_pods_inherit_sa_image_pull_secrets() {
        // #1084: SA imagePullSecrets must reach controller-created pods, which
        // bypass the api-server admission path.
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";

        let mut sa = rusternetes_common::resources::ServiceAccount::new("default", ns);
        sa.image_pull_secrets = Some(vec![
            rusternetes_common::resources::service_account::LocalObjectReference {
                name: "regcred".to_string(),
            },
        ]);
        storage
            .create("/registry/serviceaccounts/default/default", &sa)
            .await
            .unwrap();

        let ss = make_statefulset("web", ns, 1, "nginx:1.19");
        let key = format!("/registry/statefulsets/{}/web", ns);
        storage.create(&key, &ss).await.unwrap();

        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        let pod: Pod = storage
            .get(&format!("/registry/pods/{}/web-0", ns))
            .await
            .expect("pod web-0 created");
        let spec = pod.spec.unwrap();
        let secrets = spec
            .image_pull_secrets
            .as_ref()
            .expect("pod must inherit the SA's imagePullSecrets");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].name, "regcred");
    }

    /// Without a default StorageClass, an unset storageClassName stays unset
    /// (matching upstream: no default class => no defaulting).
    #[tokio::test]
    async fn test_volumeclaimtemplate_pvc_no_default_class_stays_unset() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";

        let mut ss = make_statefulset("web", ns, 1, "nginx:1.19");
        let tmpl: PersistentVolumeClaim = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "data"},
            "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Mi"}}}
        }))
        .unwrap();
        ss.spec.volume_claim_templates = Some(vec![tmpl]);
        let key = format!("/registry/statefulsets/{}/web", ns);
        storage.create(&key, &ss).await.unwrap();

        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        let pvc: PersistentVolumeClaim = storage
            .get(&format!(
                "/registry/persistentvolumeclaims/{}/data-web-0",
                ns
            ))
            .await
            .expect("PVC data-web-0 created");
        assert!(
            pvc.spec.storage_class_name.is_none(),
            "no default StorageClass => storageClassName stays unset"
        );
    }

    /// Make a pod look like it's Running and Ready
    async fn make_pod_ready(storage: &Arc<MemoryStorage>, namespace: &str, pod_name: &str) {
        let key = format!("/registry/pods/{}/{}", namespace, pod_name);
        if let Ok(mut pod) = storage.get::<Pod>(&key).await {
            pod.status = Some(PodStatus {
                phase: Some(Phase::Running),
                conditions: Some(vec![PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            });
            let _ = storage.update(&key, &pod).await;
        }
    }

    /// Simulate kubelet behavior: delete pods with deletionTimestamp from storage
    async fn simulate_kubelet_cleanup(storage: &Arc<MemoryStorage>, namespace: &str) {
        let prefix = format!("/registry/pods/{}/", namespace);
        let pods: Vec<Pod> = storage.list(&prefix).await.unwrap_or_default();
        for pod in pods {
            if pod.metadata.deletion_timestamp.is_some() {
                let key = format!("/registry/pods/{}/{}", namespace, pod.metadata.name);
                let _ = storage.delete(&key).await;
            }
        }
    }

    /// Drive reconcile cycles (readying pods + simulating kubelet cleanup) until
    /// the rollout converges (currentRevision == updateRevision and all replicas
    /// updated), or panic after a generous bound.
    async fn roll_to_completion(
        controller: &StatefulSetController<MemoryStorage>,
        storage: &Arc<MemoryStorage>,
        ns: &str,
        key: &str,
        name: &str,
        replicas: i32,
    ) {
        for cycle in 0..40 {
            simulate_kubelet_cleanup(storage, ns).await;
            for i in 0..replicas {
                let pod_name = format!("{}-{}", name, i);
                let pod_key = format!("/registry/pods/{}/{}", ns, pod_name);
                if storage.get::<Pod>(&pod_key).await.is_ok() {
                    make_pod_ready(storage, ns, &pod_name).await;
                }
            }
            let mut ss: StatefulSet = storage.get(key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();
            let ss: StatefulSet = storage.get(key).await.unwrap();
            let status = ss.status.as_ref().unwrap();
            if status.current_revision == status.update_revision
                && status.updated_replicas == Some(replicas)
            {
                return;
            }
            assert!(cycle < 39, "rollout did not converge");
        }
    }

    /// During a rolling update, currentRevision != updateRevision.
    /// After all pods are updated, currentRevision == updateRevision.
    #[tokio::test]
    async fn test_rolling_update_revision_tracking() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        let ss = make_statefulset("web", ns, 3, "nginx:1.19");

        // Store the StatefulSet
        let key = format!("/registry/statefulsets/{}/{}", ns, "web");
        storage.create(&key, &ss).await.unwrap();

        // First reconcile: creates 3 pods with revision hash for nginx:1.19
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Make all pods Running+Ready
        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("web-{}", i)).await;
        }

        // Reconcile again so status reflects ready pods
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Verify initial state: currentRevision == updateRevision
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        assert_eq!(
            status.current_revision, status.update_revision,
            "Before update: currentRevision should equal updateRevision"
        );
        let old_revision = status.current_revision.clone().unwrap();

        // Now patch the StatefulSet to use a new image (simulate a rolling update)
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();

        // Reconcile: should detect template change and begin rolling update
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Check that currentRevision != updateRevision during rolling update
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        let new_revision = status.update_revision.clone().unwrap();

        assert_ne!(
            old_revision, new_revision,
            "Update revision should differ from old revision after template change"
        );
        assert_ne!(
            status.current_revision, status.update_revision,
            "During rolling update: currentRevision should NOT equal updateRevision"
        );
        assert_eq!(
            status.current_revision.as_ref().unwrap(),
            &old_revision,
            "currentRevision should still be the old revision during rolling update"
        );

        // Now simulate completing the rolling update:
        // Each cycle: make pods ready → reconcile (deletes one old, creates one new)
        // Need enough cycles for all 3 pods to be replaced + a final reconcile
        for cycle in 0..20 {
            // Simulate kubelet: remove terminated pods from storage
            simulate_kubelet_cleanup(&storage, ns).await;

            // Make all current pods ready
            for i in 0..3 {
                let pod_name = format!("web-{}", i);
                let pod_key = format!("/registry/pods/{}/{}", ns, pod_name);
                if storage.get::<Pod>(&pod_key).await.is_ok() {
                    make_pod_ready(&storage, ns, &pod_name).await;
                }
            }

            let mut ss: StatefulSet = storage.get(&key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();

            let ss: StatefulSet = storage.get(&key).await.unwrap();
            let status = ss.status.as_ref().unwrap();
            if status.current_revision == status.update_revision {
                break;
            }
            assert!(cycle < 19, "Rolling update did not complete in 20 cycles");
        }

        // After rollout completes, currentRevision == updateRevision
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        assert_eq!(
            status.current_revision, status.update_revision,
            "After rollout completes: currentRevision should equal updateRevision"
        );
        assert_eq!(
            status.updated_replicas,
            Some(3),
            "All replicas should be updated"
        );
    }

    /// Reverting the template to a previous revision must roll pods back to that
    /// revision. Because the revision is a deterministic hash of the template,
    /// an identical template reproduces the original revision hash — upstream
    /// reuses the existing ControllerRevision rather than minting a new one.
    #[tokio::test]
    async fn test_rolling_update_rollback_to_prior_revision() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let key = format!("/registry/statefulsets/{}/web-rb", ns);
        storage
            .create(&key, &make_statefulset("web-rb", ns, 3, "nginx:1.19"))
            .await
            .unwrap();

        // Initial rollout at revA (nginx:1.19).
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        roll_to_completion(&controller, &storage, ns, &key, "web-rb", 3).await;
        let rev_a = storage
            .get::<StatefulSet>(&key)
            .await
            .unwrap()
            .status
            .unwrap()
            .current_revision
            .unwrap();

        // Update to revB (nginx:1.20) and roll out fully.
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        roll_to_completion(&controller, &storage, ns, &key, "web-rb", 3).await;
        let rev_b = storage
            .get::<StatefulSet>(&key)
            .await
            .unwrap()
            .status
            .unwrap()
            .current_revision
            .unwrap();
        assert_ne!(rev_a, rev_b, "the update must produce a distinct revision");

        // Roll BACK to nginx:1.19. The update revision must equal revA again
        // (reuse the prior revision hash), not a fresh third revision.
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.19".to_string();
        storage.update(&key, &ss).await.unwrap();
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        let rev_during_rollback = storage
            .get::<StatefulSet>(&key)
            .await
            .unwrap()
            .status
            .unwrap()
            .update_revision
            .unwrap();
        assert_eq!(
            rev_during_rollback, rev_a,
            "rollback must reuse the prior revision hash, not mint a new one"
        );

        // Complete the rollback: every pod is back on revA + nginx:1.19.
        roll_to_completion(&controller, &storage, ns, &key, "web-rb", 3).await;
        for i in 0..3 {
            let pod: Pod = storage
                .get(&format!("/registry/pods/{}/web-rb-{}", ns, i))
                .await
                .unwrap();
            assert_eq!(
                pod.spec.as_ref().unwrap().containers[0].image,
                "nginx:1.19",
                "pod web-rb-{i} must be rolled back to the original image"
            );
            let rev = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("controller-revision-hash"))
                .cloned()
                .unwrap_or_default();
            assert_eq!(
                rev, rev_a,
                "pod web-rb-{i} must carry the rolled-back revision"
            );
        }
        let status = storage
            .get::<StatefulSet>(&key)
            .await
            .unwrap()
            .status
            .unwrap();
        assert_eq!(status.current_revision.as_deref(), Some(rev_a.as_str()));
        assert_eq!(status.update_revision.as_deref(), Some(rev_a.as_str()));
    }

    /// Rolling updates replace pods from the highest ordinal down (OrderedReady):
    /// the first pod the controller marks for replacement is the highest-ordinal
    /// stale one, and lower ordinals wait their turn.
    #[tokio::test]
    async fn test_rolling_update_replaces_highest_ordinal_first() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let key = format!("/registry/statefulsets/{}/web-ord", ns);
        storage
            .create(&key, &make_statefulset("web-ord", ns, 3, "nginx:1.19"))
            .await
            .unwrap();

        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("web-ord-{}", i)).await;
        }
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Trigger an update; keep all pods Ready and reconcile exactly once.
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();
        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("web-ord-{}", i)).await;
        }
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        let terminating = |pod: &Pod| pod.metadata.deletion_timestamp.is_some();
        let p0: Pod = storage
            .get(&format!("/registry/pods/{}/web-ord-0", ns))
            .await
            .unwrap();
        let p1: Pod = storage
            .get(&format!("/registry/pods/{}/web-ord-1", ns))
            .await
            .unwrap();
        let p2: Pod = storage
            .get(&format!("/registry/pods/{}/web-ord-2", ns))
            .await
            .unwrap();
        assert!(
            terminating(&p2),
            "highest-ordinal pod web-ord-2 must be replaced first"
        );
        assert!(
            !terminating(&p0) && !terminating(&p1),
            "lower-ordinal pods must not be replaced before web-ord-2"
        );
    }

    /// Upstream `updateStatefulSet` (stateful_set_control.go:732-751): after a
    /// pod matching the update revision is found, "wait for unavailable Pods on
    /// update" (`isUnavailable`) ends the sync BEFORE the next lower ordinal is
    /// considered. A replacement that is not yet Ready must hold the rollout.
    #[tokio::test]
    async fn test_rolling_update_waits_for_unavailable_replacement() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let key = format!("/registry/statefulsets/{}/web-wait", ns);
        storage
            .create(&key, &make_statefulset("web-wait", ns, 3, "nginx:1.19"))
            .await
            .unwrap();
        for _ in 0..3 {
            let mut ss: StatefulSet = storage.get(&key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();
            for i in 0..3 {
                make_pod_ready(&storage, ns, &format!("web-wait-{}", i)).await;
            }
        }
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();

        // Step 1: web-wait-2 is deleted.
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        simulate_kubelet_cleanup(&storage, ns).await;
        // Step 2: web-wait-2 is recreated at the update revision, not Ready.
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        let p2: Pod = storage
            .get(&format!("/registry/pods/{}/web-wait-2", ns))
            .await
            .unwrap();
        assert!(p2.metadata.deletion_timestamp.is_none());
        // Step 3..5: the replacement stays unavailable, nothing else may go.
        for _ in 0..3 {
            let mut ss: StatefulSet = storage.get(&key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();
        }
        let p1: Pod = storage
            .get(&format!("/registry/pods/{}/web-wait-1", ns))
            .await
            .unwrap();
        assert!(
            p1.metadata.deletion_timestamp.is_none(),
            "web-wait-1 must not be replaced while web-wait-2 is unavailable"
        );
    }

    /// Partition should be respected: only pods with ordinal >= partition are updated.
    #[tokio::test]
    async fn test_canary_update_respects_partition() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        let mut ss = make_statefulset("web", ns, 3, "nginx:1.19");
        // Set partition=2 so only pod web-2 should be updated
        ss.spec.update_strategy = Some(StatefulSetUpdateStrategy {
            strategy_type: Some("RollingUpdate".to_string()),
            rolling_update: Some(RollingUpdateStatefulSetStrategy {
                partition: Some(2),
                max_unavailable: None,
            }),
        });

        let key = format!("/registry/statefulsets/{}/{}", ns, "web");
        storage.create(&key, &ss).await.unwrap();

        // Create pods and make them ready
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("web-{}", i)).await;
        }

        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Record the old revision from pod-0
        let pod0: Pod = storage
            .get(&format!("/registry/pods/{}/web-0", ns))
            .await
            .unwrap();
        let old_rev = pod0
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();

        // Patch image
        let mut ss: StatefulSet = storage.get(&key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(&key, &ss).await.unwrap();

        // Run several reconcile cycles
        for _ in 0..5 {
            simulate_kubelet_cleanup(&storage, ns).await;

            let pod_prefix = format!("/registry/pods/{}/", ns);
            let pods: Vec<Pod> = storage.list(&pod_prefix).await.unwrap();
            for pod in &pods {
                if pod
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("app"))
                    .map(|a| a == "web")
                    .unwrap_or(false)
                {
                    make_pod_ready(&storage, ns, &pod.metadata.name).await;
                }
            }

            let mut ss: StatefulSet = storage.get(&key).await.unwrap();
            controller.reconcile(&mut ss).await.unwrap();
        }

        // Check that pod-0 and pod-1 still have the old revision (partition=2 protects them)
        let pod0: Pod = storage
            .get(&format!("/registry/pods/{}/web-0", ns))
            .await
            .unwrap();
        let pod0_rev = pod0
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_eq!(
            pod0_rev, old_rev,
            "Pod-0 should keep old revision (below partition)"
        );

        let pod1: Pod = storage
            .get(&format!("/registry/pods/{}/web-1", ns))
            .await
            .unwrap();
        let pod1_rev = pod1
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_eq!(
            pod1_rev, old_rev,
            "Pod-1 should keep old revision (below partition)"
        );

        // Pod-2 should have the new revision
        let pod2: Pod = storage
            .get(&format!("/registry/pods/{}/web-2", ns))
            .await
            .unwrap();
        let pod2_rev = pod2
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("controller-revision-hash"))
            .cloned()
            .unwrap();
        assert_ne!(
            pod2_rev, old_rev,
            "Pod-2 should have new revision (at or above partition)"
        );

        // currentRevision should NOT equal updateRevision (partition prevents full rollout)
        let ss: StatefulSet = storage.get(&key).await.unwrap();
        let status = ss.status.as_ref().unwrap();
        assert_ne!(
            status.current_revision, status.update_revision,
            "With partition, currentRevision should not equal updateRevision"
        );
    }

    /// Test that scale-down sets deletionTimestamp instead of direct delete.
    #[tokio::test]
    async fn test_scale_down_sets_deletion_timestamp() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        // Create a StatefulSet with 3 replicas
        let ss = make_statefulset("ss-scale", ns, 3, "busybox");
        storage
            .create("/registry/statefulsets/default/ss-scale", &ss)
            .await
            .unwrap();

        // First reconcile: creates 3 pods
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Make all pods ready
        for i in 0..3 {
            make_pod_ready(&storage, ns, &format!("ss-scale-{}", i)).await;
        }

        // Reconcile again to update status
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Scale down to 2
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        ss.spec.replicas = Some(2);
        storage
            .update("/registry/statefulsets/default/ss-scale", &ss)
            .await
            .unwrap();

        // Reconcile — should set deletionTimestamp on pod ss-scale-2
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Pod ss-scale-2 should have deletionTimestamp set, not be deleted
        let pod2: Pod = storage
            .get("/registry/pods/default/ss-scale-2")
            .await
            .expect("Pod ss-scale-2 should still exist (graceful termination)");

        assert!(
            pod2.metadata.deletion_timestamp.is_some(),
            "Pod ss-scale-2 should have deletionTimestamp set for graceful termination"
        );

        // Pods 0 and 1 should not have deletionTimestamp
        let pod0: Pod = storage
            .get("/registry/pods/default/ss-scale-0")
            .await
            .unwrap();
        assert!(
            pod0.metadata.deletion_timestamp.is_none(),
            "Pod ss-scale-0 should not be terminating"
        );

        // Status should not count terminating pods
        let ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-scale")
            .await
            .unwrap();
        let status = ss.status.unwrap();
        assert_eq!(
            status.replicas, 2,
            "replicas should exclude terminating pods"
        );
        assert_eq!(
            status.ready_replicas.unwrap_or(0),
            2,
            "readyReplicas should exclude terminating pods"
        );
    }

    /// Rolling update should use graceful termination (set deletionTimestamp)
    /// instead of direct deletion. This ensures the kubelet can perform cleanup
    /// and the pod gets properly recreated.
    #[tokio::test]
    async fn test_rolling_update_uses_graceful_termination() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let ss = make_statefulset("ss-grace", ns, 2, "nginx:1.19");
        let key = "/registry/statefulsets/default/ss-grace";
        storage.create(key, &ss).await.unwrap();

        // Create initial pods
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        for i in 0..2 {
            make_pod_ready(&storage, ns, &format!("ss-grace-{}", i)).await;
        }
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Change image to trigger rolling update
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        ss.spec.template.spec.containers[0].image = "nginx:1.20".to_string();
        storage.update(key, &ss).await.unwrap();

        // Reconcile — should set deletionTimestamp on highest-ordinal stale pod
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // The pod should still exist (not directly deleted) with deletionTimestamp set
        let pod1_key = "/registry/pods/default/ss-grace-1";
        let pod1: Pod = storage.get(pod1_key).await.expect(
            "Pod ss-grace-1 should still exist after rolling update (graceful termination)",
        );
        assert!(
            pod1.metadata.deletion_timestamp.is_some(),
            "Rolling update should set deletionTimestamp for graceful termination, not direct delete"
        );
        assert!(
            pod1.metadata.deletion_grace_period_seconds.is_some(),
            "Rolling update should set deletion_grace_period_seconds"
        );
    }

    /// Current replicas count should exclude terminating pods so that
    /// the controller can recreate them with the new template.
    #[tokio::test]
    async fn test_current_replicas_excludes_terminating() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());
        let ns = "default";
        let ss = make_statefulset("ss-term", ns, 2, "nginx:1.19");
        let key = "/registry/statefulsets/default/ss-term";
        storage.create(key, &ss).await.unwrap();

        // Create initial pods
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();
        for i in 0..2 {
            make_pod_ready(&storage, ns, &format!("ss-term-{}", i)).await;
        }

        // Manually set deletionTimestamp on one pod (simulating graceful termination)
        let pod_key = "/registry/pods/default/ss-term-1";
        let mut pod: Pod = storage.get(pod_key).await.unwrap();
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        storage.update(pod_key, &pod).await.unwrap();

        // Reconcile — controller should see only 1 active replica (not 2)
        // and attempt to recreate the terminating one
        let mut ss: StatefulSet = storage.get(key).await.unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Status should reflect the non-terminating count
        let ss: StatefulSet = storage.get(key).await.unwrap();
        let status = ss.status.unwrap();
        // replicas should be the total non-terminating pod count
        assert!(
            status.replicas <= 2,
            "replicas ({}) should not double-count terminating pods",
            status.replicas
        );
    }

    #[tokio::test]
    async fn test_scale_down_blocked_when_pods_unhealthy() {
        // Reproduces the conformance test "should not scale past 3 replicas"
        // When pods are Running but NOT Ready, scale-down must be blocked.
        let storage = Arc::new(MemoryStorage::new());
        let controller = StatefulSetController::new(storage.clone());

        let ns = "default";
        let mut ss = make_statefulset("ss-block", ns, 3, "busybox");
        // Use OrderedReady policy (the K8s default, used by conformance tests)
        ss.spec.pod_management_policy = Some("OrderedReady".to_string());
        storage
            .create("/registry/statefulsets/default/ss-block", &ss)
            .await
            .unwrap();

        // Create all 3 pods by reconciling with Ready status between each
        for round in 0..3 {
            let mut ss: StatefulSet = storage
                .get("/registry/statefulsets/default/ss-block")
                .await
                .unwrap();
            controller.reconcile(&mut ss).await.unwrap();
            // Make the newly created pod Ready so the next one can be created
            let pod_key = format!("/registry/pods/default/ss-block-{}", round);
            if storage.get::<Pod>(&pod_key).await.is_ok() {
                make_pod_ready(&storage, ns, &format!("ss-block-{}", round)).await;
            }
        }
        // One more reconcile to ensure all pods are created
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-block")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // Now make all pods Running but NOT Ready (simulate broken readiness probe)
        for i in 0..3 {
            let pod_key = format!("/registry/pods/default/ss-block-{}", i);
            let mut pod: Pod = storage.get(&pod_key).await.unwrap();
            pod.status = Some(PodStatus {
                phase: Some(Phase::Running),
                conditions: Some(vec![PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: Some("ContainersNotReady".to_string()),
                    message: Some("Not all containers are ready".to_string()),
                    last_probe_time: None,
                    last_transition_time: Some(chrono::Utc::now()),
                    observed_generation: None,
                }]),
                ..pod.status.unwrap_or_default()
            });
            storage.update(&pod_key, &pod).await.unwrap();
        }

        // Scale down to 0 — should be BLOCKED because pods are unhealthy
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-block")
            .await
            .unwrap();
        ss.spec.replicas = Some(0);
        storage
            .update("/registry/statefulsets/default/ss-block", &ss)
            .await
            .unwrap();

        // Reconcile — should NOT delete any pods
        let mut ss: StatefulSet = storage
            .get("/registry/statefulsets/default/ss-block")
            .await
            .unwrap();
        controller.reconcile(&mut ss).await.unwrap();

        // ALL 3 pods should still exist (no deletionTimestamp)
        for i in 0..3 {
            let pod_key = format!("/registry/pods/default/ss-block-{}", i);
            let pod: Pod = storage
                .get(&pod_key)
                .await
                .unwrap_or_else(|_| panic!("Pod ss-block-{} should still exist", i));
            assert!(
                pod.metadata.deletion_timestamp.is_none(),
                "Pod ss-block-{} should NOT have deletionTimestamp — scale-down should be blocked when pods are unhealthy",
                i
            );
        }
    }

    // --- pod event handlers on the shared pod informer (#3724) -------------
    // stateful_set.go addPod :202, updatePod :236, deletePod :296.

    use rusternetes_storage::WatchEvent;

    fn sts_pod_json(name: &str, owner: Option<(&str, &str)>, labels: &[(&str, &str)]) -> String {
        let labels: HashMap<&str, &str> = labels.iter().copied().collect();
        let mut v = serde_json::json!({
            "metadata": {"name": name, "namespace": "default", "labels": labels},
            "spec": {"containers": []},
        });
        if let Some((owner, uid)) = owner {
            v["metadata"]["ownerReferences"] = serde_json::json!([{
                "apiVersion": "apps/v1", "kind": "StatefulSet", "name": owner,
                "uid": uid, "controller": true
            }]);
        }
        v.to_string()
    }

    /// The next queued key, or `None` if nothing arrives (never hangs).
    async fn next(queue: &WorkQueue) -> Option<String> {
        tokio::time::timeout(Duration::from_secs(2), queue.get())
            .await
            .ok()
            .flatten()
    }

    /// A controller registered on a fresh informer, plus its queue, with the
    /// StatefulSet "web" (uid "web-uid") stored.
    async fn registered() -> (
        Arc<StatefulSetController<MemoryStorage>>,
        Arc<SharedPodInformer<MemoryStorage>>,
        WorkQueue,
    ) {
        let storage = Arc::new(MemoryStorage::new());
        let mut web = make_statefulset("web", "default", 1, "img");
        web.metadata.uid = "web-uid".to_string();
        storage
            .create("/registry/statefulsets/default/web", &web)
            .await
            .unwrap();
        let informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_secs(5),
        ));
        let controller = Arc::new(StatefulSetController::with_pod_informer(
            storage,
            informer.clone(),
        ));
        let queue = WorkQueue::new();
        informer
            .add_event_handler(controller.clone(), queue.clone())
            .await;
        (controller, informer, queue)
    }

    #[tokio::test]
    async fn an_owned_pod_enqueues_only_its_statefulset() {
        let (_c, informer, queue) = registered().await;
        let owned = sts_pod_json("web-0", Some(("web", "web-uid")), &[("app", "web")]);
        informer
            .on_event(&WatchEvent::Added(
                "/registry/pods/default/web-0".into(),
                owned,
            ))
            .await;
        assert_eq!(
            next(&queue).await.as_deref(),
            Some("statefulsets/default/web")
        );
        assert_eq!(queue.len().await, 0);
    }

    /// `resolveControllerRef`: a UID that does not match the StatefulSet found
    /// by name resolves to nothing (stateful_set.go:408-424).
    #[tokio::test]
    async fn a_pod_owned_by_another_uid_enqueues_nothing() {
        let (_c, informer, queue) = registered().await;
        let stale = sts_pod_json("web-0", Some(("web", "other-uid")), &[("app", "web")]);
        informer
            .on_event(&WatchEvent::Added(
                "/registry/pods/default/web-0".into(),
                stale,
            ))
            .await;
        assert_eq!(queue.len().await, 0);
    }

    /// An unrelated StatefulSet in the namespace is not enqueued by an
    /// owned pod's deletion (the old handler enqueued every set in the
    /// namespace; deletePod :296-325 enqueues only the controller).
    #[tokio::test]
    async fn deleting_an_owned_pod_enqueues_its_set_not_the_namespace() {
        let (c, informer, queue) = registered().await;
        let other = make_statefulset("other", "default", 1, "img");
        c.storage
            .create("/registry/statefulsets/default/other", &other)
            .await
            .unwrap();
        let owned = sts_pod_json("web-0", Some(("web", "web-uid")), &[("app", "web")]);
        let key = "/registry/pods/default/web-0".to_string();
        informer
            .on_event(&WatchEvent::Added(key.clone(), owned.clone()))
            .await;
        assert_eq!(
            next(&queue).await.as_deref(),
            Some("statefulsets/default/web")
        );
        queue.done("statefulsets/default/web").await;
        informer.on_event(&WatchEvent::Deleted(key, owned)).await;
        assert_eq!(
            next(&queue).await.as_deref(),
            Some("statefulsets/default/web")
        );
        assert_eq!(queue.len().await, 0);
    }

    /// An orphan whose labels match a set's selector wakes that set so it can
    /// adopt it (addPod :215-227, `getStatefulSetsForPod` :387); a
    /// non-matching orphan wakes nobody.
    #[tokio::test]
    async fn an_orphan_wakes_the_sets_whose_selector_matches() {
        let (_c, informer, queue) = registered().await;
        informer
            .on_event(&WatchEvent::Added(
                "/registry/pods/default/stray".into(),
                sts_pod_json("stray", None, &[("app", "nope")]),
            ))
            .await;
        assert_eq!(queue.len().await, 0);
        informer
            .on_event(&WatchEvent::Added(
                "/registry/pods/default/adoptable".into(),
                sts_pod_json("adoptable", None, &[("app", "web")]),
            ))
            .await;
        assert_eq!(
            next(&queue).await.as_deref(),
            Some("statefulsets/default/web")
        );
    }
}
