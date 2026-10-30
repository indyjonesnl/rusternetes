//! Unit tests moved from `src/controllers/resource_quota.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::Utc;
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::quota;
    pub use rusternetes_common::resources::{Pod, ResourceQuota, ResourceQuotaStatus, Service};
    pub use rusternetes_controller_manager::controllers::worker_pool::spawn_workers;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::{debug, error, info};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{
        Container, PodSpec, ResourceQuotaSpec, ScopeSelector, ScopedResourceSelectorRequirement,
    };
    use rusternetes_common::types::{ObjectMeta, ResourceRequirements, TypeMeta};
    use rusternetes_controller_manager::controllers::resource_quota::*;
    use rusternetes_storage::memory::MemoryStorage;

    fn make_container(name: &str, resources: Option<ResourceRequirements>) -> Container {
        Container {
            name: name.to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources,
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
        }
    }

    fn make_pod(name: &str, namespace: &str, resources: Option<ResourceRequirements>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            spec: Some(PodSpec {
                containers: vec![make_container("test", resources)],
                ..Default::default()
            }),
            status: None,
        }
    }

    fn make_pod_with_deadline(name: &str, namespace: &str, active_deadline: Option<i64>) -> Pod {
        let mut pod = make_pod(name, namespace, None);
        if let Some(spec) = &mut pod.spec {
            spec.active_deadline_seconds = active_deadline;
        }
        pod
    }

    #[test]
    fn test_is_pod_best_effort() {
        // Pod with no resources is BestEffort
        let pod = make_pod("test", "default", None);
        assert!(quota::is_best_effort(&pod));

        // Pod with empty resources is BestEffort
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: None,
                limits: None,
                claims: None,
            }),
        );
        assert!(quota::is_best_effort(&pod));

        // Pod with empty maps is BestEffort
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(HashMap::new()),
                limits: Some(HashMap::new()),
                claims: None,
            }),
        );
        assert!(quota::is_best_effort(&pod));

        // Pod with CPU request is NOT BestEffort
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        assert!(!quota::is_best_effort(&pod));

        // Pod with only limits is NOT BestEffort
        let mut limits = HashMap::new();
        limits.insert("memory".to_string(), "128Mi".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: None,
                limits: Some(limits),
                claims: None,
            }),
        );
        assert!(!quota::is_best_effort(&pod));
    }

    #[test]
    fn test_pod_matches_scopes_terminating() {
        // Pod with active_deadline_seconds is Terminating
        let pod = make_pod_with_deadline("test", "default", Some(30));
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["Terminating".to_string()],
                None
            )
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotTerminating".to_string()],
                None
            )
        );

        // Pod without active_deadline_seconds is NotTerminating
        let pod = make_pod_with_deadline("test", "default", None);
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["Terminating".to_string()],
                None
            )
        );
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotTerminating".to_string()],
                None
            )
        );
    }

    #[test]
    fn test_pod_matches_scopes_best_effort() {
        // Pod with no resources = BestEffort
        let pod = make_pod("test", "default", None);
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["BestEffort".to_string()],
                None
            )
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotBestEffort".to_string()],
                None
            )
        );

        // Pod with resources = NotBestEffort
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod = make_pod(
            "test",
            "default",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        assert!(
            !ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["BestEffort".to_string()],
                None
            )
        );
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &["NotBestEffort".to_string()],
                None
            )
        );
    }

    #[test]
    fn test_pod_matches_scope_selector() {
        let pod = make_pod_with_deadline("test", "default", Some(30));
        let selector = ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "Terminating".to_string(),
                operator: "Exists".to_string(),
                values: None,
            }],
        };
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &[],
                Some(&selector)
            )
        );

        // `podMatchesScopeFunc` ignores the operator of the pod-state scopes
        // (pods.go:335-342); validation only admits `Exists` for them.
        let selector_not = ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "Terminating".to_string(),
                operator: "DoesNotExist".to_string(),
                values: None,
            }],
        };
        assert!(
            ResourceQuotaController::<MemoryStorage>::pod_matches_scopes(
                &pod,
                &[],
                Some(&selector_not)
            )
        );
    }

    #[tokio::test]
    async fn test_calculate_usage_with_scopes() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Create a BestEffort pod (no resources)
        let pod1 = make_pod("be-pod", "test-ns", None);
        storage
            .create("/registry/pods/test-ns/be-pod", &pod1)
            .await
            .unwrap();

        // Create a NotBestEffort pod (has resources)
        let mut reqs = HashMap::new();
        reqs.insert("cpu".to_string(), "100m".to_string());
        let pod2 = make_pod(
            "nbe-pod",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        storage
            .create("/registry/pods/test-ns/nbe-pod", &pod2)
            .await
            .unwrap();

        // BestEffort scope should count only the BE pod
        let usage = controller
            .calculate_usage(
                "test-ns",
                &["BestEffort".to_string()],
                None,
                &["pods".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "1");

        // NotBestEffort scope should count only the NBE pod
        let usage = controller
            .calculate_usage(
                "test-ns",
                &["NotBestEffort".to_string()],
                None,
                &["pods".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "1");

        // No scope should count both
        let usage = controller
            .calculate_usage("test-ns", &[], None, &["pods".to_string()])
            .await
            .unwrap();
        assert_eq!(usage.get("pods").unwrap(), "2");
    }

    #[tokio::test]
    async fn test_reconcile_quota_sets_status_used() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Create a quota
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        hard.insert("requests.cpu".to_string(), "4".to_string());
        let quota = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-quota").with_namespace("test-ns"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/test-ns/test-quota", &quota)
            .await
            .unwrap();

        // Reconcile
        controller.reconcile_all().await.unwrap();

        // Check status was set
        let updated: ResourceQuota = storage
            .get("/registry/resourcequotas/test-ns/test-quota")
            .await
            .unwrap();
        let status = updated.status.unwrap();
        assert!(status.hard.is_some());
        assert!(status.used.is_some());
        let used = status.used.unwrap();
        assert_eq!(used.get("pods").unwrap(), "0");
        // `NewMilliQuantity(0, DecimalSI).String()` is "0" — upstream's
        // `CanonicalizeBytes` short-circuits on `IsZero` (`quantity.go:426`),
        // so a zero cpu usage never carries a suffix. Was "0m".
        assert_eq!(used.get("requests.cpu").unwrap(), "0");
    }

    #[tokio::test]
    async fn test_enqueue_quotas_for_event_reacts_to_pod_change() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        // Two quotas in the same namespace should both be enqueued
        let mut hard = HashMap::new();
        hard.insert("pods".to_string(), "10".to_string());
        for name in ["quota-a", "quota-b"] {
            let q = ResourceQuota {
                type_meta: TypeMeta {
                    kind: "ResourceQuota".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta::new(name).with_namespace("ns-1"),
                spec: ResourceQuotaSpec {
                    hard: Some(hard.clone()),
                    scopes: None,
                    scope_selector: None,
                },
                status: None,
            };
            storage
                .create(&format!("/registry/resourcequotas/ns-1/{}", name), &q)
                .await
                .unwrap();
        }
        // And one in a different namespace that should NOT be enqueued
        let other = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("quota-x").with_namespace("ns-2"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/ns-2/quota-x", &other)
            .await
            .unwrap();

        let queue = WorkQueue::new();
        // Simulate a pod create event in ns-1
        let ev = rusternetes_storage::WatchEvent::Added(
            "/registry/pods/ns-1/some-pod".to_string(),
            "{}".to_string(),
        );
        controller.enqueue_quotas_for_event(&queue, &ev).await;

        // Both ns-1 quotas should be queued, ns-2 should not
        assert_eq!(queue.len().await, 2);
        let k1 = queue.get().await.unwrap();
        let k2 = queue.get().await.unwrap();
        let mut keys = vec![k1, k2];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "resourcequotas/ns-1/quota-a".to_string(),
                "resourcequotas/ns-1/quota-b".to_string(),
            ]
        );
    }

    /// `status.used` for an extended resource used to go through
    /// `qty.parse::<i64>().unwrap_or(0)`, so any quantity carrying a suffix —
    /// `"2k"`, `"1Ki"` — reported as `0` and the dimension went unaccounted.
    #[tokio::test]
    async fn test_calculate_usage_extended_resource_with_suffix() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let mut reqs = HashMap::new();
        reqs.insert("example.com/dongle".to_string(), "2k".to_string());
        let pod = make_pod(
            "dongle-pod",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            }),
        );
        storage
            .create("/registry/pods/test-ns/dongle-pod", &pod)
            .await
            .unwrap();

        let usage = controller
            .calculate_usage(
                "test-ns",
                &[],
                None,
                &["requests.example.com/dongle".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(
            usage.get("requests.example.com/dongle").map(String::as_str),
            Some("2k")
        );
        // Upstream charges an extended resource only under `requests.<name>`
        // (`podComputeUsageHelper`, `pods.go:324-328`).
        assert!(!usage.contains_key("example.com/dongle"));
    }

    /// `pvcEvaluator.UsageStats` (persistent_volume_claims.go:226): the
    /// controller reports `requests.storage` and the per-class keys quota
    /// admission reads, and a tracked key with no claims is zero, not absent.
    #[tokio::test]
    async fn test_calculate_usage_pvc_storage_and_class() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());
        for (name, class, size) in [
            ("a", "gold", "10Gi"),
            ("b", "gold", "5Gi"),
            ("c", "tin", "1Gi"),
        ] {
            let pvc: serde_json::Value = serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": name, "namespace": "test-ns"},
                "spec": {"storageClassName": class,
                         "resources": {"requests": {"storage": size}}},
            });
            storage
                .create(
                    &format!("/registry/persistentvolumeclaims/test-ns/{name}"),
                    &pvc,
                )
                .await
                .unwrap();
        }
        let hard: Vec<String> = [
            "persistentvolumeclaims",
            "requests.storage",
            "gold.storageclass.storage.k8s.io/requests.storage",
            "gold.storageclass.storage.k8s.io/persistentvolumeclaims",
            "bronze.storageclass.storage.k8s.io/requests.storage",
        ]
        .map(String::from)
        .to_vec();
        let usage = controller
            .calculate_usage("test-ns", &[], None, &hard)
            .await
            .unwrap();
        assert_eq!(usage["persistentvolumeclaims"], "3");
        assert_eq!(usage["requests.storage"], "16Gi");
        assert_eq!(
            usage["gold.storageclass.storage.k8s.io/requests.storage"],
            "15Gi"
        );
        assert_eq!(
            usage["gold.storageclass.storage.k8s.io/persistentvolumeclaims"],
            "2"
        );
        assert_eq!(
            usage["bronze.storageclass.storage.k8s.io/requests.storage"],
            "0"
        );
    }

    /// `pvcEvaluator.UsageStats` with `pvcMatchesScopeFunc`
    /// (persistent_volume_claims.go:250-257, 293-309): a quota scoped to the
    /// VolumeAttributesClass counts only the claims that reference the class
    /// (spec, current or modify-target), and any other scope counts none.
    #[tokio::test]
    async fn test_calculate_usage_pvc_volume_attributes_class_scope() {
        use rusternetes_common::resources::{ScopeSelector, ScopedResourceSelectorRequirement};
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());
        let claims = [
            (
                "spec-gold",
                serde_json::json!({"volumeAttributesClassName": "gold"}),
                serde_json::json!({}),
            ),
            (
                "current-gold",
                serde_json::json!({}),
                serde_json::json!({"currentVolumeAttributesClassName": "gold"}),
            ),
            (
                "target-gold",
                serde_json::json!({}),
                serde_json::json!({"modifyVolumeStatus": {"targetVolumeAttributesClassName": "gold", "status": "Pending"}}),
            ),
            (
                "silver",
                serde_json::json!({"volumeAttributesClassName": "silver"}),
                serde_json::json!({}),
            ),
            ("none", serde_json::json!({}), serde_json::json!({})),
        ];
        for (name, spec_extra, status) in claims {
            let mut spec = serde_json::json!({"resources": {"requests": {"storage": "1Gi"}}});
            for (k, v) in spec_extra.as_object().unwrap() {
                spec[k] = v.clone();
            }
            let pvc = serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": name, "namespace": "test-ns"},
                "spec": spec, "status": status,
            });
            storage
                .create(
                    &format!("/registry/persistentvolumeclaims/test-ns/{name}"),
                    &pvc,
                )
                .await
                .unwrap();
        }
        let hard: Vec<String> = ["persistentvolumeclaims", "requests.storage"]
            .map(String::from)
            .to_vec();
        let selector = |op: &str, values: Option<Vec<String>>| ScopeSelector {
            match_expressions: vec![ScopedResourceSelectorRequirement {
                scope_name: "VolumeAttributesClass".to_string(),
                operator: op.to_string(),
                values,
            }],
        };
        let in_gold = selector("In", Some(vec!["gold".to_string()]));
        let usage = controller
            .calculate_usage("test-ns", &[], Some(&in_gold), &hard)
            .await
            .unwrap();
        assert_eq!(usage["persistentvolumeclaims"], "3");
        assert_eq!(usage["requests.storage"], "3Gi");

        let exists = selector("Exists", None);
        let usage = controller
            .calculate_usage("test-ns", &[], Some(&exists), &hard)
            .await
            .unwrap();
        assert_eq!(usage["persistentvolumeclaims"], "4");

        let does_not_exist = selector("DoesNotExist", None);
        let usage = controller
            .calculate_usage("test-ns", &[], Some(&does_not_exist), &hard)
            .await
            .unwrap();
        assert_eq!(usage["persistentvolumeclaims"], "1");

        // `scopes: [BestEffort]` is an `Exists` selector on a scope the PVC
        // evaluator does not know: it matches no claim.
        let usage = controller
            .calculate_usage("test-ns", &["BestEffort".to_string()], None, &hard)
            .await
            .unwrap();
        assert_eq!(usage["persistentvolumeclaims"], "0");
    }

    /// A pod's peak footprint includes its init containers: upstream
    /// `PodRequests` takes `max(sum(containers), max over init containers)`.
    /// Summing only `spec.containers` charged the 1Gi app container and ignored
    /// the 4Gi init container entirely.
    #[tokio::test]
    async fn test_calculate_usage_charges_init_container_peak() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let mut app_reqs = HashMap::new();
        app_reqs.insert("memory".to_string(), "1Gi".to_string());
        let mut init_reqs = HashMap::new();
        init_reqs.insert("memory".to_string(), "4Gi".to_string());

        let mut pod = make_pod(
            "init-heavy",
            "test-ns",
            Some(ResourceRequirements {
                requests: Some(app_reqs),
                limits: None,
                claims: None,
            }),
        );
        pod.spec.as_mut().unwrap().init_containers = Some(vec![make_container(
            "init",
            Some(ResourceRequirements {
                requests: Some(init_reqs),
                limits: None,
                claims: None,
            }),
        )]);
        storage
            .create("/registry/pods/test-ns/init-heavy", &pod)
            .await
            .unwrap();

        let usage = controller
            .calculate_usage("test-ns", &[], None, &["requests.memory".to_string()])
            .await
            .unwrap();
        assert_eq!(
            usage.get("requests.memory").map(String::as_str),
            Some("4Gi")
        );
    }

    /// A fractional memory request must not round to zero, and the total must
    /// come back in canonical form rather than raw bytes.
    #[tokio::test]
    async fn test_calculate_usage_fractional_memory() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        for name in ["a", "b"] {
            let mut reqs = HashMap::new();
            reqs.insert("memory".to_string(), "0.5Gi".to_string());
            let pod = make_pod(
                name,
                "test-ns",
                Some(ResourceRequirements {
                    requests: Some(reqs),
                    limits: None,
                    claims: None,
                }),
            );
            storage
                .create(&format!("/registry/pods/test-ns/{name}"), &pod)
                .await
                .unwrap();
        }

        let usage = controller
            .calculate_usage("test-ns", &[], None, &["requests.memory".to_string()])
            .await
            .unwrap();
        assert_eq!(
            usage.get("requests.memory").map(String::as_str),
            Some("1Gi")
        );
    }

    /// Upstream charges a *terminating* pod until its deletion grace period has
    /// elapsed (`QuotaV1Pod`, `pods.go:499-506`); only a terminal phase drops
    /// the compute charge outright. `count/pods` is charged either way.
    #[tokio::test]
    async fn test_calculate_usage_terminal_and_terminating_pods() {
        use rusternetes_common::resources::PodStatus;
        use rusternetes_common::types::Phase;

        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let with_cpu = || {
            let mut reqs = HashMap::new();
            reqs.insert("cpu".to_string(), "100m".to_string());
            Some(ResourceRequirements {
                requests: Some(reqs),
                limits: None,
                claims: None,
            })
        };

        let running = make_pod("running", "test-ns", with_cpu());
        storage
            .create("/registry/pods/test-ns/running", &running)
            .await
            .unwrap();

        let mut succeeded = make_pod("succeeded", "test-ns", with_cpu());
        succeeded.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        storage
            .create("/registry/pods/test-ns/succeeded", &succeeded)
            .await
            .unwrap();

        let mut terminating = make_pod("terminating", "test-ns", with_cpu());
        terminating.metadata.deletion_timestamp = Some(chrono::Utc::now());
        terminating.metadata.deletion_grace_period_seconds = Some(3600);
        storage
            .create("/registry/pods/test-ns/terminating", &terminating)
            .await
            .unwrap();

        let usage = controller
            .calculate_usage(
                "test-ns",
                &[],
                None,
                &[
                    "pods".to_string(),
                    "count/pods".to_string(),
                    "requests.cpu".to_string(),
                ],
            )
            .await
            .unwrap();

        // running + terminating (still inside its grace period)
        assert_eq!(usage.get("pods").map(String::as_str), Some("2"));
        assert_eq!(usage.get("requests.cpu").map(String::as_str), Some("200m"));
        // object-count quota tracks everything in storage, terminal included
        assert_eq!(usage.get("count/pods").map(String::as_str), Some("3"));
    }

    /// `status.used` carries exactly the quota's hard keys — upstream masks the
    /// evaluator's output down to them (`syncResourceQuota`). A key the
    /// evaluator computes but the quota does not constrain must not leak.
    #[tokio::test]
    async fn test_reconcile_quota_masks_status_used_to_hard_keys() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ResourceQuotaController::new(storage.clone());

        let mut hard = HashMap::new();
        hard.insert("requests.cpu".to_string(), "4".to_string());
        let quota = ResourceQuota {
            type_meta: TypeMeta {
                kind: "ResourceQuota".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("masked").with_namespace("test-ns"),
            spec: ResourceQuotaSpec {
                hard: Some(hard),
                scopes: None,
                scope_selector: None,
            },
            status: None,
        };
        storage
            .create("/registry/resourcequotas/test-ns/masked", &quota)
            .await
            .unwrap();

        controller.reconcile_quota(&quota).await.unwrap();

        let stored: ResourceQuota = storage
            .get("/registry/resourcequotas/test-ns/masked")
            .await
            .unwrap();
        let used = stored.status.unwrap().used.unwrap();
        assert_eq!(used.keys().collect::<Vec<_>>(), vec!["requests.cpu"]);
        assert_eq!(used.get("requests.cpu").map(String::as_str), Some("0"));
    }
    /// #1775: an auxiliary watch stream that has ended must be retired, never
    /// polled again.
    ///
    /// `WatchStream` is a `futures::stream::unfold`, which panics with
    /// "Unfold must not be polled after it returned `Poll::Ready(None)`" on a
    /// second poll. Before the fix the select arm matched only `Some(Ok(ev))`,
    /// so a terminated stream stayed enabled and the next iteration panicked —
    /// killing the controller task (tokio::spawn swallows panics) and freezing
    /// every quota's `status.used` until the controller-manager restarted.
    ///
    /// Polling the raw stream twice here is what the old code did; going
    /// through `next_aux_event` twice must be safe.
    #[tokio::test]
    async fn an_exhausted_auxiliary_watch_is_retired_not_repolled() {
        use futures::stream::StreamExt;

        // A stream that ends immediately, built the same way storage builds its
        // watches (unfold), so it carries the same "no polling after None" rule.
        let make_stream = || -> rusternetes_storage::WatchStream {
            futures::stream::unfold(false, |done| async move {
                if done {
                    None
                } else {
                    // End on the first poll: no events, just termination.
                    None::<(
                        rusternetes_common::Result<rusternetes_storage::WatchEvent>,
                        bool,
                    )>
                }
            })
            .boxed()
        };

        let mut aux: Option<rusternetes_storage::WatchStream> = Some(make_stream());

        // First call observes the end of the stream and retires it.
        let first = next_aux_event(&mut aux, "Pod").await;
        assert!(first.is_none(), "a terminated stream yields no event");
        assert!(
            aux.is_none(),
            "the stream must be retired so the select arm's `is_some()` guard disables it"
        );

        // Second call must short-circuit on the None rather than poll the dead
        // stream — this is the call that used to panic.
        let second = next_aux_event(&mut aux, "Pod").await;
        assert!(
            second.is_none(),
            "a retired stream must keep yielding None without being polled again"
        );
    }

    /// #1775: an auxiliary watch that errors is retired too — the same
    /// re-poll panic applies once an unfold stream has reported failure and
    /// terminated.
    #[tokio::test]
    async fn an_erroring_auxiliary_watch_is_retired() {
        use futures::stream::StreamExt;

        let stream: rusternetes_storage::WatchStream = futures::stream::once(async {
            Err(rusternetes_common::Error::Storage("watch died".to_string()))
        })
        .boxed();
        let mut aux: Option<rusternetes_storage::WatchStream> = Some(stream);

        assert!(next_aux_event(&mut aux, "Pod").await.is_none());
        assert!(
            aux.is_none(),
            "an errored stream must be retired, not left enabled for another poll"
        );
    }

    /// A live auxiliary watch still delivers its events — the fix must not
    /// silence the fast path that keeps `status.used` fresh between resyncs.
    #[tokio::test]
    async fn a_live_auxiliary_watch_still_delivers_events() {
        use futures::stream::StreamExt;

        let stream: rusternetes_storage::WatchStream = futures::stream::iter(vec![Ok(
            rusternetes_storage::WatchEvent::Added("pods/default/p1".to_string(), "{}".to_string()),
        )])
        .boxed();
        let mut aux: Option<rusternetes_storage::WatchStream> = Some(stream);

        let ev = next_aux_event(&mut aux, "Pod").await;
        assert!(ev.is_some(), "a live stream's event must be delivered");
        assert!(aux.is_some(), "a live stream must stay enabled");
    }

    /// `serviceEvaluator.Usage` (pkg/quota/v1/evaluator/core/services.go:
    /// 117-147): `services.nodeports` counts ports, not Services, and a
    /// LoadBalancer without node-port allocation counts only its explicit
    /// node ports.
    #[tokio::test]
    async fn node_ports_are_counted_per_port() {
        let storage = Arc::new(MemoryStorage::new());
        for (name, spec) in [
            (
                "np",
                serde_json::json!({"type": "NodePort",
                                   "ports": [{"port": 80}, {"port": 81}]}),
            ),
            (
                "lb",
                serde_json::json!({"type": "LoadBalancer",
                                   "allocateLoadBalancerNodePorts": false,
                                   "ports": [{"port": 80, "nodePort": 30001},
                                             {"port": 81}]}),
            ),
            ("cip", serde_json::json!({"ports": [{"port": 80}]})),
        ] {
            let svc: Service = serde_json::from_value(serde_json::json!({
                "metadata": {"name": name, "namespace": "test-ns"}, "spec": spec
            }))
            .unwrap();
            storage
                .create(&format!("/registry/services/test-ns/{name}"), &svc)
                .await
                .unwrap();
        }
        let controller = ResourceQuotaController::new(storage);
        let usage = controller
            .calculate_usage("test-ns", &[], None, &["services.nodeports".to_string()])
            .await
            .unwrap();
        assert_eq!(usage["services.nodeports"], "3");
        assert_eq!(usage["services.loadbalancers"], "1");
    }

    /// Quota admission charges any `count/<resource>[.<group>]` through the
    /// object-count evaluator, so the controller reports its usage too, or
    /// admission rejects with `status unknown for quota`.
    #[tokio::test]
    async fn object_counts_are_computed_for_any_resource() {
        let storage = Arc::new(MemoryStorage::new());
        for (key, name) in [
            ("/registry/deployments/test-ns/a", "a"),
            ("/registry/deployments/test-ns/b", "b"),
            ("/registry/deployments/other/c", "c"),
            ("/registry/example_com_widgets/test-ns/w", "w"),
        ] {
            storage
                .create(key, &serde_json::json!({"metadata": {"name": name}}))
                .await
                .unwrap();
        }
        let controller = ResourceQuotaController::new(storage);
        let usage = controller
            .calculate_usage(
                "test-ns",
                &[],
                None,
                &[
                    "count/deployments.apps".to_string(),
                    "count/widgets.example.com".to_string(),
                    "count/jobs.batch".to_string(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(usage["count/deployments.apps"], "2");
        assert_eq!(usage["count/widgets.example.com"], "1");
        assert_eq!(usage["count/jobs.batch"], "0");
    }

    fn ev_pod(kind: &str, name: &str, phase: &str) -> rusternetes_storage::WatchEvent {
        let v = format!(
            r#"{{"apiVersion":"v1","kind":"Pod","metadata":{{"name":"{name}","namespace":"ns"}},"spec":{{"containers":[{{"name":"c","image":"i"}}]}},"status":{{"phase":"{phase}"}}}}"#
        );
        let k = format!("/registry/pods/ns/{name}");
        match kind {
            "add" => rusternetes_storage::WatchEvent::Added(k, v),
            "mod" => rusternetes_storage::WatchEvent::Modified(k, v),
            _ => rusternetes_storage::WatchEvent::Deleted(k, v),
        }
    }

    /// Upstream registers no AddFunc (resource_quota_monitor.go:146-172): a pod
    /// create must not recompute quota (#2812), a status-only update must not,
    /// a pod leaving the charged set or being deleted must.
    #[test]
    fn replenish_filter_ignores_creates_and_plain_updates() {
        let mut f = ReplenishFilter::default();
        assert!(!f.should_replenish(&ev_pod("add", "p", "Pending")));
        assert!(!f.should_replenish(&ev_pod("mod", "p", "Running")));
        assert!(f.should_replenish(&ev_pod("mod", "p", "Succeeded")));
        assert!(f.should_replenish(&ev_pod("del", "p", "Succeeded")));
        // Update of a pod never seen: no oldObj to compare, enqueue.
        assert!(f.should_replenish(&ev_pod("mod", "q", "Running")));
    }

    #[test]
    fn replenish_filter_configmap_and_secret_only_on_delete() {
        let mut f = ReplenishFilter::default();
        for kind in ["configmaps", "secrets"] {
            let k = format!("/registry/{kind}/ns/x");
            assert!(!f.should_replenish(&rusternetes_storage::WatchEvent::Added(
                k.clone(),
                "{}".into()
            )));
            assert!(
                !f.should_replenish(&rusternetes_storage::WatchEvent::Modified(
                    k.clone(),
                    "{}".into()
                ))
            );
            assert!(f.should_replenish(&rusternetes_storage::WatchEvent::Deleted(k, "{}".into())));
        }
    }
}
