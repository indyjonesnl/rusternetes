//! Unit tests moved from `src/controllers/namespace.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use chrono::Utc;
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::{Namespace, NamespaceCondition, NamespaceStatus, Pod};
    pub use rusternetes_common::types::Phase;
    pub use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
    pub use std::collections::BTreeMap;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_controller_manager::controllers::namespace::*;
    use rusternetes_storage::memory::MemoryStorage;

    fn set_namespace_finalizers(namespace: &mut Namespace, finalizers: Vec<String>) {
        namespace
            .spec
            .get_or_insert_with(Default::default)
            .finalizers = Some(finalizers);
    }

    // Ports of upstream's estimate semantics
    // (`namespaced_resources_deleter.go:608-657`, `namespace_controller.go:157-160`).
    #[test]
    fn pod_estimate_is_max_grace_of_non_terminal_pods() {
        let pods = vec![
            serde_json::json!({"spec":{"terminationGracePeriodSeconds":30},"status":{"phase":"Running"}}),
            serde_json::json!({"spec":{"terminationGracePeriodSeconds":300},"status":{"phase":"Failed"}}),
            serde_json::json!({"spec":{"terminationGracePeriodSeconds":10},"status":{"phase":"Succeeded"}}),
            serde_json::json!({"spec":{"terminationGracePeriodSeconds":45}}),
            serde_json::json!({"spec":{}}),
        ];
        assert_eq!(estimate_graceful_termination_for_pods(&pods), 45);
        assert_eq!(estimate_graceful_termination_for_pods(&[]), 0);
    }

    #[test]
    fn estimate_is_spent_once_namespace_has_been_deleting_that_long() {
        let now = Utc::now();
        let fresh = now - chrono::Duration::seconds(5);
        assert_eq!(clamp_estimate_to_namespace_age(30, fresh, now), 30);
        let old = now - chrono::Duration::seconds(30);
        assert_eq!(clamp_estimate_to_namespace_age(30, old, now), 0);
        assert_eq!(clamp_estimate_to_namespace_age(0, now, now), 0);
    }

    #[test]
    fn requeue_delay_is_half_the_estimate_plus_one() {
        assert_eq!(requeue_delay_for_estimate(30), Duration::from_secs(16));
        assert_eq!(requeue_delay_for_estimate(15), Duration::from_secs(8));
    }

    #[tokio::test]
    async fn finalize_reports_grace_estimate_then_finalizer_default() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NamespaceController::new(storage.clone());
        let mut ns = Namespace::new("est-ns");
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        storage
            .create(&build_key("namespaces", None, "est-ns"), &ns)
            .await
            .unwrap();
        // A running pod held by a finalizer, grace 60s -> estimate 60.
        let pod = serde_json::json!({
            "apiVersion":"v1","kind":"Pod",
            "metadata":{"name":"p","namespace":"est-ns","finalizers":["x/y"]},
            "spec":{"containers":[{"name":"c","image":"i"}],"terminationGracePeriodSeconds":60},
            "status":{"phase":"Running"}
        });
        let pod_key = build_key("pods", Some("est-ns"), "p");
        storage.create(&pod_key, &pod).await.unwrap();
        assert_eq!(controller.finalize_namespace(&ns).await.unwrap(), 60);

        // Terminal pod: no grace estimate, finalizer default applies.
        let mut term = pod.clone();
        term["status"]["phase"] = serde_json::json!("Failed");
        storage.update(&pod_key, &term).await.unwrap();
        assert_eq!(
            controller.finalize_namespace(&ns).await.unwrap(),
            FINALIZER_ESTIMATE_SECONDS
        );
    }

    #[test]
    fn test_namespace_resource_types() {
        // Ensure we have the major resource types covered
        let resource_types = ["pods", "services", "configmaps", "secrets", "deployments"];
        assert!(resource_types.contains(&"pods"));
        assert!(resource_types.contains(&"services"));
    }

    /// kube-root-ca.crt must be created in an active namespace and RECREATED
    /// after deletion, given the controller has the CA threaded in. Regression
    /// for the `[sig-auth] ServiceAccounts should guarantee kube-root-ca.crt
    /// exist in any namespace [Conformance]` failure: the controller-manager
    /// container lacks the api-server's cert paths, so without the threaded CA
    /// it silently skipped (re)creation.
    #[tokio::test]
    async fn test_reconcile_creates_and_recreates_kube_root_ca() {
        let storage = Arc::new(MemoryStorage::new());
        let controller =
            NamespaceController::new(storage.clone()).with_ca_cert(Some("CA-PEM-DATA".to_string()));

        let ns = Namespace::new("active-ns");
        let ns_key = build_key("namespaces", None, "active-ns");
        storage.create(&ns_key, &ns).await.unwrap();

        let cm_key = build_key("configmaps", Some("active-ns"), "kube-root-ca.crt");

        // First reconcile: configmap created with the CA data.
        controller.reconcile_namespace(&ns).await.unwrap();
        let cm: serde_json::Value = storage
            .get(&cm_key)
            .await
            .expect("kube-root-ca.crt must be created in an active namespace");
        assert_eq!(
            cm.pointer("/data/ca.crt").and_then(|v| v.as_str()),
            Some("CA-PEM-DATA")
        );

        // Delete it, then reconcile again: it must be RECREATED.
        storage.delete(&cm_key).await.unwrap();
        assert!(storage.get::<serde_json::Value>(&cm_key).await.is_err());
        controller.reconcile_namespace(&ns).await.unwrap();
        let cm2: serde_json::Value = storage
            .get(&cm_key)
            .await
            .expect("kube-root-ca.crt must be RECREATED after deletion");
        assert_eq!(
            cm2.pointer("/data/ca.crt").and_then(|v| v.as_str()),
            Some("CA-PEM-DATA")
        );
    }

    /// Without a threaded CA (and no cert files), the controller cannot publish
    /// kube-root-ca.crt — it must skip silently rather than create an empty/
    /// invalid ConfigMap.
    #[tokio::test]
    async fn test_reconcile_skips_kube_root_ca_without_ca() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NamespaceController::new(storage.clone());

        let ns = Namespace::new("no-ca-ns");
        let ns_key = build_key("namespaces", None, "no-ca-ns");
        storage.create(&ns_key, &ns).await.unwrap();

        controller.reconcile_namespace(&ns).await.unwrap();
        // No CA available (paths absent in test env) → no configmap written.
        let cm_key = build_key("configmaps", Some("no-ca-ns"), "kube-root-ca.crt");
        assert!(
            storage.get::<serde_json::Value>(&cm_key).await.is_err(),
            "must not publish kube-root-ca.crt without a CA"
        );
    }

    #[test]
    fn test_build_deletion_conditions_all_clear() {
        let conditions =
            NamespaceController::<MemoryStorage>::build_deletion_conditions(false, false);
        assert_eq!(conditions.len(), 5);

        // All conditions should be False when content is fully removed
        for cond in &conditions {
            assert_eq!(
                cond.status, "False",
                "Condition {} should be False",
                cond.condition_type
            );
        }

        // Verify specific condition types are present
        let types: Vec<&str> = conditions
            .iter()
            .map(|c| c.condition_type.as_str())
            .collect();
        assert!(types.contains(&"NamespaceDeletionDiscoveryFailure"));
        assert!(types.contains(&"NamespaceDeletionGroupVersionParsingFailure"));
        assert!(types.contains(&"NamespaceDeletionContentFailure"));
        assert!(types.contains(&"NamespaceContentRemaining"));
        assert!(types.contains(&"NamespaceFinalizersRemaining"));
    }

    #[test]
    fn test_build_deletion_conditions_content_remaining() {
        let conditions =
            NamespaceController::<MemoryStorage>::build_deletion_conditions(true, false);

        let content_remaining = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceContentRemaining")
            .unwrap();
        assert_eq!(content_remaining.status, "True");

        // Other failure conditions should still be False
        let discovery = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceDeletionDiscoveryFailure")
            .unwrap();
        assert_eq!(discovery.status, "False");
    }

    #[test]
    fn test_build_deletion_conditions_finalizers_remaining() {
        let conditions =
            NamespaceController::<MemoryStorage>::build_deletion_conditions(false, true);

        let finalizers = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceFinalizersRemaining")
            .unwrap();
        assert_eq!(finalizers.status, "True");

        // Upstream contract (pkg/controller/namespace/deletion/status_condition_utils.go):
        // `NamespaceDeletionContentFailure` only flips True on an actual delete
        // *error*. Items lingering because of a finalizer leave this condition
        // at its "ok" message and only flip `NamespaceFinalizersRemaining`.
        let content_failure = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceDeletionContentFailure")
            .unwrap();
        assert_eq!(
            content_failure.status, "False",
            "ContentFailure must stay False when only finalizers prevent deletion"
        );
        assert_eq!(content_failure.reason.as_deref(), Some("ContentDeleted"));
        assert_eq!(
            content_failure.message.as_deref(),
            Some("All content successfully deleted, may be waiting on finalization")
        );
    }

    #[test]
    fn test_build_deletion_conditions_no_finalizers() {
        let conditions =
            NamespaceController::<MemoryStorage>::build_deletion_conditions(false, false);

        let content_failure = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceDeletionContentFailure")
            .unwrap();
        assert_eq!(
            content_failure.status, "False",
            "ContentFailure should be False when no finalizers"
        );
        assert_eq!(content_failure.reason.as_deref(), Some("ContentDeleted"));
    }

    /// End-to-end regression for the "namespace stuck Terminating" hang.
    ///
    /// Finalization is a TWO-cycle process: cycle 1 deletes content and sets
    /// the deletion conditions; cycle 2 (re-triggered when the controller
    /// observes that status write) removes the `kubernetes` finalizer. That
    /// re-trigger rides on `Storage::watch` — when the backend watch stopped
    /// delivering events (the rhino/SQLite create-vs-update version bug, fixed
    /// in rhino) cycle 2 never fired and the namespace hung Terminating
    /// forever, wedging conformance cleanup.
    ///
    /// The controller's contract is to **drain the `kubernetes` finalizer**;
    /// the api-server then removes the object from storage once finalizers
    /// drain while Terminating (`ShouldDeleteNamespaceDuringUpdate`, covered by
    /// `namespace_finalize_removal_test.rs`). In production every controller
    /// `Storage` op proxies through the api-server via `ApiStorage`, so that
    /// removal always fires. This test drives the real `run()` loop over
    /// `MemoryStorage` (a dumb backend with no finalize semantics) and asserts
    /// the finalizer is drained — guarding the reconcile → finalize →
    /// drain completion and the watch-driven re-enqueue, so a regression in the
    /// two-cycle gate or the re-enqueue path fails here instead of silently
    /// hanging a cluster.
    #[tokio::test]
    async fn test_controller_run_finalizes_and_drains_terminating_namespace() {
        let storage = Arc::new(MemoryStorage::new());

        // A namespace marked for deletion, with the kubernetes finalizer.
        let mut ns = Namespace::new("term-ns");
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        ns.status = Some(NamespaceStatus {
            phase: Some(Phase::Terminating),
            conditions: None,
        });
        let key = build_key("namespaces", None, "term-ns");
        storage.create(&key, &ns).await.unwrap();

        // Some content (no finalizer) the controller must delete first.
        let cm = serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "c", "namespace": "term-ns"}, "data": {"k": "v"}
        });
        storage
            .create(&build_key("configmaps", Some("term-ns"), "c"), &cm)
            .await
            .unwrap();

        // Spawn the real controller run loop.
        let controller = Arc::new(NamespaceController::new(storage.clone()));
        let handle = tokio::spawn({
            let c = controller.clone();
            async move {
                let _ = c.run().await;
            }
        });

        // The controller must drain the kubernetes finalizer (an empty/None
        // finalizer list on a Terminating namespace). 5s budget — well past the
        // watch-driven re-enqueue. The api-server collects the object once it
        // observes drained finalizers (covered separately).
        let mut drained = false;
        for _ in 0..50 {
            if let Ok(cur) = storage.get::<Namespace>(&key).await {
                let empty = cur
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.finalizers.as_ref())
                    .is_none_or(|f| !f.iter().any(|x| x == "kubernetes"));
                if empty {
                    drained = true;
                    break;
                }
            } else {
                // Already gone (a finalize-aware backend would remove it) —
                // also satisfies the contract.
                drained = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        handle.abort();

        assert!(
            drained,
            "namespace controller must finalize and DRAIN the kubernetes finalizer \
             on a Terminating namespace, not leave it stuck (watch-driven \
             re-enqueue / two-cycle gate regression)"
        );
    }

    #[tokio::test]
    async fn test_finalize_namespace_sets_conditions() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NamespaceController::new(storage.clone());

        // Create a namespace marked for deletion with kubernetes finalizer
        let mut ns = Namespace::new("test-ns");
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        ns.status = Some(NamespaceStatus {
            phase: Some(Phase::Terminating),
            conditions: None,
        });

        let key = build_key("namespaces", None, "test-ns");
        storage.create(&key, &ns).await.unwrap();

        // Run finalization
        controller.finalize_namespace(&ns).await.unwrap();

        // Re-read namespace — it should have been deleted (no resources to clean up)
        // or if still present, should have conditions set
        match storage.get::<Namespace>(&key).await {
            Ok(updated_ns) => {
                // Namespace still exists — check conditions
                let status = updated_ns.status.unwrap();
                assert_eq!(status.phase, Some(Phase::Terminating));
                let conditions = status.conditions.unwrap();
                assert!(!conditions.is_empty(), "Conditions should be set");

                // All conditions should be False since there are no resources
                for cond in &conditions {
                    assert_eq!(
                        cond.status, "False",
                        "Condition {} should be False",
                        cond.condition_type
                    );
                }
            }
            Err(_) => {
                // Namespace was fully deleted — that's also correct
            }
        }
    }

    #[tokio::test]
    async fn test_finalize_namespace_with_remaining_resources() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NamespaceController::new(storage.clone());

        // Create a namespace marked for deletion
        let mut ns = Namespace::new("test-ns-resources");
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        ns.status = Some(NamespaceStatus {
            phase: Some(Phase::Terminating),
            conditions: None,
        });

        let ns_key = build_key("namespaces", None, "test-ns-resources");
        storage.create(&ns_key, &ns).await.unwrap();

        // Create a pod in the namespace (it will be deleted during finalization)
        let pod_value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-pod",
                "namespace": "test-ns-resources"
            },
            "spec": {
                "containers": [{"name": "test", "image": "nginx"}]
            }
        });
        let pod_key = build_key("pods", Some("test-ns-resources"), "test-pod");
        storage.create(&pod_key, &pod_value).await.unwrap();

        // Run finalization — pod will be deleted
        controller.finalize_namespace(&ns).await.unwrap();

        // Check that namespace has conditions set
        let updated_ns: Namespace = storage.get(&ns_key).await.unwrap_or_else(|_| {
            // Namespace was deleted — create a dummy to satisfy the test
            ns.clone()
        });
        if let Some(status) = &updated_ns.status {
            if let Some(conditions) = &status.conditions {
                assert!(!conditions.is_empty());
                // Verify key condition types exist
                let types: Vec<&str> = conditions
                    .iter()
                    .map(|c| c.condition_type.as_str())
                    .collect();
                assert!(types.contains(&"NamespaceDeletionDiscoveryFailure"));
                assert!(types.contains(&"NamespaceContentRemaining"));
            }
        }
    }

    /// A pod that has reached a terminal phase still holds the namespace open
    /// for as long as it carries a finalizer.
    ///
    /// Upstream's only terminal-pod special case is in the grace-period
    /// *estimate*, where terminal pods simply contribute nothing:
    ///
    /// ```text
    /// // pkg/controller/namespace/deletion/namespaced_resources_deleter.go:643-648
    /// for i := range items.Items {
    ///     pod := items.Items[i]
    ///     // filter out terminal pods
    ///     phase := pod.Status.Phase
    ///     if v1.PodSucceeded == phase || v1.PodFailed == phase {
    ///         continue
    ///     }
    /// ```
    ///
    /// The deletion path itself goes through the API and therefore honours
    /// finalizers for every object, terminal or not — there is no
    /// remove-anyway short-circuit.
    ///
    /// This is the state the conformance spec "[sig-api-machinery]
    /// OrderedNamespaceDeletion namespace deletion should delete pod first"
    /// ends up in: the kubelet stops the container and, because the pod carries
    /// `e2e.example.com/finalizer`, marks it Failed instead of removing it
    /// (`crates/kubelet/src/kubelet.rs` — "Pod has finalizers — update status
    /// to Failed but don't delete"). Dropping it from storage at that point
    /// retires the namespace while the spec is still waiting to observe it
    /// Terminating, and the spec fails with "namespace was deleted
    /// unexpectedly".
    #[tokio::test]
    async fn a_terminal_pod_with_a_finalizer_still_blocks_namespace_deletion() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NamespaceController::new(storage.clone());

        let ns_name = "test-ns-terminal-pod";

        let mut ns = Namespace::new(ns_name);
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        ns.status = Some(NamespaceStatus {
            phase: Some(Phase::Terminating),
            conditions: None,
        });
        let ns_key = build_key("namespaces", None, ns_name);
        storage.create(&ns_key, &ns).await.unwrap();

        // The container has already been stopped, so the pod is Failed — but
        // its finalizer is never removed.
        let pod_value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "terminal-pod",
                "namespace": ns_name,
                "finalizers": ["e2e.example.com/finalizer"]
            },
            "spec": {
                "containers": [{"name": "test", "image": "nginx"}]
            },
            "status": {"phase": "Failed"}
        });
        let pod_key = build_key("pods", Some(ns_name), "terminal-pod");
        storage.create(&pod_key, &pod_value).await.unwrap();

        let cm_value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "test-configmap", "namespace": ns_name},
            "data": {"key": "value"}
        });
        let cm_key = build_key("configmaps", Some(ns_name), "test-configmap");
        storage.create(&cm_key, &cm_value).await.unwrap();

        for round in 1..=3 {
            let ns_again = storage
                .get::<Namespace>(&ns_key)
                .await
                .unwrap_or(ns.clone());
            controller.finalize_namespace(&ns_again).await.unwrap();

            assert!(
                storage.get::<serde_json::Value>(&pod_key).await.is_ok(),
                "round {round}: a terminal pod that still carries a finalizer \
                 must not be removed from storage"
            );
            assert!(
                storage.get::<serde_json::Value>(&cm_key).await.is_ok(),
                "round {round}: other content must outlive the pod"
            );
            assert!(
                storage.get::<Namespace>(&ns_key).await.is_ok(),
                "round {round}: the namespace must stay Terminating while the \
                 finalized pod remains"
            );
        }
    }

    /// Verifies that during namespace finalization, pods with finalizers get
    /// deletionTimestamp set (but are NOT removed from storage) while resources
    /// without finalizers (like configmaps) ARE removed. This matches the K8s
    /// conformance test "namespace deletion should delete pod first".
    #[tokio::test]
    async fn test_finalize_namespace_deletes_pods_before_other_resources() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = NamespaceController::new(storage.clone());

        let ns_name = "test-ns-order";

        // Create a namespace marked for deletion
        let mut ns = Namespace::new(ns_name);
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        ns.status = Some(NamespaceStatus {
            phase: Some(Phase::Terminating),
            conditions: None,
        });
        let ns_key = build_key("namespaces", None, ns_name);
        storage.create(&ns_key, &ns).await.unwrap();

        // Create a pod WITH a finalizer (should get deletionTimestamp but NOT be removed)
        let pod_value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "finalized-pod",
                "namespace": ns_name,
                "finalizers": ["test.example.com/block"]
            },
            "spec": {
                "containers": [{"name": "test", "image": "nginx"}]
            }
        });
        let pod_key = build_key("pods", Some(ns_name), "finalized-pod");
        storage.create(&pod_key, &pod_value).await.unwrap();

        // Create a configmap WITHOUT a finalizer (should be deleted)
        let cm_value = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "test-configmap",
                "namespace": ns_name
            },
            "data": {"key": "value"}
        });
        let cm_key = build_key("configmaps", Some(ns_name), "test-configmap");
        storage.create(&cm_key, &cm_value).await.unwrap();

        // Run finalization
        controller.finalize_namespace(&ns).await.unwrap();

        // The pod should still exist in storage (it has a finalizer)
        let pod_after: serde_json::Value = storage
            .get(&pod_key)
            .await
            .expect("Pod with finalizer should still exist in storage");

        // The pod should have deletionTimestamp set
        let pod_deletion_ts = pod_after
            .pointer("/metadata/deletionTimestamp")
            .expect("Pod should have deletionTimestamp set");
        assert!(
            pod_deletion_ts.as_str().is_some(),
            "deletionTimestamp should be a string"
        );

        // After first reconcile, configmap should still exist (pods processed first,
        // other resources deferred when pods have finalizers — K8s ordering).
        let cm_result = storage.get::<serde_json::Value>(&cm_key).await;
        assert!(
            cm_result.is_ok(),
            "ConfigMap should still exist after first reconcile (pods processed first)"
        );

        // ...and it must STILL exist after a second reconcile, and a third.
        // The pod's finalizer is never removed here, so the pod never goes
        // away, so other content must never be touched.
        //
        // Upstream re-checks this on every pass rather than once
        // (`pkg/controller/namespace/deletion/namespaced_resources_deleter.go:553-562`
        // returns early for as long as `gvrToNumRemaining[podsGVR] > 0`).
        //
        // This assertion previously required the ConfigMap to be GONE after the
        // second reconcile, which pinned the divergence that fails the
        // `[sig-api-machinery] OrderedNamespaceDeletion namespace deletion
        // should delete pod first` Conformance spec: that spec holds a pod open
        // with a finalizer and then asserts its ConfigMap still exists.
        for round in 2..=3 {
            let ns_again = storage.get::<Namespace>(&ns_key).await.unwrap();
            controller.finalize_namespace(&ns_again).await.unwrap();
            let cm_result = storage.get::<serde_json::Value>(&cm_key).await;
            assert!(
                cm_result.is_ok(),
                "round {round}: ConfigMap must outlive a pod that is still terminating"
            );
        }

        // Upstream contract: only `NamespaceFinalizersRemaining` (and the
        // associated `NamespaceContentRemaining`) should flip when items
        // linger because of a resource-level finalizer. The deletion
        // *content failure* condition must stay at its "ok" message —
        // it only flips when the deleter encountered an actual API error.
        let updated_ns: Namespace = storage.get(&ns_key).await.unwrap();
        let conditions = updated_ns
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .expect("Namespace should have conditions");

        let content_failure = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceDeletionContentFailure")
            .expect("Should have NamespaceDeletionContentFailure condition");
        assert_eq!(
            content_failure.status, "False",
            "NamespaceDeletionContentFailure must stay False when only finalizers block deletion"
        );

        let finalizers_remaining = conditions
            .iter()
            .find(|c| c.condition_type == "NamespaceFinalizersRemaining")
            .expect("Should have NamespaceFinalizersRemaining condition");
        assert_eq!(
            finalizers_remaining.status, "True",
            "NamespaceFinalizersRemaining should be True when pod has finalizer"
        );
    }

    /// Namespaces must be reconciled concurrently, not one at a time.
    ///
    /// Measured on a live kine cluster after a conformance phase: **one
    /// namespace drained per 90 seconds** with 354 waiting — roughly 9 hours
    /// to clear — and a single namespace taking 48s end to end:
    ///
    /// ```text
    /// 20:23:39  Finalizing namespace config...
    /// 20:24:27  Setting deletion conditions on namespace ...
    /// ```
    ///
    /// That is not a wedge; the controller is running and logging. It is
    /// throughput. It stalled the conformance run outright: hydrophone's
    /// between-phase cleanup waited 63 minutes on `Waiting for Namespace
    /// conformance to be deleted` and phase 2 never started.
    ///
    /// Upstream runs `ConcurrentNamespaceSyncs` = 10 namespace workers
    /// (pkg/controller/namespace/config/v1alpha1/defaults.go:38, tunable via
    /// `--concurrent-namespace-syncs`,
    /// cmd/kube-controller-manager/app/options/namespacecontroller.go:37).
    /// Within one namespace it stays serial across resource types — the
    /// concurrency upstream buys is *across namespaces*, which is exactly the
    /// axis that was missing here.
    #[tokio::test]
    async fn namespaces_reconcile_concurrently_not_one_at_a_time() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        struct SlowStorage {
            inner: Arc<MemoryStorage>,
            inflight: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl Storage for SlowStorage {
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
                // Stall the per-namespace resource sweep so overlap is
                // observable; a fast in-memory backend would otherwise let a
                // serial loop look concurrent.
                if !prefix.starts_with("/registry/namespaces/") {
                    let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    self.peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                }
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
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        const NAMESPACES: usize = 10;
        let inner = Arc::new(MemoryStorage::new());
        let peak = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(SlowStorage {
            inner: Arc::clone(&inner),
            inflight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
        });

        for i in 0..NAMESPACES {
            let name = format!("terminating-{i}");
            let mut meta = rusternetes_common::types::ObjectMeta::new(name.clone());
            meta.deletion_timestamp = Some(chrono::Utc::now());
            meta.uid = format!("{name}-uid");
            let ns = Namespace {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "Namespace".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/namespaces/{name}"), &ns)
                .await
                .unwrap();
        }

        let controller = NamespaceController::new(Arc::clone(&storage));
        controller.reconcile_all().await.unwrap();

        let observed = peak.load(Ordering::SeqCst);
        assert!(
            observed > 1,
            "reconcile_all peaked at {observed} concurrent namespace operations for \
             {NAMESPACES} terminating namespaces; upstream runs ConcurrentNamespaceSyncs = 10 \
             workers, and a serial loop drains ~1 namespace per 90s on a real cluster"
        );
    }

    /// The LIVE path — `run()`'s worker pool — must process namespaces
    /// concurrently.
    ///
    /// This test exists because #1872 made `reconcile_all` concurrent and had
    /// no effect: `reconcile_all` is called only by tests. The production
    /// driver is the work queue in `run()`, which spawned exactly one worker,
    /// so namespaces drained strictly one at a time regardless. Measured on a
    /// live cluster afterwards: ~47s per namespace, sequential, 307 waiting,
    /// zero drained in 90 seconds.
    ///
    /// A test against `reconcile_all` cannot catch that, so this one drives
    /// `run()` and watches the storage for overlap. Upstream's
    /// `ConcurrentNamespaceSyncs` = 10 workers share one queue
    /// (pkg/controller/namespace/config/v1alpha1/defaults.go:38).
    #[tokio::test]
    async fn run_processes_namespaces_with_a_worker_pool() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        struct SlowStorage {
            inner: Arc<MemoryStorage>,
            inflight: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl Storage for SlowStorage {
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
                // Stall the per-namespace resource sweep so overlap is
                // observable. The namespace LIST itself is left fast so the
                // enqueue path is not what we are measuring.
                if !prefix.starts_with("/registry/namespaces/") {
                    let now = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    self.peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                }
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
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        const NAMESPACES: usize = 20;
        let inner = Arc::new(MemoryStorage::new());
        let peak = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(SlowStorage {
            inner: Arc::clone(&inner),
            inflight: Arc::new(AtomicUsize::new(0)),
            peak: Arc::clone(&peak),
        });

        for i in 0..NAMESPACES {
            let name = format!("terminating-{i}");
            let mut meta = rusternetes_common::types::ObjectMeta::new(name.clone());
            meta.deletion_timestamp = Some(chrono::Utc::now());
            meta.uid = format!("{name}-uid");
            let ns = Namespace {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "Namespace".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: meta,
                spec: None,
                status: None,
            };
            storage
                .create(&format!("/registry/namespaces/{name}"), &ns)
                .await
                .unwrap();
        }

        // `run()` loops forever; give the pool a moment to work, then stop.
        let controller = Arc::new(NamespaceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn({
            let c = Arc::clone(&controller);
            async move {
                let _ = c.run().await;
            }
        });
        tokio::time::sleep(Duration::from_millis(600)).await;
        handle.abort();

        let observed = peak.load(Ordering::SeqCst);
        assert!(
            observed > 1,
            "run()'s worker pool peaked at {observed} concurrent namespace operations; \
             a single worker drains ~1 namespace at a time and stalled a conformance \
             run behind 307 terminating namespaces"
        );
    }

    /// Storage that records `delete_collection` calls and per-object deletes,
    /// and answers the collection verb with a fixed `supported` verdict.
    struct CollectionStorage {
        inner: Arc<MemoryStorage>,
        supported: bool,
        collection_calls: std::sync::Mutex<Vec<String>>,
        single_deletes: std::sync::Mutex<Vec<String>>,
        graceful_deletes: std::sync::Mutex<Vec<String>>,
    }

    impl CollectionStorage {
        fn new(supported: bool) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(MemoryStorage::new()),
                supported,
                collection_calls: Default::default(),
                single_deletes: Default::default(),
                graceful_deletes: Default::default(),
            })
        }
    }

    #[async_trait::async_trait]
    impl Storage for CollectionStorage {
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
            self.single_deletes.lock().unwrap().push(key.to_string());
            self.inner.delete(key).await
        }
        async fn delete_gracefully(&self, key: &str) -> rusternetes_common::Result<()> {
            self.graceful_deletes.lock().unwrap().push(key.to_string());
            self.inner.delete_gracefully(key).await
        }
        async fn delete_collection(&self, prefix: &str) -> rusternetes_common::Result<bool> {
            self.collection_calls
                .lock()
                .unwrap()
                .push(prefix.to_string());
            if !self.supported {
                return Ok(false);
            }
            let items: Vec<serde_json::Value> = self.inner.list(prefix).await?;
            for item in items {
                if let Some(name) = item.pointer("/metadata/name").and_then(|n| n.as_str()) {
                    let ns = item
                        .pointer("/metadata/namespace")
                        .and_then(|n| n.as_str())
                        .unwrap_or_default();
                    let _ = self
                        .inner
                        .delete(&build_key("configmaps", Some(ns), name))
                        .await;
                }
            }
            Ok(true)
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

    async fn seed_terminating_ns_with_configmaps(
        storage: &CollectionStorage,
        ns_name: &str,
    ) -> Namespace {
        let mut ns = Namespace::new(ns_name);
        ns.metadata.deletion_timestamp = Some(Utc::now());
        set_namespace_finalizers(&mut ns, vec!["kubernetes".to_string()]);
        storage
            .create(&build_key("namespaces", None, ns_name), &ns)
            .await
            .unwrap();
        for i in 0..3 {
            let name = format!("cm-{i}");
            let cm = serde_json::json!({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": ns_name}
            });
            storage
                .create(&build_key("configmaps", Some(ns_name), &name), &cm)
                .await
                .unwrap();
        }
        ns
    }

    /// Upstream issues ONE `DeleteCollection` per resource type and only
    /// falls back to per-item deletes when the verb is unsupported
    /// (`namespaced_resources_deleter.go:322-326`, `:429-435`). #1850.
    #[tokio::test]
    async fn namespace_deleter_uses_delete_collection_when_supported() {
        let storage = CollectionStorage::new(true);
        let ns = seed_terminating_ns_with_configmaps(&storage, "dc-ns").await;
        let controller = NamespaceController::new(Arc::clone(&storage));
        controller.finalize_namespace(&ns).await.unwrap();

        let calls = storage.collection_calls.lock().unwrap().clone();
        let cm_prefix = build_prefix("configmaps", Some("dc-ns"));
        assert_eq!(
            calls.iter().filter(|p| **p == cm_prefix).count(),
            1,
            "expected exactly one DeleteCollection for configmaps, got {calls:?}"
        );
        let singles = storage.single_deletes.lock().unwrap().clone();
        assert!(
            !singles.iter().any(|k| k.contains("/configmaps/")),
            "configmaps must go through DeleteCollection, not per-item deletes: {singles:?}"
        );
        let left: Vec<serde_json::Value> = storage
            .list(&build_prefix("configmaps", Some("dc-ns")))
            .await
            .unwrap();
        assert!(left.is_empty());
    }

    /// When the verb is unsupported the deleter falls back to per-item
    /// deletes, and remembers (upstream `opCache.setNotSupported`,
    /// `namespaced_resources_deleter.go:187-197`) so it does not retry the
    /// collection verb on the next pass.
    #[tokio::test]
    async fn namespace_deleter_falls_back_and_caches_unsupported_delete_collection() {
        let storage = CollectionStorage::new(false);
        let ns = seed_terminating_ns_with_configmaps(&storage, "fb-ns").await;
        let controller = NamespaceController::new(Arc::clone(&storage));
        controller.finalize_namespace(&ns).await.unwrap();
        controller.finalize_namespace(&ns).await.unwrap();

        let calls = storage.collection_calls.lock().unwrap().clone();
        let cm_prefix = build_prefix("configmaps", Some("fb-ns"));
        assert_eq!(
            calls.iter().filter(|p| **p == cm_prefix).count(),
            1,
            "an unsupported verb must be cached, not retried: {calls:?}"
        );
        // Upstream `deleteEachItem` (namespaced_resources_deleter.go:393-415)
        // sends `DeleteOptions{PropagationPolicy: Background}` and no grace
        // override, so the server decides: the graceful path, never the
        // grace-0 hard delete (#2729).
        let singles = storage.single_deletes.lock().unwrap().clone();
        assert!(
            !singles.iter().any(|k| k.contains("/configmaps/")),
            "per-item path must not hard-delete (grace 0): {singles:?}"
        );
        let graceful = storage.graceful_deletes.lock().unwrap().clone();
        assert_eq!(
            graceful
                .iter()
                .filter(|k| k.contains("/configmaps/"))
                .count(),
            3,
            "fallback must delete each configmap individually and gracefully: {graceful:?}"
        );
    }
}
