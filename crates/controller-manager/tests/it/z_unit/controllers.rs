//! Unit tests moved from `src/controllers/mod.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_controller_core::{apiservice, clusterrole_aggregation};
}

mod gather_metrics_tests {
    #[test]
    fn serves_version_info_from_default_registry() {
        rusternetes_common::compat_version_metrics::record_compat_version_info(
            "kube", "1.35", "1.35", "1.34",
        );
        assert!(rusternetes_controller_manager::controllers::gather_metrics().contains(
            "version_info{binary=\"1.35\",component=\"kube\",emulation=\"1.35\",min_compat=\"1.34\"} 1"
        ));
    }
}

mod error_predicate_tests {
    use rusternetes_common::Error;
    use rusternetes_controller_manager::controllers::{
        is_already_exists, is_already_exists_anyhow,
    };

    #[test]
    fn typed_already_exists_is_matched() {
        assert!(is_already_exists(&Error::AlreadyExists("k".into())));
    }

    /// A non-AlreadyExists variant whose message merely mentions the token
    /// must not be classified benign (upstream branches on the Status reason,
    /// never on message text).
    #[test]
    fn other_variants_with_the_token_are_not_matched() {
        assert!(!is_already_exists(&Error::Storage(
            "backend: lease already exists in cache".into()
        )));
        assert!(!is_already_exists(&Error::BadRequest(
            "label AlreadyExists is reserved".into()
        )));
        assert!(!is_already_exists(&Error::Conflict(
            "resourceVersion mismatch; object already exists elsewhere".into()
        )));
    }

    #[test]
    fn anyhow_wrapped_variants_are_classified_by_variant() {
        let typed: anyhow::Error = Error::AlreadyExists("k".into()).into();
        assert!(is_already_exists_anyhow(&typed));
        let wrapped = anyhow::Error::from(Error::AlreadyExists("k".into())).context("creating pod");
        assert!(is_already_exists_anyhow(&wrapped));
        let other: anyhow::Error = Error::Storage("lease already exists in cache".into()).into();
        assert!(!is_already_exists_anyhow(&other));
        let plain = anyhow::anyhow!("pod already exists");
        assert!(!is_already_exists_anyhow(&plain));
    }

    #[test]
    fn untokened_non_matching_variants_are_not_matched() {
        assert!(!is_already_exists(&Error::NotFound("k".into())));
    }
}

mod worker_pool_tests {
    //! One test per controller, each driving the real `run()` (not
    //! `reconcile_all`, which only tests call — see #1872) and asserting the
    //! worker pool is wider than one.
    use rusternetes_controller_manager::controllers::worker_pool::test_support::{
        meta, peak_concurrency, template,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn workload(kind: &str, api: &str, i: usize, selector: serde_json::Value) -> serde_json::Value {
        json!({
            "apiVersion": api, "kind": kind, "metadata": meta(i),
            "spec": {"replicas": 1, "selector": selector, "template": template()}
        })
    }

    fn match_labels() -> serde_json::Value {
        json!({"matchLabels": {"a": "b"}})
    }

    #[tokio::test]
    async fn deployment_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "deployments",
            12,
            |i| workload("Deployment", "apps/v1", i, match_labels()),
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::deployment::DeploymentController::new(
                    s, 1,
                ))
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "deployment run() peaked at {peak}");
    }

    #[tokio::test]
    async fn replicaset_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "replicasets",
            12,
            |i| workload("ReplicaSet", "apps/v1", i, match_labels()),
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::replicaset::ReplicaSetController::new(
                    s, 1,
                ))
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "replicaset run() peaked at {peak}");
    }

    #[tokio::test]
    async fn replicationcontroller_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "replicationcontrollers",
            12,
            |i| workload("ReplicationController", "v1", i, json!({"a": "b"})),
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::replicationcontroller::ReplicationControllerController::new(
                        s, 1,
                    ),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "replicationcontroller run() peaked at {peak}");
    }

    #[tokio::test]
    async fn daemonset_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "daemonsets",
            12,
            |i| workload("DaemonSet", "apps/v1", i, match_labels()),
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::daemonset::DaemonSetController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "daemonset run() peaked at {peak}");
    }

    #[tokio::test]
    async fn endpoints_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "services",
            12,
            |i| {
                json!({
                    "apiVersion": "v1", "kind": "Service", "metadata": meta(i),
                    "spec": {"selector": {"a": "b"}, "ports": [{"port": 80}]}
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::endpoints::EndpointsController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "endpoints run() peaked at {peak}");
    }

    #[tokio::test]
    async fn resource_quota_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "resourcequotas",
            12,
            |i| {
                json!({
                    "apiVersion": "v1", "kind": "ResourceQuota", "metadata": meta(i),
                    "spec": {"hard": {"pods": "10"}}
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::resource_quota::ResourceQuotaController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "resource_quota run() peaked at {peak}");
    }

    #[tokio::test]
    async fn hpa_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "horizontalpodautoscalers",
            12,
            |i| {
                json!({
                    "apiVersion": "autoscaling/v2", "kind": "HorizontalPodAutoscaler",
                    "metadata": meta(i),
                    "spec": {
                        "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": "d"},
                        "minReplicas": 1, "maxReplicas": 3
                    }
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::hpa::HorizontalPodAutoscalerController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "hpa run() peaked at {peak}");
    }

    #[tokio::test]
    async fn ttl_after_finished_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "jobs",
            12,
            |i| {
                json!({
                    "apiVersion": "batch/v1", "kind": "Job", "metadata": meta(i),
                    "spec": {"template": template(), "ttlSecondsAfterFinished": 1000}
                })
            },
            |s| async move {
                Arc::new(
                    rusternetes_controller_manager::controllers::ttl_controller::TTLController::new(
                        s,
                    ),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "ttl run() peaked at {peak}");
    }

    #[tokio::test]
    async fn csr_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "certificatesigningrequests",
            12,
            |i| {
                json!({
                    "apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
                    "metadata": meta(i),
                    "spec": {"request": "", "signerName": "example.com/x", "usages": ["digital signature"]}
                })
            },
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::certificate_signing_request::CertificateSigningRequestController::new(s),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "csr run() peaked at {peak}");
    }

    #[tokio::test]
    async fn statefulset_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "statefulsets",
            12,
            |i| {
                let mut o = workload("StatefulSet", "apps/v1", i, match_labels());
                o["spec"]["serviceName"] = json!("svc");
                o
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::statefulset::StatefulSetController::new(
                    s,
                ))
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "statefulset run() peaked at {peak}");
    }

    #[tokio::test]
    async fn job_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "jobs",
            12,
            |i| {
                json!({
                    "apiVersion": "batch/v1", "kind": "Job", "metadata": meta(i),
                    "spec": {"selector": match_labels(), "template": template()}
                })
            },
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::job::JobController::new(s),
                )
                .run()
                .await;
            },
        )
        .await;
        // Job's startup path also `get`s each Job outside the worker (the orphan
        // sweep), so one worker can already show a peak of 2; a pool shows more.
        assert!(peak > 2, "job run() peaked at {peak}");
    }

    #[tokio::test]
    async fn cronjob_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "cronjobs",
            12,
            |i| {
                json!({
                    "apiVersion": "batch/v1", "kind": "CronJob", "metadata": meta(i),
                    "spec": {"schedule": "0 0 1 1 *", "jobTemplate": {"spec": {"template": template()}}}
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::cronjob::CronJobController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "cronjob run() peaked at {peak}");
    }

    #[tokio::test]
    async fn servicecidr_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "servicecidrs",
            12,
            |i| {
                json!({
                    "apiVersion": "networking.k8s.io/v1", "kind": "ServiceCIDR",
                    "metadata": meta(i),
                    "spec": {"cidrs": [format!("10.{i}.0.0/16")]}
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::servicecidr::ServiceCIDRController::new(
                    s,
                ))
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "servicecidr run() peaked at {peak}");
    }

    #[tokio::test]
    async fn apiservice_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "apiservices",
            12,
            |i| {
                json!({
                    "apiVersion": "apiregistration.k8s.io/v1", "kind": "APIService",
                    "metadata": meta(i),
                    "spec": {"group": "g", "version": "v1", "groupPriorityMinimum": 1, "versionPriority": 1}
                })
            },
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::apiservice::APIServiceAvailabilityController::new(s),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "apiservice run() peaked at {peak}");
    }

    #[tokio::test]
    async fn volume_expansion_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "persistentvolumeclaims",
            12,
            |i| {
                json!({
                    "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                    "metadata": meta(i),
                    "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}
                })
            },
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::volume_expansion::VolumeExpansionController::new(s),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "volume_expansion run() peaked at {peak}");
    }

    #[tokio::test]
    async fn resourceclaim_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "resourceclaims",
            12,
            |i| {
                json!({
                    "apiVersion": "resource.k8s.io/v1", "kind": "ResourceClaim",
                    "metadata": meta(i),
                    "spec": {}
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::resourceclaim::ResourceClaimController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "resourceclaim run() peaked at {peak}");
    }

    #[tokio::test]
    async fn node_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "nodes",
            12,
            |i| {
                json!({
                    "apiVersion": "v1", "kind": "Node",
                    "metadata": meta(i),
                    "spec": {}
                })
            },
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::node::NodeController::new(s),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "node run() peaked at {peak}");
    }

    /// Upstream runs `podUpdateWorkerSize = 4` pod workers (`Run`, :493-497)
    /// beside the node pool. Red before the split: the fused controller had no
    /// pod pool, so no pod `get` ever overlapped.
    #[tokio::test]
    async fn node_lifecycle_runs_a_pod_update_pool() {
        let peak = peak_concurrency(
            "pods",
            12,
            |i| {
                json!({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": meta(i),
                    "spec": {"nodeName": "n1", "containers": [{"name": "c", "image": "i"}]}
                })
            },
            |s| async move {
                let _ = Arc::new(
                    rusternetes_controller_manager::controllers::node::NodeController::new(s),
                )
                .run()
                .await;
            },
        )
        .await;
        assert!(peak > 1, "node-lifecycle pod pool peaked at {peak}");
    }

    #[tokio::test]
    async fn volume_snapshot_runs_a_worker_pool() {
        let peak = peak_concurrency(
            "volumesnapshots",
            12,
            |i| {
                json!({
                    "apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshot",
                    "metadata": meta(i),
                    "spec": {"source": {"persistentVolumeClaimName": "pvc"}, "volumeSnapshotClassName": "c"}
                })
            },
            |s| async move {
                let _ = Arc::new(rusternetes_controller_manager::controllers::volume_snapshot::VolumeSnapshotController::new(s))
                    .run()
                    .await;
            },
        )
        .await;
        assert!(peak > 1, "volume_snapshot run() peaked at {peak}");
    }
}
