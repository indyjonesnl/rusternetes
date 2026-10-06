//! One test per controller, each driving the real `run()` (not
//! `reconcile_all`, which only tests call — see #1872) and asserting the
//! worker pool is wider than one.
use super::worker_pool::test_support::{meta, peak_concurrency, template};
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
            let _ = Arc::new(crate::controllers::deployment::DeploymentController::new(
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
            let _ = Arc::new(crate::controllers::replicaset::ReplicaSetController::new(
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
                crate::controllers::replicationcontroller::ReplicationControllerController::new(
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
            let _ = Arc::new(crate::controllers::daemonset::DaemonSetController::new(s))
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
            let _ = Arc::new(crate::controllers::endpoints::EndpointsController::new(s))
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
            let _ = Arc::new(crate::controllers::resource_quota::ResourceQuotaController::new(s))
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
            let _ = Arc::new(crate::controllers::hpa::HorizontalPodAutoscalerController::new(s))
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
            Arc::new(crate::controllers::ttl_controller::TTLController::new(s))
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
                crate::controllers::certificate_signing_request::CertificateSigningRequestController::new(s),
            )
            .run()
            .await;
        },
    )
    .await;
    assert!(peak > 1, "csr run() peaked at {peak}");
}
