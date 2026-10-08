//! `podutil.DropDisabledTemplateFields` (pkg/api/pod/util.go:677) called from
//! every workload strategy's `PrepareForCreate` / `PrepareForUpdate` (#2699):
//! `pkg/registry/{apps/{deployment,replicaset,statefulset,daemonset},
//! batch/{job,cronjob},core/{replicationcontroller,podtemplate}}/strategy.go`.
//!
//! `GenericWorkload` (Alpha) is off in 1.35, so `spec.workloadRef`
//! (`dropDisabledWorkloadRef`, util.go:1836) must not survive in a template.

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

fn pod_spec() -> Value {
    json!({
        "restartPolicy": "Always",
        "workloadRef": {"name": "w", "podGroup": "g"},
        "containers": [{"name": "c", "image": "busybox"}]
    })
}

fn template() -> Value {
    json!({"metadata": {"labels": {"app": "x"}}, "spec": pod_spec()})
}

fn job_template() -> Value {
    let mut t = template();
    t["spec"]["restartPolicy"] = json!("Never");
    t
}

fn selector() -> Value {
    json!({"matchLabels": {"app": "x"}})
}

async fn assert_dropped(path: &str, body: Value, tmpl_path: &[&str]) {
    let api = TestApiServer::new();
    let (s, created) = api.post(path, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    let mut v = &created;
    for k in tmpl_path {
        v = &v[*k];
    }
    assert!(v.is_object(), "template missing at {tmpl_path:?}: {created}");
    assert!(v.get("workloadRef").is_none(), "{created}");
}

#[tokio::test]
async fn deployment_template_drops_workload_ref() {
    let body = json!({"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "d"},
        "spec": {"selector": selector(), "template": template()}});
    assert_dropped(
        "/apis/apps/v1/namespaces/default/deployments",
        body,
        &["spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn replicaset_template_drops_workload_ref() {
    let body = json!({"apiVersion": "apps/v1", "kind": "ReplicaSet",
        "metadata": {"name": "r"},
        "spec": {"selector": selector(), "template": template()}});
    assert_dropped(
        "/apis/apps/v1/namespaces/default/replicasets",
        body,
        &["spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn statefulset_template_drops_workload_ref() {
    let body = json!({"apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": {"name": "s"},
        "spec": {"serviceName": "s", "selector": selector(), "template": template()}});
    assert_dropped(
        "/apis/apps/v1/namespaces/default/statefulsets",
        body,
        &["spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn daemonset_template_drops_workload_ref() {
    let body = json!({"apiVersion": "apps/v1", "kind": "DaemonSet",
        "metadata": {"name": "ds"},
        "spec": {"selector": selector(), "template": template()}});
    assert_dropped(
        "/apis/apps/v1/namespaces/default/daemonsets",
        body,
        &["spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn job_template_drops_workload_ref() {
    let body = json!({"apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": "j"},
        "spec": {"template": job_template()}});
    assert_dropped(
        "/apis/batch/v1/namespaces/default/jobs",
        body,
        &["spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn cronjob_template_drops_workload_ref() {
    let body = json!({"apiVersion": "batch/v1", "kind": "CronJob",
        "metadata": {"name": "cj"},
        "spec": {"schedule": "* * * * *",
                 "jobTemplate": {"spec": {"template": job_template()}}}});
    assert_dropped(
        "/apis/batch/v1/namespaces/default/cronjobs",
        body,
        &["spec", "jobTemplate", "spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn replicationcontroller_template_drops_workload_ref() {
    let body = json!({"apiVersion": "v1", "kind": "ReplicationController",
        "metadata": {"name": "rc"},
        "spec": {"selector": {"app": "x"}, "template": template()}});
    assert_dropped(
        "/api/v1/namespaces/default/replicationcontrollers",
        body,
        &["spec", "template", "spec"],
    )
    .await;
}

#[tokio::test]
async fn podtemplate_drops_workload_ref() {
    let body = json!({"apiVersion": "v1", "kind": "PodTemplate",
        "metadata": {"name": "pt"}, "template": template()});
    assert_dropped(
        "/api/v1/namespaces/default/podtemplates",
        body,
        &["template", "spec"],
    )
    .await;
}

/// PrepareForUpdate: a PUT that adds the gated field is dropped too.
#[tokio::test]
async fn deployment_update_drops_workload_ref() {
    let api = TestApiServer::new();
    let path = "/apis/apps/v1/namespaces/default/deployments";
    let mut clean = template();
    clean["spec"].as_object_mut().unwrap().remove("workloadRef");
    let body = json!({"apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "u"},
        "spec": {"selector": selector(), "template": clean}});
    let (s, mut created) = api.post(path, &body).await;
    assert_eq!(s, StatusCode::CREATED, "{created}");
    created["spec"]["template"]["spec"]["workloadRef"] = json!({"name": "w", "podGroup": "g"});
    let (s, updated) = api.put(&format!("{path}/u"), &created).await;
    assert_eq!(s, StatusCode::OK, "{updated}");
    assert!(
        updated["spec"]["template"]["spec"]
            .get("workloadRef")
            .is_none(),
        "{updated}"
    );
}
