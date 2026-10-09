// RED tests for #2954: the DaemonSet ControllerRevision `data` must keep the
// explicit pointer zeros Go keeps.
//
// Upstream: pkg/controller/daemon/update.go getPatch() does json.Marshal(ds)
// of the typed apps.DaemonSet, so `*int64`/`*bool` fields with `omitempty`
// (PodSpec.TerminationGracePeriodSeconds, PodSecurityContext.RunAsUser,
// SecurityContext.Privileged; staging/src/k8s.io/api/core/v1/types.go:2749,
// 4826, 8197) are omitted only when nil: an explicit 0/false is kept.

use rusternetes_common::resources::pod::*;
use rusternetes_common::resources::*;
use rusternetes_common::types::{LabelSelector, ObjectMeta, TypeMeta};
use rusternetes_controller_manager::controllers::daemonset::DaemonSetController;
use rusternetes_storage::{build_key, memory::MemoryStorage, Storage};
use std::collections::HashMap;
use std::sync::Arc;

fn template(with_zeros: bool) -> PodTemplateSpec {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "zero".to_string());
    let mut meta = ObjectMeta::new("zero-pod");
    meta.labels = Some(labels);
    let mut spec = PodSpec {
        containers: vec![Container {
            name: "c".to_string(),
            image: "busybox:latest".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    if with_zeros {
        spec.termination_grace_period_seconds = Some(0);
        spec.security_context = Some(PodSecurityContext {
            run_as_user: Some(0),
            ..Default::default()
        });
        spec.containers[0].security_context = Some(SecurityContext {
            privileged: Some(false),
            ..Default::default()
        });
    }
    PodTemplateSpec {
        metadata: Some(meta),
        spec,
    }
}

fn daemonset(with_zeros: bool) -> DaemonSet {
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "zero".to_string());
    DaemonSet {
        type_meta: TypeMeta {
            kind: "DaemonSet".to_string(),
            api_version: "apps/v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new("zero-ds");
            m.namespace = Some("default".to_string());
            m.uid = uuid::Uuid::new_v4().to_string();
            m
        },
        spec: DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(labels),
                match_expressions: None,
            },
            template: template(with_zeros),
            update_strategy: None,
            min_ready_seconds: None,
            revision_history_limit: None,
        },
        status: None,
    }
}

fn assert_zeros_kept(data: &serde_json::Value) {
    let tmpl = &data["spec"]["template"]["spec"];
    assert_eq!(
        tmpl.get("terminationGracePeriodSeconds"),
        Some(&serde_json::json!(0)),
        "terminationGracePeriodSeconds: 0 dropped; data = {data}"
    );
    assert_eq!(
        tmpl["securityContext"].get("runAsUser"),
        Some(&serde_json::json!(0)),
        "securityContext.runAsUser: 0 dropped; data = {data}"
    );
    assert_eq!(
        tmpl["containers"][0]["securityContext"].get("privileged"),
        Some(&serde_json::json!(false)),
        "containers[0].securityContext.privileged: false dropped; data = {data}"
    );
}

/// Layer (a): the real patch builder (wraps strip_go_omitempty_zeros).
#[test]
fn build_patch_data_keeps_pointer_zeros() {
    let data = DaemonSetController::<MemoryStorage>::build_patch_data(&template(true)).unwrap();
    assert_zeros_kept(&data);
}

/// Layer (b): through the real reconcile, read the stored ControllerRevision.
#[tokio::test]
async fn reconciled_controller_revision_keeps_pointer_zeros() {
    let data = reconcile_and_read_data(true).await;
    assert_zeros_kept(&data);
}

/// Control: absent (nil) fields must stay absent.
#[tokio::test]
async fn reconciled_controller_revision_control_absent_stays_absent() {
    let data = reconcile_and_read_data(false).await;
    let tmpl = &data["spec"]["template"]["spec"];
    assert!(
        tmpl.get("terminationGracePeriodSeconds").is_none(),
        "{data}"
    );
    assert!(tmpl.get("securityContext").is_none(), "{data}");
    assert!(
        tmpl["containers"][0].get("securityContext").is_none(),
        "{data}"
    );
}

async fn reconcile_and_read_data(with_zeros: bool) -> serde_json::Value {
    let storage = Arc::new(MemoryStorage::new());
    let node = Node {
        type_meta: TypeMeta {
            kind: "Node".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: {
            let mut m = ObjectMeta::new("node-1");
            m.ensure_uid();
            m
        },
        spec: Some(NodeSpec {
            pod_cidr: None,
            pod_cidrs: None,
            provider_id: None,
            unschedulable: None,
            taints: None,
        }),
        status: None,
    };
    storage
        .create(&build_key("nodes", None, "node-1"), &node)
        .await
        .unwrap();
    let ds = daemonset(with_zeros);
    storage
        .create(&build_key("daemonsets", Some("default"), "zero-ds"), &ds)
        .await
        .unwrap();
    DaemonSetController::new(storage.clone())
        .reconcile_all()
        .await
        .unwrap();
    let revisions: Vec<ControllerRevision> = storage
        .list("/registry/controllerrevisions/default/")
        .await
        .unwrap();
    assert_eq!(revisions.len(), 1, "expected one ControllerRevision");
    revisions[0].data.clone().expect("revision has data")
}
