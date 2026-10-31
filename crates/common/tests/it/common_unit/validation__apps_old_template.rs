//! Update validation derives the pod options from the OLD template
//! (`GetValidationOptionsFromPodTemplate(new, old)`):
//! pkg/registry/apps/{deployment:131,replicaset:144,daemonset:144,statefulset:163}/strategy.go
//! versus the create path (`..., nil`) at :85/:114/:119/:131. Mirrors the
//! RC test for #3947.
use rusternetes_common::feature_gates::{with_feature, Feature};
use rusternetes_common::resources::deployment::Deployment;
use rusternetes_common::resources::workloads::{DaemonSet, ReplicaSet, StatefulSet};
use rusternetes_common::validation::apps::*;
use serde_json::{json, Value};

const RELAXED: &str = "my_search.example.com";

fn template(search: &str) -> Value {
    json!({"metadata": {"labels": {"app": "x"}}, "spec": {
        "restartPolicy": "Always", "dnsPolicy": "ClusterFirst",
        "containers": [{"name": "c", "image": "x", "imagePullPolicy": "IfNotPresent",
            "terminationMessagePolicy": "File"}],
        "dnsConfig": {"searches": [search]}}})
}

fn deployment(search: &str) -> Deployment {
    serde_json::from_value(json!({
        "metadata": {"name": "d", "namespace": "ns", "resourceVersion": "1"},
        "spec": {"selector": {"matchLabels": {"app": "x"}}, "template": template(search)}
    }))
    .unwrap()
}

fn replicaset(search: &str) -> ReplicaSet {
    serde_json::from_value(json!({
        "metadata": {"name": "r", "namespace": "ns", "resourceVersion": "1"},
        "spec": {"selector": {"matchLabels": {"app": "x"}}, "template": template(search)}
    }))
    .unwrap()
}

fn daemonset(search: &str) -> DaemonSet {
    serde_json::from_value(json!({
        "metadata": {"name": "d", "namespace": "ns", "resourceVersion": "1"},
        "spec": {"selector": {"matchLabels": {"app": "x"}},
            "updateStrategy": {"type": "OnDelete"}, "template": template(search)}
    }))
    .unwrap()
}

fn statefulset(search: &str) -> StatefulSet {
    serde_json::from_value(json!({
        "metadata": {"name": "s", "namespace": "ns", "resourceVersion": "1"},
        "spec": {"serviceName": "svc", "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "RollingUpdate"},
            "selector": {"matchLabels": {"app": "x"}}, "template": template(search)}
    }))
    .unwrap()
}

#[test]
#[serial_test::serial]
fn deployment_update_derives_pod_options_from_old_template() {
    let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
    assert!(!validate_deployment(&deployment(RELAXED)).is_empty());
    assert!(validate_deployment_update(&deployment(RELAXED), &deployment(RELAXED)).is_empty());
    assert!(
        !validate_deployment_update(&deployment(RELAXED), &deployment("a.example.com")).is_empty()
    );
}

#[test]
#[serial_test::serial]
fn replicaset_update_derives_pod_options_from_old_template() {
    let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
    assert!(!validate_replicaset(&replicaset(RELAXED)).is_empty());
    assert!(validate_replicaset_update(&replicaset(RELAXED), &replicaset(RELAXED)).is_empty());
    assert!(
        !validate_replicaset_update(&replicaset(RELAXED), &replicaset("a.example.com")).is_empty()
    );
}

#[test]
#[serial_test::serial]
fn daemonset_update_derives_pod_options_from_old_template() {
    let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
    assert!(!validate_daemonset(&daemonset(RELAXED)).is_empty());
    let e = validate_daemonset_update(&daemonset(RELAXED), &daemonset(RELAXED));
    assert!(e.is_empty(), "{e:?}");
    assert!(
        !validate_daemonset_update(&daemonset(RELAXED), &daemonset("a.example.com")).is_empty()
    );
}

#[test]
#[serial_test::serial]
fn statefulset_update_derives_pod_options_from_old_template() {
    let _g = with_feature(Feature::RelaxedDNSSearchValidation, false);
    assert!(!validate_statefulset(&statefulset(RELAXED)).is_empty());
    assert!(validate_statefulset_update(&statefulset(RELAXED), &statefulset(RELAXED)).is_empty());
    assert!(
        !validate_statefulset_update(&statefulset(RELAXED), &statefulset("a.example.com"))
            .is_empty()
    );
}
