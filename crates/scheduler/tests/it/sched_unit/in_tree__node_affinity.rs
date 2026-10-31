//! Filter cases of `TestNodeAffinity` (`plugins/nodeaffinity/node_affinity_test.go`)
//! and the added-affinity cases of `TestNodeAffinityPriority`/`TestNodeAffinity`.
use rusternetes_common::resources::pod::NodeAffinity;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, FilterPlugin, NodeInfo, Status};
use rusternetes_scheduler::in_tree::{NodeAffinityPlugin, ERR_REASON_ENFORCED, ERR_REASON_POD};
use serde_json::{json, Value};

fn required(key: &str, op: &str, values: Value) -> Value {
    json!({"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
        {"matchExpressions": [{"key": key, "operator": op, "values": values}]}
    ]}})
}

async fn run(spec: Value, labels: Value, added: Option<Value>) -> Status {
    let pod: Pod =
        serde_json::from_value(json!({"metadata": {"name": "p"}, "spec": spec})).unwrap();
    let node: Node =
        serde_json::from_value(json!({"metadata": {"name": "n", "labels": labels}})).unwrap();
    let added: Option<NodeAffinity> = added.map(|v| serde_json::from_value(v).unwrap());
    let pods: Vec<Pod> = vec![];
    NodeAffinityPlugin::new(added.as_ref())
        .filter(&CycleState::new(), &pod, &NodeInfo::new(&node, &pods))
        .await
}

#[tokio::test]
async fn no_selector_matches() {
    assert!(run(json!({}), json!({}), None).await.is_success());
}

#[tokio::test]
async fn node_selector_matches_labels() {
    let s = run(
        json!({"nodeSelector": {"foo": "bar"}}),
        json!({"foo": "bar", "x": "y"}),
        None,
    )
    .await;
    assert!(s.is_success());
}

#[tokio::test]
async fn node_selector_mismatch_is_unresolvable() {
    let s = run(
        json!({"nodeSelector": {"foo": "bar"}}),
        json!({"foo": "baz"}),
        None,
    )
    .await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_POD.to_string()]);
}

#[tokio::test]
async fn node_selector_on_unlabeled_node_fails() {
    let s = run(json!({"nodeSelector": {"foo": "bar"}}), Value::Null, None).await;
    assert_eq!(s.reasons(), vec![ERR_REASON_POD.to_string()]);
}

#[tokio::test]
async fn required_affinity_in_matches() {
    let spec =
        json!({"affinity": {"nodeAffinity": required("foo", "In", json!(["bar", "value2"]))}});
    assert!(run(spec, json!({"foo": "bar"}), None).await.is_success());
}

#[tokio::test]
async fn required_affinity_not_in_rejects() {
    let spec = json!({"affinity": {"nodeAffinity": required("foo", "NotIn", json!(["bar"]))}});
    let s = run(spec, json!({"foo": "bar"}), None).await;
    assert_eq!(s.reasons(), vec![ERR_REASON_POD.to_string()]);
}

#[tokio::test]
async fn selector_and_affinity_must_both_match() {
    let spec = json!({
        "nodeSelector": {"foo": "bar"},
        "affinity": {"nodeAffinity": required("zone", "In", json!(["a"]))}
    });
    assert!(run(spec.clone(), json!({"foo": "bar", "zone": "a"}), None)
        .await
        .is_success());
    let s = run(spec, json!({"foo": "bar", "zone": "b"}), None).await;
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
}

#[tokio::test]
async fn added_affinity_is_enforced_first() {
    let added = json!(required("disk", "In", json!(["ssd"])));
    let s = run(json!({}), json!({"disk": "hdd"}), Some(added.clone())).await;
    assert_eq!(s.reasons(), vec![ERR_REASON_ENFORCED.to_string()]);
    assert!(run(json!({}), json!({"disk": "ssd"}), Some(added))
        .await
        .is_success());
}
