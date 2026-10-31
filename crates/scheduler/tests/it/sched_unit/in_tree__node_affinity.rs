//! Filter cases of `TestNodeAffinity` (`plugins/nodeaffinity/node_affinity_test.go`)
//! and the added-affinity cases of `TestNodeAffinityPriority`/`TestNodeAffinity`.
use rusternetes_common::resources::pod::NodeAffinity;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{
    Code, CycleState, FilterPlugin, NodeInfo, PreFilterPlugin, PreFilterResult, Status,
};
use rusternetes_scheduler::in_tree::{
    NodeAffinityPlugin, ERR_REASON_CONFLICT, ERR_REASON_ENFORCED, ERR_REASON_POD,
};
use serde_json::{json, Value};
use std::collections::HashSet;

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

fn pod_of(spec: Value) -> Pod {
    serde_json::from_value(json!({"metadata": {"name": "p"}, "spec": spec})).unwrap()
}

fn match_fields(terms: Vec<Vec<(&str, Vec<&str>)>>) -> Value {
    let terms: Vec<Value> = terms
        .into_iter()
        .map(|reqs| {
            let reqs: Vec<Value> = reqs
                .into_iter()
                .map(|(k, vs)| json!({"key": k, "operator": "In", "values": vs}))
                .collect();
            json!({"matchFields": reqs})
        })
        .collect();
    json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": terms}}}})
}

async fn pre_filter(spec: Value) -> (Option<PreFilterResult>, Status) {
    NodeAffinityPlugin::new(None)
        .pre_filter(&mut CycleState::new(), &pod_of(spec), &[])
        .await
}

fn names(result: Option<PreFilterResult>) -> HashSet<String> {
    result.unwrap().node_names.unwrap()
}

fn set(n: &[&str]) -> HashSet<String> {
    n.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn pre_filter_skips_a_pod_with_no_affinity() {
    let (r, s) = pre_filter(json!({})).await;
    assert!(r.is_none() && s.is_skip());
}

#[tokio::test]
async fn pre_filter_skips_affinity_without_required_terms() {
    let (r, s) = pre_filter(json!({"affinity": {"nodeAffinity": {}}})).await;
    assert!(r.is_none() && s.is_skip());
}

#[tokio::test]
async fn added_affinity_alone_does_not_skip() {
    let added: NodeAffinity =
        serde_json::from_value(required("foo", "In", json!(["bar"]))).unwrap();
    let (r, s) = NodeAffinityPlugin::new(Some(&added))
        .pre_filter(&mut CycleState::new(), &pod_of(json!({})), &[])
        .await;
    assert!(r.is_none() && s.is_success());
}

#[tokio::test]
async fn pre_filter_match_fields_in_narrows_to_the_node() {
    let (r, s) = pre_filter(match_fields(vec![vec![("metadata.name", vec!["node1"])]])).await;
    assert!(s.is_success());
    assert_eq!(names(r), set(&["node1"]));
}

#[tokio::test]
async fn pre_filter_two_name_terms_are_unioned() {
    let (r, s) = pre_filter(match_fields(vec![
        vec![("metadata.name", vec!["node1"])],
        vec![("metadata.name", vec!["node2"])],
    ]))
    .await;
    assert!(s.is_success());
    assert_eq!(names(r), set(&["node1", "node2"]));
}

#[tokio::test]
async fn pre_filter_conflicting_requirements_in_one_term() {
    let (r, s) = pre_filter(match_fields(vec![vec![
        ("metadata.name", vec!["node1"]),
        ("metadata.name", vec!["node2"]),
    ]]))
    .await;
    assert!(r.is_none());
    assert_eq!(s.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(s.reasons(), vec![ERR_REASON_CONFLICT.to_string()]);
}

#[tokio::test]
async fn pre_filter_term_without_a_name_requirement_means_all_nodes() {
    // The terms are ORed, so a matchExpressions-only term admits any node.
    let spec = json!({"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
        {"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["node1"]}]},
        {"matchExpressions": [{"key": "foo", "operator": "In", "values": ["bar"]}]}
    ]}}}});
    let (r, s) = pre_filter(spec).await;
    assert!(r.is_none() && s.is_success());
}

#[tokio::test]
async fn filter_uses_the_state_pre_filter_wrote() {
    let pod = pod_of(json!({"nodeSelector": {"foo": "bar"}}));
    let node: Node =
        serde_json::from_value(json!({"metadata": {"name": "n", "labels": {"foo": "bar"}}}))
            .unwrap();
    let plugin = NodeAffinityPlugin::new(None);
    let mut state = CycleState::new();
    let (_, s) = plugin.pre_filter(&mut state, &pod, &[]).await;
    assert!(s.is_success());
    let pods: Vec<Pod> = vec![];
    assert!(plugin
        .filter(&state, &pod, &NodeInfo::new(&node, &pods))
        .await
        .is_success());
}
