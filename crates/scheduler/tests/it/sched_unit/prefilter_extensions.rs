//! `PreFilterExtensions` `AddPod`/`RemovePod` of InterPodAffinity and
//! PodTopologySpread, `CycleState.Clone`, and their use by nominated pods
//! (`addGENominatedPods`, framework.go:1192-1220) and the preemption dry run
//! (`SelectVictimsOnNode`, default_preemption.go:207-307).
//!
//! Ports of `TestPreFilterStateAddRemovePod` / `TestPreFilterStateClone`
//! (interpodaffinity/filtering_test.go) and `TestPreFilterStateAddPod` /
//! `TestPreFilterStateRemovePod` (podtopologyspread/filtering_test.go).
use super::listers::EmptyListers;
use k3rs_common::resources::{Node, Pod};
use k3rs_scheduler::default_preemption::{
    select_victims_on_node, select_victims_on_node_with_state,
};
use k3rs_scheduler::fwk::{
    Code, CycleState, NoHandle, NodeInfo, PreFilterExtensions, PreFilterPlugin,
};
use k3rs_scheduler::in_tree::{InterPodAffinity, PodTopologySpreadFilter};
use k3rs_scheduler::inter_pod_affinity_filter::{CountsSnapshot, PreFilterState as IpaState};
use k3rs_scheduler::pod_topology_spread_filter::PreFilterState as PtsState;
use k3rs_scheduler::profile::Profile;
use serde_json::{json, Value};
use std::collections::BTreeMap;

const IPA_KEY: &str = "PreFilterInterPodAffinity";
const PTS_KEY: &str = "PreFilterPodTopologySpread";

fn node(name: &str, labels: Value) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": name, "labels": labels},
        "status": {"allocatable": {"cpu": "64", "memory": "100Gi", "pods": "110"}}
    }))
    .unwrap()
}

fn pod(name: &str, ns: &str, node_name: &str, labels: Value, spec: Value) -> Pod {
    let mut spec = spec;
    if !node_name.is_empty() {
        spec["nodeName"] = json!(node_name);
    }
    spec["containers"] = json!([{"name": "c"}]);
    serde_json::from_value(json!({
        "metadata": {"name": name, "namespace": ns, "uid": name, "labels": labels},
        "spec": spec,
        "status": {"phase": "Running"}
    }))
    .unwrap()
}

fn node_of<'a>(nodes: &'a [Node], pods: &[Pod], name: &str) -> NodeInfo<'a> {
    // The pod list is irrelevant to the extensions; only the node is used.
    let _ = pods;
    NodeInfo::new(nodes.iter().find(|n| n.metadata.name == name).unwrap(), &[])
}

// ---- InterPodAffinity -----------------------------------------------------

fn anti_foo_bar() -> Value {
    json!({"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
        "labelSelector": {"matchExpressions": [{"key": "foo", "operator": "In", "values": ["bar"]}]},
        "topologyKey": "region"
    }]}})
}

fn anti_complex() -> Value {
    json!({"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
        {"labelSelector": {"matchExpressions": [{"key": "foo", "operator": "In", "values": ["bar", "buzz"]}]},
         "topologyKey": "region"},
        {"labelSelector": {"matchExpressions": [{"key": "service", "operator": "NotIn", "values": ["bar", "security", "test"]}]},
         "topologyKey": "zone"}
    ]}})
}

fn affinity_complex() -> Value {
    json!({"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
        {"labelSelector": {"matchExpressions": [{"key": "foo", "operator": "In", "values": ["bar", "buzz"]}]},
         "topologyKey": "region"},
        {"labelSelector": {"matchExpressions": [{"key": "service", "operator": "NotIn", "values": ["bar", "security", "test"]}]},
         "topologyKey": "zone"}
    ]}})
}

fn counts_of(state: &CycleState) -> (CountsSnapshot, CountsSnapshot, CountsSnapshot) {
    state.read::<IpaState>(IPA_KEY).unwrap().counts()
}

async fn ipa_state(pending: &Pod, nodes: &[Node], pods: &[Pod]) -> CycleState {
    let mut state = CycleState::new();
    let (_, s) = InterPodAffinity
        .pre_filter(&mut state, pending, nodes, pods, &EmptyListers)
        .await;
    assert!(s.is_success(), "prefilter failed: {s:?}");
    state
}

fn pairs(v: &[(&str, &str, i64)]) -> CountsSnapshot {
    v.iter()
        .map(|(k, val, n)| ((k.to_string(), val.to_string()), *n))
        .collect()
}

#[tokio::test]
#[allow(clippy::type_complexity)]
async fn ipa_pre_filter_state_add_remove_pod() {
    let nodes = vec![
        node("nodeA", json!({"region": "r1", "zone": "z11"})),
        node("nodeB", json!({"region": "r1", "zone": "z12"})),
        node("nodeC", json!({"region": "r2", "zone": "z21"})),
    ];
    let foo = json!({"foo": "bar"});
    let cases: Vec<(&str, Pod, Pod, Vec<Pod>, CountsSnapshot, CountsSnapshot)> = vec![
        (
            "anti-affinity terms are updated correctly after adding and removing a pod",
            pod("pending", "default", "", foo.clone(), json!({"affinity": anti_foo_bar()})),
            pod("addedPod", "default", "nodeB", foo.clone(), json!({"affinity": anti_foo_bar()})),
            vec![
                pod("p1", "default", "nodeA", foo.clone(), json!({})),
                pod("p2", "default", "nodeC", json!({}), json!({"affinity": anti_foo_bar()})),
            ],
            pairs(&[("region", "r1", 2)]),
            pairs(&[]),
        ),
        (
            "complex anti-affinity terms are updated correctly after adding and removing a pod",
            pod("pending", "default", "", foo.clone(), json!({"affinity": anti_complex()})),
            pod("addedPod", "default", "nodeA", foo.clone(), json!({"affinity": anti_complex()})),
            vec![
                pod("p1", "default", "nodeA", foo.clone(), json!({})),
                pod("p2", "default", "nodeC", json!({}), json!({"affinity": anti_foo_bar()})),
            ],
            pairs(&[("region", "r1", 2), ("zone", "z11", 2), ("zone", "z21", 1)]),
            pairs(&[]),
        ),
        (
            "matching pod affinity and anti-affinity are updated correctly after adding and removing a pod",
            pod("pending", "default", "", foo.clone(), json!({"affinity": affinity_complex()})),
            pod("addedPod", "default", "nodeA", foo.clone(), json!({"affinity": anti_complex()})),
            vec![
                pod("p1", "default", "nodeA", foo.clone(), json!({})),
                pod(
                    "p2",
                    "default",
                    "nodeC",
                    json!({}),
                    json!({"affinity": {
                        "podAntiAffinity": anti_foo_bar()["podAntiAffinity"],
                        "podAffinity": affinity_complex()["podAffinity"],
                    }}),
                ),
            ],
            pairs(&[]),
            pairs(&[("region", "r1", 2), ("zone", "z11", 2)]),
        ),
    ];
    for (name, pending, added, existing, want_anti, want_affinity) in cases {
        // The state produced when all pods, including the added one, are given
        // to PreFilter.
        let mut all = existing.clone();
        all.push(added.clone());
        let all_state = ipa_state(&pending, &nodes, &all).await;

        let mut state = ipa_state(&pending, &nodes, &existing).await;
        let original = counts_of(&state);
        let on = node_of(
            &nodes,
            &existing,
            added.spec.as_ref().unwrap().node_name.as_deref().unwrap(),
        );

        let s = InterPodAffinity.add_pod(&mut state, &pending, &added, &on);
        assert!(s.is_success(), "{name}: {s:?}");
        let (_, got_affinity, got_anti) = counts_of(&state);
        assert_eq!(got_anti, want_anti, "{name}: anti-affinity counts");
        assert_eq!(got_affinity, want_affinity, "{name}: affinity counts");
        assert_eq!(
            counts_of(&state),
            counts_of(&all_state),
            "{name}: equals all-pods state"
        );

        let s = InterPodAffinity.remove_pod(&mut state, &pending, &added, &on);
        assert!(s.is_success(), "{name}: {s:?}");
        assert_eq!(counts_of(&state), original, "{name}: restored after remove");
    }
}

/// `TestPreFilterStateClone`: the copy is equal but independent.
#[tokio::test]
async fn ipa_state_clone_is_independent() {
    let nodes = vec![node("a", json!({"zone": "z1"}))];
    let pending = pod(
        "p",
        "default",
        "",
        json!({"foo": "bar"}),
        json!({"affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
            {"labelSelector": {"matchLabels": {"foo": "bar"}}, "topologyKey": "zone"}
        ]}}}),
    );
    let mut state = ipa_state(&pending, &nodes, &[]).await;
    let clone = state.clone();
    assert_eq!(counts_of(&state), counts_of(&clone));
    let other = pod("o", "default", "a", json!({"foo": "bar"}), json!({}));
    let s = InterPodAffinity.add_pod(&mut state, &pending, &other, &node_of(&nodes, &[], "a"));
    assert!(s.is_success());
    assert_ne!(counts_of(&state), counts_of(&clone));
    assert!(
        counts_of(&clone).2.is_empty(),
        "the clone did not see the pod"
    );
}

/// A plugin whose PreFilter returned Skip has no state; the framework skips
/// its extensions (`GetSkipFilterPlugins`), and calling them directly is an
/// error status like upstream's `ErrNotFound`.
#[tokio::test]
async fn extensions_without_state_report_an_error() {
    let nodes = vec![node("a", json!({"zone": "z1"}))];
    let p = pod("p", "default", "", json!({}), json!({}));
    let mut state = CycleState::new();
    let s = InterPodAffinity.add_pod(&mut state, &p, &p, &node_of(&nodes, &[], "a"));
    assert_eq!(s.code(), Code::Error);

    let fw = Profile::named("default-scheduler").framework;
    let (_, pre, _) = fw
        .run_pre_filter_plugins(&mut state, &p, &nodes, &[], &EmptyListers)
        .await;
    assert!(pre.is_success());
    // IPA and PTS both Skipped: the framework-level extension is a no-op.
    let s = fw.run_pre_filter_extension_add_pod(&mut state, &p, &p, &node_of(&nodes, &[], "a"));
    assert!(s.is_success(), "{s:?}");
}

// ---- PodTopologySpread ------------------------------------------------------

fn foo_selector() -> Value {
    json!({"matchExpressions": [{"key": "foo", "operator": "Exists"}]})
}

fn spread(key: &str) -> Value {
    json!({"maxSkew": 1, "topologyKey": key, "whenUnsatisfiable": "DoNotSchedule",
           "labelSelector": foo_selector()})
}

fn pts_nodes(spec: &[(&str, &str)]) -> Vec<Node> {
    spec.iter()
        .map(|(n, z)| node(n, json!({"zone": z, "node": n})))
        .collect()
}

fn foo_pod(name: &str, ns: &str, node_name: &str) -> Pod {
    pod(name, ns, node_name, json!({"foo": ""}), json!({}))
}

type Paths = Vec<[(String, i64); 2]>;
type Nums = Vec<BTreeMap<String, i64>>;

fn p(v: &[(&str, i64)]) -> [(String, i64); 2] {
    [(v[0].0.to_string(), v[0].1), (v[1].0.to_string(), v[1].1)]
}

fn m(v: &[(&str, i64)]) -> BTreeMap<String, i64> {
    v.iter().map(|(k, n)| (k.to_string(), *n)).collect()
}

async fn pts_state(preemptor: &Pod, nodes: &[Node], pods: &[Pod]) -> CycleState {
    let mut state = CycleState::new();
    let (_, s) = PodTopologySpreadFilter::default()
        .pre_filter(&mut state, preemptor, nodes, pods, &EmptyListers)
        .await;
    assert!(s.is_success(), "prefilter failed: {s:?}");
    state
}

fn pts_of(state: &CycleState) -> (Paths, Nums) {
    let s = state.read::<PtsState>(PTS_KEY).unwrap();
    // Ties between the two paths are ordered by map iteration upstream too
    // (the test's `cmpopts` ignores it): compare each pair sorted.
    let sorted = |mut p: [(String, i64); 2]| {
        p.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        p
    };
    (
        s.critical_paths().into_iter().map(sorted).collect(),
        s.tp_value_to_match_num(),
    )
}

/// `TestPreFilterStateAddPod`.
#[tokio::test]
async fn pts_pre_filter_state_add_pod() {
    struct Case {
        name: &'static str,
        constraints: Vec<&'static str>,
        added: Pod,
        existing: Vec<Pod>,
        nodes: Vec<Node>,
        paths: Paths,
        nums: Nums,
    }
    let ab = pts_nodes(&[("node-a", "zone1"), ("node-b", "zone1")]);
    let ax = pts_nodes(&[("node-a", "zone1"), ("node-x", "zone2")]);
    let cases = vec![
        Case {
            name: "node a and b both impact current min match",
            constraints: vec!["node"],
            added: foo_pod("p-a1", "default", "node-a"),
            existing: vec![],
            nodes: ab.clone(),
            paths: vec![p(&[("node-b", 0), ("node-a", 1)])],
            nums: vec![m(&[("node-a", 1), ("node-b", 0)])],
        },
        Case {
            name: "only node a impacts current min match",
            constraints: vec!["node"],
            added: foo_pod("p-a1", "default", "node-a"),
            existing: vec![foo_pod("p-b1", "default", "node-b")],
            nodes: ab.clone(),
            paths: vec![p(&[("node-a", 1), ("node-b", 1)])],
            nums: vec![m(&[("node-a", 1), ("node-b", 1)])],
        },
        Case {
            name: "add a pod in a different namespace doesn't change topologyKeyToMinPodsMap",
            constraints: vec!["node"],
            added: foo_pod("p-a1", "ns1", "node-a"),
            existing: vec![foo_pod("p-b1", "default", "node-b")],
            nodes: ab.clone(),
            paths: vec![p(&[("node-a", 0), ("node-b", 1)])],
            nums: vec![m(&[("node-a", 0), ("node-b", 1)])],
        },
        Case {
            name: "add pod on non-critical node won't trigger re-calculation",
            constraints: vec!["node"],
            added: foo_pod("p-b2", "default", "node-b"),
            existing: vec![foo_pod("p-b1", "default", "node-b")],
            nodes: ab.clone(),
            paths: vec![p(&[("node-a", 0), ("node-b", 2)])],
            nums: vec![m(&[("node-a", 0), ("node-b", 2)])],
        },
        Case {
            name: "node a and x both impact topologyKeyToMinPodsMap on zone and node",
            constraints: vec!["zone", "node"],
            added: foo_pod("p-a1", "default", "node-a"),
            existing: vec![],
            nodes: ax.clone(),
            paths: vec![
                p(&[("zone2", 0), ("zone1", 1)]),
                p(&[("node-x", 0), ("node-a", 1)]),
            ],
            nums: vec![
                m(&[("zone1", 1), ("zone2", 0)]),
                m(&[("node-a", 1), ("node-x", 0)]),
            ],
        },
        Case {
            name: "only node a impacts topologyKeyToMinPodsMap on zone and node",
            constraints: vec!["zone", "node"],
            added: foo_pod("p-a1", "default", "node-a"),
            existing: vec![foo_pod("p-x1", "default", "node-x")],
            nodes: ax.clone(),
            paths: vec![
                p(&[("zone1", 1), ("zone2", 1)]),
                p(&[("node-a", 1), ("node-x", 1)]),
            ],
            nums: vec![
                m(&[("zone1", 1), ("zone2", 1)]),
                m(&[("node-a", 1), ("node-x", 1)]),
            ],
        },
    ];
    for c in cases {
        let constraints: Vec<Value> = c.constraints.iter().map(|k| spread(k)).collect();
        let preemptor = pod(
            "p",
            "default",
            "",
            json!({"foo": ""}),
            json!({"topologySpreadConstraints": constraints}),
        );
        let mut state = pts_state(&preemptor, &c.nodes, &c.existing).await;
        let on = c.added.spec.as_ref().unwrap().node_name.clone().unwrap();
        let s = PodTopologySpreadFilter::default().add_pod(
            &mut state,
            &preemptor,
            &c.added,
            &node_of(&c.nodes, &[], &on),
        );
        assert!(s.is_success(), "{}: {s:?}", c.name);
        assert_eq!(pts_of(&state), (c.paths, c.nums), "{}", c.name);
    }
}

/// `TestPreFilterStateRemovePod`.
#[tokio::test]
async fn pts_pre_filter_state_remove_pod() {
    let preemptor = pod(
        "p",
        "default",
        "",
        json!({"foo": ""}),
        json!({"topologySpreadConstraints": [spread("zone")]}),
    );
    // topologyKeyToMinPodsMap unchanged
    let nodes3 = pts_nodes(&[
        ("node-a", "zone1"),
        ("node-b", "zone1"),
        ("node-x", "zone2"),
    ]);
    let existing = vec![
        foo_pod("p-a1", "default", "node-a"),
        foo_pod("p-b1", "default", "node-b"),
        foo_pod("p-x1", "default", "node-x"),
    ];
    let mut state = pts_state(&preemptor, &nodes3, &existing).await;
    let s = PodTopologySpreadFilter::default().remove_pod(
        &mut state,
        &preemptor,
        &existing[0],
        &node_of(&nodes3, &[], "node-a"),
    );
    assert!(s.is_success());
    assert_eq!(
        pts_of(&state),
        (
            vec![p(&[("zone1", 1), ("zone2", 1)])],
            vec![m(&[("zone1", 1), ("zone2", 1)])]
        )
    );

    // topologyKeyToMinPodsMap changed
    let nodes4 = pts_nodes(&[
        ("node-a", "zone1"),
        ("node-b", "zone1"),
        ("node-x", "zone2"),
        ("node-y", "zone2"),
    ]);
    let existing = vec![
        foo_pod("p-a1", "default", "node-a"),
        foo_pod("p-b1", "default", "node-b"),
        foo_pod("p-x1", "default", "node-x"),
        foo_pod("p-y1", "default", "node-y"),
    ];
    let mut state = pts_state(&preemptor, &nodes4, &existing).await;
    PodTopologySpreadFilter::default().remove_pod(
        &mut state,
        &preemptor,
        &existing[0],
        &node_of(&nodes4, &[], "node-a"),
    );
    assert_eq!(
        pts_of(&state),
        (
            vec![p(&[("zone1", 1), ("zone2", 2)])],
            vec![m(&[("zone1", 1), ("zone2", 2)])]
        )
    );

    // delete an irrelevant pod won't help
    let irrelevant = pod("p-a0", "default", "node-a", json!({"bar": ""}), json!({}));
    let mut existing = existing;
    existing.insert(0, irrelevant.clone());
    let mut state = pts_state(&preemptor, &nodes4, &existing).await;
    PodTopologySpreadFilter::default().remove_pod(
        &mut state,
        &preemptor,
        &irrelevant,
        &node_of(&nodes4, &[], "node-a"),
    );
    assert_eq!(
        pts_of(&state),
        (
            vec![p(&[("zone1", 2), ("zone2", 2)])],
            vec![m(&[("zone1", 2), ("zone2", 2)])]
        )
    );
}

// ---- Nominated pods and the dry run ------------------------------------------

fn nominate(mut p: Pod, node_name: &str) -> Pod {
    p.status = Some(serde_json::from_value(json!({"nominatedNodeName": node_name})).unwrap());
    p
}

/// `addGENominatedPods`: a pod nominated to the node counts in PodTopologySpread's
/// first Filter pass (and not in the second).
#[tokio::test]
async fn nominated_pods_count_for_pod_topology_spread() {
    let fw = Profile::named("default-scheduler").framework;
    let nodes = pts_nodes(&[("node-a", "zone1"), ("node-b", "zone2")]);
    let candidate = pod(
        "p",
        "default",
        "",
        json!({"foo": ""}),
        json!({"priority": 10, "topologySpreadConstraints": [spread("zone")]}),
    );
    let existing = vec![
        foo_pod("p-a1", "default", "node-a"),
        foo_pod("p-b1", "default", "node-b"),
    ];
    let mut state = CycleState::new();
    let (_, s, _) = fw
        .run_pre_filter_plugins(&mut state, &candidate, &nodes, &existing, &EmptyListers)
        .await;
    assert!(s.is_success());

    let without = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &nodes[0],
            &existing,
            &nodes,
            vec![],
        )
        .await;
    assert!(without.is_success(), "1 + 1 - 1 <= maxSkew");

    let mut nominee = nominate(foo_pod("nom", "default", ""), "node-a");
    nominee.spec.as_mut().unwrap().priority = Some(10);
    let with = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &nodes[0],
            &existing,
            &nodes,
            vec![&nominee],
        )
        .await;
    assert!(
        with.is_rejected(),
        "the nominee counts toward node-a's zone"
    );
    assert_eq!(with.plugin(), "PodTopologySpread");
    // The caller's state was not patched: it is the clone that gained the pod.
    assert_eq!(pts_of(&state).1, vec![m(&[("zone1", 1), ("zone2", 1)])]);
}

/// `addGENominatedPods` for InterPodAffinity: a nominated pod's required
/// anti-affinity keeps the candidate out of its topology.
#[tokio::test]
async fn nominated_pods_count_for_inter_pod_affinity() {
    let fw = Profile::named("default-scheduler").framework;
    let nodes = vec![
        node("node-a", json!({"zone": "z1"})),
        node("node-b", json!({"zone": "z2"})),
    ];
    let candidate = pod(
        "p",
        "default",
        "",
        json!({"app": "web"}),
        json!({"priority": 10}),
    );
    let mut nominee = pod(
        "nom",
        "default",
        "",
        json!({}),
        json!({"priority": 10, "affinity": {"podAntiAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": [
            {"labelSelector": {"matchLabels": {"app": "web"}}, "topologyKey": "zone"}
        ]}}}),
    );
    nominee = nominate(nominee, "node-a");
    // An existing pod with required anti-affinity makes PreFilter write state;
    // the nominee's own terms only reach the state through AddPod.
    let existing = vec![pod(
        "e",
        "default",
        "node-b",
        json!({}),
        json!({"affinity": {"podAntiAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": [
            {"labelSelector": {"matchLabels": {"app": "web"}}, "topologyKey": "zone"}
        ]}}}),
    )];
    let mut state = CycleState::new();
    let (_, s, _) = fw
        .run_pre_filter_plugins(&mut state, &candidate, &nodes, &existing, &EmptyListers)
        .await;
    assert!(s.is_success());

    let without = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &nodes[0],
            &existing,
            &nodes,
            vec![],
        )
        .await;
    assert!(without.is_success());
    let with = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &nodes[0],
            &existing,
            &nodes,
            vec![&nominee],
        )
        .await;
    assert!(with.is_rejected());
    assert_eq!(with.plugin(), "InterPodAffinity");
    // node-b stays rejected by the existing pod either way.
    let other = fw
        .run_filter_plugins_with_nominated_pods(
            &state,
            &candidate,
            &nodes[1],
            &existing,
            &nodes,
            vec![],
        )
        .await;
    assert!(other.is_rejected());
}

fn victim_names(
    r: Result<k3rs_scheduler::default_preemption::NodeVictims, k3rs_scheduler::fwk::Status>,
) -> Vec<String> {
    r.unwrap_or_else(|s| panic!("{:?}: {}", s.code(), s.message()))
        .victims
        .iter()
        .map(|v| v.metadata.name.clone())
        .collect()
}

/// `SelectVictimsOnNode` patches the PreFilter state: removing the pod that a
/// required anti-affinity term matches lets the preemptor fit, adding it back
/// does not, while an unrelated low-priority pod is reprieved.
#[tokio::test]
async fn dry_run_removes_and_re_adds_pods_in_the_inter_pod_affinity_state() {
    let nodes = vec![
        node("node-a", json!({"zone": "z1"})),
        node("node-b", json!({"zone": "z2"})),
    ];
    let preemptor = pod(
        "p",
        "default",
        "",
        json!({"app": "p"}),
        json!({"priority": 100, "affinity": {"podAntiAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": [
            {"labelSelector": {"matchLabels": {"app": "web"}}, "topologyKey": "zone"}
        ]}}}),
    );
    let mut web = pod("web", "default", "node-a", json!({"app": "web"}), json!({}));
    web.spec.as_mut().unwrap().priority = Some(1);
    let mut other = pod(
        "other",
        "default",
        "node-a",
        json!({"app": "other"}),
        json!({}),
    );
    other.spec.as_mut().unwrap().priority = Some(1);
    let pods = vec![web, other];
    let fw = Profile::named("default-scheduler").framework;
    let r = select_victims_on_node(&fw, &NoHandle, &preemptor, &nodes[0], &pods, &nodes, &[]).await;
    assert_eq!(victim_names(r), vec!["web"]);
}

/// The same for PodTopologySpread: evicting a matching pod from the
/// over-populated zone lowers the skew, and the pod that does not match the
/// constraint's selector is reprieved.
#[tokio::test]
async fn dry_run_removes_and_re_adds_pods_in_the_pod_topology_spread_state() {
    let nodes = pts_nodes(&[("node-a", "zone1"), ("node-b", "zone2")]);
    let preemptor = pod(
        "p",
        "default",
        "",
        json!({"foo": ""}),
        json!({"priority": 100, "topologySpreadConstraints": [spread("zone")]}),
    );
    let mk = |name: &str, node_name: &str, labels: Value, prio: i32| {
        let mut x = pod(name, "default", node_name, labels, json!({}));
        x.spec.as_mut().unwrap().priority = Some(prio);
        x
    };
    // zone1 holds two matching pods, zone2 none: placing on node-a gives
    // 2 + 1 - 0 > maxSkew.
    let pods = vec![
        mk("s1", "node-a", json!({"foo": ""}), 1),
        mk("s2", "node-a", json!({"foo": ""}), 1),
        mk("bystander", "node-a", json!({"bar": ""}), 1),
    ];
    let fw = Profile::named("default-scheduler").framework;
    let r = select_victims_on_node(&fw, &NoHandle, &preemptor, &nodes[0], &pods, &nodes, &[]).await;
    let mut names = victim_names(r);
    names.sort();
    // Removing one of the two leaves 1 + 1 - 0 = 2 > 1, so both go.
    assert_eq!(names, vec!["s1", "s2"]);
}

/// `DryRunPreemption` hands each node a clone of the cycle's state: the dry
/// run over a successful PreFilter's state selects the same victims as the
/// one that runs PreFilter itself, and leaves the cycle's state untouched.
#[tokio::test]
async fn dry_run_on_a_clone_of_the_cycle_state_matches_and_does_not_mutate_it() {
    let nodes = pts_nodes(&[("node-a", "zone1"), ("node-b", "zone2")]);
    let preemptor = pod(
        "p",
        "default",
        "",
        json!({"foo": ""}),
        json!({"priority": 100, "topologySpreadConstraints": [spread("zone")]}),
    );
    let mk = |name: &str, labels: Value| {
        let mut x = pod(name, "default", "node-a", labels, json!({}));
        x.spec.as_mut().unwrap().priority = Some(1);
        x
    };
    let pods = vec![
        mk("s1", json!({"foo": ""})),
        mk("s2", json!({"foo": ""})),
        mk("bystander", json!({"bar": ""})),
    ];
    let fw = Profile::named("default-scheduler").framework;
    let mut cycle = CycleState::new();
    let (_, s, _) = fw
        .run_pre_filter_plugins(&mut cycle, &preemptor, &nodes, &pods, &NoHandle)
        .await;
    assert!(s.is_success());
    let before = pts_of(&cycle);

    let with = select_victims_on_node_with_state(
        &fw,
        &NoHandle,
        Some(&cycle),
        &preemptor,
        &nodes[0],
        &pods,
        &nodes,
        &[],
    )
    .await;
    let without =
        select_victims_on_node(&fw, &NoHandle, &preemptor, &nodes[0], &pods, &nodes, &[]).await;
    let (mut a, mut b) = (victim_names(with), victim_names(without));
    a.sort();
    b.sort();
    assert_eq!(a, vec!["s1", "s2"]);
    assert_eq!(a, b);
    assert_eq!(pts_of(&cycle), before, "the dry run patched a clone");
}
