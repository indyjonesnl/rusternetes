//! `fwk.Status`, `fwk.CycleState` and `fwk.NodeInfo` (`kube-scheduler/framework`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, NoHandle, NodeInfo, PreFilterResult, Status};
use rusternetes_scheduler::profile::Profile;

#[test]
fn status_predicates_follow_the_code() {
    assert!(Status::success().is_success());
    assert!(!Status::success().is_rejected());
    for c in [
        Code::Unschedulable,
        Code::UnschedulableAndUnresolvable,
        Code::Pending,
    ] {
        assert!(Status::new(c, vec![]).is_rejected(), "{}", c.as_str());
    }
    assert!(!Status::new(Code::Error, vec![]).is_rejected());
    assert!(Status::new(Code::Wait, vec![]).is_wait());
    assert!(Status::new(Code::Skip, vec![]).is_skip());
}

#[test]
fn as_error_is_none_for_success_wait_skip() {
    for c in [Code::Success, Code::Wait, Code::Skip] {
        assert_eq!(Status::new(c, vec!["x".into()]).as_error(), None);
    }
    assert_eq!(
        Status::new(Code::Unschedulable, vec!["a".into(), "b".into()]).as_error(),
        Some("a, b".to_string())
    );
    assert_eq!(Status::from_error("boom").as_error(), Some("boom".into()));
}

#[test]
fn reasons_put_the_error_first_and_message_joins() {
    let s = Status::from_error("boom");
    assert_eq!(s.code(), Code::Error);
    assert_eq!(s.message(), "boom");
    let s = Status::new(Code::Unschedulable, vec!["a".into(), "b".into()]);
    assert_eq!(s.message(), "a, b");
}

#[test]
fn cycle_state_round_trips_typed_values() {
    let mut st = CycleState::new();
    st.write("k", 5_i32);
    assert_eq!(st.read::<i32>("k"), Some(&5));
    assert_eq!(st.read::<String>("k"), None);
    assert_eq!(st.read::<i32>("missing"), None);
}

#[test]
fn node_info_pods_are_the_pods_bound_to_the_node() {
    let node: Node =
        serde_json::from_value(serde_json::json!({"metadata": {"name": "n"}})).unwrap();
    let mk = |name: &str, node: &str| -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name}, "spec": {"nodeName": node}
        }))
        .unwrap()
    };
    let pods = vec![mk("a", "n"), mk("b", "other"), mk("c", "n")];
    let info = NodeInfo::new(&node, &pods);
    let names: Vec<&str> = info.pods().map(|p| p.metadata.name.as_str()).collect();
    assert_eq!(names, vec!["a", "c"]);
}

fn res(names: &[&str]) -> Option<PreFilterResult> {
    Some(PreFilterResult {
        node_names: Some(names.iter().map(|s| s.to_string()).collect()),
    })
}

/// `TestPreFilterResultMerge` (`framework/interface_test.go`).
#[test]
fn pre_filter_result_merge() {
    let all = Some(PreFilterResult { node_names: None });
    // All nodes merged with all nodes is nil (all nodes).
    assert_eq!(PreFilterResult::merge(&None, &None), None);
    assert_eq!(PreFilterResult::merge(&all, &None), None);
    assert!(PreFilterResult::all_nodes(&None));
    assert!(PreFilterResult::all_nodes(&all));
    assert!(!PreFilterResult::all_nodes(&res(&[])));
    // One restricted side wins.
    assert_eq!(
        PreFilterResult::merge(&all, &res(&["a", "b"])),
        res(&["a", "b"])
    );
    assert_eq!(PreFilterResult::merge(&res(&["a"]), &None), res(&["a"]));
    // Two restricted sides intersect.
    assert_eq!(
        PreFilterResult::merge(&res(&["a", "b"]), &res(&["b", "c"])),
        res(&["b"])
    );
    assert_eq!(PreFilterResult::merge(&res(&["a"]), &res(&["b"])), res(&[]));
}

fn pod_with(spec: serde_json::Value) -> Pod {
    serde_json::from_value(serde_json::json!({"metadata": {"name": "p"}, "spec": spec})).unwrap()
}

/// `RunPreFilterPlugins`: a plugin that returns Skip is recorded in the
/// cycle state's skip set; the others are not.
#[tokio::test]
async fn run_pre_filter_plugins_records_skipped_plugins() {
    let fw = Profile::named("default-scheduler").framework;
    let mut state = CycleState::new();
    let pod = pod_with(serde_json::json!({"containers": [{"name": "c"}]}));
    let (result, status, with_nodes) = fw
        .run_pre_filter_plugins(&mut state, &pod, &[], &NoHandle)
        .await;
    assert!(result.is_none() && status.is_success() && with_nodes.is_empty());
    let skipped = state.skip_filter_plugins();
    assert!(skipped.contains("NodeAffinity"));
    assert!(skipped.contains("NodePorts"));
    assert!(!skipped.contains("NodeResourcesFit"));
}

/// A skipped plugin's Filter does not run: NodePorts would be an Error
/// without PreFilter state.
#[tokio::test]
async fn skipped_filters_do_not_run() {
    let fw = Profile::named("default-scheduler").framework;
    let mut state = CycleState::new();
    let pod = pod_with(serde_json::json!({"containers": [{"name": "c"}]}));
    let node: Node = serde_json::from_value(serde_json::json!({
        "metadata": {"name": "n"},
        "status": {"allocatable": {"cpu": "1", "pods": "10"}}
    }))
    .unwrap();
    fw.run_pre_filter_plugins(&mut state, &pod, std::slice::from_ref(&node), &NoHandle)
        .await;
    let pods: Vec<Pod> = vec![];
    let s = fw
        .run_filter_plugins(&state, &pod, &NodeInfo::new(&node, &pods))
        .await;
    assert!(s.is_success(), "{s:?}");
}

/// A PreFilter that rejects as UnschedulableAndUnresolvable ends the run and
/// carries the plugin name (`framework.go:889-896`).
#[tokio::test]
async fn run_pre_filter_plugins_stops_on_unresolvable() {
    let fw = Profile::named("default-scheduler").framework;
    let mut state = CycleState::new();
    let pod = pod_with(serde_json::json!({
        "containers": [{"name": "c"}],
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchFields": [
                {"key": "metadata.name", "operator": "In", "values": ["a"]},
                {"key": "metadata.name", "operator": "In", "values": ["b"]}
            ]}
        ]}}}
    }));
    let (result, status, _) = fw
        .run_pre_filter_plugins(&mut state, &pod, &[], &NoHandle)
        .await;
    assert!(result.is_none());
    assert_eq!(status.code(), Code::UnschedulableAndUnresolvable);
    assert_eq!(status.plugin(), "NodeAffinity");
}

/// A PreFilterResult names the plugins that restricted the nodes
/// (`pluginsWithNodes`).
#[tokio::test]
async fn run_pre_filter_plugins_reports_restricting_plugins() {
    let fw = Profile::named("default-scheduler").framework;
    let mut state = CycleState::new();
    let pod = pod_with(serde_json::json!({
        "containers": [{"name": "c"}],
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
            {"matchFields": [{"key": "metadata.name", "operator": "In", "values": ["a"]}]}
        ]}}}
    }));
    let (result, status, with_nodes) = fw
        .run_pre_filter_plugins(&mut state, &pod, &[], &NoHandle)
        .await;
    assert!(status.is_success());
    assert_eq!(result, self::res(&["a"]));
    assert_eq!(
        with_nodes,
        ["NodeAffinity".to_string()].into_iter().collect()
    );
}
