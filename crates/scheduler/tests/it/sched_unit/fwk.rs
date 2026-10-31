//! `fwk.Status`, `fwk.CycleState` and `fwk.NodeInfo` (`kube-scheduler/framework`).
use rusternetes_common::resources::{Node, Pod};
use rusternetes_scheduler::fwk::{Code, CycleState, NodeInfo, Status};

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
