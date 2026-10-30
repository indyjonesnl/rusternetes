//! Unit tests moved from `src/queue/topology_spread_hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{Node, Pod};
    pub use rusternetes_common::types::{
        label_selector_as_selector, LabelSelector, LabelSelectorRequirement, Selector,
    };
    pub use rusternetes_scheduler::queue::events::*;
    pub use std::collections::HashMap;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::topology_spread_hints::*;
    use serde_json::{json, Value};

    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }

    fn pod(v: Value) -> Pod {
        serde_json::from_value(v).unwrap()
    }

    fn node(v: Value) -> Node {
        serde_json::from_value(v).unwrap()
    }

    fn constraint(key: &str, sel: &str, honor: Option<&str>) -> Value {
        let mut c = json!({
            "maxSkew": 1, "topologyKey": key, "whenUnsatisfiable": "DoNotSchedule",
            "labelSelector": {"matchExpressions": [{"key": sel, "operator": "Exists"}]}
        });
        if let Some(h) = honor {
            c["nodeTaintsPolicy"] = json!(h);
        }
        c
    }

    fn target(labels: Value, cs: Vec<Value>) -> Pod {
        pod(json!({
            "metadata": {"name": "p", "namespace": "default", "uid": "p", "labels": labels},
            "spec": {"topologySpreadConstraints": cs}
        }))
    }

    fn other(uid: &str, node: Option<&str>, ns: &str, labels: Value, tol: Option<&str>) -> Pod {
        let mut spec = json!({});
        if let Some(n) = node {
            spec["nodeName"] = json!(n);
        }
        if let Some(t) = tol {
            spec["tolerations"] = json!([{"key": t}]);
        }
        pod(json!({
            "metadata": {"name": uid, "namespace": ns, "uid": uid, "labels": labels},
            "spec": spec
        }))
    }

    fn run_pod(p: &Pod, old: Option<Pod>, new: Option<Pod>) -> Result<QueueingHint, String> {
        let o = old.map(obj);
        let n = new.map(obj);
        is_schedulable_after_pod_change(p, o.as_deref(), n.as_deref())
    }

    /// `Test_isSchedulableAfterPodChange` (`plugin_test.go:173-381`).
    #[test]
    fn is_schedulable_after_pod_change_table() {
        use QueueingHint::{Queue, QueueSkip};
        let foo = || constraint("zone", "foo", None);
        let bar = || constraint("node", "bar", None);
        let two = || target(json!({"foo": ""}), vec![foo(), bar()]);
        let one = || target(json!({"foo": ""}), vec![foo()]);
        let d = "default";
        let fake = Some("fake-node");
        type Case = (&'static str, Pod, Option<Pod>, Option<Pod>, QueueingHint);
        let cases: Vec<Case> = vec![
            (
                "add pod with labels match",
                one(),
                None,
                Some(other("p2", fake, d, json!({"foo": ""}), None)),
                Queue,
            ),
            (
                "add un-scheduled pod",
                one(),
                None,
                Some(other("p2", None, d, json!({"foo": ""}), None)),
                QueueSkip,
            ),
            (
                "update un-scheduled pod",
                one(),
                Some(other("p2", None, d, json!({"bar": ""}), None)),
                Some(other("p2", None, d, json!({"foo": ""}), None)),
                QueueSkip,
            ),
            (
                "delete un-scheduled pod",
                one(),
                Some(other("p2", None, d, json!({"foo": ""}), None)),
                None,
                QueueSkip,
            ),
            (
                "add pod with different namespace",
                one(),
                None,
                Some(other(
                    "p2",
                    fake,
                    "fake-namespace",
                    json!({"foo": ""}),
                    None,
                )),
                QueueSkip,
            ),
            (
                "add pod with labels don't match",
                one(),
                None,
                Some(other("p2", fake, d, json!({"bar": ""}), None)),
                QueueSkip,
            ),
            (
                "delete pod with labels that match",
                one(),
                Some(other("p2", fake, d, json!({"foo": ""}), None)),
                None,
                Queue,
            ),
            (
                "delete pod with labels that don't match",
                one(),
                Some(other("p2", fake, d, json!({"bar": ""}), None)),
                None,
                QueueSkip,
            ),
            (
                "update pod's non-related label",
                one(),
                Some(other(
                    "p2",
                    fake,
                    d,
                    json!({"foo": "", "bar": "bar1"}),
                    None,
                )),
                Some(other(
                    "p2",
                    fake,
                    d,
                    json!({"foo": "", "bar": "bar2"}),
                    None,
                )),
                QueueSkip,
            ),
            (
                "add pod's label that matches",
                one(),
                Some(other("p2", fake, d, json!({}), None)),
                Some(other("p2", fake, d, json!({"foo": ""}), None)),
                Queue,
            ),
            (
                "delete pod label that matches",
                one(),
                Some(other("p2", fake, d, json!({"foo": ""}), None)),
                Some(other("p2", fake, d, json!({}), None)),
                Queue,
            ),
            (
                "change pod's label that matches (value change only)",
                one(),
                Some(other("p2", fake, d, json!({"foo": "foo1"}), None)),
                Some(other("p2", fake, d, json!({"foo": "foo2"}), None)),
                QueueSkip,
            ),
            (
                "change pod's label that doesn't match",
                one(),
                Some(other(
                    "p2",
                    fake,
                    d,
                    json!({"foo": "", "bar": "bar1"}),
                    None,
                )),
                Some(other(
                    "p2",
                    fake,
                    d,
                    json!({"foo": "", "bar": "bar2"}),
                    None,
                )),
                QueueSkip,
            ),
            (
                "multi: add label matching second constraint",
                two(),
                Some(other("p2", fake, d, json!({"foo": ""}), None)),
                Some(other(
                    "p2",
                    fake,
                    d,
                    json!({"foo": "", "bar": "bar2"}),
                    None,
                )),
                Queue,
            ),
            (
                "multi: change unrelated label",
                two(),
                Some(other("p2", fake, d, json!({"foo": ""}), None)),
                Some(other("p2", fake, d, json!({"foo": "", "baz": ""}), None)),
                QueueSkip,
            ),
            (
                "multi: change matching label",
                two(),
                Some(other("p2", fake, d, json!({"foo": "", "bar": ""}), None)),
                Some(other(
                    "p2",
                    fake,
                    d,
                    json!({"foo": "", "bar": "bar2"}),
                    None,
                )),
                QueueSkip,
            ),
            (
                "target with NodeTaintsPolicy:Honor got a new toleration",
                target(
                    json!({"foo": ""}),
                    vec![constraint("zone", "foo", Some("Honor")), bar()],
                ),
                Some(other("p", None, d, json!({"foo": ""}), None)),
                Some(other(
                    "p",
                    None,
                    d,
                    json!({"foo": ""}),
                    Some("node.kubernetes.io/unschedulable"),
                )),
                Queue,
            ),
            (
                "target without NodeTaintsPolicy:Honor got a new toleration",
                target(
                    json!({"foo": ""}),
                    vec![constraint("zone", "foo", Some("Ignore")), bar()],
                ),
                Some(other("p", None, d, json!({"foo": ""}), None)),
                Some(other(
                    "p",
                    None,
                    d,
                    json!({"foo": ""}),
                    Some("node.kubernetes.io/unschedulable"),
                )),
                QueueSkip,
            ),
            (
                "target got a new label matching a constraint",
                target(
                    json!({"foo": ""}),
                    vec![constraint("zone", "foo", Some("Ignore")), bar()],
                ),
                Some(other("p", None, d, json!({}), None)),
                Some(other("p", None, d, json!({"foo": ""}), None)),
                Queue,
            ),
            (
                "target got a new unrelated label",
                target(
                    json!({"foo": ""}),
                    vec![constraint("zone", "foo", Some("Ignore")), bar()],
                ),
                Some(other("p", None, d, json!({}), None)),
                Some(other("p", None, d, json!({"unrelated": ""}), None)),
                QueueSkip,
            ),
        ];
        for (name, p, old, new, want) in cases {
            assert_eq!(run_pod(&p, old, new).unwrap(), want, "{name}");
        }
    }

    #[test]
    fn pod_hint_rejects_wrong_object_type() {
        let p = target(json!({}), vec![]);
        let bad = obj("not-a-pod".to_string());
        assert!(is_schedulable_after_pod_change(&p, Some(&*bad), None).is_err());
        assert!(is_schedulable_after_pod_change(&p, None, Some(&*bad)).is_err());
    }

    /// An invalid selector surfaces as an error (`getConstraints`), which the
    /// queue treats as Queue.
    #[test]
    fn invalid_selector_is_an_error() {
        let bad = json!({
            "maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule",
            "labelSelector": {"matchExpressions": [{"key": "a", "operator": "Bogus"}]}
        });
        let p = target(json!({}), vec![bad]);
        let n = obj(node(json!({"metadata": {"name": "n"}})));
        assert!(is_schedulable_after_node_change(&p, None, Some(&*n)).is_err());
    }

    /// `ScheduleAnyway` constraints are not hard, so a pod with only those
    /// never makes a hint Queue.
    #[test]
    fn soft_constraints_are_ignored() {
        let mut c = constraint("zone", "foo", None);
        c["whenUnsatisfiable"] = json!("ScheduleAnyway");
        let p = target(json!({"foo": ""}), vec![c]);
        let added = other("p2", Some("n"), "default", json!({"foo": ""}), None);
        assert_eq!(
            run_pod(&p, None, Some(added)).unwrap(),
            QueueingHint::QueueSkip
        );
    }

    /// `matchLabelKeys` are merged into the selector (`common.go:101-111`).
    #[test]
    fn match_label_keys_narrow_the_selector() {
        let c = json!({
            "maxSkew": 1, "topologyKey": "zone", "whenUnsatisfiable": "DoNotSchedule",
            "labelSelector": {"matchLabels": {"app": "x"}},
            "matchLabelKeys": ["rev"]
        });
        let p = target(json!({"app": "x", "rev": "1"}), vec![c]);
        let d = "default";
        let same = other("a", Some("n"), d, json!({"app": "x", "rev": "1"}), None);
        let diff = other("b", Some("n"), d, json!({"app": "x", "rev": "2"}), None);
        assert_eq!(run_pod(&p, None, Some(same)).unwrap(), QueueingHint::Queue);
        assert_eq!(
            run_pod(&p, None, Some(diff)).unwrap(),
            QueueingHint::QueueSkip
        );
    }

    fn run_node(p: &Pod, old: Option<Node>, new: Option<Node>) -> QueueingHint {
        let o = old.map(obj);
        let n = new.map(obj);
        is_schedulable_after_node_change(p, o.as_deref(), n.as_deref()).unwrap()
    }

    fn n(labels: Value, taints: Value) -> Node {
        node(json!({"metadata": {"name": "node-a", "labels": labels}, "spec": {"taints": taints}}))
    }

    /// `Test_isSchedulableAfterNodeChange` (`plugin_test.go:34-171`).
    #[test]
    fn is_schedulable_after_node_change_table() {
        use QueueingHint::{Queue, QueueSkip};
        let zone = || target(json!({}), vec![constraint("zone", "foo", None)]);
        let zone_node = || {
            target(
                json!({}),
                vec![
                    constraint("zone", "foo", None),
                    constraint("node", "foo", None),
                ],
            )
        };
        let nt = json!([]);
        let taint = |k: &str| json!([{"key": k, "value": "bbb", "effect": "NoSchedule"}]);
        type Case = (&'static str, Pod, Option<Node>, Option<Node>, QueueingHint);
        let cases: Vec<Case> = vec![
            (
                "node updates label which matches topologyKey",
                zone(),
                Some(n(json!({"zone": "zone1"}), nt.clone())),
                Some(n(json!({"zone": "zone2"}), nt.clone())),
                Queue,
            ),
            (
                "non-matching node updates non-related label",
                zone(),
                Some(n(json!({"foo": "bar1"}), nt.clone())),
                Some(n(json!({"foo": "bar2"}), nt.clone())),
                QueueSkip,
            ),
            (
                "matching node adds non-related label",
                zone_node(),
                Some(n(
                    json!({"zone": "zone1", "node": "node1", "foo": "bar"}),
                    nt.clone(),
                )),
                Some(n(json!({"zone": "zone1", "node": "node1"}), nt.clone())),
                QueueSkip,
            ),
            (
                "create node with non-related labels",
                zone(),
                None,
                Some(n(json!({"foo": "bar"}), nt.clone())),
                QueueSkip,
            ),
            (
                "create node with related labels",
                zone(),
                None,
                Some(n(json!({"zone": "zone1"}), nt.clone())),
                Queue,
            ),
            (
                "delete node with non-related labels",
                zone(),
                Some(n(json!({"foo": "bar"}), nt.clone())),
                None,
                QueueSkip,
            ),
            (
                "delete node with related labels",
                zone(),
                Some(n(json!({"zone": "zone1"}), nt.clone())),
                None,
                Queue,
            ),
            (
                "add node matching only one constraint",
                zone_node(),
                None,
                Some(n(json!({"zone": "zone1"}), nt.clone())),
                QueueSkip,
            ),
            (
                "add node matching all constraints",
                zone_node(),
                None,
                Some(n(json!({"zone": "zone1", "node": "node1"}), nt.clone())),
                Queue,
            ),
            (
                "update node matching only one constraint",
                zone_node(),
                Some(n(json!({"zone": "zone1"}), nt.clone())),
                Some(n(json!({"zone": "zone1"}), nt.clone())),
                QueueSkip,
            ),
            (
                "update node matching all constraints",
                zone_node(),
                Some(n(json!({"zone": "zone1", "node": "node1"}), nt.clone())),
                Some(n(json!({"zone": "zone1", "node": "node2"}), nt.clone())),
                Queue,
            ),
            (
                "different taints, matches all constraints",
                zone_node(),
                Some(n(json!({"zone": "zone1", "node": "node1"}), taint("aaa"))),
                Some(n(json!({"zone": "zone1", "node": "node1"}), taint("ccc"))),
                Queue,
            ),
            (
                "different taints, matches only one constraint",
                zone_node(),
                Some(n(json!({"node": "node1"}), taint("aaa"))),
                Some(n(json!({"node": "node1"}), taint("ccc"))),
                QueueSkip,
            ),
        ];
        for (name, p, old, new, want) in cases {
            assert_eq!(run_node(&p, old, new), want, "{name}");
        }
    }
}
