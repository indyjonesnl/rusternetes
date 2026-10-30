//! Unit tests moved from `src/queue/hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{Node, NodeSelector, Pod};
    pub use rusternetes_scheduler::framework_runtime::names;
    pub use rusternetes_scheduler::profile::Profile;
    pub use rusternetes_scheduler::queue::events::*;
    pub use rusternetes_scheduler::queue::scheduling_queue::{
        QueueingHintFunction, QueueingHintMap, QueueingHintMapPerProfile,
    };
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::hints::*;
    use serde_json::json;

    fn pod_req(cpu: &str, mem: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "p"},
            "spec": {"containers": [{"name": "c", "resources": {"requests": {"cpu": cpu, "memory": mem}}}]}
        }))
        .unwrap()
    }

    fn assigned_pod(name: &str, cpu: &str, mem: &str, node: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": "ns", "uid": name},
            "spec": {"nodeName": node, "containers": [{"name": "c", "resources": {"requests": {"cpu": cpu, "memory": mem}}}]}
        }))
        .unwrap()
    }

    fn node(name: &str, cpu: &str, mem: &str) -> Node {
        serde_json::from_value(json!({
            "metadata": {"name": name},
            "status": {"allocatable": {"cpu": cpu, "memory": mem, "pods": "32"}}
        }))
        .unwrap()
    }

    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }

    fn nominated_pod(name: &str, cpu: &str, node: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": "ns", "uid": name},
            "spec": {"containers": [{"name": "c", "resources": {"requests": {"cpu": cpu}}}]},
            "status": {"nominatedNodeName": node}
        }))
        .unwrap()
    }

    /// `Test_isSchedulableAfterPodChange` (`fit_test.go:1484-1600`), the rows
    /// that do not need pod-level resources (`PodLevelResourceRequests` is
    /// not modelled by `pod_request_amounts`). InPlacePodVerticalScaling is
    /// GA locked-on in 1.35, so the disabled row is not ported.
    #[test]
    fn fit_is_schedulable_after_pod_change() {
        let target_1cpu = || {
            let mut p = pod_req("1", "0");
            p.metadata.uid = "uid0".into();
            p
        };
        let other = |cpu: &str, node: Option<&str>| {
            let mut p = assigned_pod("pod2", cpu, "0", node.unwrap_or(""));
            if node.is_none() {
                p.spec.as_mut().unwrap().node_name = None;
            }
            p.metadata.uid = "uid1".into();
            p
        };
        struct Case {
            name: &'static str,
            pod: Pod,
            old: Option<Box<EventObject>>,
            new: Option<Box<EventObject>>,
            expected: Result<QueueingHint, ()>,
        }
        let cases = vec![
            Case {
                name: "backoff-wrong-old-object",
                pod: pod_req("0", "0"),
                old: Some(obj("not-a-pod".to_string())),
                new: None,
                expected: Err(()),
            },
            Case {
                name: "backoff-wrong-new-object",
                pod: pod_req("0", "0"),
                old: None,
                new: Some(obj("not-a-pod".to_string())),
                expected: Err(()),
            },
            Case {
                name: "queue-on-other-pod-deleted",
                pod: target_1cpu(),
                old: Some(obj(other("1", Some("fake")))),
                new: None,
                expected: Ok(QueueingHint::Queue),
            },
            Case {
                name: "skip-queue-on-unscheduled-pod-deleted",
                pod: pod_req("0", "0"),
                old: Some(obj(other("0", None))),
                new: None,
                expected: Ok(QueueingHint::QueueSkip),
            },
            // fit.go:380: a deleted pod that was only *nominated* still
            // reserved room on its nominated node (preemptor deleted before
            // it was bound) → Queue.
            Case {
                name: "queue-on-nominated-pod-deleted",
                pod: target_1cpu(),
                old: Some(obj(nominated_pod("pod2", "1", "node-1"))),
                new: None,
                expected: Ok(QueueingHint::Queue),
            },
            Case {
                name: "skip-queue-on-other-unscheduled-pod",
                pod: target_1cpu(),
                old: Some(obj(other("2", None))),
                new: Some(obj(other("1", None))),
                expected: Ok(QueueingHint::QueueSkip),
            },
            Case {
                name: "skip-queue-on-other-pod-unrelated-resource-scaled-down",
                pod: target_1cpu(),
                old: Some(obj(assigned_pod("pod2", "0", "2", "fake"))),
                new: Some(obj(assigned_pod("pod2", "0", "1", "fake"))),
                expected: Ok(QueueingHint::QueueSkip),
            },
            Case {
                name: "queue-on-other-pod-some-resource-scale-down",
                pod: target_1cpu(),
                old: Some(obj(other("2", Some("fake")))),
                new: Some(obj(other("1", Some("fake")))),
                expected: Ok(QueueingHint::Queue),
            },
            // fit.go:413-416: a scale-down of the target pod itself queues it
            // even though it is (still) unscheduled.
            Case {
                name: "queue-on-target-pod-some-resource-scale-down",
                pod: target_1cpu(),
                old: Some(obj({
                    let mut p = pod_req("2", "0");
                    p.metadata.uid = "uid0".into();
                    p
                })),
                new: Some(obj(target_1cpu())),
                expected: Ok(QueueingHint::Queue),
            },
        ];
        for c in cases {
            let got = node_resources_fit::is_schedulable_after_pod_event(
                &c.pod,
                c.old.as_deref(),
                c.new.as_deref(),
            );
            match (c.expected, got) {
                (Err(()), Err(_)) => {}
                (Ok(want), Ok(got)) => assert_eq!(got, want, "{}", c.name),
                (want, got) => panic!("{}: want {want:?}, got {got:?}", c.name),
            }
        }
    }

    /// `Test_isSchedulableAfterNodeChange` (`fit_test.go:1602-1723`).
    #[test]
    fn fit_is_schedulable_after_node_change() {
        let target = pod_req("2", "2Gi");
        // backoff-wrong object → error
        assert!(node_resources_fit::is_schedulable_after_node_change(
            &target,
            None,
            Some(&*obj(pod_req("1", "1Gi")))
        )
        .is_err());
        // node add fits → queue
        assert_eq!(
            node_resources_fit::is_schedulable_after_node_change(
                &target,
                None,
                Some(&*obj(node("n", "4", "4Gi")))
            )
            .unwrap(),
            QueueingHint::Queue
        );
        // node add doesn't fit → skip
        assert_eq!(
            node_resources_fit::is_schedulable_after_node_change(
                &target,
                None,
                Some(&*obj(node("n", "1", "4Gi")))
            )
            .unwrap(),
            QueueingHint::QueueSkip
        );
        // allocatable increased for a requested resource → queue
        assert_eq!(
            node_resources_fit::is_schedulable_after_node_change(
                &target,
                Some(&*obj(node("n", "1", "4Gi"))),
                Some(&*obj(node("n", "4", "4Gi")))
            )
            .unwrap(),
            QueueingHint::Queue
        );
        // fits before and after, nothing increased → skip
        assert_eq!(
            node_resources_fit::is_schedulable_after_node_change(
                &target,
                Some(&*obj(node("n", "4", "4Gi"))),
                Some(&*obj(node("n", "4", "4Gi")))
            )
            .unwrap(),
            QueueingHint::QueueSkip
        );
        // increased an unrequested resource only → skip
        let mut n_old = node("n", "4", "4Gi");
        n_old
            .status
            .as_mut()
            .unwrap()
            .allocatable
            .as_mut()
            .unwrap()
            .insert("example.com/gpu".into(), "1".into());
        let mut n_new = n_old.clone();
        n_new
            .status
            .as_mut()
            .unwrap()
            .allocatable
            .as_mut()
            .unwrap()
            .insert("example.com/gpu".into(), "2".into());
        assert_eq!(
            node_resources_fit::is_schedulable_after_node_change(
                &target,
                Some(&*obj(n_old)),
                Some(&*obj(n_new))
            )
            .unwrap(),
            QueueingHint::QueueSkip
        );
        // allowed pods 0 → never fits
        let mut n0 = node("n", "4", "4Gi");
        n0.status
            .as_mut()
            .unwrap()
            .allocatable
            .as_mut()
            .unwrap()
            .insert("pods".into(), "0".into());
        assert_eq!(
            node_resources_fit::is_schedulable_after_node_change(&target, None, Some(&*obj(n0)))
                .unwrap(),
            QueueingHint::QueueSkip
        );
    }

    fn labelled_node(name: &str, labels: serde_json::Value) -> Node {
        serde_json::from_value(json!({"metadata": {"name": name, "labels": labels}})).unwrap()
    }

    fn affinity_pod(key: &str, value: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "p"},
            "spec": {"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{"matchExpressions": [{"key": key, "operator": "In", "values": [value]}]}]
            }}}}
        }))
        .unwrap()
    }

    /// `Test_isSchedulableAfterNodeChange` (`node_affinity_test.go:1237-1371`).
    #[test]
    fn node_affinity_is_schedulable_after_node_change() {
        let pod = affinity_pod("foo", "bar");
        let f = |added: Option<&NodeSelector>, old: Option<Node>, new: Node| {
            let old_b = old.map(obj);
            node_affinity::is_schedulable_after_node_change(
                added,
                &pod,
                old_b.as_deref(),
                Some(&*obj(new)),
            )
        };
        // backoff-wrong object → error
        assert!(node_affinity::is_schedulable_after_node_change(
            None,
            &pod,
            None,
            Some(&*obj(pod_req("1", "1Gi")))
        )
        .is_err());
        // add node without label → skip
        assert_eq!(
            f(None, None, labelled_node("n", json!({}))).unwrap(),
            QueueingHint::QueueSkip
        );
        // add node with the label → queue
        assert_eq!(
            f(None, None, labelled_node("n", json!({"foo": "bar"}))).unwrap(),
            QueueingHint::Queue
        );
        // update node to the label → queue
        assert_eq!(
            f(
                None,
                Some(labelled_node("n", json!({}))),
                labelled_node("n", json!({"foo": "bar"}))
            )
            .unwrap(),
            QueueingHint::Queue
        );
        // update node keeping the label → skip
        assert_eq!(
            f(
                None,
                Some(labelled_node("n", json!({"foo": "bar"}))),
                labelled_node("n", json!({"foo": "bar"}))
            )
            .unwrap(),
            QueueingHint::QueueSkip
        );
        // update node removing the label → skip
        assert_eq!(
            f(
                None,
                Some(labelled_node("n", json!({"foo": "bar"}))),
                labelled_node("n", json!({}))
            )
            .unwrap(),
            QueueingHint::QueueSkip
        );
        // addedAffinity not matched → skip even though pod affinity matches
        let added: NodeSelector = serde_json::from_value(json!({
            "nodeSelectorTerms": [{"matchExpressions": [{"key": "zone", "operator": "In", "values": ["a"]}]}]
        }))
        .unwrap();
        assert_eq!(
            f(
                Some(&added),
                None,
                labelled_node("n", json!({"foo": "bar"}))
            )
            .unwrap(),
            QueueingHint::QueueSkip
        );
        assert_eq!(
            f(
                Some(&added),
                None,
                labelled_node("n", json!({"foo": "bar", "zone": "a"}))
            )
            .unwrap(),
            QueueingHint::Queue
        );
    }

    fn gated_pod(uid: &str, gates: &[&str]) -> Pod {
        let gates: Vec<_> = gates.iter().map(|g| json!({"name": g})).collect();
        serde_json::from_value(json!({
            "metadata": {"name": "p", "uid": uid},
            "spec": {"containers": [], "schedulingGates": gates}
        }))
        .unwrap()
    }

    /// `Test_isSchedulableAfterPodChange` (`scheduling_gates_test.go:66-115`).
    #[test]
    fn scheduling_gates_is_schedulable_after_pod_change() {
        use scheduling_gates::is_schedulable_after_update_pod_scheduling_gates_eliminated as f;
        let pod = empty_pod();
        // backoff-wrong-old-object / backoff-wrong-new-object
        let any = obj(empty_pod());
        assert!(f(&pod, Some(&"not-a-pod"), Some(&*any)).is_err());
        assert!(f(&pod, None, Some(&"not-a-pod")).is_err());
        // skip-queue-on-other-pod-updated
        let target = gated_pod("uid0", &["foo", "bar"]);
        let old = obj(gated_pod("uid1", &["foo", "bar"]));
        let new = obj(gated_pod("uid1", &["foo"]));
        assert_eq!(
            f(&target, Some(&*old), Some(&*new)).unwrap(),
            QueueingHint::QueueSkip
        );
        // queue-on-the-unsched-pod-updated
        let target = gated_pod("", &["foo"]);
        let old = obj(gated_pod("", &["foo"]));
        let new = obj(gated_pod("", &[]));
        assert_eq!(
            f(&target, Some(&*old), Some(&*new)).unwrap(),
            QueueingHint::Queue
        );
    }

    // ---- NodePorts / NodeUnschedulable / TaintToleration -----------------

    fn host_port_pod(node: Option<&str>, port: Option<u16>) -> Pod {
        let mut spec = json!({"containers": [{"name": "c"}]});
        if let Some(n) = node {
            spec["nodeName"] = json!(n);
        }
        if let Some(p) = port {
            spec["containers"][0]["ports"] = json!([{"containerPort": p, "hostPort": p}]);
        }
        serde_json::from_value(json!({"metadata": {"name": "p"}, "spec": spec})).unwrap()
    }

    /// `Test_isSchedulableAfterPodDeleted` (`node_ports_test.go:223-273`).
    #[test]
    fn node_ports_is_schedulable_after_pod_deleted() {
        use node_ports::is_schedulable_after_pod_deleted as f;
        let target = host_port_pod(None, Some(8080));
        // backoff-wrong-old-object
        assert!(f(&target, Some(&"not-a-pod"), None).is_err());
        let cases = [
            (
                "skip-queue-on-unscheduled",
                host_port_pod(None, None),
                QueueingHint::QueueSkip,
            ),
            (
                "skip-queue-on-non-hostport",
                host_port_pod(Some("fake-node"), None),
                QueueingHint::QueueSkip,
            ),
            (
                "skip-queue-on-unrelated-hostport",
                host_port_pod(Some("fake-node"), Some(8081)),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-on-released-hostport",
                host_port_pod(Some("fake-node"), Some(8080)),
                QueueingHint::Queue,
            ),
        ];
        for (name, old, want) in cases {
            let old = obj(old);
            assert_eq!(f(&target, Some(&*old), None).unwrap(), want, "{name}");
        }
    }

    fn unsched_node(unschedulable: bool, taints: serde_json::Value) -> Node {
        serde_json::from_value(json!({
            "metadata": {"name": "n"},
            "spec": {"unschedulable": unschedulable, "taints": taints}
        }))
        .unwrap()
    }

    /// `TestIsSchedulableAfterNodeChange` (`node_unschedulable_test.go:93-211`).
    #[test]
    fn node_unschedulable_is_schedulable_after_node_change() {
        use node_unschedulable::is_schedulable_after_node_change as f;
        let pod = empty_pod();
        let none = json!([]);
        let not_ready = json!([{"key": "node.kubernetes.io/not-ready", "effect": "NoExecute"}]);
        // error-wrong-new-object / error-wrong-old-object
        assert!(f(&pod, None, Some(&"not-a-node")).is_err());
        let new = obj(unsched_node(true, none.clone()));
        assert!(f(&pod, Some(&"not-a-node"), Some(&*new)).is_err());
        let cases = [
            (
                "skip-queue-on-unschedulable-node-added",
                None,
                unsched_node(true, none.clone()),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-on-schedulable-node-added",
                None,
                unsched_node(false, none.clone()),
                QueueingHint::Queue,
            ),
            (
                "skip-unrelated-change-unschedulable-true",
                Some(unsched_node(true, none.clone())),
                unsched_node(true, not_ready.clone()),
                QueueingHint::QueueSkip,
            ),
            (
                "skip-unrelated-change-unschedulable-false",
                Some(unsched_node(false, none.clone())),
                unsched_node(false, not_ready.clone()),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-on-unschedulable-field-change",
                Some(unsched_node(true, none.clone())),
                unsched_node(false, none.clone()),
                QueueingHint::Queue,
            ),
        ];
        for (name, old, new, want) in cases {
            let old = old.map(obj);
            let new = obj(new);
            assert_eq!(
                f(&pod, old.as_deref(), Some(&*new)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    fn empty_pod() -> Pod {
        serde_json::from_value(json!({"metadata": {"name": "p"}, "spec": {"containers": []}}))
            .unwrap()
    }

    fn tol_pod(uid: &str, tolerations: serde_json::Value) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": uid},
            "spec": {"containers": [], "tolerations": tolerations}
        }))
        .unwrap()
    }

    /// `TestIsSchedulableAfterPodTolerationChange`
    /// (`node_unschedulable_test.go:214-272`).
    #[test]
    fn node_unschedulable_is_schedulable_after_pod_toleration_change() {
        use node_unschedulable::is_schedulable_after_pod_toleration_change as f;
        let pod = tol_pod("uid", json!([]));
        assert!(f(&pod, None, Some(&"not-a-pod")).is_err());
        let new = obj(empty_pod());
        assert!(f(&pod, Some(&"not-a-pod"), Some(&*new)).is_err());
        let unsched = json!([{"key": "node.kubernetes.io/unschedulable", "operator": "Exists"}]);
        let cases = [
            (
                "skip-different-pod-update",
                "different-uid",
                unsched.clone(),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-when-the-unsched-pod-gets-toleration",
                "uid",
                unsched.clone(),
                QueueingHint::Queue,
            ),
            (
                "skip-when-the-unsched-pod-gets-unrelated-toleration",
                "uid",
                json!([{"key": "unrelated-key", "operator": "Exists"}]),
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, uid, tols, want) in cases {
            let old = obj(tol_pod(uid, json!([])));
            let new = obj(tol_pod(uid, tols));
            assert_eq!(f(&pod, Some(&*old), Some(&*new)).unwrap(), want, "{name}");
        }
    }

    fn taint_node(taints: serde_json::Value) -> Node {
        serde_json::from_value(json!({"metadata": {"name": "nodeA"}, "spec": {"taints": taints}}))
            .unwrap()
    }

    fn taint(value: &str) -> serde_json::Value {
        json!({"key": "dedicated", "value": value, "effect": "NoSchedule"})
    }

    /// `TestIsSchedulableAfterNodeChange` (`taint_toleration_test.go:535-597`).
    #[test]
    fn taint_toleration_is_schedulable_after_node_change() {
        use taint_toleration::is_schedulable_after_node_change as f;
        let pod = tol_pod(
            "u",
            json!([{"key": "dedicated", "operator": "Equal", "value": "user2", "effect": "NoSchedule"}]),
        );
        assert!(f(&pod, None, Some(&"not-a-node")).is_err());
        let new = obj(taint_node(json!([taint("user1")])));
        assert!(f(&pod, Some(&"not-a-node"), Some(&*new)).is_err());
        let cases = [
            (
                "skip-queue-on-untoleratedtaint-node-added",
                None,
                json!([taint("user1")]),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-on-toleratedtaint-node-added",
                None,
                json!([taint("user2")]),
                QueueingHint::Queue,
            ),
            (
                "skip-unrelated-change",
                Some(json!([taint("user1")])),
                json!([taint("user1"), taint("user3")]),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-on-taint-change",
                Some(json!([taint("user1")])),
                json!([taint("user2")]),
                QueueingHint::Queue,
            ),
        ];
        for (name, old, new, want) in cases {
            let old = old.map(|t| obj(taint_node(t)));
            let new = obj(taint_node(new));
            assert_eq!(
                f(&pod, old.as_deref(), Some(&*new)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    /// `Test_isSchedulableAfterPodTolerationChange`
    /// (`taint_toleration_test.go:600-702`).
    #[test]
    fn taint_toleration_is_schedulable_after_pod_toleration_change() {
        use taint_toleration::is_schedulable_after_pod_toleration_change as f;
        let pod = empty_pod();
        assert!(f(&pod, None, Some(&"not-a-pod")).is_err());
        let new = obj(empty_pod());
        assert!(f(&pod, Some(&"not-a-pod"), Some(&*new)).is_err());
        let foo = json!([{"key": "foo", "effect": "NoSchedule"}]);
        // skip-updates-other-pod
        let target = tol_pod("uid0", json!([]));
        let old = obj(tol_pod("uid1", json!([])));
        let new = obj(tol_pod("uid1", foo.clone()));
        assert_eq!(
            f(&target, Some(&*old), Some(&*new)).unwrap(),
            QueueingHint::QueueSkip
        );
        // queue-on-toleration-added
        let target = tol_pod("uid0", json!([]));
        let old = obj(tol_pod("uid0", json!([])));
        let new = obj(tol_pod("uid0", foo));
        assert_eq!(
            f(&target, Some(&*old), Some(&*new)).unwrap(),
            QueueingHint::Queue
        );
    }

    /// `Test_buildQueueingHintMap` (`scheduler_test.go:328-449`), the
    /// UpdateNodeTaint rows.
    #[test]
    fn build_queueing_hint_map_registers_node_taint_for_node_add() {
        let no_hint = vec![(
            "fakeNodePlugin".to_string(),
            vec![ClusterEventWithHint::new(ClusterEvent::new(
                EventResource::Node,
                ActionType::ADD,
            ))],
        )];
        let m = build_queueing_hint_map(&no_hint);
        let taint = ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_TAINT);
        assert_eq!(m.len(), 2);
        assert_eq!(m[&taint].len(), 1);
        assert_eq!(m[&taint][0].plugin_name, "fakeNodePlugin");
        // registers taint itself → not duplicated
        let with_taint = vec![(
            "fakeNodePlugin".to_string(),
            vec![ClusterEventWithHint::new(ClusterEvent::new(
                EventResource::Node,
                ActionType::ADD | ActionType::UPDATE_NODE_TAINT,
            ))],
        )];
        let m = build_queueing_hint_map(&with_taint);
        assert_eq!(m.len(), 1);
        // non-node plugin: nothing extra
        let pod_only = vec![(
            "fakePodPlugin".to_string(),
            vec![ClusterEventWithHint::new(ClusterEvent::new(
                EventResource::Pod,
                ActionType::ADD,
            ))],
        )];
        assert_eq!(build_queueing_hint_map(&pod_only).len(), 1);
        // nil hint fn → default (Queue)
        let dummy = pod_req("1", "1Gi");
        let f = &m[&ClusterEvent::new(
            EventResource::Node,
            ActionType::ADD | ActionType::UPDATE_NODE_TAINT,
        )][0];
        assert_eq!(
            (f.queueing_hint_fn)(&dummy, None, None).unwrap(),
            QueueingHint::Queue
        );
    }

    // ---- InterPodAffinity (`plugin_test.go`) ----

    #[derive(Clone, Copy)]
    enum Kind {
        Affinity,
        AntiAffinity,
    }

    fn selector_in(key: &str, values: &[&str]) -> serde_json::Value {
        json!({"matchExpressions": [{"key": key, "operator": "In", "values": values}]})
    }

    /// `st.MakePod().Name("p")` with the given required terms appended.
    fn ipa_pod(terms: Vec<(Kind, serde_json::Value, &str)>) -> Pod {
        let mut aff: Vec<serde_json::Value> = vec![];
        let mut anti: Vec<serde_json::Value> = vec![];
        for (kind, sel, tk) in terms {
            let t = json!({"labelSelector": sel, "topologyKey": tk});
            match kind {
                Kind::Affinity => aff.push(t),
                Kind::AntiAffinity => anti.push(t),
            }
        }
        let mut affinity = serde_json::Map::new();
        if !aff.is_empty() {
            affinity.insert(
                "podAffinity".into(),
                json!({"requiredDuringSchedulingIgnoredDuringExecution": aff}),
            );
        }
        if !anti.is_empty() {
            affinity.insert(
                "podAntiAffinity".into(),
                json!({"requiredDuringSchedulingIgnoredDuringExecution": anti}),
            );
        }
        serde_json::from_value(json!({
            "metadata": {"name": "p"},
            "spec": {"affinity": affinity}
        }))
        .unwrap()
    }

    fn labelled_pod(node: Option<&str>, labels: &[(&str, &str)]) -> Pod {
        let labels: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect();
        let mut spec = serde_json::Map::new();
        if let Some(n) = node {
            spec.insert("nodeName".into(), json!(n));
        }
        serde_json::from_value(json!({
            "metadata": {"labels": labels},
            "spec": spec
        }))
        .unwrap()
    }

    fn named_node(labels: &[(&str, &str)]) -> Node {
        let labels: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect();
        serde_json::from_value(json!({"metadata": {"name": "node-a", "labels": labels}})).unwrap()
    }

    /// `Test_isSchedulableAfterPodChange` (`plugin_test.go:37-151`).
    #[test]
    fn inter_pod_affinity_is_schedulable_after_pod_change() {
        use Kind::*;
        let svc_aff = || {
            ipa_pod(vec![(
                Affinity,
                selector_in("service", &["securityscan", "value2"]),
                "region",
            )])
        };
        let svc_anti = || {
            ipa_pod(vec![(
                AntiAffinity,
                selector_in("service", &["securityscan", "value2"]),
                "region",
            )])
        };
        let exists = |k: &str| json!({"matchExpressions": [{"key": k, "operator": "Exists"}]});
        let two_terms = || {
            ipa_pod(vec![
                (Affinity, exists("aaa"), "hostname"),
                (Affinity, exists("bbb"), "hostname"),
            ])
        };
        let n = Some("fake-node");
        type PodCase<'a> = (&'a str, Pod, Option<Pod>, Option<Pod>, QueueingHint);
        let cases: Vec<PodCase> = vec![
            (
                "add a pod which matches the pod affinity",
                svc_aff(),
                None,
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::Queue,
            ),
            (
                "add an un-scheduled pod",
                svc_aff(),
                None,
                Some(labelled_pod(None, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "add a pod which doesn't match the pod affinity",
                svc_aff(),
                None,
                Some(labelled_pod(n, &[("aaa", "a")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from non-match to match pod affinity",
                svc_aff(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::Queue,
            ),
            (
                "the updating pod matches target pod's affinity both before and after label changes",
                svc_aff(),
                Some(labelled_pod(n, &[("service", "value2")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update an un-scheduled pod",
                svc_aff(),
                Some(labelled_pod(None, &[("service", "securityscan")])),
                Some(labelled_pod(None, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from match to non-match the pod affinity",
                svc_aff(),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                Some(labelled_pod(n, &[("aaa", "a")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from match to non-match pod's affinity - multiple terms case",
                two_terms(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from non-match to match pod's affinity - multiple terms case",
                two_terms(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                Some(labelled_pod(n, &[("aaa", ""), ("bbb", "")])),
                QueueingHint::Queue,
            ),
            (
                "modify pod label to change it from satisfying pod anti-affinity to not satisfying anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                Some(labelled_pod(n, &[("service", "aaaa")])),
                QueueingHint::Queue,
            ),
            (
                "modify pod label to change it from not satisfying pod anti-affinity to satisfying anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("service", "bbb")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "delete a pod which doesn't match pod's anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                None,
                QueueingHint::QueueSkip,
            ),
            (
                "delete a pod which matches pod's anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                None,
                QueueingHint::Queue,
            ),
        ];
        for (name, pod, old, new, want) in cases {
            let old_b = old.map(obj);
            let new_b = new.map(obj);
            let got = inter_pod_affinity::is_schedulable_after_pod_change(
                &pod,
                old_b.as_deref(),
                new_b.as_deref(),
            )
            .unwrap();
            assert_eq!(got, want, "{name}");
        }
        // util.As error path: a wrong-typed object is an error (backoff).
        assert!(inter_pod_affinity::is_schedulable_after_pod_change(
            &svc_aff(),
            None,
            Some(&*obj("not-a-pod".to_string()))
        )
        .is_err());
    }

    /// `Test_isSchedulableAfterNodeChange` (`plugin_test.go:153-262`).
    #[test]
    fn inter_pod_affinity_is_schedulable_after_node_change() {
        use Kind::*;
        let aff = |tk: &str| {
            ipa_pod(vec![(
                Affinity,
                selector_in("service", &["securityscan", "value2"]),
                tk,
            )])
        };
        let anti = || {
            ipa_pod(vec![(
                AntiAffinity,
                json!({"matchLabels": {"another": "label"}}),
                "zone",
            )])
        };
        let cases: Vec<(&str, Pod, Option<Node>, Node, QueueingHint)> = vec![
            (
                "add a new node with matched pod affinity topologyKey",
                aff("zone"),
                None,
                named_node(&[("zone", "zone1")]),
                QueueingHint::Queue,
            ),
            (
                "add a new node without matched topologyKey",
                aff("region"),
                None,
                named_node(&[("zone", "zone1")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node label but not topologyKey",
                aff("zone"),
                Some(named_node(&[("aaa", "a")])),
                named_node(&[("aaa", "b")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node label that isn't related to the pod affinity",
                aff("zone"),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone1"), ("unrelated-label", "unrelated")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node with different affinity topologyKey value",
                aff("zone"),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone2")]),
                QueueingHint::Queue,
            ),
            (
                "update node to have the affinity topology label",
                aff("zone"),
                Some(named_node(&[("aaa", "a")])),
                named_node(&[("zone", "zone1")]),
                QueueingHint::Queue,
            ),
            (
                "add a new node with matched pod anti-affinity topologyKey",
                anti(),
                None,
                named_node(&[("zone", "zone1")]),
                QueueingHint::Queue,
            ),
            (
                "add a new node without matched pod anti-affinity topologyKey",
                anti(),
                None,
                named_node(&[]),
                QueueingHint::Queue,
            ),
            (
                "update node label that isn't related to the pod anti-affinity",
                anti(),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone1"), ("unrelated-label", "unrelated")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node with different anti-affinity topologyKey value",
                anti(),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone2")]),
                QueueingHint::Queue,
            ),
            (
                "update node to have the anti-affinity topology label",
                anti(),
                Some(named_node(&[("aaa", "a")])),
                named_node(&[("zone", "zone1")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node label not to have anti-affinity topology label",
                anti(),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("aaa", "a")]),
                QueueingHint::Queue,
            ),
        ];
        for (name, pod, old, new, want) in cases {
            let old_b = old.map(obj);
            let got = inter_pod_affinity::is_schedulable_after_node_change(
                &pod,
                old_b.as_deref(),
                Some(&*obj(new)),
            )
            .unwrap();
            assert_eq!(got, want, "{name}");
        }
        assert!(inter_pod_affinity::is_schedulable_after_node_change(
            &aff("zone"),
            None,
            Some(&*obj("not-a-node".to_string()))
        )
        .is_err());
    }

    /// `newAffinityTerm` / `AffinityTerm.Matches` (`kube-scheduler/framework/
    /// types.go:387-445`): the namespace set, namespaceSelector and an absent
    /// labelSelector (`labels.Nothing()`) all take part in the match.
    #[test]
    fn inter_pod_affinity_term_namespace_and_selector_semantics() {
        let mk = |term: serde_json::Value, ns: &str| -> Pod {
            serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": ns},
                "spec": {"affinity": {"podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [term]}}}
            }))
            .unwrap()
        };
        let scheduled = |ns: &str| -> Pod {
            serde_json::from_value(json!({
                "metadata": {"namespace": ns, "labels": {"a": "b"}},
                "spec": {"nodeName": "n"}
            }))
            .unwrap()
        };
        let del = |target: &Pod, p: Pod| {
            inter_pod_affinity::is_schedulable_after_pod_change(target, Some(&*obj(p)), None)
                .unwrap()
        };
        let sel = json!({"matchLabels": {"a": "b"}});
        // default namespace = the target pod's own
        let t = mk(json!({"labelSelector": sel, "topologyKey": "z"}), "ns1");
        assert_eq!(del(&t, scheduled("ns1")), QueueingHint::Queue);
        assert_eq!(del(&t, scheduled("ns2")), QueueingHint::QueueSkip);
        // explicit namespaces
        let t = mk(
            json!({"labelSelector": sel, "topologyKey": "z", "namespaces": ["ns2"]}),
            "ns1",
        );
        assert_eq!(del(&t, scheduled("ns2")), QueueingHint::Queue);
        assert_eq!(del(&t, scheduled("ns1")), QueueingHint::QueueSkip);
        // namespaceSelector set, nsLabels nil: empty selector matches all
        let t = mk(
            json!({"labelSelector": sel, "topologyKey": "z", "namespaceSelector": {}}),
            "ns1",
        );
        assert_eq!(del(&t, scheduled("ns9")), QueueingHint::Queue);
        // absent labelSelector is labels.Nothing(): never matches
        let t = mk(json!({"topologyKey": "z"}), "ns1");
        assert_eq!(del(&t, scheduled("ns1")), QueueingHint::QueueSkip);
        // an invalid selector is an error (backoff)
        let t = mk(
            json!({"labelSelector": {"matchExpressions": [{"key": "a", "operator": "Bogus"}]}, "topologyKey": "z"}),
            "ns1",
        );
        assert!(inter_pod_affinity::is_schedulable_after_pod_change(
            &t,
            Some(&*obj(scheduled("ns1"))),
            None
        )
        .is_err());
    }
}
