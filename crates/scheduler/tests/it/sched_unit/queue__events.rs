//! Unit tests moved from `src/queue/events.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{Node, Pod};
    pub use std::any::Any;
    pub use std::collections::HashMap;
    pub use std::fmt;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::events::*;

    fn pod_json(v: serde_json::Value) -> Pod {
        serde_json::from_value(v).unwrap()
    }
    fn node_json(v: serde_json::Value) -> Node {
        serde_json::from_value(v).unwrap()
    }

    /// `TestMatchClusterEvents` (`pkg/scheduler/framework/types_test.go`).
    #[test]
    fn match_cluster_events_table() {
        let n = |a: ActionType| ClusterEvent::new(EventResource::Node, a);
        let cases: Vec<(&str, ClusterEvent, ClusterEvent, bool)> = vec![
            (
                "wildcard event matches with all kinds of coming events",
                ClusterEvent::new(EventResource::WildCard, ActionType::ALL),
                ClusterEvent::new(EventResource::Pod, ActionType::UPDATE_POD_LABEL),
                true,
            ),
            (
                "wildcard event matches with coming events",
                ClusterEvent::new(EventResource::WildCard, ActionType::ALL),
                ClusterEvent::new(EventResource::WildCard, ActionType::ALL),
                true,
            ),
            (
                "Node/All matches Node/UpdateNodeLabel",
                n(ActionType::ALL),
                n(ActionType::UPDATE_NODE_LABEL),
                true,
            ),
            (
                "Node/Update matches Node/UpdateNodeLabel",
                n(ActionType::UPDATE),
                n(ActionType::UPDATE_NODE_LABEL),
                true,
            ),
            (
                "Node/UpdateNodeLabel does not match Node/Update",
                n(ActionType::UPDATE_NODE_LABEL),
                n(ActionType::UPDATE),
                false,
            ),
            (
                "Node/UpdateNodeLabel|UpdateNodeTaint matches Node/UpdateNodeLabel",
                n(ActionType::UPDATE_NODE_LABEL | ActionType::UPDATE_NODE_TAINT),
                n(ActionType::UPDATE_NODE_LABEL),
                true,
            ),
            (
                "Node/Add does not match Node/UpdateNodeLabel",
                n(ActionType::ADD),
                n(ActionType::UPDATE_NODE_LABEL),
                false,
            ),
            (
                "Pod/Add matches internal AssignedPod/Add",
                ClusterEvent::new(EventResource::Pod, ActionType::ADD),
                EVENT_ASSIGNED_POD_ADD,
                true,
            ),
            (
                "Pod/Update matches internal UnschedulablePod/UpdatePodLabel",
                ClusterEvent::new(EventResource::Pod, ActionType::UPDATE),
                ClusterEvent::new(
                    EventResource::UnschedulablePod,
                    ActionType::UPDATE_POD_LABEL,
                ),
                true,
            ),
            (
                "Node/Add does not match Pod/Add",
                n(ActionType::ADD),
                ClusterEvent::new(EventResource::Pod, ActionType::ADD),
                false,
            ),
            (
                "*/Add matches Node/Add (wildcard resource, specific action)",
                ClusterEvent::new(EventResource::WildCard, ActionType::ADD),
                n(ActionType::ADD),
                true,
            ),
            (
                "Node/Add does not match a wildcard incoming event",
                n(ActionType::ADD),
                ClusterEvent::new(EventResource::WildCard, ActionType::ALL),
                false,
            ),
        ];
        for (name, ce, incoming, want) in cases {
            assert_eq!(match_cluster_events(&ce, &incoming), want, "{name}");
        }
    }

    #[test]
    fn labels_and_strings() {
        assert_eq!(
            ClusterEvent::new(EventResource::Node, ActionType::ADD).label(),
            "NodeAdd"
        );
        assert_eq!(
            ClusterEvent::new(EventResource::Pod, ActionType::UPDATE_POD_SCALE_DOWN).label(),
            "PodUpdatePodScaleDown"
        );
        assert_eq!(EVENT_UNSCHEDULABLE_TIMEOUT.label(), "UnschedulableTimeout");
        assert_eq!(EVENT_FORCE_ACTIVATE.label(), "ForceActivate");
        assert_eq!(
            ClusterEvent::new(EventResource::CsiNode, ActionType::UPDATE).label(),
            "storage.k8s.io/CSINodeUpdate"
        );
        assert_eq!(ActionType::ALL.to_string(), "All");
        assert_eq!(ActionType::UPDATE.to_string(), "Update");
        assert!(cluster_event_is_wild_card(&EVENT_FORCE_ACTIVATE));
        assert!(!cluster_event_is_wild_card(&ClusterEvent::new(
            EventResource::WildCard,
            ActionType::ADD
        )));
    }

    /// `TestNodeSchedulingPropertiesChange` (`events_test.go`).
    #[test]
    fn node_scheduling_properties_change_table() {
        let base = node_json(serde_json::json!({"metadata": {"name": "n"}}));
        let cases: Vec<(&str, Node, Node, Vec<ClusterEvent>)> = vec![
            (
                "no specific changed applied",
                base.clone(),
                base.clone(),
                vec![],
            ),
            (
                "only node spec unavailable changed",
                node_json(
                    serde_json::json!({"metadata": {"name": "n"}, "spec": {"unschedulable": true}}),
                ),
                base.clone(),
                vec![ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_TAINT,
                )],
            ),
            (
                "only node allocatable changed",
                node_json(
                    serde_json::json!({"metadata": {"name": "n"}, "status": {"allocatable": {"cpu": "1"}}}),
                ),
                base.clone(),
                vec![ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_ALLOCATABLE,
                )],
            ),
            (
                "only node label changed",
                base.clone(),
                node_json(serde_json::json!({"metadata": {"name": "n", "labels": {"foo": "bar"}}})),
                vec![ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_LABEL,
                )],
            ),
            (
                "only node taint changed",
                base.clone(),
                node_json(
                    serde_json::json!({"metadata": {"name": "n"}, "spec": {"taints": [{"key": "k", "effect": "NoSchedule"}]}}),
                ),
                vec![ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_TAINT,
                )],
            ),
            (
                "only node annotation changed",
                base.clone(),
                node_json(
                    serde_json::json!({"metadata": {"name": "n", "annotations": {"foo": "bar"}}}),
                ),
                vec![ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_ANNOTATION,
                )],
            ),
            (
                "only node condition changed",
                base.clone(),
                node_json(
                    serde_json::json!({"metadata": {"name": "n"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}),
                ),
                vec![ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_CONDITION,
                )],
            ),
            (
                "condition message change is not a condition change",
                node_json(
                    serde_json::json!({"metadata": {"name": "n"}, "status": {"conditions": [{"type": "Ready", "status": "True", "message": "a"}]}}),
                ),
                node_json(
                    serde_json::json!({"metadata": {"name": "n"}, "status": {"conditions": [{"type": "Ready", "status": "True", "message": "b"}]}}),
                ),
                vec![],
            ),
            (
                "both node label and node taint changed",
                base.clone(),
                node_json(
                    serde_json::json!({"metadata": {"name": "n", "labels": {"foo": "bar"}}, "spec": {"taints": [{"key": "k", "effect": "NoSchedule"}]}}),
                ),
                vec![
                    ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_LABEL),
                    ClusterEvent::new(EventResource::Node, ActionType::UPDATE_NODE_TAINT),
                ],
            ),
        ];
        for (name, old, new, want) in cases {
            assert_eq!(
                node_scheduling_properties_change(&new, &old),
                want,
                "{name}"
            );
        }
    }

    /// `TestPodSchedulingPropertiesChange` (`events_test.go`).
    #[test]
    fn pod_scheduling_properties_change_table() {
        let base = pod_json(serde_json::json!({"metadata": {"name": "p"}}));
        let assigned =
            pod_json(serde_json::json!({"metadata": {"name": "p"}, "spec": {"nodeName": "n"}}));
        let u = EventResource::UnschedulablePod;
        let cases: Vec<(&str, Pod, Pod, Vec<ClusterEvent>)> = vec![
            (
                "only label is updated",
                base.clone(),
                pod_json(serde_json::json!({"metadata": {"name": "p", "labels": {"foo": "bar"}}})),
                vec![ClusterEvent::new(u, ActionType::UPDATE_POD_LABEL)],
            ),
            (
                "pod's scheduling gates are eliminated",
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"schedulingGates": [{"name": "foo"}]}}),
                ),
                base.clone(),
                vec![ClusterEvent::new(
                    u,
                    ActionType::UPDATE_POD_SCHEDULING_GATES_ELIMINATED,
                )],
            ),
            (
                "pod's scheduling gates are updated but not eliminated",
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"schedulingGates": [{"name": "foo"}, {"name": "bar"}]}}),
                ),
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"schedulingGates": [{"name": "bar"}]}}),
                ),
                vec![ClusterEvent::new(u, ActionType::UPDATE)],
            ),
            (
                "pod's toleration is added",
                base.clone(),
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"tolerations": [{"key": "k", "operator": "Exists"}]}}),
                ),
                vec![ClusterEvent::new(u, ActionType::UPDATE_POD_TOLERATION)],
            ),
            (
                "assigned pod's cpu request is scaled down",
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"nodeName": "n", "containers": [{"name": "c", "resources": {"requests": {"cpu": "2"}}}]}}),
                ),
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"nodeName": "n", "containers": [{"name": "c", "resources": {"requests": {"cpu": "1"}}}]}}),
                ),
                vec![ClusterEvent::new(
                    EventResource::AssignedPod,
                    ActionType::UPDATE_POD_SCALE_DOWN,
                )],
            ),
            (
                "pod's cpu request is scaled up",
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"containers": [{"name": "c", "resources": {"requests": {"cpu": "1"}}}]}}),
                ),
                pod_json(
                    serde_json::json!({"metadata": {"name": "p"}, "spec": {"containers": [{"name": "c", "resources": {"requests": {"cpu": "2"}}}]}}),
                ),
                vec![ClusterEvent::new(u, ActionType::UPDATE)],
            ),
            (
                "no specific changed applied on an assigned pod",
                assigned.clone(),
                assigned.clone(),
                vec![ClusterEvent::new(
                    EventResource::AssignedPod,
                    ActionType::UPDATE,
                )],
            ),
        ];
        for (name, old, new, want) in cases {
            assert_eq!(pod_scheduling_properties_change(&new, &old), want, "{name}");
        }
    }

    #[test]
    fn as_typed_downcasts_and_rejects() {
        let node = node_json(serde_json::json!({"metadata": {"name": "n"}}));
        let pod = pod_json(serde_json::json!({"metadata": {"name": "p"}}));
        let (o, n) = as_typed::<Node>(None, Some(&node)).unwrap();
        assert!(o.is_none());
        assert_eq!(n.unwrap().metadata.name, "n");
        assert!(as_typed::<Node>(Some(&pod), Some(&node)).is_err());
        assert!(as_typed::<Node>(None, Some(&pod)).is_err());
    }
}
