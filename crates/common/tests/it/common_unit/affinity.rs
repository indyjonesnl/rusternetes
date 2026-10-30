//! Unit tests moved from `src/affinity.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{
        Node, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, Pod,
    };
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::affinity::*;
    use rusternetes_common::resources::{Affinity, Node, Pod};
    use rusternetes_common::resources::{PodAffinity, PodAffinityTerm, PodSpec, PodStatus};
    use rusternetes_common::types::{LabelSelector, Phase};
    use std::collections::HashMap;

    /// Build a node carrying a single topology label.
    fn node_with_topology(name: &str, topo_key: &str, topo_val: &str) -> Node {
        let mut node = Node::new(name);
        let mut labels = HashMap::new();
        labels.insert(topo_key.to_string(), topo_val.to_string());
        node.metadata.labels = Some(labels);
        node
    }

    /// Build a scheduled pod with the given labels and namespace, on `node_name`.
    fn scheduled_pod(name: &str, namespace: &str, labels: &[(&str, &str)], node_name: &str) -> Pod {
        let mut label_map = HashMap::new();
        for (k, v) in labels {
            label_map.insert(k.to_string(), v.to_string());
        }
        let spec = PodSpec {
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.metadata = pod.metadata.with_namespace(namespace);
        pod.metadata.labels = Some(label_map);
        pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Build a candidate pod (the one carrying the affinity) in `namespace` with
    /// a required podAffinity term selecting `app=web` over `topology_key`, and
    /// no explicit namespaces.
    fn candidate_with_affinity(name: &str, namespace: &str, topology_key: &str) -> Pod {
        let mut match_labels = HashMap::new();
        match_labels.insert("app".to_string(), "web".to_string());
        let term = PodAffinityTerm {
            label_selector: Some(LabelSelector {
                match_labels: Some(match_labels),
                match_expressions: None,
            }),
            namespaces: None,
            topology_key: topology_key.to_string(),
            ..Default::default()
        };
        let spec = PodSpec {
            affinity: Some(Affinity {
                node_affinity: None,
                pod_affinity: Some(PodAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![term]),
                    preferred_during_scheduling_ignored_during_execution: None,
                }),
                pod_anti_affinity: None,
            }),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.metadata = pod.metadata.with_namespace(namespace);
        pod
    }

    /// #23: a podAffinityTerm with `namespaces == None` (and no namespaceSelector)
    /// must default to the candidate pod's OWN namespace, not all namespaces.
    #[test]
    fn affinity_term_default_namespace_is_candidate_namespace() {
        let topo_key = "kubernetes.io/hostname";
        let node = node_with_topology("node-1", topo_key, "node-1");

        // Candidate pod lives in namespace "ns-a".
        let candidate = candidate_with_affinity("candidate", "ns-a", topo_key);

        let term = match &candidate
            .spec
            .as_ref()
            .unwrap()
            .affinity
            .as_ref()
            .unwrap()
            .pod_affinity
            .as_ref()
            .unwrap()
            .required_during_scheduling_ignored_during_execution
        {
            Some(terms) => &terms[0],
            None => unreachable!(),
        };

        // A matching pod (app=web) on node-1 but in a DIFFERENT namespace must
        // NOT satisfy the term (cross-namespace default is same-namespace).
        let other_ns_pod = scheduled_pod("web-other", "ns-b", &[("app", "web")], "node-1");
        assert!(
            !matches_pod_affinity_term(
                &node,
                &candidate,
                term,
                std::slice::from_ref(&other_ns_pod),
                std::slice::from_ref(&node),
                true,
            ),
            "a pod in a different namespace must NOT match a namespaces=None term"
        );

        // A matching pod (app=web) on node-1 in the SAME namespace MUST satisfy it.
        let same_ns_pod = scheduled_pod("web-same", "ns-a", &[("app", "web")], "node-1");
        assert!(
            matches_pod_affinity_term(
                &node,
                &candidate,
                term,
                std::slice::from_ref(&same_ns_pod),
                std::slice::from_ref(&node),
                true,
            ),
            "a pod in the same namespace MUST match a namespaces=None term"
        );
    }

    /// A term with **no** `labelSelector` selects no pods, and one with an
    /// *empty* selector selects every pod. Upstream draws that line in
    /// `LabelSelectorAsSelector`
    /// (`apimachinery/pkg/apis/meta/v1/helpers.go:37-43`): `nil` is
    /// `labels.Nothing()`, `&LabelSelector{}` is `labels.Everything()`. The two
    /// are opposite, which is why the field is `Option` rather than a defaulted
    /// value (#1939).
    #[test]
    fn an_absent_label_selector_matches_nothing_and_an_empty_one_matches_everything() {
        let node = node_with_topology("node-1", "zone", "z1");
        let candidate = candidate_with_affinity("candidate", "ns-a", "zone");
        let running = scheduled_pod("web", "ns-a", &[("app", "web")], "node-1");

        let absent = PodAffinityTerm {
            label_selector: None,
            namespaces: None,
            topology_key: "zone".to_string(),
            ..Default::default()
        };
        assert!(
            !matches_pod_affinity_term(
                &node,
                &candidate,
                &absent,
                std::slice::from_ref(&running),
                std::slice::from_ref(&node),
                true,
            ),
            "a term with no labelSelector must match no pods (labels.Nothing())"
        );

        let empty = PodAffinityTerm {
            label_selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: None,
            }),
            ..absent.clone()
        };
        assert!(
            matches_pod_affinity_term(
                &node,
                &candidate,
                &empty,
                std::slice::from_ref(&running),
                std::slice::from_ref(&node),
                true,
            ),
            "a term with an empty labelSelector must match every pod (labels.Everything())"
        );
    }
}
