//! Re-registering an existing Node must not overwrite its `spec`.
//!
//! The kubelet builds a fresh Node object on every start. On restart the object
//! already exists, so it re-GETs it and grafts its own fields on before the
//! update. Grafting the whole `spec` wipes everything another component owns:
//!
//! * `spec.podCIDR` / `podCIDRs`, assigned by kube-controller-manager's node-ipam
//!   — a strict api-server rejects the update outright
//!   (`spec.podCIDRs: Forbidden: node updates may not change podCIDR except from
//!   "" to valid`), so the kubelet fails to register at all and the node never
//!   goes Ready;
//! * `spec.taints`, which the taint-eviction controller and `kubectl taint` write.
//!
//! Upstream never touches the existing spec on this path: `tryRegisterWithAPIServer`
//! (k8s.io/kubernetes/pkg/kubelet/kubelet_node_status.go:110-130) re-gets the
//! node and then reconciles only the CMAD annotation, the default labels, and
//! extended resources:
//!
//! ```go
//! existingNode, err := kl.kubeClient.CoreV1().Nodes().Get(...)
//! ...
//! requiresUpdate := kl.reconcileCMADAnnotationWithExistingNode(node, existingNode)
//! requiresUpdate = kl.updateDefaultLabels(node, existingNode) || requiresUpdate
//! requiresUpdate = kl.reconcileExtendedResource(node, existingNode) || requiresUpdate
//! ```

use rusternetes_kubelet::kubelet::reconcile_existing_node;

use rusternetes_common::resources::Node;

fn node_from_json(v: serde_json::Value) -> Node {
    serde_json::from_value(v).expect("node fixture must decode")
}

/// The node-ipam-assigned pod CIDR must survive a kubelet restart.
#[test]
fn reconcile_preserves_pod_cidr() {
    let mut existing = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-1", "resourceVersion": "42", "uid": "u-1"},
        "spec": {"podCIDR": "10.244.1.0/24", "podCIDRs": ["10.244.1.0/24"]},
        "status": {},
    }));
    // What the kubelet freshly built: it knows nothing about pod CIDRs.
    let desired = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-1", "labels": {"kubernetes.io/hostname": "node-1"}},
        "spec": {"unschedulable": false},
        "status": {},
    }));

    reconcile_existing_node(&mut existing, &desired);

    let spec = existing.spec.as_ref().expect("spec must remain");
    assert_eq!(
        spec.pod_cidr.as_deref(),
        Some("10.244.1.0/24"),
        "podCIDR assigned by node-ipam must be preserved"
    );
    assert_eq!(
        spec.pod_cidrs.as_deref(),
        Some(&["10.244.1.0/24".to_string()][..]),
        "podCIDRs must be preserved"
    );
}

/// Taints are owned by other actors (taint-eviction controller, `kubectl taint`);
/// a kubelet restart must not drop them and let pods schedule onto a node that
/// was deliberately cordoned off.
#[test]
fn reconcile_preserves_taints_and_unschedulable() {
    let mut existing = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-2", "resourceVersion": "7", "uid": "u-2"},
        "spec": {
            "unschedulable": true,
            "taints": [{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}],
        },
        "status": {},
    }));
    let desired = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-2", "labels": {"kubernetes.io/os": "linux"}},
        "spec": {"unschedulable": false},
        "status": {},
    }));

    reconcile_existing_node(&mut existing, &desired);

    let spec = existing.spec.as_ref().unwrap();
    assert_eq!(
        spec.taints.as_ref().map(Vec::len),
        Some(1),
        "existing taints must survive kubelet re-registration"
    );
    assert_eq!(
        spec.unschedulable,
        Some(true),
        "a cordoned node must stay cordoned"
    );
}

/// The labels the kubelet owns (`kubernetes.io/hostname`, os/arch) are still
/// refreshed — that is the reconcile's actual job.
///
/// Upstream ref: `updateDefaultLabels`, kubelet_node_status.go.
#[test]
fn reconcile_updates_kubelet_owned_labels() {
    let mut existing = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-1", "resourceVersion": "3", "labels": {"stale": "yes"}},
        "spec": {"podCIDR": "10.244.0.0/24"},
        "status": {},
    }));
    let desired = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": "node-1",
            "labels": {"kubernetes.io/hostname": "node-1", "kubernetes.io/os": "linux"},
        },
        "spec": {},
        "status": {},
    }));

    reconcile_existing_node(&mut existing, &desired);

    let labels = existing.metadata.labels.as_ref().expect("labels present");
    assert_eq!(
        labels.get("kubernetes.io/hostname").map(String::as_str),
        Some("node-1")
    );
    assert_eq!(
        labels.get("kubernetes.io/os").map(String::as_str),
        Some("linux")
    );
    // resourceVersion is what makes the update a safe CAS — it must not be lost.
    assert_eq!(existing.metadata.resource_version.as_deref(), Some("3"));
}

/// A node that genuinely has no spec yet gets the kubelet's initial one, so a
/// first registration that raced with something else still ends up schedulable.
#[test]
fn reconcile_fills_in_a_missing_spec() {
    let mut existing = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-3", "resourceVersion": "1"},
        "status": {},
    }));
    let desired = node_from_json(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node-3"},
        "spec": {"unschedulable": false},
        "status": {},
    }));

    reconcile_existing_node(&mut existing, &desired);

    assert_eq!(
        existing.spec.as_ref().and_then(|s| s.unschedulable),
        Some(false),
        "an absent spec is populated from the kubelet's initial node"
    );
}
