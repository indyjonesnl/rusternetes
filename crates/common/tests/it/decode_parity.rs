//! Decode-parity regression tests: real Kubernetes wire objects — as actual
//! clients (kubelet, kubeadm, controllers) send them — MUST deserialize into
//! our resource structs.
//!
//! These catch a whole recurring bug class at CI time instead of in a live
//! conformance run: a field that upstream marks `json:",omitempty"` /
//! `protobuf:",opt"` is sent absent (or empty) by a real client, but our struct
//! requires it, so serde errors "missing field `X`" and the request is rejected
//! (400). Synthetic fixtures with every field populated never exercise this —
//! only the *minimal / real* wire form does.
//!
//! Fixtures under `tests/fixtures/` are captured verbatim off the wire (e.g.
//! `kubelet_node_registration.json` is the exact body a v1.35 kubelet POSTs to
//! `/api/v1/nodes`, decoded from `application/vnd.kubernetes.protobuf`). When a
//! new decode gap is found, add the offending object here so it never regresses.

use rusternetes_common::resources::Node;
use serde_json::Value;

/// Decode every object in a real vanilla-cluster snapshot into its typed struct,
/// by kind. Any "missing field" / type error = a decode-parity gap that would
/// reject the object at runtime (this is the systematic guard that catches the
/// whole class in CI). `real_cluster_snapshot.json` is a verbatim dump of a live
/// kubeadm/kind cluster's RBAC + kube-system addons (206 objects).
#[test]
fn real_cluster_snapshot_decodes_by_kind() {
    use rusternetes_common::resources::{
        ClusterRole, ClusterRoleBinding, ConfigMap, DaemonSet, Deployment, Namespace,
        PriorityClass, Role, RoleBinding, Service, ServiceAccount,
    };

    let list: Value =
        serde_json::from_str(include_str!("../fixtures/real_cluster_snapshot.json")).unwrap();
    let items = list["items"].as_array().expect("snapshot has items[]");

    // (kind, name) -> deserialize error, for every object that fails to decode.
    let mut failures: Vec<String> = Vec::new();
    macro_rules! try_decode {
        ($ty:ty, $v:expr, $kind:expr, $name:expr) => {
            if let Err(e) = serde_json::from_value::<$ty>($v.clone()) {
                failures.push(format!("{} {}: {}", $kind, $name, e));
            }
        };
    }

    for item in items {
        let kind = item["kind"].as_str().unwrap_or("");
        let name = item["metadata"]["name"].as_str().unwrap_or("?");
        match kind {
            "Namespace" => try_decode!(Namespace, item, kind, name),
            "ClusterRole" => try_decode!(ClusterRole, item, kind, name),
            "ClusterRoleBinding" => try_decode!(ClusterRoleBinding, item, kind, name),
            "Role" => try_decode!(Role, item, kind, name),
            "RoleBinding" => try_decode!(RoleBinding, item, kind, name),
            "ServiceAccount" => try_decode!(ServiceAccount, item, kind, name),
            "ConfigMap" => try_decode!(ConfigMap, item, kind, name),
            "DaemonSet" => try_decode!(DaemonSet, item, kind, name),
            "Deployment" => try_decode!(Deployment, item, kind, name),
            "Service" => try_decode!(Service, item, kind, name),
            "PriorityClass" => try_decode!(PriorityClass, item, kind, name),
            other => failures.push(format!("UNHANDLED kind {other} ({name})")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} real objects failed to decode into their typed struct:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The exact Node a v1.35 kubelet sends on registration. Regression for #1666:
/// `status.runtimeHandlers[0]` has no `name` (the CRI default handler; upstream
/// `NodeRuntimeHandler.Name` is `protobuf:",opt"`), which used to fail decode
/// with "missing field `name`" and reject the whole node registration.
#[test]
fn kubelet_node_registration_decodes() {
    let body = include_str!("../fixtures/kubelet_node_registration.json");
    let node: Node = serde_json::from_str(body)
        .expect("a real kubelet Node registration body must deserialize into Node");
    // Sanity: the subject-less runtime handler round-tripped to an empty name.
    let handlers = node
        .status
        .as_ref()
        .and_then(|s| s.runtime_handlers.as_ref())
        .expect("fixture has status.runtimeHandlers");
    assert!(
        handlers.iter().any(|h| h.name.is_empty()),
        "the default CRI runtime handler (empty name) must decode, not error"
    );
}

/// Minimal-form guard independent of the fixture: a Node whose
/// `status.runtimeHandlers` entry omits `name` must decode.
#[test]
fn node_runtime_handler_without_name_decodes() {
    let json = r#"{
        "apiVersion":"v1","kind":"Node","metadata":{"name":"n"},
        "status":{"runtimeHandlers":[{"features":{"recursiveReadOnlyMounts":true}}]}
    }"#;
    let node: Node = serde_json::from_str(json).expect("runtimeHandler without name must decode");
    assert_eq!(node.status.unwrap().runtime_handlers.unwrap()[0].name, "");
}
