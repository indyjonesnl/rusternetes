//! Pod-signature fragments for batching and other scheduling optimizations,
//! ported from `staging/src/k8s.io/kube-scheduler/framework/signers.go`
//! (release-1.35): the signer names and the helpers plugins share.
//!
//! Values are `serde_json::Value`s ("json-marshallable" upstream). Where
//! order does not matter the lists are sorted, so equal pods produce equal
//! values. Maps serialize with sorted keys.

use rusternetes_common::resources::pod::{
    NodeSelectorRequirement, NodeSelectorTerm, PreferredSchedulingTerm,
};
use rusternetes_common::resources::Pod;
use serde_json::{json, Value};

pub const DYNAMIC_RESOURCES_SIGNER_NAME: &str = "v1.Pod.Spec.DynamicResources";
pub const LABELS_SIGNER_NAME: &str = "v1.Pod.Labels";
pub const NODE_NAME_SIGNER_NAME: &str = "v1.Pod.Spec.NodeName";
pub const NODE_AFFINITY_SIGNER_NAME: &str = "v1.Pod.Spec.Affinity.NodeAffinity";
pub const NODE_SELECTOR_SIGNER_NAME: &str = "v1.Pod.Spec.Affinity.NodeSelector";
pub const HOST_PORTS_SIGNER_NAME: &str = "v1.Pod.Spec.HostPorts()";
pub const RESOURCES_SIGNER_NAME: &str = "v1.Pod.Spec.ContainerRequestsAndOverheads()";
pub const SCHEDULER_NAME_SIGNER_NAME: &str = "v1.Pod.Spec.SchedulerName";
pub const TOLERATIONS_SIGNER_NAME: &str = "v1.Pod.Spec.Tolerations";
pub const VOLUMES_SIGNER_NAME: &str = "v1.Pod.Spec.Volumes.NonSyntheticSources()";

/// `HostPortsSigner`: the sorted, de-duplicated non-zero host ports of the
/// containers and init containers.
pub fn host_ports_signer(pod: &Pod) -> Value {
    let mut ports: Vec<u16> = Vec::new();
    if let Some(spec) = &pod.spec {
        let init = spec.init_containers.iter().flatten();
        for container in spec.containers.iter().chain(init) {
            for port in container.ports.iter().flatten() {
                if let Some(p) = port.host_port.filter(|&p| p != 0) {
                    ports.push(p);
                }
            }
        }
    }
    ports.sort_unstable();
    ports.dedup();
    json!(ports)
}

/// `NodeSelectorRequirementsSigner`: each requirement with its values
/// sorted, as a JSON string, the strings sorted.
pub fn node_selector_requirements_signer(reqs: &[NodeSelectorRequirement]) -> Vec<String> {
    let mut ret: Vec<String> = reqs
        .iter()
        .map(|r| {
            let mut r = r.clone();
            if let Some(v) = r.values.as_mut() {
                v.sort();
            }
            serde_json::to_string(&r).unwrap_or_default()
        })
        .collect();
    ret.sort();
    ret
}

/// `NodeSelectorTermSigner`.
fn node_selector_term_signer(t: &NodeSelectorTerm) -> Value {
    json!({
        "MatchExpressions": node_selector_requirements_signer(t.match_expressions.as_deref().unwrap_or(&[])),
        "MatchFields": node_selector_requirements_signer(t.match_fields.as_deref().unwrap_or(&[])),
    })
}

/// `PreferredSchedulingTermSigner`.
pub fn preferred_scheduling_term_signer(terms: &[PreferredSchedulingTerm]) -> Vec<String> {
    let mut out: Vec<String> = terms
        .iter()
        .map(|t| {
            // Field order as upstream's struct marshals (Weight, Preference).
            format!(
                r#"{{"Weight":{},"Preference":{}}}"#,
                t.weight,
                node_selector_term_signer(&t.preference)
            )
        })
        .collect();
    out.sort();
    out
}

/// `NodeSelectorTermsSigner`.
pub fn node_selector_terms_signer(terms: &[NodeSelectorTerm]) -> Vec<String> {
    let mut out: Vec<String> = terms
        .iter()
        .map(|t| node_selector_term_signer(t).to_string())
        .collect();
    out.sort();
    out
}

/// `NodeAffinitySigner`: `Null` (upstream's nil) without node affinity.
pub fn node_affinity_signer(pod: &Pod) -> Value {
    let Some(n) = pod
        .spec
        .as_ref()
        .and_then(|s| s.affinity.as_ref())
        .and_then(|a| a.node_affinity.as_ref())
    else {
        return Value::Null;
    };
    let preferred = n
        .preferred_during_scheduling_ignored_during_execution
        .as_deref()
        .map(preferred_scheduling_term_signer)
        .unwrap_or_default();
    let required = n
        .required_during_scheduling_ignored_during_execution
        .as_ref()
        .map(|r| node_selector_terms_signer(&r.node_selector_terms))
        .unwrap_or_default();
    json!({"Required": required, "Preferred": preferred})
}

/// `TolerationsSigner`: the tolerations sorted by key, then value.
pub fn tolerations_signer(pod: &Pod) -> Value {
    let mut ret = pod
        .spec
        .as_ref()
        .and_then(|s| s.tolerations.clone())
        .unwrap_or_default();
    ret.sort_by(|a, b| {
        (
            a.key.as_deref().unwrap_or(""),
            a.value.as_deref().unwrap_or(""),
        )
            .cmp(&(
                b.key.as_deref().unwrap_or(""),
                b.value.as_deref().unwrap_or(""),
            ))
    });
    serde_json::to_value(ret).unwrap_or(Value::Null)
}

/// `VolumesSigner`: the volume sources other than ConfigMap and Secret
/// (which do not affect scheduling but are specific to individual pods, so
/// including them would leave no two pods with matching signatures), sorted.
pub fn volumes_signer(pod: &Pod) -> Value {
    let mut ret: Vec<String> = pod
        .spec
        .as_ref()
        .and_then(|s| s.volumes.as_ref())
        .into_iter()
        .flatten()
        .filter(|v| v.config_map.is_none() && v.secret.is_none())
        .map(|v| {
            let mut value = serde_json::to_value(v).unwrap_or(Value::Null);
            // A `VolumeSource` has no name.
            if let Some(m) = value.as_object_mut() {
                m.remove("name");
            }
            value.to_string()
        })
        .collect();
    ret.sort();
    json!(ret)
}
