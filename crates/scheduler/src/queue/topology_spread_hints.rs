//! PodTopologySpread queueing hints.
//!
//! Ported from upstream (release-1.35)
//! `pkg/scheduler/framework/plugins/podtopologyspread/`:
//! - `plugin.go`: `EventsToRegister` (`:138-156`, registered in
//!   `hints.rs`), `involvedInTopologySpreading` (`:158-162`),
//!   `hasConstraintWithNodeTaintsPolicyHonor` (`:164-172`),
//!   `isSchedulableAfterPodChange` (`:174-252`), `getConstraints`
//!   (`:254-275`), `isSchedulableAfterNodeChange` (`:277-345`),
//!   `checkTopologyKeyLabelsChanged` (`:347-356`).
//! - `common.go`: `topologySpreadConstraint`, `filterTopologySpreadConstraints`,
//!   `mergeLabelSetWithSelector`, `nodeLabelsMatchSpreadConstraints`,
//!   `podLabelsMatchSpreadConstraints`.
//!
//! Feature gates are at their 1.35 defaults: `SchedulerQueueingHints`,
//! `NodeInclusionPolicyInPodTopologySpread` and
//! `MatchLabelKeysInPodTopologySpread` are all on.
//!
//! Not ported: `buildDefaultConstraints` for a Pod without its own
//! constraints under `defaultingType: List` (needs the Service / RC / RS /
//! StatefulSet listers behind `helper.DefaultSelector`). The default
//! `System` constraints are all `ScheduleAnyway`, and `getConstraints` only
//! wants `DoNotSchedule`, so they contribute no constraints — which is what
//! this returns.

use super::events::*;
use rusternetes_common::resources::{Node, Pod};
use rusternetes_common::types::{
    label_selector_as_selector, LabelSelector, LabelSelectorRequirement, Selector,
};
use std::collections::HashMap;
use tracing::debug;

fn pod_name(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `topologySpreadConstraint` (`common.go:32-39`), reduced to the fields the
/// hints read.
struct SpreadConstraint {
    topology_key: String,
    selector: Selector,
    node_taints_honor: bool,
}

/// `mergeLabelSetWithSelector` (`common.go:132-146`): the selector built from
/// the `matchLabelKeys` label set, plus every requirement of `s`. A `Nothing`
/// selector (nil `LabelSelector`) has no requirements and is returned as-is.
pub fn merge_label_set_with_selector(
    match_labels: HashMap<String, String>,
    original: Option<&LabelSelector>,
    s: Selector,
) -> Result<Selector, String> {
    let Some(original) = original else {
        return Ok(s);
    };
    let mut expressions: Vec<LabelSelectorRequirement> = Vec::new();
    for (k, v) in original.match_labels.iter().flatten() {
        expressions.push(LabelSelectorRequirement {
            key: k.clone(),
            operator: "In".to_string(),
            values: Some(vec![v.clone()]),
        });
    }
    expressions.extend(original.match_expressions.iter().flatten().cloned());
    let merged = LabelSelector {
        match_labels: Some(match_labels),
        match_expressions: if expressions.is_empty() {
            None
        } else {
            Some(expressions)
        },
    };
    label_selector_as_selector(Some(&merged))
}

/// `filterTopologySpreadConstraints` (`common.go:92-130`) for
/// `action == DoNotSchedule`.
fn filter_topology_spread_constraints(pod: &Pod) -> Result<Vec<SpreadConstraint>, String> {
    let mut result = Vec::new();
    let Some(constraints) = pod
        .spec
        .as_ref()
        .and_then(|s| s.topology_spread_constraints.as_ref())
    else {
        return Ok(result);
    };
    for c in constraints {
        if c.when_unsatisfiable != "DoNotSchedule" {
            continue;
        }
        let mut selector = label_selector_as_selector(c.label_selector.as_ref())?;
        if let Some(keys) = c.match_label_keys.as_ref().filter(|k| !k.is_empty()) {
            let pod_labels = pod.metadata.labels.as_ref();
            let match_labels: HashMap<String, String> = keys
                .iter()
                .filter_map(|k| {
                    pod_labels
                        .and_then(|l| l.get(k))
                        .map(|v| (k.clone(), v.clone()))
                })
                .collect();
            if !match_labels.is_empty() {
                selector = merge_label_set_with_selector(
                    match_labels,
                    c.label_selector.as_ref(),
                    selector,
                )?;
            }
        }
        result.push(SpreadConstraint {
            topology_key: c.topology_key.clone(),
            selector,
            // NodeTaintsPolicy defaults to Ignore.
            node_taints_honor: c.node_taints_policy.as_deref() == Some("Honor"),
        });
    }
    Ok(result)
}

/// `getConstraints` (`plugin.go:254-275`).
fn get_constraints(pod: &Pod) -> Result<Vec<SpreadConstraint>, String> {
    let has_own = pod
        .spec
        .as_ref()
        .and_then(|s| s.topology_spread_constraints.as_ref())
        .is_some_and(|c| !c.is_empty());
    if has_own {
        return filter_topology_spread_constraints(pod)
            .map_err(|e| format!("obtaining pod's hard topology spread constraints: {e}"));
    }
    // `buildDefaultConstraints`: see the module doc.
    Ok(Vec::new())
}

fn node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

/// `involvedInTopologySpreading` (`plugin.go:158-162`).
fn involved_in_topology_spreading(incoming: &Pod, with_spreading: &Pod) -> bool {
    incoming.metadata.uid == with_spreading.metadata.uid
        || (!node_name(incoming).is_empty()
            && incoming.metadata.namespace == with_spreading.metadata.namespace)
}

/// `podLabelsMatchSpreadConstraints` (`common.go`).
fn pod_labels_match_spread_constraints(
    constraints: &[SpreadConstraint],
    labels: Option<&HashMap<String, String>>,
) -> bool {
    constraints.iter().any(|c| c.selector.matches(labels))
}

/// `nodeLabelsMatchSpreadConstraints` (`common.go`): ALL topology keys are
/// present in the node labels.
fn node_labels_match_spread_constraints(
    node_labels: Option<&HashMap<String, String>>,
    constraints: &[SpreadConstraint],
) -> bool {
    constraints
        .iter()
        .all(|c| node_labels.is_some_and(|l| l.contains_key(&c.topology_key)))
}

/// `checkTopologyKeyLabelsChanged` (`plugin.go:347-356`).
fn check_topology_key_labels_changed(
    original: Option<&HashMap<String, String>>,
    modified: Option<&HashMap<String, String>>,
    constraints: &[SpreadConstraint],
) -> bool {
    constraints.iter().any(|c| {
        original.and_then(|l| l.get(&c.topology_key))
            != modified.and_then(|l| l.get(&c.topology_key))
    })
}

/// `equality.Semantic.DeepEqual` on label maps: nil and empty are equal.
fn labels_eq(a: Option<&HashMap<String, String>>, b: Option<&HashMap<String, String>>) -> bool {
    let empty = HashMap::new();
    a.unwrap_or(&empty) == b.unwrap_or(&empty)
}

fn tolerations_json(pod: &Pod) -> serde_json::Value {
    serde_json::to_value(
        pod.spec
            .as_ref()
            .and_then(|s| s.tolerations.clone())
            .unwrap_or_default(),
    )
    .unwrap_or_default()
}

/// `isSchedulableAfterPodChange` (`plugin.go:174-252`).
pub fn is_schedulable_after_pod_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (original_pod, modified_pod) = as_typed::<Pod>(old_obj, new_obj)?;

    if modified_pod.is_some_and(|m| !involved_in_topology_spreading(m, pod))
        || original_pod.is_some_and(|o| !involved_in_topology_spreading(o, pod))
    {
        debug!(pod = %pod_name(pod), "the added/updated/deleted pod is unscheduled or has different namespace with target pod, so it doesn't make the target pod schedulable");
        return Ok(QueueingHint::QueueSkip);
    }

    let constraints = get_constraints(pod)?;

    // Pod is modified. Return Queue when the label(s) matching topologySpread's selector is added, changed, or deleted.
    if let (Some(modified), Some(original)) = (modified_pod, original_pod) {
        if pod.metadata.uid == modified.metadata.uid
            && tolerations_json(modified) != tolerations_json(original)
            && constraints.iter().any(|c| c.node_taints_honor)
        {
            // If any constraint has `NodeTaintsPolicy: Honor`, we can return Queue when the target Pod has got a new toleration.
            debug!(pod = %pod_name(pod), "the unschedulable pod has got a new toleration, which could make it schedulable");
            return Ok(QueueingHint::Queue);
        }

        if labels_eq(
            modified.metadata.labels.as_ref(),
            original.metadata.labels.as_ref(),
        ) {
            debug!(pod = %pod_name(pod), "the pod's update doesn't include the label update, which doesn't make the target pod schedulable");
            return Ok(QueueingHint::QueueSkip);
        }
        for c in &constraints {
            if c.selector.matches(original.metadata.labels.as_ref())
                != c.selector.matches(modified.metadata.labels.as_ref())
            {
                // This modification makes this Pod match(or not match) with this constraint.
                debug!(pod = %pod_name(pod), "a scheduled pod's label was updated and it makes the updated pod match or unmatch the pod's topology spread constraints");
                return Ok(QueueingHint::Queue);
            }
        }
        // This modification of labels doesn't change whether this Pod would match selector or not in any constraints.
        debug!(pod = %pod_name(pod), "a scheduled pod's label was updated, but it's a change unrelated to the pod's topology spread constraints");
        return Ok(QueueingHint::QueueSkip);
    }

    // Pod is added. Return Queue when the added Pod has a label that matches with topologySpread's selector.
    if let Some(modified) = modified_pod {
        if pod_labels_match_spread_constraints(&constraints, modified.metadata.labels.as_ref()) {
            debug!(pod = %pod_name(pod), "a scheduled pod was created and it matches with the pod's topology spread constraints");
            return Ok(QueueingHint::Queue);
        }
        debug!(pod = %pod_name(pod), "a scheduled pod was created, but it doesn't matches with the pod's topology spread constraints");
        return Ok(QueueingHint::QueueSkip);
    }

    // Pod is deleted. Return Queue when the deleted Pod has a label that matches with topologySpread's selector.
    let original = original_pod.ok_or_else(|| "pod event without any object".to_string())?;
    if pod_labels_match_spread_constraints(&constraints, original.metadata.labels.as_ref()) {
        debug!(pod = %pod_name(pod), "a scheduled pod which matches with the pod's topology spread constraints was deleted, and the pod may be schedulable now");
        return Ok(QueueingHint::Queue);
    }
    debug!(pod = %pod_name(pod), "a scheduled pod was deleted, but it's unrelated to the pod's topology spread constraints");
    Ok(QueueingHint::QueueSkip)
}

fn taints_json(node: &Node) -> serde_json::Value {
    serde_json::to_value(
        node.spec
            .as_ref()
            .and_then(|s| s.taints.clone())
            .unwrap_or_default(),
    )
    .unwrap_or_default()
}

/// `isSchedulableAfterNodeChange` (`plugin.go:277-345`): Queue when the node
/// has every constraint's topologyKey in its labels (and, on update, that
/// matching, a topologyKey value, or the taints changed), else QueueSkip.
pub fn is_schedulable_after_node_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (original_node, modified_node) = as_typed::<Node>(old_obj, new_obj)?;
    let constraints = get_constraints(pod)?;

    let original_matching = original_node.is_some_and(|n| {
        node_labels_match_spread_constraints(n.metadata.labels.as_ref(), &constraints)
    });
    let modified_matching = modified_node.is_some_and(|n| {
        node_labels_match_spread_constraints(n.metadata.labels.as_ref(), &constraints)
    });

    // We return Queue in the following cases:
    // 1. Node/UpdateNodeLabel: matching flipped, or matching with a different value for any topologyKey.
    // 2. Node/UpdateNodeTaint: the modified node matches and the taints differ.
    // 3. Node/Add: The created node matches the pod's topology spread constraints.
    // 4. Node/Delete: The original node matched the pod's topology spread constraints.
    if let (Some(original), Some(modified)) = (original_node, modified_node) {
        if original_matching != modified_matching {
            debug!(pod = %pod_name(pod), node = %modified.metadata.name, original_matching, modified_matching, "the node is updated and now pod topology spread constraints has changed, and the pod may be schedulable now");
            return Ok(QueueingHint::Queue);
        }
        if modified_matching
            && (check_topology_key_labels_changed(
                original.metadata.labels.as_ref(),
                modified.metadata.labels.as_ref(),
                &constraints,
            ) || taints_json(original) != taints_json(modified))
        {
            debug!(pod = %pod_name(pod), node = %modified.metadata.name, "the node is updated and now has different taints or labels, and the pod may be schedulable now");
            return Ok(QueueingHint::Queue);
        }
        return Ok(QueueingHint::QueueSkip);
    }

    if let Some(modified) = modified_node {
        if !modified_matching {
            debug!(pod = %pod_name(pod), node = %modified.metadata.name, "the created node doesn't match pod topology spread constraints");
            return Ok(QueueingHint::QueueSkip);
        }
        debug!(pod = %pod_name(pod), node = %modified.metadata.name, "the created node matches topology spread constraints, and the pod may be schedulable now");
        return Ok(QueueingHint::Queue);
    }

    let original = original_node.ok_or_else(|| "node event without any object".to_string())?;
    if !original_matching {
        debug!(pod = %pod_name(pod), node = %original.metadata.name, "the deleted node doesn't match pod topology spread constraints");
        return Ok(QueueingHint::QueueSkip);
    }
    debug!(pod = %pod_name(pod), node = %original.metadata.name, "the deleted node matches topology spread constraints, and the pod may be schedulable now");
    Ok(QueueingHint::Queue)
}
