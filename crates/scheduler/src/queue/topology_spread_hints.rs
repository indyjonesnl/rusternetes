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
fn merge_label_set_with_selector(
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

#[cfg(test)]
mod tests {
    use super::*;
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
