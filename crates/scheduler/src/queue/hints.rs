//! Per-plugin `EventsToRegister` lists and the queueing-hint functions that
//! go with them, plus `buildQueueingHintMap`.
//!
//! Ported from upstream:
//! - `pkg/scheduler/scheduler.go`: `buildQueueingHintMap` (`:468-533`),
//!   `defaultQueueingHintFn` (`:462-466`).
//! - `pkg/scheduler/framework/plugins/noderesources/fit.go`:
//!   `EventsToRegister` (`:341-371`), `isSchedulableAfterPodEvent`
//!   (`:373-408`), `isSchedulableAfterPodScaleDown` (`:410-460`),
//!   `isSchedulableAfterNodeChange` (`:462-489`), `isFit` (`:573`),
//!   `haveAnyRequestedResourcesIncreased` (`:530-571`).
//! - `pkg/scheduler/framework/plugins/nodeaffinity/node_affinity.go`:
//!   `EventsToRegister` (`:101-117`), `isSchedulableAfterNodeChange`
//!   (`:119-174`).
//!
//! Only NodeResourcesFit and NodeAffinity ship hint functions in this slice;
//! every other in-tree plugin registers its events hint-less, which
//! `build_queueing_hint_map` turns into `defaultQueueingHintFn` (always
//! `Queue`) — exactly upstream's behaviour for a nil `QueueingHintFn`.

use super::events::*;
use super::scheduling_queue::{QueueingHintFunction, QueueingHintMap, QueueingHintMapPerProfile};
use crate::framework_runtime::names;
use crate::profile::Profile;
use rusternetes_common::resources::{Node, NodeSelector, Pod};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

fn pod_name(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `defaultQueueingHintFn` (`scheduler.go:462-466`): a plugin without a hint
/// function has its Pods retried (after backoff) on every registered event.
pub fn default_queueing_hint_fn() -> QueueingHintFn {
    Arc::new(|_, _, _| Ok(QueueingHint::Queue))
}

/// `buildQueueingHintMap` (`scheduler.go:468-533`) for one profile.
///
/// A plugin registering `Node/Add` but not `Node/UpdateNodeTaint` also gets
/// `Node/UpdateNodeTaint` with the default hint: an Unschedulable node is
/// signalled by a taint added shortly after Node/Add, so the Pod rejected on
/// Add must be retried on the taint removal (kubernetes/kubernetes#109437).
pub fn build_queueing_hint_map(plugins: &[(String, Vec<ClusterEventWithHint>)]) -> QueueingHintMap {
    let mut queueing_hint_map: QueueingHintMap = HashMap::new();
    for (plugin_name, events) in plugins {
        let mut register_node_added = false;
        let mut register_node_taint_updated = false;
        for event in events {
            let fun = event
                .queueing_hint_fn
                .clone()
                .unwrap_or_else(default_queueing_hint_fn);
            if event.event.resource == EventResource::Node {
                if event.event.action_type.contains(ActionType::ADD) {
                    register_node_added = true;
                }
                if event
                    .event
                    .action_type
                    .contains(ActionType::UPDATE_NODE_TAINT)
                {
                    register_node_taint_updated = true;
                }
            }
            queueing_hint_map
                .entry(event.event)
                .or_default()
                .push(QueueingHintFunction {
                    plugin_name: plugin_name.clone(),
                    queueing_hint_fn: fun,
                });
        }
        if register_node_added && !register_node_taint_updated {
            // Temporally fix for the issue https://github.com/kubernetes/kubernetes/issues/109437
            // NodeAdded QueueingHint isn't always called because of preCheck.
            // It's definitely not something expected for plugin developers,
            // and registering UpdateNodeTaint event is the only mitigation for now.
            queueing_hint_map
                .entry(ClusterEvent::new(
                    EventResource::Node,
                    ActionType::UPDATE_NODE_TAINT,
                ))
                .or_default()
                .push(QueueingHintFunction {
                    plugin_name: plugin_name.clone(),
                    queueing_hint_fn: default_queueing_hint_fn(),
                });
        }
    }
    queueing_hint_map
}

/// `buildQueueingHintMap` run for every profile (`scheduler.go:394-401`):
/// each profile contributes the events of the plugins it enables.
pub fn build_queueing_hint_map_per_profile<'a>(
    profiles: impl IntoIterator<Item = &'a Profile>,
) -> QueueingHintMapPerProfile {
    profiles
        .into_iter()
        .map(|p| {
            let plugins: Vec<(String, Vec<ClusterEventWithHint>)> = p
                .framework
                .enqueue_extension_plugins()
                .iter()
                .map(|name| (name.to_string(), events_to_register(name, p)))
                .collect();
            (p.scheduler_name.clone(), build_queueing_hint_map(&plugins))
        })
        .collect()
}

/// `EventsToRegister` of every in-tree plugin (feature gates at their 1.35
/// defaults). Plugins without `EnqueueExtensions` get
/// `UnrollWildCardResource` (`fillEnqueueExtensions`, `framework.go:673-689`).
pub fn events_to_register(plugin_name: &str, profile: &Profile) -> Vec<ClusterEventWithHint> {
    use ActionType as A;
    use EventResource as R;
    let ev = |r: R, a: A| ClusterEventWithHint::new(ClusterEvent::new(r, a));
    match plugin_name {
        // fit.go:341-371
        names::NODE_RESOURCES_FIT => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::DELETE | A::UPDATE_POD_SCALE_DOWN),
                Arc::new(node_resources_fit::is_schedulable_after_pod_event),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Node, A::ADD | A::UPDATE_NODE_ALLOCATABLE),
                Arc::new(node_resources_fit::is_schedulable_after_node_change),
            ),
        ],
        // node_affinity.go:101-117
        names::NODE_AFFINITY => {
            // `addedNodeSelector` = NewNodeSelector(addedAffinity.Required…)
            // (node_affinity.go:321).
            let added = profile.added_affinity.as_ref().and_then(|a| {
                a.required_during_scheduling_ignored_during_execution
                    .clone()
            });
            vec![ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
                Arc::new(move |pod, old, new| {
                    node_affinity::is_schedulable_after_node_change(added.as_ref(), pod, old, new)
                }),
            )]
        }
        // nodename/node_name.go:48-58
        names::NODE_NAME => vec![ev(R::Node, A::ADD)],
        // nodeports/node_ports.go:117-132
        names::NODE_PORTS => vec![ev(R::Pod, A::DELETE), ev(R::Node, A::ADD)],
        // nodeunschedulable/node_unschedulable.go:50-61
        names::NODE_UNSCHEDULABLE => vec![
            ev(R::Node, A::ADD | A::UPDATE_NODE_TAINT),
            ev(R::Pod, A::UPDATE_POD_TOLERATION),
        ],
        // tainttoleration/taint_toleration.go:55-66
        names::TAINT_TOLERATION => vec![
            ev(R::Node, A::ADD | A::UPDATE_NODE_TAINT),
            ev(R::Pod, A::UPDATE_POD_TOLERATION),
        ],
        // interpodaffinity/plugin.go:65-80
        names::INTER_POD_AFFINITY => vec![
            ev(R::Pod, A::ADD | A::UPDATE_POD_LABEL | A::DELETE),
            ev(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
        ],
        // podtopologyspread/plugin.go:138-156
        names::POD_TOPOLOGY_SPREAD => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(
                    R::Pod,
                    A::ADD | A::UPDATE_POD_LABEL | A::UPDATE_POD_TOLERATION | A::DELETE,
                ),
                Arc::new(super::topology_spread_hints::is_schedulable_after_pod_change),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(
                    R::Node,
                    A::ADD | A::DELETE | A::UPDATE_NODE_LABEL | A::UPDATE_NODE_TAINT,
                ),
                Arc::new(super::topology_spread_hints::is_schedulable_after_node_change),
            ),
        ],
        // volumebinding/volume_binding.go:145-192 — hint fns are the subject
        // of #3617's remainder.
        names::VOLUME_BINDING => vec![
            ev(R::StorageClass, A::ADD | A::UPDATE),
            ev(R::PersistentVolumeClaim, A::ADD | A::UPDATE),
            ev(R::PersistentVolume, A::ADD | A::UPDATE),
            ev(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
            ev(R::CsiNode, A::ADD | A::UPDATE),
            ev(R::CsiDriver, A::UPDATE),
            ev(R::CsiStorageCapacity, A::ADD | A::UPDATE),
        ],
        // volumerestrictions/volume_restrictions.go:336-352
        names::VOLUME_RESTRICTIONS => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::DELETE),
                Arc::new(
                    super::volume_hints::volume_restrictions::is_schedulable_after_pod_deleted,
                ),
            ),
            ev(R::Node, A::ADD),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::PersistentVolumeClaim, A::ADD),
                Arc::new(super::volume_hints::volume_restrictions::is_schedulable_after_pvc_added),
            ),
        ],
        // volumezone/volume_zone.go:267-285
        names::VOLUME_ZONE => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::StorageClass, A::ADD),
                Arc::new(
                    super::volume_hints::volume_zone::is_schedulable_after_storage_class_added,
                ),
            ),
            ev(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::PersistentVolumeClaim, A::ADD | A::UPDATE),
                Arc::new(super::volume_hints::volume_zone::is_schedulable_after_pvc_change),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::PersistentVolume, A::ADD | A::UPDATE),
                Arc::new(super::volume_hints::volume_zone::is_schedulable_after_pv_change),
            ),
        ],
        // nodevolumelimits/csi.go:90-109
        names::NODE_VOLUME_LIMITS => vec![
            // No hint on CSINode/Add intentionally: any new CSINode could make
            // pods rejected by CSI volumes schedulable (csi.go:90-91).
            ev(R::CsiNode, A::ADD),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::CsiNode, A::UPDATE),
                Arc::new(super::csi_hints::is_schedulable_after_csi_node_updated),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::DELETE),
                Arc::new(super::csi_hints::is_schedulable_after_pod_deleted),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::PersistentVolumeClaim, A::ADD),
                Arc::new(super::csi_hints::is_schedulable_after_pvc_added),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::VolumeAttachment, A::DELETE),
                Arc::new(super::csi_hints::is_schedulable_after_volume_attachment_deleted),
            ),
        ],
        // dynamicresources/dynamicresources.go:262-304
        names::DYNAMIC_RESOURCES => vec![
            ev(
                R::Node,
                A::ADD | A::UPDATE_NODE_LABEL | A::UPDATE_NODE_ALLOCATABLE,
            ),
            ev(R::ResourceClaim, A::ADD | A::UPDATE),
            ev(R::Pod, A::UPDATE_POD_GENERATED_RESOURCE_CLAIM),
            ev(R::DeviceClass, A::ADD | A::UPDATE),
            ev(R::ResourceSlice, A::ADD | A::UPDATE),
        ],
        // schedulinggates/scheduling_gates.go:60-67
        names::SCHEDULING_GATES => vec![ev(R::Pod, A::UPDATE_POD_SCHEDULING_GATES_ELIMINATED)],
        // defaultpreemption/default_preemption.go:107-116 (async preemption on)
        names::DEFAULT_PREEMPTION => vec![ClusterEventWithHint::with_hint(
            ClusterEvent::new(R::Pod, A::DELETE),
            Arc::new(super::preemption_hints::is_pod_schedulable_after_pod_deletion),
        )],
        // Plugins without EnqueueExtensions (score-only, bind, etc.).
        _ => unroll_wild_card_resource(),
    }
}

/// `noderesources/fit.go` queueing hints.
pub mod node_resources_fit {
    use super::*;
    use crate::advanced::pod_request_amounts;
    use rusternetes_common::quantity::parse_resource_value;

    /// `isSchedulableAfterPodEvent` (`fit.go:373-408`): a Pod delete of a
    /// scheduled (or nominated) pod, or a scale-down that frees a resource
    /// the target requests, may make the target schedulable.
    /// InPlacePodVerticalScaling is GA locked-on in 1.35
    /// (`kube_features.go:1376-1380`), so the disabled branch is not ported.
    pub fn is_schedulable_after_pod_event(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_pod, modified_pod) = as_typed::<Pod>(old_obj, new_obj)?;
        let Some(modified_pod) = modified_pod else {
            let original =
                original_pod.ok_or_else(|| "pod delete event without an old object".to_string())?;
            if node_name(original).is_empty() && nominated_node_name(original).is_empty() {
                debug!(pod = %pod_name(pod), deleted_pod = %pod_name(original), "the deleted pod was unscheduled and it wouldn't make the unscheduled pod schedulable");
                return Ok(QueueingHint::QueueSkip);
            }
            // any deletion event to a scheduled pod could make the unscheduled pod schedulable.
            debug!(pod = %pod_name(pod), deleted_pod = %pod_name(original), "another scheduled pod was deleted, and it may make the unscheduled pod schedulable");
            return Ok(QueueingHint::Queue);
        };

        if !is_schedulable_after_pod_scale_down(pod, original_pod, modified_pod) {
            debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified_pod), "pod got scaled down, but the modification isn't related to the resource requests of the target pod");
            return Ok(QueueingHint::QueueSkip);
        }
        debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified_pod), "another scheduled pod or the target pod itself got scaled down, and it may make the unscheduled pod schedulable");
        Ok(QueueingHint::Queue)
    }

    /// `isSchedulableAfterPodScaleDown` (`fit.go:410-460`): true when the
    /// update is for the target pod itself, or when a scheduled pod's
    /// request for a resource the target also requests got reduced.
    fn is_schedulable_after_pod_scale_down(
        target_pod: &Pod,
        original_pod: Option<&Pod>,
        modified_pod: &Pod,
    ) -> bool {
        if modified_pod.metadata.uid == target_pod.metadata.uid {
            // If the scaling down event is for targetPod, it would make targetPod schedulable.
            return true;
        }
        if node_name(modified_pod).is_empty() {
            // If the update event is for a unscheduled Pod, it wouldn't make targetPod schedulable.
            return false;
        }
        let Some(original_pod) = original_pod else {
            return false;
        };
        // the other pod was scheduled, so modification or deletion may free up some resources.
        let original_requests = pod_request_amounts(original_pod);
        let modified_requests = pod_request_amounts(modified_pod);
        // check whether the resource request of the modified pod is less than the original pod.
        let pod_requests = pod_request_amounts(target_pod);
        for (name, requested) in &pod_requests {
            if *requested == 0 {
                // We only care about the resources requested by the pod we are trying to schedule.
                continue;
            }
            let before = original_requests.get(name).copied().unwrap_or(0);
            let after = modified_requests.get(name).copied().unwrap_or(0);
            if before > after {
                return true;
            }
        }
        false
    }

    fn node_name(pod: &Pod) -> &str {
        pod.spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("")
    }

    fn nominated_node_name(pod: &Pod) -> &str {
        pod.status
            .as_ref()
            .and_then(|s| s.nominated_node_name.as_deref())
            .unwrap_or("")
    }

    /// `isSchedulableAfterNodeChange` (`fit.go:462-489`): Node add, or an
    /// allocatable update that increased a requested resource, when the
    /// (new) node fits the Pod on its own.
    pub fn is_schedulable_after_node_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_node, modified_node) = as_typed::<Node>(old_obj, new_obj)?;
        let modified_node =
            modified_node.ok_or_else(|| "node event without a new object".to_string())?;
        // Leaving in the queue if the node is not fit for the pod.
        if !is_fit(pod, modified_node) {
            debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created or updated, but it doesn't have enough resource(s) to accommodate this pod");
            return Ok(QueueingHint::QueueSkip);
        }
        // The node is updated, and the pod can be scheduled on it now.
        if let Some(original) = original_node {
            if !have_any_requested_resources_increased(pod, original, modified_node) {
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was updated, but haven't changed the pod's resource requestments fit assessment");
                return Ok(QueueingHint::QueueSkip);
            }
        }
        debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created or updated, and it may make the pod schedulable");
        Ok(QueueingHint::Queue)
    }

    /// `haveAnyRequestedResourcesIncreased` (`fit.go:530-571`): true when
    /// any resource the Pod requests has a larger allocatable on the new
    /// node than on the old one (allowed pods count included).
    pub fn have_any_requested_resources_increased(
        pod: &Pod,
        original: &Node,
        modified: &Node,
    ) -> bool {
        let requests = pod_request_amounts(pod);
        let orig = allocatable_amounts(original);
        let modi = allocatable_amounts(modified);
        let increased = |name: &str| {
            modi.get(name).copied().unwrap_or(0) > orig.get(name).copied().unwrap_or(0)
        };
        if increased("pods") {
            return true;
        }
        for (name, requested) in &requests {
            if *requested > 0 && increased(name) {
                return true;
            }
        }
        false
    }

    /// `isFit` (`fit.go:573`): does the node, assumed empty, have enough
    /// allocatable for the Pod's requests? Mirrors `Fits(pod, NewNodeInfo(node))`:
    /// at least one allowed Pod, and every requested resource covered.
    pub fn is_fit(pod: &Pod, node: &Node) -> bool {
        let alloc = allocatable_amounts(node);
        if alloc.get("pods").copied().unwrap_or(0) < 1 {
            return false;
        }
        let requests = pod_request_amounts(pod);
        for (name, requested) in requests {
            if requested <= 0 {
                continue;
            }
            if alloc.get(&name).copied().unwrap_or(0) < requested {
                return false;
            }
        }
        true
    }

    fn allocatable_amounts(node: &Node) -> HashMap<String, i64> {
        node.status
            .as_ref()
            .and_then(|s| s.allocatable.as_ref())
            .map(|a| {
                a.iter()
                    .map(|(k, v)| (k.clone(), parse_resource_value(v, k).unwrap_or(0)))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// `nodeaffinity/node_affinity.go` queueing hints.
pub mod node_affinity {
    use super::*;
    use rusternetes_common::affinity::{check_node_affinity, matches_node_selector};

    /// `isSchedulableAfterNodeChange` (`node_affinity.go:119-174`): a node
    /// add that matches the Pod's required affinity (incl. the profile's
    /// `addedAffinity`) → Queue; a label update → Queue only when the node
    /// went from not matching to matching.
    pub fn is_schedulable_after_node_change(
        added_node_selector: Option<&NodeSelector>,
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_node, modified_node) = as_typed::<Node>(old_obj, new_obj)?;
        let modified_node =
            modified_node.ok_or_else(|| "node event without a new object".to_string())?;

        if let Some(sel) = added_node_selector {
            if !matches_node_selector(modified_node, sel) {
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "added or modified node didn't match scheduler-enforced node affinity and this event won't make the Pod schedulable");
                return Ok(QueueingHint::QueueSkip);
            }
        }

        let is_matched = required_affinity_matches(pod, modified_node);
        if original_node.is_none() {
            // Node add.
            if is_matched {
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created and matches required node affinity, and the pod may be schedulable now");
                return Ok(QueueingHint::Queue);
            }
            debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created but doesn't match required node affinity, so the event won't make the pod schedulable");
            return Ok(QueueingHint::QueueSkip);
        }
        let original = original_node.unwrap();
        let was_matched = required_affinity_matches(pod, original);
        if !was_matched && is_matched {
            debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was updated and matches required node affinity now, and the pod may be schedulable now");
            return Ok(QueueingHint::Queue);
        }
        debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was updated but doesn't match required node affinity, so the event won't make the pod schedulable");
        Ok(QueueingHint::QueueSkip)
    }

    /// `requiredNodeAffinity.Match` (`component-helpers/.../nodeaffinity.go`):
    /// nodeSelector AND required node affinity terms.
    fn required_affinity_matches(pod: &Pod, node: &Node) -> bool {
        check_node_affinity(node, pod).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
