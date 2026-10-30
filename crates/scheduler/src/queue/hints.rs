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
//! - `pkg/scheduler/framework/plugins/interpodaffinity/plugin.go`:
//!   `EventsToRegister` (`:83-104`), `isSchedulableAfterPodChange`
//!   (`:171-232`), `isSchedulableAfterNodeChange` (`:234-320`), with
//!   `podMatchesAllAffinityTerms` (`filtering.go:186-199`) and the
//!   `GetAffinityTerms` helpers (`kube-scheduler/framework/types.go:380-490`).
//!
//! - `nodeports/node_ports.go` (`:117-158`), `nodeunschedulable/
//!   node_unschedulable.go` (`:50-128`), `tainttoleration/taint_toleration.go`
//!   (`:55-115`, `:228-245`).
//! - `pkg/scheduler/framework/plugins/schedulinggates/scheduling_gates.go`
//!   (`:59-94`).
//!
//! NodeResourcesFit, NodeAffinity, NodePorts, NodeUnschedulable,
//! TaintToleration, SchedulingGates and InterPodAffinity ship hint functions;
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
    claims: &super::dra_hints::ResourceClaimCache,
) -> QueueingHintMapPerProfile {
    profiles
        .into_iter()
        .map(|p| {
            let plugins: Vec<(String, Vec<ClusterEventWithHint>)> = p
                .framework
                .enqueue_extension_plugins()
                .iter()
                .map(|name| (name.to_string(), events_to_register(name, p, claims)))
                .collect();
            (p.scheduler_name.clone(), build_queueing_hint_map(&plugins))
        })
        .collect()
}

/// `EventsToRegister` of every in-tree plugin (feature gates at their 1.35
/// defaults). Plugins without `EnqueueExtensions` get
/// `UnrollWildCardResource` (`fillEnqueueExtensions`, `framework.go:673-689`).
pub fn events_to_register(
    plugin_name: &str,
    profile: &Profile,
    claims: &super::dra_hints::ResourceClaimCache,
) -> Vec<ClusterEventWithHint> {
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
        names::NODE_PORTS => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::DELETE),
                Arc::new(node_ports::is_schedulable_after_pod_deleted),
            ),
            ev(R::Node, A::ADD),
        ],
        // nodeunschedulable/node_unschedulable.go:50-61
        names::NODE_UNSCHEDULABLE => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Node, A::ADD | A::UPDATE_NODE_TAINT),
                Arc::new(node_unschedulable::is_schedulable_after_node_change),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::UPDATE_POD_TOLERATION),
                Arc::new(node_unschedulable::is_schedulable_after_pod_toleration_change),
            ),
        ],
        // tainttoleration/taint_toleration.go:55-66
        names::TAINT_TOLERATION => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Node, A::ADD | A::UPDATE_NODE_TAINT),
                Arc::new(taint_toleration::is_schedulable_after_node_change),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::UPDATE_POD_TOLERATION),
                Arc::new(taint_toleration::is_schedulable_after_pod_toleration_change),
            ),
        ],
        // interpodaffinity/plugin.go:83-104
        names::INTER_POD_AFFINITY => vec![
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::ADD | A::UPDATE_POD_LABEL | A::DELETE),
                Arc::new(inter_pod_affinity::is_schedulable_after_pod_change),
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
                Arc::new(inter_pod_affinity::is_schedulable_after_node_change),
            ),
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
        // volumebinding/volume_binding.go:105-141
        names::VOLUME_BINDING => {
            use super::volume_binding_hints as vb;
            vec![
                ClusterEventWithHint::with_hint(
                    ClusterEvent::new(R::StorageClass, A::ADD | A::UPDATE),
                    Arc::new(vb::is_schedulable_after_storage_class_change),
                ),
                ClusterEventWithHint::with_hint(
                    ClusterEvent::new(R::PersistentVolumeClaim, A::ADD | A::UPDATE),
                    Arc::new(vb::is_schedulable_after_pvc_change),
                ),
                ev(R::PersistentVolume, A::ADD | A::UPDATE),
                ev(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
                ClusterEventWithHint::with_hint(
                    ClusterEvent::new(R::CsiNode, A::ADD | A::UPDATE),
                    Arc::new(vb::is_schedulable_after_csi_node_change),
                ),
                ClusterEventWithHint::with_hint(
                    ClusterEvent::new(R::CsiDriver, A::UPDATE),
                    Arc::new(vb::is_schedulable_after_csi_driver_change),
                ),
                ClusterEventWithHint::with_hint(
                    ClusterEvent::new(R::CsiStorageCapacity, A::ADD | A::UPDATE),
                    Arc::new(vb::is_schedulable_after_csi_storage_capacity_change),
                ),
            ]
        }
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
        // dynamicresources/dynamicresources.go:211-244
        names::DYNAMIC_RESOURCES => vec![
            ev(
                R::Node,
                A::ADD | A::UPDATE_NODE_LABEL | A::UPDATE_NODE_ALLOCATABLE,
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::ResourceClaim, A::ADD | A::UPDATE),
                {
                    let claims = claims.clone();
                    Arc::new(move |pod, old, new| {
                        super::dra_hints::is_schedulable_after_claim_change(&claims, pod, old, new)
                    })
                },
            ),
            ClusterEventWithHint::with_hint(
                ClusterEvent::new(R::Pod, A::UPDATE_POD_GENERATED_RESOURCE_CLAIM),
                {
                    let claims = claims.clone();
                    Arc::new(move |pod, old, new| {
                        super::dra_hints::is_schedulable_after_pod_change(&claims, pod, old, new)
                    })
                },
            ),
            ev(R::DeviceClass, A::ADD | A::UPDATE),
            ev(R::ResourceSlice, A::ADD | A::UPDATE),
        ],
        // schedulinggates/scheduling_gates.go:60-67
        names::SCHEDULING_GATES => vec![ClusterEventWithHint::with_hint(
            ClusterEvent::new(R::Pod, A::UPDATE_POD_SCHEDULING_GATES_ELIMINATED),
            Arc::new(scheduling_gates::is_schedulable_after_update_pod_scheduling_gates_eliminated),
        )],
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

/// `nodeports/node_ports.go` queueing hints.
pub mod node_ports {
    use super::*;
    use crate::advanced::host_ips_overlap;
    use rusternetes_common::resources::ContainerPort;

    /// `isSchedulableAfterPodDeleted` (`node_ports.go:126-158`): deleting a
    /// scheduled pod only helps when it held a host port the target wants.
    pub fn is_schedulable_after_pod_deleted(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        _new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (deleted_pod, _) = as_typed::<Pod>(old_obj, None)?;
        let deleted_pod =
            deleted_pod.ok_or_else(|| "pod delete event without an old object".to_string())?;

        // If the deleted pod is unscheduled, it doesn't make the target pod schedulable.
        let node_name = deleted_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("");
        let nominated = deleted_pod
            .status
            .as_ref()
            .and_then(|s| s.nominated_node_name.as_deref())
            .unwrap_or("");
        if node_name.is_empty() && nominated.is_empty() {
            debug!(pod = %pod_name(pod), deleted_pod = %pod_name(deleted_pod), "the deleted pod is unscheduled and it doesn't make the target pod schedulable");
            return Ok(QueueingHint::QueueSkip);
        }

        // If the deleted pod doesn't use any host ports, it doesn't make the target pod schedulable.
        let ports_in_use = get_host_ports(deleted_pod);
        if ports_in_use.is_empty() {
            return Ok(QueueingHint::QueueSkip);
        }

        // Verify that `pod` and the deleted pod don't have any common port(s).
        if fits_ports(&get_host_ports(pod), &ports_in_use) {
            debug!(pod = %pod_name(pod), deleted_pod = %pod_name(deleted_pod), "the deleted pod and the target pod don't have any common port(s), returning QueueSkip as deleting this Pod won't make the Pod schedulable");
            return Ok(QueueingHint::QueueSkip);
        }

        debug!(pod = %pod_name(pod), deleted_pod = %pod_name(deleted_pod), "the deleted pod and the target pod have any common port(s), returning Queue as deleting this Pod may make the Pod schedulable");
        Ok(QueueingHint::Queue)
    }

    /// `util.GetHostPorts` (`pkg/scheduler/util/utils.go:183-`): host ports of
    /// the containers and of the restartable (sidecar) init containers.
    fn get_host_ports(pod: &Pod) -> Vec<&ContainerPort> {
        let mut ports = Vec::new();
        let Some(spec) = pod.spec.as_ref() else {
            return ports;
        };
        let host_port = |p: &&ContainerPort| p.host_port.unwrap_or(0) > 0;
        for c in spec.init_containers.iter().flatten() {
            // Only consider initContainers that will be running the entire
            // duration of the Pod.
            if c.restart_policy.as_deref() != Some("Always") {
                continue;
            }
            ports.extend(c.ports.iter().flatten().filter(host_port));
        }
        for c in &spec.containers {
            ports.extend(c.ports.iter().flatten().filter(host_port));
        }
        ports
    }

    /// `fitsPorts` (`node_ports.go:182-190`) against `HostPortInfo.CheckConflict`
    /// (`k8s.io/kube-scheduler/framework/types.go`): same protocol and port,
    /// and overlapping host IPs (an unset/`0.0.0.0` IP overlaps everything).
    fn fits_ports(want: &[&ContainerPort], in_use: &[&ContainerPort]) -> bool {
        let protocol = |p: &ContainerPort| {
            if p.protocol.is_empty() {
                "TCP".to_string()
            } else {
                p.protocol.clone()
            }
        };
        let ip = |p: &ContainerPort| p.host_ip.clone().unwrap_or_default();
        !want.iter().any(|w| {
            in_use.iter().any(|u| {
                w.host_port == u.host_port
                    && protocol(w) == protocol(u)
                    && host_ips_overlap(&ip(w), &ip(u))
            })
        })
    }
}

/// Taint helpers shared by NodeUnschedulable and TaintToleration.
mod taints {
    use crate::advanced::toleration_matches_taint;
    use rusternetes_common::resources::{Taint, Toleration};

    /// `TolerationsTolerateTaint` (`component-helpers/scheduling/corev1/helpers.go`).
    pub fn tolerations_tolerate_taint(tolerations: &[Toleration], taint: &Taint) -> bool {
        tolerations
            .iter()
            .any(|t| toleration_matches_taint(t, taint))
    }

    /// `FindMatchingUntoleratedTaint` with `DoNotScheduleTaintsFilterFunc`
    /// (`helpers.go:79-87`): is there a NoSchedule/NoExecute taint the
    /// tolerations don't tolerate?
    pub fn has_untolerated_do_not_schedule_taint(
        taints: &[Taint],
        tolerations: &[Toleration],
    ) -> bool {
        taints
            .iter()
            .filter(|t| t.effect == "NoSchedule" || t.effect == "NoExecute")
            .any(|t| !tolerations_tolerate_taint(tolerations, t))
    }
}

fn pod_tolerations(pod: &Pod) -> &[rusternetes_common::resources::Toleration] {
    pod.spec
        .as_ref()
        .and_then(|s| s.tolerations.as_deref())
        .unwrap_or(&[])
}

fn node_taints(node: &Node) -> &[rusternetes_common::resources::Taint] {
    node.spec
        .as_ref()
        .and_then(|s| s.taints.as_deref())
        .unwrap_or(&[])
}

fn node_unschedulable_flag(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|s| s.unschedulable)
        .unwrap_or(false)
}

/// `nodeunschedulable/node_unschedulable.go` queueing hints.
pub mod node_unschedulable {
    use super::*;
    use rusternetes_common::resources::Taint;

    /// `isSchedulableAfterPodTolerationChange` (`node_unschedulable.go:78-105`).
    pub fn is_schedulable_after_pod_toleration_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (_, modified_pod) = as_typed::<Pod>(old_obj, new_obj)?;
        let modified_pod =
            modified_pod.ok_or_else(|| "pod event without a new object".to_string())?;

        if pod.metadata.uid == modified_pod.metadata.uid {
            // Note: we don't need to check oldPod tolerations the taint because:
            // - Taint can be added, but can't be modified nor removed.
            // - If the Pod already has the toleration, it shouldn't have rejected by this plugin in the first place.
            //   Meaning, here this Pod has been rejected by this plugin, and hence it shouldn't have the toleration yet.
            let unschedulable = Taint {
                key: "node.kubernetes.io/unschedulable".to_string(),
                value: None,
                effect: "NoSchedule".to_string(),
                time_added: None,
            };
            if taints::tolerations_tolerate_taint(pod_tolerations(modified_pod), &unschedulable) {
                // This update makes the pod tolerate the unschedulable taint.
                debug!(pod = %pod_name(modified_pod), "a new toleration is added for the unschedulable Pod, and it may make it schedulable");
                return Ok(QueueingHint::Queue);
            }
            debug!(pod = %pod_name(modified_pod), "a new toleration is added for the unschedulable Pod, but it's an unrelated toleration");
            return Ok(QueueingHint::QueueSkip);
        }

        debug!(pod = %pod_name(modified_pod), "a new toleration is added for a Pod, but it's an unrelated Pod and wouldn't change the TaintToleration plugin's decision");
        Ok(QueueingHint::QueueSkip)
    }

    /// `isSchedulableAfterNodeChange` (`node_unschedulable.go:107-128`).
    pub fn is_schedulable_after_node_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_node, modified_node) = as_typed::<Node>(old_obj, new_obj)?;
        let modified_node =
            modified_node.ok_or_else(|| "node event without a new object".to_string())?;

        // We queue this Pod when -
        // 1. the node is updated from unschedulable to schedulable.
        // 2. the node is added and is schedulable.
        let modified_unschedulable = node_unschedulable_flag(modified_node);
        if original_node.is_some_and(|o| node_unschedulable_flag(o) && !modified_unschedulable)
            || (original_node.is_none() && !modified_unschedulable)
        {
            debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created or updated, pod may be schedulable now");
            return Ok(QueueingHint::Queue);
        }

        debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created or updated, but it doesn't make this pod schedulable");
        Ok(QueueingHint::QueueSkip)
    }
}

/// `tainttoleration/taint_toleration.go` queueing hints.
pub mod taint_toleration {
    use super::*;

    /// `isSchedulableAfterNodeChange` (`taint_toleration.go:96-115`).
    pub fn is_schedulable_after_node_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_node, modified_node) = as_typed::<Node>(old_obj, new_obj)?;
        let modified_node =
            modified_node.ok_or_else(|| "node event without a new object".to_string())?;

        let tolerations = pod_tolerations(pod);
        let mut was_untolerated = true;
        if let Some(original) = original_node {
            was_untolerated =
                taints::has_untolerated_do_not_schedule_taint(node_taints(original), tolerations);
        }
        let is_untolerated =
            taints::has_untolerated_do_not_schedule_taint(node_taints(modified_node), tolerations);

        if was_untolerated && !is_untolerated {
            debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created or updated, and this may make the Pod rejected by TaintToleration plugin in the previous scheduling cycle schedulable");
            return Ok(QueueingHint::Queue);
        }

        debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "node was created or updated, but it doesn't change the TaintToleration plugin's decision");
        Ok(QueueingHint::QueueSkip)
    }

    /// `isSchedulableAfterPodTolerationChange` (`taint_toleration.go:228-245`).
    pub fn is_schedulable_after_pod_toleration_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (_, modified_pod) = as_typed::<Pod>(old_obj, new_obj)?;
        let modified_pod =
            modified_pod.ok_or_else(|| "pod event without a new object".to_string())?;

        if pod.metadata.uid == modified_pod.metadata.uid {
            // The updated Pod is the unschedulable Pod.
            debug!(pod = %pod_name(modified_pod), "a new toleration is added for the unschedulable Pod, and it may make it schedulable");
            return Ok(QueueingHint::Queue);
        }

        debug!(pod = %pod_name(modified_pod), "a new toleration is added for a Pod, but it's an unrelated Pod and wouldn't change the TaintToleration plugin's decision");
        Ok(QueueingHint::QueueSkip)
    }
}

/// `schedulinggates/scheduling_gates.go` queueing hints.
pub mod scheduling_gates {
    use super::*;

    /// `isSchedulableAfterUpdatePodSchedulingGatesEliminated`
    /// (`scheduling_gates.go:82-94`).
    pub fn is_schedulable_after_update_pod_scheduling_gates_eliminated(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (_, modified_pod) = as_typed::<Pod>(old_obj, new_obj)?;
        let modified_pod =
            modified_pod.ok_or_else(|| "pod event without a new object".to_string())?;

        if modified_pod.metadata.uid != pod.metadata.uid {
            // If the update event is not for targetPod, it wouldn't make targetPod schedulable.
            return Ok(QueueingHint::QueueSkip);
        }

        Ok(QueueingHint::Queue)
    }
}

/// `interpodaffinity/plugin.go` queueing hints.
pub mod inter_pod_affinity {
    use super::*;
    use rusternetes_common::resources::PodAffinityTerm;
    use rusternetes_common::types::{label_selector_as_selector, Selector};
    use std::collections::HashSet;

    /// `fwk.AffinityTerm` (`kube-scheduler/framework/types.go:380-385`).
    struct AffinityTerm {
        namespaces: HashSet<String>,
        selector: Selector,
        topology_key: String,
        namespace_selector: Selector,
    }

    impl AffinityTerm {
        /// `AffinityTerm.Matches` (`types.go:387-392`) with `nsLabels == nil`,
        /// the only way `podMatchesAllAffinityTerms` calls it.
        fn matches(&self, pod: &Pod) -> bool {
            if self
                .namespaces
                .contains(pod.metadata.namespace.as_deref().unwrap_or(""))
                || self.namespace_selector.matches(None)
            {
                return self.selector.matches(pod.metadata.labels.as_ref());
            }
            false
        }
    }

    /// `newAffinityTerm` (`types.go:419-433`) with
    /// `getNamespacesFromPodAffinityTerm` (`types.go:436-445`).
    fn new_affinity_term(pod: &Pod, term: &PodAffinityTerm) -> Result<AffinityTerm, String> {
        let selector = label_selector_as_selector(term.label_selector.as_ref())?;
        let namespaces_given = term.namespaces.as_ref().is_some_and(|n| !n.is_empty());
        let namespaces = if !namespaces_given && term.namespace_selector.is_none() {
            HashSet::from([pod.metadata.namespace.clone().unwrap_or_default()])
        } else {
            term.namespaces.iter().flatten().cloned().collect()
        };
        let namespace_selector = label_selector_as_selector(term.namespace_selector.as_ref())?;
        Ok(AffinityTerm {
            namespaces,
            selector,
            topology_key: term.topology_key.clone(),
            namespace_selector,
        })
    }

    /// `GetAffinityTerms` (`types.go:402-417`).
    fn get_affinity_terms(
        pod: &Pod,
        v1_terms: &[PodAffinityTerm],
    ) -> Result<Vec<AffinityTerm>, String> {
        v1_terms.iter().map(|t| new_affinity_term(pod, t)).collect()
    }

    /// `GetPodAffinityTerms` (`types.go:447-458`).
    fn get_pod_affinity_terms(pod: &Pod) -> &[PodAffinityTerm] {
        pod.spec
            .as_ref()
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.pod_affinity.as_ref())
            .and_then(|a| {
                a.required_during_scheduling_ignored_during_execution
                    .as_deref()
            })
            .unwrap_or(&[])
    }

    /// `GetPodAntiAffinityTerms` (`types.go:479-490`).
    fn get_pod_anti_affinity_terms(pod: &Pod) -> &[PodAffinityTerm] {
        pod.spec
            .as_ref()
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.pod_anti_affinity.as_ref())
            .and_then(|a| {
                a.required_during_scheduling_ignored_during_execution
                    .as_deref()
            })
            .unwrap_or(&[])
    }

    /// `podMatchesAllAffinityTerms` (`filtering.go:186-199`): no terms is
    /// "no match".
    fn pod_matches_all_affinity_terms(terms: &[AffinityTerm], pod: &Pod) -> bool {
        if terms.is_empty() {
            return false;
        }
        terms.iter().all(|t| t.matches(pod))
    }

    fn spec_node_name(pod: &Pod) -> &str {
        pod.spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .unwrap_or("")
    }

    fn node_label<'a>(node: &'a Node, key: &str) -> Option<&'a String> {
        node.metadata.labels.as_ref().and_then(|l| l.get(key))
    }

    /// `isSchedulableAfterPodChange` (`plugin.go:171-232`).
    pub fn is_schedulable_after_pod_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_pod, modified_pod) = as_typed::<Pod>(old_obj, new_obj)?;
        if modified_pod.is_some_and(|p| spec_node_name(p).is_empty())
            || original_pod.is_some_and(|p| spec_node_name(p).is_empty())
        {
            debug!(pod = %pod_name(pod), "the added/updated/deleted pod is unscheduled, so it doesn't make the target pod schedulable");
            return Ok(QueueingHint::QueueSkip);
        }

        let terms = get_affinity_terms(pod, get_pod_affinity_terms(pod))?;
        let anti_terms = get_affinity_terms(pod, get_pod_anti_affinity_terms(pod))?;

        // Pod is updated. Return Queue when the updated pod matching the target pod's affinity or not matching anti-affinity.
        // Note that, we don't need to check each affinity individually when the Pod has more than one affinity
        // because the current PodAffinity looks for a **single** existing pod that can satisfy **all** the terms of inter-pod affinity of an incoming pod.
        if let (Some(modified), Some(original)) = (modified_pod, original_pod) {
            if !pod_matches_all_affinity_terms(&terms, original)
                && pod_matches_all_affinity_terms(&terms, modified)
            {
                debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified), "a scheduled pod was updated to match the target pod's affinity, and the pod may be schedulable now");
                return Ok(QueueingHint::Queue);
            }
            if pod_matches_all_affinity_terms(&anti_terms, original)
                && !pod_matches_all_affinity_terms(&anti_terms, modified)
            {
                debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified), "a scheduled pod was updated not to match the target pod's anti affinity, and the pod may be schedulable now");
                return Ok(QueueingHint::Queue);
            }
            debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified), "a scheduled pod was updated but it doesn't match the target pod's affinity or does match the target pod's anti-affinity");
            return Ok(QueueingHint::QueueSkip);
        }

        // Pod is added. Return Queue when the added pod matching the target pod's affinity.
        if let Some(modified) = modified_pod {
            if pod_matches_all_affinity_terms(&terms, modified) {
                debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified), "a scheduled pod was added and it matches the target pod's affinity");
                return Ok(QueueingHint::Queue);
            }
            debug!(pod = %pod_name(pod), modified_pod = %pod_name(modified), "a scheduled pod was added and it doesn't match the target pod's affinity");
            return Ok(QueueingHint::QueueSkip);
        }

        // Pod is deleted. Return Queue when the deleted pod matching the target pod's anti-affinity.
        // (upstream dereferences originalPod here; as_typed with neither
        // object cannot reach this arm in a well-formed event)
        let original =
            original_pod.ok_or_else(|| "pod event without an old or new object".to_string())?;
        if !pod_matches_all_affinity_terms(&anti_terms, original) {
            debug!(pod = %pod_name(pod), "a scheduled pod was deleted but it doesn't match the target pod's anti-affinity");
            return Ok(QueueingHint::QueueSkip);
        }
        debug!(pod = %pod_name(pod), "a scheduled pod was deleted and it matches the target pod's anti-affinity. The pod may be schedulable now");
        Ok(QueueingHint::Queue)
    }

    /// `isSchedulableAfterNodeChange` (`plugin.go:234-320`).
    pub fn is_schedulable_after_node_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original_node, modified_node) = as_typed::<Node>(old_obj, new_obj)?;
        let modified_node =
            modified_node.ok_or_else(|| "node event without a new object".to_string())?;

        let terms = get_affinity_terms(pod, get_pod_affinity_terms(pod))?;

        // When queuing this Pod:
        // - 1. A new node is added with the pod affinity topologyKey, the pod may become schedulable.
        // - 2. The original node does not have the pod affinity topologyKey but the modified node does, the pod may become schedulable.
        // - 3. Both the original and modified nodes have the pod affinity topologyKey and they differ, the pod may become schedulable.
        for term in &terms {
            let Some(original_node) = original_node else {
                if node_label(modified_node, &term.topology_key).is_some() {
                    // Case 1: A new node is added with the pod affinity topologyKey.
                    debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "A node with a matched pod affinity topologyKey was added and it may make the pod schedulable");
                    return Ok(QueueingHint::Queue);
                }
                continue;
            };
            let original_value = node_label(original_node, &term.topology_key);
            let modified_value = node_label(modified_node, &term.topology_key);

            if original_value.is_none() && modified_value.is_some() {
                // Case 2: Original node does not have the pod affinity topologyKey, but the modified node does.
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "A node got updated to have the topology key of pod affinity, which may make the pod schedulable");
                return Ok(QueueingHint::Queue);
            }

            if original_value.is_some()
                && modified_value.is_some()
                && original_value != modified_value
            {
                // Case 3: Both nodes have the pod affinity topologyKey, but the values differ.
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "A node is moved to a different domain of pod affinity, which may make the pod schedulable");
                return Ok(QueueingHint::Queue);
            }
        }

        let anti_terms = get_affinity_terms(pod, get_pod_anti_affinity_terms(pod))?;

        // When queuing this Pod:
        // - 1. A new node is added, the pod may become schedulable.
        // - 2. The original node have the pod anti-affinity topologyKey but the modified node does not, the pod may become schedulable.
        // - 3. Both the original and modified nodes have the pod anti-affinity topologyKey and they differ, the pod may become schedulable.
        for term in &anti_terms {
            let Some(original_node) = original_node else {
                // Case 1: A new node is added.
                // We always requeue the Pod with anti-affinity because:
                // - the node without the topology key is always allowed to have a Pod with anti-affinity.
                // - the addition of a node with the topology key makes Pods schedulable only when the topology it joins doesn't have any Pods that the Pod hates.
                //   But, it's out-of-scope of this QHint to check which Pods are in the topology this Node is in.
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "A node was added and it may make the pod schedulable");
                return Ok(QueueingHint::Queue);
            };

            let original_value = node_label(original_node, &term.topology_key);
            let modified_value = node_label(modified_node, &term.topology_key);

            if original_value.is_some() && modified_value.is_none() {
                // Case 2: The original node have the pod anti-affinity topologyKey but the modified node does not.
                // Note that we don't need to check the opposite case (!originalHasKey && modifiedHasKey)
                // because the node without the topology label can always accept pods with pod anti-affinity.
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "A node got updated to not have the topology key of pod anti-affinity, which may make the pod schedulable");
                return Ok(QueueingHint::Queue);
            }

            if original_value.is_some()
                && modified_value.is_some()
                && original_value != modified_value
            {
                // Case 3: Both nodes have the pod anti-affinity topologyKey, but the values differ.
                debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "A node is moved to a different domain of pod anti-affinity, which may make the pod schedulable");
                return Ok(QueueingHint::Queue);
            }
        }
        debug!(pod = %pod_name(pod), node = %modified_node.metadata.name, "a node is added/updated but doesn't have any topologyKey which matches pod affinity/anti-affinity");
        Ok(QueueingHint::QueueSkip)
    }
}
