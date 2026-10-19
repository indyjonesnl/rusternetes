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
//! - `nodeports/node_ports.go` (`:117-158`), `nodeunschedulable/
//!   node_unschedulable.go` (`:50-128`), `tainttoleration/taint_toleration.go`
//!   (`:55-115`, `:228-245`).
//! - `pkg/scheduler/framework/plugins/schedulinggates/scheduling_gates.go`
//!   (`:59-94`).
//!
//! NodeResourcesFit, NodeAffinity, NodePorts, NodeUnschedulable,
//! TaintToleration and SchedulingGates ship hint functions; every other in-tree plugin registers its events hint-less, which
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
}
