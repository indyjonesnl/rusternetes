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
//! NodeResourcesFit, NodeAffinity and InterPodAffinity ship hint functions
//! in this slice; every other in-tree plugin registers its events hint-less, which
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
            ev(
                R::Pod,
                A::ADD | A::UPDATE_POD_LABEL | A::UPDATE_POD_TOLERATION | A::DELETE,
            ),
            ev(
                R::Node,
                A::ADD | A::DELETE | A::UPDATE_NODE_LABEL | A::UPDATE_NODE_TAINT,
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
            ev(R::Pod, A::DELETE),
            ev(R::Node, A::ADD),
            ev(R::PersistentVolumeClaim, A::ADD),
        ],
        // volumezone/volume_zone.go:267-285
        names::VOLUME_ZONE => vec![
            ev(R::StorageClass, A::ADD),
            ev(R::Node, A::ADD | A::UPDATE_NODE_LABEL),
            ev(R::PersistentVolumeClaim, A::ADD | A::UPDATE),
            ev(R::PersistentVolume, A::ADD | A::UPDATE),
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

    // ---- InterPodAffinity (`plugin_test.go`) ----

    #[derive(Clone, Copy)]
    enum Kind {
        Affinity,
        AntiAffinity,
    }

    fn selector_in(key: &str, values: &[&str]) -> serde_json::Value {
        json!({"matchExpressions": [{"key": key, "operator": "In", "values": values}]})
    }

    /// `st.MakePod().Name("p")` with the given required terms appended.
    fn ipa_pod(terms: Vec<(Kind, serde_json::Value, &str)>) -> Pod {
        let mut aff: Vec<serde_json::Value> = vec![];
        let mut anti: Vec<serde_json::Value> = vec![];
        for (kind, sel, tk) in terms {
            let t = json!({"labelSelector": sel, "topologyKey": tk});
            match kind {
                Kind::Affinity => aff.push(t),
                Kind::AntiAffinity => anti.push(t),
            }
        }
        let mut affinity = serde_json::Map::new();
        if !aff.is_empty() {
            affinity.insert(
                "podAffinity".into(),
                json!({"requiredDuringSchedulingIgnoredDuringExecution": aff}),
            );
        }
        if !anti.is_empty() {
            affinity.insert(
                "podAntiAffinity".into(),
                json!({"requiredDuringSchedulingIgnoredDuringExecution": anti}),
            );
        }
        serde_json::from_value(json!({
            "metadata": {"name": "p"},
            "spec": {"affinity": affinity}
        }))
        .unwrap()
    }

    fn labelled_pod(node: Option<&str>, labels: &[(&str, &str)]) -> Pod {
        let labels: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect();
        let mut spec = serde_json::Map::new();
        if let Some(n) = node {
            spec.insert("nodeName".into(), json!(n));
        }
        serde_json::from_value(json!({
            "metadata": {"labels": labels},
            "spec": spec
        }))
        .unwrap()
    }

    fn named_node(labels: &[(&str, &str)]) -> Node {
        let labels: serde_json::Map<String, serde_json::Value> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v)))
            .collect();
        serde_json::from_value(json!({"metadata": {"name": "node-a", "labels": labels}})).unwrap()
    }

    /// `Test_isSchedulableAfterPodChange` (`plugin_test.go:37-151`).
    #[test]
    fn inter_pod_affinity_is_schedulable_after_pod_change() {
        use Kind::*;
        let svc_aff = || {
            ipa_pod(vec![(
                Affinity,
                selector_in("service", &["securityscan", "value2"]),
                "region",
            )])
        };
        let svc_anti = || {
            ipa_pod(vec![(
                AntiAffinity,
                selector_in("service", &["securityscan", "value2"]),
                "region",
            )])
        };
        let exists = |k: &str| json!({"matchExpressions": [{"key": k, "operator": "Exists"}]});
        let two_terms = || {
            ipa_pod(vec![
                (Affinity, exists("aaa"), "hostname"),
                (Affinity, exists("bbb"), "hostname"),
            ])
        };
        let n = Some("fake-node");
        type PodCase<'a> = (&'a str, Pod, Option<Pod>, Option<Pod>, QueueingHint);
        let cases: Vec<PodCase> = vec![
            (
                "add a pod which matches the pod affinity",
                svc_aff(),
                None,
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::Queue,
            ),
            (
                "add an un-scheduled pod",
                svc_aff(),
                None,
                Some(labelled_pod(None, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "add a pod which doesn't match the pod affinity",
                svc_aff(),
                None,
                Some(labelled_pod(n, &[("aaa", "a")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from non-match to match pod affinity",
                svc_aff(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::Queue,
            ),
            (
                "the updating pod matches target pod's affinity both before and after label changes",
                svc_aff(),
                Some(labelled_pod(n, &[("service", "value2")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update an un-scheduled pod",
                svc_aff(),
                Some(labelled_pod(None, &[("service", "securityscan")])),
                Some(labelled_pod(None, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from match to non-match the pod affinity",
                svc_aff(),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                Some(labelled_pod(n, &[("aaa", "a")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from match to non-match pod's affinity - multiple terms case",
                two_terms(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "update a pod from non-match to match pod's affinity - multiple terms case",
                two_terms(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                Some(labelled_pod(n, &[("aaa", ""), ("bbb", "")])),
                QueueingHint::Queue,
            ),
            (
                "modify pod label to change it from satisfying pod anti-affinity to not satisfying anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                Some(labelled_pod(n, &[("service", "aaaa")])),
                QueueingHint::Queue,
            ),
            (
                "modify pod label to change it from not satisfying pod anti-affinity to satisfying anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("service", "bbb")])),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                QueueingHint::QueueSkip,
            ),
            (
                "delete a pod which doesn't match pod's anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("aaa", "a")])),
                None,
                QueueingHint::QueueSkip,
            ),
            (
                "delete a pod which matches pod's anti-affinity",
                svc_anti(),
                Some(labelled_pod(n, &[("service", "securityscan")])),
                None,
                QueueingHint::Queue,
            ),
        ];
        for (name, pod, old, new, want) in cases {
            let old_b = old.map(obj);
            let new_b = new.map(obj);
            let got = inter_pod_affinity::is_schedulable_after_pod_change(
                &pod,
                old_b.as_deref(),
                new_b.as_deref(),
            )
            .unwrap();
            assert_eq!(got, want, "{name}");
        }
        // util.As error path: a wrong-typed object is an error (backoff).
        assert!(inter_pod_affinity::is_schedulable_after_pod_change(
            &svc_aff(),
            None,
            Some(&*obj("not-a-pod".to_string()))
        )
        .is_err());
    }

    /// `Test_isSchedulableAfterNodeChange` (`plugin_test.go:153-262`).
    #[test]
    fn inter_pod_affinity_is_schedulable_after_node_change() {
        use Kind::*;
        let aff = |tk: &str| {
            ipa_pod(vec![(
                Affinity,
                selector_in("service", &["securityscan", "value2"]),
                tk,
            )])
        };
        let anti = || {
            ipa_pod(vec![(
                AntiAffinity,
                json!({"matchLabels": {"another": "label"}}),
                "zone",
            )])
        };
        let cases: Vec<(&str, Pod, Option<Node>, Node, QueueingHint)> = vec![
            (
                "add a new node with matched pod affinity topologyKey",
                aff("zone"),
                None,
                named_node(&[("zone", "zone1")]),
                QueueingHint::Queue,
            ),
            (
                "add a new node without matched topologyKey",
                aff("region"),
                None,
                named_node(&[("zone", "zone1")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node label but not topologyKey",
                aff("zone"),
                Some(named_node(&[("aaa", "a")])),
                named_node(&[("aaa", "b")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node label that isn't related to the pod affinity",
                aff("zone"),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone1"), ("unrelated-label", "unrelated")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node with different affinity topologyKey value",
                aff("zone"),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone2")]),
                QueueingHint::Queue,
            ),
            (
                "update node to have the affinity topology label",
                aff("zone"),
                Some(named_node(&[("aaa", "a")])),
                named_node(&[("zone", "zone1")]),
                QueueingHint::Queue,
            ),
            (
                "add a new node with matched pod anti-affinity topologyKey",
                anti(),
                None,
                named_node(&[("zone", "zone1")]),
                QueueingHint::Queue,
            ),
            (
                "add a new node without matched pod anti-affinity topologyKey",
                anti(),
                None,
                named_node(&[]),
                QueueingHint::Queue,
            ),
            (
                "update node label that isn't related to the pod anti-affinity",
                anti(),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone1"), ("unrelated-label", "unrelated")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node with different anti-affinity topologyKey value",
                anti(),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("zone", "zone2")]),
                QueueingHint::Queue,
            ),
            (
                "update node to have the anti-affinity topology label",
                anti(),
                Some(named_node(&[("aaa", "a")])),
                named_node(&[("zone", "zone1")]),
                QueueingHint::QueueSkip,
            ),
            (
                "update node label not to have anti-affinity topology label",
                anti(),
                Some(named_node(&[("zone", "zone1")])),
                named_node(&[("aaa", "a")]),
                QueueingHint::Queue,
            ),
        ];
        for (name, pod, old, new, want) in cases {
            let old_b = old.map(obj);
            let got = inter_pod_affinity::is_schedulable_after_node_change(
                &pod,
                old_b.as_deref(),
                Some(&*obj(new)),
            )
            .unwrap();
            assert_eq!(got, want, "{name}");
        }
        assert!(inter_pod_affinity::is_schedulable_after_node_change(
            &aff("zone"),
            None,
            Some(&*obj("not-a-node".to_string()))
        )
        .is_err());
    }

    /// `newAffinityTerm` / `AffinityTerm.Matches` (`kube-scheduler/framework/
    /// types.go:387-445`): the namespace set, namespaceSelector and an absent
    /// labelSelector (`labels.Nothing()`) all take part in the match.
    #[test]
    fn inter_pod_affinity_term_namespace_and_selector_semantics() {
        let mk = |term: serde_json::Value, ns: &str| -> Pod {
            serde_json::from_value(json!({
                "metadata": {"name": "p", "namespace": ns},
                "spec": {"affinity": {"podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [term]}}}
            }))
            .unwrap()
        };
        let scheduled = |ns: &str| -> Pod {
            serde_json::from_value(json!({
                "metadata": {"namespace": ns, "labels": {"a": "b"}},
                "spec": {"nodeName": "n"}
            }))
            .unwrap()
        };
        let del = |target: &Pod, p: Pod| {
            inter_pod_affinity::is_schedulable_after_pod_change(target, Some(&*obj(p)), None)
                .unwrap()
        };
        let sel = json!({"matchLabels": {"a": "b"}});
        // default namespace = the target pod's own
        let t = mk(json!({"labelSelector": sel, "topologyKey": "z"}), "ns1");
        assert_eq!(del(&t, scheduled("ns1")), QueueingHint::Queue);
        assert_eq!(del(&t, scheduled("ns2")), QueueingHint::QueueSkip);
        // explicit namespaces
        let t = mk(
            json!({"labelSelector": sel, "topologyKey": "z", "namespaces": ["ns2"]}),
            "ns1",
        );
        assert_eq!(del(&t, scheduled("ns2")), QueueingHint::Queue);
        assert_eq!(del(&t, scheduled("ns1")), QueueingHint::QueueSkip);
        // namespaceSelector set, nsLabels nil: empty selector matches all
        let t = mk(
            json!({"labelSelector": sel, "topologyKey": "z", "namespaceSelector": {}}),
            "ns1",
        );
        assert_eq!(del(&t, scheduled("ns9")), QueueingHint::Queue);
        // absent labelSelector is labels.Nothing(): never matches
        let t = mk(json!({"topologyKey": "z"}), "ns1");
        assert_eq!(del(&t, scheduled("ns1")), QueueingHint::QueueSkip);
        // an invalid selector is an error (backoff)
        let t = mk(
            json!({"labelSelector": {"matchExpressions": [{"key": "a", "operator": "Bogus"}]}, "topologyKey": "z"}),
            "ns1",
        );
        assert!(inter_pod_affinity::is_schedulable_after_pod_change(
            &t,
            Some(&*obj(scheduled("ns1"))),
            None
        )
        .is_err());
    }
}
