//! Cluster events and queueing hints — the framework half of the
//! scheduling-queue requeue mechanism.
//!
//! Ported from upstream:
//! - `staging/src/k8s.io/kube-scheduler/framework/types.go`: `ActionType`
//!   (`:31-119`), `EventResource` (`:121-183`), `ClusterEventWithHint`
//!   (`:185-193`), `QueueingHintFn` (`:195-206`), `QueueingHint` (`:208-227`),
//!   `ClusterEvent` + `Label()` (`:229-249`).
//! - `staging/src/k8s.io/kube-scheduler/framework/interface.go`:
//!   `EnqueueExtensions` (`:397-428`).
//! - `pkg/scheduler/framework/types.go`: `assignedPod`/`unschedulablePod`
//!   (`:55-60`), `ClusterEventIsWildCard` (`:101-103`), `MatchClusterEvents`
//!   (`:120-123`), `matchEventResources` (`:126-134`), `MatchAnyClusterEvent`
//!   (`:136-143`), `UnrollWildCardResource` (`:145-162`).
//! - `pkg/scheduler/framework/events.go`: the special event labels (`:32-45`),
//!   the `Event*` constants (`:47-64`), `PodSchedulingPropertiesChange`
//!   (`:68-97`) with its extractors (`:101-152`) and
//!   `NodeSchedulingPropertiesChange` (`:164-184`) with its extractors
//!   (`:188-240`).
//! - `pkg/scheduler/util/utils.go`: `As` (`:158-179`).
//!
//! Feature gates fixed at their 1.35 defaults: `SchedulerQueueingHints` is GA
//! and locked on (`pkg/features/kube_features.go:1762-1766`);
//! `DynamicResourceAllocation` is GA (default on) so its
//! `UpdatePodGeneratedResourceClaim` extractor is always active;
//! `NodeDeclaredFeatures`' `extractNodeFeaturesChange` is omitted (that gate is
//! off / the resource is not scheduled here yet).

use rusternetes_common::resources::{Node, Pod};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// `ActionType` (`types.go:33`): an integer representing one type of resource
/// change. Different ActionTypes can be bit-wised to compose new semantics.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ActionType(pub i64);

impl ActionType {
    pub const ADD: ActionType = ActionType(1 << 0);
    pub const DELETE: ActionType = ActionType(1 << 1);
    // `UpdateNodeXYZ` is only applicable for Node events (`types.go:43-55`).
    pub const UPDATE_NODE_ALLOCATABLE: ActionType = ActionType(1 << 2);
    pub const UPDATE_NODE_LABEL: ActionType = ActionType(1 << 3);
    /// An update for node's taints or `node.Spec.Unschedulable`.
    pub const UPDATE_NODE_TAINT: ActionType = ActionType(1 << 4);
    pub const UPDATE_NODE_CONDITION: ActionType = ActionType(1 << 5);
    pub const UPDATE_NODE_ANNOTATION: ActionType = ActionType(1 << 6);
    pub const UPDATE_NODE_DECLARED_FEATURE: ActionType = ActionType(1 << 7);
    // `UpdatePodXYZ` is only applicable for Pod events (`types.go:57-72`).
    pub const UPDATE_POD_LABEL: ActionType = ActionType(1 << 8);
    /// An update for pod's scale down (i.e., any resource request is reduced).
    pub const UPDATE_POD_SCALE_DOWN: ActionType = ActionType(1 << 9);
    /// An addition for pod's tolerations (API validation only allows adding).
    pub const UPDATE_POD_TOLERATION: ActionType = ActionType(1 << 10);
    /// An update that eliminates all scheduling gates in the Pod.
    pub const UPDATE_POD_SCHEDULING_GATES_ELIMINATED: ActionType = ActionType(1 << 11);
    pub const UPDATE_POD_GENERATED_RESOURCE_CLAIM: ActionType = ActionType(1 << 12);

    /// `All ActionType = 1<<iota - 1` (`types.go:74`).
    pub const ALL: ActionType = ActionType((1 << 13) - 1);

    /// The general Update type (`types.go:77`).
    pub const UPDATE: ActionType = ActionType(
        Self::UPDATE_NODE_ALLOCATABLE.0
            | Self::UPDATE_NODE_LABEL.0
            | Self::UPDATE_NODE_TAINT.0
            | Self::UPDATE_NODE_CONDITION.0
            | Self::UPDATE_NODE_ANNOTATION.0
            | Self::UPDATE_NODE_DECLARED_FEATURE.0
            | Self::UPDATE_POD_LABEL.0
            | Self::UPDATE_POD_SCALE_DOWN.0
            | Self::UPDATE_POD_TOLERATION.0
            | Self::UPDATE_POD_SCHEDULING_GATES_ELIMINATED.0
            | Self::UPDATE_POD_GENERATED_RESOURCE_CLAIM.0,
    );

    /// `None` is a special ActionType that is only used internally.
    pub const NONE: ActionType = ActionType(0);

    pub fn contains(self, other: ActionType) -> bool {
        self.0 & other.0 != 0
    }
}

impl std::ops::BitOr for ActionType {
    type Output = ActionType;
    fn bitor(self, rhs: ActionType) -> ActionType {
        ActionType(self.0 | rhs.0)
    }
}

impl std::ops::BitAnd for ActionType {
    type Output = ActionType;
    fn bitand(self, rhs: ActionType) -> ActionType {
        ActionType(self.0 & rhs.0)
    }
}

impl std::ops::BitOrAssign for ActionType {
    fn bitor_assign(&mut self, rhs: ActionType) {
        self.0 |= rhs.0;
    }
}

/// `ActionType.String()` (`types.go:83-119`).
impl fmt::Display for ActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match *self {
            Self::ADD => "Add",
            Self::DELETE => "Delete",
            Self::UPDATE_NODE_ALLOCATABLE => "UpdateNodeAllocatable",
            Self::UPDATE_NODE_LABEL => "UpdateNodeLabel",
            Self::UPDATE_NODE_TAINT => "UpdateNodeTaint",
            Self::UPDATE_NODE_CONDITION => "UpdateNodeCondition",
            Self::UPDATE_NODE_ANNOTATION => "UpdateNodeAnnotation",
            Self::UPDATE_NODE_DECLARED_FEATURE => "UpdateNodeDeclaredFeature",
            Self::UPDATE_POD_LABEL => "UpdatePodLabel",
            Self::UPDATE_POD_SCALE_DOWN => "UpdatePodScaleDown",
            Self::UPDATE_POD_TOLERATION => "UpdatePodToleration",
            Self::UPDATE_POD_SCHEDULING_GATES_ELIMINATED => "UpdatePodSchedulingGatesEliminated",
            Self::UPDATE_POD_GENERATED_RESOURCE_CLAIM => "UpdatePodGeneratedResourceClaim",
            Self::ALL => "All",
            Self::UPDATE => "Update",
            // Shouldn't reach here.
            _ => "",
        };
        f.write_str(s)
    }
}

impl fmt::Debug for ActionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.to_string();
        if s.is_empty() {
            write!(f, "ActionType({:#b})", self.0)
        } else {
            f.write_str(&s)
        }
    }
}

/// `EventResource` (`types.go:122`): short for group/version/kind.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum EventResource {
    Pod,
    /// Internal resource representing an assigned Pod (`pkg/.../types.go:58`).
    /// Never used in `EventsToRegister`; only for incoming events.
    AssignedPod,
    /// Internal resource representing an unscheduled Pod (`pkg/.../types.go:59`).
    UnschedulablePod,
    Node,
    PersistentVolume,
    PersistentVolumeClaim,
    CsiNode,
    CsiDriver,
    VolumeAttachment,
    CsiStorageCapacity,
    StorageClass,
    ResourceClaim,
    ResourceSlice,
    DeviceClass,
    Workload,
    /// Special resource matching all resources (`types.go:175-182`).
    WildCard,
}

impl fmt::Display for EventResource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pod => "Pod",
            Self::AssignedPod => "AssignedPod",
            Self::UnschedulablePod => "UnschedulablePod",
            Self::Node => "Node",
            Self::PersistentVolume => "PersistentVolume",
            Self::PersistentVolumeClaim => "PersistentVolumeClaim",
            Self::CsiNode => "storage.k8s.io/CSINode",
            Self::CsiDriver => "storage.k8s.io/CSIDriver",
            Self::VolumeAttachment => "storage.k8s.io/VolumeAttachment",
            Self::CsiStorageCapacity => "storage.k8s.io/CSIStorageCapacity",
            Self::StorageClass => "storage.k8s.io/StorageClass",
            Self::ResourceClaim => "resource.k8s.io/ResourceClaim",
            Self::ResourceSlice => "resource.k8s.io/ResourceSlice",
            Self::DeviceClass => "resource.k8s.io/DeviceClass",
            Self::Workload => "scheduling.k8s.io/Workload",
            Self::WildCard => "*",
        })
    }
}

/// `ClusterEvent` (`types.go:232-240`): how a system resource's state gets
/// changed. `Resource` is the API resource, `ActionType` the specific change.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ClusterEvent {
    pub resource: EventResource,
    pub action_type: ActionType,
    /// `CustomLabel` controls `Label()`; only used for special events like
    /// `UnschedulableTimeout`.
    pub custom_label: &'static str,
}

impl ClusterEvent {
    pub const fn new(resource: EventResource, action_type: ActionType) -> Self {
        Self {
            resource,
            action_type,
            custom_label: "",
        }
    }

    /// `Label()` (`types.go:243-249`), used for logging and metrics.
    pub fn label(&self) -> String {
        if !self.custom_label.is_empty() {
            return self.custom_label.to_string();
        }
        format!("{}{}", self.resource, self.action_type)
    }
}

/// `QueueingHint` (`types.go:208-217`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum QueueingHint {
    /// The cluster event has no impact on scheduling of the pod.
    QueueSkip,
    /// The Pod may be schedulable by the event.
    Queue,
}

impl fmt::Display for QueueingHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::QueueSkip => "QueueSkip",
            Self::Queue => "Queue",
        })
    }
}

/// The `interface{}` object carried by an event (`oldObj`/`newObj`). Hint
/// functions downcast it with [`as_typed`] (`util.As`).
pub type EventObject = dyn Any + Send + Sync;

/// `QueueingHintFn` (`types.go:206`): signals whether the event can make a Pod
/// rejected by this plugin in the past schedulable. Called before a Pod moves
/// from unschedulableQ to backoffQ or activeQ. On `Err`, the caller takes the
/// hint as `Queue` so the Pod cannot get stuck in the unschedulable pool.
///
/// - `pod`: the Pod to be enqueued, rejected by this plugin in the past.
/// - `old_obj`/`new_obj`: the object involved in the event; `old_obj` is
///   `None` for an add event, `new_obj` is `None` for a delete event.
pub type QueueingHintFn = Arc<
    dyn Fn(&Pod, Option<&EventObject>, Option<&EventObject>) -> Result<QueueingHint, String>
        + Send
        + Sync,
>;

/// `ClusterEventWithHint` (`types.go:185-193`).
#[derive(Clone)]
pub struct ClusterEventWithHint {
    pub event: ClusterEvent,
    /// Optional. If not set, the scheduling of Pods will always be retried
    /// with backoff when this event happens (the same as `Queue`).
    pub queueing_hint_fn: Option<QueueingHintFn>,
}

impl ClusterEventWithHint {
    pub fn new(event: ClusterEvent) -> Self {
        Self {
            event,
            queueing_hint_fn: None,
        }
    }

    pub fn with_hint(event: ClusterEvent, hint: QueueingHintFn) -> Self {
        Self {
            event,
            queueing_hint_fn: Some(hint),
        }
    }
}

impl fmt::Debug for ClusterEventWithHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterEventWithHint")
            .field("event", &self.event)
            .field("queueing_hint_fn", &self.queueing_hint_fn.is_some())
            .finish()
    }
}

/// `EnqueueExtensions` (`interface.go:414-428`): an optional interface that
/// plugins implement to efficiently move unschedulable Pods in the internal
/// scheduling queues. Pods can be made unschedulable by PreEnqueue, PreFilter,
/// Filter, Reserve and Permit plugins, and Pods rejected by these plugins are
/// requeued based on this extension point.
pub trait EnqueueExtensions: Send + Sync {
    fn name(&self) -> &str;
    /// Returns a series of possible events that may cause a Pod failed by this
    /// plugin schedulable. Determined once at startup; never changes at runtime.
    fn events_to_register(&self) -> Result<Vec<ClusterEventWithHint>, String>;
}

/// `util.As` (`pkg/scheduler/util/utils.go:158-179`): downcast the
/// `oldObj`/`newObj` pair of an event to `T`. `None` stays `None`; a present
/// object of another type is an error.
pub fn as_typed<'a, T: 'static>(
    old_obj: Option<&'a EventObject>,
    new_obj: Option<&'a EventObject>,
) -> Result<(Option<&'a T>, Option<&'a T>), String> {
    let new_typed = match new_obj {
        Some(o) => Some(o.downcast_ref::<T>().ok_or_else(|| {
            format!(
                "expected {}, but got {:?}",
                std::any::type_name::<T>(),
                o.type_id()
            )
        })?),
        None => None,
    };
    let old_typed = match old_obj {
        Some(o) => Some(o.downcast_ref::<T>().ok_or_else(|| {
            format!(
                "expected {}, but got {:?}",
                std::any::type_name::<T>(),
                o.type_id()
            )
        })?),
        None => None,
    };
    Ok((old_typed, new_typed))
}

// ---------------------------------------------------------------------------
// pkg/scheduler/framework/types.go
// ---------------------------------------------------------------------------

/// `ClusterEventIsWildCard` (`pkg/.../types.go:101-103`).
pub fn cluster_event_is_wild_card(ce: &ClusterEvent) -> bool {
    ce.resource == EventResource::WildCard && ce.action_type == ActionType::ALL
}

/// `MatchClusterEvents` (`pkg/.../types.go:120-123`): true if `ce` is matched
/// with `incoming_event`, i.e. the incoming event is the same or more specific
/// than `ce`. If `ce.resource` is `*` there is no requirement on the incoming
/// resource. A wildcard *incoming* event is unmatched unless `ce` is wildcard
/// too.
pub fn match_cluster_events(ce: &ClusterEvent, incoming_event: &ClusterEvent) -> bool {
    cluster_event_is_wild_card(ce)
        || (match_event_resources(ce.resource, incoming_event.resource)
            && (ce.action_type & incoming_event.action_type) != ActionType::NONE
            && incoming_event.action_type <= ce.action_type)
}

/// `matchEventResources` (`pkg/.../types.go:126-134`).
fn match_event_resources(r: EventResource, resource: EventResource) -> bool {
    // WildCard matches all resources
    r == EventResource::WildCard
        // Exact match
        || r == resource
        // Pod matches assignedPod and unschedulablePod.
        || (r == EventResource::Pod
            && (resource == EventResource::AssignedPod
                || resource == EventResource::UnschedulablePod))
}

/// `MatchAnyClusterEvent` (`pkg/.../types.go:136-143`).
pub fn match_any_cluster_event(ce: &ClusterEvent, incoming_events: &[ClusterEvent]) -> bool {
    incoming_events.iter().any(|e| match_cluster_events(e, ce))
}

/// `UnrollWildCardResource` (`pkg/.../types.go:145-162`): the events a plugin
/// without `EnqueueExtensions` is registered for (`GenericWorkload` is off).
pub fn unroll_wild_card_resource() -> Vec<ClusterEventWithHint> {
    [
        EventResource::Pod,
        EventResource::Node,
        EventResource::PersistentVolume,
        EventResource::PersistentVolumeClaim,
        EventResource::CsiNode,
        EventResource::CsiDriver,
        EventResource::CsiStorageCapacity,
        EventResource::StorageClass,
        EventResource::ResourceClaim,
        EventResource::DeviceClass,
    ]
    .into_iter()
    .map(|r| ClusterEventWithHint::new(ClusterEvent::new(r, ActionType::ALL)))
    .collect()
}

// ---------------------------------------------------------------------------
// pkg/scheduler/framework/events.go
// ---------------------------------------------------------------------------

/// Special event labels (`events.go:32-45`).
pub const SCHEDULE_ATTEMPT_FAILURE: &str = "ScheduleAttemptFailure";
pub const BACKOFF_COMPLETE: &str = "BackoffComplete";
pub const POP_FROM_BACKOFF_Q: &str = "PopFromBackoffQ";
pub const FORCE_ACTIVATE: &str = "ForceActivate";
pub const UNSCHEDULABLE_TIMEOUT: &str = "UnschedulableTimeout";

/// `EventAssignedPodAdd` (`events.go:49`).
pub const EVENT_ASSIGNED_POD_ADD: ClusterEvent =
    ClusterEvent::new(EventResource::AssignedPod, ActionType::ADD);
/// `EventAssignedPodUpdate` (`events.go:51`).
pub const EVENT_ASSIGNED_POD_UPDATE: ClusterEvent =
    ClusterEvent::new(EventResource::AssignedPod, ActionType::UPDATE);
/// `EventAssignedPodDelete` (`events.go:53`).
pub const EVENT_ASSIGNED_POD_DELETE: ClusterEvent =
    ClusterEvent::new(EventResource::AssignedPod, ActionType::DELETE);
/// `EventUnscheduledPodAdd` (`events.go:55`).
pub const EVENT_UNSCHEDULED_POD_ADD: ClusterEvent =
    ClusterEvent::new(EventResource::UnschedulablePod, ActionType::ADD);
/// `EventUnscheduledPodUpdate` (`events.go:57`).
pub const EVENT_UNSCHEDULED_POD_UPDATE: ClusterEvent =
    ClusterEvent::new(EventResource::UnschedulablePod, ActionType::UPDATE);
/// `EventUnscheduledPodDelete` (`events.go:59`).
pub const EVENT_UNSCHEDULED_POD_DELETE: ClusterEvent =
    ClusterEvent::new(EventResource::UnschedulablePod, ActionType::DELETE);
/// `EventUnschedulableTimeout` (`events.go:61`).
pub const EVENT_UNSCHEDULABLE_TIMEOUT: ClusterEvent = ClusterEvent {
    resource: EventResource::WildCard,
    action_type: ActionType::ALL,
    custom_label: UNSCHEDULABLE_TIMEOUT,
};
/// `EventForceActivate` (`events.go:63`).
pub const EVENT_FORCE_ACTIVATE: ClusterEvent = ClusterEvent {
    resource: EventResource::WildCard,
    action_type: ActionType::ALL,
    custom_label: FORCE_ACTIVATE,
};

fn pod_node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

/// `PodSchedulingPropertiesChange` (`events.go:68-97`): interprets the update
/// of a pod and returns the corresponding `UpdatePodXYZ` event(s). When no
/// specific event is found the general Update action is used, which only
/// triggers plugins registering a general Pod/Update event.
pub fn pod_scheduling_properties_change(new_pod: &Pod, old_pod: &Pod) -> Vec<ClusterEvent> {
    let r = if pod_node_name(new_pod).is_empty() {
        EventResource::UnschedulablePod
    } else {
        EventResource::AssignedPod
    };

    let extractors: [fn(&Pod, &Pod) -> ActionType; 5] = [
        extract_pod_labels_change,
        extract_pod_scale_down,
        extract_pod_scheduling_gate_eliminated_change,
        extract_pod_toleration_change,
        // `features.DynamicResourceAllocation` is GA in 1.35 (always on).
        extract_pod_generated_resource_claim_change,
    ];

    let mut events = Vec::new();
    for f in extractors {
        let event = f(new_pod, old_pod);
        if event != ActionType::NONE {
            events.push(ClusterEvent::new(r, event));
        }
    }

    if events.is_empty() {
        events.push(ClusterEvent::new(r, ActionType::UPDATE));
    }
    events
}

/// `extractPodScaleDown` (`events.go:102-124`): `UpdatePodScaleDown` if any
/// resource request is removed or reduced. `InPlacePodVerticalScaling` is on
/// by default in 1.35, so requests are read with `UseStatusResources`.
fn extract_pod_scale_down(new_pod: &Pod, old_pod: &Pod) -> ActionType {
    let new_requests = rusternetes_common::quota::pod_requests_opts(new_pod, None, true);
    let old_requests = rusternetes_common::quota::pod_requests_opts(old_pod, None, true);

    for (name, old_req) in &old_requests {
        let Some(new_req) = new_requests.get(name) else {
            // The resource request of rName is removed.
            return ActionType::UPDATE_POD_SCALE_DOWN;
        };
        if old_req.milli_value() > new_req.milli_value() {
            // The resource request of rName is scaled down.
            return ActionType::UPDATE_POD_SCALE_DOWN;
        }
    }
    ActionType::NONE
}

/// `extractPodLabelsChange` (`events.go:126-131`).
fn extract_pod_labels_change(new_pod: &Pod, old_pod: &Pod) -> ActionType {
    if is_label_changed(
        new_pod.metadata.labels.as_ref(),
        old_pod.metadata.labels.as_ref(),
    ) {
        return ActionType::UPDATE_POD_LABEL;
    }
    ActionType::NONE
}

/// `extractPodTolerationChange` (`events.go:133-143`): a Pod got a new
/// toleration (API validation only allows adding), so a length check suffices.
fn extract_pod_toleration_change(new_pod: &Pod, old_pod: &Pod) -> ActionType {
    let len = |p: &Pod| {
        p.spec
            .as_ref()
            .and_then(|s| s.tolerations.as_ref())
            .map_or(0, |t| t.len())
    };
    if len(new_pod) != len(old_pod) {
        return ActionType::UPDATE_POD_TOLERATION;
    }
    ActionType::NONE
}

/// `extractPodSchedulingGateEliminatedChange` (`events.go:145-152`).
fn extract_pod_scheduling_gate_eliminated_change(new_pod: &Pod, old_pod: &Pod) -> ActionType {
    let len = |p: &Pod| {
        p.spec
            .as_ref()
            .and_then(|s| s.scheduling_gates.as_ref())
            .map_or(0, |g| g.len())
    };
    if len(new_pod) == 0 && len(old_pod) != 0 {
        // A scheduling gate on the pod is completely removed.
        return ActionType::UPDATE_POD_SCHEDULING_GATES_ELIMINATED;
    }
    ActionType::NONE
}

/// `extractPodGeneratedResourceClaimChange` (`events.go:154-160`) over
/// `resourceclaim.PodStatusEqual` / `PodExtendedStatusEqual`
/// (`staging/src/k8s.io/dynamic-resource-allocation/resourceclaim/pod.go:34-69`):
/// order-sensitive, as upstream ("this cannot be guaranteed" not to be modified).
fn extract_pod_generated_resource_claim_change(new_pod: &Pod, old_pod: &Pod) -> ActionType {
    let status = |p: &Pod| p.status.clone().unwrap_or_default();
    let (new_status, old_status) = (status(new_pod), status(old_pod));

    let a = new_status.resource_claim_statuses.unwrap_or_default();
    let b = old_status.resource_claim_statuses.unwrap_or_default();
    let statuses_equal = a.len() == b.len()
        && a.iter()
            .zip(&b)
            .all(|(x, y)| x.name == y.name && x.resource_claim_name == y.resource_claim_name);

    let extended_equal = match (
        new_status.extended_resource_claim_status.as_ref(),
        old_status.extended_resource_claim_status.as_ref(),
    ) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            x.resource_claim_name == y.resource_claim_name
                && x.request_mappings.len() == y.request_mappings.len()
                && x.request_mappings
                    .iter()
                    .zip(&y.request_mappings)
                    .all(|(m, n)| {
                        m.container_name == n.container_name
                            && m.request_name == n.request_name
                            && m.resource_name == n.resource_name
                    })
        }
        _ => false,
    };

    if !statuses_equal || !extended_equal {
        return ActionType::UPDATE_POD_GENERATED_RESOURCE_CLAIM;
    }
    ActionType::NONE
}

/// `NodeSchedulingPropertiesChange` (`events.go:164-184`): interprets the
/// update of a node and returns the corresponding `UpdateNodeXYZ` event(s).
pub fn node_scheduling_properties_change(new_node: &Node, old_node: &Node) -> Vec<ClusterEvent> {
    let extractors: [fn(&Node, &Node) -> ActionType; 6] = [
        extract_node_spec_unschedulable_change,
        extract_node_allocatable_change,
        extract_node_labels_change,
        extract_node_taints_change,
        extract_node_conditions_change,
        extract_node_annotations_change,
    ];
    let mut events = Vec::new();
    for f in extractors {
        let event = f(new_node, old_node);
        if event != ActionType::NONE {
            events.push(ClusterEvent::new(EventResource::Node, event));
        }
    }
    events
}

/// `extractNodeAllocatableChange` (`events.go:188-193`).
fn extract_node_allocatable_change(new_node: &Node, old_node: &Node) -> ActionType {
    let alloc = |n: &Node| n.status.as_ref().and_then(|s| s.allocatable.clone());
    if alloc(old_node) != alloc(new_node) {
        return ActionType::UPDATE_NODE_ALLOCATABLE;
    }
    ActionType::NONE
}

/// `extractNodeLabelsChange` (`events.go:195-200`).
fn extract_node_labels_change(new_node: &Node, old_node: &Node) -> ActionType {
    if is_label_changed(
        new_node.metadata.labels.as_ref(),
        old_node.metadata.labels.as_ref(),
    ) {
        return ActionType::UPDATE_NODE_LABEL;
    }
    ActionType::NONE
}

/// `isLabelChanged` (`events.go:202-204`). `equality.Semantic.DeepEqual`
/// treats a nil and an empty map as equal.
fn is_label_changed(
    new_labels: Option<&HashMap<String, String>>,
    old_labels: Option<&HashMap<String, String>>,
) -> bool {
    let empty = HashMap::new();
    new_labels.unwrap_or(&empty) != old_labels.unwrap_or(&empty)
}

/// `extractNodeTaintsChange` (`events.go:206-211`).
fn extract_node_taints_change(new_node: &Node, old_node: &Node) -> ActionType {
    // `Taint` has no `PartialEq`; `equality.Semantic.DeepEqual` over its
    // serialized form (nil and empty compare equal).
    let taints = |n: &Node| {
        serde_json::to_value(
            n.spec
                .as_ref()
                .and_then(|s| s.taints.as_ref())
                .map(|t| t.as_slice())
                .unwrap_or(&[]),
        )
        .unwrap_or(serde_json::Value::Null)
    };
    if taints(new_node) != taints(old_node) {
        return ActionType::UPDATE_NODE_TAINT;
    }
    ActionType::NONE
}

/// `extractNodeConditionsChange` (`events.go:213-225`): only the
/// `type -> status` map matters.
fn extract_node_conditions_change(new_node: &Node, old_node: &Node) -> ActionType {
    let strip = |n: &Node| -> HashMap<String, String> {
        n.status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .map(|cs| {
                cs.iter()
                    .map(|c| (c.condition_type.clone(), c.status.clone()))
                    .collect()
            })
            .unwrap_or_default()
    };
    if strip(old_node) != strip(new_node) {
        return ActionType::UPDATE_NODE_CONDITION;
    }
    ActionType::NONE
}

/// `extractNodeSpecUnschedulableChange` (`events.go:227-233`): flipping
/// `spec.unschedulable` to false is reported as `UpdateNodeTaint`.
fn extract_node_spec_unschedulable_change(new_node: &Node, old_node: &Node) -> ActionType {
    let unsched = |n: &Node| {
        n.spec
            .as_ref()
            .and_then(|s| s.unschedulable)
            .unwrap_or(false)
    };
    if unsched(new_node) != unsched(old_node) && !unsched(new_node) {
        // TODO(upstream): create UpdateNodeSpecUnschedulable ActionType
        return ActionType::UPDATE_NODE_TAINT;
    }
    ActionType::NONE
}

/// `extractNodeAnnotationsChange` (`events.go:235-240`).
fn extract_node_annotations_change(new_node: &Node, old_node: &Node) -> ActionType {
    let empty = HashMap::new();
    if old_node.metadata.annotations.as_ref().unwrap_or(&empty)
        != new_node.metadata.annotations.as_ref().unwrap_or(&empty)
    {
        return ActionType::UPDATE_NODE_ANNOTATION;
    }
    ActionType::NONE
}
