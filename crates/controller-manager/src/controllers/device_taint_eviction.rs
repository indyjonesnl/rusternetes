//! Device taint eviction controller.
//!
//! Ported from `pkg/controller/devicetainteviction/device_taint_eviction.go`
//! (kubernetes release-1.35), its `metrics` package, `nopqueue.go` and the
//! `resourceclaim.{Name,IsForPod,IsReservedForPod,ToleratesTaint}` helpers of
//! `staging/src/k8s.io/dynamic-resource-allocation/resourceclaim`.
//!
//! The controller listens to taint changes of DRA devices (taints published
//! by a driver in a `ResourceSlice`, or applied by a `DeviceTaintRule`) and to
//! the tolerations recorded in allocated `ResourceClaim`s, and deletes the
//! pods using a claim whose device carries a `NoExecute` taint that the claim
//! does not tolerate (or no longer tolerates, once `tolerationSeconds` ran
//! out). It also keeps the `EvictionInProgress` condition of every
//! `DeviceTaintRule` up to date.
//!
//! Like upstream, ALL of the logic that identifies pods to evict lives in the
//! `handle_*` event handlers on [`State`]. They never block. Blocking work
//! (deleting a pod, writing a rule status) happens in the workers, driven by
//! a rate-limiting delaying work queue; `State::delete_pod_at` is the single
//! source of truth for whether and when a queued pod really needs to go, so a
//! cancelled eviction is simply recognised as obsolete when its work item
//! comes up (a work queue cannot remove an item).
//!
//! Rust-only differences (expression, not mechanism):
//!
//! - There is no client-go informer framework. [`run_informer`] list+watches a
//!   storage prefix, keeps the "indexer" and turns each change into the
//!   `OnAdd`/`OnUpdate`/`OnDelete` pair upstream's `ResourceEventHandlerFuncs`
//!   would get. The pod and `DeviceTaintRule` indexers double as the
//!   `podLister` / `ruleLister`.
//! - Go guards everything with one `sync.Mutex`, also held across the
//!   `ApplyStatus` call in `maybeUpdateRuleStatus`. Here the guard is a std
//!   mutex that is never held across an `.await`: the rule status is computed
//!   under it and written after releasing it.
//! - `Recorder.Eventf` is a non-blocking local call upstream (the broadcaster
//!   does the I/O in its own goroutines). The same split is kept: handlers push
//!   onto an unbounded channel that a task drains into the `EventRecorder`.
//! - The server-side-apply of the condition goes through `Storage::apply`
//!   (`FieldManager: <controller name>`, `Force: true`) on the status
//!   subresource, with the rule's UID in the apply configuration.
//! - The pod delete carries upstream's `Preconditions{UID}`, but the Rust
//!   `Storage` has no graceful-delete-with-precondition, so the UID is
//!   re-checked on a fresh read immediately before `delete_gracefully`; the
//!   window is the read-to-delete gap instead of an atomic precondition.
//! - Go's `evictPodHook` / `cancelEvictHook` (benchmark-only test seams) are
//!   not ported.

use crate::controllers::device_taint_eviction_metrics::{self as metrics, Metrics};
use crate::controllers::taint_eviction::update_pod_condition;
use crate::controllers::typed_workqueue::{NopQueue, RateLimitingQueue, WorkItemQueue};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use rusternetes_common::resources::devicetaintrule::DeviceTaintRule;
use rusternetes_common::resources::dra::{
    AllocationResult, Device, DeviceRequestAllocationResult, DeviceTaint, DeviceTaintEffect,
    DeviceToleration, ResourceClaim, ResourceSlice, TolerationOperator,
};
use rusternetes_common::resources::pod::PodResourceClaim;
use rusternetes_common::resources::{EventSource, EventType, ObjectReference, Pod, PodCondition};
use rusternetes_common::types::Condition;
use rusternetes_common::Error;
use rusternetes_storage::{build_key, build_prefix, EventRecorder, Storage, WatchEvent};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

/// `names.DeviceTaintEvictionController`
/// (`cmd/kube-controller-manager/names/controller_names.go:72`). Event source
/// component and server-side-apply field manager.
pub const CONTROLLER_NAME: &str = "device-taint-eviction-controller";

/// `DeviceTaintEvictionControllerConfiguration.ConcurrentSyncs` default
/// (`pkg/controller/devicetainteviction/config/v1alpha1/defaults.go`): 10.
pub const DEFAULT_CONCURRENT_SYNCS: usize = 10;

/// `ruleStatusPeriod`: the shortest time between `DeviceTaintRule` status
/// updates while eviction is in progress. Once it is done, the status is not
/// updated again until eviction is in progress again.
pub const RULE_STATUS_PERIOD: Duration = Duration::from_secs(10);

/// `resourcealpha.DeviceTaintConditionEvictionInProgress`.
pub const CONDITION_EVICTION_IN_PROGRESS: &str = "EvictionInProgress";

/// Injectable `time.Now`.
pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// `tainteviction.NamespacedObject`: namespaced name plus UID. Cluster-scoped
/// objects (`DeviceTaintRule`) have an empty namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct NamespacedObject {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

/// `workItem`: either a pod that may need to be deleted (`podRef`) or a
/// `DeviceTaintRule` whose status may have to be updated (`ruleRef`).
///
/// The Go struct carries both refs and tells them apart by `podRef.Name`; the
/// two cases are mutually exclusive, so an enum says the same thing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkItem {
    Pod(NamespacedObject),
    Rule(NamespacedObject),
}

/// `workItemForRule`.
pub fn work_item_for_rule(rule: &DeviceTaintRule) -> WorkItem {
    WorkItem::Rule(NamespacedObject {
        namespace: String::new(),
        name: rule.metadata.name.clone(),
        uid: rule.metadata.uid.clone(),
    })
}

/// `newObject(pod)`.
pub fn pod_object(pod: &Pod) -> NamespacedObject {
    NamespacedObject {
        namespace: pod.metadata.namespace.clone().unwrap_or_default(),
        name: pod.metadata.name.clone(),
        uid: pod.metadata.uid.clone(),
    }
}

/// `(namespace, name)` of a namespaced object, the `types.NamespacedName` key.
pub type ClaimKey = (String, String);

pub fn claim_key(claim: &ResourceClaim) -> ClaimKey {
    (
        claim.metadata.namespace.clone().unwrap_or_default(),
        claim.metadata.name.clone(),
    )
}

// ---------------------------------------------------------------------------
// resourceclaim helpers
// ---------------------------------------------------------------------------

/// `resourceclaim.ToleratesTaint` (`devicetoleration.go:35`): whether the
/// toleration tolerates the taint. Callers must separately check the effect
/// and only react to known effects; unknown effects and `None` are ignored.
pub fn tolerates_taint(toleration: &DeviceToleration, taint: &DeviceTaint) -> bool {
    // `toleration.Effect != "" && toleration.Effect != taint.Effect`.
    if toleration.effect.is_some() && toleration.effect != taint.effect {
        return false;
    }
    if !toleration.key.is_empty() && toleration.key != taint.key {
        return false;
    }
    match &toleration.operator {
        // Empty operator means Equal, set by defaulting.
        None | Some(TolerationOperator::Equal) => {
            toleration.value.as_deref().unwrap_or("") == taint.value.as_deref().unwrap_or("")
        }
        Some(TolerationOperator::Exists) => true,
        Some(TolerationOperator::Unknown(_)) => false,
    }
}

/// Why `resourceclaim.Name` found no usable claim name (a returned error
/// upstream, which the caller treats as "not created yet or unsupported").
#[derive(Debug, PartialEq, Eq)]
pub enum ClaimNameError {
    /// `ErrClaimNotFound`.
    NotCreatedYet,
    /// `ErrAPIUnsupported`.
    Unsupported,
}

/// `resourceclaim.Name`: the name of the ResourceClaim referenced by or
/// created for the PodResourceClaim, whether the owner must be checked with
/// `IsForPod`, or `Ok(None)` when the claim intentionally was not created.
pub fn resource_claim_name(
    pod: &Pod,
    pod_claim: &PodResourceClaim,
) -> std::result::Result<Option<(String, bool)>, ClaimNameError> {
    if let Some(name) = &pod_claim.resource_claim_name {
        return Ok(Some((name.clone(), false)));
    }
    if pod_claim.resource_claim_template_name.is_some() {
        let statuses = pod
            .status
            .as_ref()
            .and_then(|s| s.resource_claim_statuses.as_ref());
        for status in statuses.into_iter().flatten() {
            if status.name == pod_claim.name {
                return Ok(status.resource_claim_name.clone().map(|n| (n, true)));
            }
        }
        return Err(ClaimNameError::NotCreatedYet);
    }
    Err(ClaimNameError::Unsupported)
}

/// `resourceclaim.IsForPod`: the claim was created for (is controlled by) the
/// pod. `metav1.IsControlledBy` compares the controller reference's UID.
fn is_for_pod(pod: &Pod, claim: &ResourceClaim) -> bool {
    if claim.metadata.namespace != pod.metadata.namespace {
        return false;
    }
    claim
        .metadata
        .owner_references
        .iter()
        .flatten()
        .any(|r| r.controller == Some(true) && r.uid == pod.metadata.uid)
}

/// `resourceclaim.IsReservedForPod`.
fn is_reserved_for_pod(pod: &Pod, claim: &ResourceClaim) -> bool {
    claim
        .status
        .as_ref()
        .is_some_and(|s| s.reserved_for.iter().any(|r| r.uid == pod.metadata.uid))
}

fn allocation(claim: &ResourceClaim) -> Option<&AllocationResult> {
    claim.status.as_ref().and_then(|s| s.allocation.as_ref())
}

/// `ruleMatchesDevice`.
fn rule_matches_device(
    rule: &DeviceTaintRule,
    driver_name: &str,
    pool_name: &str,
    device_name: &str,
) -> bool {
    let Some(selector) = &rule.spec.device_selector else {
        return false;
    };
    if selector.driver.as_deref().is_some_and(|d| d != driver_name)
        || selector.pool.as_deref().is_some_and(|p| p != pool_name)
        || selector.device.as_deref().is_some_and(|d| d != device_name)
    {
        return false;
    }
    true
}

fn is_no_execute(effect: &Option<DeviceTaintEffect>) -> bool {
    matches!(effect, Some(DeviceTaintEffect::NoExecute))
}

// ---------------------------------------------------------------------------
// Pools and slices
// ---------------------------------------------------------------------------

/// `poolID`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolId {
    pub driver_name: String,
    pub pool_name: String,
}

/// `pool`: all slices of one driver/pool, indexed by slice name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Pool {
    /// Maps the global name to the current instance under that name.
    pub slices: HashMap<String, Arc<ResourceSlice>>,
    pub max_generation: i64,
}

/// `taintedDevice`.
struct TaintedDevice {
    device_name: String,
    taint: DeviceTaint,
}

impl Pool {
    /// `pool.addSlice`.
    pub fn add_slice(&mut self, slice: &Arc<ResourceSlice>) {
        if self.slices.is_empty() {
            self.max_generation = i64::MIN;
        }
        self.slices
            .insert(slice.metadata.name.clone(), Arc::clone(slice));
        // Adding a slice can only increase the generation.
        if slice.spec.pool.generation > self.max_generation {
            self.max_generation = slice.spec.pool.generation;
        }
    }

    /// `pool.removeSlice`. The slice must have been added before.
    pub fn remove_slice(&mut self, slice: &ResourceSlice) {
        self.slices.remove(&slice.metadata.name);
        // Removing a slice might have decreased the generation to that of some
        // other slice.
        if slice.spec.pool.generation == self.max_generation {
            self.max_generation = self
                .slices
                .values()
                .map(|s| s.spec.pool.generation)
                .max()
                .unwrap_or(i64::MIN);
        }
    }

    /// `pool.getTaintedDevices`: all device taints with `NoExecute` effect,
    /// sorted by device name (stable, so the taint order within a device is
    /// kept).
    fn tainted_devices(&self) -> Vec<TaintedDevice> {
        let mut buffer = Vec::new();
        for slice in self.slices.values() {
            if slice.spec.pool.generation != self.max_generation {
                continue;
            }
            for device in &slice.spec.devices {
                for taint in &device.taints {
                    if !is_no_execute(&taint.effect) {
                        continue;
                    }
                    buffer.push(TaintedDevice {
                        device_name: device.name.clone(),
                        taint: taint.clone(),
                    });
                }
            }
        }
        buffer.sort_by(|a, b| a.device_name.cmp(&b.device_name));
        buffer
    }

    /// `pool.getDevice`: one device by name; out-dated slices are ignored.
    pub fn device(&self, device_name: &str) -> Option<(Arc<ResourceSlice>, usize)> {
        for slice in self.slices.values() {
            if slice.spec.pool.generation != self.max_generation {
                continue;
            }
            if let Some(i) = slice
                .spec
                .devices
                .iter()
                .position(|d| d.name == device_name)
            {
                return Some((Arc::clone(slice), i));
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Tracked taints and eviction times
// ---------------------------------------------------------------------------

/// `sliceDeviceTaint`: one taint entry in a ResourceSlice device.
#[derive(Debug, Clone, PartialEq)]
pub struct SliceDeviceTaint {
    pub slice: Arc<ResourceSlice>,
    pub device_name: String,
    pub taint_index: usize,
}

/// `trackedTaint`: a DeviceTaint augmented with a pointer to its origin
/// (exactly one of rule and slice is set upstream; an enum says so).
#[derive(Debug, Clone, PartialEq)]
pub enum TrackedTaint {
    Rule(Arc<DeviceTaintRule>),
    Slice(SliceDeviceTaint),
}

impl TrackedTaint {
    /// `trackedTaint.deviceTaint`.
    fn device_taint(&self) -> DeviceTaint {
        match self {
            // TODO when GA (upstream): directly point to rule.Spec.Taint.
            TrackedTaint::Rule(rule) => rule.spec.taint.clone(),
            TrackedTaint::Slice(s) => s
                .slice
                .spec
                .devices
                .iter()
                .find(|d| d.name == s.device_name)
                .and_then(|d| d.taints.get(s.taint_index))
                .cloned()
                .unwrap_or_default(),
        }
    }

    /// Go's `==` on a `trackedTaint`: pointer identity of the origin object
    /// plus the slice coordinates. Used by `evictionAndReason.equal` and the
    /// `sets.Set[trackedTaint]` in `claimEvictionTime`; the derived
    /// `PartialEq` is the deep comparison the tests use.
    fn same(&self, other: &TrackedTaint) -> bool {
        match (self, other) {
            (TrackedTaint::Rule(a), TrackedTaint::Rule(b)) => Arc::ptr_eq(a, b),
            (TrackedTaint::Slice(a), TrackedTaint::Slice(b)) => {
                Arc::ptr_eq(&a.slice, &b.slice)
                    && a.device_name == b.device_name
                    && a.taint_index == b.taint_index
            }
            _ => false,
        }
    }

    /// `trackedTaint.Compare`: rules first (by name), then slice taints by
    /// driver/pool/device and taint index.
    fn compare(&self, other: &TrackedTaint) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (TrackedTaint::Rule(a), TrackedTaint::Rule(b)) => a.metadata.name.cmp(&b.metadata.name),
            (TrackedTaint::Rule(_), TrackedTaint::Slice(_)) => Ordering::Less,
            (TrackedTaint::Slice(_), TrackedTaint::Rule(_)) => Ordering::Greater,
            (TrackedTaint::Slice(a), TrackedTaint::Slice(b)) => a
                .slice
                .spec
                .driver
                .cmp(&b.slice.spec.driver)
                .then_with(|| a.slice.spec.pool.name.cmp(&b.slice.spec.pool.name))
                .then_with(|| a.device_name.cmp(&b.device_name))
                .then_with(|| a.taint_index.cmp(&b.taint_index)),
        }
    }
}

impl std::fmt::Display for TrackedTaint {
    /// `trackedTaint.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackedTaint::Rule(rule) => write!(f, "DeviceTaintRule {}", rule.metadata.name),
            TrackedTaint::Slice(s) => write!(
                f,
                "ResourceSlice {} {}/{}/{} taint #{}",
                s.slice.metadata.name,
                s.slice.spec.driver,
                s.slice.spec.pool.name,
                s.device_name,
                s.taint_index
            ),
        }
    }
}

/// `evictionAndReason`: the time when eviction needs to start together with
/// all reasons (taints) why eviction needs to start.
#[derive(Debug, Clone, PartialEq)]
pub struct EvictionAndReason {
    pub when: DateTime<Utc>,
    /// `evictionReason`: sorted by [`TrackedTaint::compare`].
    pub reason: Vec<TrackedTaint>,
}

impl EvictionAndReason {
    /// `evictionAndReason.equal` (nil-aware, pointer-identity of reasons).
    fn equal(a: &Option<EvictionAndReason>, b: &Option<EvictionAndReason>) -> bool {
        match (a, b) {
            (Some(a), Some(b)) => {
                a.when == b.when
                    && a.reason.len() == b.reason.len()
                    && a.reason.iter().zip(&b.reason).all(|(x, y)| x.same(y))
            }
            // Go: `(et == nil) != (other == nil) || et == nil` -> false, so two
            // nil evictions are NOT "equal" either. Kept: callers then treat
            // nil -> nil as a change and re-handle the claim's pods.
            _ => false,
        }
    }
}

/// `allocatedClaim`: a ResourceClaim which has an allocation result. It may
/// or may not be tainted such that pods need to be evicted.
#[derive(Debug, Clone, PartialEq)]
pub struct AllocatedClaim {
    pub claim: Arc<ResourceClaim>,
    /// The time at which pods using this claim need to be evicted: the
    /// smallest value of all per-device values, each `<time of setting the
    /// taint> + <toleration seconds, 0 if not set>`.
    pub eviction: Option<EvictionAndReason>,
}

/// `taintRuleStats`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaintRuleStats {
    /// Number of pods evicted because of this rule since starting the
    /// controller.
    pub num_evicted_pods: i64,
}

// ---------------------------------------------------------------------------
// Listers (informer indexers)
// ---------------------------------------------------------------------------

/// The informer indexer of an object kind, doubling as its lister.
pub struct Store<T> {
    items: Mutex<HashMap<String, Arc<T>>>,
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Self {
            items: Mutex::new(HashMap::new()),
        }
    }
}

impl<T> Store<T> {
    pub fn put(&self, key: String, obj: Arc<T>) {
        self.items.lock().unwrap().insert(key, obj);
    }

    pub fn remove(&self, key: &str) {
        self.items.lock().unwrap().remove(key);
    }

    pub fn get(&self, key: &str) -> Option<Arc<T>> {
        self.items.lock().unwrap().get(key).cloned()
    }

    pub fn list(&self) -> Vec<Arc<T>> {
        self.items.lock().unwrap().values().cloned().collect()
    }
}

pub fn pod_store_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// A `PodEvent` to be recorded: `emitPodDeletionEvent` /
/// `emitCancelPodDeletionEvent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodEvent {
    pub pod: NamespacedObject,
    pub message: &'static str,
}

/// What the event handlers need besides [`State`]: the work queue, the pod
/// lister, the clock and the (optional) event sink. The eviction simulation
/// runs with a no-op queue and no sink, like the `&Controller{workqueue:
/// &NOPQueue, recorder: nil}` upstream builds.
pub struct Env<'a> {
    pub queue: &'a dyn WorkItemQueue<WorkItem>,
    pub pods: &'a Store<Pod>,
    pub events: Option<&'a mpsc::UnboundedSender<PodEvent>>,
    pub clock: &'a Clock,
}

// ---------------------------------------------------------------------------
// State and handlers
// ---------------------------------------------------------------------------

/// The data `Controller.mutex` protects.
#[derive(Debug, Clone, Default)]
pub struct State {
    /// `deletePodAt`: maps a pod to the time when it is meant to be evicted.
    /// The entry gets deleted when eviction is no longer necessary and
    /// updated when the time changes.
    pub delete_pod_at: HashMap<NamespacedObject, EvictionAndReason>,
    /// `maybeDeletePodCount`: how often a worker checked a pod.
    pub maybe_delete_pod_count: i64,
    /// `allocatedClaims`: all currently known allocated claims.
    pub allocated_claims: HashMap<ClaimKey, AllocatedClaim>,
    /// `pools`: all slices by driver and pool name.
    pub pools: HashMap<PoolId, Pool>,
    /// `evictingRules`: all DeviceTaintRules by name which cause pod eviction.
    pub evicting_rules: HashMap<String, Arc<DeviceTaintRule>>,
    /// `taintRuleStats`, keyed by rule UID. Potentially a different set than
    /// `evicting_rules`: a rule that no longer evicts might have evicted in
    /// the past.
    pub taint_rule_stats: HashMap<String, TaintRuleStats>,
}

impl State {
    /// `evictPod`: ensure the pod gets evicted at the specified time. Does not
    /// block.
    fn evict_pod(&mut self, env: &Env<'_>, pod_ref: NamespacedObject, eviction: EvictionAndReason) {
        let now = (env.clock)();
        let delay = eviction.when - now;
        debug!(
            pod = %format!("{}/{}", pod_ref.namespace, pod_ref.name),
            %delay,
            "Adding pod eviction"
        );
        self.delete_pod_at.insert(pod_ref.clone(), eviction);
        match delay.to_std() {
            Ok(d) if !d.is_zero() => env.queue.add_after(WorkItem::Pod(pod_ref), d),
            _ => env.queue.add(WorkItem::Pod(pod_ref)),
        }
    }

    /// `cancelEvict`: idempotent, false if there was nothing to cancel. A work
    /// queue cannot drop an item; the worker detects the entry as obsolete by
    /// checking `delete_pod_at`.
    fn cancel_evict(&mut self, pod_ref: &NamespacedObject) -> bool {
        self.delete_pod_at.remove(pod_ref).is_some()
    }

    /// `cancelWorkWithEvent`.
    fn cancel_work_with_event(&mut self, env: &Env<'_>, pod_ref: &NamespacedObject) -> bool {
        if self.cancel_evict(pod_ref) {
            emit_event(env, pod_ref, "Cancelling deletion");
            return true;
        }
        false
    }

    /// `handleClaimChange`.
    pub fn handle_claim_change(
        &mut self,
        env: &Env<'_>,
        old_claim: Option<&Arc<ResourceClaim>>,
        new_claim: Option<&Arc<ResourceClaim>>,
    ) {
        let Some(claim) = new_claim.or(old_claim) else {
            return;
        };
        let name = claim_key(claim);

        // Deleted?
        let Some(new_claim) = new_claim else {
            self.allocated_claims.remove(&name);
            self.handle_pods(env, claim);
            return;
        };

        // Added?
        let Some(old_claim) = old_claim else {
            if allocation(claim).is_none() {
                return;
            }
            let eviction = self.claim_eviction_time(claim);
            self.allocated_claims.insert(
                name,
                AllocatedClaim {
                    claim: Arc::clone(claim),
                    eviction,
                },
            );
            self.handle_pods(env, claim);
            return;
        };

        // If we have two claims, the UID might still be different. Unlikely,
        // but not impossible... Treat this like a remove + add.
        if old_claim.metadata.uid != new_claim.metadata.uid {
            self.handle_claim_change(env, Some(old_claim), None);
            self.handle_claim_change(env, None, Some(new_claim));
            return;
        }

        let reserved = |c: &ResourceClaim| {
            c.status
                .as_ref()
                .map(|s| s.reserved_for.clone())
                .unwrap_or_default()
        };
        // ReservedFor may have changed. If it did, sync both old and new
        // lists, otherwise only once (same list).
        let sync_both_claims = |this: &mut State| {
            if reserved(old_claim) != reserved(new_claim) {
                this.handle_pods(env, old_claim);
                this.handle_pods(env, new_claim);
            } else {
                this.handle_pods(env, claim);
            }
        };

        let (old_alloc, new_alloc) = (allocation(old_claim), allocation(new_claim));

        // Allocation added?
        if old_alloc.is_none() && new_alloc.is_some() {
            let eviction = self.claim_eviction_time(claim);
            self.allocated_claims.insert(
                name,
                AllocatedClaim {
                    claim: Arc::clone(claim),
                    eviction,
                },
            );
            sync_both_claims(self);
            return;
        }

        // Allocation removed?
        if old_alloc.is_some() && new_alloc.is_none() {
            self.allocated_claims.remove(&name);
            sync_both_claims(self);
            return;
        }

        // Allocated before and after?
        if allocation(claim).is_some() {
            // The Allocation is immutable, so we don't need to recompute the
            // eviction time. Storing the newer claim is enough.
            let eviction = self
                .allocated_claims
                .get(&name)
                .and_then(|a| a.eviction.clone());
            self.allocated_claims.insert(
                name,
                AllocatedClaim {
                    claim: Arc::clone(claim),
                    eviction,
                },
            );
            sync_both_claims(self);
        }

        // If we get here, nothing changed.
    }

    /// `claimEvictionTime`: the earliest `TimeAdded` of any `NoExecute` taint
    /// in any allocated device unless that taint is tolerated, `None` if none.
    /// May only be called for allocated claims.
    pub fn claim_eviction_time(&self, claim: &Arc<ResourceClaim>) -> Option<EvictionAndReason> {
        let mut when: Option<DateTime<Utc>> = None;
        let mut taints: Vec<TrackedTaint> = Vec::new();

        let allocation = allocation(claim)?;
        for allocated_device in &allocation.devices.results {
            let id = PoolId {
                driver_name: allocated_device.driver.clone(),
                pool_name: allocated_device.pool.clone(),
            };
            let slice_device = self
                .pools
                .get(&id)
                .and_then(|p| p.device(&allocated_device.device));

            'next_taint: for taint in self.all_evicting_device_taints(
                allocated_device,
                slice_device.as_ref().map(|(s, i)| (s, *i)),
            ) {
                let device_taint = taint.device_taint();
                let mut new_eviction_time = device_taint.time_added;
                let mut have_toleration = false;
                let mut toleration_seconds = i64::MAX;
                for toleration in &allocated_device.tolerations {
                    if is_no_execute(&toleration.effect)
                        && tolerates_taint(toleration, &device_taint)
                    {
                        let Some(seconds) = toleration.toleration_seconds else {
                            // Tolerate forever -> ignore taint.
                            continue 'next_taint;
                        };
                        // Negative values count as zero.
                        let seconds = seconds.max(0);
                        if seconds < toleration_seconds {
                            toleration_seconds = seconds;
                        }
                        have_toleration = true;
                    }
                }
                if have_toleration {
                    // Go would dereference a nil TimeAdded here; the API server
                    // always sets it, so a missing one just stays missing.
                    new_eviction_time = new_eviction_time.map(|t| {
                        ChronoDuration::try_seconds(toleration_seconds)
                            .and_then(|d| t.checked_add_signed(d))
                            .unwrap_or(DateTime::<Utc>::MAX_UTC)
                    });
                }
                if !taints.iter().any(|t| t.same(&taint)) {
                    taints.push(taint.clone());
                }
                let Some(current) = when else {
                    when = new_eviction_time;
                    debug!(
                        claim = %claim.metadata.name,
                        taint = %taint,
                        eviction_time = ?when,
                        "Claim is affected by device taint"
                    );
                    continue;
                };
                if let Some(new_time) = new_eviction_time {
                    if new_time < current {
                        when = Some(new_time);
                        debug!(
                            claim = %claim.metadata.name,
                            taint = %taint,
                            eviction_time = ?when,
                            "Claim is affected by device taint"
                        );
                    }
                }
            }
        }

        let when = when?;
        taints.sort_by(|a, b| a.compare(b));
        Some(EvictionAndReason {
            when,
            reason: taints,
        })
    }

    /// `allEvictingDeviceTaints`: all `NoExecute` taints which affect the
    /// allocated device. A taint may come from either the ResourceSlice
    /// informer or from a DeviceTaintRule, but not both.
    fn all_evicting_device_taints(
        &self,
        allocated_device: &DeviceRequestAllocationResult,
        slice_device: Option<(&Arc<ResourceSlice>, usize)>,
    ) -> Vec<TrackedTaint> {
        let mut out = Vec::new();
        if let Some((slice, index)) = slice_device {
            let device: &Device = &slice.spec.devices[index];
            for (i, taint) in device.taints.iter().enumerate() {
                if !is_no_execute(&taint.effect) {
                    continue;
                }
                out.push(TrackedTaint::Slice(SliceDeviceTaint {
                    slice: Arc::clone(slice),
                    device_name: device.name.clone(),
                    taint_index: i,
                }));
            }
        }
        for rule in self.evicting_rules.values() {
            if !rule_matches_device(
                rule,
                &allocated_device.driver,
                &allocated_device.pool,
                &allocated_device.device,
            ) {
                continue;
            }
            out.push(TrackedTaint::Rule(Arc::clone(rule)));
        }
        out
    }

    /// `handleRuleChange`.
    pub fn handle_rule_change(
        &mut self,
        env: &Env<'_>,
        old_rule: Option<&Arc<DeviceTaintRule>>,
        new_rule: Option<&Arc<DeviceTaintRule>>,
    ) {
        if old_rule.is_none() {
            if let Some(new_rule) = new_rule {
                // Update the status at least once, immediately and before
                // evicting any pods.
                debug!(rule = %new_rule.metadata.name, "Adding immediate status update because of new rule");
                env.queue.add(work_item_for_rule(new_rule));
            }
        }

        if new_rule.is_none() {
            if let Some(old_rule) = old_rule {
                // Clean up to avoid a memory leak. Removal from the work queue
                // is handled when a worker handles the work item.
                self.taint_rule_stats.remove(&old_rule.metadata.uid);
                self.evicting_rules.remove(&old_rule.metadata.name);
            }
        }

        if let (Some(old_rule), Some(new_rule)) = (old_rule, new_rule) {
            if old_rule.metadata.uid == new_rule.metadata.uid {
                if old_rule.spec == new_rule.spec {
                    return;
                }
                // Update the status at least once, immediately and before
                // evicting any pods.
                debug!(rule = %new_rule.metadata.name, "Adding immediate status update because of modified rule spec");
                env.queue.add(work_item_for_rule(new_rule));
            }
        }

        // Rule spec changes should be rare. Simply do a brute-force
        // re-evaluation of all allocated claims. Same with trying to avoid
        // delete+add in evictingRules, the logic just becomes unnecessarily
        // complex.
        if let Some(old_rule) = old_rule {
            if is_no_execute(&old_rule.spec.taint.effect) {
                self.evicting_rules.remove(&old_rule.metadata.name);
            }
        }
        if let Some(new_rule) = new_rule {
            if is_no_execute(&new_rule.spec.taint.effect) {
                self.evicting_rules
                    .insert(new_rule.metadata.name.clone(), Arc::clone(new_rule));
            }
        }
        let names: Vec<ClaimKey> = self.allocated_claims.keys().cloned().collect();
        for name in names {
            let old_allocated = self.allocated_claims[&name].clone();
            let new_allocated = AllocatedClaim {
                claim: Arc::clone(&old_allocated.claim),
                eviction: self.claim_eviction_time(&old_allocated.claim),
            };
            self.allocated_claims.insert(name, new_allocated.clone());
            if !EvictionAndReason::equal(&new_allocated.eviction, &old_allocated.eviction) {
                self.handle_pods(env, &new_allocated.claim);
            }
        }
    }

    /// `handleSliceChange`.
    pub fn handle_slice_change(
        &mut self,
        env: &Env<'_>,
        old_slice: Option<&Arc<ResourceSlice>>,
        new_slice: Option<&Arc<ResourceSlice>>,
    ) {
        let Some(slice) = new_slice.or(old_slice) else {
            return;
        };
        let pool_id = PoolId {
            driver_name: slice.spec.driver.clone(),
            pool_name: slice.spec.pool.name.clone(),
        };

        // Determine old and new device taints. Only devices where something
        // changes trigger additional checks for claims using them.
        let pool = self.pools.entry(pool_id.clone()).or_default();
        let old_device_taints = pool.tainted_devices();
        if let Some(old_slice) = old_slice {
            pool.remove_slice(old_slice);
        }
        if let Some(new_slice) = new_slice {
            pool.add_slice(new_slice);
        }
        let new_device_taints = pool.tainted_devices();
        if pool.slices.is_empty() {
            self.pools.remove(&pool_id);
        }

        // Now determine differences. This depends on both lists having been
        // sorted by device name.
        if old_device_taints.is_empty() && new_device_taints.is_empty() {
            // Both empty, no changes.
            return;
        }
        let mut modified_devices: HashSet<String> = HashSet::new();
        let (mut o, mut n) = (0usize, 0usize);
        while o < old_device_taints.len() || n < new_device_taints.len() {
            // Iterate over devices in both slices with the same name.
            while o < old_device_taints.len()
                && n < new_device_taints.len()
                && old_device_taints[o].device_name == new_device_taints[n].device_name
            {
                if old_device_taints[o].taint != new_device_taints[n].taint {
                    modified_devices.insert(old_device_taints[o].device_name.clone());
                }
                o += 1;
                n += 1;
            }

            // Step over old devices which were removed.
            let new_device_name = new_device_taints
                .get(n)
                .map(|t| t.device_name.as_str())
                .unwrap_or("");
            while o < old_device_taints.len() && old_device_taints[o].device_name != new_device_name
            {
                modified_devices.insert(old_device_taints[o].device_name.clone());
                o += 1;
            }

            // Step over new devices which were added.
            let old_device_name = old_device_taints
                .get(o)
                .map(|t| t.device_name.as_str())
                .unwrap_or("");
            if n < new_device_taints.len() && new_device_taints[n].device_name != old_device_name {
                modified_devices.insert(new_device_taints[n].device_name.clone());
                n += 1;
            }
        }

        // Now find all claims using at least one modified device, update their
        // eviction time, and handle their consuming pods.
        let names: Vec<ClaimKey> = self.allocated_claims.keys().cloned().collect();
        for name in names {
            let mut claim = self.allocated_claims[&name].clone();
            let Some(alloc) = allocation(&claim.claim) else {
                continue;
            };
            if !uses_device(alloc, &pool_id, &modified_devices) {
                continue;
            }
            let new_eviction = self.claim_eviction_time(&claim.claim);
            if EvictionAndReason::equal(&new_eviction, &claim.eviction) {
                // No change.
                continue;
            }
            claim.eviction = new_eviction;
            self.allocated_claims.insert(name, claim.clone());
            // We could collect pods which depend on claims with changes. In
            // practice, most pods probably depend on one claim, so it is
            // probably more efficient to avoid building such a map to keep the
            // common case simple.
            self.handle_pods(env, &claim.claim);
        }
    }

    /// `handlePodChange`.
    pub fn handle_pod_change(
        &mut self,
        env: &Env<'_>,
        old_pod: Option<&Arc<Pod>>,
        new_pod: Option<&Arc<Pod>>,
    ) {
        let Some(new_pod) = new_pod else {
            // Nothing left to do for it. No need to emit an event here, it's
            // gone.
            if let Some(old_pod) = old_pod {
                self.cancel_evict(&pod_object(old_pod));
            }
            return;
        };

        // Pods get updated quite frequently. There's no need to check them
        // again unless something changed regarding their claims or they got
        // scheduled. In particular this prevents adding the pod again directly
        // after the eviction condition got added to it.
        if let Some(old_pod) = old_pod {
            let node = |p: &Pod| p.spec.as_ref().and_then(|s| s.node_name.clone());
            let statuses = |p: &Pod| {
                p.status
                    .as_ref()
                    .and_then(|s| s.resource_claim_statuses.clone())
                    .unwrap_or_default()
            };
            if node(old_pod) == node(new_pod) && statuses(old_pod) == statuses(new_pod) {
                return;
            }
        }

        self.handle_pod(env, new_pod);
    }

    /// `handlePods`: every pod in the claim's `ReservedFor`.
    ///
    /// Upstream `return`s (not `continue`s) on a missing or replaced pod; that
    /// is kept.
    pub fn handle_pods(&mut self, env: &Env<'_>, claim: &ResourceClaim) {
        let namespace = claim.metadata.namespace.as_deref().unwrap_or("");
        let Some(status) = &claim.status else {
            return;
        };
        for consumer in &status.reserved_for {
            let api_group_empty = consumer.api_group.as_deref().unwrap_or("").is_empty();
            if api_group_empty && consumer.resource == "pods" {
                let Some(pod) = env.pods.get(&format!("{namespace}/{}", consumer.name)) else {
                    return;
                };
                if pod.metadata.uid != consumer.uid {
                    // Not the pod we were looking for.
                    return;
                }
                self.handle_pod(env, &pod);
            }
        }
    }

    /// `handlePod`.
    fn handle_pod(&mut self, env: &Env<'_>, pod: &Arc<Pod>) {
        let eviction = self.pod_eviction_time(pod);
        let pod_ref = pod_object(pod);
        let Some(eviction) = eviction else {
            if self.cancel_work_with_event(env, &pod_ref) {
                debug!(pod = %pod_ref.name, "Canceled pod eviction");
            }
            return;
        };

        let reasons = eviction.reason.clone();
        self.evict_pod(env, pod_ref, eviction);

        // If any reason is because of a taint, then eviction is in progress
        // and the status may need to be updated. But don't do it immediately
        // because more pod changes may be coming in.
        for reason in &reasons {
            if let TrackedTaint::Rule(rule) = reason {
                debug!(rule = %rule.metadata.name, "Adding delayed status update because of pod change");
                env.queue
                    .add_after(work_item_for_rule(rule), RULE_STATUS_PERIOD);
            }
        }
    }

    /// `podEvictionTime`.
    pub fn pod_eviction_time(&self, pod: &Pod) -> Option<EvictionAndReason> {
        // Not scheduled yet? No need to evict.
        let spec = pod.spec.as_ref()?;
        if spec.node_name.as_deref().unwrap_or("").is_empty() {
            return None;
        }

        // If any claim in use by the pod is tainted such that the taint is not
        // tolerated, the pod needs to be evicted.
        let mut eviction: Option<EvictionAndReason> = None;
        for pod_claim in spec.resource_claims.iter().flatten() {
            let (claim_name, must_check_owner) = match resource_claim_name(pod, pod_claim) {
                // Not created yet or unsupported. Definitely not tainted.
                Err(_) => continue,
                // Claim not needed.
                Ok(None) => continue,
                Ok(Some(found)) => found,
            };
            let key = (
                pod.metadata.namespace.clone().unwrap_or_default(),
                claim_name,
            );
            // Referenced, but not found or not allocated. Also not tainted.
            let Some(allocated_claim) = self.allocated_claims.get(&key) else {
                continue;
            };
            if must_check_owner && !is_for_pod(pod, &allocated_claim.claim) {
                // Claim and pod don't match. Ignore the claim.
                continue;
            }
            if !is_reserved_for_pod(pod, &allocated_claim.claim) {
                // The pod isn't the one which is allowed and/or supposed to
                // use the claim. Perhaps that pod instance already got deleted
                // and we are looking at its replacement under the same name.
                // Either way, ignore.
                continue;
            }
            let Some(claim_eviction) = &allocated_claim.eviction else {
                continue;
            };
            match &eviction {
                None => {
                    // Use the new eviction time as-is.
                    eviction = Some(claim_eviction.clone());
                }
                Some(existing) => {
                    // Join reasons and figure out the new time. Might not
                    // actually lead to any change.
                    let mut joined = EvictionAndReason {
                        when: claim_eviction.when,
                        reason: claim_eviction.reason.clone(),
                    };
                    // Multiple reasons affecting the same pod should be so
                    // rare, a simple insertion sort is fine.
                    for reason in &existing.reason {
                        match joined
                            .reason
                            .binary_search_by(|probe| probe.compare(reason))
                        {
                            Ok(_) => {}
                            Err(index) => joined.reason.insert(index, reason.clone()),
                        }
                    }
                    if existing.when < joined.when {
                        joined.when = existing.when;
                    }
                    if !EvictionAndReason::equal(&Some(existing.clone()), &Some(joined.clone())) {
                        eviction = Some(joined);
                    }
                }
            }
        }

        eviction
    }

    /// `countPendingPods`: pods (not yet terminating) that this rule is a
    /// reason to evict, and the number of namespaces they are in.
    fn count_pending_pods(&self, env: &Env<'_>, rule: &DeviceTaintRule) -> (i64, i64) {
        let mut namespaces: HashSet<String> = HashSet::new();
        let mut num_pending_pods = 0i64;
        for pod in env.pods.list() {
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let Some(eviction) = self.pod_eviction_time(&pod) else {
                continue;
            };
            for reason in &eviction.reason {
                if let TrackedTaint::Rule(r) = reason {
                    if r.metadata.uid == rule.metadata.uid {
                        num_pending_pods += 1;
                        namespaces.insert(pod.metadata.namespace.clone().unwrap_or_default());
                    }
                }
            }
        }
        (num_pending_pods, namespaces.len() as i64)
    }

    /// `countTaintedDevices`: the number of devices in slices matching the
    /// rule and the number of allocated devices matching the rule.
    fn count_tainted_devices(&self, rule: &DeviceTaintRule) -> (i64, i64) {
        let (mut slice_devices, mut allocated_devices) = (0i64, 0i64);
        for (pool_id, pool) in &self.pools {
            for slice in pool.slices.values() {
                if slice.spec.pool.generation != pool.max_generation {
                    continue;
                }
                for device in &slice.spec.devices {
                    if rule_matches_device(
                        rule,
                        &pool_id.driver_name,
                        &pool_id.pool_name,
                        &device.name,
                    ) {
                        slice_devices += 1;
                    }
                }
            }
        }
        for claim in self.allocated_claims.values() {
            let Some(alloc) = allocation(&claim.claim) else {
                continue;
            };
            for device in &alloc.devices.results {
                if rule_matches_device(rule, &device.driver, &device.pool, &device.device) {
                    allocated_devices += 1;
                }
            }
        }
        (slice_devices, allocated_devices)
    }

    /// The temporary controller `maybeUpdateRuleStatus` builds to simulate the
    /// `None` effect as `NoExecute`: a copy of the claims, shared (read-only)
    /// pools, and an empty `deletePodAt` / `evictingRules`.
    fn simulation(&self) -> State {
        State {
            delete_pod_at: HashMap::new(),
            maybe_delete_pod_count: 0,
            allocated_claims: self.allocated_claims.clone(),
            pools: self.pools.clone(),
            evicting_rules: HashMap::new(),
            taint_rule_stats: HashMap::new(),
        }
    }
}

/// `usesDevice`.
fn uses_device(
    allocation: &AllocationResult,
    pool: &PoolId,
    modified_devices: &HashSet<String>,
) -> bool {
    allocation.devices.results.iter().any(|device| {
        device.driver == pool.driver_name
            && device.pool == pool.pool_name
            && modified_devices.contains(&device.device)
    })
}

/// `emitPodDeletionEvent` / `emitCancelPodDeletionEvent`: hand the event to
/// the sink (a no-op when there is no recorder, like the simulation).
fn emit_event(env: &Env<'_>, pod_ref: &NamespacedObject, message: &'static str) {
    if let Some(events) = env.events {
        let _ = events.send(PodEvent {
            pod: pod_ref.clone(),
            message,
        });
    }
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

/// `Controller`.
pub struct DeviceTaintEvictionController<S: Storage> {
    name: String,
    storage: Arc<S>,
    queue: Arc<dyn WorkItemQueue<WorkItem>>,
    clock: Clock,
    metrics: Arc<Metrics>,
    pods: Arc<Store<Pod>>,
    /// The `ruleLister`; `None` when `DRADeviceTaintRules` is disabled: then
    /// nothing is done with (eviction) or for (status) any DeviceTaintRule.
    rules: Option<Arc<Store<DeviceTaintRule>>>,
    state: Mutex<State>,
    events_tx: mpsc::UnboundedSender<PodEvent>,
    events_rx: Mutex<Option<mpsc::UnboundedReceiver<PodEvent>>>,
    has_synced: AtomicBool,
}

/// An `Arc<S>` whose work items fail the way `apierrors.IsNotFound` detects.
fn is_not_found(err: &Error) -> bool {
    matches!(err, Error::NotFound(_))
}

impl<S: Storage + 'static> DeviceTaintEvictionController<S> {
    /// `New`: creates the controller; spawns nothing (that happens in
    /// [`run`](Self::run)). The `DeviceTaintRule` handling is only enabled
    /// when the `DRADeviceTaintRules` feature gate is.
    pub fn new(storage: Arc<S>) -> Self {
        let rules_enabled = rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::DRADeviceTaintRules,
        );
        Self::with_parts(
            storage,
            CONTROLLER_NAME,
            Arc::new(RateLimitingQueue::<WorkItem>::new()),
            Arc::new(Utc::now),
            metrics::global(),
            rules_enabled,
        )
    }

    /// `New` with every collaborator injected (the unit-test seam).
    pub fn with_parts(
        storage: Arc<S>,
        name: &str,
        queue: Arc<dyn WorkItemQueue<WorkItem>>,
        clock: Clock,
        metrics: Arc<Metrics>,
        rules_enabled: bool,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        Self {
            name: name.to_string(),
            storage,
            queue,
            clock,
            metrics,
            pods: Arc::new(Store::default()),
            rules: rules_enabled.then(|| Arc::new(Store::default())),
            state: Mutex::new(State::default()),
            events_tx,
            events_rx: Mutex::new(Some(events_rx)),
            has_synced: AtomicBool::new(false),
        }
    }

    /// The controller's pod lister / indexer.
    pub fn pod_store(&self) -> &Arc<Store<Pod>> {
        &self.pods
    }

    /// The controller's `DeviceTaintRule` lister / indexer.
    pub fn rule_store(&self) -> Option<&Arc<Store<DeviceTaintRule>>> {
        self.rules.as_ref()
    }

    /// Whether the informers have synced (`hasSynced`).
    pub fn has_synced(&self) -> bool {
        self.has_synced.load(Ordering::SeqCst)
    }

    /// The protected state, for tests.
    pub fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Take the receiving end of the event channel (tests drain it instead of
    /// running the recorder task).
    pub fn take_event_receiver(&self) -> Option<mpsc::UnboundedReceiver<PodEvent>> {
        self.events_rx.lock().unwrap().take()
    }

    fn with_env<R>(&self, f: impl FnOnce(&mut State, &Env<'_>) -> R) -> R {
        let env = Env {
            queue: self.queue.as_ref(),
            pods: &self.pods,
            events: Some(&self.events_tx),
            clock: &self.clock,
        };
        let mut state = self.state.lock().unwrap();
        f(&mut state, &env)
    }

    /// Event-handler entry points, called with the object pair an informer
    /// would hand to `ResourceEventHandlerFuncs`.
    pub fn on_claim_change(
        &self,
        old: Option<&Arc<ResourceClaim>>,
        new: Option<&Arc<ResourceClaim>>,
    ) {
        self.with_env(|s, env| s.handle_claim_change(env, old, new));
    }

    pub fn on_slice_change(
        &self,
        old: Option<&Arc<ResourceSlice>>,
        new: Option<&Arc<ResourceSlice>>,
    ) {
        self.with_env(|s, env| s.handle_slice_change(env, old, new));
    }

    pub fn on_rule_change(
        &self,
        old: Option<&Arc<DeviceTaintRule>>,
        new: Option<&Arc<DeviceTaintRule>>,
    ) {
        self.with_env(|s, env| s.handle_rule_change(env, old, new));
    }

    pub fn on_pod_change(&self, old: Option<&Arc<Pod>>, new: Option<&Arc<Pod>>) {
        self.with_env(|s, env| s.handle_pod_change(env, old, new));
    }

    /// `Run`: start the informers, wait for them to sync, then process work
    /// with `num_workers` workers until the future is dropped. Dropping the
    /// future aborts every task it spawned (the `JoinSet`), the equivalent of
    /// cancelling the `ctx` upstream.
    pub async fn run(self: Arc<Self>, num_workers: usize) -> Result<()> {
        info!(controller = %self.name, num_workers, "Starting");
        let mut tasks: JoinSet<()> = JoinSet::new();

        // Delayed construction of the recorder, like the broadcaster upstream:
        // handlers only push onto the channel, this task does the API calls.
        let events_rx = self.events_rx.lock().unwrap().take();
        if let Some(mut rx) = events_rx {
            let recorder = EventRecorder::new(Arc::clone(&self.storage));
            let component = self.name.clone();
            tasks.spawn(async move {
                while let Some(event) = rx.recv().await {
                    record_pod_event(&recorder, &component, &event).await;
                }
            });
        }

        let mut synced_flags: Vec<Arc<AtomicBool>> = Vec::new();

        // Claims.
        let flag = Arc::new(AtomicBool::new(false));
        synced_flags.push(flag.clone());
        let this = Arc::clone(&self);
        tasks.spawn(run_informer::<S, ResourceClaim, _>(
            Arc::clone(&self.storage),
            build_prefix("resourceclaims", None),
            |c| {
                format!(
                    "{}/{}",
                    c.metadata.namespace.as_deref().unwrap_or(""),
                    c.metadata.name
                )
            },
            flag,
            move |old, new| this.on_claim_change(old, new),
        ));

        // Pods. The indexer is the pod lister, so it is updated before the
        // handler sees the change, like an informer's store.
        let flag = Arc::new(AtomicBool::new(false));
        synced_flags.push(flag.clone());
        let this = Arc::clone(&self);
        tasks.spawn(run_informer::<S, Pod, _>(
            Arc::clone(&self.storage),
            build_prefix("pods", None),
            pod_store_key,
            flag,
            move |old, new| {
                match new {
                    Some(pod) => this.pods.put(pod_store_key(pod), Arc::clone(pod)),
                    None => {
                        if let Some(old) = old {
                            this.pods.remove(&pod_store_key(old));
                        }
                    }
                }
                this.on_pod_change(old, new);
            },
        ));

        // DeviceTaintRules, only with the feature enabled.
        if let Some(rules) = self.rules.clone() {
            let flag = Arc::new(AtomicBool::new(false));
            synced_flags.push(flag.clone());
            let this = Arc::clone(&self);
            tasks.spawn(run_informer::<S, DeviceTaintRule, _>(
                Arc::clone(&self.storage),
                build_prefix("devicetaintrules", None),
                |r| r.metadata.name.clone(),
                flag,
                move |old, new| {
                    match new {
                        Some(rule) => rules.put(rule.metadata.name.clone(), Arc::clone(rule)),
                        None => {
                            if let Some(old) = old {
                                rules.remove(&old.metadata.name);
                            }
                        }
                    }
                    this.on_rule_change(old, new);
                },
            ));
        }

        // Slices.
        let flag = Arc::new(AtomicBool::new(false));
        synced_flags.push(flag.clone());
        let this = Arc::clone(&self);
        tasks.spawn(run_informer::<S, ResourceSlice, _>(
            Arc::clone(&self.storage),
            build_prefix("resourceslices", None),
            |s| s.metadata.name.clone(),
            flag,
            move |old, new| this.on_slice_change(old, new),
        ));

        // `WaitForNamedCacheSyncWithContext`.
        while !synced_flags.iter().all(|f| f.load(Ordering::SeqCst)) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        debug!("Underlying informers have synced");
        self.has_synced.store(true, Ordering::SeqCst);

        for _ in 0..num_workers.max(1) {
            let this = Arc::clone(&self);
            tasks.spawn(async move { this.worker().await });
        }

        // Run until dropped; a finished task means the controller is broken.
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("device taint eviction task failed: {e}");
            }
        }
        self.queue.shut_down();
        Ok(())
    }

    /// `worker`: blocks until the work queue is shut down.
    pub async fn worker(&self) {
        while let Some(item) = self.queue.get().await {
            match self.handle_work(&item).await {
                Err(e) => {
                    debug!(error = %e, "Processing work item failed, will retry");
                    self.queue.add_rate_limited(item.clone());
                }
                Ok(again_after) if !again_after.is_zero() => {
                    self.queue.add_after(item.clone(), again_after);
                }
                Ok(_) => self.queue.forget(&item),
            }
            self.queue.done(&item);
        }
    }

    /// `handleWork`.
    pub async fn handle_work(&self, item: &WorkItem) -> Result<Duration> {
        match item {
            WorkItem::Pod(pod_ref) => self.maybe_delete_pod(pod_ref).await,
            WorkItem::Rule(rule_ref) => self.maybe_update_rule_status(rule_ref).await,
        }
    }

    /// `maybeDeletePod`: check whether the pod needs to be deleted now and if
    /// so, do it. An error means the operation must be repeated, a positive
    /// delay means repeat in the future, zero means done or obsolete.
    pub async fn maybe_delete_pod(&self, pod_ref: &NamespacedObject) -> Result<Duration> {
        // The mutex must not be held while doing blocking API calls.
        let eviction = {
            let mut state = self.state.lock().unwrap();
            state.maybe_delete_pod_count += 1;
            state.delete_pod_at.get(pod_ref).cloned()
        };
        let Some(eviction) = eviction else {
            debug!(pod = %pod_ref.name, "Work item for pod deletion obsolete, nothing to do");
            return Ok(Duration::ZERO);
        };

        let now = (self.clock)();
        if let Ok(again_after) = (eviction.when - now).to_std() {
            if !again_after.is_zero() {
                // Not yet. Maybe the fireAt time got updated.
                return Ok(again_after);
            }
        }

        let result = self.add_condition_and_delete_pod(pod_ref).await;
        match result {
            Err(e) if is_not_found(&e) => {
                // Not a problem, the work is done. But we didn't do it, so
                // don't bump the metric. Forget the deletion time.
                self.state.lock().unwrap().delete_pod_at.remove(pod_ref);
                return Ok(Duration::ZERO);
            }
            Err(e) => return Err(anyhow!(e)),
            Ok(()) => {}
        }

        let latency = ((self.clock)() - eviction.when)
            .to_std()
            .unwrap_or_default()
            .as_secs_f64();
        info!(pod = %pod_ref.name, latency, "Evicted pod by deleting it");
        self.metrics.pod_deletions_total.inc();
        self.metrics.pod_deletions_latency.observe(latency);

        let mut state = self.state.lock().unwrap();
        // Forget the deletion time, we are done.
        state.delete_pod_at.remove(pod_ref);
        for reason in &eviction.reason {
            if let TrackedTaint::Rule(rule) = reason {
                state
                    .taint_rule_stats
                    .entry(rule.metadata.uid.clone())
                    .or_default()
                    .num_evicted_pods += 1;

                // Ensure that the status gets updated eventually. Doing this
                // immediately is not useful because it would just race with
                // the informer's update (rule status reads from cache!).
                self.queue
                    .add_after(work_item_for_rule(rule), RULE_STATUS_PERIOD);
            }
        }
        Ok(Duration::ZERO)
    }

    /// `addConditionAndDeletePod`.
    async fn add_condition_and_delete_pod(
        &self,
        pod_ref: &NamespacedObject,
    ) -> std::result::Result<(), Error> {
        let key = build_key("pods", Some(&pod_ref.namespace), &pod_ref.name);
        let mut pod: Pod = self.storage.get(&key).await?;

        if pod.metadata.uid != pod_ref.uid {
            // This special error suppresses retries in our caller. We can stop
            // because the pod we were meant to evict is already gone and
            // happens to be replaced by some other pod which reuses the same
            // name.
            return Err(Error::NotFound(format!(
                "pods \"{}\" not found",
                pod_ref.name
            )));
        }

        if pod.metadata.deletion_timestamp.is_some() {
            // Already deleted, no need to evict.
            return Ok(());
        }

        // Emit the event only if we are actually doing something.
        let _ = self.events_tx.send(PodEvent {
            pod: pod_ref.clone(),
            message: "Marking for deletion",
        });

        let status = pod.status.get_or_insert_with(Default::default);
        let conditions = status.conditions.get_or_insert_with(Vec::new);
        let updated = update_pod_condition(
            conditions,
            PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                reason: Some("DeletionByDeviceTaintManager".to_string()),
                message: Some("Device Taint manager: deleting due to NoExecute taint".to_string()),
                last_probe_time: None,
                last_transition_time: Some((self.clock)()),
                observed_generation: None,
            },
        );
        if updated {
            self.storage.update_status(&key, &pod).await?;
        }

        // Unlikely, but it could happen that the pod we got above got replaced
        // with another pod using the same name in the meantime. Upstream
        // includes a UID precondition in the DELETE; here the UID is
        // re-checked right before the delete (see the module docs).
        let current: Pod = self.storage.get(&key).await?;
        if current.metadata.uid != pod_ref.uid {
            return Err(Error::Conflict(format!(
                "Precondition failed: UID in precondition: {}, UID in object meta: {}",
                pod_ref.uid, current.metadata.uid
            )));
        }
        self.storage.delete_gracefully(&key).await
    }

    /// `maybeUpdateRuleStatus`.
    pub async fn maybe_update_rule_status(&self, rule_ref: &NamespacedObject) -> Result<Duration> {
        debug!(rule = %rule_ref.name, "Processing DeviceTaintRule status work item");
        let Some(rules) = &self.rules else {
            return Ok(Duration::ZERO);
        };

        // Everything up to the API call runs under the lock, like upstream.
        let plan = {
            let state = self.state.lock().unwrap();
            let Some(rule) = rules.get(&rule_ref.name) else {
                debug!("DeviceTaintRule got deleted, removing from work queue");
                return Ok(Duration::ZERO);
            };
            if rule.metadata.uid != rule_ref.uid {
                debug!("DeviceTaintRule got replaced, removing old one from work queue");
                return Ok(Duration::ZERO);
            }
            match self.rule_status_condition(&state, &rule) {
                RulePlan::Wait(delay) => return Ok(delay),
                RulePlan::Unchanged => return Ok(Duration::ZERO),
                RulePlan::Apply(condition) => (rule, condition),
            }
        };

        let (rule, condition) = plan;
        // Apply the new condition, but only if the UID matches.
        let apply_config = serde_json::json!({
            "apiVersion": rule.api_version,
            "kind": rule.kind,
            "metadata": {"name": rule.metadata.name, "uid": rule.metadata.uid},
            "status": {"conditions": [condition]},
        });
        let key = build_key("devicetaintrules", None, &rule.metadata.name);
        let _: DeviceTaintRule = self
            .storage
            .apply(&key, &apply_config, &self.name, true, Some("status"))
            .await
            .map_err(|e| anyhow!("add condition to DeviceTaintRule status: {e}"))?;

        // No further updates needed until some more pods get evicted or ready
        // for eviction.
        Ok(Duration::ZERO)
    }

    /// The decision part of `maybeUpdateRuleStatus`: compute the new
    /// `EvictionInProgress` condition (or why none is needed yet).
    fn rule_status_condition(&self, state: &State, rule: &Arc<DeviceTaintRule>) -> RulePlan {
        let existing = rule
            .status
            .conditions
            .iter()
            .find(|c| c.condition_type == CONDITION_EVICTION_IN_PROGRESS)
            .cloned();

        // LastTransitionTime gets bumped each time we make any change to the
        // condition, even if it is only a change of the message. We use this
        // to track when it is time for another update.
        //
        // This is intentionally checked before counting pending pods because
        // that might be expensive. The effect is that "eviction in progress"
        // gets set to false only with a certain delay instead of immediately
        // after deleting the last pod.
        let now = (self.clock)();
        let generation = rule.metadata.generation.unwrap_or(0);
        if let Some(existing) = &existing {
            let since = now
                - existing
                    .last_transition_time
                    .unwrap_or(DateTime::<Utc>::MIN_UTC);
            let period = ChronoDuration::from_std(RULE_STATUS_PERIOD).unwrap();
            if existing.observed_generation.unwrap_or(0) == generation && since < period {
                // Don't update quite yet.
                return RulePlan::Wait((period - since).to_std().unwrap_or_default());
            }
        }

        let env = Env {
            queue: &NopQueue,
            pods: &self.pods,
            events: None,
            clock: &self.clock,
        };

        // Checking all pods might be expensive. Only do it if really needed.
        let (mut num_tainted_slice_devices, mut num_tainted_allocated_devices) = (0i64, 0i64);
        let (mut num_pending_pods, mut num_pending_namespaces) = (0i64, 0i64);
        match &rule.spec.taint.effect {
            Some(DeviceTaintEffect::None) => {
                // Temporarily change the effect from None to NoExecute to
                // simulate. We pretend to do that through informer events on a
                // throw-away state, so there is no lasting impact on the real
                // one.
                let mut rule_evict = (**rule).clone();
                rule_evict.spec.taint.effect = Some(DeviceTaintEffect::NoExecute);
                let mut simulated = state.simulation();
                simulated.handle_rule_change(&env, Some(rule), Some(&Arc::new(rule_evict)));
                (num_pending_pods, num_pending_namespaces) =
                    simulated.count_pending_pods(&env, rule);
                (num_tainted_slice_devices, num_tainted_allocated_devices) =
                    simulated.count_tainted_devices(rule);
            }
            Some(DeviceTaintEffect::NoExecute) => {
                (num_pending_pods, num_pending_namespaces) = state.count_pending_pods(&env, rule);
            }
            _ => {}
        }

        let effect_name = effect_name(&rule.spec.taint.effect);
        let num_evicted_pods = state
            .taint_rule_stats
            .get(&rule.metadata.uid)
            .map_or(0, |s| s.num_evicted_pods);

        // Some fields are tentative and get updated below.
        let mut new_condition = Condition {
            condition_type: CONDITION_EVICTION_IN_PROGRESS.to_string(),
            status: "False".to_string(),
            reason: Some(format!("Effect{effect_name}")),
            message: Some(String::new()),
            observed_generation: Some(generation),
            // To avoid a false "is different" in the comparison; gets updated
            // later.
            last_transition_time: existing.as_ref().and_then(|c| c.last_transition_time),
        };
        let plural = |n: i64, one: &str, many: &str| {
            if n == 1 {
                one.to_string()
            } else {
                many.replace("{}", &n.to_string())
            }
        };
        let mut message = String::new();
        match &rule.spec.taint.effect {
            Some(DeviceTaintEffect::NoExecute) => {
                if num_pending_pods > 0 {
                    new_condition.reason = Some("PodsPendingEviction".to_string());
                    new_condition.status = "True".to_string();
                    message = plural(
                        num_pending_pods,
                        "1 pod needs to be evicted in ",
                        "{} pods need to be evicted in ",
                    );
                    message += &plural(
                        num_pending_namespaces,
                        "1 namespace.",
                        "{} different namespaces.",
                    );
                } else if num_evicted_pods > 0 {
                    new_condition.reason = Some("Completed".to_string());
                } else {
                    new_condition.reason = Some("NotStarted".to_string());
                }
            }
            Some(DeviceTaintEffect::None) => {
                new_condition.reason = Some("NoEffect".to_string());
                message += &plural(
                    num_tainted_slice_devices,
                    "1 published device selected. ",
                    "{} published devices selected. ",
                );
                message += &plural(
                    num_tainted_allocated_devices,
                    "1 allocated device selected. ",
                    "{} allocated devices selected. ",
                );
                message += &plural(
                    num_pending_pods,
                    "1 pod would be evicted in ",
                    "{} pods would be evicted in ",
                );
                message += &plural(
                    num_pending_namespaces,
                    "1 namespace ",
                    "{} different namespaces ",
                );
                message += "if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.";
            }
            _ => {
                new_condition.reason = Some("OtherEffect".to_string());
                message = "Eviction only happens for the NoExecute effect.".to_string();
            }
        }
        if num_evicted_pods > 0 {
            if !message.is_empty() {
                message += " ";
            }
            message += &plural(num_evicted_pods, "1 pod ", "{} pods ");
            message += "evicted since starting the controller.";
        }
        new_condition.message = Some(message);

        let unchanged = existing.as_ref().is_some_and(|e| {
            e.status == new_condition.status
                && e.reason == new_condition.reason
                && e.message.as_deref().unwrap_or("")
                    == new_condition.message.as_deref().unwrap_or("")
                && e.observed_generation.unwrap_or(0) == generation
        });
        if unchanged {
            return RulePlan::Unchanged;
        }
        new_condition.last_transition_time = Some(now);
        debug!(rule = %rule.metadata.name, condition = ?new_condition, "Calculated new condition");
        RulePlan::Apply(new_condition)
    }
}

/// The outcome of the decision part of `maybeUpdateRuleStatus`.
enum RulePlan {
    /// Updated too recently: check again after this long.
    Wait(Duration),
    /// The calculated condition equals the existing one.
    Unchanged,
    /// Write this condition.
    Apply(Condition),
}

/// `string(rule.Spec.Taint.Effect)`.
fn effect_name(effect: &Option<DeviceTaintEffect>) -> String {
    match effect {
        None => String::new(),
        Some(DeviceTaintEffect::None) => "None".to_string(),
        Some(DeviceTaintEffect::NoSchedule) => "NoSchedule".to_string(),
        Some(DeviceTaintEffect::NoExecute) => "NoExecute".to_string(),
        Some(DeviceTaintEffect::Unknown(s)) => s.clone(),
    }
}

/// Record one `DeviceTaintManagerEviction` event on the pod.
async fn record_pod_event<S: Storage + 'static>(
    recorder: &EventRecorder<S>,
    component: &str,
    event: &PodEvent,
) {
    let involved = ObjectReference {
        api_version: Some("v1".to_string()),
        kind: Some("Pod".to_string()),
        namespace: Some(event.pod.namespace.clone()),
        name: Some(event.pod.name.clone()),
        uid: Some(event.pod.uid.clone()),
        ..Default::default()
    };
    let source = EventSource {
        component: component.to_string(),
        host: None,
    };
    if let Err(e) = recorder
        .event(
            &involved,
            &source,
            EventType::Normal,
            "DeviceTaintManagerEviction",
            event.message,
        )
        .await
    {
        warn!("failed to record DeviceTaintManagerEviction event: {e}");
    }
}

// ---------------------------------------------------------------------------
// Informer
// ---------------------------------------------------------------------------

/// A minimal shared-informer stand-in: list + watch `prefix`, keep the
/// indexer, and call `on_change(old, new)` exactly like a client-go
/// `ResourceEventHandlerFuncs` (`OnAdd` = `(None, Some)`, `OnUpdate` =
/// `(Some, Some)`, `OnDelete` = `(Some, None)`). `synced` flips to true after
/// the first list has been delivered (`HasSynced`). On a broken watch the
/// reflector relists and the differences are delivered (`DeltaFIFO.Replace`).
pub async fn run_informer<S, T, F>(
    storage: Arc<S>,
    prefix: String,
    key_of: fn(&T) -> String,
    synced: Arc<AtomicBool>,
    mut on_change: F,
) where
    S: Storage + 'static,
    T: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
    F: FnMut(Option<&Arc<T>>, Option<&Arc<T>>) + Send + 'static,
{
    let mut cache: HashMap<String, Arc<T>> = HashMap::new();
    loop {
        // List, remembering the revision to watch from.
        let revision = storage.current_revision().await.unwrap_or(0);
        match storage.list::<T>(&prefix).await {
            Ok(items) => {
                let mut seen: HashSet<String> = HashSet::new();
                for item in items {
                    let key = key_of(&item);
                    seen.insert(key.clone());
                    let new = Arc::new(item);
                    let old = cache.insert(key, Arc::clone(&new));
                    on_change(old.as_ref(), Some(&new));
                }
                let gone: Vec<String> = cache
                    .keys()
                    .filter(|k| !seen.contains(*k))
                    .cloned()
                    .collect();
                for key in gone {
                    if let Some(old) = cache.remove(&key) {
                        on_change(Some(&old), None);
                    }
                }
                synced.store(true, Ordering::SeqCst);
            }
            Err(e) => {
                warn!(%prefix, "informer list failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }

        let stream = match storage.watch_from_revision(&prefix, revision + 1).await {
            Ok(s) => Ok(s),
            // Too old (compacted): a plain watch after the relist above.
            Err(_) => storage.watch(&prefix).await,
        };
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                warn!(%prefix, "informer watch failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        while let Some(event) = stream.next().await {
            let event = match event {
                Ok(e) => e,
                Err(e) => {
                    warn!(%prefix, "informer watch error: {e}");
                    break;
                }
            };
            match event {
                WatchEvent::Added(_, value) | WatchEvent::Modified(_, value) => {
                    let Ok(item) = serde_json::from_str::<T>(&value) else {
                        continue;
                    };
                    let key = key_of(&item);
                    let new = Arc::new(item);
                    let old = cache.insert(key, Arc::clone(&new));
                    on_change(old.as_ref(), Some(&new));
                }
                WatchEvent::Deleted(_, value) => {
                    let Ok(item) = serde_json::from_str::<T>(&value) else {
                        continue;
                    };
                    let key = key_of(&item);
                    // `DeletedFinalStateUnknown` unwrapping: prefer the cached
                    // object, fall back to the last known state in the event.
                    let old = cache.remove(&key).unwrap_or_else(|| Arc::new(item));
                    on_change(Some(&old), None);
                }
            }
        }
        // The watch ended: relist.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The controller descriptor's `requiredFeatureGates`
/// (`cmd/kube-controller-manager/app/core.go:255-259`):
/// `DynamicResourceAllocation` and `DRADeviceTaints`.
pub fn feature_enabled() -> bool {
    use rusternetes_common::feature_gates::{enabled, Feature};
    enabled(Feature::DynamicResourceAllocation) && enabled(Feature::DRADeviceTaints)
}
