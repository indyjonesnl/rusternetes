use chrono::Utc;
use rusternetes_common::{
    resources::{EventSource, EventType, Node, ObjectReference, Pod, PriorityClass},
    types::Phase,
};
use rusternetes_storage::{
    build_key, build_prefix, EventRecorder, Storage, StorageBackend, WatchEvent,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

use crate::advanced::parse_resource_quantity;
use crate::data_plane::{ApiBackend, DataPlane};
use crate::diagnosis::{fit_error_message, Diagnosis};
use crate::framework_runtime::{names, ExtensionPoint};
use crate::fwk::CycleState;
use crate::profile::DEFAULT_SCHEDULER_NAME;
use crate::queue::{
    hints, node_scheduling_properties_change, pod_scheduling_properties_change, priority_sort_less,
    ActionType, ClusterEvent, EventObject, EventResource, NominatingInfo, PriorityQueue,
    PriorityQueueOptions, QueuedPodInfo, EVENT_ASSIGNED_POD_ADD, EVENT_ASSIGNED_POD_DELETE,
};
use crate::volume_binding;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Outcome of one scheduling cycle for `handleSchedulingFailure`.
#[derive(Debug)]
pub enum ScheduleOutcome {
    /// Bound, or nothing to do (deleted / already assigned / not ours).
    Done,
    /// `FitError`: no node fit; the plugins that rejected the pod
    /// (`Diagnosis.UnschedulablePlugins`). Empty when the failure was not a
    /// fit error (e.g. no nodes registered), which the queue treats as an
    /// error and backs off.
    Unschedulable(std::collections::HashSet<String>),
}

/// Last-seen pods (by `ns/name`) and nodes (by name): the `oldObj` the
/// informer would pass to the update/delete handlers.
#[derive(Default)]
pub struct SeenObjects {
    pods: HashMap<String, Pod>,
    nodes: HashMap<String, Node>,
}

/// `assignedPod` (eventhandlers.go:470-472).
fn assigned_pod(pod: &Pod) -> bool {
    !pod_node_name(pod).is_empty()
}

fn pod_node_name(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

/// `newPodInformer`'s field selector (scheduler.go:651-656):
/// `status.phase!=Succeeded,status.phase!=Failed`.
fn is_terminal_pod(pod: &Pod) -> bool {
    matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_ref()),
        Some(Phase::Succeeded) | Some(Phase::Failed)
    )
}

fn pod_key(pod: &Pod) -> String {
    crate::queue::pod_key(pod)
}

/// Both objects carry a resourceVersion and it is the same one: the relist
/// returned the object the watch already delivered.
fn same_resource_version(old: &Pod, new: &Pod) -> bool {
    old.metadata.resource_version.is_some()
        && old.metadata.resource_version == new.metadata.resource_version
}

/// `corev1helpers.PodPriority`.
fn pod_priority(pod: &Pod) -> i32 {
    pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0)
}

/// `hasNominatedNodeNameChanged` (eventhandlers.go:342-344).
fn has_nominated_node_name_changed(old_pod: &Pod, new_pod: &Pod) -> bool {
    let nn = |p: &Pod| {
        p.status
            .as_ref()
            .and_then(|s| s.nominated_node_name.clone())
            .unwrap_or_default()
    };
    let old = nn(old_pod);
    !old.is_empty() && old != nn(new_pod)
}

pub struct Scheduler<S: Storage + Send + Sync + 'static = StorageBackend> {
    /// All reads/writes flow through the data plane — either storage directly
    /// (all-in-one) or the api-server over HTTP (in-cluster static pod). See
    /// `data_plane.rs` for the classification of the original 22 storage sites.
    pub data: DataPlane<S>,
    pub interval: Duration,
    /// Scheduling profiles by scheduler name (`Scheduler.Profiles`,
    /// pkg/scheduler/scheduler.go). A single `default-scheduler` profile unless
    /// the configuration lists more.
    pub profiles: crate::profile::Map,
    /// Unified event recorder for STORAGE mode — the scheduler is the source of
    /// truth for the `Scheduled` / `FailedScheduling` events, routed through the
    /// shared `EventCorrelator` (dedup/count/series + spam-filter) like
    /// upstream's `recorder.Eventf` after bind. `None` in API mode, where events
    /// POST through the data plane's client recorder instead.
    pub recorder: Option<EventRecorder<S>>,
    /// Long-lived VolumeBinding assume caches (`volumeBinder.pvCache` /
    /// `pvcCache`), refreshed from the volume listers every cycle.
    pub volume_binder: Arc<std::sync::Mutex<crate::volume_assume::VolumeBinder>>,
    /// Poll interval of `BindPodVolumes` (1s upstream).
    pub volume_bind_poll: Duration,
    /// `Scheduler.nextStartNodeIndex` (scheduler.go): where the next cycle
    /// starts looking for feasible nodes, so every node gets examined.
    pub next_start_node_index: AtomicUsize,
    /// Test override of `getOffsetRand` (default_preemption.go:185-189, a
    /// package variable upstream tests replace); `usize::MAX` = random.
    pub preemption_offset_override: AtomicUsize,
}

impl Scheduler<StorageBackend> {
    /// Construct an api-server-backed scheduler (in-cluster static pod mode)
    /// over the given profiles. Reads come from the pods/nodes informers;
    /// writes go through the binding subresource + status PUT; events POST to
    /// `/api/v1/.../events`.
    pub fn new_api_with_profiles(
        api: ApiBackend,
        interval_secs: u64,
        profiles: crate::profile::Map,
    ) -> Self {
        Self {
            data: DataPlane::Api(api),
            recorder: None,
            interval: Duration::from_secs(interval_secs),
            profiles,
            volume_binder: Arc::default(),
            volume_bind_poll: crate::volume_bind::BIND_POLL_INTERVAL,
            next_start_node_index: AtomicUsize::new(0),
            preemption_offset_override: AtomicUsize::new(usize::MAX),
        }
    }
}

/// A map holding one defaulted profile named `scheduler_name`.
fn single_profile(scheduler_name: &str) -> crate::profile::Map {
    crate::profile::Map::new_map(vec![crate::profile::Profile::named(scheduler_name)])
        .expect("a single named, defaulted profile is always valid")
}

impl<S: Storage + Send + Sync + 'static> Scheduler<S> {
    pub fn new_with_name(storage: Arc<S>, interval_secs: u64, scheduler_name: String) -> Self {
        Self::new_with_profiles(storage, interval_secs, single_profile(&scheduler_name))
    }

    /// Construct over a multi-profile [`crate::profile::Map`].
    pub fn new_with_profiles(
        storage: Arc<S>,
        interval_secs: u64,
        profiles: crate::profile::Map,
    ) -> Self {
        Self {
            recorder: Some(EventRecorder::new(Arc::clone(&storage))),
            data: DataPlane::Storage(storage),
            interval: Duration::from_secs(interval_secs),
            profiles,
            volume_binder: Arc::default(),
            volume_bind_poll: crate::volume_bind::BIND_POLL_INTERVAL,
            next_start_node_index: AtomicUsize::new(0),
            preemption_offset_override: AtomicUsize::new(usize::MAX),
        }
    }

    /// `frameworkForPod` (schedule_one.go:390-396).
    pub fn profile_for(&self, pod: &Pod) -> Result<&crate::profile::Profile, String> {
        self.profiles.framework_for_pod(pod)
    }

    /// Record a preemption nomination and persist it to
    /// `status.nominatedNodeName` via the `/status` subresource.
    ///
    /// The queue's nominator (`AddNominatedPod`, scheduling_queue.go:628-632,
    /// nominator.go:70-107) is updated BEFORE the API write, "to avoid races
    /// with the following scheduling cycles" (nominator.go:62-65). Deviation:
    /// upstream calls it from `handleSchedulingFailure` with the PostFilter
    /// result (schedule_one.go:1095-1101); here eviction and the status write
    /// happen inside `try_schedule_pod`, so the nomination is recorded at the
    /// same point.
    async fn nominate(&self, queue: &PriorityQueue, pod: &Pod, pod_key: &str, node_name: &str) {
        queue.add_nominated_pod(pod, Some(&NominatingInfo::override_node(node_name)));
        let node = node_name.to_string();
        self.update_pod_status_with_retry(pod_key, move |p| {
            if let Some(ref mut status) = p.status {
                status.nominated_node_name = Some(node.clone());
            }
        })
        .await;
    }

    /// Overlay the queue nominator's nominations onto a freshly-read pod list
    /// so the nominated-pod space reservation sees them before the informer
    /// propagates the `/status` write (upstream reads
    /// `NominatedPodsForNode`, runtime/framework.go:1200; our reservation
    /// reads `status.nominatedNodeName` from the pod list instead). Bound pods
    /// are skipped; the nominator drops them on delete / after scheduling.
    fn apply_nominations(&self, queue: &PriorityQueue, all_pods: &mut [Pod]) {
        let noms = queue.nominated_nodes();
        if noms.is_empty() {
            return;
        }
        for pod in all_pods.iter_mut() {
            let bound = pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .is_some();
            if bound {
                continue;
            }
            if let Some(node) = noms.get(&pod.metadata.uid) {
                let status = pod.status.get_or_insert_with(Default::default);
                if status.nominated_node_name.is_none() {
                    status.nominated_node_name = Some(node.clone());
                }
            }
        }
    }

    /// `responsibleForPod` (eventhandlers.go:474-476):
    /// `profiles.HandlesSchedulerName(pod.Spec.SchedulerName)`.
    fn responsible_for_pod(&self, pod: &Pod) -> bool {
        self.profiles
            .handles_scheduler_name(crate::profile::pod_scheduler_name(pod))
    }

    /// Emit a pod-scoped event through the unified recorder, sourced from the
    /// pod's profile (`source.component` = its `schedulerName`, the recorder
    /// `NewRecorderFactory` builds per profile, profile.go:85-90). Errors are
    /// logged, never propagated — a failed event must not abort a
    /// bind/scheduling decision.
    async fn emit_pod_event(&self, pod: &Pod, event_type: EventType, reason: &str, message: &str) {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        let component = crate::profile::pod_scheduler_name(pod);
        // API mode: POST through the data plane's client recorder.
        if let Some(recorder) = self.recorder.as_ref() {
            let involved = ObjectReference {
                kind: Some("Pod".to_string()),
                namespace: Some(ns.to_string()),
                name: Some(pod.metadata.name.clone()),
                uid: Some(pod.metadata.uid.clone()),
                api_version: Some("v1".to_string()),
                resource_version: pod.metadata.resource_version.clone(),
                field_path: None,
            };
            let source = EventSource {
                component: component.to_string(),
                host: None,
            };
            if let Err(e) = recorder
                .event(&involved, &source, event_type, reason, message)
                .await
            {
                warn!(
                    "Failed to record {} event for pod {}/{}: {}",
                    reason, ns, pod.metadata.name, e
                );
            }
        } else {
            self.data
                .emit_event_api(
                    component,
                    ns,
                    reason,
                    message,
                    event_type,
                    ("Pod", ns, &pod.metadata.name, &pod.metadata.uid),
                )
                .await;
        }
    }

    /// Persist a pod **status** change with one-attempt retry on
    /// resource-version Conflict, via the `/status` subresource (API mode) or a
    /// whole-pod write (storage mode). Used for `nominatedNodeName` and the
    /// `Unschedulable` PodScheduled condition: the api-server rejects status
    /// changes on a whole-pod PUT, and without the persisted `nominatedNodeName`
    /// reservation preemption live-locks (preempt → victims recreated → preempt
    /// again). The mutator re-applies to the freshly-read pod on Conflict, so it
    /// must be idempotent.
    async fn update_pod_status_with_retry<F>(&self, pod_key: &str, mutate: F)
    where
        F: Fn(&mut Pod) + Send,
    {
        let (ns, name) = parse_pod_storage_key(pod_key);
        for attempt in 0..2 {
            let mut pod: Pod = match self.data.get_pod(ns, name).await {
                Ok(p) => p,
                Err(rusternetes_common::Error::NotFound(_)) => return,
                Err(e) => {
                    error!(error = %e, pod = pod_key, "scheduler: failed to read pod");
                    return;
                }
            };
            mutate(&mut pod);
            match self.data.update_pod_status(ns, name, &pod).await {
                Ok(_) => return,
                Err(rusternetes_common::Error::Conflict(_)) if attempt == 0 => continue,
                Err(e) => {
                    error!(error = %e, pod = pod_key, "scheduler: failed to update pod status");
                    return;
                }
            }
        }
    }

    pub async fn run(self: Arc<Self>) -> rusternetes_common::Result<()> {
        use futures::StreamExt;

        info!(
            "Scheduler profiles {:?} started (watch-based, resync every {:?})",
            self.profiles.names(),
            self.interval
        );

        // `New` (scheduler.go:282-341): the per-profile QueueingHintMap feeds
        // the scheduling queue; PrioritySort is the only queue-sort plugin.
        // The DynamicResources hints read claims through this lister, which the
        // ResourceClaim feed keeps current.
        let claim_cache = crate::queue::dra_hints::ResourceClaimCache::default();
        let queueing_hint_map =
            hints::build_queueing_hint_map_per_profile(self.profiles.profiles(), &claim_cache);
        // `unionedGVKs` (scheduler.go:635-647) decides which resource
        // handlers `addAllEventHandlers` installs below.
        let gvk_map = crate::resource_events::unioned_gvks(&queueing_hint_map);
        let queue = PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                queueing_hint_map,
                // `WithPreEnqueuePluginMap` (scheduler.go:388-414): gated pods
                // stay out of activeQ.
                pre_enqueue_plugin_map:
                    crate::queue::scheduling_gates::build_pre_enqueue_plugin_map(
                        self.profiles.profiles(),
                    ),
                // Upstream default (5m): pods rejected by the volume plugins
                // are requeued by the PV/PVC/StorageClass/CSI* events fed by
                // `add_resource_event_handlers`, not by a short flush.
                ..Default::default()
            },
        );
        crate::resource_events::add_resource_event_handlers(
            &self.data,
            &queue,
            &gvk_map,
            self.interval,
            &claim_cache,
        );
        queue.run();

        let worker_queue = Arc::clone(&queue);
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        // The informer hands `addPod`/`updatePod`/`deletePod` both the old and
        // the new object; our watches deliver only the new one, so keep the
        // last-seen copy per key to supply `oldObj`.
        let mut seen = SeenObjects::default();

        // API mode: drive the queue from the pods/nodes reflectors instead of
        // a storage watch.
        if let DataPlane::Api(api) = &self.data {
            let pods = Arc::clone(&api.pods);
            let nodes = Arc::clone(&api.nodes);
            let priority_classes = Arc::clone(&api.priority_classes);
            tokio::spawn(async move { pods.run().await });
            tokio::spawn(async move { nodes.run().await });
            tokio::spawn(async move { priority_classes.run().await });
            api.volumes.spawn();
            // Upstream starts the informers, then WaitForCacheSync before
            // `sched.Run` (cmd/kube-scheduler/app/server.go:271-285). Pods only
            // enter the queue from the loop below, so no cycle runs before the
            // listers (notably NodeVolumeLimits') are populated.
            api.wait_for_cache_sync().await;

            let mut pod_sub = api.pods.subscribe();
            let mut node_sub = api.nodes.subscribe();
            let mut resync = tokio::time::interval(self.interval);
            resync.tick().await; // consume the immediate first tick
            loop {
                use rusternetes_client::reflector::StoreEvent;
                tokio::select! {
                    ev = pod_sub.recv() => {
                        match ev {
                            Ok(StoreEvent::Added(p) | StoreEvent::Modified(p)) => {
                                self.on_pod_event(&queue, &mut seen, p);
                            }
                            Ok(StoreEvent::Deleted(p)) => {
                                self.on_pod_deleted(&queue, &mut seen, &p);
                            }
                            // Lagged/closed: fall back to a full resync below.
                            Err(_) => { self.resync(&queue, &mut seen).await; }
                        }
                    }
                    ev = node_sub.recv() => {
                        match ev {
                            Ok(StoreEvent::Added(n) | StoreEvent::Modified(n)) => {
                                self.on_node_event(&queue, &mut seen, n);
                            }
                            Ok(StoreEvent::Deleted(n)) => {
                                self.on_node_deleted(&queue, &mut seen, &n);
                            }
                            Err(_) => { self.resync(&queue, &mut seen).await; }
                        }
                    }
                    _ = resync.tick() => {
                        self.resync(&queue, &mut seen).await;
                    }
                }
            }
        }

        // Storage mode (all-in-one): watch /registry/pods and /registry/nodes
        // directly.
        let storage = match &self.data {
            DataPlane::Storage(s) => Arc::clone(s),
            DataPlane::Api(_) => unreachable!("API mode handled above"),
        };
        loop {
            self.resync(&queue, &mut seen).await;

            let pod_watch = storage.watch(&build_prefix("pods", None)).await;
            let node_watch = storage.watch(&build_prefix("nodes", None)).await;
            let (mut pod_watch, mut node_watch) = match (pod_watch, node_watch) {
                (Ok(p), Ok(n)) => (p, n),
                (Err(e), _) | (_, Err(e)) => {
                    error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };

            // Resync interval as a safety net — shorter than other controllers
            // because scheduling latency directly impacts pod startup time
            let mut resync = tokio::time::interval(self.interval);
            resync.tick().await; // consume the immediate first tick

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v))) => {
                                match serde_json::from_str::<Pod>(&v) {
                                    Ok(p) => self.on_pod_event(&queue, &mut seen, p),
                                    Err(e) => warn!("Cannot decode watched pod: {}", e),
                                }
                            }
                            Some(Ok(WatchEvent::Deleted(_, v))) => {
                                match serde_json::from_str::<Pod>(&v) {
                                    Ok(p) => self.on_pod_deleted(&queue, &mut seen, &p),
                                    Err(e) => warn!("Cannot decode deleted pod: {}", e),
                                }
                            }
                            Some(Err(e)) => {
                                warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = node_watch.next() => {
                        match event {
                            Some(Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v))) => {
                                match serde_json::from_str::<Node>(&v) {
                                    Ok(n) => self.on_node_event(&queue, &mut seen, n),
                                    Err(e) => warn!("Cannot decode watched node: {}", e),
                                }
                            }
                            Some(Ok(WatchEvent::Deleted(_, v))) => {
                                match serde_json::from_str::<Node>(&v) {
                                    Ok(n) => self.on_node_deleted(&queue, &mut seen, &n),
                                    Err(e) => warn!("Cannot decode deleted node: {}", e),
                                }
                            }
                            Some(Err(e)) => {
                                warn!("Node watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                warn!("Node watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.resync(&queue, &mut seen).await;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Event handlers (pkg/scheduler/eventhandlers.go)
    // -----------------------------------------------------------------------

    /// `addPod` / `updatePod` (eventhandlers.go:130-183): the first sighting
    /// of a key is an add, later ones are updates with the last-seen copy as
    /// `oldPod`. The pod informer lists only non-terminal pods
    /// (`newPodInformer`, scheduler.go:651-656), so a pod turning
    /// Succeeded/Failed is seen as a delete.
    fn on_pod_event(&self, queue: &PriorityQueue, seen: &mut SeenObjects, pod: Pod) {
        if is_terminal_pod(&pod) {
            self.on_pod_deleted(queue, seen, &pod);
            return;
        }
        let key = pod_key(&pod);
        match seen.pods.insert(key, pod.clone()) {
            None => self.add_pod(queue, &pod),
            // The informer never fires UpdateFunc for an unchanged object
            // (resync period 0, options.go:336); the periodic relist here
            // must not either, or every assigned pod would emit a spurious
            // AssignedPodUpdate per tick.
            Some(old) if same_resource_version(&old, &pod) => {}
            Some(old) => self.update_pod(queue, &old, &pod),
        }
    }

    /// `deletePod` (eventhandlers.go:184-227).
    fn on_pod_deleted(&self, queue: &PriorityQueue, seen: &mut SeenObjects, pod: &Pod) {
        if seen.pods.remove(&pod_key(pod)).is_none() {
            // Never seen (e.g. already terminal at first sight): nothing to undo.
            return;
        }
        if assigned_pod(pod) {
            self.delete_assigned_pod_from_cache(queue, pod);
        } else if self.responsible_for_pod(pod) {
            self.delete_pod_from_scheduling_queue(queue, pod, false);
        }
    }

    /// `addPod` (eventhandlers.go:130-148).
    fn add_pod(&self, queue: &PriorityQueue, pod: &Pod) {
        if assigned_pod(pod) {
            self.add_assigned_pod_to_cache(queue, pod);
        } else if self.responsible_for_pod(pod) {
            // `addPodToSchedulingQueue` (eventhandlers.go:228-238)
            debug!(pod = %pod_key(pod), "Add event for unscheduled pod");
            queue.add(pod.clone());
        }
    }

    /// `updatePod` (eventhandlers.go:149-183).
    fn update_pod(&self, queue: &PriorityQueue, old_pod: &Pod, new_pod: &Pod) {
        if assigned_pod(old_pod) {
            self.update_assigned_pod_in_cache(queue, old_pod, new_pod);
        } else if assigned_pod(new_pod) {
            // This update means binding operation. We can treat it as adding
            // the pod to a cache.
            self.add_assigned_pod_to_cache(queue, new_pod);
            if self.responsible_for_pod(old_pod) {
                // Pod shouldn't be in the scheduling queue, but in unlikely
                // event that the pod has been bound by another component, it
                // should be removed from scheduling queue for correctness.
                self.delete_pod_from_scheduling_queue(queue, old_pod, true);
            }
        } else if self.responsible_for_pod(old_pod) {
            self.update_pod_in_scheduling_queue(queue, old_pod, new_pod);
        }
    }

    /// `updatePodInSchedulingQueue` (eventhandlers.go:297-340), minus the
    /// assumed-pod branch (Rusternetes binds synchronously, so a pod is never
    /// assumed while unassigned in the informer).
    fn update_pod_in_scheduling_queue(&self, queue: &PriorityQueue, old_pod: &Pod, new_pod: &Pod) {
        // Bypass update event that carries identical objects; otherwise, a
        // duplicated Pod may go through scheduling and cause unexpected
        // behavior (see #96071).
        if old_pod.metadata.resource_version == new_pod.metadata.resource_version {
            return;
        }
        debug!(pod = %pod_key(new_pod), "Update event for unscheduled pod");
        queue.update(Some(old_pod), new_pod);
        if has_nominated_node_name_changed(old_pod, new_pod) {
            // Nominated node changed in pod, so we need to treat it as if the
            // pod was deleted from the old nominated node, because the
            // scheduler treats such a pod as if it was already assigned when
            // scheduling lower or equal priority pods.
            let prio = pod_priority(old_pod);
            let pre_check = move |p: &Pod| pod_priority(p) <= prio;
            queue.move_all_to_active_or_backoff_queue(
                &EVENT_ASSIGNED_POD_DELETE,
                Some(Arc::new(old_pod.clone())),
                None,
                Some(&pre_check),
            );
        }
    }

    /// `deletePodFromSchedulingQueue` (eventhandlers.go:346-377).
    fn delete_pod_from_scheduling_queue(&self, queue: &PriorityQueue, pod: &Pod, in_binding: bool) {
        debug!(pod = %pod_key(pod), "Delete event for unscheduled pod");
        queue.delete(pod);
        if in_binding {
            // Not really a pod removal operation, but a binding; the binding
            // process sends any necessary notifications.
            return;
        }
        if pod
            .status
            .as_ref()
            .and_then(|s| s.nominated_node_name.as_deref())
            .is_some_and(|n| !n.is_empty())
        {
            // When a pod that had nominated node is deleted, it can unblock
            // scheduling of other pods, because the lower or equal priority
            // pods treat such a pod as if it was assigned.
            let prio = pod_priority(pod);
            let pre_check = move |p: &Pod| pod_priority(p) <= prio;
            queue.move_all_to_active_or_backoff_queue(
                &EVENT_ASSIGNED_POD_DELETE,
                Some(Arc::new(pod.clone())),
                None,
                Some(&pre_check),
            );
        }
    }

    /// `addAssignedPodToCache` (eventhandlers.go:385-409), the queue half
    /// (the node snapshot is rebuilt from the lister every cycle).
    fn add_assigned_pod_to_cache(&self, queue: &PriorityQueue, pod: &Pod) {
        debug!(pod = %pod_key(pod), "Add event for scheduled pod");
        queue.move_all_to_active_or_backoff_queue(
            &EVENT_ASSIGNED_POD_ADD,
            None,
            Some(Arc::new(pod.clone())),
            None,
        );
    }

    /// `updateAssignedPodInCache` (eventhandlers.go:412-453).
    fn update_assigned_pod_in_cache(&self, queue: &PriorityQueue, old_pod: &Pod, new_pod: &Pod) {
        debug!(pod = %pod_key(old_pod), "Update event for scheduled pod");
        let old: Arc<EventObject> = Arc::new(old_pod.clone());
        let new: Arc<EventObject> = Arc::new(new_pod.clone());
        for evt in pod_scheduling_properties_change(new_pod, old_pod) {
            queue.move_all_to_active_or_backoff_queue(
                &evt,
                Some(Arc::clone(&old)),
                Some(Arc::clone(&new)),
                None,
            );
        }
    }

    /// `deleteAssignedPodFromCache` (eventhandlers.go:455-467).
    fn delete_assigned_pod_from_cache(&self, queue: &PriorityQueue, pod: &Pod) {
        debug!(pod = %pod_key(pod), "Delete event for scheduled pod");
        queue.move_all_to_active_or_backoff_queue(
            &EVENT_ASSIGNED_POD_DELETE,
            Some(Arc::new(pod.clone())),
            None,
            None,
        );
    }

    /// `addNodeToCache` / `updateNodeInCache` (eventhandlers.go:53-98).
    /// `preCheckForNode` is nil with QueueingHints enabled
    /// (eventhandlers.go:727-736).
    fn on_node_event(&self, queue: &PriorityQueue, seen: &mut SeenObjects, node: Node) {
        let name = node.metadata.name.clone();
        match seen.nodes.insert(name, node.clone()) {
            None => {
                debug!(node = %node.metadata.name, "Add event for node");
                queue.move_all_to_active_or_backoff_queue(
                    &ClusterEvent::new(EventResource::Node, ActionType::ADD),
                    None,
                    Some(Arc::new(node)),
                    None,
                );
            }
            Some(old_node) => {
                if old_node.metadata.resource_version == node.metadata.resource_version {
                    return;
                }
                debug!(node = %node.metadata.name, "Update event for node");
                let old: Arc<EventObject> = Arc::new(old_node.clone());
                let new: Arc<EventObject> = Arc::new(node.clone());
                // Only requeue unschedulable pods if the node became more schedulable.
                for evt in node_scheduling_properties_change(&node, &old_node) {
                    queue.move_all_to_active_or_backoff_queue(
                        &evt,
                        Some(Arc::clone(&old)),
                        Some(Arc::clone(&new)),
                        None,
                    );
                }
            }
        }
    }

    /// `deleteNodeFromCache` (eventhandlers.go:100-128).
    fn on_node_deleted(&self, queue: &PriorityQueue, seen: &mut SeenObjects, node: &Node) {
        if seen.nodes.remove(&node.metadata.name).is_none() {
            return;
        }
        debug!(node = %node.metadata.name, "Delete event for node");
        queue.move_all_to_active_or_backoff_queue(
            &ClusterEvent::new(EventResource::Node, ActionType::DELETE),
            Some(Arc::new(node.clone())),
            None,
            None,
        );
    }

    /// Periodic relist (the reflector's resync, which upstream's informers
    /// also perform): replays adds/updates/deletes the watch may have missed,
    /// The queue's own ticker runs `flushUnschedulablePodsLeftover`.
    pub async fn resync(&self, queue: &PriorityQueue, seen: &mut SeenObjects) {
        match self.data.list_pods().await {
            Ok(pods) => {
                let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
                for pod in pods {
                    if !is_terminal_pod(&pod) {
                        live.insert(pod_key(&pod));
                    }
                    self.on_pod_event(queue, seen, pod);
                }
                let gone: Vec<Pod> = seen
                    .pods
                    .iter()
                    .filter(|(k, _)| !live.contains(*k))
                    .map(|(_, p)| p.clone())
                    .collect();
                for pod in gone {
                    self.on_pod_deleted(queue, seen, &pod);
                }
            }
            Err(e) => error!("Failed to list pods for resync: {}", e),
        }
        match self.data.list_nodes().await {
            Ok(nodes) => {
                let live: std::collections::HashSet<String> =
                    nodes.iter().map(|n| n.metadata.name.clone()).collect();
                for node in nodes {
                    self.on_node_event(queue, seen, node);
                }
                let gone: Vec<Node> = seen
                    .nodes
                    .iter()
                    .filter(|(k, _)| !live.contains(*k))
                    .map(|(_, n)| n.clone())
                    .collect();
                for node in gone {
                    self.on_node_deleted(queue, seen, &node);
                }
            }
            Err(e) => error!("Failed to list nodes for resync: {}", e),
        }
    }

    // -----------------------------------------------------------------------
    // Scheduling loop (pkg/scheduler/schedule_one.go)
    // -----------------------------------------------------------------------

    /// `Scheduler.Run` → `ScheduleOne` (schedule_one.go:65-139): pop one pod
    /// per cycle; on failure hand it to `handleSchedulingFailure`, which
    /// requeues it with the plugins that rejected it so the queue only
    /// re-evaluates it on events those plugins registered.
    async fn worker(&self, queue: Arc<PriorityQueue>) {
        while let Some(mut p_info) = queue.pop().await {
            let ns = p_info.pod.metadata.namespace.clone().unwrap_or_default();
            let key = format!("pods/{}/{}", ns, p_info.pod.metadata.name);
            match self.try_schedule_pod(&queue, &key).await {
                Ok(ScheduleOutcome::Done) => {
                    // `assume` (schedule_one.go:966-970) and the delete /
                    // assigned paths: a bound, gone or foreign pod is no
                    // longer nominated.
                    queue.delete_nominated_pod_if_exists(&p_info.pod);
                    queue.done(&p_info.pod.metadata.uid)
                }
                Ok(ScheduleOutcome::Unschedulable(plugins)) => {
                    // FitError: inject UnschedulablePlugins into the PodInfo.
                    p_info.unschedulable_plugins = plugins;
                    self.handle_scheduling_failure(&queue, p_info).await;
                }
                Err(e) => {
                    debug!("Failed to schedule pod {}: {}", key, e);
                    // Not a FitError: no plugins → an error backoff.
                    p_info.unschedulable_plugins.clear();
                    self.handle_scheduling_failure(&queue, p_info).await;
                }
            }
        }
    }

    /// `handleSchedulingFailure` (schedule_one.go:1033-1113), the queue
    /// half: re-read the pod from the cache; a pod that is gone or got
    /// assigned meanwhile is only `Done`, otherwise it goes back through
    /// `AddUnschedulableIfNotPresent`. The FailedScheduling event and the
    /// nomination are already handled by `try_schedule_pod`.
    ///
    /// Deviation: upstream reads the informer cache, which can only miss.
    /// Our storage-mode read can fail transiently; that is not "gone", so
    /// the in-flight copy is requeued as an error (backoff) instead of being
    /// `Done`d out of every queue.
    async fn handle_scheduling_failure(&self, queue: &PriorityQueue, mut p_info: QueuedPodInfo) {
        let uid = p_info.pod.metadata.uid.clone();
        let ns = p_info.pod.metadata.namespace.clone().unwrap_or_default();
        let name = p_info.pod.metadata.name.clone();
        match self.data.cached_pod(&ns, &name).await {
            Ok(None) => {
                info!(pod = %format!("{ns}/{name}"), "Pod doesn't exist in informer cache");
                // AddUnschedulableIfNotPresent isn't called: Done explicitly.
                queue.done(&uid);
            }
            Ok(Some(cached)) if assigned_pod(&cached) => {
                info!(pod = %format!("{ns}/{name}"), node = %pod_node_name(&cached), "Pod has been assigned to node. Abort adding it back to queue.");
                queue.done(&uid);
            }
            Ok(Some(cached)) if cached.metadata.uid != uid => {
                debug!(pod = %format!("{ns}/{name}"), old_uid = %uid, new_uid = %cached.metadata.uid, "Pod was recreated while handling scheduling failure. Skip requeueing and status updates.");
                queue.done(&uid);
            }
            Ok(Some(cached)) => {
                p_info.pod = cached;
                if let Err(e) =
                    queue.add_unschedulable_if_not_present(p_info, queue.scheduling_cycle())
                {
                    error!(error = %e, "Error occurred");
                }
            }
            Err(e) => {
                warn!(pod = %format!("{ns}/{name}"), error = %e, "Transient error re-reading pod after scheduling failure; requeuing with backoff");
                // Not a fit error any more: no plugins → error backoff queue.
                p_info.unschedulable_plugins.clear();
                if let Err(e) =
                    queue.add_unschedulable_if_not_present(p_info, queue.scheduling_cycle())
                {
                    error!(error = %e, "Error occurred");
                }
            }
        }
    }

    /// Try to schedule a single pod by its queue key (pods/{ns}/{name}).
    /// K8s schedules one pod per cycle in scheduleOne().
    pub async fn try_schedule_pod(
        &self,
        queue: &PriorityQueue,
        key: &str,
    ) -> rusternetes_common::Result<ScheduleOutcome> {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        let (ns, name) = match parts.len() {
            3 => (parts[1], parts[2]),
            _ => return Ok(ScheduleOutcome::Done), // invalid key
        };

        let pod_key = build_key("pods", Some(ns), name);
        let pod: Pod = match self.data.get_pod(ns, name).await {
            Ok(p) => p,
            // Gone → nothing to do; a transient read error is an error
            // outcome (backoff + retry), not a deletion.
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(ScheduleOutcome::Done),
            Err(e) => return Err(e),
        };

        // Check if pod needs scheduling
        let has_node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .is_some_and(|n| !n.is_empty());
        if has_node {
            return Ok(ScheduleOutcome::Done); // already scheduled
        }
        let is_pending = pod
            .status
            .as_ref()
            .map(|s| s.phase.is_none() || s.phase == Some(Phase::Pending))
            .unwrap_or(true);
        if !is_pending {
            return Ok(ScheduleOutcome::Done); // not pending
        }
        // `responsibleForPod` (eventhandlers.go:474-476): only pods naming one
        // of our profiles enter the queue.
        if !self.responsible_for_pod(&pod) {
            return Ok(ScheduleOutcome::Done); // wrong scheduler
        }
        // `frameworkForPod` (schedule_one.go:85-92): cannot fail after the
        // check above; upstream logs and drops the pod if it does.
        if let Err(e) = self.profile_for(&pod) {
            error!(error = %e, pod = key, "Error occurred");
            return Ok(ScheduleOutcome::Done);
        }

        // Get nodes and all pods for scheduling context
        let nodes: Vec<Node> = self.data.list_nodes().await?;
        if nodes.is_empty() {
            // `ErrNoNodesAvailable` (scheduler.go:70): not a
            // FitError, so no plugins are recorded and the pod backs off.
            debug!(
                pod = key,
                "Unable to schedule pod; no nodes are registered to the cluster; waiting"
            );
            return Ok(ScheduleOutcome::Unschedulable(Default::default()));
        }
        let priority_classes = self.load_priority_classes().await?;
        // Preemption needs these; a failure to list them must not stop
        // scheduling, so fall back to "no budgets" and log rather than bail.
        let pdbs = self.load_pod_disruption_budgets().await;
        let mut all_pods: Vec<Pod> = self.data.list_pods().await?;
        // Stamp resolved priority onto every in-memory pod from its
        // priorityClassName. Controller-created pods (ReplicaSet, etc.) never
        // get spec.priority set — controllers write straight to storage,
        // bypassing api-server priority admission — so without this the
        // preemption victim selection and the nominated-pod space reservation
        // (both read spec.priority directly) treat every such pod as priority 0
        // and pick the wrong victims. Resolving here, in-memory, is what
        // admission would have done. NOT persisted: the api-server rejects spec
        // mutations on update.
        self.stamp_priorities(&mut all_pods, &priority_classes);
        // Overlay in-memory preemption nominations so the space reservation in
        // select_node sees a just-nominated preemptor before its /status write
        // propagates back through the informer (prevents the live-lock).
        self.apply_nominations(queue, &mut all_pods);

        // Same in-memory priority resolution for the candidate being scheduled.
        let mut pod = pod;
        if pod.spec.as_ref().and_then(|s| s.priority).is_none() {
            let resolved = self.get_pod_priority_sync(&pod, &priority_classes);
            if resolved != 0 {
                if let Some(ref mut spec) = pod.spec {
                    spec.priority = Some(resolved);
                }
            }
        }

        // Try to schedule
        let (selected, cycle_state, diagnosis) = self
            .select_node_diag_volumes(&pod, &nodes, &all_pods, &priority_classes)
            .await;
        if let (Some(node), Some(cycle_state)) = (selected, cycle_state) {
            if let Err(e) = self
                .reserve_prebind_bind(pod, &node.metadata.name, cycle_state)
                .await
            {
                error!("Failed to bind pod {}/{} to node: {}", ns, name, e);
                // Return the error so the work queue retries with backoff
                // (handleBindingCycleError re-queues the pod upstream).
                return Err(e);
            }
        } else if let Some(msg) = diagnosis.scheduling_error.clone() {
            // `schedulePod` returned a non-FitError error: `ScheduleOne`
            // (schedule_one.go:161-164) logs "Error selecting node for pod"
            // and skips PostFilter/preemption; `handleSchedulingFailure`
            // records the error with reason SchedulerError (:1045-1068) and
            // requeues with backoff (no UnschedulablePlugins).
            error!("Error selecting node for pod {}/{}: {}", ns, name, msg);
            self.emit_pod_event(&pod, EventType::Warning, "FailedScheduling", &msg)
                .await;
            return Err(rusternetes_common::Error::Internal(msg));
        } else if let Some((node_name, victims)) = self
            .try_preempt(
                &pod,
                &nodes,
                &all_pods,
                &priority_classes,
                &pdbs,
                &diagnosis,
            )
            .await
        {
            // No node fits — preempt lower-priority pods. The production
            // watch/work-queue path (this method) previously never attempted
            // preemption; it only existed in the test-only schedule_pending_pods,
            // so [sig-scheduling] SchedulerPreemption never worked. Evict the
            // victims and set nominatedNodeName; once the victims terminate, the
            // resync re-enqueues this pod and select_node binds it to the node.
            info!(
                "Preempting {} pod(s) on node {} for higher-priority pod {}/{}",
                victims.len(),
                node_name,
                ns,
                name
            );
            for victim in &victims {
                if let Err(e) = self.evict_pod(&pod, victim).await {
                    error!(
                        "Failed to evict victim pod {}/{}: {}",
                        victim.metadata.namespace.as_deref().unwrap_or("default"),
                        victim.metadata.name,
                        e
                    );
                }
            }
            self.nominate(queue, &pod, &pod_key, &node_name).await;
            // The preemptor stays unschedulable this cycle (PostFilter only
            // nominates); the victims' Pod/Delete events requeue it.
            return Ok(ScheduleOutcome::Unschedulable(
                diagnosis.unschedulable_plugins,
            ));
        } else {
            // No node fits and preemption can't help — surface FailedScheduling.
            // The production path previously stayed silent here; the
            // [sig-scheduling] SchedulerPredicates specs perform an action and
            // wait (via WaitForSchedulerAfterAction) for a Warning/
            // FailedScheduling event for the pod, so without it they time out.
            // The recorder's correlator dedups across cycles, matching upstream
            // recorder.Eventf(pod, Warning, "FailedScheduling", …).
            // Aggregate per-plugin reasons (VolumeBinding etc.) the way
            // upstream FitError.Error() does; fall back to the generic text
            // when no plugin recorded a reason.
            let msg = if diagnosis.is_empty() {
                format!(
                    "0/{} nodes are available: no nodes match the pod's scheduling requirements.",
                    nodes.len()
                )
            } else {
                fit_error_message(nodes.len(), &diagnosis)
            };
            self.emit_pod_event(&pod, EventType::Warning, "FailedScheduling", &msg)
                .await;
            return Ok(ScheduleOutcome::Unschedulable(
                diagnosis.unschedulable_plugins,
            ));
        }

        Ok(ScheduleOutcome::Done)
    }

    /// Run one scheduling cycle — schedules all pending pods.
    ///
    /// Public for testing. The production bin path uses `run()` →
    /// `worker()` (watch + work-queue driven), not this direct method.
    pub async fn schedule_pending_pods(&self) -> rusternetes_common::Result<()> {
        debug!("Looking for pending pods to schedule");

        // Get all pods
        let all_pods: Vec<Pod> = self.data.list_pods().await?;

        // Filter pending pods without a node assignment that are assigned to this scheduler
        let pending_pods: Vec<Pod> = all_pods
            .iter()
            .filter(|p| {
                // Check if pod is pending and unscheduled
                // A pod is considered pending if:
                // 1. It has no node assignment, AND
                // 2. Either it has no status, OR phase is None, OR phase is Pending
                // A pod is unscheduled if node_name is None OR empty string.
                // Some K8s clients send "nodeName": "" in pod templates,
                // which deserializes to Some("") instead of None.
                let has_node = p
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .is_some_and(|n| !n.is_empty());
                let is_pending = !has_node
                    && p.status
                        .as_ref()
                        .map(|s| s.phase.is_none() || s.phase == Some(Phase::Pending))
                        .unwrap_or(true);

                if !is_pending {
                    return false;
                }

                // `responsibleForPod` (eventhandlers.go:474-476): the pod's
                // schedulerName (default "default-scheduler") names a profile.
                self.responsible_for_pod(p)
            })
            .cloned()
            .collect();

        if pending_pods.is_empty() {
            debug!("No pending pods to schedule");
            return Ok(());
        }

        debug!("Found {} pending pods to schedule", pending_pods.len());

        // Get all nodes
        let nodes: Vec<Node> = self.data.list_nodes().await?;

        if nodes.is_empty() {
            warn!("No nodes available for scheduling");
            return Ok(());
        }

        // Load all PriorityClasses for pod priority resolution
        let priority_classes = self.load_priority_classes().await?;
        // Preemption needs these; a failure to list them must not stop
        // scheduling, so fall back to "no budgets" and log rather than bail.
        let pdbs = self.load_pod_disruption_budgets().await;

        // Sort pending pods by priority (descending) — K8s scheduling queue
        // processes higher-priority pods first. Without this, lower-priority
        // replacement pods (from RS controller) can be scheduled before the
        // preemptor, consuming the resources that preemption freed and causing
        // a live-lock: preempt → replacement scheduled → preempt again → ...
        let mut pending_pods = pending_pods;
        pending_pods.sort_by(|a, b| {
            let a_pri = self.get_pod_priority_sync(a, &priority_classes);
            let b_pri = self.get_pod_priority_sync(b, &priority_classes);
            b_pri.cmp(&a_pri) // Descending: highest priority first
        });

        // Re-read all_pods before each scheduling decision. K8s re-evaluates
        // cluster state per-pod. Using stale pod data causes preemption to
        // fail for the second pod because evicted victims are still counted.
        let mut all_pods = all_pods;

        // Schedule each pod with a timeout to prevent one slow pod from
        // blocking all others. K8s processes pods concurrently in the
        // scheduling queue; we process sequentially but with a per-pod timeout.
        for mut pod in pending_pods {
            // Resolve priority in-memory if not explicitly set. Not persisted:
            // the api-server rejects spec mutations on update, and on-the-fly
            // resolution via get_pod_priority_sync (spec.priority, else the live
            // PriorityClass map) is sufficient for every consumer. See the same
            // note in try_schedule_pod.
            if pod.spec.as_ref().and_then(|s| s.priority).is_none() {
                let resolved = self.get_pod_priority_sync(&pod, &priority_classes);
                if resolved != 0 {
                    if let Some(ref mut spec) = pod.spec {
                        spec.priority = Some(resolved);
                    }
                }
            }
            // 5-second timeout per pod — if scheduling takes longer (e.g.,
            // complex preemption calculation), skip and retry next cycle.
            let schedule_result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let (selected, diag) = self
                    .select_node_diag(&pod, &nodes, &all_pods, &priority_classes)
                    .await;
                if let Some(node) = selected {
                    if let Err(e) = self
                        .bind_pod_to_node(pod.clone(), &node.metadata.name)
                        .await
                    {
                        error!("Failed to bind pod to node: {}", e);
                    }
                    return (true, diag);
                }
                (false, diag)
            })
            .await;

            let diagnosis: Diagnosis = match schedule_result {
                Ok((true, _)) => {
                    // Pod bound — re-read all_pods so next pod sees updated state
                    all_pods = self.data.list_pods().await.unwrap_or_default();
                    continue;
                }
                Ok((false, d)) => d, // No node found, try preemption below
                Err(_) => {
                    warn!(
                        "Scheduling timed out for pod {}/{}, will retry",
                        pod.metadata.namespace.as_deref().unwrap_or(""),
                        pod.metadata.name
                    );
                    continue;
                }
            };

            {
                // No suitable node found, try preemption if pod has priority
                if let Some(preemption_result) = self
                    .try_preempt(
                        &pod,
                        &nodes,
                        &all_pods,
                        &priority_classes,
                        &pdbs,
                        &diagnosis,
                    )
                    .await
                {
                    let (node_name, pods_to_evict) = preemption_result;
                    info!(
                        "Preempting {} pods on node {} for high-priority pod {}",
                        pods_to_evict.len(),
                        node_name,
                        pod.metadata.name
                    );

                    // Evict lower-priority pods
                    for victim in &pods_to_evict {
                        if let Err(e) = self.evict_pod(&pod, victim).await {
                            error!(
                                "Failed to evict pod {}/{}: {}",
                                victim.metadata.namespace.as_deref().unwrap_or("default"),
                                victim.metadata.name,
                                e
                            );
                        }
                    }

                    // After evicting victims, try to bind the preemptor immediately.
                    // K8s sets nominatedNodeName and waits for the next cycle, but in
                    // our architecture the RS controller may create replacement pods
                    // that steal the freed resources before the next scheduling cycle.
                    // To avoid this race, re-read cluster state and attempt binding now.
                    all_pods = self.data.list_pods().await.unwrap_or_default();
                    let fresh_nodes: Vec<Node> = self.data.list_nodes().await.unwrap_or_default();

                    // Re-read the pod for fresh state
                    let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                    let pod_key =
                        rusternetes_storage::build_key("pods", Some(pod_ns), &pod.metadata.name);
                    let fresh_pod = self.data.get_pod(pod_ns, &pod.metadata.name).await.ok();

                    if let Some(fresh_pod) = fresh_pod {
                        // Try to schedule directly to the nominated node first
                        if let Some(target_node) =
                            fresh_nodes.iter().find(|n| n.metadata.name == node_name)
                        {
                            let resource_score = self.calculate_resource_score_with_overhead(
                                target_node,
                                &fresh_pod,
                                &all_pods,
                            );
                            if resource_score > 0 {
                                // Resources are free — bind immediately
                                if let Err(e) = self.bind_pod_to_node(fresh_pod, &node_name).await {
                                    error!("Failed to bind preemptor to nominated node: {}", e);
                                    // Fall back to setting nominatedNodeName
                                    let nn = node_name.clone();
                                    self.update_pod_status_with_retry(&pod_key, |p| {
                                        if let Some(ref mut status) = p.status {
                                            status.nominated_node_name = Some(nn.clone());
                                        }
                                    })
                                    .await;
                                } else {
                                    info!(
                                        "Immediately bound preemptor pod {} to node {}",
                                        pod.metadata.name, node_name
                                    );
                                }
                            } else {
                                // Resources not yet free (victims still running) — set nominatedNodeName
                                let nn = node_name.clone();
                                self.update_pod_status_with_retry(&pod_key, |p| {
                                    if let Some(ref mut status) = p.status {
                                        status.nominated_node_name = Some(nn.clone());
                                    }
                                })
                                .await;
                                info!(
                                    "Set nominatedNodeName={} on preempting pod {} (resources not yet free)",
                                    node_name, pod.metadata.name
                                );
                            }
                        } else {
                            // Node not found — set nominatedNodeName anyway
                            let nn = node_name.clone();
                            self.update_pod_status_with_retry(&pod_key, |p| {
                                if let Some(ref mut status) = p.status {
                                    status.nominated_node_name = Some(nn.clone());
                                }
                            })
                            .await;
                        }
                    }

                    // Re-read all_pods after potential binding so next pod sees updated state
                    all_pods = self.data.list_pods().await.unwrap_or_default();
                } else {
                    warn!(
                        "No suitable node found for pod {} (even with preemption)",
                        pod.metadata.name
                    );
                    // Set pod condition to Unschedulable so tests can observe it
                    let pod_ns = pod.metadata.namespace.as_deref().unwrap_or("default");
                    let sched_message = if diagnosis.is_empty() {
                        format!(
                            "0/{} nodes are available: no node matched the scheduling constraints",
                            nodes.len()
                        )
                    } else {
                        fit_error_message(nodes.len(), &diagnosis)
                    };
                    let pod_key =
                        rusternetes_storage::build_key("pods", Some(pod_ns), &pod.metadata.name);
                    let msg = sched_message.clone();
                    self.update_pod_status_with_retry(&pod_key, |p| {
                        let condition = rusternetes_common::resources::PodCondition {
                            condition_type: "PodScheduled".to_string(),
                            status: "False".to_string(),
                            reason: Some("Unschedulable".to_string()),
                            message: Some(msg.clone()),
                            last_probe_time: None,
                            last_transition_time: Some(chrono::Utc::now()),
                            observed_generation: None,
                        };
                        if let Some(ref mut status) = p.status {
                            let conditions = status.conditions.get_or_insert_with(Vec::new);
                            conditions.retain(|c| c.condition_type != "PodScheduled");
                            conditions.push(condition);
                        }
                    })
                    .await;

                    // Emit FailedScheduling via the unified recorder — its
                    // correlator dedups + bumps `count` across scheduling cycles
                    // for a stuck pod (instead of flooding one event per cycle),
                    // matching upstream's `recorder.Eventf(pod, Warning,
                    // "FailedScheduling", …)`.
                    self.emit_pod_event(
                        &pod,
                        EventType::Warning,
                        "FailedScheduling",
                        &sched_message,
                    )
                    .await;
                }
            }
        }

        Ok(())
    }

    /// `select_node` plus the scheduling `Diagnosis` (upstream
    /// `framework.Diagnosis`) used to build the `FailedScheduling` message.
    async fn select_node_diag(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
    ) -> (Option<Node>, Diagnosis) {
        let (picked, _vols, diag) = self
            .select_node_diag_volumes(pod, nodes, all_pods, priority_classes)
            .await;
        (picked, diag)
    }

    /// `select_node_diag` that also returns the scheduling cycle's
    /// [`CycleState`], which Reserve/PreBind/Unreserve consume.
    async fn select_node_diag_volumes(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
    ) -> (Option<Node>, Option<CycleState>, Diagnosis) {
        let mut diag = Diagnosis::default();
        let picked = self
            .select_node_inner(pod, nodes, all_pods, priority_classes, &mut diag)
            .await;
        match picked {
            Some((node, state)) => (Some(node), Some(state), diag),
            None => (None, None, diag),
        }
    }

    /// `RunReservePluginsReserve` -> `RunPreBindPreFlights` ->
    /// `RunPreBindPlugins` -> Bind, in `schedulingCycle`/`bindingCycle` order
    /// (schedule_one.go:211 Reserve, :282 PreBindPreFlights, :327
    /// RunPreBindPlugins, :332 bind). Any failure runs
    /// `RunReservePluginsUnreserve` (:213 and `handleBindingCycleError` :368).
    ///
    /// Deviation: upstream runs the binding cycle in a goroutine
    /// (schedule_one.go:123); here it runs inline in the work-queue worker, so
    /// a pod waiting on its PV controller occupies a worker for up to
    /// BindTimeoutSeconds. Permit is not run (no Permit plugin objects yet).
    async fn reserve_prebind_bind(
        &self,
        pod: Pod,
        node_name: &str,
        mut cycle_state: CycleState,
    ) -> rusternetes_common::Result<()> {
        let profile = self
            .profile_for(&pod)
            .map_err(rusternetes_common::Error::Internal)?;
        let fw = &profile.framework;
        let handle = CycleHandle {
            data: &self.data,
            binder: &self.volume_binder,
            bind_timeout: profile.volume_bind_timeout,
            bind_poll: self.volume_bind_poll,
        };

        // Reserve (schedule_one.go:211); Unreserve on a failure (:213).
        let status = fw
            .run_reserve_plugins_reserve(&cycle_state, &pod, node_name, &handle)
            .await;
        if !status.is_success() {
            fw.run_reserve_plugins_unreserve(&cycle_state, &pod, node_name, &handle)
                .await;
            return Err(rusternetes_common::Error::Internal(status.message()));
        }

        // PreBindPreFlights (:282) and PreBind (:327); Unreserve on a failure
        // (`handleBindingCycleError` :368).
        let status = fw.run_pre_bind_pre_flights(&mut cycle_state, &pod, node_name);
        if status.code() == crate::fwk::Code::Error || status.is_rejected() {
            fw.run_reserve_plugins_unreserve(&cycle_state, &pod, node_name, &handle)
                .await;
            return Err(rusternetes_common::Error::Internal(status.message()));
        }
        let status = fw
            .run_pre_bind_plugins(&cycle_state, &pod, node_name, &handle)
            .await;
        if !status.is_success() {
            fw.run_reserve_plugins_unreserve(&cycle_state, &pod, node_name, &handle)
                .await;
            return Err(rusternetes_common::Error::Internal(status.message()));
        }

        // Bind (schedule_one.go:332)
        if let Err(e) = self.bind_pod_to_node(pod.clone(), node_name).await {
            fw.run_reserve_plugins_unreserve(&cycle_state, &pod, node_name, &handle)
                .await;
            return Err(e);
        }
        Ok(())
    }

    pub async fn select_node_inner(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        _priority_classes: &HashMap<String, PriorityClass>,
        diag: &mut Diagnosis,
    ) -> Option<(Node, CycleState)> {
        // Advanced scheduling algorithm:
        // 1. Filter out unschedulable nodes
        // 2. Check taints and tolerations
        // 3. Check node selectors
        // 4. Check DRA device availability
        // 5. Check node affinity
        // 6. Calculate resource scores
        // 7. Select node with highest score

        // The pod's own framework (`frameworkForPod`): which plugins run at
        // each extension point for this profile (`NewFramework`, #3401).
        let fallback_profile;
        let profile = match self.profile_for(pod) {
            Ok(p) => p,
            Err(_) => {
                fallback_profile = crate::profile::Profile::named(DEFAULT_SCHEDULER_NAME);
                &fallback_profile
            }
        };
        let fw = &profile.framework;
        // `RunPreFilterPlugins` over the PreFilter plugin objects, ahead of
        // the PreFilters still inline below (`findNodesThatFitPod`,
        // schedule_one.go:482-502): a rejection ends the cycle with the
        // status' message, any other failure is a scheduling error, and the
        // plugins that restricted the nodes are recorded as unschedulable
        // plugins either way (`diagnosis.UnschedulablePlugins`).
        let mut cycle_state = CycleState::new();
        let handle = CycleHandle {
            data: &self.data,
            binder: &self.volume_binder,
            bind_timeout: profile.volume_bind_timeout,
            bind_poll: self.volume_bind_poll,
        };
        let (pre_filter_result, pre_filter_status, plugins_with_nodes) = fw
            .run_pre_filter_plugins(&mut cycle_state, pod, nodes, all_pods, &handle)
            .await;
        diag.unschedulable_plugins
            .extend(plugins_with_nodes.iter().cloned());
        if !pre_filter_status.is_success() {
            if !pre_filter_status.is_rejected() {
                diag.scheduling_error = Some(pre_filter_status.message());
                return None;
            }
            let msg = pre_filter_status.message();
            debug!(
                "Status after running PreFilter plugins for pod {}/{}: {}",
                pod.metadata.namespace.as_deref().unwrap_or("default"),
                pod.metadata.name,
                msg
            );
            diag.pre_filter_msg = msg;
            // `SetAbsentNodesStatus` (schedule_one.go:495): every node gets the
            // PreFilter status so preemption handles them alike.
            diag.absent_nodes_code = Some(pre_filter_status.code());
            diag.add_plugin_status(pre_filter_status.plugin());
            return None;
        }
        // `findNodesThatPassFilters` (schedule_one.go:624-708): visit the nodes
        // starting where the previous cycle left off (`nextStartNodeIndex`,
        // :544-548) and stop as soon as `numFeasibleNodesToFind` nodes pass
        // every filter.
        let num_all_nodes = nodes.len();
        if num_all_nodes == 0 {
            return None;
        }
        // schedule_one.go:634-636: without scoring (and without extender
        // filters, which Rusternetes has none of) one feasible node is enough.
        let num_nodes_to_find = if fw.has_score_plugins() {
            num_feasible_nodes_to_find(
                fw.percentage_of_nodes_to_score,
                self.profiles.percentage_of_nodes_to_score,
                num_all_nodes as i32,
            ) as usize
        } else {
            1
        };
        let start_index = self.next_start_node_index.load(Ordering::Relaxed);
        // `addGENominatedPods` (framework.go): the pods nominated to a node by
        // preemption, of greater or equal priority than this pod, that are
        // not yet bound there. The Filter plugins run with them added.
        let nominated_pods_for_node = |node_name: &str| -> Vec<&Pod> {
            crate::default_preemption::nominated_pods_for_node(pod, all_pods, node_name)
        };

        let mut feasible: Vec<NodeEval> = Vec::new();
        // `findNodesThatFitPod` (schedule_one.go:518-530): "NominatedNodeName"
        // set by a previous cycle's preemption is tried first, alone
        // (`evaluateNominatedNode`, :575-599); if it passes every filter it is
        // the only candidate and the search below (and its
        // `nextStartNodeIndex` update, :547-548) is skipped. A node missing
        // from the snapshot is an error that is only logged (:522-523).
        let nominated_idx = pod
            .status
            .as_ref()
            .and_then(|s| s.nominated_node_name.as_deref())
            .filter(|n| !n.is_empty())
            .and_then(|n| nodes.iter().position(|x| x.metadata.name == n));
        let mut nominated_failed = false;
        let mut filter_error: Option<String> = None;
        // (node indexes to visit, stop after this many feasible, is the full search)
        let mut passes: Vec<(Vec<usize>, usize, bool)> = Vec::new();
        if let Some(idx) = nominated_idx {
            passes.push((vec![idx], 1, false));
        }
        // `findNodesThatFitPod` (schedule_one.go:549-562): a PreFilterResult
        // restricts the search to the named nodes that exist in the
        // snapshot; the others are absent, UnschedulableAndUnresolvable.
        let narrowed: Option<&std::collections::HashSet<String>> = pre_filter_result
            .as_ref()
            .and_then(|r| r.node_names.as_ref());
        passes.push((
            (0..num_all_nodes)
                .map(|i| (start_index + i) % num_all_nodes)
                .filter(|&ix| narrowed.is_none_or(|n| n.contains(&nodes[ix].metadata.name)))
                .collect(),
            num_nodes_to_find,
            true,
        ));
        for (order, to_find, full_search) in passes {
            feasible.clear();
            crate::in_tree::clear_pod_volumes_by_node(&cycle_state);
            let mut processed_nodes = 0usize;
            for &ix in &order {
                let node = &nodes[ix];
                processed_nodes += 1;
                let eval = async {
                    // The Filter plugins that are objects (`RunFilterPlugins`,
                    // framework.go:1033): NodeUnschedulable, NodeName, TaintToleration, NodeAffinity, NodePorts. They run first,
                    // as in the default plugin order, ahead of the filters
                    // still inline below.
                    let status = fw
                        .run_filter_plugins_with_nominated_pods(
                            &cycle_state,
                            pod,
                            node,
                            all_pods,
                            nodes,
                            nominated_pods_for_node(&node.metadata.name),
                        )
                        .await;
                    if !status.is_success() {
                        if status.is_rejected() {
                            diag.add_node_reasons(status.reasons());
                            diag.add_plugin_status(status.plugin());
                            diag.set_node_code(&node.metadata.name, status.code());
                        } else {
                            // `findNodesThatPassFilters` (schedule_one.go:664-668):
                            // an Error status aborts the whole search.
                            filter_error = Some(status.message());
                        }
                        return None;
                    }
                    // InterPodAffinity and PodTopologySpread Filters are
                    // plugin objects (run above).
                    Some(NodeEval { node })
                }
                .await;
                if let Some(e) = filter_error.take() {
                    // `errCh.ReceiveError()` (schedule_one.go:703-706): the
                    // error replaces the whole result; no feasible nodes.
                    diag.scheduling_error = Some(e);
                    return None;
                }
                if let Some(eval) = eval {
                    feasible.push(eval);
                    if feasible.len() >= to_find {
                        break;
                    }
                }
            }
            if !full_search {
                if feasible.is_empty() {
                    nominated_failed = true;
                    continue;
                }
                break;
            }
            // schedule_one.go:545-548: `processedNodes` is the feasible nodes
            // plus every node with a recorded status, and the nominated node's
            // failed status stays in `diagnosis.NodeToStatus`; always advance,
            // so every node gets a chance to be searched across cycles.
            if let (true, Some(idx)) = (nominated_failed, nominated_idx) {
                if !order.iter().take(processed_nodes).any(|&ix| ix == idx) {
                    processed_nodes += 1;
                }
            }
            self.next_start_node_index.store(
                (start_index + processed_nodes) % num_all_nodes,
                Ordering::Relaxed,
            );
        }

        if feasible.is_empty() {
            if let Some(narrowed) = narrowed {
                // `SetAbsentNodesStatus` (schedule_one.go:561): the nodes the
                // PreFilterResult left out, other than a nominated node whose
                // own failure was recorded above.
                let mut plugins: Vec<&str> =
                    plugins_with_nodes.iter().map(String::as_str).collect();
                plugins.sort();
                let reason = format!("node(s) didn't satisfy plugin(s) [{}]", plugins.join(" "));
                let absent = nodes.iter().enumerate().filter(|(ix, n)| {
                    !narrowed.contains(&n.metadata.name)
                        && !(nominated_failed && nominated_idx == Some(*ix))
                });
                diag.add_node_reasons(absent.map(|_| reason.clone()));
            }
            debug!("No nodes satisfy the pod's constraints");
            return None;
        }

        // `schedulePod` (schedule_one.go:443-451): "When only one node after
        // predicate, just use it" - no Score plugin runs.
        let chosen = if feasible.len() == 1 {
            0
        } else {
            // `prioritizeNodes` (schedule_one.go:791-824) -> `RunScorePlugins`
            // (framework.go:1273-1382), then `newSortedNodeScores(..).Pop()`
            // (schedule_one.go:465-466).
            let n = feasible.len();
            let totals = if !fw.has_score_plugins() {
                // prioritizeNodes :802-811: every node scores 1.
                vec![1i64; n]
            } else {
                // `RunPreScorePlugins` and `RunScorePlugins` over the plugin
                // objects (TaintToleration, NodeAffinity); the plugins below
                // still score inline.
                let feasible_nodes: Vec<&Node> = feasible.iter().map(|e| e.node).collect();
                let status = fw
                    .run_pre_score_plugins(
                        &mut cycle_state,
                        pod,
                        &feasible_nodes,
                        all_pods,
                        nodes,
                        &handle,
                    )
                    .await;
                if !status.is_success() {
                    diag.scheduling_error = Some(status.message());
                    return None;
                }
                let ran: Vec<crate::score::PluginScores> =
                    match fw.run_score_plugins(&cycle_state, pod, &feasible_nodes, all_pods, nodes)
                    {
                        Ok(r) => r,
                        Err(e) => {
                            diag.scheduling_error = Some(e);
                            return None;
                        }
                    };
                match crate::score::total_scores(&ran, n) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("applying score defaultWeights on Score plugins: {}", e);
                        return None;
                    }
                }
            };
            let idx = crate::score::select_host(&totals)?;
            debug!(
                "Selected node {} with score {} for pod {}",
                feasible[idx].node.metadata.name, totals[idx], pod.metadata.name
            );
            idx
        };
        let best_owned = feasible[chosen].node.metadata.name.clone();
        let best_node_name = &best_owned;

        // The cycle state (the VolumeBinding Filter's per-node result among
        // it) is carried to Reserve/PreBind/Unreserve.
        nodes
            .iter()
            .find(|n| &n.metadata.name == best_node_name)
            .cloned()
            .map(|n| (n, cycle_state))
    }

    async fn bind_pod_to_node(
        &self,
        mut pod: Pod,
        node_name: &str,
    ) -> rusternetes_common::Result<()> {
        // K8s scheduler validates node_name is non-empty before binding.
        // An empty node_name means select_node returned a node with no name,
        // which should never happen but would leave the pod stuck Pending.
        if node_name.is_empty() {
            return Err(rusternetes_common::Error::Internal(
                "cannot bind pod to empty node name".to_string(),
            ));
        }

        debug!(
            "Binding pod {}/{} to node {}",
            pod.metadata
                .namespace
                .as_ref()
                .unwrap_or(&"default".to_string()),
            pod.metadata.name,
            node_name
        );

        // Update pod spec with node name
        if let Some(ref mut spec) = pod.spec {
            spec.node_name = Some(node_name.to_string());
        }

        // Update pod status with PodScheduled condition
        let scheduled_condition = rusternetes_common::resources::PodCondition {
            condition_type: "PodScheduled".to_string(),
            status: "True".to_string(),
            last_probe_time: None,
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("Scheduled".to_string()),
            message: Some(format!("Successfully assigned to {}", node_name)),
            observed_generation: None,
        };

        if let Some(ref mut status) = pod.status {
            status.phase = Some(Phase::Pending);
            status.message = Some("Pod scheduled".to_string());
            // Add or update PodScheduled condition
            let conditions = status.conditions.get_or_insert_with(Vec::new);
            if let Some(existing) = conditions
                .iter_mut()
                .find(|c| c.condition_type == "PodScheduled")
            {
                *existing = scheduled_condition;
            } else {
                conditions.push(scheduled_condition);
            }
        } else {
            pod.status = Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Pending),
                message: Some("Pod scheduled".to_string()),
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: Some(vec![scheduled_condition]),
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                host_i_ps: None,
                pod_i_ps: None,
                nominated_node_name: None,
                qos_class: None,
                start_time: None,
                ..Default::default()
            });
        }

        // Bind: storage mode writes the fully-mutated pod (spec.nodeName +
        // PodScheduled condition) in one update with a re-GET-on-conflict retry;
        // API mode POSTs the binding subresource (the only way to set the
        // immutable spec.nodeName) and PUTs the PodScheduled condition via the
        // status subresource. See `DataPlane::bind`.
        let ns = pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        match self.data.bind(&ns, &pod, node_name).await {
            Ok(()) => info!("Successfully bound pod to node {}", node_name),
            Err(e) => return Err(e),
        }
        let bound_pod = pod;

        // Emit the Scheduled event once on successful bind via the unified
        // recorder, regardless of which attempt succeeded — upstream's
        // default-scheduler always records `recorder.Eventf(pod, Normal,
        // "Scheduled", "Successfully assigned %v/%v to %v")` after bind. The
        // recorder routes it through the correlator and stores it at the stable
        // (object.reason.uid) key so it deduplicates against any other source.
        let ns = bound_pod.metadata.namespace.as_deref().unwrap_or("default");
        let message = format!(
            "Successfully assigned {}/{} to {}",
            ns, bound_pod.metadata.name, node_name
        );
        self.emit_pod_event(&bound_pod, EventType::Normal, "Scheduled", &message)
            .await;
        Ok(())
    }

    /// Try to preempt lower-priority pods to make room for a high-priority pod
    /// Returns Some((node_name, pods_to_evict)) if preemption is possible, None otherwise
    pub async fn try_preempt(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
        pdbs: &[rusternetes_common::resources::PodDisruptionBudget],
        diagnosis: &Diagnosis,
    ) -> Option<(String, Vec<Pod>)> {
        // `RunPostFilterPlugins` runs only the pod's profile's PostFilter
        // plugins; DefaultPreemption is the one that evicts victims
        // (schedule_one.go:172 `HasPostFilterPlugins`).
        let runs_default_preemption = self.profile_for(pod).map_or(true, |p| {
            p.framework
                .has(ExtensionPoint::PostFilter, names::DEFAULT_PREEMPTION)
        });
        if !runs_default_preemption {
            debug!(
                "Pod {} profile does not run DefaultPreemption, skipping preemption",
                pod.metadata.name
            );
            return None;
        }
        // If the pod's preemptionPolicy is "Never", skip preemption entirely.
        // Fall back to the PriorityClass's policy when the pod spec doesn't
        // carry one (mirrors the spec.priority backstop in the schedule
        // loops — admission normally copies the class policy onto the pod,
        // but pods written directly to storage may miss it).
        let preemption_policy = pod
            .spec
            .as_ref()
            .and_then(|s| s.preemption_policy.as_deref())
            .or_else(|| {
                pod.spec
                    .as_ref()
                    .and_then(|s| s.priority_class_name.as_deref())
                    .and_then(|name| priority_classes.get(name))
                    .and_then(|pc| pc.preemption_policy.as_deref())
            })
            .unwrap_or("PreemptLowerPriority");
        if preemption_policy == "Never" {
            debug!(
                "Pod {} has preemptionPolicy=Never, skipping preemption",
                pod.metadata.name
            );
            return None;
        }

        // A pod that already preempted must wait for the room it made instead of
        // preempting a second set of victims somewhere else. Victims terminate
        // gracefully, so for up to their grace period the node still accounts for
        // them and the preemptor still doesn't fit; retrying without this gate
        // killed a medium-priority pod on the *other* node and broke
        // [sig-scheduling] SchedulerPreemption (#1130).
        // Upstream: PodEligibleToPreemptOthers
        // (pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:317-341).
        // `nominatedNodeStatus := m.Get(pod.Status.NominatedNodeName)`
        // (preemption.go:283): the nominated node's status in the failed cycle.
        let nominated_node_code = diagnosis.node_code(
            pod.status
                .as_ref()
                .and_then(|s| s.nominated_node_name.as_deref())
                .unwrap_or_default(),
        );
        if !crate::advanced::pod_eligible_to_preempt_others(pod, all_pods, nominated_node_code) {
            debug!(
                "Pod {} already nominated to {:?} with a victim still terminating; not preempting again",
                pod.metadata.name,
                pod.status
                    .as_ref()
                    .and_then(|s| s.nominated_node_name.as_deref())
            );
            return None;
        }

        // Collect EVERY node preemption could use, then score them. Returning
        // the first feasible node made the victim depend on node iteration
        // order: with a low-priority victim on node-1 and a medium-priority one
        // on node-2 we preempted whichever came first, so
        // [sig-scheduling] SchedulerPreemption passed or failed by luck (#1130).
        // Upstream: pickOneNodeForPreemption
        // (pkg/scheduler/framework/preemption/preemption.go:651-730).
        // `findCandidates` (preemption.go:343-365): the nodes whose failure
        // (Unschedulable, not UnschedulableAndUnresolvable) removing pods may fix.
        let profile = self.profile_for(pod).ok();
        let potential_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| diagnosis.node_code(&n.metadata.name) == crate::fwk::Code::Unschedulable)
            .collect();

        // `GetOffsetAndNumCandidates` (default_preemption.go:191-193) then
        // `DryRunPreemption` (preemption.go:768-821): dry-run the potential
        // nodes from a random offset and stop once `candidatesNum` candidates
        // are found (at least one without PDB violations).
        let num_potential = potential_nodes.len();
        let candidates_num = profile.map_or(100, |p| {
            calculate_num_candidates(
                num_potential as i32,
                p.min_candidate_nodes_percentage,
                p.min_candidate_nodes_absolute,
            )
        }) as usize;
        let offset = if num_potential == 0 {
            0
        } else {
            match self.preemption_offset_override.load(Ordering::Relaxed) {
                usize::MAX => random_offset(num_potential),
                fixed => fixed % num_potential,
            }
        };
        let fallback_profile;
        let profile_ref = match profile {
            Some(p) => p,
            None => {
                fallback_profile = crate::profile::Profile::named(DEFAULT_SCHEDULER_NAME);
                &fallback_profile
            }
        };
        let fw = &profile_ref.framework;
        let handle = CycleHandle {
            data: &self.data,
            binder: &self.volume_binder,
            bind_timeout: profile_ref.volume_bind_timeout,
            bind_poll: self.volume_bind_poll,
        };
        let mut non_violating: Vec<crate::advanced::PreemptionCandidate> = Vec::new();
        let mut violating: Vec<crate::advanced::PreemptionCandidate> = Vec::new();
        for i in 0..num_potential {
            let node = potential_nodes[(offset + i) % num_potential];
            // `SelectVictimsOnNode` over the Filter plugin objects
            // (default_preemption.go:207-307), with the PDB-violating victims
            // reprieved first.
            let selected = crate::default_preemption::select_victims_on_node(
                fw, &handle, pod, node, all_pods, nodes, pdbs,
            )
            .await;
            if let Ok(crate::default_preemption::NodeVictims {
                victims: pods_to_evict,
                num_pdb_violations,
            }) = selected
            {
                if pods_to_evict.is_empty() {
                    continue;
                }
                let c = crate::advanced::PreemptionCandidate {
                    num_pdb_violations,
                    node_name: node.metadata.name.clone(),
                    victims: pods_to_evict,
                };
                // candidateList.add: each list holds at most `candidatesNum`.
                let list = if num_pdb_violations == 0 {
                    &mut non_violating
                } else {
                    &mut violating
                };
                if list.len() < candidates_num {
                    list.push(c);
                }
                if !non_violating.is_empty()
                    && non_violating.len() + violating.len() >= candidates_num
                {
                    break;
                }
            }
        }
        let mut candidates = non_violating;
        candidates.extend(violating);

        let chosen = crate::advanced::pick_one_node_for_preemption(&candidates)?;
        candidates
            .into_iter()
            .find(|c| c.node_name == chosen)
            .map(|c| (c.node_name, c.victims))
    }

    /// Evict a pod by setting its deletionTimestamp (graceful delete).
    /// The kubelet will detect the deletionTimestamp and handle graceful shutdown.
    ///
    /// The victim is identified by namespace AND name, like upstream
    /// `util.DeletePod` (`cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, ...)`,
    /// pkg/scheduler/util/utils.go:137-138). Matching on name alone evicted a
    /// same-named pod in every other namespace too (#2477).
    pub async fn evict_pod(&self, preemptor: &Pod, victim: &Pod) -> rusternetes_common::Result<()> {
        let pod_name = victim.metadata.name.as_str();
        let victim_ns = victim.metadata.namespace.as_deref().unwrap_or("default");
        let all_pods: Vec<Pod> = self.data.list_pods().await?;

        for mut pod in all_pods {
            let pod_ns_matches =
                pod.metadata.namespace.as_deref().unwrap_or("default") == victim_ns;
            if pod.metadata.name == pod_name && pod_ns_matches {
                let pod_ns = victim_ns.to_string();

                // Set deletionTimestamp and add DisruptionTarget condition
                if pod.metadata.deletion_timestamp.is_none() {
                    pod.metadata.deletion_timestamp = Some(Utc::now());
                    // K8s uses the pod's termination grace period, not 0.
                    // See: pkg/scheduler/framework/preemption/preemption.go — DeletePod
                    pod.metadata.deletion_grace_period_seconds = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.termination_grace_period_seconds)
                        .or(Some(30));
                    // Update the status phase to indicate termination
                    if let Some(ref mut status) = pod.status {
                        status.phase = Some(rusternetes_common::types::Phase::Failed);
                        status.reason = Some("Preempted".to_string());
                        status.message =
                            Some("Pod was preempted by a higher-priority pod".to_string());
                        // Add DisruptionTarget condition (K8s conformance requirement)
                        let disruption_condition =
                            crate::data_plane::preemption_disruption_condition(
                                crate::profile::pod_scheduler_name(preemptor),
                            );
                        let conditions = status.conditions.get_or_insert_with(Vec::new);
                        conditions.push(disruption_condition);
                    }
                    // Storage mode persists the whole pod (kubelet sees the
                    // deletionTimestamp); API mode stamps DisruptionTarget via
                    // /status then issues a real DELETE (the api-server rejects
                    // a PUT that sets deletionTimestamp). See
                    // DataPlane::evict_pod_for_preemption.
                    let grace = pod.metadata.deletion_grace_period_seconds.unwrap_or(30);
                    self.data
                        .evict_pod_for_preemption(&pod_ns, pod_name, &pod, grace)
                        .await?;
                    info!(
                        "Evicted pod {} for preemption (DisruptionTarget + termination, grace={}s)",
                        pod_name, grace
                    );
                }
                return Ok(());
            }
        }

        warn!("Pod {}/{} not found for eviction", victim_ns, pod_name);
        Ok(())
    }

    /// Load all PriorityClasses from storage into a HashMap for fast lookup
    /// List PodDisruptionBudgets for preemption victim selection.
    ///
    /// Never fatal: a listing failure degrades preemption to budget-blind
    /// (which is what it always did before #1797) rather than stopping the
    /// scheduling cycle outright.
    async fn load_pod_disruption_budgets(
        &self,
    ) -> Vec<rusternetes_common::resources::PodDisruptionBudget> {
        match self.data.list_pod_disruption_budgets().await {
            Ok(pdbs) => pdbs,
            Err(e) => {
                warn!(
                    "Failed to list PodDisruptionBudgets ({}); preemption will not honour budgets this cycle",
                    e
                );
                Vec::new()
            }
        }
    }

    async fn load_priority_classes(
        &self,
    ) -> rusternetes_common::Result<HashMap<String, PriorityClass>> {
        let priority_classes: Vec<PriorityClass> = self.data.list_priority_classes().await?;

        let mut map = HashMap::new();
        for pc in priority_classes {
            map.insert(pc.metadata.name.clone(), pc);
        }

        Ok(map)
    }

    /// Calculate resource score with pod overhead
    /// Pod overhead represents additional resources required beyond container requests
    fn calculate_resource_score_with_overhead(
        &self,
        node: &Node,
        pod: &Pod,
        all_pods: &[Pod],
    ) -> i32 {
        use crate::advanced::calculate_resource_score_with_pods;

        // Get base resource score accounting for existing pod usage
        let base_score = calculate_resource_score_with_pods(node, pod, all_pods);

        // If no overhead specified, return base score
        let overhead = match &pod.spec {
            Some(spec) => match &spec.overhead {
                Some(o) => o,
                None => return base_score,
            },
            None => return base_score,
        };

        // Parse overhead resources
        let mut cpu_overhead = 0i64;
        let mut memory_overhead = 0i64;

        if let Some(cpu) = overhead.get("cpu") {
            cpu_overhead = parse_resource_quantity(cpu, "cpu");
        }
        if let Some(memory) = overhead.get("memory") {
            memory_overhead = parse_resource_quantity(memory, "memory");
        }

        // Get node allocatable resources
        let allocatable = match &node.status {
            Some(status) => match &status.allocatable {
                Some(a) => a,
                None => return base_score,
            },
            None => return base_score,
        };

        let available_cpu = allocatable
            .get("cpu")
            .map(|s| parse_resource_quantity(s, "cpu"))
            .unwrap_or(0);
        let available_memory = allocatable
            .get("memory")
            .map(|s| parse_resource_quantity(s, "memory"))
            .unwrap_or(0);

        // Check if overhead alone would prevent scheduling
        if cpu_overhead > available_cpu || memory_overhead > available_memory {
            return 0; // Can't schedule
        }

        // Reduce the base score proportionally to overhead impact
        let cpu_overhead_ratio = if available_cpu > 0 {
            (cpu_overhead * 100 / available_cpu) as i32
        } else {
            0
        };
        let memory_overhead_ratio = if available_memory > 0 {
            (memory_overhead * 100 / available_memory) as i32
        } else {
            0
        };

        let overhead_penalty = (cpu_overhead_ratio + memory_overhead_ratio) / 2;

        // Return score minus overhead penalty (but not less than 0)
        (base_score - overhead_penalty).max(0)
    }

    /// Stamp `spec.priority` (in-memory) on every pod that lacks it, resolving
    /// from `priorityClassName` via the PriorityClass map. Downstream preemption
    /// and the nominated-pod space reservation read `spec.priority` directly, so
    /// controller-created pods (which never get it stamped by admission) must be
    /// resolved here or they all look like priority 0. Not persisted.
    fn stamp_priorities(
        &self,
        pods: &mut [Pod],
        priority_classes: &HashMap<String, PriorityClass>,
    ) {
        for pod in pods.iter_mut() {
            if pod.spec.as_ref().and_then(|s| s.priority).is_some() {
                continue;
            }
            let resolved = self.get_pod_priority_sync(pod, priority_classes);
            if resolved != 0 {
                if let Some(spec) = pod.spec.as_mut() {
                    spec.priority = Some(resolved);
                }
            }
        }
    }

    /// Get the priority value for a pod (synchronous version using pre-loaded PriorityClasses)
    /// If pod.spec.priority is set, use it directly
    /// Otherwise, look up the PriorityClass specified by pod.spec.priorityClassName
    /// If neither is set, return 0 (default priority)
    fn get_pod_priority_sync(
        &self,
        pod: &Pod,
        priority_classes: &HashMap<String, PriorityClass>,
    ) -> i32 {
        let spec = match pod.spec.as_ref() {
            Some(s) => s,
            None => return 0,
        };

        // If priority is explicitly set, use it
        if let Some(priority) = spec.priority {
            return priority;
        }

        // If priorityClassName is set, look it up
        if let Some(class_name) = &spec.priority_class_name {
            if let Some(priority_class) = priority_classes.get(class_name) {
                debug!(
                    "Resolved priority {} from PriorityClass {} for pod {}",
                    priority_class.value, class_name, pod.metadata.name
                );
                return priority_class.value;
            } else {
                warn!(
                    "PriorityClass {} not found for pod {}, using default priority 0",
                    class_name, pod.metadata.name
                );
                return 0;
            }
        }

        // No priority specified
        0
    }

    // DRA (Dynamic Resource Allocation) Integration Methods
}

/// Split a `/registry/pods/{ns}/{name}` storage key into `(ns, name)`. Tolerates
/// a missing namespace segment (defaults to `default`). The scheduler builds
/// these keys via `build_key("pods", Some(ns), name)`, so the format is stable.
/// `minFeasibleNodesToFind` (schedule_one.go:52-56): below this many nodes all
/// of them are examined.
const MIN_FEASIBLE_NODES_TO_FIND: i32 = 100;
/// `minFeasibleNodesPercentageToFind` (schedule_one.go:57-61).
const MIN_FEASIBLE_NODES_PERCENTAGE_TO_FIND: i32 = 5;

/// `Scheduler.numFeasibleNodesToFind` (schedule_one.go:710-738): the number of
/// feasible nodes after which the filter search stops. `profile_percentage` is
/// the profile's `percentageOfNodesToScore`, which overrides the global
/// `global_percentage` when set.
pub fn num_feasible_nodes_to_find(
    profile_percentage: Option<i32>,
    global_percentage: i32,
    num_all_nodes: i32,
) -> i32 {
    if num_all_nodes < MIN_FEASIBLE_NODES_TO_FIND {
        return num_all_nodes;
    }
    // Use the profile percentageOfNodesToScore if it's set; otherwise the
    // global one.
    let mut percentage = profile_percentage.unwrap_or(global_percentage);
    if percentage == 0 {
        percentage = 50 - num_all_nodes / 125;
        if percentage < MIN_FEASIBLE_NODES_PERCENTAGE_TO_FIND {
            percentage = MIN_FEASIBLE_NODES_PERCENTAGE_TO_FIND;
        }
    }
    let num_nodes = num_all_nodes * percentage / 100;
    if num_nodes < MIN_FEASIBLE_NODES_TO_FIND {
        return MIN_FEASIBLE_NODES_TO_FIND;
    }
    num_nodes
}

/// `calculateNumCandidates` (default_preemption.go:170-183): how many
/// preemption candidates the dry run must find, from
/// `minCandidateNodesPercentage` / `minCandidateNodesAbsolute`; never more than
/// `num_nodes`.
pub fn calculate_num_candidates(num_nodes: i32, min_percentage: i32, min_absolute: i32) -> i32 {
    let mut n = num_nodes * min_percentage / 100;
    if n < min_absolute {
        n = min_absolute;
    }
    if n > num_nodes {
        n = num_nodes;
    }
    n
}

/// `getOffsetRand` (default_preemption.go:185-189): a random offset in
/// `0..num_nodes` (`rand.Int31n`).
fn random_offset(num_nodes: usize) -> usize {
    use std::hash::BuildHasher;
    (std::collections::hash_map::RandomState::new().hash_one(0u8) as usize) % num_nodes
}

/// A node that passed every filter, with the per-plugin score inputs computed
/// while filtering.
struct NodeEval<'a> {
    node: &'a Node,
}

fn parse_pod_storage_key(pod_key: &str) -> (&str, &str) {
    let rest = pod_key
        .strip_prefix("/registry/pods/")
        .unwrap_or(pod_key)
        .trim_end_matches('/');
    match rest.split_once('/') {
        Some((ns, name)) => (ns, name),
        None => ("default", rest),
    }
}

/// The scheduler's [`crate::fwk::Handle`]: the data plane's listers, and the
/// binder whose assume caches the volume listers feed.
struct CycleHandle<'a, S: Storage + Send + Sync + 'static> {
    data: &'a DataPlane<S>,
    binder: &'a std::sync::Mutex<crate::volume_assume::VolumeBinder>,
    /// `VolumeBindingArgs.BindTimeoutSeconds` and the poll interval of
    /// `BindPodVolumes`.
    bind_timeout: Duration,
    bind_poll: Duration,
}

impl<S: Storage + Send + Sync + 'static> CycleHandle<'_, S> {
    /// The binder's view of the cluster for a pod of namespace `ns`.
    fn binder_env(&self, pod: &Pod) -> crate::volume_bind::DataPlaneBinderEnv<'_, S> {
        crate::volume_bind::DataPlaneBinderEnv {
            binder: self.binder,
            data: self.data,
            namespace: pod
                .metadata
                .namespace
                .clone()
                .unwrap_or_else(|| "default".into()),
        }
    }
}

#[async_trait::async_trait]
impl<S: Storage + Send + Sync + 'static> crate::fwk::Handle for CycleHandle<'_, S> {
    async fn node_volume_limits_snapshot(
        &self,
    ) -> Result<crate::node_volume_limits::NodeVolumeLimitsSnapshot, String> {
        self.data
            .list_node_volume_limits_snapshot()
            .await
            .map_err(|e| e.to_string())
    }

    async fn resource_claim(
        &self,
        ns: &str,
        name: &str,
    ) -> Result<rusternetes_common::resources::ResourceClaim, String> {
        self.data
            .get_resource_claim(ns, name)
            .await
            .map_err(|e| e.to_string())
    }

    async fn default_selector_inputs(
        &self,
        pod: &Pod,
    ) -> Result<crate::pod_topology_spread_score::DefaultSelectorInputs, String> {
        crate::pod_topology_spread_score::read_default_selector_inputs(self.data, pod)
            .await
            .map_err(|e| e.to_string())
    }

    fn volume_reserve(
        &self,
        state: &mut crate::volume_bind::VolumeBindingState,
        pod: &Pod,
        node_name: &str,
    ) -> Result<(), String> {
        let mut b = self.binder.lock().unwrap_or_else(|e| e.into_inner());
        crate::volume_bind::reserve(&mut b, state, pod, node_name)
    }

    async fn volume_pre_bind(
        &self,
        state: &mut crate::volume_bind::VolumeBindingState,
        pod: &Pod,
        node_name: &str,
    ) -> Result<(), String> {
        let env = self.binder_env(pod);
        crate::volume_bind::pre_bind(
            &env,
            state,
            pod,
            node_name,
            self.bind_timeout,
            self.bind_poll,
        )
        .await
    }

    fn volume_unreserve(
        &self,
        state: &crate::volume_bind::VolumeBindingState,
        pod: &Pod,
        node_name: &str,
    ) {
        crate::volume_bind::unreserve(&self.binder_env(pod), state, node_name);
    }

    async fn list_namespaces(
        &self,
    ) -> Result<Vec<rusternetes_common::resources::Namespace>, String> {
        self.data.list_namespaces().await.map_err(|e| e.to_string())
    }

    async fn resource_slices(
        &self,
    ) -> Result<Vec<rusternetes_common::resources::ResourceSlice>, String> {
        self.data
            .list_resource_slices()
            .await
            .map_err(|e| e.to_string())
    }

    async fn volume_binding_snapshot(
        &self,
        ns: &str,
    ) -> Result<volume_binding::VolumeSnapshot, String> {
        let snap = self
            .data
            .list_volume_snapshot(ns)
            .await
            .map_err(|e| e.to_string())?;
        // Feed the assume caches (the informer feed upstream).
        self.binder
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sync_namespace(ns, &snap);
        Ok(snap)
    }
}
