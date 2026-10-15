use chrono::Utc;
use rusternetes_common::{
    resources::{EventSource, EventType, Node, ObjectReference, Pod, PriorityClass},
    types::Phase,
};
use rusternetes_storage::{
    build_key, build_prefix, extract_key, EventRecorder, Storage, StorageBackend, WorkQueue,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

use crate::advanced::{
    check_host_port_conflicts, check_node_affinity, check_pod_affinity, check_pod_anti_affinity,
    check_taints_tolerations, check_topology_spread_constraints, parse_resource_quantity,
    NodeScore,
};
use crate::data_plane::{ApiBackend, DataPlane};
use crate::diagnosis::{fit_error_message, Diagnosis};
use crate::framework_runtime::{names, ExtensionPoint};
use crate::profile::DEFAULT_SCHEDULER_NAME;
use crate::volume_binding::{self, PreFilterOutcome};

pub struct Scheduler<S: Storage + Send + Sync + 'static = StorageBackend> {
    /// All reads/writes flow through the data plane — either storage directly
    /// (all-in-one) or the api-server over HTTP (in-cluster static pod). See
    /// `data_plane.rs` for the classification of the original 22 storage sites.
    data: DataPlane<S>,
    interval: Duration,
    /// Scheduling profiles by scheduler name (`Scheduler.Profiles`,
    /// pkg/scheduler/scheduler.go). A single `default-scheduler` profile unless
    /// the configuration lists more.
    pub(crate) profiles: crate::profile::Map,
    /// Unified event recorder for STORAGE mode — the scheduler is the source of
    /// truth for the `Scheduled` / `FailedScheduling` events, routed through the
    /// shared `EventCorrelator` (dedup/count/series + spam-filter) like
    /// upstream's `recorder.Eventf` after bind. `None` in API mode, where events
    /// POST through the data plane's client recorder instead.
    recorder: Option<EventRecorder<S>>,
    /// In-memory nominator: pod key (`ns/name`) → node reserved by preemption.
    /// Set synchronously the instant the scheduler decides to preempt, BEFORE
    /// the async `nominatedNodeName` `/status` write propagates back through the
    /// pods informer. Overlaid onto the pod list each scheduling cycle so the
    /// nominated-pod space reservation (advanced.rs) sees the reservation
    /// immediately — without it, a lower-priority pod fills the space the
    /// preemptor just freed before the informer catches up, and preemption
    /// live-locks. Mirrors upstream's in-memory `SchedulingQueue` nominator.
    nominations: Arc<std::sync::Mutex<HashMap<String, String>>>,
    /// Long-lived VolumeBinding assume caches (`volumeBinder.pvCache` /
    /// `pvcCache`), refreshed from the volume listers every cycle.
    volume_binder: Arc<std::sync::Mutex<crate::volume_assume::VolumeBinder>>,
    /// Poll interval of `BindPodVolumes` (1s upstream).
    pub(crate) volume_bind_poll: Duration,
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
            nominations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            volume_binder: Arc::default(),
            volume_bind_poll: crate::volume_bind::BIND_POLL_INTERVAL,
        }
    }
}

/// A map holding one defaulted profile named `scheduler_name`.
#[allow(dead_code)] // test/library helper behind `new_with_name`
fn single_profile(scheduler_name: &str) -> crate::profile::Map {
    crate::profile::Map::new_map(vec![crate::profile::Profile::named(scheduler_name)])
        .expect("a single named, defaulted profile is always valid")
}

impl<S: Storage + Send + Sync + 'static> Scheduler<S> {
    #[allow(dead_code)] // library/test API; the binary builds profiles from config
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
            nominations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            volume_binder: Arc::default(),
            volume_bind_poll: crate::volume_bind::BIND_POLL_INTERVAL,
        }
    }

    /// `frameworkForPod` (schedule_one.go:390-396).
    pub(crate) fn profile_for(&self, pod: &Pod) -> Result<&crate::profile::Profile, String> {
        self.profiles.framework_for_pod(pod)
    }

    /// Record a preemption nomination synchronously (in-memory) and persist it
    /// to `status.nominatedNodeName` via the `/status` subresource.
    async fn nominate(&self, pod_key: &str, node_name: &str) {
        let (ns, name) = parse_pod_storage_key(pod_key);
        self.nominations
            .lock()
            .unwrap()
            .insert(format!("{ns}/{name}"), node_name.to_string());
        let node = node_name.to_string();
        self.update_pod_status_with_retry(pod_key, move |p| {
            if let Some(ref mut status) = p.status {
                status.nominated_node_name = Some(node.clone());
            }
        })
        .await;
    }

    /// Overlay the in-memory nominations onto a freshly-read pod list so the
    /// space reservation sees them before the informer propagates the `/status`
    /// write. Prunes stale entries: a nomination is dropped once the pod is
    /// bound (`spec.nodeName` set) or no longer present.
    fn apply_nominations(&self, all_pods: &mut [Pod]) {
        let mut noms = self.nominations.lock().unwrap();
        if noms.is_empty() {
            return;
        }
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        for pod in all_pods.iter_mut() {
            let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
            let key = format!("{}/{}", ns, pod.metadata.name);
            let bound = pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .is_some();
            if let Some(node) = noms.get(&key) {
                if bound {
                    continue; // pruned below — reservation no longer needed
                }
                live.insert(key.clone());
                let status = pod.status.get_or_insert_with(Default::default);
                if status.nominated_node_name.is_none() {
                    status.nominated_node_name = Some(node.clone());
                }
            }
        }
        // Keep only nominations whose pod is still present and unbound.
        noms.retain(|k, _| live.contains(k));
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

        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        // API mode: drive the work queue from the pods/nodes reflectors instead
        // of a storage watch. Spawn both reflector run loops, then subscribe to
        // the pods store and enqueue the work-queue key for every mutation.
        if let DataPlane::Api(api) = &self.data {
            let pods = Arc::clone(&api.pods);
            let nodes = Arc::clone(&api.nodes);
            let priority_classes = Arc::clone(&api.priority_classes);
            tokio::spawn(async move { pods.run().await });
            tokio::spawn(async move { nodes.run().await });
            tokio::spawn(async move { priority_classes.run().await });

            let mut sub = api.pods.subscribe();
            let mut resync = tokio::time::interval(self.interval);
            resync.tick().await; // consume the immediate first tick
            loop {
                tokio::select! {
                    ev = sub.recv() => {
                        use rusternetes_client::reflector::StoreEvent;
                        match ev {
                            Ok(StoreEvent::Added(p) | StoreEvent::Modified(p)) => {
                                let ns = p.metadata.namespace.as_deref().unwrap_or("default");
                                queue.add(format!("pods/{}/{}", ns, p.metadata.name)).await;
                            }
                            Ok(StoreEvent::Deleted(_)) => {}
                            // Lagged/closed: fall back to a full resync below.
                            Err(_) => { self.enqueue_all(&queue).await; }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }

        // Storage mode (all-in-one): watch /registry/pods directly.
        let storage = match &self.data {
            DataPlane::Storage(s) => Arc::clone(s),
            DataPlane::Api(_) => unreachable!("API mode handled above"),
        };
        loop {
            self.enqueue_all(&queue).await;

            // Watch for pod changes (new pods, status changes)
            let prefix = build_prefix("pods", None);
            let watch_result = storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
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
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
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
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            // K8s schedules ONE pod per cycle (scheduleOne), not all pending
            // pods. This prevents the worker from blocking for minutes when
            // many pods are pending. Each pod key gets its own fast cycle.
            match self.try_schedule_pod(&key).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    debug!("Failed to schedule pod {}: {}", key, e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Enqueue all pending pods for scheduling.
    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.data.list_pods().await {
            Ok(pods) => {
                for pod in &pods {
                    // Only enqueue pods that need scheduling
                    // Treat nodeName="" same as None (Go JSON produces empty string)
                    let has_node = pod
                        .spec
                        .as_ref()
                        .and_then(|s| s.node_name.as_deref())
                        .is_some_and(|n| !n.is_empty());
                    let needs_scheduling = !has_node
                        && matches!(
                            pod.status.as_ref().and_then(|s| s.phase.as_ref()),
                            None | Some(Phase::Pending)
                        );
                    if needs_scheduling {
                        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
                        let key = format!("pods/{}/{}", ns, pod.metadata.name);
                        queue.add(key).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to list pods for enqueue: {}", e);
            }
        }
    }

    /// Try to schedule a single pod by its queue key (pods/{ns}/{name}).
    /// K8s schedules one pod per cycle in scheduleOne().
    async fn try_schedule_pod(&self, key: &str) -> rusternetes_common::Result<()> {
        let parts: Vec<&str> = key.splitn(3, '/').collect();
        let (ns, name) = match parts.len() {
            3 => (parts[1], parts[2]),
            _ => return Ok(()), // invalid key
        };

        let pod_key = build_key("pods", Some(ns), name);
        let pod: Pod = match self.data.get_pod(ns, name).await {
            Ok(p) => p,
            Err(_) => return Ok(()), // pod deleted
        };

        // Check if pod needs scheduling
        let has_node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .is_some_and(|n| !n.is_empty());
        if has_node {
            return Ok(()); // already scheduled
        }
        let is_pending = pod
            .status
            .as_ref()
            .map(|s| s.phase.is_none() || s.phase == Some(Phase::Pending))
            .unwrap_or(true);
        if !is_pending {
            return Ok(()); // not pending
        }
        // `responsibleForPod` (eventhandlers.go:474-476): only pods naming one
        // of our profiles enter the queue.
        if !self.responsible_for_pod(&pod) {
            return Ok(()); // wrong scheduler
        }
        // `frameworkForPod` (schedule_one.go:85-92): cannot fail after the
        // check above; upstream logs and drops the pod if it does.
        if let Err(e) = self.profile_for(&pod) {
            error!(error = %e, pod = key, "Error occurred");
            return Ok(());
        }

        // Get nodes and all pods for scheduling context
        let nodes: Vec<Node> = self.data.list_nodes().await?;
        if nodes.is_empty() {
            return Ok(());
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
        self.apply_nominations(&mut all_pods);

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
        let (selected, chosen_volumes, diagnosis) = self
            .select_node_diag_volumes(&pod, &nodes, &all_pods, &priority_classes)
            .await;
        if let Some(node) = selected {
            if let Err(e) = self
                .reserve_prebind_bind(pod, &node.metadata.name, chosen_volumes)
                .await
            {
                error!("Failed to bind pod {}/{} to node: {}", ns, name, e);
                // Return the error so the work queue retries with backoff
                // (handleBindingCycleError re-queues the pod upstream).
                return Err(e);
            }
        } else if let Some((node_name, victims)) = self
            .try_preempt(&pod, &nodes, &all_pods, &priority_classes, &pdbs)
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
            self.nominate(&pod_key, &node_name).await;
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
        }

        Ok(())
    }

    /// Run one scheduling cycle — schedules all pending pods.
    ///
    /// Public for testing. The production bin path uses `run()` →
    /// `worker()` (watch + work-queue driven), not this direct method;
    /// `dead_code` is allowed because the unit tests below DO call it
    /// but the bin doesn't, and the test cfg is gated out of the bin
    /// compilation unit.
    #[allow(dead_code)]
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
                    .try_preempt(&pod, &nodes, &all_pods, &priority_classes, &pdbs)
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

    /// Feed the assume caches (the informer feed upstream), then PreFilter.
    fn sync_and_pre_filter(
        &self,
        ns: &str,
        pod: &Pod,
        snap: &volume_binding::VolumeSnapshot,
    ) -> PreFilterOutcome {
        self.volume_binder
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .sync_namespace(ns, snap);
        volume_binding::pre_filter(pod, snap)
    }

    /// `select_node_diag` that also returns the chosen node's `PodVolumes`
    /// (`stateData.podVolumesByNode[node]`), which Reserve/PreBind consume.
    async fn select_node_diag_volumes(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
    ) -> (Option<Node>, Option<volume_binding::PodVolumes>, Diagnosis) {
        let mut diag = Diagnosis::default();
        let picked = self
            .select_node_inner(pod, nodes, all_pods, priority_classes, &mut diag)
            .await;
        match picked {
            Some((node, vols)) => (Some(node), vols, diag),
            None => (None, None, diag),
        }
    }

    /// Reserve -> PreBindPreFlight/PreBind -> Bind for the VolumeBinding plugin,
    /// in `schedulingCycle`/`bindingCycle` order (schedule_one.go:211 Reserve,
    /// :327 RunPreBindPlugins, :332 bind; any failure runs Unreserve:
    /// :213 and `handleBindingCycleError` :368).
    ///
    /// Deviation: upstream runs the binding cycle in a goroutine
    /// (schedule_one.go:123); here it runs inline in the work-queue worker, so
    /// a pod waiting on its PV controller occupies a worker for up to
    /// BindTimeoutSeconds.
    async fn reserve_prebind_bind(
        &self,
        pod: Pod,
        node_name: &str,
        vols: Option<volume_binding::PodVolumes>,
    ) -> rusternetes_common::Result<()> {
        use crate::volume_bind::{self as vb, PreBindPreFlight, VolumeBindingState};
        let profile = self
            .profile_for(&pod)
            .map_err(rusternetes_common::Error::Internal)?;
        let bind_timeout = profile.volume_bind_timeout;
        // VolumeBinding's Reserve / PreBind run only when the pod's profile
        // enables the plugin at those extension points (RunReservePluginsReserve
        // :211, RunPreBindPlugins :327 iterate the profile's plugin lists).
        let reserve_on = profile
            .framework
            .has(ExtensionPoint::Reserve, names::VOLUME_BINDING);
        let prebind_on = profile
            .framework
            .has(ExtensionPoint::PreBind, names::VOLUME_BINDING);
        let ns = pod
            .metadata
            .namespace
            .clone()
            .unwrap_or_else(|| "default".into());
        let mut state = VolumeBindingState::default();
        if let Some(v) = vols {
            state.pod_volumes_by_node.insert(node_name.to_string(), v);
        }
        let env = vb::DataPlaneBinderEnv {
            binder: &self.volume_binder,
            data: &self.data,
            namespace: ns,
        };

        // Reserve (volume_binding.go:531-553)
        if reserve_on {
            let mut b = self.volume_binder.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = vb::reserve(&mut b, &mut state, &pod, node_name) {
                drop(b);
                // schedule_one.go:213 — Unreserve on a failed Reserve.
                vb::unreserve(&env, &state, node_name);
                return Err(rusternetes_common::Error::Internal(format!(
                    "VolumeBinding Reserve failed: {e}"
                )));
            }
        }

        // PreBindPreFlight + PreBind (volume_binding.go:555-601)
        let prebind = match vb::pre_bind_preflight(&state, node_name) {
            _ if !prebind_on => Ok(()),
            Ok(PreBindPreFlight::Skip) => Ok(()),
            Ok(PreBindPreFlight::Run) => {
                vb::pre_bind(
                    &env,
                    &mut state,
                    &pod,
                    node_name,
                    bind_timeout,
                    self.volume_bind_poll,
                )
                .await
            }
            Err(e) => Err(e),
        };
        if let Err(e) = prebind {
            // handleBindingCycleError (schedule_one.go:368)
            vb::unreserve(&env, &state, node_name);
            return Err(rusternetes_common::Error::Internal(format!(
                "running PreBind plugin \"VolumeBinding\": {e}"
            )));
        }

        // Bind (schedule_one.go:332)
        if let Err(e) = self.bind_pod_to_node(pod, node_name).await {
            vb::unreserve(&env, &state, node_name);
            return Err(e);
        }
        Ok(())
    }

    async fn select_node_inner(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
        diag: &mut Diagnosis,
    ) -> Option<(Node, Option<volume_binding::PodVolumes>)> {
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
        let filter_on = |name: &str| fw.has(ExtensionPoint::Filter, name);
        // Phase 0: VolumeBinding PreFilter (volume_binding.go PreFilter). A pod
        // with a missing/lost/deleting PVC or an unbound Immediate claim is
        // UnschedulableAndUnresolvable; the pod is retried on the next resync.
        let volume_ctx = if volume_binding::pod_references_pvcs(pod)
            && fw.has(ExtensionPoint::PreFilter, names::VOLUME_BINDING)
        {
            let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
            match self.data.list_volume_snapshot(ns).await {
                Ok(snap) => match self.sync_and_pre_filter(ns, pod, &snap) {
                    PreFilterOutcome::Skip => None,
                    PreFilterOutcome::Ready(claims) => Some((snap, claims)),
                    PreFilterOutcome::Unresolvable(m) | PreFilterOutcome::Error(m) => {
                        diag.pre_filter_msg = m.clone();
                        debug!(
                            "VolumeBinding PreFilter rejected pod {}/{}: {}",
                            ns, pod.metadata.name, m
                        );
                        return None;
                    }
                },
                Err(e) => {
                    warn!("VolumeBinding: failed to list volume objects: {}", e);
                    return None;
                }
            }
        } else {
            None
        };

        // Phase 1: Filter schedulable nodes
        // (NodeUnschedulable filter.)
        let unschedulable_on = filter_on(names::NODE_UNSCHEDULABLE);
        let schedulable_nodes: Vec<&Node> = nodes
            .iter()
            .filter(|n| {
                !unschedulable_on
                    || !n
                        .spec
                        .as_ref()
                        .and_then(|s| s.unschedulable)
                        .unwrap_or(false)
            })
            .collect();

        if schedulable_nodes.is_empty() {
            return None;
        }

        // Phase 2: Filter by taints and tolerations (TaintToleration filter).
        let taint_on = filter_on(names::TAINT_TOLERATION);
        let tolerated_nodes: Vec<&Node> = schedulable_nodes
            .iter()
            .filter(|node| !taint_on || check_taints_tolerations(node, pod))
            .copied()
            .collect();

        if tolerated_nodes.is_empty() {
            debug!("No nodes tolerate pod taints");
            return None;
        }

        // Phase 3: Check node selectors (basic label matching)
        // (`spec.nodeSelector` is part of the NodeAffinity filter.)
        let node_affinity_filter_on = filter_on(names::NODE_AFFINITY);
        let selector_matched_nodes: Vec<&Node> = if !node_affinity_filter_on {
            tolerated_nodes
        } else if let Some(node_selector) = pod.spec.as_ref().and_then(|s| s.node_selector.as_ref())
        {
            tolerated_nodes
                .iter()
                .filter(|node| self.matches_node_selector(node, node_selector))
                .copied()
                .collect()
        } else {
            tolerated_nodes
        };

        if selector_matched_nodes.is_empty() {
            debug!("No nodes match node selector");
            return None;
        }

        // Phase 4: Check DRA device availability
        // Filter nodes that have required devices for ResourceClaims
        let dra_on = filter_on(names::DYNAMIC_RESOURCES);
        let mut dra_matched_nodes: Vec<&Node> = Vec::new();
        for node in selector_matched_nodes {
            if !dra_on || self.check_dra_device_availability(node, pod).await {
                dra_matched_nodes.push(node);
            } else {
                debug!(
                    "Node {} does not have required DRA devices for pod {}",
                    node.metadata.name, pod.metadata.name
                );
            }
        }

        if dra_matched_nodes.is_empty() {
            debug!("No nodes have required DRA devices");
            return None;
        }

        // Phase 4b: Check hostPort conflicts
        let ports_on = filter_on(names::NODE_PORTS);
        let port_ok_nodes: Vec<&Node> = dra_matched_nodes
            .into_iter()
            .filter(|node| {
                if ports_on && !check_host_port_conflicts(node, pod, all_pods) {
                    debug!(
                        "Node {} rejected for pod {}: hostPort conflict",
                        node.metadata.name, pod.metadata.name
                    );
                    false
                } else {
                    true
                }
            })
            .collect();

        if port_ok_nodes.is_empty() {
            debug!("No nodes without hostPort conflicts");
            return None;
        }

        // Phase 4c: VolumeBinding Filter (FindPodVolumes) — bound PV node
        // affinity, matching/provisionable unbound WaitForFirstConsumer claims.
        // `podVolumesByNode` (volume_binding.go stateData): Filter's per-node
        // result, read back by PreScore/Score.
        let mut pod_volumes_by_node: std::collections::HashMap<String, volume_binding::PodVolumes> =
            std::collections::HashMap::new();
        let port_ok_nodes: Vec<&Node> = match volume_ctx {
            Some((snap, claims)) if filter_on(names::VOLUME_BINDING) => port_ok_nodes
                .into_iter()
                .filter(
                    |node| match volume_binding::find_pod_volumes(pod, &claims, node, &snap) {
                        Ok((vols, reasons)) if reasons.is_empty() => {
                            pod_volumes_by_node.insert(node.metadata.name.clone(), vols);
                            true
                        }
                        Ok((_, reasons)) => {
                            diag.add_node_reasons(reasons.iter().map(|r| r.to_string()));
                            debug!(
                                "Node {} rejected for pod {}: {}",
                                node.metadata.name,
                                pod.metadata.name,
                                reasons.join(", ")
                            );
                            false
                        }
                        Err(e) => {
                            debug!(
                                "VolumeBinding filter error on node {} for pod {}: {}",
                                node.metadata.name, pod.metadata.name, e
                            );
                            false
                        }
                    },
                )
                .collect(),
            _ => port_ok_nodes,
        };

        if port_ok_nodes.is_empty() {
            debug!("No nodes satisfy the pod's volume requirements");
            return None;
        }

        // VolumeBinding PreScore (volume_binding.go:456-468): `pl.scorer` is
        // only built when StorageCapacityScoring is on (volume_binding.go:642-653);
        // otherwise, or when PreScore says Skip, Score is not run.
        // Per-profile VolumeBindingArgs.Shape (the pod's own framework).
        let volume_shape = profile.volume_shape.clone();
        let scoring_enabled = rusternetes_common::feature_gates::enabled(
            rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
        );
        let volume_scorer = if scoring_enabled
            && !volume_binding::pre_score_skip(&pod_volumes_by_node, scoring_enabled)
        {
            Some(volume_binding::build_scorer_function(&volume_shape))
        } else {
            None
        };

        // Phase 5, 6 & 7: Score nodes based on affinity, pod affinity/anti-affinity, topology spread, and resources
        let mut node_scores: Vec<NodeScore> = Vec::new();
        let inter_pod_filter_on = filter_on(names::INTER_POD_AFFINITY);
        let spread_filter_on = filter_on(names::POD_TOPOLOGY_SPREAD);
        let fit_filter_on = filter_on(names::NODE_RESOURCES_FIT);

        for node in port_ok_nodes {
            // Check node affinity (hard requirements and scoring)
            let (affinity_ok, node_affinity_score) = check_node_affinity(node, pod);
            if node_affinity_filter_on && !affinity_ok {
                continue; // Skip nodes that don't meet hard affinity requirements
            }

            // Check pod affinity (hard requirements and scoring)
            let (pod_affinity_ok, pod_affinity_score) =
                check_pod_affinity(node, pod, all_pods, nodes);
            if inter_pod_filter_on && !pod_affinity_ok {
                continue; // Skip nodes that don't meet hard pod affinity requirements
            }

            // Check pod anti-affinity (hard requirements and penalty scoring)
            let (pod_anti_affinity_ok, pod_anti_affinity_penalty) =
                check_pod_anti_affinity(node, pod, all_pods, nodes);
            if inter_pod_filter_on && !pod_anti_affinity_ok {
                continue; // Skip nodes that violate hard pod anti-affinity requirements
            }

            // Check topology spread constraints (hard requirements and penalty scoring)
            let (topology_ok, topology_penalty) =
                check_topology_spread_constraints(node, pod, all_pods, nodes);
            if spread_filter_on && !topology_ok {
                continue; // Skip nodes that violate hard topology spread constraints
            }

            // Calculate resource-based score (accounting for pod overhead and existing pod usage)
            let resource_score = self.calculate_resource_score_with_overhead(node, pod, all_pods);

            // If pod doesn't fit resource-wise, skip
            if fit_filter_on && resource_score == 0 {
                debug!(
                    "Node {} rejected for pod {}: resource_score=0 (insufficient resources)",
                    node.metadata.name, pod.metadata.name,
                );
                continue;
            }

            // Priority score (resolve from PriorityClass if needed)
            let priority_score = self.get_pod_priority_sync(pod, priority_classes);

            // Combined score:
            // - resource (weight 25%)
            // - node affinity (weight 20%)
            // - pod affinity (weight 18%)
            // - priority (weight 15%)
            // - pod anti-affinity penalty (weight 12%)
            // - topology spread penalty (weight 10%)
            // VolumeBinding Score, plugin weight 1 (default_plugins.go:44: no
            // `Weight` => 1) on the 0..=MaxNodeScore range.
            let volume_score = volume_scorer.as_ref().map_or(0, |scorer| {
                volume_binding::score(
                    pod_volumes_by_node.get(&node.metadata.name),
                    scorer,
                    scoring_enabled,
                )
            });
            // Each component belongs to the Score plugin that owns it upstream
            // and is scaled by that plugin's configured weight relative to
            // its default weight (identical to the unscaled blend under the
            // default profile); a profile that does not run the plugin at
            // Score contributes nothing for it.
            let w = |plugin: &str, component: i64, pct: i64| {
                weighted_component(fw, plugin, component, pct)
            };
            let total_score = volume_score * i64::from(fw.score_weight(names::VOLUME_BINDING))
                + w(names::NODE_RESOURCES_FIT, resource_score as i64, 25)
                + w(names::NODE_AFFINITY, node_affinity_score as i64, 20)
                + w(names::INTER_POD_AFFINITY, pod_affinity_score as i64, 18)
                + (priority_score as i64 * 15 / 100)
                - w(
                    names::INTER_POD_AFFINITY,
                    pod_anti_affinity_penalty as i64,
                    12,
                )
                - w(names::POD_TOPOLOGY_SPREAD, topology_penalty as i64, 10);

            node_scores.push(NodeScore {
                node_name: node.metadata.name.clone(),
                score: total_score.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
            });
        }

        if node_scores.is_empty() {
            return None;
        }

        // Sort by score (descending). On tie, prefer the node with fewer
        // pods already scheduled (LeastAllocated). This spreads pods across
        // nodes when scores are equal (e.g. identical node configurations).
        let pod_counts: std::collections::HashMap<String, usize> = {
            let mut counts = std::collections::HashMap::new();
            for p in all_pods {
                if let Some(node) = p.spec.as_ref().and_then(|s| s.node_name.as_ref()) {
                    *counts.entry(node.clone()).or_insert(0) += 1;
                }
            }
            counts
        };
        node_scores.sort_by(|a, b| {
            let score_cmp = b.score.cmp(&a.score);
            if score_cmp == std::cmp::Ordering::Equal {
                // Fewer pods = better (ascending)
                let a_pods = pod_counts.get(&a.node_name).unwrap_or(&0);
                let b_pods = pod_counts.get(&b.node_name).unwrap_or(&0);
                a_pods.cmp(b_pods)
            } else {
                score_cmp
            }
        });

        let best_node_name = &node_scores[0].node_name;
        debug!(
            "Selected node {} with score {} for pod {}",
            best_node_name, node_scores[0].score, pod.metadata.name
        );

        // `state.podVolumesByNode[nodeName]` (volume_binding.go:537) is carried
        // to Reserve/PreBind for the chosen node only.
        let chosen_volumes = pod_volumes_by_node.remove(best_node_name);
        nodes
            .iter()
            .find(|n| &n.metadata.name == best_node_name)
            .cloned()
            .map(|n| (n, chosen_volumes))
    }

    fn matches_node_selector(&self, node: &Node, selector: &HashMap<String, String>) -> bool {
        let node_labels = node.metadata.labels.as_ref();

        if node_labels.is_none() {
            return selector.is_empty();
        }

        let labels = node_labels.unwrap();

        for (key, value) in selector {
            if labels.get(key) != Some(value) {
                return false;
            }
        }

        true
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
    async fn try_preempt(
        &self,
        pod: &Pod,
        nodes: &[Node],
        all_pods: &[Pod],
        priority_classes: &HashMap<String, PriorityClass>,
        pdbs: &[rusternetes_common::resources::PodDisruptionBudget],
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
        if !crate::advanced::pod_eligible_to_preempt_others(pod, all_pods) {
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
        let mut candidates: Vec<crate::advanced::PreemptionCandidate> = Vec::new();

        // Only consider nodes that pass basic scheduling constraints (except resources)
        for node in nodes {
            // Skip unschedulable nodes
            if node
                .spec
                .as_ref()
                .and_then(|s| s.unschedulable)
                .unwrap_or(false)
            {
                continue;
            }

            // Check taints/tolerations
            if !check_taints_tolerations(node, pod) {
                continue;
            }

            // Check node selector
            if let Some(node_selector) = pod.spec.as_ref().and_then(|s| s.node_selector.as_ref()) {
                if !self.matches_node_selector(node, node_selector) {
                    continue;
                }
            }

            // Check node affinity (hard requirements only)
            let (affinity_ok, _) = check_node_affinity(node, pod);
            if !affinity_ok {
                continue;
            }

            // PDB-aware selection: check_preemption_with_pdbs sorts
            // budget-protected pods last and reprieves them where the
            // preemptor still fits without them, which is upstream's
            // filterPodsWithPDBViolation
            // (pkg/scheduler/framework/preemption/preemption.go). Passing an
            // empty slice here — as this path did until #1797 — silently
            // disabled all of it.
            let (can_preempt, pods_to_evict) = crate::advanced::select_preemption_victims(
                node,
                pod,
                all_pods,
                pdbs,
                priority_classes,
            );
            if can_preempt && !pods_to_evict.is_empty() {
                candidates.push(crate::advanced::PreemptionCandidate {
                    num_pdb_violations: crate::advanced::count_pdb_violating_victims(
                        &pods_to_evict,
                        all_pods,
                        pdbs,
                    ),
                    node_name: node.metadata.name.clone(),
                    victims: pods_to_evict,
                });
            }
        }

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
    async fn evict_pod(&self, preemptor: &Pod, victim: &Pod) -> rusternetes_common::Result<()> {
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

    /// Check if node has available devices for DRA ResourceClaims
    /// Returns true if all required devices are available on the node, or if no resource claims are specified
    async fn check_dra_device_availability(&self, node: &Node, pod: &Pod) -> bool {
        use rusternetes_common::resources::{ResourceClaim, ResourceSlice};

        // Extract resourceClaims from pod.spec
        let spec = match &pod.spec {
            Some(s) => s,
            None => return true, // No spec, no claims to check
        };

        let resource_claims_refs = match &spec.resource_claims {
            Some(claims) => claims,
            None => return true, // No resource claims, all nodes are suitable
        };

        if resource_claims_refs.is_empty() {
            return true;
        }

        let pod_namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // For each claim reference, resolve the ResourceClaim object
        for claim_ref in resource_claims_refs {
            let claim_name = if let Some(name) = &claim_ref.resource_claim_name {
                name.as_str()
            } else if let Some(template_name) = &claim_ref.resource_claim_template_name {
                // TODO: In a full implementation, we'd need to resolve the template
                // and create a ResourceClaim from it. For now, we'll treat the template
                // name as the claim name (simplified)
                debug!(
                    "ResourceClaimTemplate '{}' referenced, treating as claim name",
                    template_name
                );
                template_name.as_str()
            } else {
                warn!("ResourceClaim reference has no name or template");
                return false;
            };

            // Get the ResourceClaim from the data plane.
            let claim: ResourceClaim = match self
                .data
                .get_resource_claim(pod_namespace, claim_name)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to get ResourceClaim {}/{}: {}",
                        pod_namespace, claim_name, e
                    );
                    return false;
                }
            };

            // Check if the claim is allocated
            let allocation = match &claim.status {
                Some(status) => match &status.allocation {
                    Some(alloc) => alloc,
                    None => {
                        debug!(
                            "ResourceClaim {}/{} is not yet allocated",
                            pod_namespace, claim_name
                        );
                        return false; // Claim not allocated yet
                    }
                },
                None => {
                    debug!(
                        "ResourceClaim {}/{} has no status",
                        pod_namespace, claim_name
                    );
                    return false;
                }
            };

            // Check if the allocation has a node selector and if this node matches
            if let Some(node_selector) = &allocation.node_selector {
                // Check if node matches the node selector
                let node_labels = node.metadata.labels.as_ref();
                if let Some(required_labels) = &node_selector.node_selector_terms.first() {
                    if let Some(match_expressions) = &required_labels.match_expressions {
                        for expr in match_expressions {
                            let node_label_value = node_labels.and_then(|l| l.get(&expr.key));

                            match expr.operator.as_str() {
                                "In" => {
                                    if let Some(values) = &expr.values {
                                        let matches = node_label_value
                                            .map(|v| values.contains(v))
                                            .unwrap_or(false);
                                        if !matches {
                                            debug!(
                                                "Node {} does not match ResourceClaim node selector (key={}, operator=In)",
                                                node.metadata.name, expr.key
                                            );
                                            return false;
                                        }
                                    }
                                }
                                "NotIn" => {
                                    if let Some(values) = &expr.values {
                                        let matches = node_label_value
                                            .map(|v| !values.contains(v))
                                            .unwrap_or(true);
                                        if !matches {
                                            debug!(
                                                "Node {} does not match ResourceClaim node selector (key={}, operator=NotIn)",
                                                node.metadata.name, expr.key
                                            );
                                            return false;
                                        }
                                    }
                                }
                                "Exists" => {
                                    if node_label_value.is_none() {
                                        debug!(
                                            "Node {} does not match ResourceClaim node selector (key={}, operator=Exists)",
                                            node.metadata.name, expr.key
                                        );
                                        return false;
                                    }
                                }
                                "DoesNotExist" => {
                                    if node_label_value.is_some() {
                                        debug!(
                                            "Node {} does not match ResourceClaim node selector (key={}, operator=DoesNotExist)",
                                            node.metadata.name, expr.key
                                        );
                                        return false;
                                    }
                                }
                                _ => {
                                    warn!("Unknown node selector operator: {}", expr.operator);
                                }
                            }
                        }
                    }
                }
            }

            // Verify devices are available on this node
            // Check each allocated device to ensure it's on this node
            for device_result in &allocation.devices.results {
                // Get ResourceSlices to find which node has this device
                let slices: Vec<ResourceSlice> = match self.data.list_resource_slices().await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to list ResourceSlices: {}", e);
                        return false;
                    }
                };

                let mut device_found_on_node = false;

                for slice in slices {
                    // Check if this slice is for the right driver and pool
                    if slice.spec.driver != device_result.driver {
                        continue;
                    }
                    if slice.spec.pool.name != device_result.pool {
                        continue;
                    }

                    // Check if slice has node name specified
                    let slice_node_name = match &slice.spec.node_name {
                        Some(name) => name,
                        None => continue, // Slice not associated with a specific node
                    };

                    // Check if this is the target node
                    if slice_node_name != &node.metadata.name {
                        continue;
                    }

                    // Check if the device exists in this slice
                    for device in &slice.spec.devices {
                        if device.name == device_result.device {
                            device_found_on_node = true;
                            break;
                        }
                    }

                    if device_found_on_node {
                        break;
                    }
                }

                if !device_found_on_node {
                    debug!(
                        "Device {} from pool {} (driver {}) not found on node {}",
                        device_result.device,
                        device_result.pool,
                        device_result.driver,
                        node.metadata.name
                    );
                    return false;
                }
            }
        }

        // All resource claims are satisfied on this node
        true
    }
}

/// Split a `/registry/pods/{ns}/{name}` storage key into `(ns, name)`. Tolerates
/// a missing namespace segment (defaults to `default`). The scheduler builds
/// these keys via `build_key("pods", Some(ns), name)`, so the format is stable.
/// Default `Score` plugin weights (default_plugins.go:35-48) the compiled-in
/// score blend is calibrated against.
fn default_score_weight(plugin: &str) -> i64 {
    match plugin {
        names::NODE_AFFINITY | names::INTER_POD_AFFINITY | names::POD_TOPOLOGY_SPREAD => 2,
        _ => 1,
    }
}

/// One component of the compiled-in score blend, scaled by the profile's
/// weight for the plugin that owns it: `component * pct / 100` at the default
/// weight, 0 when the profile does not run `plugin` at Score.
fn weighted_component(
    fw: &crate::framework_runtime::Framework,
    plugin: &str,
    component: i64,
    pct: i64,
) -> i64 {
    if !fw.has(ExtensionPoint::Score, plugin) {
        return 0;
    }
    component * pct * i64::from(fw.score_weight(plugin)) / (100 * default_score_weight(plugin))
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

// Unit tests for the DisruptionTarget condition are verified inline:
#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{Container, PodSpec, PodStatus};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;

    fn make_node(name: &str) -> Node {
        Node {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Node".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.labels = Some({
                    let mut labels = HashMap::new();
                    labels.insert("kubernetes.io/os".to_string(), "linux".to_string());
                    labels.insert("kubernetes.io/arch".to_string(), "amd64".to_string());
                    labels.insert("kubernetes.io/hostname".to_string(), name.to_string());
                    labels
                });
                m
            },
            spec: None,
            status: Some(rusternetes_common::resources::NodeStatus {
                conditions: Some(vec![rusternetes_common::resources::NodeCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: Some("KubeletReady".to_string()),
                    message: Some("kubelet is posting ready status".to_string()),
                    last_heartbeat_time: Some(Utc::now()),
                    last_transition_time: Some(Utc::now()),
                }]),
                capacity: Some({
                    let mut m = HashMap::new();
                    m.insert("cpu".to_string(), "4".to_string());
                    m.insert("memory".to_string(), "8Gi".to_string());
                    m.insert("pods".to_string(), "110".to_string());
                    m
                }),
                allocatable: Some({
                    let mut m = HashMap::new();
                    m.insert("cpu".to_string(), "4".to_string());
                    m.insert("memory".to_string(), "8Gi".to_string());
                    m.insert("pods".to_string(), "110".to_string());
                    m
                }),
                addresses: None,
                daemon_endpoints: None,
                node_info: None,
                images: None,
                volumes_in_use: None,
                volumes_attached: None,
                config: None,
                runtime_handlers: None,
                features: None,
                declared_features: None,
            }),
        }
    }

    fn make_pending_pod(name: &str, ns: &str) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name);
                m.namespace = Some(ns.to_string());
                m
            },
            spec: Some(PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "main".to_string(),
                    image: "busybox".to_string(),
                    command: None,
                    args: None,
                    working_dir: None,
                    ports: None,
                    env: None,
                    env_from: None,
                    resources: None,
                    volume_mounts: None,
                    volume_devices: None,
                    liveness_probe: None,
                    readiness_probe: None,
                    startup_probe: None,
                    lifecycle: None,
                    termination_message_path: None,
                    termination_message_policy: None,
                    image_pull_policy: None,
                    security_context: None,
                    stdin: None,
                    stdin_once: None,
                    tty: None,
                    resize_policy: None,
                    restart_policy: None,
                    ..Default::default()
                }],
                scheduler_name: Some("default-scheduler".to_string()),
                init_containers: None,
                ephemeral_containers: None,
                restart_policy: None,
                termination_grace_period_seconds: None,
                dns_policy: None,
                node_selector: None,
                service_account_name: None,
                service_account: None,
                automount_service_account_token: None,
                node_name: None,
                host_network: None,
                host_pid: None,
                host_ipc: None,
                security_context: None,
                image_pull_secrets: None,
                hostname: None,
                subdomain: None,
                affinity: None,
                tolerations: None,
                host_aliases: None,
                priority_class_name: None,
                priority: None,
                preemption_policy: None,
                overhead: None,
                topology_spread_constraints: None,
                volumes: None,
                active_deadline_seconds: None,
                dns_config: None,
                enable_service_links: None,
                readiness_gates: None,
                runtime_class_name: None,
                os: None,
                set_hostname_as_fqdn: None,
                share_process_namespace: None,
                scheduling_gates: None,
                resource_claims: None,
                host_users: None,
                resources: None,
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                message: None,
                reason: None,
                host_ip: None,
                pod_ip: None,
                conditions: None,
                container_statuses: None,
                init_container_statuses: None,
                ephemeral_container_statuses: None,
                start_time: None,
                qos_class: None,
                nominated_node_name: None,
                host_i_ps: None,
                pod_i_ps: None,
                resize: None,
                resource_claim_statuses: None,
                observed_generation: None,
                ..Default::default()
            }),
        }
    }

    #[tokio::test]
    async fn test_scheduler_assigns_pod_to_node() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        // Create two nodes
        let node1 = make_node("node-1");
        let node2 = make_node("node-2");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();
        storage
            .create("/registry/nodes/node-2", &node2)
            .await
            .unwrap();

        // Create a pending pod
        let pod = make_pending_pod("test-pod", "default");
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        // Run one scheduling cycle
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should now have a node name assigned
        let scheduled_pod: Pod = storage
            .get("/registry/pods/default/test-pod")
            .await
            .unwrap();
        let node_name = scheduled_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref());
        assert!(
            node_name.is_some(),
            "Pod should be assigned to a node after scheduling"
        );
        let node_name = node_name.unwrap();
        assert!(
            node_name == "node-1" || node_name == "node-2",
            "Pod should be on node-1 or node-2, got: {}",
            node_name
        );

        // Pod should have PodScheduled condition
        let conditions = scheduled_pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        assert!(conditions.is_some(), "Pod should have conditions");
        let has_scheduled = conditions
            .unwrap()
            .iter()
            .any(|c| c.condition_type == "PodScheduled" && c.status == "True");
        assert!(has_scheduled, "Pod should have PodScheduled=True condition");
    }

    /// VolumeBinding wiring (#3017): a pod whose PVC does not exist is
    /// UnschedulableAndUnresolvable; once a bound PVC whose PV fits only
    /// `node-2` exists, the pod lands on `node-2`.
    #[tokio::test]
    async fn test_volume_binding_filters_nodes_by_pv_affinity() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        for n in ["node-1", "node-2"] {
            let mut node = make_node(n);
            node.metadata.labels =
                Some(HashMap::from([("kubernetes.io/hostname".into(), n.into())]));
            storage
                .create(&format!("/registry/nodes/{n}"), &node)
                .await
                .unwrap();
        }
        let mut pod = make_pending_pod("vol-pod", "default");
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "v", "persistentVolumeClaim": {"claimName": "claim"}}),
        )
        .unwrap()]);
        storage
            .create("/registry/pods/default/vol-pod", &pod)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();
        let p: Pod = storage.get("/registry/pods/default/vol-pod").await.unwrap();
        assert!(
            p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none(),
            "pod with a missing PVC must not be scheduled"
        );

        let pvc: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default",
                    "annotations": {"pv.kubernetes.io/bind-completed": "yes"}},
                "spec": {"volumeName": "pv1", "storageClassName": "std",
                    "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Bound"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/claim", &pvc)
            .await
            .unwrap();
        let pv: rusternetes_common::resources::PersistentVolume =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolume",
                "metadata": {"name": "pv1"},
                "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "std", "hostPath": {"path": "/x"},
                    "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                        {"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-2"]}]}]}}},
                "status": {"phase": "Bound"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumes/pv1", &pv)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();
        let p: Pod = storage.get("/registry/pods/default/vol-pod").await.unwrap();
        assert_eq!(
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-2")
        );
    }

    /// VolumeBinding Score wiring (#3036), StorageCapacityScoring on: with the
    /// default shape (utilization 0 -> 100, 100 -> 0) the node whose static
    /// PV leaves more room wins even though `node-1` would win the tie-break.
    // `serial`: it flips a process-global feature gate that
    // `scheduler_config::tests::wired_into_validate_profiles` (also serial)
    // resets; unserialised, plain `cargo test` raced them.
    #[tokio::test]
    #[serial_test::serial]
    async fn test_volume_binding_score_prefers_node_with_more_free_capacity() {
        let _gate = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::StorageCapacityScoring,
            true,
        );
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        for n in ["node-1", "node-2"] {
            let mut node = make_node(n);
            node.metadata.labels =
                Some(HashMap::from([("kubernetes.io/hostname".into(), n.into())]));
            storage
                .create(&format!("/registry/nodes/{n}"), &node)
                .await
                .unwrap();
        }
        let class: rusternetes_common::resources::StorageClass =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": "wait"},
                "provisioner": "kubernetes.io/no-provisioner",
                "volumeBindingMode": "WaitForFirstConsumer"
            }))
            .unwrap();
        storage
            .create("/registry/storageclasses/wait", &class)
            .await
            .unwrap();
        let pvc: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default"},
                "spec": {"storageClassName": "wait",
                    "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/claim", &pvc)
            .await
            .unwrap();
        for (node, size) in [("node-1", "1Gi"), ("node-2", "10Gi")] {
            let pv: rusternetes_common::resources::PersistentVolume =
                serde_json::from_value(serde_json::json!({
                    "apiVersion": "v1", "kind": "PersistentVolume",
                    "metadata": {"name": format!("pv-{node}")},
                    "spec": {"capacity": {"storage": size}, "accessModes": ["ReadWriteOnce"],
                        "storageClassName": "wait", "hostPath": {"path": "/x"},
                        "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                            {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}}},
                    "status": {"phase": "Available"}
                }))
                .unwrap();
            storage
                .create(&format!("/registry/persistentvolumes/pv-{node}"), &pv)
                .await
                .unwrap();
        }
        let mut pod = make_pending_pod("score-pod", "default");
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "v", "persistentVolumeClaim": {"claimName": "claim"}}),
        )
        .unwrap()]);
        storage
            .create("/registry/pods/default/score-pod", &pod)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();
        let p: Pod = storage
            .get("/registry/pods/default/score-pod")
            .await
            .unwrap();
        assert_eq!(
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-2")
        );
    }

    /// Fixture: two nodes, a WaitForFirstConsumer class, an unbound claim and
    /// one static PV on node-2, plus a pod using the claim.
    async fn wffc_fixture(storage: &Arc<MemoryStorage>) {
        for n in ["node-1", "node-2"] {
            let mut node = make_node(n);
            node.metadata.labels =
                Some(HashMap::from([("kubernetes.io/hostname".into(), n.into())]));
            storage
                .create(&format!("/registry/nodes/{n}"), &node)
                .await
                .unwrap();
        }
        let class: rusternetes_common::resources::StorageClass =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
                "metadata": {"name": "wait"},
                "provisioner": "kubernetes.io/no-provisioner",
                "volumeBindingMode": "WaitForFirstConsumer"
            }))
            .unwrap();
        storage
            .create("/registry/storageclasses/wait", &class)
            .await
            .unwrap();
        let pvc: rusternetes_common::resources::PersistentVolumeClaim =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolumeClaim",
                "metadata": {"name": "claim", "namespace": "default", "uid": "claim-uid"},
                "spec": {"storageClassName": "wait",
                    "resources": {"requests": {"storage": "1Gi"}}},
                "status": {"phase": "Pending"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumeclaims/default/claim", &pvc)
            .await
            .unwrap();
        let pv: rusternetes_common::resources::PersistentVolume =
            serde_json::from_value(serde_json::json!({
                "apiVersion": "v1", "kind": "PersistentVolume",
                "metadata": {"name": "pv-node-2"},
                "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                    "storageClassName": "wait", "hostPath": {"path": "/x"},
                    "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                        {"key": "kubernetes.io/hostname", "operator": "In", "values": ["node-2"]}]}]}}},
                "status": {"phase": "Available"}
            }))
            .unwrap();
        storage
            .create("/registry/persistentvolumes/pv-node-2", &pv)
            .await
            .unwrap();
        let mut pod = make_pending_pod("wffc-pod", "default");
        pod.spec.as_mut().unwrap().volumes = Some(vec![serde_json::from_value(
            serde_json::json!({"name": "v", "persistentVolumeClaim": {"claimName": "claim"}}),
        )
        .unwrap()]);
        storage
            .create("/registry/pods/default/wffc-pod", &pod)
            .await
            .unwrap();
    }

    /// Reserve -> PreBind -> Bind (schedule_one.go:211, :327, :332): the pod is
    /// only bound once PreBind has written the PV claimRef and the PV
    /// controller (simulated here) has completed the PVC binding.
    #[tokio::test]
    async fn test_try_schedule_pod_prebinds_volumes_before_binding_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let mut scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        scheduler
            .profiles
            .get_mut("default-scheduler")
            .unwrap()
            .volume_bind_timeout = Duration::from_secs(10);
        scheduler.volume_bind_poll = Duration::from_millis(20);
        wffc_fixture(&storage).await;

        // PV controller: once the scheduler has set claimRef, bind the PVC.
        let st = storage.clone();
        let controller = tokio::spawn(async move {
            for _ in 0..250 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let pv: rusternetes_common::resources::PersistentVolume = st
                    .get("/registry/persistentvolumes/pv-node-2")
                    .await
                    .unwrap();
                if pv.spec.claim_ref.is_some() {
                    let mut pvc: rusternetes_common::resources::PersistentVolumeClaim = st
                        .get("/registry/persistentvolumeclaims/default/claim")
                        .await
                        .unwrap();
                    pvc.spec.volume_name = Some("pv-node-2".into());
                    pvc.metadata
                        .annotations
                        .get_or_insert_with(HashMap::new)
                        .insert("pv.kubernetes.io/bind-completed".into(), "yes".into());
                    st.update("/registry/persistentvolumeclaims/default/claim", &pvc)
                        .await
                        .unwrap();
                    return;
                }
            }
        });

        scheduler
            .try_schedule_pod("pods/default/wffc-pod")
            .await
            .unwrap();
        controller.await.unwrap();

        let pv: rusternetes_common::resources::PersistentVolume = storage
            .get("/registry/persistentvolumes/pv-node-2")
            .await
            .unwrap();
        let cr = pv.spec.claim_ref.expect("PreBind must set the PV claimRef");
        assert_eq!(cr.name.as_deref(), Some("claim"));
        let p: Pod = storage
            .get("/registry/pods/default/wffc-pod")
            .await
            .unwrap();
        assert_eq!(
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-2")
        );
    }

    /// A PreBind failure (BindTimeoutSeconds elapsed without the PV controller
    /// binding) must leave the pod unscheduled and return an error so the
    /// work queue retries (handleBindingCycleError, schedule_one.go:356-368).
    #[tokio::test]
    async fn test_try_schedule_pod_prebind_timeout_leaves_pod_unbound() {
        let storage = Arc::new(MemoryStorage::new());
        let mut scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());
        scheduler
            .profiles
            .get_mut("default-scheduler")
            .unwrap()
            .volume_bind_timeout = Duration::from_millis(200);
        scheduler.volume_bind_poll = Duration::from_millis(20);
        wffc_fixture(&storage).await;

        let r = scheduler.try_schedule_pod("pods/default/wffc-pod").await;
        assert!(r.is_err(), "PreBind timeout must surface as an error");
        let p: Pod = storage
            .get("/registry/pods/default/wffc-pod")
            .await
            .unwrap();
        assert!(p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none());
    }

    #[tokio::test]
    async fn test_scheduler_emits_event_for_unschedulable_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        // Create a node with a label
        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        // Create a pod with a nodeSelector that doesn't match any node
        let mut pod = make_pending_pod("unsched-pod", "default");
        if let Some(ref mut spec) = pod.spec {
            spec.node_selector = Some({
                let mut m = HashMap::new();
                m.insert("disktype".to_string(), "ssd".to_string());
                m
            });
        }
        storage
            .create("/registry/pods/default/unsched-pod", &pod)
            .await
            .unwrap();

        // Run scheduling
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should NOT have a node name
        let unsched_pod: Pod = storage
            .get("/registry/pods/default/unsched-pod")
            .await
            .unwrap();
        assert!(
            unsched_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .is_none(),
            "Unschedulable pod should not be assigned to a node"
        );

        // Pod should have PodScheduled=False condition
        let conditions = unsched_pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref());
        assert!(conditions.is_some(), "Pod should have conditions");
        let has_unschedulable = conditions.unwrap().iter().any(|c| {
            c.condition_type == "PodScheduled"
                && c.status == "False"
                && c.reason.as_deref() == Some("Unschedulable")
        });
        assert!(
            has_unschedulable,
            "Pod should have PodScheduled=False with Unschedulable reason"
        );

        // Should have a FailedScheduling event
        let events: Vec<serde_json::Value> = storage
            .list("/registry/events/default/")
            .await
            .unwrap_or_default();
        let has_failed_event = events.iter().any(|e| {
            e.get("reason")
                .and_then(|r| r.as_str())
                .map(|r| r == "FailedScheduling")
                .unwrap_or(false)
        });
        assert!(
            has_failed_event,
            "Should have a FailedScheduling event for unschedulable pod"
        );
    }

    #[tokio::test]
    async fn test_scheduler_emits_scheduled_event_via_recorder() {
        use rusternetes_common::resources::{Event, EventType, ObjectReference};

        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        let mut pod = make_pending_pod("sched-pod", "default");
        pod.metadata.uid = "sched-pod-uid-1234".to_string();
        storage
            .create("/registry/pods/default/sched-pod", &pod)
            .await
            .unwrap();

        scheduler.schedule_pending_pods().await.unwrap();

        // Pod must be bound.
        let bound: Pod = storage
            .get("/registry/pods/default/sched-pod")
            .await
            .unwrap();
        assert_eq!(
            bound.spec.as_ref().and_then(|s| s.node_name.as_deref()),
            Some("node-1"),
            "pod should be bound to node-1"
        );

        // The Scheduled event must be retrievable at the recorder's STABLE key
        // (object.reason.uid) — proving it was routed through EventRecorder, not
        // the old ad-hoc `{pod}.sched.{timestamp}` write that never deduplicates.
        let involved = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("sched-pod".to_string()),
            uid: Some("sched-pod-uid-1234".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };
        let name = Event::generate_name(&involved, "Scheduled");
        let key = format!("/registry/events/default/{}", name);
        let ev: Event = storage
            .get(&key)
            .await
            .expect("Scheduled event must be stored at the recorder's stable key");

        assert_eq!(ev.reason, "Scheduled");
        assert!(matches!(ev.event_type, EventType::Normal));
        assert_eq!(ev.source.component, "default-scheduler");
        assert_eq!(
            ev.message,
            "Successfully assigned default/sched-pod to node-1"
        );
    }

    /// #3346: with several profiles the scheduler schedules every pod whose
    /// `spec.schedulerName` names a profile (`responsibleForPod`,
    /// eventhandlers.go:474-476), reports each Scheduled event under that
    /// profile's name (`NewRecorderFactory`, profile.go:85-90), and leaves pods
    /// of unknown scheduler names alone.
    #[tokio::test]
    async fn test_multi_profile_routes_pods_by_scheduler_name() {
        use rusternetes_common::resources::{Event, ObjectReference};
        let storage = Arc::new(MemoryStorage::new());
        let profiles = crate::profile::Map::new_map(vec![
            crate::profile::Profile::named("default-scheduler"),
            crate::profile::Profile::named("my-sched"),
        ])
        .unwrap();
        let scheduler = Scheduler::new_with_profiles(storage.clone(), 1, profiles);
        storage
            .create("/registry/nodes/node-1", &make_node("node-1"))
            .await
            .unwrap();
        for (name, sn) in [
            ("p-default", "default-scheduler"),
            ("p-mine", "my-sched"),
            ("p-other", "other-sched"),
        ] {
            let mut pod = make_pending_pod(name, "default");
            pod.metadata.uid = format!("{name}-uid");
            pod.spec.as_mut().unwrap().scheduler_name = Some(sn.to_string());
            storage
                .create(&format!("/registry/pods/default/{name}"), &pod)
                .await
                .unwrap();
        }
        scheduler.schedule_pending_pods().await.unwrap();
        let node_of = |name: &'static str| {
            let storage = storage.clone();
            async move {
                storage
                    .get::<Pod>(&format!("/registry/pods/default/{name}"))
                    .await
                    .unwrap()
                    .spec
                    .and_then(|s| s.node_name)
            }
        };
        assert_eq!(node_of("p-default").await.as_deref(), Some("node-1"));
        assert_eq!(node_of("p-mine").await.as_deref(), Some("node-1"));
        assert_eq!(node_of("p-other").await, None);
        for (name, comp) in [("p-default", "default-scheduler"), ("p-mine", "my-sched")] {
            let involved = ObjectReference {
                kind: Some("Pod".to_string()),
                namespace: Some("default".to_string()),
                name: Some(name.to_string()),
                uid: Some(format!("{name}-uid")),
                api_version: Some("v1".to_string()),
                resource_version: None,
                field_path: None,
            };
            let key = format!(
                "/registry/events/default/{}",
                Event::generate_name(&involved, "Scheduled")
            );
            let ev: Event = storage.get(&key).await.unwrap();
            assert_eq!(ev.source.component, comp, "{name}");
        }
    }

    /// #3346: the `VolumeBinding` bind timeout is read from the pod's own
    /// profile (each profile has its own framework + plugin args).
    #[tokio::test]
    async fn test_profile_for_pod_resolves_per_profile_args() {
        let mut slow = crate::profile::Profile::named("slow");
        slow.volume_bind_timeout = Duration::from_secs(7);
        let profiles = crate::profile::Map::new_map(vec![
            crate::profile::Profile::named("default-scheduler"),
            slow,
        ])
        .unwrap();
        let scheduler = Scheduler::new_with_profiles(Arc::new(MemoryStorage::new()), 1, profiles);
        let mut pod = make_pending_pod("x", "default");
        pod.spec.as_mut().unwrap().scheduler_name = Some("slow".to_string());
        assert_eq!(
            scheduler.profile_for(&pod).unwrap().volume_bind_timeout,
            Duration::from_secs(7)
        );
        pod.spec.as_mut().unwrap().scheduler_name = Some("nope".to_string());
        assert_eq!(
            scheduler.profile_for(&pod).unwrap_err(),
            "profile not found for scheduler name \"nope\""
        );
    }

    /// The compiled-in score blend scales each component by the profile's
    /// weight for its owning plugin (relative to the default weight), and a
    /// plugin not run at Score contributes nothing (`getScoreWeights`).
    #[test]
    fn test_weighted_component_follows_profile_score_weights() {
        use crate::framework_runtime::{Plugin, PluginSet};
        let default_fw = crate::profile::Profile::named("d").framework;
        // Default weights: identical to the unscaled `component * pct / 100`.
        assert_eq!(
            weighted_component(&default_fw, names::NODE_AFFINITY, 80, 20),
            80 * 20 / 100
        );
        assert_eq!(
            weighted_component(&default_fw, names::NODE_RESOURCES_FIT, 77, 25),
            77 * 25 / 100
        );
        // NodeAffinity re-weighted 2 -> 4 doubles its component.
        let mut p = crate::profile::Profile::named("w");
        let mut plugins = p.plugins.take().unwrap();
        plugins.score = PluginSet {
            enabled: vec![Plugin::weighted(names::NODE_AFFINITY, 4)],
            disabled: vec![Plugin::new(names::POD_TOPOLOGY_SPREAD)],
        };
        p.plugins = Some(plugins);
        let m = crate::profile::Map::new_map(vec![p]).unwrap();
        let fw = &m.get("w").unwrap().framework;
        assert_eq!(
            weighted_component(fw, names::NODE_AFFINITY, 80, 20),
            2 * (80 * 20 / 100)
        );
        // Disabled at Score: no contribution.
        assert_eq!(
            weighted_component(fw, names::POD_TOPOLOGY_SPREAD, 50, 10),
            0
        );
    }

    #[tokio::test]
    async fn test_scheduler_does_not_reschedule_already_scheduled_pod() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 1, "default-scheduler".to_string());

        let node1 = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node1)
            .await
            .unwrap();

        // Create a pod that is already scheduled
        let mut pod = make_pending_pod("scheduled-pod", "default");
        if let Some(ref mut spec) = pod.spec {
            spec.node_name = Some("node-1".to_string());
        }
        storage
            .create("/registry/pods/default/scheduled-pod", &pod)
            .await
            .unwrap();

        // Run scheduling — should not touch already-scheduled pod
        scheduler.schedule_pending_pods().await.unwrap();

        let result_pod: Pod = storage
            .get("/registry/pods/default/scheduled-pod")
            .await
            .unwrap();
        assert_eq!(
            result_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref()),
            Some("node-1"),
            "Already-scheduled pod should remain on its node"
        );
    }

    /// Preemption should set deletionTimestamp and DisruptionTarget condition
    /// on evicted pods, but NOT delete them from storage. The kubelet handles
    /// actual cleanup. The conformance test needs to observe the condition.
    #[tokio::test]
    async fn test_preemption_sets_disruption_target_condition() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 2, "default-scheduler".to_string());

        // Create a node
        let node = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node)
            .await
            .unwrap();

        // Create a low-priority pod already scheduled on node-1
        let mut low_pod = make_pending_pod("low-pod", "default");
        low_pod.spec.as_mut().unwrap().node_name = Some("node-1".to_string());
        low_pod.spec.as_mut().unwrap().priority = Some(0);
        low_pod.status = Some(PodStatus {
            phase: Some(Phase::Running),
            message: None,
            reason: None,
            host_ip: None,
            pod_ip: None,
            conditions: None,
            container_statuses: None,
            init_container_statuses: None,
            ephemeral_container_statuses: None,
            start_time: None,
            qos_class: None,
            nominated_node_name: None,
            host_i_ps: None,
            pod_i_ps: None,
            resize: None,
            resource_claim_statuses: None,
            observed_generation: None,
            ..Default::default()
        });
        storage
            .create("/registry/pods/default/low-pod", &low_pod)
            .await
            .unwrap();

        // Evict the pod (private method, accessible from within the module)
        let mut preemptor = make_pending_pod("high-pod", "default");
        preemptor.spec.as_mut().unwrap().scheduler_name = Some("my-sched".to_string());
        scheduler.evict_pod(&preemptor, &low_pod).await.unwrap();

        // The pod should still exist in storage with:
        // 1. deletionTimestamp set
        // 2. DisruptionTarget condition
        // 3. Phase = Failed
        let evicted: Pod = storage
            .get("/registry/pods/default/low-pod")
            .await
            .expect("Evicted pod should still exist in storage (not hard-deleted)");

        assert!(
            evicted.metadata.deletion_timestamp.is_some(),
            "Evicted pod should have deletionTimestamp set"
        );

        let status = evicted.status.as_ref().expect("Should have status");
        assert_eq!(
            status.phase,
            Some(Phase::Failed),
            "Evicted pod phase should be Failed"
        );
        assert_eq!(
            status.reason.as_deref(),
            Some("Preempted"),
            "Evicted pod reason should be Preempted"
        );

        let conditions = status.conditions.as_ref().expect("Should have conditions");
        let disruption = conditions
            .iter()
            .find(|c| c.condition_type == "DisruptionTarget");
        assert!(
            disruption.is_some(),
            "Evicted pod must have DisruptionTarget condition"
        );
        let dt = disruption.unwrap();
        assert_eq!(dt.status, "True");
        // preemption.go:191: the PREEMPTOR's scheduler name, not a global one.
        assert_eq!(
            dt.message.as_deref(),
            Some("my-sched: preempting to accommodate a higher priority pod")
        );
        assert_eq!(dt.reason.as_deref(), Some("PreemptionByScheduler"));
    }

    /// #2477: evicting a victim deletes that namespace's pod only. Upstream
    /// `util.DeletePod` deletes `Pods(pod.Namespace)` / `pod.Name`
    /// (pkg/scheduler/util/utils.go:137-138); a name-only scan evicted the
    /// same-named pod in every namespace.
    #[tokio::test]
    async fn test_evict_pod_leaves_same_named_pod_in_other_namespace() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 2, "default-scheduler".to_string());
        for ns in ["ns-a", "ns-b"] {
            let mut p = make_pending_pod("w", ns);
            p.spec.as_mut().unwrap().node_name = Some("node-1".to_string());
            storage
                .create(&format!("/registry/pods/{ns}/w"), &p)
                .await
                .unwrap();
        }
        let victim: Pod = storage.get("/registry/pods/ns-a/w").await.unwrap();

        let preemptor = make_pending_pod("high-pod", "ns-c");
        scheduler.evict_pod(&preemptor, &victim).await.unwrap();

        let a: Pod = storage.get("/registry/pods/ns-a/w").await.unwrap();
        let b: Pod = storage.get("/registry/pods/ns-b/w").await.unwrap();
        assert!(a.metadata.deletion_timestamp.is_some(), "victim evicted");
        assert!(
            b.metadata.deletion_timestamp.is_none(),
            "same-named pod in another namespace must be untouched"
        );
    }

    /// K8s treats nodeName="" the same as nodeName=nil (unscheduled).
    /// Some Go JSON serialization produces "nodeName":"" in pod templates.
    /// The scheduler MUST consider these pods as pending and schedule them.
    /// K8s ref: pkg/scheduler/schedule_one.go — pod is unscheduled if
    /// len(pod.Spec.NodeName) == 0
    #[tokio::test]
    async fn test_empty_node_name_treated_as_unscheduled() {
        let storage = Arc::new(MemoryStorage::new());
        let scheduler =
            Scheduler::new_with_name(storage.clone(), 5, "default-scheduler".to_string());

        // Create a node
        let node = make_node("node-1");
        storage
            .create("/registry/nodes/node-1", &node)
            .await
            .unwrap();

        // Create a pod with nodeName="" (empty string, not None)
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-pod").with_namespace("default"),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "test".to_string(),
                    image: "busybox".to_string(),
                    ..Default::default()
                }],
                // Key: nodeName is Some("") — empty string, not None
                node_name: Some(String::new()),
                ..Default::default()
            }),
            status: Some(PodStatus {
                phase: Some(Phase::Pending),
                ..Default::default()
            }),
        };
        storage
            .create("/registry/pods/default/test-pod", &pod)
            .await
            .unwrap();

        // Schedule
        scheduler.schedule_pending_pods().await.unwrap();

        // Pod should be scheduled to node-1
        let scheduled: Pod = storage
            .get("/registry/pods/default/test-pod")
            .await
            .unwrap();
        assert!(
            scheduled
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .is_some_and(|n| !n.is_empty()),
            "Pod with nodeName='' must be treated as unscheduled and get assigned a node"
        );
    }
}
