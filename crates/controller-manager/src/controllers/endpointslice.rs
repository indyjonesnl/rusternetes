use super::endpointslice_tracker::EndpointSliceTracker;
use anyhow::Result;
use futures::StreamExt;
use rusternetes_common::resources::endpointslice::{
    Endpoint, EndpointConditions, EndpointPort, EndpointReference,
};
use rusternetes_common::resources::{EndpointSlice, Endpoints, Pod, Service};
use rusternetes_common::types::Phase;
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WatchEvent, WorkQueue};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// How often the watch loop re-enqueues every Service/Endpoints as a safety
/// net. Upstream has no such controller-level sweep: it is purely event
/// driven, and only its informers resync, every `MinResyncPeriod`
/// (`staging/src/k8s.io/controller-manager/config/v1alpha1/defaults.go:31-33`,
/// 12h; `ResyncPeriod` in `cmd/kube-controller-manager/app/controllermanager.go:176-181`
/// multiplies it by a random 1-2 factor, see [`resync_period`]). Each
/// resync re-fires the service handler, `onServiceUpdate`
/// (`endpointslice_controller.go:122-126`). This replaces a 5s sweep (#2208).
/// Carry the identity of the stored slice over to the slice about to replace it.
///
/// Upstream updates `existingSlice.DeepCopy()` with the new endpoints and
/// labels (staging/src/k8s.io/endpointslice/reconciler.go:551, :559), so the
/// object it PUTs keeps the stored UID. The API server turns a non-empty
/// `metadata.uid` on an update into a UID precondition
/// (`defaultUpdatedObjectInfo.Preconditions`,
/// staging/src/k8s.io/apiserver/pkg/registry/rest/update.go:188-203), so a
/// freshly built slice carrying a new random UID is a permanent 409 once
/// EndpointSlice is served by the generic Store (#2108).
fn adopt_existing_identity(slice: &mut EndpointSlice, existing: &EndpointSlice) {
    slice.metadata.uid = existing.metadata.uid.clone();
    slice.metadata.creation_timestamp = existing.metadata.creation_timestamp;
    slice.metadata.resource_version = existing.metadata.resource_version.clone();
}

const MANAGED_BY_LABEL: &str = "endpointslice.kubernetes.io/managed-by";
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
const CONTROLLER_NAME: &str = "endpointslice-controller.k8s.io";

/// `endpointSliceChangeMinSyncDelay`
/// (pkg/controller/endpointslice/endpointslice_controller.go:69).
const ENDPOINT_SLICE_CHANGE_MIN_SYNC_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// `ManagedByController` (staging/src/k8s.io/endpointslice/reconciler.go:666-669).
fn managed_by_controller(slice: &EndpointSlice) -> bool {
    slice
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(MANAGED_BY_LABEL))
        .map(String::as_str)
        == Some(CONTROLLER_NAME)
}

/// `ServiceControllerKey` (staging/src/k8s.io/endpointslice/utils.go:201-210,
/// `ns/<service-name label>`; no label -> error -> nothing queued), in the
/// `services/ns/name` form this controller's queue uses.
fn service_controller_key(slice: &EndpointSlice) -> Option<String> {
    let svc = slice
        .metadata
        .labels
        .as_ref()?
        .get(SERVICE_NAME_LABEL)
        .filter(|s| !s.is_empty())?;
    Some(format!(
        "services/{}/{}",
        slice.metadata.namespace.as_deref().unwrap_or(""),
        svc
    ))
}

/// What the informer's indexer remembers of a slice, for
/// `onEndpointSliceUpdate`'s `prevObj`: only the labels the handler compares.
#[derive(Clone, Debug, PartialEq)]
struct SliceLabels {
    service_name: Option<String>,
    managed_by_controller: bool,
}

impl SliceLabels {
    fn of(slice: &EndpointSlice) -> Self {
        Self {
            service_name: slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(SERVICE_NAME_LABEL))
                .cloned(),
            managed_by_controller: managed_by_controller(slice),
        }
    }
}

/// Port of `onEndpointSliceAdd` / `onEndpointSliceUpdate` /
/// `onEndpointSliceDelete` (endpointslice_controller.go:542-592): the
/// Services to queue (`queueServiceForEndpointSlice`, :596-610) for one slice
/// watch event. `known` is the informer cache of previous label state, updated
/// here as the indexer is before handlers fire.
fn services_to_queue_for_slice_event(
    tracker: &EndpointSliceTracker,
    known: &mut HashMap<String, SliceLabels>,
    event: &WatchEvent,
) -> Vec<String> {
    let (key, value) = match event {
        WatchEvent::Added(k, v) | WatchEvent::Modified(k, v) | WatchEvent::Deleted(k, v) => (k, v),
    };
    let Ok(slice) = serde_json::from_str::<EndpointSlice>(value) else {
        return vec![];
    };
    let current = SliceLabels::of(&slice);
    let add_path = |slice: &EndpointSlice| -> Vec<String> {
        if managed_by_controller(slice) && tracker.should_sync(slice) {
            service_controller_key(slice).into_iter().collect()
        } else {
            vec![]
        }
    };
    match event {
        WatchEvent::Added(..) => {
            known.insert(key.clone(), current);
            add_path(&slice)
        }
        WatchEvent::Modified(..) => {
            let Some(prev) = known.insert(key.clone(), current.clone()) else {
                // Never seen (cannot happen after the initial LIST): an add.
                return add_path(&slice);
            };
            // Slice generation does not change when labels change. Although
            // the controller will never change LabelServiceName, users might
            // (:564-574): queue both the new and the previous Service.
            if current.service_name != prev.service_name {
                let mut keys: Vec<String> = service_controller_key(&slice).into_iter().collect();
                let mut prev_slice = slice.clone();
                prev_slice.metadata.labels = Some(
                    prev.service_name
                        .iter()
                        .map(|n| (SERVICE_NAME_LABEL.to_string(), n.clone()))
                        .collect(),
                );
                keys.extend(service_controller_key(&prev_slice));
                return keys;
            }
            if current.managed_by_controller != prev.managed_by_controller
                || (current.managed_by_controller && tracker.should_sync(&slice))
            {
                return service_controller_key(&slice).into_iter().collect();
            }
            vec![]
        }
        WatchEvent::Deleted(..) => {
            known.remove(key);
            // `HandleDeletion` returns false if we did not expect the slice
            // to be deleted; then the Service needs another sync (:585-591).
            if managed_by_controller(&slice)
                && tracker.has(&slice)
                && !tracker.handle_deletion(&slice)
            {
                return service_controller_key(&slice).into_iter().collect();
            }
            vec![]
        }
    }
}

const INFORMER_RESYNC_PERIOD: std::time::Duration = std::time::Duration::from_secs(12 * 3600);

/// Port of `ResyncPeriod` (`cmd/kube-controller-manager/app/controllermanager.go:176-181`):
/// `MinResyncPeriod * (rand.Float64() + 1)`, so controllers do not resync in
/// lock-step. Drawn once per informer start, as upstream calls the returned
/// function once per informer.
fn resync_period() -> std::time::Duration {
    use rand::Rng;
    INFORMER_RESYNC_PERIOD.mul_f64(rand::rng().random::<f64>() + 1.0)
}

type LabelSet = HashMap<String, String>;

/// Port of `endpointsliceutil.PodProjectionKey`
/// (`staging/src/k8s.io/endpointslice/util/controller_utils.go:48-53`): all pod
/// information needed to find the services that may need an update.
#[derive(Debug, Clone, PartialEq, Default)]
struct PodProjectionKey {
    namespace: String,
    /// The pod's current labels.
    labels: LabelSet,
    /// Set if the pod's labels changed (an Update event).
    old_labels: Option<LabelSet>,
    /// Set if the pod changed in a way that may affect endpoint membership.
    pod_changed: bool,
}

/// Port of `podEndpointsChanged` (`controller_utils.go`, same file): returns
/// `(podChanged, labelsChanged)`.
fn pod_endpoints_changed(old: &Pod, new: &Pod) -> (bool, bool) {
    let hostname = |p: &Pod| p.spec.as_ref().and_then(|s| s.hostname.clone());
    let subdomain = |p: &Pod| p.spec.as_ref().and_then(|s| s.subdomain.clone());
    // `reflect.DeepEqual(newPod.Labels, oldPod.Labels)`: nil and empty maps
    // are unequal in Go; here both decode alike, so treat them as equal.
    let labels = |p: &Pod| p.metadata.labels.clone().unwrap_or_default();
    let labels_changed = labels(new) != labels(old)
        || hostname(new) != hostname(old)
        || subdomain(new) != subdomain(old);

    // `newPod.DeletionTimestamp != oldPod.DeletionTimestamp` compares
    // *pointers*, and informer objects are distinct allocations, so it is true
    // whenever either timestamp is set.
    if new.metadata.deletion_timestamp.is_some() || old.metadata.deletion_timestamp.is_some() {
        return (true, labels_changed);
    }
    if rusternetes_common::podutil::is_pod_ready(old)
        != rusternetes_common::podutil::is_pod_ready(new)
    {
        return (true, labels_changed);
    }
    let ips = |p: &Pod| -> Vec<String> {
        p.status
            .as_ref()
            .and_then(|s| s.pod_i_ps.as_ref())
            .map(|v| v.iter().map(|i| i.ip.clone()).collect())
            .unwrap_or_default()
    };
    if ips(old) != ips(new) {
        return (true, labels_changed);
    }
    (false, labels_changed)
}

/// Port of `GetPodUpdateProjectionKey` (`controller_utils.go:58-103`).
/// `old` is `None` for an add, `new` is `None` for a delete.
fn get_pod_update_projection_key(old: Option<&Pod>, new: Option<&Pod>) -> Option<PodProjectionKey> {
    let key_of = |p: &Pod| PodProjectionKey {
        namespace: p.metadata.namespace.clone().unwrap_or_default(),
        labels: p.metadata.labels.clone().unwrap_or_default(),
        ..Default::default()
    };
    let (old, new) = match (old, new) {
        (None, None) => return None,
        (None, Some(n)) => return Some(key_of(n)),
        (Some(o), None) => return Some(key_of(o)),
        (Some(o), Some(n)) => (o, n),
    };
    // "Safe to ignore pod informer resync events as service informer already
    // handles resync for all services." (controller_utils.go:79). Go compares
    // strings, so two unset resourceVersions are equal too.
    if old.metadata.resource_version == new.metadata.resource_version {
        return None;
    }
    let (pod_changed, labels_changed) = pod_endpoints_changed(old, new);
    if !pod_changed && !labels_changed {
        return None;
    }
    if !labels_changed {
        return Some(key_of(new));
    }
    Some(PodProjectionKey {
        old_labels: Some(old.metadata.labels.clone().unwrap_or_default()),
        pod_changed,
        ..key_of(new)
    })
}

/// Port of `determineNeededServiceUpdates` (`controller_utils.go:268-278`).
fn determine_needed_service_updates(
    old_services: &std::collections::HashSet<String>,
    services: &std::collections::HashSet<String>,
    pod_changed: bool,
) -> std::collections::HashSet<String> {
    if pod_changed {
        // the labels and pod changed: all services need to be updated
        services.union(old_services).cloned().collect()
    } else {
        // only the labels changed: the symmetric difference
        services
            .symmetric_difference(old_services)
            .cloned()
            .collect()
    }
}

/// Port of `GetServicesToUpdate` + `getServicesForPod` (`controller_utils.go`),
/// over an already-listed namespace's services. Returns work-queue keys.
fn get_services_to_update(
    services: &[Service],
    key: &PodProjectionKey,
) -> std::collections::HashSet<String> {
    let matching = |labels: &LabelSet| -> std::collections::HashSet<String> {
        services
            .iter()
            .filter_map(|svc| {
                // "a nil selector means selectors match nothing, not everything"
                let selector = svc.spec.selector.as_ref()?;
                selector
                    .iter()
                    .all(|(k, v)| labels.get(k) == Some(v))
                    .then(|| format!("services/{}/{}", key.namespace, svc.metadata.name))
            })
            .collect()
    };
    let current = matching(&key.labels);
    match &key.old_labels {
        Some(old) => determine_needed_service_updates(&matching(old), &current, key.pod_changed),
        None => current,
    }
}

/// Pods by namespace then name: the stand-in for upstream's shared pod
/// informer cache (`c.podLister`, `endpointslice_controller.go:137,208-210`).
type PodSnapshot = HashMap<String, HashMap<String, Pod>>;

/// EndpointSliceController builds EndpointSlices directly from Services and Pods,
/// following the same approach as the K8s endpointslice controller.
///
/// Key behavior (matching K8s):
/// - Iterates over Services with selectors
/// - Finds matching pods by label selector
/// - For each pod, computes which service ports the pod actually serves
///   using FindPort logic (matching containerPort to service targetPort)
/// - Groups pods by their port mapping
/// - Creates separate EndpointSlices for each port group
///
/// This ensures that pods only appear in EndpointSlices with the ports
/// they actually serve, fixing the conformance test failure where pods
/// were incorrectly associated with all service ports.
/// Workers draining the service queue (and services reconciled at once by the
/// `reconcile_all` sweep).
///
/// Upstream's `ConcurrentServiceEndpointSyncs` default
/// (pkg/controller/endpointslice/config/v1alpha1/defaults.go:35), the number
/// of workers draining its service workqueue. Tunable there from 1 to 50 via
/// `--concurrent-service-endpoint-syncs`
/// (cmd/kube-controller-manager/app/options/endpointslicecontroller.go:28-45).
const CONCURRENT_SERVICE_ENDPOINT_SYNCS: usize = 5;

pub struct EndpointSliceController<S: Storage> {
    storage: Arc<S>,
    /// Snapshot of every pod, shared by all service workers and kept current
    /// from the pod watch, as upstream's `podLister` is by the pod informer
    /// (`endpointslice_controller.go:137,410`). `None` until the initial LIST
    /// of a (re)connect has landed; `reconcile_service` then falls back to its
    /// own LIST (direct callers such as tests, and the window before sync --
    /// upstream instead blocks workers on `WaitForNamedCacheSync`, :294).
    pod_cache: Arc<std::sync::RwLock<Option<PodSnapshot>>>,
    /// `c.podsSynced` (`endpointslice_controller.go:294`): flips to `true`
    /// once the first pod LIST has landed and never goes back, like
    /// `HasSynced`. `run` workers do not start syncing before it.
    pods_synced: tokio::sync::watch::Sender<bool>,
    /// `c.podQueue` (`endpointslice_controller.go:247-250`): projection keys
    /// from pod events, resolved to services by `pod_queue_worker`. Upstream
    /// keys it by pointer, so every key is distinct and none is deduplicated;
    /// an unbounded channel has the same semantics.
    pod_queue_tx: tokio::sync::mpsc::UnboundedSender<PodQueueItem>,
    pod_queue_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<PodQueueItem>>>,
    /// `c.endpointSliceTracker` (`endpointslice_controller.go:154,224`).
    slice_tracker: EndpointSliceTracker,
}

/// A `podQueue` entry plus its `NumRequeues` count (`handlePodErr`).
struct PodQueueItem {
    key: PodProjectionKey,
    requeues: u32,
}

/// `maxRetries` (`endpointslice_controller.go`): how often `handlePodErr`
/// requeues a failing pod key before dropping it.
const POD_QUEUE_MAX_RETRIES: u32 = 15;

impl<S: Storage + 'static> EndpointSliceController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        let (pod_queue_tx, pod_queue_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            storage,
            pod_cache: Arc::new(std::sync::RwLock::new(None)),
            pods_synced: tokio::sync::watch::channel(false).0,
            pod_queue_tx,
            pod_queue_rx: Arc::new(tokio::sync::Mutex::new(pod_queue_rx)),
            slice_tracker: EndpointSliceTracker::new(),
        }
    }

    /// Initial/relist half of the pod informer: replace the snapshot with one
    /// LIST of every pod.
    async fn sync_pod_cache(&self) -> bool {
        match self.storage.list::<Pod>(&build_prefix("pods", None)).await {
            Ok(pods) => {
                let mut snapshot = PodSnapshot::new();
                for pod in pods {
                    let ns = pod.metadata.namespace.clone().unwrap_or_default();
                    snapshot
                        .entry(ns)
                        .or_default()
                        .insert(pod.metadata.name.clone(), pod);
                }
                *self.pod_cache.write().unwrap() = Some(snapshot);
                self.pods_synced.send_replace(true);
                true
            }
            Err(e) => {
                tracing::error!("Failed to list pods for the pod snapshot: {}", e);
                *self.pod_cache.write().unwrap() = None;
                false
            }
        }
    }

    /// Watch half of the pod informer: fold one event into the snapshot.
    /// Must run before the event is turned into service enqueues, so a worker
    /// woken by it sees the new pod (the informer indexer is updated before
    /// handlers fire).
    ///
    /// Returns `(old, new)` as the informer's update handler sees them: `old`
    /// is the snapshot's previous copy (`None` for an add), `new` is `None`
    /// for a delete.
    fn apply_pod_event(
        &self,
        event: &rusternetes_storage::WatchEvent,
    ) -> (Option<Pod>, Option<Pod>) {
        use rusternetes_storage::WatchEvent;
        let (value, deleted) = match event {
            WatchEvent::Added(_, v) | WatchEvent::Modified(_, v) => (v, false),
            WatchEvent::Deleted(_, v) => (v, true),
        };
        let Ok(pod) = serde_json::from_str::<Pod>(value) else {
            return (None, None);
        };
        let ns = pod.metadata.namespace.clone().unwrap_or_default();
        let mut guard = self.pod_cache.write().unwrap();
        let previous = guard
            .as_ref()
            .and_then(|snap| snap.get(&ns))
            .and_then(|pods| pods.get(&pod.metadata.name))
            .cloned();
        if let Some(snapshot) = guard.as_mut() {
            if deleted {
                if let Some(pods) = snapshot.get_mut(&ns) {
                    pods.remove(&pod.metadata.name);
                }
            } else {
                snapshot
                    .entry(ns)
                    .or_default()
                    .insert(pod.metadata.name.clone(), pod.clone());
            }
        }
        if deleted {
            (Some(pod), None)
        } else {
            (previous, Some(pod))
        }
    }

    /// `c.podLister.Pods(ns).List(labels.Everything())` against the snapshot;
    /// `None` when the snapshot is not synced.
    fn cached_pods(&self, namespace: &str) -> Option<Vec<Pod>> {
        let guard = self.pod_cache.read().unwrap();
        let snapshot = guard.as_ref()?;
        Some(
            snapshot
                .get(namespace)
                .map(|pods| pods.values().cloned().collect())
                .unwrap_or_default(),
        )
    }

    /// Watch-based run loop. Watches services, pods, AND endpoints as primary resources.
    /// When a pod changes, we find services whose selector matches the pod
    /// and enqueue them for reconciliation.
    /// When an endpoint changes, we enqueue it for mirroring (services without selectors).
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();
        // Separate queue for endpoints mirroring
        let mirror_queue = WorkQueue::new();

        // A pool of workers over one queue, as upstream does: `Run` starts
        // `workers` goroutines each looping `processNextServiceWorkItem` over
        // `serviceQueue` (pkg/controller/endpointslice/
        // endpointslice_controller.go, `Run`), with workers defaulting to
        // `ConcurrentServiceEndpointSyncs` = 5
        // (endpointslice/config/v1alpha1/defaults.go:35). WorkQueue never
        // hands one key to two workers (see `WorkQueue::get`). A single worker
        // let the 5s resync walk N services serially (#1869).
        for _ in 0..CONCURRENT_SERVICE_ENDPOINT_SYNCS {
            let worker_queue = queue.clone();
            let worker_self = Arc::clone(&self);
            tokio::spawn(async move {
                // `cache.WaitForNamedCacheSyncWithContext(ctx, c.podsSynced, ...)`
                // before any worker starts (endpointslice_controller.go:294).
                worker_self.wait_for_pods_synced().await;
                worker_self.worker(worker_queue).await;
            });
            // `wait.Until(c.podQueueWorker ...)` runs beside each service
            // worker (endpointslice_controller.go:301-305).
            let pod_worker_queue = queue.clone();
            let pod_worker_self = Arc::clone(&self);
            tokio::spawn(async move {
                pod_worker_self.wait_for_pods_synced().await;
                pod_worker_self.pod_queue_worker(pod_worker_queue).await;
            });
        }

        let mirror_worker_queue = mirror_queue.clone();
        let mirror_worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            mirror_worker_self.mirror_worker(mirror_worker_queue).await;
        });

        loop {
            // Unsynced until this connect's LIST lands; workers fall back to
            // their own LIST meanwhile.
            *self.pod_cache.write().unwrap() = None;

            let svc_prefix = build_prefix("services", None);
            let pod_prefix = build_prefix("pods", None);
            let ep_prefix = build_prefix("endpoints", None);

            let svc_watch = match self.storage.watch(&svc_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish service watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            let pod_watch = match self.storage.watch(&pod_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish pod watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            let slice_watch = match self
                .storage
                .watch(&build_prefix("endpointslices", None))
                .await
            {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish endpointslice watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            let ep_watch = match self.storage.watch(&ep_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish endpoints watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Informer order: watches are open first, then the LIST, so no
            // pod event falls between them; buffered events replay on top of
            // the snapshot. Only then enqueue (relist -> handlers fire).
            if !self.sync_pod_cache().await {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            self.enqueue_all(&queue).await;
            self.enqueue_all_endpoints(&mirror_queue).await;

            let mut svc_watch = svc_watch;
            let mut pod_watch = pod_watch;
            let mut ep_watch = ep_watch;
            let mut slice_watch = slice_watch;
            // The slice informer's indexer: previous labels for update events.
            let mut known_slices: HashMap<String, SliceLabels> = self
                .storage
                .list::<EndpointSlice>(&build_prefix("endpointslices", None))
                .await
                .unwrap_or_default()
                .iter()
                .map(|s| {
                    let key = build_key(
                        "endpointslices",
                        s.metadata.namespace.as_deref(),
                        &s.metadata.name,
                    );
                    (key, SliceLabels::of(s))
                })
                .collect();
            let mut resync = tokio::time::interval(resync_period());
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = svc_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Service watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Service watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = pod_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let (old, new) = self.apply_pod_event(&ev);
                                self.on_pod_update(old.as_ref(), new.as_ref());
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Pod watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Pod watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = slice_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                for key in services_to_queue_for_slice_event(
                                    &self.slice_tracker,
                                    &mut known_slices,
                                    &ev,
                                ) {
                                    queue
                                        .add_after(key, ENDPOINT_SLICE_CHANGE_MIN_SYNC_DELAY)
                                        .await;
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!("EndpointSlice watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("EndpointSlice watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = ep_watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                // Enqueue the endpoint for mirroring
                                let key = extract_key(&ev);
                                // Convert endpoints/ns/name to same format
                                mirror_queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Endpoints watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Endpoints watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                        self.enqueue_all_endpoints(&mirror_queue).await;
                    }
                }
            }
        }
    }

    /// Blocks until the first pod LIST has landed: `WaitForNamedCacheSync`
    /// (`endpointslice_controller.go:294`).
    async fn wait_for_pods_synced(&self) {
        let mut rx = self.pods_synced.subscribe();
        let _ = rx.wait_for(|synced| *synced).await;
    }

    /// `onPodUpdate` (`endpointslice_controller.go:529-535`): project the pod
    /// event to a [`PodProjectionKey`] and put it on `podQueue`; a
    /// `pod_queue_worker` finds the matching services later.
    fn on_pod_update(&self, old: Option<&Pod>, new: Option<&Pod>) {
        if let Some(key) = get_pod_update_projection_key(old, new) {
            let _ = self.pod_queue_tx.send(PodQueueItem { key, requeues: 0 });
        }
    }

    /// `podQueueWorker` / `processNextPodWorkItem` / `handlePodErr`
    /// (`endpointslice_controller.go:456-489`).
    async fn pod_queue_worker(&self, service_queue: WorkQueue) {
        loop {
            // Hold the receiver lock only while waiting, as `podQueue.Get`.
            let item = self.pod_queue_rx.lock().await.recv().await;
            let Some(mut item) = item else { return };
            match self.sync_pod(&item.key, &service_queue).await {
                Ok(()) => {}
                Err(e) if item.requeues < POD_QUEUE_MAX_RETRIES => {
                    tracing::debug!("Error syncing pod, retrying: {}", e);
                    // AddRateLimited: DefaultTypedControllerRateLimiter's
                    // per-item exponential backoff (5ms base, doubling).
                    let delay = std::time::Duration::from_millis(5u64 << item.requeues.min(10));
                    item.requeues += 1;
                    let tx = self.pod_queue_tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send(item);
                    });
                }
                Err(e) => tracing::error!("Dropping pod out of the queue: {}", e),
            }
        }
    }

    /// `syncPod` (`endpointslice_controller.go:491-508`): resolve the key to
    /// the services it affects and queue exactly those, instead of every
    /// service in the namespace.
    async fn sync_pod(&self, key: &PodProjectionKey, service_queue: &WorkQueue) -> Result<()> {
        let services = self
            .storage
            .list::<Service>(&build_prefix("services", Some(&key.namespace)))
            .await?;
        for svc_key in get_services_to_update(&services, key) {
            // `AddAfter(service, c.endpointUpdatesBatchPeriod)`: the period
            // defaults to 0 (kube-controller-manager config), i.e. `Add`.
            service_queue.add(svc_key).await;
        }
        Ok(())
    }

    /// Main reconciliation loop — syncs EndpointSlices for all Services
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            let storage_key = build_key("services", Some(ns), name);
            match self.storage.get::<Service>(&storage_key).await {
                Ok(service) => match self.reconcile_service(&service).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        tracing::error!("Failed to reconcile {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                },
                Err(_) => {
                    // `c.endpointSliceTracker.DeleteService`
                    // (endpointslice_controller.go:390).
                    self.slice_tracker.delete_service(ns, name);
                    queue.forget(&key).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Worker for mirroring Endpoints to EndpointSlices.
    /// K8s has a separate endpointslice-mirroring-controller that watches Endpoints
    /// and creates EndpointSlices for services without selectors and standalone Endpoints.
    async fn mirror_worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            match self.mirror_endpoint(ns, name).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    tracing::error!("Failed to mirror endpoint {}/{}: {}", ns, name, e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    /// Mirror a single Endpoints resource into an EndpointSlice.
    /// Only mirrors Endpoints for services without selectors or standalone Endpoints.
    /// K8s ref: pkg/controller/endpointslicemirroring/reconciler.go
    async fn mirror_endpoint(&self, ns: &str, name: &str) -> Result<()> {
        let ep_key = build_key("endpoints", Some(ns), name);

        // Check if the Endpoints resource still exists
        let ep: Endpoints = match self.storage.get(&ep_key).await {
            Ok(ep) => ep,
            Err(_) => {
                // Endpoints deleted — clean up mirrored EndpointSlices
                let es_prefix = build_prefix("endpointslices", Some(ns));
                let existing_slices: Vec<EndpointSlice> =
                    self.storage.list(&es_prefix).await.unwrap_or_default();
                for slice in &existing_slices {
                    let managed = slice
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                        .map(|m| m == "endpointslice-mirroring-controller.k8s.io")
                        .unwrap_or(false);
                    let owned = slice
                        .metadata
                        .labels
                        .as_ref()
                        .and_then(|l| l.get("kubernetes.io/service-name"))
                        .map(|n| n == name)
                        .unwrap_or(false);
                    if managed && owned {
                        let slice_key = build_key("endpointslices", Some(ns), &slice.metadata.name);
                        let _ = self.storage.delete(&slice_key).await;
                        debug!(
                            "Deleted mirrored EndpointSlice {}/{} (source Endpoints deleted)",
                            ns, slice.metadata.name
                        );
                    }
                }
                return Ok(());
            }
        };

        // Skip Endpoints that have the skip-mirror label
        // K8s ref: endpointslicemirroring/utils.go — shouldMirror()
        if let Some(ref labels) = ep.metadata.labels {
            if labels.get("endpointslice.kubernetes.io/skip-mirror") == Some(&"true".to_string()) {
                return Ok(());
            }
        }

        // Check if a Service with a SELECTOR exists for this name.
        // If so, the main endpointslice controller handles it, skip mirroring.
        let svc_key = build_key("services", Some(ns), name);
        if let Ok(svc) = self.storage.get::<Service>(&svc_key).await {
            let has_selector = svc
                .spec
                .selector
                .as_ref()
                .map(|sel| !sel.is_empty())
                .unwrap_or(false);
            if has_selector {
                return Ok(());
            }
        }

        // Mirror the Endpoints to EndpointSlice(s).
        // Use a unique slice name with "-mirror" suffix to avoid conflicts
        // with selector-based EndpointSlices that use the service name directly.
        let endpointslices = EndpointSlice::from_endpoints(&ep);

        // If Endpoints has empty subsets, still create at least one empty EndpointSlice
        // so the test can find it. K8s mirroring controller always creates at least
        // one EndpointSlice per mirrored Endpoints object.
        let slices_to_create = if endpointslices.is_empty() {
            let mut empty_slice = EndpointSlice::new(name, "IPv4");
            empty_slice.metadata.namespace = Some(ns.to_string());
            vec![empty_slice]
        } else {
            endpointslices
        };

        // Mirror under the bare endpoints name when free; otherwise suffix
        // with "-mirrored" to avoid colliding with a selector-based slice.
        let bare_owned_by_selector = self.slice_owned_by_selector_controller(ns, name).await;

        for (idx, mut slice) in slices_to_create.into_iter().enumerate() {
            let slice_name = Self::mirrored_slice_name(name, idx, bare_owned_by_selector);
            slice.metadata.name = slice_name.clone();
            slice.metadata.namespace = Some(ns.to_string());

            let labels = slice.metadata.labels.get_or_insert_with(Default::default);
            labels.insert("kubernetes.io/service-name".to_string(), name.to_string());
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-mirroring-controller.k8s.io".to_string(),
            );

            slice.metadata.owner_references =
                Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Endpoints".to_string(),
                    name: name.to_string(),
                    uid: ep.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);

            let slice_key = build_key("endpointslices", Some(ns), &slice_name);
            match self.storage.get::<EndpointSlice>(&slice_key).await {
                Ok(existing) => {
                    if existing.endpoints == slice.endpoints && existing.ports == slice.ports {
                        continue;
                    }
                    adopt_existing_identity(&mut slice, &existing);
                    match self.storage.update(&slice_key, &slice).await {
                        Ok(_) => {
                            debug!("Updated mirrored EndpointSlice {}/{}", ns, slice_name);
                        }
                        Err(e) => {
                            debug!(
                                "Failed to update mirrored EndpointSlice {}/{}: {}",
                                ns, slice_name, e
                            );
                        }
                    }
                }
                Err(_) => {
                    // Slice doesn't exist — create it
                    slice.metadata.resource_version = None;
                    match self.storage.create(&slice_key, &slice).await {
                        Ok(_) => {
                            info!("Created mirrored EndpointSlice {}/{}", ns, slice_name);
                        }
                        Err(e) => {
                            debug!(
                                "Failed to create mirrored EndpointSlice {}/{}: {}",
                                ns, slice_name, e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        match self.storage.list::<Service>("/registry/services/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("services/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to list services for enqueue: {}", e);
            }
        }
    }

    /// Enqueue all Endpoints for mirroring
    async fn enqueue_all_endpoints(&self, queue: &WorkQueue) {
        match self.storage.list::<Endpoints>("/registry/endpoints/").await {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("endpoints/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to list endpoints for enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting endpointslice reconciliation");

        // List all services across all namespaces
        let services: Vec<Service> = self
            .storage
            .list(&build_prefix("services", None))
            .await
            .unwrap_or_default();

        // Track which service names we've seen for orphan cleanup
        let mut service_names: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        // Group by namespace so the namespace's pods are fetched once and
        // reused by every service in it.
        //
        // This used to LIST every pod in the namespace per service. Measured on
        // a live kine cluster after a conformance run: 154 `latency-svc-*`
        // services in one namespace, walked at ~1 service per 1.2s. The sweep
        // could not finish inside its own 5s resync, kept the storage backend
        // at ~1013% CPU serving the repeated LISTs, and starved the rest of the
        // controller-manager badly enough that 335 namespaces sat in
        // Terminating with zero drain progress over 60s.
        //
        // Upstream reads pods from a shared informer cache and reconciles
        // services from a rate-limited workqueue with
        // `ConcurrentServiceEndpointSyncs` = 5 workers
        // (endpointslice/config/v1alpha1/defaults.go:35). We are still a sweep
        // rather than event-driven, but we no longer pay a LIST per service and
        // no longer walk them one at a time.
        let mut by_namespace: std::collections::BTreeMap<&str, Vec<&Service>> =
            std::collections::BTreeMap::new();
        for service in &services {
            let ns = service.metadata.namespace.as_deref().unwrap_or("default");
            service_names.insert((ns.to_string(), service.metadata.name.clone()));
            by_namespace.entry(ns).or_default().push(service);
        }

        for (ns, ns_services) in by_namespace {
            let all_pods: Vec<Pod> = self
                .storage
                .list(&build_prefix("pods", Some(ns)))
                .await
                .unwrap_or_default();

            for chunk in ns_services.chunks(CONCURRENT_SERVICE_ENDPOINT_SYNCS) {
                let results = futures::future::join_all(
                    chunk
                        .iter()
                        .map(|service| self.reconcile_service_with_pods(service, &all_pods)),
                )
                .await;
                for (service, result) in chunk.iter().zip(results) {
                    if let Err(e) = result {
                        error!(
                            "Failed to reconcile endpointslices for service {}/{}: {}",
                            ns, service.metadata.name, e
                        );
                    }
                }
            }
        }

        // Mirror Endpoints that don't have a corresponding Service.
        // K8s has a separate EndpointSlice mirroring controller for this.
        // This handles manually-created Endpoints and headless services without selectors.
        let all_endpoints: Vec<Endpoints> = self
            .storage
            .list(&build_prefix("endpoints", None))
            .await
            .unwrap_or_default();

        for ep in &all_endpoints {
            let ns = ep.metadata.namespace.as_deref().unwrap_or("default");
            let ep_name = &ep.metadata.name;

            // Skip if a Service with a SELECTOR exists (handled by the main loop above).
            // Services WITHOUT selectors need manual endpoint management via mirroring.
            // K8s endpointslice-mirroring-controller mirrors Endpoints for:
            // - Services without selectors
            // - Standalone Endpoints without a matching Service
            let svc_has_selector = services.iter().any(|s| {
                s.metadata.namespace.as_deref().unwrap_or("default") == ns
                    && s.metadata.name == *ep_name
                    && s.spec
                        .selector
                        .as_ref()
                        .map(|sel| !sel.is_empty())
                        .unwrap_or(false)
            });
            if svc_has_selector {
                continue;
            }

            // Mirror this Endpoints object to an EndpointSlice
            let endpointslices = EndpointSlice::from_endpoints(ep);

            // If Endpoints has empty subsets, create at least one empty EndpointSlice
            let slices_to_create = if endpointslices.is_empty() {
                let mut empty_slice = EndpointSlice::new(ep_name, "IPv4");
                empty_slice.metadata.namespace = Some(ns.to_string());
                vec![empty_slice]
            } else {
                endpointslices
            };

            let bare_owned_by_selector = self.slice_owned_by_selector_controller(ns, ep_name).await;

            for (idx, mut slice) in slices_to_create.into_iter().enumerate() {
                let slice_name = Self::mirrored_slice_name(ep_name, idx, bare_owned_by_selector);
                slice.metadata.name = slice_name.clone();
                slice.metadata.namespace = Some(ns.to_string());

                let labels = slice.metadata.labels.get_or_insert_with(Default::default);
                labels.insert("kubernetes.io/service-name".to_string(), ep_name.clone());
                labels.insert(
                    "endpointslice.kubernetes.io/managed-by".to_string(),
                    "endpointslice-mirroring-controller.k8s.io".to_string(),
                );

                slice.metadata.owner_references =
                    Some(vec![rusternetes_common::types::OwnerReference {
                        api_version: "v1".to_string(),
                        kind: "Endpoints".to_string(),
                        name: ep_name.clone(),
                        uid: ep.metadata.uid.clone(),
                        controller: Some(true),
                        block_owner_deletion: Some(true),
                    }]);

                let slice_key = build_key("endpointslices", Some(ns), &slice_name);
                match self.storage.get::<EndpointSlice>(&slice_key).await {
                    Ok(existing) => {
                        if existing.endpoints == slice.endpoints && existing.ports == slice.ports {
                            continue;
                        }
                        adopt_existing_identity(&mut slice, &existing);
                        let _ = self.storage.update(&slice_key, &slice).await;
                    }
                    Err(_) => {
                        slice.metadata.resource_version = None;
                        let _ = self.storage.create(&slice_key, &slice).await;
                    }
                }
            }
        }

        // Clean up orphaned EndpointSlices (whose Service no longer exists)
        let all_slices: Vec<EndpointSlice> = self
            .storage
            .list(&build_prefix("endpointslices", None))
            .await
            .unwrap_or_default();

        for slice in all_slices {
            let ns = slice.metadata.namespace.as_deref().unwrap_or("default");
            let svc_name = slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|s| s.as_str());

            if let Some(svc_name) = svc_name {
                // Only delete slices managed by our controllers
                let managed_by = slice
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                    .map(|v| v.as_str());

                let should_delete = match managed_by {
                    Some("endpointslice-controller.k8s.io") => {
                        // Selector-based: delete if service no longer exists
                        !service_names.contains(&(ns.to_string(), svc_name.to_string()))
                    }
                    Some("endpointslice-mirroring-controller.k8s.io") => {
                        // Mirrored: delete if source Endpoints no longer exists
                        // K8s ref: pkg/controller/endpointslicemirroring/reconciler.go
                        let ep_key = build_key("endpoints", Some(ns), svc_name);
                        self.storage
                            .get::<serde_json::Value>(&ep_key)
                            .await
                            .is_err()
                    }
                    _ => false,
                };

                if should_delete {
                    let key = build_key("endpointslices", Some(ns), &slice.metadata.name);
                    debug!(
                        "Deleting orphaned EndpointSlice {}/{}",
                        ns, slice.metadata.name
                    );
                    let _ = self.storage.delete(&key).await;
                }
            }
        }

        Ok(())
    }

    /// Reconcile EndpointSlices for a single Service
    /// Reconcile one service, fetching the namespace's pods itself.
    ///
    /// Used by the watch-driven path. It reads pods from the shared snapshot
    /// (`podLister`, endpointslice_controller.go:410) and only LISTs when the
    /// snapshot is not synced (direct callers). The sweep
    /// uses [`Self::reconcile_service_with_pods`] instead, so a namespace full
    /// of services costs one pod LIST rather than one per service.
    async fn reconcile_service(&self, service: &Service) -> Result<()> {
        let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
        let all_pods: Vec<Pod> = match self.cached_pods(namespace) {
            Some(pods) => pods,
            None => self
                .storage
                .list(&build_prefix("pods", Some(namespace)))
                .await
                .unwrap_or_default(),
        };
        self.reconcile_service_with_pods(service, &all_pods).await
    }

    /// Reconcile one service against an already-fetched pod set for its
    /// namespace.
    ///
    /// Upstream reads pods from a shared informer cache rather than listing
    /// per service (pkg/controller/endpointslice/endpointslice_controller.go);
    /// this is the same property — fetch once, reuse across every service that
    /// shares the namespace.
    async fn reconcile_service_with_pods(&self, service: &Service, all_pods: &[Pod]) -> Result<()> {
        let namespace = service.metadata.namespace.as_deref().unwrap_or("default");
        let service_name = &service.metadata.name;

        // Skip ExternalName services (K8s: services with Type ExternalName receive no endpoints)
        if matches!(
            service.spec.service_type,
            Some(rusternetes_common::resources::ServiceType::ExternalName)
        ) {
            return Ok(());
        }

        // Services without selectors (nil selector) are skipped.
        // Services with EMPTY selectors (selector: {}) still get EndpointSlices
        // but with no endpoints. K8s endpointslice controller handles both cases.
        let selector = match &service.spec.selector {
            Some(s) => s,
            None => return Ok(()), // nil selector = skip (headless without selector)
        };

        // Log at INFO so we can confirm the controller reconciles after pod changes
        info!(
            "Reconciling endpointslices for service {}/{} (matching_pods will follow)",
            namespace, service_name
        );

        // Pods come from the caller — one LIST per namespace per pass.

        // K8s endpointslice controller keeps terminating pods in slices with
        // terminating=true (rather than dropping them). Dropping them on the first
        // reconcile breaks rolling updates and the "serve" conformance tests that
        // race-check endpoint serving as pods are recreated. Only pods that are
        // fully terminal (Succeeded/Failed) are excluded.
        let publish_not_ready = service.spec.publish_not_ready_addresses.unwrap_or(false);

        let matching_pods: Vec<&Pod> = all_pods
            .iter()
            .filter(|pod| {
                // Match label selector
                if let Some(pod_labels) = &pod.metadata.labels {
                    selector.iter().all(|(k, v)| pod_labels.get(k) == Some(v))
                } else {
                    false
                }
            })
            .filter(|pod| {
                // K8s ShouldPodBeInEndpointSlice() excludes only:
                // - Pods in terminal phase (Succeeded/Failed)
                // - Pods without an IP
                // Terminating pods (with deletionTimestamp) are KEPT and marked
                // terminating=true so kube-proxy can drain them gracefully.
                let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
                !matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed))
            })
            .collect();

        info!(
            "EndpointSlice reconcile {}/{}: {} matching pods (from {} total in namespace)",
            namespace,
            service_name,
            matching_pods.len(),
            all_pods.len()
        );

        // Topology-aware routing. When a Service requests topology hints — via
        // `trafficDistribution: PreferClose` or the
        // `service.kubernetes.io/topology-mode: Auto` annotation — the
        // EndpointSlice controller stamps each endpoint with its node's zone
        // and a matching `hints.forZones` so kube-proxy can prefer same-zone
        // endpoints. Mirrors upstream pkg/controller/endpointslice/topologycache
        // (here simplified to a per-endpoint same-zone hint rather than the full
        // proportional-allocation heuristic).
        let topology_enabled = Self::topology_hints_requested(service);
        let node_zones: HashMap<String, String> = if topology_enabled {
            self.build_node_zone_map().await
        } else {
            HashMap::new()
        };

        // Group pods by their resolved port mapping.
        // Each group gets its own EndpointSlice with only the ports those pods serve.
        // This follows K8s's reconcileByAddressType / getEndpointPorts pattern.
        // Use (serialized ports, ports) as key since EndpointPort doesn't implement Hash.
        let mut port_groups: HashMap<String, (Vec<EndpointPort>, Vec<Endpoint>)> = HashMap::new();

        for pod in &matching_pods {
            let endpoint_ports = self.get_endpoint_ports(service, pod);
            if endpoint_ports.is_empty() {
                continue; // Pod doesn't serve any of the service's ports
            }

            let pod_ip = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_ref())
                .filter(|ip| !ip.is_empty());

            let Some(ip) = pod_ip else {
                continue; // Pod has no IP
            };

            let pod_ready = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conditions| {
                    conditions
                        .iter()
                        .any(|c| c.condition_type == "Ready" && c.status == "True")
                })
                .unwrap_or(false);

            let is_terminating = pod.metadata.deletion_timestamp.is_some();

            // K8s endpointslice condition semantics:
            // - ready: pod is Ready AND not terminating, OR publishNotReadyAddresses=true
            // - serving: pod is Ready (independent of terminating state)
            // - terminating: pod has deletionTimestamp
            // Ref: pkg/controller/endpointslice/utils.go — podEndpointConditions
            let ready = if publish_not_ready {
                true
            } else {
                pod_ready && !is_terminating
            };
            let serving = if publish_not_ready { true } else { pod_ready };

            let endpoint = Endpoint {
                addresses: vec![ip.clone()],
                conditions: Some(EndpointConditions {
                    ready: Some(ready),
                    serving: Some(serving),
                    terminating: Some(is_terminating),
                }),
                hostname: pod.spec.as_ref().and_then(|s| {
                    if s.subdomain.is_some() {
                        s.hostname.clone().or(Some(pod.metadata.name.clone()))
                    } else {
                        None
                    }
                }),
                target_ref: Some(EndpointReference {
                    kind: Some("Pod".to_string()),
                    namespace: pod.metadata.namespace.clone(),
                    name: Some(pod.metadata.name.clone()),
                    uid: Some(pod.metadata.uid.clone()),
                    resource_version: None,
                    field_path: None,
                    ..Default::default()
                }),
                node_name: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
                zone: None,
                hints: None,
                deprecated_topology: None,
            };

            // Stamp zone + same-zone hint when topology routing is enabled and
            // the backing node carries a topology.kubernetes.io/zone label.
            let mut endpoint = endpoint;
            if topology_enabled {
                if let Some(node_name) = endpoint.node_name.as_ref() {
                    if let Some(zone) = node_zones.get(node_name) {
                        endpoint.zone = Some(zone.clone());
                        endpoint.hints = Some(
                            rusternetes_common::resources::endpointslice::EndpointHints {
                                for_zones: Some(vec![
                                    rusternetes_common::resources::endpointslice::ForZone {
                                        name: zone.clone(),
                                    },
                                ]),
                                for_nodes: None,
                            },
                        );
                    }
                }
            }

            let port_key = serde_json::to_string(&endpoint_ports).unwrap_or_default();
            port_groups
                .entry(port_key)
                .or_insert_with(|| (endpoint_ports, Vec::new()))
                .1
                .push(endpoint);
        }

        // If no port groups were created but the service has ports,
        // create an empty slice with the service's ports
        if port_groups.is_empty() && !service.spec.ports.is_empty() {
            let ports: Vec<EndpointPort> = service
                .spec
                .ports
                .iter()
                .map(|sp| EndpointPort {
                    // K8s always sets port name, even if empty string.
                    // A nil name causes kubectl describe to crash (nil pointer deref).
                    name: Some(sp.name.clone().unwrap_or_default()),
                    port: Some(sp.port as i32),
                    protocol: sp.protocol.clone(),
                    app_protocol: sp.app_protocol.clone(),
                })
                .collect();
            let port_key = serde_json::to_string(&ports).unwrap_or_default();
            port_groups.insert(port_key, (ports, Vec::new()));
        }

        // Create/update EndpointSlices for each port group
        let port_group_count = port_groups.len();
        for (idx, (_key, (ports, endpoints))) in port_groups.into_iter().enumerate() {
            let slice_name = if idx == 0 {
                service_name.clone()
            } else {
                format!("{}-{}", service_name, idx)
            };

            let mut slice = EndpointSlice::new(&slice_name, "IPv4");
            slice.metadata.namespace = Some(namespace.to_string());
            slice.ports = ports;
            slice.endpoints = endpoints;

            // Set labels
            let labels = slice.metadata.labels.get_or_insert_with(Default::default);
            labels.insert(
                "kubernetes.io/service-name".to_string(),
                service_name.clone(),
            );
            labels.insert(
                "endpointslice.kubernetes.io/managed-by".to_string(),
                "endpointslice-controller.k8s.io".to_string(),
            );

            // Set owner reference to the Service
            slice.metadata.owner_references =
                Some(vec![rusternetes_common::types::OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Service".to_string(),
                    name: service_name.clone(),
                    uid: service.metadata.uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]);

            let slice_key = build_key("endpointslices", Some(namespace), &slice_name);

            // Check if existing slice matches — skip write if nothing changed
            match self.storage.get::<EndpointSlice>(&slice_key).await {
                Ok(existing) => {
                    if existing.endpoints == slice.endpoints && existing.ports == slice.ports {
                        continue;
                    }
                    adopt_existing_identity(&mut slice, &existing);
                    match self.storage.update(&slice_key, &slice).await {
                        Ok(updated) => {
                            // reconciler.go:464.
                            self.slice_tracker.update(&updated);
                            info!(
                                "Updated endpointslice {}/{} for service ({} endpoints)",
                                namespace,
                                slice_name,
                                slice.endpoints.len()
                            );
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                // Only an ABSENT slice may be created. Treating every read
                // error as "absent" turned a transient (or decode) failure into
                // an endless create/AlreadyExists loop that never updated the
                // slice, so endpoints went stale for as long as the error
                // lasted.
                Err(rusternetes_common::Error::NotFound(_)) => {
                    slice.metadata.resource_version = None;
                    let created = self.storage.create(&slice_key, &slice).await?;
                    // reconciler.go:453.
                    self.slice_tracker.update(&created);
                    info!(
                        "Created endpointslice {}/{} for service",
                        namespace, slice_name
                    );
                }
                Err(e) => {
                    error!(
                        "Cannot read endpointslice {}/{}: {} — not creating a duplicate",
                        namespace, slice_name, e
                    );
                    return Err(e.into());
                }
            }
        }

        // Clean up stale EndpointSlices that are no longer needed.
        // K8s reconciler deletes slices that are no longer in the desired set.
        // Without this, deleted pods leave behind stale EndpointSlices with
        // outdated endpoint entries (causes "extra port mappings" test failures).
        let es_prefix = build_prefix("endpointslices", Some(namespace));
        let existing_slices: Vec<EndpointSlice> =
            self.storage.list(&es_prefix).await.unwrap_or_default();
        for existing in &existing_slices {
            // Only manage slices owned by this service
            let owned = existing
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .map(|n| n == service_name)
                .unwrap_or(false);
            let managed = existing
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                .map(|m| m == "endpointslice-controller.k8s.io")
                .unwrap_or(false);
            if !owned || !managed {
                continue;
            }
            // Check if this slice name was in our current set
            let slice_name = &existing.metadata.name;
            let in_current_set = (0..port_group_count).any(|idx| {
                let expected = if idx == 0 {
                    service_name.clone()
                } else {
                    format!("{}-{}", service_name, idx)
                };
                slice_name == &expected
            });
            if !in_current_set {
                let stale_key = build_key("endpointslices", Some(namespace), slice_name);
                if let Err(e) = self.storage.delete(&stale_key).await {
                    debug!(
                        "Failed to delete stale endpointslice {}/{}: {}",
                        namespace, slice_name, e
                    );
                } else {
                    // reconciler.go:473.
                    self.slice_tracker.expect_deletion(existing);
                    info!(
                        "Deleted stale endpointslice {}/{} (no longer needed for service {})",
                        namespace, slice_name, service_name
                    );
                }
            }
        }

        Ok(())
    }

    /// Whether a Service requests topology-aware endpoint hints. Mirrors
    /// upstream which enables hints for `trafficDistribution: PreferClose` or
    /// the legacy `service.kubernetes.io/topology-mode: Auto` annotation.
    fn topology_hints_requested(service: &Service) -> bool {
        if service.spec.traffic_distribution.as_deref() == Some("PreferClose") {
            return true;
        }
        service
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get("service.kubernetes.io/topology-mode"))
            .map(|mode| mode == "Auto")
            .unwrap_or(false)
    }

    /// Build a map of node name -> zone from the `topology.kubernetes.io/zone`
    /// label on each Node. Nodes without the label are omitted.
    async fn build_node_zone_map(&self) -> HashMap<String, String> {
        let nodes: Vec<rusternetes_common::resources::Node> = self
            .storage
            .list(&build_prefix("nodes", None))
            .await
            .unwrap_or_default();

        let mut map = HashMap::new();
        for node in nodes {
            if let Some(zone) = node
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("topology.kubernetes.io/zone"))
            {
                map.insert(node.metadata.name.clone(), zone.clone());
            }
        }
        map
    }

    /// Compute endpoint ports for a pod based on the service's port definitions.
    /// Follows K8s's getEndpointPorts() logic:
    /// - For each service port, try to find a matching containerPort on the pod
    /// - If the service uses a named targetPort, look up the container port by name
    /// - If the service uses a numeric targetPort, use it directly
    /// - If no match is found, skip that port (don't include it)
    fn get_endpoint_ports(&self, service: &Service, pod: &Pod) -> Vec<EndpointPort> {
        let mut endpoint_ports = Vec::new();

        for sp in &service.spec.ports {
            let port_num = match self.find_port(pod, sp) {
                Some(p) => p,
                None => continue, // Pod doesn't serve this port
            };

            endpoint_ports.push(EndpointPort {
                // K8s always sets port name, even if empty string
                name: Some(sp.name.clone().unwrap_or_default()),
                port: Some(port_num),
                protocol: sp.protocol.clone(),
                app_protocol: sp.app_protocol.clone(),
            });
        }

        endpoint_ports
    }

    /// Find the container port on a pod that corresponds to a service port.
    /// Implements K8s's FindPort logic:
    /// - If targetPort is a string (named port), search pod containers for a
    ///   containerPort with that name
    /// - If targetPort is a number, use it directly
    /// - If targetPort is not set, use the service port number
    fn find_port(
        &self,
        pod: &Pod,
        service_port: &rusternetes_common::resources::ServicePort,
    ) -> Option<i32> {
        match &service_port.target_port {
            Some(rusternetes_common::resources::IntOrString::String(name)) => {
                // Named port — search pod containers
                if let Ok(port_num) = name.parse::<i32>() {
                    // It's actually a numeric string
                    return Some(port_num);
                }
                // Look up named port in pod containers
                if let Some(spec) = &pod.spec {
                    for container in &spec.containers {
                        if let Some(ports) = &container.ports {
                            for cp in ports {
                                if cp.name.as_deref() == Some(name.as_str()) {
                                    return Some(cp.container_port as i32);
                                }
                            }
                        }
                    }
                }
                None // Pod doesn't have this named port
            }
            Some(rusternetes_common::resources::IntOrString::Int(port)) => Some(*port),
            None => Some(service_port.port as i32), // Default to service port
        }
    }

    /// Check whether the EndpointSlice at `<ns>/<name>` is owned by the
    /// selector-based controller. Used to avoid collisions between selector-
    /// and mirror-managed slices that would otherwise share a name.
    async fn slice_owned_by_selector_controller(&self, ns: &str, name: &str) -> bool {
        let key = build_key("endpointslices", Some(ns), name);
        match self.storage.get::<EndpointSlice>(&key).await {
            Ok(existing) => existing
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                .map(|m| m == "endpointslice-controller.k8s.io")
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Resolve the slice name a mirroring controller should use for the
    /// `idx`-th mirror slice of an Endpoints resource named `name`. Falls back
    /// to a "-mirrored" suffix when the bare name is already owned by a
    /// selector-based slice.
    fn mirrored_slice_name(name: &str, idx: usize, bare_owned: bool) -> String {
        match (idx, bare_owned) {
            (0, false) => name.to_string(),
            (0, true) => format!("{}-mirrored", name),
            (i, true) => format!("{}-mirrored-{}", name, i),
            (i, false) => format!("{}-{}", name, i),
        }
    }

    /// Clean up orphaned EndpointSlices whose owning Service or Endpoints no
    /// longer exists. Used by external callers and tests; the regular
    /// reconcile loop also runs this cleanup.
    #[allow(dead_code)]
    pub async fn cleanup_orphans(&self) -> Result<()> {
        let all_slices: Vec<EndpointSlice> = self
            .storage
            .list(&build_prefix("endpointslices", None))
            .await
            .unwrap_or_default();

        for slice in all_slices {
            let ns = slice.metadata.namespace.as_deref().unwrap_or("default");
            let svc_name = slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("kubernetes.io/service-name"))
                .cloned();

            let Some(svc_name) = svc_name else { continue };

            let managed_by = slice
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("endpointslice.kubernetes.io/managed-by"))
                .cloned();

            let should_delete = match managed_by.as_deref() {
                Some("endpointslice-controller.k8s.io") => {
                    // Selector-based: delete if owning Service no longer exists
                    self.storage
                        .get::<Service>(&build_key("services", Some(ns), &svc_name))
                        .await
                        .is_err()
                }
                Some("endpointslice-mirroring-controller.k8s.io") => {
                    // Mirrored: delete if source Endpoints no longer exists
                    self.storage
                        .get::<serde_json::Value>(&build_key("endpoints", Some(ns), &svc_name))
                        .await
                        .is_err()
                }
                _ => false,
            };

            if should_delete {
                let key = build_key("endpointslices", Some(ns), &slice.metadata.name);
                let _ = self.storage.delete(&key).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::{ContainerPort, ServicePort, ServiceSpec};
    use rusternetes_common::types::ObjectMeta;
    use rusternetes_storage::MemoryStorage;

    #[tokio::test]
    async fn test_endpointslice_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = EndpointSliceController::new(storage);
    }

    #[tokio::test]
    async fn test_pod_port_filtering_named_ports() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // Create a service with two named target ports
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("test-svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![
                    ServicePort {
                        name: Some("portname1".to_string()),
                        port: 80,
                        target_port: Some(rusternetes_common::resources::IntOrString::String(
                            "svc1".to_string(),
                        )),
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    },
                    ServicePort {
                        name: Some("portname2".to_string()),
                        port: 81,
                        target_port: Some(rusternetes_common::resources::IntOrString::String(
                            "svc2".to_string(),
                        )),
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    },
                ],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };

        // Pod1 only has containerPort named "svc1" (serves portname1 only)
        let pod1 = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("pod1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c1".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 100,
                        name: Some("svc1".to_string()),
                        protocol: "TCP".to_string(),
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: None,
        };

        // Pod1 should only get portname1 (svc1→100), NOT portname2 (svc2 not found)
        let ports = controller.get_endpoint_ports(&service, &pod1);
        assert_eq!(ports.len(), 1, "Pod1 should only match portname1");
        assert_eq!(ports[0].name, Some("portname1".to_string()));
        assert_eq!(ports[0].port, Some(100));
    }

    #[tokio::test]
    async fn test_orphan_detection_skips_externally_managed_slices() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // Create an EndpointSlice NOT managed by the controller
        let mut external_slice = EndpointSlice::new("external-slice", "IPv4");
        external_slice.metadata.namespace = Some("default".to_string());
        let key = build_key("endpointslices", Some("default"), "external-slice");
        storage.create(&key, &external_slice).await.unwrap();

        // Create an EndpointSlice managed by the controller
        let mut managed_slice = EndpointSlice::new("managed-slice", "IPv4");
        managed_slice.metadata.namespace = Some("default".to_string());
        let mut labels = HashMap::new();
        labels.insert(
            "endpointslice.kubernetes.io/managed-by".to_string(),
            "endpointslice-controller.k8s.io".to_string(),
        );
        labels.insert(
            "kubernetes.io/service-name".to_string(),
            "nonexistent-service".to_string(),
        );
        managed_slice.metadata.labels = Some(labels);
        let key2 = build_key("endpointslices", Some("default"), "managed-slice");
        storage.create(&key2, &managed_slice).await.unwrap();

        controller.reconcile_all().await.unwrap();

        // External slice should survive
        assert!(
            storage.get::<EndpointSlice>(&key).await.is_ok(),
            "externally-managed EndpointSlice should NOT be deleted"
        );

        // Managed slice should be deleted (orphaned)
        assert!(
            storage.get::<EndpointSlice>(&key2).await.is_err(),
            "controller-managed orphan EndpointSlice should be deleted"
        );
    }

    /// Regression test: when a service is created before its pods, the
    /// EndpointSlice is initially empty. When pods become ready, the
    /// EndpointSlice must be UPDATED with the pod's IP and ready=true.
    /// Bug: the controller created empty slices but never updated them,
    /// causing kube-proxy to never generate DNAT rules for the service.
    #[tokio::test]
    async fn test_endpointslice_updates_when_pod_becomes_ready() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        // 1. Create a service with one port (port 80, no targetPort)
        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("endpoint-test2").with_namespace("test-ns"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        let svc_key = build_key("services", Some("test-ns"), "endpoint-test2");
        storage.create(&svc_key, &service).await.unwrap();

        // 2. Reconcile with no pods — should create empty EndpointSlice
        controller.reconcile_service(&service).await.unwrap();

        let es_key = build_key("endpointslices", Some("test-ns"), "endpoint-test2");
        let es = storage.get::<EndpointSlice>(&es_key).await.unwrap();
        assert!(
            es.endpoints.is_empty(),
            "EndpointSlice should be empty when no pods exist"
        );
        assert!(
            !es.ports.is_empty(),
            "EndpointSlice should have ports from the service"
        );

        // 3. Create a matching pod with IP and Ready condition
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("pod1")
                .with_namespace("test-ns")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "agnhost".to_string(),
                    ports: Some(vec![ContainerPort {
                        container_port: 80,
                        name: None,
                        protocol: "TCP".to_string(),
                        host_port: None,
                        host_ip: None,
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                pod_ip: Some("10.89.0.22".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        let pod_key = build_key("pods", Some("test-ns"), "pod1");
        storage.create(&pod_key, &pod).await.unwrap();

        // 4. Reconcile again — EndpointSlice MUST be updated with the pod
        controller.reconcile_service(&service).await.unwrap();

        let es_updated = storage.get::<EndpointSlice>(&es_key).await.unwrap();
        assert_eq!(
            es_updated.endpoints.len(),
            1,
            "EndpointSlice must have 1 endpoint after pod becomes ready"
        );
        assert_eq!(es_updated.endpoints[0].addresses, vec!["10.89.0.22"]);
        assert_eq!(
            es_updated.endpoints[0]
                .conditions
                .as_ref()
                .and_then(|c| c.ready),
            Some(true),
            "Endpoint must be marked ready"
        );
    }

    /// Terminating pods (with deletionTimestamp) must remain in EndpointSlices
    /// with terminating=true, ready=false. K8s kube-proxy uses these to drain
    /// connections during rolling updates. Dropping them entirely causes
    /// in-flight requests to fail mid-flight.
    #[tokio::test]
    async fn test_endpointslice_keeps_terminating_pods() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // Pod with deletionTimestamp set (terminating) but still Ready
        let mut pod_meta = ObjectMeta::new("p1")
            .with_namespace("default")
            .with_labels(HashMap::from([("app".to_string(), "test".to_string())]));
        pod_meta.deletion_timestamp = Some(chrono::Utc::now());
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: pod_meta,
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.1".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();

        let es: EndpointSlice = storage
            .get(&build_key("endpointslices", Some("default"), "svc"))
            .await
            .unwrap();

        assert_eq!(
            es.endpoints.len(),
            1,
            "terminating pod must still appear in the EndpointSlice"
        );
        let conds = es.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(conds.terminating, Some(true), "terminating must be true");
        assert_eq!(
            conds.ready,
            Some(false),
            "ready must be false for terminating pods (unless publishNotReadyAddresses)"
        );
        assert_eq!(
            conds.serving,
            Some(true),
            "serving must mirror Ready condition independent of terminating state"
        );
    }

    /// publishNotReadyAddresses=true forces ready=true and serving=true on every
    /// endpoint, regardless of pod readiness. This is required for headless
    /// services that fronts gossip-style protocols (e.g. peer-discovery) where
    /// clients need to see all pods, including those still starting up.
    #[tokio::test]
    async fn test_endpointslice_publish_not_ready_addresses() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                publish_not_ready_addresses: Some(true),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // A pod that is Running but NOT Ready
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.2".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "False".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();

        let es: EndpointSlice = storage
            .get(&build_key("endpointslices", Some("default"), "svc"))
            .await
            .unwrap();
        assert_eq!(es.endpoints.len(), 1);
        let conds = es.endpoints[0].conditions.as_ref().unwrap();
        assert_eq!(
            conds.ready,
            Some(true),
            "publishNotReadyAddresses=true must force ready=true even for unready pods"
        );
        assert_eq!(
            conds.serving,
            Some(true),
            "publishNotReadyAddresses=true must force serving=true"
        );
    }

    /// One pod LIST per namespace per pass, not one per service.
    ///
    /// `reconcile_service` used to LIST every pod in the namespace for each
    /// service it reconciled, so a namespace holding N services cost N
    /// identical namespace-wide LISTs every 5 seconds.
    ///
    /// Measured on a live kine cluster after a conformance run: 154
    /// `latency-svc-*` services in one namespace (from the sig-network
    /// endpoint-latency spec) were being walked at ~1 service per 1.2s. The
    /// sweep could not finish inside its own 5s resync, it kept kine at
    /// ~1013% CPU serving the repeated LISTs, and it starved the rest of the
    /// controller-manager so completely that 335 namespaces sat in
    /// Terminating with zero drain progress over 60 seconds.
    ///
    /// Upstream never does this: pods come from a shared informer cache, and
    /// services are reconciled from a rate-limited workqueue by
    /// `ConcurrentServiceEndpointSyncs` = 5 workers
    /// (pkg/controller/endpointslice/endpointslice_controller.go:106-118,
    /// endpointslice/config/v1alpha1/defaults.go:35).
    #[tokio::test]
    async fn one_pod_list_per_namespace_not_per_service() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingStorage {
            inner: Arc<MemoryStorage>,
            pod_lists: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl Storage for CountingStorage {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                key: &str,
                value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                self.inner.update_raw(key, value).await
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                if prefix.starts_with("/registry/pods/") {
                    self.pod_lists.fetch_add(1, Ordering::SeqCst);
                }
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        const SERVICES: usize = 12;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(CountingStorage {
            inner: Arc::clone(&inner),
            pod_lists: AtomicUsize::new(0),
        });

        // One backing pod, and SERVICES services all selecting it.
        let mut pod_labels = HashMap::new();
        pod_labels.insert("app".to_string(), "latency".to_string());
        let mut pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("backing-pod").with_namespace("svc-latency"),
            spec: None,
            status: None,
        };
        pod.metadata.labels = Some(pod_labels.clone());
        storage
            .create("/registry/pods/svc-latency/backing-pod", &pod)
            .await
            .unwrap();

        for i in 0..SERVICES {
            let name = format!("latency-svc-{i}");
            let service = Service {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "Service".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta::new(name.clone()).with_namespace("svc-latency"),
                spec: ServiceSpec {
                    selector: Some(pod_labels.clone()),
                    ports: vec![ServicePort {
                        name: None,
                        port: 80,
                        target_port: None,
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    }],
                    ..Default::default()
                },
                status: None,
            };
            storage
                .create(&build_key("services", Some("svc-latency"), &name), &service)
                .await
                .unwrap();
        }

        let controller = EndpointSliceController::new(Arc::clone(&storage));
        controller.reconcile_all().await.unwrap();

        let lists = storage.pod_lists.load(Ordering::SeqCst);
        assert!(
            lists < SERVICES,
            "reconcile_all issued {lists} pod LISTs for {SERVICES} services in ONE namespace; \
             the pod set is the same for all of them and must be fetched once per namespace"
        );
    }

    /// The live `run()` path must drain its service queue with a worker POOL.
    ///
    /// It used to spawn exactly one worker, so after a conformance run left
    /// 154 services in one namespace the 5s resync re-enqueued all of them
    /// and a single worker walked them serially (~1 service / 1.2s), starving
    /// the controller-manager (#1869). Upstream runs
    /// `ConcurrentServiceEndpointSyncs` = 5 workers over the service queue
    /// (pkg/controller/endpointslice/endpointslice_controller.go:106-118,
    /// endpointslice/config/v1alpha1/defaults.go:35).
    #[tokio::test]
    async fn run_drains_service_queue_with_concurrent_workers() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct SlowStorage {
            inner: Arc<MemoryStorage>,
            in_flight: AtomicUsize,
            max_in_flight: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl Storage for SlowStorage {
            async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.create(key, value).await
            }
            async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
            where
                T: serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.get(key).await
            }
            async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                self.inner.update(key, value).await
            }
            async fn update_raw(
                &self,
                key: &str,
                value: &serde_json::Value,
            ) -> rusternetes_common::Result<()> {
                self.inner.update_raw(key, value).await
            }
            async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
                self.inner.delete(key).await
            }
            async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
            where
                T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
            {
                if prefix.starts_with("/registry/endpointslices/") {
                    let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    self.max_in_flight.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    self.in_flight.fetch_sub(1, Ordering::SeqCst);
                }
                self.inner.list(prefix).await
            }
            async fn watch(
                &self,
                prefix: &str,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch(prefix).await
            }
            async fn watch_from_revision(
                &self,
                prefix: &str,
                revision: i64,
            ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
                self.inner.watch_from_revision(prefix, revision).await
            }
            async fn current_revision(&self) -> rusternetes_common::Result<i64> {
                self.inner.current_revision().await
            }
            async fn is_revision_compacted(
                &self,
                revision: i64,
            ) -> rusternetes_common::Result<bool> {
                self.inner.is_revision_compacted(revision).await
            }
        }

        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(SlowStorage {
            inner: Arc::clone(&inner),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        });

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "latency".to_string());
        for i in 0..20 {
            let name = format!("svc-{i}");
            let service = Service {
                type_meta: rusternetes_common::types::TypeMeta {
                    kind: "Service".to_string(),
                    api_version: "v1".to_string(),
                },
                metadata: ObjectMeta::new(name.clone()).with_namespace("ns"),
                spec: ServiceSpec {
                    selector: Some(labels.clone()),
                    ports: vec![ServicePort {
                        name: None,
                        port: 80,
                        target_port: None,
                        protocol: "TCP".to_string(),
                        node_port: None,
                        app_protocol: None,
                    }],
                    ..Default::default()
                },
                status: None,
            };
            storage
                .create(&build_key("services", Some("ns"), &name), &service)
                .await
                .unwrap();
        }

        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        handle.abort();

        let max = storage.max_in_flight.load(Ordering::SeqCst);
        assert!(
            max > 1,
            "only {max} service sync(s) ever ran at once; the service queue must be drained \
             by a pool of CONCURRENT_SERVICE_ENDPOINT_SYNCS workers"
        );
    }

    /// Storage that counts pod LISTs, for the shared pod snapshot tests.
    struct PodListCounter {
        inner: Arc<MemoryStorage>,
        pod_lists: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl Storage for PodListCounter {
        async fn create<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.create(key, value).await
        }
        async fn get<T>(&self, key: &str) -> rusternetes_common::Result<T>
        where
            T: serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.get(key).await
        }
        async fn update<T>(&self, key: &str, value: &T) -> rusternetes_common::Result<T>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            self.inner.update(key, value).await
        }
        async fn update_raw(
            &self,
            key: &str,
            value: &serde_json::Value,
        ) -> rusternetes_common::Result<()> {
            self.inner.update_raw(key, value).await
        }
        async fn delete(&self, key: &str) -> rusternetes_common::Result<()> {
            self.inner.delete(key).await
        }
        async fn list<T>(&self, prefix: &str) -> rusternetes_common::Result<Vec<T>>
        where
            T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
        {
            if prefix.starts_with("/registry/pods/") {
                self.pod_lists
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.list(prefix).await
        }
        async fn watch(
            &self,
            prefix: &str,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch(prefix).await
        }
        async fn watch_from_revision(
            &self,
            prefix: &str,
            revision: i64,
        ) -> rusternetes_common::Result<rusternetes_storage::WatchStream> {
            self.inner.watch_from_revision(prefix, revision).await
        }
        async fn current_revision(&self) -> rusternetes_common::Result<i64> {
            self.inner.current_revision().await
        }
        async fn is_revision_compacted(&self, revision: i64) -> rusternetes_common::Result<bool> {
            self.inner.is_revision_compacted(revision).await
        }
    }

    fn snapshot_service(name: &str) -> Service {
        Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name).with_namespace("ns"),
            spec: ServiceSpec {
                selector: Some(HashMap::from([("app".to_string(), "snap".to_string())])),
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                ..Default::default()
            },
            status: None,
        }
    }

    fn snapshot_pod(name: &str, ip: &str) -> Pod {
        Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new(name)
                .with_namespace("ns")
                .with_labels(HashMap::from([("app".to_string(), "snap".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(rusternetes_common::types::Phase::Running),
                pod_ip: Some(ip.to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        }
    }

    /// The live path must read pods from a snapshot shared by every worker,
    /// not LIST per service sync. Upstream's `syncService` reads
    /// `c.podLister.Pods(ns).List(selector)` from the shared pod informer's
    /// cache (endpointslice_controller.go:410); the informer LISTs once.
    /// Stand-in for informer cache (#2208).
    #[tokio::test]
    async fn run_shares_one_pod_snapshot_across_service_syncs() {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(PodListCounter {
            inner: Arc::clone(&inner),
            pod_lists: std::sync::atomic::AtomicUsize::new(0),
        });
        for i in 0..20 {
            let name = format!("svc-{i}");
            storage
                .create(
                    &build_key("services", Some("ns"), &name),
                    &snapshot_service(&name),
                )
                .await
                .unwrap();
        }
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        handle.abort();

        let lists = storage.pod_lists.load(Ordering::SeqCst);
        assert!(
            lists <= 1,
            "{lists} pod LISTs for 20 services; workers must share one pod snapshot \
             (upstream podLister reads the informer cache)"
        );
    }

    /// The snapshot is kept current from the pod watch, so a pod created
    /// after the initial sync reaches the slice without any 5s full resync
    /// and without a fresh LIST (informer event handler -> podQueue ->
    /// service enqueue, endpointslice_controller.go:132-135, 532).
    #[tokio::test]
    async fn pod_snapshot_follows_pod_watch_events() {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(MemoryStorage::new());
        let storage = Arc::new(PodListCounter {
            inner: Arc::clone(&inner),
            pod_lists: std::sync::atomic::AtomicUsize::new(0),
        });
        storage
            .create(
                &build_key("services", Some("ns"), "svc"),
                &snapshot_service("svc"),
            )
            .await
            .unwrap();
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        let before = storage.pod_lists.load(Ordering::SeqCst);

        storage
            .create(
                &build_key("pods", Some("ns"), "p1"),
                &snapshot_pod("p1", "10.0.0.7"),
            )
            .await
            .unwrap();
        let es_key = build_key("endpointslices", Some("ns"), "svc");
        let mut seen = false;
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(es) = storage.get::<EndpointSlice>(&es_key).await {
                if es.endpoints.iter().any(|e| e.addresses == vec!["10.0.0.7"]) {
                    seen = true;
                    break;
                }
            }
        }
        handle.abort();
        assert!(seen, "pod created after sync never reached the slice");
        assert_eq!(
            storage.pod_lists.load(Ordering::SeqCst),
            before,
            "a pod event must update the snapshot, not trigger a pod LIST"
        );
    }

    /// Conformance "should create Endpoints and EndpointSlices for Pods
    /// matching a Service" deletes the slices of a named-port Service and
    /// expects them back (test/e2e/network/endpointslice.go:261-263).
    /// Upstream gets that from `onEndpointSliceDelete`
    /// (pkg/controller/endpointslice/endpointslice_controller.go:583-592),
    /// which queues the owning Service.
    #[tokio::test]
    async fn deleted_slice_is_recreated_without_waiting_for_resync() {
        let storage = Arc::new(MemoryStorage::new());
        let mut svc = snapshot_service("svc");
        svc.spec.selector = Some(HashMap::from([("app".to_string(), "x".to_string())]));
        storage
            .create(&build_key("services", Some("ns"), "svc"), &svc)
            .await
            .unwrap();
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        let es_key = build_key("endpointslices", Some("ns"), "svc");
        let mut created = false;
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if storage.get::<EndpointSlice>(&es_key).await.is_ok() {
                created = true;
                break;
            }
        }
        assert!(created, "slice never created");
        storage.delete(&es_key).await.unwrap();
        let mut back = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if storage.get::<EndpointSlice>(&es_key).await.is_ok() {
                back = true;
                break;
            }
        }
        handle.abort();
        assert!(back, "deleted slice was not recreated");
    }

    fn ev_slice(svc: Option<&str>, managed: &str, uid: &str, generation: i64) -> EndpointSlice {
        let mut l = HashMap::from([(MANAGED_BY_LABEL.to_string(), managed.to_string())]);
        if let Some(s) = svc {
            l.insert(SERVICE_NAME_LABEL.to_string(), s.to_string());
        }
        let mut es = EndpointSlice::new("x-abc", "IPv4");
        es.metadata.namespace = Some("ns".to_string());
        es.metadata.uid = uid.to_string();
        es.metadata.generation = Some(generation);
        es.metadata.labels = Some(l);
        es
    }

    const EV_KEY: &str = "/registry/endpointslices/ns/x-abc";

    fn added(es: &EndpointSlice) -> WatchEvent {
        WatchEvent::Added(EV_KEY.to_string(), serde_json::to_string(es).unwrap())
    }
    fn modified(es: &EndpointSlice) -> WatchEvent {
        WatchEvent::Modified(EV_KEY.to_string(), serde_json::to_string(es).unwrap())
    }
    fn deleted(es: &EndpointSlice) -> WatchEvent {
        WatchEvent::Deleted(EV_KEY.to_string(), serde_json::to_string(es).unwrap())
    }

    /// `onEndpointSliceDelete` (endpointslice_controller.go:583-592): only a
    /// delete the tracker did not expect queues the Service; keyed by
    /// `ServiceControllerKey` (utils.go:201-210).
    #[test]
    fn slice_delete_queues_service_only_when_unexpected() {
        let me = CONTROLLER_NAME;
        let t = EndpointSliceTracker::new();
        let mut known = HashMap::new();
        let s = ev_slice(Some("svc"), me, "u1", 1);
        // Untracked slice (`Has` false): nothing to repair.
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&s)).is_empty());
        // Tracked, deleted by someone else: queue.
        t.update(&s);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &deleted(&s)),
            vec!["services/ns/svc".to_string()]
        );
        // Our own delete (ExpectDeletion): no extra sync.
        t.update(&s);
        t.expect_deletion(&s);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&s)).is_empty());
        // ManagedByController false (reconciler.go:666-669).
        let other = ev_slice(Some("svc"), "someone-else", "u2", 1);
        t.update(&other);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&other)).is_empty());
        // No service-name label: ServiceControllerKey errors.
        let nolabel = ev_slice(None, me, "u3", 1);
        t.update(&nolabel);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &deleted(&nolabel)).is_empty());
    }

    /// `onEndpointSliceAdd`/`Update` (:542-578): our own write (generation
    /// == tracked) does not queue; an external edit (newer generation) does.
    #[test]
    fn slice_update_queues_service_only_for_external_changes() {
        let me = CONTROLLER_NAME;
        let t = EndpointSliceTracker::new();
        let mut known = HashMap::new();
        let own = ev_slice(Some("svc"), me, "u1", 1);
        t.update(&own);
        assert!(services_to_queue_for_slice_event(&t, &mut known, &added(&own)).is_empty());
        assert!(services_to_queue_for_slice_event(&t, &mut known, &modified(&own)).is_empty());
        let edited = ev_slice(Some("svc"), me, "u1", 2);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &modified(&edited)),
            vec!["services/ns/svc".to_string()]
        );
        // An add the tracker has never seen (e.g. after a restart) syncs.
        let fresh = ev_slice(Some("svc"), me, "u9", 1);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &added(&fresh)),
            vec!["services/ns/svc".to_string()]
        );
    }

    /// Label edits do not bump the generation, so `onEndpointSliceUpdate`
    /// handles them explicitly (:564-577).
    #[test]
    fn slice_label_changes_queue_old_and_new_service() {
        let me = CONTROLLER_NAME;
        let t = EndpointSliceTracker::new();
        let mut known = HashMap::new();
        let a = ev_slice(Some("a"), me, "u1", 1);
        t.update(&a);
        services_to_queue_for_slice_event(&t, &mut known, &added(&a));
        let b = ev_slice(Some("b"), me, "u1", 1);
        assert_eq!(
            services_to_queue_for_slice_event(&t, &mut known, &modified(&b)),
            vec!["services/ns/b".to_string(), "services/ns/a".to_string()]
        );
        // managed-by flipped away from this controller: ManagedByChanged.
        let t2 = EndpointSliceTracker::new();
        let mut known2 = HashMap::new();
        let m = ev_slice(Some("a"), me, "u1", 1);
        t2.update(&m);
        services_to_queue_for_slice_event(&t2, &mut known2, &added(&m));
        let unmanaged = ev_slice(Some("a"), "someone-else", "u1", 1);
        assert_eq!(
            services_to_queue_for_slice_event(&t2, &mut known2, &modified(&unmanaged)),
            vec!["services/ns/a".to_string()]
        );
    }

    /// An external edit to a managed slice is repaired without waiting for
    /// the resync (the add/update half of the tracker port).
    #[tokio::test]
    async fn externally_edited_slice_is_repaired_without_waiting_for_resync() {
        let storage = Arc::new(MemoryStorage::new());
        let mut svc = snapshot_service("svc");
        svc.spec.selector = Some(HashMap::from([("app".to_string(), "x".to_string())]));
        storage
            .create(&build_key("services", Some("ns"), "svc"), &svc)
            .await
            .unwrap();
        let controller = Arc::new(EndpointSliceController::new(Arc::clone(&storage)));
        let handle = tokio::spawn(async move { controller.run().await });
        let es_key = build_key("endpointslices", Some("ns"), "svc");
        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if storage.get::<EndpointSlice>(&es_key).await.is_ok() {
                break;
            }
        }
        let mut es: EndpointSlice = storage.get(&es_key).await.unwrap();
        let ports_before = es.ports.clone();
        es.ports = vec![];
        es.metadata.generation = Some(es.metadata.generation.unwrap_or(0) + 1);
        storage.update(&es_key, &es).await.unwrap();
        let mut repaired = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let now: EndpointSlice = storage.get(&es_key).await.unwrap();
            if now.ports == ports_before {
                repaired = true;
                break;
            }
        }
        handle.abort();
        assert!(repaired, "external edit was not repaired");
    }

    /// Upstream has no periodic full resync of its own: only the informers
    /// resync, every MinResyncPeriod (12h default,
    /// staging/src/k8s.io/controller-manager/config/v1alpha1/defaults.go:32).
    #[test]
    fn informer_resync_period_is_not_the_old_5s_sweep() {
        assert!(INFORMER_RESYNC_PERIOD >= std::time::Duration::from_secs(12 * 3600));
    }

    /// `ResyncPeriod` (controllermanager.go:176-181): `Min * (rand + 1)`,
    /// i.e. in [12h, 24h), and not the same every call.
    #[test]
    fn resync_period_is_jittered_between_1x_and_2x() {
        let samples: Vec<_> = (0..50).map(|_| resync_period()).collect();
        for d in &samples {
            assert!(*d >= INFORMER_RESYNC_PERIOD && *d < INFORMER_RESYNC_PERIOD * 2);
        }
        assert!(samples.iter().any(|d| *d != samples[0]), "no jitter");
    }

    fn labelled(name: &str, labels: &[(&str, &str)]) -> Pod {
        let mut pod = snapshot_pod(name, "10.0.0.1");
        pod.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        pod.metadata.resource_version = Some("1".into());
        pod
    }

    fn svc_with_selector(name: &str, sel: Option<&[(&str, &str)]>) -> Service {
        let mut s = snapshot_service(name);
        s.spec.selector = sel.map(|l| {
            l.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        s
    }

    fn set(keys: &[&str]) -> std::collections::HashSet<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    /// TestDetermineNeededServiceUpdates
    /// (staging/src/k8s.io/endpointslice/util/controller_utils_test.go:35).
    #[test]
    fn determine_needed_service_updates_cases() {
        let (a, b, c) = (set(&["a", "b"]), set(&["b", "c"]), set(&[]));
        assert_eq!(
            determine_needed_service_updates(&a, &b, true),
            set(&["a", "b", "c"])
        );
        assert_eq!(
            determine_needed_service_updates(&a, &b, false),
            set(&["a", "c"])
        );
        assert_eq!(determine_needed_service_updates(&a, &a, false), c);
    }

    /// Add / delete / resync / unrelated-update projection
    /// (controller_utils.go:58-103).
    #[test]
    fn projection_key_add_delete_and_ignored_updates() {
        let p = labelled("p", &[("app", "x")]);
        let add = get_pod_update_projection_key(None, Some(&p)).unwrap();
        assert_eq!(add.namespace, "ns");
        assert_eq!(add.old_labels, None);
        assert!(!add.pod_changed);
        assert_eq!(get_pod_update_projection_key(Some(&p), None), Some(add));
        assert_eq!(get_pod_update_projection_key(None, None), None);
        // same resourceVersion == informer resync: ignored
        assert_eq!(
            get_pod_update_projection_key(Some(&p), Some(&p.clone())),
            None
        );
        // new RV but nothing endpoint-relevant changed: ignored
        let mut q = p.clone();
        q.metadata.resource_version = Some("2".into());
        assert_eq!(get_pod_update_projection_key(Some(&p), Some(&q)), None);
    }

    /// `newPod.ResourceVersion == oldPod.ResourceVersion` (controller_utils.go:79)
    /// compares Go strings, so two UNSET resourceVersions are equal and the
    /// update is ignored as a resync (#2365).
    #[test]
    fn projection_key_two_unset_resource_versions_are_equal() {
        let mut p = labelled("p", &[("app", "x")]);
        p.metadata.resource_version = None;
        let mut relabelled = p.clone();
        relabelled.metadata.labels = Some([("app".to_string(), "y".to_string())].into());
        assert_eq!(
            get_pod_update_projection_key(Some(&p), Some(&relabelled)),
            None
        );
    }

    /// Test_podChanged (controller_utils_test.go:448): readiness / IP change
    /// is a pod change with unchanged labels; a label change carries both
    /// label sets.
    #[test]
    fn projection_key_pod_and_label_changes() {
        let p = labelled("p", &[("app", "x")]);
        let mut ready = p.clone();
        ready.metadata.resource_version = Some("2".into());
        ready.status.as_mut().unwrap().conditions = None;
        let k = get_pod_update_projection_key(Some(&p), Some(&ready)).unwrap();
        assert_eq!(k.old_labels, None);

        let mut relabelled = p.clone();
        relabelled.metadata.resource_version = Some("2".into());
        relabelled.metadata.labels = Some(HashMap::from([("app".into(), "y".into())]));
        let k = get_pod_update_projection_key(Some(&p), Some(&relabelled)).unwrap();
        assert_eq!(k.labels["app"], "y");
        assert_eq!(k.old_labels.as_ref().unwrap()["app"], "x");
        assert!(!k.pod_changed);

        relabelled.status.as_mut().unwrap().conditions = None;
        let k = get_pod_update_projection_key(Some(&p), Some(&relabelled)).unwrap();
        assert!(k.pod_changed);
    }

    /// TestGetPodServicesToUpdate (controller_utils_test.go:333): a delete
    /// enqueues only matching services; nil selectors match nothing; a pure
    /// relabel enqueues only the symmetric difference.
    #[test]
    fn services_to_update_projection() {
        let services = vec![
            svc_with_selector("x", Some(&[("app", "x")])),
            svc_with_selector("y", Some(&[("app", "y")])),
            svc_with_selector("none", None),
        ];
        let p = labelled("p", &[("app", "x")]);
        let key = get_pod_update_projection_key(Some(&p), None).unwrap();
        assert_eq!(
            get_services_to_update(&services, &key),
            set(&["services/ns/x"])
        );

        let mut relabelled = p.clone();
        relabelled.metadata.resource_version = Some("2".into());
        relabelled.metadata.labels = Some(HashMap::from([("app".into(), "y".into())]));
        let key = get_pod_update_projection_key(Some(&p), Some(&relabelled)).unwrap();
        assert_eq!(
            get_services_to_update(&services, &key),
            set(&["services/ns/x", "services/ns/y"])
        );
    }

    /// `WaitForNamedCacheSyncWithContext(ctx, c.podsSynced, ...)`
    /// (endpointslice_controller.go:294): blocks until the first pod LIST has
    /// landed, then stays open (`HasSynced` never regresses).
    #[tokio::test]
    async fn wait_for_pods_synced_blocks_until_first_pod_list() {
        let controller = EndpointSliceController::new(Arc::new(MemoryStorage::new()));
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            controller.wait_for_pods_synced(),
        )
        .await;
        assert!(
            pending.is_err(),
            "wait returned before the pod cache synced"
        );
        assert!(controller.sync_pod_cache().await);
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            controller.wait_for_pods_synced(),
        )
        .await
        .expect("wait must return once synced");
        // a later relist start clears the snapshot but not the sync gate
        *controller.pod_cache.write().unwrap() = None;
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            controller.wait_for_pods_synced(),
        )
        .await
        .expect("gate must not regress");
    }

    /// podQueue (endpointslice_controller.go:247, onPodUpdate :532, syncPod
    /// :491): a pod event only lands a projection key on the pod queue; a
    /// separate sync resolves it to the matching services.
    #[tokio::test]
    async fn pod_event_goes_through_pod_queue_to_matching_services() {
        let storage = Arc::new(MemoryStorage::new());
        let mut hit = snapshot_service("hit");
        hit.spec.selector = Some([("app".to_string(), "x".to_string())].into());
        let mut miss = snapshot_service("miss");
        miss.spec.selector = Some([("app".to_string(), "other".to_string())].into());
        for svc in [&hit, &miss] {
            let name = svc.metadata.name.clone();
            storage
                .create(&build_key("services", Some("ns"), &name), svc)
                .await
                .unwrap();
        }
        let controller = EndpointSliceController::new(storage);
        let service_queue = WorkQueue::new();

        let pod = labelled("p", &[("app", "x")]);
        controller.on_pod_update(None, Some(&pod));
        // nothing is resolved until the pod worker syncs the key
        assert!(service_queue.is_empty().await);

        let item = controller.pod_queue_rx.lock().await.recv().await.unwrap();
        controller
            .sync_pod(&item.key, &service_queue)
            .await
            .unwrap();
        assert_eq!(service_queue.len().await, 1);
        assert_eq!(service_queue.get().await.unwrap(), "services/ns/hit");

        // an ignored update (resync) never reaches the queue
        controller.on_pod_update(Some(&pod), Some(&pod.clone()));
        assert!(controller.pod_queue_rx.lock().await.try_recv().is_err());
    }

    /// The API server's generic Store turns a non-empty `metadata.uid` on a PUT
    /// into a UID precondition (rest/update.go:188-203). A slice the controller
    /// rebuilt from scratch carries a new random UID, so every update of an
    /// existing slice was a 409 and the slice stayed at its first (empty)
    /// content: the sig-network regression after #2108.
    #[tokio::test]
    async fn updating_an_existing_slice_keeps_its_uid() {
        use crate::controllers::uid_precondition_double::UidPreconditionStorage;

        let storage = Arc::new(UidPreconditionStorage::new());
        let controller = EndpointSliceController::new(Arc::clone(&storage));

        let service = Service {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Service".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("svc").with_namespace("default"),
            spec: ServiceSpec {
                ports: vec![ServicePort {
                    name: None,
                    port: 80,
                    target_port: None,
                    protocol: "TCP".to_string(),
                    node_port: None,
                    app_protocol: None,
                }],
                selector: Some(HashMap::from([("app".to_string(), "test".to_string())])),
                ..Default::default()
            },
            status: None,
        };
        storage
            .create(&build_key("services", Some("default"), "svc"), &service)
            .await
            .unwrap();

        // No pod yet: the slice is created empty.
        controller.reconcile_service(&service).await.unwrap();
        let slice_key = build_key("endpointslices", Some("default"), "svc");
        let first: EndpointSlice = storage.get(&slice_key).await.unwrap();
        assert!(first.endpoints.is_empty());

        // A ready pod appears: the existing slice must be updated.
        let pod = Pod {
            type_meta: rusternetes_common::types::TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta::new("p1")
                .with_namespace("default")
                .with_labels(HashMap::from([("app".to_string(), "test".to_string())])),
            spec: Some(rusternetes_common::resources::PodSpec {
                containers: vec![rusternetes_common::resources::Container {
                    name: "c".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(rusternetes_common::resources::PodStatus {
                phase: Some(Phase::Running),
                pod_ip: Some("10.0.0.1".to_string()),
                conditions: Some(vec![rusternetes_common::resources::PodCondition {
                    condition_type: "Ready".to_string(),
                    status: "True".to_string(),
                    reason: None,
                    message: None,
                    last_probe_time: None,
                    last_transition_time: None,
                    observed_generation: None,
                }]),
                ..Default::default()
            }),
        };
        storage
            .create(&build_key("pods", Some("default"), "p1"), &pod)
            .await
            .unwrap();

        controller.reconcile_service(&service).await.unwrap();
        let second: EndpointSlice = storage.get(&slice_key).await.unwrap();
        assert_eq!(second.endpoints.len(), 1);
        assert_eq!(second.metadata.uid, first.metadata.uid);
    }
}
