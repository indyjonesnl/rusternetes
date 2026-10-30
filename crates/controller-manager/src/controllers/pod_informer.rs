//! The part of a shared pod informer that the ReplicaSet and
//! ReplicationController event handlers depend on: telling an *add* from an
//! *update*, and noticing adds and deletes that happened while the watch was
//! down.
//!
//! Upstream's `ReplicaSetController` never lowers its own expectations. The
//! informer's handlers do, one event at a time
//! (`pkg/controller/replicaset/replica_set.go:addPod :418-455`,
//! `updatePod :458-529`, `deletePod :533-570`), and the informer decides which
//! handler runs by consulting its indexer: an object it already holds is an
//! update (`OnUpdate`), a new one is an add (`OnAdd`)
//! (`staging/src/k8s.io/client-go/tools/cache/controller.go:581-607`,
//! `processDeltas`: `case Sync, Replaced, Added, Updated` calls `OnUpdate` if
//! `clientState.Get` finds the object, else `OnAdd`). When a watch drops, the reflector re-lists and
//! `DeltaFIFO.Replace` turns the difference into adds for pods that appeared
//! and `DeletedFinalStateUnknown` deletes for pods that vanished
//! (`delta_fifo.go:555`, `Replace`), so no creation or deletion is lost to the
//! expectations.
//!
//! Rusternetes' controllers consume a raw storage watch with no indexer, so
//! this cache is that indexer, restricted to what the handlers read: per pod,
//! its controller reference (`metav1.GetControllerOf`), a digest of its labels
//! and its resourceVersion. The three handlers themselves are
//! [`PodInformerCache::on_event`] and [`PodInformerCache::replace`], which
//! decide *what* to do; [`apply_pod_observation`] does it against a
//! controller, the part that differs between the ReplicaSet and the
//! ReplicationController (`resolveControllerRef`, `getPodReplicaSets`).

use super::controller_utils::is_pod_ready;
use super::job_tracking::has_job_tracking_finalizer;
use futures::StreamExt;
use rusternetes_common::resources::Pod;
use rusternetes_storage::{build_prefix, Storage, WatchEvent, WorkQueue};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{error, warn};

/// `metav1.OwnerReference`, restricted to what `resolveControllerRef` reads
/// (replica_set.go:305-321): the kind, the name to look the controller up by,
/// and the UID that must still match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerRef {
    pub(crate) kind: String,
    pub(crate) name: String,
    pub(crate) uid: String,
}

/// One thing a handler does for a pod event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PodEffect {
    /// `addPod` of a controlled live pod: resolve the controller, then
    /// `expectations.CreationObserved` and enqueue it (replica_set.go:426-440).
    ObserveCreation(ControllerRef),
    /// `deletePod` of a controlled pod: resolve the controller, then
    /// `expectations.DeletionObserved(key, PodKey(pod))` and enqueue it
    /// (replica_set.go:533-570).
    ObserveDeletion(ControllerRef),
    /// `enqueueRS` of the controller the reference resolves to
    /// (`updatePod`, :484-514).
    Enqueue(ControllerRef),
    /// `enqueueRSAfter(rs, MinReadySeconds)` of the controller the reference
    /// resolves to, when it has `minReadySeconds > 0`: the pod just became
    /// Ready, so availability is re-checked once `minReadySeconds` have
    /// passed (`updatePod`, :493-515).
    EnqueueAfterMinReady(ControllerRef),
    /// `getPodReplicaSets(pod)` then `enqueueRS` for each: an orphan was
    /// added, or changed, and the controllers that could adopt it are synced
    /// (:441-453, :517-528). Carries the pod's labels.
    EnqueueMatching(HashMap<String, String>),
    /// The Job controller's `deletePod` for a pod it controls
    /// (`job_controller.go:434-488`): unlike `ObserveDeletion` it depends on
    /// whether the Job is finished and on the pod's tracking finalizer. Only
    /// produced for a pod whose controller is a `Job`.
    ObserveTrackedDeletion {
        controller: ControllerRef,
        uid: String,
        has_finalizer: bool,
        /// `deletePod(..., final)`: the pod is gone, not just terminating.
        final_delete: bool,
    },
    /// `finalizerExpectations.finalizerRemovalObserved(key, curPod.UID)` for
    /// the Job the reference resolves to (`updatePod`, :381-387, :408-413).
    /// Only produced for a pod whose controller is a `Job` and whose
    /// tracking finalizer is gone.
    ObserveFinalizerRemoval {
        controller: ControllerRef,
        uid: String,
    },
    /// `enqueueOrphanPod(pod, false)`: a pod without a controller still holds
    /// the Job tracking finalizer (:344-347, :417-420, :458-462).
    ReleaseOrphan,
}

/// What a pod event means for the controllers' expectations and queue: the
/// effects, in the order the handler performs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodObservation {
    pub(crate) pod_key: String,
    pub(crate) effects: Vec<PodEffect>,
}

/// What the informer's indexer holds about one pod.
#[derive(Debug)]
struct KnownPod {
    /// `metav1.GetControllerOf(pod)`.
    controller: Option<ControllerRef>,
    /// Digest of `pod.Labels`, for `labelChanged` (:467).
    labels: u64,
    /// Digest of `pod.ResourceVersion`, for the "periodic resync" guard
    /// (:459-463). `None` when the pod carries none.
    resource_version: Option<u64>,
    /// `podutil.IsPodReady(pod)`, for the Ready-transition check (:509).
    ready: bool,
    /// `pod.DeletionTimestamp != nil`, for replaying `addPod`'s
    /// "already pending deletion" branch to a handler added later.
    terminating: bool,
    /// `pod.UID` (`finalizerRemovalObserved` is keyed by it).
    uid: String,
    /// `hasJobTrackingFinalizer(pod)`.
    tracking_finalizer: bool,
}

/// Pod key ("namespace/name") to what the handlers read of it.
#[derive(Debug, Default)]
pub struct PodInformerCache {
    known: Mutex<HashMap<String, KnownPod>>,
}

fn digest<T: Hash + ?Sized>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn labels_digest(labels: Option<&HashMap<String, String>>) -> u64 {
    let mut pairs: Vec<(&String, &String)> = labels.into_iter().flatten().collect();
    pairs.sort();
    digest(&pairs)
}

/// `metav1.GetControllerOf`: the first owner reference with `controller: true`
/// (`apimachinery/pkg/apis/meta/v1/controller_ref.go`).
fn controller_of(pod: &Pod) -> Option<ControllerRef> {
    pod.metadata
        .owner_references
        .iter()
        .flatten()
        .find(|r| r.controller == Some(true))
        .map(|r| ControllerRef {
            kind: r.kind.clone(),
            name: r.name.clone(),
            uid: r.uid.clone(),
        })
}

fn known_of(pod: &Pod) -> KnownPod {
    KnownPod {
        controller: controller_of(pod),
        labels: labels_digest(pod.metadata.labels.as_ref()),
        resource_version: pod.metadata.resource_version.as_deref().map(digest),
        ready: is_pod_ready(pod),
        terminating: pod.metadata.deletion_timestamp.is_some(),
        uid: pod.metadata.uid.clone(),
        tracking_finalizer: has_job_tracking_finalizer(pod),
    }
}

/// "namespace/name" from a storage key `[/]registry/pods/<ns>/<name>` or
/// `pods/<ns>/<name>`.
pub(crate) fn pod_key_from_storage_key(storage_key: &str) -> Option<String> {
    let parts: Vec<&str> = storage_key.trim_start_matches('/').split('/').collect();
    let pods = parts.iter().position(|p| *p == "pods")?;
    match (parts.get(pods + 1), parts.get(pods + 2)) {
        (Some(ns), Some(name)) => Some(format!("{ns}/{name}")),
        _ => None,
    }
}

/// `deletePod` for a pod (replica_set.go:533-570): no controller reference,
/// nothing ("No controller should care about orphans being deleted").
///
/// The Job controller's `deletePod` (job_controller.go:434-488) differs: an
/// orphan still holding the tracking finalizer is released ("But this pod
/// might have belonged to a Job and the GC removed the reference"), and the
/// observation depends on the pod's finalizer and on whether the delete is
/// final, so a pod controlled by a `Job` gets its own effect.
fn delete_pod(
    controller: Option<ControllerRef>,
    uid: &str,
    has_finalizer: bool,
    final_delete: bool,
) -> Option<PodEffect> {
    match controller {
        None => has_finalizer.then_some(PodEffect::ReleaseOrphan),
        Some(controller) if controller.kind == "Job" => Some(PodEffect::ObserveTrackedDeletion {
            controller,
            uid: uid.to_string(),
            has_finalizer,
            final_delete,
        }),
        Some(controller) => Some(PodEffect::ObserveDeletion(controller)),
    }
}

impl PodInformerCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The informer's handler for a pod that is present: `OnAdd` when the
    /// indexer did not hold it, `OnUpdate` when it did. Updates `known`.
    fn handle_present(
        known: &mut HashMap<String, KnownPod>,
        pod_key: &str,
        pod: &Pod,
    ) -> Vec<PodEffect> {
        let cur = known_of(pod);
        let old = known.insert(pod_key.to_string(), known_of(pod));
        let mut effects = Vec::new();

        let Some(old) = old else {
            // `addPod` (:418-455).
            if pod.metadata.deletion_timestamp.is_some() {
                // "is already pending deletion. Prevent the pod from being a
                // creation observation" (:418-422).
                effects.extend(delete_pod(
                    cur.controller,
                    &cur.uid,
                    cur.tracking_finalizer,
                    false,
                ));
            } else if let Some(controller) = cur.controller {
                // "If it has a ControllerRef, that's all that matters."
                effects.push(PodEffect::ObserveCreation(controller));
            } else {
                // "Otherwise, it's an orphan. Get a list of all matching
                // ReplicaSets and sync them to see if anyone wants to adopt
                // it. DO NOT observe creation because no controller should be
                // waiting for an orphan."
                // The Job controller first "Clean[s] the finalizer"
                // (job_controller.go:344-347).
                if cur.tracking_finalizer {
                    effects.push(PodEffect::ReleaseOrphan);
                }
                effects.push(PodEffect::EnqueueMatching(
                    pod.metadata.labels.clone().unwrap_or_default(),
                ));
            }
            return effects;
        };

        // `updatePod` (:458-529).
        if cur.resource_version.is_some() && cur.resource_version == old.resource_version {
            // "Periodic resync will send update events for all known pods.
            // Two different versions of the same pod will always have
            // different RVs."
            return effects;
        }
        let label_changed = cur.labels != old.labels;
        if pod.metadata.deletion_timestamp.is_some() {
            // A graceful delete first MODIFIES the pod; the controller must
            // not wait for the kubelet's final delete (:468-480).
            effects.extend(delete_pod(
                cur.controller,
                &cur.uid,
                cur.tracking_finalizer,
                false,
            ));
            // The ReplicaSet's `updatePod` also observes the old controller
            // when the labels changed (:468-480); the Job's does not
            // (job_controller.go:370-377).
            if label_changed && old.controller.as_ref().is_none_or(|c| c.kind != "Job") {
                effects.extend(delete_pod(
                    old.controller,
                    &old.uid,
                    old.tracking_finalizer,
                    false,
                ));
            }
            return effects;
        }

        let controller_ref_changed = cur.controller != old.controller;
        // The Job's `updatePod` (job_controller.go:379-413): "Don't check if
        // oldPod has the finalizer, as during ownership transfer finalizers
        // might be re-added and removed again in behalf of the new owner."
        let finalizer_removed = !cur.tracking_finalizer;
        let removal = |controller: &ControllerRef| {
            (finalizer_removed && controller.kind == "Job").then(|| {
                PodEffect::ObserveFinalizerRemoval {
                    controller: controller.clone(),
                    uid: cur.uid.clone(),
                }
            })
        };
        if controller_ref_changed {
            if let Some(old_controller) = old.controller {
                // "The ControllerRef was changed. Sync the old controller, if
                // any."
                effects.extend(removal(&old_controller));
                effects.push(PodEffect::Enqueue(old_controller));
            }
        }
        if let Some(controller) = cur.controller {
            effects.extend(removal(&controller));
            effects.push(PodEffect::Enqueue(controller.clone()));
            // "MinReadySeconds in the Pod will generate an Available
            // condition ... we can fake the update by resyncing the
            // controller MinReadySeconds after the it is requeued because a
            // Pod transitioned to Ready."
            if !old.ready && cur.ready {
                effects.push(PodEffect::EnqueueAfterMinReady(controller));
            }
        } else {
            // The Job's "Otherwise, it's an orphan. Clean the finalizer."
            // (job_controller.go:417-420).
            if cur.tracking_finalizer {
                effects.push(PodEffect::ReleaseOrphan);
            }
            if label_changed || controller_ref_changed {
                // "Otherwise, it's an orphan. If anything changed, sync
                // matching controllers to see if anyone wants to adopt it
                // now."
                effects.push(PodEffect::EnqueueMatching(
                    pod.metadata.labels.clone().unwrap_or_default(),
                ));
            }
        }
        effects
    }

    /// Classify one watch event, updating the cache the way the informer's
    /// indexer would. `None` when the event cannot be attributed to a pod.
    pub fn on_event(&self, event: &WatchEvent) -> Option<PodObservation> {
        let (storage_key, value, deleted) = match event {
            WatchEvent::Added(k, v) | WatchEvent::Modified(k, v) => (k, v, false),
            WatchEvent::Deleted(k, v) => (k, v, true),
        };
        let pod_key = pod_key_from_storage_key(storage_key)?;
        let pod = serde_json::from_str::<Pod>(value).ok();
        let mut known = self.known.lock().unwrap();

        if deleted {
            // The event carries the last known value; fall back to what the
            // cache held (the tombstone) when it cannot be decoded.
            let cached = known.remove(&pod_key);
            let (controller, uid, has_finalizer) = match (&pod, cached) {
                (Some(pod), _) => (
                    controller_of(pod),
                    pod.metadata.uid.clone(),
                    has_job_tracking_finalizer(pod),
                ),
                (None, Some(k)) => (k.controller, k.uid, k.tracking_finalizer),
                (None, None) => (None, String::new(), false),
            };
            return Some(PodObservation {
                pod_key,
                effects: delete_pod(controller, &uid, has_finalizer, true)
                    .into_iter()
                    .collect(),
            });
        }

        let effects = Self::handle_present(&mut known, &pod_key, &pod?);
        Some(PodObservation { pod_key, effects })
    }

    /// What `AddEventHandler` replays to a handler added after the informer
    /// holds pods: an `addPod` for each held pod. An orphan's
    /// `getPodReplicaSets` enqueue is not replayed (the labels are not kept,
    /// to keep the cache small); the controller's own initial full enqueue
    /// covers it.
    fn held_as_adds(&self) -> Vec<PodObservation> {
        let known = self.known.lock().unwrap();
        known
            .iter()
            .filter_map(|(pod_key, k)| {
                let effect = if k.terminating {
                    delete_pod(k.controller.clone(), &k.uid, k.tracking_finalizer, false)?
                } else if let Some(controller) = k.controller.clone() {
                    PodEffect::ObserveCreation(controller)
                } else if k.tracking_finalizer {
                    // `addPod` of an orphan holding the Job tracking
                    // finalizer (job_controller.go:344-347).
                    PodEffect::ReleaseOrphan
                } else {
                    return None;
                };
                Some(PodObservation {
                    pod_key: pod_key.clone(),
                    effects: vec![effect],
                })
            })
            .collect()
    }

    /// The reflector's re-list on (re)connect: `DeltaFIFO.Replace`. Pods not
    /// seen before are adds, pods already held are updates, and pods that
    /// disappeared while the watch was down are deletes
    /// (`DeletedFinalStateUnknown`).
    pub(crate) fn replace(&self, pods: &[Pod]) -> Vec<PodObservation> {
        let mut observations = Vec::new();
        let mut known = self.known.lock().unwrap();
        let mut present: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(pods.len());
        for pod in pods {
            let pod_key = format!(
                "{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or(""),
                pod.metadata.name
            );
            let effects = Self::handle_present(&mut known, &pod_key, pod);
            present.insert(pod_key.clone());
            if !effects.is_empty() {
                observations.push(PodObservation { pod_key, effects });
            }
        }
        let vanished: Vec<String> = known
            .keys()
            .filter(|k| !present.contains(*k))
            .cloned()
            .collect();
        for pod_key in vanished {
            let (controller, uid, has_finalizer) = known
                .remove(&pod_key)
                .map(|k| (k.controller, k.uid, k.tracking_finalizer))
                .unwrap_or_default();
            observations.push(PodObservation {
                pod_key,
                effects: delete_pod(controller, &uid, has_finalizer, true)
                    .into_iter()
                    .collect(),
            });
        }
        observations
    }
}

/// What `resolveControllerRef` returns: upstream hands back the controller
/// object, and the handlers read its name and `Spec.MinReadySeconds`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedController {
    pub(crate) name: String,
    /// `rs.Spec.MinReadySeconds` (0 when unset).
    pub(crate) min_ready_seconds: i32,
}

/// The controller-specific half of the pod event handlers.
pub trait PodEventTarget: Sync {
    /// The work-queue key prefix: `replicasets` or `replicationcontrollers`.
    fn queue_prefix(&self) -> &'static str;

    /// `resolveControllerRef` (replica_set.go:302-321): the controller
    /// `controller_ref` points at, or `None` when it is of another
    /// kind, no longer exists, or has a different UID.
    fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> impl Future<Output = Option<ResolvedController>> + Send;

    /// `getPodReplicaSets` (replica_set.go:289-300): the names of the
    /// controllers in `namespace` that could adopt a pod with these labels.
    fn pod_controllers(
        &self,
        namespace: &str,
        pod_labels: &HashMap<String, String>,
    ) -> impl Future<Output = Vec<String>> + Send;

    /// `expectations.CreationObserved(key)`.
    fn creation_observed(&self, key: &str);

    /// `expectations.DeletionObserved(key, PodKey(pod))`.
    fn deletion_observed(&self, key: &str, pod_key: &str);

    /// `expectations.CreationObserved(key)` for the pod `pod_key`. Defaults to
    /// the plain count; a controller that tracks WHICH pods it is waiting for
    /// (the DaemonSet controller's per-pod creation set, the Job controller's
    /// created-pod keys) overrides it.
    fn creation_observed_of(&self, key: &str, _pod_key: &str) {
        self.creation_observed(key);
    }

    /// How long after a pod turns Ready the controller is requeued to
    /// re-check availability. `enqueueRSAfter(rs, MinReadySeconds)`
    /// (replica_set.go:511) waits exactly `minReadySeconds`; the DaemonSet
    /// controller adds a second (daemon_controller.go, `updatePod`).
    fn min_ready_requeue_delay(&self, min_ready_seconds: i32) -> Duration {
        Duration::from_secs(min_ready_seconds as u64)
    }

    /// How long a handler's enqueue is delayed: the Job controller's
    /// `enqueueSyncJobBatched` adds `SyncJobBatchPeriod` (job_controller.go:593-600);
    /// the ReplicaSet's `enqueueRS` is immediate.
    fn enqueue_delay(&self) -> Duration {
        Duration::ZERO
    }

    /// `util.IsJobFinished(job)`, for `deletePod` (job_controller.go:468-474).
    fn controller_finished(
        &self,
        _namespace: &str,
        _name: &str,
    ) -> impl Future<Output = bool> + Send {
        async { false }
    }

    /// `jm.finalizerExpectations.finalizerRemovalObserved(logger, key, uid)`
    /// (job_controller.go:389-394, :404-411, :484).
    fn finalizer_removal_observed(&self, _key: &str, _uid: &str) {}

    /// `jm.enqueueOrphanPod(pod, false)` (job_controller.go:347, :420, :463,
    /// :471): handle the pod `pod_name` in `namespace`, which holds the
    /// tracking finalizer with no Job able to account for it.
    fn orphan_pod(&self, _namespace: &str, _pod_name: &str) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// Run the effects of one observation against `target`.
pub async fn apply_pod_observation<T: PodEventTarget>(
    target: &T,
    queue: &WorkQueue,
    observation: &PodObservation,
) {
    let pod_key = observation.pod_key.as_str();
    let Some((namespace, pod_name)) = pod_key.split_once('/') else {
        return;
    };
    let enqueue = |name: &str| format!("{}/{namespace}/{name}", target.queue_prefix());
    let delay = target.enqueue_delay();
    // `enqueueRS` / `enqueueSyncJobBatched`: now, or after the target's batch
    // period.
    let add = |name: String| async move {
        if delay.is_zero() {
            queue.add(name).await;
        } else {
            queue.add_after(name, delay).await;
        }
    };
    for effect in &observation.effects {
        match effect {
            PodEffect::ObserveCreation(controller_ref) => {
                if let Some(ResolvedController { name, .. }) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    target.creation_observed_of(&format!("{namespace}/{name}"), pod_key);
                    add(enqueue(&name)).await;
                }
            }
            PodEffect::ObserveDeletion(controller_ref) => {
                if let Some(ResolvedController { name, .. }) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    target.deletion_observed(&format!("{namespace}/{name}"), pod_key);
                    add(enqueue(&name)).await;
                }
            }
            PodEffect::Enqueue(controller_ref) => {
                if let Some(ResolvedController { name, .. }) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    add(enqueue(&name)).await;
                }
            }
            PodEffect::EnqueueAfterMinReady(controller_ref) => {
                if let Some(ResolvedController {
                    name,
                    min_ready_seconds,
                }) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    if min_ready_seconds > 0 {
                        queue
                            .add_after(
                                enqueue(&name),
                                target.min_ready_requeue_delay(min_ready_seconds),
                            )
                            .await;
                    }
                }
            }
            PodEffect::ObserveTrackedDeletion {
                controller,
                uid,
                has_finalizer,
                final_delete,
            } => {
                // `deletePod` (job_controller.go:434-488).
                let resolved = target.resolve_controller_ref(namespace, controller).await;
                let finished = match &resolved {
                    Some(ResolvedController { name, .. }) => {
                        target.controller_finished(namespace, name).await
                    }
                    None => false,
                };
                let Some(ResolvedController { name, .. }) = resolved.filter(|_| !finished) else {
                    // "syncJob will not remove this finalizer."
                    if *has_finalizer {
                        target.orphan_pod(namespace, pod_name).await;
                    }
                    continue;
                };
                let job_key = format!("{namespace}/{name}");
                target.deletion_observed(&job_key, pod_key);
                // "Consider the finalizer removed if this is the final delete.
                // Otherwise, it's an update for the deletion timestamp, then
                // check finalizer."
                if *final_delete || !*has_finalizer {
                    target.finalizer_removal_observed(&job_key, uid);
                }
                add(enqueue(&name)).await;
            }
            PodEffect::ObserveFinalizerRemoval { controller, uid } => {
                if let Some(ResolvedController { name, .. }) =
                    target.resolve_controller_ref(namespace, controller).await
                {
                    target.finalizer_removal_observed(&format!("{namespace}/{name}"), uid);
                }
            }
            PodEffect::ReleaseOrphan => target.orphan_pod(namespace, pod_name).await,
            PodEffect::EnqueueMatching(labels) => {
                for name in target.pod_controllers(namespace, labels).await {
                    add(enqueue(&name)).await;
                }
            }
        }
    }
}

/// One registered `ResourceEventHandler`: a [`PodEventTarget`] and the queue
/// its handlers enqueue onto. Object-safe so one informer can hold targets of
/// different controllers (`PodEventTarget` returns `impl Future`).
trait PodObservationHandler: Send + Sync {
    fn handle<'a>(
        &'a self,
        observation: &'a PodObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

struct RegisteredTarget<T> {
    target: Arc<T>,
    queue: WorkQueue,
}

impl<T: PodEventTarget + Send + Sync + 'static> PodObservationHandler for RegisteredTarget<T> {
    fn handle<'a>(
        &'a self,
        observation: &'a PodObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(apply_pod_observation(
            &*self.target,
            &self.queue,
            observation,
        ))
    }
}

/// The shared pod informer: ONE watch + re-list task owning ONE
/// [`PodInformerCache`], fanning each classified [`PodObservation`] out to
/// every registered [`PodEventTarget`].
///
/// Upstream classifies each delta once against the informer's single indexer
/// (`client-go/tools/cache/controller.go:581-607`, `processDeltas`: the
/// `clientState.Get` hit chooses `OnUpdate`, a miss `OnAdd`) and
/// `sharedIndexInformer.OnAdd`/`OnUpdate` hand the notification to
/// `sharedProcessor.distribute` (`shared_informer.go:863-880`), which delivers
/// it to every listener. Classifying once per controller against a shared
/// cache would instead show the second controller an update where the first
/// saw the add, and the add is what lowers `ControllerExpectations`
/// (replica_set.go:436). Here the cache is consulted exactly once per event,
/// before any target is told.
pub struct SharedPodInformer<S: Storage> {
    storage: Arc<S>,
    interval: Duration,
    cache: PodInformerCache,
    handlers: Mutex<Vec<Arc<dyn PodObservationHandler>>>,
    /// `sharedIndexInformer.blockDeltas` (shared_informer.go:444-446): held
    /// by event distribution and by a late handler's registration + replay.
    block_deltas: tokio::sync::Mutex<()>,
    started: AtomicBool,
}

impl<S: Storage + 'static> SharedPodInformer<S> {
    pub fn new(storage: Arc<S>, interval: Duration) -> Self {
        Self {
            storage,
            interval,
            cache: PodInformerCache::new(),
            handlers: Mutex::new(Vec::new()),
            block_deltas: tokio::sync::Mutex::new(()),
            started: AtomicBool::new(false),
        }
    }

    /// The informer's indexer, for a target driving its own pass.
    pub fn cache(&self) -> &PodInformerCache {
        &self.cache
    }

    /// `AddEventHandler` (`shared_informer.go:656`,
    /// `sharedProcessor.addListener` :821): from now on every observation reaches `target`, enqueuing onto
    /// `queue`. Like upstream, a handler added after the informer holds pods
    /// is told about them: the held pods are replayed to it as adds
    /// (`shared_informer.go:702`: "for _, item := range s.indexer.List()
    /// { listener.add(addNotification{...}) }").
    pub(crate) async fn add_event_handler<T: PodEventTarget + Send + Sync + 'static>(
        &self,
        target: Arc<T>,
        queue: WorkQueue,
    ) {
        let handler: Arc<dyn PodObservationHandler> = Arc::new(RegisteredTarget { target, queue });
        // shared_informer.go:698-699: `s.blockDeltas.Lock()` across
        // addListener and the replay, so no delta is distributed in between.
        let _block = self.block_deltas.lock().await;
        for observation in self.cache.held_as_adds() {
            handler.handle(&observation).await;
        }
        self.handlers.lock().unwrap().push(handler);
    }

    /// `sharedProcessor.distribute` (shared_informer.go:863): every listener
    /// gets the notification.
    async fn distribute(&self, observation: &PodObservation) {
        let handlers: Vec<_> = self.handlers.lock().unwrap().clone();
        for handler in handlers {
            handler.handle(observation).await;
        }
    }

    /// One pod watch event: classified once, delivered to every handler.
    pub(crate) async fn on_event(&self, event: &WatchEvent) {
        // HandleDeltas (shared_informer.go:717-718): `s.blockDeltas.Lock()`.
        let _block = self.block_deltas.lock().await;
        if let Some(observation) = self.cache.on_event(event) {
            self.distribute(&observation).await;
        }
    }

    /// The reflector's list (`DeltaFIFO.Replace`) delivered to every handler.
    pub(crate) async fn relist(&self) {
        match self.storage.list::<Pod>(&build_prefix("pods", None)).await {
            Ok(pods) => {
                let _block = self.block_deltas.lock().await;
                for observation in self.cache.replace(&pods) {
                    self.distribute(&observation).await;
                }
            }
            Err(e) => error!("Failed to list pods for the informer re-list: {}", e),
        }
    }

    /// Start the one informer task; later calls are no-ops, so every
    /// controller sharing the informer can call this from its `run()`.
    pub(crate) fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let informer = Arc::clone(self);
        tokio::spawn(async move { informer.run().await });
    }

    async fn run(&self) {
        loop {
            let mut watch = match self.storage.watch(&build_prefix("pods", None)).await {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        "Failed to establish pod watch: {}, retrying in {:?}",
                        e, self.interval
                    );
                    tokio::time::sleep(self.interval).await;
                    continue;
                }
            };
            // The reflector's list: with the watch established first, nothing
            // that happens from here on is missed, and the pods created or
            // deleted while the previous watch was down are observed now.
            self.relist().await;
            loop {
                match watch.next().await {
                    Some(Ok(event)) => self.on_event(&event).await,
                    Some(Err(e)) => {
                        warn!("Pod watch error: {}, reconnecting", e);
                        break;
                    }
                    None => {
                        warn!("Pod watch stream ended, reconnecting");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod_json_with(
        name: &str,
        owner: Option<(&str, bool)>,
        labels: &[(&str, &str)],
        terminating: bool,
        rv: Option<&str>,
    ) -> String {
        let labels: HashMap<&str, &str> = labels.iter().copied().collect();
        let mut v = json!({
            "metadata": {"name": name, "namespace": "default", "labels": labels},
            "spec": {"containers": []},
        });
        if let Some((o, controller)) = owner {
            v["metadata"]["ownerReferences"] = json!([{
                "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": o, "uid": "u",
                "controller": controller
            }]);
        }
        if terminating {
            v["metadata"]["deletionTimestamp"] = json!("2026-10-15T00:00:00Z");
        }
        if let Some(rv) = rv {
            v["metadata"]["resourceVersion"] = json!(rv);
        }
        v.to_string()
    }

    fn pod_json(name: &str, owner: Option<&str>, terminating: bool) -> String {
        pod_json_with(name, owner.map(|o| (o, true)), &[], terminating, None)
    }

    fn key(name: &str) -> String {
        format!("/registry/pods/default/{name}")
    }

    fn pod(name: &str, owner: &str, rv: Option<&str>) -> Pod {
        serde_json::from_str(&pod_json_with(name, Some((owner, true)), &[], false, rv)).unwrap()
    }

    fn rs_ref(name: &str) -> ControllerRef {
        ControllerRef {
            kind: "ReplicaSet".into(),
            name: name.into(),
            uid: "u".into(),
        }
    }

    fn effects(observation: Option<PodObservation>) -> Vec<PodEffect> {
        observation.expect("attributable to a pod").effects
    }

    /// `processDeltas` (controller.go:581-607): the first sight of an object is `OnAdd`, every later
    /// event for it `OnUpdate` (shared_informer.go); only `addPod` observes a
    /// creation (replica_set.go:436).
    #[test]
    fn only_the_first_sight_of_a_pod_is_a_creation() {
        let cache = PodInformerCache::new();
        let first = cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            first,
            Some(PodObservation {
                pod_key: "default/p".into(),
                effects: vec![PodEffect::ObserveCreation(rs_ref("rs"))],
            })
        );
        // A duplicate Added (the re-list racing the watch) and an ordinary
        // Modified are updates: they wake the owner, observe nothing.
        for event in [
            WatchEvent::Added(key("p"), pod_json("p", Some("rs"), false)),
            WatchEvent::Modified(key("p"), pod_json("p", Some("rs"), false)),
        ] {
            assert_eq!(
                effects(cache.on_event(&event)),
                vec![PodEffect::Enqueue(rs_ref("rs"))]
            );
        }
    }

    /// A Modified for a pod the informer never saw is an add (`processDeltas`
    /// treats Updated of an unknown object as `OnAdd`).
    #[test]
    fn a_modified_pod_never_seen_is_an_add() {
        let cache = PodInformerCache::new();
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json("p", Some("rs"), false)
            ))),
            vec![PodEffect::ObserveCreation(rs_ref("rs"))]
        );
    }

    /// `addPod`: "is already pending deletion. Prevent the pod from being a
    /// creation observation" (replica_set.go:418-422); `updatePod` observes a
    /// deletion as soon as the deletionTimestamp is set (:468-480).
    #[test]
    fn a_terminating_pod_is_a_deletion_not_a_creation() {
        let cache = PodInformerCache::new();
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(
                key("a"),
                pod_json("a", Some("rs"), true)
            ))),
            vec![PodEffect::ObserveDeletion(rs_ref("rs"))]
        );
        cache.on_event(&WatchEvent::Added(
            key("b"),
            pod_json("b", Some("rs"), false),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("b"),
                pod_json("b", Some("rs"), true)
            ))),
            vec![PodEffect::ObserveDeletion(rs_ref("rs"))]
        );
    }

    /// `updatePod` of a terminating pod whose labels also changed runs
    /// `deletePod` for the old pod too (:468-480), so a controller the old
    /// labels pointed at is not left waiting.
    #[test]
    fn a_terminating_relabelled_pod_observes_the_old_controller_too() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json_with("p", Some(("old", true)), &[("a", "1")], false, None),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json_with("p", Some(("new", true)), &[("a", "2")], true, None),
            ))),
            vec![
                PodEffect::ObserveDeletion(rs_ref("new")),
                PodEffect::ObserveDeletion(rs_ref("old")),
            ]
        );
    }

    /// `deletePod` reads the controller from the tombstone's last known state;
    /// the pod is forgotten so a recreation of the name is an add again.
    #[test]
    fn a_deleted_pod_is_forgotten() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        let deleted = cache.on_event(&WatchEvent::Deleted(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            deleted,
            Some(PodObservation {
                pod_key: "default/p".into(),
                effects: vec![PodEffect::ObserveDeletion(rs_ref("rs"))],
            })
        );
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(
                key("p"),
                pod_json("p", Some("rs"), false)
            ))),
            vec![PodEffect::ObserveCreation(rs_ref("rs"))]
        );
    }

    /// An undecodable DELETED value falls back to the cached controller
    /// (`DeletedFinalStateUnknown`).
    #[test]
    fn a_delete_with_an_unreadable_value_uses_the_cached_controller() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Deleted(key("p"), "{".into()))),
            vec![PodEffect::ObserveDeletion(rs_ref("rs"))]
        );
    }

    /// `deletePod`: "No controller should care about orphans being deleted"
    /// (:553-557); and a reference that is not `controller: true` is no
    /// controller (`GetControllerOf`).
    #[test]
    fn orphans_and_non_controller_refs_are_not_controlled() {
        let cache = PodInformerCache::new();
        let plain = pod_json_with("p", Some(("rs", false)), &[("a", "1")], false, None);
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(key("p"), plain.clone()))),
            vec![PodEffect::EnqueueMatching(
                [("a".to_string(), "1".to_string())].into_iter().collect()
            )]
        );
        assert!(effects(cache.on_event(&WatchEvent::Deleted(key("p"), plain))).is_empty());
    }

    /// `updatePod` (:458-529) for an unchanged pod: the same resourceVersion
    /// is a periodic resync and does nothing; an orphan wakes matching
    /// controllers only when its labels or controller reference changed.
    #[test]
    fn update_pod_resync_and_orphan_change_rules() {
        let cache = PodInformerCache::new();
        let labelled = |l: &str, rv: &str| pod_json_with("p", None, &[("a", l)], false, Some(rv));
        cache.on_event(&WatchEvent::Added(key("p"), labelled("1", "1")));
        // Same RV: ignored, even though the labels differ.
        assert!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), labelled("2", "1")))).is_empty()
        );
        // New RV, same labels: the orphan is not worth waking anyone for.
        assert!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), labelled("2", "2")))).is_empty()
        );
        // New RV, changed labels: wake whoever matches now.
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), labelled("3", "3")))),
            vec![PodEffect::EnqueueMatching(
                [("a".to_string(), "3".to_string())].into_iter().collect()
            )]
        );
    }

    /// `updatePod`: when the controller reference changes, the old controller
    /// is synced first, then the current one (:484-514).
    #[test]
    fn a_changed_controller_ref_wakes_old_then_current() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("old"), false),
        ));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json("p", Some("new"), false)
            ))),
            vec![
                PodEffect::Enqueue(rs_ref("old")),
                PodEffect::Enqueue(rs_ref("new"))
            ]
        );
    }

    /// `DeltaFIFO.Replace`: after a watch gap the re-list yields adds for new
    /// pods and deletes for vanished ones, and nothing for pods already known
    /// at the same resourceVersion.
    #[test]
    fn a_relist_reports_what_the_watch_gap_hid() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(
            key("kept"),
            pod_json_with("kept", Some(("rs", true)), &[], false, Some("1")),
        ));
        cache.on_event(&WatchEvent::Added(
            key("gone"),
            pod_json("gone", Some("rs"), false),
        ));

        let observed = cache.replace(&[pod("kept", "rs", Some("1")), pod("new", "rs", Some("1"))]);

        assert_eq!(observed.len(), 2);
        assert!(observed.contains(&PodObservation {
            pod_key: "default/new".into(),
            effects: vec![PodEffect::ObserveCreation(rs_ref("rs"))],
        }));
        assert!(observed.contains(&PodObservation {
            pod_key: "default/gone".into(),
            effects: vec![PodEffect::ObserveDeletion(rs_ref("rs"))],
        }));
        // A second identical re-list is silent.
        assert!(cache
            .replace(&[pod("kept", "rs", Some("1")), pod("new", "rs", Some("1"))])
            .is_empty());
    }

    fn with_ready(json: String, ready: bool) -> String {
        let mut v: serde_json::Value = serde_json::from_str(&json).unwrap();
        v["status"] = json!({"conditions": [{
            "type": "Ready", "status": if ready { "True" } else { "False" }
        }]});
        v.to_string()
    }

    fn owned(rv: &str, ready: bool) -> String {
        with_ready(
            pod_json_with("p", Some(("rs", true)), &[], false, Some(rv)),
            ready,
        )
    }

    /// `updatePod` (replica_set.go:509): `!IsPodReady(old) && IsPodReady(cur)`
    /// follows `enqueueRS` with `enqueueRSAfter`.
    #[test]
    fn a_ready_transition_schedules_the_availability_recheck() {
        let cache = PodInformerCache::new();
        cache.on_event(&WatchEvent::Added(key("p"), owned("1", false)));
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), owned("2", true)))),
            vec![
                PodEffect::Enqueue(rs_ref("rs")),
                PodEffect::EnqueueAfterMinReady(rs_ref("rs"))
            ]
        );
        // Already Ready on both sides: no recheck.
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), owned("3", true)))),
            vec![PodEffect::Enqueue(rs_ref("rs"))]
        );
        // Ready to not Ready is not a transition to Ready.
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Modified(key("p"), owned("4", false)))),
            vec![PodEffect::Enqueue(rs_ref("rs"))]
        );
    }

    /// `addPod` never schedules the recheck, and an orphan has no controller
    /// whose `MinReadySeconds` could apply (:493-515 sits inside
    /// `if curControllerRef != nil`).
    #[test]
    fn only_a_controlled_update_schedules_the_recheck() {
        let cache = PodInformerCache::new();
        assert_eq!(
            effects(cache.on_event(&WatchEvent::Added(key("p"), owned("1", true)))),
            vec![PodEffect::ObserveCreation(rs_ref("rs"))]
        );
        let orphan = |rv: &str, ready: bool| {
            with_ready(
                pod_json_with("o", None, &[("a", "1")], false, Some(rv)),
                ready,
            )
        };
        cache.on_event(&WatchEvent::Added(key("o"), orphan("1", false)));
        assert!(
            effects(cache.on_event(&WatchEvent::Modified(key("o"), orphan("2", true)))).is_empty()
        );
    }

    struct Target {
        min_ready_seconds: i32,
    }

    impl PodEventTarget for Target {
        fn queue_prefix(&self) -> &'static str {
            "replicasets"
        }
        async fn resolve_controller_ref(
            &self,
            _namespace: &str,
            controller_ref: &ControllerRef,
        ) -> Option<ResolvedController> {
            Some(ResolvedController {
                name: controller_ref.name.clone(),
                min_ready_seconds: self.min_ready_seconds,
            })
        }
        async fn pod_controllers(&self, _: &str, _: &HashMap<String, String>) -> Vec<String> {
            Vec::new()
        }
        fn creation_observed(&self, _: &str) {}
        fn deletion_observed(&self, _: &str, _: &str) {}
    }

    fn recheck() -> PodObservation {
        PodObservation {
            pod_key: "default/p".into(),
            effects: vec![PodEffect::EnqueueAfterMinReady(rs_ref("rs"))],
        }
    }

    /// `rs.Spec.MinReadySeconds > 0` gates `enqueueRSAfter`: the key is not
    /// queued now, and is queued once the duration has passed.
    #[tokio::test]
    async fn the_recheck_enqueues_after_min_ready_seconds() {
        let queue = WorkQueue::new();
        apply_pod_observation(
            &Target {
                min_ready_seconds: 1,
            },
            &queue,
            &recheck(),
        )
        .await;
        assert_eq!(queue.len().await, 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), queue.get())
                .await
                .is_err(),
            "must not be available before minReadySeconds"
        );
        let got = tokio::time::timeout(Duration::from_secs(3), queue.get())
            .await
            .expect("available after minReadySeconds");
        assert_eq!(got.as_deref(), Some("replicasets/default/rs"));
    }

    #[tokio::test]
    async fn no_min_ready_seconds_means_no_recheck() {
        let queue = WorkQueue::new();
        apply_pod_observation(
            &Target {
                min_ready_seconds: 0,
            },
            &queue,
            &recheck(),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), queue.get())
                .await
                .is_err()
        );
    }

    /// A target that records what the informer's handlers lowered.
    struct Recording {
        prefix: &'static str,
        log: Mutex<Vec<String>>,
    }

    impl Recording {
        fn new(prefix: &'static str) -> Arc<Self> {
            Arc::new(Self {
                prefix,
                log: Mutex::new(Vec::new()),
            })
        }
        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    impl PodEventTarget for Recording {
        fn queue_prefix(&self) -> &'static str {
            self.prefix
        }
        async fn resolve_controller_ref(
            &self,
            _: &str,
            controller_ref: &ControllerRef,
        ) -> Option<ResolvedController> {
            Some(ResolvedController {
                name: controller_ref.name.clone(),
                min_ready_seconds: 0,
            })
        }
        async fn pod_controllers(&self, _: &str, _: &HashMap<String, String>) -> Vec<String> {
            Vec::new()
        }
        fn creation_observed(&self, key: &str) {
            self.log.lock().unwrap().push(format!("created {key}"));
        }
        fn deletion_observed(&self, key: &str, pod_key: &str) {
            self.log
                .lock()
                .unwrap()
                .push(format!("deleted {key} {pod_key}"));
        }
    }

    /// `processDeltas` classifies once and `sharedProcessor.distribute` tells
    /// every listener (controller.go:581-607, shared_informer.go:863): two
    /// targets on one informer both see ONE add (so both lower their
    /// expectations), the next event as an update for both, and a pod that
    /// vanished across a re-list as a delete for both.
    #[tokio::test]
    async fn every_target_sees_the_same_add_update_and_relist_delete() {
        use rusternetes_storage::memory::MemoryStorage;
        let storage = Arc::new(MemoryStorage::new());
        let informer = Arc::new(SharedPodInformer::new(
            storage.clone(),
            Duration::from_millis(10),
        ));
        let rs = Recording::new("replicasets");
        let rc = Recording::new("replicationcontrollers");
        let (rs_queue, rc_queue) = (WorkQueue::new(), WorkQueue::new());
        informer
            .add_event_handler(rs.clone(), rs_queue.clone())
            .await;
        informer
            .add_event_handler(rc.clone(), rc_queue.clone())
            .await;

        informer
            .on_event(&WatchEvent::Added(
                key("p"),
                pod_json_with("p", Some(("c", true)), &[], false, Some("1")),
            ))
            .await;
        informer
            .on_event(&WatchEvent::Modified(
                key("p"),
                pod_json_with("p", Some(("c", true)), &[], false, Some("2")),
            ))
            .await;
        for t in [&rs, &rc] {
            assert_eq!(
                t.log(),
                vec!["created default/c"],
                "one add, then an update"
            );
        }
        assert_eq!(rs_queue.len().await, 1);
        assert_eq!(rc_queue.len().await, 1);

        // The pod is gone from storage; the re-list finds it vanished.
        informer.relist().await;
        for t in [&rs, &rc] {
            assert_eq!(
                t.log(),
                vec!["created default/c", "deleted default/c default/p"]
            );
        }
    }

    /// A handler added after the informer holds pods is replayed them as adds
    /// (`AddEventHandlerWithOptions`), so it does not miss a creation.
    #[tokio::test]
    async fn a_late_handler_is_replayed_the_held_pods() {
        use rusternetes_storage::memory::MemoryStorage;
        let informer = Arc::new(SharedPodInformer::new(
            Arc::new(MemoryStorage::new()),
            Duration::from_millis(10),
        ));
        informer
            .on_event(&WatchEvent::Added(
                key("p"),
                pod_json_with("p", Some(("c", true)), &[], false, Some("1")),
            ))
            .await;
        let late = Recording::new("replicasets");
        informer
            .add_event_handler(late.clone(), WorkQueue::new())
            .await;
        assert_eq!(late.log(), vec!["created default/c"]);
    }

    /// A target whose controller-ref lookup parks until released, like the
    /// storage read `resolve_controller_ref` does in API mode.
    struct Gated {
        inner: Arc<Recording>,
        entered: tokio::sync::Notify,
        gate: tokio::sync::Semaphore,
    }

    impl PodEventTarget for Gated {
        fn queue_prefix(&self) -> &'static str {
            self.inner.queue_prefix()
        }
        async fn resolve_controller_ref(
            &self,
            ns: &str,
            controller_ref: &ControllerRef,
        ) -> Option<ResolvedController> {
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
            self.inner.resolve_controller_ref(ns, controller_ref).await
        }
        async fn pod_controllers(&self, ns: &str, l: &HashMap<String, String>) -> Vec<String> {
            self.inner.pod_controllers(ns, l).await
        }
        fn creation_observed(&self, key: &str) {
            self.inner.creation_observed(key)
        }
        fn deletion_observed(&self, key: &str, pod_key: &str) {
            self.inner.deletion_observed(key, pod_key)
        }
    }

    /// `AddEventHandlerWithOptions` holds `s.blockDeltas` across
    /// `addListener` and the replay (shared_informer.go:698-699), and
    /// `HandleDeltas` takes the same lock around `processDeltas`
    /// (shared_informer.go:717-718), so an event arriving while the replay is
    /// in flight is delivered to the new handler afterwards, never lost.
    #[tokio::test]
    async fn an_event_during_a_late_handlers_replay_is_not_lost() {
        use rusternetes_storage::memory::MemoryStorage;
        let informer = Arc::new(SharedPodInformer::new(
            Arc::new(MemoryStorage::new()),
            Duration::from_millis(10),
        ));
        let added = |name: &str, rv: &str| {
            WatchEvent::Added(
                key(name),
                pod_json_with(name, Some(("c", true)), &[], false, Some(rv)),
            )
        };
        informer.on_event(&added("a", "1")).await;

        let late = Arc::new(Gated {
            inner: Recording::new("replicasets"),
            entered: tokio::sync::Notify::new(),
            gate: tokio::sync::Semaphore::new(0),
        });
        let register = {
            let (informer, late) = (informer.clone(), late.clone());
            tokio::spawn(async move { informer.add_event_handler(late, WorkQueue::new()).await })
        };
        late.entered.notified().await; // replay of "a" is in flight
        let event = {
            let informer = informer.clone();
            tokio::spawn(async move { informer.on_event(&added("b", "2")).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        late.gate.add_permits(100);
        register.await.unwrap();
        event.await.unwrap();
        assert_eq!(
            late.inner.log(),
            vec!["created default/c", "created default/c"],
            "the replayed add of a and the live add of b"
        );
    }
}
