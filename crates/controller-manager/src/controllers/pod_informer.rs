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

use rusternetes_common::resources::Pod;
use rusternetes_storage::{WatchEvent, WorkQueue};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

/// `metav1.OwnerReference`, restricted to what `resolveControllerRef` reads
/// (replica_set.go:305-321): the kind, the name to look the controller up by,
/// and the UID that must still match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ControllerRef {
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
    /// `getPodReplicaSets(pod)` then `enqueueRS` for each: an orphan was
    /// added, or changed, and the controllers that could adopt it are synced
    /// (:441-453, :517-528). Carries the pod's labels.
    EnqueueMatching(HashMap<String, String>),
}

/// What a pod event means for the controllers' expectations and queue: the
/// effects, in the order the handler performs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PodObservation {
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
}

/// Pod key ("namespace/name") to what the handlers read of it.
#[derive(Debug, Default)]
pub(crate) struct PodInformerCache {
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
fn delete_pod(controller: Option<ControllerRef>) -> Option<PodEffect> {
    controller.map(PodEffect::ObserveDeletion)
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
                effects.extend(delete_pod(cur.controller));
            } else if let Some(controller) = cur.controller {
                // "If it has a ControllerRef, that's all that matters."
                effects.push(PodEffect::ObserveCreation(controller));
            } else {
                // "Otherwise, it's an orphan. Get a list of all matching
                // ReplicaSets and sync them to see if anyone wants to adopt
                // it. DO NOT observe creation because no controller should be
                // waiting for an orphan."
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
            effects.extend(delete_pod(cur.controller));
            if label_changed {
                effects.extend(delete_pod(old.controller));
            }
            return effects;
        }

        let controller_ref_changed = cur.controller != old.controller;
        if controller_ref_changed {
            if let Some(old_controller) = old.controller {
                // "The ControllerRef was changed. Sync the old controller, if
                // any."
                effects.push(PodEffect::Enqueue(old_controller));
            }
        }
        if let Some(controller) = cur.controller {
            effects.push(PodEffect::Enqueue(controller));
        } else if label_changed || controller_ref_changed {
            // "Otherwise, it's an orphan. If anything changed, sync matching
            // controllers to see if anyone wants to adopt it now."
            effects.push(PodEffect::EnqueueMatching(
                pod.metadata.labels.clone().unwrap_or_default(),
            ));
        }
        effects
    }

    /// Classify one watch event, updating the cache the way the informer's
    /// indexer would. `None` when the event cannot be attributed to a pod.
    pub(crate) fn on_event(&self, event: &WatchEvent) -> Option<PodObservation> {
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
            let cached = known.remove(&pod_key).and_then(|k| k.controller);
            let controller = pod.as_ref().map(controller_of).unwrap_or(cached);
            return Some(PodObservation {
                pod_key,
                effects: delete_pod(controller).into_iter().collect(),
            });
        }

        let effects = Self::handle_present(&mut known, &pod_key, &pod?);
        Some(PodObservation { pod_key, effects })
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
            let controller = known.remove(&pod_key).and_then(|k| k.controller);
            observations.push(PodObservation {
                pod_key,
                effects: delete_pod(controller).into_iter().collect(),
            });
        }
        observations
    }
}

/// The controller-specific half of the pod event handlers.
pub(crate) trait PodEventTarget: Sync {
    /// The work-queue key prefix: `replicasets` or `replicationcontrollers`.
    fn queue_prefix(&self) -> &'static str;

    /// `resolveControllerRef` (replica_set.go:302-321): the name of the
    /// controller `controller_ref` points at, or `None` when it is of another
    /// kind, no longer exists, or has a different UID.
    fn resolve_controller_ref(
        &self,
        namespace: &str,
        controller_ref: &ControllerRef,
    ) -> impl Future<Output = Option<String>> + Send;

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
}

/// Run the effects of one observation against `target`.
pub(crate) async fn apply_pod_observation<T: PodEventTarget>(
    target: &T,
    queue: &WorkQueue,
    observation: &PodObservation,
) {
    let pod_key = observation.pod_key.as_str();
    let Some((namespace, _)) = pod_key.split_once('/') else {
        return;
    };
    let enqueue = |name: &str| format!("{}/{namespace}/{name}", target.queue_prefix());
    for effect in &observation.effects {
        match effect {
            PodEffect::ObserveCreation(controller_ref) => {
                if let Some(name) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    target.creation_observed(&format!("{namespace}/{name}"));
                    queue.add(enqueue(&name)).await;
                }
            }
            PodEffect::ObserveDeletion(controller_ref) => {
                if let Some(name) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    target.deletion_observed(&format!("{namespace}/{name}"), pod_key);
                    queue.add(enqueue(&name)).await;
                }
            }
            PodEffect::Enqueue(controller_ref) => {
                if let Some(name) = target
                    .resolve_controller_ref(namespace, controller_ref)
                    .await
                {
                    queue.add(enqueue(&name)).await;
                }
            }
            PodEffect::EnqueueMatching(labels) => {
                for name in target.pod_controllers(namespace, labels).await {
                    queue.add(enqueue(&name)).await;
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
}
