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
//! this cache is that indexer, restricted to what the handlers read: pod key
//! to the names of its controller-kind owners.

use rusternetes_common::resources::Pod;
use rusternetes_storage::WatchEvent;
use std::collections::HashMap;
use std::sync::Mutex;

/// What a pod event means for the owning controllers' expectations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PodObservation {
    /// `addPod` of a live pod: `expectations.CreationObserved` per owner
    /// (replica_set.go:436).
    Created {
        pod_key: String,
        owners: Vec<String>,
    },
    /// `deletePod`, or `addPod`/`updatePod` of a pod carrying a
    /// deletionTimestamp: `expectations.DeletionObserved(owner, podKey)`
    /// (replica_set.go:420, :468-480, :568).
    Deleted {
        pod_key: String,
        owners: Vec<String>,
    },
    /// `OnUpdate` of a pod already held: nothing to observe, but the owners
    /// still need waking (`enqueueRS`).
    Updated {
        pod_key: String,
        owners: Vec<String>,
    },
}

impl PodObservation {
    pub(crate) fn pod_key(&self) -> &str {
        match self {
            PodObservation::Created { pod_key, .. }
            | PodObservation::Deleted { pod_key, .. }
            | PodObservation::Updated { pod_key, .. } => pod_key,
        }
    }

    pub(crate) fn owners(&self) -> &[String] {
        match self {
            PodObservation::Created { owners, .. }
            | PodObservation::Deleted { owners, .. }
            | PodObservation::Updated { owners, .. } => owners,
        }
    }
}

/// Pod key ("namespace/name") to the names of the pod's owners of the
/// controller kind this cache serves.
#[derive(Debug)]
pub(crate) struct PodInformerCache {
    owner_kind: &'static str,
    known: Mutex<HashMap<String, Vec<String>>>,
}

fn owners_of(pod: &Pod, kind: &str) -> Vec<String> {
    pod.metadata
        .owner_references
        .iter()
        .flatten()
        .filter(|r| r.kind == kind)
        .map(|r| r.name.clone())
        .collect()
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

impl PodInformerCache {
    pub(crate) fn new(owner_kind: &'static str) -> Self {
        Self {
            owner_kind,
            known: Mutex::new(HashMap::new()),
        }
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
            // cache held when it cannot be decoded.
            let cached = known.remove(&pod_key);
            let owners = pod
                .as_ref()
                .map(|p| owners_of(p, self.owner_kind))
                .filter(|o| !o.is_empty())
                .or(cached)
                .unwrap_or_default();
            return Some(PodObservation::Deleted { pod_key, owners });
        }

        let pod = pod?;
        let owners = owners_of(&pod, self.owner_kind);
        let is_new = known.insert(pod_key.clone(), owners.clone()).is_none();
        if pod.metadata.deletion_timestamp.is_some() {
            // addPod: "is already pending deletion. Prevent the pod from being
            // a creation observation" (replica_set.go:418-422); updatePod:
            // a graceful delete first MODIFIES the pod (:468-480).
            Some(PodObservation::Deleted { pod_key, owners })
        } else if is_new {
            Some(PodObservation::Created { pod_key, owners })
        } else {
            Some(PodObservation::Updated { pod_key, owners })
        }
    }

    /// The reflector's re-list on (re)connect: `DeltaFIFO.Replace`. Pods not
    /// seen before are adds; pods that disappeared while the watch was down
    /// are deletes.
    pub(crate) fn replace(&self, pods: &[Pod]) -> Vec<PodObservation> {
        let mut observations = Vec::new();
        let mut next: HashMap<String, Vec<String>> = HashMap::with_capacity(pods.len());
        let mut known = self.known.lock().unwrap();
        for pod in pods {
            let pod_key = format!(
                "{}/{}",
                pod.metadata.namespace.as_deref().unwrap_or(""),
                pod.metadata.name
            );
            let owners = owners_of(pod, self.owner_kind);
            if !known.contains_key(&pod_key) {
                let observation = if pod.metadata.deletion_timestamp.is_some() {
                    PodObservation::Deleted {
                        pod_key: pod_key.clone(),
                        owners: owners.clone(),
                    }
                } else {
                    PodObservation::Created {
                        pod_key: pod_key.clone(),
                        owners: owners.clone(),
                    }
                };
                observations.push(observation);
            }
            next.insert(pod_key, owners);
        }
        for (pod_key, owners) in known.iter() {
            if !next.contains_key(pod_key) {
                observations.push(PodObservation::Deleted {
                    pod_key: pod_key.clone(),
                    owners: owners.clone(),
                });
            }
        }
        *known = next;
        observations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod_json(name: &str, owner: Option<&str>, terminating: bool) -> String {
        let mut v = json!({
            "metadata": {"name": name, "namespace": "default"},
            "spec": {"containers": []},
        });
        if let Some(o) = owner {
            v["metadata"]["ownerReferences"] = json!([{
                "apiVersion": "apps/v1", "kind": "ReplicaSet", "name": o, "uid": "u",
                "controller": true
            }]);
        }
        if terminating {
            v["metadata"]["deletionTimestamp"] = json!("2026-10-15T00:00:00Z");
        }
        v.to_string()
    }

    fn key(name: &str) -> String {
        format!("/registry/pods/default/{name}")
    }

    fn pod(name: &str, owner: &str, terminating: bool) -> Pod {
        serde_json::from_str(&pod_json(name, Some(owner), terminating)).unwrap()
    }

    /// `processDeltas` (controller.go:581-607): the first sight of an object is `OnAdd`, every later
    /// event for it `OnUpdate` (shared_informer.go); only `addPod` observes a
    /// creation (replica_set.go:436).
    #[test]
    fn only_the_first_sight_of_a_pod_is_a_creation() {
        let cache = PodInformerCache::new("ReplicaSet");
        let first = cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            first,
            Some(PodObservation::Created {
                pod_key: "default/p".into(),
                owners: vec!["rs".into()]
            })
        );
        // A duplicate Added (the re-list racing the watch) and an ordinary
        // Modified are updates.
        assert!(matches!(
            cache.on_event(&WatchEvent::Added(
                key("p"),
                pod_json("p", Some("rs"), false)
            )),
            Some(PodObservation::Updated { .. })
        ));
        assert!(matches!(
            cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json("p", Some("rs"), false)
            )),
            Some(PodObservation::Updated { .. })
        ));
    }

    /// A Modified for a pod the informer never saw is an add (`processDeltas`
    /// treats Updated of an unknown object as `OnAdd`).
    #[test]
    fn a_modified_pod_never_seen_is_an_add() {
        let cache = PodInformerCache::new("ReplicaSet");
        assert!(matches!(
            cache.on_event(&WatchEvent::Modified(
                key("p"),
                pod_json("p", Some("rs"), false)
            )),
            Some(PodObservation::Created { .. })
        ));
    }

    /// `addPod`: "is already pending deletion. Prevent the pod from being a
    /// creation observation" (replica_set.go:418-422); `updatePod` observes a
    /// deletion as soon as the deletionTimestamp is set (:468-480).
    #[test]
    fn a_terminating_pod_is_a_deletion_not_a_creation() {
        let cache = PodInformerCache::new("ReplicaSet");
        assert!(matches!(
            cache.on_event(&WatchEvent::Added(
                key("a"),
                pod_json("a", Some("rs"), true)
            )),
            Some(PodObservation::Deleted { .. })
        ));
        cache.on_event(&WatchEvent::Added(
            key("b"),
            pod_json("b", Some("rs"), false),
        ));
        assert!(matches!(
            cache.on_event(&WatchEvent::Modified(
                key("b"),
                pod_json("b", Some("rs"), true)
            )),
            Some(PodObservation::Deleted { .. })
        ));
    }

    /// `deletePod` reads the owner from the tombstone's last known state;
    /// the pod is forgotten so a recreation of the name is an add again.
    #[test]
    fn a_deleted_pod_is_forgotten() {
        let cache = PodInformerCache::new("ReplicaSet");
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
            Some(PodObservation::Deleted {
                pod_key: "default/p".into(),
                owners: vec!["rs".into()]
            })
        );
        assert!(matches!(
            cache.on_event(&WatchEvent::Added(
                key("p"),
                pod_json("p", Some("rs"), false)
            )),
            Some(PodObservation::Created { .. })
        ));
    }

    /// `DeltaFIFO.Replace`: after a watch gap the re-list yields adds for new
    /// pods and deletes for vanished ones, and nothing for pods already known.
    #[test]
    fn a_relist_reports_what_the_watch_gap_hid() {
        let cache = PodInformerCache::new("ReplicaSet");
        cache.on_event(&WatchEvent::Added(
            key("kept"),
            pod_json("kept", Some("rs"), false),
        ));
        cache.on_event(&WatchEvent::Added(
            key("gone"),
            pod_json("gone", Some("rs"), false),
        ));

        let observed = cache.replace(&[pod("kept", "rs", false), pod("new", "rs", false)]);

        assert_eq!(observed.len(), 2);
        assert!(observed.contains(&PodObservation::Created {
            pod_key: "default/new".into(),
            owners: vec!["rs".into()]
        }));
        assert!(observed.contains(&PodObservation::Deleted {
            pod_key: "default/gone".into(),
            owners: vec!["rs".into()]
        }));
        // A second identical re-list is silent.
        assert!(cache
            .replace(&[pod("kept", "rs", false), pod("new", "rs", false)])
            .is_empty());
    }

    /// Only owners of this cache's kind are reported.
    #[test]
    fn other_owner_kinds_are_ignored() {
        let cache = PodInformerCache::new("ReplicationController");
        let observed = cache.on_event(&WatchEvent::Added(
            key("p"),
            pod_json("p", Some("rs"), false),
        ));
        assert_eq!(
            observed,
            Some(PodObservation::Created {
                pod_key: "default/p".into(),
                owners: vec![]
            })
        );
    }
}
