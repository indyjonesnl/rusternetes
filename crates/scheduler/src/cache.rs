//! The scheduler cache's pod state machine: assumed pods.
//!
//! Ported from `pkg/scheduler/backend/cache/cache.go` (release-1.35):
//! `AssumePod` (:369), `FinishBinding` (:384), `ForgetPod` (:412),
//! `AddPod` (:496), `UpdatePod` (:529), `RemovePod` (:556), `IsAssumedPod`
//! (:581), `GetPod` (:595) and the expiry of `cleanupAssumedPods` (:740).
//!
//! A pod the scheduler has picked a node for is "assumed": it is recorded on
//! that node before the binding is written, "we assume that binding will
//! succeed" (`schedule_one.go` `assume`, :955-975), so the next cycle already
//! counts it. The informer's Add event for the bound pod "confirms" it; a
//! failed binding forgets it. With `ttl == 0` (the scheduler's
//! `durationToExpireAssumedPod`, scheduler.go:66) an assumed pod never
//! expires.
//!
//! Not ported here: `NodeInfo` / `Snapshot` (`UpdateSnapshot`, node tree,
//! image states). The node snapshot is rebuilt from the lister every cycle,
//! so nothing consumes per-node accounting from the cache yet; only the pod
//! state is kept, plus [`SchedulerCache::pods_on_node`] to observe placement.
//!
//! Deviation: where upstream calls `klog.FlushAndExit` on a corrupted cache
//! (`UpdatePod`, `RemovePod`, `cleanupAssumedPods`) this returns an error and
//! leaves the state untouched.

use k3rs_common::resources::Pod;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// `durationToExpireAssumedPod` (scheduler.go:66): assumed pods never expire.
pub const DURATION_TO_EXPIRE_ASSUMED_POD: Duration = Duration::ZERO;

/// `framework.GetPodKey` (types.go): the pod's UID.
fn pod_key(pod: &Pod) -> Result<String, String> {
    if pod.metadata.uid.is_empty() {
        return Err(format!(
            "cannot get cache key for pod with empty UID: {}",
            pod.metadata.name
        ));
    }
    Ok(pod.metadata.uid.clone())
}

fn node_of(pod: &Pod) -> &str {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("")
}

fn kobj(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `podState` (cache.go:84-90).
#[derive(Debug, Clone)]
struct PodState {
    pod: Pod,
    /// Used by an assumed pod to determine expiration; `None` never expires.
    deadline: Option<Instant>,
    /// Blocks expiry while the binding still runs.
    binding_finished: bool,
}

#[derive(Debug, Default)]
struct Inner {
    /// Keys of the assumed pods; each is also in `pod_states`.
    assumed_pods: HashSet<String>,
    pod_states: HashMap<String, PodState>,
}

impl Inner {
    /// `addPod` (cache.go:437-458).
    fn add_pod(&mut self, pod: &Pod, assume: bool) -> Result<(), String> {
        let key = pod_key(pod)?;
        self.pod_states.insert(
            key.clone(),
            PodState {
                pod: pod.clone(),
                deadline: None,
                binding_finished: false,
            },
        );
        if assume {
            self.assumed_pods.insert(key);
        }
        Ok(())
    }

    /// `removePod` (cache.go:471-494).
    fn remove_pod(&mut self, pod: &Pod) -> Result<(), String> {
        let key = pod_key(pod)?;
        self.pod_states.remove(&key);
        self.assumed_pods.remove(&key);
        Ok(())
    }

    /// `updatePod` (cache.go:460-469).
    fn update_pod(&mut self, old: &Pod, new: &Pod) -> Result<(), String> {
        self.remove_pod(old)?;
        self.add_pod(new, false)
    }
}

/// The pod half of `cacheImpl`.
#[derive(Debug)]
pub struct SchedulerCache {
    ttl: Duration,
    inner: Mutex<Inner>,
}

impl Default for SchedulerCache {
    fn default() -> Self {
        Self::new(DURATION_TO_EXPIRE_ASSUMED_POD)
    }
}

impl SchedulerCache {
    /// `newCache`; `ttl` is how long an assumed pod lives once its binding
    /// finished (0 = forever).
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `AssumePod` (cache.go:369-382). The pod carries the chosen node name.
    pub fn assume_pod(&self, pod: &Pod) -> Result<(), String> {
        let key = pod_key(pod)?;
        let mut g = self.lock();
        if g.pod_states.contains_key(&key) {
            return Err(format!(
                "pod {key}({}) is in the cache, so can't be assumed",
                kobj(pod)
            ));
        }
        g.add_pod(pod, true)
    }

    /// `FinishBinding` (cache.go:384-387).
    pub fn finish_binding(&self, pod: &Pod) -> Result<(), String> {
        self.finish_binding_at(pod, Instant::now())
    }

    /// `finishBinding` (cache.go:389-410): `now` is injected for tests.
    pub fn finish_binding_at(&self, pod: &Pod, now: Instant) -> Result<(), String> {
        let key = pod_key(pod)?;
        let ttl = self.ttl;
        let mut g = self.lock();
        let assumed = g.assumed_pods.contains(&key);
        if let Some(st) = g.pod_states.get_mut(&key) {
            if assumed {
                st.deadline = if ttl.is_zero() { None } else { Some(now + ttl) };
                st.binding_finished = true;
            }
        }
        Ok(())
    }

    /// `ForgetPod` (cache.go:412-435).
    pub fn forget_pod(&self, pod: &Pod) -> Result<(), String> {
        let key = pod_key(pod)?;
        let mut g = self.lock();
        let Some(cur) = g.pod_states.get(&key) else {
            // Pod does not exist in the cache anymore.
            return Ok(());
        };
        if node_of(&cur.pod) != node_of(pod) {
            return Err(format!(
                "pod {key}({}) was assumed on {} but assigned to {}",
                kobj(pod),
                node_of(pod),
                node_of(&cur.pod)
            ));
        }
        // Only assumed pod can be forgotten.
        if g.assumed_pods.contains(&key) {
            return g.remove_pod(pod);
        }
        Err(format!(
            "pod {key}({}) wasn't assumed so cannot be forgotten",
            kobj(pod)
        ))
    }

    /// `AddPod` (cache.go:496-527): the informer's Add event for an assigned
    /// pod. Confirms an assumed pod.
    pub fn add_pod(&self, pod: &Pod) -> Result<(), String> {
        let key = pod_key(pod)?;
        let mut g = self.lock();
        let cur = g.pod_states.get(&key).map(|s| s.pod.clone());
        let assumed = g.assumed_pods.contains(&key);
        match cur {
            // "When assuming, we've already added the Pod to cache, just
            // update here to make sure the Pod's status is up-to-date."
            Some(cur) if assumed => {
                if let Err(e) = g.update_pod(&cur, pod) {
                    tracing::error!(error = %e, "Error occurred while updating pod");
                }
                if node_of(&cur) != node_of(pod) {
                    tracing::info!(
                        pod = %kobj(pod),
                        assumed_node = node_of(pod),
                        current_node = node_of(&cur),
                        "Pod was added to a different node than it was assumed"
                    );
                }
                Ok(())
            }
            // "Pod was expired. We should add it back."
            None => {
                if let Err(e) = g.add_pod(pod, false) {
                    tracing::error!(error = %e, "Error occurred while adding pod");
                }
                Ok(())
            }
            Some(_) => Err(format!(
                "pod {key}({}) was already in added state",
                kobj(pod)
            )),
        }
    }

    /// `UpdatePod` (cache.go:529-554).
    pub fn update_pod(&self, old: &Pod, new: &Pod) -> Result<(), String> {
        let key = pod_key(old)?;
        let mut g = self.lock();
        let Some(cur) = g.pod_states.get(&key) else {
            return Err(format!(
                "pod {key}({}) is not added to scheduler cache, so cannot be updated",
                kobj(old)
            ));
        };
        // "An assumed pod won't have Update/Remove event. It needs to have Add
        // event before Update event, in which case the state would change from
        // Assumed to Added."
        if g.assumed_pods.contains(&key) {
            return Err(format!(
                "assumed pod {key}({}) should not be updated",
                kobj(old)
            ));
        }
        if node_of(&cur.pod) != node_of(new) {
            return Err(format!(
                "pod {key}({}) updated on a different node than previously added to; \
                 scheduler cache is corrupted",
                kobj(old)
            ));
        }
        g.update_pod(old, new)
    }

    /// `RemovePod` (cache.go:556-579).
    pub fn remove_pod(&self, pod: &Pod) -> Result<(), String> {
        let key = pod_key(pod)?;
        let mut g = self.lock();
        let Some(cur) = g.pod_states.get(&key) else {
            return Err(format!(
                "pod {key}({}) is not found in scheduler cache, so cannot be removed from it",
                kobj(pod)
            ));
        };
        // "An empty NodeName is possible when the scheduler misses a Delete
        // event and it gets the last known state from the informer cache."
        if node_of(&cur.pod) != node_of(pod) && !node_of(pod).is_empty() {
            return Err(format!(
                "pod {key}({}) was added to a different node than it was assumed; \
                 scheduler cache is corrupted",
                kobj(pod)
            ));
        }
        let cur = cur.pod.clone();
        g.remove_pod(&cur)
    }

    /// `IsAssumedPod` (cache.go:581-593).
    pub fn is_assumed_pod(&self, pod: &Pod) -> Result<bool, String> {
        let key = pod_key(pod)?;
        Ok(self.lock().assumed_pods.contains(&key))
    }

    /// `GetPod` (cache.go:595-610).
    pub fn get_pod(&self, pod: &Pod) -> Result<Pod, String> {
        let key = pod_key(pod)?;
        self.lock()
            .pod_states
            .get(&key)
            .map(|s| s.pod.clone())
            .ok_or_else(|| format!("pod {key}({}) does not exist in scheduler cache", kobj(pod)))
    }

    /// `cleanupAssumedPods` (cache.go:740-764): expire assumed pods whose
    /// binding finished and whose deadline passed.
    pub fn cleanup_assumed_pods(&self, now: Instant) {
        let ttl = self.ttl;
        let mut g = self.lock();
        let keys: Vec<String> = g.assumed_pods.iter().cloned().collect();
        for key in keys {
            let Some(ps) = g.pod_states.get(&key) else {
                tracing::error!(pod_key = %key, "Key found in assumed set but not in podStates");
                g.assumed_pods.remove(&key);
                continue;
            };
            if !ps.binding_finished {
                continue;
            }
            if !ttl.is_zero() && ps.deadline.is_some_and(|d| now > d) {
                tracing::info!(pod = %kobj(&ps.pod), "Pod expired");
                let pod = ps.pod.clone();
                if let Err(e) = g.remove_pod(&pod) {
                    tracing::error!(error = %e, "ExpirePod failed");
                }
            }
        }
    }

    /// Pods recorded on `node` (assumed or confirmed), sorted by name.
    pub fn pods_on_node(&self, node: &str) -> Vec<Pod> {
        let mut v: Vec<Pod> = self
            .lock()
            .pod_states
            .values()
            .filter(|s| node_of(&s.pod) == node)
            .map(|s| s.pod.clone())
            .collect();
        v.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        v
    }

    /// The assumed pods, as assumed (with their chosen node): the cache's
    /// contribution to a scheduling cycle's view of the nodes.
    pub fn assumed_pods(&self) -> Vec<Pod> {
        let g = self.lock();
        g.assumed_pods
            .iter()
            .filter_map(|k| g.pod_states.get(k).map(|s| s.pod.clone()))
            .collect()
    }

    /// `PodCount` (cache.go:356-367).
    pub fn pod_count(&self) -> usize {
        self.lock().pod_states.len()
    }

    /// Number of assumed pods (`assumed_pods` cache-size metric).
    pub fn assumed_count(&self) -> usize {
        self.lock().assumed_pods.len()
    }
}
