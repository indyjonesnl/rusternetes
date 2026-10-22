//! Pod nominator — port of `pkg/scheduler/backend/queue/nominator.go`
//! (`nominator`) and `NominatingInfo` (`staging/.../kube-scheduler/framework/
//! interface.go:296-316`).
//!
//! It stores the pods nominated to run on nodes (by preemption). It exists
//! because `status.nominatedNodeName` of the pod objects held by the queue may
//! differ from what the scheduler recorded here; pods are found by UID.
//! Locking: upstream's separate `nLock` (lowest in the lock order) is subsumed
//! by the queue's single mutex, which owns this struct.

use rusternetes_common::resources::Pod;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// `fwk.NominatingMode` (`interface.go:300-304`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NominatingMode {
    /// `ModeNoop`: keep whatever the pod's `status.nominatedNodeName` says.
    Noop,
    /// `ModeOverride`: use `NominatedNodeName`.
    Override,
}

/// `fwk.NominatingInfo` (`interface.go:306-309`). A missing info (Go `nil`)
/// behaves as `ModeNoop`.
#[derive(Clone, Debug)]
pub struct NominatingInfo {
    pub nominated_node_name: String,
    pub nominating_mode: NominatingMode,
}

impl NominatingInfo {
    /// `ModeOverride` with `node`.
    pub fn override_node(node: &str) -> Self {
        Self {
            nominated_node_name: node.to_string(),
            nominating_mode: NominatingMode::Override,
        }
    }
    /// `ModeNoop`.
    pub fn noop() -> Self {
        Self {
            nominated_node_name: String::new(),
            nominating_mode: NominatingMode::Noop,
        }
    }
    /// `(*NominatingInfo).Mode` (`:311-316`): nil is Noop.
    pub fn mode(info: Option<&NominatingInfo>) -> NominatingMode {
        info.map_or(NominatingMode::Noop, |i| i.nominating_mode)
    }
}

/// `podRef` (`nominator.go:196-200`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodRef {
    pub name: String,
    pub namespace: String,
    pub uid: String,
}

impl PodRef {
    /// `podToRef` (`:202-208`).
    pub fn from_pod(pod: &Pod) -> Self {
        Self {
            name: pod.metadata.name.clone(),
            namespace: pod.metadata.namespace.clone().unwrap_or_default(),
            uid: pod.metadata.uid.clone(),
        }
    }
}

/// `listersv1.PodLister.Pods(ns).Get(name)`: `None` when the pod is not in the
/// cache.
pub type PodLister = Arc<dyn Fn(&str, &str) -> Option<Pod> + Send + Sync>;

/// `nominator` (`nominator.go:31-50`).
#[derive(Default)]
pub struct Nominator {
    /// Used to verify the given pod is alive (`WithPodLister`); `None` skips
    /// the check, as upstream does for a nil lister.
    pod_lister: Option<PodLister>,
    /// Node name -> pods nominated to run on it (in activeQ, backoffQ or
    /// unschedulablePods).
    pub(crate) nominated_pods: HashMap<String, Vec<PodRef>>,
    /// Pod UID -> node name where it is nominated.
    pub(crate) nominated_pod_to_node: HashMap<String, String>,
}

fn nominated_node_name(pod: &Pod) -> &str {
    pod.status
        .as_ref()
        .and_then(|s| s.nominated_node_name.as_deref())
        .unwrap_or("")
}

impl Nominator {
    /// `newPodNominator` (`:52-58`).
    pub fn new(pod_lister: Option<PodLister>) -> Self {
        Self {
            pod_lister,
            ..Default::default()
        }
    }

    /// `addNominatedPodUnlocked` (`:70-107`). Called during preemption when a
    /// node is nominated to run the pod; the nominator is updated before the
    /// API write to avoid racing with following scheduling cycles.
    pub fn add_nominated_pod(&mut self, pod: &Pod, nominating_info: Option<&NominatingInfo>) {
        // Always delete the pod if it already exists, to ensure we never store
        // more than one instance of the pod.
        self.delete(pod);

        let node_name = match NominatingInfo::mode(nominating_info) {
            NominatingMode::Override => nominating_info
                .map(|i| i.nominated_node_name.clone())
                .unwrap_or_default(),
            NominatingMode::Noop => {
                let nnn = nominated_node_name(pod);
                if nnn.is_empty() {
                    return;
                }
                nnn.to_string()
            }
        };

        if let Some(lister) = &self.pod_lister {
            // If the pod was removed or if it was already scheduled, don't nominate it.
            let ns = pod.metadata.namespace.as_deref().unwrap_or("");
            let Some(updated) = lister(ns, &pod.metadata.name) else {
                debug!(pod = %pod.metadata.name, "Pod doesn't exist in podLister, aborted adding it to the nominator");
                return;
            };
            if let Some(n) = updated
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_deref())
                .filter(|n| !n.is_empty())
            {
                debug!(pod = %pod.metadata.name, node = n, "Pod is already scheduled to a node, aborted adding it to the nominator");
                return;
            }
        }

        self.nominated_pod_to_node
            .insert(pod.metadata.uid.clone(), node_name.clone());
        let list = self.nominated_pods.entry(node_name).or_default();
        if list.iter().any(|np| np.uid == pod.metadata.uid) {
            debug!(pod = %pod.metadata.uid, "Pod already exists in the nominator");
            return;
        }
        list.push(PodRef::from_pod(pod));
    }

    /// `UpdateNominatedPod` (`:110-136`).
    pub fn update_nominated_pod(&mut self, old_pod: &Pod, new_pod: &Pod) {
        // In some cases, an Update event with no "NominatedNode" present is
        // received right after a node is reserved for this pod in memory; keep
        // reserving it when updating the pod pointer. Not reached when the
        // update adds, changes or removes the nominated node info.
        let mut nominating_info = None;
        if nominated_node_name(old_pod).is_empty() && nominated_node_name(new_pod).is_empty() {
            if let Some(nnn) = self.nominated_pod_to_node.get(&old_pod.metadata.uid) {
                nominating_info = Some(NominatingInfo::override_node(nnn));
            }
        }
        // Update irrespective of whether nominatedNodeName changed, to ensure
        // the pod pointer is updated.
        self.delete(old_pod);
        self.add_nominated_pod(new_pod, nominating_info.as_ref());
    }

    /// `DeleteNominatedPodIfExists` / `deleteUnlocked` (`:138-163`).
    pub fn delete(&mut self, pod: &Pod) {
        let Some(nnn) = self.nominated_pod_to_node.get(&pod.metadata.uid).cloned() else {
            return;
        };
        if let Some(list) = self.nominated_pods.get_mut(&nnn) {
            if let Some(i) = list.iter().position(|np| np.uid == pod.metadata.uid) {
                list.remove(i);
            }
            if list.is_empty() {
                self.nominated_pods.remove(&nnn);
            }
        }
        self.nominated_pod_to_node.remove(&pod.metadata.uid);
    }

    /// `nominatedPodsForNode` (`:165-169`).
    pub fn nominated_pods_for_node(&self, node_name: &str) -> Vec<PodRef> {
        self.nominated_pods
            .get(node_name)
            .cloned()
            .unwrap_or_default()
    }
}
