//! Asynchronous preemption bookkeeping (KEP-4832, `SchedulerAsyncPreemption`,
//! Beta and on by default since 1.33).
//!
//! Ported from upstream (release-1.35):
//! - `pkg/scheduler/framework/preemption/preemption.go`: the `Evaluator`
//!   fields `preempting` / `lastVictimsPendingPreemption` (`:136-144`),
//!   `IsPodRunningPreemption` (`:224-249`) and the state transitions of
//!   `prepareCandidateAsync` (`:534-632`).
//! - `pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go`:
//!   `PreEnqueue` (`:134-144`), which keeps a pod out of the scheduling cycle
//!   while its preemption runs.
//!
//! The task that evicts the victims is [`Scheduler::prepare_candidate_async`];
//! this module owns the shared state the queue consults.
//!
//! Deviation: upstream's `IsPodRunningPreemption` reads the last victim from
//! the pod lister (`ev.PodLister.Pods(ns).Get(name)`): not found, or carrying
//! a `deletionTimestamp`, means the preemption is done. The queue's
//! `PreEnqueue` is synchronous and storage mode has no synchronous pod cache,
//! so the same signal is fed from the pod watch instead: the event handlers
//! call [`AsyncPreemption::observe_pod`] for every pod update/delete (before
//! they move any pods in the queue) and a pending last victim is recorded as
//! done when it is seen deleted or terminating. A victim that was never
//! observed yet is still running, which is what a lister hit without a
//! deletion timestamp reports.

use crate::queue::scheduling_queue::PreEnqueuePlugin;
use rusternetes_common::resources::Pod;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// `pendingVictim` (preemption.go:146-149) plus whether the watch has shown
/// the victim deleted or terminating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingVictim {
    pub namespace: String,
    pub name: String,
    /// The last victim's deletion has been observed (gone, or a
    /// `deletionTimestamp`).
    pub gone: bool,
}

#[derive(Debug, Default)]
struct State {
    /// `preempting sets.Set[types.UID]`: the pods currently triggering
    /// preemption asynchronously, which must not enter the scheduling cycle.
    preempting: HashSet<String>,
    /// `lastVictimsPendingPreemption`: for a preemptor waiting for one last
    /// victim, that victim.
    last_victims_pending_preemption: HashMap<String, PendingVictim>,
}

/// The shared `preempting` / `lastVictimsPendingPreemption` state of the
/// `Evaluator`, guarded by `ev.mu`.
#[derive(Debug, Default)]
pub struct AsyncPreemption {
    state: Mutex<State>,
}

impl AsyncPreemption {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `ev.preempting.Insert(pod.UID)` (preemption.go:565).
    pub fn start(&self, preemptor_uid: &str) {
        self.state
            .lock()
            .unwrap()
            .preempting
            .insert(preemptor_uid.to_string());
    }

    /// `ev.lastVictimsPendingPreemption[pod.UID] = pendingVictim{..}`
    /// (preemption.go:614).
    pub fn set_last_victim(&self, preemptor_uid: &str, victim: &Pod) {
        self.state
            .lock()
            .unwrap()
            .last_victims_pending_preemption
            .insert(
                preemptor_uid.to_string(),
                PendingVictim {
                    namespace: victim.metadata.namespace.clone().unwrap_or_default(),
                    name: victim.metadata.name.clone(),
                    gone: false,
                },
            );
    }

    /// `ev.preempting.Delete(pod.UID)` and
    /// `delete(ev.lastVictimsPendingPreemption, pod.UID)` (preemption.go:623-624).
    pub fn finish(&self, preemptor_uid: &str) {
        let mut s = self.state.lock().unwrap();
        s.preempting.remove(preemptor_uid);
        s.last_victims_pending_preemption.remove(preemptor_uid);
    }

    /// `IsPodRunningPreemption` (preemption.go:224-249).
    pub fn is_pod_running_preemption(&self, preemptor_uid: &str) -> bool {
        let s = self.state.lock().unwrap();
        if !s.preempting.contains(preemptor_uid) {
            return false;
        }
        match s.last_victims_pending_preemption.get(preemptor_uid) {
            // In `preempting` but the last victim is not registered yet: the
            // async preemption is pending.
            None => true,
            // Waiting for one last victim: done once its deletion is seen.
            Some(v) => !v.gone,
        }
    }

    /// Feed a pod event (see the module docs): a pending last victim that is
    /// `deleted`, or carries a `deletionTimestamp`, is done.
    pub fn observe_pod(&self, pod: &Pod, deleted: bool) {
        if !deleted && pod.metadata.deletion_timestamp.is_none() {
            return;
        }
        let ns = pod.metadata.namespace.as_deref().unwrap_or_default();
        let mut s = self.state.lock().unwrap();
        for v in s.last_victims_pending_preemption.values_mut() {
            if v.namespace == ns && v.name == pod.metadata.name {
                v.gone = true;
            }
        }
    }

    /// The `preempting` set (test inspection, `pe.preempting`).
    pub fn preempting(&self) -> HashSet<String> {
        self.state.lock().unwrap().preempting.clone()
    }

    /// The `lastVictimsPendingPreemption` map (test inspection).
    pub fn last_victims_pending_preemption(&self) -> HashMap<String, PendingVictim> {
        self.state
            .lock()
            .unwrap()
            .last_victims_pending_preemption
            .clone()
    }
}

/// `DefaultPreemption.PreEnqueue` (default_preemption.go:134-144).
pub struct DefaultPreemptionPreEnqueue {
    state: Arc<AsyncPreemption>,
}

impl DefaultPreemptionPreEnqueue {
    pub fn new(state: Arc<AsyncPreemption>) -> Self {
        Self { state }
    }
}

impl PreEnqueuePlugin for DefaultPreemptionPreEnqueue {
    fn name(&self) -> &str {
        crate::framework_runtime::names::DEFAULT_PREEMPTION
    }

    /// `if !pl.fts.EnableAsyncPreemption { return nil }`, then
    /// `UnschedulableAndUnresolvable` while the pod runs its preemption.
    fn pre_enqueue(&self, pod: &Pod) -> Result<(), String> {
        if !async_preemption_enabled() {
            return Ok(());
        }
        if self
            .state
            .is_pod_running_preemption(&crate::advanced::pod_key(pod))
        {
            return Err("waiting for the preemption for this pod to be finished".to_string());
        }
        Ok(())
    }
}

/// `utilfeature.DefaultFeatureGate.Enabled(features.SchedulerAsyncPreemption)`
/// (`EnableAsyncPreemption`, Beta, default on in 1.35).
pub fn async_preemption_enabled() -> bool {
    rusternetes_common::feature_gates::gate_enabled("SchedulerAsyncPreemption").unwrap_or(false)
}
