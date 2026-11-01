//! `OpportunisticBatch` (`pkg/scheduler/framework/runtime/batch.go`):
//! caches the filtering and scoring results of one pod so that the next pods
//! with the same signature, scheduled one per node, skip them.
//!
//! The batch is owned by the [`crate::framework_runtime::Framework`]
//! (`frameworkImpl.batch`, framework.go:102) and reached through
//! `Framework::get_node_hint` / `Framework::store_schedule_results`
//! (framework.go:1402-1408).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::fwk::PodSignature;
use crate::score::SortedNodeScores;

/// `maxBatchAge` (batch.go:55): a batch older than this is thrown away, so
/// neither a long run of compatible pods nor a long wait between pods keeps
/// stale scores alive.
pub const MAX_BATCH_AGE: Duration = Duration::from_millis(500);

/// `batchState` (batch.go:41-45).
#[derive(Debug, Clone)]
struct BatchState {
    signature: PodSignature,
    sorted_nodes: SortedNodeScores,
    creation_time: Instant,
}

/// `schedulingCycle` (batch.go:47-51).
#[derive(Debug, Clone, Default)]
struct SchedulingCycle {
    cycle_count: i64,
    chosen_node: String,
    succeeded: bool,
}

#[derive(Debug, Default)]
struct Inner {
    state: Option<BatchState>,
    last_cycle: SchedulingCycle,
    /// "Used primarily for tests, count the total pods we have successfully
    /// batched" (batch.go:36-38).
    batched_pods: i64,
}

/// Why the cached state was dropped (`metrics.BatchFlush*`,
/// `logUnusableState`, batch.go:160-166).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    PodSkipped,
    PodFailed,
    PodIncompatible,
    Expired,
    NodeMissing,
    NodeNotFull,
    PodNotBatchable,
    EmptyList,
}

impl FlushReason {
    /// The `metrics.BatchFlush*` label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PodSkipped => "pod_skipped",
            Self::PodFailed => "pod_failed",
            Self::PodIncompatible => "pod_incompatible",
            Self::Expired => "expired",
            Self::NodeMissing => "node_missing",
            Self::NodeNotFull => "node_not_full",
            Self::PodNotBatchable => "pod_not_batchable",
            Self::EmptyList => "empty_list",
        }
    }
}

/// What `batchStateCompatible` (batch.go:168-232) decides without running
/// the Filter plugins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precheck {
    /// The state cannot serve this pod; it has been dropped when it was
    /// stale (`logUnusableState`).
    Unusable,
    /// The state matches; it is usable if the node the previous pod went to
    /// now rejects this pod.
    CheckLastChosenNode(String),
}

/// `OpportunisticBatch` (batch.go:26-39).
#[derive(Debug, Default)]
pub struct OpportunisticBatch {
    inner: Mutex<Inner>,
}

impl OpportunisticBatch {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `logUnusableState` (batch.go:160-166). Like upstream it only records
    /// the reason: a state found unusable stays in place until the next
    /// `StoreScheduleResults` replaces or clears it.
    pub fn log_unusable_state(cycle_count: i64, reason: FlushReason) {
        tracing::debug!(
            cycle_count,
            reason = reason.as_str(),
            "OpportunisticBatch found unusable state"
        );
    }

    /// The first half of `batchStateCompatible` (batch.go:168-232), up to
    /// the Filter run on the node the previous pod was placed on. The caller
    /// runs that Filter (it needs the cycle's nodes and pods, which the
    /// framework does not hold) and reports the result by
    /// calling [`OpportunisticBatch::take_hint`] (the node rejects the pod) or
    /// [`OpportunisticBatch::log_unusable_state`] (it is missing or still fits,
    /// `BatchFlushNodeMissing`/`BatchFlushNodeNotFull`, batch.go:213-225:
    /// "If we *could* put this pod on the node we just used, then we can't use
    /// our cache because we don't know how to rescore the used node").
    pub fn precheck(&self, signature: Option<&PodSignature>, cycle_count: i64) -> Precheck {
        let inner = self.lock();
        // "Just return if we don't have any state to use."
        let state_empty = inner
            .state
            .as_ref()
            .is_none_or(|s| s.sorted_nodes.is_empty());
        if state_empty {
            return Precheck::Unusable;
        }
        // "A previous pod was scheduled by another profile, meaning we can't
        // use the state anymore."
        if cycle_count != inner.last_cycle.cycle_count + 1 {
            Self::log_unusable_state(cycle_count, FlushReason::PodSkipped);
            return Precheck::Unusable;
        }
        // "If our last pod failed we can't use the state."
        if !inner.last_cycle.succeeded {
            Self::log_unusable_state(cycle_count, FlushReason::PodFailed);
            return Precheck::Unusable;
        }
        let state = inner.state.as_ref().expect("checked non-empty above");
        // "If the new pod is incompatible with the current state, throw the
        // state away."
        if signature.is_none_or(|s| *s != state.signature) {
            Self::log_unusable_state(cycle_count, FlushReason::PodIncompatible);
            return Precheck::Unusable;
        }
        // "If the state is too old, throw the state away."
        if Instant::now() > state.creation_time + MAX_BATCH_AGE {
            Self::log_unusable_state(cycle_count, FlushReason::Expired);
            return Precheck::Unusable;
        }
        Precheck::CheckLastChosenNode(inner.last_cycle.chosen_node.clone())
    }

    /// The tail of `batchStateCompatible` for a rejected Filter on the last
    /// chosen node, then `GetNodeHint`'s `sortedNodes.Pop()` (batch.go:84-92).
    pub fn take_hint(&self, cycle_count: i64) -> Option<String> {
        let mut inner = self.lock();
        let hint = inner.state.as_mut()?.sorted_nodes.pop();
        if let Some(h) = &hint {
            tracing::debug!(
                cycle_count,
                hint = %h,
                remaining_nodes = inner.state.as_ref().map_or(0, |s| s.sorted_nodes.len()),
                "OpportunisticBatch provided node hint"
            );
        }
        hint
    }

    /// `StoreScheduleResults` (batch.go:96-158): remember the cycle for the
    /// next pod and, when this pod's results are batchable, the other nodes.
    pub fn store_schedule_results(
        &self,
        signature: Option<PodSignature>,
        hinted_node: &str,
        chosen_node: &str,
        other_nodes: Option<SortedNodeScores>,
        cycle_count: i64,
    ) {
        let mut inner = self.lock();
        // "Set our cycle information for next time."
        inner.last_cycle = SchedulingCycle {
            cycle_count,
            chosen_node: chosen_node.to_string(),
            succeeded: true,
        };
        if hinted_node == chosen_node {
            // "skipping set state because hint was provided"
            inner.batched_pods += 1;
            return;
        }
        // "Only store new results if we didn't give a hint or it wasn't
        // used."
        if let (Some(signature), Some(other)) = (signature, other_nodes) {
            if !other.is_empty() {
                inner.state = Some(BatchState {
                    signature,
                    sorted_nodes: other,
                    creation_time: Instant::now(),
                });
                return;
            }
            Self::log_unusable_state(cycle_count, FlushReason::EmptyList);
        } else {
            Self::log_unusable_state(cycle_count, FlushReason::PodNotBatchable);
        }
        inner.state = None;
    }

    /// The signature and the remaining nodes (best first) of the cached
    /// state; what upstream's `batch_test.go` reads from `batch.state`.
    pub fn cached_state(&self) -> Option<(PodSignature, Vec<String>)> {
        let inner = self.lock();
        let state = inner.state.as_ref()?;
        let mut nodes = state.sorted_nodes.clone();
        let mut names = Vec::new();
        while let Some(n) = nodes.pop() {
            names.push(n);
        }
        Some((state.signature.clone(), names))
    }

    /// `batchedPods` (framework.go:1936): the pods that were scheduled with a
    /// hint.
    pub fn batched_pods(&self) -> i64 {
        self.lock().batched_pods
    }
}
