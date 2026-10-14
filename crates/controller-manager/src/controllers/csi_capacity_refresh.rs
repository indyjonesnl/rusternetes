//! Refresh triggers of the CSIStorageCapacity publisher (#3220): the
//! `capacities` set and work queue of kubernetes-csi/external-provisioner
//! v6.4.0 `pkg/capacity/capacity.go` (`Controller.refreshTopology`,
//! `refreshSC`, `pollCapacities`, `processNextWorkItem`) and the callers in
//! `pkg/capacity/provision.go` (`provisionWrapper`).

use super::csi_capacity::Segment;
use rusternetes_common::resources::volume::VolumeNodeAffinity;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// `WorkItem` (capacity.go:96-99): a topology segment and a StorageClass name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ItemKey {
    pub segment: Segment,
    pub storage_class_name: String,
}

/// `Controller.capacities` + `Controller.queue`, shared between the publisher
/// loop and the provisioner.
pub struct CapacityRefresher {
    state: Mutex<State>,
    notify: Notify,
}

#[derive(Default)]
struct State {
    capacities: HashSet<ItemKey>,
    queue: VecDeque<ItemKey>,
    failures: HashMap<ItemKey, u32>,
    delayed: Vec<(Instant, ItemKey)>,
}

impl CapacityRefresher {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            notify: Notify::new(),
        }
    }

    /// `addWorkItem` / `removeWorkItem` (capacity.go:469-513) collapsed to the
    /// level-triggered pass: `items` is the full set of objects that are
    /// supposed to exist. Items that dropped out stay queued, as upstream
    /// leaves them in the workqueue; `syncCapacity` then finds them gone.
    pub fn set_capacities(&self, items: HashSet<ItemKey>) {
        self.state.lock().unwrap().capacities = items;
    }

    /// The `found` of `syncCapacity`'s `c.capacities[item]` lookup (:572-583).
    pub fn is_capacity(&self, key: &ItemKey) -> bool {
        self.state.lock().unwrap().capacities.contains(key)
    }

    /// `queue.Add`: a workqueue holds each item at most once.
    fn add(&self, st: &mut State, key: ItemKey) {
        if !st.queue.contains(&key) {
            st.queue.push_back(key);
        }
        self.notify.notify_one();
    }

    /// `refreshTopology` (capacity.go:409-431): enqueue every item whose
    /// segment equals one of the affinity's terms. The affinity is expected to
    /// come from `GenerateVolumeNodeAffinity`.
    pub fn refresh_topology(&self, affinity: &VolumeNodeAffinity) {
        let Some(selector) = &affinity.required else {
            tracing::error!(
                "Capacity Controller: skipping refresh: unexpected VolumeNodeAffinity, missing NodeSelectorTerms: {affinity:?}"
            );
            return;
        };
        let mut st = self.state.lock().unwrap();
        for term in &selector.node_selector_terms {
            let segment = match super::csi_capacity::term_to_segment(term) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!(
                        "Capacity Controller: skipping refresh: unexpected node selector term {term:?}: {e}"
                    );
                    continue;
                }
            };
            let matching: Vec<ItemKey> = st
                .capacities
                .iter()
                .filter(|i| i.segment == segment)
                .cloned()
                .collect();
            for item in matching {
                tracing::trace!("Capacity Controller: enqueuing {item:?} because of the topology");
                self.add(&mut st, item);
            }
        }
    }

    /// `refreshSC` (capacity.go:455-466).
    pub fn refresh_sc(&self, storage_class_name: &str) {
        let mut st = self.state.lock().unwrap();
        let matching: Vec<ItemKey> = st
            .capacities
            .iter()
            .filter(|i| i.storage_class_name == storage_class_name)
            .cloned()
            .collect();
        for item in matching {
            tracing::trace!("Capacity Controller: enqueuing {item:?} because of the storage class");
            self.add(&mut st, item);
        }
    }

    /// `queue.Get` (without blocking): moves due `AddRateLimited` items onto
    /// the queue first.
    pub fn pop(&self) -> Option<ItemKey> {
        let mut st = self.state.lock().unwrap();
        let now = Instant::now();
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut st.delayed)
            .into_iter()
            .partition(|(t, _)| *t <= now);
        st.delayed = later;
        for (_, key) in due {
            if !st.queue.contains(&key) {
                st.queue.push_back(key);
            }
        }
        st.queue.pop_front()
    }

    /// The queued items, oldest first.
    #[allow(dead_code)] // used by tests; the bin target re-includes this module
    pub fn queued(&self) -> Vec<ItemKey> {
        self.state.lock().unwrap().queue.iter().cloned().collect()
    }

    #[allow(dead_code)] // used by tests; the bin target re-includes this module
    pub fn clear_queue(&self) {
        self.state.lock().unwrap().queue.clear();
    }

    /// `queue.Done` (stub until #3548).
    pub fn done(&self, _key: &ItemKey) {}

    /// `queue.Forget`.
    pub fn forget(&self, key: &ItemKey) {
        self.state.lock().unwrap().failures.remove(key);
    }

    /// `queue.AddRateLimited` with `NewTypedItemExponentialFailureRateLimiter
    /// (start, max)` (`When`: `base * 2^failures`, capped at `max`). Returns
    /// the delay.
    pub fn add_rate_limited(&self, key: ItemKey, start: Duration, max: Duration) -> Duration {
        let mut st = self.state.lock().unwrap();
        let exp = st.failures.entry(key.clone()).or_insert(0);
        let n = *exp;
        *exp += 1;
        let delay = start
            .checked_mul(2u32.saturating_pow(n))
            .map_or(max, |d| d.min(max));
        st.delayed.push((Instant::now() + delay, key));
        delay
    }

    /// When the earliest rate-limited item becomes due.
    pub fn next_delayed(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap()
            .delayed
            .iter()
            .map(|(t, _)| *t)
            .min()
    }

    /// Resolves when an item was queued since the last wait.
    pub async fn notified(&self) {
        self.notify.notified().await
    }
}

impl Default for CapacityRefresher {
    fn default() -> Self {
        Self::new()
    }
}
