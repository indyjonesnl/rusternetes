//! State of the CSIStorageCapacity publisher (#3220, #3548): the
//! `capacities` map, the informer caches the handlers read, and the work queue
//! of kubernetes-csi/external-provisioner v6.4.0 `pkg/capacity/capacity.go`
//! (`Controller.capacities`, `queue`, `refreshTopology`, `refreshSC`,
//! `pollCapacities`, `processNextWorkItem`) and the callers in
//! `pkg/capacity/provision.go` (`provisionWrapper`). The event handlers
//! themselves are `CapacityController::on_*` in `csi_capacity.rs`.

use super::csi_capacity::Segment;
use super::csi_capacity_metrics::CapacityStats;
use rusternetes_common::resources::csi::CSIStorageCapacity;
use rusternetes_common::resources::volume::{StorageClass, VolumeNodeAffinity};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// `WorkItem` (capacity.go:105-108): a topology segment and a StorageClass name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ItemKey {
    pub segment: Segment,
    pub storage_class_name: String,
}

impl ItemKey {
    /// `WorkItem.equals` (capacity.go:110-113).
    pub fn equals(&self, c: &CSIStorageCapacity) -> bool {
        c.storage_class_name == self.storage_class_name
            && c.node_topology.as_ref().is_some_and(|t| {
                t.match_labels.as_ref() == Some(&self.segment.label_map())
                    && t.match_expressions.as_ref().is_none_or(|e| e.is_empty())
            })
    }
}

/// `QueueKey` (capacity.go:115-120): either a work item to sync, or an
/// obsolete CSIStorageCapacity object to delete. The Go key holds a *pointer*
/// (`item *WorkItem`, `capacity *CSIStorageCapacity`) and every
/// `queue.Add(QueueKey{item: &item})` uses a fresh one, so the workqueue (which
/// keys by `==`, i.e. by pointer) does not collapse repeated adds of the same
/// logical item: each add is its own entry and costs its own sync. `id` is
/// that pointer identity: [`State::add_item`] / [`State::add_capacity`] mint a
/// new one per add, and a retry (`AddRateLimited(obj)`) re-adds the same key.
/// `deleteCapacity` only uses the namespace and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueueKey {
    Item {
        id: u64,
        item: ItemKey,
    },
    Capacity {
        id: u64,
        namespace: String,
        name: String,
    },
}

/// `Controller.capacities`, the informer caches and `Controller.queue`,
/// shared between the publisher loop and the provisioner.
pub struct CapacityRefresher {
    state: Mutex<State>,
    notify: Arc<Notify>,
}

/// Everything `capacitiesLock` guards, plus the cache contents the handlers
/// read through listers in Go.
#[derive(Default)]
pub struct State {
    /// `topologyInformer.List()`.
    pub segments: Vec<Segment>,
    /// `scInformer.Lister()`.
    pub classes: HashMap<String, StorageClass>,
    /// `cInformer.Lister()`, by object name (one namespace).
    pub cache: HashMap<String, CSIStorageCapacity>,
    /// `capacities` (capacity.go:96-101): one entry per object that is
    /// supposed to exist; entries that exist on the API server hold it.
    pub capacities: HashMap<ItemKey, Option<CSIStorageCapacity>>,
    queue: VecDeque<QueueKey>,
    dirty: HashSet<QueueKey>,
    processing: HashSet<QueueKey>,
    failures: HashMap<QueueKey, u32>,
    delayed: Vec<(Instant, QueueKey)>,
    notify: Arc<Notify>,
    /// Source of the pointer identities of [`QueueKey`].
    next_id: u64,
}

impl State {
    /// `queue.Add` (client-go `workqueue.Typed.Add`): an item that is queued
    /// or being processed is not queued twice; one that is being processed is
    /// queued again by `Done`.
    pub fn add(&mut self, key: QueueKey) {
        if self.dirty.contains(&key) {
            return;
        }
        self.dirty.insert(key.clone());
        if self.processing.contains(&key) {
            return;
        }
        self.queue.push_back(key);
        self.notify.notify_one();
    }

    fn fresh_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// `queue.Add(QueueKey{item: &item})`: a fresh pointer, so a new entry.
    pub fn add_item(&mut self, item: ItemKey) {
        let id = self.fresh_id();
        self.add(QueueKey::Item { id, item });
    }

    /// `queue.Add(QueueKey{capacity: capacity})`.
    pub fn add_capacity(&mut self, namespace: String, name: String) {
        let id = self.fresh_id();
        self.add(QueueKey::Capacity {
            id,
            namespace,
            name,
        });
    }
}

impl CapacityRefresher {
    pub fn new() -> Self {
        let notify = Arc::new(Notify::new());
        Self {
            state: Mutex::new(State {
                notify: notify.clone(),
                ..State::default()
            }),
            notify,
        }
    }

    /// Hold `capacitiesLock`.
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    /// Seed `capacities` with entries that have no object yet (what
    /// `addWorkItem` does, capacity.go:469-484, minus the enqueue).
    pub fn set_capacities(&self, items: HashSet<ItemKey>) {
        self.lock().capacities = items.into_iter().map(|i| (i, None)).collect();
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
        let mut st = self.lock();
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
                .keys()
                .filter(|i| i.segment == segment)
                .cloned()
                .collect();
            for item in matching {
                tracing::trace!("Capacity Controller: enqueuing {item:?} because of the topology");
                st.add_item(item);
            }
        }
    }

    /// `refreshSC` (capacity.go:455-466).
    pub fn refresh_sc(&self, storage_class_name: &str) {
        let mut st = self.lock();
        let matching: Vec<ItemKey> = st
            .capacities
            .keys()
            .filter(|i| i.storage_class_name == storage_class_name)
            .cloned()
            .collect();
        for item in matching {
            tracing::trace!("Capacity Controller: enqueuing {item:?} because of the storage class");
            st.add_item(item);
        }
    }

    /// `getObjectsGoal/Current/Obsolete` (capacity.go:772-817), evaluated now.
    /// `is_managed` is `Controller.isManaged`.
    pub fn stats(&self, is_managed: &dyn Fn(&CSIStorageCapacity) -> bool) -> CapacityStats {
        let st = self.lock();
        CapacityStats {
            goal: st.capacities.len() as i64,
            current: st.capacities.values().filter(|c| c.is_some()).count() as i64,
            obsolete: st
                .cache
                .values()
                .filter(|c| is_managed(c))
                .filter(|c| !st.capacities.keys().any(|i| i.equals(c)))
                .count() as i64,
        }
    }

    /// `queue.Get` (without blocking): moves due `AddRateLimited` items onto
    /// the queue first. The caller must call [`Self::done`] afterwards.
    pub fn pop(&self) -> Option<QueueKey> {
        let mut st = self.lock();
        let now = Instant::now();
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut st.delayed)
            .into_iter()
            .partition(|(t, _)| *t <= now);
        st.delayed = later;
        for (_, key) in due {
            st.add(key);
        }
        let key = st.queue.pop_front()?;
        st.processing.insert(key.clone());
        st.dirty.remove(&key);
        if !st.queue.is_empty() {
            // Chain the wake-up so several workers drain a burst.
            self.notify.notify_one();
        }
        Some(key)
    }

    /// `queue.Done`.
    pub fn done(&self, key: &QueueKey) {
        let mut st = self.lock();
        st.processing.remove(key);
        if st.dirty.contains(key) {
            st.queue.push_back(key.clone());
            self.notify.notify_one();
        }
    }

    /// The queued work items, oldest first.
    pub fn queued(&self) -> Vec<ItemKey> {
        self.lock()
            .queue
            .iter()
            .filter_map(|k| match k {
                QueueKey::Item { item, .. } => Some(item.clone()),
                QueueKey::Capacity { .. } => None,
            })
            .collect()
    }

    pub fn clear_queue(&self) {
        let mut st = self.lock();
        st.queue.clear();
        st.dirty.clear();
    }

    /// `queue.Forget`.
    pub fn forget(&self, key: &QueueKey) {
        self.lock().failures.remove(key);
    }

    /// `queue.AddRateLimited` with `NewTypedItemExponentialFailureRateLimiter
    /// (start, max)` (`When`: `base * 2^failures`, capped at `max`). Returns
    /// the delay.
    pub fn add_rate_limited(&self, key: QueueKey, start: Duration, max: Duration) -> Duration {
        let mut st = self.lock();
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
        self.lock().delayed.iter().map(|(t, _)| *t).min()
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
