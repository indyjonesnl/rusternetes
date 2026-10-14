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
        todo!()
    }
    pub fn set_capacities(&self, _items: HashSet<ItemKey>) {
        todo!()
    }
    pub fn is_capacity(&self, _key: &ItemKey) -> bool {
        todo!()
    }
    pub fn refresh_topology(&self, _affinity: &VolumeNodeAffinity) {
        todo!()
    }
    pub fn refresh_sc(&self, _storage_class_name: &str) {
        todo!()
    }
    pub fn poll_capacities(&self) {
        todo!()
    }
    pub fn pop(&self) -> Option<ItemKey> {
        todo!()
    }
    pub fn queued(&self) -> Vec<ItemKey> {
        todo!()
    }
    pub fn clear_queue(&self) {
        todo!()
    }
    pub fn forget(&self, _key: &ItemKey) {
        todo!()
    }
    pub fn add_rate_limited(&self, _key: ItemKey, _start: Duration, _max: Duration) -> Duration {
        todo!()
    }
    pub fn next_delayed(&self) -> Option<Instant> {
        todo!()
    }
    pub async fn notified(&self) {
        self.notify.notified().await
    }
}

impl Default for CapacityRefresher {
    fn default() -> Self {
        Self::new()
    }
}
