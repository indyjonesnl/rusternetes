//! Resource event handlers that feed the scheduling queue.
//!
//! Ported from upstream (release-1.35):
//! - `pkg/scheduler/scheduler.go`: `unionedGVKs` (`:635-647`), the union of
//!   every profile's registered `ClusterEvent` action types per resource.
//! - `pkg/scheduler/eventhandlers.go`: `addAllEventHandlers` (`:479-726`),
//!   in particular `buildEvtResHandler` (`:535-579`) and the per-resource
//!   `switch gvk` (`:581-710`): an informer is only started for a resource
//!   some enabled plugin's `EventsToRegister` names, a handler is only
//!   installed for the action types registered for it, and each handler calls
//!   `SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, oldObj, newObj, nil)`.
//!
//! Covered here: PersistentVolume, PersistentVolumeClaim, StorageClass,
//! CSINode, CSIDriver, CSIStorageCapacity, VolumeAttachment, ResourceClaim,
//! ResourceSlice and DeviceClass. Workload (GenericWorkload gate, off in
//! 1.35) and the dynamic-informer fallback for custom resources are not
//! watched. Deviation: upstream's DRA handlers hang off the DRA manager's
//! caches (`resourceClaimCache`, `resourceSliceTracker`, the extended-resource
//! cache for DeviceClass); here they are plain list/watches.

use crate::data_plane::DataPlane;
use crate::queue::dra_hints::ResourceClaimCache;
use crate::queue::{
    ActionType, ClusterEvent, EventObject, EventResource, PriorityQueue, QueueingHintMapPerProfile,
};
use futures::StreamExt;
use rusternetes_common::resources::{
    CSIDriver, CSINode, CSIStorageCapacity, DeviceClass, PersistentVolume, PersistentVolumeClaim,
    ResourceClaim, ResourceSlice, StorageClass, VolumeAttachment,
};
use rusternetes_common::types::ObjectMeta;
use rusternetes_storage::{Storage, WatchEvent};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// `unionedGVKs` (`scheduler.go:635-647`).
pub fn unioned_gvks(per_profile: &QueueingHintMapPerProfile) -> HashMap<EventResource, ActionType> {
    let mut gvk_map: HashMap<EventResource, ActionType> = HashMap::new();
    for hints in per_profile.values() {
        for evt in hints.keys() {
            *gvk_map.entry(evt.resource).or_insert(ActionType::NONE) |= evt.action_type;
        }
    }
    gvk_map
}

/// An object an event handler can key and version.
pub trait Watched: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    fn meta(&self) -> &ObjectMeta;
}

macro_rules! watched {
    ($($t:ty),*) => {$(impl Watched for $t { fn meta(&self) -> &ObjectMeta { &self.metadata } })*};
}
watched!(
    PersistentVolume,
    PersistentVolumeClaim,
    StorageClass,
    CSINode,
    CSIDriver,
    CSIStorageCapacity,
    VolumeAttachment,
    ResourceClaim,
    ResourceSlice,
    DeviceClass
);

fn object_key(m: &ObjectMeta) -> String {
    format!("{}/{}", m.namespace.as_deref().unwrap_or(""), m.name)
}

/// Mirrors an upsert (`false`) / delete (`true`) into an informer-style cache.
type StoreFn<T> = Box<dyn Fn(&T, bool) + Send + Sync>;

/// `buildEvtResHandler` (`eventhandlers.go:535-579`) for one resource. Our
/// watches deliver only the new object, so the last-seen copy per key
/// supplies the informer's `oldObj`.
pub struct ResourceEventHandler<T> {
    queue: Arc<PriorityQueue>,
    resource: EventResource,
    at: ActionType,
    seen: HashMap<String, T>,
    /// The informer cache the handler runs behind: kept current before an
    /// event is dispatched, so hint functions reading it see the new state.
    store: Option<StoreFn<T>>,
}

impl<T: Watched> ResourceEventHandler<T> {
    pub fn new(queue: Arc<PriorityQueue>, resource: EventResource, at: ActionType) -> Self {
        Self {
            queue,
            resource,
            at,
            seen: HashMap::new(),
            store: None,
        }
    }

    fn dispatch(&self, at: ActionType, old: Option<T>, new: Option<T>) {
        let evt = ClusterEvent::new(self.resource, at);
        debug!(event = %evt.label(), "resource event");
        self.queue.move_all_to_active_or_backoff_queue(
            &evt,
            old.map(|o| Arc::new(o) as Arc<EventObject>),
            new.map(|n| Arc::new(n) as Arc<EventObject>),
            None,
        );
    }

    /// `AddFunc` / `UpdateFunc`: the first sighting of a key is an add, later
    /// ones updates. An unchanged resourceVersion never fires (informer
    /// resync period 0, `options.go:336`), so relists stay quiet.
    pub fn on_upsert(&mut self, obj: T) {
        if let Some(store) = &self.store {
            store(&obj, false);
        }
        let key = object_key(obj.meta());
        match self.seen.insert(key, obj.clone()) {
            None => {
                if self.at.contains(ActionType::ADD) {
                    self.dispatch(ActionType::ADD, None, Some(obj));
                }
            }
            Some(old) => {
                let rv = obj.meta().resource_version.as_ref();
                if rv.is_some() && rv == old.meta().resource_version.as_ref() {
                    return;
                }
                if self.at.contains(ActionType::UPDATE) {
                    self.dispatch(ActionType::UPDATE, Some(old), Some(obj));
                }
            }
        }
    }

    /// `DeleteFunc`: only objects the handler knew about.
    pub fn on_delete(&mut self, obj: T) {
        if let Some(store) = &self.store {
            store(&obj, true);
        }
        if self.seen.remove(&object_key(obj.meta())).is_none() {
            return;
        }
        if self.at.contains(ActionType::DELETE) {
            self.dispatch(ActionType::DELETE, Some(obj), None);
        }
    }

    /// Replay what a (re)list shows: adds/updates, and deletes for keys that
    /// disappeared while the watch was down.
    pub fn resync(&mut self, items: Vec<T>) {
        let live: HashSet<String> = items.iter().map(|o| object_key(o.meta())).collect();
        for item in items {
            self.on_upsert(item);
        }
        let gone: Vec<T> = self
            .seen
            .iter()
            .filter(|(k, _)| !live.contains(*k))
            .map(|(_, o)| o.clone())
            .collect();
        for obj in gone {
            self.on_delete(obj);
        }
    }
}

/// Storage mode: watch `/registry/<prefix>`; (re)list on every (re)connect
/// to cover the gap, like an informer's relist.
async fn run_storage_feed<T: Watched, S: Storage + Send + Sync + 'static>(
    storage: Arc<S>,
    prefix: String,
    mut handler: ResourceEventHandler<T>,
    retry: Duration,
) {
    loop {
        // Watch first, then list: events between the two are replayed from
        // the stream and deduplicated by resourceVersion.
        let mut watch = match storage.watch(&prefix).await {
            Ok(w) => w,
            Err(e) => {
                warn!("scheduler: watch {} failed: {}, retrying", prefix, e);
                tokio::time::sleep(retry).await;
                continue;
            }
        };
        match storage.list::<T>(&prefix).await {
            Ok(items) => handler.resync(items),
            Err(e) => warn!("scheduler: list {} failed: {}", prefix, e),
        }
        loop {
            match watch.next().await {
                Some(Ok(WatchEvent::Added(_, v) | WatchEvent::Modified(_, v))) => {
                    match serde_json::from_str::<T>(&v) {
                        Ok(o) => handler.on_upsert(o),
                        Err(e) => warn!("scheduler: cannot decode watched {}: {}", prefix, e),
                    }
                }
                Some(Ok(WatchEvent::Deleted(_, v))) => match serde_json::from_str::<T>(&v) {
                    Ok(o) => handler.on_delete(o),
                    Err(e) => warn!("scheduler: cannot decode deleted {}: {}", prefix, e),
                },
                Some(Err(e)) => {
                    warn!("scheduler: watch {} error: {}, reconnecting", prefix, e);
                    break;
                }
                None => break,
            }
        }
        tokio::time::sleep(retry).await;
    }
}

/// API mode: a reflector over `path`, subscribed before it runs. A lagged or
/// closed subscription falls back to a relist from the reflector store.
fn spawn_api_feed<T: Watched>(
    api: &crate::data_plane::ApiBackend,
    path: &'static str,
    mut handler: ResourceEventHandler<T>,
) {
    use rusternetes_client::reflector::{ApiListWatch, Reflector, StoreEvent};
    let lw = Arc::new(ApiListWatch::new(Arc::clone(&api.client), path));
    let key_fn: fn(&T) -> String = |o| object_key(o.meta());
    let reflector = Arc::new(Reflector::new(lw, key_fn));
    let mut sub = reflector.subscribe();
    let runner = Arc::clone(&reflector);
    tokio::spawn(async move { runner.run().await });
    tokio::spawn(async move {
        loop {
            match sub.recv().await {
                // The reflector's initial list does not emit, so a modify of a
                // key never seen is surfaced as the add the informer reported.
                Ok(StoreEvent::Added(o) | StoreEvent::Modified(o)) => handler.on_upsert(o),
                Ok(StoreEvent::Deleted(o)) => handler.on_delete(o),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    handler.resync(reflector.store().items());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn start_feed<T: Watched, S: Storage + Send + Sync + 'static>(
    data: &DataPlane<S>,
    queue: &Arc<PriorityQueue>,
    gvk_map: &HashMap<EventResource, ActionType>,
    resource: EventResource,
    prefix: &str,
    path: &'static str,
    retry: Duration,
    store: Option<StoreFn<T>>,
) {
    let Some(at) = gvk_map.get(&resource).copied() else {
        return;
    };
    let mut handler = ResourceEventHandler::<T>::new(Arc::clone(queue), resource, at);
    handler.store = store;
    match data {
        DataPlane::Api(api) => spawn_api_feed::<T>(api, path, handler),
        DataPlane::Storage(s) => {
            tokio::spawn(run_storage_feed::<T, S>(
                Arc::clone(s),
                rusternetes_storage::build_prefix(prefix, None),
                handler,
                retry,
            ));
        }
    }
}

/// The resource arms of `addAllEventHandlers`' `switch gvk`
/// (`eventhandlers.go:581-710`): start a feed for each resource named in
/// `gvk_map`.
pub fn add_resource_event_handlers<S: Storage + Send + Sync + 'static>(
    data: &DataPlane<S>,
    queue: &Arc<PriorityQueue>,
    gvk_map: &HashMap<EventResource, ActionType>,
    retry: Duration,
    claims: &ResourceClaimCache,
) {
    use EventResource as R;
    start_feed::<CSINode, S>(
        data,
        queue,
        gvk_map,
        R::CsiNode,
        "csinodes",
        "/apis/storage.k8s.io/v1/csinodes",
        retry,
        None,
    );
    start_feed::<CSIDriver, S>(
        data,
        queue,
        gvk_map,
        R::CsiDriver,
        "csidrivers",
        "/apis/storage.k8s.io/v1/csidrivers",
        retry,
        None,
    );
    start_feed::<CSIStorageCapacity, S>(
        data,
        queue,
        gvk_map,
        R::CsiStorageCapacity,
        "csistoragecapacities",
        "/apis/storage.k8s.io/v1/csistoragecapacities",
        retry,
        None,
    );
    start_feed::<PersistentVolume, S>(
        data,
        queue,
        gvk_map,
        R::PersistentVolume,
        "persistentvolumes",
        "/api/v1/persistentvolumes",
        retry,
        None,
    );
    start_feed::<PersistentVolumeClaim, S>(
        data,
        queue,
        gvk_map,
        R::PersistentVolumeClaim,
        "persistentvolumeclaims",
        "/api/v1/persistentvolumeclaims",
        retry,
        None,
    );
    start_feed::<StorageClass, S>(
        data,
        queue,
        gvk_map,
        R::StorageClass,
        "storageclasses",
        "/apis/storage.k8s.io/v1/storageclasses",
        retry,
        None,
    );
    start_feed::<VolumeAttachment, S>(
        data,
        queue,
        gvk_map,
        R::VolumeAttachment,
        "volumeattachments",
        "/apis/storage.k8s.io/v1/volumeattachments",
        retry,
        None,
    );
    // DynamicResourceAllocation is GA/on in 1.35 (`eventhandlers.go:638-668`).
    start_feed::<ResourceClaim, S>(
        data,
        queue,
        gvk_map,
        R::ResourceClaim,
        "resourceclaims",
        "/apis/resource.k8s.io/v1/resourceclaims",
        retry,
        Some(Box::new({
            let claims = claims.clone();
            move |claim: &ResourceClaim, deleted| {
                if deleted {
                    claims.remove(claim)
                } else {
                    claims.insert(claim.clone())
                }
            }
        })),
    );
    start_feed::<ResourceSlice, S>(
        data,
        queue,
        gvk_map,
        R::ResourceSlice,
        "resourceslices",
        "/apis/resource.k8s.io/v1/resourceslices",
        retry,
        None,
    );
    start_feed::<DeviceClass, S>(
        data,
        queue,
        gvk_map,
        R::DeviceClass,
        "deviceclasses",
        "/apis/resource.k8s.io/v1/deviceclasses",
        retry,
        None,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{
        priority_sort_less, ClusterEvent, PriorityQueueOptions, QueueingHint, QueueingHintFunction,
        QueueingHintMap,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pv(name: &str, rv: &str) -> PersistentVolume {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": name, "resourceVersion": rv},
            "spec": {}
        }))
        .unwrap()
    }

    /// A queue whose only plugin "P" registers `events` and counts the hint
    /// calls it receives.
    fn queue_with(events: &[ClusterEvent], count: &Arc<AtomicUsize>) -> Arc<PriorityQueue> {
        let mut m: QueueingHintMap = HashMap::new();
        for ev in events {
            let c = Arc::clone(count);
            m.entry(*ev).or_default().push(QueueingHintFunction {
                plugin_name: "P".to_string(),
                queueing_hint_fn: Arc::new(move |_, _, _| {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(QueueingHint::QueueSkip)
                }),
            });
        }
        PriorityQueue::new(
            priority_sort_less(),
            PriorityQueueOptions {
                queueing_hint_map: HashMap::from([(
                    crate::profile::DEFAULT_SCHEDULER_NAME.to_string(),
                    m,
                )]),
                ..Default::default()
            },
        )
    }

    /// Park one pod rejected by plugin "P" in unschedulablePods.
    async fn park_pod(q: &Arc<PriorityQueue>) {
        let pod: rusternetes_common::resources::Pod = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {}
        }))
        .unwrap();
        q.add(pod);
        let mut p_info = q.pop().await.unwrap();
        p_info.unschedulable_plugins = ["P".to_string()].into();
        q.add_unschedulable_if_not_present(p_info, q.scheduling_cycle())
            .unwrap();
    }

    #[test]
    fn unioned_gvks_ors_action_types_across_profiles() {
        let ev = ClusterEvent::new;
        let mk = |evs: &[ClusterEvent]| -> QueueingHintMap {
            evs.iter().map(|e| (*e, Vec::new())).collect()
        };
        let per_profile: QueueingHintMapPerProfile = HashMap::from([
            (
                "a".to_string(),
                mk(&[ev(EventResource::PersistentVolume, ActionType::ADD)]),
            ),
            (
                "b".to_string(),
                mk(&[
                    ev(EventResource::PersistentVolume, ActionType::UPDATE),
                    ev(EventResource::StorageClass, ActionType::ADD),
                ]),
            ),
        ]);
        let got = unioned_gvks(&per_profile);
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[&EventResource::PersistentVolume],
            ActionType::ADD | ActionType::UPDATE
        );
        assert_eq!(got[&EventResource::StorageClass], ActionType::ADD);
    }

    /// `buildEvtResHandler`: add on first sight, update (with oldObj) on a
    /// changed resourceVersion, nothing for an unchanged one, delete only for
    /// known objects.
    #[tokio::test]
    async fn handler_dispatches_add_update_delete() {
        let count = Arc::new(AtomicUsize::new(0));
        let pvr = EventResource::PersistentVolume;
        let all = [
            ClusterEvent::new(pvr, ActionType::ADD),
            ClusterEvent::new(pvr, ActionType::UPDATE),
            ClusterEvent::new(pvr, ActionType::DELETE),
        ];
        let q = queue_with(&all, &count);
        park_pod(&q).await;
        let mut h = ResourceEventHandler::<PersistentVolume>::new(
            Arc::clone(&q),
            pvr,
            ActionType::ADD | ActionType::UPDATE | ActionType::DELETE,
        );
        h.on_delete(pv("a", "1")); // unknown: no event
        assert_eq!(count.load(Ordering::SeqCst), 0);
        h.on_upsert(pv("a", "1")); // add: hint runs, QueueSkip keeps the pod parked
        assert_eq!(count.load(Ordering::SeqCst), 1);
        h.on_upsert(pv("a", "1")); // unchanged rv: no event
        assert_eq!(count.load(Ordering::SeqCst), 1);
        h.on_upsert(pv("a", "2")); // update
        assert_eq!(count.load(Ordering::SeqCst), 2);
        h.on_delete(pv("a", "2"));
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    /// Only the registered action types get a handler (`at&Add != 0`, ...).
    #[tokio::test]
    async fn handler_honours_registered_action_types() {
        let count = Arc::new(AtomicUsize::new(0));
        let pvr = EventResource::PersistentVolume;
        let q = queue_with(&[ClusterEvent::new(pvr, ActionType::ADD)], &count);
        park_pod(&q).await;
        let mut h =
            ResourceEventHandler::<PersistentVolume>::new(Arc::clone(&q), pvr, ActionType::ADD);
        h.on_upsert(pv("a", "1"));
        h.on_upsert(pv("a", "2"));
        h.on_delete(pv("a", "2"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// A relist replays deletes missed while the watch was down.
    #[tokio::test]
    async fn resync_replays_missed_deletes() {
        let count = Arc::new(AtomicUsize::new(0));
        let pvr = EventResource::PersistentVolume;
        let q = queue_with(&[ClusterEvent::new(pvr, ActionType::DELETE)], &count);
        park_pod(&q).await;
        let mut h =
            ResourceEventHandler::<PersistentVolume>::new(Arc::clone(&q), pvr, ActionType::DELETE);
        h.resync(vec![pv("a", "1"), pv("b", "1")]);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        h.resync(vec![pv("a", "1")]);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
