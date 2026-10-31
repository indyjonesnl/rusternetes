//! Unit tests moved from `src/resource_events.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use futures::StreamExt;
    pub use rusternetes_common::resources::{
        CSIDriver, CSINode, CSIStorageCapacity, DeviceClass, PersistentVolume,
        PersistentVolumeClaim, ResourceClaim, ResourceSlice, StorageClass, VolumeAttachment,
    };
    pub use rusternetes_common::types::ObjectMeta;
    pub use rusternetes_scheduler::data_plane::DataPlane;
    pub use rusternetes_scheduler::queue::dra_hints::ResourceClaimCache;
    pub use rusternetes_scheduler::queue::{
        ActionType, ClusterEvent, EventObject, EventResource, PriorityQueue,
        QueueingHintMapPerProfile,
    };
    pub use rusternetes_storage::{Storage, WatchEvent};
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use std::collections::{HashMap, HashSet};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::{
        priority_sort_less, ClusterEvent, PriorityQueueOptions, QueueingHint, QueueingHintFunction,
        QueueingHintMap,
    };
    use rusternetes_scheduler::resource_events::*;
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
                    rusternetes_scheduler::profile::DEFAULT_SCHEDULER_NAME.to_string(),
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

    /// The default profile must start feeds for every resource the
    /// NodeVolumeLimits hints register for (`nodevolumelimits/csi.go:90-109`),
    /// otherwise the hints are registered but can never fire (#3962).
    #[test]
    fn default_profile_unions_node_volume_limits_resources() {
        use rusternetes_scheduler::profile::Profile;
        use rusternetes_scheduler::queue::hints::build_queueing_hint_map_per_profile;
        let profile = Profile::named("default-scheduler");
        let map = build_queueing_hint_map_per_profile([&profile], &Default::default());
        let got = unioned_gvks(&map);
        assert!(got[&EventResource::CsiNode].contains(ActionType::ADD | ActionType::UPDATE));
        assert!(got[&EventResource::PersistentVolumeClaim].contains(ActionType::ADD));
        assert!(got[&EventResource::VolumeAttachment].contains(ActionType::DELETE));
        assert!(got[&EventResource::Pod].contains(ActionType::DELETE));
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
