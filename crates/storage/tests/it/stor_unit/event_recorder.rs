//! Unit tests moved from `src/event_recorder.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::event_correlator::{
        CorrelatorEvent, CorrelatorEventSource, CorrelatorEventType, CorrelatorObjectReference,
        EventCorrelator, RealClock,
    };
    pub use rusternetes_common::resources::{
        Event, EventSeries, EventSource, EventType, ObjectReference,
    };
    pub use rusternetes_common::Result;
    pub use rusternetes_storage::Storage;
    pub use std::sync::atomic::{AtomicU64, Ordering};
    pub use std::sync::{Arc, Mutex};
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::event_recorder::*;
    use rusternetes_storage::MemoryStorage;

    fn obj_ref(name: &str, namespace: &str) -> ObjectReference {
        ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some(namespace.to_string()),
            name: Some(name.to_string()),
            uid: Some(format!("{}-uid-0000", name)),
            api_version: Some("v1".to_string()),
            ..Default::default()
        }
    }

    fn source() -> EventSource {
        EventSource {
            component: "kubelet".to_string(),
            host: Some("node-1".to_string()),
        }
    }

    async fn stored(storage: &MemoryStorage, ns: &str, name: &str, reason: &str) -> Option<Event> {
        let obj = obj_ref(name, ns);
        let event_name = Event::generate_name(&obj, reason);
        let key = format!("/registry/events/{}/{}", ns, event_name);
        storage.get::<Event>(&key).await.ok()
    }

    #[tokio::test]
    async fn recorded_events_expire_after_the_event_ttl() {
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage)).with_event_ttl(1);
        let involved = obj_ref("web", "default");
        recorder
            .event(&involved, &source(), EventType::Normal, "Started", "m")
            .await
            .unwrap();
        assert!(stored(&storage, "default", "web", "Started")
            .await
            .is_some());
        // A recurrence rewrites the object and must re-arm the lease.
        recorder
            .event(&involved, &source(), EventType::Normal, "Started", "m2")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert!(
            stored(&storage, "default", "web", "Started")
                .await
                .is_none(),
            "in-process events must carry the event TTL like API-written ones"
        );
    }

    #[tokio::test]
    async fn new_recorders_follow_the_process_event_ttl() {
        // Mutates the process-wide TTL only for the instant between set and
        // `new()` (restored immediately); no other test here waits on expiry.
        set_process_event_ttl(1);
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        set_process_event_ttl(DEFAULT_EVENT_TTL_SECONDS);
        let involved = obj_ref("ttl-web", "default");
        recorder
            .event(&involved, &source(), EventType::Normal, "Started", "m")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert!(
            stored(&storage, "default", "ttl-web", "Started")
                .await
                .is_none(),
            "a recorder built by EventRecorder::new must use the configured --event-ttl"
        );
    }

    #[tokio::test]
    async fn first_emission_creates_event_with_source() {
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));

        recorder
            .event(
                &obj_ref("web", "default"),
                &source(),
                EventType::Normal,
                "Started",
                "Started container web",
            )
            .await
            .unwrap();

        let ev = stored(&storage, "default", "web", "Started")
            .await
            .expect("event should be created");
        assert_eq!(ev.reason, "Started");
        assert_eq!(ev.message, "Started container web");
        assert_eq!(ev.count, 1);
        assert_eq!(ev.source.component, "kubelet");
        assert_eq!(ev.source.host.as_deref(), Some("node-1"));
    }

    #[tokio::test]
    async fn recurrence_bumps_count_and_series() {
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let obj = obj_ref("web", "default");

        recorder
            .event(&obj, &source(), EventType::Normal, "Pulled", "pulled v1")
            .await
            .unwrap();
        recorder
            .event(&obj, &source(), EventType::Normal, "Pulled", "pulled v2")
            .await
            .unwrap();

        let ev = stored(&storage, "default", "web", "Pulled").await.unwrap();
        assert_eq!(ev.count, 2, "second occurrence bumps count");
        assert_eq!(ev.message, "pulled v2", "message advances to latest");
        let series = ev.series.expect("series should track count");
        assert_eq!(series.count, 2);
    }

    #[tokio::test]
    async fn spam_filter_caps_a_flood_at_the_burst_size() {
        // 40 identical emissions in a tight loop. The spam key excludes reason
        // and message, so all share one token bucket: burst is 25, refill is
        // 1/300s (negligible in <1s), so exactly the first 25 are accepted and
        // the rest are dropped. Since all 25 share one (object, reason) name,
        // they collapse onto a single Event whose count equals the burst.
        let storage = Arc::new(MemoryStorage::new());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let obj = obj_ref("noisy", "default");

        for i in 0..40 {
            recorder
                .event(
                    &obj,
                    &source(),
                    EventType::Warning,
                    "BackOff",
                    &format!("back-off restarting #{i}"),
                )
                .await
                .unwrap();
        }

        let ev = stored(&storage, "default", "noisy", "BackOff")
            .await
            .unwrap();
        assert_eq!(
            ev.count, 25,
            "spam filter must cap the flood at burst=25, got {}",
            ev.count
        );
    }
}
