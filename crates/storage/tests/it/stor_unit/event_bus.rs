//! Unit tests moved from `src/event_bus.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::Error;
    pub use rusternetes_storage::{WatchEvent, WatchStream};
    pub use tokio::sync::broadcast;
}

mod tests {
    use super::__parent::*;
    use futures::StreamExt;
    use rusternetes_storage::event_bus::*;
    use rusternetes_storage::WatchEvent;

    #[tokio::test]
    async fn delivers_and_filters_by_prefix() {
        let bus = EventBus::new(16);
        let mut sub = bus.subscribe("/registry/pods/");
        bus.publish(WatchEvent::Added("/registry/pods/a".into(), "v".into()));
        bus.publish(WatchEvent::Added("/registry/services/x".into(), "v".into()));
        bus.publish(WatchEvent::Modified("/registry/pods/a".into(), "v2".into()));

        let e1 = sub.next().await.unwrap().unwrap();
        assert!(matches!(e1, WatchEvent::Added(ref k, _) if k == "/registry/pods/a"));
        let e2 = sub.next().await.unwrap().unwrap();
        assert!(matches!(e2, WatchEvent::Modified(ref k, _) if k == "/registry/pods/a"));
    }

    #[tokio::test]
    async fn lagged_yields_err() {
        let bus = EventBus::new(2);
        let mut sub = bus.subscribe("/registry/pods/");
        for i in 0..10 {
            bus.publish(WatchEvent::Added(format!("/registry/pods/{i}"), "v".into()));
        }
        let first = sub.next().await.unwrap();
        assert!(first.is_err(), "expected Lagged error, got {first:?}");
    }
}
