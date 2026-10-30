//! Unit tests moved from `src/latency_trackers.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::future::Future;
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::latency_trackers::*;

    /// `TestLatencyTrackersFrom` (webhook_duration_test.go:27-77).
    #[test]
    fn latency_trackers_sum_and_max() {
        let durations = [100u64, 200, 300, 200, 400, 300, 100];
        let wd = LatencyTrackers::new();
        assert_eq!(wd.excluded_latency(), Duration::ZERO);
        for d in durations {
            let d = Duration::from_nanos(d);
            wd.mutating_webhook.track_duration(d);
            wd.validating_webhook.track_duration(d);
            wd.apf_queue_wait.track_duration(d);
        }
        assert_eq!(
            wd.mutating_webhook.get_latency(),
            Duration::from_nanos(1600)
        );
        assert_eq!(
            wd.validating_webhook.get_latency(),
            Duration::from_nanos(400)
        );
        assert_eq!(wd.apf_queue_wait.get_latency(), Duration::from_nanos(400));
    }

    #[tokio::test]
    async fn context_lookup_and_apf_helper() {
        assert!(latency_trackers_from_context().is_none());
        // Without trackers the helper is a no-op (upstream `ok` check).
        track_apf_queue_wait_latency(Duration::from_millis(5));
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            assert!(latency_trackers_from_context().is_some());
            track_apf_queue_wait_latency(Duration::from_millis(5));
            track_apf_queue_wait_latency(Duration::from_millis(3));
        })
        .await;
        assert_eq!(t.apf_queue_wait.get_latency(), Duration::from_millis(5));
    }

    #[tokio::test]
    async fn sync_transform_and_serialize_helpers_sum_into_their_trackers() {
        // No trackers: just runs.
        assert_eq!(track_transform_response_object_latency_sync(|| 3), 3);
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            for _ in 0..2 {
                track_transform_response_object_latency_sync(|| {
                    std::thread::sleep(Duration::from_millis(10))
                });
                track_serialize_response_object_latency_sync(|| {
                    std::thread::sleep(Duration::from_millis(10))
                });
            }
        })
        .await;
        assert!(t.transform.get_latency() >= Duration::from_millis(20));
        assert!(t.serialization.get_latency() >= Duration::from_millis(20));
        assert_eq!(t.decode.get_latency(), Duration::ZERO);
    }

    #[tokio::test]
    async fn track_future_measures_elapsed() {
        let t = LatencyTrackers::new();
        t.mutating_webhook
            .track(tokio::time::sleep(Duration::from_millis(20)))
            .await;
        t.mutating_webhook
            .track(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        assert!(t.mutating_webhook.get_latency() >= Duration::from_millis(50));
    }

    /// `TestLatencyTrackersFrom` extended to the remaining trackers plus the
    /// annotation map (webhook_duration.go:289-345): zero trackers are
    /// omitted, sum/max aggregation per tracker, Go duration formatting.
    #[test]
    fn audit_annotations_cover_every_tracker() {
        let t = LatencyTrackers::new();
        assert!(annotations_from(&t).is_empty());
        for d in [Duration::from_millis(100), Duration::from_millis(250)] {
            t.authentication.track_duration(d);
            t.authorization.track_duration(d);
            t.storage.track_duration(d);
            t.transform.track_duration(d);
            t.serialization.track_duration(d);
            t.response_write.track_duration(d);
            t.decode.track_duration(d);
        }
        let m: std::collections::BTreeMap<_, _> = annotations_from(&t).into_iter().collect();
        assert_eq!(m.len(), 7);
        assert_eq!(m["apiserver.latency.k8s.io/authentication"], "350ms");
        assert_eq!(m["apiserver.latency.k8s.io/authorization"], "250ms");
        assert_eq!(m["apiserver.latency.k8s.io/etcd"], "350ms");
        assert_eq!(
            m["apiserver.latency.k8s.io/transform-response-object"],
            "350ms"
        );
        assert_eq!(
            m["apiserver.latency.k8s.io/serialize-response-object"],
            "350ms"
        );
        assert_eq!(m["apiserver.latency.k8s.io/response-write"], "350ms");
        assert_eq!(
            m["apiserver.latency.k8s.io/decode-response-object"],
            "350ms"
        );
    }

    #[tokio::test]
    async fn annotations_none_without_trackers_and_helpers_route() {
        assert!(audit_annotations_from_latency_trackers().is_none());
        let t = Arc::new(LatencyTrackers::new());
        with_latency_trackers(t.clone(), async {
            track_authentication_latency(Duration::from_millis(1));
            track_storage_latency(Duration::from_millis(2));
            track_decode_latency(Duration::from_millis(3));
            track_response_write_latency(Duration::from_millis(4));
            track_authorization_latency(Duration::from_millis(5));
            track_serialize_response_object_latency(tokio::time::sleep(Duration::from_millis(5)))
                .await;
            track_transform_response_object_latency(async {}).await;
            assert!(audit_annotations_from_latency_trackers().unwrap().len() >= 6);
        })
        .await;
        assert_eq!(t.storage.get_latency(), Duration::from_millis(2));
        assert!(t.serialization.get_latency() >= Duration::from_millis(5));
    }
}
