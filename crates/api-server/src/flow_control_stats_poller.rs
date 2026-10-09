//! API Priority and Fairness: the producer of the object-count tracker.
//!
//! Port of `registry/generic/registry/store.go` `Store.startObservingCount`
//! (release-1.35, :1663-1682): every `CountMetricPollPeriod` (1m,
//! `server/options/etcd.go:87`, jitter 1.2 = `resourceCountPollPeriodJitter`,
//! store.go:263) it calls `Storage.Stats` for the resource and, on success,
//! `objectCountTracker.Set(resourceName, stats)`.

use std::sync::Arc;
use std::time::Duration;

use rusternetes_storage::Storage;

use crate::flow_control_object_count::ObjectCountTracker;

/// One poll of every observed resource.
pub async fn poll_once<S: Storage>(_storage: &S, _tracker: &ObjectCountTracker) {}

/// Poll until `stop` fires.
pub async fn run<S: Storage>(
    _storage: Arc<S>,
    _tracker: Arc<ObjectCountTracker>,
    _period: Duration,
    _stop: tokio::sync::watch::Receiver<bool>,
) {
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow_control_work_estimator::StatsError;
    use rusternetes_storage::memory::MemoryStorage;

    async fn seed(s: &MemoryStorage, key: &str) {
        let v = serde_json::json!({"metadata": {"name": "x"}});
        s.create(key, &v).await.unwrap();
    }

    #[tokio::test]
    async fn poll_sets_count_and_size_under_the_group_resource_name() {
        let s = MemoryStorage::new();
        seed(&s, "/registry/pods/ns/a").await;
        seed(&s, "/registry/pods/ns/b").await;
        seed(&s, "/registry/deployments/ns/d").await;
        let t = ObjectCountTracker::new();
        poll_once(&s, &t).await;
        let (st, err) = t.get("pods");
        assert_eq!(err, None);
        assert_eq!(st.object_count, 2);
        assert!(st.estimated_average_object_size_bytes > 0);
        // Group-qualified like `schema.GroupResource.String()`.
        let (st, err) = t.get("deployments.apps");
        assert_eq!(err, None);
        assert_eq!(st.object_count, 1);
    }

    #[tokio::test]
    async fn an_empty_resource_is_recorded_as_zero_objects() {
        let s = MemoryStorage::new();
        let t = ObjectCountTracker::new();
        poll_once(&s, &t).await;
        let (st, err) = t.get("pods");
        assert_eq!(err, None, "polled and empty is not NotFound");
        assert_eq!(st.object_count, 0);
        assert_eq!(
            t.get("nonexistent.example.com").1,
            Some(StatsError::NotFound)
        );
    }

    #[tokio::test]
    async fn run_polls_immediately_then_stops_on_signal() {
        let s = Arc::new(MemoryStorage::new());
        seed(&s, "/registry/pods/ns/a").await;
        let t = Arc::new(ObjectCountTracker::new());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let h = tokio::spawn(run(s, t.clone(), Duration::from_secs(3600), rx));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(t.get("pods").0.object_count, 1);
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("poller exits on stop")
            .unwrap();
    }
}
