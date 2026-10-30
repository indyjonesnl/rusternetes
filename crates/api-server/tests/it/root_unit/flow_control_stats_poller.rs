//! Unit tests moved from `src/flow_control_stats_poller.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::flow_control_object_count::ObjectCountTracker;
    pub use rusternetes_api_server::flow_control_work_estimator::Stats;
    pub use rusternetes_storage::Storage;
    pub use std::sync::Arc;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::flow_control_stats_poller::*;
    use rusternetes_api_server::flow_control_work_estimator::StatsError;
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

    /// Upstream starts an observer per registered store, CRD stores included
    /// (`customresource_handler.go` builds a `genericregistry.Store` per CRD
    /// version), so a custom resource's list is costed by its object count.
    #[tokio::test]
    async fn a_registered_crd_is_observed_under_plural_dot_group() {
        let s = MemoryStorage::new();
        let crd = serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "scope": "Namespaced",
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }
        });
        s.create(
            "/registry/customresourcedefinitions/widgets.example.com",
            &crd,
        )
        .await
        .unwrap();
        let cr = serde_json::json!({"metadata": {"name": "w1", "namespace": "ns"}});
        s.create("/registry/example_com_widgets/ns/w1", &cr)
            .await
            .unwrap();
        let t = ObjectCountTracker::new();
        poll_once(&s, &t).await;
        let (st, err) = t.get("widgets.example.com");
        assert_eq!(err, None, "CRD resources are polled, not NotFound");
        assert_eq!(st.object_count, 1);
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
