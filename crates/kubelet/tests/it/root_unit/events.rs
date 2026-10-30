//! Unit tests moved from `src/events.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{EventType, ObjectReference, Pod};
    pub use rusternetes_storage::{EventRecorder, Storage};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::{Event, PodSpec};
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_kubelet::events::*;
    use rusternetes_storage::{Storage, StorageBackend};
    use std::sync::Arc;

    fn make_pod(name: &str, namespace: &str, node: Option<&str>) -> Pod {
        Pod {
            type_meta: TypeMeta {
                kind: "Pod".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: {
                let mut m = ObjectMeta::new(name).with_namespace(namespace);
                m.uid = format!("{}-uid-abcdef01", name);
                m
            },
            spec: Some(PodSpec {
                node_name: node.map(|s| s.to_string()),
                ..Default::default()
            }),
            status: None,
        }
    }

    #[test]
    fn container_field_path_matches_upstream_form() {
        assert_eq!(container_field_path("web"), "spec.containers{web}");
    }

    #[test]
    fn container_reference_carries_field_path_and_uid() {
        let pod = make_pod("web", "default", Some("node-1"));
        let r = container_object_reference(&pod, "app");
        assert_eq!(r.field_path.as_deref(), Some("spec.containers{app}"));
        assert_eq!(r.kind.as_deref(), Some("Pod"));
        assert_eq!(r.name.as_deref(), Some("web"));
        assert_eq!(r.uid.as_deref(), Some("web-uid-abcdef01"));
    }

    #[tokio::test]
    async fn emit_writes_container_event_with_kubelet_source_and_field_path() {
        let storage = Arc::new(StorageBackend::new_memory());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let pod = make_pod("web", "default", Some("node-7"));

        emit_lifecycle_event(
            &recorder,
            &pod,
            Some("app"),
            STARTED_CONTAINER,
            EventType::Normal,
            "Started container app",
        )
        .await
        .unwrap();

        let obj = container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, STARTED_CONTAINER)
        );
        let ev: Event = storage.get(&key).await.expect("event should be recorded");
        assert_eq!(ev.reason, "Started");
        assert_eq!(ev.source.component, "kubelet");
        assert_eq!(ev.source.host.as_deref(), Some("node-7"));
        assert_eq!(
            ev.involved_object.field_path.as_deref(),
            Some("spec.containers{app}")
        );
    }

    #[tokio::test]
    async fn emit_failed_pull_is_a_warning() {
        let storage = Arc::new(StorageBackend::new_memory());
        let recorder = EventRecorder::new(Arc::clone(&storage));
        let pod = make_pod("web", "default", None);

        emit_lifecycle_event(
            &recorder,
            &pod,
            Some("app"),
            FAILED_TO_PULL_IMAGE,
            EventType::Warning,
            "Failed to pull image \"nope:latest\"",
        )
        .await
        .unwrap();

        let obj = container_object_reference(&pod, "app");
        let key = format!(
            "/registry/events/default/{}",
            Event::generate_name(&obj, FAILED_TO_PULL_IMAGE)
        );
        let ev: Event = storage.get(&key).await.unwrap();
        assert_eq!(ev.event_type, EventType::Warning);
        assert!(ev.source.host.is_none(), "no node → no host");
    }
}
