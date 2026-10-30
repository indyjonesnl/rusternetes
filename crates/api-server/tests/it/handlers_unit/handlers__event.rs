//! Unit tests moved from `src/handlers/event.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::{Body, Bytes},
        extract::{OriginalUri, Path, Query, State},
        http::{header, HeaderMap, Uri},
        response::{IntoResponse, Response},
        Extension,
    };
    pub use rusternetes_api_server::endpoints::handlers::{self as endpoints, RequestScope};
    pub use rusternetes_api_server::registry::core::event;
    pub use rusternetes_api_server::{middleware::AuthContext, state::ApiServerState};
    pub use rusternetes_common::admission::{GroupVersionKind, GroupVersionResource};
    pub use rusternetes_common::dump::decode_request_body;
    pub use rusternetes_common::{
        authz::{Decision, RequestAttributes},
        resources::{Event, EventList, EventV1, EventV1List},
        Result,
    };
    pub use rusternetes_storage::build_prefix;
    pub use std::collections::HashMap;
    pub use std::sync::Arc;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::event::*;
    use rusternetes_common::resources::EventType;
    use rusternetes_common::resources::ObjectReference;

    #[test]
    fn test_event_creation() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "PodStarted".to_string(),
            "Pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.metadata.name, "test-event".to_string());
        assert_eq!(event.metadata.namespace, Some("default".to_string()));
        assert_eq!(event.reason, "PodStarted");
        assert_eq!(event.count, 1);
    }

    #[test]
    fn test_event_field_selector_filtering() {
        // Simulate what list_all does: filter events using apply_selectors
        let mut events = vec![
            Event::new(
                "event-1".to_string(),
                "default".to_string(),
                ObjectReference {
                    kind: Some("Pod".to_string()),
                    namespace: Some("default".to_string()),
                    name: Some("pod-a".to_string()),
                    uid: Some("uid1".to_string()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
                "Started".to_string(),
                "Pod started".to_string(),
                EventType::Normal,
            ),
            Event::new(
                "event-2".to_string(),
                "kube-system".to_string(),
                ObjectReference {
                    kind: Some("Pod".to_string()),
                    namespace: Some("kube-system".to_string()),
                    name: Some("pod-b".to_string()),
                    uid: Some("uid2".to_string()),
                    api_version: Some("v1".to_string()),
                    resource_version: None,
                    field_path: None,
                },
                "Failed".to_string(),
                "Pod failed".to_string(),
                EventType::Warning,
            ),
        ];

        // Filter by involvedObject.name
        let mut params = HashMap::new();
        params.insert(
            "fieldSelector".to_string(),
            "involvedObject.name=pod-a".to_string(),
        );
        rusternetes_api_server::handlers::filtering::apply_selectors(&mut events, &params).unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].metadata.name, "event-1");
        assert_eq!(events[0].involved_object.name, Some("pod-a".to_string()));
    }
}
