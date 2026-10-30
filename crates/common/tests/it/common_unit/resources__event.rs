//! Unit tests moved from `src/resources/event.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use rusternetes_common::resources::ObjectReference;
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::event::*;

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
            obj_ref.clone(),
            "PodStarted".to_string(),
            "Pod test-pod started successfully".to_string(),
            EventType::Normal,
        );

        assert_eq!(event.metadata.name, "test-event");
        assert_eq!(event.metadata.namespace, Some("default".to_string()));
        assert_eq!(event.reason, "PodStarted");
        assert_eq!(event.message, "Pod test-pod started successfully");
        assert_eq!(event.event_type, EventType::Normal);
        assert_eq!(event.count, 1);
        assert_eq!(event.involved_object.kind, Some("Pod".to_string()));
    }

    #[test]
    fn test_generate_event_name() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("test-pod".to_string()),
            uid: Some("abc123def456".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let name = Event::generate_name(&obj_ref, "PodStarted");
        assert!(name.starts_with("test-pod.podstarted.abc123de"));
    }

    #[test]
    fn test_event_serialization() {
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
            "Pod test-pod started successfully".to_string(),
            EventType::Normal,
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("PodStarted"));
        assert!(json.contains("Pod test-pod started successfully"));
        assert!(json.contains("Normal"));

        // Test deserialization
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.reason, event.reason);
        assert_eq!(deserialized.message, event.message);
        assert_eq!(deserialized.event_type, event.event_type);
    }

    #[test]
    fn test_event_list() {
        let list = EventList::default();
        assert_eq!(list.api_version, "v1");
        assert_eq!(list.kind, "EventList");
        assert_eq!(list.items.len(), 0);
    }

    #[test]
    fn test_event_field_selector_involved_object() {
        use rusternetes_common::field_selector::FieldSelector;

        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("my-pod".to_string()),
            uid: Some("uid-123".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };

        let event = Event::new(
            "test-event".to_string(),
            "default".to_string(),
            obj_ref,
            "Started".to_string(),
            "Pod started".to_string(),
            EventType::Normal,
        );

        let json_val = serde_json::to_value(&event).unwrap();

        // Field selector on involvedObject.name should work (camelCase serialization)
        let selector = FieldSelector::parse("involvedObject.name=my-pod").unwrap();
        assert!(
            selector.matches(&json_val),
            "involvedObject.name selector should match"
        );

        // Non-matching selector
        let selector = FieldSelector::parse("involvedObject.name=other-pod").unwrap();
        assert!(
            !selector.matches(&json_val),
            "Non-matching selector should not match"
        );

        // Field selector on metadata.namespace
        let selector = FieldSelector::parse("metadata.namespace=default").unwrap();
        assert!(
            selector.matches(&json_val),
            "metadata.namespace selector should match"
        );

        // Combined selectors
        let selector =
            FieldSelector::parse("involvedObject.name=my-pod,involvedObject.kind=Pod").unwrap();
        assert!(
            selector.matches(&json_val),
            "Combined selector should match"
        );
    }

    /// `Convert_v1_Event_To_core_Event` (`conversion.go:29-43`) maps the four
    /// `deprecated*` fields plus `note` and `regarding` onto the core names.
    /// Unconditional, as upstream is: the v1 wire type has no core field to
    /// lose (#1914, #1926).
    #[test]
    fn events_v1_into_core_maps_every_deprecated_field() {
        let body = serde_json::json!({
            "apiVersion": "events.k8s.io/v1",
            "kind": "Event",
            "metadata": { "name": "e" },
            "reason": "Probe",
            "type": "Normal",
            "note": "a note",
            "regarding": { "kind": "Pod", "name": "p", "namespace": "default" },
            "deprecatedSource": { "component": "probe", "host": "node-1" },
            "deprecatedFirstTimestamp": "2026-09-08T10:00:00Z",
            "deprecatedLastTimestamp": "2026-09-08T10:05:00Z",
            "deprecatedCount": 2,
        });
        let v1: EventV1 = serde_json::from_value(body).expect("decode");
        let core = v1.into_core();

        assert_eq!(core.count, 2);
        assert_eq!(core.source.component, "probe");
        assert_eq!(core.source.host.as_deref(), Some("node-1"));
        assert_eq!(
            core.first_timestamp.map(|t| t.to_rfc3339()),
            Some("2026-09-08T10:00:00+00:00".to_string())
        );
        assert_eq!(
            core.last_timestamp.map(|t| t.to_rfc3339()),
            Some("2026-09-08T10:05:00+00:00".to_string())
        );
        assert_eq!(core.message, "a note");
        assert_eq!(core.involved_object.name.as_deref(), Some("p"));
        // Stored as the core version, whatever the request version was.
        assert_eq!(core.api_version, "v1");
    }

    /// `Convert_core_Event_To_v1_Event` (`conversion.go:46-60`) is the inverse,
    /// and the response it produces carries the `events.k8s.io/v1` schema only:
    /// no `count`, `message`, `source` or `involvedObject`, which that schema
    /// does not have (#1926).
    #[test]
    fn core_to_events_v1_answers_with_the_v1_schema_only() {
        let obj_ref = ObjectReference {
            kind: Some("Pod".to_string()),
            namespace: Some("default".to_string()),
            name: Some("p".to_string()),
            uid: Some("abc".to_string()),
            api_version: Some("v1".to_string()),
            resource_version: None,
            field_path: None,
        };
        let mut core = Event::new(
            "e".to_string(),
            "default".to_string(),
            obj_ref,
            "Probe".to_string(),
            "a note".to_string(),
            EventType::Warning,
        );
        core.count = 3;
        core.source = EventSource {
            component: "probe".to_string(),
            host: Some("node-1".to_string()),
        };
        core.reporting_component = Some("kubernetes.io/kubelet".to_string());

        let v1 = core.to_events_v1();
        assert_eq!(v1.note, "a note");
        assert_eq!(v1.deprecated_count, 3);
        assert_eq!(v1.deprecated_source.component, "probe");
        assert_eq!(v1.regarding.name.as_deref(), Some("p"));
        assert_eq!(v1.event_type, EventType::Warning);
        assert_eq!(v1.reporting_controller, "kubernetes.io/kubelet");

        let wire = serde_json::to_value(&v1).expect("encode");
        for absent in [
            "count",
            "message",
            "source",
            "involvedObject",
            "firstTimestamp",
        ] {
            assert!(
                wire.get(absent).is_none(),
                "{absent} is not an events.k8s.io/v1 field: {wire}"
            );
        }
        // `omitempty` is a no-op on a struct upstream, so these are always sent.
        for present in ["regarding", "deprecatedSource", "eventTime"] {
            assert!(wire.get(present).is_some(), "{present} missing: {wire}");
        }
        assert_eq!(wire["apiVersion"], serde_json::json!("events.k8s.io/v1"));
    }

    /// Round trip: a v1 body that goes to storage as core and comes back out as
    /// v1 must be unchanged. This is the property the two-type split buys, and
    /// what the single-struct version could not provide.
    #[test]
    fn a_v1_event_round_trips_through_the_core_shape() {
        let body = serde_json::json!({
            "apiVersion": "events.k8s.io/v1",
            "kind": "Event",
            "metadata": { "name": "e", "namespace": "default" },
            "eventTime": "2026-09-08T10:00:00.000000Z",
            "reason": "Probe",
            "action": "Restart",
            "type": "Warning",
            "note": "a note",
            "reportingController": "kubernetes.io/kubelet",
            "reportingInstance": "kubelet-xyz",
            "regarding": { "kind": "Pod", "name": "p", "namespace": "default" },
            "deprecatedSource": { "component": "probe" },
            "deprecatedFirstTimestamp": "2026-09-08T10:00:00Z",
            "deprecatedLastTimestamp": "2026-09-08T10:05:00Z",
            "deprecatedCount": 2,
        });
        let sent: EventV1 = serde_json::from_value(body).expect("decode");
        let round_tripped = sent.clone().into_core().to_events_v1();
        assert_eq!(sent, round_tripped);
    }
}
