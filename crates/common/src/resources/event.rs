use crate::resources::ObjectReference;
use crate::types::ObjectMeta;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// The RFC3339-second (`metav1.Time`) and RFC3339Micro (`metav1.MicroTime`)
// serde modules used below live in `crate::types` — this file used to carry
// its own private copies. A second copy of a shared serialization rule is how
// #1895 happened: the duplicate drifted to chrono's nanosecond default while
// nobody was looking. One definition, used everywhere.

/// Event represents a single event in the system
///
/// Many fields are optional to tolerate varied event formats from conformance tests
/// and different Kubernetes client implementations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(default = "default_api_version")]
    pub api_version: String,

    #[serde(default = "default_kind")]
    pub kind: String,

    #[serde(default)]
    pub metadata: ObjectMeta,

    /// InvolvedObject is the object this event is about
    #[serde(default)]
    pub involved_object: ObjectReference,

    /// Reason is a short, machine understandable string that gives the reason for this event
    #[serde(default)]
    pub reason: String,

    /// Message is a human-readable description of the status of this operation
    #[serde(default)]
    pub message: String,

    /// Source component generating the event
    #[serde(default)]
    pub source: EventSource,

    /// Type of this event (Normal, Warning), new types could be added in the future
    #[serde(rename = "type", default)]
    pub event_type: EventType,

    /// The time at which the event was first recorded
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub first_timestamp: Option<DateTime<Utc>>,

    /// The time at which the most recent occurrence of this event was recorded
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub last_timestamp: Option<DateTime<Utc>>,

    /// The number of times this event has occurred
    #[serde(default)]
    pub count: i32,

    /// Optional action that was taken/failed regarding the Regarding object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,

    /// Optional related object (e.g., Node for Pod event)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related: Option<ObjectReference>,

    /// Event series data (for aggregated events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<EventSeries>,

    /// Time when this Event was first observed (MicroTime precision)
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::types::k8s_micro_time::serialize",
        deserialize_with = "crate::types::k8s_micro_time::deserialize"
    )]
    pub event_time: Option<DateTime<Utc>>,

    /// Name of the controller that emitted this Event (e.g., "kubernetes.io/kubelet")
    /// K8s events.k8s.io/v1 calls this "reportingController"
    #[serde(
        rename = "reportingController",
        skip_serializing_if = "Option::is_none",
        alias = "reportingComponent"
    )]
    pub reporting_component: Option<String>,

    /// ID of the controller instance (e.g., "kubelet-xyzf")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reporting_instance: Option<String>,

    /// Note is a human-readable description (used by events.k8s.io/v1 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,

    /// Regarding contains the object this Event is about (events.k8s.io/v1 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regarding: Option<ObjectReference>,

    // -----------------------------------------------------------------------
    // events.k8s.io/v1 `deprecated*` aliases.
    //
    // The new API version carries the four core Event fields below under
    // `deprecated*` names and converts them onto the internal object
    // (`Convert_v1_Event_To_core_Event`, pkg/apis/events/v1/conversion.go:29-43).
    // Rusternetes models both versions with this one struct, so the aliases
    // need somewhere to land before `convert_from_events_v1` moves them onto
    // the core fields. Without them they fell into `extra` and every strict
    // "needs to be unset" rule read a zero value (#1914).
    //
    // They are never serialized: the conversion drains them, so a stored
    // Event only ever holds the core shape.
    // -----------------------------------------------------------------------
    /// events.k8s.io/v1 alias for `source`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_source: Option<EventSource>,

    /// events.k8s.io/v1 alias for `firstTimestamp`.
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub deprecated_first_timestamp: Option<DateTime<Utc>>,

    /// events.k8s.io/v1 alias for `lastTimestamp`.
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub deprecated_last_timestamp: Option<DateTime<Utc>>,

    /// events.k8s.io/v1 alias for `count`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecated_count: Option<i32>,

    /// Catch-all for any additional fields not explicitly defined
    #[serde(flatten)]
    pub extra: Option<std::collections::HashMap<String, serde_json::Value>>,
}

impl Event {
    /// Create a new Event
    pub fn new(
        name: String,
        namespace: String,
        involved_object: ObjectReference,
        reason: String,
        message: String,
        event_type: EventType,
    ) -> Self {
        let now = Utc::now();
        Self {
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            metadata: ObjectMeta::new(name).with_namespace(namespace),
            involved_object,
            reason,
            message,
            source: EventSource {
                component: "rusternetes".to_string(),
                host: None,
            },
            event_type,
            first_timestamp: Some(now),
            last_timestamp: Some(now),
            count: 1,
            action: None,
            related: None,
            series: None,
            event_time: None,
            reporting_component: None,
            reporting_instance: None,
            note: None,
            regarding: None,
            deprecated_source: None,
            deprecated_first_timestamp: None,
            deprecated_last_timestamp: None,
            deprecated_count: None,
            extra: None,
        }
    }

    /// Convert an `events.k8s.io/v1` request body onto the core field names.
    ///
    /// Port of `Convert_v1_Event_To_core_Event`
    /// (`../kubernetes/pkg/apis/events/v1/conversion.go:29-43`):
    ///
    /// ```text
    /// Convert_v1_EventSource_To_core_EventSource(&in.DeprecatedSource, &out.Source, s)
    /// out.InvolvedObject  = in.Regarding
    /// out.Message         = in.Note
    /// out.FirstTimestamp  = in.DeprecatedFirstTimestamp
    /// out.LastTimestamp   = in.DeprecatedLastTimestamp
    /// out.Count           = in.DeprecatedCount
    /// ```
    ///
    /// Upstream assigns unconditionally because the two versions are separate
    /// Go types: an `events.k8s.io/v1` body simply has no `count`, `message`,
    /// `source` or `involvedObject` field to lose. Here one struct serves both
    /// versions and reads of `events.k8s.io/v1` still answer with the core
    /// names, so a read-modify-write client legitimately sends those back.
    /// Each alias is therefore applied only when the client actually set it,
    /// which yields upstream's result for every body upstream can express.
    /// (Converting the *response* too is tracked in #1926.)
    ///
    /// Runs before validation: the strict branch of `ValidateEventCreate`
    /// requires `source`, `firstTimestamp`, `lastTimestamp` and `count` to be
    /// unset, and can only see them once the aliases have been moved (#1914).
    pub fn convert_from_events_v1(&mut self) {
        if let Some(source) = self.deprecated_source.take() {
            self.source = source;
        }
        if let Some(first) = self.deprecated_first_timestamp.take() {
            self.first_timestamp = Some(first);
        }
        if let Some(last) = self.deprecated_last_timestamp.take() {
            self.last_timestamp = Some(last);
        }
        if let Some(count) = self.deprecated_count.take() {
            self.count = count;
        }
        if self.message.is_empty() {
            if let Some(note) = &self.note {
                self.message = note.clone();
            }
        }
        if self
            .involved_object
            .name
            .as_deref()
            .unwrap_or("")
            .is_empty()
        {
            if let Some(regarding) = &self.regarding {
                self.involved_object = regarding.clone();
            }
        }
    }

    /// Generate event name based on involved object and reason
    pub fn generate_name(involved_object: &ObjectReference, reason: &str) -> String {
        let obj_name = involved_object.name.as_deref().unwrap_or("unknown");
        let uid = involved_object.uid.as_deref().unwrap_or("unknown");
        // Use a stable name (no timestamp) so duplicate events can be detected
        // and deduplicated. The event's first/last timestamp fields track timing.
        format!(
            "{}.{}.{}",
            obj_name,
            reason.to_lowercase(),
            uid[..8.min(uid.len())].to_lowercase()
        )
    }
}

/// EventSource contains information for an event
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventSource {
    /// Component from which the event is generated
    #[serde(default)]
    pub component: String,

    /// Node name on which the event is generated (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

/// EventType is the type of an event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum EventType {
    #[default]
    Normal,
    Warning,
}

/// EventSeries contains information on series of events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventSeries {
    /// Number of occurrences in this series up to the last heartbeat time
    pub count: i32,

    /// Time of the last occurrence observed (MicroTime format for K8s compatibility)
    #[serde(
        serialize_with = "crate::types::k8s_micro_time_required::serialize",
        deserialize_with = "crate::types::k8s_micro_time_required::deserialize"
    )]
    pub last_observed_time: DateTime<Utc>,
}

/// EventList is a list of events
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventList {
    #[serde(default = "default_api_version")]
    pub api_version: String,

    #[serde(default = "default_kind_list")]
    pub kind: String,

    pub metadata: crate::types::ListMeta,

    pub items: Vec<Event>,
}

impl Default for EventList {
    fn default() -> Self {
        Self {
            api_version: "v1".to_string(),
            kind: "EventList".to_string(),
            metadata: crate::types::ListMeta::default(),
            items: Vec::new(),
        }
    }
}

fn default_api_version() -> String {
    "v1".to_string()
}

fn default_kind() -> String {
    "Event".to_string()
}

fn default_kind_list() -> String {
    "EventList".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        use crate::field_selector::FieldSelector;

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

    /// `Convert_v1_Event_To_core_Event` moves the four `deprecated*` aliases
    /// onto the core fields and leaves nothing behind, so the stored object
    /// only ever holds the core shape (#1914).
    #[test]
    fn convert_from_events_v1_drains_the_deprecated_aliases() {
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
        let mut event: Event = serde_json::from_value(body).expect("decode");

        // Pre-condition: the aliases decode into their own fields, not `extra`,
        // and the core fields are still unset.
        assert_eq!(event.deprecated_count, Some(2));
        assert_eq!(event.count, 0);

        event.convert_from_events_v1();

        assert_eq!(event.count, 2);
        assert_eq!(event.source.component, "probe");
        assert_eq!(event.source.host.as_deref(), Some("node-1"));
        assert_eq!(
            event.first_timestamp.map(|t| t.to_rfc3339()),
            Some("2026-09-08T10:00:00+00:00".to_string())
        );
        assert_eq!(
            event.last_timestamp.map(|t| t.to_rfc3339()),
            Some("2026-09-08T10:05:00+00:00".to_string())
        );
        assert_eq!(event.message, "a note");
        assert_eq!(event.involved_object.name.as_deref(), Some("p"));

        assert!(event.deprecated_count.is_none());
        assert!(event.deprecated_source.is_none());
        assert!(event.deprecated_first_timestamp.is_none());
        assert!(event.deprecated_last_timestamp.is_none());

        // Nothing deprecated survives serialization either.
        let round_tripped = serde_json::to_value(&event).expect("encode");
        for key in [
            "deprecatedCount",
            "deprecatedSource",
            "deprecatedFirstTimestamp",
            "deprecatedLastTimestamp",
        ] {
            assert!(
                round_tripped.get(key).is_none(),
                "{key} survived: {round_tripped}"
            );
        }
    }

    /// Each alias is applied only when the client set it: reads of
    /// `events.k8s.io/v1` still answer with the core names, so a
    /// read-modify-write PUT carries `count`/`message` and no alias, and must
    /// not have them cleared.
    #[test]
    fn convert_from_events_v1_leaves_core_fields_alone_when_no_alias_is_set() {
        let body = serde_json::json!({
            "apiVersion": "events.k8s.io/v1",
            "kind": "Event",
            "metadata": { "name": "e" },
            "involvedObject": { "kind": "Pod", "name": "p", "namespace": "default" },
            "message": "kept",
            "source": { "component": "kubelet" },
            "count": 7,
            "firstTimestamp": "2026-09-08T10:00:00Z",
        });
        let mut event: Event = serde_json::from_value(body).expect("decode");
        event.convert_from_events_v1();

        assert_eq!(event.count, 7);
        assert_eq!(event.message, "kept");
        assert_eq!(event.source.component, "kubelet");
        assert!(event.first_timestamp.is_some());
    }
}
