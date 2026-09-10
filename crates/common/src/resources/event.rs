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
            extra: None,
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

// ---------------------------------------------------------------------------
// events.k8s.io/v1
// ---------------------------------------------------------------------------

/// `events.k8s.io/v1` Event — the wire type, distinct from the core one.
///
/// Port of `staging/src/k8s.io/api/events/v1/types.go:34-100`. Upstream models
/// the two API versions as two Go types and converts between them
/// (`pkg/apis/events/v1/conversion.go:29-60`); this is that second type, so a
/// `events.k8s.io/v1` response carries the `events.k8s.io/v1` schema and
/// nothing else. Before it existed the handlers answered with the stored core
/// shape and only rewrote `apiVersion`, so the body carried `count`,
/// `message`, `source` and `involvedObject` — fields this schema does not have
/// — and omitted every `deprecated*` field (#1926).
///
/// Field presence follows upstream's json tags: `encoding/json`'s `omitempty`
/// is a no-op on a struct, so `regarding` and `deprecatedSource` are always
/// emitted, as are the two `metav1.Time` fields (`null` when unset, from
/// `Time.MarshalJSON`). `deprecatedCount` is an `int32` with `omitempty`, so it
/// is omitted at zero.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EventV1 {
    #[serde(default = "default_events_v1_api_version")]
    pub api_version: String,

    #[serde(default = "default_kind")]
    pub kind: String,

    #[serde(default)]
    pub metadata: ObjectMeta,

    /// `eventTime` is required and carries no `omitempty`.
    #[serde(
        default,
        serialize_with = "crate::types::k8s_micro_time::serialize",
        deserialize_with = "crate::types::k8s_micro_time::deserialize"
    )]
    pub event_time: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<EventSeries>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reporting_controller: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reporting_instance: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,

    #[serde(default)]
    pub regarding: ObjectReference,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub related: Option<ObjectReference>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,

    /// Typed like the core field rather than upstream's bare `string`: the
    /// core endpoint already rejects an unknown value at decode time, and
    /// coercing one to `Normal` here would silently accept it instead.
    #[serde(rename = "type", default)]
    pub event_type: EventType,

    #[serde(default)]
    pub deprecated_source: EventSource,

    #[serde(
        default,
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub deprecated_first_timestamp: Option<DateTime<Utc>>,

    #[serde(
        default,
        serialize_with = "crate::types::k8s_time::serialize",
        deserialize_with = "crate::types::k8s_time::deserialize"
    )]
    pub deprecated_last_timestamp: Option<DateTime<Utc>>,

    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub deprecated_count: i32,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

fn default_events_v1_api_version() -> String {
    "events.k8s.io/v1".to_string()
}

impl EventV1 {
    /// Convert an `events.k8s.io/v1` body onto the core Event.
    ///
    /// Port of `Convert_v1_Event_To_core_Event`
    /// (`pkg/apis/events/v1/conversion.go:29-43`):
    ///
    /// ```text
    /// Convert_v1_ObjectReference_To_core_ObjectReference(&in.Regarding, &out.InvolvedObject, s)
    /// Convert_v1_EventSource_To_core_EventSource(&in.DeprecatedSource, &out.Source, s)
    /// out.Message        = in.Note
    /// out.FirstTimestamp = in.DeprecatedFirstTimestamp
    /// out.LastTimestamp  = in.DeprecatedLastTimestamp
    /// out.Count          = in.DeprecatedCount
    /// ```
    ///
    /// Unconditional, as upstream is: the v1 body has no `count`, `message`,
    /// `source` or `involvedObject` field to lose. The earlier
    /// `Event::convert_from_events_v1` had to guard each assignment because
    /// reads still answered with the core names, so a read-modify-write client
    /// legitimately sent them back; with responses converted too (this type is
    /// what the handlers now answer with) that carve-out is gone.
    pub fn into_core(self) -> Event {
        Event {
            // The stored object is the core one.
            api_version: "v1".to_string(),
            kind: self.kind,
            metadata: self.metadata,
            involved_object: self.regarding.clone(),
            reason: self.reason,
            message: self.note.clone(),
            source: self.deprecated_source,
            event_type: self.event_type,
            first_timestamp: self.deprecated_first_timestamp,
            last_timestamp: self.deprecated_last_timestamp,
            count: self.deprecated_count,
            action: if self.action.is_empty() {
                None
            } else {
                Some(self.action)
            },
            related: self.related,
            series: self.series,
            event_time: self.event_time,
            reporting_component: if self.reporting_controller.is_empty() {
                None
            } else {
                Some(self.reporting_controller)
            },
            reporting_instance: if self.reporting_instance.is_empty() {
                None
            } else {
                Some(self.reporting_instance)
            },
            // `note` and `regarding` have no core counterpart; they are kept on
            // the core struct only so the fields the v1 wire type owns survive
            // a round trip through storage byte-for-byte.
            note: if self.note.is_empty() {
                None
            } else {
                Some(self.note)
            },
            regarding: Some(self.regarding),
            extra: None,
        }
    }
}

impl Event {
    /// Convert the stored core Event into the `events.k8s.io/v1` wire type.
    ///
    /// Port of `Convert_core_Event_To_v1_Event`
    /// (`pkg/apis/events/v1/conversion.go:46-60`):
    ///
    /// ```text
    /// Convert_core_ObjectReference_To_v1_ObjectReference(&in.InvolvedObject, &out.Regarding, s)
    /// Convert_core_EventSource_To_v1_EventSource(&in.Source, &out.DeprecatedSource, s)
    /// out.Note                     = in.Message
    /// out.DeprecatedFirstTimestamp = in.FirstTimestamp
    /// out.DeprecatedLastTimestamp  = in.LastTimestamp
    /// out.DeprecatedCount          = in.Count
    /// ```
    pub fn to_events_v1(&self) -> EventV1 {
        EventV1 {
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            metadata: self.metadata.clone(),
            event_time: self.event_time,
            series: self.series.clone(),
            reporting_controller: self.reporting_component.clone().unwrap_or_default(),
            reporting_instance: self.reporting_instance.clone().unwrap_or_default(),
            action: self.action.clone().unwrap_or_default(),
            reason: self.reason.clone(),
            regarding: self.involved_object.clone(),
            related: self.related.clone(),
            note: self.message.clone(),
            event_type: self.event_type.clone(),
            deprecated_source: self.source.clone(),
            deprecated_first_timestamp: self.first_timestamp,
            deprecated_last_timestamp: self.last_timestamp,
            deprecated_count: self.count,
        }
    }
}

/// EventList in the `events.k8s.io/v1` schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventV1List {
    #[serde(default = "default_events_v1_api_version")]
    pub api_version: String,

    #[serde(default = "default_kind_list")]
    pub kind: String,

    pub metadata: crate::types::ListMeta,

    pub items: Vec<EventV1>,
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
