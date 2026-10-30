//! Central metadata stamping for resource creation.
//!
//! Mirrors how upstream Kubernetes stamps system-managed `ObjectMeta` fields in
//! the generic registry store (`registry.Store.Create` -> `rest.BeforeCreate`),
//! where `metadata.uid`, `metadata.creationTimestamp` and `metadata.generation`
//! are assigned for *every* resource — no individual handler can forget.
//!
//! In rusternetes the equivalent chokepoint is `Storage::create`. Stamping here
//! (rather than ad-hoc in each HTTP handler) guarantees that resources written
//! directly to storage — e.g. the scheduler POSTing a raw `default-scheduler`
//! Event, or bootstrap namespaces — still receive a `creationTimestamp`, so
//! clients like Lens can compute an age instead of showing `<unknown>`.

use serde_json::Value;

/// Ensure the system-managed creation metadata fields are present on a resource
/// JSON value, without overwriting any value the caller already supplied.
///
/// Sets, only when missing/null/empty:
/// - `metadata` object itself (created if absent)
/// - `metadata.uid` — a fresh v4 UUID
/// - `metadata.creationTimestamp` — RFC3339, seconds precision (matches the wire
///   format of Go's `metav1.Time`, which truncates sub-second digits)
/// - `metadata.generation` — `1`
///
/// Idempotent: an existing non-empty `uid`, a present `creationTimestamp`
/// (including an explicit `null` left by the caller is *replaced*, matching
/// Go where a zero `creationTimestamp` is filled in), and an existing
/// `generation` are preserved.
pub fn ensure_create_metadata(value: &mut Value) {
    // Ensure a metadata object exists.
    if value.get("metadata").is_none() || value.get("metadata").is_some_and(|m| m.is_null()) {
        value["metadata"] = serde_json::json!({});
    }
    let Some(metadata) = value.get_mut("metadata").and_then(|m| m.as_object_mut()) else {
        return;
    };

    // uid: generate unless a non-empty string is already present.
    let has_uid = matches!(metadata.get("uid"), Some(Value::String(s)) if !s.is_empty());
    if !has_uid {
        metadata.insert(
            "uid".to_string(),
            Value::String(uuid::Uuid::new_v4().to_string()),
        );
    }

    // creationTimestamp: stamp unless already set to a real (non-null) value.
    if metadata.get("creationTimestamp").is_none()
        || metadata.get("creationTimestamp") == Some(&Value::Null)
    {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        metadata.insert("creationTimestamp".to_string(), Value::String(now));
    }

    // generation: default to 1 on creation.
    if metadata.get("generation").is_none_or(|g| g.is_null()) {
        metadata.insert("generation".to_string(), serde_json::json!(1));
    }
}
