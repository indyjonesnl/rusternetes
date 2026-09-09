//! `events.k8s.io/v1` carries the four core Event fields under `deprecated*`
//! names, and upstream converts them onto the internal object *before*
//! validation runs. Port of `Convert_v1_Event_To_core_Event`
//! (`pkg/apis/events/v1/conversion.go:29-43`):
//!
//! ```go
//! if err := k8s_api_v1.Convert_v1_EventSource_To_core_EventSource(&in.DeprecatedSource, &out.Source, s); err != nil {
//!     return err
//! }
//! out.Message = in.Note
//! out.FirstTimestamp = in.DeprecatedFirstTimestamp
//! out.LastTimestamp = in.DeprecatedLastTimestamp
//! out.Count = in.DeprecatedCount
//! ```
//!
//! Because the strict branch of `ValidateEventCreate`
//! (`pkg/apis/core/validation/events.go:41-71`) then requires
//! `firstTimestamp`, `lastTimestamp`, `count` and `source` to be unset, a
//! create that sets any `deprecated*` field must be rejected. Rusternetes
//! models both API versions with one `Event` struct, so without the
//! conversion the `deprecated*` names decode into nothing and every strict
//! rule reads a zero value — the request is accepted (#1914).

use axum::http::StatusCode;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

fn v1_events_path() -> String {
    format!("/apis/events.k8s.io/v1/namespaces/{NS}/events")
}

/// A minimal, *valid* events.k8s.io/v1 create body. Every test below starts
/// from this and adds exactly one offending field, so a rejection can only be
/// attributed to that field.
fn valid_body(name: &str) -> Value {
    json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": { "name": name },
        "eventTime": "2026-09-08T10:05:00.123456Z",
        "reportingController": "probe",
        "reportingInstance": "probe-0",
        "action": "Probe",
        "reason": "Probe",
        "type": "Normal",
        "regarding": { "kind": "Pod", "name": "p", "namespace": NS },
    })
}

/// Collect the `field` of every cause on an Invalid Status body.
fn cause_fields(status: &Value) -> Vec<String> {
    status
        .get("details")
        .and_then(|d| d.get("causes"))
        .and_then(|c| c.as_array())
        .map(|causes| {
            causes
                .iter()
                .filter_map(|c| c.get("field").and_then(|f| f.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The baseline must be accepted, otherwise every rejection below is vacuous.
#[tokio::test]
async fn a_valid_events_v1_create_is_accepted() {
    let state = TestApiServer::new();
    let (code, body) = state
        .send(
            "POST",
            &v1_events_path(),
            Some("application/json"),
            Some(&valid_body("baseline")),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "baseline rejected: {body}");
}

#[tokio::test]
async fn deprecated_count_first_and_last_timestamp_are_rejected() {
    let state = TestApiServer::new();
    let mut body = valid_body("leniency-probe");
    body["deprecatedCount"] = json!(2);
    body["deprecatedFirstTimestamp"] = json!("2026-09-08T10:00:00Z");
    body["deprecatedLastTimestamp"] = json!("2026-09-08T10:05:00Z");

    let (code, status) = state
        .send(
            "POST",
            &v1_events_path(),
            Some("application/json"),
            Some(&body),
        )
        .await;

    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "deprecated* fields accepted: {status}"
    );
    let fields = cause_fields(&status);
    for expected in ["count", "firstTimestamp", "lastTimestamp"] {
        assert!(
            fields.iter().any(|f| f == expected),
            "no cause for {expected}; causes={fields:?} status={status}"
        );
    }
    // Upstream's message for all three (`events.go:56-67`).
    assert!(
        status
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .contains("needs to be unset"),
        "unexpected message: {status}"
    );
}

#[tokio::test]
async fn deprecated_source_is_rejected() {
    let state = TestApiServer::new();
    let mut body = valid_body("deprecated-source-probe");
    body["deprecatedSource"] = json!({ "component": "probe", "host": "node-1" });

    let (code, status) = state
        .send(
            "POST",
            &v1_events_path(),
            Some("application/json"),
            Some(&body),
        )
        .await;

    assert_eq!(
        code,
        StatusCode::UNPROCESSABLE_ENTITY,
        "deprecatedSource accepted: {status}"
    );
    assert!(
        cause_fields(&status).iter().any(|f| f == "source"),
        "no cause for source: {status}"
    );
}

/// `out.Message = in.Note` is unconditional upstream, and `note` is the only
/// name for the message on this API version — so a note must still land in the
/// stored object. Guards the conversion refactor against dropping the mapping
/// the handler used to do inline.
#[tokio::test]
async fn note_and_regarding_still_convert_onto_the_core_fields() {
    let state = TestApiServer::new();
    let mut body = valid_body("note-probe");
    body["note"] = json!("something happened");

    let (code, created) = state
        .send(
            "POST",
            &v1_events_path(),
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{created}");
    assert_eq!(created["message"], json!("something happened"));
    assert_eq!(created["involvedObject"]["name"], json!("p"));
}

/// The core `/api/v1` endpoint is legacy-only (`ValidateEventCreate` returns
/// right after the legacy pass for `v1`), and `deprecated*` is not part of its
/// schema at all — such a body must not start being rejected there.
#[tokio::test]
async fn core_v1_create_is_unaffected_by_the_deprecated_names() {
    let state = TestApiServer::new();
    let body = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": { "name": "core-probe" },
        "involvedObject": { "kind": "Pod", "name": "p", "namespace": NS },
        "reason": "Probe",
        "message": "m",
        "type": "Normal",
        "count": 3,
        "deprecatedCount": 9,
    });

    let (code, created) = state
        .send(
            "POST",
            &format!("/api/v1/namespaces/{NS}/events"),
            Some("application/json"),
            Some(&body),
        )
        .await;
    assert_eq!(code, StatusCode::CREATED, "{created}");
    assert_eq!(
        created["count"],
        json!(3),
        "core/v1 must keep its own count, not the events.k8s.io alias"
    );
}
