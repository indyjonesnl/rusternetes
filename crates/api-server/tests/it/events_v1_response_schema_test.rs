//! An `events.k8s.io/v1` response carries the `events.k8s.io/v1` schema.
//!
//! Upstream serves the two API versions from two Go types and converts on both
//! edges: `Convert_v1_Event_To_core_Event` on the way in and
//! `Convert_core_Event_To_v1_Event` on the way out
//! (`pkg/apis/events/v1/conversion.go:29-60`). The v1 type
//! (`staging/src/k8s.io/api/events/v1/types.go:34-100`) has no `count`,
//! `message`, `source`, `involvedObject`, `firstTimestamp` or `lastTimestamp`
//! field at all — those live on the core type, under `deprecated*` names here.
//!
//! Rusternetes stored the core object and rewrote only `apiVersion` on the way
//! out, so every `events.k8s.io/v1` read answered with core field names and
//! omitted all four `deprecated*` fields (#1926). A client that reads `note`
//! or `regarding` — `kubectl get events`, `client-go`'s events informer — saw
//! nothing.

use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};

const NS: &str = "default";

/// Field names that exist only on the core Event type.
const CORE_ONLY: &[&str] = &[
    "count",
    "message",
    "source",
    "involvedObject",
    "firstTimestamp",
    "lastTimestamp",
];

/// Field names the v1 type always emits: `omitempty` is a no-op on a Go struct,
/// and `metav1.Time`'s marshaller writes `null` rather than nothing.
const V1_ALWAYS: &[&str] = &["regarding", "deprecatedSource", "eventTime"];

fn v1_path() -> String {
    format!("/apis/events.k8s.io/v1/namespaces/{NS}/events")
}

fn body(name: &str) -> Value {
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
        "note": "the note",
        "regarding": { "kind": "Pod", "name": "p", "namespace": NS },
    })
}

fn assert_v1_shape(obj: &Value, whose: &str) {
    assert_eq!(
        obj["apiVersion"],
        json!("events.k8s.io/v1"),
        "{whose}: {obj}"
    );
    assert_eq!(obj["kind"], json!("Event"), "{whose}: {obj}");
    for core in CORE_ONLY {
        assert!(
            obj.get(core).is_none(),
            "{whose} carries `{core}`, which is not an events.k8s.io/v1 field: {obj}"
        );
    }
    for always in V1_ALWAYS {
        assert!(
            obj.get(always).is_some(),
            "{whose} omits `{always}`, which upstream always emits: {obj}"
        );
    }
}

#[tokio::test]
async fn create_get_and_list_answer_in_the_v1_schema() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            &v1_path(),
            Some("application/json"),
            Some(&body("e1")),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");
    assert_v1_shape(&created, "the create response");
    // The note survives as the v1 field, not as `message`.
    assert_eq!(created["note"], json!("the note"));
    assert_eq!(created["regarding"]["name"], json!("p"));
    assert_eq!(created["reportingController"], json!("probe"));

    let (status, got) = api.get(&format!("{}/e1", v1_path())).await;
    assert!(status.is_success(), "{status} {got}");
    assert_v1_shape(&got, "the get response");
    assert_eq!(got["note"], json!("the note"));

    let (status, list) = api.get(&v1_path()).await;
    assert!(status.is_success(), "{status} {list}");
    assert_eq!(list["apiVersion"], json!("events.k8s.io/v1"));
    assert_eq!(list["kind"], json!("EventList"));
    let items = list["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "{list}");
    assert_v1_shape(&items[0], "the list item");

    // The cluster-wide list too.
    let (status, list) = api.get("/apis/events.k8s.io/v1/events").await;
    assert!(status.is_success(), "{status} {list}");
    assert_v1_shape(
        &list["items"].as_array().expect("items")[0],
        "the cluster-wide list item",
    );
}

/// The core endpoint is unchanged: it still answers with the core names, which
/// is what `kubectl get events` (core v1) and every existing client reads.
#[tokio::test]
async fn the_core_endpoint_still_answers_in_the_core_schema() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            &format!("/api/v1/namespaces/{NS}/events"),
            Some("application/json"),
            Some(&json!({
                "apiVersion": "v1",
                "kind": "Event",
                "metadata": { "name": "core-e" },
                "involvedObject": { "kind": "Pod", "name": "p", "namespace": NS },
                "reason": "Started",
                "message": "the message",
                "type": "Normal",
            })),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");
    assert_eq!(created["apiVersion"], json!("v1"));
    assert_eq!(created["message"], json!("the message"));
    assert_eq!(created["involvedObject"]["name"], json!("p"));
    assert!(
        created.get("note").is_none(),
        "core v1 has no `note` field: {created}"
    );
}

/// The same object, read through both endpoints. The core read sees `message` /
/// `involvedObject`; the v1 read sees `note` / `regarding`. This is the whole
/// point of the two-type split, and the single-struct version could not do it.
#[tokio::test]
async fn one_stored_event_reads_correctly_through_both_versions() {
    let api = TestApiServer::new();

    let (status, _) = api
        .send(
            "POST",
            &v1_path(),
            Some("application/json"),
            Some(&body("both")),
        )
        .await;
    assert!(status.is_success());

    let (status, v1) = api.get(&format!("{}/both", v1_path())).await;
    assert!(status.is_success(), "{status} {v1}");
    assert_eq!(v1["note"], json!("the note"));

    let (status, core) = api
        .get(&format!("/api/v1/namespaces/{NS}/events/both"))
        .await;
    assert!(status.is_success(), "{status} {core}");
    assert_eq!(core["apiVersion"], json!("v1"));
    assert_eq!(core["message"], json!("the note"));
    assert_eq!(core["involvedObject"]["name"], json!("p"));
}

/// A read-modify-write cycle through `events.k8s.io/v1`. With responses in the
/// v1 schema the conversion on the way back in can be unconditional, as
/// upstream's is: the body the client returns has no core field to clobber.
#[tokio::test]
async fn a_v1_read_modify_write_preserves_every_field() {
    let api = TestApiServer::new();

    let (status, created) = api
        .send(
            "POST",
            &v1_path(),
            Some("application/json"),
            Some(&body("rmw")),
        )
        .await;
    assert!(status.is_success(), "{status} {created}");

    // Send the response back with the one field upstream lets an update change
    // -- `series`, the heartbeat counter. Everything else is immutable on this
    // version (`ValidateEventUpdate`, `pkg/apis/core/validation/events.go:89-113`),
    // so a client's read-modify-write must round-trip the rest byte for byte or
    // the update is rejected. That is exactly what an unconverted response
    // broke: the client sent back `message`/`involvedObject`, which this
    // version has no field for.
    let mut put = created.clone();
    put["series"] = json!({
        "count": 2,
        "lastObservedTime": "2026-09-08T10:06:00.000000Z",
    });
    let (status, updated) = api
        .send(
            "PUT",
            &format!("{}/rmw", v1_path()),
            Some("application/json"),
            Some(&put),
        )
        .await;
    assert!(status.is_success(), "{status} {updated}");
    assert_v1_shape(&updated, "the update response");
    assert_eq!(updated["note"], json!("the note"));
    assert_eq!(updated["series"]["count"], json!(2));
    assert_eq!(updated["regarding"]["name"], json!("p"));
    assert_eq!(updated["reportingController"], json!("probe"));
    assert_eq!(updated["action"], json!("Probe"));

    // And the core view of the same object followed along.
    let (status, core) = api
        .get(&format!("/api/v1/namespaces/{NS}/events/rmw"))
        .await;
    assert!(status.is_success(), "{status} {core}");
    assert_eq!(core["message"], json!("the note"));
    assert_eq!(core["series"]["count"], json!(2));
}

/// The watch stream is served from the same conversion, so an informer sees the
/// same schema a GET does. This was the wide part of #1926: the watch path
/// serialized the stored `Event` directly.
#[tokio::test]
async fn the_watch_stream_answers_in_the_v1_schema() {
    let api = TestApiServer::new();

    let (status, _) = api
        .send(
            "POST",
            &v1_path(),
            Some("application/json"),
            Some(&body("watched")),
        )
        .await;
    assert!(status.is_success());

    let resp = api
        .respond(
            "GET",
            &format!(
                "{}?watch=true&resourceVersion=0&timeoutSeconds=2",
                v1_path()
            ),
            None,
            None,
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200);

    let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .expect("read watch body");
    let text = String::from_utf8_lossy(&body_bytes);
    let frame: Value = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("watch frame"))
        .find(|f| f["type"] == json!("ADDED"))
        .unwrap_or_else(|| panic!("no ADDED frame in: {text}"));

    assert_v1_shape(&frame["object"], "the watch frame object");
    assert_eq!(frame["object"]["note"], json!("the note"));
}
