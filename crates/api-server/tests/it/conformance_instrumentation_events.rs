//! Per-step assertions for the `[sig-instrumentation]` Events area.
//!
//! Upstream Go sources (release-1.35):
//!   `k8s.io/kubernetes/test/e2e/instrumentation/core_events.go` — the
//!     core/v1 Event cases
//!   `k8s.io/kubernetes/test/e2e/instrumentation/events.go` — the
//!     `events.k8s.io/v1` cases
//!
//! The four `framework.ConformanceIt` cases in this area are:
//!   - core_events.go:58  "should manage the lifecycle of an event" (core/v1)
//!   - core_events.go:176 "should delete a collection of events"    (core/v1)
//!   - events.go:100      "should ensure that an event can be fetched,
//!     patched, deleted, and listed" (events.k8s.io/v1)
//!   - events.go:211      "should delete a collection of events"
//!     (events.k8s.io/v1)
//!
//! **All four are mirrored end-to-end in `conformance_events_api_test.rs`,
//! not here.** This file decomposes them into one test per step, which is
//! useful for localising a failure but means no test in it is itself a
//! conformance case. Nothing here carries a `[Conformance]` marker.
//!
//! Harness:
//!   `spawn_router()` → `TestApiServer::new()` (the shared
//!   `rusternetes-test-support` harness: `build_router` + `MemoryStorage` +
//!   `AlwaysAllowAuthorizer`), driven via a `send` helper that returns
//!   `(u16, serde_json::Value)`. Both the core/v1 (`/api/v1/…`) and
//!   `events.k8s.io/v1` (`/apis/events.k8s.io/v1/…`) surfaces are exercised.
//!
//! ---------------------------------------------------------------------------
//! Mirror audit (#1749, 2026-08-27)
//! ---------------------------------------------------------------------------
//!
//! Verified against `../kubernetes` at `release-1.35`. Findings do NOT carry
//! over to other mirror files; coverage was checked suite-wide.
//!
//! Every citation in this file was wrong, in one of three ways:
//!
//!   1. **Wrong file.** The core/v1 cases live in `core_events.go`, but every
//!      core/v1 test here cited `events.go`, which holds only the
//!      `events.k8s.io/v1` cases. The module doc conflated the two files, and
//!      each test inherited the conflation.
//!   2. **Past end of file.** `events.go` is 241 lines long. The label-
//!      selector deletecollection test cited `events.go:244-270`. A citation
//!      pointing beyond EOF cannot be checked against anything, so it could
//!      never have been invalidated by an upstream change.
//!   3. **Wrong step within the right case.** Where the file was right, the
//!      line ranges named a different `ginkgo.By` block than the one the test
//!      mirrors — the create test cited the update step, the get test cited a
//!      line inside update, the patch test cited the post-update get.
//!
//! Two citations described assertions upstream does not make: a `HaveLen(1)`
//! on the event list (upstream searches an all-namespaces list for a
//! (name, namespace) pair, because a live cluster has other events), and a
//! `NotFound` check after delete (upstream asserts absence from a list).
//!
//! Conformance framing withdrawn. The four `framework.ConformanceIt` cases in
//! this area are mirrored end-to-end in `conformance_events_api_test.rs`.
//! This file decomposes them into per-step tests, so no test in it is itself
//! a conformance case; the `[Conformance]` marker on the deletecollection
//! banner has been removed.
//!
//! Two tests mirror no upstream step and are now cited on their own footing:
//!   * `events_lifecycle_update_count_core_v1` — the core/v1 case sets
//!     `count: 1` at creation and never increments it. It patches `message`
//!     and updates `series`. `Event.count` is a real API field, so the test
//!     stands, but it mirrors nothing.
//!   * `events_get_nonexistent_returns_404` — neither case asserts a 404 on a
//!     direct GET.
//!
//! Kept deliberately: `events_deletecollection_label_selector_only_removes_matching`
//! asserts that a NON-matching event survives a label-selected delete. Neither
//! upstream case does, and that bystander is what makes the selector
//! observable rather than incidental — the same assertion shape that exposed
//! the `list_apiservices` selector bug in the aggregation/discovery area.

use axum::http::Method;
use rusternetes_storage::memory::MemoryStorage;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Harness — thin shims over the shared `TestApiServer`.
// ---------------------------------------------------------------------------

fn spawn_router() -> (Arc<MemoryStorage>, TestApiServer) {
    let api = TestApiServer::new();
    let mem = api.storage.clone();
    (mem, api)
}

/// Drive one request through the router and return `(status_u16, body_json)`.
async fn send(
    router: TestApiServer,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: Option<Value>,
) -> (u16, Value) {
    let (status, value) = router
        .send(method.as_str(), uri, content_type, body.as_ref())
        .await;
    (status.as_u16(), value)
}

// ---------------------------------------------------------------------------
// Wire-format fixture helpers (core/v1 Events)
// ---------------------------------------------------------------------------

/// Minimal core/v1 Event body, matching the structure `event_handler_test.rs`
/// uses, which mirrors the Go `&v1.Event{…}` literal in events.go:66-93.
fn core_event(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": ns
        },
        "involvedObject": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": "test-pod",
            "namespace": ns,
            "uid": "pod-uid-abc"
        },
        "reason": "ConformanceTest",
        "message": "Event lifecycle conformance test",
        "source": {
            "component": "e2e-test"
        },
        "type": "Normal",
        "count": 1
    })
}

/// Minimal `events.k8s.io/v1` Event body (Go `&eventsv1.Event{…}`).
/// Upstream: events.go:143-167, fields: eventTime (MicroTime), action,
/// reason, regarding, note, reportingController, type.
fn events_v1_event(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "events.k8s.io/v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": ns
        },
        "eventTime": "2026-01-01T00:00:00.000000Z",
        "action": "ConformanceAction",
        "reason": "ConformanceReason",
        "regarding": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": "test-pod",
            "namespace": ns,
            "uid": "pod-uid-abc"
        },
        "note": "Conformance event note",
        "reportingController": "e2e-test-controller",
        "reportingInstance": "e2e-test-controller-0",
        "type": "Normal"
    })
}

// ===========================================================================
// Per-step assertions for [sig-instrumentation] Events should manage the
// lifecycle of an event (core/v1)
//
// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:58.
// The case itself is mirrored end-to-end in `conformance_events_api_test.rs`
// as `core_v1_should_manage_lifecycle_of_an_event`.
//
// Mirror audit (#1749, 2026-08-27): the old banner cited `events.go:55`,
// which is inside the `newTestEvent` helper of the *events.k8s.io/v1* file.
// The core/v1 cases live in `core_events.go`; the two files were conflated
// throughout this section.
// ===========================================================================

/// POST a core/v1 Event → 201 Created; body contains the name and apiVersion.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:58
///   ("should manage the lifecycle of an event") — the create step at
///   core_events.go:64-82.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `events.go:96-99` is in the
/// wrong file and names a `ginkgo.By` block of a different case.
#[tokio::test]
async fn events_lifecycle_create_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-create";
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "lifecycle-event")),
    )
    .await;
    assert_eq!(status, 201, "expected 201 Created, got {status}: {body}");
    assert_eq!(
        body["metadata"]["name"].as_str(),
        Some("lifecycle-event"),
        "name must round-trip; got {body}",
    );
    assert_eq!(
        body["apiVersion"].as_str(),
        Some("v1"),
        "apiVersion must be v1; got {body}",
    );
}

/// GET a previously created core/v1 Event → 200; fields are preserved.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:58 — the fetch step
///   at core_events.go:114-119.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; the old range was in the wrong
/// file and straddled the `events.go:100` case boundary.
#[tokio::test]
async fn events_lifecycle_get_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-get";

    // Seed
    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-get")),
    )
    .await;
    assert_eq!(s, 201);

    // Fetch
    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events/ev-get"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "expected 200 OK; got {status}: {body}");
    assert_eq!(body["metadata"]["name"].as_str(), Some("ev-get"));
    assert_eq!(body["reason"].as_str(), Some("ConformanceTest"));
}

/// LIST events in namespace returns the seeded event in `items`.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:58 — the
///   "listing all events in all namespaces" step at core_events.go:83-102.
///   Note upstream lists across ALL namespaces with a label selector and
///   searches for the (name, namespace) pair; it does not assert a list
///   length of 1, because other events exist in a live cluster.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; wrong file. The old citation
/// also described a `HaveLen(1)` assertion that upstream does not make.
#[tokio::test]
async fn events_lifecycle_list_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-list";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-list")),
    )
    .await;
    assert_eq!(s, 201);

    let (status, body) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "list failed {status}: {body}");
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "expected 1 event; got {body}");
    assert_eq!(items[0]["metadata"]["name"].as_str(), Some("ev-list"));
}

/// PUT (update) a core/v1 Event — count increments and is reflected.
///
/// Upstream: no conformance case. The core/v1 lifecycle case sets `count: 1`
/// at creation (core_events.go:76) and never increments it — it patches
/// `message` (core_events.go:103-113) and updates `series`
/// (core_events.go:120-136). `Event.count` is a real field of the core/v1
/// API, so this test stands on its own, but it mirrors no upstream step.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `events.go:108-115` is in the
/// wrong file and describes a step that does not exist.
#[tokio::test]
async fn events_lifecycle_update_count_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-update";

    let (s, created) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-update")),
    )
    .await;
    assert_eq!(s, 201);

    // Build updated body: bump count to 5
    let mut updated = created.clone();
    updated["count"] = json!(5);

    let (status, body) = send(
        router,
        Method::PUT,
        &format!("/api/v1/namespaces/{ns}/events/ev-update"),
        Some("application/json"),
        Some(updated),
    )
    .await;
    assert!(
        status == 200,
        "expected 200/201 after update; got {status}: {body}",
    );
    assert_eq!(
        body["count"].as_i64(),
        Some(5),
        "count must be 5 after update; got {body}",
    );
}

/// DELETE a core/v1 Event → 200; subsequent GET returns 404.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:58 — the delete
///   step at core_events.go:147-151, followed by the all-namespaces list at
///   :152-175 which requires the event to be gone. Upstream asserts absence
///   from that list rather than a 404 on a direct GET.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; wrong file, and the old
/// citation described a `NotFound` assertion upstream does not make here.
#[tokio::test]
async fn events_lifecycle_delete_core_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-lifecycle-delete";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "ev-delete")),
    )
    .await;
    assert_eq!(s, 201);

    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/events/ev-delete"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200,
        "expected 200/202 on delete; got {del_status}",
    );

    let (get_status, _) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events/ev-delete"),
        None,
        None,
    )
    .await;
    assert_eq!(get_status, 404, "event must be gone after DELETE");
}

// ===========================================================================
// [sig-instrumentation] Events API should ensure that an event can be
// fetched, patched, deleted, and listed
//
// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:100.
// The case itself is mirrored end-to-end in `conformance_events_api_test.rs`
// as `events_v1_should_fetch_patch_delete_list`.
//
// Mirror audit (#1749, 2026-08-27): the banner cited `events.go:112`, which
// is a `ginkgo.By` line inside the case, not the case.
// ===========================================================================

/// POST to `events.k8s.io/v1` surface → 201 with correct apiVersion.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:100 — the create step
///   at events.go:103-107.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:169-173` is the update step
/// of the same case, not the create step.
#[tokio::test]
async fn events_api_create_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-create";
    let (status, body) = send(
        router,
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-create")),
    )
    .await;
    assert_eq!(status, 201, "expected 201; got {status}: {body}");
    assert_eq!(
        body["metadata"]["name"].as_str(),
        Some("api-ev-create"),
        "name must round-trip; got {body}",
    );
    assert_eq!(
        body["apiVersion"].as_str(),
        Some("events.k8s.io/v1"),
        "apiVersion must be events.k8s.io/v1; got {body}",
    );
}

/// GET via `events.k8s.io/v1` returns the stored event.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:100 — the get step at
///   events.go:134-137.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:174-177` is inside the update
/// step.
#[tokio::test]
async fn events_api_get_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-get";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-get")),
    )
    .await;
    assert_eq!(s, 201);

    let (status, body) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-get"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "expected 200; got {status}: {body}");
    assert_eq!(body["metadata"]["name"].as_str(), Some("api-ev-get"));
    assert_eq!(body["reason"].as_str(), Some("ConformanceReason"));
}

/// PATCH (merge-patch) via `events.k8s.io/v1` updates a field.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:100 — the patch step
///   at events.go:138-154. Upstream patches `series`, not `reason`, and
///   verifies it with a whole-object `apiequality.Semantic.DeepEqual`
///   (events.go:155-168) rather than by reading back the patched field.
///   That DeepEqual is mirrored in `conformance_events_api_test.rs`.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:178-191` is the post-update
/// get, and the old citation described a field upstream never patches.
#[tokio::test]
async fn events_api_patch_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-patch";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-patch")),
    )
    .await;
    assert_eq!(s, 201);

    let patch_body = json!({"reason": "PatchedReason"});
    let (status, body) = send(
        router,
        Method::PATCH,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-patch"),
        Some("application/merge-patch+json"),
        Some(patch_body),
    )
    .await;
    assert!(
        status == 200,
        "expected 200/201 after patch; got {status}: {body}",
    );
    assert_eq!(
        body["reason"].as_str(),
        Some("PatchedReason"),
        "reason must be updated; got {body}",
    );
}

/// DELETE via `events.k8s.io/v1`; subsequent GET returns 404.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:100 — the delete step
///   at events.go:188-191. Upstream then requires the event to be absent
///   from both the all-namespaces and the namespaced list (:192-210); it
///   does not assert a 404 on a direct GET.
///
/// Mirror audit (#1749, 2026-08-27): re-cited.
#[tokio::test]
async fn events_api_delete_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-delete";

    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(events_v1_event(ns, "api-ev-delete")),
    )
    .await;
    assert_eq!(s, 201);

    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-delete"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200,
        "expected 200/202 on delete; got {del_status}",
    );

    let (get_status, _) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events/api-ev-delete"),
        None,
        None,
    )
    .await;
    assert_eq!(get_status, 404, "event must be gone after DELETE");
}

/// LIST via `events.k8s.io/v1` returns all events in the namespace.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:100 — the list steps
///   at events.go:108-119 (all namespaces, then the test namespace).
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:203-213` straddles the end of
/// this case and the start of `events.go:211`.
#[tokio::test]
async fn events_api_list_events_k8s_io_v1() {
    let (_, router) = spawn_router();
    let ns = "e2e-api-list";

    for i in 0..3u32 {
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(events_v1_event(ns, &format!("api-ev-list-{i}"))),
        )
        .await;
        assert_eq!(s, 201, "seed event {i} failed");
    }

    let (status, body) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(status, 200, "list failed {status}: {body}");
    let items = body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 3, "expected 3 events; got {body}");
}

// ===========================================================================
// Per-step assertions for the two "should delete a collection of events"
// cases — core/v1 (core_events.go:176) and events.k8s.io/v1 (events.go:211).
//
// Both cases are mirrored end-to-end in `conformance_events_api_test.rs`.
//
// Mirror audit (#1749, 2026-08-27): the banner carried a `[Conformance]`
// marker and cited `events.go:217`, a `ginkgo.By` line inside the case. It
// also conflated the two cases into one, so the core/v1 variant below was
// attributed to the events.k8s.io/v1 file.
// ===========================================================================

/// DELETE collection on core/v1 surface wipes all events in the namespace.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:176
///   ("should delete a collection of events") — create set :179-200,
///   list by label :201-209, deletecollection :210-217, then
///   `checkEventListQuantity(..., 0)` at :218-225.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; the core/v1 case lives in
/// `core_events.go`, not `events.go`.
#[tokio::test]
async fn events_deletecollection_core_v1_clears_namespace() {
    let (_, router) = spawn_router();
    let ns = "e2e-deletecoll-core";

    // Seed three events.
    for i in 0..3u32 {
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/api/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(core_event(ns, &format!("dc-core-{i}"))),
        )
        .await;
        assert_eq!(s, 201, "seed {i} failed");
    }

    // Verify they exist.
    let (ls, lb) = send(
        router.clone(),
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls, 200);
    assert_eq!(
        lb["items"].as_array().unwrap().len(),
        3,
        "expected 3 before deletecollection; got {lb}",
    );

    // DeleteCollection — DELETE on the collection URL.
    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200,
        "expected 200/204 on deletecollection; got {del_status}",
    );

    // Must be empty now.
    let (ls2, lb2) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls2, 200);
    assert_eq!(
        lb2["items"].as_array().unwrap().len(),
        0,
        "namespace must be empty after deletecollection; got {lb2}",
    );
}

/// DELETE collection on `events.k8s.io/v1` surface wipes all events in the
/// namespace.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:211
///   ("should delete a collection of events") — create set :214-219,
///   list by label :220-226, delete list :227-233, check quantity :234-240.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; `:217-243` names a `ginkgo.By`
/// line and runs past the end of the case.
#[tokio::test]
async fn events_deletecollection_events_k8s_io_v1_clears_namespace() {
    let (_, router) = spawn_router();
    let ns = "e2e-deletecoll-v1";

    for i in 0..3u32 {
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(events_v1_event(ns, &format!("dc-v1-{i}"))),
        )
        .await;
        assert_eq!(s, 201, "seed {i} failed");
    }

    let (ls, lb) = send(
        router.clone(),
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls, 200);
    assert_eq!(lb["items"].as_array().unwrap().len(), 3);

    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert!(
        del_status == 200,
        "expected 200/204 on deletecollection; got {del_status}",
    );

    let (ls2, lb2) = send(
        router,
        Method::GET,
        &format!("/apis/events.k8s.io/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls2, 200);
    assert_eq!(
        lb2["items"].as_array().unwrap().len(),
        0,
        "namespace must be empty after deletecollection; got {lb2}",
    );
}

/// DELETE collection with a label selector only removes matching events.
///
/// Upstream: k8s.io/kubernetes/test/e2e/instrumentation/events.go:211 and
///   k8s.io/kubernetes/test/e2e/instrumentation/core_events.go:176 — both cases delete
///   by label selector (events.go:227-233, core_events.go:210-217). Neither
///   asserts that a NON-matching object survives; this test adds that, which
///   is what makes the selector observable rather than incidental.
///
/// Mirror audit (#1749, 2026-08-27): re-cited. `events.go:244-270` is past the
/// end of the file — `events.go` is 241 lines long — so the old citation
/// could never have been checked against anything.
/// Sonobuoy: PASS — the handler forwards `?labelSelector=…` through
/// `apply_selectors` before deleting.
#[tokio::test]
async fn events_deletecollection_label_selector_only_removes_matching() {
    let (_, router) = spawn_router();
    let ns = "e2e-deletecoll-label";

    // Two events with label, one without.
    for i in 0..2u32 {
        let mut body = core_event(ns, &format!("labeled-{i}"));
        body["metadata"]["labels"] = json!({"conformance": "yes"});
        let (s, _) = send(
            router.clone(),
            Method::POST,
            &format!("/api/v1/namespaces/{ns}/events"),
            Some("application/json"),
            Some(body),
        )
        .await;
        assert_eq!(s, 201);
    }
    let (s, _) = send(
        router.clone(),
        Method::POST,
        &format!("/api/v1/namespaces/{ns}/events"),
        Some("application/json"),
        Some(core_event(ns, "unlabeled")),
    )
    .await;
    assert_eq!(s, 201);

    // DeleteCollection with label selector.
    let (del_status, _) = send(
        router.clone(),
        Method::DELETE,
        &format!("/api/v1/namespaces/{ns}/events?labelSelector=conformance%3Dyes"),
        None,
        None,
    )
    .await;
    assert!(del_status == 200, "expected 200/204; got {del_status}",);

    // Only the unlabeled event should remain.
    let (ls, lb) = send(
        router,
        Method::GET,
        &format!("/api/v1/namespaces/{ns}/events"),
        None,
        None,
    )
    .await;
    assert_eq!(ls, 200);
    let items = lb["items"].as_array().unwrap();
    assert_eq!(
        items.len(),
        1,
        "exactly 1 event must survive label-selector deletecollection; got {lb}",
    );
    assert_eq!(
        items[0]["metadata"]["name"].as_str(),
        Some("unlabeled"),
        "the surviving event must be the unlabeled one; got {lb}",
    );
}

/// GET on a non-existent Event returns 404.
///
/// Upstream: no conformance case. Neither Events case asserts a 404 on a
/// direct GET — both check absence from a list after deletion
/// (core_events.go:152-175, events.go:192-210). The 404 contract itself is
/// `staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go`
/// (`Get` → `NewNotFound`), which is what this test pins.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; the old citation named
/// `events.go:116-122`, which is in the wrong file and is a list step.
#[tokio::test]
async fn events_get_nonexistent_returns_404() {
    let (_, router) = spawn_router();
    let (status, _) = send(
        router,
        Method::GET,
        "/api/v1/namespaces/default/events/does-not-exist",
        None,
        None,
    )
    .await;
    assert_eq!(status, 404, "missing event must return 404");
}

/// Events in different namespaces do not bleed across: list in ns-a must not
/// include events created in ns-b.
///
/// Upstream: no conformance case. Namespace isolation is a precondition
/// every e2e case relies on via `framework.NewDefaultFramework`, but no
/// Events case asserts it. Kept on that footing.
///
/// Mirror audit (#1749, 2026-08-27): re-cited; not a conformance case.
#[tokio::test]
async fn events_namespace_isolation() {
    let (_, router) = spawn_router();

    let (s1, _) = send(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/ns-iso-a/events",
        Some("application/json"),
        Some(core_event("ns-iso-a", "ev-a")),
    )
    .await;
    assert_eq!(s1, 201);

    let (s2, _) = send(
        router.clone(),
        Method::POST,
        "/api/v1/namespaces/ns-iso-b/events",
        Some("application/json"),
        Some(core_event("ns-iso-b", "ev-b")),
    )
    .await;
    assert_eq!(s2, 201);

    // List ns-iso-a — must contain only ev-a.
    let (_, la) = send(
        router.clone(),
        Method::GET,
        "/api/v1/namespaces/ns-iso-a/events",
        None,
        None,
    )
    .await;
    let items_a = la["items"].as_array().unwrap();
    assert_eq!(items_a.len(), 1);
    assert_eq!(items_a[0]["metadata"]["name"].as_str(), Some("ev-a"));

    // List ns-iso-b — must contain only ev-b.
    let (_, lb) = send(
        router,
        Method::GET,
        "/api/v1/namespaces/ns-iso-b/events",
        None,
        None,
    )
    .await;
    let items_b = lb["items"].as_array().unwrap();
    assert_eq!(items_b.len(), 1);
    assert_eq!(items_b[0]["metadata"]["name"].as_str(), Some("ev-b"));
}
