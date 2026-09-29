//! `sendInitialEvents` is three-way, not a bool.
//!
//! `ListOptions.SendInitialEvents` is a `*bool` upstream, and
//! `watchCache.getAllEventsSinceLocked`
//! (`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go:878-913`)
//! branches on all three states:
//!
//! ```text
//! if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
//!     return w.getIntervalFromStoreLocked(key, matchesSingle)
//! }
//! ...
//! if resourceVersion == 0 {
//!     if opts.SendInitialEvents == nil {
//!         // resourceVersion = 0 means that we don't require any specific starting point
//!         // and we would like to start watching from ~now.
//!         // However, to keep backward compatibility, we additionally need to return the
//!         // current state and only then start watching from that point.
//!         return w.getIntervalFromStoreLocked(key, matchesSingle)
//!     }
//!     // SendInitialEvents = false and resourceVersion = 0
//!     // means that the request would like to start watching
//!     // from Any resourceVersion
//!     resourceVersion = w.resourceVersion
//! }
//! ```
//!
//! So `sendInitialEvents=false` with `resourceVersion=0` is the ONE way a
//! client asks for "stream from ~now, don't replay the world" — and collapsing
//! the parameter with `unwrap_or(false)` made that request indistinguishable
//! from "absent", which means the opposite.
//!
//! The `initial-events-end` bookmark is gated separately:
//! `setInitialEventsEndBookmarkIfRequested` (`cacher.go:1297-1315`) sets it
//! only when `*opts.SendInitialEvents && opts.Predicate.AllowWatchBookmarks`,
//! mirroring `isListWatchRequest` (`cacher.go:1241-1243`).
//!
//! The matrix below is ported from upstream's own pin,
//! `RunWatchSemantics` (`staging/src/k8s.io/apiserver/pkg/storage/testing/
//! watcher_tests.go:1330-1510`), restricted to the `RV=unset` / `RV=0` rows —
//! upstream's `RV=1` rows are served by ring replay, which this server answers
//! with a store snapshot instead (documented divergence in `watch.rs`).

use futures::StreamExt;
use rusternetes_storage::{build_key, Storage};
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

const NS: &str = "watchsie";

/// Collect every `\n`-delimited watch envelope that arrives within `window`.
/// Unlike the `max`-bounded collectors in the sibling watch tests this always
/// runs the whole window, because half these cases assert that something does
/// NOT arrive.
async fn collect_for(router: TestApiServer, uri: String, window: Duration) -> Vec<Value> {
    let resp = router.respond("GET", &uri, None, None).await;
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut events = Vec::new();
    let run = async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    events.push(v);
                }
            }
        }
    };
    let _ = timeout(window, run).await;
    events
}

/// The `ADDED` object names, in arrival order.
fn added_names(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e.get("type").and_then(|v| v.as_str()) == Some("ADDED"))
        .filter_map(|e| {
            e.pointer("/object/metadata/name")
                .and_then(|v| v.as_str())
                .map(String::from)
        })
        .collect()
}

/// Whether an `initial-events-end` bookmark was sent.
fn has_initial_events_end(events: &[Value]) -> bool {
    events.iter().any(|e| {
        e.get("type").and_then(|v| v.as_str()) == Some("BOOKMARK")
            && e.pointer("/object/metadata/annotations/k8s.io~1initial-events-end")
                .and_then(|v| v.as_str())
                == Some("true")
    })
}

fn configmap(name: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": name, "namespace": NS },
        "data": { "k": "v" }
    })
}

struct Case {
    name: &'static str,
    allow_watch_bookmarks: Option<bool>,
    send_initial_events: Option<bool>,
    resource_version: Option<&'static str>,
    /// The three pre-existing objects must arrive as synthetic `ADDED`.
    expect_initial_events: bool,
    expect_initial_events_end_bookmark: bool,
}

/// Upstream `RunWatchSemantics`' `RV=unset` and `RV=0` rows.
fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "allowWatchBookmarks=true, sendInitialEvents=true, RV=unset",
            allow_watch_bookmarks: Some(true),
            send_initial_events: Some(true),
            resource_version: None,
            expect_initial_events: true,
            expect_initial_events_end_bookmark: true,
        },
        Case {
            name: "allowWatchBookmarks=true, sendInitialEvents=false, RV=unset",
            allow_watch_bookmarks: Some(true),
            send_initial_events: Some(false),
            resource_version: None,
            expect_initial_events: false,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "allowWatchBookmarks=false, sendInitialEvents=false, RV=unset",
            allow_watch_bookmarks: None,
            send_initial_events: Some(false),
            resource_version: None,
            expect_initial_events: false,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "allowWatchBookmarks=false, sendInitialEvents=true, RV=unset",
            allow_watch_bookmarks: None,
            send_initial_events: Some(true),
            resource_version: None,
            expect_initial_events: true,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "allowWatchBookmarks=true, sendInitialEvents=true, RV=0",
            allow_watch_bookmarks: Some(true),
            send_initial_events: Some(true),
            resource_version: Some("0"),
            expect_initial_events: true,
            expect_initial_events_end_bookmark: true,
        },
        Case {
            name: "allowWatchBookmarks=true, sendInitialEvents=false, RV=0",
            allow_watch_bookmarks: Some(true),
            send_initial_events: Some(false),
            resource_version: Some("0"),
            expect_initial_events: false,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "allowWatchBookmarks=false, sendInitialEvents=false, RV=0",
            allow_watch_bookmarks: None,
            send_initial_events: Some(false),
            resource_version: Some("0"),
            expect_initial_events: false,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "allowWatchBookmarks=false, sendInitialEvents=true, RV=0",
            allow_watch_bookmarks: None,
            send_initial_events: Some(true),
            resource_version: Some("0"),
            expect_initial_events: true,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "legacy, RV=0",
            allow_watch_bookmarks: None,
            send_initial_events: None,
            resource_version: Some("0"),
            expect_initial_events: true,
            expect_initial_events_end_bookmark: false,
        },
        Case {
            name: "legacy, RV=unset",
            allow_watch_bookmarks: None,
            send_initial_events: None,
            resource_version: None,
            expect_initial_events: true,
            expect_initial_events_end_bookmark: false,
        },
    ]
}

/// Seed three ConfigMaps, open the watch the case describes, create two more,
/// and report what diverges from the upstream row.
async fn run_case(case: &Case) -> Vec<String> {
    let api = TestApiServer::new();
    let mut problems = Vec::new();

    api.storage
        .create(
            &build_key("namespaces", None, NS),
            &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":NS}}),
        )
        .await
        .expect("seed namespace");

    let collection = format!("/api/v1/namespaces/{NS}/configmaps");
    for name in ["cm1", "cm2", "cm3"] {
        let (status, body) = api.post(&collection, &configmap(name)).await;
        assert!(status.is_success(), "seed {name}: {status} {body}");
    }

    let mut uri = format!("{collection}?watch=true");
    if let Some(rv) = case.resource_version {
        uri.push_str(&format!("&resourceVersion={rv}"));
    }
    if let Some(b) = case.allow_watch_bookmarks {
        uri.push_str(&format!("&allowWatchBookmarks={b}"));
    }
    if let Some(b) = case.send_initial_events {
        uri.push_str(&format!("&sendInitialEvents={b}"));
    }

    let handle = tokio::spawn(collect_for(api.clone(), uri, Duration::from_millis(1_500)));
    tokio::time::sleep(Duration::from_millis(300)).await;

    for name in ["cm4", "cm5"] {
        let (status, body) = api.post(&collection, &configmap(name)).await;
        assert!(status.is_success(), "create {name}: {status} {body}");
    }

    let events = handle.await.unwrap();
    let names = added_names(&events);

    let saw_initial = names.iter().any(|n| n == "cm1" || n == "cm2" || n == "cm3");
    if saw_initial != case.expect_initial_events {
        problems.push(format!(
            "{}: expected initial events = {}, got ADDED {:?}",
            case.name, case.expect_initial_events, names
        ));
    }
    if case.expect_initial_events {
        for want in ["cm1", "cm2", "cm3"] {
            if !names.iter().any(|n| n == want) {
                problems.push(format!(
                    "{}: initial ADDED for {want} missing, got {:?}",
                    case.name, names
                ));
            }
        }
    }

    // Live events are delivered in every row of the matrix.
    for want in ["cm4", "cm5"] {
        if !names.iter().any(|n| n == want) {
            problems.push(format!(
                "{}: live ADDED for {want} missing, got {:?}",
                case.name, names
            ));
        }
    }

    let bookmarked = has_initial_events_end(&events);
    if bookmarked != case.expect_initial_events_end_bookmark {
        problems.push(format!(
            "{}: expected initial-events-end bookmark = {}, got {}",
            case.name, case.expect_initial_events_end_bookmark, bookmarked
        ));
    }

    problems
}

#[tokio::test]
async fn send_initial_events_follows_the_upstream_matrix() {
    let all = cases();
    let results = futures::future::join_all(all.iter().map(run_case)).await;
    let problems: Vec<String> = results.into_iter().flatten().collect();
    assert!(
        problems.is_empty(),
        "sendInitialEvents diverges from upstream:\n{}",
        problems.join("\n")
    );
}
