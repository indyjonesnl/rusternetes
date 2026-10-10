//! #3071 / #3029: `waitForServiceAccountInNamespace`
//! (`test/e2e/framework/util.go:278-307`) LISTs serviceaccounts with
//! `fieldSelector=metadata.name=default`, then WATCHes from the list's
//! resourceVersion with the same selector, and waits for the `default`
//! ServiceAccount to appear. An ADD that lands after the LIST must be
//! delivered to that watch.

use futures::StreamExt;
use rusternetes_test_support::harness::TestApiServer;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::timeout;

async fn first_events(resp: axum::response::Response, n: usize) -> Vec<Value> {
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut events = Vec::new();
    let run = async {
        while let Some(Ok(bytes)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&bytes));
            while let Some(i) = buf.find('\n') {
                let line = buf[..i].to_string();
                buf.drain(..=i);
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    if v["type"] == "BOOKMARK" {
                        continue;
                    }
                    events.push(v);
                    if events.len() >= n {
                        return;
                    }
                }
            }
        }
    };
    let _ = timeout(Duration::from_secs(2), run).await;
    events
}

fn sa(ns: &str, name: &str) -> Value {
    json!({"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":name,"namespace":ns}})
}

#[tokio::test]
async fn watch_at_list_rv_with_name_selector_sees_later_default_sa() {
    for round in 0..30 {
        let api = TestApiServer::new();
        let ns = format!("ns-{round}");
        let (s, b) = api
            .post(
                "/api/v1/namespaces",
                &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":ns}}),
            )
            .await;
        assert!(s.is_success(), "{s} {b}");
        let base = format!("/api/v1/namespaces/{ns}/serviceaccounts");
        let sel = "fieldSelector=metadata.name%3Ddefault";
        let (s, list) = api.get(&format!("{base}?{sel}")).await;
        assert!(s.is_success(), "{s} {list}");
        assert!(list["items"].as_array().unwrap().is_empty());
        let rv = list["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();

        let watch = api
            .respond(
                "GET",
                &format!("{base}?watch=true&resourceVersion={rv}&{sel}&allowWatchBookmarks=true"),
                None,
                None,
            )
            .await;
        let (s, b) = api.post(&base, &sa(&ns, "default")).await;
        assert!(s.is_success(), "{s} {b}");
        let (s, b) = api.post(&base, &sa(&ns, "other")).await;
        assert!(s.is_success(), "{s} {b}");

        let ev = first_events(watch, 1).await;
        let first = ev
            .first()
            .unwrap_or_else(|| panic!("round {round}: no event (rv {rv})"));
        assert_eq!(first["type"], "ADDED", "round {round}: {first}");
        assert_eq!(first["object"]["metadata"]["name"], "default");
    }
}

/// client-go 1.35 reflectors open with a watch-list (sendInitialEvents).
#[tokio::test]
async fn watchlist_with_name_selector_delivers_existing_and_later_default_sa() {
    for existing_first in [true, false] {
        let api = TestApiServer::new();
        let ns = "wl";
        let (s, _) = api
            .post(
                "/api/v1/namespaces",
                &json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":ns}}),
            )
            .await;
        assert!(s.is_success());
        let base = format!("/api/v1/namespaces/{ns}/serviceaccounts");
        if existing_first {
            let (s, b) = api.post(&base, &sa(ns, "default")).await;
            assert!(s.is_success(), "{s} {b}");
        }
        let watch = api
            .respond(
                "GET",
                &format!("{base}?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true&fieldSelector=metadata.name%3Ddefault"),
                None,
                None,
            )
            .await;
        if !existing_first {
            let (s, b) = api.post(&base, &sa(ns, "default")).await;
            assert!(s.is_success(), "{s} {b}");
        }
        let ev = first_events(watch, 1).await;
        let first = ev
            .first()
            .unwrap_or_else(|| panic!("existing_first={existing_first}: no event"));
        assert_eq!(first["type"], "ADDED");
        assert_eq!(first["object"]["metadata"]["name"], "default");
    }
}
