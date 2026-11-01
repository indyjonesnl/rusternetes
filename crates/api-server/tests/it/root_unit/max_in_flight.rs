//! `WithMaxInFlightLimit` (`server/filters/maxinflight.go`) and the limits'
//! validation (`server/options/server_run_options.go:197-203`), mirroring
//! `maxinflight_test.go` (`TestMaxInFlightNonMutating`,
//! `TestMaxInFlightMutating`, `TestMaxInFlightSkippedForLongRunningRequest`
//! and the `system:masters` bypass).

use axum::{body::Body, routing::any, Router};
use rusternetes_api_server::max_in_flight_filter::{
    validate_limits, with_max_in_flight_limit, MaxInFlight,
};
use rusternetes_common::auth::UserInfo;
use rusternetes_middleware::AuthContext;
use std::sync::Arc;
use tokio::sync::Notify;
use tower::ServiceExt;

/// A router whose handler parks until `release` is notified, behind the filter.
fn app(non_mutating: usize, mutating: usize, release: Arc<Notify>, masters: bool) -> Router {
    let limits = Arc::new(MaxInFlight::new(non_mutating, mutating).unwrap());
    Router::new()
        .route(
            "/*path",
            any(move || {
                let release = release.clone();
                async move {
                    release.notified().await;
                    "ok"
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            limits,
            with_max_in_flight_limit,
        ))
        .layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| async move {
                let mut user = UserInfo::anonymous();
                if masters {
                    user.groups.push("system:masters".into());
                }
                req.extensions_mut().insert(AuthContext { user });
                next.run(req).await
            },
        ))
}

fn req(method: &str, uri: &str) -> axum::http::Request<Body> {
    axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

/// Park the `parked` requests in the handler, then send `extra`: `Some` of
/// its immediate answer (status, Retry-After), or `None` when it was admitted
/// and is itself parked in the handler.
async fn extra_status(
    app: &Router,
    release: &Arc<Notify>,
    parked: &[(&str, &str)],
    extra: (&str, &str),
) -> Option<(u16, Option<String>)> {
    let mut tasks = Vec::new();
    for (m, u) in parked {
        let app = app.clone();
        let r = req(m, u);
        tasks.push(tokio::spawn(async move { app.oneshot(r).await.unwrap() }));
    }
    // Let the parked requests reach the handler.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let extra_app = app.clone();
    let extra_req = req(extra.0, extra.1);
    let mut extra_task = tokio::spawn(async move { extra_app.oneshot(extra_req).await.unwrap() });
    let out =
        match tokio::time::timeout(std::time::Duration::from_millis(200), &mut extra_task).await {
            Ok(resp) => {
                let resp = resp.unwrap();
                Some((
                    resp.status().as_u16(),
                    resp.headers()
                        .get("retry-after")
                        .map(|v| v.to_str().unwrap().to_string()),
                ))
            }
            Err(_) => None,
        };
    release.notify_waiters();
    if out.is_none() {
        let _ = extra_task.await;
    }
    for t in tasks {
        let _ = t.await;
    }
    out
}

#[tokio::test]
async fn non_mutating_limit_rejects_with_429_and_retry_after() {
    let release = Arc::new(Notify::new());
    let app = app(1, 0, release.clone(), false);
    let (status, retry) = extra_status(
        &app,
        &release,
        &[("GET", "/api/v1/namespaces/n/pods/a")],
        ("GET", "/api/v1/namespaces/n/pods/b"),
    )
    .await
    .expect("rejected at once");
    assert_eq!(status, 429);
    assert_eq!(retry.as_deref(), Some("1"));
}

#[tokio::test]
async fn mutating_and_non_mutating_are_counted_separately() {
    // A parked GET does not take the mutating slot: the POST is admitted.
    let release = Arc::new(Notify::new());
    let a = app(1, 1, release.clone(), false);
    let out = extra_status(
        &a,
        &release,
        &[("GET", "/api/v1/namespaces/n/pods/a")],
        ("POST", "/api/v1/namespaces/n/pods"),
    )
    .await;
    assert!(out.is_none(), "admitted: {out:?}");
    // A parked POST takes it: a DELETE (mutating too) is rejected.
    let release = Arc::new(Notify::new());
    let a = app(1, 1, release.clone(), false);
    let out = extra_status(
        &a,
        &release,
        &[("POST", "/api/v1/namespaces/n/pods")],
        ("DELETE", "/api/v1/namespaces/n/pods/a"),
    )
    .await;
    assert_eq!(out.map(|o| o.0), Some(429));
}

#[tokio::test]
async fn zero_limit_leaves_that_kind_unlimited() {
    let release = Arc::new(Notify::new());
    let app = app(0, 1, release.clone(), false);
    // The mutating limit is 1 and held, but reads have no limit.
    let out = extra_status(
        &app,
        &release,
        &[("POST", "/api/v1/namespaces/n/pods")],
        ("GET", "/api/v1/namespaces/n/pods/a"),
    )
    .await;
    assert!(out.is_none(), "admitted: {out:?}");
    assert!(MaxInFlight::new(0, 0).is_none());
}

#[tokio::test]
async fn long_running_requests_are_neither_counted_nor_rejected() {
    // The one read-only slot is taken by a plain GET; each of these still
    // gets through, because `BasicLongRunningRequestCheck` exempts them.
    for uri in [
        "/api/v1/namespaces/n/pods?watch=true",
        "/api/v1/namespaces/n/pods/a/exec?command=sh",
        "/api/v1/namespaces/n/pods/a/attach",
        "/api/v1/namespaces/n/pods/a/portforward",
        "/api/v1/namespaces/n/pods/a/log?follow=true",
    ] {
        let release = Arc::new(Notify::new());
        let app = app(1, 1, release.clone(), false);
        let out = extra_status(
            &app,
            &release,
            &[("GET", "/api/v1/namespaces/n/pods/a")],
            ("GET", uri),
        )
        .await;
        assert!(out.is_none(), "{uri} admitted: {out:?}");
    }
}

#[tokio::test]
async fn long_running_requests_do_not_take_a_slot() {
    let release = Arc::new(Notify::new());
    let app = app(1, 1, release.clone(), false);
    let out = extra_status(
        &app,
        &release,
        &[("GET", "/api/v1/namespaces/n/pods?watch=true")],
        ("GET", "/api/v1/namespaces/n/pods/a"),
    )
    .await;
    assert!(out.is_none(), "admitted: {out:?}");
}

#[tokio::test]
async fn system_masters_are_never_rejected() {
    let release = Arc::new(Notify::new());
    let app = app(1, 1, release.clone(), true);
    let out = extra_status(
        &app,
        &release,
        &[("GET", "/api/v1/namespaces/n/pods/a")],
        ("GET", "/api/v1/namespaces/n/pods/b"),
    )
    .await;
    assert!(out.is_none(), "admitted: {out:?}");
}

#[test]
fn negative_limits_are_rejected() {
    assert!(validate_limits(400, 200).is_empty());
    assert!(validate_limits(0, 0).is_empty());
    assert_eq!(
        validate_limits(-1, -1),
        vec![
            "--max-requests-inflight can not be negative value".to_string(),
            "--max-mutating-requests-inflight can not be negative value".to_string()
        ]
    );
}
