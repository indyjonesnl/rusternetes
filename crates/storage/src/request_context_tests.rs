//! `StorageBackend` observes the request context at its dispatch point.

use super::*;
use rusternetes_common::cancellation::{
    request_cancellation, request_cancellation_with_deadline, with_request_cancellation,
};
use serde_json::json;
use std::time::Duration;

fn is_timeout(e: &rusternetes_common::Error) -> bool {
    matches!(e, rusternetes_common::Error::Status(s) if s.code == Some(504))
}

// etcd3 store: a `ctx` that is done fails the call with `ctx.Err()` before it
// touches the backend.
#[tokio::test]
async fn done_context_fails_every_backend_call_without_touching_it() {
    let backend = StorageBackend::new_memory();
    let key = "/registry/configmaps/default/c";
    let (canceller, ctx) = request_cancellation();
    canceller.cancel();
    with_request_cancellation(ctx, async {
        let r: Result<serde_json::Value> =
            Storage::create(&backend, key, &json!({"metadata": {"name": "c"}})).await;
        assert!(is_timeout(&r.unwrap_err()));
        assert!(is_timeout(
            &Storage::list::<serde_json::Value>(&backend, "/registry/configmaps/")
                .await
                .unwrap_err()
        ));
        assert!(is_timeout(
            &Storage::delete(&backend, key).await.unwrap_err()
        ));
        assert!(is_timeout(
            &Storage::current_revision(&backend).await.unwrap_err()
        ));
    })
    .await;
    // Nothing was written.
    assert!(Storage::get::<serde_json::Value>(&backend, key)
        .await
        .is_err());
}

#[tokio::test]
async fn expired_deadline_fails_the_call() {
    let backend = StorageBackend::new_memory();
    let (_c, ctx) = request_cancellation_with_deadline(Some(tokio::time::Instant::now()));
    tokio::time::sleep(Duration::from_millis(5)).await;
    let r = with_request_cancellation(ctx, Storage::current_revision(&backend)).await;
    assert!(is_timeout(&r.unwrap_err()));
}

#[tokio::test]
async fn live_context_and_no_context_pass_through() {
    let backend = StorageBackend::new_memory();
    let key = "/registry/configmaps/default/ok";
    let _: serde_json::Value = Storage::create(&backend, key, &json!({"metadata": {"name": "ok"}}))
        .await
        .unwrap();
    // No context in scope.
    assert!(Storage::get::<serde_json::Value>(&backend, key)
        .await
        .is_ok());
    let (_c, ctx) = request_cancellation_with_deadline(Some(
        tokio::time::Instant::now() + Duration::from_secs(30),
    ));
    let got = with_request_cancellation(ctx, Storage::get::<serde_json::Value>(&backend, key))
        .await
        .unwrap();
    assert_eq!(got["metadata"]["name"], "ok");
}

// A call blocked in the backend is abandoned the moment the context is
// cancelled (`clientv3` returns ctx.Err() mid-call).
#[tokio::test]
async fn cancel_abandons_a_blocked_call() {
    let (canceller, ctx) = request_cancellation();
    let r = with_request_cancellation(ctx, async {
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            canceller.cancel();
        });
        cancellable(std::future::pending::<Result<()>>()).await
    })
    .await;
    assert!(is_timeout(&r.unwrap_err()));
}
