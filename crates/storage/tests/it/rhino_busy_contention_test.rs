//! #3877 / #2344: `retry_busy` against REAL SQLite lock contention.
//!
//! A second connection holds the WAL write lock (`BEGIN IMMEDIATE`) while the
//! storage writes. rhino's `busy_timeout` is shortened via
//! `SqliteConfig::busy_timeout` so the test does not wait out the 30s default
//! (kine `pkg/drivers/sqlite/sqlite.go` DefaultParams `_busy_timeout=30000`).
//!
//! Upstream mapping an exhausted retry must produce: apiserver
//! `storage/errors/storage.go` -> `errors.NewServerTimeout(resource, op, 2)`
//! (apimachinery `api/errors/errors.go` `NewServerTimeout`): reason
//! `ServerTimeout`, HTTP 500, `details.retryAfterSeconds = 2`.
#![cfg(feature = "sqlite")]

use rhino::SqliteConfig;
use rusternetes_common::Error;
use rusternetes_storage::rhino::RhinoStorage;
use rusternetes_storage::Storage;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};
use std::path::Path;
use std::time::Duration;

/// Opening a fresh database (WAL pragma, startup VACUUM connection still
/// closing, schema setup) can itself see "database is locked" within the
/// shortened 50ms `busy_timeout` when the runner's disk stalls: nothing else is
/// contending, rhino's own open sequence is. That is open-time noise, not the
/// contention under test. So create and initialise the file once under the
/// default busy timeout (which waits the stall out), let that handle close,
/// then reopen with the short timeout; the reopen only has to attach to an
/// initialised WAL database. The reopen is still retried a bounded number of
/// times for the connection that is closing in the background.
async fn storage(path: &Path) -> RhinoStorage<rhino::SqliteBackend> {
    let dsn = path.to_string_lossy().into_owned();
    drop(
        RhinoStorage::with_config(SqliteConfig {
            dsn: dsn.clone(),
            compact_interval: Duration::ZERO,
            ..Default::default()
        })
        .await
        .expect("initialise the database under the default busy timeout"),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut last = None;
    for _ in 0..10 {
        match RhinoStorage::with_config(SqliteConfig {
            dsn: dsn.clone(),
            compact_interval: Duration::ZERO,
            busy_timeout: Duration::from_millis(50),
            ..Default::default()
        })
        .await
        {
            Ok(st) => return st,
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("storage: {:?}", last.unwrap());
}

async fn hold_write_lock(path: &Path) -> SqliteConnection {
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .connect()
        .await
        .expect("holder connection");
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .expect("take write lock");
    conn
}

#[tokio::test]
async fn exhausted_retry_surfaces_server_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.sqlite");
    let st = storage(&path).await;
    let mut holder = hold_write_lock(&path).await;

    let obj = serde_json::json!({"metadata": {"name": "a", "namespace": "default"}});
    let err = st
        .create::<serde_json::Value>("/registry/configmaps/default/a", &obj)
        .await
        .expect_err("write must fail while another connection holds the lock");

    match err {
        Error::Status(status) => {
            assert_eq!(status.reason.as_deref(), Some("ServerTimeout"));
            assert_eq!(status.code, Some(500));
            assert_eq!(
                status.details.as_ref().and_then(|d| d.retry_after_seconds),
                Some(2)
            );
        }
        other => panic!("expected ServerTimeout status, got {other:?}"),
    }

    sqlx::query("ROLLBACK").execute(&mut holder).await.unwrap();
}

#[tokio::test]
async fn retry_succeeds_once_lock_is_released() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.sqlite");
    let st = storage(&path).await;
    let mut holder = hold_write_lock(&path).await;

    // Release after the first attempt(s) have already hit the 50ms
    // busy_timeout; the default policy (5 attempts, 100ms linear step) keeps
    // retrying for about a second, so the write must land.
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sqlx::query("ROLLBACK").execute(&mut holder).await.unwrap();
        holder
    });

    let obj = serde_json::json!({"metadata": {"name": "b", "namespace": "default"}});
    st.create::<serde_json::Value>("/registry/configmaps/default/b", &obj)
        .await
        .expect("retry_busy must ride out a short lock");

    let _holder = release.await.unwrap();
    let got: serde_json::Value = st.get("/registry/configmaps/default/b").await.unwrap();
    assert_eq!(got["metadata"]["name"], "b");
}
