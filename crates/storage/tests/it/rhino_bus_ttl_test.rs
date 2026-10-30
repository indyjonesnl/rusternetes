//! #3027: a key reaped by rhino's TTL loop must surface as `Deleted` on the
//! in-process bus, and the TTL loop must run without any native `watch()`.
//!
//! Upstream: lease expiry in etcd is an ordinary DELETE watch event
//! (`staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go` `parseEvent`),
//! and kine emits real delete rows from its TTL loop. With the bus attached
//! `watch()` never reaches the backend, so neither the lazily-started TTL loop
//! nor its deletes were seen.
#![cfg(feature = "sqlite")]

use futures::StreamExt;
use rusternetes_storage::{Storage, StorageBackend, StorageConfig, WatchEvent};
use std::time::Duration;

#[tokio::test]
async fn ttl_expiry_publishes_deleted_on_bus() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ttl.sqlite").to_string_lossy().into_owned();
    let mut backend = StorageBackend::new(StorageConfig::Sqlite { path })
        .await
        .expect("backend");
    backend.enable_event_bus();

    let key = "/registry/events/default/e1";
    let mut stream = backend.watch("/registry/events/").await.expect("watch");
    let obj = serde_json::json!({"metadata": {"name": "e1", "namespace": "default"}});
    let _: serde_json::Value = backend
        .create_with_ttl(key, &obj, 1)
        .await
        .expect("create_with_ttl");

    let added = stream.next().await.expect("event").expect("ok");
    assert!(matches!(added, WatchEvent::Added(ref k, _) if k == key));

    let deleted = tokio::time::timeout(Duration::from_secs(6), stream.next())
        .await
        .expect("TTL expiry never reached the bus (#3027)")
        .expect("event")
        .expect("ok");
    match deleted {
        WatchEvent::Deleted(ref k, ref prev) => {
            assert_eq!(k, key);
            assert!(prev.contains("\"name\":\"e1\""), "prev carried: {prev}");
        }
        other => panic!("expected Deleted, got {other:?}"),
    }
}
