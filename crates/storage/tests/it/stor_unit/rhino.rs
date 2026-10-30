//! Unit tests moved from `src/rhino.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rhino::backend::Backend;
    pub use rusternetes_common::{authz::AuthzStorage, Error, Result};
    pub use rusternetes_storage::busy::{map_backend_error, retry_busy, RetryPolicy};
    pub use rusternetes_storage::concurrency;
    pub use rusternetes_storage::{Storage, WatchEvent, WatchStream};
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{debug, error, info};
}

mod busy_read_tests {
    //! Reads must ride out SQLite lock contention like writes do (#2344,
    //! follow-up to #1621). A backend whose reads fail `database is locked`
    //! a set number of times stands in for a writer holding BEGIN IMMEDIATE.
    use super::__parent::*;
    use rhino::backend::{Backend, BackendError, KeyValue, Result as BResult, WatchResult};
    use rusternetes_storage::rhino::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Flaky {
        /// Reads fail with "database is locked" while this is > 0.
        locked_reads: AtomicU32,
        calls: AtomicU32,
    }

    impl Flaky {
        fn new(locked_reads: u32) -> Self {
            Self {
                locked_reads: AtomicU32::new(locked_reads),
                calls: AtomicU32::new(0),
            }
        }
        fn gate(&self) -> BResult<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let left = self.locked_reads.load(Ordering::SeqCst);
            if left > 0 {
                self.locked_reads.store(left - 1, Ordering::SeqCst);
                return Err(BackendError::Internal("database is locked".into()));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Backend for Flaky {
        async fn start(&self) -> BResult<()> {
            Ok(())
        }
        async fn get(
            &self,
            key: &str,
            _: &str,
            _: i64,
            _: i64,
            _: bool,
        ) -> BResult<(i64, Option<KeyValue>)> {
            self.gate()?;
            Ok((
                7,
                Some(KeyValue {
                    key: key.to_string(),
                    value: br#"{"a":1}"#.to_vec(),
                    version: 1,
                    create_revision: 7,
                    mod_revision: 7,
                    lease: 0,
                }),
            ))
        }
        async fn create(&self, _: &str, _: &[u8], _: i64) -> BResult<i64> {
            unimplemented!()
        }
        async fn delete(&self, _: &str, _: i64) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn delete_prefix(&self, _: &str) -> BResult<(i64, i64, Vec<KeyValue>)> {
            unimplemented!()
        }
        async fn list(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
            _: bool,
        ) -> BResult<(i64, Vec<KeyValue>)> {
            self.gate()?;
            Ok((7, vec![]))
        }
        async fn count(&self, _: &str, _: &str, _: i64) -> BResult<(i64, i64)> {
            unimplemented!()
        }
        async fn update(
            &self,
            _: &str,
            _: &[u8],
            _: i64,
            _: i64,
        ) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn watch(&self, _: &str, _: i64) -> BResult<WatchResult> {
            unimplemented!()
        }
        async fn db_size(&self) -> BResult<i64> {
            Ok(0)
        }
        async fn current_revision(&self) -> BResult<i64> {
            self.gate()?;
            Ok(7)
        }
        async fn compact_revision(&self) -> BResult<i64> {
            self.gate()?;
            Ok(0)
        }
        async fn wait_for_sync_to(&self, _: i64) {}
        async fn compact(&self, r: i64) -> BResult<i64> {
            Ok(r)
        }
    }

    fn store(locked: u32) -> RhinoStorage<Flaky> {
        RhinoStorage {
            backend: Arc::new(Flaky::new(locked)),
            bus: None,
            sizes: Default::default(),
        }
    }

    fn assert_server_timeout(e: Error) {
        let Error::Status(s) = e else {
            panic!("want ServerTimeout Status, got {e:?}")
        };
        assert_eq!(s.reason.as_deref(), Some("ServerTimeout"));
    }

    #[tokio::test]
    async fn get_retries_through_lock() {
        let s = store(2);
        let v: serde_json::Value = Storage::get(&s, "/registry/pods/ns/p").await.unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(s.backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn get_exhausted_lock_is_server_timeout() {
        let s = store(1000);
        let e = Storage::get::<serde_json::Value>(&s, "/registry/pods/ns/p")
            .await
            .unwrap_err();
        assert_server_timeout(e);
    }

    #[tokio::test]
    async fn list_retries_through_lock() {
        let s = store(2);
        let v: Vec<serde_json::Value> = Storage::list(&s, "/registry/pods/").await.unwrap();
        assert!(v.is_empty());
        assert_eq!(s.backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn list_exhausted_lock_is_server_timeout() {
        let s = store(1000);
        let e = Storage::list::<serde_json::Value>(&s, "/registry/pods/")
            .await
            .unwrap_err();
        assert_server_timeout(e);
    }

    #[tokio::test]
    async fn current_revision_retries_through_lock() {
        let s = store(2);
        assert_eq!(s.current_revision().await.unwrap(), 7);
    }
}

mod stats_tests {
    //! `Storage::stats` must be a keys-only read plus a per-key size cache
    //! (#2869; upstream `storage/etcd3/stats.go` `resourceSizeEstimator.Stats`),
    //! not a list that decodes and re-encodes every object once a minute.
    use super::__parent::*;
    use futures::StreamExt;
    use rhino::backend::{Backend, Event, KeyValue, Result as BResult, WatchResult};
    use rusternetes_storage::rhino::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recording {
        /// Events the backend watch replays, one batch.
        watch_events: Mutex<Vec<Event>>,
        saw_keys_only: AtomicBool,
        saw_value_read: AtomicBool,
    }

    fn kv(key: &str, value: &[u8], rev: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_vec(),
            version: 1,
            create_revision: rev,
            mod_revision: rev,
            lease: 0,
        }
    }

    #[async_trait]
    impl Backend for Recording {
        async fn start(&self) -> BResult<()> {
            Ok(())
        }
        async fn get(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
            _: bool,
        ) -> BResult<(i64, Option<KeyValue>)> {
            unimplemented!()
        }
        async fn create(&self, _: &str, _: &[u8], _: i64) -> BResult<i64> {
            unimplemented!()
        }
        async fn delete(&self, _: &str, _: i64) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn delete_prefix(&self, _: &str) -> BResult<(i64, i64, Vec<KeyValue>)> {
            unimplemented!()
        }
        async fn list(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: i64,
            keys_only: bool,
        ) -> BResult<(i64, Vec<KeyValue>)> {
            if keys_only {
                self.saw_keys_only.store(true, Ordering::SeqCst);
            } else {
                self.saw_value_read.store(true, Ordering::SeqCst);
            }
            let value = |v: &'static [u8]| if keys_only { Vec::new() } else { v.to_vec() };
            Ok((
                9,
                vec![
                    kv(
                        "/registry/pods/ns/a",
                        &value(br#"{"metadata":{"name":"a"}}"#),
                        8,
                    ),
                    kv(
                        "/registry/pods/ns/b",
                        &value(br#"{"metadata":{"name":"b"}}"#),
                        9,
                    ),
                ],
            ))
        }
        async fn count(&self, _: &str, _: &str, _: i64) -> BResult<(i64, i64)> {
            unimplemented!()
        }
        async fn update(
            &self,
            _: &str,
            _: &[u8],
            _: i64,
            _: i64,
        ) -> BResult<(i64, Option<KeyValue>, bool)> {
            unimplemented!()
        }
        async fn watch(&self, _: &str, _: i64) -> BResult<WatchResult> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let batch = std::mem::take(&mut *self.watch_events.lock().unwrap());
            tx.send(batch).await.unwrap();
            Ok(WatchResult {
                current_revision: 9,
                compact_revision: 0,
                events: rx,
            })
        }
        async fn db_size(&self) -> BResult<i64> {
            Ok(0)
        }
        async fn current_revision(&self) -> BResult<i64> {
            Ok(9)
        }
        async fn compact_revision(&self) -> BResult<i64> {
            Ok(0)
        }
        async fn wait_for_sync_to(&self, _: i64) {}
        async fn compact(&self, r: i64) -> BResult<i64> {
            Ok(r)
        }
    }

    #[tokio::test]
    async fn stats_average_size_comes_from_what_a_list_already_read() {
        let s = RhinoStorage {
            backend: Arc::new(Recording::default()),
            bus: None,
            sizes: Default::default(),
        };
        let _: Vec<serde_json::Value> = Storage::list(&s, "/registry/pods/").await.unwrap();
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.object_count, 2);
        assert_eq!(
            st.estimated_average_object_size_bytes,
            br#"{"metadata":{"name":"a"}}"#.len() as i64
        );
    }

    #[tokio::test]
    async fn stats_reads_keys_only_and_never_the_values() {
        let s = RhinoStorage {
            backend: Arc::new(Recording::default()),
            bus: None,
            sizes: Default::default(),
        };
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.object_count, 2);
        assert!(s.backend.saw_keys_only.load(Ordering::SeqCst));
        assert!(
            !s.backend.saw_value_read.load(Ordering::SeqCst),
            "stats must not read object values"
        );
    }

    fn put(key: &str, value: &[u8], rev: i64, create: bool) -> Event {
        Event {
            delete: false,
            create,
            kv: kv(key, value, rev),
            prev_kv: None,
        }
    }

    async fn drain(s: &RhinoStorage<Recording>) {
        let mut w = Storage::watch_from_revision(s, "/registry/pods/", 1)
            .await
            .unwrap();
        while w.next().await.is_some() {}
    }

    /// Upstream feeds the estimator from watch events
    /// (`storage/etcd3/watcher.go:408-415`: `UpdateKey` on PUT, `DeleteKey`
    /// on DELETE), so a resource nobody lists/gets/writes still reports a
    /// size (#2991).
    #[tokio::test]
    async fn watch_put_event_feeds_the_size_estimator() {
        let value = br#"{"metadata":{"name":"a"},"spec":{}}"#;
        let b = Recording::default();
        b.watch_events
            .lock()
            .unwrap()
            .push(put("/registry/pods/ns/a", value, 10, true));
        let s = RhinoStorage {
            backend: Arc::new(b),
            bus: None,
            sizes: Default::default(),
        };
        drain(&s).await;
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.estimated_average_object_size_bytes, value.len() as i64);
    }

    #[tokio::test]
    async fn watch_delete_event_drops_the_key_from_the_size_estimator() {
        let value = br#"{"metadata":{"name":"a"},"spec":{}}"#;
        let b = Recording::default();
        b.watch_events.lock().unwrap().extend([
            put("/registry/pods/ns/a", value, 10, true),
            Event {
                delete: true,
                create: false,
                kv: kv("/registry/pods/ns/a", b"", 11),
                prev_kv: Some(kv("/registry/pods/ns/a", value, 10)),
            },
        ]);
        let s = RhinoStorage {
            backend: Arc::new(b),
            bus: None,
            sizes: Default::default(),
        };
        drain(&s).await;
        // Recording lists keys a and b; only b's size is unknown, and a's
        // delete must have evicted its cached size.
        let st = Storage::stats(&s, "/registry/pods/").await.unwrap();
        assert_eq!(st.estimated_average_object_size_bytes, 0);
    }
}

#[cfg(feature = "sqlite")]
mod preconditions_tests {
    use super::__parent::*;
    use rusternetes_storage::rhino::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_with_preconditions_suite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.db");
        let store = RhinoStorage::new(path.to_str().unwrap()).await.unwrap();
        crate::stor_unit::lib::preconditions_suite::run_all(Arc::new(store)).await;
    }
}
