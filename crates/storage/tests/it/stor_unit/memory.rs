//! Unit tests moved from `src/memory.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::{Storage, WatchEvent, WatchStream};
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use std::collections::HashMap;
    pub use std::collections::VecDeque;
    pub use std::sync::atomic::{AtomicUsize, Ordering};
    pub use std::sync::{Arc, Mutex, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::memory::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestResource {
        name: String,
        value: i32,
    }

    fn rv_of(v: &serde_json::Value) -> i64 {
        v["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }

    fn cm(name: &str) -> serde_json::Value {
        serde_json::json!({"metadata": {"name": name}})
    }

    /// Replay then live, in order, with no duplicate at the seam (#2239).
    #[tokio::test]
    async fn watch_from_revision_replays_then_goes_live() {
        use futures::StreamExt;
        let s = MemoryStorage::new();
        let a: serde_json::Value = s.create("/r/cm/a", &cm("a")).await.unwrap();
        let _b: serde_json::Value = s.create("/r/cm/b", &cm("b")).await.unwrap();
        let _o: serde_json::Value = s.create("/r/other/x", &cm("x")).await.unwrap();
        let mut w = s
            .watch_from_revision("/r/cm/", rv_of(&a) + 1)
            .await
            .unwrap();
        let _c: serde_json::Value = s.create("/r/cm/c", &cm("c")).await.unwrap();
        s.delete("/r/cm/b").await.unwrap();
        let mut got = vec![];
        for _ in 0..3 {
            got.push(w.next().await.unwrap().unwrap());
        }
        assert!(matches!(&got[0], WatchEvent::Added(k, _) if k == "/r/cm/b"));
        assert!(matches!(&got[1], WatchEvent::Added(k, _) if k == "/r/cm/c"));
        assert!(matches!(&got[2], WatchEvent::Deleted(k, _) if k == "/r/cm/b"));
    }

    /// A DELETED event carries the delete's revision, not the object's last
    /// write (etcd3 `parseEvent` stamps `e.rev`), so a watch snapshot can
    /// dedupe it by rv (#2223).
    #[tokio::test]
    async fn deleted_event_carries_the_delete_revision() {
        use futures::StreamExt;
        let s = MemoryStorage::new();
        let a: serde_json::Value = s.create("/r/cm/a", &cm("a")).await.unwrap();
        let mut w = s.watch("/r/cm/").await.unwrap();
        s.delete("/r/cm/a").await.unwrap();
        let Some(Ok(WatchEvent::Deleted(_, prev))) = w.next().await else {
            panic!("expected Deleted");
        };
        let prev: serde_json::Value = serde_json::from_str(&prev).unwrap();
        assert!(rv_of(&prev) > rv_of(&a));
        assert_eq!(rv_of(&prev) as i64, s.current_revision().await.unwrap());
    }

    /// A resume point older than the retained history is "too old resource
    /// version" (`cacher/watch_cache.go:915-916`), surfaced as `Gone`.
    #[tokio::test]
    async fn watch_from_revision_too_old_is_gone() {
        let s = MemoryStorage::with_history_capacity(2);
        let a: serde_json::Value = s.create("/r/cm/a", &cm("a")).await.unwrap();
        for n in ["b", "c", "d"] {
            let _: serde_json::Value = s.create(&format!("/r/cm/{n}"), &cm(n)).await.unwrap();
        }
        // History holds only c and d; a resume at a+1 (== b) needs the evicted b.
        let err = s
            .watch_from_revision("/r/cm/", rv_of(&a) + 1)
            .await
            .err()
            .expect("must be too old");
        assert!(matches!(err, Error::Gone(m) if m.contains("too old resource version")));
        // c's revision is still covered.
        let c_rv = rv_of(&s.get::<serde_json::Value>("/r/cm/c").await.unwrap());
        assert!(s.watch_from_revision("/r/cm/", c_rv).await.is_ok());
    }

    #[tokio::test]
    async fn watch_from_compacted_revision_is_gone() {
        let s = MemoryStorage::new();
        let a: serde_json::Value = s.create("/r/cm/a", &cm("a")).await.unwrap();
        let _: serde_json::Value = s
            .create("/r/b", &serde_json::json!({"metadata": {"name": "b"}}))
            .await
            .unwrap();
        s.compact_to(rv_of(&a));
        assert!(matches!(
            s.watch_from_revision("/r/cm/", rv_of(&a)).await.err(),
            Some(Error::Gone(_))
        ));
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();
        let retrieved: TestResource = storage.get("/test/key").await.unwrap();

        assert_eq!(resource, retrieved);
    }

    #[tokio::test]
    async fn test_create_duplicate_fails() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();
        let result = storage.create("/test/key", &resource).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();

        let updated = TestResource {
            name: "updated".to_string(),
            value: 100,
        };
        storage.update("/test/key", &updated).await.unwrap();

        let retrieved: TestResource = storage.get("/test/key").await.unwrap();
        assert_eq!(updated, retrieved);
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = MemoryStorage::new();
        let resource = TestResource {
            name: "test".to_string(),
            value: 42,
        };

        storage.create("/test/key", &resource).await.unwrap();
        storage.delete("/test/key").await.unwrap();

        let result: Result<TestResource> = storage.get("/test/key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let storage = MemoryStorage::new();

        storage
            .create(
                "/test/ns1/pod1",
                &TestResource {
                    name: "pod1".to_string(),
                    value: 1,
                },
            )
            .await
            .unwrap();
        storage
            .create(
                "/test/ns1/pod2",
                &TestResource {
                    name: "pod2".to_string(),
                    value: 2,
                },
            )
            .await
            .unwrap();
        storage
            .create(
                "/test/ns2/pod3",
                &TestResource {
                    name: "pod3".to_string(),
                    value: 3,
                },
            )
            .await
            .unwrap();

        let ns1_pods: Vec<TestResource> = storage.list("/test/ns1/").await.unwrap();
        assert_eq!(ns1_pods.len(), 2);

        let all_pods: Vec<TestResource> = storage.list("/test/").await.unwrap();
        assert_eq!(all_pods.len(), 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = MemoryStorage::new();

        storage
            .create(
                "/test/key1",
                &TestResource {
                    name: "test1".to_string(),
                    value: 1,
                },
            )
            .await
            .unwrap();
        storage
            .create(
                "/test/key2",
                &TestResource {
                    name: "test2".to_string(),
                    value: 2,
                },
            )
            .await
            .unwrap();

        assert_eq!(storage.len(), 2);

        storage.clear();

        assert_eq!(storage.len(), 0);
        assert!(storage.is_empty());
    }

    /// `patch_strategic_merge` applies a delta to the LIVE object: fields the
    /// patch does not name (here a label another writer set after the caller's
    /// read) survive.
    #[tokio::test]
    async fn patch_strategic_merge_applies_delta_to_live_object() {
        let s = MemoryStorage::new();
        let _: serde_json::Value = s
            .create(
                "/r/nodes/n1",
                &serde_json::json!({"metadata": {"name": "n1", "labels": {"a": "1"}}, "spec": {"unschedulable": true}}),
            )
            .await
            .unwrap();
        // A concurrent writer adds a label.
        let mut live: serde_json::Value = s.get("/r/nodes/n1").await.unwrap();
        live["metadata"]["labels"]["b"] = "2".into();
        let _: serde_json::Value = s.update("/r/nodes/n1", &live).await.unwrap();

        let patched: serde_json::Value = s
            .patch_strategic_merge(
                "/r/nodes/n1",
                &serde_json::json!({"spec": {"podCIDR": "10.0.0.0/24", "podCIDRs": ["10.0.0.0/24"]}}),
            )
            .await
            .unwrap();
        assert_eq!(patched["spec"]["podCIDR"], "10.0.0.0/24");
        assert_eq!(patched["spec"]["unschedulable"], true);
        assert_eq!(patched["metadata"]["labels"]["b"], "2");
        let stored: serde_json::Value = s.get("/r/nodes/n1").await.unwrap();
        assert_eq!(stored["spec"]["podCIDRs"][0], "10.0.0.0/24");
    }

    #[tokio::test]
    async fn patch_strategic_merge_missing_object_is_not_found() {
        let s = MemoryStorage::new();
        let err = s
            .patch_strategic_merge::<serde_json::Value>(
                "/r/nodes/nope",
                &serde_json::json!({"spec": {}}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }

    /// `list_at_revision` rebuilds the collection as it stood at a past
    /// revision: etcd's `Range(WithRev(rev))`, which `GetList` issues for
    /// `resourceVersionMatch=Exact` (`etcd3/store.go` GetList, `withRev` from
    /// `ValidateListOptions`, `storage/interfaces.go:374-375`). Ported from
    /// `RunTestList` "resource version of second write, match=Exact"
    /// (`storage/testing/store_tests.go:1580`).
    #[tokio::test]
    async fn list_at_revision_reads_the_collection_as_of_that_revision() {
        let s = MemoryStorage::new();
        let names = |l: Vec<serde_json::Value>| -> Vec<String> {
            l.iter()
                .map(|v| v["metadata"]["name"].as_str().unwrap().to_string())
                .collect()
        };
        let obj = |n: &str| serde_json::json!({"metadata": {"name": n}, "data": "v1"});

        let a: serde_json::Value = s.create("/r/a", &obj("a")).await.unwrap();
        let b: serde_json::Value = s.create("/r/b", &obj("b")).await.unwrap();
        let mut a2 = a.clone();
        a2["data"] = "v2".into();
        let a2: serde_json::Value = s.update("/r/a", &a2).await.unwrap();
        s.delete("/r/b").await.unwrap();
        let _c: serde_json::Value = s.create("/r/c", &obj("c")).await.unwrap();

        let at = |r: i64| {
            let s = s.clone();
            async move {
                s.list_at_revision::<serde_json::Value>("/r/", r)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(names(at(rv_of(&a)).await), ["a"]);
        let l = at(rv_of(&b)).await;
        assert_eq!(names(l.clone()), ["a", "b"]);
        assert_eq!(l[0]["data"], "v1");
        // After the update: a is v2, b still there.
        let l = at(rv_of(&a2)).await;
        assert_eq!(names(l.clone()), ["a", "b"]);
        assert_eq!(l[0]["data"], "v2");
        // After the delete: b is gone, c not yet created.
        assert_eq!(names(at(rv_of(&a2) + 1).await), ["a"]);
        // Now.
        let now = s.current_revision().await.unwrap();
        assert_eq!(names(at(now).await), ["a", "c"]);
    }

    /// A revision older than the retained history, or at/below a compaction,
    /// cannot be rebuilt: 410 `The resourceVersion for the provided list is
    /// too old.` (`etcd3/errors.go:53,68-72`, `interpretListError`).
    #[tokio::test]
    async fn list_at_revision_below_history_or_compaction_is_gone() {
        let s = MemoryStorage::with_history_capacity(2);
        for n in ["a", "b", "c", "d"] {
            let _: serde_json::Value = s
                .create(
                    &format!("/r/{n}"),
                    &serde_json::json!({"metadata": {"name": n}}),
                )
                .await
                .unwrap();
        }
        let err = s
            .list_at_revision::<serde_json::Value>("/r/", 2)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Gone(_)), "{err:?}");

        let s = MemoryStorage::new();
        let a: serde_json::Value = s
            .create("/r/a", &serde_json::json!({"metadata": {"name": "a"}}))
            .await
            .unwrap();
        let _: serde_json::Value = s
            .create("/r/b", &serde_json::json!({"metadata": {"name": "b"}}))
            .await
            .unwrap();
        s.compact_to(rv_of(&a));
        let err = s
            .list_at_revision::<serde_json::Value>("/r/", rv_of(&a))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Gone(_)), "{err:?}");
    }

    #[tokio::test]
    async fn stats_counts_objects_and_averages_their_size() {
        let s = MemoryStorage::new();
        for i in 0..3 {
            let v = serde_json::json!({"metadata": {"name": format!("p{i}")}});
            s.create(&format!("/registry/pods/ns/p{i}"), &v)
                .await
                .unwrap();
        }
        let v = serde_json::json!({"metadata": {"name": "n"}});
        s.create("/registry/nodes/n", &v).await.unwrap();
        let st = s.stats("/registry/pods/").await.unwrap();
        assert_eq!(st.object_count, 3);
        assert!(st.estimated_average_object_size_bytes > 0);
        assert_eq!(
            s.stats("/registry/none/").await.unwrap(),
            rusternetes_storage::ResourceStats::default()
        );
    }
}

mod preconditions_tests {
    use super::__parent::*;
    use rusternetes_storage::memory::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_with_preconditions_suite() {
        crate::stor_unit::lib::preconditions_suite::run_all(Arc::new(MemoryStorage::new())).await;
    }
}
