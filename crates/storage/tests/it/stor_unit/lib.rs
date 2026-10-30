//! Unit tests moved from `src/lib.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::authz::AuthzStorage;
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::event_bus::EventBus;
    pub use rusternetes_storage::event_recorder::EventRecorder;
    pub use rusternetes_storage::memory::MemoryStorage;
    pub use rusternetes_storage::preconditions::Preconditions;
    pub use rusternetes_storage::workqueue::{
        extract_key, WorkQueue, WorkQueueConfig, RECONCILE_ALL_SENTINEL,
    };
    pub use serde::{de::DeserializeOwned, Serialize};
    pub use std::sync::Arc;
}

mod evict_tests {
    use super::__parent::*;
    use rusternetes_storage::*;
    use serde_json::json;

    /// Storage-mode eviction must behaviour-preservingly persist the mutated pod
    /// (deletionTimestamp + Evicted status) via a whole-object write — i.e.
    /// identical to the prior `update` path. #1284.
    #[tokio::test]
    async fn evict_pod_storage_mode_persists_mutated_pod() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/victim";
        let pod = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": "victim", "namespace": "default" },
            "status": { "phase": "Running" }
        });
        let _: serde_json::Value = Storage::create(&backend, key, &pod).await.unwrap();

        let mut evicted = pod.clone();
        evicted["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
        evicted["status"]["phase"] = json!("Failed");
        evicted["status"]["reason"] = json!("Evicted");

        backend.evict_pod(key, &evicted, 30).await.unwrap();

        let stored: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        assert_eq!(stored["status"]["phase"], "Failed");
        assert_eq!(stored["status"]["reason"], "Evicted");
        assert!(
            stored["metadata"]["deletionTimestamp"].is_string(),
            "deletionTimestamp must persist in storage mode: {stored}"
        );
    }
}

mod graceful_delete_tests {
    use super::__parent::*;
    use rusternetes_storage::*;
    use serde_json::json;

    fn running_pod(name: &str, grace: i64) -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": name, "namespace": "default" },
            "spec": { "terminationGracePeriodSeconds": grace },
            "status": { "phase": "Running" }
        })
    }

    /// Direct-store backends have no api-server in the path, so the graceful
    /// delete emulates one: stamp deletionTimestamp + the pod's own grace
    /// period and leave the object for the kubelet to drain. This is exactly
    /// what the StatefulSet/DaemonSet/taint-eviction controllers used to do
    /// inline, so their behaviour on the compose and all-in-one stacks is
    /// unchanged by moving them onto this call.
    #[tokio::test]
    async fn graceful_delete_stamps_deletion_timestamp_without_a_server() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/ss-2";
        let _: serde_json::Value = Storage::create(&backend, key, &running_pod("ss-2", 30))
            .await
            .unwrap();

        Storage::delete_gracefully(&backend, key).await.unwrap();

        let stored: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        assert!(
            stored["metadata"]["deletionTimestamp"].is_string(),
            "graceful delete must stamp deletionTimestamp: {stored}"
        );
        assert_eq!(
            stored["metadata"]["deletionGracePeriodSeconds"], 30,
            "grace must come from the pod's own terminationGracePeriodSeconds: {stored}"
        );
    }

    /// Re-issuing it is a no-op, not a second timestamp: controllers reconcile
    /// repeatedly and must not keep rewriting a pod that is already draining
    /// (rewriting it is what an upstream api-server rejects as immutable).
    #[tokio::test]
    async fn graceful_delete_is_idempotent() {
        let backend = StorageBackend::new_memory();
        let key = "/registry/pods/default/ss-1";
        let _: serde_json::Value = Storage::create(&backend, key, &running_pod("ss-1", 5))
            .await
            .unwrap();

        Storage::delete_gracefully(&backend, key).await.unwrap();
        let first: serde_json::Value = Storage::get(&backend, key).await.unwrap();
        Storage::delete_gracefully(&backend, key).await.unwrap();
        let second: serde_json::Value = Storage::get(&backend, key).await.unwrap();

        assert_eq!(
            first["metadata"]["deletionTimestamp"], second["metadata"]["deletionTimestamp"],
            "a second graceful delete must not re-stamp the timestamp"
        );
    }

    /// And an object that is already gone is success, not an error — the
    /// caller wanted it deleted.
    #[tokio::test]
    async fn graceful_delete_of_a_missing_object_succeeds() {
        let backend = StorageBackend::new_memory();
        Storage::delete_gracefully(&backend, "/registry/pods/default/never-existed")
            .await
            .expect("deleting an absent object is not an error");
    }
}

mod continue_token_tests {
    use super::__parent::*;
    use rusternetes_storage::*;

    /// A backend that has compacted every revision below `compacted_below`.
    ///
    /// `list_at_revision` fails the way a real etcd does for a read below the
    /// compaction point, so a `list_paginated` that reads before it checks for
    /// compaction surfaces that error instead of the `GoneWithContinue` the
    /// caller is owed. Above the compaction point it returns the *historical*
    /// two-object view, while `list` returns a three-object live view — the
    /// difference is what proves a page actually read at its pin instead of
    /// falling through to a live read — `list_at_revision` answers with the
    /// historical view at *any* uncompacted revision, the current one included,
    /// precisely so the two calls are distinguishable. Everything the test does
    /// not exercise is
    /// `unimplemented!()` on purpose — reaching it is a test bug.
    struct CompactingStore {
        compacted_below: i64,
        /// When false, `is_revision_compacted` reports "not compacted" for
        /// everything — the swallow (`Err(_) => Ok(false)`) that a real etcd
        /// probe performs on a transient failure. The pinned read then has to
        /// produce the 410 by itself.
        probe_reports_compaction: bool,
    }

    impl CompactingStore {
        fn new(compacted_below: i64) -> Self {
            Self {
                compacted_below,
                probe_reports_compaction: true,
            }
        }

        /// The historical (pinned) view: two objects.
        fn pinned_items() -> Vec<serde_json::Value> {
            vec![
                serde_json::json!({"metadata": {"namespace": "default", "name": "aaa"}}),
                serde_json::json!({"metadata": {"namespace": "default", "name": "bbb"}}),
            ]
        }

        /// The live view: the same two, bracketed by objects written after the
        /// pin. One sorts before the historical view and one after, so a page
        /// taken anywhere in the sequence can tell the two reads apart.
        fn live_items() -> Vec<serde_json::Value> {
            let mut v =
                vec![serde_json::json!({"metadata": {"namespace": "default", "name": "000-live"}})];
            v.extend(Self::pinned_items());
            v.push(serde_json::json!({"metadata": {"namespace": "default", "name": "zzz-live"}}));
            v
        }
    }

    #[async_trait]
    impl Storage for CompactingStore {
        async fn create<T>(&self, _key: &str, _value: &T) -> Result<T>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            unimplemented!("not exercised by these tests")
        }

        async fn get<T>(&self, _key: &str) -> Result<T>
        where
            T: DeserializeOwned + Send + Sync,
        {
            unimplemented!("not exercised by these tests")
        }

        async fn update<T>(&self, _key: &str, _value: &T) -> Result<T>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            unimplemented!("not exercised by these tests")
        }

        async fn update_raw(&self, _key: &str, _value: &serde_json::Value) -> Result<()> {
            unimplemented!("not exercised by these tests")
        }

        async fn delete(&self, _key: &str) -> Result<()> {
            unimplemented!("not exercised by these tests")
        }

        async fn list<T>(&self, _prefix: &str) -> Result<Vec<T>>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            Ok(serde_json::from_value(serde_json::Value::Array(Self::live_items())).unwrap())
        }

        async fn list_at_revision<T>(&self, _prefix: &str, revision: i64) -> Result<Vec<T>>
        where
            T: Serialize + DeserializeOwned + Send + Sync,
        {
            if revision < self.compacted_below {
                // Verbatim shape of what etcd returns below its compaction
                // point, wrapped the way `EtcdStorage::list_at_revision` does.
                return Err(Error::Storage(format!(
                    "Failed to list at revision {}: status: OutOfRange, message: \
                     \"etcdserver: mvcc: required revision has been compacted\"",
                    revision
                )));
            }
            Ok(serde_json::from_value(serde_json::Value::Array(Self::pinned_items())).unwrap())
        }

        async fn watch(&self, _prefix: &str) -> Result<WatchStream> {
            unimplemented!("not exercised by these tests")
        }

        async fn watch_from_revision(&self, _prefix: &str, _revision: i64) -> Result<WatchStream> {
            unimplemented!("not exercised by these tests")
        }

        async fn current_revision(&self) -> Result<i64> {
            Ok(1000)
        }

        async fn is_revision_compacted(&self, revision: i64) -> Result<bool> {
            Ok(self.probe_reports_compaction && revision < self.compacted_below)
        }
    }

    /// Regression: the compaction check must run BEFORE the pinned read.
    ///
    /// Pinning the page sequence to the first page's revision means a strict
    /// continue token can outlive its revision. When it does, the caller is
    /// owed a 410 Expired carrying a fresh `rv = -1` token so the walk can
    /// resume (inconsistently) at the current revision — upstream
    /// `handleCompactedErrorForPaging` (etcd3/errors.go). Reading at the
    /// compacted revision first turns that into a backend error, i.e. a 500.
    #[tokio::test]
    async fn a_compacted_token_returns_gone_with_a_resumable_token() {
        let store = CompactingStore::new(100);
        let token = encode_default_token("default/foo", 42);

        let err = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            2,
            Some(&token),
        )
        .await
        .expect_err("a compacted continue token must not succeed");

        match err {
            Error::GoneWithContinue { continue_token, .. } => assert_eq!(
                continue_token,
                encode_default_token("default/foo", INCONSISTENT_CONTINUE_RV),
                "the 410 must carry a fresh inconsistent token at the same start key"
            ),
            other => panic!(
                "expected GoneWithContinue, got {other:?} — the pinned read ran \
                 before the compaction check and buried the 410"
            ),
        }
    }

    /// Regression: a compacted revision that the *probe* misses must still
    /// yield the same resumable 410.
    ///
    /// `is_revision_compacted` swallows its own errors — `unwrap_or(false)` at
    /// the call site, `Err(_) => Ok(false)` in `EtcdStorage` — deliberately, so
    /// a transient blip does not turn every list into a 410. The cost is that a
    /// genuinely compacted revision can slip past the probe and reach the
    /// pinned read. Before the pin existed, that fell through to a live `list`
    /// that simply succeeded; now it hits `list_at_revision`, whose backend
    /// error would surface as a 500 unless the call site recognises it.
    #[tokio::test]
    async fn a_compaction_the_probe_missed_still_returns_gone() {
        let store = CompactingStore {
            compacted_below: 100,
            probe_reports_compaction: false,
        };
        let token = encode_default_token("default/foo", 42);

        let err = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            2,
            Some(&token),
        )
        .await
        .expect_err("reading at a compacted revision must not succeed");

        match err {
            Error::GoneWithContinue { continue_token, .. } => assert_eq!(
                continue_token,
                encode_default_token("default/foo", INCONSISTENT_CONTINUE_RV),
                "the probe and the read must produce an identical 410 + resume token"
            ),
            other => panic!(
                "expected GoneWithContinue, got {other:?} — a compacted pinned \
                 read leaked the backend error as a 500"
            ),
        }
    }

    /// The `rv = -1` token issued by that 410 skips the compaction check and
    /// reads at the current revision, so the resumed walk completes.
    #[tokio::test]
    async fn an_inconsistent_token_resumes_at_the_current_revision() {
        let store = CompactingStore::new(100);
        let token = encode_default_token("default/aaa", INCONSISTENT_CONTINUE_RV);

        let (items, next) = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            10,
            Some(&token),
        )
        .await
        .expect("an inconsistent token must read at the latest rv, not fail on compaction");

        // Read at `current_revision()` (1000), which is above the compaction
        // point, so the walk completes from the recorded start key.
        assert_eq!(items.len(), 2, "resumed walk should complete: {items:?}");
        assert!(next.is_none());
    }

    /// And a token whose revision is still served does take the pinned read —
    /// the reorder must not have quietly dropped the pin.
    ///
    /// The fixture's live view carries an extra `zzz-live` object that the
    /// historical view does not, so a page containing it proves the read fell
    /// through to `list` instead of `list_at_revision`.
    #[tokio::test]
    async fn a_live_token_still_reads_at_its_pinned_revision() {
        let store = CompactingStore::new(5);
        let token = encode_default_token("default/aaa", 42);

        let (items, next) = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            10,
            Some(&token),
        )
        .await
        .expect("an uncompacted pinned token must read at its own revision");

        let names: Vec<&str> = items
            .iter()
            .map(|v| v["metadata"]["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            ["aaa", "bbb"],
            "a pinned page must not see objects written after the pin"
        );
        assert!(next.is_none());
    }

    /// Regression: the outgoing token must carry the SAME revision the incoming
    /// one did.
    ///
    /// Re-deriving it from `current_revision()` per page re-pins the sequence to
    /// "now" one page at a time — a live view wearing a snapshot's clothes. The
    /// end-to-end proof of that lives in the contract suite's `list_paging`,
    /// which only runs where Docker does; this covers the carry-forward on
    /// every runner.
    #[tokio::test]
    async fn the_outgoing_token_carries_the_incoming_pin() {
        let store = CompactingStore::new(5);
        let token = encode_default_token("default/aaa", 42);

        let (items, next) = Storage::list_paginated::<serde_json::Value>(
            &store,
            "/registry/pods/",
            1,
            Some(&token),
        )
        .await
        .expect("pinned read failed");

        assert_eq!(items.len(), 1);
        let next = next.expect("two items and limit=1 must leave a second page");
        assert_eq!(
            next,
            encode_default_token("default/bbb", 42),
            "the pin must be threaded through, not re-read from current_revision() \
             (which this fixture reports as 1000)"
        );
    }

    /// And the first page establishes the pin from a revision read BEFORE the
    /// data, so the token can never name a revision newer than the page it
    /// describes. `current_revision()` is 1000 here and the data comes from
    /// `list_at_revision(1000)`, never the live `list`.
    #[tokio::test]
    async fn the_first_page_pins_the_revision_it_read_at() {
        let store = CompactingStore::new(5);

        let (items, next) =
            Storage::list_paginated::<serde_json::Value>(&store, "/registry/pods/", 1, None)
                .await
                .expect("first page failed");

        assert_eq!(items[0]["metadata"]["name"], "aaa");
        assert_eq!(
            next.expect("a second page is owed"),
            encode_default_token("default/bbb", 1000),
            "the first page must stamp the revision it was read at"
        );
    }
}

pub mod preconditions_suite {
    //! Backend-generic conformance suite for `Storage::delete_with_preconditions`,
    //! ported from upstream `storage/testing/store_tests.go`
    //! (`RunTestPreconditionalDeleteWithSuggestion`,
    //! `RunTestPreconditionalDeleteWithOnlySuggestionPass`) and
    //! `etcd3/store_test.go` `TestPreconditions`. Each backend's test module calls
    //! `run_all` with a fresh-store constructor.

    use rusternetes_common::Error;
    use rusternetes_storage::{Preconditions, Storage};
    use serde_json::{json, Value};
    use std::sync::Arc;

    const KEY: &str = "/registry/pods/test-ns/name";

    async fn seed<S: Storage>(s: &S) -> Value {
        s.create(
            KEY,
            &json!({"apiVersion":"v1","kind":"Pod",
                    "metadata":{"name":"name","namespace":"test-ns"}}),
        )
        .await
        .unwrap()
    }

    async fn set_uid<S: Storage>(s: &S, uid: &str) -> Value {
        let mut cur: Value = s.get(KEY).await.unwrap();
        cur["metadata"]["uid"] = json!(uid);
        s.update(KEY, &cur).await.unwrap()
    }

    async fn exists<S: Storage>(s: &S) -> bool {
        s.get::<Value>(KEY).await.is_ok()
    }

    fn rv_of(v: &Value) -> String {
        v["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Runs every case against one shared store (cases clean up their own key,
    /// so a single etcd container can serve the whole suite).
    pub(crate) async fn run_all<S: Storage + 'static>(s: Arc<S>) {
        let _ = s.delete(KEY).await;
        matching_uid_deletes(&*s).await;
        mismatching_uid_conflicts_and_keeps_object(&*s).await;
        let _ = s.delete(KEY).await;
        matching_resource_version_deletes_and_stale_conflicts(&*s).await;
        both_preconditions_must_hold(&*s).await;
        missing_key_is_not_found(&*s).await;
        empty_preconditions_delete_unconditionally(&*s).await;
        racing_uid_change_never_deletes_the_new_object(s.clone()).await;
        let _ = s.delete(KEY).await;
        patch_with_preconditions(&*s).await;
        racing_recreate_never_patches_the_new_object(s.clone()).await;
        let _ = s.delete(KEY).await;
    }

    /// `GuaranteedUpdate` with `Preconditions` (`etcd3/store.go:463`, `:500-517`)
    /// driven through `patch_strategic_merge_with_preconditions`.
    async fn patch_with_preconditions<S: Storage>(s: &S) {
        let _ = s.delete(KEY).await;
        seed(s).await;
        let cur = set_uid(s, "myUID").await;
        let patch = json!({"metadata": {"labels": {"k": "v"}}});

        // Mismatching UID: Conflict, object untouched.
        let err = s
            .patch_strategic_merge_with_preconditions::<Value>(
                KEY,
                &patch,
                &Preconditions::new_uid("other"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err:?}");
        let now: Value = s.get(KEY).await.unwrap();
        assert!(now["metadata"]["labels"].is_null());

        // Stale resourceVersion: Conflict.
        let err = s
            .patch_strategic_merge_with_preconditions::<Value>(
                KEY,
                &patch,
                &Preconditions::new_resource_version("1"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err:?}");

        // Matching UID + resourceVersion: patched.
        let both = Preconditions {
            uid: Some("myUID".into()),
            resource_version: Some(rv_of(&cur)),
        };
        let out = s
            .patch_strategic_merge_with_preconditions::<Value>(KEY, &patch, &both)
            .await
            .unwrap();
        assert_eq!(out["metadata"]["labels"]["k"], "v");
        assert_eq!(out["metadata"]["uid"], "myUID");

        // Empty preconditions patch unconditionally; missing key is NotFound.
        s.patch_strategic_merge_with_preconditions::<Value>(
            KEY,
            &json!({"metadata": {"labels": {"j": "w"}}}),
            &Preconditions::default(),
        )
        .await
        .unwrap();
        s.delete(KEY).await.unwrap();
        let err = s
            .patch_strategic_merge_with_preconditions::<Value>(
                KEY,
                &patch,
                &Preconditions::new_uid("myUID"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }

    /// The object is deleted and recreated (new UID) while a UID-guarded patch is
    /// in flight: the patch may land on the old generation or be rejected, but it
    /// must never land on the new one.
    async fn racing_recreate_never_patches_the_new_object<S: Storage + 'static>(s: Arc<S>) {
        for i in 0..50 {
            let _ = s.delete(KEY).await;
            seed(&*s).await;
            set_uid(&*s, "old").await;
            let r = {
                let s = s.clone();
                tokio::spawn(async move {
                    s.delete(KEY).await?;
                    seed(&*s).await;
                    set_uid(&*s, "new").await;
                    Ok::<_, Error>(())
                })
            };
            let p = {
                let s = s.clone();
                tokio::spawn(async move {
                    s.patch_strategic_merge_with_preconditions::<Value>(
                        KEY,
                        &json!({"metadata": {"labels": {"k": "v"}}}),
                        &Preconditions::new_uid("old"),
                    )
                    .await
                })
            };
            let _ = r.await.unwrap();
            let _ = p.await.unwrap();
            if let Ok(now) = s.get::<Value>(KEY).await {
                if now["metadata"]["uid"] == "new" {
                    assert!(
                        now["metadata"]["labels"].is_null(),
                        "iter {i}: patch for UID old landed on the recreated object"
                    );
                }
            }
        }
    }

    /// `RunTestPreconditionalDeleteWithSuggestion`.
    async fn matching_uid_deletes<S: Storage>(s: &S) {
        seed(s).await;
        set_uid(s, "myUID").await;
        s.delete_with_preconditions(KEY, &Preconditions::new_uid("myUID"))
            .await
            .unwrap();
        assert!(matches!(s.get::<Value>(KEY).await, Err(Error::NotFound(_))));
    }

    /// `RunTestPreconditionalDeleteWithOnlySuggestionPass`: the object has moved
    /// to another UID, so the delete must fail the precondition and leave the
    /// object intact (409 via `InterpretDeleteError`).
    async fn mismatching_uid_conflicts_and_keeps_object<S: Storage>(s: &S) {
        seed(s).await;
        let updated = set_uid(s, "otherUID").await;
        let err = s
            .delete_with_preconditions(KEY, &Preconditions::new_uid("myUID"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "got {err:?}");
        let now: Value = s.get(KEY).await.unwrap();
        assert_eq!(now, updated, "object must be untouched");
    }

    async fn matching_resource_version_deletes_and_stale_conflicts<S: Storage>(s: &S) {
        let created = seed(s).await;
        let stale_rv = rv_of(&created);
        let cur_rv = rv_of(&set_uid(s, "u").await);
        assert_ne!(stale_rv, cur_rv);
        let err = s
            .delete_with_preconditions(KEY, &Preconditions::new_resource_version(stale_rv))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "got {err:?}");
        assert!(exists(s).await);
        s.delete_with_preconditions(KEY, &Preconditions::new_resource_version(cur_rv))
            .await
            .unwrap();
        assert!(!exists(s).await);
    }

    async fn both_preconditions_must_hold<S: Storage>(s: &S) {
        seed(s).await;
        let rv = rv_of(&set_uid(s, "u").await);
        let bad = Preconditions {
            uid: Some("u".into()),
            resource_version: Some("0".into()),
        };
        assert!(matches!(
            s.delete_with_preconditions(KEY, &bad).await,
            Err(Error::Conflict(_))
        ));
        assert!(exists(s).await);
        let good = Preconditions {
            uid: Some("u".into()),
            resource_version: Some(rv),
        };
        s.delete_with_preconditions(KEY, &good).await.unwrap();
        assert!(!exists(s).await);
    }

    async fn missing_key_is_not_found<S: Storage>(s: &S) {
        let err = s
            .delete_with_preconditions(KEY, &Preconditions::new_uid("x"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    async fn empty_preconditions_delete_unconditionally<S: Storage>(s: &S) {
        seed(s).await;
        s.delete_with_preconditions(KEY, &Preconditions::default())
            .await
            .unwrap();
        assert!(!exists(s).await);
    }

    /// The atomicity the get+compare+delete emulation lacked: a writer that
    /// changes the UID concurrently must never lose its object to a delete keyed
    /// on the OLD uid. Either the delete wins (object gone) or the writer wins
    /// (delete is a Conflict, object stays with the new UID).
    async fn racing_uid_change_never_deletes_the_new_object<S: Storage + 'static>(s: Arc<S>) {
        for i in 0..50 {
            let _ = s.delete(KEY).await;
            seed(&*s).await;
            set_uid(&*s, "old").await;
            let w = {
                let s = s.clone();
                tokio::spawn(async move {
                    let mut cur: Value = s.get(KEY).await?;
                    cur["metadata"]["uid"] = json!("new");
                    s.update(KEY, &cur).await.map(|_| ())
                })
            };
            let d = {
                let s = s.clone();
                tokio::spawn(async move {
                    s.delete_with_preconditions(KEY, &Preconditions::new_uid("old"))
                        .await
                })
            };
            let (w, d) = (w.await.unwrap(), d.await.unwrap());
            match (w, d) {
                (Ok(()), Err(Error::Conflict(_))) => {
                    let now: Value = s.get(KEY).await.unwrap();
                    assert_eq!(now["metadata"]["uid"], "new", "iter {i}");
                }
                // Delete won; the writer then hits a missing key. A writer that
                // ALSO succeeded means the delete removed the NEW object.
                (Err(_), Ok(())) => assert!(!exists(&*s).await, "iter {i}: deleted => gone"),
                other => panic!("iter {i}: unexpected {other:?}"),
            }
        }
    }
}

mod request_context_tests {
    //! `StorageBackend` observes the request context at its dispatch point.

    use super::__parent::*;
    use rusternetes_common::cancellation::{
        request_cancellation, request_cancellation_with_deadline, with_request_cancellation,
    };
    use rusternetes_storage::*;
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
        let _: serde_json::Value =
            Storage::create(&backend, key, &json!({"metadata": {"name": "ok"}}))
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
}
