//! Unit tests moved from `src/handlers/list_options.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::field::{Error as FieldError, Path};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::Storage;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::list_options::*;
    use rusternetes_storage::{MemoryStorage, StorageBackend};

    /// #3078: items and the collection RV must come from one read. A write
    /// landing between `list_items` and `list_resource_version` is absent from
    /// `items`, so it must also be absent from the stamped RV; otherwise
    /// LIST -> WATCH(rv) never delivers its ADDED. Upstream stamps the RV from
    /// the same etcd range response (`etcd3/store.go` GetList,
    /// `getResp.Header.Revision`).
    #[tokio::test]
    async fn list_rv_never_includes_a_write_absent_from_items() {
        let storage = StorageBackend::Memory(std::sync::Arc::new(MemoryStorage::new()));
        let obj = |n: &str| serde_json::json!({"metadata": {"name": n}});
        let _: serde_json::Value = storage.create("/r/a", &obj("a")).await.unwrap();
        with_list_snapshot(async {
            let params = HashMap::new();
            let items: Vec<serde_json::Value> = list_items(&storage, "/r/", &params).await.unwrap();
            assert_eq!(items.len(), 1);
            // The racing write, between the two reads.
            let b: serde_json::Value = storage.create("/r/b", &obj("b")).await.unwrap();
            let b_rv: i64 = b["metadata"]["resourceVersion"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            let rv: i64 = list_resource_version(&storage, &params, &items)
                .await
                .parse()
                .unwrap();
            assert!(
                rv < b_rv,
                "list RV {rv} includes write at {b_rv} that is missing from its items"
            );
        })
        .await;
    }

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn errs(pairs: &[(&str, &str)]) -> Vec<String> {
        validate_list_options(&q(pairs))
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    /// `TestValidateListOptions` (internalversion/validation/validation_test.go).
    #[test]
    fn validate_matches_upstream() {
        assert!(errs(&[]).is_empty());
        assert!(errs(&[("resourceVersion", "1"), ("resourceVersionMatch", "Exact")]).is_empty());
        assert!(errs(&[
            ("resourceVersion", "0"),
            ("resourceVersionMatch", "NotOlderThan")
        ])
        .is_empty());
        assert_eq!(
            errs(&[("resourceVersionMatch", "NotOlderThan")]),
            vec!["resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden unless resourceVersion is provided"]
        );
        assert_eq!(
            errs(&[
                ("resourceVersion", "1"),
                ("resourceVersionMatch", "Exact"),
                ("continue", "abc")
            ]),
            vec!["resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden when continue is provided"]
        );
        assert_eq!(
            errs(&[("resourceVersion", "0"), ("resourceVersionMatch", "Exact")]),
            vec!["resourceVersionMatch: Forbidden: resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\""]
        );
        assert_eq!(
            errs(&[("resourceVersion", "1"), ("resourceVersionMatch", "bogus")]),
            vec!["resourceVersionMatch: Unsupported value: \"bogus\": supported values: \"Exact\", \"NotOlderThan\", \"\""]
        );
        assert_eq!(
            errs(&[("sendInitialEvents", "true")]),
            vec!["sendInitialEvents: Forbidden: sendInitialEvents is forbidden for list"]
        );
    }

    /// The one `validate_list_options` covers the watch rules too
    /// (validation.go:53-76), with WatchList on (kube_features.go:503-509) and
    /// `SetListOptionsDefaults` applied (#2686).
    #[test]
    fn validate_covers_watch_rules() {
        assert!(errs(&[("watch", "true")]).is_empty());
        assert!(errs(&[
            ("watch", "true"),
            ("sendInitialEvents", "true"),
            ("resourceVersionMatch", "NotOlderThan"),
        ])
        .is_empty());
        assert_eq!(
            errs(&[("watch", "true"), ("resourceVersionMatch", "NotOlderThan")]),
            vec!["resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided"]
        );
        assert_eq!(
            errs(&[("watch", "true"), ("sendInitialEvents", "true")]),
            vec!["resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan"]
        );
        assert_eq!(
            errs(&[
                ("watch", "true"),
                ("sendInitialEvents", "true"),
                ("resourceVersionMatch", "Exact"),
            ]),
            vec![
                "resourceVersionMatch: Forbidden: sendInitialEvents requires setting resourceVersionMatch to NotOlderThan",
                r#"resourceVersionMatch: Unsupported value: "Exact": supported values: "NotOlderThan""#,
            ]
        );
        assert_eq!(
            errs(&[
                ("watch", "true"),
                ("sendInitialEvents", "true"),
                ("resourceVersionMatch", "NotOlderThan"),
                ("continue", "123"),
            ]),
            vec!["resourceVersionMatch: Forbidden: resourceVersionMatch is forbidden when continue is provided"]
        );
    }

    #[tokio::test]
    async fn invalid_options_are_422() {
        let storage = MemoryStorage::new();
        let err = prepare_list(&storage, &q(&[("resourceVersionMatch", "Exact")]))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Status(_) | Error::Invalid(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn rv_zero_and_empty_never_wait() {
        let storage = MemoryStorage::new();
        prepare_list(&storage, &q(&[])).await.unwrap();
        prepare_list(&storage, &q(&[("resourceVersion", "0")]))
            .await
            .unwrap();
    }

    /// A resourceVersion the store has not reached fails with 504
    /// `Too large resource version` after the block timeout
    /// (storage/errors.go:229-242), with the retry-after cause.
    #[tokio::test(start_paused = true)]
    async fn too_large_resource_version_is_504_with_retry_after() {
        let storage = MemoryStorage::new();
        let err = prepare_list(
            &storage,
            &q(&[
                ("resourceVersion", "99999999"),
                ("resourceVersionMatch", "NotOlderThan"),
            ]),
        )
        .await
        .unwrap_err();
        let Error::Status(status) = err else {
            panic!("want Status, got {err:?}")
        };
        assert_eq!(status.code, Some(504));
        assert!(status
            .message
            .as_deref()
            .unwrap()
            .starts_with("Timeout: Too large resource version: 99999999"));
        assert_eq!(status.details.unwrap().retry_after_seconds, Some(1));
    }

    #[tokio::test]
    async fn malformed_resource_version_is_a_bad_request() {
        // `storage.ValidateListOptions` (storage/interfaces.go:366-369):
        // `apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))`
        // where `err` is the `InvalidError` whose text is the aggregate.
        let storage = MemoryStorage::new();
        let err = prepare_list(&storage, &q(&[("resourceVersion", "abc")]))
            .await
            .unwrap_err();
        match err {
            Error::BadRequest(msg) => assert_eq!(
                msg,
                "invalid resource version: resourceVersion: Invalid value: \"abc\": strconv.ParseUint: parsing \"abc\": invalid syntax"
            ),
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    /// The wait is woken by the storage's writes, not by a timer (upstream's
    /// `w.cond.Wait()` in `waitUntilFreshAndBlock`,
    /// storage/cacher/watch_cache.go:448-488, broadcast on every event
    /// processed, `watch_cache.go` `processEvent`). On the paused clock a
    /// poll loop only ever finishes a timer tick after the write, so the
    /// elapsed virtual time between the write and the wake-up is nonzero.
    #[tokio::test(start_paused = true)]
    async fn wait_is_woken_by_a_write_not_a_poll_tick() {
        let storage = std::sync::Arc::new(MemoryStorage::new());
        let want = storage.current_revision().await.unwrap() + 1;
        let waiter = {
            let storage = storage.clone();
            tokio::spawn(async move {
                prepare_list(&*storage, &q(&[("resourceVersion", &want.to_string())]))
                    .await
                    .map(|_| tokio::time::Instant::now())
            })
        };
        // Let the waiter register and park.
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        let cm = serde_json::json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"a","namespace":"ns"}});
        storage
            .create("/registry/configmaps/ns/a", &cm)
            .await
            .unwrap();
        let wrote = tokio::time::Instant::now();
        let woke = waiter.await.unwrap().unwrap();
        assert!(
            woke - wrote < std::time::Duration::from_millis(10),
            "woke {:?} after the write",
            woke - wrote
        );
    }
}
