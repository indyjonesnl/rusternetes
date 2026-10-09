//! `ListOptions` handling shared by the list handlers and DELETE of a
//! collection (#2224, #2686: one `validate_list_options`).
//!
//! Upstream decodes `ListOptions`, runs `ValidateListOptions`, and hands the
//! options to the storage, which enforces the resourceVersion floor:
//!
//! * `ValidateListOptions`
//!   (apimachinery `pkg/apis/meta/internalversion/validation/validation.go:28-51`)
//!   rejects `resourceVersionMatch` without `resourceVersion`, with `continue`,
//!   with a value other than `Exact`/`NotOlderThan`, `Exact` with
//!   `resourceVersion=0`, and any `sendInitialEvents` on a list.
//! * The list's `resourceVersion` is a floor, not a pin: "Get the result from
//!   at least the specified resource version" (etcd3 `GetList` ->
//!   `validateMinimumResourceVersion`, storage/etcd3/store.go:1094-1108; the
//!   watch cache's `waitUntilFreshAndBlock`, cacher/watch_cache.go:448-488). A
//!   version the storage has not reached yet waits `blockTimeout`, then fails
//!   with `NewTooLargeResourceVersionError` (storage/errors.go:229-242).
//!
//! * `resourceVersionMatch=Exact` reads the collection AS OF that revision:
//!   `ValidateListOptions` sets `withRev = parsedRV`
//!   (`storage/interfaces.go:374-375`), `GetList` ranges at it
//!   (`etcd3/store.go:781-786`), and the list is stamped with it
//!   (`UpdateList(..., withRev, ...)`, `:898`). A revision the store has
//!   compacted is 410 `The resourceVersion for the provided list is too old.`
//!   (`etcd3/errors.go:53,68-72`). See [`list_items`] and
//!   [`list_resource_version`].

use std::collections::HashMap;

use rusternetes_common::validation::field::{Error as FieldError, Path};
use rusternetes_common::{Error, Result};
use rusternetes_storage::Storage;

/// `ValidateListOptions` (apimachinery
/// `pkg/apis/meta/internalversion/validation/validation.go:28-76`) over the
/// query parameters `ListOptions` decodes. The WatchList gate is on by
/// default in 1.35 (kube_features.go:503-509), so `isWatchListFeatureEnabled`
/// is true here, and `SetListOptionsDefaults` (defaults.go:25-38) is applied
/// first: a legacy watch (rv "" or "0") defaults to sendInitialEvents=true
/// with resourceVersionMatch=NotOlderThan.
pub fn validate_list_options(params: &HashMap<String, String>) -> Vec<FieldError> {
    let get = |k: &str| params.get(k).map(String::as_str).unwrap_or("");
    let flag = |v: &str| matches!(v, "true" | "1" | "True" | "TRUE" | "t" | "T");
    let watch = flag(get("watch"));
    let rv = get("resourceVersion");
    let mut matched = get("resourceVersionMatch");
    let mut send_initial = params.get("sendInitialEvents").map(|v| flag(v));
    let cont = get("continue");

    // SetListOptionsDefaults.
    if send_initial.is_none() && matched.is_empty() && watch && (rv.is_empty() || rv == "0") {
        send_initial = Some(true);
        matched = "NotOlderThan";
    }

    let rvm = || Path::new("resourceVersionMatch");
    let mut errs = Vec::new();
    if watch {
        // validateWatchOptions (validation.go:53-76).
        if send_initial.is_some() && matched != "NotOlderThan" {
            errs.push(FieldError::forbidden(
                &rvm(),
                "sendInitialEvents requires setting resourceVersionMatch to NotOlderThan",
            ));
        }
        if !matched.is_empty() {
            if send_initial.is_none() {
                errs.push(FieldError::forbidden(
                    &rvm(),
                    "resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided",
                ));
            }
            if matched != "NotOlderThan" {
                errs.push(FieldError::not_supported(
                    &rvm(),
                    matched.to_string(),
                    &["NotOlderThan"],
                ));
            }
            if !cont.is_empty() {
                errs.push(FieldError::forbidden(
                    &rvm(),
                    "resourceVersionMatch is forbidden when continue is provided",
                ));
            }
        }
        return errs;
    }
    if !matched.is_empty() {
        if rv.is_empty() {
            errs.push(FieldError::forbidden(
                &rvm(),
                "resourceVersionMatch is forbidden unless resourceVersion is provided",
            ));
        }
        if !cont.is_empty() {
            errs.push(FieldError::forbidden(
                &rvm(),
                "resourceVersionMatch is forbidden when continue is provided",
            ));
        }
        if matched != "Exact" && matched != "NotOlderThan" {
            errs.push(FieldError::not_supported(
                &rvm(),
                matched.to_string(),
                &["Exact", "NotOlderThan", ""],
            ));
        }
        if matched == "Exact" && rv == "0" {
            errs.push(FieldError::forbidden(
                &rvm(),
                "resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\"",
            ));
        }
    }
    if send_initial.is_some() {
        errs.push(FieldError::forbidden(
            &Path::new("sendInitialEvents"),
            "sendInitialEvents is forbidden for list",
        ));
    }
    errs
}

/// Validate the list options, then wait for the storage to reach the
/// requested `resourceVersion`. Call after authorization and before reading
/// the collection.
pub async fn prepare_list<S: Storage + ?Sized>(
    storage: &S,
    params: &HashMap<String, String>,
) -> Result<()> {
    let errs = validate_list_options(params);
    if !errs.is_empty() {
        // `NewInvalid(meta.k8s.io ListOptions, "", errs)`.
        return Err(Error::new_invalid("meta.k8s.io", "ListOptions", "", errs));
    }
    // A continue token carries its own snapshot revision; the floor applies to
    // the first page only (ValidateListOptions forbids the match with it).
    if params.get("continue").is_some_and(|c| !c.is_empty()) {
        return Ok(());
    }
    let rv = params
        .get("resourceVersion")
        .map(String::as_str)
        .unwrap_or("");
    crate::registry::generic::store::wait_until_resource_version(storage, rv).await
}

/// The revision a list pins to: `Some(rv)` for `resourceVersionMatch=Exact`
/// (`ValidateListOptions`, `storage/interfaces.go:374-375`), `None` for every
/// other combination (a floor, or a live read). Call after [`prepare_list`] has
/// validated the options.
pub fn exact_list_revision(params: &HashMap<String, String>) -> Option<i64> {
    if params.get("resourceVersionMatch").map(String::as_str) != Some("Exact") {
        return None;
    }
    params
        .get("resourceVersion")
        .and_then(|rv| rv.parse::<i64>().ok())
        .filter(|rv| *rv > 0)
}

/// Read the collection under `prefix` for a list request: as of the pinned
/// revision for `resourceVersionMatch=Exact` (etcd `WithRev`), live otherwise.
pub async fn list_items<T, S>(
    storage: &S,
    prefix: &str,
    params: &HashMap<String, String>,
) -> Result<Vec<T>>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    S: Storage + ?Sized,
{
    match exact_list_revision(params) {
        Some(rv) => storage.list_at_revision(prefix, rv).await,
        None => storage.list(prefix).await,
    }
}

/// The list's `metadata.resourceVersion`: the pinned revision for an Exact
/// list (`UpdateList(listObj, withRev, ...)`, `etcd3/store.go:898`), otherwise
/// [`crate::handlers::list_collection_resource_version`].
pub async fn list_resource_version<T: serde::Serialize>(
    storage: &rusternetes_storage::StorageBackend,
    params: &HashMap<String, String>,
    items: &[T],
) -> String {
    match exact_list_revision(params) {
        Some(rv) => rv.to_string(),
        None => crate::handlers::list_collection_resource_version(storage, items).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::MemoryStorage;

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
    async fn malformed_resource_version_is_invalid() {
        let storage = MemoryStorage::new();
        let err = prepare_list(&storage, &q(&[("resourceVersion", "abc")]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Invalid(_)), "{err:?}");
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
