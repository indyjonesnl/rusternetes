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
    // `storage.ValidateListOptions` (storage/interfaces.go:366-369), run first
    // by `CacheDelegator.GetList` (cacher/delegator.go:179-182): an
    // unparseable version is `apierrors.NewBadRequest(fmt.Sprintf("invalid
    // resource version: %v", err))`, `err` being the `InvalidError` whose text
    // is the aggregate of its errors.
    if let Err(errs) = crate::registry::generic::store::parse_resource_version_str(rv) {
        return Err(Error::BadRequest(format!(
            "invalid resource version: {}",
            Error::aggregate_message(&errs)
        )));
    }
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

tokio::task_local! {
    /// The store revision the current request's last LIST was read at
    /// ([`list_items`]) and that [`list_resource_version`] stamps.
    static LIST_SNAPSHOT_REV: std::cell::Cell<Option<i64>>;
}

/// Scope one request's list snapshot. Every handler pairs `list_items` with a
/// later `list_resource_version`; this carries the revision from the first to
/// the second so both describe one read (installed per request by
/// [`list_snapshot_middleware`]).
pub async fn with_list_snapshot<F: std::future::Future>(fut: F) -> F::Output {
    LIST_SNAPSHOT_REV
        .scope(std::cell::Cell::new(None), fut)
        .await
}

/// Axum layer wrapping each request in [`with_list_snapshot`].
pub async fn list_snapshot_middleware(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    with_list_snapshot(next.run(req)).await
}

fn record_list_snapshot(rev: Option<i64>) {
    let _ = LIST_SNAPSHOT_REV.try_with(|c| c.set(rev));
}

fn recorded_list_snapshot() -> Option<i64> {
    LIST_SNAPSHOT_REV.try_with(|c| c.get()).ok().flatten()
}

/// Read the collection under `prefix` for a list request: as of the pinned
/// revision for `resourceVersionMatch=Exact` (etcd `WithRev`), otherwise as of
/// a revision read first.
///
/// Upstream's `GetList` takes the list and its `ResourceVersion` from one etcd
/// range response (`etcd3/store.go`: `getResp.Header.Revision` ->
/// `UpdateList(listObj, uint64(withRev), ...)`), so a write cannot land between
/// "the items" and "the RV". Reading the revision first and listing at it
/// (as `watch::watch_snapshot` does) gives the same property here; the
/// revision is remembered for [`list_resource_version`]. A backend that cannot
/// read at a revision falls back to a live list and the old behaviour.
pub async fn list_items<T, S>(
    storage: &S,
    prefix: &str,
    params: &HashMap<String, String>,
) -> Result<Vec<T>>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
    S: Storage + ?Sized,
{
    if let Some(rv) = exact_list_revision(params) {
        record_list_snapshot(None);
        return storage.list_at_revision(prefix, rv).await;
    }
    if let Some(rev) = crate::handlers::list_snapshot_revision(storage).await {
        if let Ok(items) = storage.list_at_revision(prefix, rev).await {
            record_list_snapshot(Some(rev));
            return Ok(items);
        }
    }
    record_list_snapshot(None);
    storage.list(prefix).await
}

/// The list's `metadata.resourceVersion`: the pinned revision for an Exact
/// list (`UpdateList(listObj, withRev, ...)`, `etcd3/store.go:898`), the
/// revision [`list_items`] read at when one was recorded for this request,
/// otherwise [`crate::handlers::list_collection_resource_version`].
pub async fn list_resource_version<T: serde::Serialize>(
    storage: &rusternetes_storage::StorageBackend,
    params: &HashMap<String, String>,
    items: &[T],
) -> String {
    if let Some(rv) = exact_list_revision(params) {
        return rv.to_string();
    }
    if let Some(rev) = recorded_list_snapshot() {
        // Never below an item returned (a backend whose `list_at_revision`
        // is a live read can hand back newer items; see #1824).
        return crate::handlers::collection_resource_version(
            Some(rev),
            &crate::handlers::list_resource_version(items),
        );
    }
    crate::handlers::list_collection_resource_version(storage, items).await
}
