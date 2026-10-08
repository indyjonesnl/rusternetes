//! API Priority and Fairness: the work estimator (request width).
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/util/flowcontrol/request`
//! (release-1.35): `width.go`, `config.go`, `list_work_estimator.go`,
//! `mutating_work_estimator.go`, plus the `delegator.ShouldDelegateListMeta`
//! decision (`storage/cacher/delegator/interface.go`) that the list estimator
//! calls with `CacheWithoutSnapshots{}`.
//!
//! The estimator is HTTP-framework-free: it takes the request's
//! [`RequestInfo`] (upstream: `apirequest.RequestInfoFrom(r.Context())`) and
//! its raw query string (upstream: `r.URL.Query()`).
//!
//! DELIBERATE DEVIATIONS (Rust expression, not mechanism):
//!
//! - Go's feature gates become [`WorkEstimatorConfig`] fields:
//!   `size_based_list_cost_estimate` (`SizeBasedListCostEstimate`, Beta,
//!   default true since 1.34), `watch_list` (`WatchList`, default true since
//!   1.34) and `consistent_read_supported` (`delegator.ConsistentReadSupported`
//!   = `ConsistentListFromCache` && etcd `RequestWatchProgress`; default false
//!   here because it depends on the storage backend, as in upstream's unit
//!   tests where no etcd checker is wired).
//! - `metrics.ObserveWatchCount` is not called (no APF metrics registry yet).
//! - The object-count tracker (`object_count_tracker.go`) that feeds
//!   `statsGetterFn` is not ported here; callers supply the closure.

use std::time::Duration;

use crate::flow_control_queueset::WorkEstimate;

/// `apirequest.RequestInfo` (the fields the estimator reads).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestInfo {
    pub verb: String,
    pub api_group: String,
    pub resource: String,
    pub subresource: String,
    pub name: String,
}

/// `storage.Stats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub object_count: i64,
    pub estimated_average_object_size_bytes: i64,
}

/// The errors `statsGetterFunc` can return (`ObjectCountStaleErr`,
/// `ObjectCountNotFoundErr`, or anything else).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatsError {
    Stale,
    NotFound,
    Other(String),
}

/// `ListWorkEstimatorConfig` (config.go).
#[derive(Clone, Debug)]
pub struct ListWorkEstimatorConfig {
    pub objects_per_seat: f64,
}

/// `MutatingWorkEstimatorConfig` (config.go).
#[derive(Clone, Debug)]
pub struct MutatingWorkEstimatorConfig {
    pub enabled: bool,
    pub event_additional_duration: Duration,
    pub watches_per_seat: f64,
}

/// `WorkEstimatorConfig` (config.go).
#[derive(Clone, Debug)]
pub struct WorkEstimatorConfig {
    pub list: ListWorkEstimatorConfig,
    pub mutating: MutatingWorkEstimatorConfig,
    pub minimum_seats: u64,
    pub maximum_list_seats_limit: u64,
    pub maximum_mutating_seats_limit: u64,
    pub size_based_list_cost_estimate: bool,
    pub watch_list: bool,
    pub consistent_read_supported: bool,
}

impl Default for WorkEstimatorConfig {
    /// `DefaultWorkEstimatorConfig` (config.go) with the 1.35 gate defaults.
    fn default() -> Self {
        Self::with_gates(true, true, false)
    }
}

impl WorkEstimatorConfig {
    /// `DefaultWorkEstimatorConfig` (config.go:205) under explicit gates.
    pub fn with_gates(
        size_based_list_cost_estimate: bool,
        watch_list: bool,
        consistent_read_supported: bool,
    ) -> Self {
        let _ = (
            size_based_list_cost_estimate,
            watch_list,
            consistent_read_supported,
        );
        Self {
            list: ListWorkEstimatorConfig {
                objects_per_seat: 0.0,
            },
            mutating: MutatingWorkEstimatorConfig {
                enabled: false,
                event_additional_duration: Duration::ZERO,
                watches_per_seat: 0.0,
            },
            minimum_seats: 0,
            maximum_list_seats_limit: 0,
            maximum_mutating_seats_limit: 0,
            size_based_list_cost_estimate: true,
            watch_list: true,
            consistent_read_supported: false,
        }
    }
}

pub type StatsGetter = Box<dyn Fn(&str) -> Result<Stats, StatsError> + Send + Sync>;
pub type WatchCountGetter = Box<dyn Fn(&RequestInfo) -> i64 + Send + Sync>;
pub type MaxSeatsFn = Box<dyn Fn(&str) -> u64 + Send + Sync>;

/// `NewWorkEstimator` (width.go).
pub struct WorkEstimator {
    #[allow(dead_code)]
    stats_getter: StatsGetter,
    #[allow(dead_code)]
    watch_count_getter: WatchCountGetter,
    #[allow(dead_code)]
    config: WorkEstimatorConfig,
    #[allow(dead_code)]
    max_seats_fn: MaxSeatsFn,
}

impl WorkEstimator {
    pub fn new(
        stats_getter: StatsGetter,
        watch_count_getter: WatchCountGetter,
        config: WorkEstimatorConfig,
        max_seats_fn: MaxSeatsFn,
    ) -> Self {
        Self {
            stats_getter,
            watch_count_getter,
            config,
            max_seats_fn,
        }
    }

    /// `workEstimator.estimate` (width.go).
    pub fn estimate_work(
        &self,
        _info: Option<&RequestInfo>,
        _query: &str,
        _flow_schema: &str,
        _priority_level: &str,
    ) -> WorkEstimate {
        WorkEstimate::default()
    }

    /// `listWorkEstimator.seatsBasedOnObjectCount` (list_work_estimator.go).
    pub fn seats_based_on_object_count(
        &self,
        _stats: Stats,
        _opts: &ListOptions,
        _is_list_from_cache: bool,
        _matches_single: bool,
    ) -> u64 {
        0
    }

    /// `listWorkEstimator.seatsBasedOnObjectSize` (list_work_estimator.go).
    pub fn seats_based_on_object_size(
        &self,
        _stats: Stats,
        _opts: &ListOptions,
        _is_list_from_cache: bool,
        _matches_single: bool,
    ) -> u64 {
        0
    }
}

/// The `metav1.ListOptions` fields the list estimator reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListOptions {
    pub label_selector: String,
    pub field_selector: String,
    pub resource_version: String,
    pub resource_version_match: String,
    pub limit: i64,
    pub continue_token: String,
    pub send_initial_events: Option<bool>,
}

impl ListOptions {
    /// `metav1.Convert_url_Values_To_v1_ListOptions`.
    pub fn from_query(_query: &str) -> Result<Self, String> {
        Ok(Self::default())
    }

    /// `delegator.ShouldDelegateListMeta(opts, CacheWithoutSnapshots{})`.
    pub fn should_delegate_list(&self, _consistent_read_supported: bool) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Case {
        name: &'static str,
        uri: &'static str,
        info: (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            &'static str,
        ),
        stats: (i64, i64),
        err: Option<StatsError>,
        watch_count: i64,
        max_seats: u64,
        initial: u64,
        final_: u64,
        latency_ms: u64,
    }

    /// `TestWorkEstimator` (width_test.go:32): every table row, verbatim.
    #[test]
    fn work_estimator_table() {
        let cases = vec![
        Case { name: "request verb is not list, expect minSeats", uri: "http://server/apis/", info: ("get", "", "", "", ""), stats: (0, 0), err: None, watch_count: 0, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, conversion to ListOptions returns error, expect maxSeats", uri: "http://server/apis/foo.bar/v1/events?limit=invalid", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 100, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, limit 399, expect read 4MB read from etcd", uri: "http://server/apis/foo.bar/v1/events?limit=399&resourceVersion=1", info: ("list", "foo.bar", "events", "", ""), stats: (699, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, expect seats capped by cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1", info: ("list", "foo.bar", "events", "", ""), stats: (699, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, expect read 7MB read from cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1", info: ("list", "foo.bar", "events", "", ""), stats: (69, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 7, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, limit 399, expect 4MB read from etcd", uri: "http://server/apis/foo.bar/v1/events?limit=399", info: ("list", "foo.bar", "events", "", ""), stats: (699, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, expect read 4MB from etcd", uri: "http://server/apis/foo.bar/v1/events", info: ("list", "foo.bar", "events", "", ""), stats: (399, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, count not known, expect minSeats", uri: "http://server/apis/foo.bar/v1/events", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, continuation is set, limit 399, expect read 4MB from etcd", uri: "http://server/apis/foo.bar/v1/events?continue=token&limit=399", info: ("list", "foo.bar", "events", "", ""), stats: (699, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version is zero, limit 299, expect seats capped by cache", uri: "http://server/apis/foo.bar/v1/events?limit=299&resourceVersion=0", info: ("list", "foo.bar", "events", "", ""), stats: (399, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version is zero, limit 10, expect read 400KB from cache", uri: "http://server/apis/foo.bar/v1/events?limit=20&resourceVersion=0", info: ("list", "foo.bar", "events", "", ""), stats: (399, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 2, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version is zero, expect seats capped by cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version is zero, expect read 8MB from cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0", info: ("list", "foo.bar", "events", "", ""), stats: (79, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, match is Exact, expect read 4MB from etcd", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1&resourceVersionMatch=Exact&limit=399", info: ("list", "foo.bar", "events", "", ""), stats: (699, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, match is NotOlderThan, expect seats capped by cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1&resourceVersionMatch=NotOlderThan", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, match is NotOlderThan, expect read 8 MB from cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1&resourceVersionMatch=NotOlderThan", info: ("list", "foo.bar", "events", "", ""), stats: (79, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, match is Exact, expect seats capped by max", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1&resourceVersionMatch=Exact", info: ("list", "foo.bar", "events", "", ""), stats: (5000, 10_000), err: None, watch_count: 0, max_seats: 20, initial: 20, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, bad match, expect read 2MB from etcd", uri: "http://server/apis/foo.bar/v1/events?resourceVersionMatch=foo", info: ("list", "foo.bar", "events", "", ""), stats: (200, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 20, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, bad match, limit 399, expect read 4MB from etcd", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=foo&resourceVersionMatch=Exact&limit=399", info: ("list", "foo.bar", "events", "", ""), stats: (699, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 1, match exact, expect seats capped by max seats", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=1&resourceVersionMatch=Exact", info: ("list", "foo.bar", "events", "", ""), stats: (5000, 10_000), err: None, watch_count: 0, max_seats: 20, initial: 20, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 0, count is not found, expect min seats", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, object count is stale, expect max seats", uri: "http://server/apis/foo.bar/v1/events", info: ("list", "foo.bar", "events", "", ""), stats: (1, 1), err: Some(StatsError::Stale), watch_count: 0, max_seats: 100, initial: 100, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, no object size, expect seats capped by cache", uri: "http://server/apis/foo.bar/v1/events", info: ("list", "foo.bar", "events", "", ""), stats: (1, 0), err: None, watch_count: 0, max_seats: 100, initial: 15, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, object count is not found, expect min seats", uri: "http://server/apis/foo.bar/v1/events", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, count getter throws unknown error, expect max seats", uri: "http://server/apis/foo.bar/v1/events", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::Other("unknown error".into())), watch_count: 0, max_seats: 100, initial: 100, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 0, count not known, limit 1, expect min seats", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0&limit=1", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, object count is stale, limit 1, expect read max object size from etcd", uri: "http://server/apis/foo.bar/v1/events?limit=1", info: ("list", "foo.bar", "events", "", ""), stats: (1, 10_000), err: Some(StatsError::Stale), watch_count: 0, max_seats: 100, initial: 15, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, no object size, limit 1, expect read max object size from etcd", uri: "http://server/apis/foo.bar/v1/events&limit=1", info: ("list", "foo.bar", "events", "", ""), stats: (1, 0), err: None, watch_count: 0, max_seats: 100, initial: 15, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, object count is not found, limit 1, expect min seats", uri: "http://server/apis/foo.bar/v1/events?limit=1", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, count getter throws unknown error, limit 1, expect read max object size from etcd", uri: "http://server/apis/foo.bar/v1/events?limit=1", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::Other("unknown error".into())), watch_count: 0, max_seats: 100, initial: 15, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, resource version 0, count not known, limit 499, expect min seats", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0&limit=499", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, object count is stale, limit 499, expect max seats", uri: "http://server/apis/foo.bar/v1/events?limit=499", info: ("list", "foo.bar", "events", "", ""), stats: (1, 1), err: Some(StatsError::Stale), watch_count: 0, max_seats: 100, initial: 100, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, no object size, limit 499, expect read max object size from etcd", uri: "http://server/apis/foo.bar/v1/events?limit=499", info: ("list", "foo.bar", "events", "", ""), stats: (1, 0), err: None, watch_count: 0, max_seats: 100, initial: 15, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, no object size, resource version 0, limit 499, expect capped by cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0&limit=499", info: ("list", "foo.bar", "events", "", ""), stats: (1, 0), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, object count is not found, limit 499, expect min seats", uri: "http://server/apis/foo.bar/v1/events?limit=499", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::NotFound), watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, count getter throws unknown error, limit 499, expect max seats", uri: "http://server/apis/foo.bar/v1/events?limit=499", info: ("list", "foo.bar", "events", "", ""), stats: (0, 0), err: Some(StatsError::Other("unknown error".into())), watch_count: 0, max_seats: 100, initial: 100, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, metadata.name specified, expect read 200KB from etcd", uri: "http://server/apis/foo.bar/v1/events?fieldSelector=metadata.name%3Dtest", info: ("list", "foo.bar", "events", "test", ""), stats: (799, 200_000), err: None, watch_count: 0, max_seats: 100, initial: 2, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, metadata.name specified, expect read 1.5MB from etcd", uri: "http://server/apis/foo.bar/v1/events?fieldSelector=metadata.name%3Dtest", info: ("list", "foo.bar", "events", "test", ""), stats: (799, 1_500_000), err: None, watch_count: 0, max_seats: 100, initial: 15, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, metadata.name, resource version 0, limit 500, expect read 200KB from cache", uri: "http://server/apis/foo.bar/v1/events?fieldSelector=metadata.name%3Dtest&limit=500&resourceVersion=0", info: ("list", "foo.bar", "events", "test", ""), stats: (799, 200_000), err: None, watch_count: 0, max_seats: 100, initial: 2, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, metadata.name, resource version 0, limit 500, expect seats capped by cache", uri: "http://server/apis/foo.bar/v1/events?fieldSelector=metadata.name%3Dtest&limit=500&resourceVersion=0", info: ("list", "foo.bar", "events", "test", ""), stats: (799, 1_500_000), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, labelSelector, expect read 8MB from etcd", uri: "http://server/apis/foo.bar/v1/events?labelSelector=app%3Dtest", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 80, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, labelSelector, limit 49, expect read 4MB from etcd by pagination", uri: "http://server/apis/foo.bar/v1/events?labelSelector=app%3Dtest&limit=49", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 40, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, labelSelector, limit 699, expect read 7MB from etcd", uri: "http://server/apis/foo.bar/v1/events?labelSelector=app%3Dtest&limit=699", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 70, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, labelSelector, resource version 0, expect seats capped cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0&labelSelector=app%3Dtest", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 10, final_: 0, latency_ms: 0 },
        Case { name: "request verb is list, labelSelector, resource version 0, limit 299, expect read 300KB from cache", uri: "http://server/apis/foo.bar/v1/events?resourceVersion=0&labelSelector=app%3Dtest&limit=29", info: ("list", "foo.bar", "events", "", ""), stats: (799, 10_000), err: None, watch_count: 0, max_seats: 100, initial: 3, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is nil and RV unset (legacy watch with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is nil and RV set to 0 (legacy watch with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&resourceVersion=0", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is nil and RV set to non-zero (legacy watch without init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&resourceVersion=1", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is false and RV unset (legacy watch with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&sendInitialEvents=false", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is false and RV set to 0 (legacy watch with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&sendInitialEvents=false&resourceVersion=0", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is false and RV set to non-zero (legacy watch without init events))", uri: "http://server/apis/foo.bar/v1/events?watch=true&sendInitialEvents=false&resourceVersion=1", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is true and RV unset (streaming list with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&sendInitialEvents=true", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is true and RV set to 0 (streaming list with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&sendInitialEvents=true&resourceVersion=0", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is watch, sendInitialEvents is true and RV set to non-zero (streaming list with init events)", uri: "http://server/apis/foo.bar/v1/events?watch=true&sendInitialEvents=true&resourceVersion=0", info: ("watch", "foo.bar", "events", "", ""), stats: (799, 1_000), err: None, watch_count: 0, max_seats: 100, initial: 8, final_: 0, latency_ms: 0 },
        Case { name: "request verb is create, no watches", uri: "http://server/apis/foo.bar/v1/foos", info: ("create", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 0, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is create, watches registered", uri: "http://server/apis/foo.bar/v1/foos", info: ("create", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 29, max_seats: 10, initial: 1, final_: 3, latency_ms: 5 },
        Case { name: "request verb is create, watches registered, no additional latency", uri: "http://server/apis/foo.bar/v1/foos", info: ("create", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 5, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is create, watches registered, capped by watch cache", uri: "http://server/apis/foo.bar/v1/foos", info: ("create", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 199, max_seats: 10, initial: 1, final_: 10, latency_ms: 10 },
        Case { name: "request verb is update, no watches", uri: "http://server/apis/foo.bar/v1/foos/myfoo", info: ("update", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 0, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is update, watches registered", uri: "http://server/apis/foor.bar/v1/foos/myfoo", info: ("update", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 29, max_seats: 10, initial: 1, final_: 3, latency_ms: 5 },
        Case { name: "request verb is patch, no watches", uri: "http://server/apis/foo.bar/v1/foos/myfoo", info: ("patch", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 0, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is patch, watches registered", uri: "http://server/apis/foo.bar/v1/foos/myfoo", info: ("patch", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 29, max_seats: 10, initial: 1, final_: 3, latency_ms: 5 },
        Case { name: "request verb is patch, watches registered, lower max seats", uri: "http://server/apis/foo.bar/v1/foos/myfoo", info: ("patch", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 100, max_seats: 5, initial: 1, final_: 5, latency_ms: 10 },
        Case { name: "request verb is delete, no watches", uri: "http://server/apis/foo.bar/v1/foos/myfoo", info: ("delete", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 0, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "request verb is delete, watches registered", uri: "http://server/apis/foo.bar/v1/foos/myfoo", info: ("delete", "foo.bar", "foos", "", ""), stats: (0, 0), err: None, watch_count: 29, max_seats: 10, initial: 1, final_: 3, latency_ms: 5 },
        Case { name: "creating token for service account", uri: "http://server/api/v1/namespaces/foo/serviceaccounts/default/token", info: ("create", "v1", "serviceaccounts", "", "token"), stats: (0, 0), err: None, watch_count: 5777, max_seats: 10, initial: 1, final_: 0, latency_ms: 0 },
        Case { name: "creating service account", uri: "http://server/api/v1/namespaces/foo/serviceaccounts", info: ("create", "v1", "serviceaccounts", "", ""), stats: (0, 0), err: None, watch_count: 1000, max_seats: 20, initial: 1, final_: 10, latency_ms: 50 },
        ];
        for c in cases {
            let stats = Stats {
                object_count: c.stats.0,
                estimated_average_object_size_bytes: c.stats.1,
            };
            let err = c.err.clone();
            let wc = c.watch_count;
            let ms = c.max_seats;
            let est = WorkEstimator::new(
                Box::new(move |_| match &err {
                    Some(e) => Err(e.clone()),
                    None => Ok(stats),
                }),
                Box::new(move |_| wc),
                WorkEstimatorConfig::default(),
                Box::new(move |_| ms),
            );
            let info = RequestInfo {
                verb: c.info.0.into(),
                api_group: c.info.1.into(),
                resource: c.info.2.into(),
                name: c.info.3.into(),
                subresource: c.info.4.into(),
            };
            let query = url::Url::parse(c.uri)
                .ok()
                .and_then(|u| u.query().map(str::to_string))
                .unwrap_or_default();
            let got = est.estimate_work(Some(&info), &query, "testFS", "testPL");
            assert_eq!(got.initial_seats, c.initial, "initial seats: {}", c.name);
            assert_eq!(got.final_seats, c.final_, "final seats: {}", c.name);
            assert_eq!(
                got.additional_latency,
                Duration::from_millis(c.latency_ms),
                "additional latency: {}",
                c.name
            );
        }
    }

    /// First row of `TestWorkEstimator` ("request has no RequestInfo, expect
    /// maxSeats"); the table cannot hold `requestInfo: nil`.
    #[test]
    fn no_request_info_expects_max_seats() {
        let est = WorkEstimator::new(
            Box::new(|_| Ok(Stats::default())),
            Box::new(|_| 0),
            WorkEstimatorConfig::default(),
            Box::new(|_| 100),
        );
        let got = est.estimate_work(None, "", "testFS", "testPL");
        assert_eq!(got.initial_seats, 100);
        assert_eq!(got.final_seats, 0);
        assert_eq!(got.additional_latency, Duration::ZERO);
    }

    /// width.go:112-116: maxSeats == 0 or above the larger of the two limits
    /// is clamped to `maximumSeatsLimit`.
    #[test]
    fn no_request_info_clamps_to_maximum_seats_limit() {
        let mk = |ms: u64| {
            WorkEstimator::new(
                Box::new(|_| Ok(Stats::default())),
                Box::new(|_| 0),
                WorkEstimatorConfig::default(),
                Box::new(move |_| ms),
            )
        };
        // DefaultWorkEstimatorConfig: max(100, 10) = 100.
        assert_eq!(mk(0).estimate_work(None, "", "", "").initial_seats, 100);
        assert_eq!(mk(5000).estimate_work(None, "", "", "").initial_seats, 100);
        assert_eq!(mk(7).estimate_work(None, "", "", "").initial_seats, 7);
    }

    /// `WorkEstimate.MaxSeats` (width.go:52).
    #[test]
    fn work_estimate_max_seats() {
        let we = WorkEstimate {
            initial_seats: 3,
            final_seats: 7,
            additional_latency: Duration::ZERO,
        };
        assert_eq!(we.max_seats(), 7);
        let we = WorkEstimate {
            initial_seats: 9,
            final_seats: 7,
            additional_latency: Duration::ZERO,
        };
        assert_eq!(we.max_seats(), 9);
    }

    /// `DefaultWorkEstimatorConfig` (config.go:205-216).
    #[test]
    fn default_config_matches_upstream() {
        let c = WorkEstimatorConfig::default();
        assert_eq!(c.minimum_seats, 1);
        assert_eq!(c.maximum_list_seats_limit, 100);
        assert_eq!(c.maximum_mutating_seats_limit, 10);
        assert_eq!(c.list.objects_per_seat, 100.0);
        assert!(c.mutating.enabled);
        assert_eq!(
            c.mutating.event_additional_duration,
            Duration::from_millis(5)
        );
        assert_eq!(c.mutating.watches_per_seat, 10.0);
        // Gate off: maximumListSeatsLimit = 10 (config.go:206-209).
        assert_eq!(
            WorkEstimatorConfig::with_gates(false, true, false).maximum_list_seats_limit,
            10
        );
    }

    /// `TestListWorkEstimator` (list_work_estimator_test.go:26): every row.
    #[test]
    fn list_work_estimator_table() {
        // (name, totalSize, objectCount, limit, labelSelector, fromCache,
        //  matchesSingle, expectObjectCountEstimate, expectObjectSizeEstimate)
        #[allow(clippy::type_complexity)]
        let rows: Vec<(&str, i64, i64, i64, bool, bool, bool, u64, u64)> = vec![
            ("100KB resource", 99999, 99, 0, false, false, false, 2, 1),
            ("100KB resource", 99999, 10, 0, false, false, false, 1, 1),
            ("100KB resource", 99999, 99, 0, false, true, false, 1, 1),
            ("100KB resource", 99999, 10, 0, false, true, false, 1, 1),
            ("1MB resource", 999999, 1000, 0, false, false, false, 20, 10),
            ("1MB resource", 999999, 1000, 100, false, false, false, 2, 1),
            ("1MB resource", 999999, 1000, 100, true, false, false, 11, 5),
            ("1MB resource", 999999, 1000, 0, false, true, false, 10, 10),
            ("1MB resource", 999999, 1000, 100, false, true, false, 10, 1),
            ("1MB resource", 999999, 1000, 100, true, true, false, 10, 1),
            ("1MB resource", 999999, 99, 0, false, false, false, 2, 10),
            ("1MB resource", 999999, 99, 10, false, false, false, 1, 2),
            ("1MB resource", 999999, 99, 10, true, false, false, 2, 5),
            ("1MB resource", 999999, 99, 0, false, true, false, 1, 10),
            (
                "10MB resource",
                9999999,
                1000,
                0,
                false,
                false,
                false,
                20,
                100,
            ),
            (
                "10MB resource",
                9999999,
                1000,
                100,
                false,
                false,
                false,
                2,
                10,
            ),
            (
                "10MB resource",
                9999999,
                1000,
                100,
                true,
                false,
                false,
                11,
                50,
            ),
            (
                "10MB resource",
                9999999,
                1000,
                0,
                false,
                true,
                false,
                10,
                10,
            ),
            (
                "10MB resource",
                9999999,
                1000,
                100,
                false,
                true,
                false,
                10,
                10,
            ),
            (
                "10MB resource",
                9999999,
                1000,
                100,
                true,
                true,
                false,
                10,
                10,
            ),
            ("10MB resource", 9999999, 99, 0, false, false, false, 2, 100),
            ("10MB resource", 9999999, 99, 10, false, false, false, 1, 11),
            ("10MB resource", 9999999, 99, 10, true, false, false, 2, 50),
            ("10MB resource", 9999999, 99, 0, false, true, false, 1, 10),
            ("10MB resource", 9999999, 99, 10, false, true, false, 1, 10),
            ("10MB resource", 9999999, 99, 10, true, true, false, 1, 10),
            (
                "1000MB resource",
                999999999,
                1000,
                0,
                false,
                false,
                true,
                1,
                10,
            ),
            (
                "1000MB resource",
                999999999,
                1000,
                0,
                false,
                true,
                true,
                1,
                10,
            ),
        ];
        let est = WorkEstimator::new(
            Box::new(|_| Ok(Stats::default())),
            Box::new(|_| 0),
            WorkEstimatorConfig::default(),
            Box::new(|_| 0),
        );
        for (name, total, count, limit, sel, cache, single, want_count, want_size) in rows {
            let stats = Stats {
                object_count: count,
                estimated_average_object_size_bytes: total / count,
            };
            let opts = ListOptions {
                limit,
                label_selector: if sel { "a".into() } else { String::new() },
                ..Default::default()
            };
            assert_eq!(
                est.seats_based_on_object_count(stats, &opts, cache, single),
                want_count,
                "object count estimate: {name}"
            );
            assert_eq!(
                est.seats_based_on_object_size(stats, &opts, cache, single),
                want_size,
                "object size estimate: {name}"
            );
        }
    }

    /// `Convert_url_Values_To_v1_ListOptions` error cases (zz_generated.conversion.go:370).
    #[test]
    fn list_options_from_query() {
        assert!(ListOptions::from_query("limit=invalid").is_err());
        assert!(ListOptions::from_query("limit=1&sendInitialEvents=x").is_ok());
        let o = ListOptions::from_query(
            "labelSelector=app%3Dtest&fieldSelector=metadata.name%3Dt&resourceVersion=3\
             &resourceVersionMatch=Exact&limit=5&continue=tok&sendInitialEvents=0",
        )
        .unwrap();
        assert_eq!(o.label_selector, "app=test");
        assert_eq!(o.field_selector, "metadata.name=t");
        assert_eq!(o.resource_version, "3");
        assert_eq!(o.resource_version_match, "Exact");
        assert_eq!(o.limit, 5);
        assert_eq!(o.continue_token, "tok");
        assert_eq!(o.send_initial_events, Some(false));
        // Convert_Slice_string_To_Pointer_bool: "" and "false" and "0" are false.
        assert_eq!(
            ListOptions::from_query("sendInitialEvents=")
                .unwrap()
                .send_initial_events,
            Some(false)
        );
        assert_eq!(
            ListOptions::from_query("sendInitialEvents=FALSE")
                .unwrap()
                .send_initial_events,
            Some(false)
        );
        assert_eq!(
            ListOptions::from_query("sendInitialEvents=yes")
                .unwrap()
                .send_initial_events,
            Some(true)
        );
        assert_eq!(
            ListOptions::from_query("").unwrap().send_initial_events,
            None
        );
    }

    /// `ShouldDelegateList` with `CacheWithoutSnapshots` (delegator/interface.go:27-80).
    #[test]
    fn should_delegate_list_without_snapshots() {
        let q = |s: &str| ListOptions::from_query(s).unwrap();
        // Exact: delegate. NotOlderThan: cache. Unknown match: delegate.
        assert!(q("resourceVersionMatch=Exact&resourceVersion=1").should_delegate_list(false));
        assert!(
            !q("resourceVersionMatch=NotOlderThan&resourceVersion=1").should_delegate_list(false)
        );
        assert!(q("resourceVersionMatch=foo").should_delegate_list(false));
        // Continue: delegate.
        assert!(q("continue=t&limit=3").should_delegate_list(false));
        // Legacy exact (limit + RV other than 0): delegate.
        assert!(q("limit=3&resourceVersion=1").should_delegate_list(false));
        // RV=0: cache.
        assert!(!q("resourceVersion=0&limit=3").should_delegate_list(false));
        assert!(!q("resourceVersion=1").should_delegate_list(false));
        // Consistent read: delegate unless ConsistentReadSupported.
        assert!(q("").should_delegate_list(false));
        assert!(!q("").should_delegate_list(true));
    }
}
