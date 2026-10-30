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

use crate::flow_control_queueset::{seats_times_duration, WorkEstimate};

/// `apirequest.RequestInfo` (the fields the estimator reads).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestInfo {
    pub verb: String,
    pub api_group: String,
    pub resource: String,
    pub subresource: String,
    pub namespace: String,
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

/// config.go:164-166 `objectsPerSeat`, `watchesPerSeat`,
/// `enableMutatingWorkEstimator`; config.go:169 `eventAdditionalDuration`.
const OBJECTS_PER_SEAT: f64 = 100.0;
const WATCHES_PER_SEAT: f64 = 10.0;
const ENABLE_MUTATING_WORK_ESTIMATOR: bool = true;
const EVENT_ADDITIONAL_DURATION: Duration = Duration::from_millis(5);

/// list_work_estimator.go:270-277.
const BYTES_PER_SEAT: i64 = 100_000;
const CACHE_WITH_STREAMING_MAX_MEMORY_USAGE: i64 = 1_000_000;
const MAX_OBJECT_SIZE: i64 = 1_500_000;
const INFINITE_OBJECT_COUNT: i64 = 1_000_000_000;

impl WorkEstimatorConfig {
    /// `DefaultWorkEstimatorConfig` (config.go:205) under explicit gates.
    pub fn with_gates(
        size_based_list_cost_estimate: bool,
        watch_list: bool,
        consistent_read_supported: bool,
    ) -> Self {
        // config.go:206-209: 10, or 100 under SizeBasedListCostEstimate.
        let maximum_list_seats_limit = if size_based_list_cost_estimate {
            100
        } else {
            10
        };
        Self {
            list: ListWorkEstimatorConfig {
                objects_per_seat: OBJECTS_PER_SEAT,
            },
            mutating: MutatingWorkEstimatorConfig {
                enabled: ENABLE_MUTATING_WORK_ESTIMATOR,
                event_additional_duration: EVENT_ADDITIONAL_DURATION,
                watches_per_seat: WATCHES_PER_SEAT,
            },
            minimum_seats: 1,
            maximum_list_seats_limit,
            maximum_mutating_seats_limit: 10,
            size_based_list_cost_estimate,
            watch_list,
            consistent_read_supported,
        }
    }
}

pub type StatsGetter = Box<dyn Fn(&str) -> Result<Stats, StatsError> + Send + Sync>;
pub type WatchCountGetter = Box<dyn Fn(&RequestInfo) -> i64 + Send + Sync>;
pub type MaxSeatsFn = Box<dyn Fn(&str) -> u64 + Send + Sync>;

/// `NewWorkEstimator` (width.go:75): dispatches to the list and mutating
/// estimators by verb.
pub struct WorkEstimator {
    stats_getter: StatsGetter,
    watch_count_getter: WatchCountGetter,
    config: WorkEstimatorConfig,
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

    /// The interested-watcher count the mutating estimator would use.
    pub(crate) fn watch_count(&self, info: &RequestInfo) -> i64 {
        (self.watch_count_getter)(info)
    }

    /// `workEstimator.estimate` (width.go:107). `query` is `r.URL.RawQuery`.
    pub fn estimate_work(
        &self,
        info: Option<&RequestInfo>,
        query: &str,
        flow_schema: &str,
        priority_level: &str,
    ) -> WorkEstimate {
        let Some(info) = info else {
            // width.go:109-116
            let maximum_seats_limit = self
                .config
                .maximum_list_seats_limit
                .max(self.config.maximum_mutating_seats_limit);
            let mut max_seats = (self.max_seats_fn)(priority_level);
            if max_seats == 0 || max_seats > maximum_seats_limit {
                max_seats = maximum_seats_limit;
            }
            return WorkEstimate {
                initial_seats: max_seats,
                ..Default::default()
            };
        };

        match info.verb.as_str() {
            "list" => return self.estimate_list(info, query, priority_level),
            // width.go:122-130: a watch is costed as a list only under WatchList.
            "watch" if self.config.watch_list => {
                return self.estimate_list(info, query, priority_level)
            }
            "create" | "update" | "patch" | "delete" => {
                return self.estimate_mutating(info, flow_schema, priority_level)
            }
            _ => {}
        }

        // width.go:135
        WorkEstimate {
            initial_seats: self.config.minimum_seats,
            ..Default::default()
        }
    }

    /// `listWorkEstimator.estimate` (list_work_estimator.go:295).
    fn estimate_list(&self, info: &RequestInfo, query: &str, priority_level: &str) -> WorkEstimate {
        let min_seats = self.config.minimum_seats;
        let mut max_seats = (self.max_seats_fn)(priority_level);
        if max_seats == 0 || max_seats > self.config.maximum_list_seats_limit {
            max_seats = self.config.maximum_list_seats_limit;
        }
        let seats = |n: u64| WorkEstimate {
            initial_seats: n,
            ..Default::default()
        };

        let matches_single = !info.name.is_empty();

        let list_options = match ListOptions::from_query(query) {
            Ok(o) => o,
            // :318-323 conversion error: assume the worst.
            Err(_) => return seats(max_seats),
        };

        // :328-334 a watch without initial events costs the minimum.
        if info.verb == "watch" {
            let send_init_events = list_options.send_initial_events == Some(true);
            let legacy_watch =
                list_options.resource_version.is_empty() || list_options.resource_version == "0";
            if !send_init_events && !legacy_watch {
                return seats(self.config.minimum_seats);
            }
        }

        // :336-344
        let list_from_storage =
            list_options.should_delegate_list(self.config.consistent_read_supported);
        let is_list_from_cache = info.verb == "watch" || !list_from_storage;

        // :346-373
        let stats = match (self.stats_getter)(&group_resource_key(info)) {
            Ok(s) => s,
            // ObjectCountStaleErr, or an unexpected error: assume the worst.
            Err(StatsError::Stale) | Err(StatsError::Other(_)) => Stats {
                object_count: INFINITE_OBJECT_COUNT,
                estimated_average_object_size_bytes: MAX_OBJECT_SIZE,
            },
            // ObjectCountNotFoundErr: the resource has no objects (or no CRD).
            Err(StatsError::NotFound) => return seats(min_seats),
        };

        let mut n = if self.config.size_based_list_cost_estimate {
            self.seats_based_on_object_size(
                stats,
                &list_options,
                is_list_from_cache,
                matches_single,
            )
        } else {
            self.seats_based_on_object_count(
                stats,
                &list_options,
                is_list_from_cache,
                matches_single,
            )
        };

        // :383-388
        if n < min_seats {
            n = min_seats;
        }
        if n > max_seats {
            n = max_seats;
        }
        seats(n)
    }

    /// `listWorkEstimator.seatsBasedOnObjectCount` (list_work_estimator.go:392).
    pub fn seats_based_on_object_count(
        &self,
        stats: Stats,
        opts: &ListOptions,
        is_list_from_cache: bool,
        matches_single: bool,
    ) -> u64 {
        let num_stored = stats.object_count;
        let mut limit = num_stored;
        if opts.limit > 0 && opts.limit < num_stored {
            limit = opts.limit;
        }

        let estimated_objects_to_be_processed = if matches_single {
            1
        } else if is_list_from_cache {
            num_stored
        } else if !opts.field_selector.is_empty() || !opts.label_selector.is_empty() {
            num_stored + limit
        } else {
            2 * limit
        };

        (estimated_objects_to_be_processed as f64 / self.config.list.objects_per_seat).ceil() as u64
    }

    /// `listWorkEstimator.seatsBasedOnObjectSize` (list_work_estimator.go:421).
    pub fn seats_based_on_object_size(
        &self,
        mut stats: Stats,
        opts: &ListOptions,
        is_list_from_cache: bool,
        matches_single: bool,
    ) -> u64 {
        if stats.estimated_average_object_size_bytes <= 0 && stats.object_count != 0 {
            stats.estimated_average_object_size_bytes = MAX_OBJECT_SIZE;
        }
        let mut limited = stats.object_count;
        if opts.limit > 0 && opts.limit < limited {
            limited = opts.limit;
        }
        let objects_loaded_in_memory = if matches_single {
            1
        } else if is_list_from_cache {
            limited
        } else if !opts.field_selector.is_empty() || !opts.label_selector.is_empty() {
            limited.max(stats.object_count / 2)
        } else {
            limited
        };

        let mut memory_used_at_once =
            objects_loaded_in_memory * stats.estimated_average_object_size_bytes;
        if is_list_from_cache {
            // :442-444 the cache streams, so its memory use is bounded.
            memory_used_at_once = memory_used_at_once.min(CACHE_WITH_STREAMING_MAX_MEMORY_USAGE);
        }
        (memory_used_at_once as f64 / BYTES_PER_SEAT as f64).ceil() as u64
    }

    /// `mutatingWorkEstimator.estimate` (mutating_work_estimator.go:498).
    fn estimate_mutating(
        &self,
        info: &RequestInfo,
        flow_schema: &str,
        priority_level: &str,
    ) -> WorkEstimate {
        let cfg = &self.config;
        let min_seats = cfg.minimum_seats;
        let mut max_seats = (self.max_seats_fn)(priority_level);
        if max_seats == 0 || max_seats > cfg.maximum_mutating_seats_limit {
            max_seats = cfg.maximum_mutating_seats_limit;
        }

        // :507-511
        if !cfg.mutating.enabled {
            return WorkEstimate {
                initial_seats: min_seats,
                ..Default::default()
            };
        }

        // :524-530
        if is_request_exempt_from_watch_events(info) {
            return WorkEstimate {
                initial_seats: min_seats,
                final_seats: 0,
                additional_latency: Duration::ZERO,
            };
        }

        let watch_count = (self.watch_count_getter)(info);
        // :78
        crate::flow_control_metrics::observe_watch_count(priority_level, flow_schema, watch_count);

        // :549-596
        let mut final_seats: u64 = 0;
        let mut additional_latency = Duration::ZERO;
        if watch_count >= cfg.mutating.watches_per_seat as i64 {
            final_seats = (watch_count as f64 / cfg.mutating.watches_per_seat).ceil() as u64;
            let final_work = seats_times_duration(
                final_seats as f64,
                cfg.mutating
                    .event_additional_duration
                    .as_nanos()
                    .min(i64::MAX as u128) as i64,
            );
            if final_seats > max_seats {
                final_seats = max_seats;
            }
            additional_latency = Duration::from_nanos(
                final_work.duration_per_seat(final_seats as f64).max(0) as u64,
            );
        }

        WorkEstimate {
            initial_seats: 1,
            final_seats,
            additional_latency,
        }
    }
}

/// `isRequestExemptFromWatchEvents` (mutating_work_estimator.go:605).
fn is_request_exempt_from_watch_events(info: &RequestInfo) -> bool {
    info.resource == "serviceaccounts" && info.subresource == "token"
}

/// `key` (list_work_estimator.go:449): `schema.GroupResource.String()`.
fn group_resource_key(info: &RequestInfo) -> String {
    if info.api_group.is_empty() {
        info.resource.clone()
    } else {
        format!("{}.{}", info.resource, info.api_group)
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
    /// `metav1.Convert_url_Values_To_v1_ListOptions`
    /// (zz_generated.conversion.go:370): first value of each key; `limit`
    /// goes through `strconv.ParseInt` and so fails on non-integers; the
    /// bool conversions are `"0"`/`"false"` (any case) -> false, else true.
    pub fn from_query(query: &str) -> Result<Self, String> {
        let mut first: std::collections::HashMap<String, String> = Default::default();
        for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
            first
                .entry(k.into_owned())
                .or_insert_with(|| v.into_owned());
        }
        let s = |k: &str| first.get(k).cloned().unwrap_or_default();
        let limit = match first.get("limit") {
            Some(v) => v.parse::<i64>().map_err(|e| format!("limit {v:?}: {e}"))?,
            None => 0,
        };
        // timeoutSeconds is converted upstream too, so a bad value errors.
        if let Some(v) = first.get("timeoutSeconds") {
            v.parse::<i64>()
                .map_err(|e| format!("timeoutSeconds {v:?}: {e}"))?;
        }
        let go_bool = |v: &str| !(v == "0" || v.eq_ignore_ascii_case("false"));
        Ok(Self {
            label_selector: s("labelSelector"),
            field_selector: s("fieldSelector"),
            resource_version: s("resourceVersion"),
            resource_version_match: s("resourceVersionMatch"),
            limit,
            continue_token: s("continue"),
            send_initial_events: first.get("sendInitialEvents").map(|v| go_bool(v)),
        })
    }

    /// `delegator.ShouldDelegateListMeta(opts, CacheWithoutSnapshots{})`
    /// (delegator/interface.go:27-79): true when the list is served from
    /// storage (etcd) rather than the watch cache.
    pub fn should_delegate_list(&self, consistent_read_supported: bool) -> bool {
        match self.resource_version_match.as_str() {
            // ShouldDelegateExactRV (CacheWithoutSnapshots): delegate.
            "Exact" => true,
            "NotOlderThan" => false,
            "" => {
                // ShouldDelegateContinue (CacheWithoutSnapshots): delegate.
                if !self.continue_token.is_empty() {
                    return true;
                }
                // Legacy exact match.
                if self.limit > 0
                    && !self.resource_version.is_empty()
                    && self.resource_version != "0"
                {
                    return true;
                }
                // Consistent read: ShouldDelegate = !ConsistentReadSupported().
                if self.resource_version.is_empty() {
                    return !consistent_read_supported;
                }
                false
            }
            _ => true,
        }
    }
}
