//! `apiserver_flowcontrol_*` metrics.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/util/flowcontrol/metrics/metrics.go`
//! (release-1.35): the metric names, help strings, labels and buckets
//! (:42-362) and the `Add*`/`Observe*` helpers (:528-610). Like upstream's
//! `legacyregistry`, they live in the process-global registry
//! (`prometheus::default_registry()`), which `/metrics` serves.
//!
//! Call sites mirror upstream:
//! - `queueset.go`: `AddReject` (:321 `concurrency-limit`, :340 `queue-full`,
//!   :433 `time-out`), `AddRequestsInQueues`/`AddSeatsInQueues` (:646, :705,
//!   :434), `AddRequestsExecuting`/`AddSeatConcurrencyInUse` (:678, :724,
//!   :866, :878), `ObserveQueueLength` (:577).
//! - `apf_filter.go` `Handle` (:164-201): `AddDispatch`,
//!   `ObserveExecutionDuration`, `observeQueueWaitTime`.
//! - `priority-and-fairness.go:134`: `ObserveWorkEstimatedSeats`.
//!
//! NOT ported here (tracked as follow-ups of #2809): the timing-ratio
//! histograms and `read_vs_write_current_requests` watermarks, the queueset
//! virtual-time gauges (`current_r`, `dispatch_r`, ...), the seat-demand/limit
//! gauges set by the concurrency adjuster, `request_queue_length_after_enqueue` and `watch_count_samples`.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};

const NAMESPACE: &str = "apiserver";
const SUBSYSTEM: &str = "flowcontrol";

/// `requestDurationSecondsBuckets` (metrics.go:50).
const REQUEST_DURATION_SECONDS_BUCKETS: &[f64] = &[
    0.0, 0.005, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0,
];
/// `work_estimated_seats` buckets (metrics.go:329).
const WORK_ESTIMATED_SEATS_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 100.0];

const PL_FS: &[&str] = &["priority_level", "flow_schema"];

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help)
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM)
}

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(opts(name, help), labels).expect("valid metric");
    // `Register()` (metrics.go:56-60) is a once; a duplicate registration is
    // harmless (the vector we keep is still the one we update).
    let _ = register(Box::new(c.clone()));
    c
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let g = IntGaugeVec::new(opts(name, help), labels).expect("valid metric");
    let _ = register(Box::new(g.clone()));
    g
}

fn histogram_vec(name: &str, help: &str, buckets: &[f64], labels: &[&str]) -> HistogramVec {
    let o = HistogramOpts::new(name, help)
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM)
        .buckets(buckets.to_vec());
    let h = HistogramVec::new(o, labels).expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
}

static REJECTED_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "rejected_requests_total",
        "Number of requests rejected by API Priority and Fairness subsystem",
        &["priority_level", "flow_schema", "reason"],
    )
});
static DISPATCHED_REQUESTS_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter_vec(
        "dispatched_requests_total",
        "Number of requests executed by API Priority and Fairness subsystem",
        PL_FS,
    )
});
static CURRENT_INQUEUE_REQUESTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "current_inqueue_requests",
        "Number of requests currently pending in queues of the API Priority and Fairness subsystem",
        PL_FS,
    )
});
static CURRENT_INQUEUE_SEATS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "current_inqueue_seats",
        "Number of seats currently pending in queues of the API Priority and Fairness subsystem",
        PL_FS,
    )
});
static CURRENT_EXECUTING_REQUESTS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "current_executing_requests",
        "Number of requests in initial (for a WATCH) or any (for a non-WATCH) execution stage in the API Priority and Fairness subsystem",
        PL_FS,
    )
});
static CURRENT_EXECUTING_SEATS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "current_executing_seats",
        "Concurrency (number of seats) occupied by the currently executing (initial stage for a WATCH, any stage otherwise) requests in the API Priority and Fairness subsystem",
        PL_FS,
    )
});
// Deprecated upstream (1.31) but still exposed, with the same value as
// `current_executing_seats` (`AddSeatConcurrencyInUse`, metrics.go:559-563).
static REQUEST_CONCURRENCY_IN_USE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "request_concurrency_in_use",
        "Concurrency (number of seats) occupied by the currently executing (initial stage for a WATCH, any stage otherwise) requests in the API Priority and Fairness subsystem",
        PL_FS,
    )
});
static REQUEST_WAIT_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "request_wait_duration_seconds",
        "Length of time a request spent waiting in its queue",
        REQUEST_DURATION_SECONDS_BUCKETS,
        &["priority_level", "flow_schema", "execute"],
    )
});
static REQUEST_EXECUTION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "request_execution_seconds",
        "Duration of initial stage (for a WATCH) or any (for a non-WATCH) stage of request execution in the API Priority and Fairness subsystem",
        REQUEST_DURATION_SECONDS_BUCKETS,
        &["priority_level", "flow_schema", "type"],
    )
});
static WORK_ESTIMATED_SEATS: LazyLock<HistogramVec> = LazyLock::new(|| {
    histogram_vec(
        "work_estimated_seats",
        "Number of estimated seats (maximum of initial and final seats) associated with requests in API Priority and Fairness",
        WORK_ESTIMATED_SEATS_BUCKETS,
        PL_FS,
    )
});

/// Touch every metric so all families are registered (`Register()`,
/// metrics.go:56-60). Called when the filter is built.
pub fn register_all() {
    LazyLock::force(&REJECTED_REQUESTS_TOTAL);
    LazyLock::force(&DISPATCHED_REQUESTS_TOTAL);
    LazyLock::force(&CURRENT_INQUEUE_REQUESTS);
    LazyLock::force(&CURRENT_INQUEUE_SEATS);
    LazyLock::force(&CURRENT_EXECUTING_REQUESTS);
    LazyLock::force(&CURRENT_EXECUTING_SEATS);
    LazyLock::force(&REQUEST_CONCURRENCY_IN_USE);
    LazyLock::force(&REQUEST_WAIT_DURATION_SECONDS);
    LazyLock::force(&REQUEST_EXECUTION_SECONDS);
    LazyLock::force(&WORK_ESTIMATED_SEATS);
}

/// `AddReject` (metrics.go:565).
pub fn add_reject(priority_level: &str, flow_schema: &str, reason: &str) {
    REJECTED_REQUESTS_TOTAL
        .with_label_values(&[priority_level, flow_schema, reason])
        .inc();
}

/// `AddDispatch` (metrics.go:570).
pub fn add_dispatch(priority_level: &str, flow_schema: &str) {
    DISPATCHED_REQUESTS_TOTAL
        .with_label_values(&[priority_level, flow_schema])
        .inc();
}

/// `AddRequestsInQueues` + `AddSeatsInQueues` (metrics.go:528-537).
pub fn add_in_queues(priority_level: &str, flow_schema: &str, requests: i64, seats: i64) {
    CURRENT_INQUEUE_REQUESTS
        .with_label_values(&[priority_level, flow_schema])
        .add(requests);
    CURRENT_INQUEUE_SEATS
        .with_label_values(&[priority_level, flow_schema])
        .add(seats);
}

/// `AddRequestsExecuting` + `AddSeatConcurrencyInUse` (metrics.go:538, :559).
pub fn add_executing(priority_level: &str, flow_schema: &str, requests: i64, seats: i64) {
    let l = [priority_level, flow_schema];
    CURRENT_EXECUTING_REQUESTS
        .with_label_values(&l)
        .add(requests);
    CURRENT_EXECUTING_SEATS.with_label_values(&l).add(seats);
    REQUEST_CONCURRENCY_IN_USE.with_label_values(&l).add(seats);
}

/// `ObserveWaitingDuration` (metrics.go:580).
pub fn observe_waiting_duration(
    priority_level: &str,
    flow_schema: &str,
    execute: bool,
    wait: Duration,
) {
    REQUEST_WAIT_DURATION_SECONDS
        .with_label_values(&[priority_level, flow_schema, &execute.to_string()])
        .observe(wait.as_secs_f64());
}

/// `ObserveExecutionDuration` (metrics.go:585): `type` is `watch` for a WATCH
/// request and `regular` otherwise.
pub fn observe_execution_duration(
    priority_level: &str,
    flow_schema: &str,
    is_watch: bool,
    execution: Duration,
) {
    let kind = if is_watch { "watch" } else { "regular" };
    REQUEST_EXECUTION_SECONDS
        .with_label_values(&[priority_level, flow_schema, kind])
        .observe(execution.as_secs_f64());
}

/// `ObserveWorkEstimatedSeats` (metrics.go:604).
pub fn observe_work_estimated_seats(priority_level: &str, flow_schema: &str, seats: usize) {
    WORK_ESTIMATED_SEATS
        .with_label_values(&[priority_level, flow_schema])
        .observe(seats as f64);
}
