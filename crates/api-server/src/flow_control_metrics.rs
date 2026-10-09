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
//! Also ported (#2960): `TimingRatioHistogramVec` (metrics/timing_ratio_histogram.go
//! over component-base/metrics/prometheusextension/timing_histogram.go and
//! weighted_histogram.go), the per-level `priority_level_seat_utilization` /
//! `priority_level_request_utilization`, `read_vs_write_current_requests`, and
//! the limit/demand gauges the concurrency adjuster sets.
//!
//! NOT ported here (tracked as follow-ups): the `demand_seats` timing-ratio
//! histogram, the queueset virtual-time gauges (`current_r`, `dispatch_r`,
//! ...), `request_queue_length_after_enqueue`, `epoch_advance_total`,
//! `request_dispatch_no_accommodation_total`, `watch_count_samples`,
//! `RecordDroppedRequest`/`RecordRequestTermination`.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{
    register, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
};

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
    LazyLock::force(&PRIORITY_LEVEL_SEAT_UTILIZATION);
    LazyLock::force(&PRIORITY_LEVEL_REQUEST_UTILIZATION);
    LazyLock::force(&READ_VS_WRITE_CURRENT_REQUESTS);
    LazyLock::force(&REQUEST_CONCURRENCY_LIMIT);
    LazyLock::force(&NOMINAL_LIMIT_SEATS);
    LazyLock::force(&LOWER_LIMIT_SEATS);
    LazyLock::force(&UPPER_LIMIT_SEATS);
    LazyLock::force(&SEAT_DEMAND_HIGH_WATERMARK);
    LazyLock::force(&SEAT_DEMAND_AVERAGE);
    LazyLock::force(&SEAT_DEMAND_STDEV);
    LazyLock::force(&SEAT_DEMAND_SMOOTHED);
    LazyLock::force(&TARGET_SEATS);
    LazyLock::force(&CURRENT_LIMIT_SEATS);
    LazyLock::force(&SEAT_FAIR_FRAC);
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

// ---- timing-ratio histograms and the remaining gauges (#2960) ----

/// A clock the timing histograms read (upstream `nowFunc`).
pub type NowFn = Arc<dyn Fn() -> Instant + Send + Sync>;

/// One element of a [`TimingRatioHistogramVec`]: `timingRatioHistogramInner`
/// (metrics/timing_ratio_histogram.go:50-56) over a `timingHistogram`
/// (component-base/metrics/prometheusextension/timing_histogram.go:112-117)
/// whose value is the ratio.
struct Member {
    numerator: f64,
    denominator: f64,
    /// `lastSetTime`
    last_set: Instant,
    /// Nanoseconds spent in each bucket (the last one is `+Inf`).
    buckets: Vec<u64>,
    /// The integral over time (in nanoseconds) of the ratio.
    sum: f64,
}

struct TimingInner {
    now: NowFn,
    fq_name: String,
    help: String,
    upper_bounds: Vec<f64>,
    const_labels: Vec<(String, String)>,
    label_names: Vec<String>,
    desc: Desc,
    members: Mutex<BTreeMap<Vec<String>, Member>>,
}

/// `TimingRatioHistogramVec` (timing_ratio_histogram.go:128-195): a gauge for
/// a ratio whose numerator and denominator are controlled independently;
/// scraped, it is a histogram of the ratio weighted by the nanoseconds spent
/// at each value. Members are created on first use, as upstream's
/// `NewForLabelValuesSafe(0, 1, ...)` does (initial numerator 0, denominator 1).
#[derive(Clone)]
pub struct TimingRatioHistogramVec(Arc<TimingInner>);

impl TimingRatioHistogramVec {
    /// `NewTestableTimingRatioHistogramVec`; `buckets` must be strictly
    /// increasing (`newWeightedHistogram`, weighted_histogram.go:55-71; a
    /// trailing `+Inf` is dropped).
    pub fn with_clock(
        now: NowFn,
        fq_name: &str,
        help: &str,
        buckets: &[f64],
        const_labels: &[(&str, &str)],
        label_names: &[&str],
    ) -> Self {
        let mut upper_bounds = buckets.to_vec();
        if upper_bounds.last().is_some_and(|b| b.is_infinite()) {
            upper_bounds.pop();
        }
        assert!(
            upper_bounds.windows(2).all(|w| w[0] < w[1]),
            "histogram buckets must be in increasing order"
        );
        // `wrapTimingHelp` / `wrapWeightedHelp`.
        let help = format!("EXPERIMENTAL: {help}");
        let const_labels: Vec<(String, String)> = const_labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let desc = Desc::new(
            fq_name.to_string(),
            help.clone(),
            label_names.iter().map(|l| l.to_string()).collect(),
            const_labels.iter().cloned().collect(),
        )
        .expect("valid desc");
        Self(Arc::new(TimingInner {
            now,
            fq_name: fq_name.to_string(),
            help,
            upper_bounds,
            const_labels,
            label_names: label_names.iter().map(|l| l.to_string()).collect(),
            desc,
            members: Mutex::new(BTreeMap::new()),
        }))
    }

    pub fn new(
        fq_name: &str,
        help: &str,
        buckets: &[f64],
        const_labels: &[(&str, &str)],
        label_names: &[&str],
    ) -> Self {
        Self::with_clock(
            Arc::new(Instant::now),
            fq_name,
            help,
            buckets,
            const_labels,
            label_names,
        )
    }

    /// `timingHistogram.update`: account the time since the last change at
    /// the old ratio, then apply `f` to the member.
    fn update(&self, labels: &[&str], f: impl FnOnce(&mut Member)) {
        let inner = &self.0;
        let now = (inner.now)();
        let mut members = inner.members.lock().unwrap();
        let m = members
            .entry(labels.iter().map(|l| l.to_string()).collect())
            .or_insert_with(|| Member {
                numerator: 0.0,
                denominator: 1.0,
                last_set: now,
                buckets: vec![0; inner.upper_bounds.len() + 1],
                sum: 0.0,
            });
        inner.account(m, now);
        f(m);
    }

    /// `Set` (timing_ratio_histogram.go:77).
    pub fn set(&self, labels: &[&str], numerator: f64) {
        self.update(labels, |m| m.numerator = numerator);
    }

    /// `Add` (timing_ratio_histogram.go:86).
    pub fn add(&self, labels: &[&str], delta: f64) {
        self.update(labels, |m| m.numerator += delta);
    }

    /// `SetDenominator` (timing_ratio_histogram.go:115).
    pub fn set_denominator(&self, labels: &[&str], denominator: f64) {
        self.update(labels, |m| m.denominator = denominator);
    }
}

impl TimingInner {
    /// `weightedHistogram.observeWithWeightLocked` for the time since
    /// `last_set` at the member's current ratio (`if delta > 0`,
    /// timing_histogram.go:178-182).
    fn account(&self, m: &mut Member, now: Instant) {
        let delta = now.saturating_duration_since(m.last_set).as_nanos() as u64;
        if delta == 0 {
            return;
        }
        let ratio = m.numerator / m.denominator;
        // `sort.SearchFloat64s`: the first bound >= the value.
        let idx = self.upper_bounds.partition_point(|b| *b < ratio);
        m.buckets[idx] += delta;
        m.sum += delta as f64 * ratio;
        m.last_set = now;
    }
}

impl Collector for TimingRatioHistogramVec {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    /// `timingHistogram.Write` (`th.Add(0)` accounts for the time since the
    /// last update) then `weightedHistogram.Write` (:160-180).
    fn collect(&self) -> Vec<MetricFamily> {
        let inner = &self.0;
        let now = (inner.now)();
        let mut members = inner.members.lock().unwrap();
        let mut metrics = Vec::with_capacity(members.len());
        for (values, m) in members.iter_mut() {
            inner.account(m, now);
            let mut labels: Vec<(String, String)> = inner.const_labels.clone();
            labels.extend(
                inner
                    .label_names
                    .iter()
                    .cloned()
                    .zip(values.iter().cloned()),
            );
            labels.sort();
            let mut metric = Metric::default();
            metric.set_label(
                labels
                    .into_iter()
                    .map(|(k, v)| {
                        let mut lp = LabelPair::default();
                        lp.set_name(k);
                        lp.set_value(v);
                        lp
                    })
                    .collect(),
            );
            let mut h = Histogram::default();
            let mut cumulative = 0u64;
            let mut bs = Vec::with_capacity(inner.upper_bounds.len());
            for (idx, ub) in inner.upper_bounds.iter().enumerate() {
                cumulative += m.buckets[idx];
                let mut b = Bucket::default();
                b.set_upper_bound(*ub);
                b.set_cumulative_count(cumulative);
                bs.push(b);
            }
            cumulative += m.buckets[inner.upper_bounds.len()];
            h.set_bucket(bs);
            h.set_sample_count(cumulative);
            h.set_sample_sum(m.sum);
            metric.set_histogram(h);
            metrics.push(metric);
        }
        let mut mf = MetricFamily::default();
        mf.set_name(inner.fq_name.clone());
        mf.set_help(inner.help.clone());
        mf.set_field_type(MetricType::HISTOGRAM);
        mf.set_metric(metrics);
        vec![mf]
    }
}

fn float_gauge_vec(name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(opts(name, help), labels).expect("valid metric");
    let _ = register(Box::new(g.clone()));
    g
}

fn timing_ratio_vec(
    name: &str,
    help: &str,
    buckets: &[f64],
    const_labels: &[(&str, &str)],
    labels: &[&str],
) -> TimingRatioHistogramVec {
    let v = TimingRatioHistogramVec::new(
        &format!("{NAMESPACE}_{SUBSYSTEM}_{name}"),
        help,
        buckets,
        const_labels,
        labels,
    );
    let _ = register(Box::new(v.clone()));
    v
}

const PHASE_WAITING: &str = "waiting";
const PHASE_EXECUTING: &str = "executing";
/// `epmetrics.ReadOnlyKind` / `MutatingKind` (endpoints/metrics/metrics.go:369-371).
const KIND_READ_ONLY: &str = "readOnly";
const KIND_MUTATING: &str = "mutating";

/// `PriorityLevelExecutionSeatsGaugeVec` (metrics.go:112-123).
static PRIORITY_LEVEL_SEAT_UTILIZATION: LazyLock<TimingRatioHistogramVec> = LazyLock::new(|| {
    timing_ratio_vec(
        "priority_level_seat_utilization",
        "Observations, at the end of every nanosecond, of utilization of seats for any stage of execution (but only initial stage for WATCHes)",
        &[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0],
        &[("phase", PHASE_EXECUTING)],
        &["priority_level"],
    )
});
/// `PriorityLevelConcurrencyGaugeVec` (metrics.go:126-137).
static PRIORITY_LEVEL_REQUEST_UTILIZATION: LazyLock<TimingRatioHistogramVec> = LazyLock::new(
    || {
        timing_ratio_vec(
            "priority_level_request_utilization",
            "Observations, at the end of every nanosecond, of number of requests (as a fraction of the relevant limit) waiting or in any stage of execution (but only initial stage for WATCHes)",
            &[0.0, 0.001, 0.003, 0.01, 0.03, 0.1, 0.25, 0.5, 0.75, 1.0],
            &[],
            &["phase", "priority_level"],
        )
    },
);
/// `readWriteConcurrencyGaugeVec` (metrics.go:140-151).
static READ_VS_WRITE_CURRENT_REQUESTS: LazyLock<TimingRatioHistogramVec> = LazyLock::new(|| {
    timing_ratio_vec(
        "read_vs_write_current_requests",
        "Observations, at the end of every nanosecond, of the number of requests (as a fraction of the relevant limit) waiting or in regular stage of execution",
        &[
            0.0, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0,
        ],
        &[],
        &["phase", "request_kind"],
    )
});

static REQUEST_CONCURRENCY_LIMIT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "request_concurrency_limit",
        "Nominal number of execution seats configured for each priority level",
        &["priority_level"],
    )
});
static NOMINAL_LIMIT_SEATS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "nominal_limit_seats",
        "Nominal number of execution seats configured for each priority level",
        &["priority_level"],
    )
});
static LOWER_LIMIT_SEATS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "lower_limit_seats",
        "Configured lower bound on number of execution seats available to each priority level",
        &["priority_level"],
    )
});
static UPPER_LIMIT_SEATS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    gauge_vec(
        "upper_limit_seats",
        "Configured upper bound on number of execution seats available to each priority level",
        &["priority_level"],
    )
});
static SEAT_DEMAND_HIGH_WATERMARK: LazyLock<GaugeVec> = LazyLock::new(|| {
    float_gauge_vec(
        "demand_seats_high_watermark",
        "High watermark, over last adjustment period, of demand_seats",
        &["priority_level"],
    )
});
static SEAT_DEMAND_AVERAGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    float_gauge_vec(
        "demand_seats_average",
        "Time-weighted average, over last adjustment period, of demand_seats",
        &["priority_level"],
    )
});
static SEAT_DEMAND_STDEV: LazyLock<GaugeVec> = LazyLock::new(|| {
    float_gauge_vec(
        "demand_seats_stdev",
        "Time-weighted standard deviation, over last adjustment period, of demand_seats",
        &["priority_level"],
    )
});
static SEAT_DEMAND_SMOOTHED: LazyLock<GaugeVec> = LazyLock::new(|| {
    float_gauge_vec(
        "demand_seats_smoothed",
        "Smoothed seat demands",
        &["priority_level"],
    )
});
static TARGET_SEATS: LazyLock<GaugeVec> = LazyLock::new(|| {
    float_gauge_vec(
        "target_seats",
        "Seat allocation targets",
        &["priority_level"],
    )
});
static CURRENT_LIMIT_SEATS: LazyLock<GaugeVec> = LazyLock::new(|| {
    float_gauge_vec(
        "current_limit_seats",
        "current derived number of execution seats available to each priority level",
        &["priority_level"],
    )
});
static SEAT_FAIR_FRAC: LazyLock<Gauge> = LazyLock::new(|| {
    let g = Gauge::with_opts(opts(
        "seat_fair_frac",
        "Fair fraction of server's concurrency to allocate to each priority level that can use it",
    ))
    .expect("valid metric");
    let _ = register(Box::new(g.clone()));
    g
});

/// `SetPriorityLevelConfiguration` (metrics.go:614), called from
/// `finishQueueSetReconfigsLocked` (apf_controller.go:874).
pub fn set_priority_level_configuration(pl: &str, nominal: i64, min: i64, max: i64) {
    REQUEST_CONCURRENCY_LIMIT
        .with_label_values(&[pl])
        .set(nominal);
    NOMINAL_LIMIT_SEATS.with_label_values(&[pl]).set(nominal);
    LOWER_LIMIT_SEATS.with_label_values(&[pl]).set(min);
    UPPER_LIMIT_SEATS.with_label_values(&[pl]).set(max);
}

/// `NotePriorityLevelConcurrencyAdjustment` (metrics.go:621), called from
/// `updateBorrowingLocked` (apf_controller.go:481).
pub fn note_priority_level_concurrency_adjustment(
    pl: &str,
    hwm: f64,
    avg: f64,
    stdev: f64,
    smoothed: f64,
    target: f64,
    current_cl: i64,
) {
    let l = [pl];
    SEAT_DEMAND_HIGH_WATERMARK.with_label_values(&l).set(hwm);
    SEAT_DEMAND_AVERAGE.with_label_values(&l).set(avg);
    SEAT_DEMAND_STDEV.with_label_values(&l).set(stdev);
    SEAT_DEMAND_SMOOTHED.with_label_values(&l).set(smoothed);
    TARGET_SEATS.with_label_values(&l).set(target);
    CURRENT_LIMIT_SEATS
        .with_label_values(&l)
        .set(current_cl as f64);
}

/// `SetFairFrac` (metrics.go:630).
pub fn set_fair_frac(fair_frac: f64) {
    SEAT_FAIR_FRAC.set(fair_frac);
}

/// `queueset.go:282-284`: the denominators of a level's gauges, the queue
/// capacity (`qll`) for requests waiting and `ConcurrencyDenominator` for
/// requests and seats executing.
pub fn set_level_denominators(pl: &str, queue_capacity: f64, concurrency_denominator: f64) {
    PRIORITY_LEVEL_REQUEST_UTILIZATION.set_denominator(&[PHASE_WAITING, pl], queue_capacity);
    PRIORITY_LEVEL_REQUEST_UTILIZATION
        .set_denominator(&[PHASE_EXECUTING, pl], concurrency_denominator);
    PRIORITY_LEVEL_SEAT_UTILIZATION.set_denominator(&[pl], concurrency_denominator);
}

/// `qs.reqsGaugePair.RequestsWaiting.Add` (queueset.go:437, :649, :708).
pub fn add_level_waiting(pl: &str, delta: f64) {
    PRIORITY_LEVEL_REQUEST_UTILIZATION.add(&[PHASE_WAITING, pl], delta);
}

/// `RequestsExecuting.Add` + `execSeatsGauge.Add` (queueset.go:680-681,
/// :726-727, :867, :879).
pub fn add_level_executing(pl: &str, requests: f64, seats: f64) {
    PRIORITY_LEVEL_REQUEST_UTILIZATION.add(&[PHASE_EXECUTING, pl], requests);
    PRIORITY_LEVEL_SEAT_UTILIZATION.add(&[pl], seats);
}

/// The phase of a request for the read-vs-write histograms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Waiting,
    Executing,
}

/// `apf_controller.go:705-708`: the read-vs-write denominators are the sums,
/// over all levels, of queue capacity (`maxWaitingRequests`) and nominal
/// concurrency (`maxExecutingRequests`).
pub fn set_read_write_denominators(max_waiting: f64, max_executing: f64) {
    for kind in [KIND_READ_ONLY, KIND_MUTATING] {
        READ_VS_WRITE_CURRENT_REQUESTS.set_denominator(&[PHASE_WAITING, kind], max_waiting);
        READ_VS_WRITE_CURRENT_REQUESTS.set_denominator(&[PHASE_EXECUTING, kind], max_executing);
    }
}

/// `noteWaitingDelta` / `noteExecutingDelta` (priority-and-fairness.go:150-156).
pub fn add_read_write(phase: Phase, mutating: bool, delta: i64) {
    let phase = match phase {
        Phase::Waiting => PHASE_WAITING,
        Phase::Executing => PHASE_EXECUTING,
    };
    let kind = if mutating {
        KIND_MUTATING
    } else {
        KIND_READ_ONLY
    };
    READ_VS_WRITE_CURRENT_REQUESTS.add(&[phase, kind], delta as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::core::Collector;
    use prometheus::proto::MetricFamily;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    /// A settable clock (upstream's `testclock.FakeClock`).
    struct Fake {
        base: Instant,
        nanos: AtomicU64,
    }
    impl Fake {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                base: Instant::now(),
                nanos: AtomicU64::new(0),
            })
        }
        fn advance(&self, d: Duration) {
            self.nanos.fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
        }
        fn now_fn(self: &Arc<Self>) -> NowFn {
            let me = self.clone();
            Arc::new(move || me.base + Duration::from_nanos(me.nanos.load(Ordering::SeqCst)))
        }
    }

    fn only(mfs: Vec<MetricFamily>) -> MetricFamily {
        assert_eq!(mfs.len(), 1, "one family");
        mfs.into_iter().next().unwrap()
    }

    /// `(upper_bound, cumulative_count)` pairs of the first metric.
    fn buckets(mf: &MetricFamily) -> Vec<(f64, u64)> {
        mf.get_metric()[0]
            .get_histogram()
            .get_bucket()
            .iter()
            .map(|b| (b.upper_bound(), b.cumulative_count()))
            .collect()
    }

    // TestTimingRatioHistogramVecElementSimple
    // (flowcontrol/metrics/timing_ratio_histogram_test.go:39) and
    // TestTimeIntegrationDirect (prometheusextension/timing_histogram_test.go:145):
    // the histogram is weighted by nanoseconds spent at each ratio.
    #[test]
    fn a_ratio_is_weighted_by_the_nanoseconds_spent_at_it() {
        let clk = Fake::new();
        let v = TimingRatioHistogramVec::with_clock(
            clk.now_fn(),
            "test_ratio",
            "help",
            &[0.0, 0.5, 1.0],
            &[("phase", "executing")],
            &["priority_level"],
        );
        let l = ["pl"];
        v.set_denominator(&l, 4.0);
        v.set(&l, 1.0); // ratio 0.25
        clk.advance(Duration::from_nanos(100));
        v.add(&l, 2.0); // ratio 0.75
        clk.advance(Duration::from_nanos(10));
        v.set_denominator(&l, 2.0); // ratio 1.5
        clk.advance(Duration::from_nanos(1));
        let mf = only(v.collect());
        let h = mf.get_metric()[0].get_histogram();
        assert_eq!(h.get_sample_count(), 111);
        // 100 * 0.25 + 10 * 0.75 + 1 * 1.5
        assert!(
            (h.get_sample_sum() - 34.0).abs() < 1e-9,
            "{}",
            h.get_sample_sum()
        );
        // le=0:0  le=0.5:100  le=1:110  (+Inf: 111)
        assert_eq!(buckets(&mf), vec![(0.0, 0), (0.5, 100), (1.0, 110)]);
        let labels: Vec<_> = mf.get_metric()[0]
            .get_label()
            .iter()
            .map(|l| (l.name().to_string(), l.value().to_string()))
            .collect();
        assert_eq!(
            labels,
            vec![
                ("phase".to_string(), "executing".to_string()),
                ("priority_level".to_string(), "pl".to_string())
            ]
        );
        assert!(mf.help().starts_with("EXPERIMENTAL: "), "{}", mf.help());
    }

    // A scrape accounts for the time since the last update at the current
    // value (`timingHistogram.Write`: `th.Add(0)`).
    #[test]
    fn a_scrape_accounts_for_time_since_the_last_update() {
        let clk = Fake::new();
        let v = TimingRatioHistogramVec::with_clock(
            clk.now_fn(),
            "test_ratio2",
            "help",
            &[0.5, 1.0],
            &[],
            &["k"],
        );
        v.set_denominator(&["a"], 1.0);
        v.set(&["a"], 1.0);
        clk.advance(Duration::from_nanos(7));
        assert_eq!(
            only(v.collect()).get_metric()[0]
                .get_histogram()
                .get_sample_count(),
            7
        );
        clk.advance(Duration::from_nanos(3));
        assert_eq!(
            only(v.collect()).get_metric()[0]
                .get_histogram()
                .get_sample_count(),
            10
        );
    }

    // The default denominator is 1 and the initial numerator 0
    // (`NewForLabelValuesSafe(0, 1, ...)`, apf_controller.go:722).
    #[test]
    fn members_are_per_label_values() {
        let clk = Fake::new();
        let v = TimingRatioHistogramVec::with_clock(
            clk.now_fn(),
            "test_ratio3",
            "help",
            &[0.5],
            &[],
            &["k"],
        );
        v.set(&["a"], 1.0);
        v.set(&["b"], 0.25);
        clk.advance(Duration::from_nanos(4));
        let mf = only(v.collect());
        assert_eq!(mf.get_metric().len(), 2);
    }
}
