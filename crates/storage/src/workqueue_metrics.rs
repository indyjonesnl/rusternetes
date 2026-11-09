//! `workqueue_*` metrics of a named [`crate::WorkQueue`].
//!
//! Ported from client-go `util/workqueue/metrics.go` (`defaultQueueMetrics`,
//! `defaultRetryMetrics`, `newQueueMetrics`) and the Prometheus provider
//! `component-base/metrics/prometheus/workqueue/metrics.go`, release-1.35:
//! seven series labelled by queue `name`:
//!
//! | metric | kind | updated |
//! |---|---|---|
//! | `workqueue_depth` | gauge | `add` +1, `get` -1 |
//! | `workqueue_adds_total` | counter | `add` |
//! | `workqueue_queue_duration_seconds` | histogram | `get`: time since `add` |
//! | `workqueue_work_duration_seconds` | histogram | `done`: time since `get` |
//! | `workqueue_unfinished_work_seconds` | gauge | sum of ages of items in processing |
//! | `workqueue_longest_running_processor_seconds` | gauge | oldest item in processing |
//! | `workqueue_retries_total` | counter | every `AddAfter` (`delaying_queue.go:255`) |
//!
//! As upstream, a queue without a name records nothing (`newQueueMetrics`:
//! `len(name) == 0` selects `noMetrics`).
//!
//! Deviation: `unfinished_work_seconds` / `longest_running_processor_seconds`
//! are refreshed by a task every [`UNFINISHED_WORK_UPDATE_PERIOD`]
//! (`updateUnfinishedWorkLoop`, queue.go:359) which holds only a weak
//! reference to the queue and ends with it or at shutdown.

use prometheus::{
    exponential_buckets, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, Opts,
};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

/// `defaultUnfinishedWorkUpdatePeriod` (queue.go:184).
pub const UNFINISHED_WORK_UPDATE_PERIOD: Duration = Duration::from_millis(500);

const SUBSYSTEM: &str = "workqueue";

struct Vecs {
    depth: GaugeVec,
    adds: IntCounterVec,
    latency: HistogramVec,
    work_duration: HistogramVec,
    unfinished: GaugeVec,
    longest_running_processor: GaugeVec,
    retries: IntCounterVec,
}

static VECS: LazyLock<Vecs> =
    LazyLock::new(|| {
        let label = &["name"];
        // `ExponentialBuckets(10e-9, 10, 10)` (metrics.go:57,66).
        let buckets = || exponential_buckets(10e-9, 10.0, 10).expect("valid buckets");
        let v = Vecs {
        depth: GaugeVec::new(
            Opts::new("depth", "Current depth of workqueue").subsystem(SUBSYSTEM),
            label,
        )
        .expect("valid opts"),
        adds: IntCounterVec::new(
            Opts::new("adds_total", "Total number of adds handled by workqueue")
                .subsystem(SUBSYSTEM),
            label,
        )
        .expect("valid opts"),
        latency: HistogramVec::new(
            HistogramOpts::new(
                "queue_duration_seconds",
                "How long in seconds an item stays in workqueue before being requested.",
            )
            .subsystem(SUBSYSTEM)
            .buckets(buckets()),
            label,
        )
        .expect("valid opts"),
        work_duration: HistogramVec::new(
            HistogramOpts::new(
                "work_duration_seconds",
                "How long in seconds processing an item from workqueue takes.",
            )
            .subsystem(SUBSYSTEM)
            .buckets(buckets()),
            label,
        )
        .expect("valid opts"),
        unfinished: GaugeVec::new(
            Opts::new(
                "unfinished_work_seconds",
                "How many seconds of work has done that is in progress and hasn't been \
                 observed by work_duration. Large values indicate stuck threads. One can \
                 deduce the number of stuck threads by observing the rate at which this \
                 increases.",
            )
            .subsystem(SUBSYSTEM),
            label,
        )
        .expect("valid opts"),
        longest_running_processor: GaugeVec::new(
            Opts::new(
                "longest_running_processor_seconds",
                "How many seconds has the longest running processor for workqueue been running.",
            )
            .subsystem(SUBSYSTEM),
            label,
        )
        .expect("valid opts"),
        retries: IntCounterVec::new(
            Opts::new("retries_total", "Total number of retries handled by workqueue")
                .subsystem(SUBSYSTEM),
            label,
        )
        .expect("valid opts"),
    };
        let r = prometheus::default_registry();
        let _ = r.register(Box::new(v.depth.clone()));
        let _ = r.register(Box::new(v.adds.clone()));
        let _ = r.register(Box::new(v.latency.clone()));
        let _ = r.register(Box::new(v.work_duration.clone()));
        let _ = r.register(Box::new(v.unfinished.clone()));
        let _ = r.register(Box::new(v.longest_running_processor.clone()));
        let _ = r.register(Box::new(v.retries.clone()));
        v
    });

/// `defaultQueueMetrics` + `defaultRetryMetrics` of one named queue.
pub struct QueueMetrics {
    depth: Gauge,
    adds: IntCounter,
    latency: Histogram,
    work_duration: Histogram,
    unfinished: Gauge,
    longest_running_processor: Gauge,
    retries: IntCounter,
    /// When each queued key was added (`addTimes`).
    add_times: HashMap<String, Instant>,
    /// When each key being processed was handed out (`processingStartTimes`).
    processing_start_times: HashMap<String, Instant>,
}

impl QueueMetrics {
    /// `newQueueMetrics` for a non-empty `name`.
    pub fn new(name: &str) -> Self {
        let v = &*VECS;
        Self {
            depth: v.depth.with_label_values(&[name]),
            adds: v.adds.with_label_values(&[name]),
            latency: v.latency.with_label_values(&[name]),
            work_duration: v.work_duration.with_label_values(&[name]),
            unfinished: v.unfinished.with_label_values(&[name]),
            longest_running_processor: v.longest_running_processor.with_label_values(&[name]),
            retries: v.retries.with_label_values(&[name]),
            add_times: HashMap::new(),
            processing_start_times: HashMap::new(),
        }
    }

    /// `add` (metrics.go:91-100). A key that still has an `add` time was
    /// moved out of the queue by the re-process cooldown (see [`Self::defer`])
    /// and is being re-queued: it counts toward depth again but is not a new
    /// add, and keeps its original time so the latency covers the whole wait.
    pub fn add(&mut self, key: &str) {
        if !self.add_times.contains_key(key) {
            self.adds.inc();
            self.add_times.insert(key.to_string(), Instant::now());
        }
        self.depth.inc();
    }

    /// `get` (metrics.go:102-113).
    pub fn get(&mut self, key: &str) {
        self.depth.dec();
        self.processing_start_times
            .insert(key.to_string(), Instant::now());
        if let Some(start) = self.add_times.remove(key) {
            self.latency.observe(start.elapsed().as_secs_f64());
        }
    }

    /// `done` (metrics.go:115-124).
    pub fn done(&mut self, key: &str) {
        if let Some(start) = self.processing_start_times.remove(key) {
            self.work_duration.observe(start.elapsed().as_secs_f64());
        }
    }

    /// A dirty key leaves the queue's count while the re-process cooldown
    /// holds it back (`WorkQueue::done`); its `add` time is kept.
    pub fn defer(&mut self) {
        self.depth.dec();
    }

    /// `retry` (metrics.go:165-171), called from every `AddAfter`.
    pub fn retry(&self) {
        self.retries.inc();
    }

    /// `updateUnfinishedWork` (metrics.go:126-141).
    pub fn update_unfinished_work(&self) {
        let mut total = 0.0;
        let mut oldest = 0.0_f64;
        for start in self.processing_start_times.values() {
            let age = start.elapsed().as_secs_f64();
            total += age;
            oldest = oldest.max(age);
        }
        self.unfinished.set(total);
        self.longest_running_processor.set(oldest);
    }
}
