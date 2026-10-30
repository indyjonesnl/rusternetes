//! Unit tests moved from `src/flow_control_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::core::{Collector, Desc};
    pub use prometheus::proto::{Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType};
    pub use prometheus::{
        register, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
    };
    pub use std::collections::BTreeMap;
    pub use std::sync::{Arc, LazyLock, Mutex};
    pub use std::time::{Duration, Instant};
}

mod tests {
    use super::__parent::*;
    use prometheus::core::Collector;
    use prometheus::proto::MetricFamily;
    use rusternetes_api_server::flow_control_metrics::*;
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
