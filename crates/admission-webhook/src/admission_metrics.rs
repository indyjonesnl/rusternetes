//! `apiserver_admission_*` step / controller / webhook metrics.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/admission/metrics/metrics.go`
//! (release-1.35): `newAdmissionMetrics` (:128-270) for names, help, labels
//! and buckets, `pluginHandlerWithMetrics` (:82-113) as
//! [`with_step_metrics`] / [`with_controller_metrics`], and
//! `ObserveAdmissionStep` (:278-281), `ObserveAdmissionController`
//! (:283-286), `ObserveWebhook` (:288-296), `ObserveWebhookRejection`
//! (:298-306) and `ObserveWebhookFailOpen` (:313-316). and the
//! `match_condition_*` metrics (:225-262, :318-331).
//!
//! Like upstream's `legacyregistry` they live in the process-global registry
//! (`prometheus::default_registry()`), which `/metrics` serves.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{LabelPair, Metric, MetricFamily, MetricType, Summary};
use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};
use rusternetes_common::admission::Operation;
use rusternetes_common::Result;

const NAMESPACE: &str = "apiserver";
const SUBSYSTEM: &str = "admission";

/// metrics.go:59 `stepValidate`.
pub const STEP_VALIDATE: &str = "validate";
/// metrics.go:60 `stepAdmit`.
pub const STEP_ADMIT: &str = "admit";
/// The step type the validating dispatcher passes (validating/dispatcher.go:151,
/// :181-195): `"validating"`, not [`STEP_VALIDATE`]. Upstream's mutating
/// dispatcher passes `"admit"`.
pub const STEP_VALIDATING_WEBHOOK: &str = "validating";

/// `WebhookRejectionErrorType` (metrics.go:30-44).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookRejectionErrorType {
    /// `WebhookRejectionCallingWebhookError`.
    CallingWebhookError,
    /// `WebhookRejectionAPIServerInternalError`.
    ApiServerInternalError,
    /// `WebhookRejectionNoError`.
    NoError,
}

impl WebhookRejectionErrorType {
    fn as_str(self) -> &'static str {
        match self {
            Self::CallingWebhookError => "calling_webhook_error",
            Self::ApiServerInternalError => "apiserver_internal_error",
            Self::NoError => "no_error",
        }
    }
}

/// `string(attr.GetOperation())`.
pub fn operation_label(op: &Operation) -> &'static str {
    match op {
        Operation::Create => "CREATE",
        Operation::Update => "UPDATE",
        Operation::Delete => "DELETE",
        Operation::Connect => "CONNECT",
    }
}

/// `legacyregistry` prefixes the help of a non-stable metric with its level.
fn alpha(help: &str) -> String {
    format!("[ALPHA] {help}")
}

fn histogram(name: &str, help: &str, buckets: Vec<f64>, labels: &[&str]) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM)
            .buckets(buckets),
        labels,
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
}

fn counter(name: &str, help: String, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(SUBSYSTEM),
        labels,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
}

/// A `metrics.SummaryVec` without objectives (metrics.go:146-156 sets only
/// `MaxAge`, so client_golang exports `_sum` and `_count` per label set);
/// `prometheus` has no summary type of its own.
struct SummaryVec {
    desc: Desc,
    series: Mutex<HashMap<Vec<String>, (u64, f64)>>,
}

impl SummaryVec {
    fn new(name: &str, help: String, labels: &[&str]) -> Self {
        let desc = Desc::new(
            format!("{NAMESPACE}_{SUBSYSTEM}_{name}"),
            help,
            labels.iter().map(|l| l.to_string()).collect(),
            HashMap::new(),
        )
        .expect("valid desc");
        Self {
            desc,
            series: Mutex::new(HashMap::new()),
        }
    }

    fn observe(&self, labels: &[&str], v: f64) {
        let mut series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let e = series
            .entry(labels.iter().map(|l| l.to_string()).collect())
            .or_insert((0, 0.0));
        e.0 += 1;
        e.1 += v;
    }
}

/// The registry owns a boxed collector; the metric set keeps observing.
struct SummaryHandle(Arc<SummaryVec>);

impl Collector for SummaryHandle {
    fn desc(&self) -> Vec<&Desc> {
        self.0.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.0.collect()
    }
}

impl Collector for SummaryVec {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let series = self.series.lock().unwrap_or_else(|e| e.into_inner());
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(MetricType::SUMMARY);
        let metrics = series
            .iter()
            .map(|(values, (count, sum))| {
                let mut m = Metric::default();
                let pairs = self
                    .desc
                    .variable_labels
                    .iter()
                    .zip(values)
                    .map(|(n, v)| {
                        let mut p = LabelPair::default();
                        p.set_name(n.clone());
                        p.set_value(v.clone());
                        p
                    })
                    .collect::<Vec<_>>();
                m.set_label(pairs);
                let mut s = Summary::default();
                s.set_sample_count(*count);
                s.set_sample_sum(*sum);
                m.set_summary(s);
                m
            })
            .collect::<Vec<_>>();
        mf.set_metric(metrics);
        vec![mf]
    }
}

/// `metricSet` (metrics.go:333-336).
struct MetricSet {
    latencies: HistogramVec,
    latencies_summary: Option<Arc<SummaryVec>>,
}

impl MetricSet {
    /// `metricSet.observe` (metrics.go:355-361).
    fn observe(&self, elapsed: Duration, labels: &[&str]) {
        let secs = elapsed.as_secs_f64();
        self.latencies.with_label_values(labels).observe(secs);
        if let Some(s) = &self.latencies_summary {
            s.observe(labels, secs);
        }
    }
}

const STEP_LABELS: &[&str] = &["type", "operation", "rejected"];
const NAMED_LABELS: &[&str] = &["name", "type", "operation", "rejected"];
/// "Use buckets ranging from 5 ms to 2.5 seconds." (metrics.go:132, :160).
const LATENCY_BUCKETS: [f64; 6] = [0.005, 0.025, 0.1, 0.5, 1.0, 2.5];

/// metrics.go:133-157.
static STEP: LazyLock<MetricSet> = LazyLock::new(|| {
    let summary = Arc::new(SummaryVec::new(
        "step_admission_duration_seconds_summary",
        alpha("Admission sub-step latency summary in seconds, broken out for each operation and API resource and step type (validate or admit)."),
        STEP_LABELS,
    ));
    let _ = register(Box::new(SummaryHandle(summary.clone())));
    MetricSet {
        latencies: histogram(
            "step_admission_duration_seconds",
            "Admission sub-step latency histogram in seconds, broken out for each operation and API resource and step type (validate or admit).",
            LATENCY_BUCKETS.to_vec(),
            STEP_LABELS,
        ),
        latencies_summary: Some(summary),
    }
});

/// metrics.go:161-175.
static CONTROLLER: LazyLock<MetricSet> = LazyLock::new(|| {
    MetricSet {
    latencies: histogram(
        "controller_admission_duration_seconds",
        "Admission controller latency histogram in seconds, identified by name and broken out for each operation and API resource and type (validate or admit).",
        LATENCY_BUCKETS.to_vec(),
        NAMED_LABELS,
    ),
    latencies_summary: None,
}
});

/// metrics.go:179-193: buckets up to 25s, "admission webhooks timeout at 30
/// seconds by default".
static WEBHOOK: LazyLock<MetricSet> = LazyLock::new(|| {
    MetricSet {
    latencies: histogram(
        "webhook_admission_duration_seconds",
        "Admission webhook latency histogram in seconds, identified by name and broken out for each operation and API resource and type (validate or admit).",
        vec![0.005, 0.025, 0.1, 0.5, 1.0, 2.5, 10.0, 25.0],
        NAMED_LABELS,
    ),
    latencies_summary: None,
}
});

/// metrics.go:195-203.
static WEBHOOK_REJECTION: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "webhook_rejection_count",
        alpha("Admission webhook rejection count, identified by name and broken out for each admission type (validating or admit) and operation. Additional labels specify an error type (calling_webhook_error or apiserver_internal_error if an error occurred; no_error otherwise) and optionally a non-zero rejection code if the webhook rejects the request with an HTTP status code (honored by the apiserver when the code is greater or equal to 400). Codes greater than 600 are truncated to 600, to keep the metrics cardinality bounded."),
        &["name", "type", "operation", "error_type", "rejection_code"],
    )
});

/// metrics.go:205-213.
static WEBHOOK_FAIL_OPEN: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "webhook_fail_open_count",
        alpha("Admission webhook fail open count, identified by name and broken out for each admission type (validating or admit)."),
        &["name", "type"],
    )
});

/// metrics.go:215-223.
static WEBHOOK_REQUEST: LazyLock<IntCounterVec> = LazyLock::new(|| {
    counter(
        "webhook_request_total",
        alpha("Admission webhook request total, identified by name and broken out for each admission type (validating or admit) and operation. Additional labels specify whether the request was rejected or not and an HTTP status code. Codes greater than 600 are truncated to 600, to keep the metrics cardinality bounded."),
        &["name", "type", "operation", "code", "rejected"],
    )
});

/// Register every family up front, as `newAdmissionMetrics` does at package
/// init (metrics.go:260-268), so `/metrics` lists them before the first
/// observation.
pub fn register_all() {
    LazyLock::force(&STEP);
    LazyLock::force(&CONTROLLER);
    LazyLock::force(&WEBHOOK);
    LazyLock::force(&WEBHOOK_REJECTION);
    LazyLock::force(&WEBHOOK_FAIL_OPEN);
    LazyLock::force(&WEBHOOK_REQUEST);
    LazyLock::force(&MATCH_CONDITION_EVAL_ERRORS);
    LazyLock::force(&MATCH_CONDITION_EXCLUSIONS);
    LazyLock::force(&MATCH_CONDITION_EVALUATION_SECONDS);
}

/// `ObserveAdmissionStep` (:278-281): `append(extraLabels, stepType, op, rejected)`.
pub fn observe_admission_step(elapsed: Duration, rejected: bool, op: &Operation, step_type: &str) {
    STEP.observe(
        elapsed,
        &[step_type, operation_label(op), &rejected.to_string()],
    );
}

/// `ObserveAdmissionController` (:283-286): the plugin name is the extra label.
pub fn observe_admission_controller(
    elapsed: Duration,
    rejected: bool,
    op: &Operation,
    step_type: &str,
    name: &str,
) {
    CONTROLLER.observe(
        elapsed,
        &[name, step_type, operation_label(op), &rejected.to_string()],
    );
}

/// `ObserveWebhook` (:288-296).
pub fn observe_webhook(
    name: &str,
    elapsed: Duration,
    rejected: bool,
    op: &Operation,
    step_type: &str,
    code: i32,
) {
    // We truncate codes greater than 600 to keep the cardinality bounded.
    let code = code.min(600);
    let rejected = rejected.to_string();
    WEBHOOK_REQUEST
        .with_label_values(&[
            name,
            step_type,
            operation_label(op),
            &code.to_string(),
            &rejected,
        ])
        .inc();
    WEBHOOK.observe(elapsed, &[name, step_type, operation_label(op), &rejected]);
}

/// `ObserveWebhookRejection` (:298-306).
pub fn observe_webhook_rejection(
    name: &str,
    step_type: &str,
    operation: &str,
    error_type: WebhookRejectionErrorType,
    rejection_code: i32,
) {
    // We truncate codes greater than 600 to keep the cardinality bounded.
    let rejection_code = rejection_code.min(600);
    WEBHOOK_REJECTION
        .with_label_values(&[
            name,
            step_type,
            operation,
            error_type.as_str(),
            &rejection_code.to_string(),
        ])
        .inc();
}

/// `ObserveWebhookFailOpen` (:313-316).
pub fn observe_webhook_fail_open(name: &str, step_type: &str) {
    WEBHOOK_FAIL_OPEN
        .with_label_values(&[name, step_type])
        .inc();
}

/// `WithStepMetrics` (:68-71) around one admit or validate phase:
/// `pluginHandlerWithMetrics.Admit/Validate` (:89-113) time the call and
/// report `rejected = err != nil`.
pub async fn with_step_metrics<T>(
    op: &Operation,
    step_type: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    let start = Instant::now();
    let r = fut.await;
    observe_admission_step(start.elapsed(), r.is_err(), op, step_type);
    r
}

/// `WithControllerMetrics` (:63-66) around one named plugin's call.
pub async fn with_controller_metrics<T>(
    name: &str,
    op: &Operation,
    step_type: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    let start = Instant::now();
    let r = fut.await;
    observe_admission_controller(start.elapsed(), r.is_err(), op, step_type, name);
    r
}

/// metrics.go:56 `kindWebhook` / :57 `kindPolicy`.
pub const KIND_WEBHOOK: &str = "webhook";
pub const KIND_POLICY: &str = "policy";

const MATCH_CONDITION_LABELS: &[&str] = &["name", "kind", "type", "operation"];

pub static MATCH_CONDITION_EVAL_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "match_condition_evaluation_errors_total",
            "[ALPHA] Admission match condition evaluation errors count, identified by name of resource containing the match condition and broken out for each kind containing matchConditions (webhook or policy), operation and admission type (validate or admit).",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        MATCH_CONDITION_LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

pub static MATCH_CONDITION_EXCLUSIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "match_condition_exclusions_total",
            "[ALPHA] Admission match condition evaluation exclusions count, identified by name of resource containing the match condition and broken out for each kind containing matchConditions (webhook or policy), operation and admission type (validate or admit).",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        MATCH_CONDITION_LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

pub static MATCH_CONDITION_EVALUATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "match_condition_evaluation_seconds",
            "[ALPHA] Admission match condition evaluation time in seconds, identified by name and broken out for each kind containing matchConditions (webhook or policy), operation and type (validate or admit).",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM)
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.1, 0.2, 0.25]),
        MATCH_CONDITION_LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
});

/// `ObserveMatchConditionEvalError` (:318-321).
pub fn observe_match_condition_eval_error(
    name: &str,
    kind: &str,
    step_type: &str,
    operation: &str,
) {
    MATCH_CONDITION_EVAL_ERRORS
        .with_label_values(&[name, kind, step_type, operation])
        .inc();
}

/// `ObserveMatchConditionExclusion` (:323-326).
pub fn observe_match_condition_exclusion(name: &str, kind: &str, step_type: &str, operation: &str) {
    MATCH_CONDITION_EXCLUSIONS
        .with_label_values(&[name, kind, step_type, operation])
        .inc();
}

/// `ObserveMatchConditionEvaluationTime` (:328-331).
pub fn observe_match_condition_evaluation_time(
    elapsed: Duration,
    name: &str,
    kind: &str,
    step_type: &str,
    operation: &str,
) {
    MATCH_CONDITION_EVALUATION_SECONDS
        .with_label_values(&[name, kind, step_type, operation])
        .observe(elapsed.as_secs_f64());
}

#[cfg(test)]
mod match_condition_tests {
    use super::*;

    fn counter(c: &IntCounterVec, name: &str, kind: &str, step: &str) -> u64 {
        c.with_label_values(&[name, kind, step, "CREATE"]).get()
    }

    /// metrics_test.go `TestObserveMatchConditionEvalError`.
    #[test]
    fn observe_match_condition_eval_error_counts_per_label_set() {
        for kind in [KIND_WEBHOOK, KIND_POLICY] {
            for step in [STEP_ADMIT, STEP_VALIDATE] {
                observe_match_condition_eval_error("err-x", kind, step, "CREATE");
            }
        }
        for kind in [KIND_WEBHOOK, KIND_POLICY] {
            for step in [STEP_ADMIT, STEP_VALIDATE] {
                assert_eq!(
                    counter(&MATCH_CONDITION_EVAL_ERRORS, "err-x", kind, step),
                    1
                );
            }
        }
    }

    /// metrics_test.go `TestObserveMatchConditionExclusion`.
    #[test]
    fn observe_match_condition_exclusion_counts_per_label_set() {
        for kind in [KIND_WEBHOOK, KIND_POLICY] {
            for step in [STEP_ADMIT, STEP_VALIDATE] {
                observe_match_condition_exclusion("excl-x", kind, step, "CREATE");
            }
        }
        for kind in [KIND_WEBHOOK, KIND_POLICY] {
            for step in [STEP_ADMIT, STEP_VALIDATE] {
                assert_eq!(
                    counter(&MATCH_CONDITION_EXCLUSIONS, "excl-x", kind, step),
                    1
                );
            }
        }
    }

    /// metrics_test.go `TestObserveMatchConditionEvaluationTime`: 2s lands
    /// only in `+Inf`.
    #[test]
    fn observe_match_condition_evaluation_time_records_seconds() {
        observe_match_condition_evaluation_time(
            Duration::from_secs(2),
            "time-x",
            KIND_POLICY,
            STEP_ADMIT,
            "CREATE",
        );
        let h = MATCH_CONDITION_EVALUATION_SECONDS.with_label_values(&[
            "time-x",
            KIND_POLICY,
            STEP_ADMIT,
            "CREATE",
        ]);
        assert_eq!(h.get_sample_count(), 1);
        assert_eq!(h.get_sample_sum(), 2.0);
    }

    /// The registered families carry upstream's names, help prefix, buckets.
    #[test]
    fn families_are_registered_with_upstream_names() {
        observe_match_condition_eval_error("reg-x", KIND_POLICY, STEP_VALIDATE, "CREATE");
        observe_match_condition_exclusion("reg-x", KIND_POLICY, STEP_VALIDATE, "CREATE");
        observe_match_condition_evaluation_time(
            Duration::from_millis(1),
            "reg-x",
            KIND_POLICY,
            STEP_VALIDATE,
            "CREATE",
        );
        let families = prometheus::gather();
        for want in [
            "apiserver_admission_match_condition_evaluation_errors_total",
            "apiserver_admission_match_condition_exclusions_total",
            "apiserver_admission_match_condition_evaluation_seconds",
        ] {
            let mf = families
                .iter()
                .find(|f| f.name() == want)
                .unwrap_or_else(|| panic!("{want} not registered"));
            assert!(mf.help().starts_with("[ALPHA] Admission match condition"));
        }
        let mf = families
            .iter()
            .find(|f| f.name() == "apiserver_admission_match_condition_evaluation_seconds")
            .unwrap();
        let bounds: Vec<f64> = mf.get_metric()[0]
            .get_histogram()
            .get_bucket()
            .iter()
            .map(|b| b.upper_bound())
            .collect();
        assert_eq!(bounds, vec![0.001, 0.005, 0.01, 0.025, 0.1, 0.2, 0.25]);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use prometheus::proto::{Metric, MetricFamily};

    fn find(name: &str, labels: &[(&str, &str)]) -> Option<Metric> {
        let families: Vec<MetricFamily> = prometheus::gather();
        let mf = families.into_iter().find(|f| f.name() == name)?;
        mf.get_metric()
            .iter()
            .find(|m| {
                labels.iter().all(|(k, v)| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                }) && m.get_label().len() == labels.len()
            })
            .cloned()
    }

    /// `expectCounterValue` (testutil_test.go).
    pub(crate) fn counter(name: &str, labels: &[(&str, &str)]) -> u64 {
        find(name, labels)
            .unwrap_or_else(|| panic!("{name}{labels:?} not found"))
            .get_counter()
            .value() as u64
    }

    /// `expectHistogramCountTotal`.
    pub(crate) fn hist_count(name: &str, labels: &[(&str, &str)]) -> u64 {
        find(name, labels)
            .unwrap_or_else(|| panic!("{name}{labels:?} not found"))
            .get_histogram()
            .get_sample_count()
    }

    /// `hist_count`, 0 for a series not yet observed.
    pub(crate) fn hist_count_or_zero(name: &str, labels: &[(&str, &str)]) -> u64 {
        find(name, labels)
            .map(|m| m.get_histogram().get_sample_count())
            .unwrap_or(0)
    }

    /// `expectFindMetric`.
    fn summary_count(name: &str, labels: &[(&str, &str)]) -> u64 {
        find(name, labels)
            .unwrap_or_else(|| panic!("{name}{labels:?} not found"))
            .get_summary()
            .sample_count()
    }

    /// metrics_test.go `TestObserveAdmissionStep` (via `WithStepMetrics`).
    #[tokio::test]
    async fn step_metrics_observe_admit_and_validate() {
        // Operation::Update keeps this test's series apart from the others.
        let op = Operation::Update;
        with_step_metrics(&op, STEP_ADMIT, async { Ok(()) })
            .await
            .unwrap();
        with_step_metrics(&op, STEP_VALIDATE, async { Ok(()) })
            .await
            .unwrap();
        for step in ["admit", "validate"] {
            let l = [
                ("operation", "UPDATE"),
                ("type", step),
                ("rejected", "false"),
            ];
            assert_eq!(
                hist_count("apiserver_admission_step_admission_duration_seconds", &l),
                1
            );
            assert_eq!(
                summary_count(
                    "apiserver_admission_step_admission_duration_seconds_summary",
                    &l
                ),
                1
            );
        }
    }

    /// metrics_test.go `TestObserveAdmissionController` / `TestWithMetrics`:
    /// `rejected` is `err != nil`.
    #[tokio::test]
    async fn controller_metrics_rejected_is_err() {
        let op = Operation::Create;
        with_controller_metrics("ctl-a", &op, STEP_ADMIT, async { Ok(()) })
            .await
            .unwrap();
        let rejected: Result<()> = with_controller_metrics("ctl-a", &op, STEP_VALIDATE, async {
            Err(rusternetes_common::Error::Forbidden("no".into()))
        })
        .await;
        assert!(rejected.is_err());
        let n = "apiserver_admission_controller_admission_duration_seconds";
        assert_eq!(
            hist_count(
                n,
                &[
                    ("name", "ctl-a"),
                    ("operation", "CREATE"),
                    ("type", "admit"),
                    ("rejected", "false")
                ]
            ),
            1
        );
        assert_eq!(
            hist_count(
                n,
                &[
                    ("name", "ctl-a"),
                    ("operation", "CREATE"),
                    ("type", "validate"),
                    ("rejected", "true")
                ]
            ),
            1
        );
    }

    /// metrics_test.go `TestObserveWebhook`: codes above 600 truncate.
    #[test]
    fn observe_webhook_counts_request_and_latency() {
        let op = Operation::Create;
        observe_webhook("wh-x", Duration::from_secs(2), false, &op, STEP_ADMIT, 200);
        observe_webhook(
            "wh-x",
            Duration::from_secs(2),
            true,
            &op,
            STEP_VALIDATE,
            654,
        );
        assert_eq!(
            counter(
                "apiserver_admission_webhook_request_total",
                &[
                    ("name", "wh-x"),
                    ("type", "admit"),
                    ("operation", "CREATE"),
                    ("code", "200"),
                    ("rejected", "false")
                ]
            ),
            1
        );
        assert_eq!(
            counter(
                "apiserver_admission_webhook_request_total",
                &[
                    ("name", "wh-x"),
                    ("type", "validate"),
                    ("operation", "CREATE"),
                    ("code", "600"),
                    ("rejected", "true")
                ]
            ),
            1
        );
        let h = "apiserver_admission_webhook_admission_duration_seconds";
        assert_eq!(
            hist_count(
                h,
                &[
                    ("name", "wh-x"),
                    ("type", "admit"),
                    ("operation", "CREATE"),
                    ("rejected", "false")
                ]
            ),
            1
        );
        assert_eq!(
            hist_count(
                h,
                &[
                    ("name", "wh-x"),
                    ("type", "validate"),
                    ("operation", "CREATE"),
                    ("rejected", "true")
                ]
            ),
            1
        );
    }

    /// metrics_test.go `TestObserveWebhookRejection`.
    #[test]
    fn observe_webhook_rejection_labels() {
        use WebhookRejectionErrorType::*;
        observe_webhook_rejection("rej-x", STEP_ADMIT, "CREATE", NoError, 500);
        observe_webhook_rejection("rej-x", STEP_ADMIT, "CREATE", NoError, 654);
        observe_webhook_rejection("rej-x", STEP_VALIDATE, "UPDATE", CallingWebhookError, 501);
        observe_webhook_rejection("rej-x", STEP_VALIDATE, "UPDATE", ApiServerInternalError, 0);
        let n = "apiserver_admission_webhook_rejection_count";
        for (op, ty, et, code) in [
            ("CREATE", "admit", "no_error", "500"),
            ("CREATE", "admit", "no_error", "600"),
            ("UPDATE", "validate", "calling_webhook_error", "501"),
            ("UPDATE", "validate", "apiserver_internal_error", "0"),
        ] {
            assert_eq!(
                counter(
                    n,
                    &[
                        ("name", "rej-x"),
                        ("operation", op),
                        ("type", ty),
                        ("error_type", et),
                        ("rejection_code", code)
                    ]
                ),
                1
            );
        }
    }

    /// metrics_test.go `TestObserveWebhookFailOpen`.
    #[test]
    fn observe_webhook_fail_open_labels() {
        observe_webhook_fail_open("fo-x", STEP_ADMIT);
        observe_webhook_fail_open("fo-x", STEP_VALIDATE);
        let n = "apiserver_admission_webhook_fail_open_count";
        assert_eq!(counter(n, &[("name", "fo-x"), ("type", "admit")]), 1);
        assert_eq!(counter(n, &[("name", "fo-x"), ("type", "validate")]), 1);
    }

    /// metrics.go:140,168,186: bucket boundaries.
    #[test]
    fn bucket_boundaries_match_upstream() {
        observe_admission_step(Duration::from_millis(1), false, &Operation::Delete, "bkt");
        observe_admission_controller(
            Duration::from_millis(1),
            false,
            &Operation::Delete,
            "bkt",
            "bkt",
        );
        observe_webhook(
            "bkt",
            Duration::from_millis(1),
            false,
            &Operation::Delete,
            "bkt",
            200,
        );
        let bounds = |n: &str| -> Vec<f64> {
            prometheus::gather()
                .into_iter()
                .find(|f| f.name() == n)
                .unwrap_or_else(|| panic!("{n} missing"))
                .get_metric()[0]
                .get_histogram()
                .get_bucket()
                .iter()
                .map(|b| b.upper_bound())
                .collect()
        };
        let six = vec![0.005, 0.025, 0.1, 0.5, 1.0, 2.5];
        assert_eq!(
            bounds("apiserver_admission_step_admission_duration_seconds"),
            six
        );
        assert_eq!(
            bounds("apiserver_admission_controller_admission_duration_seconds"),
            six
        );
        assert_eq!(
            bounds("apiserver_admission_webhook_admission_duration_seconds"),
            vec![0.005, 0.025, 0.1, 0.5, 1.0, 2.5, 10.0, 25.0]
        );
    }
}
