//! Stub; implemented in the next commit.

use std::future::Future;
use std::time::Duration;

use rusternetes_common::admission::Operation;
use rusternetes_common::Result;

pub const STEP_VALIDATE: &str = "validate";
pub const STEP_ADMIT: &str = "admit";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookRejectionErrorType {
    CallingWebhookError,
    ApiServerInternalError,
    NoError,
}

pub fn observe_admission_step(_: Duration, _: bool, _: &Operation, _: &str) {}
pub fn observe_admission_controller(_: Duration, _: bool, _: &Operation, _: &str, _: &str) {}
pub fn observe_webhook(_: &str, _: Duration, _: bool, _: &Operation, _: &str, _: i32) {}
pub fn observe_webhook_rejection(_: &str, _: &str, _: &str, _: WebhookRejectionErrorType, _: i32) {}
pub fn observe_webhook_fail_open(_: &str, _: &str) {}

pub async fn with_step_metrics<T>(
    _op: &Operation,
    _step_type: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    fut.await
}

pub async fn with_controller_metrics<T>(
    _name: &str,
    _op: &Operation,
    _step_type: &str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    fut.await
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
