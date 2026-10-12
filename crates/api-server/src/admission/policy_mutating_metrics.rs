//! `apiserver_mutating_admission_policy_*` metrics.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/metrics/metrics.go` (release-1.35): names, help, labels and
//! buckets (:27-92), `ObserveAdmission` (:100-104) and `ObserveRejection`
//! (:106-110), and its test `metrics_test.go` `TestNoUtils`. Registered in the
//! process-global registry like upstream's `legacyregistry`.

#![allow(dead_code)]

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};

const NAMESPACE: &str = "apiserver";
const SUBSYSTEM: &str = "mutating_admission_policy";

/// `MutationErrorType` (:32-46).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationErrorType {
    /// `MutationCompileError`: the expression fails to compile.
    CompileError,
    /// `MutatingInvalidError`: the expression fails due to internal errors
    /// that are out of the control of the user.
    InvalidError,
    /// `MutatingOutOfBudget`: the expression ran out of cost budget, or the
    /// budget cannot be obtained.
    OutOfBudget,
    /// `MutationNoError`: the expression returns without an error.
    NoError,
}

impl MutationErrorType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CompileError => "compile_error",
            Self::InvalidError => "invalid_error",
            Self::OutOfBudget => "out_of_budget",
            Self::NoError => "no_error",
        }
    }
}

const LABELS: &[&str] = &["policy", "policy_binding", "error_type"];

pub(crate) static POLICY_CHECK: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "check_total",
            "[ALPHA] Mutation admission policy check total, labeled by policy and further identified by binding.",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

pub(crate) static POLICY_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let h = HistogramVec::new(
        HistogramOpts::new(
            "check_duration_seconds",
            "[ALPHA] Mutation admission latency for individual mutation expressions in seconds, labeled by policy and binding.",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM)
        .buckets(vec![0.0000005, 0.001, 0.01, 0.1, 1.0]),
        LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(h.clone()));
    h
});

fn record(elapsed: Duration, policy: &str, binding: &str, error_type: MutationErrorType) {
    let labels = [policy, binding, error_type.as_str()];
    POLICY_CHECK.with_label_values(&labels).inc();
    POLICY_LATENCY
        .with_label_values(&labels)
        .observe(elapsed.as_secs_f64());
}

/// `ObserveAdmission` (:100-104).
pub fn observe_admission(
    elapsed: Duration,
    policy: &str,
    binding: &str,
    error_type: MutationErrorType,
) {
    record(elapsed, policy, binding, error_type);
}

/// `ObserveRejection` (:106-110).
pub fn observe_rejection(
    elapsed: Duration,
    policy: &str,
    binding: &str,
    error_type: MutationErrorType,
) {
    record(elapsed, policy, binding, error_type);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(policy: &str, binding: &str, et: MutationErrorType) -> (u64, u64, f64) {
        let l = [policy, binding, et.as_str()];
        let h = POLICY_LATENCY.with_label_values(&l);
        (
            POLICY_CHECK.with_label_values(&l).get(),
            h.get_sample_count(),
            h.get_sample_sum(),
        )
    }

    /// `TestNoUtils` "observe policy admission".
    #[test]
    fn observe_admission_counts_and_times() {
        observe_admission(
            Duration::from_secs(10),
            "adm.example.com",
            "binding.example.com",
            MutationErrorType::InvalidError,
        );
        assert_eq!(
            check(
                "adm.example.com",
                "binding.example.com",
                MutationErrorType::InvalidError
            ),
            (1, 1, 10.0)
        );
    }

    /// `TestNoUtils` "observe policy rejection".
    #[test]
    fn observe_rejection_counts_and_times() {
        observe_rejection(
            Duration::from_secs(10),
            "rej.example.com",
            "binding.example.com",
            MutationErrorType::InvalidError,
        );
        assert_eq!(
            check(
                "rej.example.com",
                "binding.example.com",
                MutationErrorType::InvalidError
            ),
            (1, 1, 10.0)
        );
    }

    /// Names, `[ALPHA]` help and buckets as in the test's expected
    /// exposition (metrics_test.go:37-60).
    #[test]
    fn families_match_upstream_exposition() {
        observe_admission(
            Duration::from_secs(10),
            "fam.example.com",
            "binding.example.com",
            MutationErrorType::NoError,
        );
        let families = prometheus::gather();
        let lat = families
            .iter()
            .find(|f| f.name() == "apiserver_mutating_admission_policy_check_duration_seconds")
            .expect("latency registered");
        assert_eq!(
            lat.help(),
            "[ALPHA] Mutation admission latency for individual mutation expressions in seconds, labeled by policy and binding."
        );
        let bounds: Vec<f64> = lat.get_metric()[0]
            .get_histogram()
            .get_bucket()
            .iter()
            .map(|b| b.upper_bound())
            .collect();
        assert_eq!(bounds, vec![0.0000005, 0.001, 0.01, 0.1, 1.0]);
        let total = families
            .iter()
            .find(|f| f.name() == "apiserver_mutating_admission_policy_check_total")
            .expect("counter registered");
        assert_eq!(
            total.help(),
            "[ALPHA] Mutation admission policy check total, labeled by policy and further identified by binding."
        );
    }

    #[test]
    fn error_type_label_values() {
        assert_eq!(MutationErrorType::CompileError.as_str(), "compile_error");
        assert_eq!(MutationErrorType::InvalidError.as_str(), "invalid_error");
        assert_eq!(MutationErrorType::OutOfBudget.as_str(), "out_of_budget");
        assert_eq!(MutationErrorType::NoError.as_str(), "no_error");
    }
}
