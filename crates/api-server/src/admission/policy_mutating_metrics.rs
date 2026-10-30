//! `apiserver_mutating_admission_policy_*` metrics.
//!
//! Port of `staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/
//! mutating/metrics/metrics.go` (release-1.35): names, help, labels and
//! buckets (:27-92), `ObserveAdmission` (:100-104) and `ObserveRejection`
//! (:106-110), and its test `metrics_test.go` `TestNoUtils`. Registered in the
//! process-global registry like upstream's `legacyregistry`.

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

pub static POLICY_CHECK: LazyLock<IntCounterVec> = LazyLock::new(|| {
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

pub static POLICY_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
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
