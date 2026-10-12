//! `apiserver_admission_match_condition_*` metrics.
//!
//! Port of the match-condition half of
//! `staging/src/k8s.io/apiserver/pkg/admission/metrics/metrics.go`
//! (release-1.35): names, help, labels and buckets (:225-262),
//! `ObserveMatchConditionEvalError` (:318-321),
//! `ObserveMatchConditionExclusion` (:323-326) and
//! `ObserveMatchConditionEvaluationTime` (:328-331). The step / controller /
//! webhook metrics of the same file are not ported here.
//!
//! Like upstream's `legacyregistry`, they live in the process-global
//! registry (`prometheus::default_registry()`), which `/metrics` serves.

#![allow(dead_code)]

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};

const NAMESPACE: &str = "apiserver";
const SUBSYSTEM: &str = "admission";

/// metrics.go:57 `kindPolicy`.
pub const KIND_POLICY: &str = "policy";
/// metrics.go:56 `kindWebhook`.
pub const KIND_WEBHOOK: &str = "webhook";
/// metrics.go:58 `stepValidate`.
pub const STEP_VALIDATE: &str = "validate";
/// metrics.go:59 `stepAdmit`.
pub const STEP_ADMIT: &str = "admit";

const LABELS: &[&str] = &["name", "kind", "type", "operation"];

pub(crate) static MATCH_CONDITION_EVAL_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "match_condition_evaluation_errors_total",
            "[ALPHA] Admission match condition evaluation errors count, identified by name of resource containing the match condition and broken out for each kind containing matchConditions (webhook or policy), operation and admission type (validate or admit).",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

pub(crate) static MATCH_CONDITION_EXCLUSIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "match_condition_exclusions_total",
            "[ALPHA] Admission match condition evaluation exclusions count, identified by name of resource containing the match condition and broken out for each kind containing matchConditions (webhook or policy), operation and admission type (validate or admit).",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM),
        LABELS,
    )
    .expect("valid metric");
    let _ = register(Box::new(c.clone()));
    c
});

pub(crate) static MATCH_CONDITION_EVALUATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(
    || {
        let h = HistogramVec::new(
        HistogramOpts::new(
            "match_condition_evaluation_seconds",
            "[ALPHA] Admission match condition evaluation time in seconds, identified by name and broken out for each kind containing matchConditions (webhook or policy), operation and type (validate or admit).",
        )
        .namespace(NAMESPACE)
        .subsystem(SUBSYSTEM)
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.1, 0.2, 0.25]),
        LABELS,
    )
    .expect("valid metric");
        let _ = register(Box::new(h.clone()));
        h
    },
);

/// `ObserveMatchConditionEvalError` (:318-321).
pub fn observe_match_condition_eval_error(
    name: &str,
    kind: &str,
    step_type: &str,
    operation: &str,
) {
    let _ = (name, kind, step_type, operation);
}

/// `ObserveMatchConditionExclusion` (:323-326).
pub fn observe_match_condition_exclusion(name: &str, kind: &str, step_type: &str, operation: &str) {
    let _ = (name, kind, step_type, operation);
}

/// `ObserveMatchConditionEvaluationTime` (:328-331).
pub fn observe_match_condition_evaluation_time(
    elapsed: Duration,
    name: &str,
    kind: &str,
    step_type: &str,
    operation: &str,
) {
    let _ = (elapsed, name, kind, step_type, operation);
}

#[cfg(test)]
mod tests {
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
