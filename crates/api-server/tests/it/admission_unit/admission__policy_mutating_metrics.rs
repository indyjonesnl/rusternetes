//! Unit tests moved from `src/admission/policy_mutating_metrics.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{register, HistogramOpts, HistogramVec, IntCounterVec, Opts};
    pub use std::sync::LazyLock;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::admission::policy_mutating_metrics::*;

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
