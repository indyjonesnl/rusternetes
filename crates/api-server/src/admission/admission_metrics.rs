//! `apiserver_admission_match_condition_*` metrics.
//!
//! The statics and observers live in `rusternetes_admission_webhook`
//! (`admission_metrics`), next to the step / controller / webhook metrics of
//! upstream's `admission/metrics/metrics.go`, so the webhook dispatchers and
//! the policy plugin observe the SAME process-global series. Re-exported here
//! for the policy plugin.

#![allow(unused_imports)]

pub use rusternetes_admission_webhook::admission_metrics::{
    observe_match_condition_eval_error, observe_match_condition_evaluation_time,
    observe_match_condition_exclusion, KIND_POLICY, KIND_WEBHOOK,
    MATCH_CONDITION_EVALUATION_SECONDS, MATCH_CONDITION_EVAL_ERRORS, MATCH_CONDITION_EXCLUSIONS,
    STEP_ADMIT, STEP_VALIDATE,
};
