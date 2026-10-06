//! One-for-one port of upstream `TestStatusStrategy_ValidateUpdate`
//! (`pkg/registry/batch/job/strategy_test.go:2194-3897`). Like upstream's
//! `cmp.Diff(tc.wantErrs, errs, ignoreErrValueDetail)` each case asserts the
//! ordered list of (error type, field); the detail text is ignored. Case names
//! are upstream's verbatim. `enableJobSuccessPolicy` has no effect here (the
//! gate is not consulted by `getStatusValidationOptions`).

use crate::resources::workloads::{
    Job, JobCondition, JobSpec, JobStatus, SuccessPolicy, SuccessPolicyRule,
    UncountedTerminatedPods,
};
use crate::validation::field::ErrorType::{self, Invalid, Required};
use crate::validation::job::{
    get_status_validation_options, get_status_validation_options_with_gates,
    validate_job_update_status, JobStatusValidationGates,
};
use chrono::{DateTime, TimeZone, Utc};

/// `now` / `nowPlusMinute` (strategy_test.go:2224-2225), whole seconds.
fn now() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}
fn now_plus_minute() -> DateTime<Utc> {
    now() + chrono::Duration::minutes(1)
}

fn cs(list: &[(&str, &str)]) -> Option<Vec<JobCondition>> {
    Some(
        list.iter()
            .map(|(t, s)| JobCondition {
                condition_type: t.to_string(),
                status: s.to_string(),
                last_probe_time: None,
                last_transition_time: None,
                reason: None,
                message: None,
            })
            .collect(),
    )
}

fn mk(spec: impl FnOnce(&mut JobSpec), st: impl FnOnce(&mut JobStatus)) -> Job {
    let mut s = JobSpec::default();
    spec(&mut s);
    let mut j = Job::new("myjob", "default", s);
    // validObjectMeta (strategy_test.go:2216-2220): a fixed resourceVersion;
    // uid / creationTimestamp pinned so old and new agree.
    j.metadata.resource_version = Some("10".to_string());
    j.metadata.uid = "uid-1".to_string();
    j.metadata.creation_timestamp = Some(now());
    let mut status = JobStatus::default();
    st(&mut status);
    j.status = Some(status);
    j
}

fn indexed(s: &mut JobSpec) {
    s.completion_mode = Some("Indexed".to_string());
}

fn success_policy() -> Option<SuccessPolicy> {
    Some(SuccessPolicy {
        rules: vec![SuccessPolicyRule {
            succeeded_indexes: Some("0-2".to_string()),
            succeeded_count: None,
        }],
    })
}

/// Indexed Job, completions=10 with a SuccessPolicy (the SuccessPolicy block).
fn sp_spec(s: &mut JobSpec) {
    indexed(s);
    s.completions = Some(10);
    s.success_policy = success_policy();
}

fn uncounted(succeeded: &[&str], failed: &[&str]) -> Option<UncountedTerminatedPods> {
    let v = |x: &[&str]| {
        if x.is_empty() {
            None
        } else {
            Some(x.iter().map(|s| s.to_string()).collect())
        }
    };
    Some(UncountedTerminatedPods {
        succeeded: v(succeeded),
        failed: v(failed),
    })
}

struct Case {
    name: &'static str,
    gates: JobStatusValidationGates,
    old: Job,
    new: Job,
    want: Vec<(ErrorType, &'static str)>,
}

const ON: JobStatusValidationGates = JobStatusValidationGates {
    job_managed_by: true,
    job_pod_replacement_policy: true,
};
const MANAGED_BY_OFF: JobStatusValidationGates = JobStatusValidationGates {
    job_managed_by: false,
    job_pod_replacement_policy: false,
};
const ONLY_PRP: JobStatusValidationGates = JobStatusValidationGates {
    job_managed_by: false,
    job_pod_replacement_policy: true,
};

fn case(
    name: &'static str,
    gates: JobStatusValidationGates,
    old: Job,
    new: Job,
    want: Vec<(ErrorType, &'static str)>,
) -> Case {
    Case {
        name,
        gates,
        old,
        new,
        want,
    }
}

fn cases() -> Vec<Case> {
    let none = |_: &mut JobSpec| {};
    let nostat = |_: &mut JobStatus| {};
    let nonindexed5 = |s: &mut JobSpec| {
        s.completions = Some(5);
        s.completion_mode = Some("NonIndexed".to_string());
    };
    let indexed5 = |s: &mut JobSpec| {
        s.completions = Some(5);
        indexed(s);
    };
    let indexed5_bl = |s: &mut JobSpec| {
        s.completions = Some(5);
        indexed(s);
        s.backoff_limit_per_index = Some(1);
    };
    let c = "status.conditions";
    vec![
        case(
            "invalid addition of both Failed=True and Complete=True; allowed because feature gate disabled",
            MANAGED_BY_OFF,
            mk(none, nostat),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True"), ("Failed", "True")]);
            }),
            vec![],
        ),
        case(
            "invalid addition of both Failed=True and Complete=True",
            ON,
            mk(none, nostat),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True"), ("Failed", "True")]);
            }),
            vec![(Invalid, c), (Invalid, c), (Invalid, c)],
        ),
        case(
            "invalid addition of Complete=True without SuccessCriteriaMet=True",
            ON,
            mk(none, nostat),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True")]);
            }),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of Failed=True without FailureTarget=True",
            ON,
            mk(none, nostat),
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("Failed", "True")]);
            }),
            vec![(Invalid, c)],
        ),
        case(
            "completionTime can be removed to fix still running job",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
            }),
            mk(none, |s| s.start_time = Some(now())),
            vec![],
        ),
        case(
            "invalid attempt to transition to Failed=True without startTime",
            ON,
            mk(none, |s| s.conditions = cs(&[("FailureTarget", "True")])),
            mk(none, |s| {
                s.conditions = cs(&[("FailureTarget", "True"), ("Failed", "True")])
            }),
            vec![(Required, "status.startTime")],
        ),
        case(
            "invalid attempt to transition to Complete=True without startTime",
            ON,
            mk(none, |s| s.conditions = cs(&[("SuccessCriteriaMet", "True")])),
            mk(none, |s| {
                s.completion_time = Some(now());
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")]);
            }),
            vec![(Required, "status.startTime")],
        ),
        case(
            "invalid attempt to transition to Complete=True with active > 0",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.active = Some(1);
                s.conditions = cs(&[("SuccessCriteriaMet", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.active = Some(1);
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")]);
            }),
            vec![(Invalid, "status.active")],
        ),
        case(
            "invalid attempt to transition to Failed=True with terminating > 0",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("FailureTarget", "True")]);
                s.terminating = Some(1);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("FailureTarget", "True"), ("Failed", "True")]);
                s.terminating = Some(1);
            }),
            vec![(Invalid, "status.terminating")],
        ),
        case(
            "invalid attempt to transition to Failed=True with active > 0",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("FailureTarget", "True")]);
                s.active = Some(1);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("FailureTarget", "True"), ("Failed", "True")]);
                s.active = Some(1);
            }),
            vec![(Invalid, "status.active")],
        ),
        case(
            "invalid attempt to transition to Failed=True with uncountedTerminatedPods.Failed>0",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&[], &["a"]);
                s.conditions = cs(&[("FailureTarget", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&[], &["a"]);
                s.conditions = cs(&[("FailureTarget", "True"), ("Failed", "True")]);
            }),
            vec![(Invalid, "status.uncountedTerminatedPods")],
        ),
        case(
            "invalid attempt to update uncountedTerminatedPods.Succeeded for Complete job",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&[], &["a"]);
                s.conditions = cs(&[("Complete", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&[], &["b"]);
                s.conditions = cs(&[("Complete", "True")]);
            }),
            vec![(Invalid, "status.uncountedTerminatedPods")],
        ),
        case(
            "non-empty uncountedTerminatedPods for complete job, unrelated update",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&[], &["a"]);
                s.conditions = cs(&[("Complete", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&[], &["a"]);
                s.conditions = cs(&[("Complete", "True"), ("CustomJobCondition", "True")]);
            }),
            vec![],
        ),
        case(
            "invalid attempt to transition to Complete=True with uncountedTerminatedPods.Succeeded>0",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&["a"], &[]);
                s.conditions = cs(&[("SuccessCriteriaMet", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.uncounted_terminated_pods = uncounted(&["a"], &[]);
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")]);
            }),
            vec![(Invalid, "status.uncountedTerminatedPods")],
        ),
        case(
            "invalid addition Complete=True without setting CompletionTime",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("SuccessCriteriaMet", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")]);
            }),
            vec![(Required, "status.completionTime")],
        ),
        case(
            "invalid attempt to remove completionTime",
            ON,
            mk(none, |s| {
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("Complete", "True")]);
            }),
            vec![(Required, "status.completionTime")],
        ),
        case(
            "verify startTime can be cleared for suspended job",
            ON,
            mk(|s| s.suspend = Some(true), |s| s.start_time = Some(now())),
            mk(|s| s.suspend = Some(true), nostat),
            vec![],
        ),
        case(
            "verify startTime cannot be removed for unsuspended job",
            ON,
            mk(none, |s| s.start_time = Some(now())),
            mk(none, nostat),
            vec![(Required, "status.startTime")],
        ),
        case(
            "verify startTime cannot be updated for unsuspended job",
            ON,
            mk(none, |s| s.start_time = Some(now())),
            mk(none, |s| s.start_time = Some(now_plus_minute())),
            vec![(Required, "status.startTime")],
        ),
        case(
            "verify startTime can be updated when resuming job (JobSuspended: True -> False)",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.conditions = cs(&[("Suspended", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now_plus_minute());
                s.conditions = cs(&[("Suspended", "False")]);
            }),
            vec![],
        ),
        case(
            "invalid attempt to set completionTime before startTime",
            ON,
            mk(none, |s| {
                s.start_time = Some(now_plus_minute());
                s.conditions = cs(&[("SuccessCriteriaMet", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now_plus_minute());
                s.completion_time = Some(now());
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")]);
            }),
            vec![(Invalid, "status.completionTime")],
        ),
        case(
            "invalid attempt to modify completionTime",
            ON,
            mk(none, |s| {
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True")]);
            }),
            mk(none, |s| {
                s.completion_time = Some(now_plus_minute());
                s.start_time = Some(now());
                s.conditions = cs(&[("Complete", "True")]);
            }),
            vec![(Invalid, "status.completionTime")],
        ),
        case(
            "invalid removal of terminal condition Failed=True",
            ON,
            mk(none, |s| s.conditions = cs(&[("Failed", "True")])),
            mk(none, nostat),
            vec![(Invalid, c)],
        ),
        case(
            "invalid removal of terminal condition Complete=True",
            ON,
            mk(none, |s| s.conditions = cs(&[("Complete", "True")])),
            mk(none, nostat),
            vec![(Invalid, c)],
        ),
        case(
            "invalid removal of terminal condition FailureTarget=True",
            ON,
            mk(none, |s| s.conditions = cs(&[("FailureTarget", "True")])),
            mk(none, nostat),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of FailureTarget=True when Complete=True",
            ON,
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True")]);
            }),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True"), ("FailureTarget", "True")]);
            }),
            vec![(Invalid, c)],
        ),
        case(
            "invalid attempt setting of CompletionTime when there is no Complete condition",
            ON,
            mk(none, nostat),
            mk(none, |s| s.completion_time = Some(now())),
            vec![(Invalid, "status.completionTime")],
        ),
        case(
            "invalid CompletionTime when there is no Complete condition, but allowed",
            ON,
            mk(none, |s| s.completion_time = Some(now())),
            mk(none, |s| {
                s.completion_time = Some(now());
                s.active = Some(1);
            }),
            vec![],
        ),
        case(
            "invalid attempt setting CompletedIndexes when non-indexed completion mode is used",
            ON,
            mk(nonindexed5, nostat),
            mk(nonindexed5, |s| {
                s.start_time = Some(now());
                s.completed_indexes = Some("0".to_string());
            }),
            vec![(Invalid, "status.completedIndexes")],
        ),
        case(
            "invalid because CompletedIndexes set when non-indexed completion mode is used; but allowed",
            ON,
            mk(nonindexed5, |s| s.completed_indexes = Some("0".to_string())),
            mk(nonindexed5, |s| {
                s.completed_indexes = Some("0".to_string());
                s.active = Some(1);
            }),
            vec![],
        ),
        case(
            "invalid attempt setting FailedIndexes when not backoffLimitPerIndex",
            ON,
            mk(indexed5, nostat),
            mk(indexed5, |s| s.failed_indexes = Some("0".to_string())),
            vec![(Invalid, "status.failedIndexes")],
        ),
        case(
            "invalid attempt to decrease the failed counter",
            ON,
            mk(|s| s.completions = Some(5), |s| s.failed = Some(3)),
            mk(|s| s.completions = Some(5), |s| s.failed = Some(1)),
            vec![(Invalid, "status.failed")],
        ),
        case(
            "invalid attempt to decrease the succeeded counter",
            ON,
            mk(|s| s.completions = Some(5), |s| s.succeeded = Some(3)),
            mk(|s| s.completions = Some(5), |s| s.succeeded = Some(1)),
            vec![(Invalid, "status.succeeded")],
        ),
        case(
            "invalid attempt to set bad format for CompletedIndexes",
            ON,
            mk(indexed5, nostat),
            mk(indexed5, |s| {
                s.completed_indexes = Some("invalid format".to_string())
            }),
            vec![(Invalid, "status.completedIndexes")],
        ),
        case(
            "invalid format for CompletedIndexes, but allowed",
            ON,
            mk(indexed5, |s| {
                s.completed_indexes = Some("invalid format".to_string())
            }),
            mk(indexed5, |s| {
                s.completed_indexes = Some("invalid format".to_string());
                s.active = Some(1);
            }),
            vec![],
        ),
        case(
            "invalid attempt to set bad format for FailedIndexes",
            ON,
            mk(indexed5_bl, nostat),
            mk(indexed5_bl, |s| {
                s.failed_indexes = Some("invalid format".to_string())
            }),
            vec![(Invalid, "status.failedIndexes")],
        ),
        case(
            "invalid format for FailedIndexes, but allowed",
            ON,
            mk(indexed5_bl, |s| {
                s.failed_indexes = Some("invalid format".to_string())
            }),
            mk(indexed5_bl, |s| {
                s.failed_indexes = Some("invalid format".to_string());
                s.active = Some(1);
            }),
            vec![],
        ),
        case(
            "more ready pods than active, but allowed",
            ON,
            mk(
                |s| s.completions = Some(5),
                |s| {
                    s.active = Some(1);
                    s.ready = Some(2);
                },
            ),
            mk(
                |s| s.completions = Some(5),
                |s| {
                    s.active = Some(1);
                    s.ready = Some(2);
                    s.succeeded = Some(1);
                },
            ),
            vec![],
        ),
        case(
            "invalid addition of both FailureTarget=True and Complete=True",
            ON,
            mk(none, nostat),
            mk(none, |s| {
                s.start_time = Some(now());
                s.completion_time = Some(now());
                s.conditions = cs(&[("Complete", "True"), ("FailureTarget", "True")]);
            }),
            vec![(Invalid, c), (Invalid, c)],
        ),
        case(
            "invalid failedIndexes, which overlap with completedIndexes",
            ON,
            mk(indexed5, |s| {
                s.failed_indexes = Some("0,2".to_string());
                s.completed_indexes = Some("3-4".to_string());
            }),
            mk(indexed5, |s| {
                s.failed_indexes = Some("0,2".to_string());
                s.completed_indexes = Some("2-4".to_string());
            }),
            vec![(Invalid, "status.failedIndexes")],
        ),
        case(
            "failedIndexes overlap with completedIndexes, unrelated field change",
            ON,
            mk(indexed5, |s| {
                s.failed_indexes = Some("0,2".to_string());
                s.completed_indexes = Some("2-4".to_string());
            }),
            mk(indexed5, |s| {
                s.failed_indexes = Some("0,2".to_string());
                s.completed_indexes = Some("2-4".to_string());
                s.active = Some(1);
            }),
            vec![],
        ),
        case(
            "invalid addition of SuccessCriteriaMet for NonIndexed Job",
            MANAGED_BY_OFF,
            mk(|s| s.success_policy = success_policy(), nostat),
            mk(
                |s| s.success_policy = success_policy(),
                |s| s.conditions = cs(&[("SuccessCriteriaMet", "True")]),
            ),
            vec![(Invalid, c)],
        ),
        case(
            "valid update of Job if SuccessCriteriaMet already present for NonIndexed Jobs; JobSuccessPolicy enabled, while JobManagedBy and JobPodReplacementPolicy disabled",
            MANAGED_BY_OFF,
            mk(none, |s| s.conditions = cs(&[("SuccessCriteriaMet", "True")])),
            mk(none, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")])
            }),
            vec![],
        ),
        case(
            "invalid addition of SuccessCriteriaMet for Job with Failed",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| s.conditions = cs(&[("Failed", "True")])),
            mk(sp_spec, |s| {
                s.conditions = cs(&[("Failed", "True"), ("SuccessCriteriaMet", "True")])
            }),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of Failed for Job with SuccessCriteriaMet",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True")])
            }),
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Failed", "True")])
            }),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of SuccessCriteriaMet for Job with FailureTarget",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| s.conditions = cs(&[("FailureTarget", "True")])),
            mk(sp_spec, |s| {
                s.conditions = cs(&[("FailureTarget", "True"), ("SuccessCriteriaMet", "True")])
            }),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of FailureTarget for Job with SuccessCriteriaMet",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True")])
            }),
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("FailureTarget", "True")])
            }),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of SuccessCriteriaMet for Job with Complete",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| s.conditions = cs(&[("Complete", "True")])),
            mk(sp_spec, |s| {
                s.conditions = cs(&[("Complete", "True"), ("SuccessCriteriaMet", "True")])
            }),
            vec![(Invalid, c)],
        ),
        case(
            "valid addition of Complete for Job with SuccessCriteriaMet",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True")])
            }),
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")])
            }),
            vec![],
        ),
        case(
            "invalid addition of SuccessCriteriaMet for Job without SuccessPolicy",
            MANAGED_BY_OFF,
            mk(
                |s| {
                    indexed(s);
                    s.completions = Some(10);
                },
                nostat,
            ),
            mk(
                |s| {
                    indexed(s);
                    s.completions = Some(10);
                },
                |s| s.conditions = cs(&[("SuccessCriteriaMet", "True")]),
            ),
            vec![(Invalid, c)],
        ),
        case(
            "invalid addition of Complete for Job with SuccessPolicy unless SuccessCriteriaMet",
            MANAGED_BY_OFF,
            mk(sp_spec, nostat),
            mk(sp_spec, |s| s.conditions = cs(&[("Complete", "True")])),
            vec![(Invalid, c)],
        ),
        case(
            "invalid disabling of SuccessCriteriaMet for Job",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True")])
            }),
            mk(sp_spec, |s| s.conditions = cs(&[("Complete", "False")])),
            vec![(Invalid, c)],
        ),
        case(
            "invalid removing of SuccessCriteriaMet for Job",
            MANAGED_BY_OFF,
            mk(sp_spec, |s| {
                s.conditions = cs(&[("SuccessCriteriaMet", "True")])
            }),
            mk(sp_spec, nostat),
            vec![(Invalid, c)],
        ),
        case(
            "valid addition of SuccessCriteriaMet when JobManagedBy is enabled",
            ON,
            mk(none, nostat),
            mk(none, |s| s.conditions = cs(&[("SuccessCriteriaMet", "True")])),
            vec![],
        ),
        case(
            "valid addition of SuccessCriteriaMet when JobPodReplacementPolicy is enabled",
            ONLY_PRP,
            mk(none, nostat),
            mk(none, |s| s.conditions = cs(&[("SuccessCriteriaMet", "True")])),
            vec![],
        ),
        case(
            "invalid attempt to set more ready pods than active",
            ON,
            mk(|s| s.completions = Some(5), nostat),
            mk(
                |s| s.completions = Some(5),
                |s| {
                    s.active = Some(1);
                    s.ready = Some(2);
                },
            ),
            vec![(Invalid, "status.ready")],
        ),
        case(
            "valid transition to Complete for suspended Job with completions=0; without startTime",
            ON,
            mk(
                |s| {
                    s.completions = Some(0);
                    s.suspend = Some(true);
                },
                nostat,
            ),
            mk(
                |s| {
                    s.completions = Some(0);
                    s.suspend = Some(true);
                },
                |s| {
                    s.completion_time = Some(now());
                    s.conditions = cs(&[("SuccessCriteriaMet", "True"), ("Complete", "True")]);
                },
            ),
            vec![],
        ),
    ]
}

#[test]
fn status_strategy_validate_update() {
    let mut failures = Vec::new();
    for tc in cases() {
        let opts = get_status_validation_options_with_gates(&tc.new, &tc.old, tc.gates);
        let got: Vec<(ErrorType, String)> = validate_job_update_status(&tc.new, &tc.old, &opts)
            .into_iter()
            .map(|e| (e.error_type, e.field))
            .collect();
        let want: Vec<(ErrorType, String)> =
            tc.want.iter().map(|(t, f)| (*t, f.to_string())).collect();
        if got != want {
            failures.push(format!("{}: want {want:?}, got {got:?}", tc.name));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Upstream's table has 58 entries; the first ("incoming resource version on
/// update should not be mutated") is a metadata/PrepareForUpdate concern with
/// no validation errors, covered by the api-server registry tests. The other 57
/// are ported above.
#[test]
fn table_is_complete() {
    assert_eq!(cases().len(), 57);
}

/// `metav1.Time` is serialized at second granularity, so a sub-second
/// difference is not a change: `getStatusValidationOptions` must not flip
/// `RejectStartTimeUpdateForUnsuspendedJob` for it (#2207 item 3).
#[test]
fn sub_second_time_difference_is_not_a_change() {
    let base = now();
    let old = mk(|_| {}, |s| s.start_time = Some(base));
    let new = mk(
        |_| {},
        |s| s.start_time = Some(base + chrono::Duration::milliseconds(900)),
    );
    let opts = get_status_validation_options(&new, &old);
    assert!(!opts.reject_start_time_update_for_unsuspended_job);
    assert!(validate_job_update_status(&new, &old, &opts).is_empty());
    let newer = mk(
        |_| {},
        |s| s.start_time = Some(base + chrono::Duration::seconds(1)),
    );
    assert!(
        get_status_validation_options(&newer, &old).reject_start_time_update_for_unsuspended_job
    );
}
