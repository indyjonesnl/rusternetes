//! Unit tests moved from `src/validation/job.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::{Affinity, PodSpec};
    pub use rusternetes_common::resources::workloads::{
        Job, JobCondition, JobSpec, JobTemplateSpec, PodFailurePolicyRule, SuccessPolicyRule,
    };
    pub use rusternetes_common::types::LabelSelector;
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1123_label, is_dns1123_subdomain, is_qualified_name, label_selector_matches_labels,
        validate_label_selector, LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_immutable_field, validate_nonnegative_field,
        validate_object_meta, validate_object_meta_update,
    };
    pub use std::collections::HashSet;
}

mod success_policy_indexes_tests {
    use rusternetes_common::validation::job::{parse_index_interval, validate_indexes_format};

    #[test]
    fn single_and_range_intervals() {
        assert_eq!(parse_index_interval("3", 5), Ok((3, 3)));
        assert_eq!(parse_index_interval("1-3", 5), Ok((1, 3)));
    }

    #[test]
    fn index_out_of_range_rejected() {
        assert!(parse_index_interval("5", 5).is_err()); // >= completions
        assert!(parse_index_interval("2-5", 5).is_err());
    }

    #[test]
    fn non_increasing_interval_rejected() {
        assert!(parse_index_interval("3-1", 5).is_err());
    }

    #[test]
    fn total_count_for_valid_format() {
        assert_eq!(validate_indexes_format("", 5), Ok(0));
        assert_eq!(validate_indexes_format("0-2", 5), Ok(3));
        assert_eq!(validate_indexes_format("1,3-5,7", 8), Ok(5));
    }

    #[test]
    fn non_increasing_across_intervals_rejected() {
        // second interval starts at/below the previous end
        assert!(validate_indexes_format("0-3,2", 8).is_err());
        assert!(validate_indexes_format("2,2", 8).is_err());
    }

    #[test]
    fn three_part_interval_rejected() {
        assert!(parse_index_interval("1-2-3", 8).is_err());
    }
}

mod parity_tests {
    /// The create path's options: `RequirePrefixedLabels: true`.
    fn validate_job(job: &Job) -> ErrorList {
        rusternetes_common::validation::job::validate_job(
            job,
            &JobValidationOptions {
                require_prefixed_labels: true,
                ..Default::default()
            },
        )
    }
    use super::__parent::*;
    use rusternetes_common::resources::pod::{Container, PodSpec};
    use rusternetes_common::resources::workloads::{
        Job, JobSpec, PodFailurePolicy, PodFailurePolicyOnPodConditionsPattern,
        PodFailurePolicyRule, PodTemplateSpec,
    };
    use rusternetes_common::types::{
        LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta,
    };
    use rusternetes_common::validation::job::*;
    use std::collections::HashMap;

    fn labels(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn container() -> Container {
        Container {
            name: "main".to_string(),
            // `image` is required upstream (`ValidateContainers`), and the job
            // validator now reaches it through `ValidatePodTemplateSpec`.
            image: "nginx".to_string(),
            ..Default::default()
        }
    }

    /// A Job whose auto-generated selector + template labels are consistent,
    /// as the api-server's `generateSelector` would produce: prefixed + legacy
    /// controller-uid / job-name labels, selector on the prefixed controller-uid.
    fn valid_generated_job() -> Job {
        let uid = "abc-123".to_string();
        let name = "myjob".to_string();
        let template_labels = labels(&[
            ("controller-uid", &uid),
            ("job-name", &name),
            ("batch.kubernetes.io/controller-uid", &uid),
            ("batch.kubernetes.io/job-name", &name),
        ]);
        let mut meta = ObjectMeta::new(&name).with_namespace("default");
        meta.uid = uid.clone();
        Job {
            type_meta: TypeMeta {
                kind: "Job".to_string(),
                api_version: "batch/v1".to_string(),
            },
            metadata: meta,
            spec: JobSpec {
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(template_labels),
                        ..ObjectMeta::default()
                    }),
                    spec: PodSpec {
                        containers: vec![container()],
                        restart_policy: Some("Never".to_string()),
                        ..PodSpec::default()
                    },
                },
                completions: None,
                parallelism: None,
                backoff_limit: None,
                active_deadline_seconds: None,
                selector: Some(LabelSelector {
                    match_labels: Some(labels(&[("batch.kubernetes.io/controller-uid", &uid)])),
                    match_expressions: None,
                }),
                manual_selector: None,
                suspend: None,
                ttl_seconds_after_finished: None,
                completion_mode: None,
                backoff_limit_per_index: None,
                max_failed_indexes: None,
                pod_failure_policy: None,
                pod_replacement_policy: None,
                success_policy: None,
                managed_by: None,
            },
            status: None,
        }
    }

    fn has_error_containing(errs: &ErrorList, detail_needle: &str) -> bool {
        errs.iter().any(|e| e.detail.contains(detail_needle))
    }

    fn has_error_on_field(errs: &ErrorList, field_needle: &str) -> bool {
        errs.iter().any(|e| e.field.contains(field_needle))
    }

    // --- baseline -----------------------------------------------------------

    /// Cases from upstream `TestValidateJobUpdate`
    /// (pkg/apis/batch/validation/validation_test.go).
    mod update {
        use super::*;
        use rusternetes_common::resources::pod::{
            Affinity, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
        };

        fn stored() -> Job {
            let mut job = valid_generated_job();
            job.metadata.resource_version = Some("1".into());
            job
        }

        fn update_errs(new: &Job, old: &Job, opts: JobValidationOptions) -> Vec<String> {
            validate_job_update(new, old, &opts)
                .iter()
                .map(|e| e.field.to_string())
                .collect()
        }

        #[test]
        fn mutable_fields() {
            let old = stored();
            let mut new = old.clone();
            new.spec.parallelism = Some(2);
            new.spec.active_deadline_seconds = Some(3);
            new.spec.suspend = Some(true);
            new.spec.ttl_seconds_after_finished = Some(1);
            assert_eq!(
                update_errs(&new, &old, Default::default()),
                Vec::<String>::new()
            );
        }

        #[test]
        fn immutable_fields() {
            let old = stored();
            for (field, mutate) in [
                (
                    "spec.completions",
                    (|j: &mut Job| j.spec.completions = Some(3)) as fn(&mut Job),
                ),
                ("spec.completionMode", |j| {
                    j.spec.completion_mode = Some("Indexed".into());
                    j.spec.completions = Some(1);
                }),
                ("spec.template", |j| {
                    j.spec.template.spec.containers[0].image = "other".into()
                }),
                ("spec.backoffLimitPerIndex", |j| {
                    j.spec.backoff_limit_per_index = Some(1)
                }),
                ("spec.managedBy", |j| {
                    j.spec.managed_by = Some("example.com/foo".into())
                }),
            ] {
                let mut new = old.clone();
                mutate(&mut new);
                let got = update_errs(&new, &old, Default::default());
                assert!(got.iter().any(|f| f == field), "{field}: {got:?}");
            }
        }

        /// `validateCompletions`: an Indexed Job's completions may change in
        /// tandem with parallelism.
        #[test]
        fn indexed_completions_move_with_parallelism() {
            let mut old = stored();
            old.spec.completion_mode = Some("Indexed".into());
            old.spec.completions = Some(2);
            old.spec.parallelism = Some(2);
            let mut both = old.clone();
            both.spec.completions = Some(4);
            both.spec.parallelism = Some(4);
            assert_eq!(
                update_errs(&both, &old, Default::default()),
                Vec::<String>::new()
            );

            let mut alone = old.clone();
            alone.spec.completions = Some(4);
            let errs = validate_job_update(&alone, &old, &Default::default());
            assert!(
                errs.iter().any(|e| e.field == "spec.completions"
                    && e.detail == "can only be modified in tandem with spec.parallelism"),
                "{errs:?}"
            );
        }

        /// "update node affinity, node selector and tolerations for a
        /// suspended, not started job": allowed only with
        /// `AllowMutableSchedulingDirectives`, and only when suspended.
        #[test]
        fn scheduling_directives_are_mutable_only_when_allowed() {
            let mut old = stored();
            old.spec.suspend = Some(true);
            let mut new = old.clone();
            new.spec.template.spec.node_selector =
                Some(labels(&[("disk", "ssd")]).into_iter().collect());
            new.spec.template.spec.affinity = Some(Affinity {
                node_affinity: Some(NodeAffinity {
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm {
                            match_expressions: Some(vec![NodeSelectorRequirement {
                                key: "foo".into(),
                                operator: "In".into(),
                                values: Some(vec!["bar".into()]),
                            }]),
                            match_fields: None,
                        }],
                    }),
                    preferred_during_scheduling_ignored_during_execution: None,
                }),
                pod_affinity: None,
                pod_anti_affinity: None,
            });
            let allowed = JobValidationOptions {
                allow_mutable_scheduling_directives: true,
                ..Default::default()
            };
            assert_eq!(update_errs(&new, &old, allowed), Vec::<String>::new());
            assert!(update_errs(&new, &old, Default::default())
                .iter()
                .any(|f| f == "spec.template"));

            // Scheduling directives only: an image change is still refused.
            let mut image = new.clone();
            image.spec.template.spec.containers[0].image = "other".into();
            assert!(update_errs(&image, &old, allowed)
                .iter()
                .any(|f| f == "spec.template"));
        }

        #[test]
        fn metadata_update_is_validated() {
            let old = stored();
            let mut new = old.clone();
            new.metadata.namespace = Some("other".into());
            assert!(update_errs(&new, &old, Default::default())
                .iter()
                .any(|f| f == "metadata.namespace"));
        }
    }

    #[test]
    fn valid_generated_job_passes() {
        let errs = validate_job(&valid_generated_job());
        assert!(errs.is_empty(), "expected no errors, got: {errs:?}");
    }

    // --- rule 1: podFailurePolicy ⇒ restartPolicy must be Never --------------

    #[test]
    fn pod_failure_policy_requires_never_restart_policy() {
        let mut job = valid_generated_job();
        job.spec.template.spec.restart_policy = Some("OnFailure".to_string());
        job.spec.pod_failure_policy = Some(PodFailurePolicy {
            rules: vec![PodFailurePolicyRule {
                action: "Ignore".to_string(),
                on_exit_codes: None,
                on_pod_conditions: vec![PodFailurePolicyOnPodConditionsPattern {
                    condition_type: "DisruptionTarget".to_string(),
                    status: Some("True".to_string()),
                }],
            }],
        });
        let errs = validate_job(&job);
        assert!(
            has_error_containing(
                &errs,
                "only \"Never\" is supported when podFailurePolicy is specified"
            ),
            "got: {errs:?}"
        );
        assert!(has_error_on_field(&errs, "template.spec.restartPolicy"));
    }

    #[test]
    fn pod_failure_policy_with_never_restart_policy_ok() {
        let mut job = valid_generated_job();
        job.spec.template.spec.restart_policy = Some("Never".to_string());
        job.spec.pod_failure_policy = Some(PodFailurePolicy {
            rules: vec![PodFailurePolicyRule {
                action: "Ignore".to_string(),
                on_exit_codes: None,
                on_pod_conditions: vec![PodFailurePolicyOnPodConditionsPattern {
                    condition_type: "DisruptionTarget".to_string(),
                    status: Some("True".to_string()),
                }],
            }],
        });
        let errs = validate_job(&job);
        assert!(
            !has_error_containing(
                &errs,
                "only \"Never\" is supported when podFailurePolicy is specified"
            ),
            "got: {errs:?}"
        );
    }

    // --- rule 2: validateGeneratedSelector -----------------------------------

    #[test]
    fn generated_selector_missing_prefixed_labels_rejected() {
        let mut job = valid_generated_job();
        // Drop the prefixed labels, leaving only the legacy ones.
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[
            ("controller-uid", "abc-123"),
            ("job-name", "myjob"),
        ]));
        // Selector still references the prefixed key, so it won't match either.
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "must be 'abc-123'")
                || has_error_containing(&errs, "must be 'myjob'"),
            "expected ValidateHasLabel errors, got: {errs:?}"
        );
        assert!(has_error_on_field(
            &errs,
            "spec.template.metadata.labels[batch.kubernetes.io/controller-uid]"
        ));
    }

    #[test]
    fn generated_selector_wrong_uid_label_rejected() {
        let mut job = valid_generated_job();
        // controller-uid label disagrees with metadata.uid.
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[
            ("controller-uid", "wrong"),
            ("job-name", "myjob"),
            ("batch.kubernetes.io/controller-uid", "wrong"),
            ("batch.kubernetes.io/job-name", "myjob"),
        ]));
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "must be 'abc-123'"),
            "got: {errs:?}"
        );
    }

    #[test]
    fn generated_selector_mismatch_reported() {
        let mut job = valid_generated_job();
        // Selector points at a uid that no generated label carries.
        job.spec.selector = Some(LabelSelector {
            match_labels: Some(labels(&[(
                "batch.kubernetes.io/controller-uid",
                "different-uid",
            )])),
            match_expressions: None,
        });
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "`selector` not auto-generated"),
            "got: {errs:?}"
        );
    }

    #[test]
    fn generated_selector_missing_uid_rejected() {
        let mut job = valid_generated_job();
        job.metadata.uid = String::new();
        // Keep labels matching the (now empty) uid so only the uid-required
        // error is the headline; selector still references the old uid value.
        let errs = validate_job(&job);
        assert!(has_error_on_field(&errs, "metadata.uid"), "got: {errs:?}");
    }

    #[test]
    fn manual_selector_skips_generated_checks() {
        let mut job = valid_generated_job();
        job.spec.manual_selector = Some(true);
        // Only matchLabels that the template carries; no prefixed labels needed.
        job.spec.selector = Some(LabelSelector {
            match_labels: Some(labels(&[("app", "x")])),
            match_expressions: None,
        });
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[("app", "x")]));
        let errs = validate_job(&job);
        assert!(
            !has_error_containing(&errs, "`selector` not auto-generated"),
            "got: {errs:?}"
        );
        assert!(
            !has_error_containing(&errs, "must be 'abc-123'"),
            "manualSelector should skip generated-label checks, got: {errs:?}"
        );
    }

    // --- rule 3: selector match honors matchExpressions ----------------------

    #[test]
    fn selector_match_expressions_satisfied_ok() {
        let mut job = valid_generated_job();
        job.spec.manual_selector = Some(true); // isolate the match check
        job.spec.selector = Some(LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "tier".to_string(),
                operator: "In".to_string(),
                values: Some(vec!["fe".to_string(), "be".to_string()]),
            }]),
        });
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[("tier", "fe")]));
        let errs = validate_job(&job);
        assert!(
            !has_error_containing(&errs, "`selector` does not match template `labels`"),
            "matchExpressions should be honored, got: {errs:?}"
        );
    }

    #[test]
    fn selector_match_expressions_violated_rejected() {
        let mut job = valid_generated_job();
        job.spec.manual_selector = Some(true);
        job.spec.selector = Some(LabelSelector {
            match_labels: None,
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "tier".to_string(),
                operator: "In".to_string(),
                values: Some(vec!["fe".to_string()]),
            }]),
        });
        // Template label value not in the In-set ⇒ no match.
        job.spec.template.metadata.as_mut().unwrap().labels = Some(labels(&[("tier", "db")]));
        let errs = validate_job(&job);
        assert!(
            has_error_containing(&errs, "`selector` does not match template `labels`"),
            "matchExpressions mismatch must be caught, got: {errs:?}"
        );
    }
}

/// Cases from upstream `TestValidateJobUpdateStatus`
/// (`pkg/apis/batch/validation/validation_test.go`) and the
/// `getStatusValidationOptions` gating in `pkg/registry/batch/job/strategy.go`.
mod status_validation_tests {
    use super::__parent::*;
    use chrono::{DateTime, TimeZone, Utc};
    use rusternetes_common::resources::workloads::{JobStatus, UncountedTerminatedPods};
    use rusternetes_common::validation::job::*;
    use rusternetes_common::validation::objectmeta::IS_NEGATIVE_ERROR_MSG;

    fn t(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap()
    }

    fn cond(c_type: &str) -> JobCondition {
        JobCondition {
            condition_type: c_type.to_string(),
            status: "True".to_string(),
            last_probe_time: None,
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    fn job(mutate: impl FnOnce(&mut Job)) -> Job {
        let mut j = Job::new(
            "j",
            "default",
            JobSpec {
                completions: Some(5),
                parallelism: Some(2),
                ..Default::default()
            },
        );
        j.status = Some(JobStatus::default());
        mutate(&mut j);
        j
    }

    fn status(j: &mut Job) -> &mut JobStatus {
        j.status.get_or_insert_with(Default::default)
    }

    fn conds(j: &mut Job, types: &[&str]) {
        status(j).conditions = Some(types.iter().map(|c| cond(c)).collect());
    }

    fn full(new: &Job, old: &Job) -> ErrorList {
        validate_job_status_update(new, old, &get_status_validation_options(new, old))
    }

    fn errs(new: &Job, old: &Job) -> Vec<(String, String)> {
        full(new, old)
            .into_iter()
            .map(|e| (e.field, e.detail))
            .collect()
    }

    fn one(field: &str, detail: &str) -> Vec<(String, String)> {
        vec![(field.to_string(), detail.to_string())]
    }

    fn has(got: &[(String, String)], field: &str, detail: &str) -> bool {
        got.contains(&(field.to_string(), detail.to_string()))
    }

    #[test]
    fn failed_requires_failure_target() {
        let old = job(|j| status(j).start_time = Some(t(0)));
        let new = job(|j| {
            status(j).start_time = Some(t(0));
            conds(j, &["Failed"]);
        });
        assert_eq!(
            errs(&new, &old),
            one(
                "status.conditions",
                "cannot set Failed=True condition without the FailureTarget=true condition"
            )
        );
        let ok = job(|j| {
            status(j).start_time = Some(t(0));
            conds(j, &["FailureTarget", "Failed"]);
        });
        assert!(errs(&ok, &old).is_empty());
    }

    #[test]
    fn complete_requires_success_criteria_met_and_completion_time() {
        let old = job(|j| status(j).start_time = Some(t(0)));
        let new = job(|j| {
            status(j).start_time = Some(t(0));
            conds(j, &["Complete"]);
        });
        let got = errs(&new, &old);
        assert!(has(
            &got,
            "status.conditions",
            "cannot set Complete=True condition without the SuccessCriteriaMet=true condition"
        ));
        assert!(has(
            &got,
            "status.completionTime",
            "completionTime is required for Complete jobs"
        ));
        let ok = job(|j| {
            let s = status(j);
            s.start_time = Some(t(0));
            s.completion_time = Some(t(5));
            conds(j, &["SuccessCriteriaMet", "Complete"]);
        });
        assert!(errs(&ok, &old).is_empty());
    }

    #[test]
    fn complete_and_failed_conditions_conflict() {
        let old = job(|j| status(j).start_time = Some(t(0)));
        let new = job(|j| {
            let s = status(j);
            s.start_time = Some(t(0));
            s.completion_time = Some(t(1));
            conds(
                j,
                &["SuccessCriteriaMet", "FailureTarget", "Complete", "Failed"],
            );
        });
        let got = errs(&new, &old);
        for want in [
            "cannot set Complete=True and Failed=true conditions",
            "cannot set Complete=True and FailureTarget=true conditions",
            "cannot set SuccessCriteriaMet=True and Failed=true conditions",
            "cannot set SuccessCriteriaMet=True and FailureTarget=true conditions",
        ] {
            assert!(
                has(&got, "status.conditions", want),
                "missing {want}: {got:?}"
            );
        }
    }

    #[test]
    fn completion_time_rules() {
        let old = job(|j| status(j).start_time = Some(t(10)));
        let new = job(|j| {
            let s = status(j);
            s.start_time = Some(t(10));
            s.completion_time = Some(t(5));
        });
        let got = errs(&new, &old);
        assert!(has(
            &got,
            "status.completionTime",
            "cannot set completionTime when there is no Complete=True condition"
        ));
        assert!(has(
            &got,
            "status.completionTime",
            "must be equal to or after `startTime`"
        ));
        // Mutating an already-set completionTime is rejected.
        let old = job(|j| {
            let s = status(j);
            s.start_time = Some(t(0));
            s.completion_time = Some(t(5));
            conds(j, &["SuccessCriteriaMet", "Complete"]);
        });
        let mut new = old.clone();
        status(&mut new).completion_time = Some(t(6));
        assert_eq!(
            errs(&new, &old),
            one("status.completionTime", "field is immutable")
        );
    }

    #[test]
    fn finished_job_invariants() {
        let old = job(|_| {});
        let new = job(|j| {
            let s = status(j);
            s.active = Some(1);
            s.terminating = Some(2);
            s.uncounted_terminated_pods = Some(UncountedTerminatedPods {
                succeeded: Some(vec!["a".into()]),
                failed: None,
            });
            s.completion_time = Some(t(1));
            conds(j, &["SuccessCriteriaMet", "Complete"]);
        });
        let got = errs(&new, &old);
        for (f, d) in [
            ("status.active", "active>0 is invalid for finished job"),
            ("status.startTime", "startTime is required for finished job"),
            (
                "status.terminating",
                "terminating>0 is invalid for finished job",
            ),
            (
                "status.uncountedTerminatedPods",
                "must be empty for finished job",
            ),
        ] {
            assert!(has(&got, f, d), "missing {f}: {got:?}");
        }
    }

    #[test]
    fn suspended_with_zero_completions_may_finish_without_start_time() {
        let old = job(|j| {
            j.spec.suspend = Some(true);
            j.spec.completions = Some(0);
        });
        let new = job(|j| {
            j.spec.suspend = Some(true);
            j.spec.completions = Some(0);
            status(j).completion_time = Some(t(1));
            conds(j, &["SuccessCriteriaMet", "Complete"]);
        });
        assert!(errs(&new, &old).is_empty());
    }

    #[test]
    fn ready_cannot_exceed_active() {
        let old = job(|_| {});
        let new = job(|j| {
            let s = status(j);
            s.active = Some(1);
            s.ready = Some(2);
        });
        assert_eq!(
            errs(&new, &old),
            one("status.ready", "cannot set more ready pods than active")
        );
    }

    #[test]
    fn negative_counters_and_uncounted_uids() {
        let old = job(|_| {});
        let new = job(|j| {
            let s = status(j);
            s.ready = Some(-1);
            s.terminating = Some(-1);
            s.uncounted_terminated_pods = Some(UncountedTerminatedPods {
                succeeded: Some(vec!["a".into(), "".into()]),
                failed: Some(vec!["a".into()]),
            });
        });
        let got = errs(&new, &old);
        assert!(has(&got, "status.ready", IS_NEGATIVE_ERROR_MSG));
        assert!(has(&got, "status.terminating", IS_NEGATIVE_ERROR_MSG));
        assert!(has(
            &got,
            "status.uncountedTerminatedPods.succeeded[1]",
            "must not be empty"
        ));
        let dup = full(&new, &old)
            .into_iter()
            .find(|e| e.field == "status.uncountedTerminatedPods.failed[0]")
            .expect("duplicate uid across the two lists");
        assert_eq!(
            dup.error_type,
            rusternetes_common::validation::field::ErrorType::Duplicate
        );
    }

    #[test]
    fn completed_indexes_format_and_mode() {
        let old = job(|j| j.spec.completion_mode = Some("Indexed".into()));
        let new = job(|j| {
            j.spec.completion_mode = Some("Indexed".into());
            status(j).completed_indexes = Some("0-9".into());
        });
        assert_eq!(
            errs(&new, &old),
            one(
                "status.completedIndexes",
                "error parsing completedIndexes: too large index: \"9\""
            )
        );
        let old = job(|_| {});
        let new = job(|j| status(j).completed_indexes = Some("0-2".into()));
        assert_eq!(
            errs(&new, &old),
            one(
                "status.completedIndexes",
                "cannot set non-empty completedIndexes when non-indexed completion mode"
            )
        );
    }

    #[test]
    fn unchanged_violation_does_not_block_unrelated_update() {
        // strategy.go:354-367: a rule fires only when its field changed, so a
        // status already outside the bounds (e.g. after a spec scale-down) can
        // still be written by the controller.
        let old = job(|j| {
            j.spec.completion_mode = Some("Indexed".into());
            status(j).completed_indexes = Some("0-9".into());
        });
        let mut new = old.clone();
        status(&mut new).active = Some(1);
        assert!(errs(&new, &old).is_empty());
    }

    #[test]
    fn failed_indexes_rules() {
        let old = job(|j| j.spec.completion_mode = Some("Indexed".into()));
        let new = job(|j| {
            j.spec.completion_mode = Some("Indexed".into());
            status(j).failed_indexes = Some("1".into());
        });
        assert!(has(
            &errs(&new, &old),
            "status.failedIndexes",
            "cannot set non-null failedIndexes when backoffLimitPerIndex is null"
        ));
        let new = job(|j| {
            j.spec.completion_mode = Some("Indexed".into());
            j.spec.backoff_limit_per_index = Some(1);
            let s = status(j);
            s.completed_indexes = Some("0-2".into());
            s.failed_indexes = Some("2-3".into());
        });
        assert!(has(
            &errs(&new, &old),
            "status.failedIndexes",
            "failedIndexes and completedIndexes overlap at index: 2"
        ));
        let new = job(|j| {
            j.spec.completion_mode = Some("Indexed".into());
            j.spec.backoff_limit_per_index = Some(1);
            status(j).failed_indexes = Some("9".into());
        });
        assert!(has(
            &errs(&new, &old),
            "status.failedIndexes",
            "error parsing failedIndexes: too large index: \"9\""
        ));
    }

    #[test]
    fn terminal_conditions_cannot_be_disabled() {
        let old = job(|j| {
            let s = status(j);
            s.start_time = Some(t(0));
            s.completion_time = Some(t(1));
            conds(j, &["SuccessCriteriaMet", "Complete"]);
        });
        let mut new = old.clone();
        status(&mut new).conditions = Some(vec![]);
        let got = errs(&new, &old);
        assert!(has(
            &got,
            "status.conditions",
            "cannot disable the terminal Complete=True condition"
        ));
        assert!(has(
            &got,
            "status.conditions",
            "cannot disable the SuccessCriteriaMet=True condition"
        ));
    }

    #[test]
    fn success_criteria_met_not_after_complete() {
        let old = job(|j| {
            let s = status(j);
            s.start_time = Some(t(0));
            s.completion_time = Some(t(1));
            conds(j, &["Complete"]);
        });
        let mut new = old.clone();
        conds(&mut new, &["Complete", "SuccessCriteriaMet"]);
        assert!(has(
            &errs(&new, &old),
            "status.conditions",
            "cannot set SuccessCriteriaMet=True for Job already has Complete=true conditions"
        ));
    }

    #[test]
    fn success_policy_job_needs_criteria_met_to_complete() {
        let old = job(|j| {
            j.spec.success_policy = Some(Default::default());
            status(j).start_time = Some(t(0));
        });
        let new = job(|j| {
            j.spec.success_policy = Some(Default::default());
            let s = status(j);
            s.start_time = Some(t(0));
            s.completion_time = Some(t(1));
            conds(j, &["Complete"]);
        });
        assert!(has(
            &errs(&new, &old),
            "status.conditions",
            "cannot set Complete=True for Job with SuccessPolicy unless SuccessCriteriaMet=True"
        ));
    }

    #[test]
    fn start_time_cannot_be_removed_for_unsuspended_job() {
        let old = job(|j| status(j).start_time = Some(t(0)));
        let new = job(|_| {});
        assert_eq!(
            errs(&new, &old),
            one(
                "status.startTime",
                "startTime cannot be removed for unsuspended job"
            )
        );
        // JobSuspended True -> False (resume) is exempt (strategy.go:372-376).
        let old = job(|j| {
            j.spec.suspend = Some(true);
            status(j).start_time = Some(t(0));
            conds(j, &["Suspended"]);
        });
        let new = job(|j| {
            j.spec.suspend = Some(false);
            status(j).conditions = Some(vec![JobCondition {
                status: "False".into(),
                ..cond("Suspended")
            }]);
        });
        assert!(errs(&new, &old).is_empty());
    }

    #[test]
    fn counters_still_cannot_decrease() {
        let old = job(|j| {
            let s = status(j);
            s.failed = Some(2);
            s.succeeded = Some(2);
        });
        let new = job(|_| {});
        assert_eq!(
            errs(&new, &old),
            vec![
                (
                    "status.failed".to_string(),
                    "cannot decrease the failed counter".to_string()
                ),
                (
                    "status.succeeded".to_string(),
                    "cannot decrease the succeeded counter".to_string()
                ),
            ]
        );
    }

    #[test]
    fn option_gating_follows_changed_fields() {
        let old = job(|_| {});
        let opts = get_status_validation_options(&old, &old);
        assert!(opts.reject_decreasing_failed_counter);
        assert!(opts.reject_mutating_completion_time);
        assert!(opts.allow_for_success_criteria_met_in_extended_scope);
        assert!(!opts.reject_failed_job_without_failure_target);
        assert!(!opts.reject_finished_job_without_start_time);
        assert!(!opts.reject_invalid_completed_indexes);
        let mut new = old.clone();
        conds(&mut new, &["Failed"]);
        let opts = get_status_validation_options(&new, &old);
        assert!(opts.reject_failed_job_without_failure_target);
        assert!(opts.reject_finished_job_with_active_pods);
        assert!(opts.reject_finished_job_without_start_time);
    }
}
