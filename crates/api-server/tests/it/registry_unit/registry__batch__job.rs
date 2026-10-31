//! Unit tests moved from `src/registry/batch/job.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::registry::generic::{
        CreateOptions, Deleted, Store, UpdateOptions,
    };
    pub use rusternetes_api_server::registry::rest::{
        DeletedCollection, GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy,
        RequestContext, RestCreateStrategy, RestDeleteStrategy, RestStorage, RestUpdateStrategy,
        UpdatedObjectInfo, ValidateObject, ValidateObjectUpdate,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::deletion::DeleteOptions;
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::resources::{Job, JobSpec, JobStatus};
    pub use rusternetes_common::types::LabelSelector;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::job::{
        get_status_validation_options, validate_job, validate_job_update,
        validate_job_update_status, JobValidationOptions,
    };
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_common::Result;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::batch::job::*;
    use rusternetes_common::resources::JobStatus;

    fn job() -> Job {
        let mut job: Job = serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": {"name": "j", "namespace": "default", "uid": "u1",
                         "generation": 4, "resourceVersion": "1"},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"name": "c", "image": "i"}],
                        "restartPolicy": "Never"
                    }
                }
            },
            "status": {"failed": 2}
        }))
        .unwrap();
        convert_to_internal(&mut job);
        generate_selector_if_needed(&mut job);
        job
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("default"))
    }

    #[test]
    fn strategy_flags_match_upstream() {
        assert!(Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        assert_eq!(
            Strategy.default_garbage_collection_policy(&ctx()),
            Some(GarbageCollectionPolicy::OrphanDependents)
        );
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    /// `TestJobStrategy_PrepareForCreate` (strategy_test.go): a manual
    /// selector is left alone.
    #[test]
    fn a_manual_selector_is_not_generated() {
        let mut job = job();
        job.spec.selector = None;
        job.spec.manual_selector = Some(true);
        job.spec.template.metadata = None;
        generate_selector_if_needed(&mut job);
        assert!(job.spec.selector.is_none());
        assert!(job.spec.template.metadata.is_none());
    }

    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut job = job();
        Strategy.prepare_for_create(&ctx(), &mut job);
        assert_eq!(job.status, Some(JobStatus::default()));
        assert_eq!(job.metadata.generation, Some(1));
        let labels = job.spec.template.metadata.unwrap().labels.unwrap();
        assert_eq!(labels[CONTROLLER_UID_LABEL], "u1");
        assert_eq!(labels[JOB_NAME_LABEL], "j");
    }

    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = job();
        let mut touched = old.clone();
        touched.status = None;
        Strategy.prepare_for_update(&ctx(), &mut touched, &old);
        assert_eq!(touched.status, old.status);
        assert_eq!(touched.metadata.generation, Some(4));

        let mut wider = old.clone();
        wider.spec.parallelism = Some(3);
        Strategy.prepare_for_update(&ctx(), &mut wider, &old);
        assert_eq!(wider.metadata.generation, Some(5));
    }

    /// `validationOptionsForJob` (strategy.go:175-207).
    #[test]
    fn validation_options_follow_the_old_job() {
        assert!(validation_options_for_job(None).require_prefixed_labels);

        let mut old = job();
        old.spec.suspend = Some(true);
        old.status = Some(JobStatus::default());
        let opts = validation_options_for_job(Some(&old));
        assert!(opts.allow_mutable_scheduling_directives);
        assert!(opts.require_prefixed_labels);

        // Started once: no longer mutable.
        old.status.as_mut().unwrap().start_time = Some(chrono::Utc::now());
        assert!(!validation_options_for_job(Some(&old)).allow_mutable_scheduling_directives);

        // An old Job without the prefixed labels is not failed for lacking them.
        let labels = old
            .spec
            .template
            .metadata
            .as_mut()
            .unwrap()
            .labels
            .as_mut()
            .unwrap();
        labels.remove(JOB_NAME_LABEL);
        assert!(!validation_options_for_job(Some(&old)).require_prefixed_labels);
    }

    /// `WarningsForJobSpec` (pkg/api/job/warnings.go:35-51).
    #[test]
    fn a_huge_indexed_job_warns() {
        let mut job = job();
        job.spec.completion_mode = Some("Indexed".into());
        job.spec.completions = Some(COMPLETIONS_SOFT_LIMIT + 1);
        job.spec.parallelism = Some(PARALLELISM_SOFT_LIMIT_FOR_UNLIMITED_COMPLETIONS + 1);
        assert_eq!(warnings_for_job_spec("spec", &job.spec).len(), 1);
        job.spec.parallelism = Some(PARALLELISM_SOFT_LIMIT_FOR_UNLIMITED_COMPLETIONS);
        assert!(warnings_for_job_spec("spec", &job.spec).is_empty());
    }

    /// `WarningsOnUpdate` (strategy.go:308-316) compares plain int64
    /// generations, so an unset generation equals `Some(0)`.
    #[test]
    fn warnings_on_update_treats_absent_generation_as_zero() {
        let mut huge = job();
        huge.spec.completion_mode = Some("Indexed".into());
        huge.spec.completions = Some(COMPLETIONS_SOFT_LIMIT + 1);
        huge.spec.parallelism = Some(PARALLELISM_SOFT_LIMIT_FOR_UNLIMITED_COMPLETIONS + 1);
        let (mut new, mut old) = (huge.clone(), huge);
        new.metadata.generation = None;
        old.metadata.generation = Some(0);
        assert!(Strategy.warnings_on_update(&ctx(), &new, &old).is_empty());
        old.metadata.generation = Some(1);
        assert_eq!(Strategy.warnings_on_update(&ctx(), &new, &old).len(), 1);
    }

    #[test]
    fn status_prepare_for_update_keeps_spec() {
        let old = job();
        let mut new = old.clone();
        new.spec.parallelism = Some(9);
        new.status = Some(JobStatus {
            failed: Some(3),
            ..JobStatus::default()
        });
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.parallelism, old.spec.parallelism);
        assert_eq!(new.status.unwrap().failed, Some(3));
    }
}
