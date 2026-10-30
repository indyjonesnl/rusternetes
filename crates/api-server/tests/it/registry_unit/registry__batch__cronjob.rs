//! Unit tests moved from `src/registry/batch/cronjob.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::batch::job::warnings_for_job_spec;
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::equality::semantic_equal;
    pub use rusternetes_common::resources::{CronJob, CronJobStatus};
    pub use rusternetes_common::validation::cronjob::{
        validate_cron_job_create, validate_cron_job_update,
    };
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::metav1::is_dns1123_label;
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::batch::cronjob::*;

    fn cron_job() -> CronJob {
        let mut cj: CronJob = serde_json::from_value(serde_json::json!({
            "apiVersion": "batch/v1", "kind": "CronJob",
            "metadata": {"name": "c", "namespace": "default", "generation": 4,
                         "resourceVersion": "1"},
            "spec": {
                "schedule": "*/5 * * * *",
                "jobTemplate": {"spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "i"}],
                    "restartPolicy": "OnFailure"
                }}}}
            },
            "status": {"lastScheduleTime": "2026-01-01T00:00:00Z"}
        }))
        .unwrap();
        convert_to_internal(&mut cj);
        cj
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
            Some(GarbageCollectionPolicy::DeleteDependents)
        );
        assert!(!StatusStrategy.allow_create_on_update());
        assert!(StatusStrategy.allow_unconditional_update());
    }

    #[test]
    fn prepare_for_create_resets_status_and_generation() {
        let mut cj = cron_job();
        Strategy.prepare_for_create(&ctx(), &mut cj);
        assert_eq!(cj.status, Some(CronJobStatus::default()));
        assert_eq!(cj.metadata.generation, Some(1));
        let errs = Strategy.validate(&ctx(), &cj);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `TestCronJobStrategy` (strategy_test.go): status is kept and only a
    /// spec change bumps the generation.
    #[test]
    fn prepare_for_update_keeps_status_and_bumps_generation_on_spec() {
        let old = cron_job();
        let mut touched = old.clone();
        touched.status = None;
        Strategy.prepare_for_update(&ctx(), &mut touched, &old);
        assert_eq!(touched.status, old.status);
        assert_eq!(touched.metadata.generation, Some(4));

        let mut hourly = old.clone();
        hourly.spec.schedule = "0 * * * *".into();
        Strategy.prepare_for_update(&ctx(), &mut hourly, &old);
        assert_eq!(hourly.metadata.generation, Some(5));
        let errs = Strategy.validate_update(&ctx(), &hourly, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `TestCronJobStrategy_WarningsOnUpdate` (strategy_test.go:284): a TZ in the
    /// schedule warns on every update, not just spec changes.
    #[test]
    fn an_inline_tz_warns_on_update() {
        let mut old = cron_job();
        old.spec.schedule = "CRON_TZ=UTC */5 * * * *".into();
        let new = old.clone();
        let warnings = Strategy.warnings_on_update(&ctx(), &new, &old);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].starts_with("cannot use TZ or CRON_TZ in spec.spec.schedule"));
    }

    /// SetDefaults_CronJob (pkg/apis/batch/v1/defaults.go:83-88) defaults the
    /// history limits to 3 / 1 so the controller can treat nil as "unset"
    /// (cronjob_controllerv2.go:684-686); explicit values are kept.
    #[test]
    fn history_limits_are_defaulted_at_the_api() {
        let mut cj = cron_job();
        cj.spec.successful_jobs_history_limit = None;
        cj.spec.failed_jobs_history_limit = None;
        convert_to_internal(&mut cj);
        assert_eq!(cj.spec.successful_jobs_history_limit, Some(3));
        assert_eq!(cj.spec.failed_jobs_history_limit, Some(1));

        cj.spec.successful_jobs_history_limit = Some(0);
        cj.spec.failed_jobs_history_limit = Some(7);
        convert_to_internal(&mut cj);
        assert_eq!(cj.spec.successful_jobs_history_limit, Some(0));
        assert_eq!(cj.spec.failed_jobs_history_limit, Some(7));
    }

    #[test]
    fn status_prepare_for_update_keeps_spec() {
        let old = cron_job();
        let mut new = old.clone();
        new.spec.schedule = "0 * * * *".into();
        new.status = Some(CronJobStatus::default());
        StatusStrategy.prepare_for_update(&ctx(), &mut new, &old);
        assert_eq!(new.spec.schedule, old.spec.schedule);
        assert_eq!(new.status, Some(CronJobStatus::default()));
    }
}
