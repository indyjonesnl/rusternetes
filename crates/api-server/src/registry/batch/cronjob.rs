//! CronJob strategies and storage — port of
//! `pkg/registry/batch/cronjob/strategy.go` and
//! `pkg/registry/batch/cronjob/storage/storage.go`.

use std::sync::Arc;

use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::{CronJob, CronJobStatus};
use rusternetes_common::validation::cronjob::{validate_cron_job_create, validate_cron_job_update};
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_storage::StorageBackend;

use super::job::warnings_for_job_spec;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// Where `WarningsForJobSpec` reports for a CronJob (strategy.go:126, :158).
const JOB_TEMPLATE_SPEC_PATH: &str = "spec.jobTemplate.spec";

/// The v1 defaulting a decoded CronJob goes through: `SetDefaults_CronJob`
/// (pkg/apis/batch/v1/defaults.go) and the job template's pod defaults.
pub fn convert_to_internal(cj: &mut CronJob) {
    crate::handlers::defaults::apply_cronjob_defaults(cj);
}

/// `cronJobStrategy` (strategy.go:42-48).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<CronJob> for Strategy {
    /// `PrepareForCreate` (strategy.go:87-93): status is cleared and the
    /// generation starts at 1. `DropDisabledTemplateFields` drops nothing we
    /// model (see the Deployment strategy).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut CronJob) {
        obj.status = Some(CronJobStatus::default());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &CronJob) -> ErrorList {
        validate_cron_job_create(obj)
    }

    /// `WarningsOnCreate` (strategy.go:119-128).
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &CronJob) -> Vec<String> {
        let mut warnings = Vec::new();
        let msgs = is_dns1123_label(&obj.metadata.name);
        if !msgs.is_empty() {
            warnings.push(format!(
                "metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: [{}]",
                msgs.join(" ")
            ));
        }
        warnings.extend(warnings_for_job_spec(
            JOB_TEMPLATE_SPEC_PATH,
            &obj.spec.job_template.spec,
        ));
        warnings
    }
}

impl RestUpdateStrategy<CronJob> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:97-109): status is kept and a spec
    /// change bumps the generation.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut CronJob, old: &CronJob) {
        obj.status = old.status.clone();
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    fn validate_update(&self, _ctx: &RequestContext, obj: &CronJob, old: &CronJob) -> ErrorList {
        validate_cron_job_update(obj, old)
    }

    /// `WarningsOnUpdate` (strategy.go:152-163). The field path in the TZ
    /// warning is upstream's, `spec.spec.schedule`, verbatim.
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &CronJob,
        old: &CronJob,
    ) -> Vec<String> {
        let mut warnings = Vec::new();
        if obj.metadata.generation != old.metadata.generation {
            warnings.extend(warnings_for_job_spec(
                JOB_TEMPLATE_SPEC_PATH,
                &obj.spec.job_template.spec,
            ));
        }
        if obj.spec.schedule.contains("TZ") {
            warnings.push("cannot use TZ or CRON_TZ in spec.spec.schedule, use timeZone instead, see https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/ for more details".to_string());
        }
        warnings
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `DefaultGarbageCollectionPolicy` (strategy.go:52-65): `DeleteDependents`
/// for batch/v1. Only batch/v1beta1, which is not served, orphans.
impl RestDeleteStrategy<CronJob> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::DeleteDependents)
    }
}

/// `cronJobStatusStrategy` (strategy.go:165-199): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<CronJob> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:185-189: only status may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut CronJob, old: &CronJob) {
        obj.spec = old.spec.clone();
    }

    /// strategy.go:191-193: nothing beyond the common metadata checks.
    fn validate_update(&self, _ctx: &RequestContext, _obj: &CronJob, _old: &CronJob) -> ErrorList {
        ErrorList::new()
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go:41-65): the CronJob store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<CronJob, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("batch", "cronjobs"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the CronJob store updating with [`StatusStrategy`]
/// (storage.go:60-62).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<CronJob, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

#[cfg(test)]
mod tests {
    use super::*;

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
