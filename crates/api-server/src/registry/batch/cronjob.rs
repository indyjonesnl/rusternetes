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
use crate::ssa::ResetFields;

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
    /// generation starts at 1; `DropDisabledTemplateFields` is strategy.go:93.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut CronJob) {
        obj.status = Some(CronJobStatus::default());
        obj.metadata.generation = Some(1);
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.job_template.spec.template,
            None,
        );
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
        rusternetes_common::pod_drop_disabled::drop_disabled_template_fields(
            &mut obj.spec.job_template.spec.template,
            Some(&old.spec.job_template.spec.template),
        );
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
        // A plain int64 upstream: an absent generation is 0 (#2955).
        if obj.metadata.generation.unwrap_or(0) != old.metadata.generation.unwrap_or(0) {
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

    /// `GetResetFields` (batch/cronjob/strategy.go:73-84): `status`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new()
            .with("batch/v1", &[&["status"]])
            .with("batch/v1beta1", &[&["status"]])
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

    /// `GetResetFields` (batch/cronjob/strategy.go:174-183): `spec`.
    fn get_reset_fields(&self) -> ResetFields {
        ResetFields::new()
            .with("batch/v1", &[&["spec"]])
            .with("batch/v1beta1", &[&["spec"]])
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
