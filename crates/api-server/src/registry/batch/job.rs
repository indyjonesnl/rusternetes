//! Job strategies and storage — port of `pkg/registry/batch/job/strategy.go`
//! and `pkg/registry/batch/job/storage/storage.go`.
//!
//! The feature-gated field drops in `PrepareForCreate` / `PrepareForUpdate`
//! (`JobManagedBy`, `JobSuccessPolicy`, `JobBackoffLimitPerIndex`,
//! `JobPodReplacementPolicy`) are not ported: every one of those gates is GA
//! and locked on in 1.35 (`pkg/features/kube_features.go`), so the branches
//! never run.

use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::deletion::DeleteOptions;
use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::{Job, JobSpec, JobStatus};
use rusternetes_common::types::LabelSelector;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::job::{
    get_status_validation_options, validate_job, validate_job_update, validate_job_update_status,
    JobValidationOptions,
};
use rusternetes_common::validation::metav1::is_dns1123_label;
use rusternetes_common::Result;
use rusternetes_storage::StorageBackend;

use crate::registry::generic::{CreateOptions, Deleted, Store, UpdateOptions};
use crate::registry::rest::{
    GarbageCollectionPolicy, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestStorage, RestUpdateStrategy, UpdatedObjectInfo,
    ValidateObject, ValidateObjectUpdate,
};

// `pkg/apis/batch/types.go` label keys.
const LEGACY_JOB_NAME_LABEL: &str = "job-name";
const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";
const LEGACY_CONTROLLER_UID_LABEL: &str = "controller-uid";
const CONTROLLER_UID_LABEL: &str = "batch.kubernetes.io/controller-uid";

// `pkg/api/job/warnings.go:31-32`.
const COMPLETIONS_SOFT_LIMIT: i32 = 100_000;
const PARALLELISM_SOFT_LIMIT_FOR_UNLIMITED_COMPLETIONS: i32 = 10_000;

/// The v1 defaulting a decoded Job goes through: `SetDefaults_Job`
/// (pkg/apis/batch/v1/defaults.go) and the pod template's defaults.
pub fn convert_to_internal(job: &mut Job) {
    crate::handlers::defaults::apply_job_defaults(job);
}

/// `generateSelectorIfNeeded` (strategy.go:221-225). `manualSelector` is
/// defaulted to `false` (defaults.go:38-40), so absent means "generate".
fn generate_selector_if_needed(job: &mut Job) {
    if job.spec.manual_selector != Some(true) {
        generate_selector(job);
    }
}

/// `generateSelector` (strategy.go:230-278): the job-name and controller-uid
/// labels (legacy and prefixed) on the template, and the prefixed
/// controller-uid in the selector. A label the user already set is left for
/// validation to reject if it conflicts.
fn generate_selector(job: &mut Job) {
    let name = job.metadata.name.clone();
    let uid = job.metadata.uid.clone();
    let labels = job
        .spec
        .template
        .metadata
        .get_or_insert_with(Default::default)
        .labels
        .get_or_insert_with(Default::default);
    for key in [LEGACY_JOB_NAME_LABEL, JOB_NAME_LABEL] {
        labels
            .entry(key.to_string())
            .or_insert_with(|| name.clone());
    }
    for key in [LEGACY_CONTROLLER_UID_LABEL, CONTROLLER_UID_LABEL] {
        labels.entry(key.to_string()).or_insert_with(|| uid.clone());
    }
    job.spec
        .selector
        .get_or_insert_with(LabelSelector::default)
        .match_labels
        .get_or_insert_with(Default::default)
        .entry(CONTROLLER_UID_LABEL.to_string())
        .or_insert(uid);
}

/// `validationOptionsForJob` (strategy.go:175-207), with
/// `MutablePodResourcesForSuspendedJobs` and
/// `MutableSchedulingDirectivesForSuspendedJobs` at their 1.35 defaults (off).
fn validation_options_for_job(old: Option<&Job>) -> JobValidationOptions {
    let mut opts = JobValidationOptions {
        require_prefixed_labels: true,
        ..Default::default()
    };
    if let Some(old) = old {
        // Updating node affinity, node selector and tolerations is allowed
        // only for suspended jobs that never started before.
        let suspended = old.spec.suspend == Some(true);
        let not_started = old.status.as_ref().is_none_or(|s| s.start_time.is_none());
        opts.allow_mutable_scheduling_directives = suspended && not_started;
        // Validation should not fail jobs if they don't have the new labels.
        let labels = old
            .spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.as_ref());
        opts.require_prefixed_labels = labels.is_some_and(|l| {
            l.contains_key(JOB_NAME_LABEL) && l.contains_key(CONTROLLER_UID_LABEL)
        });
    }
    opts
}

/// `WarningsForJobSpec` (pkg/api/job/warnings.go:35-51), minus
/// `GetWarningsForPodTemplate` (#1996). `path` is where the spec sits: `spec`
/// for a Job, `spec.jobTemplate.spec` for a CronJob.
pub(crate) fn warnings_for_job_spec(path: &str, spec: &JobSpec) -> Vec<String> {
    if spec.completion_mode.as_deref() == Some("Indexed")
        && spec.completions.unwrap_or(0) > COMPLETIONS_SOFT_LIMIT
        && spec.parallelism.unwrap_or(0) > PARALLELISM_SOFT_LIMIT_FOR_UNLIMITED_COMPLETIONS
    {
        return vec![format!("{path}: In Indexed Jobs with a number of completions higher than 10^5 and a parallelism higher than 10^4, Kubernetes might not be able to track completedIndexes when a big number of indexes fail")];
    }
    Vec::new()
}

/// `jobStrategy` (strategy.go:52-58).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Job> for Strategy {
    /// `PrepareForCreate` (strategy.go:94-129): the selector is generated,
    /// status is cleared and the generation starts at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Job) {
        generate_selector_if_needed(obj);
        obj.status = Some(JobStatus::default());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &Job) -> ErrorList {
        validate_job(obj, &validation_options_for_job(None))
    }

    /// `WarningsOnCreate` (strategy.go:210-218).
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Job) -> Vec<String> {
        let mut warnings = Vec::new();
        let msgs = is_dns1123_label(&obj.metadata.name);
        if !msgs.is_empty() {
            warnings.push(format!(
                "metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: [{}]",
                msgs.join(" ")
            ));
        }
        warnings.extend(warnings_for_job_spec("spec", &obj.spec));
        warnings
    }
}

impl RestUpdateStrategy<Job> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:132-166): status is kept and a spec
    /// change bumps the generation.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Job, old: &Job) {
        obj.status = old.status.clone();
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateUpdate` (strategy.go:297-305): `ValidateJob` and
    /// `ValidateJobUpdate` with the update's options.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Job, old: &Job) -> ErrorList {
        let opts = validation_options_for_job(Some(old));
        let mut errs = validate_job(obj, &opts);
        errs.extend(validate_job_update(obj, old, &opts));
        errs
    }

    /// `WarningsOnUpdate` (strategy.go:308-316): only a spec change warns.
    fn warnings_on_update(&self, _ctx: &RequestContext, obj: &Job, old: &Job) -> Vec<String> {
        if obj.metadata.generation != old.metadata.generation {
            return warnings_for_job_spec("spec", &obj.spec);
        }
        Vec::new()
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `DefaultGarbageCollectionPolicy` (strategy.go:62-74): `OrphanDependents`
/// for batch/v1, "for back compatibility". batch/v1 is the only version
/// served, so the request's group-version need not be consulted.
impl RestDeleteStrategy<Job> for Strategy {
    fn default_garbage_collection_policy(
        &self,
        _ctx: &RequestContext,
    ) -> Option<GarbageCollectionPolicy> {
        Some(GarbageCollectionPolicy::OrphanDependents)
    }
}

/// `jobStatusStrategy` (strategy.go:318-435): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<Job> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:334-338: only status may change.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Job, old: &Job) {
        obj.spec = old.spec.clone();
    }

    /// `ValidateJobUpdateStatus` (strategy.go:340-346). Options
    /// derive from the old and new objects (`get_status_validation_options`).
    fn validate_update(&self, _ctx: &RequestContext, obj: &Job, old: &Job) -> ErrorList {
        validate_job_update_status(obj, old, &get_status_validation_options(obj, old))
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `deleteOptionWarnings` (storage/storage.go:56-57).
pub const DELETE_OPTION_WARNINGS: &str = "child pods are preserved by default when jobs are deleted; set propagationPolicy=Background to remove them or set propagationPolicy=Orphan to suppress this warning";

/// Job's `REST` (storage/storage.go:50-122): the Store, whose `Delete` and
/// `DeleteCollection` warn when the request leaves the propagation to the
/// batch/v1 default, which orphans the Job's pods.
pub struct JobRest {
    store: Store<Job, StorageBackend>,
}

/// The `if` of `REST.Delete` / `REST.DeleteCollection` (storage.go:102-108,
/// 113-119).
fn warn_on_default_propagation(ctx: &RequestContext, options: &DeleteOptions) {
    if options.propagation_policy.is_none()
        && options.orphan_dependents.is_none()
        && Strategy.default_garbage_collection_policy(ctx)
            == Some(GarbageCollectionPolicy::OrphanDependents)
    {
        ctx.add_warning(DELETE_OPTION_WARNINGS.to_string());
    }
}

#[async_trait]
impl RestStorage<Job> for JobRest {
    fn qualified_resource(&self) -> &GroupResource {
        self.store.qualified_resource()
    }

    fn namespace_scoped(&self) -> bool {
        RestStorage::namespace_scoped(&self.store)
    }

    async fn get(
        &self,
        ctx: &RequestContext,
        name: &str,
        options: &crate::registry::generic::GetOptions,
    ) -> Result<Job> {
        RestStorage::get(&self.store, ctx, name, options).await
    }

    async fn create(
        &self,
        ctx: &RequestContext,
        obj: Job,
        create_validation: Option<&dyn ValidateObject<Job>>,
        options: &CreateOptions,
    ) -> Result<Job> {
        RestStorage::create(&self.store, ctx, obj, create_validation, options).await
    }

    async fn update(
        &self,
        ctx: &RequestContext,
        name: &str,
        obj_info: &dyn UpdatedObjectInfo<Job>,
        create_validation: Option<&dyn ValidateObject<Job>>,
        update_validation: Option<&dyn ValidateObjectUpdate<Job>>,
        force_allow_create: bool,
        options: &UpdateOptions,
    ) -> Result<(Job, bool)> {
        RestStorage::update(
            &self.store,
            ctx,
            name,
            obj_info,
            create_validation,
            update_validation,
            force_allow_create,
            options,
        )
        .await
    }

    async fn delete(
        &self,
        ctx: &RequestContext,
        name: &str,
        delete_validation: Option<&dyn ValidateObject<Job>>,
        options: DeleteOptions,
    ) -> Result<(Deleted<Job>, bool)> {
        warn_on_default_propagation(ctx, &options);
        RestStorage::delete(&self.store, ctx, name, delete_validation, options).await
    }

    async fn delete_collection(
        &self,
        ctx: &RequestContext,
        delete_validation: Option<&dyn ValidateObject<Job>>,
        options: &DeleteOptions,
        list_options: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<Job>> {
        warn_on_default_propagation(ctx, options);
        RestStorage::delete_collection(&self.store, ctx, delete_validation, options, list_options)
            .await
    }
}

/// The Job endpoint's storage: [`JobRest`] over [`new_store`].
pub fn new_rest(storage: Arc<StorageBackend>) -> JobRest {
    JobRest {
        store: new_store(storage),
    }
}

/// `NewREST` (storage/storage.go:65-90): the Job store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Job, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("batch", "jobs"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}

/// The `/status` store: the Job store updating with [`StatusStrategy`]
/// (storage.go:85-87).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<Job, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}

#[cfg(test)]
mod tests {
    use super::*;
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
