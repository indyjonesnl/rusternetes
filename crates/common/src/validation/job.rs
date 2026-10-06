//! Job validation — port of upstream Kubernetes
//! `pkg/apis/batch/validation/validation.go` (release-1.35).
//!
//! Covers the create-path spec validation exercised by clients and conformance:
//! non-negative numeric fields, `completionMode` enum + the Indexed-job rules
//! (completions required, parallelism/maxFailedIndexes caps, high-completions
//! soft limits), the indexed-job pod-hostname DNS-label check, the
//! `restartPolicy ∈ {OnFailure, Never}` rule, and selector validity + match.
//!
//! Run *after* the api-server has defaulted the spec and generated the selector
//! (`generateSelector`), mirroring upstream `ValidateJob`, which validates the
//! post-defaulting object.
//!
//! Also covers the policy sub-objects `podFailurePolicy`, `successPolicy`,
//! `podReplacementPolicy` and `managedBy` (#1326), including the
//! `successPolicy.rules[].succeededIndexes` interval-format validation
//! (`validateIndexesFormat`) and the `succeededCount <= totalIndexes`
//! cross-check (#1344).

use crate::resources::pod::{Affinity, PodSpec};
use crate::resources::workloads::{
    Job, JobCondition, JobSpec, JobTemplateSpec, PodFailurePolicyRule, SuccessPolicyRule,
};
use crate::types::LabelSelector;
use crate::validation::field::{BadValue, Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_label, is_dns1123_subdomain, is_qualified_name, label_selector_matches_labels,
    validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_immutable_field, validate_nonnegative_field,
    validate_object_meta, validate_object_meta_update,
};
use std::collections::HashSet;

// Upstream `pkg/apis/batch/types.go` label keys (prefix `batch.kubernetes.io/`).
const LEGACY_JOB_NAME_LABEL: &str = "job-name";
const LEGACY_CONTROLLER_UID_LABEL: &str = "controller-uid";
const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";
const CONTROLLER_UID_LABEL: &str = "batch.kubernetes.io/controller-uid";

// Upstream constants (`pkg/apis/batch/validation/validation.go`).
const MAX_PARALLELISM_FOR_INDEXED_JOB: i32 = 100_000;
const MAX_FAILED_INDEXES_FOR_INDEXED_JOB: i32 = 100_000;
const COMPLETIONS_SOFT_LIMIT: i32 = 100_000;
const PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS: i32 = 10_000;
const MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS: i32 = 10_000;
const MAX_POD_FAILURE_POLICY_RULES: usize = 20;
const MAX_ON_EXIT_CODES_VALUES: usize = 255;
const MAX_ON_POD_CONDITIONS_PATTERNS: usize = 20;
const MAX_SUCCESS_POLICY_RULES: usize = 20;
/// Upstream `maxJobSuccessPolicySucceededIndexesLimit` — 64 KiB cap on the
/// `succeededIndexes` string.
const MAX_JOB_SUCCESS_POLICY_SUCCEEDED_INDEXES_LIMIT: usize = 64 * 1024;

/// Parse one `succeededIndexes` interval (`"3"` or `"3-5"`) against
/// `completions`, returning the inclusive `(start, end)`. Mirrors upstream
/// `parseIndexInterval` (pkg/apis/batch/validation).
fn parse_index_interval(interval: &str, completions: i32) -> Result<(i32, i32), String> {
    let limits: Vec<&str> = interval.split('-').collect();
    if limits.len() > 2 {
        return Err(format!(
            "the fragment {interval:?} violates the requirement that an index interval can have at most two parts separated by '-'"
        ));
    }
    let x: i32 = limits[0].parse().map_err(|_| {
        format!(
            "cannot convert string to integer for index: {:?}",
            limits[0]
        )
    })?;
    if x >= completions {
        return Err(format!("too large index: {:?}", limits[0]));
    }
    if limits.len() == 2 {
        let y: i32 = limits[1].parse().map_err(|_| {
            format!(
                "cannot convert string to integer for index: {:?}",
                limits[1]
            )
        })?;
        if y >= completions {
            return Err(format!("too large index: {:?}", limits[1]));
        }
        if x >= y {
            return Err(format!("non-increasing order, previous: {x}, current: {y}"));
        }
        return Ok((x, y));
    }
    Ok((x, x))
}

/// Parse a `succeededIndexes` string (`"1,3-5,7"`) against `completions`,
/// returning the total number of covered indexes. Intervals must be in strictly
/// increasing, non-overlapping order. Mirrors upstream `validateIndexesFormat`.
fn validate_indexes_format(indexes: &str, completions: i32) -> Result<i32, String> {
    if indexes.is_empty() {
        return Ok(0);
    }
    let mut last_index: Option<i32> = None;
    let mut total: i32 = 0;
    for interval in indexes.split(',') {
        let (x, y) = parse_index_interval(interval, completions)?;
        if let Some(last) = last_index {
            if last >= x {
                return Err(format!(
                    "non-increasing order, previous: {last}, current: {x}"
                ));
            }
        }
        total += y - x + 1;
        last_index = Some(y);
    }
    Ok(total)
}
const MAX_MANAGED_BY_LENGTH: usize = 63;

const NON_INDEXED_COMPLETION: &str = "NonIndexed";
const INDEXED_COMPLETION: &str = "Indexed";
const POD_FAILURE_POLICY_ACTIONS: [&str; 4] = ["FailJob", "FailIndex", "Ignore", "Count"];
const ON_EXIT_CODES_OPERATORS: [&str; 2] = ["In", "NotIn"];
const ON_POD_CONDITIONS_STATUSES: [&str; 3] = ["True", "False", "Unknown"];
const POD_REPLACEMENT_POLICIES: [&str; 2] = ["Failed", "TerminatingOrFailed"];

/// Port of upstream `validateJobSpec` (the inner validator, WITHOUT the
/// selector-required / selector-match checks that `ValidateJobSpec` adds). This
/// is the part shared with CronJob's `ValidateJobTemplateSpec`, whose template
/// must NOT carry a selector.
fn validate_job_spec_core(spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(p) = spec.parallelism {
        errs.extend(validate_nonnegative_field(
            p as i64,
            &fld_path.child("parallelism"),
        ));
    }
    if let Some(c) = spec.completions {
        errs.extend(validate_nonnegative_field(
            c as i64,
            &fld_path.child("completions"),
        ));
    }
    if let Some(a) = spec.active_deadline_seconds {
        errs.extend(validate_nonnegative_field(
            a,
            &fld_path.child("activeDeadlineSeconds"),
        ));
    }
    if let Some(b) = spec.backoff_limit {
        errs.extend(validate_nonnegative_field(
            b as i64,
            &fld_path.child("backoffLimit"),
        ));
    }
    if let Some(t) = spec.ttl_seconds_after_finished {
        errs.extend(validate_nonnegative_field(
            t as i64,
            &fld_path.child("ttlSecondsAfterFinished"),
        ));
    }
    if let Some(bpi) = spec.backoff_limit_per_index {
        errs.extend(validate_nonnegative_field(
            bpi as i64,
            &fld_path.child("backoffLimitPerIndex"),
        ));
    }
    if let Some(mfi) = spec.max_failed_indexes {
        errs.extend(validate_nonnegative_field(
            mfi as i64,
            &fld_path.child("maxFailedIndexes"),
        ));
        if spec.backoff_limit_per_index.is_none() {
            errs.push(Error::required(
                &fld_path.child("backoffLimitPerIndex"),
                "when maxFailedIndexes is specified",
            ));
        }
    }

    let is_indexed = spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION);
    if let Some(mode) = &spec.completion_mode {
        if mode != NON_INDEXED_COMPLETION && mode != INDEXED_COMPLETION {
            errs.push(Error::not_supported(
                &fld_path.child("completionMode"),
                mode.clone(),
                &[NON_INDEXED_COMPLETION, INDEXED_COMPLETION],
            ));
        }
        if is_indexed {
            if spec.completions.is_none() {
                errs.push(Error::required(
                    &fld_path.child("completions"),
                    format!("when completion mode is {}", INDEXED_COMPLETION),
                ));
            }
            if let Some(p) = spec.parallelism {
                if p > MAX_PARALLELISM_FOR_INDEXED_JOB {
                    errs.push(Error::invalid(
                        &fld_path.child("parallelism"),
                        p.to_string(),
                        format!(
                            "must be less than or equal to {} when completion mode is {}",
                            MAX_PARALLELISM_FOR_INDEXED_JOB, INDEXED_COMPLETION
                        ),
                    ));
                }
            }
            if let (Some(c), Some(mfi)) = (spec.completions, spec.max_failed_indexes) {
                if mfi > c {
                    errs.push(Error::invalid(
                        &fld_path.child("maxFailedIndexes"),
                        mfi.to_string(),
                        "must be less than or equal to completions",
                    ));
                }
            }
            if let Some(mfi) = spec.max_failed_indexes {
                if mfi > MAX_FAILED_INDEXES_FOR_INDEXED_JOB {
                    errs.push(Error::invalid(
                        &fld_path.child("maxFailedIndexes"),
                        mfi.to_string(),
                        format!(
                            "must be less than or equal to {}",
                            MAX_FAILED_INDEXES_FOR_INDEXED_JOB
                        ),
                    ));
                }
            }
            if let Some(c) = spec.completions {
                if c > COMPLETIONS_SOFT_LIMIT && spec.backoff_limit_per_index.is_some() {
                    if spec.max_failed_indexes.is_none() {
                        errs.push(Error::required(
                            &fld_path.child("maxFailedIndexes"),
                            format!(
                                "must be specified when completions is above {}",
                                COMPLETIONS_SOFT_LIMIT
                            ),
                        ));
                    }
                    if let Some(p) = spec.parallelism {
                        if p > PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS {
                            errs.push(Error::invalid(
                                &fld_path.child("parallelism"),
                                p.to_string(),
                                format!(
                                    "must be less than or equal to {} when completions are above {} and used with backoff limit per index",
                                    PARALLELISM_LIMIT_FOR_HIGH_COMPLETIONS, COMPLETIONS_SOFT_LIMIT
                                ),
                            ));
                        }
                    }
                    if let Some(mfi) = spec.max_failed_indexes {
                        if mfi > MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS {
                            errs.push(Error::invalid(
                                &fld_path.child("maxFailedIndexes"),
                                mfi.to_string(),
                                format!(
                                    "must be less than or equal to {} when completions are above {} and used with backoff limit per index",
                                    MAX_FAILED_INDEXES_LIMIT_FOR_HIGH_COMPLETIONS, COMPLETIONS_SOFT_LIMIT
                                ),
                            ));
                        }
                    }
                }
            }
        }
    }

    // backoffLimitPerIndex / maxFailedIndexes require indexed completion mode.
    if !is_indexed {
        if let Some(bpi) = spec.backoff_limit_per_index {
            errs.push(Error::invalid(
                &fld_path.child("backoffLimitPerIndex"),
                bpi.to_string(),
                "requires indexed completion mode",
            ));
        }
        if let Some(mfi) = spec.max_failed_indexes {
            errs.push(Error::invalid(
                &fld_path.child("maxFailedIndexes"),
                mfi.to_string(),
                "requires indexed completion mode",
            ));
        }
    }

    // The template is held to the same rules as a standalone pod: upstream's
    // `ValidateJobSpec` calls `ValidatePodTemplateSpec` on it
    // (`pkg/apis/batch/validation/validation.go:276`), so a Job with no
    // containers answers `spec.template.spec.containers: Required value`
    // rather than a decoder error (#1939).
    errs.extend(crate::validation::podtemplate::validate_pod_template_spec(
        &spec.template,
        &fld_path.child("template"),
        false,
    ));

    // template.spec.restartPolicy must be OnFailure or Never (upstream rejects
    // the SetDefaults_PodSpec-defaulted "Always"/empty for Jobs). With a
    // podFailurePolicy, only "Never" is permitted (upstream validation.go:287).
    let rp_path = fld_path
        .child("template")
        .child("spec")
        .child("restartPolicy");
    let restart_policy = spec.template.spec.restart_policy.as_deref().unwrap_or("");
    if restart_policy.is_empty() || restart_policy == "Always" {
        errs.push(Error::required(
            &rp_path,
            "valid values: \"OnFailure\", \"Never\"",
        ));
    } else if restart_policy != "OnFailure" && restart_policy != "Never" {
        errs.push(Error::not_supported(
            &rp_path,
            restart_policy.to_string(),
            &["OnFailure", "Never"],
        ));
    } else if spec.pod_failure_policy.is_some() && restart_policy != "Never" {
        errs.push(Error::invalid(
            &rp_path,
            restart_policy.to_string(),
            "only \"Never\" is supported when podFailurePolicy is specified",
        ));
    }

    // managedBy — domain-prefixed path, length-bounded.
    if let Some(managed_by) = &spec.managed_by {
        let mb_path = fld_path.child("managedBy");
        errs.extend(validate_domain_prefixed_path(managed_by, &mb_path));
        if managed_by.len() > MAX_MANAGED_BY_LENGTH {
            errs.push(Error::too_long(&mb_path, MAX_MANAGED_BY_LENGTH));
        }
    }

    // podFailurePolicy.
    if let Some(pfp) = &spec.pod_failure_policy {
        errs.extend(validate_pod_failure_policy(
            spec,
            &pfp.rules,
            &fld_path.child("podFailurePolicy"),
        ));
    }

    // successPolicy — Indexed-only; then the per-rule checks.
    if let Some(sp) = &spec.success_policy {
        let sp_path = fld_path.child("successPolicy");
        if !is_indexed {
            errs.push(Error::invalid(
                &sp_path,
                String::new(),
                "requires indexed completion mode",
            ));
        } else {
            errs.extend(validate_success_policy(spec, &sp.rules, &sp_path));
        }
    }

    // podReplacementPolicy.
    errs.extend(validate_pod_replacement_policy(
        spec,
        &fld_path.child("podReplacementPolicy"),
    ));

    errs
}

/// Port of upstream `ValidateJobSpec`: the core spec checks plus the
/// selector-required / selector validity / selector-matches-template checks.
fn validate_job_spec(spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let mut errs = validate_job_spec_core(spec, fld_path);

    // Selector: required and valid. Upstream `ValidateJobSpec` only requires +
    // validates the selector here; the match check below runs regardless.
    match &spec.selector {
        None => errs.push(Error::required(&fld_path.child("selector"), "")),
        Some(sel) => {
            errs.extend(validate_label_selector(
                sel,
                LabelSelectorValidationOptions::default(),
                &fld_path.child("selector"),
            ));
        }
    }

    // Whether manually or automatically generated, the selector of the job must
    // match the pods it will produce — honoring `matchExpressions`, not only
    // `matchLabels` (upstream validation.go:182-187 via `LabelSelectorAsSelector`).
    if let Some(sel) = &spec.selector {
        let empty_labels = std::collections::HashMap::new();
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .and_then(|m| m.labels.as_ref())
            .unwrap_or(&empty_labels);
        if !label_selector_matches_labels(sel, template_labels) {
            errs.push(Error::invalid(
                &fld_path.child("template").child("metadata").child("labels"),
                template_labels
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(","),
                "`selector` does not match template `labels`",
            ));
        }
    }

    errs
}

/// Port of upstream `ValidateJobTemplateSpec` — validates the embedded job spec
/// of a CronJob's `jobTemplate`. The template must NOT carry a selector (it is
/// auto-generated) and must not set `manualSelector: true`.
pub fn validate_job_template_spec(template: &JobTemplateSpec, fld_path: &Path) -> ErrorList {
    let spec_path = fld_path.child("spec");
    let mut errs = validate_job_spec_core(&template.spec, &spec_path);

    if template.spec.selector.is_some() {
        errs.push(Error::invalid(
            &spec_path.child("selector"),
            String::new(),
            "`selector` will be auto-generated",
        ));
    }
    if template.spec.manual_selector == Some(true) {
        errs.push(Error::not_supported(
            &spec_path.child("manualSelector"),
            "true",
            &["nil", "false"],
        ));
    }
    errs
}

/// Port of upstream `apivalidation.ValidateHasLabel`: the label `key` must be
/// present on `meta` and equal to `expected_value`.
fn validate_has_label(
    labels: Option<&std::collections::HashMap<String, String>>,
    fld_path: &Path,
    key: &str,
    expected_value: &str,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match labels.and_then(|l| l.get(key)) {
        None => errs.push(Error::required(
            &fld_path.child("labels").key(key),
            format!("must be '{expected_value}'"),
        )),
        Some(actual) if actual != expected_value => errs.push(Error::invalid(
            &fld_path.child("labels").key(key),
            labels
                .map(|l| {
                    l.iter()
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default(),
            format!("must be '{expected_value}'"),
        )),
        _ => {}
    }
    errs
}

/// Port of upstream `validateGeneratedSelector`: when the selector is
/// auto-generated (not `manualSelector`), the pod template must carry the
/// generated `controller-uid` / `job-name` labels (prefixed + legacy) matching
/// the Job's `uid` / `name`, the Job's `uid` must be set, and the selector must
/// match those generated labels (else `selector` not auto-generated).
///
/// `validate_batch_labels` mirrors upstream `opts.RequirePrefixedLabels`; on the
/// create path it is `true`.
fn validate_generated_selector(job: &Job, validate_batch_labels: bool) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if job.spec.manual_selector == Some(true) {
        return errs;
    }
    // Already reported as required by the caller when absent.
    let Some(selector) = &job.spec.selector else {
        return errs;
    };

    // An unset uid would yield "controller-uid=" as the selector, which is bad.
    if job.metadata.uid.is_empty() {
        errs.push(Error::required(&Path::new("metadata").child("uid"), ""));
    }

    let template_meta = job.spec.template.metadata.as_ref();
    let template_labels = template_meta.and_then(|m| m.labels.as_ref());
    let template_path = Path::new("spec").child("template").child("metadata");

    // The expected (generated) labels the selector must match.
    let mut expected_labels: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    errs.extend(validate_has_label(
        template_labels,
        &template_path,
        LEGACY_CONTROLLER_UID_LABEL,
        &job.metadata.uid,
    ));
    errs.extend(validate_has_label(
        template_labels,
        &template_path,
        LEGACY_JOB_NAME_LABEL,
        &job.metadata.name,
    ));
    if validate_batch_labels {
        errs.extend(validate_has_label(
            template_labels,
            &template_path,
            CONTROLLER_UID_LABEL,
            &job.metadata.uid,
        ));
        errs.extend(validate_has_label(
            template_labels,
            &template_path,
            JOB_NAME_LABEL,
            &job.metadata.name,
        ));
        expected_labels.insert(CONTROLLER_UID_LABEL.to_string(), job.metadata.uid.clone());
        expected_labels.insert(JOB_NAME_LABEL.to_string(), job.metadata.name.clone());
    }
    // Labels created by the Kubernetes project carry a prefix; the legacy
    // (unprefixed) ones are set for backward compatibility.
    expected_labels.insert(
        LEGACY_CONTROLLER_UID_LABEL.to_string(),
        job.metadata.uid.clone(),
    );
    expected_labels.insert(LEGACY_JOB_NAME_LABEL.to_string(), job.metadata.name.clone());

    // The selector must match the generated labels.
    if !label_selector_matches_labels(selector, &expected_labels) {
        errs.push(Error::invalid(
            &Path::new("spec").child("selector"),
            selector_display(selector),
            "`selector` not auto-generated",
        ));
    }

    errs
}

/// Compact `key=value` rendering of a `LabelSelector` for error messages.
fn selector_display(sel: &LabelSelector) -> String {
    sel.match_labels
        .as_ref()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

/// Upstream `JobValidationOptions` (validation.go:83-91), minus the embedded
/// `PodValidationOptions`, which the pod-template validators do not take here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobValidationOptions {
    /// Allow mutable node affinity, selector and tolerations of the template.
    pub allow_mutable_scheduling_directives: bool,
    /// Require the `batch.kubernetes.io/job-name` and
    /// `batch.kubernetes.io/controller-uid` template labels.
    pub require_prefixed_labels: bool,
    /// Allow mutable pod resources.
    pub allow_mutable_pod_resources: bool,
}

/// Validate a `Job`: upstream `ValidateJob` (validation.go:151-167).
/// "Jobs and rcs have the same name validation": `NameIsDNSSubdomain`.
pub fn validate_job(job: &Job, opts: &JobValidationOptions) -> ErrorList {
    let mut errs = validate_object_meta(
        &job.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_generated_selector(
        job,
        opts.require_prefixed_labels,
    ));
    errs.extend(validate_job_spec(&job.spec, &Path::new("spec")));

    // Indexed job pods get a `-$INDEX` hostname suffix; the max index is
    // `completions-1`. Reject names that would yield an invalid DNS-1123 label.
    if job.spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION) {
        if let Some(c) = job.spec.completions {
            if c > 0 {
                let max_hostname = format!("{}-{}", job.metadata.name, c - 1);
                if !is_dns1123_label(&max_hostname).is_empty() {
                    errs.push(Error::invalid(
                        &Path::new("metadata").child("name"),
                        job.metadata.name.clone(),
                        format!(
                            "will not able to create pod with invalid DNS label: {}",
                            max_hostname
                        ),
                    ));
                }
            }
        }
    }

    errs
}

/// Upstream `ValidateJobUpdate` (validation.go:609-613).
pub fn validate_job_update(job: &Job, old_job: &Job, opts: &JobValidationOptions) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&job.metadata, &old_job.metadata, &Path::new("metadata"));
    errs.extend(validate_job_spec_update(
        &job.spec,
        &old_job.spec,
        &Path::new("spec"),
        opts,
    ));
    errs
}

/// Upstream `ValidateJobUpdateStatus` (validation.go:616-620): the metadata
/// update and [`validate_job_status_update`].
pub fn validate_job_update_status(
    job: &Job,
    old_job: &Job,
    opts: &JobStatusValidationOptions,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&job.metadata, &old_job.metadata, &Path::new("metadata"));
    errs.extend(validate_job_status_update(job, old_job, opts));
    errs
}

/// Upstream `ValidateJobSpecUpdate` (validation.go:623-635).
fn validate_job_spec_update(
    spec: &JobSpec,
    old_spec: &JobSpec,
    fld_path: &Path,
    opts: &JobValidationOptions,
) -> ErrorList {
    let mut errs = validate_job_spec(spec, fld_path);
    errs.extend(validate_completions(
        spec,
        old_spec,
        &fld_path.child("completions"),
    ));
    errs.extend(validate_immutable_field(
        &spec.selector,
        &old_spec.selector,
        &fld_path.child("selector"),
    ));
    errs.extend(validate_pod_template_update(spec, old_spec, fld_path, opts));
    errs.extend(validate_immutable_field(
        &spec.completion_mode,
        &old_spec.completion_mode,
        &fld_path.child("completionMode"),
    ));
    errs.extend(validate_immutable_field(
        &spec.pod_failure_policy,
        &old_spec.pod_failure_policy,
        &fld_path.child("podFailurePolicy"),
    ));
    errs.extend(validate_immutable_field(
        &spec.backoff_limit_per_index,
        &old_spec.backoff_limit_per_index,
        &fld_path.child("backoffLimitPerIndex"),
    ));
    errs.extend(validate_immutable_field(
        &spec.managed_by,
        &old_spec.managed_by,
        &fld_path.child("managedBy"),
    ));
    errs.extend(validate_immutable_field(
        &spec.success_policy,
        &old_spec.success_policy,
        &fld_path.child("successPolicy"),
    ));
    errs
}

/// Upstream `validatePodTemplateUpdate` (validation.go:637-676).
fn validate_pod_template_update(
    spec: &JobSpec,
    old_spec: &JobSpec,
    fld_path: &Path,
    opts: &JobValidationOptions,
) -> ErrorList {
    let template = &spec.template;
    let mut old_template = old_spec.template.clone();
    if opts.allow_mutable_scheduling_directives {
        match (&template.spec.affinity, &mut old_template.spec.affinity) {
            // Allow the Affinity field to be cleared if the old template had
            // no affinity directives other than NodeAffinity.
            (None, Some(old)) => {
                old.node_affinity = None;
                if old.pod_affinity.is_none() && old.pod_anti_affinity.is_none() {
                    old_template.spec.affinity = None;
                }
            }
            // Allow the NodeAffinity field to skip immutability checking.
            (Some(new), None) => {
                old_template.spec.affinity = Some(Affinity {
                    node_affinity: new.node_affinity.clone(),
                    pod_affinity: None,
                    pod_anti_affinity: None,
                });
            }
            (Some(new), Some(old)) => old.node_affinity = new.node_affinity.clone(),
            (None, None) => {}
        }
        old_template.spec.node_selector = template.spec.node_selector.clone();
        old_template.spec.tolerations = template.spec.tolerations.clone();
        old_template.spec.scheduling_gates = template.spec.scheduling_gates.clone();
        let new_meta = template.metadata.clone().unwrap_or_default();
        let old_meta = old_template.metadata.get_or_insert_with(Default::default);
        old_meta.annotations = new_meta.annotations;
        old_meta.labels = new_meta.labels;
    }

    let template_path = fld_path.child("template");
    let suspended = old_spec.suspend == Some(true);
    if suspended && opts.allow_mutable_pod_resources {
        // Only allow container resource updates.
        validate_pod_resource_updates_only(
            &template.spec,
            &old_template.spec,
            &fld_path.child("template.spec"),
        )
    } else {
        // If the job isn't suspended, then we cannot allow any mutation of
        // the template.
        validate_immutable_field(template, &old_template, &template_path)
    }
}

/// Upstream `validatePodResourceUpdatesOnly` (validation.go:679-707).
fn validate_pod_resource_updates_only(
    new_pod: &PodSpec,
    old_pod: &PodSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut old_copy = old_pod.clone();
    if new_pod.containers.len() == old_copy.containers.len() {
        for (old, new) in old_copy.containers.iter_mut().zip(&new_pod.containers) {
            if old.name == new.name {
                old.resources = new.resources.clone();
            }
        }
    }
    if let (Some(new_init), Some(old_init)) =
        (&new_pod.init_containers, &mut old_copy.init_containers)
    {
        if new_init.len() == old_init.len() {
            for (old, new) in old_init.iter_mut().zip(new_init) {
                if old.name == new.name {
                    old.resources = new.resources.clone();
                }
            }
        }
    }
    validate_immutable_field(new_pod, &old_copy, fld_path)
}

/// Upstream `validateCompletions` (validation.go:902-923): completions is
/// immutable for a non-indexed Job, and for an Indexed Job may change only in
/// tandem with parallelism.
fn validate_completions(spec: &JobSpec, old_spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let is_indexed_job = spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION);
    if !is_indexed_job {
        return validate_immutable_field(&spec.completions, &old_spec.completions, fld_path);
    }
    if spec.completions == old_spec.completions {
        return Vec::new();
    }
    // Indexed Jobs cannot set completions to nil. The nil check is already
    // performed in validateJobSpec.
    let Some(completions) = spec.completions else {
        return Vec::new();
    };
    if Some(completions) != spec.parallelism {
        return vec![Error::invalid(
            fld_path,
            completions,
            "can only be modified in tandem with spec.parallelism",
        )];
    }
    Vec::new()
}

/// Port of upstream `IsDomainPrefixedPath` (structure + host subdomain). The
/// trailing path segment's `httpPathRegexp` is not replicated.
fn validate_domain_prefixed_path(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if value.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    let segments: Vec<&str> = value.splitn(2, '/').collect();
    if segments.len() != 2 || segments[0].is_empty() || segments[1].is_empty() {
        errs.push(Error::invalid(
            fld_path,
            value.to_string(),
            "must be a domain-prefixed path (such as \"acme.io/foo\")",
        ));
        return errs;
    }
    for msg in is_dns1123_subdomain(segments[0]) {
        errs.push(Error::invalid(fld_path, segments[0].to_string(), msg));
    }
    errs
}

/// Port of upstream `validatePodReplacementPolicy`.
fn validate_pod_replacement_policy(spec: &JobSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if let Some(prp) = &spec.pod_replacement_policy {
        if spec.pod_failure_policy.is_some() {
            // With a podFailurePolicy, only "Failed" is allowed.
            if prp != "Failed" {
                errs.push(Error::not_supported(fld_path, prp.clone(), &["Failed"]));
            }
        } else if !POD_REPLACEMENT_POLICIES.contains(&prp.as_str()) {
            errs.push(Error::not_supported(
                fld_path,
                prp.clone(),
                &POD_REPLACEMENT_POLICIES,
            ));
        }
    }
    errs
}

/// Port of upstream `validatePodFailurePolicy` + `validatePodFailurePolicyRule`
/// (+ the onExitCodes / onPodConditions sub-validators).
fn validate_pod_failure_policy(
    spec: &JobSpec,
    rules: &[PodFailurePolicyRule],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let rules_path = fld_path.child("rules");
    if rules.len() > MAX_POD_FAILURE_POLICY_RULES {
        errs.push(Error::too_many(&rules_path, MAX_POD_FAILURE_POLICY_RULES));
    }

    let mut container_names: HashSet<&str> = HashSet::new();
    for c in &spec.template.spec.containers {
        container_names.insert(c.name.as_str());
    }
    if let Some(inits) = &spec.template.spec.init_containers {
        for c in inits {
            container_names.insert(c.name.as_str());
        }
    }

    for (i, rule) in rules.iter().enumerate() {
        let rule_path = rules_path.index(i);
        let action_path = rule_path.child("action");
        if rule.action.is_empty() {
            errs.push(Error::required(
                &action_path,
                "valid values: \"Count\", \"FailIndex\", \"FailJob\", \"Ignore\"",
            ));
        } else if rule.action == "FailIndex" {
            if spec.backoff_limit_per_index.is_none() {
                errs.push(Error::invalid(
                    &action_path,
                    rule.action.clone(),
                    "requires the backoffLimitPerIndex to be set",
                ));
            }
        } else if !POD_FAILURE_POLICY_ACTIONS.contains(&rule.action.as_str()) {
            errs.push(Error::not_supported(
                &action_path,
                rule.action.clone(),
                &POD_FAILURE_POLICY_ACTIONS,
            ));
        }

        if let Some(on_exit) = &rule.on_exit_codes {
            let oec_path = rule_path.child("onExitCodes");
            let op_path = oec_path.child("operator");
            if on_exit.operator.is_empty() {
                errs.push(Error::required(&op_path, "valid values: \"In\", \"NotIn\""));
            } else if !ON_EXIT_CODES_OPERATORS.contains(&on_exit.operator.as_str()) {
                errs.push(Error::not_supported(
                    &op_path,
                    on_exit.operator.clone(),
                    &ON_EXIT_CODES_OPERATORS,
                ));
            }
            if let Some(cn) = &on_exit.container_name {
                if !container_names.contains(cn.as_str()) {
                    errs.push(Error::invalid(
                        &oec_path.child("containerName"),
                        cn.clone(),
                        "must be one of the container or initContainer names in the pod template",
                    ));
                }
            }
            let values_path = oec_path.child("values");
            if on_exit.values.is_empty() {
                errs.push(Error::invalid(
                    &values_path,
                    String::new(),
                    "at least one value is required",
                ));
            } else if on_exit.values.len() > MAX_ON_EXIT_CODES_VALUES {
                errs.push(Error::too_many(&values_path, MAX_ON_EXIT_CODES_VALUES));
            }
            let mut seen: HashSet<i32> = HashSet::new();
            let mut ordered = true;
            for (j, &v) in on_exit.values.iter().enumerate() {
                let vp = values_path.index(j);
                if on_exit.operator == "In" && v == 0 {
                    errs.push(Error::invalid(&vp, v, "must not be 0 for the In operator"));
                }
                if !seen.insert(v) {
                    errs.push(Error::duplicate(&vp, v));
                }
                if j > 0 && on_exit.values[j - 1] > v {
                    ordered = false;
                }
            }
            if !ordered {
                errs.push(Error::invalid(
                    &values_path,
                    String::new(),
                    "must be ordered",
                ));
            }
        }

        if !rule.on_pod_conditions.is_empty() {
            let opc_path = rule_path.child("onPodConditions");
            if rule.on_pod_conditions.len() > MAX_ON_POD_CONDITIONS_PATTERNS {
                errs.push(Error::too_many(&opc_path, MAX_ON_POD_CONDITIONS_PATTERNS));
            }
            for (j, pattern) in rule.on_pod_conditions.iter().enumerate() {
                let p_path = opc_path.index(j);
                for msg in is_qualified_name(&pattern.condition_type) {
                    errs.push(Error::invalid(
                        &p_path.child("type"),
                        pattern.condition_type.clone(),
                        msg,
                    ));
                }
                let status_path = p_path.child("status");
                match pattern.status.as_deref() {
                    None | Some("") => errs.push(Error::required(
                        &status_path,
                        "valid values: \"False\", \"True\", \"Unknown\"",
                    )),
                    Some(s) if !ON_POD_CONDITIONS_STATUSES.contains(&s) => {
                        errs.push(Error::not_supported(
                            &status_path,
                            s.to_string(),
                            &ON_POD_CONDITIONS_STATUSES,
                        ))
                    }
                    _ => {}
                }
            }
        }

        let has_exit = rule.on_exit_codes.is_some();
        let has_cond = !rule.on_pod_conditions.is_empty();
        if has_exit && has_cond {
            errs.push(Error::invalid(
                &rule_path,
                String::new(),
                "specifying both OnExitCodes and OnPodConditions is not supported",
            ));
        }
        if !has_exit && !has_cond {
            errs.push(Error::invalid(
                &rule_path,
                String::new(),
                "specifying one of OnExitCodes and OnPodConditions is required",
            ));
        }
    }
    errs
}

/// Port of upstream `validateSuccessPolicy` + `validateSuccessPolicyRule`. The
/// `succeededIndexes` interval-format parser (`validateIndexesFormat`) is left
/// as a follow-up; presence/count + `succeededCount` bounds are ported.
fn validate_success_policy(
    spec: &JobSpec,
    rules: &[SuccessPolicyRule],
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let rules_path = fld_path.child("rules");
    if rules.is_empty() {
        errs.push(Error::required(
            &rules_path,
            "at least one rules must be specified when the successPolicy is specified",
        ));
    }
    if rules.len() > MAX_SUCCESS_POLICY_RULES {
        errs.push(Error::too_many(&rules_path, MAX_SUCCESS_POLICY_RULES));
    }
    for (i, rule) in rules.iter().enumerate() {
        let rule_path = rules_path.index(i);
        if rule.succeeded_count.is_none() && rule.succeeded_indexes.is_none() {
            errs.push(Error::required(
                &rule_path,
                "at least one of succeededCount or succeededIndexes must be specified",
            ));
        }
        // succeededIndexes: length cap + interval-format parse (upstream
        // validateSuccessPolicyRule). `total_indexes` feeds the succeededCount
        // cross-check below.
        let mut total_indexes: i32 = 0;
        if let Some(indexes) = &rule.succeeded_indexes {
            let sip = rule_path.child("succeededIndexes");
            if indexes.len() > MAX_JOB_SUCCESS_POLICY_SUCCEEDED_INDEXES_LIMIT {
                errs.push(Error::too_long(
                    &sip,
                    MAX_JOB_SUCCESS_POLICY_SUCCEEDED_INDEXES_LIMIT,
                ));
            }
            let completions = spec.completions.unwrap_or(0);
            match validate_indexes_format(indexes, completions) {
                Ok(t) => total_indexes = t,
                Err(e) => errs.push(Error::invalid(
                    &sip,
                    indexes.clone(),
                    format!("error parsing succeededIndexes: {e}"),
                )),
            }
        }
        if let Some(count) = rule.succeeded_count {
            let cp = rule_path.child("succeededCount");
            errs.extend(validate_nonnegative_field(count as i64, &cp));
            if let Some(completions) = spec.completions {
                if count > completions {
                    errs.push(Error::invalid(
                        &cp,
                        count,
                        format!(
                            "must be less than or equal to {} (the number of specified completions)",
                            completions
                        ),
                    ));
                }
            }
            if rule.succeeded_indexes.is_some() && count > total_indexes {
                errs.push(Error::invalid(
                    &cp,
                    count,
                    format!(
                        "must be less than or equal to {} (the number of indexes in the specified succeededIndexes field)",
                        total_indexes
                    ),
                ));
            }
        }
    }
    errs
}

/// Upstream `JobStatusValidationOptions`
/// (`pkg/apis/batch/validation/validation.go:1072-1096`). Every rule that
/// `validateJobStatus` / `ValidateJobStatusUpdate` can apply is gated by one
/// of these; [`get_status_validation_options`] derives them from the old and
/// new objects.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobStatusValidationOptions {
    pub reject_decreasing_succeeded_counter: bool,
    pub reject_decreasing_failed_counter: bool,
    pub reject_disabling_terminal_condition: bool,
    pub reject_invalid_completed_indexes: bool,
    pub reject_invalid_failed_indexes: bool,
    pub reject_failed_indexes_overlapping_completed: bool,
    pub reject_completed_indexes_for_non_indexed_job: bool,
    pub reject_failed_indexes_for_no_backoff_limit_per_index: bool,
    pub reject_failed_job_without_failure_target: bool,
    pub reject_complete_job_without_success_criteria_met: bool,
    pub reject_finished_job_with_active_pods: bool,
    pub reject_finished_job_without_start_time: bool,
    pub reject_finished_job_with_uncounted_terminated_pods: bool,
    pub reject_start_time_update_for_unsuspended_job: bool,
    pub reject_completion_time_before_start_time: bool,
    pub reject_mutating_completion_time: bool,
    pub reject_complete_job_without_completion_time: bool,
    pub reject_not_complete_job_with_completion_time: bool,
    pub reject_complete_job_with_failed_condition: bool,
    pub reject_complete_job_with_failure_target_condition: bool,
    pub allow_for_success_criteria_met_in_extended_scope: bool,
    pub reject_more_ready_than_active_pods: bool,
    pub reject_finished_job_with_terminating_pods: bool,
}

// Upstream `IsJobFinished` .. `IsConditionFalse` (validation.go:925-966).
fn job_conditions(job: &Job) -> &[JobCondition] {
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_deref())
        .unwrap_or(&[])
}

fn is_condition_with_status(conds: &[JobCondition], c_type: &str, status: &str) -> bool {
    conds
        .iter()
        .any(|c| c.condition_type == c_type && c.status == status)
}

/// Upstream `IsConditionTrue` (validation.go:950).
fn is_condition_true(conds: &[JobCondition], c_type: &str) -> bool {
    is_condition_with_status(conds, c_type, "True")
}

/// Upstream `IsConditionFalse` (validation.go:959).
fn is_condition_false(conds: &[JobCondition], c_type: &str) -> bool {
    is_condition_with_status(conds, c_type, "False")
}

/// Upstream `IsJobComplete` (validation.go:934).
fn is_job_complete(job: &Job) -> bool {
    is_condition_true(job_conditions(job), "Complete")
}

/// Upstream `IsJobFailed` (validation.go:938).
fn is_job_failed(job: &Job) -> bool {
    is_condition_true(job_conditions(job), "Failed")
}

/// Upstream `IsJobFinished` (validation.go:925).
fn is_job_finished(job: &Job) -> bool {
    is_job_complete(job) || is_job_failed(job)
}

/// Upstream `isJobSuccessCriteriaMet` (validation.go:942).
fn is_job_success_criteria_met(job: &Job) -> bool {
    is_condition_true(job_conditions(job), "SuccessCriteriaMet")
}

/// Upstream `isJobFailureTarget` (validation.go:946).
fn is_job_failure_target(job: &Job) -> bool {
    is_condition_true(job_conditions(job), "FailureTarget")
}

/// `metav1.Time` is serialized (and so stored and compared) at second
/// granularity.
fn time_secs(t: &Option<chrono::DateTime<chrono::Utc>>) -> Option<i64> {
    t.as_ref().map(|t| t.timestamp())
}

/// The `Invalid value:` rendering of a `*metav1.Time` (RFC3339, quoted).
fn time_bad_value(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

type UncountedUids<'a> = Option<(&'a [String], &'a [String])>;

/// `UncountedTerminatedPods` as upstream's `apiequality.Semantic.DeepEqual`
/// sees it: nil and empty slices are equal, a nil pointer is not an empty struct.
fn uncounted_normalized(job: &Job) -> UncountedUids<'_> {
    job.status
        .as_ref()
        .and_then(|s| s.uncounted_terminated_pods.as_ref())
        .map(|u| {
            (
                u.succeeded.as_deref().unwrap_or(&[]),
                u.failed.as_deref().unwrap_or(&[]),
            )
        })
}

/// Upstream `getStatusValidationOptions`
/// (`pkg/registry/batch/job/strategy.go:349-432`), the `JobManagedBy` branch
/// (feature GA and on by default in 1.35, `features.JobManagedBy`). Each rule
/// is switched on only when the field it guards CHANGED between `old_job` and
/// `new_job`, so an unrelated status write is not blocked by a status that
/// already violated a rule and the controller gets a sync to repair a status
/// a spec edit just invalidated (strategy.go:354-367).
pub fn get_status_validation_options(new_job: &Job, old_job: &Job) -> JobStatusValidationOptions {
    let new_status = new_job.status.clone().unwrap_or_default();
    let old_status = old_job.status.clone().unwrap_or_default();
    let is_indexed = new_job.spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION);

    let is_job_finished_changed = is_job_finished(old_job) != is_job_finished(new_job);
    let is_job_complete_changed = is_job_complete(old_job) != is_job_complete(new_job);
    let is_job_failed_changed = is_job_failed(old_job) != is_job_failed(new_job);
    let is_job_failure_target_changed =
        is_job_failure_target(old_job) != is_job_failure_target(new_job);
    let is_job_success_criteria_met_changed =
        is_job_success_criteria_met(old_job) != is_job_success_criteria_met(new_job);
    let is_completed_indexes_changed = old_status.completed_indexes.clone().unwrap_or_default()
        != new_status.completed_indexes.clone().unwrap_or_default();
    let is_failed_indexes_changed = old_status.failed_indexes != new_status.failed_indexes;
    let is_active_changed = old_status.active.unwrap_or(0) != new_status.active.unwrap_or(0);
    let is_start_time_changed =
        time_secs(&old_status.start_time) != time_secs(&new_status.start_time);
    let is_completion_time_changed =
        time_secs(&old_status.completion_time) != time_secs(&new_status.completion_time);
    let is_uncounted_terminated_pods_changed =
        uncounted_normalized(old_job) != uncounted_normalized(new_job);
    let is_ready_changed = old_status.ready != new_status.ready;
    let is_terminating_changed = old_status.terminating != new_status.terminating;
    let is_suspended_with_zero_completions =
        new_job.spec.suspend == Some(true) && new_job.spec.completions == Some(0);
    // strategy.go:372-376: resume detected via JobSuspended True -> False
    // (kubernetes/kubernetes#134521).
    let is_job_resuming = is_condition_true(job_conditions(old_job), "Suspended")
        && is_condition_false(job_conditions(new_job), "Suspended");

    JobStatusValidationOptions {
        // We allow to decrease the counter for succeeded pods for jobs which
        // have equal parallelism and completions, as they can be scaled-down.
        reject_decreasing_succeeded_counter: !is_indexed
            || new_job.spec.completions != new_job.spec.parallelism,
        reject_decreasing_failed_counter: true,
        reject_disabling_terminal_condition: true,
        reject_invalid_completed_indexes: is_completed_indexes_changed,
        reject_invalid_failed_indexes: is_failed_indexes_changed,
        reject_completed_indexes_for_non_indexed_job: is_completed_indexes_changed,
        reject_failed_indexes_for_no_backoff_limit_per_index: is_failed_indexes_changed,
        reject_failed_indexes_overlapping_completed: is_failed_indexes_changed
            || is_completed_indexes_changed,
        reject_failed_job_without_failure_target: is_job_failed_changed
            || is_failed_indexes_changed,
        reject_complete_job_without_success_criteria_met: is_job_complete_changed
            || is_job_success_criteria_met_changed,
        reject_finished_job_with_active_pods: is_job_finished_changed || is_active_changed,
        reject_finished_job_without_start_time: (is_job_finished_changed || is_start_time_changed)
            && !is_suspended_with_zero_completions,
        reject_finished_job_with_uncounted_terminated_pods: is_job_finished_changed
            || is_uncounted_terminated_pods_changed,
        reject_start_time_update_for_unsuspended_job: is_start_time_changed && !is_job_resuming,
        reject_completion_time_before_start_time: is_start_time_changed
            || is_completion_time_changed,
        reject_mutating_completion_time: true,
        reject_not_complete_job_with_completion_time: is_job_complete_changed
            || is_completion_time_changed,
        reject_complete_job_without_completion_time: is_job_complete_changed
            || is_completion_time_changed,
        reject_complete_job_with_failed_condition: is_job_complete_changed || is_job_failed_changed,
        reject_complete_job_with_failure_target_condition: is_job_complete_changed
            || is_job_failure_target_changed,
        allow_for_success_criteria_met_in_extended_scope: true,
        reject_more_ready_than_active_pods: is_ready_changed || is_active_changed,
        reject_finished_job_with_terminating_pods: is_job_finished_changed
            || is_terminating_changed,
    }
}

/// Upstream `validateFailedIndexesNotOverlapCompleted` (validation.go:968-1012).
fn validate_failed_indexes_not_overlap_completed(
    completed: &str,
    failed: &str,
    completions: i32,
) -> Result<(), String> {
    if completed.is_empty() || failed.is_empty() {
        return Ok(());
    }
    let completed_intervals: Vec<&str> = completed.split(',').collect();
    let failed_intervals: Vec<&str> = failed.split(',').collect();
    let (mut c_pos, mut f_pos) = (0usize, 0usize);
    let mut c = parse_index_interval(completed_intervals[c_pos], completions);
    let mut f = parse_index_interval(failed_intervals[f_pos], completions);
    while c_pos < completed_intervals.len() && f_pos < failed_intervals.len() {
        match (&c, &f) {
            (Err(_), _) => {
                // Failure to parse "completed" interval; the format check reports it.
                c_pos += 1;
                if c_pos < completed_intervals.len() {
                    c = parse_index_interval(completed_intervals[c_pos], completions);
                }
            }
            (_, Err(_)) => {
                f_pos += 1;
                if f_pos < failed_intervals.len() {
                    f = parse_index_interval(failed_intervals[f_pos], completions);
                }
            }
            (Ok((c_x, c_y)), Ok((f_x, f_y))) => {
                if c_x <= f_y && f_x <= c_y {
                    return Err(format!(
                        "failedIndexes and completedIndexes overlap at index: {}",
                        (*c_x).max(*f_x)
                    ));
                }
                // No overlap, move to the next one.
                if c_x <= f_x {
                    c_pos += 1;
                    if c_pos < completed_intervals.len() {
                        c = parse_index_interval(completed_intervals[c_pos], completions);
                    }
                } else {
                    f_pos += 1;
                    if f_pos < failed_intervals.len() {
                        f = parse_index_interval(failed_intervals[f_pos], completions);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Upstream `validateJobStatus` (validation.go:459-609).
fn validate_job_status(job: &Job, fld_path: &Path, opts: &JobStatusValidationOptions) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let status = job.status.clone().unwrap_or_default();
    let cond_err = |detail: &str| {
        Error::invalid(
            &fld_path.child("conditions"),
            BadValue::Omit,
            detail.to_string(),
        )
    };

    errs.extend(validate_nonnegative_field(
        status.active.unwrap_or(0) as i64,
        &fld_path.child("active"),
    ));
    errs.extend(validate_nonnegative_field(
        status.succeeded.unwrap_or(0) as i64,
        &fld_path.child("succeeded"),
    ));
    errs.extend(validate_nonnegative_field(
        status.failed.unwrap_or(0) as i64,
        &fld_path.child("failed"),
    ));
    if let Some(ready) = status.ready {
        errs.extend(validate_nonnegative_field(
            ready as i64,
            &fld_path.child("ready"),
        ));
    }
    if let Some(terminating) = status.terminating {
        errs.extend(validate_nonnegative_field(
            terminating as i64,
            &fld_path.child("terminating"),
        ));
    }
    if let Some(u) = &status.uncounted_terminated_pods {
        let path = fld_path.child("uncountedTerminatedPods");
        let mut seen: HashSet<&str> = HashSet::new();
        for (name, uids) in [("succeeded", &u.succeeded), ("failed", &u.failed)] {
            for (i, k) in uids.iter().flatten().enumerate() {
                let p = path.child(name).index(i);
                if k.is_empty() {
                    errs.push(Error::invalid(&p, k.clone(), "must not be empty"));
                } else if !seen.insert(k.as_str()) {
                    errs.push(Error::duplicate(&p, k.clone()));
                }
            }
        }
    }

    let complete = is_job_complete(job);
    let failed = is_job_failed(job);
    let conds = job_conditions(job);
    if opts.reject_complete_job_with_failed_condition && complete && failed {
        errs.push(cond_err(
            "cannot set Complete=True and Failed=true conditions",
        ));
    }
    if opts.reject_complete_job_with_failure_target_condition
        && complete
        && is_condition_true(conds, "FailureTarget")
    {
        errs.push(cond_err(
            "cannot set Complete=True and FailureTarget=true conditions",
        ));
    }
    if opts.reject_not_complete_job_with_completion_time && !complete {
        if let Some(t) = &status.completion_time {
            errs.push(Error::invalid(
                &fld_path.child("completionTime"),
                time_bad_value(t),
                "cannot set completionTime when there is no Complete=True condition",
            ));
        }
    }
    if opts.reject_complete_job_without_completion_time
        && status.completion_time.is_none()
        && complete
    {
        errs.push(Error::required(
            &fld_path.child("completionTime"),
            "completionTime is required for Complete jobs",
        ));
    }
    if opts.reject_completion_time_before_start_time {
        if let (Some(start), Some(completion)) = (&status.start_time, &status.completion_time) {
            if completion.timestamp() < start.timestamp() {
                errs.push(Error::invalid(
                    &fld_path.child("completionTime"),
                    time_bad_value(completion),
                    "must be equal to or after `startTime`",
                ));
            }
        }
    }
    if opts.reject_failed_job_without_failure_target && failed && !is_job_failure_target(job) {
        errs.push(cond_err(
            "cannot set Failed=True condition without the FailureTarget=true condition",
        ));
    }
    if opts.reject_complete_job_without_success_criteria_met
        && complete
        && !is_job_success_criteria_met(job)
    {
        errs.push(cond_err(
            "cannot set Complete=True condition without the SuccessCriteriaMet=true condition",
        ));
    }
    let finished = is_job_finished(job);
    if opts.reject_finished_job_with_active_pods && status.active.unwrap_or(0) > 0 && finished {
        errs.push(Error::invalid(
            &fld_path.child("active"),
            status.active.unwrap_or(0),
            "active>0 is invalid for finished job",
        ));
    }
    if opts.reject_finished_job_without_start_time && status.start_time.is_none() && finished {
        errs.push(Error::required(
            &fld_path.child("startTime"),
            "startTime is required for finished job",
        ));
    }
    if opts.reject_finished_job_with_uncounted_terminated_pods && finished {
        if let Some(u) = &status.uncounted_terminated_pods {
            let succeeded = u.succeeded.as_deref().unwrap_or(&[]);
            let failed_uids = u.failed.as_deref().unwrap_or(&[]);
            if !succeeded.is_empty() || !failed_uids.is_empty() {
                // Go's `omitempty` drops empty slices from the rendered struct.
                let mut obj = serde_json::Map::new();
                if !succeeded.is_empty() {
                    obj.insert("succeeded".into(), serde_json::json!(succeeded));
                }
                if !failed_uids.is_empty() {
                    obj.insert("failed".into(), serde_json::json!(failed_uids));
                }
                errs.push(Error::invalid(
                    &fld_path.child("uncountedTerminatedPods"),
                    serde_json::Value::Object(obj),
                    "must be empty for finished job",
                ));
            }
        }
    }
    let completed_indexes = status.completed_indexes.clone().unwrap_or_default();
    if opts.reject_invalid_completed_indexes {
        if let Some(completions) = job.spec.completions {
            if let Err(e) = validate_indexes_format(&completed_indexes, completions) {
                errs.push(Error::invalid(
                    &fld_path.child("completedIndexes"),
                    completed_indexes.clone(),
                    format!("error parsing completedIndexes: {e}"),
                ));
            }
        }
    }
    if opts.reject_invalid_failed_indexes {
        if let (Some(completions), Some(_), Some(failed_indexes)) = (
            job.spec.completions,
            job.spec.backoff_limit_per_index,
            &status.failed_indexes,
        ) {
            if let Err(e) = validate_indexes_format(failed_indexes, completions) {
                errs.push(Error::invalid(
                    &fld_path.child("failedIndexes"),
                    failed_indexes.clone(),
                    format!("error parsing failedIndexes: {e}"),
                ));
            }
        }
    }
    let is_indexed = job.spec.completion_mode.as_deref() == Some(INDEXED_COMPLETION);
    if opts.reject_completed_indexes_for_non_indexed_job
        && !completed_indexes.is_empty()
        && !is_indexed
    {
        errs.push(Error::invalid(
            &fld_path.child("completedIndexes"),
            completed_indexes.clone(),
            "cannot set non-empty completedIndexes when non-indexed completion mode",
        ));
    }
    if opts.reject_failed_indexes_for_no_backoff_limit_per_index
        && job.spec.backoff_limit_per_index.is_none()
    {
        // Also covers regular (non-indexed) jobs: backoffLimitPerIndex is nil.
        if let Some(failed_indexes) = &status.failed_indexes {
            errs.push(Error::invalid(
                &fld_path.child("failedIndexes"),
                failed_indexes.clone(),
                "cannot set non-null failedIndexes when backoffLimitPerIndex is null",
            ));
        }
    }
    if opts.reject_failed_indexes_overlapping_completed {
        if let (Some(completions), Some(failed_indexes)) =
            (job.spec.completions, &status.failed_indexes)
        {
            if let Err(e) = validate_failed_indexes_not_overlap_completed(
                &completed_indexes,
                failed_indexes,
                completions,
            ) {
                errs.push(Error::invalid(
                    &fld_path.child("failedIndexes"),
                    failed_indexes.clone(),
                    e,
                ));
            }
        }
    }
    if opts.reject_finished_job_with_terminating_pods && finished {
        if let Some(t) = status.terminating.filter(|t| *t > 0) {
            errs.push(Error::invalid(
                &fld_path.child("terminating"),
                t,
                "terminating>0 is invalid for finished job",
            ));
        }
    }
    if opts.reject_more_ready_than_active_pods {
        if let Some(ready) = status.ready {
            if ready > status.active.unwrap_or(0) {
                errs.push(Error::invalid(
                    &fld_path.child("ready"),
                    ready,
                    "cannot set more ready pods than active",
                ));
            }
        }
    }
    let success_criteria_met = is_job_success_criteria_met(job);
    if !opts.allow_for_success_criteria_met_in_extended_scope && !is_indexed && success_criteria_met
    {
        errs.push(cond_err("cannot set SuccessCriteriaMet to NonIndexed Job"));
    }
    if success_criteria_met && failed {
        errs.push(cond_err(
            "cannot set SuccessCriteriaMet=True and Failed=true conditions",
        ));
    }
    if success_criteria_met && is_job_failure_target(job) {
        errs.push(cond_err(
            "cannot set SuccessCriteriaMet=True and FailureTarget=true conditions",
        ));
    }
    if !opts.allow_for_success_criteria_met_in_extended_scope
        && job.spec.success_policy.is_none()
        && success_criteria_met
    {
        errs.push(cond_err(
            "cannot set SuccessCriteriaMet=True for Job without SuccessPolicy",
        ));
    }
    if job.spec.success_policy.is_some() && !success_criteria_met && complete {
        errs.push(cond_err(
            "cannot set Complete=True for Job with SuccessPolicy unless SuccessCriteriaMet=True",
        ));
    }
    errs
}

/// Upstream `ValidateJobStatusUpdate` (validation.go:710-754).
///
/// The Job controller recomputes the counters from the live pod list, so
/// without the decreasing-counter rules a write a real api-server refuses is
/// accepted here — which is how #1955 stayed invisible in-house and reddened
/// only the vanilla-swap leg. The error wording is the contract:
/// `status.failed: Invalid value: 0: cannot decrease the failed counter`.
pub fn validate_job_status_update(
    new_job: &Job,
    old_job: &Job,
    opts: &JobStatusValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let status_fld = Path::new("status");
    errs.extend(validate_job_status(new_job, &status_fld, opts));

    let new_status = new_job.status.clone().unwrap_or_default();
    let old_status = old_job.status.clone().unwrap_or_default();
    let cond_err = |detail: &str| {
        Error::invalid(
            &status_fld.child("conditions"),
            BadValue::Omit,
            detail.to_string(),
        )
    };

    if opts.reject_disabling_terminal_condition {
        for c_type in ["Failed", "Complete", "FailureTarget"] {
            if is_condition_true(job_conditions(old_job), c_type)
                && !is_condition_true(job_conditions(new_job), c_type)
            {
                errs.push(cond_err(&format!(
                    "cannot disable the terminal {c_type}=True condition"
                )));
            }
        }
    }
    let new_failed = new_status.failed.unwrap_or(0);
    if opts.reject_decreasing_failed_counter && new_failed < old_status.failed.unwrap_or(0) {
        errs.push(Error::invalid(
            &status_fld.child("failed"),
            new_failed,
            "cannot decrease the failed counter",
        ));
    }
    let new_succeeded = new_status.succeeded.unwrap_or(0);
    if opts.reject_decreasing_succeeded_counter && new_succeeded < old_status.succeeded.unwrap_or(0)
    {
        errs.push(Error::invalid(
            &status_fld.child("succeeded"),
            new_succeeded,
            "cannot decrease the succeeded counter",
        ));
    }
    if opts.reject_mutating_completion_time {
        // Only checked when the new completionTime is set, so a transition to
        // nil on an unfinished job is not blocked (validation.go:733-735).
        if let (Some(new_t), Some(old_t)) =
            (&new_status.completion_time, &old_status.completion_time)
        {
            if new_t.timestamp() != old_t.timestamp() {
                errs.push(Error::invalid(
                    &status_fld.child("completionTime"),
                    time_bad_value(new_t),
                    "field is immutable",
                ));
            }
        }
    }
    if opts.reject_start_time_update_for_unsuspended_job
        && old_status.start_time.is_some()
        && time_secs(&old_status.start_time) != time_secs(&new_status.start_time)
        && !new_job.spec.suspend.unwrap_or(false)
    {
        errs.push(Error::required(
            &status_fld.child("startTime"),
            "startTime cannot be removed for unsuspended job",
        ));
    }
    if is_job_success_criteria_met(old_job) && !is_job_success_criteria_met(new_job) {
        errs.push(cond_err(
            "cannot disable the SuccessCriteriaMet=True condition",
        ));
    }
    if is_job_complete(old_job)
        && !is_job_success_criteria_met(old_job)
        && is_job_success_criteria_met(new_job)
    {
        errs.push(cond_err(
            "cannot set SuccessCriteriaMet=True for Job already has Complete=true conditions",
        ));
    }
    errs
}

#[cfg(test)]
mod success_policy_indexes_tests {
    use super::{parse_index_interval, validate_indexes_format};

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

#[cfg(test)]
mod parity_tests {
    /// The create path's options: `RequirePrefixedLabels: true`.
    fn validate_job(job: &Job) -> ErrorList {
        super::validate_job(
            job,
            &JobValidationOptions {
                require_prefixed_labels: true,
                ..Default::default()
            },
        )
    }
    use super::*;
    use crate::resources::pod::{Container, PodSpec};
    use crate::resources::workloads::{
        Job, JobSpec, PodFailurePolicy, PodFailurePolicyOnPodConditionsPattern,
        PodFailurePolicyRule, PodTemplateSpec,
    };
    use crate::types::{LabelSelector, LabelSelectorRequirement, ObjectMeta, TypeMeta};
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
        use crate::resources::pod::{
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
#[cfg(test)]
mod status_validation_tests {
    use super::*;
    use crate::resources::workloads::{JobStatus, UncountedTerminatedPods};
    use crate::validation::objectmeta::IS_NEGATIVE_ERROR_MSG;
    use chrono::{DateTime, TimeZone, Utc};

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
            crate::validation::field::ErrorType::Duplicate
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
