//! PodTemplate validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePodTemplate` /
//! `ValidatePodTemplateSpec` (release-1.35).
//!
//! Validates the embedded `template`: its labels, annotations, and the pod spec
//! (reusing the shared [`validate_pod_spec`], which also forbids ephemeral
//! containers on create — upstream forbids them in a pod template too).
//! `ValidatePodTemplate` / `ValidatePodTemplateUpdate`
//! (`pkg/apis/core/validation/validation.go:6543-6555`) also validate the
//! PodTemplate's own ObjectMeta.

use std::collections::HashMap;

use crate::resources::workloads::{PodTemplate, PodTemplateSpec};
use crate::validation::field::{ErrorList, Path};
use crate::validation::metav1::validate_labels;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_annotations, validate_object_meta, validate_object_meta_update,
};
use crate::validation::pod::validate_pod_spec_with_options;
use crate::validation::pod_options::get_validation_options_from_pod_template;
use crate::validation::pod_status::{
    get_deletion_cost_from_pod_annotations, validate_pod_specific_annotations,
};

/// Upstream `ValidatePodTemplate` (validation.go:6543-6547). The name is
/// `ValidatePodName`, which is `NameIsDNSSubdomain`.
pub fn validate_pod_template(pt: &PodTemplate) -> ErrorList {
    let mut errs = validate_object_meta(
        &pt.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    // `GetValidationOptionsFromPodTemplate(&template.Template, nil)`
    // (pkg/registry/core/podtemplate/strategy.go:57).
    errs.extend(validate_pod_template_spec_with_old(
        &pt.template,
        None,
        &Path::new("template"),
        false,
    ));
    errs
}

/// Upstream `ValidatePodTemplateUpdate` (validation.go:6551-6555): the
/// metadata update rules, and the template validated as on create. Unlike a
/// workload's, a standalone template is mutable.
pub fn validate_pod_template_update(pt: &PodTemplate, old: &PodTemplate) -> ErrorList {
    let mut errs = validate_object_meta_update(&pt.metadata, &old.metadata, &Path::new("metadata"));
    // `GetValidationOptionsFromPodTemplate(&template.Template, &oldTemplate.Template)`
    // (pkg/api/pod/util.go:489-492): an already-invalid old deletion cost is
    // tolerated on update.
    let empty = HashMap::new();
    let allow_invalid_deletion_cost = get_deletion_cost_from_pod_annotations(
        old.template
            .metadata
            .as_ref()
            .and_then(|m| m.annotations.as_ref())
            .unwrap_or(&empty),
    )
    .is_err();
    // `GetValidationOptionsFromPodTemplate(&template.Template,
    // &oldTemplate.Template)` (pkg/registry/core/podtemplate/strategy.go:97).
    errs.extend(validate_pod_template_spec_opts(
        &pt.template,
        Some(&old.template),
        &Path::new("template"),
        false,
        allow_invalid_deletion_cost,
    ));
    errs
}

/// Validate an embedded pod template. Port of upstream
/// `ValidatePodTemplateSpec` (`pkg/apis/core/validation/validation.go:7066-7073`):
///
/// ```text
/// ValidateLabels(spec.Labels, fldPath.Child("labels"))
/// ValidateAnnotations(spec.Annotations, fldPath.Child("annotations"))
/// ValidatePodSpecificAnnotations(...)
/// ValidatePodSpec(&spec.Spec, nil, fldPath.Child("spec"), opts)
/// ```
///
/// Every workload validator upstream calls this on its own `spec.template` —
/// `ValidateDeploymentSpec` (`pkg/apis/apps/validation/validation.go:656` via
/// `ValidatePodTemplateSpecForReplicaSet`), `ValidateDaemonSetSpec` (`:454`),
/// `ValidateStatefulSetSpec` (`:214`), `ValidateJobSpec`
/// (`pkg/apis/batch/validation/validation.go:276`) — so a workload's template
/// is held to exactly the same rules as a standalone `PodTemplate` or a `Pod`.
pub fn validate_pod_template_spec(
    template: &PodTemplateSpec,
    fld_path: &Path,
    allow_relaxed_dns_search: bool,
) -> ErrorList {
    validate_pod_template_spec_opts(template, None, fld_path, allow_relaxed_dns_search, false)
}

/// [`validate_pod_template_spec`] with the options derived from the old
/// template (`GetValidationOptionsFromPodTemplate(template, oldTemplate)`,
/// pkg/api/pod/util.go:659), so an already-stored template stays updatable.
pub fn validate_pod_template_spec_with_old(
    template: &PodTemplateSpec,
    old: Option<&PodTemplateSpec>,
    fld_path: &Path,
    allow_relaxed_dns_search: bool,
) -> ErrorList {
    validate_pod_template_spec_opts(template, old, fld_path, allow_relaxed_dns_search, false)
}

fn validate_pod_template_spec_opts(
    template: &PodTemplateSpec,
    old: Option<&PodTemplateSpec>,
    fld_path: &Path,
    allow_relaxed_dns_search: bool,
    allow_invalid_pod_deletion_cost: bool,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(meta) = &template.metadata {
        if let Some(labels) = &meta.labels {
            errs.extend(validate_labels(labels, &fld_path.child("labels")));
        }
        if let Some(annotations) = &meta.annotations {
            errs.extend(validate_annotations(
                annotations,
                &fld_path.child("annotations"),
            ));
        }
    }

    // ONE implementation, shared with the Pod path (#2464).
    let empty = HashMap::new();
    errs.extend(validate_pod_specific_annotations(
        template
            .metadata
            .as_ref()
            .and_then(|m| m.annotations.as_ref())
            .unwrap_or(&empty),
        &template.spec,
        &fld_path.child("annotations"),
        allow_invalid_pod_deletion_cost,
    ));

    // Pod spec (also forbids ephemeral containers — upstream forbids them in a
    // pod template).
    let mut opts = get_validation_options_from_pod_template(Some(template), old);
    opts.allow_relaxed_dns_search_validation |= allow_relaxed_dns_search;
    errs.extend(validate_pod_spec_with_options(
        &template.spec,
        &fld_path.child("spec"),
        &opts,
    ));

    errs
}
