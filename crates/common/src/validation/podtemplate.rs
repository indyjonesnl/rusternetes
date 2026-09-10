//! PodTemplate validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePodTemplate` /
//! `ValidatePodTemplateSpec` (release-1.35).
//!
//! Validates the embedded `template`: its labels, annotations, and the pod spec
//! (reusing the shared [`validate_pod_spec`], which also forbids ephemeral
//! containers on create — upstream forbids them in a pod template too).
//! ObjectMeta of the PodTemplate itself is validated separately (#1087 / #1277).

use crate::resources::workloads::{PodTemplate, PodTemplateSpec};
use crate::validation::field::{ErrorList, Path};
use crate::validation::metav1::validate_labels;
use crate::validation::objectmeta::validate_annotations;
use crate::validation::pod::validate_pod_spec;

/// Validate a `PodTemplate` on create. Mirrors upstream `ValidatePodTemplate`
/// minus the PodTemplate's own ObjectMeta.
pub fn validate_pod_template(pt: &PodTemplate) -> ErrorList {
    validate_pod_template_spec(&pt.template, &Path::new("template"), false)
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

    // Pod spec (also forbids ephemeral containers — upstream forbids them in a
    // pod template).
    errs.extend(validate_pod_spec(
        &template.spec,
        &fld_path.child("spec"),
        allow_relaxed_dns_search,
    ));

    errs
}
