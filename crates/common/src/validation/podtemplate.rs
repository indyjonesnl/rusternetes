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

use crate::resources::pod::{PodSpec, Toleration};
use crate::resources::workloads::{PodTemplate, PodTemplateSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::validate_labels;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_annotations, validate_object_meta, validate_object_meta_update,
};
use crate::validation::pod::{
    allow_taint_toleration_comparison_operators, validate_local_descending_path, validate_pod_spec,
    validate_tolerations_with_options,
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
    errs.extend(validate_pod_template_spec(
        &pt.template,
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
    errs.extend(validate_pod_template_spec(
        &pt.template,
        &Path::new("template"),
        false,
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

    errs.extend(validate_pod_specific_annotations(
        template
            .metadata
            .as_ref()
            .and_then(|m| m.annotations.as_ref()),
        &template.spec,
        &fld_path.child("annotations"),
    ));

    // Pod spec (also forbids ephemeral containers — upstream forbids them in a
    // pod template).
    errs.extend(validate_pod_spec(
        &template.spec,
        &fld_path.child("spec"),
        allow_relaxed_dns_search,
        crate::validation::pod::allow_taint_toleration_comparison_operators(None),
    ));

    errs
}

/// `ValidatePodSpecificAnnotations`
/// (`pkg/apis/core/validation/validation.go:193-216`), run on a template's
/// annotations by `ValidatePodTemplateSpec` (`:7070`) and
/// `ValidatePodTemplateSpecForStatefulSet`
/// (`pkg/apis/apps/validation/validation.go:76`).
///
/// `opts.AllowInvalidPodDeletionCost` is `false` here: `PodDeletionCost` is
/// beta and on by default in 1.35 (`pkg/api/pod/util.go:414`). Upstream's
/// update-time leniency (`:488-491`, an already-invalid old object) is not
/// ported. Map iteration is sorted for a deterministic error order.
pub fn validate_pod_specific_annotations(
    annotations: Option<&HashMap<String, String>>,
    spec: &PodSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(annotations) = annotations else {
        return errs;
    };

    if let Some(value) = annotations.get(MIRROR_POD_ANNOTATION_KEY) {
        if spec.node_name.as_deref().unwrap_or("").is_empty() {
            errs.push(Error::invalid(
                &fld_path.key(MIRROR_POD_ANNOTATION_KEY),
                value.clone(),
                "must set spec.nodeName if mirror pod annotation is set",
            ));
        }
    }

    if annotations
        .get(TOLERATIONS_ANNOTATION_KEY)
        .is_some_and(|v| !v.is_empty())
    {
        errs.extend(validate_tolerations_in_pod_annotations(
            annotations,
            fld_path,
        ));
    }

    if let Some(value) = annotations.get(POD_DELETION_COST) {
        if !is_valid_deletion_cost(value) {
            errs.push(Error::invalid(
                &fld_path.key(POD_DELETION_COST),
                value.clone(),
                "must be a 32bit integer",
            ));
        }
    }

    let mut keys: Vec<&String> = annotations.keys().collect();
    keys.sort();

    // ValidateSeccompPodAnnotations (validation.go:5281-5293).
    if let Some(p) = annotations.get(SECCOMP_POD_ANNOTATION_KEY) {
        errs.extend(validate_seccomp_annotation_profile(
            p,
            &fld_path.child(SECCOMP_POD_ANNOTATION_KEY),
        ));
    }
    for k in &keys {
        if k.starts_with(SECCOMP_CONTAINER_ANNOTATION_KEY_PREFIX) {
            errs.extend(validate_seccomp_annotation_profile(
                &annotations[*k],
                &fld_path.child(k.as_str()),
            ));
        }
    }

    // ValidateAppArmorPodAnnotations (validation.go:5349-5367).
    for k in &keys {
        let Some(container_name) = k.strip_prefix(APPARMOR_BETA_CONTAINER_ANNOTATION_KEY_PREFIX)
        else {
            continue;
        };
        let p = &annotations[*k];
        if !pod_spec_has_container(spec, container_name) {
            errs.push(Error::invalid(
                &fld_path.key(k.as_str()),
                container_name.to_string(),
                "container not found",
            ));
        }
        if !(p.is_empty()
            || p == "runtime/default"
            || p == "unconfined"
            || p.starts_with("localhost/"))
        {
            errs.push(Error::invalid(
                &fld_path.key(k.as_str()),
                p.clone(),
                format!("invalid AppArmor profile name: {p:?}"),
            ));
        }
    }

    errs
}

/// `core.MirrorPodAnnotationKey`, `TolerationsAnnotationKey`,
/// `SeccompPodAnnotationKey`, `SeccompContainerAnnotationKeyPrefix`,
/// `PodDeletionCost` (`pkg/apis/core/annotation_key_constants.go:27,31,40,45,136`)
/// and `v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix`
/// (`staging/src/k8s.io/api/core/v1/annotation_key_constants.go:59`).
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";
const TOLERATIONS_ANNOTATION_KEY: &str = "scheduler.alpha.kubernetes.io/tolerations";
const SECCOMP_POD_ANNOTATION_KEY: &str = "seccomp.security.alpha.kubernetes.io/pod";
const SECCOMP_CONTAINER_ANNOTATION_KEY_PREFIX: &str =
    "container.seccomp.security.alpha.kubernetes.io/";
const POD_DELETION_COST: &str = "controller.kubernetes.io/pod-deletion-cost";
const APPARMOR_BETA_CONTAINER_ANNOTATION_KEY_PREFIX: &str =
    "container.apparmor.security.beta.kubernetes.io/";

/// `ValidateTolerationsInPodAnnotations` (validation.go:219-233) over
/// `GetTolerationsFromPodAnnotations` (helpers.go:398-407).
fn validate_tolerations_in_pod_annotations(
    annotations: &HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let raw = &annotations[TOLERATIONS_ANNOTATION_KEY];
    match serde_json::from_str::<Vec<Toleration>>(raw) {
        Err(e) => vec![Error::invalid(
            fld_path,
            TOLERATIONS_ANNOTATION_KEY.to_string(),
            e.to_string(),
        )],
        Ok(t) if t.is_empty() => Vec::new(),
        Ok(t) => validate_tolerations_with_options(
            &t,
            &fld_path.child(TOLERATIONS_ANNOTATION_KEY),
            allow_taint_toleration_comparison_operators(None),
        ),
    }
}

/// `GetDeletionCostFromPodAnnotations` (helpers.go:491-513): the first byte
/// must be `-`, a lone `0`, or `1-9`, and the value an `int32`.
fn is_valid_deletion_cost(value: &str) -> bool {
    let first_ok = match value.as_bytes().first() {
        None => false,
        Some(b'-') => true,
        Some(b'0') => value == "0",
        Some(c) => (b'1'..=b'9').contains(c),
    };
    first_ok && value.parse::<i32>().is_ok()
}

/// `ValidateSeccompProfile` (validation.go:5268-5279).
fn validate_seccomp_annotation_profile(p: &str, fld_path: &Path) -> ErrorList {
    if p == "runtime/default" || p == "docker/default" || p == "unconfined" {
        return Vec::new();
    }
    if let Some(rest) = p.strip_prefix("localhost/") {
        return validate_local_descending_path(rest, fld_path);
    }
    vec![Error::invalid(
        fld_path,
        p.to_string(),
        "must be a valid seccomp profile",
    )]
}

/// `podSpecHasContainer` (validation.go:5430-5440): `VisitContainersWithPath`
/// visits init, regular and ephemeral containers.
fn pod_spec_has_container(spec: &PodSpec, name: &str) -> bool {
    spec.containers.iter().any(|c| c.name == name)
        || spec
            .init_containers
            .iter()
            .flatten()
            .any(|c| c.name == name)
        || spec
            .ephemeral_containers
            .iter()
            .flatten()
            .any(|c| c.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(a: &[(&str, &str)], s: &PodSpec) -> Vec<String> {
        let m: HashMap<String, String> = a
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        validate_pod_specific_annotations(Some(&m), s, &Path::new("a"))
            .iter()
            .map(|e| e.error_body())
            .collect()
    }

    fn web() -> PodSpec {
        serde_json::from_value(serde_json::json!({"containers":[{"name":"web","image":"x"}]}))
            .unwrap()
    }

    #[test]
    fn mirror_annotation_requires_node_name() {
        let errs = run(&[("kubernetes.io/config.mirror", "h")], &web());
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("must set spec.nodeName if mirror pod annotation is set"));
        let s: PodSpec = serde_json::from_value(
            serde_json::json!({"nodeName":"n","containers":[{"name":"web","image":"x"}]}),
        )
        .unwrap();
        assert!(run(&[("kubernetes.io/config.mirror", "h")], &s).is_empty());
    }

    #[test]
    fn deletion_cost_must_be_canonical_int32() {
        let k = "controller.kubernetes.io/pod-deletion-cost";
        for ok in ["0", "-5", "10", "2147483647"] {
            assert!(run(&[(k, ok)], &web()).is_empty(), "{ok}");
        }
        for bad in ["", "+10", "008", "2147483648", "x", "-"] {
            let errs = run(&[(k, bad)], &web());
            assert_eq!(errs.len(), 1, "{bad}: {errs:?}");
            assert!(errs[0].contains("must be a 32bit integer"));
        }
    }

    #[test]
    fn seccomp_annotations_are_validated() {
        let pod = "seccomp.security.alpha.kubernetes.io/pod";
        assert!(run(&[(pod, "runtime/default")], &web()).is_empty());
        assert!(run(&[(pod, "localhost/p.json")], &web()).is_empty());
        let errs = run(&[(pod, "bogus")], &web());
        assert!(
            errs[0].contains("must be a valid seccomp profile"),
            "{errs:?}"
        );
        let errs = run(
            &[(
                "container.seccomp.security.alpha.kubernetes.io/web",
                "localhost/../x",
            )],
            &web(),
        );
        assert!(errs[0].contains("must not contain '..'"), "{errs:?}");
    }

    #[test]
    fn apparmor_annotations_need_a_known_container_and_profile() {
        let k = "container.apparmor.security.beta.kubernetes.io/web";
        assert!(run(&[(k, "runtime/default")], &web()).is_empty());
        let errs = run(&[(k, "bogus")], &web());
        assert!(
            errs[0].contains("invalid AppArmor profile name"),
            "{errs:?}"
        );
        let errs = run(
            &[(
                "container.apparmor.security.beta.kubernetes.io/nope",
                "unconfined",
            )],
            &web(),
        );
        assert!(errs[0].contains("container not found"), "{errs:?}");
    }

    #[test]
    fn tolerations_annotation_is_parsed_and_validated() {
        let k = "scheduler.alpha.kubernetes.io/tolerations";
        let good = r#"[{"key":"a","operator":"Exists","effect":"NoSchedule"}]"#;
        assert!(run(&[(k, good)], &web()).is_empty());
        assert_eq!(run(&[(k, "not json")], &web()).len(), 1);
        assert!(!run(&[(k, r#"[{"key":"a","operator":"Bogus"}]"#)], &web()).is_empty());
    }

    #[test]
    fn template_spec_runs_the_annotation_checks() {
        let t: PodTemplateSpec = serde_json::from_value(serde_json::json!({
            "metadata":{"annotations":{"kubernetes.io/config.mirror":"h"}},
            "spec":{"containers":[{"name":"web","image":"x"}]}
        }))
        .unwrap();
        let errs = validate_pod_template_spec(&t, &Path::new("template"), false);
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("mirror pod annotation")),
            "{errs:?}"
        );
    }
}
