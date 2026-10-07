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
use crate::validation::pod::validate_pod_spec;
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
    errs.extend(validate_pod_template_spec_opts(
        &pt.template,
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
    validate_pod_template_spec_opts(template, fld_path, allow_relaxed_dns_search, false)
}

fn validate_pod_template_spec_opts(
    template: &PodTemplateSpec,
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
    errs.extend(validate_pod_spec(
        &template.spec,
        &fld_path.child("spec"),
        allow_relaxed_dns_search,
        crate::validation::pod::allow_taint_toleration_comparison_operators(None),
    ));

    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::pod::PodSpec;
    use crate::validation::pod_status::{
        MIRROR_POD_ANNOTATION_KEY, POD_DELETION_COST, TOLERATIONS_ANNOTATION_KEY,
    };

    fn run(a: &[(&str, &str)], s: &PodSpec) -> Vec<String> {
        let m: HashMap<String, String> = a
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        validate_pod_specific_annotations(&m, s, &Path::new("a"), false)
            .iter()
            .map(|e| e.error_body())
            .collect()
    }

    fn web() -> PodSpec {
        serde_json::from_value(serde_json::json!({"containers":[{"name":"web","image":"x"}]}))
            .unwrap()
    }

    /// The call site, not just the helper: the template path reaches the one
    /// shared `validate_pod_specific_annotations` (#2464).
    fn tpl_errs(a: &[(&str, &str)], spec: serde_json::Value) -> Vec<String> {
        let t: PodTemplateSpec = serde_json::from_value(serde_json::json!({
            "metadata": {"annotations": a.iter().cloned().collect::<HashMap<_, _>>()},
            "spec": spec,
        }))
        .unwrap();
        validate_pod_template_spec(&t, &Path::new("template"), false)
            .iter()
            .map(|e| e.to_string())
            .collect()
    }

    fn ctr_spec() -> serde_json::Value {
        serde_json::json!({"containers":[{"name":"ctr","image":"x"}],
            "initContainers":[{"name":"init-ctr","image":"x"}]})
    }

    /// `json.Unmarshal` of `null` into a slice is not an error
    /// (`GetTolerationsFromPodAnnotations`, helpers.go:398-407).
    #[test]
    fn null_tolerations_annotation_is_valid() {
        let errs = tpl_errs(&[(TOLERATIONS_ANNOTATION_KEY, "null")], ctr_spec());
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// validation_test.go:12978-12993 (`invalid pod-deletion-cost` cases).
    #[test]
    fn upstream_deletion_cost_cases() {
        for bad in ["text", "008", "+10"] {
            let errs = tpl_errs(&[(POD_DELETION_COST, bad)], ctr_spec());
            let want = format!(
                "template.annotations[controller.kubernetes.io/pod-deletion-cost]: Invalid value: \"{bad}\": must be a 32bit integer"
            );
            assert!(errs.iter().any(|e| e.contains(&want)), "{bad}: {errs:?}");
        }
        for ok in ["-100", "100"] {
            assert!(tpl_errs(&[(POD_DELETION_COST, ok)], ctr_spec()).is_empty());
        }
    }

    /// validation_test.go:12337-12364 (AppArmor annotation cases).
    #[test]
    fn upstream_apparmor_annotation_cases() {
        let p = "container.apparmor.security.beta.kubernetes.io/";
        let errs = tpl_errs(
            &[
                (&format!("{p}ctr"), "runtime/default"),
                (&format!("{p}init-ctr"), "runtime/default"),
                (&format!("{p}fake-ctr"), "runtime/default"),
            ],
            ctr_spec(),
        );
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains(
            "template.annotations[container.apparmor.security.beta.kubernetes.io/fake-ctr]"
        ));
        for bad in ["bad-name", "runtime/foo"] {
            let errs = tpl_errs(&[(&format!("{p}ctr"), bad)], ctr_spec());
            assert!(
                errs.iter()
                    .any(|e| e.contains("invalid AppArmor profile name")),
                "{bad}: {errs:?}"
            );
        }
    }

    /// `TestValidateAppArmorProfileFormat` (validation_test.go:26775-26798).
    #[test]
    fn upstream_apparmor_profile_format_cases() {
        let k = "container.apparmor.security.beta.kubernetes.io/ctr";
        for (profile, valid) in [
            ("", true),
            ("runtime/default", true),
            ("unconfined", true),
            ("baz", false),
            ("localhost//usr/sbin/ntpd", true),
            ("localhost/foo-bar", true),
        ] {
            let errs = tpl_errs(&[(k, profile)], ctr_spec());
            assert_eq!(errs.is_empty(), valid, "{profile:?}: {errs:?}");
        }
    }

    /// validation_test.go:12600-12611: presence is enough, even with "".
    #[test]
    fn upstream_mirror_cases() {
        for v in ["", "foo"] {
            let errs = tpl_errs(&[(MIRROR_POD_ANNOTATION_KEY, v)], ctr_spec());
            assert!(errs.iter().any(|e| e.contains("mirror")), "{v:?}: {errs:?}");
        }
    }

    /// The error order is deterministic (sorted keys), unlike Go's map walk.
    #[test]
    fn container_annotation_errors_are_ordered() {
        let p = "container.apparmor.security.beta.kubernetes.io/";
        let pairs: Vec<(String, &str)> = ["d", "b", "c", "a"]
            .iter()
            .map(|n| (format!("{p}{n}"), "bad"))
            .collect();
        let refs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let first = tpl_errs(&refs, ctr_spec());
        for _ in 0..20 {
            assert_eq!(first, tpl_errs(&refs, ctr_spec()));
        }
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

#[cfg(test)]
mod update_leniency_tests {
    use super::*;

    fn tpl(cost: &str) -> PodTemplate {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "t", "namespace": "d", "resourceVersion": "1"},
            "template": {
                "metadata": {"annotations": {"controller.kubernetes.io/pod-deletion-cost": cost}},
                "spec": {"containers": [{"name": "c", "image": "x"}]}
            }
        }))
        .unwrap()
    }

    /// util.go:489-492 on a PodTemplate update (#2351).
    #[test]
    fn update_tolerates_an_already_invalid_deletion_cost() {
        assert!(!validate_pod_template_update(&tpl("+1"), &tpl("1")).is_empty());
        assert!(validate_pod_template_update(&tpl("+1"), &tpl("+2")).is_empty());
        assert!(!validate_pod_template(&tpl("+1")).is_empty());
    }
}
