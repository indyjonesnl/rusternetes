//! Unit tests moved from `src/validation/podtemplate.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::workloads::{PodTemplate, PodTemplateSpec};
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::metav1::validate_labels;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_annotations, validate_object_meta,
        validate_object_meta_update,
    };
    pub use rusternetes_common::validation::pod::validate_pod_spec_with_options;
    pub use rusternetes_common::validation::pod_options::get_validation_options_from_pod_template;
    pub use rusternetes_common::validation::pod_status::{
        get_deletion_cost_from_pod_annotations, validate_pod_specific_annotations,
    };
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::pod::PodSpec;
    use rusternetes_common::validation::pod_status::{
        MIRROR_POD_ANNOTATION_KEY, POD_DELETION_COST, TOLERATIONS_ANNOTATION_KEY,
    };
    use rusternetes_common::validation::podtemplate::*;

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

mod update_leniency_tests {
    use super::__parent::*;
    use rusternetes_common::validation::podtemplate::*;

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

/// `GetValidationOptionsFromPodTemplate(new, old)` (pkg/api/pod/util.go:659)
/// reaches the template's pod spec on update: an old template that already
/// uses a relaxed DNS search path keeps the new one updatable
/// (`useRelaxedDNSSearchValidation(oldPodSpec)`, util.go:439), while a create
/// of the same template stays strict.
mod old_template_options_tests {
    use super::__parent::*;
    use rusternetes_common::validation::podtemplate::*;

    fn tpl(search: &str) -> PodTemplate {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "t", "namespace": "d", "resourceVersion": "1"},
            "template": {"spec": {
                "containers": [{"name": "c", "image": "x"}],
                "dnsConfig": {"searches": [search]}
            }}
        }))
        .unwrap()
    }

    #[test]
    #[serial_test::serial]
    fn update_derives_pod_options_from_the_old_template() {
        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::RelaxedDNSSearchValidation,
            false,
        );
        let relaxed = "my_search.example.com";
        assert!(!validate_pod_template(&tpl(relaxed)).is_empty());
        assert!(validate_pod_template_update(&tpl(relaxed), &tpl(relaxed)).is_empty());
        // A strict old template does not relax the new one.
        assert!(!validate_pod_template_update(&tpl(relaxed), &tpl("a.example.com")).is_empty());
    }
}
