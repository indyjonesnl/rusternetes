//! Unit tests moved from `src/validation/pod_status.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::resources::pod::{
        ContainerRestartRule, ContainerState, ContainerStateTerminated, ContainerStatus,
        PodCondition, PodIP,
    };
    pub use rusternetes_common::resources::{Binding, Container, Pod, PodSpec};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::is_qualified_name;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_immutable_field, validate_object_meta_update,
    };
    pub use rusternetes_common::validation::pod_options::{
        get_validation_options_from_pod_spec_and_meta, PodValidationOptions,
    };
    pub use rusternetes_common::validation::service::{
        is_valid_ip_for_legacy_field, parse_ip_sloppy,
    };
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::pod_status::*;

    fn pod(extra_spec: serde_json::Value, status: serde_json::Value) -> Pod {
        let mut body = serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]},
            "status": status
        });
        for (k, v) in extra_spec.as_object().unwrap() {
            body["spec"][k] = v.clone();
        }
        serde_json::from_value(body).unwrap()
    }

    fn messages(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[test]
    fn binding_target_follows_validate_pod_binding() {
        let mut b = Binding::new("p", "ns", Default::default());
        assert!(messages(&validate_pod_binding(&b)).contains("target.name"));
        b.target.name = "n".into();
        assert!(validate_pod_binding(&b).is_empty());
        b.target.kind = Some("Pod".into());
        assert!(messages(&validate_pod_binding(&b)).contains("target.kind"));
        b.target.kind = Some("Node".into());
        assert!(validate_pod_binding(&b).is_empty());
    }

    #[test]
    fn container_should_restart_follows_rules_then_container_then_pod() {
        let spec: PodSpec = serde_json::from_value(serde_json::json!({
            "restartPolicy": "Never",
            "containers": [{"name": "c", "image": "i"}]
        }))
        .unwrap();
        let mut c = spec.containers[0].clone();
        assert!(!container_should_restart(&c, &spec, 1));
        c.restart_policy = Some("OnFailure".into());
        assert!(container_should_restart(&c, &spec, 1));
        assert!(!container_should_restart(&c, &spec, 0));
        c.restart_policy = Some("Never".into());
        c.restart_policy_rules = serde_json::from_value(serde_json::json!([
            {"action": "Restart", "exitCodes": {"operator": "In", "values": [42]}}
        ]))
        .unwrap();
        assert!(container_should_restart(&c, &spec, 42));
        assert!(!container_should_restart(&c, &spec, 1));
    }

    #[test]
    fn ephemeral_containers_never_leave_terminated() {
        let terminated: Vec<ContainerStatus> = serde_json::from_value(serde_json::json!([
            {"name": "e", "state": {"terminated": {"exitCode": 0}}}
        ]))
        .unwrap();
        let running: Vec<ContainerStatus> = serde_json::from_value(serde_json::json!([
            {"name": "e", "state": {"running": {}}}
        ]))
        .unwrap();
        let errs = validate_ephemeral_container_state_transition(
            &running,
            &terminated,
            &Path::new("status").child("ephemeralContainerStatuses"),
        );
        assert_eq!(errs.len(), 1);
    }

    #[test]
    fn a_new_invalid_pod_ip_is_refused_and_an_old_one_is_kept() {
        let old = pod(serde_json::json!({}), serde_json::json!({}));
        let bad = pod(
            serde_json::json!({}),
            serde_json::json!({"podIPs": [{"ip": "bad"}]}),
        );
        assert!(!validate_pod_ips(&bad, &old).is_empty());
        assert!(validate_pod_ips(&bad, &bad).is_empty());
    }

    fn annotations_path() -> Path {
        Path::new("metadata").child("annotations")
    }

    fn ann(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// validation.go:196-200: a mirror pod must name its node.
    #[test]
    fn mirror_pod_annotation_needs_node_name() {
        let a = ann(&[(MIRROR_POD_ANNOTATION_KEY, "x")]);
        let errs =
            validate_pod_specific_annotations(&a, &PodSpec::default(), &annotations_path(), false);
        assert!(
            messages(&errs).contains("must set spec.nodeName if mirror pod annotation is set"),
            "{errs:?}"
        );
    }

    /// validation.go:206-210: `must be a 32bit integer`, with the helper's
    /// canonical-form rule (helper/helpers.go:508-513).
    #[test]
    fn pod_deletion_cost_must_be_a_32bit_integer() {
        let spec = PodSpec::default();
        for bad in ["+10", "008", "", "abc", "2147483648"] {
            let a = ann(&[(POD_DELETION_COST, bad)]);
            let errs = validate_pod_specific_annotations(&a, &spec, &annotations_path(), false);
            assert!(messages(&errs).contains("must be a 32bit integer"), "{bad}");
            assert!(
                validate_pod_specific_annotations(&a, &spec, &annotations_path(), true).is_empty()
            );
        }
        for good in ["0", "-5", "2147483647"] {
            let a = ann(&[(POD_DELETION_COST, good)]);
            assert!(
                validate_pod_specific_annotations(&a, &spec, &annotations_path(), false).is_empty()
            );
        }
    }

    /// validation.go:5268-5293.
    #[test]
    fn seccomp_annotations_must_name_a_valid_profile() {
        let spec = PodSpec::default();
        let a = ann(&[
            ("seccomp.security.alpha.kubernetes.io/pod", "bogus"),
            (
                "container.seccomp.security.alpha.kubernetes.io/c",
                "localhost/../x",
            ),
        ]);
        let m = messages(&validate_pod_specific_annotations(
            &a,
            &spec,
            &annotations_path(),
            false,
        ));
        assert!(m.contains("must be a valid seccomp profile"), "{m}");
        assert!(m.contains("must not contain '..'"), "{m}");
        for ok in [
            "runtime/default",
            "docker/default",
            "unconfined",
            "localhost/p",
        ] {
            let a = ann(&[("seccomp.security.alpha.kubernetes.io/pod", ok)]);
            assert!(
                validate_pod_specific_annotations(&a, &spec, &annotations_path(), false).is_empty(),
                "{ok}"
            );
        }
    }

    /// validation.go:5349-5376.
    #[test]
    fn apparmor_annotations_need_a_container_and_a_valid_profile() {
        let key = |c: &str| format!("{APP_ARMOR_ANNOTATION_PREFIX}{c}");
        let spec = PodSpec::default();
        let a = ann(&[(&key("nope"), "bad")]);
        let m = messages(&validate_pod_specific_annotations(
            &a,
            &spec,
            &annotations_path(),
            false,
        ));
        assert!(m.contains("container not found"), "{m}");
        assert!(m.contains("invalid AppArmor profile name: \"bad\""), "{m}");
        let p = pod(serde_json::json!({}), serde_json::json!({}));
        let a = ann(&[(&key("c"), "localhost/p")]);
        assert!(validate_pod_specific_annotations(
            &a,
            p.spec.as_ref().unwrap(),
            &annotations_path(),
            false
        )
        .is_empty());
    }

    /// validation.go:202-204, 219-240: the serialized tolerations are validated.
    #[test]
    fn tolerations_annotation_is_validated() {
        let spec = PodSpec::default();
        let key = "scheduler.alpha.kubernetes.io/tolerations";
        let a = ann(&[(key, "{")]);
        assert!(
            !validate_pod_specific_annotations(&a, &spec, &annotations_path(), false).is_empty()
        );
        let a = ann(&[(key, r#"[{"operator":"Exists","value":"v"}]"#)]);
        let m = messages(&validate_pod_specific_annotations(
            &a,
            &spec,
            &annotations_path(),
            false,
        ));
        assert!(
            m.contains("scheduler.alpha.kubernetes.io/tolerations[0]"),
            "{m}"
        );
    }

    /// `/status` runs the tail on the NEW annotations (validation.go:6006).
    #[test]
    fn status_update_validates_the_new_annotations() {
        let old = pod(serde_json::json!({}), serde_json::json!({}));
        let mut new = old.clone();
        new.metadata.annotations = Some(ann(&[(
            "seccomp.security.alpha.kubernetes.io/pod",
            "bogus",
        )]));
        let m = messages(&validate_pod_status_update(&new, &old));
        assert!(m.contains("must be a valid seccomp profile"), "{m}");
    }

    #[test]
    fn mirror_annotation_cannot_be_added() {
        let old = pod(serde_json::json!({}), serde_json::json!({}));
        let mut new = old.clone();
        new.metadata.annotations = Some(HashMap::from([(
            MIRROR_POD_ANNOTATION_KEY.into(),
            "x".into(),
        )]));
        let errs = validate_pod_specific_annotation_updates(&new, &old, &Path::new("a"));
        assert!(messages(&errs).contains("may not add mirror pod annotation"));
    }

    /// validation_test.go:13630-13654: add (even ""), remove and change of the
    /// mirror annotation are all refused, keyed on the annotation path.
    #[test]
    fn upstream_mirror_annotation_update_cases() {
        let with = |v: Option<&str>| {
            let mut p = pod(
                serde_json::json!({"nodeName": "foo"}),
                serde_json::json!({}),
            );
            p.metadata.annotations = v.map(|v| ann(&[(MIRROR_POD_ANNOTATION_KEY, v)]));
            p
        };
        let cases = [
            (with(Some("")), with(None)),
            (with(None), with(Some(""))),
            (with(Some("foo")), with(Some("bar"))),
        ];
        for (new, old) in cases {
            let errs = validate_pod_specific_annotation_updates(&new, &old, &annotations_path());
            assert!(
                messages(&errs).contains("metadata.annotations[kubernetes.io/config.mirror]"),
                "{errs:?}"
            );
        }
    }

    #[test]
    fn allocated_resources_status_must_name_a_request() {
        let spec: PodSpec = serde_json::from_value(serde_json::json!({
            "containers": [{"name": "c", "image": "i", "resources": {"requests": {"cpu": "1"}}}]
        }))
        .unwrap();
        let statuses: Vec<ContainerStatus> = serde_json::from_value(serde_json::json!([
            {"name": "c", "allocatedResourcesStatus": [
                {"name": "cpu", "resources": [{"resourceID": "a", "health": "Healthy"}]},
                {"name": "gpu", "resources": [
                    {"resourceID": "a", "health": "Healthy"},
                    {"resourceID": "a", "health": "Sick"}
                ]}
            ]}
        ]))
        .unwrap();
        let errs = validate_container_status_allocated_resources_status(
            &statuses,
            &Path::new("status").child("containerStatuses"),
            &spec.containers,
        );
        let msg = messages(&errs);
        assert!(msg.contains("must match one of the container's resource requests"));
        assert!(msg.contains("Duplicate value"));
        assert!(msg.contains("Unsupported value"));
        assert_eq!(errs.len(), 3, "{msg}");
    }

    fn statuses(v: serde_json::Value) -> Vec<ContainerStatus> {
        serde_json::from_value(v).unwrap()
    }

    fn terminated(code: i32) -> serde_json::Value {
        serde_json::json!({"name": "c", "image": "i", "imageID": "", "ready": false,
            "restartCount": 0, "state": {"terminated": {"exitCode": code}}})
    }

    fn running() -> serde_json::Value {
        serde_json::json!({"name": "c", "image": "i", "imageID": "", "ready": true,
            "restartCount": 1, "state": {"running": {}}})
    }

    fn spec_of(v: serde_json::Value) -> PodSpec {
        serde_json::from_value(v).unwrap()
    }

    fn opts(rules: bool, restart_all: bool) -> PodValidationOptions {
        PodValidationOptions {
            allow_container_restart_policy_rules: rules,
            allow_restart_all_containers: restart_all,
            ..PodValidationOptions::default()
        }
    }

    /// `ValidateContainerStateTransition` without `AllowContainerRestartPolicyRules`
    /// (validation.go:5882-5904): the pod-level policy decides.
    #[test]
    fn container_state_transition_legacy_uses_pod_policy() {
        let old = statuses(serde_json::json!([terminated(1)]));
        let new = statuses(serde_json::json!([running()]));
        let path = Path::new("status").child("containerStatuses");
        let run = |policy: &str| {
            let spec = spec_of(serde_json::json!({"restartPolicy": policy,
                "containers": [{"name": "c", "image": "i"}]}));
            validate_container_state_transition(&new, &old, &path, &spec, &opts(false, false))
        };
        assert!(run("Always").is_empty());
        assert!(run("OnFailure").is_empty());
        let e = run("Never");
        assert_eq!(e.len(), 1);
        assert_eq!(
            e[0].to_string(),
            "status.containerStatuses[0].state: Forbidden: may not be transitioned to non-terminated state"
        );
        // A clean exit under OnFailure may not restart.
        let ok_old = statuses(serde_json::json!([terminated(0)]));
        let spec = spec_of(serde_json::json!({"restartPolicy": "OnFailure",
            "containers": [{"name": "c", "image": "i"}]}));
        assert_eq!(
            validate_container_state_transition(&new, &ok_old, &path, &spec, &opts(false, false))
                .len(),
            1
        );
    }

    /// The legacy init-container path lets a sidecar leave `terminated`
    /// (validation.go:5948-5973).
    #[test]
    fn init_container_state_transition_legacy_allows_sidecar() {
        let old = statuses(serde_json::json!([terminated(0)]));
        let new = statuses(serde_json::json!([running()]));
        let path = Path::new("status").child("initContainerStatuses");
        let run = |restart_policy: serde_json::Value| {
            let mut init = serde_json::json!({"name": "c", "image": "i"});
            if !restart_policy.is_null() {
                init["restartPolicy"] = restart_policy;
            }
            let spec = spec_of(serde_json::json!({"restartPolicy": "Never",
                "initContainers": [init], "containers": [{"name": "x", "image": "i"}]}));
            validate_init_container_state_transition(&new, &old, &path, &spec, &opts(false, false))
        };
        assert!(run(serde_json::json!("Always")).is_empty());
        assert_eq!(run(serde_json::Value::Null).len(), 1);
    }

    /// With the option on, a rule match (or the container's own policy)
    /// decides, and `RestartAllContainers` lifts the fence for every
    /// container (validation.go:5843-5880).
    #[test]
    fn container_state_transition_with_rules_and_restart_all() {
        let old = statuses(serde_json::json!([terminated(42)]));
        let new = statuses(serde_json::json!([running()]));
        let path = Path::new("status").child("containerStatuses");
        let rule = |action: &str| {
            spec_of(
                serde_json::json!({"restartPolicy": "Never", "containers": [{
                "name": "c", "image": "i", "restartPolicy": "Never",
                "restartPolicyRules": [{"action": action,
                    "exitCodes": {"operator": "In", "values": [42]}}]}]}),
            )
        };
        let check = |spec: &PodSpec, o: &PodValidationOptions| {
            validate_container_state_transition(&new, &old, &path, spec, o).len()
        };
        assert_eq!(check(&rule("Restart"), &opts(true, false)), 0);
        // RestartAllContainers: lifts the fence only with the option on.
        assert_eq!(check(&rule("RestartAllContainers"), &opts(true, true)), 0);
        assert_eq!(check(&rule("RestartAllContainers"), &opts(true, false)), 1);
        // A non-matching exit code falls back to the container policy.
        let other = statuses(serde_json::json!([terminated(1)]));
        assert_eq!(
            validate_container_state_transition(
                &new,
                &other,
                &path,
                &rule("Restart"),
                &opts(true, false)
            )
            .len(),
            1
        );
        // A status naming no spec container is not allowed to restart.
        let stranger = spec_of(serde_json::json!({"containers": [{"name": "z", "image": "i"}]}));
        assert_eq!(check(&stranger, &opts(true, false)), 1);
    }
}
