//! Unit tests moved from `src/validation/pod_options.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{enabled, Feature};
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::quota::{
        is_hugepage_resource_name, is_hugepage_resource_value_divisible,
    };
    pub use rusternetes_common::resources::pod::{EnvFromSource, EnvVar, PodSpec};
    pub use rusternetes_common::resources::workloads::PodTemplateSpec;
    pub use rusternetes_common::types::LabelSelector;
    pub use rusternetes_common::validation::field::Path;
    pub use rusternetes_common::validation::metav1::{
        is_env_var_name, is_relaxed_env_var_name, is_valid_label_value, validate_label_selector,
        LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::pod::{
        allow_taint_toleration_comparison_operators, get_sysctl_namespace,
        validate_local_non_reserved_path, validate_match_label_keys,
        validate_match_label_keys_and_mismatch_label_keys, validate_pod_affinity_term_selector,
        SysctlNamespace,
    };
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::pod_options::*;
    use serde_json::json;

    fn spec(v: serde_json::Value) -> PodSpec {
        let mut base = json!({"containers": [{"name": "c", "image": "i"}]});
        for (k, val) in v.as_object().unwrap() {
            base[k] = val.clone();
        }
        serde_json::from_value(base).unwrap()
    }

    fn opts(old: Option<&PodSpec>) -> PodValidationOptions {
        get_validation_options_from_pod_spec_and_meta(Some(&PodSpec::default()), old)
    }

    fn tsc(keys: &[&str], exprs: serde_json::Value) -> serde_json::Value {
        json!({"topologySpreadConstraints": [{
            "maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule",
            "matchLabelKeys": keys, "labelSelector": {"matchExpressions": exprs}}]})
    }

    /// `TestOldPodViolatesMatchLabelKeysValidationOption` (util_test.go:2684).
    #[test]
    fn old_pod_violates_match_label_keys_validation_option() {
        // Create.
        assert!(!opts(Some(&spec(json!({})))).old_pod_violates_match_label_keys_validation);
        // UpdateInvalidMatchLabelKeys.
        let invalid = spec(tsc(
            &["foo"],
            json!([
                {"key": "foo", "operator": "NotIn", "values": ["value1"]},
                {"key": "foo", "operator": "NotIn", "values": ["value2", "value3"]},
            ]),
        ));
        assert!(opts(Some(&invalid)).old_pod_violates_match_label_keys_validation);
        // UpdateValidMatchLabelKeys.
        let valid = spec(tsc(
            &["foo"],
            json!([{"key": "foo", "operator": "NotIn", "values": ["value1"]}]),
        ));
        assert!(!opts(Some(&valid)).old_pod_violates_match_label_keys_validation);
    }

    /// `TestOldPodViolatesLegacyMatchLabelKeysValidationOption` (:2760): with
    /// the SelectorMerge gate on (the 1.35 default) the legacy option is never
    /// derived; the helper itself is checked directly.
    #[test]
    fn legacy_match_label_keys_helper() {
        let invalid = spec(tsc(
            &["foo"],
            json!([{"key": "foo", "operator": "NotIn", "values": ["value1"]}]),
        ));
        assert!(has_legacy_invalid_topology_spread_constrain_match_label_keys(&invalid));
        let valid = spec(json!({"topologySpreadConstraints": [{
            "maxSkew": 1, "topologyKey": "z", "whenUnsatisfiable": "DoNotSchedule",
            "matchLabelKeys": ["foo"], "labelSelector": {}}]}));
        assert!(!has_legacy_invalid_topology_spread_constrain_match_label_keys(&valid));
    }

    /// `relaxedEnvVarUsed` / `useAllowEnvFilesValidation` derivation with the
    /// gates off (util.go:497, :561): only an old spec that already uses the
    /// relaxed name / a fileKeyRef keeps the option.
    #[test]
    #[serial_test::serial]
    fn env_options_from_old_spec_when_gates_off() {
        let _a = rusternetes_common::feature_gates::with_feature(
            Feature::RelaxedEnvironmentVariableValidation,
            false,
        );
        let _b = rusternetes_common::feature_gates::with_feature(Feature::EnvFiles, false);
        let with_env = |env: serde_json::Value| {
            spec(json!({"containers": [{"name": "c", "image": "i", "env": env}]}))
        };
        let derive = |new: &PodSpec, old: Option<&PodSpec>| {
            get_validation_options_from_pod_spec_and_meta(Some(new), old)
        };
        let relaxed = with_env(json!([{"name": "1bad"}]));
        let strict = with_env(json!([{"name": "ok"}]));
        // Create: the gate decides.
        assert!(!derive(&relaxed, None).allow_relaxed_environment_variable_validation);
        // Update, old pod used the name.
        assert!(derive(&relaxed, Some(&relaxed)).allow_relaxed_environment_variable_validation);
        // Update, old pod did not use it.
        assert!(!derive(&relaxed, Some(&strict)).allow_relaxed_environment_variable_validation);
        // Invalid under relaxed rules too: strict.
        let bad = with_env(json!([{"name": "a=b"}]));
        assert!(!derive(&bad, Some(&bad)).allow_relaxed_environment_variable_validation);

        let file = with_env(json!([{"name": "f", "valueFrom": {"fileKeyRef":
            {"volumeName": "v", "path": "p", "key": "K"}}}]));
        assert!(!derive(&file, None).allow_env_files_validation);
        assert!(!derive(&file, Some(&strict)).allow_env_files_validation);
        assert!(derive(&strict, Some(&file)).allow_env_files_validation);
    }

    /// `TestValidateAllowNonLocalProjectedTokenPathOption` (:2822).
    #[test]
    fn allow_non_local_projected_token_path_option() {
        assert!(!opts(None).allow_non_local_projected_token_path);
        let vol = |path: &str| {
            spec(
                json!({"volumes": [{"name": "foo", "projected": {"sources": [
                {"serviceAccountToken": {"path": path}}]}}]}),
            )
        };
        assert!(opts(Some(&vol("../foo"))).allow_non_local_projected_token_path);
        assert!(!opts(Some(&vol("foo"))).allow_non_local_projected_token_path);
        let host = spec(json!({"volumes": [{"name": "foo", "hostPath": {"path": "foo"}}]}));
        assert!(!opts(Some(&host)).allow_non_local_projected_token_path);
    }

    /// `TestValidateInvalidLabelValueInNodeSelectorOption` (:4580).
    #[test]
    fn invalid_label_value_in_required_node_affinity_option() {
        let aff = |terms: serde_json::Value| {
            spec(json!({"affinity": {"nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": terms}}}}))
        };
        assert!(!opts(None).allow_invalid_label_value_in_required_node_affinity);
        let invalid = aff(json!([{"matchExpressions": [
            {"key": "foo", "operator": "In", "values": ["-1"]}]}]));
        assert!(opts(Some(&invalid)).allow_invalid_label_value_in_required_node_affinity);
        let valid = aff(json!([{"matchExpressions": [
            {"key": "foo", "operator": "In", "values": ["bar"]}]}]));
        assert!(!opts(Some(&valid)).allow_invalid_label_value_in_required_node_affinity);
        assert!(!opts(Some(&aff(json!([])))).allow_invalid_label_value_in_required_node_affinity);
    }

    /// util.go:466-475: an old sysctl `ValidateHostSysctl` rejects.
    #[test]
    fn namespaced_sysctls_for_host_net_and_ipc_option() {
        let s = |extra: serde_json::Value| {
            let mut v = json!({"securityContext": {"sysctls": [
                {"name": "net.core.somaxconn", "value": "1"}]}});
            for (k, val) in extra.as_object().unwrap() {
                v[k] = val.clone();
            }
            spec(v)
        };
        assert!(
            opts(Some(&s(json!({"hostNetwork": true}))))
                .allow_namespaced_sysctls_for_host_net_and_host_ipc
        );
        assert!(!opts(Some(&s(json!({})))).allow_namespaced_sysctls_for_host_net_and_host_ipc);
        assert!(!opts(None).allow_namespaced_sysctls_for_host_net_and_host_ipc);
    }

    /// `useRelaxedDNSSearchValidation` / `hasDotOrUnderscore` (util.go:520-542).
    #[test]
    fn has_dot_or_underscore() {
        let dns = |searches: &[&str]| spec(json!({"dnsConfig": {"searches": searches}}));
        assert!(
            opts(Some(&dns(&["."]))).allow_relaxed_dns_search_validation
                || enabled(Feature::RelaxedDNSSearchValidation)
        );
        assert!(
            opts(Some(&dns(&["a_b"]))).allow_relaxed_dns_search_validation
                || enabled(Feature::RelaxedDNSSearchValidation)
        );
    }

    fn sleep_spec(hook: &str, secs: i64) -> PodSpec {
        spec(json!({"containers": [{"name": "c", "image": "i",
            "lifecycle": {hook: {"sleep": {"seconds": secs}}}}]}))
    }

    /// `TestValidateAllowPodLifecycleSleepActionZeroValue` (util_test.go:4654):
    /// the gate (on in 1.35) and the old-spec zero-sleep relaxation.
    #[test]
    #[serial_test::serial]
    fn allow_pod_lifecycle_sleep_action_zero_value_option() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let g = with_feature(Feature::PodLifecycleSleepActionAllowZero, true);
        let cases = [
            spec(json!({})),
            sleep_spec("preStop", 1),
            sleep_spec("postStart", 1),
            sleep_spec("preStop", 0),
            sleep_spec("postStart", 0),
        ];
        for old in &cases {
            assert!(opts(Some(old)).allow_pod_lifecycle_sleep_action_zero_value);
        }
        drop(g);
        let _g = with_feature(Feature::PodLifecycleSleepActionAllowZero, false);
        assert!(!opts(Some(&spec(json!({})))).allow_pod_lifecycle_sleep_action_zero_value);
        assert!(!opts(Some(&sleep_spec("preStop", 1))).allow_pod_lifecycle_sleep_action_zero_value);
        assert!(
            !opts(Some(&sleep_spec("postStart", 1))).allow_pod_lifecycle_sleep_action_zero_value
        );
        assert!(opts(Some(&sleep_spec("preStop", 0))).allow_pod_lifecycle_sleep_action_zero_value);
        assert!(
            opts(Some(&sleep_spec("postStart", 0))).allow_pod_lifecycle_sleep_action_zero_value
        );
        // Create (no old spec) with the gate off stays strict.
        assert!(!opts(None).allow_pod_lifecycle_sleep_action_zero_value);
    }

    /// `usesIndivisibleHugePagesValues` (util.go:333-359): containers (limits
    /// and requests), init containers and the pod overhead.
    #[test]
    fn indivisible_huge_pages_values_option() {
        let c = |res: serde_json::Value| {
            spec(json!({"containers": [{"name": "c", "image": "i", "resources": res}]}))
        };
        let on = |s: &PodSpec| opts(Some(s)).allow_indivisible_huge_pages_values;
        assert!(on(&c(json!({"limits": {"hugepages-2Mi": "3Mi"}}))));
        assert!(on(&c(json!({"requests": {"hugepages-2Mi": "1Mi"}}))));
        assert!(!on(&c(json!({"limits": {"hugepages-2Mi": "4Mi"}}))));
        // Not a hugepages resource, and an unparseable page size.
        assert!(!on(&c(json!({"limits": {"memory": "3"}}))));
        assert!(on(&c(json!({"limits": {"hugepages-bogus": "1"}}))));
        assert!(on(&spec(
            json!({"initContainers": [{"name": "i", "image": "i",
            "resources": {"limits": {"hugepages-1Gi": "1Mi"}}}]})
        )));
        assert!(on(&spec(json!({"overhead": {"hugepages-2Mi": "3Mi"}}))));
        assert!(!on(&spec(json!({"overhead": {"hugepages-2Mi": "2Mi"}}))));
        assert!(!opts(None).allow_indivisible_huge_pages_values);
    }

    fn host_users_errors(spec: &PodSpec, opts: &PodValidationOptions) -> Vec<String> {
        rusternetes_common::validation::pod::validate_pod_spec_with_options(
            spec,
            &Path::new("spec"),
            opts,
        )
        .iter()
        .map(|e| e.to_string())
        .filter(|e| e.contains("`hostUsers` is false"))
        .collect()
    }

    fn vol_dev(key: &str) -> serde_json::Value {
        let dev = json!([{"name": "v", "devicePath": "/dev/x"}]);
        match key {
            "containers" => {
                json!({"containers": [{"name": "c", "image": "i", "volumeDevices": dev}]})
            }
            "initContainers" => {
                json!({"initContainers": [{"name": "c", "image": "i", "volumeDevices": dev}]})
            }
            _ => {
                json!({"ephemeralContainers": [{"name": "c", "image": "i", "volumeDevices": dev}]})
            }
        }
    }

    /// `TestValidateHostUsers` (validation_test.go:26005): rejections.
    #[test]
    fn validate_host_users_forbidden() {
        let o = PodValidationOptions::default();
        let e = host_users_errors(&spec(json!({"hostUsers": false, "hostNetwork": true})), &o);
        assert_eq!(
            e,
            vec!["spec.hostNetwork: Forbidden: when `hostUsers` is false"]
        );
        let e = host_users_errors(&spec(json!({"hostUsers": false, "hostPID": true})), &o);
        assert_eq!(
            e,
            vec!["spec.HostPID: Forbidden: when `hostUsers` is false"]
        );
        let e = host_users_errors(&spec(json!({"hostUsers": false, "hostIPC": true})), &o);
        assert_eq!(
            e,
            vec!["spec.HostIPC: Forbidden: when `hostUsers` is false"]
        );
        for (k, path) in [
            ("containers", "spec.containers[0].volumeDevices"),
            ("initContainers", "spec.initContainers[0].volumeDevices"),
            (
                "ephemeralContainers",
                "spec.ephemeralContainers[0].volumeDevices",
            ),
        ] {
            let mut v = vol_dev(k);
            v["hostUsers"] = json!(false);
            let e = host_users_errors(&spec(v), &o);
            assert_eq!(
                e,
                vec![format!("{path}: Forbidden: when `hostUsers` is false")],
                "{k}"
            );
        }
    }

    /// `TestValidateHostUsers`: accepted cases, and the two options.
    #[test]
    fn validate_host_users_allowed() {
        let o = PodValidationOptions::default();
        assert!(host_users_errors(&spec(json!({})), &o).is_empty());
        assert!(host_users_errors(
            &spec(
                json!({"hostUsers": true, "hostNetwork": true, "hostPID": true, "hostIPC": true})
            ),
            &o
        )
        .is_empty());
        assert!(host_users_errors(&spec(json!({"hostNetwork": true})), &o).is_empty());
        let o = PodValidationOptions {
            allow_user_namespaces_with_volume_devices: true,
            ..Default::default()
        };
        for k in ["containers", "initContainers", "ephemeralContainers"] {
            let mut v = vol_dev(k);
            v["hostUsers"] = json!(false);
            assert!(host_users_errors(&spec(v), &o).is_empty(), "{k}");
        }
        let o = PodValidationOptions {
            allow_user_namespaces_host_network_support: true,
            ..Default::default()
        };
        assert!(
            host_users_errors(&spec(json!({"hostUsers": false, "hostNetwork": true})), &o)
                .is_empty()
        );
    }

    /// `TestHasUserNamespacesWithVolumeDevices` (util_test.go:6129), through
    /// the derived option.
    #[test]
    fn user_namespaces_with_volume_devices_option() {
        assert!(!opts(None).allow_user_namespaces_with_volume_devices);
        for (k, want) in [
            ("containers", true),
            ("initContainers", true),
            ("ephemeralContainers", true),
        ] {
            let mut v = vol_dev(k);
            v["hostUsers"] = json!(false);
            assert_eq!(
                opts(Some(&spec(v))).allow_user_namespaces_with_volume_devices,
                want,
                "{k}"
            );
            let mut v = vol_dev(k);
            v["hostUsers"] = json!(true);
            assert!(
                !opts(Some(&spec(v))).allow_user_namespaces_with_volume_devices,
                "{k}"
            );
        }
        assert!(
            !opts(Some(&spec(json!({"hostUsers": false}))))
                .allow_user_namespaces_with_volume_devices
        );
        assert!(!opts(Some(&spec(json!({})))).allow_user_namespaces_with_volume_devices);
    }

    /// `useAllowUserNamespacesHostNetworkSupport` (util.go:544): gate off (the
    /// 1.35 default), so only an old userns+hostNetwork pod relaxes it.
    #[test]
    fn user_namespaces_host_network_support_option() {
        assert!(!opts(None).allow_user_namespaces_host_network_support);
        let old = spec(json!({"hostUsers": false, "hostNetwork": true}));
        assert!(opts(Some(&old)).allow_user_namespaces_host_network_support);
        for v in [
            json!({"hostUsers": true, "hostNetwork": true}),
            json!({"hostUsers": false}),
            json!({"hostNetwork": true}),
        ] {
            assert!(!opts(Some(&spec(v))).allow_user_namespaces_host_network_support);
        }
    }

    /// With the gate on, the option holds on create.
    #[test]
    #[serial_test::serial]
    fn user_namespaces_host_network_support_gate() {
        let _g = rusternetes_common::feature_gates::with_feature(
            Feature::UserNamespacesHostNetworkSupport,
            true,
        );
        assert!(opts(None).allow_user_namespaces_host_network_support);
    }

    fn restart_container(policy: Option<&str>, rules: serde_json::Value) -> serde_json::Value {
        let mut c = json!({"name": "c", "image": "i", "restartPolicyRules": rules});
        if let Some(p) = policy {
            c["restartPolicy"] = json!(p);
        }
        c
    }

    fn restart_rule(action: &str) -> serde_json::Value {
        json!([{"action": action, "exitCodes": {"operator": "In", "values": [1]}}])
    }

    /// `TestAllowContainerRestartPolicyRules` (util_test.go:5900-6003).
    #[test]
    fn allow_container_restart_policy_rules_option() {
        // Gate on (the 1.35 default), create and plain update.
        assert!(opts(None).allow_container_restart_policy_rules);
        assert!(opts(Some(&spec(json!({})))).allow_container_restart_policy_rules);
        // The in-use helper, which relaxes the option when the gate is off.
        let regular = spec(json!({"containers": [restart_container(Some("Never"), json!([]))]}));
        assert!(container_restart_rules_in_use(&regular));
        let regular_rules = spec(
            json!({"containers": [restart_container(Some("Never"), restart_rule("Restart"))]}),
        );
        assert!(container_restart_rules_in_use(&regular_rules));
        let init = spec(json!({"initContainers": [restart_container(Some("Never"), json!([]))]}));
        assert!(container_restart_rules_in_use(&init));
        let init_rules = spec(json!({"initContainers": [
            restart_container(Some("Never"), restart_rule("Restart"))]}));
        assert!(container_restart_rules_in_use(&init_rules));
        // A sidecar without rules is not "in use"; with rules it is.
        let sidecar =
            spec(json!({"initContainers": [restart_container(Some("Always"), json!([]))]}));
        assert!(!container_restart_rules_in_use(&sidecar));
        let sidecar_rules = spec(json!({"initContainers": [
            restart_container(Some("Always"), restart_rule("Restart"))]}));
        assert!(container_restart_rules_in_use(&sidecar_rules));
        assert!(!container_restart_rules_in_use(&spec(json!({}))));
    }

    /// `TestAllowRestartAllContainers` (util_test.go:6500-6589).
    #[test]
    fn allow_restart_all_containers_option() {
        let action = |a: &str| {
            spec(json!({"containers": [restart_container(Some("Never"), restart_rule(a))]}))
        };
        assert!(restart_all_containers_action_in_use(&action(
            "RestartAllContainers"
        )));
        assert!(!restart_all_containers_action_in_use(&action("Restart")));
        let init_action = spec(json!({"initContainers": [
            restart_container(Some("Never"), restart_rule("RestartAllContainers"))]}));
        assert!(restart_all_containers_action_in_use(&init_action));
        // Rules on a sidecar count regardless of the action.
        let sidecar_rules = spec(json!({"initContainers": [
            restart_container(Some("Always"), restart_rule("Restart"))]}));
        assert!(restart_all_containers_action_in_use(&sidecar_rules));
        let plain_init = spec(json!({"initContainers": [
            restart_container(Some("Never"), restart_rule("Restart"))]}));
        assert!(!restart_all_containers_action_in_use(&plain_init));
        assert!(!restart_all_containers_action_in_use(&spec(json!({}))));
        // Gate off by default; an old spec that uses the action relaxes it.
        assert!(!opts(None).allow_restart_all_containers);
        assert!(opts(Some(&action("RestartAllContainers"))).allow_restart_all_containers);
    }

    fn init_spec(inits: serde_json::Value) -> PodSpec {
        spec(json!({ "initContainers": inits }))
    }

    /// `TestValidateAllowSidecarResizePolicy` (util_test.go:4408): the option
    /// is `want || gate`, and only a RESTARTABLE init container with a
    /// resizePolicy relaxes it. Create (no old spec) is `gate`.
    #[test]
    #[serial_test::serial]
    fn allow_sidecar_resize_policy_option() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let rp = json!([{"resourceName": "cpu", "restartPolicy": "NotRequired"}]);
        let side = |rp: &serde_json::Value| json!({"name": "s", "image": "i", "restartPolicy": "Always", "resizePolicy": rp});
        let plain = |rp: &serde_json::Value| json!({"name": "p", "image": "i", "resizePolicy": rp});
        let cases = [
            (None, false),
            (
                Some(init_spec(json!([side(&json!([])), plain(&json!([]))]))),
                false,
            ),
            (
                Some(init_spec(json!([side(&json!([])), plain(&rp)]))),
                false,
            ),
            (Some(init_spec(json!([side(&rp), plain(&json!([]))]))), true),
            (Some(init_spec(json!([side(&rp), plain(&rp)]))), true),
            (Some(init_spec(json!([plain(&rp), plain(&rp)]))), false),
            (
                Some(spec(json!({"containers": [
                    {"name": "c", "image": "i", "resizePolicy": rp}]}))),
                false,
            ),
        ];
        for gate in [true, false] {
            let _g = with_feature(Feature::InPlacePodVerticalScaling, gate);
            for (old, want) in &cases {
                assert_eq!(
                    opts(old.as_ref()).allow_sidecar_resize_policy,
                    *want || gate,
                    "old={old:?} gate={gate}"
                );
            }
        }
    }

    /// validation.go:3866: with the option off, any init container with a
    /// resizePolicy is `Invalid`; with it on, no error from this rule.
    #[test]
    fn sidecar_resize_policy_rule() {
        use rusternetes_common::validation::pod::validate_pod_spec_with_options;
        let s = init_spec(
            json!([{"name": "s", "image": "i", "restartPolicy": "Always",
            "terminationMessagePolicy": "File", "imagePullPolicy": "Always",
            "resizePolicy": [{"resourceName": "cpu", "restartPolicy": "NotRequired"}]}]),
        );
        let has = |o: &PodValidationOptions| {
            validate_pod_spec_with_options(&s, &Path::new("spec"), o)
                .iter()
                .any(|e| {
                    e.to_string()
                        .contains("spec.initContainers[0].resizePolicy: Invalid value")
                        && e.to_string()
                            .contains("must not be set for init containers")
                })
        };
        let on = PodValidationOptions {
            allow_sidecar_resize_policy: true,
            ..Default::default()
        };
        assert!(!has(&on));
        assert!(has(&PodValidationOptions::default()));
    }

    /// util.go:422/:427: both options follow their gate and ignore the old
    /// spec; a create and an update agree.
    #[test]
    #[serial_test::serial]
    fn pod_level_resources_options_follow_gates() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        for gate in [true, false] {
            let _a = with_feature(Feature::PodLevelResources, gate);
            let _b = with_feature(Feature::InPlacePodLevelResourcesVerticalScaling, gate);
            for old in [None, Some(spec(json!({"containers": []})))] {
                let o = opts(old.as_ref());
                assert_eq!(o.pod_level_resources_enabled, gate);
                assert_eq!(
                    o.in_place_pod_level_resources_vertical_scaling_enabled,
                    gate
                );
                assert!(!o.allow_indivisible_huge_pages_values);
            }
        }
    }

    /// validation.go:4645: the `spec.resources` rules run only with
    /// `PodLevelResourcesEnabled`.
    #[test]
    fn pod_level_resources_rule_is_gated() {
        use rusternetes_common::validation::pod::validate_pod_spec_with_options;
        let s = spec(json!({
            "containers": [{"name": "c", "image": "i",
                "terminationMessagePolicy": "File", "imagePullPolicy": "Always"}],
            "os": {"name": "windows"},
            "resources": {"limits": {"cpu": "1"}},
        }));
        let has = |o: &PodValidationOptions| {
            validate_pod_spec_with_options(&s, &Path::new("spec"), o)
                .iter()
                .any(|e| e.to_string().contains("spec.resources: Forbidden"))
        };
        assert!(!has(&PodValidationOptions::default()));
        assert!(has(&PodValidationOptions {
            pod_level_resources_enabled: true,
            ..Default::default()
        }));
    }

    fn selinux_spec(policy: &str) -> PodSpec {
        spec(json!({"securityContext": {"seLinuxChangePolicy": policy}}))
    }

    /// `useOnlyRecursiveSELinuxChangePolicy` (util.go:1635): `SELinuxMount` on
    /// allows everything; off, only Recursive unless the old spec has a policy.
    #[test]
    #[serial_test::serial]
    fn allow_only_recursive_se_linux_change_policy_option() {
        use rusternetes_common::feature_gates::{with_feature, Feature};
        let g = with_feature(Feature::SELinuxMount, true);
        assert!(!opts(None).allow_only_recursive_se_linux_change_policy);
        assert!(!opts(Some(&spec(json!({})))).allow_only_recursive_se_linux_change_policy);
        drop(g);
        let _g = with_feature(Feature::SELinuxMount, false);
        assert!(opts(None).allow_only_recursive_se_linux_change_policy);
        assert!(opts(Some(&spec(json!({})))).allow_only_recursive_se_linux_change_policy);
        assert!(
            opts(Some(&spec(json!({"securityContext": {}}))))
                .allow_only_recursive_se_linux_change_policy
        );
        for p in ["Recursive", "MountOption"] {
            assert!(
                !opts(Some(&selinux_spec(p))).allow_only_recursive_se_linux_change_policy,
                "{p}"
            );
        }
    }

    /// `TestValidateSELinuxChangePolicy` cases (validation_test.go:27884).
    #[test]
    fn se_linux_change_policy_rule() {
        use rusternetes_common::validation::pod::validate_pod_spec_with_options;
        let errs = |policy: &str, only_recursive: bool| -> Vec<String> {
            let o = PodValidationOptions {
                allow_only_recursive_se_linux_change_policy: only_recursive,
                ..Default::default()
            };
            validate_pod_spec_with_options(&selinux_spec(policy), &Path::new("spec"), &o)
                .iter()
                .map(|e| e.to_string())
                .filter(|e| e.contains("seLinuxChangePolicy"))
                .collect()
        };
        assert!(errs("Recursive", true).is_empty());
        assert!(errs("Recursive", false).is_empty());
        assert!(errs("MountOption", false).is_empty());
        assert_eq!(
            errs("MountOption", true),
            ["spec.securityContext.seLinuxChangePolicy: Unsupported value: \"MountOption\": supported values: \"Recursive\""]
        );
        assert_eq!(
            errs("InvalidValue", false),
            ["spec.securityContext.seLinuxChangePolicy: Unsupported value: \"InvalidValue\": supported values: \"MountOption\", \"Recursive\""]
        );
    }
}
