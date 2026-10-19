//! `PodValidationOptions` and `GetValidationOptionsFromPodSpecAndMeta`.
//!
//! Port of `pkg/api/pod/util.go:411` (`GetValidationOptionsFromPodSpecAndMeta`)
//! and its helpers, release-1.35, with the `PodValidationOptions` struct from
//! `pkg/apis/core/validation/validation.go`.
//!
//! Every option starts from a feature gate (or `false`) and is then relaxed
//! when the OLD spec already violates the rule, so an object that is already
//! stored stays updatable. A create has no old spec and is strict.
//!
//! Options are ported where the validator in [`crate::validation::pod`] has the
//! gated rule. The remaining upstream options (`AllowIndivisibleHugePagesValues`,
//! `AllowOnlyRecursiveSELinuxChangePolicy`,
//! `AllowSidecarResizePolicy`, `AllowContainerRestartPolicyRules`,
//! `AllowRestartAllContainers`, `AllowPodLifecycleSleepActionZeroValue`,
//! `AllowRelaxedEnvironmentVariableValidation`, `AllowEnvFilesValidation`,
//! `PodLevelResourcesEnabled`, `InPlacePodLevelResourcesVerticalScalingEnabled`)
//! are tracked in #3572 because the rule they gate is not ported yet.

use crate::feature_gates::{enabled, Feature};
use crate::resources::pod::PodSpec;
use crate::types::LabelSelector;
use crate::validation::field::Path;
use crate::validation::metav1::{
    is_valid_label_value, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::pod::{
    allow_taint_toleration_comparison_operators, get_sysctl_namespace,
    validate_local_non_reserved_path, validate_match_label_keys,
    validate_match_label_keys_and_mismatch_label_keys, validate_pod_affinity_term_selector,
    SysctlNamespace,
};

/// `apivalidation.PodValidationOptions` (validation.go), limited to the
/// options the Rust validator consumes.
#[derive(Debug, Clone, Default)]
pub struct PodValidationOptions {
    /// `AllowInvalidLabelValueInSelector`: pod (anti)affinity selectors.
    pub allow_invalid_label_value_in_selector: bool,
    /// `AllowInvalidTopologySpreadConstraintLabelSelector`.
    pub allow_invalid_topology_spread_constraint_label_selector: bool,
    /// `AllowInvalidLabelValueInRequiredNodeAffinity`.
    pub allow_invalid_label_value_in_required_node_affinity: bool,
    /// `AllowNonLocalProjectedTokenPath`.
    pub allow_non_local_projected_token_path: bool,
    /// `AllowNamespacedSysctlsForHostNetAndHostIPC`.
    pub allow_namespaced_sysctls_for_host_net_and_host_ipc: bool,
    /// `AllowMatchLabelKeysInPodTopologySpread` (gate).
    pub allow_match_label_keys_in_pod_topology_spread: bool,
    /// `AllowMatchLabelKeysInPodTopologySpreadSelectorMerge` (gate).
    pub allow_match_label_keys_in_pod_topology_spread_selector_merge: bool,
    /// `OldPodViolatesMatchLabelKeysValidation`.
    pub old_pod_violates_match_label_keys_validation: bool,
    /// `OldPodViolatesLegacyMatchLabelKeysValidation`.
    pub old_pod_violates_legacy_match_label_keys_validation: bool,
    /// `AllowRelaxedDNSSearchValidation`.
    pub allow_relaxed_dns_search_validation: bool,
    /// `AllowTaintTolerationComparisonOperators`.
    pub allow_taint_toleration_comparison_operators: bool,
    /// `AllowUserNamespacesWithVolumeDevices`.
    pub allow_user_namespaces_with_volume_devices: bool,
    /// `AllowUserNamespacesHostNetworkSupport`.
    pub allow_user_namespaces_host_network_support: bool,
}

/// `GetValidationOptionsFromPodSpecAndMeta` (util.go:411-495) for the options
/// in [`PodValidationOptions`]. `old_spec` is `None` on create.
///
/// The `podMeta`/`oldPodMeta` arguments only feed `AllowInvalidPodDeletionCost`,
/// which the pod metadata validators derive themselves
/// (`validate_pod_metadata_annotations`).
pub fn get_validation_options_from_pod_spec_and_meta(
    _spec: Option<&PodSpec>,
    old_spec: Option<&PodSpec>,
) -> PodValidationOptions {
    // Defaults from the feature gates (util.go:413-434).
    let mut opts = PodValidationOptions {
        allow_match_label_keys_in_pod_topology_spread: enabled(
            Feature::MatchLabelKeysInPodTopologySpread,
        ),
        allow_match_label_keys_in_pod_topology_spread_selector_merge: enabled(
            Feature::MatchLabelKeysInPodTopologySpreadSelectorMerge,
        ),
        ..PodValidationOptions::default()
    };

    // util.go:439 `useRelaxedDNSSearchValidation(oldPodSpec)`.
    opts.allow_relaxed_dns_search_validation = use_relaxed_dns_search_validation(old_spec);
    // util.go:444 `allowTaintTolerationComparisonOperators(oldPodSpec)`.
    opts.allow_taint_toleration_comparison_operators =
        allow_taint_toleration_comparison_operators(old_spec);

    // util.go:441 `useAllowUserNamespacesHostNetworkSupport(oldPodSpec)`.
    opts.allow_user_namespaces_host_network_support =
        use_allow_user_namespaces_host_network_support(old_spec);

    if let Some(old) = old_spec {
        // util.go:450 `hasInvalidLabelValueInAffinitySelector`.
        opts.allow_invalid_label_value_in_selector =
            has_invalid_label_value_in_affinity_selector(old);
        // util.go:451.
        opts.allow_invalid_label_value_in_required_node_affinity =
            has_invalid_label_value_in_required_node_affinity(old);
        // util.go:453.
        opts.allow_invalid_topology_spread_constraint_label_selector =
            has_invalid_topology_spread_constraint_label_selector(old);
        if opts.allow_match_label_keys_in_pod_topology_spread {
            if opts.allow_match_label_keys_in_pod_topology_spread_selector_merge {
                // util.go:457.
                opts.old_pod_violates_match_label_keys_validation =
                    has_invalid_topology_spread_constrain_match_label_keys(old);
            } else {
                // util.go:460.
                opts.old_pod_violates_legacy_match_label_keys_validation =
                    has_legacy_invalid_topology_spread_constrain_match_label_keys(old);
            }
        }
        // util.go:486: an old spec with userns and volume devices (which
        // doesn't work) may still be modified.
        opts.allow_user_namespaces_with_volume_devices =
            has_user_namespaces_with_volume_devices(old);
        // util.go:464.
        opts.allow_non_local_projected_token_path = has_non_local_projected_token_path(old);
        // util.go:467-475: an old sysctl that `ValidateHostSysctl` rejects.
        if let Some(sc) = old.security_context.as_ref() {
            let host_network = old.host_network.unwrap_or(false);
            let host_ipc = old.host_ipc.unwrap_or(false);
            opts.allow_namespaced_sysctls_for_host_net_and_host_ipc =
                sc.sysctls.as_deref().unwrap_or(&[]).iter().any(|s| {
                    match get_sysctl_namespace(&s.name) {
                        SysctlNamespace::Net => host_network,
                        SysctlNamespace::Ipc => host_ipc,
                        SysctlNamespace::Unknown => false,
                    }
                });
        }
    }
    opts
}

/// `useAllowUserNamespacesHostNetworkSupport` (util.go:544-558).
fn use_allow_user_namespaces_host_network_support(old_spec: Option<&PodSpec>) -> bool {
    if enabled(Feature::UserNamespacesHostNetworkSupport) {
        return true;
    }
    // A pod with user namespaces and hostNetwork already in the cluster keeps
    // the relaxed validation after the gate is disabled.
    old_spec.is_some_and(|s| s.host_users == Some(false) && s.host_network.unwrap_or(false))
}

/// `hasUserNamespacesWithVolumeDevices` (util.go:1664-1678).
fn has_user_namespaces_with_volume_devices(spec: &PodSpec) -> bool {
    if spec.host_users != Some(false) {
        return false;
    }
    let devices = |c: &crate::resources::pod::Container| {
        c.volume_devices.as_deref().is_some_and(|d| !d.is_empty())
    };
    spec.containers.iter().any(devices)
        || spec.init_containers.iter().flatten().any(devices)
        || spec
            .ephemeral_containers
            .iter()
            .flatten()
            .any(|c| c.volume_devices.as_deref().is_some_and(|d| !d.is_empty()))
}

/// `useRelaxedDNSSearchValidation` (util.go:520-532).
fn use_relaxed_dns_search_validation(old_spec: Option<&PodSpec>) -> bool {
    if enabled(Feature::RelaxedDNSSearchValidation) {
        return true;
    }
    old_spec
        .and_then(|s| s.dns_config.as_ref())
        .and_then(|c| c.searches.as_deref())
        // `hasDotOrUnderscore` (:535-542).
        .is_some_and(|searches| searches.iter().any(|d| d == "." || d.contains('_')))
}

/// `hasInvalidLabelValueInAffinitySelector` (util.go:1504-1536).
fn has_invalid_label_value_in_affinity_selector(spec: &PodSpec) -> bool {
    let Some(affinity) = spec.affinity.as_ref() else {
        return false;
    };
    let invalid = |term: &crate::resources::pod::PodAffinityTerm| {
        !validate_pod_affinity_term_selector(term, false, &Path::new("")).is_empty()
    };
    if let Some(pa) = affinity.pod_affinity.as_ref() {
        if pa
            .required_during_scheduling_ignored_during_execution
            .iter()
            .flatten()
            .any(invalid)
            || pa
                .preferred_during_scheduling_ignored_during_execution
                .iter()
                .flatten()
                .any(|t| invalid(&t.pod_affinity_term))
        {
            return true;
        }
    }
    if let Some(paa) = affinity.pod_anti_affinity.as_ref() {
        if paa
            .required_during_scheduling_ignored_during_execution
            .iter()
            .flatten()
            .any(invalid)
            || paa
                .preferred_during_scheduling_ignored_during_execution
                .iter()
                .flatten()
                .any(|t| invalid(&t.pod_affinity_term))
        {
            return true;
        }
    }
    false
}

/// `hasInvalidLabelValueInRequiredNodeAffinity` (util.go:1548-1556) over
/// `HasInvalidLabelValueInNodeSelectorTerms` (core/helper/helpers.go:517-528):
/// only `matchExpressions` values are inspected.
fn has_invalid_label_value_in_required_node_affinity(spec: &PodSpec) -> bool {
    spec.affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .and_then(|na| {
            na.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .is_some_and(|ns| {
            ns.node_selector_terms.iter().any(|term| {
                term.match_expressions
                    .iter()
                    .flatten()
                    .flat_map(|e| e.values.iter().flatten())
                    .any(|v| !is_valid_label_value(v).is_empty())
            })
        })
}

/// `hasInvalidTopologySpreadConstraintLabelSelector` (util.go:360-368).
fn has_invalid_topology_spread_constraint_label_selector(spec: &PodSpec) -> bool {
    spec.topology_spread_constraints.iter().flatten().any(|c| {
        c.label_selector
            .as_ref()
            .is_some_and(|sel: &LabelSelector| {
                !validate_label_selector(
                    sel,
                    LabelSelectorValidationOptions::default(),
                    &Path::new(""),
                )
                .is_empty()
            })
    })
}

/// `hasInvalidTopologySpreadConstrainMatchLabelKeys` (util.go:371-379).
fn has_invalid_topology_spread_constrain_match_label_keys(spec: &PodSpec) -> bool {
    spec.topology_spread_constraints.iter().flatten().any(|c| {
        !validate_match_label_keys_and_mismatch_label_keys(
            &Path::new(""),
            c.match_label_keys.as_deref().unwrap_or(&[]),
            &[],
            c.label_selector.as_ref(),
        )
        .is_empty()
    })
}

/// `hasLegacyInvalidTopologySpreadConstrainMatchLabelKeys` (util.go:382-390).
fn has_legacy_invalid_topology_spread_constrain_match_label_keys(spec: &PodSpec) -> bool {
    spec.topology_spread_constraints.iter().flatten().any(|c| {
        !validate_match_label_keys(
            c.match_label_keys.as_deref(),
            c.label_selector.as_ref(),
            &Path::new(""),
        )
        .is_empty()
    })
}

/// `hasNonLocalProjectedTokenPath` (util.go:393-408).
fn has_non_local_projected_token_path(spec: &PodSpec) -> bool {
    spec.volumes
        .iter()
        .flatten()
        .filter_map(|v| v.projected.as_ref())
        .flat_map(|p| p.sources.iter().flatten())
        .filter_map(|s| s.service_account_token.as_ref())
        .any(|sat| !validate_local_non_reserved_path(&sat.path, &Path::new("")).is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn host_users_errors(spec: &PodSpec, opts: &PodValidationOptions) -> Vec<String> {
        crate::validation::pod::validate_pod_spec_with_options(spec, &Path::new("spec"), opts)
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
        let _g =
            crate::feature_gates::with_feature(Feature::UserNamespacesHostNetworkSupport, true);
        assert!(opts(None).allow_user_namespaces_host_network_support);
    }
}
