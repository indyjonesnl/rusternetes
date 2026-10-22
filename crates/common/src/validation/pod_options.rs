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
//! `AllowUserNamespaces*`, `AllowOnlyRecursiveSELinuxChangePolicy`,
//! `AllowSidecarResizePolicy`, `AllowContainerRestartPolicyRules`,
//! `AllowRestartAllContainers`,
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
    /// `AllowPodLifecycleSleepActionZeroValue`: a `sleep` lifecycle action may
    /// be 0 seconds (gate `PodLifecycleSleepActionAllowZero`, or the old spec
    /// already uses a zero sleep).
    pub allow_pod_lifecycle_sleep_action_zero_value: bool,
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
        // util.go:421.
        allow_pod_lifecycle_sleep_action_zero_value: enabled(
            Feature::PodLifecycleSleepActionAllowZero,
        ),
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
        // util.go:477.
        opts.allow_pod_lifecycle_sleep_action_zero_value |=
            pod_lifecycle_sleep_action_zero_value_in_use(old);
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

/// `podLifecycleSleepActionZeroValueInUse` (util.go:981-1001) over
/// `VisitContainers(podSpec, AllContainers, ...)`: any init, regular or
/// ephemeral container with a zero-second `preStop`/`postStart` sleep.
fn pod_lifecycle_sleep_action_zero_value_in_use(spec: &PodSpec) -> bool {
    let zero = |l: &Option<crate::resources::pod::Lifecycle>| {
        l.as_ref().is_some_and(|l| {
            [&l.pre_stop, &l.post_start].into_iter().any(|h| {
                h.as_ref()
                    .and_then(|h| h.sleep.as_ref())
                    .is_some_and(|s| s.seconds == 0)
            })
        })
    };
    spec.init_containers
        .iter()
        .flatten()
        .any(|c| zero(&c.lifecycle))
        || spec.containers.iter().any(|c| zero(&c.lifecycle))
        || spec
            .ephemeral_containers
            .iter()
            .flatten()
            .any(|c| zero(&c.lifecycle))
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

    fn sleep_spec(hook: &str, secs: i64) -> PodSpec {
        spec(json!({"containers": [{"name": "c", "image": "i",
            "lifecycle": {hook: {"sleep": {"seconds": secs}}}}]}))
    }

    /// `TestValidateAllowPodLifecycleSleepActionZeroValue` (util_test.go:4654):
    /// the gate (on in 1.35) and the old-spec zero-sleep relaxation.
    #[test]
    #[serial_test::serial]
    fn allow_pod_lifecycle_sleep_action_zero_value_option() {
        use crate::feature_gates::{with_feature, Feature};
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
}
