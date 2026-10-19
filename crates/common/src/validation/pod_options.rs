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
//! `AllowRestartAllContainers`, `AllowPodLifecycleSleepActionZeroValue`,
//! `PodLevelResourcesEnabled`, `InPlacePodLevelResourcesVerticalScalingEnabled`)
//! are tracked in #3572 because the rule they gate is not ported yet.

use crate::feature_gates::{enabled, Feature};
use crate::resources::pod::{EnvFromSource, EnvVar, PodSpec};
use crate::types::LabelSelector;
use crate::validation::field::Path;
use crate::validation::metav1::{
    is_env_var_name, is_relaxed_env_var_name, is_valid_label_value, validate_label_selector,
    LabelSelectorValidationOptions,
};
use crate::validation::pod::{
    allow_taint_toleration_comparison_operators, get_sysctl_namespace,
    validate_local_non_reserved_path, validate_match_label_keys,
    validate_match_label_keys_and_mismatch_label_keys, validate_pod_affinity_term_selector,
    SysctlNamespace,
};
use std::collections::HashSet;

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
    /// `AllowRelaxedEnvironmentVariableValidation`.
    pub allow_relaxed_environment_variable_validation: bool,
    /// `AllowEnvFilesValidation`.
    pub allow_env_files_validation: bool,
}

/// `GetValidationOptionsFromPodSpecAndMeta` (util.go:411-495) for the options
/// in [`PodValidationOptions`]. `old_spec` is `None` on create.
///
/// The `podMeta`/`oldPodMeta` arguments only feed `AllowInvalidPodDeletionCost`,
/// which the pod metadata validators derive themselves
/// (`validate_pod_metadata_annotations`).
pub fn get_validation_options_from_pod_spec_and_meta(
    spec: Option<&PodSpec>,
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
    // util.go:438 `useRelaxedEnvironmentVariableValidation(podSpec, oldPodSpec)`.
    opts.allow_relaxed_environment_variable_validation =
        use_relaxed_environment_variable_validation(spec, old_spec);
    // util.go:440 `useAllowEnvFilesValidation(oldPodSpec)`.
    opts.allow_env_files_validation = use_allow_env_files_validation(old_spec);
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

/// `useRelaxedEnvironmentVariableValidation` (util.go:497-519).
fn use_relaxed_environment_variable_validation(
    spec: Option<&PodSpec>,
    old_spec: Option<&PodSpec>,
) -> bool {
    if enabled(Feature::RelaxedEnvironmentVariableValidation) {
        return true;
    }
    let old_names = old_spec.map(gather_pod_env_var_names).unwrap_or_default();
    let names = spec.map(gather_pod_env_var_names).unwrap_or_default();
    names
        .iter()
        .any(|name| relaxed_env_var_used(name, &old_names))
}

/// `gatherPodEnvVarNames` (util.go:599-634): every env name and every envFrom
/// prefix, over containers, init containers and ephemeral containers.
fn gather_pod_env_var_names(spec: &PodSpec) -> HashSet<String> {
    let mut names = HashSet::new();
    let mut add = |env: &Option<Vec<EnvVar>>, env_from: &Option<Vec<EnvFromSource>>| {
        for e in env.iter().flatten() {
            names.insert(e.name.clone());
        }
        for e in env_from.iter().flatten() {
            names.insert(e.prefix.clone().unwrap_or_default());
        }
    };
    for c in &spec.containers {
        add(&c.env, &c.env_from);
    }
    for c in spec.init_containers.iter().flatten() {
        add(&c.env, &c.env_from);
    }
    for c in spec.ephemeral_containers.iter().flatten() {
        add(&c.env, &c.env_from);
    }
    names
}

/// `relaxedEnvVarUsed` (util.go:635-656).
fn relaxed_env_var_used(name: &str, old_names: &HashSet<String>) -> bool {
    // Not an update, or the old pod has no env: the gate decides.
    if old_names.is_empty() {
        return false;
    }
    if is_env_var_name(name).is_empty() || !is_relaxed_env_var_name(name).is_empty() {
        // Valid under strict rules, or invalid under relaxed rules: strict.
        return false;
    }
    // Failed strict, passed relaxed: grandfathered when already in use.
    old_names.contains(name)
}

/// `useAllowEnvFilesValidation` (util.go:561-587) with `hasEnvFileKeyRef`.
fn use_allow_env_files_validation(old_spec: Option<&PodSpec>) -> bool {
    if enabled(Feature::EnvFiles) {
        return true;
    }
    let Some(old) = old_spec else {
        return false;
    };
    let has = |env: &Option<Vec<EnvVar>>| {
        env.iter().flatten().any(|e| {
            e.value_from
                .as_ref()
                .is_some_and(|v| v.file_key_ref.is_some())
        })
    };
    old.containers.iter().any(|c| has(&c.env))
        || old.init_containers.iter().flatten().any(|c| has(&c.env))
        || old
            .ephemeral_containers
            .iter()
            .flatten()
            .any(|c| has(&c.env))
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

    /// `relaxedEnvVarUsed` / `useAllowEnvFilesValidation` derivation with the
    /// gates off (util.go:497, :561): only an old spec that already uses the
    /// relaxed name / a fileKeyRef keeps the option.
    #[test]
    #[serial_test::serial]
    fn env_options_from_old_spec_when_gates_off() {
        let _a = crate::feature_gates::with_feature(
            Feature::RelaxedEnvironmentVariableValidation,
            false,
        );
        let _b = crate::feature_gates::with_feature(Feature::EnvFiles, false);
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
}
