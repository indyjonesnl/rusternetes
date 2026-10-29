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
//! gated rule. The remaining upstream options (`AllowOnlyRecursiveSELinuxChangePolicy`,
//! `AllowSidecarResizePolicy`, `AllowContainerRestartPolicyRules`,
//! `AllowRestartAllContainers`,
//! `PodLevelResourcesEnabled`, `InPlacePodLevelResourcesVerticalScalingEnabled`)
//! are tracked in #3572 because the rule they gate is not ported yet.

use crate::feature_gates::{enabled, Feature};
use crate::quantity::Quantity;
use crate::quota::{is_hugepage_resource_name, is_hugepage_resource_value_divisible};
use crate::resources::pod::{EnvFromSource, EnvVar, PodSpec};
use crate::resources::workloads::PodTemplateSpec;
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
    /// `AllowIndivisibleHugePagesValues`: the old spec has a hugepages
    /// quantity that is not a whole multiple of its page size.
    pub allow_indivisible_huge_pages_values: bool,
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
    /// `AllowOnlyRecursiveSELinuxChangePolicy`: `seLinuxChangePolicy` may only
    /// be `Recursive` (gate `SELinuxMount` off and the old spec has no policy).
    pub allow_only_recursive_se_linux_change_policy: bool,
    /// `AllowTaintTolerationComparisonOperators`.
    pub allow_taint_toleration_comparison_operators: bool,
    /// `AllowPodLifecycleSleepActionZeroValue`: a `sleep` lifecycle action may
    /// be 0 seconds (gate `PodLifecycleSleepActionAllowZero`, or the old spec
    /// already uses a zero sleep).
    pub allow_pod_lifecycle_sleep_action_zero_value: bool,
    /// `AllowRelaxedEnvironmentVariableValidation`.
    pub allow_relaxed_environment_variable_validation: bool,
    /// `AllowEnvFilesValidation`.
    pub allow_env_files_validation: bool,
    /// `AllowUserNamespacesWithVolumeDevices`.
    pub allow_user_namespaces_with_volume_devices: bool,
    /// `AllowUserNamespacesHostNetworkSupport`.
    pub allow_user_namespaces_host_network_support: bool,
    /// `PodLevelResourcesEnabled` (gate `PodLevelResources`, util.go:422):
    /// `spec.resources` is validated.
    pub pod_level_resources_enabled: bool,
    /// `InPlacePodLevelResourcesVerticalScalingEnabled` (gate
    /// `InPlacePodLevelResourcesVerticalScaling`, util.go:427): a pod with
    /// pod-level resources may be resized.
    pub in_place_pod_level_resources_vertical_scaling_enabled: bool,
    /// `AllowSidecarResizePolicy`: a restartable init container may carry a
    /// `resizePolicy` (gate `InPlacePodVerticalScaling`, or the old spec
    /// already has one on a sidecar).
    pub allow_sidecar_resize_policy: bool,
    /// `AllowContainerRestartPolicyRules` (gate `ContainerRestartRules`, or the
    /// old spec already uses container restart policy/rules): containers may
    /// carry `restartPolicy` and `restartPolicyRules`.
    pub allow_container_restart_policy_rules: bool,
    /// `AllowRestartAllContainers` (gate `RestartAllContainersOnContainerExits`,
    /// or the old spec already uses the action): the `RestartAllContainers`
    /// rule action, and rules on sidecar containers.
    pub allow_restart_all_containers: bool,
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
        // util.go:421.
        allow_pod_lifecycle_sleep_action_zero_value: enabled(
            Feature::PodLifecycleSleepActionAllowZero,
        ),
        // util.go:422.
        pod_level_resources_enabled: enabled(Feature::PodLevelResources),
        // util.go:427.
        in_place_pod_level_resources_vertical_scaling_enabled: enabled(
            Feature::InPlacePodLevelResourcesVerticalScaling,
        ),
        // util.go:424.
        allow_sidecar_resize_policy: enabled(Feature::InPlacePodVerticalScaling),
        // util.go:430.
        allow_container_restart_policy_rules: enabled(Feature::ContainerRestartRules),
        // util.go:433.
        allow_restart_all_containers: enabled(Feature::RestartAllContainersOnContainerExits),
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
    // util.go:443 `useOnlyRecursiveSELinuxChangePolicy(oldPodSpec)`.
    opts.allow_only_recursive_se_linux_change_policy =
        use_only_recursive_se_linux_change_policy(old_spec);
    // util.go:444 `allowTaintTolerationComparisonOperators(oldPodSpec)`.
    opts.allow_taint_toleration_comparison_operators =
        allow_taint_toleration_comparison_operators(old_spec);

    // util.go:441 `useAllowUserNamespacesHostNetworkSupport(oldPodSpec)`.
    opts.allow_user_namespaces_host_network_support =
        use_allow_user_namespaces_host_network_support(old_spec);

    if let Some(old) = old_spec {
        // util.go:446-448: if old spec used non-integer multiple of huge page
        // unit size, we must allow it.
        opts.allow_indivisible_huge_pages_values = uses_indivisible_huge_pages_values(old);
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
        // util.go:486: an old spec with userns and volume devices (which
        // doesn't work) may still be modified.
        opts.allow_user_namespaces_with_volume_devices =
            has_user_namespaces_with_volume_devices(old);
        // util.go:479: an old resize policy on a restartable init container
        // must stay updatable.
        opts.allow_sidecar_resize_policy |= has_restartable_init_container_resize_policy(old);
        // util.go:481-482.
        opts.allow_container_restart_policy_rules |= container_restart_rules_in_use(old);
        opts.allow_restart_all_containers |= restart_all_containers_action_in_use(old);
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

/// `checkContainerUseIndivisibleHugePagesValues` (util.go:313-331).
fn check_container_use_indivisible_huge_pages_values(c: &crate::resources::pod::Container) -> bool {
    let Some(res) = c.resources.as_ref() else {
        return false;
    };
    [res.limits.as_ref(), res.requests.as_ref()]
        .into_iter()
        .flatten()
        .flatten()
        .any(|(name, qty)| indivisible_huge_page_quantity(name, qty))
}

/// `IsHugePageResourceName` and not `IsHugePageResourceValueDivisible`. A value
/// that does not parse cannot be stored, so it is not an indivisible one.
fn indivisible_huge_page_quantity(name: &str, qty: &str) -> bool {
    is_hugepage_resource_name(name)
        && Quantity::parse(qty).is_ok_and(|q| !is_hugepage_resource_value_divisible(name, &q))
}

/// `usesIndivisibleHugePagesValues` (util.go:333-359): any init/regular
/// container (`VisitContainers(..., AllContainers, ...)`; an ephemeral
/// container carries no resources) or the pod overhead.
fn uses_indivisible_huge_pages_values(spec: &PodSpec) -> bool {
    spec.init_containers
        .iter()
        .flatten()
        .chain(spec.containers.iter())
        .any(check_container_use_indivisible_huge_pages_values)
        || spec
            .overhead
            .iter()
            .flatten()
            .any(|(name, qty)| indivisible_huge_page_quantity(name, qty))
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

/// `GetValidationOptionsFromPodTemplate` (util.go:659-673): the options for a
/// pod template, derived from the OLD template's spec. Either template may be
/// absent (a replication controller's `spec.template` is a pointer), in which
/// case its spec is passed on as nil, exactly like upstream.
pub fn get_validation_options_from_pod_template(
    template: Option<&PodTemplateSpec>,
    old_template: Option<&PodTemplateSpec>,
) -> PodValidationOptions {
    get_validation_options_from_pod_spec_and_meta(
        template.map(|t| &t.spec),
        old_template.map(|t| &t.spec),
    )
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

/// `seLinuxChangePolicyInUse` (util.go:1628-1633).
fn se_linux_change_policy_in_use(spec: Option<&PodSpec>) -> bool {
    spec.and_then(|s| s.security_context.as_ref())
        .is_some_and(|sc| sc.se_linux_change_policy.is_some())
}

/// `useOnlyRecursiveSELinuxChangePolicy` (util.go:1635-1650): with the
/// `SELinuxMount` gate on every policy is allowed; otherwise only `Recursive`,
/// unless the old spec already carries any policy.
fn use_only_recursive_se_linux_change_policy(old_spec: Option<&PodSpec>) -> bool {
    if enabled(Feature::SELinuxMount) {
        return false;
    }
    !se_linux_change_policy_in_use(old_spec)
}

/// `hasRestartableInitContainerResizePolicy` (util.go:1682-1693): any init
/// container with `restartPolicy: Always` and a non-empty `resizePolicy`.
fn has_restartable_init_container_resize_policy(spec: &PodSpec) -> bool {
    spec.init_containers.iter().flatten().any(|c| {
        c.restart_policy.as_deref() == Some("Always")
            && c.resize_policy.as_ref().is_some_and(|p| !p.is_empty())
    })
}

/// `containerRestartRulesInUse` (util.go:1814-1832): an init container with a
/// restart policy other than `Always` or with rules, or a regular container
/// with any restart policy.
fn container_restart_rules_in_use(spec: &PodSpec) -> bool {
    spec.init_containers.iter().flatten().any(|c| {
        c.restart_policy.as_deref().is_some_and(|p| p != "Always")
            || !c.restart_policy_rules.as_deref().unwrap_or(&[]).is_empty()
    }) || spec.containers.iter().any(|c| c.restart_policy.is_some())
}

/// `restartAllContainersActionInUse` (util.go:1850-1873): any
/// `RestartAllContainers` rule action, or rules on a sidecar init container.
fn restart_all_containers_action_in_use(spec: &PodSpec) -> bool {
    let action = |c: &crate::resources::pod::Container| {
        c.restart_policy_rules
            .iter()
            .flatten()
            .any(|r| r.action == "RestartAllContainers")
    };
    spec.containers.iter().any(action)
        || spec.init_containers.iter().flatten().any(|c| {
            action(c)
                || (c.restart_policy.as_deref() == Some("Always")
                    && !c.restart_policy_rules.as_deref().unwrap_or(&[]).is_empty())
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
        use crate::feature_gates::{with_feature, Feature};
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
        use crate::validation::pod::validate_pod_spec_with_options;
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
        use crate::feature_gates::{with_feature, Feature};
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
        use crate::validation::pod::validate_pod_spec_with_options;
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
        use crate::feature_gates::{with_feature, Feature};
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
        use crate::validation::pod::validate_pod_spec_with_options;
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
