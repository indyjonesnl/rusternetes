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
pub fn has_legacy_invalid_topology_spread_constrain_match_label_keys(spec: &PodSpec) -> bool {
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
pub fn container_restart_rules_in_use(spec: &PodSpec) -> bool {
    spec.init_containers.iter().flatten().any(|c| {
        c.restart_policy.as_deref().is_some_and(|p| p != "Always")
            || !c.restart_policy_rules.as_deref().unwrap_or(&[]).is_empty()
    }) || spec.containers.iter().any(|c| c.restart_policy.is_some())
}

/// `restartAllContainersActionInUse` (util.go:1850-1873): any
/// `RestartAllContainers` rule action, or rules on a sidecar init container.
pub fn restart_all_containers_action_in_use(spec: &PodSpec) -> bool {
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
