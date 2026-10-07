//! Pod `/status` and `/binding` validation — port of
//! `ValidatePodStatusUpdate` and its helpers, and `ValidatePodBinding`
//! (`pkg/apis/core/validation/validation.go`, release-1.35).
//!
//! Not modelled, each a gate or field this tree does not carry:
//!
//! * `RestartAllContainersOnContainerExits` (alpha, off): a
//!   `RestartAllContainers` rule action is not a reason a container may leave
//!   `Terminated`, as `podutil.ContainerShouldRestart` does with the gate off.
//! * `field.InternalError` for a dual-stack check over an unparsable IP: that
//!   IP already drew the `Invalid` of `IsValidIPForLegacyField`.

use std::collections::{HashMap, HashSet};

use crate::resources::pod::{ContainerState, ContainerStatus, PodCondition, PodIP};
use crate::resources::{Binding, Container, Pod, PodSpec};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_immutable_field, validate_object_meta_update,
};
use crate::validation::service::{is_valid_ip_for_legacy_field, parse_ip_sloppy};

/// `api.MirrorPodAnnotationKey`.
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";
/// `v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix`.
const APP_ARMOR_ANNOTATION_PREFIX: &str = "container.apparmor.security.beta.kubernetes.io/";

/// `core.TolerationsAnnotationKey`.
const TOLERATIONS_ANNOTATION_KEY: &str = "scheduler.alpha.kubernetes.io/tolerations";
/// `core.PodDeletionCost`.
const POD_DELETION_COST: &str = "controller.kubernetes.io/pod-deletion-cost";
/// `core.SeccompPodAnnotationKey`.
const SECCOMP_POD_ANNOTATION_KEY: &str = "seccomp.security.alpha.kubernetes.io/pod";
/// `core.SeccompContainerAnnotationKeyPrefix`.
const SECCOMP_CONTAINER_ANNOTATION_KEY_PREFIX: &str =
    "container.seccomp.security.alpha.kubernetes.io/";

/// `ValidatePodBinding` (validation.go:6526-6539).
pub fn validate_pod_binding(binding: &Binding) -> ErrorList {
    let mut errs = Vec::new();
    let target = Path::new("target");
    if let Some(kind) = binding.target.kind.as_deref().filter(|k| !k.is_empty()) {
        if kind != "Node" {
            errs.push(Error::not_supported(
                &target.child("kind"),
                kind.to_string(),
                &["Node", "<empty>"],
            ));
        }
    }
    if binding.target.name.is_empty() {
        errs.push(Error::required(&target.child("name"), ""));
    }
    errs
}

/// `ValidatePodStatusUpdate` (validation.go:6003-6067). The validation
/// options are the 1.35 defaults: `ContainerRestartRules` is beta and on,
/// `RestartAllContainersOnContainerExits` alpha and off.
pub fn validate_pod_status_update(new: &Pod, old: &Pod) -> ErrorList {
    let meta_path = Path::new("metadata");
    let mut errs = validate_object_meta_update(&new.metadata, &old.metadata, &meta_path);
    errs.extend(validate_pod_specific_annotation_updates(
        new,
        old,
        &meta_path.child("annotations"),
    ));

    let status_path = Path::new("status");
    let new_status = new.status.clone().unwrap_or_default();
    let old_status = old.status.clone().unwrap_or_default();
    let new_spec = new.spec.clone().unwrap_or_default();
    let old_spec = old.spec.clone().unwrap_or_default();

    errs.extend(validate_pod_conditions(
        new_status.conditions.as_deref().unwrap_or(&[]),
        &status_path.child("conditions"),
    ));

    let node_name = |s: &PodSpec| s.node_name.clone().unwrap_or_default();
    if node_name(&new_spec) != node_name(&old_spec) {
        errs.push(Error::forbidden(
            &status_path.child("nodeName"),
            "may not be changed directly",
        ));
    }

    let new_nominated = new_status.nominated_node_name.as_deref().unwrap_or("");
    let old_nominated = old_status.nominated_node_name.as_deref().unwrap_or("");
    if new_nominated != old_nominated && !new_nominated.is_empty() {
        for msg in name_is_dns_subdomain(new_nominated, false) {
            errs.push(Error::invalid(
                &status_path.child("nominatedNodeName"),
                new_nominated,
                msg,
            ));
        }
    }

    // Prevent setting NominatedNodeName on already bound pods
    // (`ClearingNominatedNodeNameAfterBinding`, beta and on in 1.35).
    if !node_name(&old_spec).is_empty()
        && !new_nominated.is_empty()
        && new_nominated != old_nominated
    {
        errs.push(Error::forbidden(
            &status_path.child("nominatedNodeName"),
            "may not be set on pods that are already bound to a node",
        ));
    }

    if let Some(generation) = new_status.observed_generation.filter(|g| *g < 0) {
        errs.push(Error::invalid(
            &status_path.child("observedGeneration"),
            generation,
            "must be a non-negative integer",
        ));
    }

    // Pod QoS is immutable.
    errs.extend(validate_immutable_field(
        new_status.qos_class.as_deref().unwrap_or(""),
        old_status.qos_class.as_deref().unwrap_or(""),
        &status_path.child("qosClass"),
    ));

    // Note: there is no check that ContainerStatuses, InitContainerStatuses
    // and EphemeralContainerStatuses don't have duplicate container names or
    // statuses of containers that are not defined in the pod spec (upstream's
    // comment, validation.go:6037-6040).
    let statuses = |s: &Option<Vec<ContainerStatus>>| s.clone().unwrap_or_default();
    let new_containers = statuses(&new_status.container_statuses);
    let new_init = statuses(&new_status.init_container_statuses);
    let new_ephemeral = statuses(&new_status.ephemeral_container_statuses);
    errs.extend(validate_container_state_transition(
        &new_containers,
        &statuses(&old_status.container_statuses),
        &status_path.child("containerStatuses"),
        &old_spec,
        &old_spec.containers,
    ));
    errs.extend(validate_container_state_transition(
        &new_init,
        &statuses(&old_status.init_container_statuses),
        &status_path.child("initContainerStatuses"),
        &old_spec,
        old_spec.init_containers.as_deref().unwrap_or(&[]),
    ));
    errs.extend(validate_ephemeral_container_state_transition(
        &new_ephemeral,
        &statuses(&old_status.ephemeral_container_statuses),
        &status_path.child("ephemeralContainerStatuses"),
    ));
    errs.extend(validate_pod_resource_claim_statuses(
        &new_status,
        &new_spec,
        &status_path.child("resourceClaimStatuses"),
    ));
    errs.extend(validate_pod_extended_resource_claim_status(
        &new_status,
        &new_spec,
        &status_path.child("extendedResourceClaimStatus"),
    ));
    errs.extend(validate_pod_ips(new, old));
    errs.extend(validate_host_ips(new, old));

    let os_name = new_spec.os.as_ref().map(|o| o.name.as_str());
    errs.extend(validate_container_status_users(
        &new_containers,
        &status_path.child("containerStatuses"),
        os_name,
    ));
    errs.extend(validate_container_status_users(
        &new_init,
        &status_path.child("initContainerStatuses"),
        os_name,
    ));
    errs.extend(validate_container_status_users(
        &new_ephemeral,
        &status_path.child("ephemeralContainerStatuses"),
        os_name,
    ));

    errs.extend(validate_container_status_allocated_resources_status(
        &new_containers,
        &status_path.child("containerStatuses"),
        &new_spec.containers,
    ));
    errs.extend(validate_container_status_allocated_resources_status(
        &new_init,
        &status_path.child("initContainerStatuses"),
        new_spec.init_containers.as_deref().unwrap_or(&[]),
    ));
    // ephemeral containers are not allowed to have resources allocated
    for (i, status) in new_ephemeral.iter().enumerate() {
        if status
            .allocated_resources_status
            .as_ref()
            .is_some_and(|a| !a.is_empty())
        {
            errs.push(Error::forbidden(
                &status_path
                    .child("ephemeralContainerStatuses")
                    .index(i)
                    .child("allocatedResourcesStatus"),
                "must not be specified in container status",
            ));
        }
    }
    errs
}

/// `ValidatePodSpecificAnnotationUpdates` (validation.go:235-263).
fn validate_pod_specific_annotation_updates(new: &Pod, old: &Pod, fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    let empty = HashMap::new();
    let new_annotations = new.metadata.annotations.as_ref().unwrap_or(&empty);
    let old_annotations = old.metadata.annotations.as_ref().unwrap_or(&empty);
    for (k, old_val) in old_annotations {
        if new_annotations.get(k) == Some(old_val) {
            continue; // No change.
        }
        if k.starts_with(APP_ARMOR_ANNOTATION_PREFIX) {
            errs.push(Error::forbidden(
                &fld_path.key(k.clone()),
                "may not remove or update AppArmor annotations",
            ));
        }
        if k == MIRROR_POD_ANNOTATION_KEY {
            errs.push(Error::forbidden(
                &fld_path.key(k.clone()),
                "may not remove or update mirror pod annotation",
            ));
        }
    }
    // Check for additions.
    for k in new_annotations.keys() {
        if old_annotations.contains_key(k) {
            continue; // No change.
        }
        if k.starts_with(APP_ARMOR_ANNOTATION_PREFIX) {
            errs.push(Error::forbidden(
                &fld_path.key(k.clone()),
                "may not add AppArmor annotations",
            ));
        }
        if k == MIRROR_POD_ANNOTATION_KEY {
            errs.push(Error::forbidden(
                &fld_path.key(k.clone()),
                "may not add mirror pod annotation",
            ));
        }
    }
    // `GetValidationOptionsFromPodSpecAndMeta` (pkg/api/pod/util.go:414,
    // 488-492): off with the gate, and on an update only when the old
    // annotations were already invalid.
    let allow_invalid_pod_deletion_cost =
        !crate::feature_gates::enabled(crate::feature_gates::Feature::PodDeletionCost)
            || get_deletion_cost_from_pod_annotations(old_annotations).is_err();
    let default_spec = PodSpec::default();
    errs.extend(validate_pod_specific_annotations(
        new_annotations,
        new.spec.as_ref().unwrap_or(&default_spec),
        fld_path,
        allow_invalid_pod_deletion_cost,
    ));
    errs
}

/// The annotation half of `validatePodMetadataAndSpec` (validation.go:4505),
/// run by `ValidatePodCreate` (`old` is `None`) and `ValidatePodUpdate`
/// (validation.go:5699, which also runs `ValidatePodSpecificAnnotationUpdates`).
/// `AllowInvalidPodDeletionCost` comes from
/// `GetValidationOptionsFromPodSpecAndMeta` (pkg/api/pod/util.go:414,488-492):
/// off with the gate, and on an update only when the old annotations were
/// already invalid.
pub fn validate_pod_metadata_annotations(pod: &Pod, old: Option<&Pod>) -> ErrorList {
    let fld_path = Path::new("metadata").child("annotations");
    let empty = HashMap::new();
    let annotations = pod.metadata.annotations.as_ref().unwrap_or(&empty);
    let mut allow_invalid_pod_deletion_cost =
        !crate::feature_gates::enabled(crate::feature_gates::Feature::PodDeletionCost);
    if let Some(old) = old {
        if !allow_invalid_pod_deletion_cost {
            allow_invalid_pod_deletion_cost = get_deletion_cost_from_pod_annotations(
                old.metadata.annotations.as_ref().unwrap_or(&empty),
            )
            .is_err();
        }
    }
    let default_spec = PodSpec::default();
    let mut errs = validate_pod_specific_annotations(
        annotations,
        pod.spec.as_ref().unwrap_or(&default_spec),
        &fld_path,
        allow_invalid_pod_deletion_cost,
    );
    if let Some(old) = old {
        errs.extend(validate_pod_specific_annotation_updates(
            pod, old, &fld_path,
        ));
    }
    errs
}

/// `ValidatePodSpecificAnnotations` (validation.go:193-217). The options
/// are `AllowInvalidPodDeletionCost`; the tolerations annotation is checked
/// with `AllowTaintTolerationComparisonOperators` from the spec itself.
pub(crate) fn validate_pod_specific_annotations(
    annotations: &HashMap<String, String>,
    spec: &PodSpec,
    fld_path: &Path,
    allow_invalid_pod_deletion_cost: bool,
) -> ErrorList {
    let mut errs = Vec::new();

    if let Some(value) = annotations.get(MIRROR_POD_ANNOTATION_KEY) {
        if spec.node_name.as_deref().unwrap_or("").is_empty() {
            errs.push(Error::invalid(
                &fld_path.key(MIRROR_POD_ANNOTATION_KEY.to_string()),
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
            crate::validation::pod::allow_taint_toleration_comparison_operators(Some(spec)),
        ));
    }

    if !allow_invalid_pod_deletion_cost
        && get_deletion_cost_from_pod_annotations(annotations).is_err()
    {
        errs.push(Error::invalid(
            &fld_path.key(POD_DELETION_COST.to_string()),
            annotations
                .get(POD_DELETION_COST)
                .cloned()
                .unwrap_or_default(),
            "must be a 32bit integer",
        ));
    }

    errs.extend(validate_seccomp_pod_annotations(annotations, fld_path));
    errs.extend(validate_app_armor_pod_annotations(
        annotations,
        spec,
        fld_path,
    ));
    errs
}

/// `ValidateTolerationsInPodAnnotations` (validation.go:219-240) over
/// `helper.GetTolerationsFromPodAnnotations` (helper/helpers.go:398-407).
fn validate_tolerations_in_pod_annotations(
    annotations: &HashMap<String, String>,
    fld_path: &Path,
    allow_comparison_operators: bool,
) -> ErrorList {
    let raw = annotations
        .get(TOLERATIONS_ANNOTATION_KEY)
        .map(String::as_str)
        .unwrap_or("");
    match serde_json::from_str::<Option<Vec<crate::resources::pod::Toleration>>>(raw) {
        Err(err) => vec![Error::invalid(
            fld_path,
            TOLERATIONS_ANNOTATION_KEY.to_string(),
            err.to_string(),
        )],
        Ok(tolerations) => {
            let tolerations = tolerations.unwrap_or_default();
            if tolerations.is_empty() {
                return Vec::new();
            }
            crate::validation::pod::validate_tolerations_with_options(
                &tolerations,
                &fld_path.child(TOLERATIONS_ANNOTATION_KEY),
                allow_comparison_operators,
            )
        }
    }
}

/// `helper.GetDeletionCostFromPodAnnotations` (helper/helpers.go:491-513):
/// a value that starts with a plus sign or a leading zero is not valid.
fn get_deletion_cost_from_pod_annotations(
    annotations: &HashMap<String, String>,
) -> Result<i32, String> {
    let Some(value) = annotations.get(POD_DELETION_COST) else {
        return Ok(0);
    };
    let valid_first_digit = match value.as_bytes().first() {
        None => false,
        Some(b'-') => true,
        Some(b'0') => value == "0",
        Some(c) => c.is_ascii_digit(),
    };
    if !valid_first_digit {
        return Err(format!("invalid value {value:?}"));
    }
    value.parse::<i32>().map_err(|e| e.to_string())
}

/// `ValidateSeccompProfile` (validation.go:5268-5279).
fn validate_seccomp_profile(p: &str, fld_path: &Path) -> ErrorList {
    if matches!(p, "runtime/default" | "docker/default" | "unconfined") {
        return Vec::new();
    }
    if let Some(rest) = p.strip_prefix("localhost/") {
        return crate::validation::pod::validate_local_descending_path(rest, fld_path);
    }
    vec![Error::invalid(
        fld_path,
        p.to_string(),
        "must be a valid seccomp profile",
    )]
}

/// `ValidateSeccompPodAnnotations` (validation.go:5281-5293).
fn validate_seccomp_pod_annotations(
    annotations: &HashMap<String, String>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    if let Some(p) = annotations.get(SECCOMP_POD_ANNOTATION_KEY) {
        errs.extend(validate_seccomp_profile(
            p,
            &fld_path.child(SECCOMP_POD_ANNOTATION_KEY),
        ));
    }
    for (k, p) in annotations {
        if k.starts_with(SECCOMP_CONTAINER_ANNOTATION_KEY_PREFIX) {
            errs.extend(validate_seccomp_profile(p, &fld_path.child(k)));
        }
    }
    errs
}

/// `ValidateAppArmorPodAnnotations` (validation.go:5349-5366) and
/// `ValidateAppArmorProfileFormat` (:5368-5376).
fn validate_app_armor_pod_annotations(
    annotations: &HashMap<String, String>,
    spec: &PodSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    for (k, p) in annotations {
        let Some(container_name) = k.strip_prefix(APP_ARMOR_ANNOTATION_PREFIX) else {
            continue;
        };
        // podSpecHasContainer: containers, init containers, ephemeral containers.
        let has_container = spec.containers.iter().any(|c| c.name == container_name)
            || spec
                .init_containers
                .iter()
                .flatten()
                .any(|c| c.name == container_name)
            || spec
                .ephemeral_containers
                .iter()
                .flatten()
                .any(|c| c.name == container_name);
        if !has_container {
            errs.push(Error::invalid(
                &fld_path.key(k.clone()),
                container_name.to_string(),
                "container not found",
            ));
        }
        let valid = p.is_empty()
            || matches!(p.as_str(), "runtime/default" | "unconfined")
            || p.starts_with("localhost/");
        if !valid {
            errs.push(Error::invalid(
                &fld_path.key(k.clone()),
                p.clone(),
                format!("invalid AppArmor profile name: {p:?}"),
            ));
        }
    }
    errs
}

/// `validatePodConditions` (validation.go:6071-6087): custom condition types
/// are qualified names, `observedGeneration` is non-negative.
fn validate_pod_conditions(conditions: &[PodCondition], fld_path: &Path) -> ErrorList {
    let mut errs = Vec::new();
    for (i, condition) in conditions.iter().enumerate() {
        if let Some(generation) = condition.observed_generation.filter(|g| *g < 0) {
            errs.push(Error::invalid(
                &fld_path.index(i).child("observedGeneration"),
                generation,
                "must be a non-negative integer",
            ));
        }
        if matches!(
            condition.condition_type.as_str(),
            "PodScheduled" | "Ready" | "Initialized"
        ) {
            continue;
        }
        // ValidateQualifiedName (validation.go:167-173).
        for msg in is_qualified_name(&condition.condition_type) {
            errs.push(
                Error::invalid(
                    &fld_path.index(i).child("Type"),
                    condition.condition_type.clone(),
                    msg,
                )
                .with_origin("format=qualified-name"),
            );
        }
    }
    errs
}

/// `podutil.ContainerShouldRestart` (pkg/api/v1/pod/util.go:419-460) with
/// `RestartAllContainersOnContainerExits` off: the first matching container
/// rule, then the container's own policy, then the pod's.
fn container_should_restart(container: &Container, spec: &PodSpec, exit_code: i32) -> bool {
    if let Some(container_policy) = container.restart_policy.as_deref() {
        // FindMatchingContainerRestartRule (util.go:465-489).
        let matched = container
            .restart_policy_rules
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .find(|rule| {
                let Some(exit_codes) = rule.exit_codes.as_ref() else {
                    return false;
                };
                let hit = exit_codes
                    .values
                    .as_deref()
                    .unwrap_or(&[])
                    .contains(&exit_code);
                match exit_codes.operator.as_str() {
                    "In" => hit,
                    "NotIn" => !hit,
                    _ => false,
                }
            });
        if matched.is_some_and(|rule| rule.action == "Restart") {
            return true;
        }
        match container_policy {
            "Always" => return true,
            "OnFailure" => return exit_code != 0,
            "Never" => return false,
            _ => {}
        }
    }
    match spec.restart_policy.as_deref() {
        Some("OnFailure") => exit_code != 0,
        Some("Never") => false,
        // Always, and the default.
        _ => true,
    }
}

fn terminated_exit_code(status: &ContainerStatus) -> Option<i32> {
    match status.state {
        Some(ContainerState::Terminated { exit_code, .. }) => Some(exit_code),
        _ => None,
    }
}

/// `ValidateContainerStateTransition` and `ValidateInitContainerStateTransition`
/// (validation.go:5841-5990) with `ContainerRestartRules` on: a terminated
/// container may become non-terminated only where `ContainerShouldRestart`
/// says it would restart. `containers` are the spec's containers (or init
/// containers) the statuses belong to; a status naming none is not allowed.
fn validate_container_state_transition(
    new_statuses: &[ContainerStatus],
    old_statuses: &[ContainerStatus],
    fld_path: &Path,
    spec: &PodSpec,
    containers: &[Container],
) -> ErrorList {
    let mut errs = Vec::new();
    for (i, old_status) in old_statuses.iter().enumerate() {
        // Skip any container that is not terminated.
        let Some(exit_code) = terminated_exit_code(old_status) else {
            continue;
        };
        if let Some(_new_status) = new_statuses
            .iter()
            .find(|n| n.name == old_status.name && terminated_exit_code(n).is_none())
        {
            let allowed = containers
                .iter()
                .find(|c| c.name == old_status.name)
                .is_some_and(|c| container_should_restart(c, spec, exit_code));
            if !allowed {
                errs.push(Error::forbidden(
                    &fld_path.index(i).child("state"),
                    "may not be transitioned to non-terminated state",
                ));
            }
        }
    }
    errs
}

/// `ValidateEphemeralContainerStateTransition` (validation.go:5992-6002).
fn validate_ephemeral_container_state_transition(
    new_statuses: &[ContainerStatus],
    old_statuses: &[ContainerStatus],
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    for (i, old_status) in old_statuses.iter().enumerate() {
        if terminated_exit_code(old_status).is_none() {
            continue;
        }
        for new_status in new_statuses {
            if old_status.name == new_status.name && terminated_exit_code(new_status).is_none() {
                errs.push(Error::forbidden(
                    &fld_path.index(i).child("state"),
                    "may not be transitioned to non-terminated state",
                ));
            }
        }
    }
    errs
}

/// `validatePodResourceClaimStatuses` (validation.go:6090-6114).
fn validate_pod_resource_claim_statuses(
    status: &crate::resources::PodStatus,
    spec: &PodSpec,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    let pod_claims = spec.resource_claims.as_deref().unwrap_or(&[]);
    let mut seen = HashSet::new();
    for (i, claim_status) in status
        .resource_claim_statuses
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .enumerate()
    {
        let idx_path = fld_path.index(i);
        // There's no need to check the content of the name. If it matches an
        // entry, then it is valid, otherwise we reject it here.
        if !pod_claims.iter().any(|c| c.name == claim_status.name) {
            errs.push(Error::invalid(
                &idx_path.child("name"),
                claim_status.name.clone(),
                "must match the name of an entry in `spec.resourceClaims`",
            ));
        }
        if !seen.insert(claim_status.name.clone()) {
            errs.push(Error::duplicate(
                &idx_path.child("name"),
                claim_status.name.clone(),
            ));
        }
        if let Some(claim_name) = claim_status.resource_claim_name.as_deref() {
            for detail in name_is_dns_subdomain(claim_name, false) {
                errs.push(Error::invalid(&idx_path.child("name"), claim_name, detail));
            }
        }
    }
    errs
}

/// `validatePodExtendedResourceClaimStatus` (validation.go:6117-6167).
fn validate_pod_extended_resource_claim_status(
    status: &crate::resources::PodStatus,
    spec: &PodSpec,
    fld_path: &Path,
) -> ErrorList {
    let Some(claim_status) = status.extended_resource_claim_status.as_ref() else {
        return Vec::new();
    };
    let mut containers: HashMap<&str, &Container> = HashMap::new();
    for c in spec.init_containers.as_deref().unwrap_or(&[]) {
        containers.insert(&c.name, c);
    }
    for c in &spec.containers {
        containers.insert(&c.name, c);
    }

    let mut errs = Vec::new();
    let rm_path = fld_path.child("requestMappings");
    if claim_status.request_mappings.is_empty() {
        errs.push(Error::required(
            &rm_path,
            "at least one request mapping is required",
        ));
    }
    let mut seen = HashSet::new();
    for (i, rm) in claim_status.request_mappings.iter().enumerate() {
        let idx_path = rm_path.index(i);
        match containers.get(rm.container_name.as_str()) {
            Some(c) => {
                let requested = c
                    .resources
                    .as_ref()
                    .and_then(|r| r.requests.as_ref())
                    .is_some_and(|r| r.contains_key(&rm.resource_name));
                if !requested {
                    errs.push(Error::invalid(
                        &idx_path.child("resourceName"),
                        rm.resource_name.clone(),
                        "must match the extended resource name of an entry in spec.initContainers.resources.requests or spec.containers.resources.requests",
                    ));
                }
            }
            None => errs.push(Error::invalid(
                &idx_path.child("containerName"),
                rm.container_name.clone(),
                "must match the name of an entry in spec.initContainers.name or spec.containers.name",
            )),
        }
        for detail in crate::validation::metav1::is_dns1123_label(&rm.request_name) {
            errs.push(Error::invalid(
                &fld_path.child("requestName"),
                rm.request_name.clone(),
                detail,
            ));
        }
        let key = (
            rm.container_name.clone(),
            rm.resource_name.clone(),
            rm.request_name.clone(),
        );
        if !seen.insert(key) {
            errs.push(Error::duplicate(
                &idx_path.child("containerName"),
                rm.container_name.clone(),
            ));
            errs.push(Error::duplicate(
                &idx_path.child("resourceName"),
                rm.resource_name.clone(),
            ));
            errs.push(Error::duplicate(
                &idx_path.child("requestName"),
                rm.request_name.clone(),
            ));
        }
    }
    if claim_status.resource_claim_name.is_empty() {
        errs.push(Error::required(&fld_path.child("resourceClaimName"), ""));
    } else {
        for detail in name_is_dns_subdomain(&claim_status.resource_claim_name, false) {
            errs.push(Error::invalid(
                &fld_path.child("resourceClaimName"),
                claim_status.resource_claim_name.clone(),
                detail,
            ));
        }
    }
    errs
}

/// Whether `ips` hold at least one address of each family
/// (`netutils.IsDualStackIPStrings`); an unparsable one is not dual-stack.
fn is_dual_stack(ips: &[&str]) -> bool {
    let mut v4 = false;
    let mut v6 = false;
    for ip in ips {
        match parse_ip_sloppy(ip) {
            Some(addr) if addr.is_ipv4() => v4 = true,
            Some(_) => v6 = true,
            None => return false,
        }
    }
    v4 && v6
}

/// `validatePodIPs` (validation.go:4533-4570).
fn validate_pod_ips(pod: &Pod, old: &Pod) -> ErrorList {
    let mut errs = Vec::new();
    let field = Path::new("status").child("podIPs");
    let ips: &[PodIP] = pod
        .status
        .as_ref()
        .and_then(|s| s.pod_i_ps.as_deref())
        .unwrap_or(&[]);
    // all new PodIPs must be valid IPs, but existing invalid ones can be kept.
    let existing: Vec<String> = old
        .status
        .as_ref()
        .and_then(|s| s.pod_i_ps.as_deref())
        .unwrap_or(&[])
        .iter()
        .map(|p| p.ip.clone())
        .collect();
    for (i, pod_ip) in ips.iter().enumerate() {
        errs.extend(is_valid_ip_for_legacy_field(
            &field.index(i),
            &pod_ip.ip,
            &existing,
        ));
    }
    // if we have more than one Pod.PodIP then we must have a dual-stack pair
    if ips.len() > 1 {
        let strs: Vec<&str> = ips.iter().map(|p| p.ip.as_str()).collect();
        // We only support one from each IP family (i.e. max two IPs).
        if !is_dual_stack(&strs) || strs.len() > 2 {
            errs.push(Error::invalid(
                &field,
                serde_json::to_value(ips).unwrap_or_default(),
                "may specify no more than one IP for each IP family",
            ));
        }
    }
    errs
}

/// `validateHostIPs` (validation.go:4572-4616).
fn validate_host_ips(pod: &Pod, old: &Pod) -> ErrorList {
    let mut errs = Vec::new();
    let status = pod.status.clone().unwrap_or_default();
    let ips = status.host_i_ps.as_deref().unwrap_or(&[]);
    if ips.is_empty() {
        return errs;
    }
    let field = Path::new("status").child("hostIPs");
    // hostIP must be equal to hostIPs[0].IP
    if status.host_ip.as_deref().unwrap_or("") != ips[0].ip {
        errs.push(Error::invalid(
            &field.index(0).child("ip"),
            ips[0].ip.clone(),
            "must be equal to `hostIP`",
        ));
    }
    // all new HostIPs must be valid IPs, but existing invalid ones can be kept.
    let existing: Vec<String> = old
        .status
        .as_ref()
        .and_then(|s| s.host_i_ps.as_deref())
        .unwrap_or(&[])
        .iter()
        .map(|p| p.ip.clone())
        .collect();
    for (i, host_ip) in ips.iter().enumerate() {
        errs.extend(is_valid_ip_for_legacy_field(
            &field.index(i),
            &host_ip.ip,
            &existing,
        ));
    }
    // if we have more than one Pod.HostIP then we must have a dual-stack pair
    if ips.len() > 1 {
        let strs: Vec<&str> = ips.iter().map(|p| p.ip.as_str()).collect();
        if !is_dual_stack(&strs) || strs.len() > 2 {
            errs.push(Error::invalid(
                &field,
                serde_json::to_value(ips).unwrap_or_default(),
                "may specify no more than one IP for each IP family",
            ));
        }
    }
    errs
}

/// `IsValidUserID` / `IsValidGroupID` (apimachinery util/validation/
/// validation.go:296-309): `[0, MaxInt32]`.
fn is_valid_id(id: i64) -> Vec<String> {
    if (0..=i32::MAX as i64).contains(&id) {
        return Vec::new();
    }
    vec![format!("must be between 0 and {}, inclusive", i32::MAX)]
}

/// `validateContainerStatusUsers` (validation.go:9392-9413).
fn validate_container_status_users(
    statuses: &[ContainerStatus],
    fld_path: &Path,
    os_name: Option<&str>,
) -> ErrorList {
    let mut errs = Vec::new();
    for (i, status) in statuses.iter().enumerate() {
        let Some(user) = status.user.as_ref() else {
            continue;
        };
        let linux_path = fld_path.index(i).child("user").child("linux");
        match os_name.unwrap_or("linux") {
            "windows" => {
                if user.linux.is_some() {
                    errs.push(Error::forbidden(
                        &linux_path,
                        "cannot be set for a windows pod",
                    ));
                }
            }
            "linux" => {
                // validateLinuxContainerUser (validation.go:9516-9534).
                let Some(linux) = user.linux.as_ref() else {
                    continue;
                };
                for msg in is_valid_id(linux.uid) {
                    errs.push(Error::invalid(&linux_path.child("uid"), linux.uid, msg));
                }
                for msg in is_valid_id(linux.gid) {
                    errs.push(Error::invalid(&linux_path.child("gid"), linux.gid, msg));
                }
                for (g, gid) in linux
                    .supplemental_groups
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .enumerate()
                {
                    for msg in is_valid_id(*gid) {
                        errs.push(Error::invalid(
                            &linux_path.child("supplementalGroups").index(g),
                            *gid,
                            msg,
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    errs
}

/// `validateContainerStatusAllocatedResourcesStatus` (validation.go:9431-):
/// each reported resource is one the container asks for, and its resource IDs
/// are unique and carry a known health.
fn validate_container_status_allocated_resources_status(
    statuses: &[ContainerStatus],
    fld_path: &Path,
    containers: &[Container],
) -> ErrorList {
    let mut errs = Vec::new();
    for (i, status) in statuses.iter().enumerate() {
        let Some(allocated) = status.allocated_resources_status.as_ref() else {
            continue;
        };
        for (j, resource) in allocated.iter().enumerate() {
            let res_path = fld_path.index(i).child("allocatedResourcesStatus").index(j);
            // ignore missing container, see kubernetes/kubernetes#124915
            if let Some(container) = containers.iter().find(|c| c.name == status.name) {
                let resources = container.resources.as_ref();
                let (found, error_str) = if resource.name.starts_with("claim:") {
                    // assume it is a claim name
                    let found = resources
                        .and_then(|r| r.claims.as_ref())
                        .into_iter()
                        .flatten()
                        .any(|c| {
                            let mut name = format!("claim:{}", c.name);
                            if let Some(request) = c.request.as_deref().filter(|r| !r.is_empty()) {
                                name.push('/');
                                name.push_str(request);
                            }
                            name == resource.name
                        });
                    (
                        found,
                        "must match one of the container's resource claims in a format 'claim:<claimName>/<request>' or 'claim:<claimName>' if request is empty",
                    )
                } else {
                    // assume it is a resource name
                    let found = resources
                        .and_then(|r| r.requests.as_ref())
                        .is_some_and(|r| r.contains_key(&resource.name));
                    (found, "must match one of the container's resource requests")
                };
                if !found {
                    errs.push(Error::invalid(
                        &res_path.child("name"),
                        resource.name.clone(),
                        error_str,
                    ));
                }
            }

            // check resource IDs are unique
            let mut unique = HashSet::new();
            for (k, r) in resource
                .resources
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .enumerate()
            {
                let health = r.health.as_deref().unwrap_or("");
                if !matches!(health, "Healthy" | "Unhealthy" | "Unknown") {
                    errs.push(Error::not_supported(
                        &res_path.child("resources").index(k).child("health"),
                        health,
                        &["Healthy", "Unhealthy", "Unknown"],
                    ));
                }
                if !unique.insert(r.resource_id.clone()) {
                    errs.push(Error::duplicate(
                        &res_path.child("resources").index(k).child("resourceID"),
                        r.resource_id.clone(),
                    ));
                }
            }
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
