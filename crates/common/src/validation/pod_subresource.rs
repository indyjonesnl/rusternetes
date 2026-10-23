//! Validation of the Pod `/ephemeralcontainers` and `/resize` subresource
//! updates: ports of `ValidatePodEphemeralContainersUpdate` and
//! `ValidatePodResize` (`pkg/apis/core/validation/validation.go:6181-6523`,
//! release-1.35) with their helpers.
//!
//! The `InPlacePodLevelResourcesVerticalScaling` branch
//! (`validatePodLevelResourcesResize`, validation.go:6384-6412) runs when the
//! gate is on; it is alpha and off by default in v1.35 (kube_features.go:1372),
//! so a pod with pod-level resources is then refused as upstream does.

use std::collections::{HashMap, HashSet};

use crate::equality::semantic_equal;
use crate::feature_gates::{enabled, Feature};
use crate::qos::{compute_pod_qos, get_pod_qos};
use crate::resources::pod::{EphemeralContainer, PodSpec};
use crate::resources::{Container, Pod};
use crate::types::ResourceRequirements;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use crate::validation::pod::{validate_container, validate_pod_create_with_options};
use crate::validation::pod_options::{
    get_validation_options_from_pod_spec_and_meta, PodValidationOptions,
};
use crate::validation::pod_status::validate_pod_metadata_annotations_with_opts;

/// `core.MirrorPodAnnotationKey`.
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";

/// `validatePodMetadataAndSpec` (validation.go:4500-4530), the part that
/// `ValidatePodUpdate` shares with the subresource validators. `old` supplies the
/// `GetValidationOptionsFromPodSpecAndMeta` options; `run_annotation_updates`
/// selects the `ValidatePodSpecificAnnotationUpdates` half (validation.go:5699).
fn validate_pod_metadata_and_spec(
    pod: &Pod,
    old: Option<&Pod>,
    run_annotation_updates: bool,
) -> ErrorList {
    let mut errs = validate_object_meta(
        &pod.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_pod_metadata_annotations_with_opts(
        pod,
        old,
        run_annotation_updates,
    ));
    // `ValidatePodSpec` (validation.go:4643) validates the ephemeral
    // containers; "cannot be set on create" belongs to `ValidatePodCreate`
    // (validation.go:5605-5608), not to `validatePodMetadataAndSpec`.
    // `GetValidationOptionsFromPodSpecAndMeta(&pod.Spec, &oldPod.Spec, ...)`
    // (strategy.go) - derived from the old pod when there is one.
    let mut opts = get_validation_options_from_pod_spec_and_meta(
        pod.spec.as_ref(),
        old.and_then(|o| o.spec.as_ref()),
    );
    opts.allow_relaxed_dns_search_validation |= enabled(Feature::RelaxedDNSSearchValidation);
    errs.extend(
        validate_pod_create_with_options(pod, &opts)
            .into_iter()
            .filter(|e| {
                !(e.field == "spec.ephemeralContainers" && e.detail == "cannot be set on create")
            }),
    );
    if let Some(spec) = pod.spec.as_ref() {
        errs.extend(validate_ephemeral_containers(
            spec,
            &Path::new("spec").child("ephemeralContainers"),
            &opts,
        ));
    }
    errs
}

/// `validateEphemeralContainers` (validation.go:3727-3783), less the
/// duplicate-name check that `validate_pod_spec` already makes across all
/// container kinds.
fn validate_ephemeral_containers(
    spec: &PodSpec,
    fld_path: &Path,
    opts: &PodValidationOptions,
) -> ErrorList {
    let mut errs = Vec::new();
    let ecs = spec.ephemeral_containers.as_deref().unwrap_or(&[]);
    if ecs.is_empty() {
        return errs;
    }
    let other_names: HashSet<&str> = spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .map(|c| c.name.as_str())
        .collect();
    let volume_names: HashSet<&str> = spec
        .volumes
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|v| v.name.as_str())
        .collect();

    for (i, ec) in ecs.iter().enumerate() {
        let idx_path = fld_path.index(i);
        // `validateContainerCommon`, on the `EphemeralContainerCommon` seen as
        // a `Container`.
        errs.extend(validate_container(
            &ephemeral_as_container(ec),
            false,
            &volume_names,
            spec.termination_grace_period_seconds,
            spec.os.as_ref(),
            spec.restart_policy.as_deref(),
            spec.host_users.unwrap_or(true),
            &idx_path,
            opts,
        ));

        // The target container name must exist and be non-ephemeral.
        if let Some(target) = ec
            .target_container_name
            .as_deref()
            .filter(|t| !t.is_empty())
        {
            if !other_names.contains(target) {
                errs.push(Error::not_found(
                    &idx_path.child("targetContainerName"),
                    target.to_string(),
                ));
            }
        }

        // `validateFieldAllowList(ec.EphemeralContainerCommon,
        // allowedEphemeralContainerFields, ...)` (validation.go:93-115): only
        // the allowed fields may be set.
        const MSG: &str = "cannot be set for an Ephemeral Container";
        let set = |is_set: bool, name: &str, errs: &mut ErrorList| {
            if is_set {
                errs.push(Error::forbidden(&idx_path.child(name), MSG));
            }
        };
        set(ec.ports.is_some(), "ports", &mut errs);
        set(
            // `reflect.DeepEqual(field, zero)` (validation.go:3799): a present
            // but empty map counts as set; only nil is the zero value.
            ec.resources
                .as_ref()
                .is_some_and(|r| r.limits.is_some() || r.requests.is_some() || r.claims.is_some()),
            "resources",
            &mut errs,
        );
        set(ec.liveness_probe.is_some(), "livenessProbe", &mut errs);
        set(ec.readiness_probe.is_some(), "readinessProbe", &mut errs);
        set(ec.startup_probe.is_some(), "startupProbe", &mut errs);
        set(ec.lifecycle.is_some(), "lifecycle", &mut errs);
        set(ec.resize_policy.is_some(), "resizePolicy", &mut errs);
        set(ec.restart_policy.is_some(), "restartPolicy", &mut errs);
        set(
            ec.restart_policy_rules.is_some(),
            "restartPolicyRules",
            &mut errs,
        );

        // VolumeMount subpaths leak bind mounts until the pod exits, so they
        // are disallowed entirely.
        for (j, vm) in ec.volume_mounts.iter().flatten().enumerate() {
            let vm_path = idx_path.child("volumeMounts").index(j);
            if vm.sub_path.as_deref().is_some_and(|s| !s.is_empty()) {
                errs.push(Error::forbidden(&vm_path.child("subPath"), MSG));
            }
            if vm.sub_path_expr.as_deref().is_some_and(|s| !s.is_empty()) {
                errs.push(Error::forbidden(&vm_path.child("subPathExpr"), MSG));
            }
        }
    }
    errs
}

/// `(*core.Container)(&ec.EphemeralContainerCommon)`.
fn ephemeral_as_container(ec: &EphemeralContainer) -> Container {
    Container {
        name: ec.name.clone(),
        image: ec.image.clone(),
        command: ec.command.clone(),
        args: ec.args.clone(),
        working_dir: ec.working_dir.clone(),
        env: ec.env.clone(),
        volume_mounts: ec.volume_mounts.clone(),
        image_pull_policy: ec.image_pull_policy.clone(),
        security_context: ec.security_context.clone(),
        stdin: ec.stdin,
        stdin_once: ec.stdin_once,
        tty: ec.tty,
        termination_message_path: ec.termination_message_path.clone(),
        termination_message_policy: ec.termination_message_policy.clone(),
        env_from: ec.env_from.clone(),
        volume_devices: ec.volume_devices.clone(),
        ..Default::default()
    }
}

fn is_mirror_pod(pod: &Pod) -> bool {
    pod.metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(MIRROR_POD_ANNOTATION_KEY))
}

/// `ValidatePodEphemeralContainersUpdate` (validation.go:6181-6212): existing
/// ephemeral containers may be neither removed nor changed.
pub fn validate_pod_ephemeral_containers_update(new: &Pod, old: &Pod) -> ErrorList {
    // Part 1: validate newPod's spec and updates to metadata.
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    // `validate_pod_metadata_and_spec` with `old` also runs
    // `ValidatePodSpecificAnnotationUpdates` (validation.go:6189).
    errs.extend(validate_pod_metadata_and_spec(new, Some(old), true));

    // static pods don't support ephemeral containers #113935
    if is_mirror_pod(old) {
        return vec![Error::forbidden(
            &Path::new(""),
            "static pods do not support ephemeral containers",
        )];
    }

    // Part 2: existing ephemeral containers may not be changed. Order isn't
    // preserved by patch, so check each individually.
    let new_containers = new
        .spec
        .as_ref()
        .and_then(|s| s.ephemeral_containers.as_deref())
        .unwrap_or(&[]);
    let old_containers = old
        .spec
        .as_ref()
        .and_then(|s| s.ephemeral_containers.as_deref())
        .unwrap_or(&[]);
    let spec_path = Path::new("spec").child("ephemeralContainers");
    for old_container in old_containers {
        match new_containers.iter().find(|c| c.name == old_container.name) {
            None => errs.push(Error::forbidden(
                &spec_path,
                format!(
                    "existing ephemeral containers {:?} may not be removed\n",
                    old_container.name
                ),
            )),
            Some(new_container)
                if !semantic_equal(
                    &ephemeral_with_plain_bools_unset(old_container),
                    &ephemeral_with_plain_bools_unset(new_container),
                ) =>
            {
                errs.push(Error::forbidden(
                    &spec_path,
                    // validation.go:6206-6207: `diff.Diff(old, *new)`.
                    format!(
                        "existing ephemeral containers {:?} may not be changed\n{}",
                        old_container.name,
                        crate::diff_schema::struct_diff(
                            crate::diff_schema::EPHEMERAL_CONTAINER,
                            &serde_json::to_value(old_container).unwrap_or_default(),
                            &serde_json::to_value(new_container).unwrap_or_default(),
                        )
                    ),
                ))
            }
            Some(_) => {}
        }
    }
    errs
}

/// `Stdin`, `StdinOnce` and `TTY` are plain `bool`s upstream, so `false` and
/// absent are one value to `apiequality.Semantic.DeepEqual`; here they are
/// `Option<bool>` and a protobuf client writes every one of them (#2382).
fn ephemeral_with_plain_bools_unset(c: &EphemeralContainer) -> EphemeralContainer {
    let mut c = c.clone();
    for b in [&mut c.stdin, &mut c.stdin_once, &mut c.tty] {
        if *b == Some(false) {
            *b = None;
        }
    }
    c
}

/// The same normalization for a regular container.
fn container_with_plain_bools_unset(c: &Container) -> Container {
    let mut c = c.clone();
    for b in [&mut c.stdin, &mut c.stdin_once, &mut c.tty] {
        if *b == Some(false) {
            *b = None;
        }
    }
    c
}

/// `ValidatePodResize` (validation.go:6217-6346).
pub fn validate_pod_resize(new: &Pod, old: &Pod) -> ErrorList {
    // Part 1: validate newPod's spec and updates to metadata.
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_pod_metadata_and_spec(new, Some(old), false));

    // static pods cannot be resized.
    if is_mirror_pod(old) {
        return vec![Error::forbidden(
            &Path::new(""),
            "static pods cannot be resized",
        )];
    }

    // windows pods are not supported.
    if old
        .spec
        .as_ref()
        .and_then(|s| s.os.as_ref())
        .is_some_and(|os| os.name == "windows")
    {
        return vec![Error::forbidden(
            &Path::new(""),
            "windows pods cannot be resized",
        )];
    }

    let spec_path = Path::new("spec");
    let (Some(old_spec), Some(new_spec)) = (old.spec.as_ref(), new.spec.as_ref()) else {
        return errs;
    };
    if get_pod_qos(old) != compute_pod_qos(new) {
        errs.push(Error::invalid(
            &spec_path,
            new.status
                .as_ref()
                .and_then(|s| s.qos_class.clone())
                .unwrap_or_default(),
            "Pod QOS Class may not change as a result of resizing",
        ));
    }

    if !is_pod_resize_request_supported(old) {
        errs.push(Error::forbidden(
            &spec_path,
            "Pod running on node without support for resize",
        ));
    }

    // Part 2: validate that changes between pod-level resources in
    // oldPod.Spec.Resources and newPod.Spec.Resources are allowed
    // (validation.go:6240-6264).
    let is_pod_level_resources_set = |s: &PodSpec| {
        s.resources.as_ref().is_some_and(|r| {
            r.requests.as_ref().map_or(0, |m| m.len()) + r.limits.as_ref().map_or(0, |m| m.len())
                > 0
        })
    };
    let mut new_spec_copy = new_spec.clone();
    if is_pod_level_resources_set(old_spec) || is_pod_level_resources_set(new_spec) {
        // pods with pod-level resources cannot be resized without the
        // InPlacePodLevelResourcesVerticalScaling feature gate being enabled.
        // `opts.InPlacePodLevelResourcesVerticalScalingEnabled`, derived as
        // strategy.go does via `GetValidationOptionsFromPodSpecAndMeta`.
        let opts = get_validation_options_from_pod_spec_and_meta(Some(new_spec), Some(old_spec));
        if !opts.in_place_pod_level_resources_vertical_scaling_enabled {
            errs.push(Error::forbidden(
                &Path::new(""),
                "resize of pods with pod-level resources is forbidden when InPlacePodLevelResourcesVerticalScalingEnabled feature gate is disabled",
            ));
            return errs;
        }

        // `new_spec_copy` is passed by reference to allow its `resources` to
        // be mutated, which is required for an intermediate validation step.
        errs.extend(validate_pod_level_resources_resize(
            new_spec,
            old_spec,
            &mut new_spec_copy,
            &spec_path,
        ));
    }

    // Part 3: container resources. The rest assumes the containers are in
    // the same order, so proceed only if that holds.
    let order_errs = validate_pod_resize_container_ordering(new_spec, old_spec, &spec_path);
    let ordering_failed = !order_errs.is_empty();
    errs.extend(order_errs);
    if ordering_failed {
        return errs;
    }

    let old_init = old_spec.init_containers.as_deref().unwrap_or(&[]);
    let new_init = new_spec.init_containers.as_deref().unwrap_or(&[]);

    // Do not allow removing resource requests/limits on resize
    // (SidecarContainers is GA and on in v1.35).
    for (ix, ctr) in old_init.iter().enumerate() {
        if !is_restartable_init_container(ctr) {
            continue;
        }
        errs.extend(validate_container_resize(
            &new_init[ix].resources,
            &ctr.resources,
            &spec_path
                .child("initContainers")
                .index(ix)
                .child("resources"),
        ));
    }
    for (ix, ctr) in old_spec.containers.iter().enumerate() {
        errs.extend(validate_container_resize(
            &new_spec.containers[ix].resources,
            &ctr.resources,
            &spec_path.child("containers").index(ix).child("resources"),
        ));
    }

    // Only cpu and memory are mutable for regular containers.
    for (ix, container) in new_spec_copy.containers.iter_mut().enumerate() {
        drop_cpu_memory_resources_from_container(container, &old_spec.containers[ix]);
        if !semantic_equal(
            &container_with_plain_bools_unset(container),
            &container_with_plain_bools_unset(&old_spec.containers[ix]),
        ) {
            errs.push(Error::forbidden(
                &spec_path,
                "only cpu and memory resources are mutable",
            ));
        }
    }

    // Only cpu and memory are mutable for restartable init containers, and
    // the other init containers are immutable.
    if let Some(init) = new_spec_copy.init_containers.as_mut() {
        for (ix, container) in init.iter_mut().enumerate() {
            if is_restartable_init_container(container) {
                drop_cpu_memory_resources_from_container(container, &old_init[ix]);
                if !semantic_equal(
                    &container_with_plain_bools_unset(container),
                    &container_with_plain_bools_unset(&old_init[ix]),
                ) {
                    errs.push(Error::forbidden(
                        &spec_path,
                        "only cpu and memory resources for sidecar containers are mutable",
                    ));
                }
            } else if !semantic_equal(
                &container_with_plain_bools_unset(container),
                &container_with_plain_bools_unset(&old_init[ix]),
            ) {
                errs.push(Error::forbidden(
                    &spec_path,
                    "resources for non-sidecar init containers are immutable",
                ));
            }
        }
    }

    if !errs.is_empty() {
        return errs;
    }

    if !specs_semantic_equal(&new_spec_copy, old_spec) {
        // This likely means that the user has made changes to resources
        // other than CPU and Memory.
        errs.push(Error::forbidden(
            &spec_path,
            "only cpu and memory resources are mutable",
        ));
    }
    errs
}

/// `apiequality.Semantic.DeepEqual(newPodSpecCopy, oldPod.Spec)`, with the
/// containers' plain-`bool` fields normalized.
fn specs_semantic_equal(a: &PodSpec, b: &PodSpec) -> bool {
    let norm = |s: &PodSpec| {
        let mut s = s.clone();
        s.containers = s
            .containers
            .iter()
            .map(container_with_plain_bools_unset)
            .collect();
        s.init_containers = s
            .init_containers
            .map(|c| c.iter().map(container_with_plain_bools_unset).collect());
        s
    };
    semantic_equal(&norm(a), &norm(b))
}

/// `validatePodResizeContainerOrdering` (validation.go:6348-6370): containers
/// may not be added, removed, re-ordered, or renamed on resize.
fn validate_pod_resize_container_ordering(
    new: &PodSpec,
    old: &PodSpec,
    spec_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();
    if new.containers.len() != old.containers.len() {
        errs.push(Error::forbidden(
            &spec_path.child("containers"),
            "containers may not be added or removed on resize",
        ));
    } else {
        for (i, old_ctr) in old.containers.iter().enumerate() {
            if new.containers[i].name != old_ctr.name {
                errs.push(Error::forbidden(
                    &spec_path.child("containers").index(i).child("name"),
                    "containers may not be renamed or reordered on resize",
                ));
            }
        }
    }
    let new_init = new.init_containers.as_deref().unwrap_or(&[]);
    let old_init = old.init_containers.as_deref().unwrap_or(&[]);
    if new_init.len() != old_init.len() {
        errs.push(Error::forbidden(
            &spec_path.child("initContainers"),
            "initContainers may not be added or removed on resize",
        ));
    } else {
        for (i, old_ctr) in old_init.iter().enumerate() {
            if new_init[i].name != old_ctr.name {
                errs.push(Error::forbidden(
                    &spec_path.child("initContainers").index(i).child("name"),
                    "initContainers may not be renamed or reordered on resize",
                ));
            }
        }
    }
    errs
}

type ResourceList = Option<HashMap<String, String>>;

/// `validatePodLevelResourcesResize` (validation.go:6384-6412): pod-level
/// resources may not be removed, and only cpu and memory may change.
fn validate_pod_level_resources_resize(
    new_spec: &PodSpec,
    old_spec: &PodSpec,
    spec_to_mutate: &mut PodSpec,
    spec_path: &Path,
) -> ErrorList {
    let mut errs = Vec::new();

    if old_spec.resources.is_some() && new_spec.resources.is_none() {
        errs.push(Error::forbidden(
            &spec_path.child("resources"),
            "pod-level resources cannot be removed",
        ));
        return errs;
    }

    if let (Some(old_res), Some(new_res)) =
        (old_spec.resources.as_ref(), new_spec.resources.as_ref())
    {
        if resources_removed(&new_res.requests, &old_res.requests) {
            errs.push(Error::forbidden(
                &spec_path.child("resources").child("requests"),
                "pod-level resource requests cannot be removed",
            ));
        }
        if resources_removed(&new_res.limits, &old_res.limits) {
            errs.push(Error::forbidden(
                &spec_path.child("resources").child("limits"),
                "pod-level resource limits cannot be removed",
            ));
        }
    }

    spec_to_mutate.resources = drop_cpu_memory_resource_requirements_updates(
        spec_to_mutate.resources.take(),
        old_spec.resources.as_ref(),
    );

    if !semantic_equal(&spec_to_mutate.resources, &old_spec.resources) {
        // This likely means that the user has made changes to pod-level
        // resources other than CPU and memory.
        errs.push(Error::forbidden(
            spec_path,
            "only cpu and memory resources are mutable at pod-level",
        ));
    }
    errs
}

/// `dropCPUMemoryResourceRequirementsUpdates` (validation.go:6447-6476): the
/// cpu and memory come from the old pod's `resources`; the whole block is nil
/// when the old one was nil and nothing but empty lists remain.
fn drop_cpu_memory_resource_requirements_updates(
    resources: Option<ResourceRequirements>,
    old: Option<&ResourceRequirements>,
) -> Option<ResourceRequirements> {
    let mut resources = resources?;
    let old_reqs = old.and_then(|o| o.requests.clone());
    let old_lims = old.and_then(|o| o.limits.clone());
    resources.requests = drop_cpu_memory_updates(&resources.requests, &old_reqs);
    resources.limits = drop_cpu_memory_updates(&resources.limits, &old_lims);
    if old.is_none()
        && resources.requests.as_ref().is_none_or(|m| m.is_empty())
        && resources.limits.as_ref().is_none_or(|m| m.is_empty())
        && resources.claims.as_ref().is_none_or(|c| c.is_empty())
    {
        return None;
    }
    Some(resources)
}

/// `dropCPUMemoryUpdates` (validation.go:6414-6436).
fn drop_cpu_memory_updates(list: &ResourceList, old: &ResourceList) -> ResourceList {
    let mut munged = match list {
        None if old.is_none() => return None,
        None => HashMap::new(),
        Some(l) => l.clone(),
    };
    munged.remove("cpu");
    munged.remove("memory");
    for name in ["cpu", "memory"] {
        if let Some(q) = old.as_ref().and_then(|o| o.get(name)) {
            munged.insert(name.to_string(), q.clone());
        }
    }
    Some(munged)
}

/// `dropCPUMemoryResourcesFromContainer` (validation.go:6438-6443): the
/// container's cpu and memory come from the old container. Upstream builds a
/// fresh `ResourceRequirements{Limits, Requests}`, which also drops `claims`.
fn drop_cpu_memory_resources_from_container(container: &mut Container, old: &Container) {
    let old_res = old.resources.clone().unwrap_or_else(empty_requirements);
    let res = container
        .resources
        .clone()
        .unwrap_or_else(empty_requirements);
    container.resources = Some(ResourceRequirements {
        limits: drop_cpu_memory_updates(&res.limits, &old_res.limits),
        requests: drop_cpu_memory_updates(&res.requests, &old_res.requests),
        claims: None,
    });
}

/// `isPodResizeRequestSupported` (validation.go:6479-6495).
fn is_pod_resize_request_supported(pod: &Pod) -> bool {
    for c in pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_deref())
        .unwrap_or(&[])
    {
        if c.state.as_ref().is_some_and(|s| s.running.is_some()) {
            return c.resources.is_some();
        }
    }
    // No running containers. We cannot tell whether the node supports resize
    // at this point, so we assume it does.
    true
}

/// `validateContainerResize` (validation.go:6497-6511).
fn validate_container_resize(
    new: &Option<ResourceRequirements>,
    old: &Option<ResourceRequirements>,
    fld_path: &Path,
) -> ErrorList {
    let new = new.clone().unwrap_or_else(empty_requirements);
    let old = old.clone().unwrap_or_else(empty_requirements);
    let mut errs = Vec::new();
    if resources_removed(&new.requests, &old.requests) {
        errs.push(Error::forbidden(
            &fld_path.child("requests"),
            "resource requests cannot be removed",
        ));
    }
    if resources_removed(&new.limits, &old.limits) {
        errs.push(Error::forbidden(
            &fld_path.child("limits"),
            "resource limits cannot be removed",
        ));
    }
    errs
}

/// `resourcesRemoved` (validation.go:6513-6523).
fn resources_removed(list: &ResourceList, old: &ResourceList) -> bool {
    let len = |l: &ResourceList| l.as_ref().map_or(0, |m| m.len());
    if len(old) > len(list) {
        return true;
    }
    old.iter()
        .flatten()
        .any(|(name, _)| !list.as_ref().is_some_and(|l| l.contains_key(name)))
}

fn empty_requirements() -> ResourceRequirements {
    ResourceRequirements {
        limits: None,
        requests: None,
        claims: None,
    }
}

/// `isRestartableInitContainer`: an init container with `restartPolicy:
/// Always` is a sidecar.
fn is_restartable_init_container(c: &Container) -> bool {
    c.restart_policy.as_deref() == Some("Always")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::PodStatus;
    use serde_json::json;

    fn pod(value: serde_json::Value) -> Pod {
        serde_json::from_value(value).unwrap()
    }

    fn running_status(resources: Option<serde_json::Value>) -> PodStatus {
        let mut status = serde_json::json!({"containerStatuses": [{
            "name": "c", "ready": true, "restartCount": 0, "image": "i", "imageID": "i",
            "state": {"running": {}}
        }]});
        if let Some(r) = resources {
            status["containerStatuses"][0]["resources"] = r;
        }
        serde_json::from_value(status).unwrap()
    }

    /// `isPodResizeRequestSupported` (validation.go:6479): a running container
    /// whose status carries no `resources` marks a node without resize support.
    #[test]
    fn resize_support_is_read_from_the_first_running_container_status() {
        let mut p = pod(serde_json::json!({"metadata": {"name": "p"}}));
        assert!(is_pod_resize_request_supported(&p));
        p.status = Some(running_status(None));
        assert!(!is_pod_resize_request_supported(&p));
        p.status = Some(running_status(Some(
            serde_json::json!({"requests": {"cpu": "1"}}),
        )));
        assert!(is_pod_resize_request_supported(&p));
    }

    /// `resourcesRemoved` (validation.go:6513): fewer entries, or a missing
    /// name, is a removal; adding an entry is not.
    #[test]
    fn resources_removed_matches_upstream() {
        let list = |kv: &[(&str, &str)]| {
            Some(
                kv.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        };
        assert!(resources_removed(&None, &list(&[("cpu", "1")])));
        assert!(resources_removed(
            &list(&[("memory", "1")]),
            &list(&[("cpu", "1")])
        ));
        assert!(!resources_removed(
            &list(&[("cpu", "2"), ("memory", "1")]),
            &list(&[("cpu", "1")])
        ));
        assert!(!resources_removed(&list(&[("cpu", "1")]), &None));
    }

    /// `dropCPUMemoryUpdates` (validation.go:6414): cpu and memory come from
    /// the old list, everything else from the new one.
    #[test]
    fn drop_cpu_memory_updates_keeps_old_cpu_and_memory() {
        let new = Some(HashMap::from([
            ("cpu".to_string(), "2".to_string()),
            ("memory".to_string(), "2Gi".to_string()),
            ("ephemeral-storage".to_string(), "2Gi".to_string()),
        ]));
        let old = Some(HashMap::from([("cpu".to_string(), "1".to_string())]));
        let munged = drop_cpu_memory_updates(&new, &old).unwrap();
        assert_eq!(munged.get("cpu").map(String::as_str), Some("1"));
        assert!(!munged.contains_key("memory"));
        assert_eq!(
            munged.get("ephemeral-storage").map(String::as_str),
            Some("2Gi")
        );
        assert_eq!(drop_cpu_memory_updates(&None, &None), None);
    }

    /// `ValidatePodResize` with pod-level resources and the
    /// `InPlacePodLevelResourcesVerticalScaling` gate off (validation.go:6247).
    #[test]
    #[serial_test::serial]
    fn pod_level_resources_cannot_be_resized_with_the_gate_off() {
        let old = pod(serde_json::json!({
            "metadata": {"name": "p"},
            "spec": {
                "containers": [{"name": "c", "image": "busybox"}],
                "resources": {"requests": {"cpu": "1"}}
            }
        }));
        let errs = validate_pod_resize(&old.clone(), &old);
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("pod-level resources is forbidden")),
            "{errs:?}"
        );
    }

    /// A pod as `TestValidatePodResize`'s `mkPod`: one container with the
    /// given requests/limits, plus optional extra spec JSON merged in.
    fn mk(req: serde_json::Value, lim: serde_json::Value, extra: serde_json::Value) -> Pod {
        let mut spec = serde_json::json!({"containers": [{
            "name": "container", "image": "busybox",
            "resources": {"requests": req, "limits": lim}
        }]});
        if let Some(m) = extra.as_object() {
            for (k, v) in m {
                spec[k] = v.clone();
            }
        }
        pod(
            serde_json::json!({"metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"}, "spec": spec}),
        )
    }

    fn resize_errs(old: &Pod, new: &Pod) -> Vec<String> {
        validate_pod_resize(new, old)
            .iter()
            .map(|e| format!("{}: {}", e.field, e.detail))
            .collect()
    }

    /// TestValidatePodResize: "Pod QoS change, *" cases (validation_test.go:27935).
    #[test]
    fn resize_refuses_qos_changes_and_allows_unchanged_classes() {
        use serde_json::json;
        let none = json!({});
        let guaranteed = |c: &str| json!({"cpu": c, "memory": "100Mi"});
        let g1 = mk(guaranteed("100m"), guaranteed("100m"), none.clone());
        let g2 = mk(guaranteed("200m"), guaranteed("200m"), none.clone());
        assert!(resize_errs(&g1, &g2).is_empty());
        let burstable = mk(json!({"cpu": "100m"}), none.clone(), none.clone());
        let burstable2 = mk(json!({"cpu": "200m"}), none.clone(), none.clone());
        assert!(resize_errs(&burstable, &burstable2).is_empty());
        let besteffort = mk(none.clone(), none.clone(), none.clone());
        for (old, new) in [
            (&g1, &burstable),
            (&burstable, &g1),
            (&besteffort, &burstable),
            (&burstable, &besteffort),
        ] {
            let errs = resize_errs(old, new);
            assert!(
                errs.iter()
                    .any(|e| e.contains("Pod QOS Class may not change as a result of resizing")),
                "{errs:?}"
            );
        }
    }

    /// "storage {limit,request} change" and the sidecar / non-sidecar init
    /// container cases.
    #[test]
    fn resize_mutates_only_cpu_and_memory_of_containers_and_sidecars() {
        use serde_json::json;
        let none = json!({});
        let old = mk(
            json!({"cpu": "1", "ephemeral-storage": "1Gi"}),
            none.clone(),
            none.clone(),
        );
        let new = mk(
            json!({"cpu": "1", "ephemeral-storage": "2Gi"}),
            none.clone(),
            none.clone(),
        );
        assert_eq!(
            resize_errs(&old, &new),
            vec!["spec: only cpu and memory resources are mutable"]
        );

        let init = |restart: Option<&str>, req: serde_json::Value| {
            let mut c = json!({"name": "init", "image": "busybox", "resources": {"requests": req}});
            if let Some(r) = restart {
                c["restartPolicy"] = json!(r);
            }
            mk(
                json!({"cpu": "1"}),
                none.clone(),
                json!({"initContainers": [c]}),
            )
        };
        // sidecar: cpu is mutable, storage is not.
        let old = init(
            Some("Always"),
            json!({"cpu": "1", "ephemeral-storage": "1Gi"}),
        );
        assert!(resize_errs(
            &old,
            &init(
                Some("Always"),
                json!({"cpu": "2", "ephemeral-storage": "1Gi"})
            )
        )
        .is_empty());
        assert_eq!(
            resize_errs(
                &old,
                &init(
                    Some("Always"),
                    json!({"cpu": "1", "ephemeral-storage": "2Gi"})
                )
            ),
            vec!["spec: only cpu and memory resources for sidecar containers are mutable"]
        );
        // a plain init container's resources are immutable.
        let old = init(None, json!({"cpu": "1"}));
        assert_eq!(
            resize_errs(&old, &init(None, json!({"cpu": "2"}))),
            vec!["spec: resources for non-sidecar init containers are immutable"]
        );
        // removing a sidecar request is refused.
        let old = init(Some("Always"), json!({"cpu": "1", "memory": "1Gi"}));
        let errs = resize_errs(&old, &init(Some("Always"), json!({"cpu": "1"})));
        assert!(
            errs.contains(
                &"spec.initContainers[0].resources.requests: resource requests cannot be removed"
                    .to_string()
            ),
            "{errs:?}"
        );
    }

    /// "pod container addition / removal / reorder / rename" and "change resize
    /// restart policy".
    #[test]
    fn resize_refuses_structural_changes() {
        use serde_json::json;
        let two = |a: &str, b: &str| {
            pod(
                json!({"metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"}, "spec": {"containers": [
                    {"name": a, "image": "busybox"}, {"name": b, "image": "busybox"}
                ]}}),
            )
        };
        let one = pod(
            json!({"metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"}, "spec": {"containers": [
                {"name": "a", "image": "busybox"}
            ]}}),
        );
        assert_eq!(
            resize_errs(&one, &two("a", "b")),
            vec!["spec.containers: containers may not be added or removed on resize"]
        );
        assert_eq!(
            resize_errs(&two("a", "b"), &one),
            vec!["spec.containers: containers may not be added or removed on resize"]
        );
        assert_eq!(
            resize_errs(&two("a", "b"), &two("b", "a")),
            vec![
                "spec.containers[0].name: containers may not be renamed or reordered on resize",
                "spec.containers[1].name: containers may not be renamed or reordered on resize",
            ]
        );

        let none = json!({});
        let old = mk(json!({"cpu": "1"}), none.clone(), none.clone());
        let mut new = old.clone();
        new.spec.as_mut().unwrap().containers[0].resize_policy = serde_json::from_value(
            json!([{"resourceName": "cpu", "restartPolicy": "RestartContainer"}]),
        )
        .unwrap();
        assert_eq!(
            resize_errs(&old, &new),
            vec!["spec: only cpu and memory resources are mutable"]
        );
    }

    /// Windows pods: refused with or without a resource change.
    #[test]
    fn resize_refuses_windows_pods() {
        use serde_json::json;
        let os = json!({"os": {"name": "windows"}});
        let old = mk(json!({"cpu": "1"}), json!({}), os.clone());
        let new = mk(json!({"cpu": "2"}), json!({}), os);
        assert_eq!(
            resize_errs(&old, &new),
            vec![": windows pods cannot be resized"]
        );
        assert_eq!(
            resize_errs(&old, &old),
            vec![": windows pods cannot be resized"]
        );
    }

    /// validation.go:6205-6207: the "may not be changed" error appends
    /// `diff.Diff(old, *new)` — a unified diff (context 3) of
    /// `json.MarshalIndent` of the INTERNAL `core.EphemeralContainer`, which has
    /// no json tags (Go field names, declaration order, `EphemeralContainerCommon`
    /// inlined, nil slices/pointers as `null`, `Resources` always an object).
    #[test]
    fn ephemeral_update_changed_error_carries_diff_text() {
        use serde_json::json;
        let with = |image: &str| {
            pod(json!({
                "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
                "spec": {"containers": [{"name": "cnt", "image": "busybox"}],
                         "restartPolicy": "OnFailure",
                         "ephemeralContainers": [{"name": "a", "image": image}]}
            }))
        };
        let errs = validate_pod_ephemeral_containers_update(&with("other"), &with("busybox"));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].detail,
            concat!(
                "existing ephemeral containers \"a\" may not be changed\n",
                "@@ -1,6 +1,6 @@\n",
                " {\n",
                "  \"Name\": \"a\",\n",
                "- \"Image\": \"busybox\",\n",
                "+ \"Image\": \"other\",\n",
                "  \"Command\": null,\n",
                "  \"Args\": null,\n",
                "  \"WorkingDir\": \"\",\n",
            )
        );
    }

    /// TestValidatePodEphemeralContainersUpdate: removal and change refused,
    /// addition and reordering allowed (validation_test.go:16059).
    #[test]
    fn ephemeral_update_allows_add_and_reorder_only() {
        use serde_json::json;
        let with = |ecs: serde_json::Value| {
            pod(json!({
                "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
                "spec": {"containers": [{"name": "cnt", "image": "busybox"}],
                         "restartPolicy": "OnFailure", "ephemeralContainers": ecs}
            }))
        };
        let a = json!({"name": "a", "image": "busybox"});
        let b = json!({"name": "b", "image": "busybox"});
        let msgs = |new: &Pod, old: &Pod| -> Vec<String> {
            validate_pod_ephemeral_containers_update(new, old)
                .into_iter()
                .map(|e| e.detail)
                .collect()
        };
        // add
        assert!(msgs(&with(json!([a, b])), &with(json!([a]))).is_empty());
        // reorder
        assert!(msgs(&with(json!([b, a])), &with(json!([a, b]))).is_empty());
        // remove
        let errs = msgs(&with(json!([b])), &with(json!([a, b])));
        assert_eq!(
            errs,
            vec!["existing ephemeral containers \"a\" may not be removed\n"]
        );
        // change
        let a2 = json!({"name": "a", "image": "other"});
        let errs = msgs(&with(json!([a2])), &with(json!([a])));
        assert!(errs[0].starts_with("existing ephemeral containers \"a\" may not be changed\n"));
        // an unknown targetContainerName is NotFound
        let t = json!({"name": "t", "image": "busybox", "targetContainerName": "nope"});
        let errs = validate_pod_ephemeral_containers_update(&with(json!([t])), &with(json!([])));
        assert!(
            errs.iter()
                .any(|e| e.field == "spec.ephemeralContainers[0].targetContainerName"),
            "{errs:?}"
        );
        // forbidden fields
        let r = json!({"name": "r", "image": "busybox", "resources": {"limits": {"cpu": "1"}}});
        let errs = validate_pod_ephemeral_containers_update(&with(json!([r])), &with(json!([])));
        assert!(
            errs.iter()
                .any(|e| e.field == "spec.ephemeralContainers[0].resources"
                    && e.detail == "cannot be set for an Ephemeral Container"),
            "{errs:?}"
        );
    }

    /// Static pods take neither subresource (validation.go:6190, :6225).
    #[test]
    fn mirror_pods_refuse_both_subresources() {
        let old = pod(serde_json::json!({
            "metadata": {"name": "p", "annotations": {MIRROR_POD_ANNOTATION_KEY: "x"}},
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        }));
        let errs = validate_pod_resize(&old.clone(), &old);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].detail, "static pods cannot be resized");
        let errs = validate_pod_ephemeral_containers_update(&old.clone(), &old);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].detail,
            "static pods do not support ephemeral containers"
        );
    }

    /// A pod with pod-level resources (`mkPodWithPodLevelResources`-style).
    fn mk_pl(pod_res: serde_json::Value) -> Pod {
        pod(serde_json::json!({
            "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
            "spec": {
                "containers": [{"name": "c", "image": "busybox"}],
                "resources": pod_res
            }
        }))
    }

    /// `validatePodLevelResourcesResize` (validation.go:6384-6412) with the
    /// `InPlacePodLevelResourcesVerticalScaling` gate on.
    #[test]
    #[serial_test::serial]
    fn pod_level_resize_with_the_gate_on() {
        let _g = crate::feature_gates::with_feature(
            Feature::InPlacePodLevelResourcesVerticalScaling,
            true,
        );
        let old = mk_pl(json!({"requests": {"cpu": "1"}, "limits": {"cpu": "1"}}));
        // cpu/memory are mutable.
        let new = mk_pl(json!({"requests": {"cpu": "1"}, "limits": {"cpu": "2"}}));
        assert_eq!(resize_errs(&old, &new), Vec::<String>::new());
        // removing the pod-level resources block.
        let mut removed = new.clone();
        removed.spec.as_mut().unwrap().resources = None;
        assert!(resize_errs(&old, &removed)
            .contains(&"spec.resources: pod-level resources cannot be removed".to_string()));
        // removing requests.
        let no_req = mk_pl(json!({"limits": {"cpu": "1"}}));
        assert!(resize_errs(&old, &no_req).contains(
            &"spec.resources.requests: pod-level resource requests cannot be removed".to_string()
        ));
        // only cpu and memory are mutable.
        let storage_old = mk_pl(json!({"requests": {"cpu": "1"}}));
        let storage_new = mk_pl(json!({"requests": {"cpu": "1", "ephemeral-storage": "1Gi"}}));
        assert!(resize_errs(&storage_old, &storage_new)
            .contains(&"spec: only cpu and memory resources are mutable at pod-level".to_string()));
    }

    /// `ValidatePodResize` -> `validatePodMetadataAndSpec(newPod, opts)`: opts
    /// come from the old pod (`GetValidationOptionsFromPodSpecAndMeta`), so an
    /// already-invalid pod-deletion-cost annotation stays tolerated.
    #[test]
    fn resize_tolerates_invalid_deletion_cost_already_on_old_pod() {
        let mut p = mk(json!({"cpu": "1"}), json!({"cpu": "1"}), json!({}));
        p.metadata.annotations = Some(HashMap::from([(
            "controller.kubernetes.io/pod-deletion-cost".to_string(),
            "abc".to_string(),
        )]));
        assert_eq!(resize_errs(&p, &p), Vec::<String>::new());
    }

    /// `reflect.DeepEqual(field, zero)` (validation.go:3799): a non-nil empty
    /// slice or map is "set", unlike nil.
    #[test]
    fn ephemeral_allow_list_treats_empty_but_present_as_set() {
        let with = |ec: serde_json::Value| {
            pod(json!({
                "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
                "spec": {"containers": [{"name": "cnt", "image": "busybox"}],
                         "ephemeralContainers": [ec]}
            }))
        };
        let empty = pod(json!({
            "metadata": {"name": "pod", "namespace": "ns", "resourceVersion": "1"},
            "spec": {"containers": [{"name": "cnt", "image": "busybox"}]}
        }));
        for (key, val) in [
            ("ports", json!([])),
            ("resizePolicy", json!([])),
            ("restartPolicyRules", json!([])),
            ("resources", json!({"limits": {}})),
        ] {
            let mut ec = json!({"name": "e", "image": "busybox"});
            ec[key] = val;
            let errs = validate_pod_ephemeral_containers_update(&with(ec), &empty);
            assert!(
                errs.iter()
                    .any(|e| e.field == format!("spec.ephemeralContainers[0].{key}")
                        && e.detail == "cannot be set for an Ephemeral Container"),
                "{key}: {errs:?}"
            );
        }
        // an empty resources block is the zero value.
        let ec = json!({"name": "e", "image": "busybox", "resources": {}});
        let errs = validate_pod_ephemeral_containers_update(&with(ec), &empty);
        assert!(
            errs.iter().all(|e| !e.field.ends_with("resources")),
            "{errs:?}"
        );
    }
}
