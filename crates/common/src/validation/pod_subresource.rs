//! Validation of the Pod `/ephemeralcontainers` and `/resize` subresource
//! updates: ports of `ValidatePodEphemeralContainersUpdate` and
//! `ValidatePodResize` (`pkg/apis/core/validation/validation.go:6181-6523`,
//! release-1.35) with their helpers.
//!
//! Not ported: the `InPlacePodLevelResourcesVerticalScaling` branch
//! (`validatePodLevelResourcesResize`, validation.go:6384-6412). The gate is
//! alpha and off in v1.35 (kube_features.go:1372), so a pod with pod-level
//! resources is refused exactly as upstream does with the gate off.

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
use crate::validation::pod::{validate_container, validate_pod_create};
use crate::validation::pod_status::validate_pod_metadata_annotations;

/// `core.MirrorPodAnnotationKey`.
const MIRROR_POD_ANNOTATION_KEY: &str = "kubernetes.io/config.mirror";

/// `validatePodMetadataAndSpec` (validation.go:4500-4530), the part that
/// `ValidatePodUpdate` shares with the subresource validators. `old` selects
/// the `ValidatePodSpecificAnnotationUpdates` half (validation.go:5699).
fn validate_pod_metadata_and_spec(pod: &Pod, old: Option<&Pod>) -> ErrorList {
    let mut errs = validate_object_meta(
        &pod.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_pod_metadata_annotations(pod, old));
    // `ValidatePodSpec` (validation.go:4643) validates the ephemeral
    // containers; "cannot be set on create" belongs to `ValidatePodCreate`
    // (validation.go:5605-5608), not to `validatePodMetadataAndSpec`.
    errs.extend(
        validate_pod_create(pod, enabled(Feature::RelaxedDNSSearchValidation))
            .into_iter()
            .filter(|e| {
                !(e.field == "spec.ephemeralContainers" && e.detail == "cannot be set on create")
            }),
    );
    if let Some(spec) = pod.spec.as_ref() {
        errs.extend(validate_ephemeral_containers(
            spec,
            &Path::new("spec").child("ephemeralContainers"),
        ));
    }
    errs
}

/// `validateEphemeralContainers` (validation.go:3727-3783), less the
/// duplicate-name check that `validate_pod_spec` already makes across all
/// container kinds.
fn validate_ephemeral_containers(spec: &PodSpec, fld_path: &Path) -> ErrorList {
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
            &idx_path,
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
        set(
            ec.ports.as_ref().is_some_and(|v| !v.is_empty()),
            "ports",
            &mut errs,
        );
        set(
            ec.resources
                .as_ref()
                .is_some_and(|r| !semantic_equal(r, &empty_requirements())),
            "resources",
            &mut errs,
        );
        set(ec.liveness_probe.is_some(), "livenessProbe", &mut errs);
        set(ec.readiness_probe.is_some(), "readinessProbe", &mut errs);
        set(ec.startup_probe.is_some(), "startupProbe", &mut errs);
        set(ec.lifecycle.is_some(), "lifecycle", &mut errs);
        set(
            ec.resize_policy.as_ref().is_some_and(|v| !v.is_empty()),
            "resizePolicy",
            &mut errs,
        );
        set(ec.restart_policy.is_some(), "restartPolicy", &mut errs);
        set(
            ec.restart_policy_rules
                .as_ref()
                .is_some_and(|v| !v.is_empty()),
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
    errs.extend(validate_pod_metadata_and_spec(new, Some(old)));

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
                    format!(
                        "existing ephemeral containers {:?} may not be changed\n",
                        old_container.name
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
    errs.extend(validate_pod_metadata_and_spec(new, None));

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

    // Part 2: pod-level resources. The InPlacePodLevelResourcesVerticalScaling
    // gate is off in v1.35 (see the module doc).
    let is_pod_level_resources_set = |s: &PodSpec| {
        s.resources.as_ref().is_some_and(|r| {
            r.requests.as_ref().map_or(0, |m| m.len()) + r.limits.as_ref().map_or(0, |m| m.len())
                > 0
        })
    };
    if is_pod_level_resources_set(old_spec) || is_pod_level_resources_set(new_spec) {
        errs.push(Error::forbidden(
            &Path::new(""),
            "resize of pods with pod-level resources is forbidden when InPlacePodLevelResourcesVerticalScalingEnabled feature gate is disabled",
        ));
        return errs;
    }
    let mut new_spec_copy = new_spec.clone();

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
}
