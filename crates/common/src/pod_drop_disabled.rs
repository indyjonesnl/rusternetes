//! `podutil.DropDisabledPodFields` — port of `pkg/api/pod/util.go`
//! (release-1.35, `DropDisabledPodFields` :697, `dropDisabledFields` :721,
//! `dropDisabledPodStatusFields` :1004).
//!
//! A gate that is off must not let a field in, unless the stored pod already
//! uses it: a client that wrote the field on a cluster that had the gate on
//! keeps it after the gate is turned off, but never gets a new use of it.
//!
//! Only the gates this tree carries a [`Feature`] for are consulted, and of
//! the gates `DropDisabledPodFields` covers only those whose 1.35 default is
//! **off** can change an object (`versioned_feature_list.yaml`):
//!
//! | gate | 1.35 | what is dropped |
//! |---|---|---|
//! | `InPlacePodLevelResourcesVerticalScaling` | Alpha, off | `status.resources`, `status.allocatedResources` |
//! | `DRAExtendedResource` | Alpha, off | `status.extendedResourceClaimStatus` |
//! | `ClusterTrustBundleProjection` | Beta, off | `clusterTrustBundle` projected sources |
//! | `PodCertificateRequest` | Beta, off | `podCertificate` projected sources |
//! | `ContainerStopSignals` | Alpha, off | `lifecycle.stopSignal` |
//! | `ResourceHealthStatus` | Alpha, off | `allocatedResourcesStatus` of container statuses |
//! | `GenericWorkload` | Alpha, off | `spec.workloadRef` |
//!
//! The gates `DropDisabledPodFields` also names that are on by default in 1.35
//! (`UserNamespacesSupport`, `SupplementalGroupsPolicy`, `ProcMountType`,
//! `NodeInclusionPolicyInPodTopologySpread`, `MatchLabelKeysInPodAffinity`,
//! `SidecarContainers`, `RecursiveReadOnlyMounts`, `PodLifecycleSleepAction`,
//! `ContainerRestartRules`, `HostnameOverride`, `EnvFiles`, `ImageVolume`,
//! `SELinuxChangePolicy`, `DynamicResourceAllocation`,
//! `PodObservedGenerationTracking`) drop nothing, so they are not modelled.
//! `InPlacePodVerticalScaling` (on, GA-locked), `PodLevelResources` and
//! `MatchLabelKeysInPodTopologySpread` are modelled because the registry may
//! turn them off in a test.

use crate::feature_gates::{enabled, Feature};
use crate::resources::pod::{Container, Lifecycle};
use crate::resources::{ContainerStatus, Pod, PodSpec, PodStatus};

/// `podutil.DropDisabledPodFields(pod, oldPod)` (util.go:697-718): drops the
/// disabled fields from `pod`'s spec and status. `old` is `None` on create.
pub fn drop_disabled_pod_fields(pod: &mut Pod, old: Option<&Pod>) {
    let old_spec = old.and_then(|p| p.spec.as_ref());
    let old_status = old.and_then(|p| p.status.as_ref());
    if let Some(spec) = pod.spec.as_mut() {
        drop_disabled_fields(spec, old_spec);
    }
    // the new status is always non-nil upstream
    let status = pod.status.get_or_insert_with(PodStatus::default);
    drop_disabled_pod_status_fields(status, old_status, old_spec);
}

/// `podutil.DropDisabledTemplateFields(podTemplate, oldPodTemplate)`
/// (util.go:677-695): the same drop as [`drop_disabled_pod_fields`], for the
/// pod template every workload strategy's `PrepareForCreate` /
/// `PrepareForUpdate` carries. `old` is `None` on create.
pub fn drop_disabled_template_fields(
    template: &mut crate::resources::workloads::PodTemplateSpec,
    old: Option<&crate::resources::workloads::PodTemplateSpec>,
) {
    drop_disabled_fields(&mut template.spec, old.map(|t| &t.spec));
}

/// `dropDisabledFields` (util.go:721-814), for the gates in the module doc.
fn drop_disabled_fields(spec: &mut PodSpec, old: Option<&PodSpec>) {
    if !enabled(Feature::PodLevelResources) && !pod_level_resources_in_use(old) {
        spec.resources = None;
    }
    if !enabled(Feature::MatchLabelKeysInPodTopologySpread) && !match_label_keys_in_use(old) {
        for constraint in spec.topology_spread_constraints.iter_mut().flatten() {
            constraint.match_label_keys = None;
        }
    }
    drop_disabled_cluster_trust_bundle_projection(spec, old);
    drop_disabled_pod_certificate_projection(spec, old);
    // dropDisabledWorkloadRef (util.go:1836)
    if !enabled(Feature::GenericWorkload) && old.is_none_or(|o| o.workload_ref.is_none()) {
        spec.workload_ref = None;
    }

    if !enabled(Feature::InPlacePodVerticalScaling) && !in_place_pod_vertical_scaling_in_use(old) {
        // Drop ResizePolicy fields. Don't drop updates to Resources field as
        // template.spec.resources field is mutable for certain controllers.
        for c in spec.containers.iter_mut() {
            c.resize_policy = None;
        }
        for c in spec.init_containers.iter_mut().flatten() {
            c.resize_policy = None;
        }
        for c in spec.ephemeral_containers.iter_mut().flatten() {
            c.resize_policy = None;
        }
    }

    drop_container_stop_signals(spec, old);
}

/// `podLevelResourcesInUse` (util.go:1316-1336).
fn pod_level_resources_in_use(spec: Option<&PodSpec>) -> bool {
    spec.and_then(|s| s.resources.as_ref()).is_some_and(|r| {
        r.requests.as_ref().is_some_and(|m| !m.is_empty())
            || r.limits.as_ref().is_some_and(|m| !m.is_empty())
    })
}

/// `matchLabelKeysInTopologySpreadInUse` (util.go:1246-1259).
fn match_label_keys_in_use(spec: Option<&PodSpec>) -> bool {
    spec.and_then(|s| s.topology_spread_constraints.as_ref())
        .is_some_and(|cs| {
            cs.iter()
                .any(|c| c.match_label_keys.as_ref().is_some_and(|k| !k.is_empty()))
        })
}

/// `inPlacePodVerticalScalingInUse` (util.go:1347-1362): a resize policy on a
/// container or init container.
fn in_place_pod_vertical_scaling_in_use(spec: Option<&PodSpec>) -> bool {
    let Some(spec) = spec else { return false };
    let has_policy = |c: &Container| c.resize_policy.as_ref().is_some_and(|p| !p.is_empty());
    spec.containers.iter().any(has_policy) || spec.init_containers.iter().flatten().any(has_policy)
}

/// `dropDisabledClusterTrustBundleProjection` (util.go:1437-1458).
fn drop_disabled_cluster_trust_bundle_projection(spec: &mut PodSpec, old: Option<&PodSpec>) {
    if enabled(Feature::ClusterTrustBundleProjection) {
        return;
    }
    // If the pod was already using it, it can keep using it.
    if projection_in_use(old, |s| s.cluster_trust_bundle.is_some()) {
        return;
    }
    for volume in spec.volumes.iter_mut().flatten() {
        for source in volume
            .projected
            .iter_mut()
            .flat_map(|p| p.sources.iter_mut().flatten())
        {
            source.cluster_trust_bundle = None;
        }
    }
}

/// `dropDisabledPodCertificateProjection` (util.go:1480-1502).
fn drop_disabled_pod_certificate_projection(spec: &mut PodSpec, old: Option<&PodSpec>) {
    if enabled(Feature::PodCertificateRequest) {
        return;
    }
    // If the pod was already using it, it can keep using it.
    if projection_in_use(old, |s| s.pod_certificate.is_some()) {
        return;
    }
    for volume in spec.volumes.iter_mut().flatten() {
        for source in volume
            .projected
            .iter_mut()
            .flat_map(|p| p.sources.iter_mut().flatten())
        {
            source.pod_certificate = None;
        }
    }
}

/// `clusterTrustBundleProjectionInUse` (util.go:1401-1418) and
/// `podCertificateProjectionInUse` (util.go:1461-1478).
fn projection_in_use(
    spec: Option<&PodSpec>,
    uses: impl Fn(&crate::resources::pod::VolumeProjection) -> bool,
) -> bool {
    spec.is_some_and(|s| {
        s.volumes
            .iter()
            .flatten()
            .filter_map(|v| v.projected.as_ref())
            .flat_map(|p| p.sources.iter().flatten())
            .any(&uses)
    })
}

/// `dropContainerStopSignals` (util.go:856-880): a lifecycle that held only a
/// stop signal goes away with it.
fn drop_container_stop_signals(spec: &mut PodSpec, old: Option<&PodSpec>) {
    if enabled(Feature::ContainerStopSignals) || container_stop_signals_in_use(old) {
        return;
    }
    let wipe = |c: &mut Container| {
        let Some(lifecycle) = c.lifecycle.as_mut() else {
            return;
        };
        if lifecycle.stop_signal.is_some() {
            lifecycle.stop_signal = None;
            if lifecycle_is_empty(lifecycle) {
                c.lifecycle = None;
            }
        }
    };
    spec.containers.iter_mut().for_each(wipe);
    spec.init_containers.iter_mut().flatten().for_each(wipe);
}

/// `*ctr.Lifecycle == (api.Lifecycle{})`.
fn lifecycle_is_empty(lifecycle: &Lifecycle) -> bool {
    lifecycle.post_start.is_none()
        && lifecycle.pre_stop.is_none()
        && lifecycle.stop_signal.is_none()
}

/// `containerStopSignalsInUse` (util.go:882-898).
fn container_stop_signals_in_use(spec: Option<&PodSpec>) -> bool {
    let Some(spec) = spec else { return false };
    let uses = |c: &Container| {
        c.lifecycle
            .as_ref()
            .is_some_and(|l| l.stop_signal.is_some())
    };
    spec.containers.iter().any(uses) || spec.init_containers.iter().flatten().any(uses)
}

/// `dropDisabledPodStatusFields` (util.go:1004-1090), for the gates in the
/// module doc.
fn drop_disabled_pod_status_fields(
    status: &mut PodStatus,
    old_status: Option<&PodStatus>,
    old_spec: Option<&PodSpec>,
) {
    if !enabled(Feature::InPlacePodLevelResourcesVerticalScaling)
        && !old_status.is_some_and(|s| s.resources.is_some() || s.allocated_resources.is_some())
    {
        // Drop Resources and AllocatedResources fields from PodStatus
        status.resources = None;
        status.allocated_resources = None;
    }

    if !enabled(Feature::InPlacePodVerticalScaling)
        && !in_place_pod_vertical_scaling_in_use(old_spec)
    {
        for_each_container_status(status, |cs| {
            cs.resources = None;
            cs.allocated_resources = None;
        });
    }

    if !enabled(Feature::DRAExtendedResource)
        && !old_status.is_some_and(|s| s.extended_resource_claim_status.is_some())
    {
        status.extended_resource_claim_status = None;
    }

    if !enabled(Feature::ResourceHealthStatus) {
        for_each_container_status(status, |cs| cs.allocated_resources_status = None);
    }
}

fn for_each_container_status(status: &mut PodStatus, mut f: impl FnMut(&mut ContainerStatus)) {
    for list in [
        &mut status.container_statuses,
        &mut status.init_container_statuses,
        &mut status.ephemeral_container_statuses,
    ] {
        list.iter_mut().flatten().for_each(&mut f);
    }
}
