//! The memory limit of `cm.ResourceConfigForPod`
//! (`pkg/kubelet/cm/helpers_linux.go:126-223`), which is the only part of the
//! pod `ResourceConfig` its callers here need: emptyDir's
//! `calculateEmptyDirMemorySize` (`pkg/volume/emptydir/empty_dir.go:122-129`)
//! reads `podResourceConfig.Memory`. CPU shares/quota, hugepage limits and
//! `Unified` are not ported (no consumer).

use rusternetes_common::qos::{get_pod_qos, QoSClass};
use rusternetes_common::quota::{parse_resource_list, pod_limits, ResourceList};
use rusternetes_common::resources::Pod;

fn memory_of(list: &ResourceList) -> i128 {
    list.get("memory").map(|q| q.value()).unwrap_or(0)
}

fn container_memory_limit(
    resources: Option<&rusternetes_common::types::ResourceRequirements>,
) -> i128 {
    memory_of(
        &resources
            .and_then(|r| r.limits.as_ref())
            .map(parse_resource_list)
            .unwrap_or_default(),
    )
}

/// `ResourceConfigForPod(pod, ..).Memory`: `None` when upstream leaves the
/// pointer nil (BestEffort, or Burstable with a container lacking a memory
/// limit), else the pod memory limit in bytes (0 when no limit is summed).
///
/// `memoryLimitsDeclared` is cleared by `PodLimits`' `ContainerFn`
/// (`component-helpers/resource/helpers.go:396-445`) for every regular
/// container and for every init container's effective limits (own limits plus
/// the restartable init containers before it; a restartable init container
/// reports the running restartable total).
pub fn resource_config_for_pod_memory(pod: &Pod) -> Option<i64> {
    let spec = pod.spec.as_ref()?;
    let mut memory_limits_declared = true;
    for c in &spec.containers {
        if container_memory_limit(c.resources.as_ref()) == 0 {
            memory_limits_declared = false;
        }
    }
    let mut restartable_total: i128 = 0;
    for c in spec.init_containers.iter().flatten() {
        let own = container_memory_limit(c.resources.as_ref());
        let effective = if c.restart_policy.as_deref() == Some("Always") {
            restartable_total += own;
            restartable_total
        } else {
            own + restartable_total
        };
        if effective == 0 {
            memory_limits_declared = false;
        }
    }
    // Pod-level limits (PodLevelResources) can declare the limit for the pod.
    if rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::PodLevelResources,
    ) {
        let pod_level = spec
            .resources
            .as_ref()
            .and_then(|r| r.limits.as_ref())
            .map(parse_resource_list)
            .unwrap_or_default();
        if memory_of(&pod_level) != 0 {
            memory_limits_declared = true;
        }
    }

    let memory_limits = i64::try_from(memory_of(&pod_limits(pod))).unwrap_or(i64::MAX);
    match get_pod_qos(pod) {
        QoSClass::Guaranteed => Some(memory_limits),
        QoSClass::Burstable => memory_limits_declared.then_some(memory_limits),
        QoSClass::BestEffort => None,
    }
}
