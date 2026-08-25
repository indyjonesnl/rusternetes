//! Pod QoS classification — the single definition of `status.qosClass`.
//!
//! Port of upstream `pkg/apis/core/v1/helper/qos/qos.go`. It lives in `common`
//! because every component that has an opinion about a pod's QoS class must
//! reach the *same* answer:
//!
//! - the **api-server** is the authoritative writer, setting `status.qosClass`
//!   in the registry strategy (`pkg/registry/core/pod/strategy.go:92`,
//!   `QOSClass: qos.GetPodQOS(pod)`);
//! - the **kubelet** recomputes it for the status it posts
//!   (`generateAPIPodStatus`, `pkg/kubelet/kubelet_pods.go:2097`) and orders
//!   eviction victims by it (`pkg/kubelet/eviction/helpers.go`).
//!
//! A disagreement between them is directly observable: `qosClass` flips after
//! the pod is scheduled, and the class a pod is *evicted* by stops matching the
//! class the API reports. Three independent hand-rolled copies existed before
//! this module (kubelet status, kubelet eviction, api-server create) and all
//! three disagreed.

use crate::quantity::Quantity;
use crate::resources::Pod;
use std::collections::HashMap;

/// A pod's QoS class.
///
/// The discriminants are the **eviction order**: the kubelet evicts ascending,
/// BestEffort first, Guaranteed last (`pkg/kubelet/eviction/helpers.go`'s
/// `qosComparator`), so `Ord` on this enum is the comparator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QoSClass {
    /// Guaranteed: requests == limits for cpu and memory on every container.
    Guaranteed = 3,
    /// Burstable: some requests or limits, but not all matched.
    Burstable = 2,
    /// BestEffort: no cpu/memory requests or limits anywhere.
    BestEffort = 1,
}

impl QoSClass {
    /// The `status.qosClass` string. Matches the upstream `v1.PodQOSClass`
    /// constants (`pkg/apis/core/types.go:4331-4335`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Guaranteed => "Guaranteed",
            Self::Burstable => "Burstable",
            Self::BestEffort => "BestEffort",
        }
    }

    /// Parse a published `status.qosClass`. `None` for anything that is not one
    /// of the three `v1.PodQOSClass` constants.
    pub fn from_status_str(value: &str) -> Option<Self> {
        match value {
            "Guaranteed" => Some(Self::Guaranteed),
            "Burstable" => Some(Self::Burstable),
            "BestEffort" => Some(Self::BestEffort),
            _ => None,
        }
    }
}

impl std::fmt::Display for QoSClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A pod's QoS class as every *reader* should ask for it. Port of upstream
/// `GetPodQOS` (`pkg/apis/core/v1/helper/qos/qos.go:37-44`):
///
/// ```text
/// func GetPodQOS(pod *v1.Pod) v1.PodQOSClass {
///     if pod.Status.QOSClass != "" {
///         return pod.Status.QOSClass
///     }
///     return ComputePodQOS(pod)
/// }
/// ```
///
/// The published `status.qosClass` wins. That is not an optimisation — it is
/// what keeps every reader agreeing with the api-server that wrote the field:
/// the kubelet's posted status (`pkg/kubelet/kubelet_pods.go:2097`), eviction
/// (`pkg/kubelet/eviction/eviction_manager.go:165`), the ResourceQuota
/// `BestEffort` scope (`pkg/quota/v1/evaluator/core/pods.go:412-414`) and the
/// CPU-resize node-feature gate
/// (`staging/src/k8s.io/component-helpers/nodedeclaredfeatures/features/inplacepodresize/guaranteed_cpu_resize.go:64`)
/// all read it this way.
///
/// Upstream returns the stored string verbatim, including a value outside the
/// three known classes. This returns the parsed class for the three known ones
/// and recomputes for anything else — an unparseable `status.qosClass` is not a
/// fourth class, and computing is the only meaningful answer a typed enum can
/// give.
///
/// The **writer** — the api-server's pod create path, mirroring
/// `pkg/registry/core/pod/strategy.go:92` — wants [`compute_pod_qos`] instead:
/// it is deriving the field, not reading it.
pub fn get_pod_qos(pod: &Pod) -> QoSClass {
    if let Some(published) = pod
        .status
        .as_ref()
        .and_then(|status| status.qos_class.as_deref())
        .and_then(QoSClass::from_status_str)
    {
        return published;
    }
    compute_pod_qos(pod)
}

/// Compute a pod's QoS class. Port of upstream `ComputePodQOS`
/// (`pkg/apis/core/v1/helper/qos/qos.go:92-172`).
///
/// Upstream, verbatim in structure:
///
/// - only **cpu and memory** count (`supportedQoSComputeResources`, qos.go:29),
///   and only quantities **strictly greater than zero** (`quantity.Cmp(
///   zeroQuantity) == 1`, qos.go:57 / 122). An `nvidia.com/gpu` request or a
///   `cpu: "0"` contributes nothing;
/// - `spec.containers` **and** `spec.initContainers` participate (qos.go:113-116).
///   Ephemeral containers do not — they cannot declare resources;
/// - requests and limits are **summed across containers** and compared by
///   numeric value (`lim.Cmp(req) != 0`, qos.go:161), not as strings — `"1"` and
///   `"1000m"` are the same CPU;
/// - a container whose limits do not cover *both* cpu and memory forfeits
///   Guaranteed for the whole pod (qos.go:149-152);
/// - empty requests **and** empty limits is BestEffort (qos.go:156-158);
/// - Guaranteed additionally requires `len(requests) == len(limits)` (qos.go:168).
///
/// The kubelet calls this rather than [`get_pod_qos`], where upstream calls
/// `GetPodQOS` (`kubelet_pods.go:2097`, `eviction_manager.go:165`): the class it
/// posts and evicts by is recomputed from the spec, so a pod carrying a
/// `status.qosClass` written before the three copies of this rule converged is
/// corrected instead of echoed forever. Both answers agree for any pod whose
/// class was computed by this port.
///
/// Pod-level resources (`spec.resources`, the `PodLevelResources` branch at
/// qos.go:95-110) are **not** implemented: rusternetes has no such feature gate
/// yet, so the container loop is always the one that runs.
///
/// ## The requests-from-limits step
///
/// `ComputePodQOS` never looks at limits when filling `requests` — it relies on
/// `SetDefaults_Pod` (`pkg/apis/core/v1/defaults.go:164-192`) having copied
/// limits into unset requests at admission, which is why a limits-only
/// container is Guaranteed upstream. Rusternetes applies that pass in the same
/// two places upstream does — pod create and static-pod decode, via
/// [`crate::defaults::default_pod_requests_from_limits`] (#1738) — so
/// re-applying it here is normally a no-op. It is re-applied to a local copy
/// anyway, because a pod read back out of storage carries no proof of it: one
/// written before that defaulting existed, or by a writer bypassing the
/// api-server, still arrives undefaulted, and reclassifying such a pod would
/// change which victim the kubelet evicts.
pub fn compute_pod_qos(pod: &Pod) -> QoSClass {
    let Some(spec) = pod.spec.as_ref() else {
        return QoSClass::BestEffort;
    };

    // Summed per resource: cpu in milli-units, memory in bytes.
    let mut requests: HashMap<&'static str, i128> = HashMap::new();
    let mut limits: HashMap<&'static str, i128> = HashMap::new();
    let mut is_guaranteed = true;

    for container in spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
    {
        let mut container = container.clone();
        crate::defaults::default_container_requests_from_limits(&mut container);
        let resources = container.resources.as_ref();

        if let Some(map) = resources.and_then(|r| r.requests.as_ref()) {
            process_resource_list(map, &mut requests);
        }

        let mut qos_limits_found = 0u8;
        if let Some(map) = resources.and_then(|r| r.limits.as_ref()) {
            for name in process_resource_list(map, &mut limits) {
                qos_limits_found |= if name == "cpu" { 1 } else { 2 };
            }
        }
        // `!qosLimitsFound.HasAll(memory, cpu)` — both bits, or not Guaranteed.
        if qos_limits_found != 3 {
            is_guaranteed = false;
        }
    }

    if requests.is_empty() && limits.is_empty() {
        return QoSClass::BestEffort;
    }

    if is_guaranteed {
        for (name, req) in &requests {
            if limits.get(name) != Some(req) {
                is_guaranteed = false;
                break;
            }
        }
    }

    if is_guaranteed && requests.len() == limits.len() {
        QoSClass::Guaranteed
    } else {
        QoSClass::Burstable
    }
}

/// Add a container's cpu/memory quantities into the running per-resource totals,
/// returning which of them it contributed. Port of upstream `processResourceList`
/// (`pkg/apis/core/v1/helper/qos/qos.go:50-66`) fused with `getQOSResources`
/// (qos.go:70-82): unsupported resources and non-positive quantities are skipped
/// by both.
fn process_resource_list(
    list: &HashMap<String, String>,
    totals: &mut HashMap<&'static str, i128>,
) -> Vec<&'static str> {
    let mut found = Vec::new();
    for name in ["cpu", "memory"] {
        let Some(raw) = list.get(name) else { continue };
        let Ok(quantity) = Quantity::parse(raw.trim()) else {
            continue;
        };
        let value = if name == "cpu" {
            quantity.milli_value()
        } else {
            quantity.value()
        };
        if value > 0 {
            *totals.entry(name).or_insert(0) += value;
            found.push(name);
        }
    }
    found
}
