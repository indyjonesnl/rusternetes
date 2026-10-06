//! ResourceQuota accounting on `Quantity`, not on `i64`.
//!
//! Port of upstream's quota arithmetic — `staging/src/k8s.io/apiserver/pkg/quota/v1/resources.go`
//! (`Add` / `Max` / `Subtract` / `LessThanOrEqual` / `Mask` / `Equals`) — plus the
//! pod usage evaluator that feeds it:
//!
//! * `staging/src/k8s.io/component-helpers/resource/helpers.go:149` `PodRequests`
//!   and `:341` `PodLimits` (container sum, sidecar-aware init-container max,
//!   pod-level resources, overhead),
//! * `pkg/quota/v1/evaluator/core/pods.go:294` `podComputeUsageHelper` (which
//!   quota keys a pod's requests and limits land under) and `:491` `QuotaV1Pod`
//!   (which pods are charged at all).
//!
//! ### Why a module and not another pair of parse helpers
//!
//! Upstream carries `spec.hard`, `status.used`, LimitRange bounds and container
//! resources as typed `resource.Quantity`: parsed once at decode time, then
//! added and compared as quantities from there on. Rusternetes carries them as
//! `String` and re-parses at each use, and the natural shortcut — reduce to an
//! `i64` of millicores or bytes, do the arithmetic there — silently changes what
//! a quantity means:
//!
//! * `Value()` rounds up away from zero, so `100m` of **memory** (a tenth of a
//!   byte, legal and occasionally written) becomes `1` byte. Two distinct
//!   quantities compare equal, and `status.used` reports the rounded figure.
//! * The unit is chosen per resource *name*, so any resource that is neither
//!   `cpu` nor byte-denominated has to be guessed at. That guess is where the
//!   `qty.parse::<i64>()` calls this module replaces came from — and a `2k`
//!   extended-resource request parsed as `None`, i.e. as asking for nothing.
//!
//! Keeping `Quantity` end to end removes both. Only the boundary — reading
//! `spec.hard` strings in, writing `status.used` strings out — converts.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Duration, Utc};

use crate::quantity::{Format, Quantity};
use crate::resources::{PersistentVolumeClaim, Pod};
use crate::types::{Phase, ResourceRequirements};

/// A resource name → quantity map. Upstream `corev1.ResourceList`.
///
/// `BTreeMap` rather than `HashMap` so [`pretty_print`] and [`resource_names`]
/// are ordered without a sort at every call — upstream sorts explicitly in
/// `prettyPrint` (`plugin/resourcequota/controller.go:741-754`).
pub type ResourceList = BTreeMap<String, Quantity>;

/// Prefix every quota key for a request-side resource carries.
/// Upstream `corev1.DefaultResourceRequestsPrefix`.
pub const REQUESTS_PREFIX: &str = "requests.";
/// Prefix every quota key for a limit-side resource carries.
/// Upstream `corev1.DefaultResourceLimitsPrefix`.
pub const LIMITS_PREFIX: &str = "limits.";
/// Upstream `corev1.ResourceHugePagesPrefix`.
pub const HUGEPAGES_PREFIX: &str = "hugepages-";
/// Upstream `corev1.ResourceDefaultNamespacePrefix`.
pub const KUBERNETES_IO_PREFIX: &str = "kubernetes.io/";

// ---------------------------------------------------------------------------
// ResourceList arithmetic — port of quota/v1/resources.go
// ---------------------------------------------------------------------------

/// Parse a raw `name -> quantity string` map into a [`ResourceList`].
///
/// Unparseable entries are dropped rather than failing the whole map. Upstream
/// never needs this: the API types are already `resource.Quantity`, so a bad
/// quantity is rejected at decode time and this layer only ever sees valid
/// ones. Rusternetes stores strings, so a value that predates a validator (or
/// arrives on a path that has none) must not take a whole quota offline —
/// dropping the key means that dimension goes unaccounted, which is what
/// upstream's decode failure would also amount to.
pub fn parse_resource_list(raw: &HashMap<String, String>) -> ResourceList {
    raw.iter()
        .filter_map(|(name, value)| {
            Quantity::parse(value.trim())
                .ok()
                .map(|q| (name.clone(), q))
        })
        .collect()
}

/// Render a [`ResourceList`] back to the canonical strings the API stores.
/// Each value round-trips through upstream `Quantity.String()`.
pub fn to_string_map(list: &ResourceList) -> HashMap<String, String> {
    list.iter()
        .map(|(name, q)| (name.clone(), q.canonical_string()))
        .collect()
}

/// True when both lists hold the same names and value-equal quantities.
/// Port of upstream `Equals` (`resources.go:30-47`).
pub fn equals(a: &ResourceList, b: &ResourceList) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .all(|(name, va)| b.get(name).is_some_and(|vb| va.value_eq(vb)))
}

/// `a <= b` for every name **present in `b`**; names only in `a` are ignored.
/// Returns the names of `a` that exceeded `b`. Port of upstream
/// `LessThanOrEqual` (`resources.go:50-62`).
///
/// The asymmetry is deliberate upstream: `b` is `status.hard`, so a resource
/// the quota does not constrain cannot fail the check.
pub fn less_than_or_equal(a: &ResourceList, b: &ResourceList) -> (bool, Vec<String>) {
    let mut exceeded = Vec::new();
    for (name, limit) in b {
        if let Some(used) = a.get(name) {
            if used.cmp_value(limit) == std::cmp::Ordering::Greater {
                exceeded.push(name.clone());
            }
        }
    }
    (exceeded.is_empty(), exceeded)
}

/// Per-name `max(a, b)`, keeping names present in either.
/// Port of upstream `Max` (`resources.go:65-82`).
pub fn max(a: &ResourceList, b: &ResourceList) -> ResourceList {
    let mut result = ResourceList::new();
    for (name, value) in a {
        match b.get(name) {
            Some(other) if value.cmp_value(other) != std::cmp::Ordering::Greater => {
                result.insert(name.clone(), *other);
            }
            _ => {
                result.insert(name.clone(), *value);
            }
        }
    }
    for (name, value) in b {
        result.entry(name.clone()).or_insert(*value);
    }
    result
}

/// Per-name `a + b`, keeping names present in either.
/// Port of upstream `Add` (`resources.go:85-100`).
pub fn add(a: &ResourceList, b: &ResourceList) -> ResourceList {
    let mut result = ResourceList::new();
    for (name, value) in a {
        let sum = match b.get(name) {
            Some(other) => saturating_add(value, other),
            None => *value,
        };
        result.insert(name.clone(), sum);
    }
    for (name, value) in b {
        result.entry(name.clone()).or_insert(*value);
    }
    result
}

/// Per-name `a - b`; a name only in `b` yields its negation.
/// Port of upstream `Subtract` (`resources.go:129-146`).
pub fn subtract(a: &ResourceList, b: &ResourceList) -> ResourceList {
    let mut result = ResourceList::new();
    for (name, value) in a {
        let diff = match b.get(name) {
            Some(other) => value.sub(other).unwrap_or(*value),
            None => *value,
        };
        result.insert(name.clone(), diff);
    }
    for (name, value) in b {
        result.entry(name.clone()).or_insert_with(|| value.neg());
    }
    result
}

/// Per-name `a - b` clamped at zero, so usage never goes negative.
/// Port of upstream `SubtractWithNonNegativeResult` (`resources.go:104-126`).
pub fn subtract_with_non_negative_result(a: &ResourceList, b: &ResourceList) -> ResourceList {
    let zero = Quantity::from_value(0, Format::DecimalSI);
    let mut result = ResourceList::new();
    for (name, value) in a {
        let diff = match b.get(name) {
            Some(other) => value.sub(other).unwrap_or(*value),
            None => *value,
        };
        let clamped = if diff.cmp_value(&zero) == std::cmp::Ordering::Greater {
            diff
        } else {
            zero
        };
        result.insert(name.clone(), clamped);
    }
    for name in b.keys() {
        result.entry(name.clone()).or_insert(zero);
    }
    result
}

/// Restrict `list` to `names`. Port of upstream `Mask` (`resources.go:149-158`).
pub fn mask(list: &ResourceList, names: &[String]) -> ResourceList {
    names
        .iter()
        .filter_map(|name| list.get(name).map(|q| (name.clone(), *q)))
        .collect()
}

/// The names in `list`. Port of upstream `ResourceNames` (`resources.go:161-167`).
pub fn resource_names(list: &ResourceList) -> Vec<String> {
    list.keys().cloned().collect()
}

/// True when every value is zero. Port of upstream `IsZero` (`resources.go:219-227`).
pub fn is_zero(list: &ResourceList) -> bool {
    list.values().all(|q| q.is_zero())
}

/// Drop the zero-valued names. Port of upstream `RemoveZeros`
/// (`resources.go:230-238`).
///
/// Admission uses this to short-circuit: a delta with nothing left in it cannot
/// exceed anything, so an UPDATE that changes no charged dimension is admitted
/// without touching the quota (`plugin/resourcequota/controller.go:555-565`).
pub fn remove_zeros(list: &ResourceList) -> ResourceList {
    list.iter()
        .filter(|(_, q)| !q.is_zero())
        .map(|(name, q)| (name.clone(), *q))
        .collect()
}

/// The names whose value is negative. Port of upstream `IsNegative`
/// (`resources.go:241-250`).
pub fn is_negative(list: &ResourceList) -> Vec<String> {
    list.iter()
        .filter(|(_, q)| q.is_negative())
        .map(|(name, _)| name.clone())
        .collect()
}

/// `name=value,name=value` in name order, values in canonical form. Port of
/// upstream `prettyPrint` (`plugin/resourcequota/controller.go:741-754`) — the
/// exact rendering the `exceeded quota` admission message is built from.
pub fn pretty_print(list: &ResourceList) -> String {
    list.iter()
        .map(|(name, q)| format!("{}={}", name, q.canonical_string()))
        .collect::<Vec<_>>()
        .join(",")
}

/// `a + b`, saturating to `i64::MAX` on `i128` mantissa overflow instead of
/// returning `None`.
///
/// Upstream's `Quantity` falls back to arbitrary-precision `inf.Dec` and cannot
/// overflow, so it has no equivalent branch. Saturating up is the safe
/// direction for a quota: an over-stated usage rejects a request that upstream
/// would also have had no room for, whereas dropping the addend would admit it.
fn saturating_add(a: &Quantity, b: &Quantity) -> Quantity {
    a.add(b)
        .unwrap_or_else(|| Quantity::from_value(i64::MAX, a.format()))
}

// ---------------------------------------------------------------------------
// Resource-name classification — port of pkg/apis/core/v1/helper
// ---------------------------------------------------------------------------

/// True for `hugepages-*`. Upstream `v1helper.IsHugePageResourceName`.
pub fn is_hugepage_resource_name(name: &str) -> bool {
    name.starts_with(HUGEPAGES_PREFIX)
}

/// True for a resource that is native to Kubernetes: either unqualified (no
/// `/`) or under the `kubernetes.io/` namespace. Upstream
/// `v1helper.IsNativeResource`.
pub fn is_native_resource_name(name: &str) -> bool {
    !name.contains('/') || name.starts_with(KUBERNETES_IO_PREFIX)
}

/// True for an extended resource — a non-native, non-`requests.`-prefixed
/// name, which quota tracks only under `requests.<name>`. Upstream
/// `v1helper.IsExtendedResourceName`.
///
/// Upstream additionally runs `IsQualifiedName` on `requests.<name>`; that
/// validation lives in `crate::validation` and every name reaching here has
/// already been through pod validation, so the shape check is not repeated.
pub fn is_extended_resource_name(name: &str) -> bool {
    !is_native_resource_name(name) && !name.starts_with(REQUESTS_PREFIX)
}

// ---------------------------------------------------------------------------
// Pod usage — port of component-helpers/resource/helpers.go + evaluator/core/pods.go
// ---------------------------------------------------------------------------

/// Names a pod-level `spec.resources` entry may carry. Upstream
/// `supportedPodLevelResources` (`component-helpers/resource/helpers.go`) plus
/// hugepages.
pub fn is_supported_pod_level_resource(name: &str) -> bool {
    name == "cpu" || name == "memory" || is_hugepage_resource_name(name)
}

/// True when `container.restartPolicy: Always` marks this init container as a
/// sidecar, so its resources are held for the pod's whole life rather than only
/// while it initialises. Upstream `v1.ContainerRestartPolicyAlways`.
fn is_restartable_init(restart_policy: Option<&String>) -> bool {
    restart_policy.is_some_and(|p| p == "Always")
}

/// One container's request (or limit) map as a [`ResourceList`].
fn side(resources: Option<&ResourceRequirements>, requests: bool) -> ResourceList {
    let raw = resources.and_then(|r| if requests { &r.requests } else { &r.limits }.as_ref());
    raw.map(parse_resource_list).unwrap_or_default()
}

/// Aggregate container requests (`requests = true`) or limits for a pod,
/// following the sidecar-containers formula.
///
/// Port of upstream `AggregateContainerRequests`
/// (`component-helpers/resource/helpers.go:191-278`) and
/// `AggregateContainerLimits` (`:382-455`), which share this shape:
///
/// ```text
/// sum(app containers)                                       // baseline
///   then, walking init containers in order:
///     restartPolicy=Always  -> add to the baseline AND to a running
///                              restartable total; the container's own
///                              figure for the max is that running total
///     otherwise             -> the container's figure is its own request
///                              plus the restartable total so far
///   max(baseline, max over init containers)
/// ```
///
/// The init-container `max` is why summing only `spec.containers` under-charges
/// quota: a pod whose init container asks for `4Gi` and whose app container asks
/// for `1Gi` occupies `4Gi` at its peak.
///
/// The `UseStatusResources` arm of upstream's version is not ported: it reads
/// actuated figures out of `status.containerStatuses` to price an in-place
/// resize in flight, and quota here is computed from `spec`.
pub fn aggregate_container_resources(pod: &Pod, requests: bool) -> ResourceList {
    let Some(spec) = &pod.spec else {
        return ResourceList::new();
    };

    let mut total = ResourceList::new();
    for container in &spec.containers {
        total = add(&total, &side(container.resources.as_ref(), requests));
    }

    let mut restartable = ResourceList::new();
    let mut init_max = ResourceList::new();
    for container in spec.init_containers.iter().flatten() {
        let own = side(container.resources.as_ref(), requests);
        let effective = if is_restartable_init(container.restart_policy.as_ref()) {
            total = add(&total, &own);
            restartable = add(&restartable, &own);
            restartable.clone()
        } else {
            add(&own, &restartable)
        };
        init_max = max(&init_max, &effective);
    }

    max(&total, &init_max)
}

/// Total pod requests. Port of upstream `PodRequests`
/// (`component-helpers/resource/helpers.go:149-185`): container aggregate, then
/// pod-level `spec.resources.requests` overriding per name, then `spec.overhead`
/// added.
///
/// Pod-level resources are applied only when `PodLevelResources` is enabled
/// (`SkipPodLevelResources: !Enabled(PodLevelResources)`,
/// `pkg/quota/v1/evaluator/core/pods.go:403-404`; `helpers.go:155`).
pub fn pod_requests(pod: &Pod) -> ResourceList {
    let mut reqs = aggregate_container_resources(pod, true);
    let Some(spec) = &pod.spec else {
        return reqs;
    };

    if let Some(pod_level) = spec
        .resources
        .as_ref()
        .filter(|_| crate::feature_gates::enabled(crate::feature_gates::Feature::PodLevelResources))
        .and_then(|r| r.requests.as_ref())
        .map(parse_resource_list)
    {
        for (name, value) in pod_level {
            if is_supported_pod_level_resource(&name) {
                reqs.insert(name, value);
            }
        }
    }

    if let Some(overhead) = spec.overhead.as_ref().map(parse_resource_list) {
        reqs = add(&reqs, &overhead);
    }

    reqs
}

/// Total pod limits. Port of upstream `PodLimits`
/// (`component-helpers/resource/helpers.go:341-376`).
///
/// Overhead is added only to limits that are already present and non-zero
/// (`:366-373`): a resource the pod does not limit must not acquire a limit
/// equal to the overhead alone.
pub fn pod_limits(pod: &Pod) -> ResourceList {
    let mut limits = aggregate_container_resources(pod, false);
    let Some(spec) = &pod.spec else {
        return limits;
    };

    if let Some(pod_level) = spec
        .resources
        .as_ref()
        .filter(|_| crate::feature_gates::enabled(crate::feature_gates::Feature::PodLevelResources))
        .and_then(|r| r.limits.as_ref())
        .map(parse_resource_list)
    {
        for (name, value) in pod_level {
            if is_supported_pod_level_resource(&name) {
                limits.insert(name, value);
            }
        }
    }

    if let Some(overhead) = spec.overhead.as_ref().map(parse_resource_list) {
        for (name, extra) in overhead {
            if let Some(existing) = limits.get(&name) {
                if !existing.is_zero() {
                    let sum = saturating_add(existing, &extra);
                    limits.insert(name, sum);
                }
            }
        }
    }

    limits
}

/// Map a pod's requests and limits onto the quota keys they are charged
/// against. Port of upstream `podComputeUsageHelper`
/// (`pkg/quota/v1/evaluator/core/pods.go:294-331`).
///
/// * `cpu` / `memory` / `ephemeral-storage` requests land under both the bare
///   name and `requests.<name>`; their limits under `limits.<name>`.
/// * `hugepages-*` requests land under both the bare name and `requests.<name>`.
/// * Extended resources land **only** under `requests.<name>` — upstream
///   deliberately refuses to let a quota constrain them by bare name.
/// * `pods` is always `1`.
pub fn pod_compute_usage(requests: &ResourceList, limits: &ResourceList) -> ResourceList {
    let mut result = ResourceList::new();
    result.insert(
        "pods".to_string(),
        Quantity::from_value(1, Format::DecimalSI),
    );

    for name in ["cpu", "memory", "ephemeral-storage"] {
        if let Some(request) = requests.get(name) {
            result.insert(name.to_string(), *request);
            result.insert(format!("{}{}", REQUESTS_PREFIX, name), *request);
        }
        if let Some(limit) = limits.get(name) {
            result.insert(format!("{}{}", LIMITS_PREFIX, name), *limit);
        }
    }

    for (name, request) in requests {
        if is_hugepage_resource_name(name) {
            result.insert(name.clone(), *request);
            result.insert(format!("{}{}", REQUESTS_PREFIX, name), *request);
        }
        if is_extended_resource_name(name) {
            result.insert(format!("{}{}", REQUESTS_PREFIX, name), *request);
        }
    }

    result
}

/// `validationSet` (`pkg/quota/v1/evaluator/core/pods.go:97-110`): the only
/// resources whose omission makes a container invalid under a quota. Upstream:
/// "do not add more resources to this list!"
const POD_VALIDATION_SET: [&str; 6] = [
    "cpu",
    "memory",
    "requests.cpu",
    "requests.memory",
    "limits.cpu",
    "limits.memory",
];

/// Port of `podEvaluator.Constraints` (`pkg/quota/v1/evaluator/core/pods.go:
/// 124-168`) with `enforcePodContainerConstraints` (`:275-291`): every
/// container and init container must cover each `required` resource that is in
/// `validationSet`, as measured by [`pod_compute_usage`].
///
/// `required` is the quota's hard names the evaluator matches
/// (`controller.go:469`). Skipped when pod-level resources are set
/// (`IsPodLevelResourcesSet`, `component-helpers/resource/helpers.go:80-90`;
/// `PodLevelResources` is on by default from 1.34).
///
/// Error text is upstream's: resources sorted, each `<resource> for:
/// <sorted,comma-joined containers>`, joined with `"; "`.
pub fn pod_constraints(pod: &Pod, required: &[String]) -> Result<(), String> {
    let Some(spec) = &pod.spec else {
        return Ok(());
    };
    if crate::feature_gates::enabled(crate::feature_gates::Feature::PodLevelResources)
        && spec.resources.as_ref().is_some_and(|r| {
            r.requests.as_ref().is_some_and(|m| !m.is_empty())
                || r.limits.as_ref().is_some_and(|m| !m.is_empty())
        })
    {
        return Ok(());
    }

    let required_set: BTreeSet<&str> = required
        .iter()
        .map(String::as_str)
        .filter(|n| POD_VALIDATION_SET.contains(n))
        .collect();

    let mut missing: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for container in spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
    {
        let usage = pod_compute_usage(
            &side(container.resources.as_ref(), true),
            &side(container.resources.as_ref(), false),
        );
        for name in &required_set {
            if !usage.contains_key(*name) {
                missing.entry(name).or_default().insert(&container.name);
            }
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    let messages: Vec<String> = missing
        .iter()
        .map(|(resource, containers)| {
            format!(
                "{} for: {}",
                resource,
                containers.iter().copied().collect::<Vec<_>>().join(",")
            )
        })
        .collect();
    Err(format!("must specify {}", messages.join("; ")))
}

/// A pod's full quota footprint: `count/pods` (charged even for a terminal pod,
/// which is what object-count quota tracks) plus, when [`is_quota_charged`],
/// [`pod_compute_usage`] over [`pod_requests`] and [`pod_limits`].
///
/// Port of upstream `PodUsageFunc` (`pkg/quota/v1/evaluator/core/pods.go:381-411`).
pub fn pod_usage(pod: &Pod, now: DateTime<Utc>) -> ResourceList {
    let mut usage = ResourceList::new();
    usage.insert(
        "count/pods".to_string(),
        Quantity::from_value(1, Format::DecimalSI),
    );
    if !is_quota_charged(pod, now) {
        return usage;
    }
    add(
        &usage,
        &pod_compute_usage(&pod_requests(pod), &pod_limits(pod)),
    )
}

/// True when a pod is charged for compute quota. Port of upstream `QuotaV1Pod`
/// (`pkg/quota/v1/evaluator/core/pods.go:491-508`).
///
/// A terminal pod is never charged. A pod that is *terminating* still is, until
/// its deletion grace period has elapsed — upstream stops charging past that
/// point so a pod wedged in `Terminating` on a lost node cannot block a scale-up
/// (`pods.go:496-498`). A `deletionTimestamp` on its own does not stop the
/// charge, so dropping every pod with one under-counts usage.
pub fn is_quota_charged(pod: &Pod, now: DateTime<Utc>) -> bool {
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref());
    if matches!(phase, Some(Phase::Succeeded) | Some(Phase::Failed)) {
        return false;
    }
    if let (Some(deleted_at), Some(grace)) = (
        pod.metadata.deletion_timestamp,
        pod.metadata.deletion_grace_period_seconds,
    ) {
        if now > deleted_at + Duration::seconds(grace) {
            return false;
        }
    }
    true
}

/// `storageClassSuffix` (`pkg/quota/v1/evaluator/core/persistent_volume_claims.go:47`).
pub const STORAGE_CLASS_SUFFIX: &str = ".storageclass.storage.k8s.io/";

/// `corev1.BetaStorageClassAnnotation`.
const BETA_STORAGE_CLASS_ANNOTATION: &str = "volume.beta.kubernetes.io/storage-class";

/// `GetPersistentVolumeClaimClass`
/// (`staging/src/k8s.io/component-helpers/storage/volume/helpers.go:43-54`):
/// the beta annotation first, then `spec.storageClassName`.
pub fn persistent_volume_claim_class(pvc: &PersistentVolumeClaim) -> String {
    if let Some(class) = pvc
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(BETA_STORAGE_CLASS_ANNOTATION))
    {
        return class.clone();
    }
    pvc.spec.storage_class_name.clone().unwrap_or_default()
}

/// `pvcEvaluator.getStorageUsage`
/// (`persistent_volume_claims.go:195-224`): the storage request rounded up
/// to whole bytes (issue 94313), or a larger `status.allocatedResources`
/// (`RecoverVolumeExpansionFailure`, beta and on by default in 1.35).
fn pvc_storage_usage(pvc: &PersistentVolumeClaim) -> Option<Quantity> {
    let round_up = |q: Quantity| Quantity::from_value(q.value() as i64, q.format());
    let request = pvc
        .spec
        .resources
        .requests
        .as_ref()
        .and_then(|r| r.get("storage"))
        .and_then(|v| Quantity::parse(v.trim()).ok())?;
    let mut result = round_up(request);
    if let Some(allocated) = pvc
        .status
        .as_ref()
        .and_then(|s| s.allocated_resources.as_ref())
        .and_then(|r| r.get("storage"))
        .and_then(|v| Quantity::parse(v.trim()).ok())
    {
        if allocated.cmp_value(&result) == std::cmp::Ordering::Greater {
            result = round_up(allocated);
        }
    }
    Some(result)
}

/// `pvcEvaluator.Usage` (`persistent_volume_claims.go:164-193`): what one
/// claim is charged, under its own keys and the per-storage-class ones.
pub fn pvc_usage(pvc: &PersistentVolumeClaim) -> ResourceList {
    let one = || Quantity::from_value(1, Format::DecimalSI);
    let mut result = ResourceList::new();
    result.insert("persistentvolumeclaims".to_string(), one());
    result.insert("count/persistentvolumeclaims".to_string(), one());
    let class = persistent_volume_claim_class(pvc);
    if !class.is_empty() {
        result.insert(
            format!("{class}{STORAGE_CLASS_SUFFIX}persistentvolumeclaims"),
            one(),
        );
    }
    if let Some(storage) = pvc_storage_usage(pvc) {
        result.insert("requests.storage".to_string(), storage);
        if !class.is_empty() {
            result.insert(
                format!("{class}{STORAGE_CLASS_SUFFIX}requests.storage"),
                storage,
            );
        }
    }
    result
}

/// `pvcEvaluator.MatchingResources` (`persistent_volume_claims.go:129-151`).
pub fn pvc_matches_resource_name(name: &str) -> bool {
    matches!(
        name,
        "persistentvolumeclaims" | "requests.storage" | "count/persistentvolumeclaims"
    ) || name.ends_with(&format!("{STORAGE_CLASS_SUFFIX}persistentvolumeclaims"))
        || name.ends_with(&format!("{STORAGE_CLASS_SUFFIX}requests.storage"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{Container, PodSpec};

    fn q(s: &str) -> Quantity {
        Quantity::parse(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
    }

    /// `name: quantity` list from string pairs.
    fn rl(pairs: &[(&str, &str)]) -> ResourceList {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), q(value)))
            .collect()
    }

    fn raw(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn container(name: &str, requests: &[(&str, &str)], limits: &[(&str, &str)]) -> Container {
        Container {
            name: name.to_string(),
            image: "busybox".to_string(),
            resources: Some(ResourceRequirements {
                requests: (!requests.is_empty()).then(|| raw(requests)),
                limits: (!limits.is_empty()).then(|| raw(limits)),
                claims: None,
            }),
            ..Default::default()
        }
    }

    fn sidecar(name: &str, requests: &[(&str, &str)]) -> Container {
        Container {
            restart_policy: Some("Always".to_string()),
            ..container(name, requests, &[])
        }
    }

    fn pod(containers: Vec<Container>, init: Vec<Container>) -> Pod {
        Pod::new(
            "p",
            PodSpec {
                containers,
                init_containers: (!init.is_empty()).then_some(init),
                ..Default::default()
            },
        )
    }

    // -----------------------------------------------------------------
    // ResourceList arithmetic — tables ported from upstream
    // staging/src/k8s.io/apiserver/pkg/quota/v1/resources_test.go
    // -----------------------------------------------------------------

    /// Port of upstream `TestEquals` (`resources_test.go:27-78`).
    #[test]
    fn test_equals_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, bool)> = vec![
            ("isEqual", rl(&[]), rl(&[]), true),
            (
                "isEqualWithKeys",
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                true,
            ),
            (
                "isNotEqualSameKeys",
                rl(&[("cpu", "200m"), ("memory", "1Gi")]),
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                false,
            ),
            (
                "isNotEqualDiffKeys",
                rl(&[("cpu", "100m"), ("memory", "1Gi")]),
                rl(&[("cpu", "100m"), ("memory", "1Gi"), ("pods", "1")]),
                false,
            ),
        ];
        for (name, a, b, expected) in cases {
            assert_eq!(equals(&a, &b), expected, "{name}");
        }
    }

    /// Port of upstream `TestLessThanOrEqual` (`resources_test.go:80-117`).
    #[test]
    fn test_less_than_or_equal_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, bool, Vec<&str>)> = vec![
            ("isEmpty", rl(&[]), rl(&[]), true, vec![]),
            (
                "isEqual",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
                true,
                vec![],
            ),
            (
                "isLessThan",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "200m")]),
                true,
                vec![],
            ),
            (
                "isGreaterThan",
                rl(&[("cpu", "200m")]),
                rl(&[("cpu", "100m")]),
                false,
                vec!["cpu"],
            ),
        ];
        for (name, a, b, allowed, exceeded) in cases {
            let (got_allowed, got_exceeded) = less_than_or_equal(&a, &b);
            assert_eq!(got_allowed, allowed, "{name}");
            assert_eq!(got_exceeded, exceeded, "{name}");
        }
    }

    /// Port of upstream `TestMax` (`resources_test.go:119-157`).
    #[test]
    fn test_max_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, ResourceList)> = vec![
            ("noKeys", rl(&[]), rl(&[]), rl(&[])),
            (
                "toEmpty",
                rl(&[("cpu", "100m")]),
                rl(&[]),
                rl(&[("cpu", "100m")]),
            ),
            (
                "matching",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "150m")]),
                rl(&[("cpu", "150m")]),
            ),
            (
                "matching(reverse)",
                rl(&[("cpu", "150m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "150m")]),
            ),
            (
                "matching-equal",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
            ),
        ];
        for (name, a, b, expected) in cases {
            assert!(equals(&expected, &max(&a, &b)), "{name}");
        }
    }

    /// Port of upstream `TestAdd` (`resources_test.go:159-187`).
    #[test]
    fn test_add_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, ResourceList)> = vec![
            ("noKeys", rl(&[]), rl(&[]), rl(&[])),
            (
                "toEmpty",
                rl(&[("cpu", "100m")]),
                rl(&[]),
                rl(&[("cpu", "100m")]),
            ),
            (
                "matching",
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "200m")]),
            ),
        ];
        for (name, a, b, expected) in cases {
            assert!(equals(&expected, &add(&a, &b)), "{name}");
        }
    }

    /// Port of upstream `TestSubtract` (`resources_test.go:189-222`).
    #[test]
    fn test_subtract_upstream_table() {
        let cases: Vec<(&str, ResourceList, ResourceList, ResourceList)> = vec![
            ("noKeys", rl(&[]), rl(&[]), rl(&[])),
            (
                "value-empty",
                rl(&[("cpu", "100m")]),
                rl(&[]),
                rl(&[("cpu", "100m")]),
            ),
            (
                "empty-value",
                rl(&[]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "-100m")]),
            ),
            (
                "value-value",
                rl(&[("cpu", "200m")]),
                rl(&[("cpu", "100m")]),
                rl(&[("cpu", "100m")]),
            ),
        ];
        for (name, a, b, expected) in cases {
            assert!(equals(&expected, &subtract(&a, &b)), "{name}");
        }
    }

    #[test]
    fn test_subtract_with_non_negative_result_clamps() {
        let got = subtract_with_non_negative_result(
            &rl(&[("cpu", "100m"), ("memory", "2Gi")]),
            &rl(&[("cpu", "500m")]),
        );
        assert!(got["cpu"].is_zero());
        assert!(got["memory"].value_eq(&q("2Gi")));
        // A name only in `b` still appears, at zero.
        let only_b = subtract_with_non_negative_result(&rl(&[]), &rl(&[("pods", "3")]));
        assert!(only_b["pods"].is_zero());
    }

    /// Upstream's `Add` adopts the addend's format when the accumulator is
    /// zero (`quantity.go:604-606`); folding from an empty list must therefore
    /// come out in the addend's suffix, not in raw bytes.
    #[test]
    fn test_add_preserves_binary_si_when_folding_from_empty() {
        let folded = add(
            &add(&rl(&[]), &rl(&[("memory", "512Mi")])),
            &rl(&[("memory", "512Mi")]),
        );
        assert_eq!(folded["memory"].canonical_string(), "1Gi");
    }

    /// The whole point of the module: memory `100m` is a tenth of a byte and
    /// must not compare equal to `1`. The `i64`-bytes route ceilinged both to 1.
    #[test]
    fn test_sub_byte_memory_quantity_survives() {
        let (allowed, exceeded) = less_than_or_equal(
            &rl(&[("requests.memory", "100m")]),
            &rl(&[("requests.memory", "1")]),
        );
        assert!(allowed);
        assert!(exceeded.is_empty());
        assert!(!q("100m").value_eq(&q("1")));
        assert_eq!(
            add(
                &rl(&[("requests.memory", "100m")]),
                &rl(&[("requests.memory", "100m")])
            )["requests.memory"]
                .canonical_string(),
            "200m"
        );
    }

    #[test]
    fn test_mask_and_resource_names_and_pretty_print() {
        let list = rl(&[
            ("requests.cpu", "1"),
            ("requests.memory", "1Gi"),
            ("pods", "2"),
        ]);
        assert_eq!(
            resource_names(&list),
            vec!["pods", "requests.cpu", "requests.memory"]
        );
        let masked = mask(&list, &["requests.cpu".to_string(), "nope".to_string()]);
        assert_eq!(resource_names(&masked), vec!["requests.cpu"]);
        // Upstream prettyPrint: name=value, name-sorted, comma-joined.
        assert_eq!(
            pretty_print(&list),
            "pods=2,requests.cpu=1,requests.memory=1Gi"
        );
    }

    #[test]
    fn test_parse_resource_list_drops_unparseable_and_trims() {
        let list = parse_resource_list(&raw(&[
            ("cpu", " 500m "),
            ("memory", "0.5Gi"),
            ("broken", "1GiGi"),
        ]));
        assert_eq!(list.len(), 2);
        assert!(list["cpu"].value_eq(&q("500m")));
        assert!(list["memory"].value_eq(&q("536870912")));
    }

    #[test]
    fn test_is_zero_and_to_string_map() {
        assert!(is_zero(&rl(&[("cpu", "0"), ("memory", "0")])));
        assert!(!is_zero(&rl(&[("cpu", "0"), ("memory", "1")])));
        let strings = to_string_map(&rl(&[("requests.cpu", "2000m")]));
        // Canonical form, not the input spelling.
        assert_eq!(strings["requests.cpu"], "2");
    }

    // -----------------------------------------------------------------
    // Resource-name classification
    // -----------------------------------------------------------------

    #[test]
    fn test_resource_name_classification() {
        assert!(is_extended_resource_name("example.com/dongle"));
        assert!(is_extended_resource_name("nvidia.com/gpu"));
        // Native: unqualified, or under kubernetes.io/.
        assert!(!is_extended_resource_name("cpu"));
        assert!(!is_extended_resource_name("memory"));
        assert!(!is_extended_resource_name("ephemeral-storage"));
        assert!(!is_extended_resource_name("hugepages-2Mi"));
        assert!(!is_extended_resource_name("kubernetes.io/foo"));
        // A quota key is not itself a resource name.
        assert!(!is_extended_resource_name("requests.example.com/dongle"));
        assert!(is_hugepage_resource_name("hugepages-1Gi"));
        assert!(!is_hugepage_resource_name("hugepages"));
    }

    // -----------------------------------------------------------------
    // Pod usage — rows ported from upstream
    // staging/src/k8s.io/component-helpers/resource/helpers_test.go
    // (TestPodResourceRequests) and pkg/quota/v1/evaluator/core/pods_test.go
    // -----------------------------------------------------------------

    /// "nil options, larger init container" (`helpers_test.go:299-320`): the
    /// init container's `4` wins over the app container's `1`. Summing only
    /// `spec.containers` — what the quota paths used to do — charges `1`.
    #[test]
    fn test_pod_requests_larger_init_container_wins() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![container("i1", &[("cpu", "4")], &[])],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "4");
    }

    /// "nil options, larger containers" (`helpers_test.go:322-350`).
    #[test]
    fn test_pod_requests_larger_containers_win() {
        let p = pod(
            vec![
                container("c1", &[("cpu", "2")], &[]),
                container("c2", &[("cpu", "3")], &[]),
            ],
            vec![container("i1", &[("cpu", "2")], &[])],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "5");
    }

    /// "restartable init container" (`helpers_test.go:691-717`): a sidecar's
    /// request is held for the pod's whole life, so it adds rather than maxes.
    #[test]
    fn test_pod_requests_restartable_init_adds() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![sidecar("ri1", &[("cpu", "1")])],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "2");
    }

    /// "multiple restartable init containers" (`helpers_test.go:718-770`):
    /// max(5, (3+2+1) + 1) = 7.
    #[test]
    fn test_pod_requests_multiple_restartable_init() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![
                container("i1", &[("cpu", "5")], &[]),
                sidecar("ri1", &[("cpu", "1")]),
                sidecar("ri2", &[("cpu", "2")]),
                sidecar("ri3", &[("cpu", "3")]),
            ],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "7");
    }

    /// "multiple restartable and regular init containers"
    /// (`helpers_test.go:771-830`): init-2 needs 5 plus the two sidecars
    /// already running (1+2) = 8; the sidecar starting after it does not count.
    #[test]
    fn test_pod_requests_restartable_then_regular_init() {
        let p = pod(
            vec![container("c1", &[("cpu", "1")], &[])],
            vec![
                container("i1", &[("cpu", "5")], &[]),
                sidecar("ri1", &[("cpu", "1")]),
                sidecar("ri2", &[("cpu", "2")]),
                container("i2", &[("cpu", "5")], &[]),
                sidecar("ri3", &[("cpu", "3")]),
            ],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "8");
    }

    /// "restartable-init, init and regular" (`helpers_test.go:831-869`):
    /// init-1's 200 plus the sidecar's 10 = 210.
    #[test]
    fn test_pod_requests_restartable_init_and_regular() {
        let p = pod(
            vec![container("c1", &[("cpu", "100")], &[])],
            vec![
                sidecar("ri1", &[("cpu", "10")]),
                container("i1", &[("cpu", "200")], &[]),
            ],
        );
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "210");
    }

    /// "pod overhead included" (`helpers_test.go:386-...`): overhead is added
    /// to requests unconditionally.
    #[test]
    fn test_pod_requests_includes_overhead() {
        let mut p = pod(vec![container("c1", &[("cpu", "5")], &[])], vec![]);
        p.spec.as_mut().unwrap().overhead = Some(raw(&[("cpu", "1"), ("memory", "1Gi")]));
        let reqs = pod_requests(&p);
        assert_eq!(reqs["cpu"].canonical_string(), "6");
        assert_eq!(reqs["memory"].canonical_string(), "1Gi");
    }

    /// `PodLimits` adds overhead only to limits that are already set and
    /// non-zero (`helpers.go:366-373`).
    #[test]
    fn test_pod_limits_overhead_only_touches_present_limits() {
        let mut p = pod(vec![container("c1", &[], &[("cpu", "2")])], vec![]);
        p.spec.as_mut().unwrap().overhead = Some(raw(&[("cpu", "1"), ("memory", "1Gi")]));
        let limits = pod_limits(&p);
        assert_eq!(limits["cpu"].canonical_string(), "3");
        assert!(
            !limits.contains_key("memory"),
            "overhead must not create a memory limit the pod never set"
        );
    }

    /// Pod-level `spec.resources` overrides the container aggregate for the
    /// names it sets (`helpers.go:168-176`), and only for supported names.
    #[test]
    fn test_pod_requests_pod_level_resources_override() {
        let mut p = pod(vec![container("c1", &[("cpu", "1")], &[])], vec![]);
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "4"), ("example.com/dongle", "2")])),
            limits: None,
            claims: None,
        });
        let reqs = pod_requests(&p);
        assert_eq!(reqs["cpu"].canonical_string(), "4");
        assert!(
            !reqs.contains_key("example.com/dongle"),
            "only cpu/memory/hugepages are supported pod-level resources"
        );
    }

    /// Port of `pods_test.go` "init container hugepages" (`:245-260`): a
    /// hugepages request lands under both the bare name and `requests.`.
    #[test]
    fn test_pod_compute_usage_hugepages() {
        let p = pod(
            vec![],
            vec![container("i1", &[("hugepages-2Mi", "100Mi")], &[])],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["hugepages-2Mi"].canonical_string(), "100Mi");
        assert_eq!(usage["requests.hugepages-2Mi"].canonical_string(), "100Mi");
    }

    /// Port of `pods_test.go` "init container extended resources" (`:262-...`):
    /// an extended resource is charged **only** under `requests.<name>`.
    #[test]
    fn test_pod_compute_usage_extended_resource_requests_key_only() {
        let p = pod(
            vec![container(
                "c1",
                &[("example.com/dongle", "3")],
                &[("example.com/dongle", "3")],
            )],
            vec![],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["requests.example.com/dongle"].canonical_string(), "3");
        assert!(!usage.contains_key("example.com/dongle"));
        assert!(!usage.contains_key("limits.example.com/dongle"));
    }

    /// An extended-resource quantity with a suffix. `qty.parse::<i64>()` — the
    /// call this module replaces — returned `None` here, so a `2k`-dongle pod
    /// counted as asking for nothing.
    #[test]
    fn test_pod_compute_usage_extended_resource_with_suffix() {
        let p = pod(
            vec![container("c1", &[("example.com/dongle", "2k")], &[])],
            vec![],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(
            usage["requests.example.com/dongle"].value(),
            2000,
            "a suffixed extended-resource quantity must not read as zero"
        );
    }

    /// `podComputeUsageHelper` charges cpu/memory/ephemeral-storage requests
    /// under both the bare name and `requests.`, and limits under `limits.`
    /// (`pods.go:294-320`).
    #[test]
    fn test_pod_compute_usage_key_layout() {
        let p = pod(
            vec![container(
                "c1",
                &[
                    ("cpu", "500m"),
                    ("memory", "1Gi"),
                    ("ephemeral-storage", "32Mi"),
                ],
                &[
                    ("cpu", "1"),
                    ("memory", "2Gi"),
                    ("ephemeral-storage", "64Mi"),
                ],
            )],
            vec![],
        );
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["pods"].canonical_string(), "1");
        for (key, expected) in [
            ("cpu", "500m"),
            ("requests.cpu", "500m"),
            ("limits.cpu", "1"),
            ("memory", "1Gi"),
            ("requests.memory", "1Gi"),
            ("limits.memory", "2Gi"),
            ("ephemeral-storage", "32Mi"),
            ("requests.ephemeral-storage", "32Mi"),
            ("limits.ephemeral-storage", "64Mi"),
        ] {
            assert_eq!(usage[key].canonical_string(), expected, "key {key}");
        }
    }

    /// A fractional request must survive to the quota key. `0.5Gi` read as 0
    /// through every hand-rolled parser this replaces.
    #[test]
    fn test_pod_compute_usage_fractional_memory() {
        let p = pod(vec![container("c1", &[("memory", "0.5Gi")], &[])], vec![]);
        let usage = pod_compute_usage(&pod_requests(&p), &pod_limits(&p));
        assert_eq!(usage["requests.memory"].value(), 536_870_912);
    }

    // -----------------------------------------------------------------
    // QuotaV1Pod
    // -----------------------------------------------------------------

    /// Port of upstream `QuotaV1Pod` (`pods.go:491-508`).
    #[test]
    fn test_is_quota_charged() {
        use crate::resources::PodStatus;
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:10:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let mut p = pod(vec![container("c1", &[("cpu", "1")], &[])], vec![]);
        assert!(is_quota_charged(&p, now), "running pod is charged");

        p.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        assert!(!is_quota_charged(&p, now), "terminal pod is not charged");

        p.status = Some(PodStatus {
            phase: Some(Phase::Failed),
            ..Default::default()
        });
        assert!(!is_quota_charged(&p, now));

        // Terminating but inside the grace period: still charged. Dropping
        // every pod with a deletionTimestamp under-counts usage here.
        p.status = None;
        p.metadata.deletion_timestamp = Some(
            DateTime::parse_from_rfc3339("2026-01-01T00:09:30Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        p.metadata.deletion_grace_period_seconds = Some(60);
        assert!(is_quota_charged(&p, now), "grace period has not elapsed");

        // Past the grace period: stop charging, so a pod wedged Terminating on
        // a lost node cannot block a scale-up (`pods.go:496-498`).
        p.metadata.deletion_grace_period_seconds = Some(5);
        assert!(!is_quota_charged(&p, now));

        // A deletionTimestamp with no grace period never stops the charge.
        p.metadata.deletion_grace_period_seconds = None;
        assert!(is_quota_charged(&p, now));
    }

    /// `pod_usage` always charges `count/pods`, even for a terminal pod —
    /// object-count quota tracks everything in storage (`pods.go:390-397`).
    #[test]
    fn test_pod_usage_count_pods_survives_terminal_phase() {
        use crate::resources::PodStatus;
        let now = Utc::now();
        let mut p = pod(vec![container("c1", &[("cpu", "1")], &[])], vec![]);
        let live = pod_usage(&p, now);
        assert_eq!(live["count/pods"].canonical_string(), "1");
        assert_eq!(live["requests.cpu"].canonical_string(), "1");

        p.status = Some(PodStatus {
            phase: Some(Phase::Succeeded),
            ..Default::default()
        });
        let terminal = pod_usage(&p, now);
        assert_eq!(terminal["count/pods"].canonical_string(), "1");
        assert!(!terminal.contains_key("requests.cpu"));
        assert!(!terminal.contains_key("pods"));
    }

    // -----------------------------------------------------------------
    // podEvaluator.Constraints - ported from pods_test.go
    // `TestPodConstraintsFunc` (`:44-170`)
    // -----------------------------------------------------------------

    fn names(n: &[&str]) -> Vec<String> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_pod_constraints_init_container_resource_missing() {
        let p = pod(
            vec![],
            vec![container("dummy", &[("cpu", "1m")], &[("cpu", "2m")])],
        );
        assert_eq!(
            pod_constraints(&p, &names(&["memory"])),
            Err("must specify memory for: dummy".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_multiple_init_containers_sorted() {
        let p = pod(
            vec![],
            vec![
                container("foo", &[("cpu", "1m")], &[("cpu", "2m")]),
                container("bar", &[("cpu", "1m")], &[("cpu", "2m")]),
            ],
        );
        assert_eq!(
            pod_constraints(&p, &names(&["memory"])),
            Err("must specify memory for: bar,foo".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_container_resource_missing() {
        let p = pod(
            vec![container("dummy", &[("cpu", "1m")], &[("cpu", "2m")])],
            vec![],
        );
        assert_eq!(
            pod_constraints(&p, &names(&["memory"])),
            Err("must specify memory for: dummy".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_requests_cpu_omitted() {
        // The issue's case: `hard: requests.cpu` and a container with no request.
        let p = pod(vec![container("c", &[], &[])], vec![]);
        assert_eq!(
            pod_constraints(&p, &names(&["requests.cpu"])),
            Err("must specify requests.cpu for: c".to_string())
        );
    }

    #[test]
    fn test_pod_constraints_satisfied_by_request_or_limit() {
        let p = pod(
            vec![container(
                "c",
                &[("cpu", "1"), ("memory", "1Gi")],
                &[("cpu", "1"), ("memory", "1Gi")],
            )],
            vec![],
        );
        assert_eq!(
            pod_constraints(
                &p,
                &names(&[
                    "cpu",
                    "memory",
                    "requests.cpu",
                    "requests.memory",
                    "limits.cpu",
                    "limits.memory"
                ])
            ),
            Ok(())
        );
    }

    #[test]
    fn test_pod_constraints_multiple_resources_joined_with_semicolon() {
        let p = pod(vec![container("c", &[], &[])], vec![]);
        assert_eq!(
            pod_constraints(&p, &names(&["memory", "limits.cpu"])),
            Err("must specify limits.cpu for: c; memory for: c".to_string())
        );
    }

    /// `validationSet` is only cpu/memory: pods, object counts,
    /// ephemeral-storage and extended resources never impose a requirement
    /// (`pods.go:97-110`).
    #[test]
    fn test_pod_constraints_ignores_non_validation_resources() {
        let p = pod(vec![container("c", &[], &[])], vec![]);
        assert_eq!(
            pod_constraints(
                &p,
                &names(&[
                    "pods",
                    "count/pods",
                    "requests.ephemeral-storage",
                    "requests.example.com/foo",
                    "hugepages-2Mi"
                ])
            ),
            Ok(())
        );
    }

    /// `IsPodLevelResourcesSet` skips the check (`pods.go:131-133`).
    #[test]
    fn test_pod_constraints_skipped_with_pod_level_resources() {
        let mut p = pod(vec![container("c", &[], &[])], vec![]);
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "1")])),
            limits: None,
            claims: None,
        });
        assert_eq!(pod_constraints(&p, &names(&["requests.cpu"])), Ok(()));
    }

    /// With `PodLevelResources` off, `SkipPodLevelResources` is set
    /// (`pods.go:403-404`) so `PodRequests` ignores `spec.resources`
    /// (`helpers.go:155`).
    #[test]
    #[serial_test::serial]
    fn test_pod_requests_limits_skip_pod_level_when_gate_off() {
        use crate::feature_gates::{with_feature, Feature};
        let mut p = pod(
            vec![container("c1", &[("cpu", "1")], &[("cpu", "2")])],
            vec![],
        );
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "4")])),
            limits: Some(raw(&[("cpu", "8")])),
            claims: None,
        });
        let _g = with_feature(Feature::PodLevelResources, false);
        assert_eq!(pod_requests(&p)["cpu"].canonical_string(), "1");
        assert_eq!(pod_limits(&p)["cpu"].canonical_string(), "2");
    }

    /// `pods.go:143`: the constraints check is only skipped when the gate is on.
    #[test]
    #[serial_test::serial]
    fn test_pod_constraints_not_skipped_when_gate_off() {
        use crate::feature_gates::{with_feature, Feature};
        let mut p = pod(vec![container("c", &[], &[])], vec![]);
        p.spec.as_mut().unwrap().resources = Some(ResourceRequirements {
            requests: Some(raw(&[("cpu", "1")])),
            limits: None,
            claims: None,
        });
        let _g = with_feature(Feature::PodLevelResources, false);
        assert!(pod_constraints(&p, &names(&["requests.cpu"])).is_err());
    }

    fn pvc(
        spec: serde_json::Value,
        status: serde_json::Value,
        ann: serde_json::Value,
    ) -> crate::resources::PersistentVolumeClaim {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "c", "namespace": "ns", "annotations": ann},
            "spec": spec, "status": status,
        }))
        .unwrap()
    }

    fn s(list: &ResourceList, k: &str) -> String {
        list[k].canonical_string()
    }

    /// `pvcEvaluator.Usage`: claim count, request, and the per-class keys.
    #[test]
    fn pvc_usage_charges_claim_storage_and_class() {
        let c = pvc(
            serde_json::json!({"storageClassName": "gold", "resources": {"requests": {"storage": "10Gi"}}}),
            serde_json::json!({"phase": "Pending"}),
            serde_json::json!({}),
        );
        let u = pvc_usage(&c);
        assert_eq!(s(&u, "persistentvolumeclaims"), "1");
        assert_eq!(s(&u, "count/persistentvolumeclaims"), "1");
        assert_eq!(s(&u, "requests.storage"), "10Gi");
        assert_eq!(
            s(
                &u,
                "gold.storageclass.storage.k8s.io/persistentvolumeclaims"
            ),
            "1"
        );
        assert_eq!(
            s(&u, "gold.storageclass.storage.k8s.io/requests.storage"),
            "10Gi"
        );
    }

    /// The beta annotation wins over `spec.storageClassName`
    /// (`GetPersistentVolumeClaimClass`), and a claim with no request
    /// charges no `requests.storage`.
    #[test]
    fn pvc_usage_beta_annotation_and_no_request() {
        let c = pvc(
            serde_json::json!({"storageClassName": "gold"}),
            serde_json::json!({}),
            serde_json::json!({"volume.beta.kubernetes.io/storage-class": "bronze"}),
        );
        let u = pvc_usage(&c);
        assert!(u.contains_key("bronze.storageclass.storage.k8s.io/persistentvolumeclaims"));
        assert!(!u.contains_key("requests.storage"));
        assert!(!u.contains_key("gold.storageclass.storage.k8s.io/persistentvolumeclaims"));
    }

    /// `getStorageUsage`: rounded up to whole bytes (issue 94313), and a
    /// larger `status.allocatedResources` wins.
    #[test]
    fn pvc_usage_rounds_up_and_prefers_larger_allocated() {
        let c = pvc(
            serde_json::json!({"resources": {"requests": {"storage": "1500m"}}}),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        assert_eq!(s(&pvc_usage(&c), "requests.storage"), "2");
        let c = pvc(
            serde_json::json!({"resources": {"requests": {"storage": "1Gi"}}}),
            serde_json::json!({"allocatedResources": {"storage": "2Gi"}}),
            serde_json::json!({}),
        );
        assert_eq!(s(&pvc_usage(&c), "requests.storage"), "2Gi");
        let c = pvc(
            serde_json::json!({"resources": {"requests": {"storage": "3Gi"}}}),
            serde_json::json!({"allocatedResources": {"storage": "2Gi"}}),
            serde_json::json!({}),
        );
        assert_eq!(s(&pvc_usage(&c), "requests.storage"), "3Gi");
    }
}
