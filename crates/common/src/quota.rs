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

use crate::label_selector::{LabelOperator, LabelRequirement};
use crate::quantity::{Format, Quantity};
use crate::resources::{
    ContainerStatus, PersistentVolumeClaim, Pod, PodAffinityTerm, ResourceQuotaSpec,
    ScopedResourceSelectorRequirement,
};
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

/// `helper.IsHugePageResourceValueDivisible`
/// (`pkg/apis/core/helper/helpers.go:43-54`): the quantity is a whole multiple
/// of the page size parsed from the `hugepages-<size>` name. An unparseable
/// size, or a non-positive / fractional one, is not divisible.
pub fn is_hugepage_resource_value_divisible(name: &str, quantity: &Quantity) -> bool {
    // `HugePageSizeFromResourceName` (helpers.go:72-78).
    let Some(size) = name.strip_prefix(HUGEPAGES_PREFIX) else {
        return false;
    };
    let Ok(page_size) = Quantity::parse(size) else {
        return false;
    };
    if page_size.is_negative() || page_size.is_zero() || page_size.milli_value() % 1000 != 0 {
        return false;
    }
    quantity.value() % page_size.value() == 0
}

/// True when the name contains `kubernetes.io/` (`strings.Contains`, not a
/// prefix check). Upstream `v1helper.IsPrefixedNativeResource`
/// (`pkg/apis/core/v1/helper/helpers.go:51-55`).
pub fn is_prefixed_native_resource(name: &str) -> bool {
    name.contains(KUBERNETES_IO_PREFIX)
}

/// True for a resource that is native to Kubernetes: either unqualified (no
/// `/`) or under the `kubernetes.io/` namespace. Upstream
/// `v1helper.IsNativeResource`.
pub fn is_native_resource_name(name: &str) -> bool {
    !name.contains('/') || is_prefixed_native_resource(name)
}

/// True for an extended resource: not native, not `requests.`-prefixed (to
/// avoid confusion with the quota convention), and `requests.<name>` satisfies
/// `IsQualifiedName`. Upstream `v1helper.IsExtendedResourceName`
/// (`pkg/apis/core/v1/helper/helpers.go:34-49`).
pub fn is_extended_resource_name(name: &str) -> bool {
    if is_native_resource_name(name) || name.starts_with(REQUESTS_PREFIX) {
        return false;
    }
    let name_for_quota = format!("{REQUESTS_PREFIX}{name}");
    crate::validation::metav1::is_qualified_name(&name_for_quota).is_empty()
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
/// This is the `UseStatusResources: false` form; quota's [`pod_usage`] uses
/// [`aggregate_container_resources_opts`] with it on.
pub fn aggregate_container_resources(pod: &Pod, requests: bool) -> ResourceList {
    aggregate_container_resources_non_missing(pod, requests, None)
}

/// Port of upstream `applyNonMissing` (`component-helpers/resource/helpers.go:322-336`):
/// every resource named in `non_missing` that the container does not set at
/// all (an explicit zero counts as set) is added to the container's list.
fn apply_non_missing(own: ResourceList, non_missing: Option<&ResourceList>) -> ResourceList {
    let Some(non_missing) = non_missing.filter(|n| !n.is_empty()) else {
        return own;
    };
    let mut cp = own.clone();
    for (name, value) in non_missing {
        if !own.contains_key(name) {
            cp.insert(name.clone(), *value);
        }
    }
    cp
}

/// [`aggregate_container_resources`] with upstream's
/// `PodResourcesOptions.NonMissingContainerRequests`
/// (`helpers.go:218-220`, `:253-255`): applied to each app container and each
/// init container before it is folded into the pod total.
pub fn aggregate_container_resources_non_missing(
    pod: &Pod,
    requests: bool,
    non_missing: Option<&ResourceList>,
) -> ResourceList {
    aggregate_container_resources_opts(pod, requests, non_missing, false)
}

/// `IsPodResizeInfeasible` (`helpers.go:302-309`): the first
/// `PodResizePending` condition decides, by its reason.
fn is_pod_resize_infeasible(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.condition_type == "PodResizePending"))
        .is_some_and(|c| c.reason.as_deref() == Some("Infeasible"))
}

/// `determineEffectiveRequests` / `determineEffectiveLimits`
/// (`helpers.go:285-299`): what a container is charged while an in-place
/// resize may be in flight. Requests are the per-resource max of spec,
/// actuated and allocated (actuated and allocated only when the resize is
/// infeasible); limits the max of spec and actuated (actuated only when
/// infeasible). Applied only when the status carries `resources`
/// (`found && cs.Resources != nil`).
fn effective_with_status(
    pod: &Pod,
    own: ResourceList,
    status: Option<&ContainerStatus>,
    requests: bool,
) -> ResourceList {
    let Some(cs) = status.filter(|cs| cs.resources.is_some()) else {
        return own;
    };
    let actuated = side(cs.resources.as_ref(), requests);
    let allocated = if requests {
        cs.allocated_resources
            .as_ref()
            .map(parse_resource_list)
            .unwrap_or_default()
    } else {
        ResourceList::new()
    };
    let infeasible = is_pod_resize_infeasible(pod);
    match (requests, infeasible) {
        (true, true) => max(&actuated, &allocated),
        (true, false) => max(&max(&own, &actuated), &allocated),
        (false, true) => actuated,
        (false, false) => max(&own, &actuated),
    }
}

/// [`aggregate_container_resources_non_missing`] with upstream's
/// `UseStatusResources` option (`AggregateContainerRequests`
/// `helpers.go:191-278`, `AggregateContainerLimits` `:382-455`): app
/// containers, and restartable init containers only, are priced from their
/// `status.containerStatuses` / `initContainerStatuses` entry (matched by
/// name) via [`effective_with_status`].
pub fn aggregate_container_resources_opts(
    pod: &Pod,
    requests: bool,
    non_missing: Option<&ResourceList>,
    use_status: bool,
) -> ResourceList {
    let Some(spec) = &pod.spec else {
        return ResourceList::new();
    };

    // Init statuses are inserted after app statuses, so they win a name clash.
    let mut statuses: HashMap<&str, &ContainerStatus> = HashMap::new();
    if use_status {
        if let Some(status) = &pod.status {
            for cs in status
                .container_statuses
                .iter()
                .flatten()
                .chain(status.init_container_statuses.iter().flatten())
            {
                statuses.insert(cs.name.as_str(), cs);
            }
        }
    }

    let mut total = ResourceList::new();
    for container in &spec.containers {
        let mut own = side(container.resources.as_ref(), requests);
        if use_status {
            own = effective_with_status(
                pod,
                own,
                statuses.get(container.name.as_str()).copied(),
                requests,
            );
        }
        let own = apply_non_missing(own, non_missing);
        total = add(&total, &own);
    }

    let mut restartable = ResourceList::new();
    let mut init_max = ResourceList::new();
    for container in spec.init_containers.iter().flatten() {
        let mut own = side(container.resources.as_ref(), requests);
        if use_status && is_restartable_init(container.restart_policy.as_ref()) {
            own = effective_with_status(
                pod,
                own,
                statuses.get(container.name.as_str()).copied(),
                requests,
            );
        }
        let own = apply_non_missing(own, non_missing);
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
    pod_requests_opts(pod, None, false)
}

/// [`pod_requests`] with upstream's `NonMissingContainerRequests` option
/// (`helpers.go:56`), used by the scheduler for `Non0CPU`/`Non0Mem`
/// (`pkg/scheduler/framework/types.go:722-745`).
pub fn pod_requests_non_missing(pod: &Pod, non_missing: Option<&ResourceList>) -> ResourceList {
    pod_requests_opts(pod, non_missing, false)
}

/// [`pod_requests`] with upstream's `UseStatusResources` option.
pub fn pod_requests_opts(
    pod: &Pod,
    non_missing: Option<&ResourceList>,
    use_status: bool,
) -> ResourceList {
    let mut reqs = aggregate_container_resources_opts(pod, true, non_missing, use_status);
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
    pod_limits_opts(pod, false)
}

/// [`pod_limits`] with upstream's `UseStatusResources` option.
pub fn pod_limits_opts(pod: &Pod, use_status: bool) -> ResourceList {
    let mut limits = aggregate_container_resources_opts(pod, false, None, use_status);
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
    // `UseStatusResources: Enabled(InPlacePodVerticalScaling)`
    // (`pods.go:400-402`).
    let use_status =
        crate::feature_gates::enabled(crate::feature_gates::Feature::InPlacePodVerticalScaling);
    add(
        &usage,
        &pod_compute_usage(
            &pod_requests_opts(pod, None, use_status),
            &pod_limits_opts(pod, use_status),
        ),
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

/// `IsTerminating` (`pkg/quota/v1/evaluator/core/pods.go:417-422`): a
/// non-negative `activeDeadlineSeconds`. A `deletionTimestamp` does not make a
/// pod Terminating for quota.
pub fn is_terminating(pod: &Pod) -> bool {
    pod.spec
        .as_ref()
        .and_then(|s| s.active_deadline_seconds)
        .is_some_and(|d| d >= 0)
}

/// `isBestEffort` (`pods.go:412-414`): `qos.GetPodQOS(pod) == PodQOSBestEffort`.
pub fn is_best_effort(pod: &Pod) -> bool {
    crate::qos::get_pod_qos(pod) == crate::qos::QoSClass::BestEffort
}

/// `crossNamespacePodAffinityTerm` (`pods.go:453-455`).
fn cross_namespace_term(term: &PodAffinityTerm) -> bool {
    term.namespaces.as_ref().is_some_and(|n| !n.is_empty()) || term.namespace_selector.is_some()
}

/// `usesCrossNamespacePodAffinity` (`pods.go:473-503`): any required or
/// preferred (anti-)affinity term naming namespaces or a namespace selector.
pub fn uses_cross_namespace_pod_affinity(pod: &Pod) -> bool {
    let Some(affinity) = pod.spec.as_ref().and_then(|s| s.affinity.as_ref()) else {
        return false;
    };
    let any = |required: &Option<Vec<PodAffinityTerm>>,
               preferred: &Option<Vec<crate::resources::WeightedPodAffinityTerm>>| {
        required.iter().flatten().any(cross_namespace_term)
            || preferred
                .iter()
                .flatten()
                .any(|t| cross_namespace_term(&t.pod_affinity_term))
    };
    affinity.pod_affinity.as_ref().is_some_and(|a| {
        any(
            &a.required_during_scheduling_ignored_during_execution,
            &a.preferred_during_scheduling_ignored_during_execution,
        )
    }) || affinity.pod_anti_affinity.as_ref().is_some_and(|a| {
        any(
            &a.required_during_scheduling_ignored_during_execution,
            &a.preferred_during_scheduling_ignored_during_execution,
        )
    })
}

/// `podMatchesSelector` (`pods.go:424-438`): the `PriorityClass` scope as a
/// label selector over `{PriorityClass: <name>}` (empty when the pod has no
/// class), via `ScopedResourceSelectorRequirementsAsSelector`
/// (`pkg/apis/core/v1/helper/helpers.go`) and `labels.NewRequirement`, whose
/// value-count rules are the errors below.
fn priority_class_matches(
    pod: &Pod,
    selector: &ScopedResourceSelectorRequirement,
) -> Result<bool, String> {
    let class = pod
        .spec
        .as_ref()
        .and_then(|s| s.priority_class_name.as_deref())
        .filter(|c| !c.is_empty());
    let values = selector.values.as_deref().unwrap_or(&[]);
    match selector.operator.as_str() {
        "In" | "NotIn" if values.is_empty() => Err(
            "failed to parse and convert selector: for 'in', 'notin' operators, values set can't be empty"
                .to_string(),
        ),
        "In" => Ok(class.is_some_and(|c| values.iter().any(|v| v == c))),
        "NotIn" => Ok(class.is_none_or(|c| !values.iter().any(|v| v == c))),
        "Exists" | "DoesNotExist" if !values.is_empty() => Err(
            "failed to parse and convert selector: values set must be empty for exists and does not exist"
                .to_string(),
        ),
        "Exists" => Ok(class.is_some()),
        "DoesNotExist" => Ok(class.is_none()),
        op => Err(format!(
            "failed to parse and convert selector: {op:?} is not a valid scope selector operator"
        )),
    }
}

/// `podMatchesScopeFunc` (`pods.go:331-356`). An unknown scope matches
/// nothing.
pub fn pod_matches_scope(
    selector: &ScopedResourceSelectorRequirement,
    pod: &Pod,
) -> Result<bool, String> {
    Ok(match selector.scope_name.as_str() {
        "Terminating" => is_terminating(pod),
        "NotTerminating" => !is_terminating(pod),
        "BestEffort" => is_best_effort(pod),
        "NotBestEffort" => !is_best_effort(pod),
        "PriorityClass" => {
            if selector.operator == "Exists" {
                // Existence of a class, without selector parsing (:343-347).
                pod.spec
                    .as_ref()
                    .and_then(|s| s.priority_class_name.as_deref())
                    .is_some_and(|c| !c.is_empty())
            } else {
                priority_class_matches(pod, selector)?
            }
        }
        "CrossNamespacePodAffinity" => uses_cross_namespace_pod_affinity(pod),
        _ => false,
    })
}

/// Whether `pod` matches every scope of a quota: the scope half of
/// `generic.Matches` (evaluator.go:190-209) and of `CalculateUsageStats`.
/// A selector that fails to evaluate matches nothing.
pub fn pod_matches_quota_scopes(pod: &Pod, spec: &ResourceQuotaSpec) -> bool {
    scope_selectors_from_quota(spec)
        .iter()
        .all(|s| pod_matches_scope(s, pod).unwrap_or(false))
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

/// `getScopeSelectorsFromQuota` (`staging/src/k8s.io/apiserver/pkg/quota/v1/
/// generic/evaluator.go:171-181`): each `spec.scopes` entry as an `Exists`
/// selector, then `spec.scopeSelector.matchExpressions`.
pub fn scope_selectors_from_quota(
    spec: &ResourceQuotaSpec,
) -> Vec<ScopedResourceSelectorRequirement> {
    let mut selectors: Vec<ScopedResourceSelectorRequirement> = spec
        .scopes
        .iter()
        .flatten()
        .map(|scope| ScopedResourceSelectorRequirement {
            scope_name: scope.clone(),
            operator: "Exists".to_string(),
            values: None,
        })
        .collect();
    if let Some(selector) = &spec.scope_selector {
        selectors.extend(selector.match_expressions.iter().cloned());
    }
    selectors
}

/// `ScopedResourceSelectorRequirementsAsSelector`
/// (`pkg/apis/core/v1/helper/helpers.go:320-341`) matched against `labels`.
pub fn scoped_selector_matches_labels(
    selector: &ScopedResourceSelectorRequirement,
    labels: &HashMap<String, String>,
) -> Result<bool, String> {
    let op = match selector.operator.as_str() {
        "In" => LabelOperator::In,
        "NotIn" => LabelOperator::NotIn,
        "Exists" => LabelOperator::Exists,
        "DoesNotExist" => LabelOperator::DoesNotExist,
        other => return Err(format!("{other:?} is not a valid scope selector operator")),
    };
    let requirement = LabelRequirement::new(
        &selector.scope_name,
        op,
        selector.values.clone().unwrap_or_default(),
    )?;
    Ok(requirement.matches(labels))
}

/// `getReferencedVolumeAttributesClassNames`
/// (`persistent_volume_claims.go:330-343`): the spec's class, the current
/// class and the modify-in-flight target class, when set.
pub fn pvc_referenced_volume_attributes_class_names(
    pvc: &PersistentVolumeClaim,
) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut insert = |name: Option<&String>| {
        if let Some(n) = name.filter(|n| !n.is_empty()) {
            names.insert(n.clone());
        }
    };
    insert(pvc.spec.volume_attributes_class_name.as_ref());
    if let Some(status) = &pvc.status {
        insert(status.current_volume_attributes_class_name.as_ref());
        insert(
            status
                .modify_volume_status
                .as_ref()
                .and_then(|m| m.target_volume_attributes_class_name.as_ref()),
        );
    }
    names
}

/// `pvcMatchesScopeFunc` (`persistent_volume_claims.go:293-309`) with
/// `pvcMatchesSelector` (`:311-328`). Only the VolumeAttributesClass scope
/// matches a claim; `VolumeAttributesClass` is GA and on by default in 1.35.
pub fn pvc_matches_scope(
    selector: &ScopedResourceSelectorRequirement,
    pvc: &PersistentVolumeClaim,
) -> Result<bool, String> {
    if selector.scope_name != "VolumeAttributesClass" {
        return Ok(false);
    }
    let names = pvc_referenced_volume_attributes_class_names(pvc);
    if selector.operator == "Exists" {
        return Ok(!names.is_empty());
    }
    if names.is_empty() {
        return scoped_selector_matches_labels(selector, &HashMap::new());
    }
    for name in names {
        let labels = HashMap::from([(selector.scope_name.clone(), name)]);
        if scoped_selector_matches_labels(selector, &labels)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `pvcEvaluator.MatchingScopes` (`persistent_volume_claims.go:122-139`): the
/// selectors the claim matches.
pub fn pvc_matching_scopes(
    pvc: &PersistentVolumeClaim,
    selectors: &[ScopedResourceSelectorRequirement],
) -> Result<Vec<ScopedResourceSelectorRequirement>, String> {
    let mut matched = Vec::new();
    for selector in selectors {
        let m = pvc_matches_scope(selector, pvc)
            .map_err(|e| format!("error on matching scope {selector:?}: {e}"))?;
        if m {
            matched.push(selector.clone());
        }
    }
    Ok(matched)
}

/// `RequiresQuotaReplenish` (`persistent_volume_claims.go:276-290`), with
/// `RecoverVolumeExpansionFailure` and `VolumeAttributesClass` both on.
pub fn pvc_requires_quota_replenish(
    pvc: &PersistentVolumeClaim,
    old: &PersistentVolumeClaim,
) -> bool {
    let allocated_storage = |c: &PersistentVolumeClaim| {
        c.status
            .as_ref()
            .and_then(|s| s.allocated_resources.as_ref())
            .and_then(|r| r.get("storage"))
            .and_then(|v| Quantity::parse(v.trim()).ok())
    };
    // `Storage()` returns a zero quantity when absent; compare as quantities.
    let zero = Quantity::from_value(0, Format::BinarySI);
    let a = allocated_storage(old).unwrap_or(zero);
    let b = allocated_storage(pvc).unwrap_or(zero);
    if a.cmp_value(&b) != std::cmp::Ordering::Equal {
        return true;
    }
    pvc_referenced_volume_attributes_class_names(old)
        != pvc_referenced_volume_attributes_class_names(pvc)
}
