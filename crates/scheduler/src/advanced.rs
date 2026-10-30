use rusternetes_common::quantity::parse_resource_value;
use rusternetes_common::resources::{
    IntOrString, Node, Pod, PodDisruptionBudget, PriorityClass, Taint, Toleration,
    TopologySpreadConstraint,
};
use std::collections::HashMap;
use tracing::debug;

// Affinity predicates relocated to `rusternetes_common::affinity`. Re-exported
// here so existing callers (`crate::advanced::check_node_affinity`, etc. in
// scheduler.rs / plugins.rs) keep resolving unchanged, and so the helpers used
// by scheduler-only predicates below (`match_selector`, `matches_pod_affinity_term`)
// are in scope.
pub use rusternetes_common::affinity::{
    check_node_affinity, check_pod_anti_affinity, match_selector, matches_pod_affinity_term,
};

/// Check if pod tolerates all node taints
pub fn check_taints_tolerations(node: &Node, pod: &Pod) -> bool {
    let node_taints = match &node.spec {
        Some(spec) => match &spec.taints {
            Some(taints) => taints,
            None => return true, // No taints, pod can be scheduled
        },
        None => return true, // No spec, no taints
    };

    let pod_tolerations = match &pod.spec.as_ref().unwrap().tolerations {
        Some(tolerations) => tolerations,
        None => {
            // No tolerations, check if there are any NoSchedule or NoExecute taints
            return !node_taints
                .iter()
                .any(|t| t.effect == "NoSchedule" || t.effect == "NoExecute");
        }
    };

    // Check each taint to see if there's a matching toleration
    for taint in node_taints {
        if !taint_is_tolerated(taint, pod_tolerations) {
            debug!(
                "Pod {} does not tolerate taint {:?} on node {}",
                pod.metadata.name, taint, node.metadata.name
            );
            return false;
        }
    }

    true
}

/// Check if a specific taint is tolerated by any of the tolerations
fn taint_is_tolerated(taint: &Taint, tolerations: &[Toleration]) -> bool {
    // PreferNoSchedule is a soft constraint, always tolerated for hard scheduling
    if taint.effect == "PreferNoSchedule" {
        return true;
    }

    for toleration in tolerations {
        if toleration_matches_taint(toleration, taint) {
            return true;
        }
    }

    false
}

/// Check if a toleration matches a taint
pub fn toleration_matches_taint(toleration: &Toleration, taint: &Taint) -> bool {
    let operator = toleration.operator.as_deref().unwrap_or("Equal");

    // Check effect
    if let Some(ref effect) = toleration.effect {
        if effect != &taint.effect {
            return false;
        }
    }

    // Check operator
    match operator {
        "Exists" => {
            // If key is empty, tolerate all taints
            if toleration.key.is_none() {
                return true;
            }
            // Otherwise check key matches
            toleration.key.as_ref() == Some(&taint.key)
        }
        "Equal" => {
            // Both key and value must match
            toleration.key.as_ref() == Some(&taint.key)
                && toleration.value.as_ref() == taint.value.as_ref()
        }
        _ => false,
    }
}

/// Check pod affinity requirements
/// Returns (passes_hard_requirements, score)
pub fn check_pod_affinity(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    let affinity = match &pod.spec.as_ref().unwrap().affinity {
        Some(a) => a,
        None => return (true, 0), // No affinity requirements
    };

    let pod_affinity = match &affinity.pod_affinity {
        Some(pa) => pa,
        None => return (true, 0),
    };

    // Check required pod affinity (hard requirement)
    if let Some(ref required) = pod_affinity.required_during_scheduling_ignored_during_execution {
        for term in required {
            if !matches_pod_affinity_term(node, pod, term, all_pods, all_nodes, true) {
                debug!(
                    "Pod {} does not meet hard pod affinity requirement on node {}",
                    pod.metadata.name, node.metadata.name
                );
                return (false, 0);
            }
        }
    }

    // Calculate score from preferred pod affinity (soft requirement)
    let mut score = 0;
    if let Some(ref preferred) = pod_affinity.preferred_during_scheduling_ignored_during_execution {
        for weighted_term in preferred {
            if matches_pod_affinity_term(
                node,
                pod,
                &weighted_term.pod_affinity_term,
                all_pods,
                all_nodes,
                true,
            ) {
                score += weighted_term.weight;
            }
        }
    }

    (true, score)
}

/// Check if a pod's hostPort requirements conflict with pods already scheduled on the node.
/// Two pods conflict if they use the same hostPort AND the same protocol AND overlapping hostIPs.
/// A hostIP of "0.0.0.0", "::", or "" (empty/unset) means "all interfaces" and overlaps with
/// any other hostIP.
pub fn check_host_port_conflicts(node: &Node, pod: &Pod, all_pods: &[Pod]) -> bool {
    // Collect hostPorts requested by the incoming pod
    let incoming_ports = collect_host_ports(pod);
    if incoming_ports.is_empty() {
        return true; // No hostPort requirements, no conflict possible
    }

    let node_name = &node.metadata.name;

    // Collect hostPorts already in use on this node
    for existing_pod in all_pods {
        // Only consider pods scheduled on this node
        let on_this_node = existing_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .map(|n| n == node_name)
            .unwrap_or(false);
        if !on_this_node {
            continue;
        }

        // K8s tracks UsedPorts for ALL pods assigned to a node (including
        // Pending). Only skip terminal pods (Succeeded/Failed) and terminating.
        let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(
            phase,
            Some(rusternetes_common::types::Phase::Succeeded)
                | Some(rusternetes_common::types::Phase::Failed)
        ) {
            continue;
        }
        if existing_pod.metadata.deletion_timestamp.is_some() {
            continue;
        }

        let existing_ports = collect_host_ports(existing_pod);
        for (inc_port, inc_protocol, inc_ip) in &incoming_ports {
            for (ex_port, ex_protocol, ex_ip) in &existing_ports {
                if inc_port == ex_port
                    && inc_protocol == ex_protocol
                    && host_ips_overlap(inc_ip, ex_ip)
                {
                    debug!(
                        "HostPort conflict: port {} protocol {} hostIP {} vs {} on node {}",
                        inc_port, inc_protocol, inc_ip, ex_ip, node_name
                    );
                    return false;
                }
            }
        }
    }

    true
}

/// Collect all (hostPort, protocol, hostIP) tuples from a pod's containers.
fn collect_host_ports(pod: &Pod) -> Vec<(u16, String, String)> {
    let mut result = Vec::new();
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ports) = &container.ports {
                for port in ports {
                    // hostPort == 0 means "no host port" — it must NOT be tracked
                    // as a used port, otherwise two pods that both leave hostPort
                    // unset (0) would falsely conflict. The conformance netexec
                    // pods (Networking Granular Checks, HostPort) declare
                    // containerPort with hostPort: 0; treating 0 as a real port
                    // made the 2nd/3rd such pod unschedulable on every node.
                    if let Some(host_port) = port.host_port.filter(|&p| p != 0) {
                        let protocol = port.protocol.clone();
                        let host_ip = port.host_ip.clone().unwrap_or_default();
                        result.push((host_port, protocol, host_ip));
                    }
                }
            }
        }
        // Also check init containers
        if let Some(init_containers) = &spec.init_containers {
            for container in init_containers {
                if let Some(ports) = &container.ports {
                    for port in ports {
                        if let Some(host_port) = port.host_port.filter(|&p| p != 0) {
                            let protocol = port.protocol.clone();
                            let host_ip = port.host_ip.clone().unwrap_or_default();
                            result.push((host_port, protocol, host_ip));
                        }
                    }
                }
            }
        }
    }
    result
}

/// Check if two hostIP values overlap.
/// "0.0.0.0", "::", and "" all mean "all interfaces" and overlap with everything.
pub fn host_ips_overlap(ip1: &str, ip2: &str) -> bool {
    let wildcard = |ip: &str| ip.is_empty() || ip == "0.0.0.0" || ip == "::";
    if wildcard(ip1) || wildcard(ip2) {
        return true;
    }
    ip1 == ip2
}

/// Calculate resource-based node score
pub fn calculate_resource_score(node: &Node, pod: &Pod) -> i32 {
    calculate_resource_score_with_pods(node, pod, &[])
}

/// Calculate resource score accounting for pods already scheduled on the node.
/// Returns 0 if the node can't fit the pod, otherwise a score 1-100.
pub fn calculate_resource_score_with_pods(node: &Node, pod: &Pod, all_pods: &[Pod]) -> i32 {
    let allocatable = match &node.status {
        Some(status) => match &status.allocatable {
            Some(a) => a,
            None => return 50,
        },
        None => return 50,
    };

    // Get pod resource requests
    let pod_amounts = pod_request_amounts(pod);
    let cpu_request = pod_amounts.get("cpu").copied().unwrap_or(0);
    let memory_request = pod_amounts.get("memory").copied().unwrap_or(0);

    // Calculate total allocatable
    let total_cpu = allocatable
        .get("cpu")
        .map(|s| parse_resource_quantity(s, "cpu"))
        .unwrap_or(0);
    let total_memory = allocatable
        .get("memory")
        .map(|s| parse_resource_quantity(s, "memory"))
        .unwrap_or(0);

    // Subtract resources used by pods already scheduled on this node.
    // K8s checks ALL resources (cpu, memory, AND extended resources like fakecpu).
    let mut used_cpu = 0i64;
    let mut used_memory = 0i64;
    // Non-zero usage for the score only (upstream scores on
    // `GetNonZeroRequested`, `noderesources/resource_allocation.go:188`).
    let mut used_cpu_non0 = 0i64;
    let mut used_memory_non0 = 0i64;
    let mut used_extended: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let node_name = &node.metadata.name;

    // Candidate identity + priority, for nominated-pod space reservation below.
    let candidate_priority = pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
    let candidate_name = pod.metadata.name.as_str();
    let candidate_ns = pod.metadata.namespace.as_deref().unwrap_or("");

    for existing_pod in all_pods {
        let scheduled_on_this_node = existing_pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .map(|n| n == node_name)
            .unwrap_or(false);

        // Reserve space for higher-or-equal-priority pods NOMINATED to this node
        // by preemption but not yet bound. Without this, a lower-priority pod
        // schedules into the space the preemptor just freed, gets preempted
        // again, and the owning controller recreates it — an endless
        // preemption live-lock (#542 PreemptionExecutionPath). Mirrors upstream
        // `addNominatedPods`, which runs the fit predicate with nominated pods
        // of >= priority added to the node.
        let is_self = existing_pod.metadata.name == candidate_name
            && existing_pod.metadata.namespace.as_deref().unwrap_or("") == candidate_ns;
        let nominated_here = existing_pod
            .status
            .as_ref()
            .and_then(|s| s.nominated_node_name.as_ref())
            .map(|n| n == node_name)
            .unwrap_or(false);
        let existing_priority = existing_pod
            .spec
            .as_ref()
            .and_then(|s| s.priority)
            .unwrap_or(0);
        let reserve_for_nominee =
            !is_self && nominated_here && existing_priority >= candidate_priority;

        if !scheduled_on_this_node && !reserve_for_nominee {
            continue;
        }
        // K8s tracks UsedPorts for ALL pods assigned to a node (including
        // Pending). Only skip terminal pods (Succeeded/Failed) and terminating.
        let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_ref());
        if matches!(
            phase,
            Some(rusternetes_common::types::Phase::Succeeded)
                | Some(rusternetes_common::types::Phase::Failed)
        ) {
            continue;
        }
        if existing_pod.metadata.deletion_timestamp.is_some() {
            continue;
        }
        // Track extended resource usage. Quantities use the full k8s format
        // (e.g. "1k" == 1000), so they are parsed as Quantities, not raw i64
        // — a canonicalized value like "1k" would otherwise parse to 0 and
        // silently under-count usage.
        let (non0_cpu, non0_mem) = non_zero_request_amounts(existing_pod);
        used_cpu_non0 += non0_cpu;
        used_memory_non0 += non0_mem;
        for (key, amount) in pod_request_amounts(existing_pod) {
            match key.as_str() {
                "cpu" => used_cpu += amount,
                "memory" => used_memory += amount,
                "ephemeral-storage" => {}
                _ => *used_extended.entry(key).or_insert(0) += amount,
            }
        }
    }

    // Check extended resources requested by the pod against node allocatable.
    // If ANY extended resource is insufficient, return 0 (can't schedule).
    for (key, requested) in &pod_amounts {
        if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
            // Both the request and the node's advertised capacity use the
            // full k8s quantity format. The conformance `AddExtendedResource`
            // helper writes `resource.MustParse`d values, which serialize
            // canonically (e.g. 1000 -> "1k"), so a raw i64 parse of the node
            // capacity yields 0 and wrongly rejects the pod. Parse both as
            // Quantities. (#542)
            let node_capacity = allocatable
                .get(key)
                .map(|s| parse_resource_quantity(s, key))
                .unwrap_or(0);
            let used = used_extended.get(key).copied().unwrap_or(0);
            if used + requested > node_capacity {
                return 0; // Extended resource insufficient
            }
        }
    }

    let available_cpu = total_cpu - used_cpu;
    let available_memory = total_memory - used_memory;

    // If node can't fit the pod, return 0
    if cpu_request > available_cpu || memory_request > available_memory {
        return 0;
    }

    // Check extended resources (non-cpu/memory/pods/ephemeral-storage)
    // K8s scheduler checks all requested resources against node allocatable.
    for (res_name, &requested) in &pod_amounts {
        if res_name == "cpu"
            || res_name == "memory"
            || res_name == "pods"
            || res_name == "ephemeral-storage"
        {
            continue; // Already handled above or not tracked
        }
        // Extended resource — check node allocatable
        let total = allocatable
            .get(res_name)
            .map(|s| parse_resource_quantity(s, res_name))
            .unwrap_or(0);
        if total == 0 {
            return 0; // Node doesn't have this resource
        }
        // Count used by other pods
        let mut used = 0i64;
        for existing_pod in all_pods {
            let on_node = existing_pod
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .map(|n| n == node_name)
                .unwrap_or(false);
            if !on_node {
                continue;
            }
            let phase = existing_pod.status.as_ref().and_then(|s| s.phase.as_ref());
            if !matches!(phase, Some(rusternetes_common::types::Phase::Running)) {
                continue;
            }
            if existing_pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            used += pod_request_amounts(existing_pod)
                .get(res_name)
                .copied()
                .unwrap_or(0);
        }
        if requested > total - used {
            return 0; // Not enough extended resource
        }
    }

    // Calculate score based on remaining capacity (0-100), over the non-zero
    // requests (`Non0CPU`/`Non0Mem`, `framework/types.go:722-751`) so
    // best-effort pods still weigh on the node. Fit above used raw requests.
    let (cpu_request_non0, memory_request_non0) = non_zero_request_amounts(pod);
    let score_cpu = total_cpu - used_cpu_non0;
    let score_memory = total_memory - used_memory_non0;
    let cpu_score = if score_cpu > 0 {
        ((score_cpu - cpu_request_non0).max(0) * 100 / score_cpu) as i32
    } else {
        0
    };

    let memory_score = if score_memory > 0 {
        ((score_memory - memory_request_non0).max(0) * 100 / score_memory) as i32
    } else {
        0
    };

    // Return average score
    (cpu_score + memory_score) / 2
}

/// Parse a `resource.Quantity` into the unit the scheduler accounts in.
///
/// CPU is returned in millicores; every other resource (memory,
/// ephemeral-storage, extended resources) in its base unit. This mirrors
/// upstream `Resource.Add`, which reads `rQuant.MilliValue()` for
/// `ResourceCPU` and `rQuant.Value()` for everything else
/// (`../kubernetes/pkg/scheduler/framework/types.go:917-918`).
///
/// Parsing itself is delegated to [`Quantity::parse`], the in-tree port of
/// `k8s.io/apimachinery/pkg/api/resource/quantity.go`, so the full grammar
/// is accepted — in particular `<number> ::= <digits> | <digits>.<digits> |
/// <digits>. | .<digits>` permits a decimal point with *every* suffix, so
/// `"0.5Gi"` is as valid as `"512Mi"` and denotes the same 536870912 bytes.
/// The previous hand-rolled `strip_suffix` chain parsed the digits with
/// `str::parse::<i64>()` and fell back to `0` for every fractional quantity,
/// which reads to the scheduler as "this pod requests nothing"; `Pi`/`Ei`
/// were missing from the binarySI set and hit the same fallback.
///
/// Callers have no error channel, so unparseable input yields 0. Values
/// beyond `i64` saturate, matching upstream `ScaledValue`.
pub fn parse_resource_quantity(quantity: &str, resource_type: &str) -> i64 {
    parse_resource_value(quantity, resource_type).unwrap_or(0)
}

/// A pod's total resource requests as the scheduler accounts for them, keyed by
/// resource name, in each resource's base unit (milli-units for `cpu`, units
/// for everything else — upstream `Resource.Add`, `types.go:917-918`).
///
/// Delegates to [`rusternetes_common::quota::pod_requests`], the port of
/// upstream `PodRequests` (`component-helpers/resource/helpers.go:149-185`):
/// containers summed, init containers folded in as a max (restartable ones
/// added), pod-level `spec.resources.requests` overriding per name (only when
/// `PodLevelResources` is enabled — `SkipPodLevelResources:
/// !EnablePodLevelResources`, `noderesources/fit.go:296-297`,
/// `framework/types.go:722-731`), and `spec.overhead` added.
pub fn pod_request_amounts(pod: &Pod) -> HashMap<String, i64> {
    rusternetes_common::quota::pod_requests(pod)
        .into_iter()
        .map(|(name, q)| {
            let v = if name == "cpu" {
                q.milli_value()
            } else {
                q.value()
            };
            (name, v.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
        })
        .collect()
}

/// `DefaultMilliCPURequest` — `pkg/scheduler/util/pod_resources.go:29`
/// (`100 // 0.1 core`).
pub const DEFAULT_MILLI_CPU_REQUEST: i64 = 100;
/// `DefaultMemoryRequest` — `pkg/scheduler/util/pod_resources.go:31`
/// (`200 * 1024 * 1024 // 200 MB`).
pub const DEFAULT_MEMORY_REQUEST: i64 = 200 * 1024 * 1024;

/// A pod's non-zero CPU (milli) and memory requests: the `Non0CPU`/`Non0Mem`
/// that upstream `PodInfo.CalculateResource` computes for scoring
/// (`pkg/scheduler/framework/types.go:722-751`, via
/// `getNonMissingContainerRequests` `:1025-1053`).
///
/// A container (or init container) that does not set cpu/memory is charged
/// `DEFAULT_MILLI_CPU_REQUEST`/`DEFAULT_MEMORY_REQUEST` so best-effort pods
/// still weigh on a node's score. Filtering keeps using the raw
/// [`pod_request_amounts`].
pub fn non_zero_request_amounts(pod: &Pod) -> (i64, i64) {
    use rusternetes_common::quantity::{Format, Quantity};
    use rusternetes_common::quota::{pod_requests, pod_requests_non_missing, ResourceList};

    let requests = pod_requests(pod);
    let pod_level_set = rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::PodLevelResources,
    ) && pod
        .spec
        .as_ref()
        .and_then(|s| s.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .is_some_and(|r| {
            r.keys()
                .any(|k| rusternetes_common::quota::is_supported_pod_level_resource(k))
        });
    let cpu = Quantity::from_milli_value(DEFAULT_MILLI_CPU_REQUEST, Format::DecimalSI);
    let mem = Quantity::from_value(DEFAULT_MEMORY_REQUEST, Format::DecimalSI);
    let mut non_missing = ResourceList::new();
    if !pod_level_set || !requests.contains_key("cpu") {
        non_missing.insert("cpu".to_string(), cpu);
    }
    if !pod_level_set || !requests.contains_key("memory") {
        non_missing.insert("memory".to_string(), mem);
    }
    // Upstream types.go:731-745 only recomputes when non-missing is non-empty.
    let non0 = if non_missing.is_empty() {
        requests
    } else {
        pod_requests_non_missing(pod, Some(&non_missing))
    };
    let clamp = |v: i128| v.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    (
        non0.get("cpu").map(|q| clamp(q.milli_value())).unwrap_or(0),
        non0.get("memory").map(|q| clamp(q.value())).unwrap_or(0),
    )
}

/// System-critical priority threshold. Pods at or above this priority
/// can only be preempted by pods with strictly higher priority.
#[allow(dead_code)]
const SYSTEM_CRITICAL_PRIORITY: i32 = 2_000_000_000;

/// Is this pod allowed to preempt *again*?
///
/// A pod that has already preempted carries `status.nominatedNodeName`, and its
/// victims take their termination grace period to actually disappear. Until they
/// do, the node still accounts for their resources, so the preemptor still does
/// not fit — and a scheduler that simply retries will preempt a *second* set of
/// victims, on another node.
///
/// That is what broke `[sig-scheduling] SchedulerPreemption [Serial]`: the
/// low-priority pod on the first node was correctly preempted, then the next
/// scheduling cycle preempted a medium-priority pod on the *other* node, and the
/// spec's assertion that every other pod survives failed with
/// `pods "pod1-1-sched-preemption-medium-priority" not found`
/// (test/e2e/scheduling/preemption.go:206).
///
/// Upstream gates this in `PodEligibleToPreemptOthers`
/// (pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:317-341):
///
/// ```go
/// nomNodeName := pod.Status.NominatedNodeName
/// if len(nomNodeName) > 0 {
///     if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
///         for _, p := range nodeInfo.GetPods() {
///             if pl.isPreemptionAllowed(nodeInfo, p, pod) && podTerminatingByPreemption(p.GetPod()) {
///                 // There is a terminating pod on the nominated node.
///                 return false, "not eligible due to a terminating pod on the nominated node."
/// ```
///
/// Returns `false` when a lower-priority pod on the nominated node is still
/// terminating, i.e. the preemptor should wait for the room it already made.
pub fn pod_eligible_to_preempt_others(pod: &Pod, all_pods: &[Pod]) -> bool {
    let nominated = match pod
        .status
        .as_ref()
        .and_then(|s| s.nominated_node_name.as_deref())
    {
        Some(n) if !n.is_empty() => n,
        _ => return true,
    };

    let incoming_priority = pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);

    !all_pods.iter().any(|p| {
        let on_nominated_node = p
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_deref())
            .map(|n| n == nominated)
            .unwrap_or(false);
        if !on_nominated_node {
            return false;
        }
        // Same lower-priority test the victim selection uses.
        let victim_priority = p.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
        if victim_priority >= incoming_priority {
            return false;
        }
        pod_terminating_by_preemption(p)
    })
}

/// Is this pod terminating *because the scheduler preempted it*?
///
/// Port of upstream `podTerminatingByPreemption`
/// (pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:355-366):
///
/// ```go
/// if p.DeletionTimestamp == nil { return false }
/// for _, condition := range p.Status.Conditions {
///     if condition.Type == v1.DisruptionTarget {
///         return condition.Status == v1.ConditionTrue && condition.Reason == v1.PodReasonPreemptionByScheduler
///     }
/// }
/// return false
/// ```
///
/// The `DisruptionTarget` requirement is not decoration. Testing only for a
/// `deletionTimestamp` treats *any* terminating pod — a finished Job's pod, the
/// previous conformance spec's cleanup, a rolling update's old replica — as
/// evidence that this preemptor already made room, so
/// [`pod_eligible_to_preempt_others`] blocks a legitimate first preemption and
/// nothing gets preempted at all. That regression showed up immediately as
/// `expected pod to be preempted, instead got pod pod0-0-sched-preemption-low-priority`
/// (test/e2e/scheduling/preemption.go:202).
fn pod_terminating_by_preemption(p: &Pod) -> bool {
    if p.metadata.deletion_timestamp.is_none() {
        return false;
    }
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .into_iter()
        .flatten()
        .find(|c| c.condition_type == "DisruptionTarget")
        .map(|c| c.status == "True" && c.reason.as_deref() == Some("PreemptionByScheduler"))
        .unwrap_or(false)
}

/// One node preemption could use, paired with the victims it would cost.
///
/// Upstream's equivalent is `extenderv1.Victims` keyed by node name inside
/// `pickOneNodeForPreemption` (pkg/scheduler/framework/preemption/preemption.go:651).
#[derive(Debug, Clone)]
pub struct PreemptionCandidate {
    pub node_name: String,
    /// Victim pod objects, as returned by [`select_preemption_victims`]
    /// (upstream `extenderv1.Victims.Pods`).
    pub victims: Vec<Pod>,
    /// How many of those victims violate a PodDisruptionBudget. The live
    /// scheduling path calls the PDB-unaware [`check_preemption`], so this is
    /// currently always 0 there; the field exists so the score chain is the
    /// real one and becomes correct the moment PDBs are threaded through.
    pub num_pdb_violations: i64,
}

/// Choose which node's victims to preempt.
///
/// Port of upstream `pickOneNodeForPreemption`
/// (pkg/scheduler/framework/preemption/preemption.go:651-730). Upstream applies
/// five score functions in order of precedence, moving to the next only while
/// more than one node ties for the best score:
///
/// 1. fewest PodDisruptionBudget violations,
/// 2. **lowest highest-priority victim** ("a node with a minimum highest
///    priority victim is preferable"),
/// 3. smallest sum of victim priorities,
/// 4. fewest victims,
/// 5. latest start time among the highest-priority victims.
///
/// Taking the *first* feasible node instead — which is what this scheduler did
/// until now — makes the outcome depend on node iteration order. That is the
/// whole of the remaining #1130 failure: with a low-priority victim on node-1
/// and a medium-priority victim on node-2, we preempted whichever node came
/// first, so `[sig-scheduling] SchedulerPreemption validates basic preemption
/// works` passed or failed by luck of ordering:
///
/// ```text
/// passing: Preempting 1 pod(s) on node node-1 ... Evicted pod0-0-sched-preemption-low-priority
/// failing: Preempting 1 pod(s) on node node-2 ... Evicted pod1-1-sched-preemption-medium-priority
/// ```
///
/// The spec asserts the lowest-priority pod is the one that dies, which is
/// exactly what criterion 2 guarantees.
///
/// Returns `None` only when `candidates` is empty.
pub fn pick_one_node_for_preemption(candidates: &[PreemptionCandidate]) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    // Upstream reads priority and start time off the victim object itself
    // (`corev1helpers.PodPriority(pod)`), never by looking a name back up.
    let priority_of = |p: &Pod| -> i32 { p.spec.as_ref().and_then(|s| s.priority).unwrap_or(0) };

    // Highest victim priority on a candidate. Upstream reads
    // `nodesToVictims[node].Pods[0]` because its victim list is sorted
    // highest-priority-first; we take the max explicitly rather than depend on
    // check_preemption's ordering.
    let highest_victim_priority =
        |c: &PreemptionCandidate| -> i32 { c.victims.iter().map(&priority_of).max().unwrap_or(0) };

    // Upstream adds MaxInt32+1 to every priority before summing, so that a node
    // with a few negative-priority pods is not preferred over a node with fewer
    // pods of the same negative priority. i64 keeps the shifted sum exact.
    let sum_shifted_priorities = |c: &PreemptionCandidate| -> i64 {
        c.victims
            .iter()
            .map(|v| priority_of(v) as i64 + i32::MAX as i64 + 1)
            .sum()
    };

    // Earliest start time among the highest-priority victims; the *latest* such
    // time scores best (upstream latestStartTimeScoreFunc via
    // util.GetEarliestPodStartTime). A victim with no startTime is treated as
    // the epoch, matching upstream's nil handling being the worst score.
    let latest_start_time = |c: &PreemptionCandidate| -> i64 {
        let top = highest_victim_priority(c);
        c.victims
            .iter()
            .filter(|v| priority_of(v) == top)
            .map(|v| {
                v.status
                    .as_ref()
                    .and_then(|st| st.start_time)
                    .map(|t| t.timestamp_nanos_opt().unwrap_or(i64::MIN))
                    .unwrap_or(i64::MIN)
            })
            .min()
            .unwrap_or(i64::MIN)
    };

    // Each closure scores a candidate; higher is better, as upstream.
    let score_funcs: [&dyn Fn(&PreemptionCandidate) -> i64; 5] = [
        &|c| -c.num_pdb_violations,
        &|c| -(highest_victim_priority(c) as i64),
        &|c| -sum_shifted_priorities(c),
        &|c| -(c.victims.len() as i64),
        &latest_start_time,
    ];

    let mut remaining: Vec<&PreemptionCandidate> = candidates.iter().collect();
    for score in score_funcs {
        if remaining.len() == 1 {
            break;
        }
        let best = remaining.iter().map(|c| score(c)).max()?;
        remaining.retain(|c| score(c) == best);
    }

    // Still tied: upstream selects the first node in the list.
    remaining.first().map(|c| c.node_name.clone())
}

/// Check if preemption should occur and return pods to evict.
///
/// This is the PDB-unaware entry point retained for callers that don't have
/// PodDisruptionBudgets loaded. Equivalent to
/// `check_preemption_with_pdbs(node, pod, all_pods, &[], &HashMap::new())`.
///
/// Returns (should_preempt, pods_to_evict).
///
/// Passes an empty PriorityClass map, so the `preemptionPolicy: Never` check
/// honours only the pod's own `spec.preemptionPolicy` (no class fallback) — its
/// callers don't load PriorityClasses. Use [`check_preemption_with_pdbs`] with a
/// populated map when the class fallback is needed.
///
/// `dead_code`: invoked from integration tests under `crates/scheduler/tests/`
/// and from the legacy direct-scheduling path in `scheduler.rs`. The bin's
/// Framework-based path doesn't call it.
#[allow(dead_code)]
pub fn check_preemption(node: &Node, pod: &Pod, all_pods: &[Pod]) -> (bool, Vec<String>) {
    check_preemption_with_pdbs(node, pod, all_pods, &[], &HashMap::new())
}

/// PDB-aware preemption victim selection.
///
/// The base algorithm (resource-fit, lowest-priority-first eviction with the
/// "remove all, then reprieve" reprieve pass) is identical to
/// `check_preemption`. After candidate victims are chosen, we run a second
/// reprieve pass that swaps PDB-covered victims for non-PDB-covered candidates
/// when resources still fit. This mirrors upstream Kubernetes' behavior at
/// `pkg/scheduler/framework/preemption/preemption.go::selectVictimsOnNode`,
/// which sorts candidates so that PDB-violating evictions are picked last.
///
/// Returns (should_preempt, pods_to_evict).
///
/// `dead_code`: exercised by integration tests; bin uses Framework path.
#[allow(dead_code)]
pub fn check_preemption_with_pdbs(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    pdbs: &[PodDisruptionBudget],
    priority_classes: &HashMap<String, PriorityClass>,
) -> (bool, Vec<String>) {
    let (ok, victims) = select_preemption_victims(node, pod, all_pods, pdbs, priority_classes);
    (ok, victims.into_iter().map(|v| v.metadata.name).collect())
}

/// Identity of a pod for preemption bookkeeping: its UID, falling back to
/// `namespace/name` for pods that carry none (unit-test fixtures).
///
/// Upstream keys everything on `types.UID` (`preempting sets.Set[types.UID]`,
/// pkg/scheduler/framework/preemption/preemption.go:140) and removes nodeInfo
/// pods by UID (`NodeInfo.RemovePod`, pkg/scheduler/framework/types.go). Name
/// alone is not an identity: two namespaces can each hold a pod called `w`.
pub fn pod_key(p: &Pod) -> String {
    if !p.metadata.uid.is_empty() {
        return p.metadata.uid.clone();
    }
    format!(
        "{}/{}",
        p.metadata.namespace.as_deref().unwrap_or(""),
        p.metadata.name
    )
}

/// Victim selection returning the victim pod *objects*, as upstream's
/// `SelectVictimsOnNode` returns `[]*v1.Pod`
/// (pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:207-213).
/// The caller deletes by `pod.Namespace`/`pod.Name` like
/// `util.DeletePod` (pkg/scheduler/util/utils.go:137-138).
///
/// Returns (should_preempt, victims).
pub fn select_preemption_victims(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    pdbs: &[PodDisruptionBudget],
    priority_classes: &HashMap<String, PriorityClass>,
) -> (bool, Vec<Pod>) {
    // Get the priority of the incoming pod
    let incoming_priority = pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);

    // If incoming pod has priority <= 0, don't preempt
    if incoming_priority <= 0 {
        return (false, vec![]);
    }

    // Check preemptionPolicy — if "Never", do not preempt. Fall back to the
    // PriorityClass's policy when the pod spec doesn't carry one (mirrors
    // scheduler.rs::try_preempt: admission normally copies the class policy
    // onto the pod, but pods written directly to storage may miss it).
    let preemption_policy = pod
        .spec
        .as_ref()
        .and_then(|s| s.preemption_policy.as_deref())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|s| s.priority_class_name.as_deref())
                .and_then(|name| priority_classes.get(name))
                .and_then(|pc| pc.preemption_policy.as_deref())
        })
        .unwrap_or("PreemptLowerPriority");
    if preemption_policy == "Never" {
        debug!(
            "Pod {} has preemptionPolicy=Never, skipping preemption",
            pod.metadata.name
        );
        return (false, vec![]);
    }

    // Find all non-terminal pods on this node (K8s considers Pending and
    // Running pods as resource consumers and potential preemption victims).
    // Only skip terminal pods (Succeeded/Failed) and pods already terminating.
    let node_pods: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            let on_this_node = p
                .spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .map(|n| n == &node.metadata.name)
                .unwrap_or(false);
            if !on_this_node {
                return false;
            }
            // Skip terminal pods (Succeeded/Failed) and terminating pods
            let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
            if matches!(
                phase,
                Some(rusternetes_common::types::Phase::Succeeded)
                    | Some(rusternetes_common::types::Phase::Failed)
            ) {
                return false;
            }
            p.metadata.deletion_timestamp.is_none()
        })
        .collect();

    // Find pods with lower priority that could be evicted
    // System-critical pods (priority >= 2000000000) are protected:
    // only pods with strictly higher priority may preempt them.
    let mut candidates: Vec<(&Pod, i32)> = node_pods
        .iter()
        .filter_map(|p| {
            let pod_priority = p.spec.as_ref().and_then(|s| s.priority).unwrap_or(0);
            if pod_priority >= incoming_priority {
                return None; // Can't evict equal or higher priority
            }
            // Protect system-critical pods — only strictly higher priority can preempt
            if pod_priority >= SYSTEM_CRITICAL_PRIORITY && incoming_priority <= pod_priority {
                return None;
            }
            Some((*p, pod_priority))
        })
        .collect();

    // If no candidates, can't preempt
    if candidates.is_empty() {
        return (false, vec![]);
    }

    // Sort by priority (lowest first) for eviction
    candidates.sort_by_key(|(_, priority)| *priority);

    // Calculate ALL resources needed by incoming pod (cpu, memory, AND extended resources)
    // K8s preemption considers all resource types, not just cpu/memory.
    // See: pkg/scheduler/framework/preemption/preemption.go
    let mut resources_needed: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for (key, amount) in pod_request_amounts(pod) {
        *resources_needed.entry(key).or_insert(0) += amount;
    }

    // Get node's total allocatable resources (all types)
    let allocatable: &std::collections::HashMap<String, String> =
        match node.status.as_ref().and_then(|s| s.allocatable.as_ref()) {
            Some(a) => a,
            None => return (false, vec![]),
        };
    let mut total_resources: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for (key, val) in allocatable {
        total_resources.insert(key.clone(), parse_resource_quantity(val, key));
    }

    // If the pod can't fit even on a completely empty node, preemption won't help
    for (key, needed) in &resources_needed {
        let total = total_resources.get(key).copied().unwrap_or(0);
        if *needed > total {
            return (false, vec![]);
        }
    }

    // Calculate resources used by ALL pods on this node (including non-candidates)
    let mut total_used: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for p in &node_pods {
        for (key, amount) in pod_request_amounts(p) {
            *total_used.entry(key).or_insert(0) += amount;
        }
    }

    // Current remaining resources (before any eviction)
    let remaining = |key: &str| -> i64 {
        let total = total_resources.get(key).copied().unwrap_or(0);
        let used = total_used.get(key).copied().unwrap_or(0);
        total - used
    };

    // Check if all resources fit without eviction
    let all_fit = resources_needed
        .iter()
        .all(|(key, needed)| remaining(key) >= *needed);
    if all_fit {
        return (true, vec![]);
    }

    // K8s preemption algorithm: "remove all, then reprieve"
    // 1. Remove ALL lower-priority candidates and check if pod fits
    // 2. If it doesn't fit even with all removed → node not suitable
    // 3. Try to add back (reprieve) candidates from highest to lowest priority
    // 4. If adding a candidate back still lets the pod fit → reprieve it
    // 5. Final victims = candidates that could NOT be reprieved
    // See: pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:233-300

    // Calculate total freed resources if ALL candidates are evicted
    let mut total_freed: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for (candidate_pod, _) in &candidates {
        for (key, amount) in pod_request_amounts(candidate_pod) {
            *total_freed.entry(key).or_insert(0) += amount;
        }
    }

    // Check if pod fits even with ALL candidates removed
    let fits_without_all = resources_needed.iter().all(|(key, needed)| {
        let rem = remaining(key);
        let free = total_freed.get(key).copied().unwrap_or(0);
        (rem + free) >= *needed
    });
    if !fits_without_all {
        return (false, vec![]);
    }

    // Sort candidates by DESCENDING priority (highest first) for reprieve pass
    // K8s tries to reprieve higher-priority pods first
    let mut candidates_for_reprieve = candidates.clone();
    candidates_for_reprieve.sort_by_key(|(_, priority)| std::cmp::Reverse(*priority));

    // Track which candidates are victims (start with all as victims)
    let mut reprieved: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Try to reprieve each candidate (highest priority first)
    for (candidate_pod, _) in &candidates_for_reprieve {
        // Calculate resources freed by all NON-reprieved candidates (excluding this one)
        let mut freed_without_this: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (other_pod, _) in &candidates {
            if pod_key(other_pod) == pod_key(candidate_pod) {
                continue; // Skip the candidate we're trying to reprieve
            }
            if reprieved.contains(&pod_key(other_pod)) {
                continue; // Skip already-reprieved pods
            }
            for (key, amount) in pod_request_amounts(other_pod) {
                *freed_without_this.entry(key).or_insert(0) += amount;
            }
        }

        // Check if pod still fits without evicting this candidate
        let fits_without = resources_needed.iter().all(|(key, needed)| {
            let rem = remaining(key);
            let free = freed_without_this.get(key).copied().unwrap_or(0);
            (rem + free) >= *needed
        });

        if fits_without {
            // Pod fits without evicting this candidate → reprieve it
            reprieved.insert(pod_key(candidate_pod));
        }
        // else: must evict this candidate
    }

    // PDB-aware reprieve: if any PDBs are supplied, re-run the reprieve pass
    // with PDB-covered candidates considered FIRST. The reprieve pass favors
    // keeping candidates that are visited early (they get their "fits without
    // me?" check while the rest of the candidates are still presumed to be
    // evicted, which gives them the best chance of being reprieved). Visiting
    // PDB-covered candidates first therefore biases the algorithm toward
    // evicting PDB-free pods. This mirrors the upstream Kubernetes scheduler's
    // dryRunPreemption/selectVictimsOnNode preference for non-PDB-violating
    // victims at pkg/scheduler/framework/preemption/preemption.go.
    if !pdbs.is_empty() {
        let pdb_covered: std::collections::HashSet<String> = candidates
            .iter()
            .filter(|(p, _)| pod_violates_any_pdb(p, pdbs, all_pods))
            .map(|(p, _)| pod_key(p))
            .collect();

        let mut pdb_aware_order = candidates.clone();
        // Primary key: PDB-covered first (reverse: true → 0, false → 1).
        // Secondary key: highest priority first (existing behavior).
        pdb_aware_order.sort_by_key(|(p, priority)| {
            let covered = pdb_covered.contains(&pod_key(p));
            (if covered { 0 } else { 1 }, std::cmp::Reverse(*priority))
        });

        let mut reprieved_pdb: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (candidate_pod, _) in &pdb_aware_order {
            let mut freed_without_this: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            for (other_pod, _) in &candidates {
                if pod_key(other_pod) == pod_key(candidate_pod) {
                    continue;
                }
                if reprieved_pdb.contains(&pod_key(other_pod)) {
                    continue;
                }
                for (key, amount) in pod_request_amounts(other_pod) {
                    *freed_without_this.entry(key).or_insert(0) += amount;
                }
            }

            let fits_without = resources_needed.iter().all(|(key, needed)| {
                let rem = remaining(key);
                let free = freed_without_this.get(key).copied().unwrap_or(0);
                (rem + free) >= *needed
            });

            if fits_without {
                reprieved_pdb.insert(pod_key(candidate_pod));
            }
        }

        reprieved = reprieved_pdb;
    }

    // Collect final victims (candidates that were NOT reprieved)
    let pods_to_evict: Vec<Pod> = candidates
        .iter()
        .filter(|(p, _)| !reprieved.contains(&pod_key(p)))
        .map(|(p, _)| (*p).clone())
        .collect();

    if pods_to_evict.is_empty() {
        // All candidates were reprieved — pod fits without any eviction
        return (true, vec![]);
    }

    debug!(
        "Preemption possible on node {}: evicting {} pods (reprieved {})",
        node.metadata.name,
        pods_to_evict.len(),
        reprieved.len()
    );
    (true, pods_to_evict)
}

/// Returns true if evicting `victim` would violate any of the supplied PDBs.
///
/// Conservative definition: any PDB whose selector matches the victim's labels
/// is considered to cover it. If the PDB's `minAvailable` is an integer, we
/// compare against the current count of running matching pods; if removing
/// `victim` drops the count below `minAvailable`, eviction is a violation.
/// Percentage minAvailable values are also evaluated against the current
/// matching population. `maxUnavailable` is treated as "1 disruption allowed"
/// from the current matching set (we don't track historical disruptions here).
/// How many of `victims` would break a PodDisruptionBudget if evicted.
///
/// Feeds [`PreemptionCandidate::num_pdb_violations`], which is the first of
/// upstream's victim-node score functions — `minNumPDBViolatingScoreFunc` in
/// `pickOneNodeForPreemption`
/// (pkg/scheduler/framework/preemption/preemption.go:662-665), scoring
/// `-nodesToVictims[node].NumPDBViolations`.
///
/// Upstream counts violations while *selecting* victims
/// (`selectVictimsOnNode` returns `numViolatingVictim` alongside the list); our
/// [`check_preemption_with_pdbs`] returns only the names, so the count is
/// recomputed here over the victims it chose. Same inputs, same predicate
/// ([`pod_violates_any_pdb`]) — so the number agrees with the selection that
/// produced it.
///
pub fn count_pdb_violating_victims(
    victims: &[Pod],
    all_pods: &[Pod],
    pdbs: &[PodDisruptionBudget],
) -> i64 {
    if pdbs.is_empty() {
        return 0;
    }
    victims
        .iter()
        .filter(|victim| pod_violates_any_pdb(victim, pdbs, all_pods))
        .count() as i64
}

fn pod_violates_any_pdb(victim: &Pod, pdbs: &[PodDisruptionBudget], all_pods: &[Pod]) -> bool {
    for pdb in pdbs {
        if !pdb_covers_pod(pdb, victim) {
            continue;
        }
        let healthy_now = all_pods
            .iter()
            .filter(|p| pdb_covers_pod(pdb, p))
            .filter(|p| !is_pod_terminal(p))
            .count() as i32;

        if let Some(ref min_avail) = pdb.spec.min_available {
            let min_avail_i = match min_avail {
                IntOrString::Int(n) => *n,
                IntOrString::String(s) => parse_min_available_percent(s, healthy_now),
            };
            if healthy_now - 1 < min_avail_i {
                return true;
            }
        }
        if let Some(ref max_unavail) = pdb.spec.max_unavailable {
            let max_unavail_i = match max_unavail {
                IntOrString::Int(n) => *n,
                IntOrString::String(s) => parse_min_available_percent(s, healthy_now),
            };
            // A single eviction counts as one unavailable.
            if max_unavail_i < 1 {
                return true;
            }
        }
    }
    false
}

/// True if the PDB's selector matches the pod's labels and the pod's namespace
/// equals the PDB's namespace.
fn pdb_covers_pod(pdb: &PodDisruptionBudget, pod: &Pod) -> bool {
    if pdb.metadata.namespace != pod.metadata.namespace
        && pdb.metadata.namespace.is_some()
        && pod.metadata.namespace.is_some()
    {
        return false;
    }
    // A null selector matches no pods: upstream reads it through
    // `LabelSelectorAsSelector`, which returns `labels.Nothing()` for nil
    // (`apimachinery/pkg/apis/meta/v1/helpers.go:37-43`).
    let Some(selector) = &pdb.spec.selector else {
        return false;
    };
    match_selector(selector, &pod.metadata.labels)
}

fn is_pod_terminal(p: &Pod) -> bool {
    let phase = p.status.as_ref().and_then(|s| s.phase.as_ref());
    if matches!(
        phase,
        Some(rusternetes_common::types::Phase::Succeeded)
            | Some(rusternetes_common::types::Phase::Failed)
    ) {
        return true;
    }
    p.metadata.deletion_timestamp.is_some()
}

/// Parse a percentage string like "20%" into an absolute count given the
/// current healthy population. Falls back to 0 on parse error.
fn parse_min_available_percent(s: &str, healthy_now: i32) -> i32 {
    if let Some(num) = s.strip_suffix('%') {
        if let Ok(pct) = num.trim().parse::<f32>() {
            // Round up like K8s does (ceil): minimum of pct% of healthy.
            return ((healthy_now as f32) * pct / 100.0).ceil() as i32;
        }
    }
    s.trim().parse::<i32>().unwrap_or(0)
}

/// Check topology spread constraints for a pod
/// Returns (passes_hard_constraints, score_penalty)
pub fn check_topology_spread_constraints(
    node: &Node,
    pod: &Pod,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    let constraints = match &pod.spec {
        Some(spec) => match &spec.topology_spread_constraints {
            Some(c) => c,
            None => return (true, 0), // No constraints
        },
        None => return (true, 0),
    };

    let mut total_penalty = 0;

    for constraint in constraints {
        let (passes, penalty) =
            check_single_topology_constraint(node, pod, constraint, all_pods, all_nodes);

        if !passes {
            return (false, 0); // Hard constraint failed
        }

        total_penalty += penalty;
    }

    (true, total_penalty)
}

/// Check a single topology spread constraint
fn check_single_topology_constraint(
    node: &Node,
    _pod: &Pod,
    constraint: &TopologySpreadConstraint,
    all_pods: &[Pod],
    all_nodes: &[Node],
) -> (bool, i32) {
    // Get the topology value for the candidate node
    let node_topology_value = match node.metadata.labels.as_ref() {
        Some(labels) => match labels.get(&constraint.topology_key) {
            Some(v) => v.clone(),
            None => {
                // Node doesn't have the topology key
                // If whenUnsatisfiable is DoNotSchedule, we can't schedule here
                if constraint.when_unsatisfiable == "DoNotSchedule" {
                    return (false, 0);
                }
                return (true, 0);
            }
        },
        None => {
            if constraint.when_unsatisfiable == "DoNotSchedule" {
                return (false, 0);
            }
            return (true, 0);
        }
    };

    // Find all pods that match the label selector
    let matching_pods: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            // Skip unscheduled pods
            if p.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_none() {
                return false;
            }

            // Check if pod matches the label selector
            if let Some(ref selector) = constraint.label_selector {
                match_selector(selector, &p.metadata.labels)
            } else {
                // No label selector means match all pods
                true
            }
        })
        .collect();

    // Count pods per topology domain
    let mut domain_counts: HashMap<String, i32> = HashMap::new();

    // Initialize counts for all domains
    for n in all_nodes {
        if let Some(labels) = &n.metadata.labels {
            if let Some(topology_value) = labels.get(&constraint.topology_key) {
                domain_counts.entry(topology_value.clone()).or_insert(0);
            }
        }
    }

    // Count matching pods per domain
    for p in &matching_pods {
        if let Some(spec) = &p.spec {
            if let Some(node_name) = &spec.node_name {
                // Find the node this pod is on
                if let Some(pod_node) = all_nodes.iter().find(|n| &n.metadata.name == node_name) {
                    if let Some(labels) = &pod_node.metadata.labels {
                        if let Some(topology_value) = labels.get(&constraint.topology_key) {
                            *domain_counts.entry(topology_value.clone()).or_insert(0) += 1;
                        }
                    }
                }
            }
        }
    }

    // Calculate skew if we place this pod on the candidate node
    let current_count = domain_counts
        .get(&node_topology_value)
        .copied()
        .unwrap_or(0);
    let new_count = current_count + 1;

    // Find min and max counts
    let min_count = domain_counts.values().min().copied().unwrap_or(0);
    let max_count = domain_counts.values().max().copied().unwrap_or(0);

    // Calculate skew after placing pod
    let skew = if new_count > min_count {
        new_count - min_count
    } else {
        max_count - min_count
    };

    // Check if skew exceeds max_skew
    if skew > constraint.max_skew {
        if constraint.when_unsatisfiable == "DoNotSchedule" {
            debug!(
                "Topology spread constraint violated: skew {} > max_skew {} for topology key {}",
                skew, constraint.max_skew, constraint.topology_key
            );
            return (false, 0);
        } else {
            // ScheduleAnyway - allow but penalize
            let penalty = (skew - constraint.max_skew) * 10; // Penalty proportional to skew violation
            return (true, penalty);
        }
    }

    // Check minDomains if specified
    if let Some(min_domains) = constraint.min_domains {
        let num_domains = domain_counts.len() as i32;
        if num_domains < min_domains {
            if constraint.when_unsatisfiable == "DoNotSchedule" {
                return (false, 0);
            } else {
                let penalty = (min_domains - num_domains) * 5;
                return (true, penalty);
            }
        }
    }

    // Constraint satisfied - add small penalty based on imbalance to prefer better spread
    let imbalance_penalty = ((new_count as f32 - min_count as f32) * 2.0) as i32;
    (true, imbalance_penalty.max(0))
}
