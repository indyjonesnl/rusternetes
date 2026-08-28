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

/// Scoring result for a node
#[derive(Debug, Clone)]
pub struct NodeScore {
    pub node_name: String,
    pub score: i32,
}

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
fn toleration_matches_taint(toleration: &Toleration, taint: &Taint) -> bool {
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
fn host_ips_overlap(ip1: &str, ip2: &str) -> bool {
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
    let mut cpu_request = 0i64;
    let mut memory_request = 0i64;

    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    if let Some(cpu) = requests.get("cpu") {
                        cpu_request += parse_resource_quantity(cpu, "cpu");
                    }
                    if let Some(memory) = requests.get("memory") {
                        memory_request += parse_resource_quantity(memory, "memory");
                    }
                }
            }
        }
    }

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
        if let Some(spec) = &existing_pod.spec {
            for container in &spec.containers {
                if let Some(ref resources) = container.resources {
                    if let Some(ref requests) = resources.requests {
                        if let Some(cpu) = requests.get("cpu") {
                            used_cpu += parse_resource_quantity(cpu, "cpu");
                        }
                        if let Some(memory) = requests.get("memory") {
                            used_memory += parse_resource_quantity(memory, "memory");
                        }
                        // Track extended resource usage. Quantities use the
                        // full k8s format (e.g. "1k" == 1000), so parse with
                        // parse_resource_quantity, not a raw i64 parse — a
                        // canonicalized value like "1k" would otherwise parse
                        // to 0 and silently under-count usage.
                        for (key, val) in requests {
                            if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
                                *used_extended.entry(key.clone()).or_insert(0) +=
                                    parse_resource_quantity(val, key);
                            }
                        }
                    }
                }
            }
        }
    }

    // Check extended resources requested by the pod against node allocatable.
    // If ANY extended resource is insufficient, return 0 (can't schedule).
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    for (key, val) in requests {
                        if key != "cpu" && key != "memory" && key != "ephemeral-storage" {
                            // Both the request and the node's advertised capacity
                            // use the full k8s quantity format. The conformance
                            // `AddExtendedResource` helper writes `resource.MustParse`d
                            // values, which serialize canonically (e.g. 1000 -> "1k"),
                            // so a raw i64 parse of the node capacity yields 0 and
                            // wrongly rejects the pod. Parse both with
                            // parse_resource_quantity. (#542)
                            let requested = parse_resource_quantity(val, key);
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
                }
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
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    for (res_name, req_qty) in requests {
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
                        let requested = parse_resource_quantity(req_qty, res_name);
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
                            if let Some(spec) = &existing_pod.spec {
                                for c in &spec.containers {
                                    if let Some(ref r) = c.resources {
                                        if let Some(ref reqs) = r.requests {
                                            if let Some(q) = reqs.get(res_name) {
                                                used += parse_resource_quantity(q, res_name);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if requested > total - used {
                            return 0; // Not enough extended resource
                        }
                    }
                }
            }
        }
    }

    // Calculate score based on remaining capacity (0-100)
    // Higher remaining capacity = higher score (balanced scheduling)
    let cpu_score = if available_cpu > 0 {
        ((available_cpu - cpu_request) * 100 / available_cpu) as i32
    } else {
        0
    };

    let memory_score = if available_memory > 0 {
        ((available_memory - memory_request) * 100 / available_memory) as i32
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
pub(crate) fn parse_resource_quantity(quantity: &str, resource_type: &str) -> i64 {
    parse_resource_value(quantity, resource_type).unwrap_or(0)
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
    /// Victim pod names, as returned by [`check_preemption`].
    pub victims: Vec<String>,
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
pub fn pick_one_node_for_preemption(
    candidates: &[PreemptionCandidate],
    all_pods: &[Pod],
) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    let priority_of = |name: &str| -> i32 {
        all_pods
            .iter()
            .find(|p| p.metadata.name == name)
            .and_then(|p| p.spec.as_ref())
            .and_then(|s| s.priority)
            .unwrap_or(0)
    };

    // Highest victim priority on a candidate. Upstream reads
    // `nodesToVictims[node].Pods[0]` because its victim list is sorted
    // highest-priority-first; we take the max explicitly rather than depend on
    // check_preemption's ordering.
    let highest_victim_priority = |c: &PreemptionCandidate| -> i32 {
        c.victims.iter().map(|v| priority_of(v)).max().unwrap_or(0)
    };

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
                all_pods
                    .iter()
                    .find(|p| p.metadata.name == *v)
                    .and_then(|p| p.status.as_ref())
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
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ref resources) = container.resources {
                if let Some(ref requests) = resources.requests {
                    for (key, val) in requests {
                        let amount = parse_resource_quantity(val, key);
                        *resources_needed.entry(key.clone()).or_insert(0) += amount;
                    }
                }
            }
        }
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
        if let Some(spec) = &p.spec {
            for container in &spec.containers {
                if let Some(ref resources) = container.resources {
                    if let Some(ref requests) = resources.requests {
                        for (key, val) in requests {
                            *total_used.entry(key.clone()).or_insert(0) +=
                                parse_resource_quantity(val, key);
                        }
                    }
                }
            }
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
        if let Some(spec) = &candidate_pod.spec {
            for container in &spec.containers {
                if let Some(ref resources) = container.resources {
                    if let Some(ref requests) = resources.requests {
                        for (key, val) in requests {
                            *total_freed.entry(key.clone()).or_insert(0) +=
                                parse_resource_quantity(val, key);
                        }
                    }
                }
            }
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
            if other_pod.metadata.name == candidate_pod.metadata.name {
                continue; // Skip the candidate we're trying to reprieve
            }
            if reprieved.contains(&other_pod.metadata.name) {
                continue; // Skip already-reprieved pods
            }
            if let Some(spec) = &other_pod.spec {
                for container in &spec.containers {
                    if let Some(ref resources) = container.resources {
                        if let Some(ref requests) = resources.requests {
                            for (key, val) in requests {
                                *freed_without_this.entry(key.clone()).or_insert(0) +=
                                    parse_resource_quantity(val, key);
                            }
                        }
                    }
                }
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
            reprieved.insert(candidate_pod.metadata.name.clone());
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
            .map(|(p, _)| p.metadata.name.clone())
            .collect();

        let mut pdb_aware_order = candidates.clone();
        // Primary key: PDB-covered first (reverse: true → 0, false → 1).
        // Secondary key: highest priority first (existing behavior).
        pdb_aware_order.sort_by_key(|(p, priority)| {
            let covered = pdb_covered.contains(&p.metadata.name);
            (if covered { 0 } else { 1 }, std::cmp::Reverse(*priority))
        });

        let mut reprieved_pdb: std::collections::HashSet<String> = std::collections::HashSet::new();
        for (candidate_pod, _) in &pdb_aware_order {
            let mut freed_without_this: std::collections::HashMap<String, i64> =
                std::collections::HashMap::new();
            for (other_pod, _) in &candidates {
                if other_pod.metadata.name == candidate_pod.metadata.name {
                    continue;
                }
                if reprieved_pdb.contains(&other_pod.metadata.name) {
                    continue;
                }
                if let Some(spec) = &other_pod.spec {
                    for container in &spec.containers {
                        if let Some(ref resources) = container.resources {
                            if let Some(ref requests) = resources.requests {
                                for (key, val) in requests {
                                    *freed_without_this.entry(key.clone()).or_insert(0) +=
                                        parse_resource_quantity(val, key);
                                }
                            }
                        }
                    }
                }
            }

            let fits_without = resources_needed.iter().all(|(key, needed)| {
                let rem = remaining(key);
                let free = freed_without_this.get(key).copied().unwrap_or(0);
                (rem + free) >= *needed
            });

            if fits_without {
                reprieved_pdb.insert(candidate_pod.metadata.name.clone());
            }
        }

        reprieved = reprieved_pdb;
    }

    // Collect final victims (candidates that were NOT reprieved)
    let pods_to_evict: Vec<String> = candidates
        .iter()
        .filter(|(p, _)| !reprieved.contains(&p.metadata.name))
        .map(|(p, _)| p.metadata.name.clone())
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
/// Victim names that match no live pod are ignored rather than counted: an
/// unknown pod cannot be shown to violate a budget, and guessing "yes" would
/// steer node choice on no evidence.
pub fn count_pdb_violating_victims(
    victims: &[String],
    all_pods: &[Pod],
    pdbs: &[PodDisruptionBudget],
) -> i64 {
    if pdbs.is_empty() {
        return 0;
    }
    victims
        .iter()
        .filter_map(|name| all_pods.iter().find(|p| &p.metadata.name == name))
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
    match_selector(&pdb.spec.selector, &pod.metadata.labels)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpu_quantity() {
        assert_eq!(parse_resource_quantity("100m", "cpu"), 100);
        assert_eq!(parse_resource_quantity("1", "cpu"), 1000);
        assert_eq!(parse_resource_quantity("2", "cpu"), 2000);
        // Decimal CPU values (common in K8s)
        assert_eq!(parse_resource_quantity("0.5", "cpu"), 500);
        assert_eq!(parse_resource_quantity("0.8", "cpu"), 800);
        assert_eq!(parse_resource_quantity("1.5", "cpu"), 1500);
        assert_eq!(parse_resource_quantity("0.1", "cpu"), 100);
        assert_eq!(parse_resource_quantity("0.25", "cpu"), 250);
    }

    #[test]
    fn test_parse_memory_quantity() {
        assert_eq!(parse_resource_quantity("1Ki", "memory"), 1024);
        assert_eq!(parse_resource_quantity("1Mi", "memory"), 1024 * 1024);
        assert_eq!(parse_resource_quantity("1Gi", "memory"), 1024 * 1024 * 1024);
        assert_eq!(
            parse_resource_quantity("8Gi", "memory"),
            8 * 1024 * 1024 * 1024
        );
        // SI units
        assert_eq!(parse_resource_quantity("128M", "memory"), 128_000_000);
        assert_eq!(parse_resource_quantity("1G", "memory"), 1_000_000_000);
        // Plain bytes
        assert_eq!(parse_resource_quantity("128974848", "memory"), 128974848);
        // Scientific notation
        assert_eq!(parse_resource_quantity("129e6", "memory"), 129_000_000);
    }

    /// The `<number>` production allows a decimal point with every suffix,
    /// so these are ordinary quantities that a pod spec may carry. They used
    /// to parse as 0, which reads to the scheduler as "requests nothing".
    ///
    /// Expected values checked against `resource.ParseQuantity(s).Value()`
    /// from `k8s.io/apimachinery`.
    #[test]
    fn test_parse_fractional_memory_quantity() {
        assert_eq!(parse_resource_quantity("0.5Gi", "memory"), 536_870_912);
        assert_eq!(parse_resource_quantity("1.5Gi", "memory"), 1_610_612_736);
        assert_eq!(parse_resource_quantity("0.5Mi", "memory"), 524_288);
        assert_eq!(parse_resource_quantity("2.5Mi", "memory"), 2_621_440);
        assert_eq!(parse_resource_quantity("0.5Ki", "memory"), 512);
        assert_eq!(parse_resource_quantity("1.5G", "memory"), 1_500_000_000);
        assert_eq!(parse_resource_quantity("0.5M", "memory"), 500_000);
        assert_eq!(parse_resource_quantity("1.5k", "memory"), 1_500);
        // A fractional quantity must never round down to "free".
        assert!(parse_resource_quantity("0.25Gi", "memory") > 0);
    }

    /// `Pi`/`Ei` complete the binarySI set; both previously parsed as 0.
    #[test]
    fn test_parse_large_binary_suffixes() {
        assert_eq!(parse_resource_quantity("1Ti", "memory"), 1_099_511_627_776);
        assert_eq!(
            parse_resource_quantity("1Pi", "memory"),
            1_125_899_906_842_624
        );
        assert_eq!(
            parse_resource_quantity("1Ei", "memory"),
            1_152_921_504_606_846_976
        );
        assert_eq!(
            parse_resource_quantity("2Pi", "memory"),
            2_251_799_813_685_248
        );
    }

    /// Decimal SI suffixes and the bare `<decimalExponent>` form.
    #[test]
    fn test_parse_decimal_si_and_exponent() {
        assert_eq!(parse_resource_quantity("1T", "memory"), 1_000_000_000_000);
        assert_eq!(
            parse_resource_quantity("1P", "memory"),
            1_000_000_000_000_000
        );
        assert_eq!(
            parse_resource_quantity("1E", "memory"),
            1_000_000_000_000_000_000
        );
        assert_eq!(parse_resource_quantity("1k", "memory"), 1_000);
        // "E"/"e" as an exponent marker rather than the exa suffix.
        assert_eq!(parse_resource_quantity("129E6", "memory"), 129_000_000);
        assert_eq!(parse_resource_quantity("1.5e3", "memory"), 1_500);
        // Sub-unit decimalSI suffixes upstream accepts for any resource.
        assert_eq!(parse_resource_quantity("2n", "memory"), 1);
        assert_eq!(parse_resource_quantity("1500u", "memory"), 1);
    }

    /// CPU accepts the same grammar; `m` is the shared 10^-3 decimalSI suffix.
    #[test]
    fn test_parse_cpu_fractional_and_milli() {
        assert_eq!(parse_resource_quantity("100m", "cpu"), 100);
        assert_eq!(parse_resource_quantity("1500m", "cpu"), 1500);
        assert_eq!(parse_resource_quantity("0.7", "cpu"), 700);
        assert_eq!(parse_resource_quantity("2.5", "cpu"), 2500);
        // Sub-millicore precision rounds up, matching `Quantity.MilliValue()`
        // (ceiling) — asking for some CPU must never account as none.
        assert_eq!(parse_resource_quantity("10.5m", "cpu"), 11);
        assert_eq!(parse_resource_quantity("10.1m", "cpu"), 11);
    }

    /// Malformed input degrades to 0; callers have no error channel.
    #[test]
    fn test_parse_quantity_rejects_malformed_input() {
        for bad in ["", "   ", "abc", "Gi", "1Xi", "--5", "inf", "NaN", "1..2"] {
            assert_eq!(
                parse_resource_quantity(bad, "memory"),
                0,
                "expected {bad:?} to parse as 0"
            );
            assert_eq!(
                parse_resource_quantity(bad, "cpu"),
                0,
                "expected {bad:?} to parse as 0"
            );
        }
    }

    /// An exponent past the i64 range is a *valid* quantity upstream —
    /// `ParseQuantity` only caps BinarySI (`quantity.go`, `maxAllowed`), so a
    /// DecimalExponent quantity saturates in `ScaledValue`. Match that rather
    /// than silently reading it as 0, which would be indistinguishable from
    /// "requests nothing".
    #[test]
    fn test_parse_quantity_saturates_beyond_i64() {
        assert_eq!(parse_resource_quantity("1e400", "memory"), i64::MAX);
        assert_eq!(parse_resource_quantity("-1e400", "memory"), i64::MIN);
    }

    #[test]
    fn test_toleration_matches_taint() {
        let taint = Taint {
            key: "key1".to_string(),
            value: Some("value1".to_string()),
            effect: "NoSchedule".to_string(),
            time_added: None,
        };

        let toleration = Toleration {
            key: Some("key1".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("value1".to_string()),
            effect: Some("NoSchedule".to_string()),
            toleration_seconds: None,
        };

        assert!(toleration_matches_taint(&toleration, &taint));
    }

    use rusternetes_common::resources::NodeStatus;

    /// Helper: create a minimal container with resource requests
    fn make_container(cpu: &str, memory: &str) -> rusternetes_common::resources::Container {
        let mut requests = HashMap::new();
        requests.insert("cpu".to_string(), cpu.to_string());
        requests.insert("memory".to_string(), memory.to_string());
        rusternetes_common::resources::Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: None,
            env: None,
            env_from: None,
            resources: Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            }),
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        }
    }

    /// Helper: create a node with allocatable CPU and memory
    fn make_node(name: &str, cpu: &str, memory: &str) -> Node {
        let mut allocatable = HashMap::new();
        allocatable.insert("cpu".to_string(), cpu.to_string());
        allocatable.insert("memory".to_string(), memory.to_string());
        let mut node = Node::new(name);
        node.status = Some(NodeStatus {
            capacity: None,
            allocatable: Some(allocatable),
            conditions: None,
            addresses: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        });
        node
    }

    /// Helper: create a pod with given name, priority, and resource requests, scheduled on a node
    fn make_scheduled_pod(
        name: &str,
        priority: i32,
        cpu: &str,
        memory: &str,
        node_name: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container(cpu, memory)],
            priority: Some(priority),
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Helper: create an unscheduled pod (the incoming pod wanting resources)
    fn make_incoming_pod(
        name: &str,
        priority: i32,
        cpu: &str,
        memory: &str,
        preemption_policy: Option<&str>,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container(cpu, memory)],
            priority: Some(priority),
            preemption_policy: preemption_policy.map(|s| s.to_string()),
            ..Default::default()
        };
        Pod::new(name, spec)
    }

    /// Extended-resource fit must honor canonical k8s quantity strings on the
    /// node's advertised capacity. The conformance `AddExtendedResource` helper
    /// writes `resource.MustParse("1000")`, which serializes as "1k"; a raw i64
    /// parse of "1k" yields 0 and wrongly rejects a pod that requests the
    /// resource (#542 — SchedulerPreemption PreemptionExecutionPath).
    #[test]
    fn test_extended_resource_fit_with_canonical_quantity() {
        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "4".to_string());
        alloc.insert("memory".to_string(), "8Gi".to_string());
        alloc.insert("example.com/fakecpu".to_string(), "1k".to_string()); // == 1000
        let mut node = Node::new("node-2");
        node.status = Some(NodeStatus {
            capacity: None,
            allocatable: Some(alloc),
            conditions: None,
            addresses: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        });

        let fakecpu_pod = |name: &str, req: &str| {
            let mut requests = HashMap::new();
            requests.insert("example.com/fakecpu".to_string(), req.to_string());
            let mut container = make_container("0", "0");
            container.resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            });
            Pod::new(
                name,
                rusternetes_common::resources::PodSpec {
                    containers: vec![container],
                    ..Default::default()
                },
            )
        };

        // 200 of 1000 ("1k") MUST fit (score > 0).
        let pod = fakecpu_pod("rs-pod1", "200");
        assert!(
            calculate_resource_score_with_pods(&node, &pod, &[]) > 0,
            "pod requesting 200 of a 1k extended resource must fit"
        );

        // 2k (2000) of 1000 MUST NOT fit (score == 0).
        let big = fakecpu_pod("too-big", "2k");
        assert_eq!(
            calculate_resource_score_with_pods(&node, &big, &[]),
            0,
            "pod requesting 2k of a 1k extended resource must not fit"
        );
    }

    /// A node's freed space must be reserved for a higher-or-equal-priority pod
    /// already NOMINATED to it (by preemption), so a lower-priority candidate
    /// does not schedule into it and get re-preempted in a loop (#542).
    #[test]
    fn test_nominated_pod_reserves_extended_resource() {
        let mut alloc = HashMap::new();
        alloc.insert("cpu".to_string(), "4".to_string());
        alloc.insert("memory".to_string(), "8Gi".to_string());
        alloc.insert("example.com/fakecpu".to_string(), "1k".to_string()); // 1000
        let mut node = Node::new("node-2");
        node.status = Some(NodeStatus {
            capacity: None,
            allocatable: Some(alloc),
            conditions: None,
            addresses: None,
            node_info: None,
            images: None,
            volumes_in_use: None,
            volumes_attached: None,
            daemon_endpoints: None,
            config: None,
            features: None,
            runtime_handlers: None,
            declared_features: None,
        });

        let fakecpu_pod = |name: &str, req: &str, priority: i32| {
            let mut requests = HashMap::new();
            requests.insert("example.com/fakecpu".to_string(), req.to_string());
            let mut container = make_container("0", "0");
            container.resources = Some(rusternetes_common::types::ResourceRequirements {
                requests: Some(requests),
                limits: None,
                claims: None,
            });
            let mut pod = Pod::new(
                name,
                rusternetes_common::resources::PodSpec {
                    containers: vec![container],
                    priority: Some(priority),
                    ..Default::default()
                },
            );
            pod.metadata = pod.metadata.with_namespace("default");
            pod
        };

        // A high-priority preemptor (900) nominated to node-2 but not yet bound.
        let mut nominee = fakecpu_pod("preemptor", "900", 100);
        nominee.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Pending),
            nominated_node_name: Some("node-2".to_string()),
            ..Default::default()
        });

        // Low-priority candidate (priority 1) requesting 200.
        let candidate = fakecpu_pod("rs-pod1", "200", 1);

        // Without the nominee, 200 of 1000 fits.
        assert!(
            calculate_resource_score_with_pods(&node, &candidate, &[]) > 0,
            "200 of 1000 must fit when nothing is reserved"
        );

        // With the higher-priority nominee reserving 900, only 100 is free → 200 must NOT fit.
        assert_eq!(
            calculate_resource_score_with_pods(&node, &candidate, std::slice::from_ref(&nominee)),
            0,
            "candidate must not schedule into space reserved for a higher-priority nominee"
        );

        // The nominee itself is not reserved against itself — it still fits its nominated node.
        assert!(
            calculate_resource_score_with_pods(&node, &nominee, std::slice::from_ref(&nominee)) > 0,
            "the nominee must still fit its own nominated node"
        );
    }

    #[test]
    fn test_preemption_policy_never_should_not_preempt() {
        // Node with 2 CPUs
        let node = make_node("node-1", "2", "4Gi");

        // Existing low-priority pod using 1 CPU on node-1
        let existing = make_scheduled_pod("low-pri-pod", 100, "1", "1Gi", "node-1");

        // Incoming high-priority pod with preemptionPolicy=Never
        let incoming = make_incoming_pod("high-pri-pod", 1000, "2", "2Gi", Some("Never"));

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[existing]);

        assert!(
            !can_preempt,
            "Pod with preemptionPolicy=Never should not preempt"
        );
        assert!(
            pods_to_evict.is_empty(),
            "No pods should be evicted when preemptionPolicy=Never"
        );
    }

    #[test]
    fn test_preemption_blocked_for_system_critical_pod() {
        // Node with 2 CPUs
        let node = make_node("node-1", "2", "4Gi");

        // Existing system-critical pod (priority 2000000000) using 1 CPU
        let system_pod = make_scheduled_pod("system-critical", 2_000_000_000, "1", "1Gi", "node-1");

        // Incoming pod with priority 1000000000 — lower than the system-critical pod
        let incoming = make_incoming_pod("wants-resources", 1_000_000_000, "2", "2Gi", None);

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[system_pod]);

        // The incoming pod has lower priority than the system-critical pod,
        // so it should NOT be able to preempt it
        assert!(
            !can_preempt || pods_to_evict.is_empty(),
            "System-critical pod (priority >= 2000000000) should not be preempted by lower-priority pod"
        );
    }

    #[test]
    fn test_preemption_works_normally_for_lower_priority_pods() {
        // Sanity check: normal preemption still works
        let node = make_node("node-1", "2000m", "4Gi");

        // Existing low-priority pod using 1500m CPU
        let existing = make_scheduled_pod("low-pri", 100, "1500m", "1Gi", "node-1");

        // Incoming high-priority pod needs 1500m CPU (won't fit without eviction)
        let incoming = make_incoming_pod("high-pri", 1000, "1500m", "1Gi", None);

        let (can_preempt, pods_to_evict) = check_preemption(&node, &incoming, &[existing]);

        assert!(can_preempt, "Normal preemption should work");
        assert!(
            pods_to_evict.contains(&"low-pri".to_string()),
            "Low-priority pod should be evicted"
        );
    }

    // ---- HostPort conflict detection tests ----

    use rusternetes_common::resources::ContainerPort;

    /// Helper: create a container with a hostPort binding
    fn make_container_with_host_port(
        host_port: u16,
        protocol: &str,
        host_ip: &str,
    ) -> rusternetes_common::resources::Container {
        rusternetes_common::resources::Container {
            name: "main".to_string(),
            image: "busybox".to_string(),
            command: None,
            args: None,
            working_dir: None,
            ports: Some(vec![ContainerPort {
                container_port: 80,
                name: None,
                protocol: protocol.to_string(),
                host_port: Some(host_port),
                host_ip: if host_ip.is_empty() {
                    None
                } else {
                    Some(host_ip.to_string())
                },
            }]),
            env: None,
            env_from: None,
            resources: None,
            volume_mounts: None,
            volume_devices: None,
            image_pull_policy: None,
            liveness_probe: None,
            readiness_probe: None,
            startup_probe: None,
            security_context: None,
            restart_policy: None,
            resize_policy: None,
            lifecycle: None,
            termination_message_path: None,
            termination_message_policy: None,
            stdin: None,
            stdin_once: None,
            tty: None,
            ..Default::default()
        }
    }

    /// Helper: create a pod with a hostPort, scheduled on a node
    fn make_host_port_pod(
        name: &str,
        host_port: u16,
        protocol: &str,
        host_ip: &str,
        node_name: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container_with_host_port(host_port, protocol, host_ip)],
            node_name: Some(node_name.to_string()),
            ..Default::default()
        };
        let mut pod = Pod::new(name, spec);
        pod.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Running),
            ..Default::default()
        });
        pod
    }

    /// Helper: create an unscheduled pod with a hostPort (incoming pod)
    fn make_incoming_host_port_pod(
        name: &str,
        host_port: u16,
        protocol: &str,
        host_ip: &str,
    ) -> Pod {
        let spec = rusternetes_common::resources::PodSpec {
            containers: vec![make_container_with_host_port(host_port, protocol, host_ip)],
            ..Default::default()
        };
        Pod::new(name, spec)
    }

    #[test]
    fn test_host_port_no_conflict_when_no_host_ports() {
        let node = make_node("node-1", "2", "4Gi");
        let incoming = make_incoming_pod("pod-a", 0, "100m", "128Mi", None);
        assert!(
            check_host_port_conflicts(&node, &incoming, &[]),
            "Pod without hostPort should have no conflicts"
        );
    }

    #[test]
    fn test_host_port_conflict_same_port_same_protocol_same_ip() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort, same protocol, same (wildcard) hostIP should conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_port() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 9090, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Different hostPort should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_protocol() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "UDP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort but different protocol should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_different_host_ip() {
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("existing", 8080, "TCP", "10.0.0.1", "node-1");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.2");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort and protocol but different specific hostIPs should not conflict"
        );
    }

    #[test]
    fn test_host_port_conflict_wildcard_vs_specific_ip() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod binds to 0.0.0.0 (all interfaces)
        let existing = make_host_port_pod("existing", 8080, "TCP", "0.0.0.0", "node-1");
        // Incoming pod binds to a specific IP
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.1");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Wildcard hostIP (0.0.0.0) should conflict with any specific IP"
        );
    }

    #[test]
    fn test_host_port_conflict_empty_vs_specific_ip() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod with empty hostIP (means all interfaces)
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        // Incoming pod binds to a specific IP
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "10.0.0.1");

        assert!(
            !check_host_port_conflicts(&node, &incoming, &[existing]),
            "Empty hostIP (wildcard) should conflict with any specific IP"
        );
    }

    #[test]
    fn test_host_port_no_conflict_on_different_node() {
        let node = make_node("node-1", "2", "4Gi");
        // Existing pod on node-2 (different node)
        let existing = make_host_port_pod("existing", 8080, "TCP", "", "node-2");
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Pods on different nodes should not conflict"
        );
    }

    #[test]
    fn test_host_port_no_conflict_terminated_pod() {
        let node = make_node("node-1", "2", "4Gi");
        let mut existing = make_host_port_pod("existing", 8080, "TCP", "", "node-1");
        // Mark existing pod as Succeeded (terminated)
        existing.status = Some(rusternetes_common::resources::PodStatus {
            phase: Some(rusternetes_common::types::Phase::Succeeded),
            ..Default::default()
        });
        let incoming = make_incoming_host_port_pod("incoming", 8080, "TCP", "");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Terminated pods should not cause conflicts"
        );
    }

    #[test]
    fn test_host_port_allows_same_port_different_ip_and_protocol() {
        // This is the exact scenario from the conformance test:
        // Two pods with same hostPort but different hostIP and protocol should coexist
        let node = make_node("node-1", "2", "4Gi");
        let existing = make_host_port_pod("pod-tcp", 8080, "TCP", "10.0.0.1", "node-1");
        let incoming = make_incoming_host_port_pod("pod-udp", 8080, "UDP", "10.0.0.2");

        assert!(
            check_host_port_conflicts(&node, &incoming, &[existing]),
            "Same hostPort with different hostIP AND different protocol should not conflict"
        );
    }

    #[test]
    fn test_host_ips_overlap() {
        // Wildcard cases
        assert!(host_ips_overlap("", "10.0.0.1"));
        assert!(host_ips_overlap("10.0.0.1", ""));
        assert!(host_ips_overlap("0.0.0.0", "10.0.0.1"));
        assert!(host_ips_overlap("10.0.0.1", "0.0.0.0"));
        assert!(host_ips_overlap("::", "10.0.0.1"));
        assert!(host_ips_overlap("", ""));
        assert!(host_ips_overlap("0.0.0.0", "0.0.0.0"));

        // Specific IPs
        assert!(host_ips_overlap("10.0.0.1", "10.0.0.1"));
        assert!(!host_ips_overlap("10.0.0.1", "10.0.0.2"));
        assert!(!host_ips_overlap("192.168.1.1", "10.0.0.1"));
    }
    /// Mark a pod as terminating *by preemption*, the way our eviction path does:
    /// deletionTimestamp plus DisruptionTarget=True/PreemptionByScheduler.
    fn mark_preempted(pod: &mut Pod) {
        pod.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let status = pod.status.get_or_insert_with(Default::default);
        status.conditions.get_or_insert_with(Vec::new).push(
            rusternetes_common::resources::PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: Some(chrono::Utc::now()),
                reason: Some("PreemptionByScheduler".to_string()),
                message: Some("Preempted by a higher-priority pod".to_string()),
                observed_generation: None,
            },
        );
    }

    /// #1130: a pod that already preempted must not preempt again while its
    /// victim is still terminating on the node it was nominated to.
    ///
    /// Without this gate the scheduler retried, found the preemptor still did
    /// not fit (the victim's resources are accounted for until it really goes
    /// away), and preempted a second victim on another node — which is how
    /// `pod1-1-sched-preemption-medium-priority` was deleted and
    /// `[sig-scheduling] SchedulerPreemption [Serial]` failed at
    /// test/e2e/scheduling/preemption.go:206.
    #[test]
    fn a_pod_with_a_terminating_victim_on_its_nominated_node_is_not_eligible() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut victim = make_scheduled_pod("victim", 1, "100m", "128Mi", "node-1");
        mark_preempted(&mut victim);

        assert!(
            !pod_eligible_to_preempt_others(&preemptor, &[victim]),
            "must wait for the room it already made"
        );
    }

    /// Once the victim is gone, the pod may preempt again (it will normally just
    /// schedule instead).
    #[test]
    fn a_pod_whose_victim_has_gone_is_eligible_again() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        // Only a healthy, higher-priority neighbour remains on the node.
        let neighbour = make_scheduled_pod("neighbour", 2000, "100m", "128Mi", "node-1");

        assert!(pod_eligible_to_preempt_others(&preemptor, &[neighbour]));
    }

    /// A terminating pod on a DIFFERENT node must not block preemption — the
    /// gate is about the node this pod was nominated to.
    #[test]
    fn a_terminating_pod_on_another_node_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut elsewhere = make_scheduled_pod("elsewhere", 1, "100m", "128Mi", "node-2");
        mark_preempted(&mut elsewhere);

        assert!(pod_eligible_to_preempt_others(&preemptor, &[elsewhere]));
    }

    /// A terminating pod of EQUAL or higher priority is not a victim of this
    /// preemptor, so it must not gate it either.
    #[test]
    fn a_terminating_higher_priority_pod_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut other = make_scheduled_pod("other", 5000, "100m", "128Mi", "node-1");
        mark_preempted(&mut other);

        assert!(pod_eligible_to_preempt_others(&preemptor, &[other]));
    }

    /// Regression: a pod terminating for an ordinary reason must NOT count as
    /// "this preemptor already made room".
    ///
    /// The first version of this gate tested only `deletionTimestamp`, so any
    /// terminating lower-priority pod on the nominated node — a finished Job's
    /// pod, the previous spec's cleanup — blocked preemption forever and
    /// `[sig-scheduling] SchedulerPreemption` failed the other way round:
    /// `expected pod to be preempted, instead got pod pod0-0-…-low-priority`.
    /// Upstream requires DisruptionTarget=True with reason
    /// PreemptionByScheduler (podTerminatingByPreemption).
    #[test]
    fn a_pod_terminating_for_an_unrelated_reason_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        // Terminating, but not by preemption: no DisruptionTarget condition.
        let mut ordinary = make_scheduled_pod("ordinary", 1, "100m", "128Mi", "node-1");
        ordinary.metadata.deletion_timestamp = Some(chrono::Utc::now());

        assert!(
            pod_eligible_to_preempt_others(&preemptor, &[ordinary]),
            "an ordinary termination must not be mistaken for our own victim"
        );
    }

    /// A DisruptionTarget condition from a *different* disruptor (eviction API,
    /// node drain) also must not gate preemption.
    #[test]
    fn a_pod_disrupted_by_something_else_does_not_block_preemption() {
        let mut preemptor = make_incoming_pod("preemptor", 1000, "100m", "128Mi", None);
        preemptor.status = Some(rusternetes_common::resources::PodStatus {
            nominated_node_name: Some("node-1".to_string()),
            ..Default::default()
        });

        let mut drained = make_scheduled_pod("drained", 1, "100m", "128Mi", "node-1");
        drained.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let status = drained.status.get_or_insert_with(Default::default);
        status.conditions.get_or_insert_with(Vec::new).push(
            rusternetes_common::resources::PodCondition {
                condition_type: "DisruptionTarget".to_string(),
                status: "True".to_string(),
                last_probe_time: None,
                last_transition_time: Some(chrono::Utc::now()),
                reason: Some("EvictionByEvictionAPI".to_string()),
                message: None,
                observed_generation: None,
            },
        );

        assert!(pod_eligible_to_preempt_others(&preemptor, &[drained]));
    }

    /// A pod that has never preempted (no nominatedNodeName) is always eligible.
    #[test]
    fn a_pod_without_a_nominated_node_is_eligible() {
        let preemptor = make_incoming_pod("fresh", 1000, "100m", "128Mi", None);
        assert!(pod_eligible_to_preempt_others(&preemptor, &[]));
    }

    // -----------------------------------------------------------------------
    // Victim-node selection (the residual half of #1130).
    //
    // try_preempt used to return the FIRST feasible node, so with a
    // low-priority victim on node-1 and a medium-priority one on node-2 the
    // outcome depended on node iteration order. Observed live, same binaries,
    // ~1 h apart:
    //
    //   Preempting 1 pod(s) on node node-1 ... Evicted pod0-0-…-low-priority
    //   Preempting 1 pod(s) on node node-2 ... Evicted pod1-1-…-medium-priority
    //
    // The second reads as a flake and fails
    // `SchedulerPreemption validates basic preemption works`, which asserts the
    // LOWEST-priority pod is the one preempted.
    // -----------------------------------------------------------------------

    fn candidate(node: &str, victims: &[&str], pdb_violations: i64) -> PreemptionCandidate {
        PreemptionCandidate {
            node_name: node.to_string(),
            victims: victims.iter().map(|v| v.to_string()).collect(),
            num_pdb_violations: pdb_violations,
        }
    }

    #[test]
    fn test_pick_node_prefers_lowest_priority_victim() {
        let all_pods = vec![
            make_scheduled_pod("pod0-0-low", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("pod1-1-medium", 500, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-1", &["pod0-0-low"], 0),
            candidate("node-2", &["pod1-1-medium"], 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &all_pods).as_deref(),
            Some("node-1"),
            "must preempt the low-priority victim, not the medium-priority one"
        );
    }

    #[test]
    fn test_pick_node_is_independent_of_candidate_order() {
        // The live bug was order dependence, so the reversed list must agree.
        let all_pods = vec![
            make_scheduled_pod("pod0-0-low", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("pod1-1-medium", 500, "100m", "64Mi", "node-2"),
        ];
        let reversed = vec![
            candidate("node-2", &["pod1-1-medium"], 0),
            candidate("node-1", &["pod0-0-low"], 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&reversed, &all_pods).as_deref(),
            Some("node-1"),
        );
    }

    #[test]
    fn test_pick_node_pdb_violations_outrank_priority() {
        // Upstream's first criterion. node-1 costs a PDB violation, so node-2
        // wins even though its victim has the higher priority.
        let all_pods = vec![
            make_scheduled_pod("low-but-guarded", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("medium-free", 500, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-1", &["low-but-guarded"], 1),
            candidate("node-2", &["medium-free"], 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &all_pods).as_deref(),
            Some("node-2"),
        );
    }

    #[test]
    fn test_pick_node_breaks_priority_tie_on_summed_priorities() {
        // Same highest victim priority on both nodes; node-2 costs less in total.
        let all_pods = vec![
            make_scheduled_pod("a-top", 500, "100m", "64Mi", "node-1"),
            make_scheduled_pod("a-extra", 400, "100m", "64Mi", "node-1"),
            make_scheduled_pod("b-top", 500, "100m", "64Mi", "node-2"),
            make_scheduled_pod("b-extra", 100, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-1", &["a-top", "a-extra"], 0),
            candidate("node-2", &["b-top", "b-extra"], 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &all_pods).as_deref(),
            Some("node-2"),
        );
    }

    #[test]
    fn test_pick_node_breaks_remaining_tie_on_victim_count() {
        // Identical priorities and sums per victim, so fewest victims wins.
        let all_pods = vec![
            make_scheduled_pod("a1", 100, "100m", "64Mi", "node-1"),
            make_scheduled_pod("b1", 100, "100m", "64Mi", "node-2"),
            make_scheduled_pod("b2", 100, "100m", "64Mi", "node-2"),
        ];
        let candidates = vec![
            candidate("node-2", &["b1", "b2"], 0),
            candidate("node-1", &["a1"], 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &all_pods).as_deref(),
            Some("node-1"),
        );
    }

    #[test]
    fn test_pick_node_prefers_latest_started_victim_on_full_tie() {
        // Everything else equal: the victim that started most recently is the
        // cheaper one to kill (upstream latestStartTimeScoreFunc).
        let mut older = make_scheduled_pod("older", 100, "100m", "64Mi", "node-1");
        let mut newer = make_scheduled_pod("newer", 100, "100m", "64Mi", "node-2");
        let base = chrono::Utc::now();
        older.status.as_mut().unwrap().start_time = Some(base - chrono::Duration::hours(2));
        newer.status.as_mut().unwrap().start_time = Some(base);
        let all_pods = vec![older, newer];
        let candidates = vec![
            candidate("node-1", &["older"], 0),
            candidate("node-2", &["newer"], 0),
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &all_pods).as_deref(),
            Some("node-2"),
        );
    }

    #[test]
    fn test_pick_node_returns_none_without_candidates() {
        assert_eq!(pick_one_node_for_preemption(&[], &[]), None);
    }

    #[test]
    fn test_pick_node_single_candidate_is_chosen() {
        let all_pods = vec![make_scheduled_pod("only", 100, "100m", "64Mi", "node-1")];
        let candidates = vec![candidate("node-1", &["only"], 7)];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &all_pods).as_deref(),
            Some("node-1"),
            "a lone candidate is used even when it violates a PDB — upstream has no better option either"
        );
    }

    // -----------------------------------------------------------------------
    // PDB-aware preemption (#1797). try_preempt called the PDB-UNAWARE
    // check_preemption, so budget-protected pods were evicted as freely as
    // unprotected ones and upstream's first victim-node criterion
    // (minNumPDBViolatingScoreFunc) had nothing to score.
    // -----------------------------------------------------------------------

    fn make_pdb_min_available(
        name: &str,
        ns: &str,
        app: &str,
        min_available: i32,
    ) -> PodDisruptionBudget {
        let mut selector_labels = HashMap::new();
        selector_labels.insert("app".to_string(), app.to_string());
        PodDisruptionBudget::new(
            name,
            ns,
            rusternetes_common::resources::PodDisruptionBudgetSpec {
                min_available: Some(IntOrString::Int(min_available)),
                max_unavailable: None,
                selector: rusternetes_common::types::LabelSelector {
                    match_labels: Some(selector_labels),
                    match_expressions: None,
                },
                unhealthy_pod_eviction_policy: None,
            },
        )
    }

    fn labelled_pod(name: &str, priority: i32, node: &str, app: &str) -> Pod {
        let mut pod = make_scheduled_pod(name, priority, "100m", "64Mi", node);
        pod.metadata.namespace = Some("default".to_string());
        let mut labels = HashMap::new();
        labels.insert("app".to_string(), app.to_string());
        pod.metadata.labels = Some(labels);
        pod
    }

    #[test]
    fn test_count_pdb_violating_victims_zero_without_budgets() {
        let pods = vec![labelled_pod("a", 100, "node-1", "web")];
        assert_eq!(
            count_pdb_violating_victims(&["a".to_string()], &pods, &[]),
            0,
            "no budgets means nothing can violate one"
        );
    }

    #[test]
    fn test_count_pdb_violating_victims_counts_covered_pod() {
        // One replica, minAvailable=1: evicting it breaks the budget.
        let pods = vec![labelled_pod("only-web", 100, "node-1", "web")];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        assert_eq!(
            count_pdb_violating_victims(&["only-web".to_string()], &pods, &pdbs),
            1,
        );
    }

    #[test]
    fn test_count_pdb_violating_victims_ignores_uncovered_pod() {
        // The budget selects app=web; the victim is app=batch.
        let pods = vec![
            labelled_pod("web-1", 100, "node-1", "web"),
            labelled_pod("web-2", 100, "node-1", "web"),
            labelled_pod("batch-1", 100, "node-1", "batch"),
        ];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        assert_eq!(
            count_pdb_violating_victims(&["batch-1".to_string()], &pods, &pdbs),
            0,
        );
    }

    #[test]
    fn test_count_pdb_violating_victims_ignores_unknown_victim_name() {
        // A name matching no live pod cannot be shown to violate anything;
        // counting it would steer node choice on no evidence.
        let pods = vec![labelled_pod("web-1", 100, "node-1", "web")];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        assert_eq!(
            count_pdb_violating_victims(&["ghost".to_string()], &pods, &pdbs),
            0,
        );
    }

    #[test]
    fn test_count_pdb_violating_victims_sums_across_victims() {
        let pods = vec![
            labelled_pod("web-1", 100, "node-1", "web"),
            labelled_pod("api-1", 100, "node-1", "api"),
        ];
        let pdbs = vec![
            make_pdb_min_available("web-pdb", "default", "web", 1),
            make_pdb_min_available("api-pdb", "default", "api", 1),
        ];
        assert_eq!(
            count_pdb_violating_victims(&["web-1".to_string(), "api-1".to_string()], &pods, &pdbs),
            2,
        );
    }

    #[test]
    fn test_pdb_violation_count_steers_node_choice() {
        // The whole point of the count: node-1's victim is cheaper by priority
        // but protected, so the score chain must prefer node-2 even though its
        // victim has the higher priority.
        let pods = vec![
            labelled_pod("guarded-low", 100, "node-1", "web"),
            labelled_pod("free-medium", 500, "node-2", "batch"),
        ];
        let pdbs = vec![make_pdb_min_available("web-pdb", "default", "web", 1)];
        let candidates = vec![
            PreemptionCandidate {
                node_name: "node-1".to_string(),
                victims: vec!["guarded-low".to_string()],
                num_pdb_violations: count_pdb_violating_victims(
                    &["guarded-low".to_string()],
                    &pods,
                    &pdbs,
                ),
            },
            PreemptionCandidate {
                node_name: "node-2".to_string(),
                victims: vec!["free-medium".to_string()],
                num_pdb_violations: count_pdb_violating_victims(
                    &["free-medium".to_string()],
                    &pods,
                    &pdbs,
                ),
            },
        ];
        assert_eq!(
            pick_one_node_for_preemption(&candidates, &pods).as_deref(),
            Some("node-2"),
            "a PDB violation must outrank a lower victim priority"
        );
    }
}
