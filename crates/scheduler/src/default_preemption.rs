//! DefaultPreemption's dry run on the Filter plugin objects.
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go`:
//! `SelectVictimsOnNode` (:207-307), `isPreemptionAllowed` (:349-352) and
//! `MoreImportantPod` (`pkg/scheduler/util/utils.go:88-95`).
//!
//! Upstream removes every lower-priority pod from the node, requires the
//! preemptor to pass `RunFilterPluginsWithNominatedPods`, then reprieves the
//! victims one by one (PDB-violating ones first, each group from the most
//! important down), keeping a pod only if the preemptor still fits with it back.
//!
//! Deviations:
//! - Upstream keeps one `CycleState` and patches it through
//!   `PreFilterExtensions.AddPod`/`RemovePod`. Those are not ported yet
//!   (`CycleState` is not clonable), so every fit check here runs PreFilter
//!   again on the snapshot without the removed pods, which yields the state the
//!   extensions would have produced.

use std::collections::HashSet;

use rusternetes_common::resources::{Node, Pod, PodDisruptionBudget};
use rusternetes_common::types::label_selector_as_selector;

use crate::advanced::pod_key;
use crate::framework_runtime::Framework;
use crate::fwk::{Code, CycleState, Handle, Status};

/// `corev1helpers.PodPriority`.
fn pod_priority(pod: &Pod) -> i32 {
    pod.spec.as_ref().and_then(|s| s.priority).unwrap_or(0)
}

/// `GetPodStartTime` (utils.go:75-81); a pod that has not started counts as
/// started now.
fn pod_start_time(pod: &Pod) -> chrono::DateTime<chrono::Utc> {
    pod.status
        .as_ref()
        .and_then(|s| s.start_time)
        .unwrap_or_else(chrono::Utc::now)
}

/// `MoreImportantPod`: higher priority first, then the earlier start time.
fn more_important(a: &Pod, b: &Pod) -> std::cmp::Ordering {
    pod_priority(b)
        .cmp(&pod_priority(a))
        .then_with(|| pod_start_time(a).cmp(&pod_start_time(b)))
}

/// `addGENominatedPods`' selection (framework.go:1196-1218): pods nominated to
/// `node_name` that are not yet bound, of greater or equal priority than `pod`.
pub fn nominated_pods_for_node<'a>(
    pod: &Pod,
    all_pods: &'a [Pod],
    node_name: &str,
) -> Vec<&'a Pod> {
    let priority = pod_priority(pod);
    all_pods
        .iter()
        .filter(|p| {
            !(p.metadata.name == pod.metadata.name
                && p.metadata.namespace == pod.metadata.namespace)
                && p.status
                    .as_ref()
                    .and_then(|s| s.nominated_node_name.as_deref())
                    == Some(node_name)
                && p.spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .is_none_or(str::is_empty)
                && pod_priority(p) >= priority
                && p.metadata.deletion_timestamp.is_none()
        })
        .collect()
}

/// `filterPodsWithPDBViolation` (default_preemption.go:373-419): the keys of the
/// `pods` whose eviction exceeds a PodDisruptionBudget, taking the pods in order
/// and spending each matching budget's `status.disruptionsAllowed` as it goes.
fn pods_with_pdb_violation(pods: &[&Pod], pdbs: &[PodDisruptionBudget]) -> HashSet<String> {
    let mut allowed: Vec<i32> = pdbs
        .iter()
        .map(|pdb| pdb.status.as_ref().map_or(0, |s| s.disruptions_allowed))
        .collect();
    let mut violating = HashSet::new();
    for pod in pods {
        // A pod with no labels matches no PDB.
        if pod.metadata.labels.as_ref().is_none_or(|l| l.is_empty()) {
            continue;
        }
        for (i, pdb) in pdbs.iter().enumerate() {
            if pdb.metadata.namespace != pod.metadata.namespace {
                continue;
            }
            // An invalid selector matches nothing, and so does a nil or empty one.
            let Ok(selector) = label_selector_as_selector(pdb.spec.selector.as_ref()) else {
                continue;
            };
            if selector.is_everything() || !selector.matches(pod.metadata.labels.as_ref()) {
                continue;
            }
            // A pod in DisruptedPods was already processed by the API server.
            let disrupted = pdb
                .status
                .as_ref()
                .and_then(|s| s.disrupted_pods.as_ref())
                .is_some_and(|d| d.contains_key(&pod.metadata.name));
            if disrupted {
                continue;
            }
            allowed[i] -= 1;
            if allowed[i] < 0 {
                violating.insert(pod_key(pod));
            }
        }
    }
    violating
}

/// What `SelectVictimsOnNode` returns on success.
pub struct NodeVictims {
    /// The pods to evict, most important first.
    pub victims: Vec<Pod>,
    /// `numViolatingVictim`.
    pub num_pdb_violations: i64,
}

/// `SelectVictimsOnNode`. `Err` carries the status that makes the node not a
/// candidate: UnschedulableAndUnresolvable when it has no victims, or the
/// Filter status of the preemptor with every victim removed.
pub async fn select_victims_on_node(
    fw: &Framework,
    handle: &dyn Handle,
    pod: &Pod,
    node: &Node,
    all_pods: &[Pod],
    all_nodes: &[Node],
    pdbs: &[PodDisruptionBudget],
) -> Result<NodeVictims, Status> {
    let node_name = node.metadata.name.as_str();
    let preemptor_priority = pod_priority(pod);
    // `nodeInfo.GetPods()` holds the pods that still occupy the node; terminal
    // pods are gone from it. `isPreemptionAllowed`: strictly lower priority.
    let mut potential: Vec<&Pod> = all_pods
        .iter()
        .filter(|p| {
            p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(node_name)
                && crate::in_tree::counts_against_node(p)
                && pod_priority(p) < preemptor_priority
        })
        .collect();
    if potential.is_empty() {
        return Err(Status::new(
            Code::UnschedulableAndUnresolvable,
            vec!["No preemption victims found for incoming pod".to_string()],
        ));
    }

    // `RunFilterPluginsWithNominatedPods` on the node as the removed pods
    // leave it (`removePod`/`addPod` plus the PreFilter extensions upstream).
    let fits = |removed: &HashSet<String>| {
        let view: Vec<Pod> = all_pods
            .iter()
            .filter(|p| !removed.contains(&pod_key(p)))
            .cloned()
            .collect();
        async move {
            let mut state = CycleState::new();
            let (_, status, _) = fw
                .run_pre_filter_plugins(&mut state, pod, all_nodes, &view, handle)
                .await;
            if !status.is_success() {
                return status;
            }
            let nominated = nominated_pods_for_node(pod, &view, node_name);
            fw.run_filter_plugins_with_nominated_pods(
                &state, pod, node, &view, all_nodes, nominated,
            )
            .await
        }
    };

    // First remove every eligible pod; without them the preemptor must fit.
    let mut removed: HashSet<String> = potential.iter().map(|p| pod_key(p)).collect();
    let status = fits(&removed).await;
    if !status.is_success() {
        return Err(status);
    }

    // Reprieve the most important pods first; PDB-violating ones before the rest.
    potential.sort_by(|a, b| more_important(a, b));
    let violating = pods_with_pdb_violation(&potential, pdbs);
    let (violating, non_violating): (Vec<&Pod>, Vec<&Pod>) = potential
        .iter()
        .copied()
        .partition(|p| violating.contains(&pod_key(p)));
    let mut victims: Vec<&Pod> = Vec::new();
    let mut num_violating = 0i64;
    for (group, counts_violation) in [(&violating, true), (&non_violating, false)] {
        for p in group {
            let key = pod_key(p);
            removed.remove(&key);
            let status = fits(&removed).await;
            // `fits := status.IsSuccess()`: any other status keeps the pod a victim.
            if !status.is_success() {
                removed.insert(key);
                victims.push(p);
                if counts_violation {
                    num_violating += 1;
                }
            }
        }
    }
    // Keep the victims sorted from most to least important after the two passes.
    if !violating.is_empty() && !non_violating.is_empty() {
        victims.sort_by(|a, b| more_important(a, b));
    }
    Ok(NodeVictims {
        victims: victims.into_iter().cloned().collect(),
        num_pdb_violations: num_violating,
    })
}
