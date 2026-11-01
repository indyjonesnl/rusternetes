//! Cases of `SelectVictimsOnNode` (`defaultpreemption/default_preemption.go:207-307`),
//! the reprieve order of `TestDryRunPreemption`/`TestSelectBestCandidate`
//! (`default_preemption_test.go`) and `addGENominatedPods` (`framework.go`).
use rusternetes_common::resources::{Node, Pod, PodDisruptionBudget};
use rusternetes_scheduler::default_preemption::{nominated_pods_for_node, select_victims_on_node};
use rusternetes_scheduler::fwk::{Code, NoHandle};
use rusternetes_scheduler::profile::Profile;
use serde_json::{json, Value};

fn node(cpu: &str) -> Node {
    serde_json::from_value(json!({
        "metadata": {"name": "n"},
        "status": {"allocatable": {"cpu": cpu, "memory": "100Gi", "pods": "32"}}
    }))
    .unwrap()
}

fn pod(name: &str, node: &str, priority: i32, cpu: &str, extra_spec: Value) -> Pod {
    let mut v = json!({
        "metadata": {"name": name, "namespace": "default", "uid": name, "labels": {"app": name}},
        "spec": {
            "priority": priority,
            "containers": [{"name": "c", "resources": {"requests": {"cpu": cpu}}}]
        },
        "status": {"phase": "Running"}
    });
    if !node.is_empty() {
        v["spec"]["nodeName"] = json!(node);
    }
    for (k, val) in extra_spec.as_object().into_iter().flatten() {
        v["spec"][k] = val.clone();
    }
    serde_json::from_value(v).unwrap()
}

fn names(pods: &[Pod]) -> Vec<&str> {
    pods.iter().map(|p| p.metadata.name.as_str()).collect()
}

async fn select(
    preemptor: &Pod,
    n: &Node,
    existing: &[Pod],
    pdbs: &[PodDisruptionBudget],
) -> Result<(Vec<String>, i64), (Code, String)> {
    let profile = Profile::named("default-scheduler");
    let nodes = [n.clone()];
    select_victims_on_node(
        &profile.framework,
        &NoHandle,
        preemptor,
        n,
        existing,
        &nodes,
        pdbs,
    )
    .await
    .map(|v| {
        (
            names(&v.victims).iter().map(|s| s.to_string()).collect(),
            v.num_pdb_violations,
        )
    })
    .map_err(|s| (s.code(), s.message()))
}

#[tokio::test]
async fn no_lower_priority_pods_means_no_victims() {
    let n = node("4");
    let existing = vec![pod("same", "n", 10, "4", json!({}))];
    let e = select(&pod("p", "", 10, "1", json!({})), &n, &existing, &[])
        .await
        .unwrap_err();
    assert_eq!(e.0, Code::UnschedulableAndUnresolvable);
    assert_eq!(e.1, "No preemption victims found for incoming pod");
}

#[tokio::test]
async fn pod_that_fits_without_removing_anything_has_no_victims() {
    let n = node("4");
    let existing = vec![pod("low", "n", 1, "1", json!({}))];
    let (victims, violations) = select(&pod("p", "", 10, "1", json!({})), &n, &existing, &[])
        .await
        .unwrap();
    assert!(victims.is_empty());
    assert_eq!(violations, 0);
}

#[tokio::test]
async fn node_that_cannot_fit_after_removing_every_victim_is_rejected() {
    let n = node("4");
    let existing = vec![
        pod("low", "n", 1, "1", json!({})),
        pod("high", "n", 100, "3", json!({})),
    ];
    let e = select(&pod("p", "", 10, "2", json!({})), &n, &existing, &[])
        .await
        .unwrap_err();
    assert_eq!(e.0, Code::Unschedulable);
    assert_eq!(e.1, "Insufficient cpu");
}

#[tokio::test]
async fn more_important_victims_are_reprieved_first() {
    let n = node("4");
    let existing = vec![
        pod("p1", "n", 1, "2", json!({})),
        pod("p5", "n", 5, "2", json!({})),
    ];
    let (victims, _) = select(&pod("p", "", 10, "2", json!({})), &n, &existing, &[])
        .await
        .unwrap();
    assert_eq!(victims, vec!["p1"]);
}

#[tokio::test]
async fn victims_are_listed_from_most_to_least_important() {
    let n = node("4");
    let existing = vec![
        pod("p1", "n", 1, "2", json!({})),
        pod("p5", "n", 5, "2", json!({})),
    ];
    let (victims, _) = select(&pod("p", "", 10, "4", json!({})), &n, &existing, &[])
        .await
        .unwrap();
    assert_eq!(victims, vec!["p5", "p1"]);
}

#[tokio::test]
async fn terminal_pods_hold_no_room_and_are_no_victims() {
    let n = node("2");
    let mut done = pod("done", "n", 1, "2", json!({}));
    done.status.as_mut().unwrap().phase = Some(rusternetes_common::types::Phase::Succeeded);
    let e = select(&pod("p", "", 10, "2", json!({})), &n, &[done], &[])
        .await
        .unwrap_err();
    // Nothing occupies the node, so there is no victim to find.
    assert_eq!(e.1, "No preemption victims found for incoming pod");
}

#[tokio::test]
async fn a_host_port_conflict_is_resolved_by_the_ports_plugin() {
    // Resources alone fit; only the NodePorts Filter keeps the pod off.
    let n = node("100");
    let port = json!({"containers": [{"name": "c", "ports": [{"containerPort": 80, "hostPort": 80, "protocol": "TCP"}]}]});
    let existing = vec![pod("holder", "n", 1, "1", port.clone())];
    let (victims, _) = select(&pod("p", "", 10, "1", port), &n, &existing, &[])
        .await
        .unwrap();
    assert_eq!(victims, vec!["holder"]);
}

#[tokio::test]
async fn pods_blocking_through_an_unresolvable_filter_are_not_worth_evicting() {
    let n = node("100");
    let existing = vec![pod("low", "n", 1, "1", json!({}))];
    let e = select(
        &pod("p", "", 10, "1", json!({"nodeSelector": {"disk": "ssd"}})),
        &n,
        &existing,
        &[],
    )
    .await
    .unwrap_err();
    assert_eq!(e.0, Code::UnschedulableAndUnresolvable);
}

#[tokio::test]
async fn pdb_violating_victims_are_reprieved_before_the_others() {
    let n = node("2");
    let existing = vec![
        pod("guarded", "n", 1, "1", json!({})),
        pod("free", "n", 1, "1", json!({})),
    ];
    let pdb: PodDisruptionBudget = serde_json::from_value(json!({
        "metadata": {"name": "b", "namespace": "default"},
        "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": "guarded"}}},
        "status": {"currentHealthy": 1, "desiredHealthy": 1, "disruptionsAllowed": 0, "expectedPods": 1}
    }))
    .unwrap();
    let (victims, violations) = select(&pod("p", "", 10, "1", json!({})), &n, &existing, &[pdb])
        .await
        .unwrap();
    assert_eq!(victims, vec!["free"]);
    assert_eq!(violations, 0);
}

#[tokio::test]
async fn a_victim_that_must_go_despite_its_pdb_is_counted() {
    let n = node("1");
    let existing = vec![pod("guarded", "n", 1, "1", json!({}))];
    let pdb: PodDisruptionBudget = serde_json::from_value(json!({
        "metadata": {"name": "b", "namespace": "default"},
        "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": "guarded"}}},
        "status": {"currentHealthy": 1, "desiredHealthy": 1, "disruptionsAllowed": 0, "expectedPods": 1}
    }))
    .unwrap();
    let (victims, violations) = select(&pod("p", "", 10, "1", json!({})), &n, &existing, &[pdb])
        .await
        .unwrap();
    assert_eq!(victims, vec!["guarded"]);
    assert_eq!(violations, 1);
}

#[tokio::test]
async fn nominated_pods_count_against_the_node_during_the_dry_run() {
    // A nominated pod of greater priority holds the room the victim frees.
    let n = node("2");
    let existing = vec![pod("low", "n", 1, "1", json!({})), {
        let mut nom = pod("nominee", "", 50, "1", json!({}));
        nom.status.as_mut().unwrap().nominated_node_name = Some("n".to_string());
        nom
    }];
    let e = select(&pod("p", "", 10, "2", json!({})), &n, &existing, &[])
        .await
        .unwrap_err();
    assert_eq!(e.0, Code::Unschedulable);
}

#[test]
fn nominated_pods_are_unbound_alive_and_at_least_as_important() {
    let nominate = |mut p: Pod, node: &str| {
        p.status.as_mut().unwrap().nominated_node_name = Some(node.to_string());
        p
    };
    let mut terminating = nominate(pod("terminating", "", 50, "1", json!({})), "n");
    terminating.metadata.deletion_timestamp = Some(chrono::Utc::now());
    let all = vec![
        nominate(pod("equal", "", 10, "1", json!({})), "n"),
        nominate(pod("lower", "", 5, "1", json!({})), "n"),
        nominate(pod("elsewhere", "", 50, "1", json!({})), "m"),
        nominate(pod("bound", "n", 50, "1", json!({})), "n"),
        nominate(pod("self", "", 50, "1", json!({})), "n"),
        terminating,
    ];
    let preemptor = pod("self", "", 10, "1", json!({}));
    let got = nominated_pods_for_node(&preemptor, &all, "n");
    assert_eq!(
        got.iter()
            .map(|p| p.metadata.name.as_str())
            .collect::<Vec<_>>(),
        vec!["equal"]
    );
}

/// `filterPodsWithPDBViolation` (default_preemption.go:373-419), seen through
/// the violation count of the victims it classifies.
fn pdb(ns: &str, selector: Value, status: Value) -> PodDisruptionBudget {
    let mut v = json!({
        "metadata": {"name": "b", "namespace": ns},
        "spec": {"minAvailable": 1, "selector": selector},
    });
    if !status.is_null() {
        v["status"] = status;
    }
    serde_json::from_value(v).unwrap()
}

fn allowed(n: i32) -> Value {
    json!({"currentHealthy": 2, "desiredHealthy": 2, "disruptionsAllowed": n, "expectedPods": 2})
}

async fn violations(pdbs: &[PodDisruptionBudget], victims: Vec<Pod>) -> i64 {
    // The preemptor needs the whole node, so every pod is a victim.
    let n = node("2");
    let (_, violations) = select(&pod("p", "", 10, "2", json!({})), &n, &victims, pdbs)
        .await
        .unwrap();
    violations
}

fn app(name: &str) -> Value {
    json!({"matchLabels": {"app": name}})
}

#[tokio::test]
async fn each_victim_spends_the_budget_and_only_the_excess_violates() {
    let victims = vec![
        pod("a", "n", 1, "1", json!({})),
        pod("b", "n", 1, "1", json!({})),
    ];
    let both =
        json!({"matchExpressions": [{"key": "app", "operator": "In", "values": ["a", "b"]}]});
    assert_eq!(
        violations(&[pdb("default", both.clone(), allowed(2))], victims.clone()).await,
        0
    );
    assert_eq!(
        violations(&[pdb("default", both.clone(), allowed(1))], victims.clone()).await,
        1
    );
    assert_eq!(
        violations(&[pdb("default", both, allowed(0))], victims).await,
        2
    );
}

#[tokio::test]
async fn a_budget_without_status_allows_no_disruption() {
    let victims = vec![pod("a", "n", 1, "2", json!({}))];
    assert_eq!(
        violations(&[pdb("default", app("a"), Value::Null)], victims).await,
        1
    );
}

#[tokio::test]
async fn pods_already_in_disrupted_pods_do_not_violate() {
    let victims = vec![pod("a", "n", 1, "2", json!({}))];
    let status = json!({
        "currentHealthy": 1, "desiredHealthy": 1, "disruptionsAllowed": 0, "expectedPods": 1,
        "disruptedPods": {"a": "2026-01-01T00:00:00Z"}
    });
    assert_eq!(
        violations(&[pdb("default", app("a"), status)], victims).await,
        0
    );
}

#[tokio::test]
async fn budgets_that_select_nothing_or_elsewhere_never_violate() {
    let victims = vec![pod("a", "n", 1, "2", json!({}))];
    // A nil or empty selector matches nothing.
    assert_eq!(
        violations(&[pdb("default", Value::Null, allowed(0))], victims.clone()).await,
        0
    );
    assert_eq!(
        violations(&[pdb("default", json!({}), allowed(0))], victims.clone()).await,
        0
    );
    // An invalid selector matches nothing.
    let bad = json!({"matchExpressions": [{"key": "app", "operator": "Bogus"}]});
    assert_eq!(
        violations(&[pdb("default", bad, allowed(0))], victims.clone()).await,
        0
    );
    // Another namespace's budget does not apply.
    assert_eq!(
        violations(&[pdb("other", app("a"), allowed(0))], victims.clone()).await,
        0
    );
    // A pod without labels matches no budget.
    let mut unlabelled = victims;
    unlabelled[0].metadata.labels = None;
    assert_eq!(
        violations(&[pdb("default", app("a"), allowed(0))], unlabelled).await,
        0
    );
}
