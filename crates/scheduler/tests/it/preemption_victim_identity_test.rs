//! #2477: preemption victims are identified by the pod object (namespace/name,
//! UID), never by name alone.
//!
//! Upstream `SelectVictimsOnNode`
//! (pkg/scheduler/framework/plugins/defaultpreemption/default_preemption.go:207)
//! returns `[]*v1.Pod`, and `DeletePod` (pkg/scheduler/util/utils.go:137-138)
//! deletes `cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, ...)`.
//! Two pods named `w` in different namespaces are two distinct victims.

use std::collections::HashMap;

use rusternetes_common::resources::{Container, Pod, PodSpec, PodStatus};
use rusternetes_common::types::{Phase, ResourceRequirements};
use rusternetes_scheduler::advanced::check_preemption;
use rusternetes_test_support::node_with_resources as make_node;

fn container(cpu: &str) -> Container {
    let mut requests = HashMap::new();
    requests.insert("cpu".to_string(), cpu.to_string());
    requests.insert("memory".to_string(), "64Mi".to_string());
    Container {
        name: "main".to_string(),
        image: "pause".to_string(),
        resources: Some(ResourceRequirements {
            requests: Some(requests),
            limits: None,
            claims: None,
        }),
        ..Default::default()
    }
}

fn pod(name: &str, ns: &str, priority: i32, cpu: &str, node: Option<&str>) -> Pod {
    let mut p = Pod::new(
        name,
        PodSpec {
            containers: vec![container(cpu)],
            priority: Some(priority),
            node_name: node.map(str::to_string),
            ..Default::default()
        },
    );
    p.metadata.namespace = Some(ns.to_string());
    p.status = Some(PodStatus {
        phase: Some(Phase::Running),
        ..Default::default()
    });
    p
}

/// Two same-named low-priority pods fill a 2-cpu node; a 1-cpu preemptor needs
/// exactly one of them gone. Name-keyed reprieve bookkeeping treated the two as
/// one pod, so neither could be reprieved and both were evicted.
#[test]
fn same_named_pods_in_different_namespaces_are_distinct_victims() {
    let node = make_node("node-1", "2", "4Gi");
    let a = pod("w", "ns-a", 1, "1", Some("node-1"));
    let b = pod("w", "ns-b", 1, "1", Some("node-1"));
    let preemptor = pod("hi", "default", 1000, "1", None);

    let (ok, victims) = check_preemption(&node, &preemptor, &[a, b]);
    assert!(ok);
    assert_eq!(
        victims.len(),
        1,
        "exactly one victim is needed: {victims:?}"
    );
}
