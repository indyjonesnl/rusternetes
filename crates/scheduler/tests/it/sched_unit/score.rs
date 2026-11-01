//! Unit tests moved from `src/score.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{Node, Pod, Toleration};
    pub use rusternetes_scheduler::advanced::{
        non_zero_request_amounts, parse_resource_quantity, pod_request_amounts,
        toleration_matches_taint,
    };
    pub use rusternetes_scheduler::scheduler_config::{
        NodeResourcesFitArgs, ResourceSpec, ScoringStrategy,
    };
    pub use rusternetes_scheduler::volume_binding::{
        build_broken_linear_function, FunctionShapePoint,
    };
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::score::*;
    use serde_json::json;

    fn node(name: &str, cpu: &str, mem: &str) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name},
            "status": {"allocatable": {"cpu": cpu, "memory": mem}}
        }))
        .unwrap()
    }

    fn pod(node_name: Option<&str>, cpu: &str, mem: &str) -> Pod {
        let mut containers = json!([{"name": "c", "image": "i"}]);
        if !cpu.is_empty() || !mem.is_empty() {
            containers[0]["resources"] = json!({"requests": {"cpu": cpu, "memory": mem}});
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "x", "namespace": "d"},
            "spec": {"nodeName": node_name, "containers": containers}
        }))
        .unwrap()
    }

    fn spec(name: &str, weight: i64) -> ResourceSpec {
        ResourceSpec {
            name: name.to_string(),
            weight,
        }
    }

    fn scorer(strategy: Strategy, use_requested: bool) -> ResourceAllocationScorer {
        ResourceAllocationScorer {
            strategy,
            resources: vec![spec("cpu", 1), spec("memory", 1)],
            use_requested,
        }
    }

    fn run(s: &ResourceAllocationScorer, n: &Node, existing: &[Pod], p: &Pod) -> i64 {
        let u = node_usages(existing);
        let usage = u.get(&n.metadata.name).cloned().unwrap_or_default();
        s.score(n, &usage, &s.pod_requests(p)).unwrap()
    }

    // least_allocated_test.go "nothing scheduled, resources requested,
    // differently sized nodes": node1 37, node2 50 (cpu in milli: "1000" cores
    // is not meaningful here; the test uses raw milli via Req).
    #[test]
    fn least_allocated_differently_sized_nodes() {
        // 4000m/10000 and 6000m/10000 nodes; requests 3000m / 5000.
        let s = scorer(Strategy::LeastAllocated, false);
        let n1 = node("node1", "4", "10000");
        let n2 = node("node2", "6", "10000");
        let p = pod(None, "3", "5000");
        assert_eq!(run(&s, &n1, &[], &p), 37);
        assert_eq!(run(&s, &n2, &[], &p), 50);
    }

    // "no resources requested, pods scheduled": existing pods with no requests
    // count at the non-zero defaults (100m / 200Mi) on huge nodes -> 99
    // (integer division: just under full capacity free).
    #[test]
    fn least_allocated_nonzero_defaults_on_huge_node() {
        let s = scorer(Strategy::LeastAllocated, false);
        let n = node("node1", "4000", "10000000000000");
        let existing = [pod(Some("node1"), "", ""), pod(Some("node1"), "", "")];
        assert_eq!(run(&s, &n, &existing, &pod(None, "", "")), 99);
    }

    // most_allocated_test.go: same shape, inverted: 4000m node with 3000m/5000
    // requested out of 10000 -> (75 + 50) / 2 = 62.
    #[test]
    fn most_allocated_differently_sized_nodes() {
        let s = scorer(Strategy::MostAllocated, false);
        let n1 = node("node1", "4", "10000");
        let n2 = node("node2", "6", "10000");
        let p = pod(None, "3", "5000");
        assert_eq!(run(&s, &n1, &[], &p), 62);
        assert_eq!(run(&s, &n2, &[], &p), 50);
    }

    // "resources requested equal node capacity" / requested > capacity.
    #[test]
    fn most_allocated_clamps_requested_to_capacity() {
        assert_eq!(most_requested_score(12, 10), 100);
        assert_eq!(least_requested_score(12, 10), 0);
        assert_eq!(least_requested_score(1, 0), 0);
    }

    // "resource weight" cases: weights bias the average.
    #[test]
    fn weights_bias_the_average() {
        let mut s = scorer(Strategy::LeastAllocated, true);
        s.resources = vec![spec("cpu", 3), spec("memory", 1)];
        // cpu 25, memory 75 -> (25*3 + 75*1)/4 = 37
        assert_eq!(s.scorer(&[3000, 2500], &[4000, 10000]), 37);
    }

    // requested_to_capacity_ratio_test.go: shape {0:0, 100:10} is bin packing
    // (score scaled x10), and BuildBrokenLinearFunction interpolates.
    #[test]
    fn requested_to_capacity_ratio_linear() {
        let shape = vec![
            FunctionShapePoint {
                utilization: 0,
                score: 0,
            },
            FunctionShapePoint {
                utilization: 100,
                score: 100,
            },
        ];
        let s = scorer(Strategy::RequestedToCapacityRatio(shape), false);
        assert_eq!(s.scorer(&[2000, 5000], &[4000, 10000]), 50);
        assert_eq!(s.scorer(&[4000, 10000], &[4000, 10000]), 100);
        // requested > capacity -> raw(maxUtilization)
        assert_eq!(s.scorer(&[8000, 10000], &[4000, 10000]), 100);
        // resource with allocatable 0 is skipped
        assert_eq!(s.scorer(&[0, 5000], &[0, 10000]), 50);
    }

    // balanced_allocation_test.go: two resources std = |f1-f2|/2.
    #[test]
    fn balanced_two_resources() {
        // fractions 0.5 / 0.5 -> 100; 0.75 / 0.25 -> 75; 1.0 / 0.0 -> 50
        assert_eq!(balanced_resource_scorer(&[2, 5], &[4, 10]), 100);
        assert_eq!(balanced_resource_scorer(&[3, 25], &[4, 100]), 75);
        assert_eq!(balanced_resource_scorer(&[4, 0], &[4, 10]), 50);
        // > 2 resources use the standard deviation
        let v = balanced_resource_scorer(&[1, 1, 4], &[4, 4, 4]);
        assert!(v < 100 && v > 0);
    }

    #[test]
    fn best_effort_pod_requests_all_zero_for_balanced() {
        let b = ResourceAllocationScorer::default_balanced();
        let reqs = b.pod_requests(&pod(None, "", ""));
        assert!(ResourceAllocationScorer::is_best_effort(&reqs));
        // Fit uses non-zero defaults instead.
        let f = ResourceAllocationScorer::default_fit();
        assert_eq!(
            f.pod_requests(&pod(None, "", "")),
            vec![100, 200 * 1024 * 1024]
        );
    }

    // fit_test / least_allocated_test "bypass extended resource if the pod does
    // not request" and "honor extended resource if the pod request".
    #[test]
    fn extended_resources_bypass_unless_requested() {
        let mut s = scorer(Strategy::LeastAllocated, false);
        s.resources = vec![
            spec("cpu", 1),
            spec("memory", 1),
            spec("example.com/gpu", 1),
        ];
        let mut n = node("n", "4", "10000");
        n.status
            .as_mut()
            .unwrap()
            .allocatable
            .as_mut()
            .unwrap()
            .insert("example.com/gpu".to_string(), "4".to_string());
        let p = pod(None, "3", "5000");
        // Not requested: (25 + 50) / 2 = 37.
        assert_eq!(run(&s, &n, &[], &p), 37);
        // Requested 2 of 4: (25 + 50 + 50) / 3 = 41.
        let mut with_gpu = p.clone();
        with_gpu.spec.as_mut().unwrap().containers[0]
            .resources
            .as_mut()
            .unwrap()
            .requests
            .as_mut()
            .unwrap()
            .insert("example.com/gpu".to_string(), "2".to_string());
        assert_eq!(run(&s, &n, &[], &with_gpu), 41);
    }

    #[test]
    fn empty_resources_is_an_error() {
        let mut s = scorer(Strategy::LeastAllocated, false);
        s.resources.clear();
        let n = node("n", "4", "10000");
        assert!(s.score(&n, &NodeUsage::default(), &[]).is_err());
    }

    // helper normalize_score_test.go.
    #[test]
    fn default_normalize() {
        let mut v = vec![10, 5, 0];
        default_normalize_score(100, false, &mut v);
        assert_eq!(v, vec![100, 50, 0]);
        let mut v = vec![10, 5, 0];
        default_normalize_score(100, true, &mut v);
        assert_eq!(v, vec![0, 50, 100]);
        let mut v = vec![0, 0];
        default_normalize_score(100, true, &mut v);
        assert_eq!(v, vec![100, 100]);
        let mut v = vec![0, 0];
        default_normalize_score(100, false, &mut v);
        assert_eq!(v, vec![0, 0]);
    }

    #[test]
    fn inter_pod_affinity_normalize() {
        let mut v = vec![-5, 0, 5];
        normalize_inter_pod_affinity(&mut v);
        assert_eq!(v, vec![0, 50, 100]);
        let mut v = vec![3, 3];
        normalize_inter_pod_affinity(&mut v);
        assert_eq!(v, vec![0, 0]);
    }

    // taint_toleration_test.go TestTaintTolerationScore.
    #[test]
    fn taint_toleration_counts_and_normalizes() {
        let mk = |name: &str, taints: serde_json::Value| -> Node {
            serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "Node",
                "metadata": {"name": name},
                "spec": {"taints": taints}
            }))
            .unwrap()
        };
        let a = mk(
            "a",
            json!([{"key": "foo", "value": "bar", "effect": "PreferNoSchedule"}]),
        );
        let b = mk(
            "b",
            json!([
                {"key": "foo", "value": "bar", "effect": "PreferNoSchedule"},
                {"key": "foo2", "value": "bar2", "effect": "PreferNoSchedule"}
            ]),
        );
        let c = mk("c", json!([]));
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "p"},
            "spec": {"containers": [], "tolerations": [
                {"key": "foo", "operator": "Equal", "value": "bar", "effect": "PreferNoSchedule"}
            ]}
        }))
        .unwrap();
        let tols = tolerations_prefer_no_schedule(&pod);
        let mut scores: Vec<i64> = [&a, &b, &c]
            .iter()
            .map(|n| count_intolerable_taints_prefer_no_schedule(n, &tols))
            .collect();
        assert_eq!(scores, vec![0, 1, 0]);
        default_normalize_score(MAX_NODE_SCORE, true, &mut scores);
        assert_eq!(scores, vec![100, 0, 100]);
    }

    // image_locality_test.go calculatePriority / sumImageScores.
    #[test]
    fn image_locality_priority() {
        assert_eq!(calculate_priority(0, 1), 0);
        assert_eq!(calculate_priority(MAX_CONTAINER_THRESHOLD, 1), 100);
        assert_eq!(calculate_priority(2 * MAX_CONTAINER_THRESHOLD, 1), 100);
        // 1 container: (500MB-23MB)*100/(1000MB-23MB) = 48
        assert_eq!(calculate_priority(500 * MB, 1), 48);
        assert_eq!(normalized_image_name("busybox"), "busybox:latest");
        assert_eq!(normalized_image_name("r:5000/app"), "r:5000/app:latest");
        assert_eq!(normalized_image_name("r:5000/app:v1"), "r:5000/app:v1");
    }

    fn select_host(totals: &[i64]) -> Option<usize> {
        SortedNodeScores::new(vec![String::new(); totals.len()], totals.to_vec()).pop_index()
    }

    // schedule_one.go newSortedNodeScores + Pop: highest total wins, a tie
    // follows container/heap order (heap.Init does not reorder equal items).
    // The remaining nodes after the first Pop come out best first, which is
    // what `OpportunisticBatch` hands to the following pods.
    #[test]
    fn sorted_node_scores_pop_best_first() {
        let names: Vec<String> = ["a", "b", "c", "d"].iter().map(|n| n.to_string()).collect();
        let mut s = SortedNodeScores::new(names, vec![10, 40, 30, 20]);
        assert_eq!(s.len(), 4);
        let order: Vec<String> = std::iter::from_fn(|| s.pop()).collect();
        assert_eq!(order, ["b", "c", "d", "a"]);
        assert!(s.is_empty());
        assert_eq!(s.pop(), None);
    }

    #[test]
    fn select_host_picks_highest_and_first_of_ties() {
        assert_eq!(select_host(&[]), None);
        assert_eq!(select_host(&[7]), Some(0));
        assert_eq!(select_host(&[1, 9, 3]), Some(1));
        assert_eq!(select_host(&[5, 5, 5]), Some(0));
        assert_eq!(select_host(&[3, 8, 8, 1]), Some(1));
    }

    #[test]
    fn total_scores_weighted_sum_and_range_check() {
        let p = vec![
            PluginScores {
                name: "A",
                weight: 3,
                scores: vec![100, 0],
            },
            PluginScores {
                name: "B",
                weight: 1,
                scores: vec![10, 90],
            },
        ];
        assert_eq!(total_scores(&p, 2).unwrap(), vec![310, 90]);
        let bad = vec![PluginScores {
            name: "A",
            weight: 1,
            scores: vec![101],
        }];
        assert!(total_scores(&bad, 1)
            .unwrap_err()
            .contains("returns an invalid score 101"));
    }
}
