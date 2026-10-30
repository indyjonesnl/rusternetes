//! Unit tests moved from `src/inter_pod_affinity_score.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::{PodAffinityTerm, WeightedPodAffinityTerm};
    pub use rusternetes_common::resources::{Namespace, Node, Pod};
    pub use rusternetes_common::types::{label_selector_as_selector, Phase, Selector};
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::inter_pod_affinity_score::*;
    use rusternetes_scheduler::score::{normalize_inter_pod_affinity, MAX_NODE_SCORE};
    use serde_json::{json, Value};

    fn ns(name: &str, team: &str) -> Namespace {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": name, "labels": {"team": team}}
        }))
        .unwrap()
    }

    /// scoring_test.go `namespaces`.
    fn namespaces() -> Vec<Namespace> {
        vec![
            ns("subteam1.team1", "team1"),
            ns("subteam2.team1", "team1"),
            ns("subteam1.team2", "team2"),
            ns("subteam2.team2", "team2"),
        ]
    }

    fn node(name: &str, labels: Value) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "labels": labels}
        }))
        .unwrap()
    }

    fn pod(node_name: &str, namespace: &str, labels: Value, affinity: Option<&Value>) -> Pod {
        let mut spec = json!({"containers": []});
        if !node_name.is_empty() {
            spec["nodeName"] = json!(node_name);
        }
        if let Some(a) = affinity {
            spec["affinity"] = a.clone();
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": namespace, "labels": labels},
            "spec": spec
        }))
        .unwrap()
    }

    fn sel(key: &str, op: &str, values: &[&str]) -> Value {
        json!({"key": key, "operator": op, "values": values})
    }

    fn weighted(weight: i32, exprs: Vec<Value>, topology_key: &str) -> Value {
        json!({
            "weight": weight,
            "podAffinityTerm": {
                "labelSelector": {"matchExpressions": exprs},
                "topologyKey": topology_key
            }
        })
    }

    fn pref_affinity(terms: Vec<Value>) -> Value {
        json!({"podAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": terms}})
    }

    fn pref_anti(terms: Vec<Value>) -> Value {
        json!({"podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": terms}})
    }

    fn s1() -> Value {
        json!({"security": "S1"})
    }
    fn s2() -> Value {
        json!({"security": "S2"})
    }

    struct Affinities {
        stay_with_s1_in_region: Value,
        stay_with_s2_in_region: Value,
        affinity3: Value,
        hard_affinity: Value,
        away_from_s1_in_az: Value,
        away_from_s2_in_az: Value,
        stay_with_s1_away_from_s2: Value,
        affinity_namespace_selector: Value,
        anti_affinity_namespace_selector: Value,
        invalid_affinity: Value,
        invalid_anti_affinity: Value,
    }

    fn affinities() -> Affinities {
        let ns_term = |anti: bool| {
            let t = json!({
                "weight": 5,
                "podAffinityTerm": {
                    "labelSelector": {"matchExpressions": [sel("security", "In", &["S1"])]},
                    "topologyKey": "region",
                    "namespaces": ["subteam2.team2"],
                    "namespaceSelector": {"matchExpressions": [sel("team", "In", &["team1"])]}
                }
            });
            if anti {
                pref_anti(vec![t])
            } else {
                pref_affinity(vec![t])
            }
        };
        Affinities {
            stay_with_s1_in_region: pref_affinity(vec![weighted(
                5,
                vec![sel("security", "In", &["S1"])],
                "region",
            )]),
            stay_with_s2_in_region: pref_affinity(vec![weighted(
                6,
                vec![sel("security", "In", &["S2"])],
                "region",
            )]),
            affinity3: pref_affinity(vec![
                weighted(
                    8,
                    vec![
                        sel("security", "NotIn", &["S1"]),
                        sel("security", "In", &["S2"]),
                    ],
                    "region",
                ),
                weighted(
                    2,
                    vec![
                        sel("security", "Exists", &[]),
                        sel("wrongkey", "DoesNotExist", &[]),
                    ],
                    "region",
                ),
            ]),
            hard_affinity: json!({"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
                {"labelSelector": {"matchExpressions": [sel("security", "In", &["S1", "value2"])]},
                 "topologyKey": "region"},
                {"labelSelector": {"matchExpressions": [
                    sel("security", "Exists", &[]), sel("wrongkey", "DoesNotExist", &[])]},
                 "topologyKey": "region"},
            ]}}),
            away_from_s1_in_az: pref_anti(vec![weighted(
                5,
                vec![sel("security", "In", &["S1"])],
                "az",
            )]),
            away_from_s2_in_az: pref_anti(vec![weighted(
                5,
                vec![sel("security", "In", &["S2"])],
                "az",
            )]),
            stay_with_s1_away_from_s2: json!({
                "podAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [
                    weighted(8, vec![sel("security", "In", &["S1"])], "region")]},
                "podAntiAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": [
                    weighted(5, vec![sel("security", "In", &["S2"])], "az")]},
            }),
            affinity_namespace_selector: ns_term(false),
            anti_affinity_namespace_selector: ns_term(true),
            invalid_affinity: pref_affinity(vec![weighted(
                8,
                vec![sel("security", "In", &["{{.bad-value.}}"])],
                "region",
            )]),
            invalid_anti_affinity: pref_anti(vec![weighted(
                5,
                vec![sel("security", "In", &["{{.bad-value.}}"])],
                "az",
            )]),
        }
    }

    /// PreScore + Score for every node + NormalizeScore, as the test harness
    /// does. `None` is the Skip status.
    fn run(
        incoming: &Pod,
        pods: &[Pod],
        nodes: &[Node],
        args: Args,
    ) -> Result<Option<Vec<i64>>, String> {
        let nss = namespaces();
        let Some(state) = pre_score(incoming, nodes, pods, &nss, &args)? else {
            return Ok(None);
        };
        let mut scores: Vec<i64> = nodes.iter().map(|n| score(&state, n)).collect();
        normalize_inter_pod_affinity(&mut scores);
        Ok(Some(scores))
    }

    const MAX: i64 = MAX_NODE_SCORE;

    fn rg(v: &str) -> Value {
        json!({"region": v})
    }
    fn az(v: &str) -> Value {
        json!({"az": v})
    }

    #[test]
    fn affinity_nil_skips() {
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", rg("India")),
            node("node3", az("az1")),
        ];
        let p = pod("", "default", s1(), None);
        assert_eq!(run(&p, &[], &nodes, Args::default()), Ok(None));
    }

    #[test]
    fn preferred_affinity_cases() {
        let a = affinities();
        let d = "default";
        let nodes3 = || {
            vec![
                node("node1", rg("China")),
                node("node2", rg("India")),
                node("node3", az("az1")),
            ]
        };
        let args = Args::default();

        // topology key + matching pods score high.
        let p = pod("", d, s1(), Some(&a.stay_with_s1_in_region));
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, s1(), None),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![MAX, 0, 0])
        );

        // all nodes in the same domain share the score.
        let p = pod("", d, json!({}), Some(&a.stay_with_s1_in_region));
        let pods = [pod("node1", d, s1(), None)];
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", json!({"region": "China", "az": "az1"})),
            node("node3", rg("India")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, MAX, 0])
        );

        // more matches in a region.
        let p = pod("", d, s1(), Some(&a.stay_with_s2_in_region));
        let pods = [
            pod("node1", d, s2(), None),
            pod("node1", d, s2(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, s2(), None),
            pod("node4", d, s2(), None),
            pod("node5", d, s2(), None),
        ];
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", rg("India")),
            node("node3", rg("China")),
            node("node4", rg("China")),
            node("node5", rg("India")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, 0, MAX, MAX, 0])
        );

        // different label operators.
        let p = pod("", d, s1(), Some(&a.affinity3));
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, s1(), None),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![20, MAX, 0])
        );

        // symmetry: only the preferred terms of existing pods.
        let p = pod("", d, s2(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.stay_with_s1_in_region)),
            pod("node2", d, s2(), Some(&a.stay_with_s2_in_region)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![0, MAX, 0])
        );

        // symmetry with namespace selector.
        let p = pod("", "subteam1.team1", s1(), None);
        let pods = [
            pod("node1", "", s1(), Some(&a.affinity_namespace_selector)),
            pod("node2", "", s2(), Some(&a.stay_with_s2_in_region)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![MAX, 0, 0])
        );

        // anti-affinity symmetry with namespace selector.
        let pods = [
            pod("node1", "", s1(), Some(&a.anti_affinity_namespace_selector)),
            pod("node2", "", s2(), Some(&a.stay_with_s2_in_region)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![0, MAX, MAX])
        );

        // required terms of existing pods count by the hard weight.
        let p = pod("", d, s1(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.hard_affinity)),
            pod("node2", d, s2(), Some(&a.hard_affinity)),
        ];
        assert_eq!(
            run(&p, &pods, &nodes3(), args).unwrap(),
            Some(vec![MAX, MAX, 0])
        );
    }

    #[test]
    fn preferred_anti_affinity_cases() {
        let a = affinities();
        let d = "default";
        let args = Args::default();

        let p = pod("", d, s1(), Some(&a.away_from_s1_in_az));
        let pods = [pod("node1", d, s1(), None), pod("node2", d, s2(), None)];
        let nodes = vec![node("node1", az("az1")), node("node2", rg("China"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));

        let pods = [pod("node1", d, s1(), None), pod("node2", d, s1(), None)];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));

        let pods = [
            pod("node1", d, s1(), None),
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
        ];
        let nodes = vec![node("node1", az("az1")), node("node2", rg("India"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));

        // symmetry.
        let p = pod("", d, s2(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.away_from_s2_in_az)),
            pod("node2", d, s2(), Some(&a.away_from_s1_in_az)),
        ];
        let nodes = vec![node("node1", az("az1")), node("node2", az("az2"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, MAX]));
    }

    #[test]
    fn affinity_and_anti_affinity_cases() {
        let a = affinities();
        let d = "default";
        let args = Args::default();

        let p = pod("", d, s1(), Some(&a.stay_with_s1_away_from_s2));
        let pods = [pod("node1", d, s1(), None), pod("node2", d, s1(), None)];
        let nodes = vec![node("node1", rg("China")), node("node2", az("az1"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![MAX, 0]));

        // same labels as the existing pods (one RC/service).
        let pods = [
            pod("node1", d, s1(), None),
            pod("node1", d, s1(), None),
            pod("node2", d, s1(), None),
            pod("node3", d, s1(), None),
            pod("node3", d, s1(), None),
            pod("node4", d, s1(), None),
            pod("node5", d, s1(), None),
        ];
        let nodes = vec![
            node("node1", json!({"region": "China", "az": "az1"})),
            node("node2", rg("India")),
            node("node3", rg("China")),
            node("node4", rg("China")),
            node("node5", rg("India")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, 0, MAX, MAX, 0])
        );

        // affinity, anti-affinity and both symmetries together.
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, s2(), None),
            pod("node3", d, json!({}), Some(&a.stay_with_s1_away_from_s2)),
            pod("node4", d, json!({}), Some(&a.away_from_s1_in_az)),
        ];
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", az("az1")),
            node("node3", rg("India")),
            node("node4", az("az2")),
        ];
        assert_eq!(
            run(&p, &pods, &nodes, args).unwrap(),
            Some(vec![MAX, 0, MAX, 0])
        );

        // Avoid panic when partial nodes in a topology don't have pods with
        // affinity (kubernetes#82796).
        let p = pod("", d, s1(), None);
        let pods = [
            pod("node1", d, s1(), None),
            pod("node2", d, json!({}), Some(&a.stay_with_s1_away_from_s2)),
        ];
        let nodes = vec![node("node1", rg("China")), node("node2", rg("China"))];
        assert_eq!(run(&p, &pods, &nodes, args).unwrap(), Some(vec![0, 0]));
    }

    #[test]
    fn invalid_affinity_fails_pre_score() {
        let a = affinities();
        let nodes = vec![node("node1", rg("China")), node("node2", rg("China"))];
        for aff in [&a.invalid_affinity, &a.invalid_anti_affinity] {
            let p = pod("", "default", json!({}), Some(aff));
            let err = run(&p, &[], &nodes, Args::default()).unwrap_err();
            assert!(err.contains("Invalid value: \"{{.bad-value.}}\""), "{err}");
        }
    }

    #[test]
    fn namespace_selector_cases() {
        let a = affinities();
        let nodes = vec![node("node1", rg("China")), node("node2", rg("India"))];
        let args = Args::default();
        let incoming = |aff: &Value| pod("", "subteam1.team1", s1(), Some(aff));
        let pods_a = [
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam1.team2", s1(), None),
            pod("node2", "subteam2.team1", s1(), None),
        ];
        assert_eq!(
            run(
                &incoming(&a.affinity_namespace_selector),
                &pods_a,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![MAX, 0])
        );
        // The Namespaces field is unioned with the selector's namespaces.
        let pods_b = [
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam1.team1", s1(), None),
            pod("node1", "subteam2.team2", s1(), None),
            pod("node2", "subteam2.team1", s1(), None),
        ];
        assert_eq!(
            run(
                &incoming(&a.affinity_namespace_selector),
                &pods_b,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![MAX, 0])
        );
        assert_eq!(
            run(
                &incoming(&a.anti_affinity_namespace_selector),
                &pods_a,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![0, MAX])
        );
        assert_eq!(
            run(
                &incoming(&a.anti_affinity_namespace_selector),
                &pods_b,
                &nodes,
                args
            )
            .unwrap(),
            Some(vec![0, MAX])
        );
    }

    #[test]
    fn ignore_preferred_terms_of_existing_pods() {
        let a = affinities();
        let d = "default";
        let p = pod("", d, s2(), None);
        let pods = [
            pod("node1", d, s1(), Some(&a.stay_with_s1_away_from_s2)),
            pod("node2", d, s2(), Some(&a.stay_with_s2_in_region)),
        ];
        let nodes = vec![node("node1", rg("China")), node("node2", rg("India"))];
        let ignore = Args {
            ignore_preferred_terms_of_existing_pods: true,
            ..Args::default()
        };
        assert_eq!(run(&p, &pods, &nodes, ignore), Ok(None));
        assert_eq!(
            run(&p, &pods, &nodes, Args::default()).unwrap(),
            Some(vec![0, MAX])
        );
    }

    #[test]
    fn no_nodes_to_score_skips() {
        let p = pod("", "default", s2(), None);
        assert_eq!(run(&p, &[], &[], Args::default()), Ok(None));
    }

    /// TestPreferredAffinityWithHardPodAffinitySymmetricWeight.
    #[test]
    fn hard_pod_affinity_symmetric_weight() {
        let hard = json!({"podAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
            "labelSelector": {"matchExpressions": [sel("service", "In", &["S1"])]},
            "namespaces": ["", "subteam2.team2"],
            "namespaceSelector": {"matchExpressions": [sel("team", "In", &["team1"])]},
            "topologyKey": "region"
        }]}});
        let nodes = vec![
            node("node1", rg("China")),
            node("node2", rg("India")),
            node("node3", az("az1")),
        ];
        let pods = [
            pod("node1", "", json!({}), Some(&hard)),
            pod("node2", "", json!({}), Some(&hard)),
        ];
        let incoming = |ns: &str| pod("", ns, json!({"service": "S1"}), None);
        let with = |w: i32| Args {
            hard_pod_affinity_weight: w,
            ..Args::default()
        };
        // default weight, NamespaceSelector match ("subteam1.team1").
        assert_eq!(
            run(&incoming("subteam1.team1"), &pods, &nodes, with(1)).unwrap(),
            Some(vec![MAX, MAX, 0])
        );
        // zero weight: Skip.
        assert_eq!(
            run(&incoming("subteam1.team1"), &pods, &nodes, with(0)),
            Ok(None)
        );
        // no matching namespace: Skip.
        assert_eq!(
            run(&incoming("subteam1.team2"), &pods, &nodes, with(1)),
            Ok(None)
        );
        // matching Namespaces field.
        assert_eq!(
            run(&incoming("subteam2.team2"), &pods, &nodes, with(1)).unwrap(),
            Some(vec![MAX, MAX, 0])
        );
    }

    #[test]
    fn needs_namespaces_only_with_a_namespace_selector() {
        let a = affinities();
        let plain = pod("", "default", s1(), Some(&a.stay_with_s1_in_region));
        let with_sel = pod("n", "default", s1(), Some(&a.affinity_namespace_selector));
        assert!(!needs_namespaces(&plain, &[]));
        assert!(needs_namespaces(&with_sel, &[]));
        assert!(needs_namespaces(&plain, &[with_sel]));
    }
}
