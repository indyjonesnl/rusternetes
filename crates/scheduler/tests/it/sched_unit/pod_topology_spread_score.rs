//! Unit tests moved from `src/pod_topology_spread_score.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::TopologySpreadConstraint;
    pub use rusternetes_common::resources::{Node, Pod};
    pub use rusternetes_common::types::{
        label_selector_as_selector, LabelSelector, LabelSelectorRequirement, Phase, Selector,
    };
    pub use rusternetes_scheduler::queue::topology_spread_hints::merge_label_set_with_selector;
    pub use rusternetes_scheduler::score::MAX_NODE_SCORE;
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::pod_topology_spread_score::*;
    use serde_json::{json, Value};

    fn node(name: &str, labels: Value, taints: Option<Value>) -> Node {
        let mut v = json!({"apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "labels": labels}});
        if let Some(t) = taints {
            v["spec"] = json!({"taints": t});
        }
        serde_json::from_value(v).unwrap()
    }

    fn lbl(pairs: &[(&str, &str)]) -> Value {
        let mut m = serde_json::Map::new();
        for (k, v) in pairs {
            m.insert((*k).to_string(), json!(v));
        }
        Value::Object(m)
    }

    fn selector_of(keys: &[&str]) -> Value {
        json!({"matchExpressions": keys.iter()
            .map(|k| json!({"key": k, "operator": "Exists"})).collect::<Vec<_>>()})
    }

    fn foo() -> Value {
        selector_of(&["foo"])
    }
    fn bar() -> Value {
        selector_of(&["bar"])
    }

    fn constraint(
        max_skew: i32,
        key: &str,
        action: &str,
        selector: Option<Value>,
        extra: Value,
    ) -> Value {
        let mut c = json!({"maxSkew": max_skew, "topologyKey": key, "whenUnsatisfiable": action});
        if let Some(s) = selector {
            c["labelSelector"] = s;
        }
        if let Value::Object(m) = extra {
            for (k, v) in m {
                c[k] = v;
            }
        }
        c
    }

    fn pod_with(
        name: &str,
        ns: &str,
        node_name: &str,
        labels: Value,
        spec_extra: Value,
        terminating: bool,
    ) -> Pod {
        let mut spec = json!({"containers": []});
        if !node_name.is_empty() {
            spec["nodeName"] = json!(node_name);
        }
        if let Value::Object(m) = spec_extra {
            for (k, v) in m {
                spec[k] = v;
            }
        }
        let mut meta = json!({"name": name, "namespace": ns, "labels": labels});
        if terminating {
            meta["deletionTimestamp"] = json!("2024-01-01T00:00:00Z");
        }
        serde_json::from_value(json!({"apiVersion": "v1", "kind": "Pod",
            "metadata": meta, "spec": spec}))
        .unwrap()
    }

    fn incoming(labels: Value, constraints: Vec<Value>, extra: Value) -> Pod {
        let mut e = json!({});
        if !constraints.is_empty() {
            e["topologySpreadConstraints"] = json!(constraints);
        }
        if let Value::Object(m) = extra {
            for (k, v) in m {
                e[k] = v;
            }
        }
        pod_with("p", "default", "", labels, e, false)
    }

    fn existing(name: &str, node_name: &str, labels: &[&str]) -> Pod {
        let pairs: Vec<(&str, &str)> = labels.iter().map(|l| (*l, "")).collect();
        pod_with(name, "default", node_name, lbl(&pairs), json!({}), false)
    }

    const D: &str = "default";
    const ANY: &str = "ScheduleAnyway";

    fn list_args() -> Args {
        Args {
            default_constraints: Vec::new(),
            system_defaulted: false,
        }
    }

    struct Run {
        state: Option<PreScoreState>,
        scores: Vec<(String, i64)>,
    }

    /// PreScore + Score for the candidate `nodes` + NormalizeScore. The
    /// snapshot is `nodes` + `failed`.
    fn run(
        pod: &Pod,
        existing: &[Pod],
        nodes: &[Node],
        failed: &[Node],
        args: &Args,
        default_sel: Option<&Selector>,
    ) -> Run {
        let all: Vec<Node> = nodes.iter().chain(failed.iter()).cloned().collect();
        let by_node = pods_by_node(existing);
        let filtered: Vec<&Node> = nodes.iter().collect();
        let state = pre_score(pod, &filtered, &all, &by_node, args, default_sel).unwrap();
        let mut scores = Vec::new();
        if let Some(st) = &state {
            let mut v: Vec<i64> = nodes
                .iter()
                .map(|n| {
                    let pods = by_node
                        .get(n.metadata.name.as_str())
                        .cloned()
                        .unwrap_or_default();
                    score(st, pod, n, &pods)
                })
                .collect();
            let names: Vec<&str> = nodes.iter().map(|n| n.metadata.name.as_str()).collect();
            normalize_score(st, &names, &mut v);
            scores = nodes
                .iter()
                .map(|n| n.metadata.name.clone())
                .zip(v)
                .collect();
        }
        Run { state, scores }
    }

    fn got(r: &Run) -> Vec<(&str, i64)> {
        r.scores.iter().map(|(n, s)| (n.as_str(), *s)).collect()
    }

    fn host(n: &str) -> Value {
        lbl(&[(LABEL_HOSTNAME, n)])
    }

    #[test]
    fn pre_score_skip() {
        // the pod doesn't have soft topology spread constraints (List defaulting).
        let p = incoming(json!({}), vec![], json!({}));
        let nodes = vec![
            node("node-a", host("node-a"), None),
            node("node-b", host("node-b"), None),
        ];
        assert!(run(&p, &[], &nodes, &[], &list_args(), None)
            .state
            .is_none());

        // default constraints and a replicaset that doesn't match: the default
        // selector is empty.
        let args = Args {
            default_constraints: vec![serde_json::from_value(constraint(
                2,
                "planet",
                ANY,
                None,
                json!({}),
            ))
            .unwrap()],
            system_defaulted: false,
        };
        let p = pod_with(
            "p",
            D,
            "",
            lbl(&[("foo", "bar"), ("baz", "sup")]),
            json!({}),
            false,
        );
        let sel = default_selector(
            &p,
            &DefaultSelectorInputs {
                service_selectors: vec![],
                owner: None,
            },
        );
        let nodes = vec![node("node-a", lbl(&[("planet", "mars")]), None)];
        assert!(run(&p, &[], &nodes, &[], &args, Some(&sel)).state.is_none());
    }

    fn counts(state: &PreScoreState) -> Vec<Vec<(String, i64)>> {
        state
            .topology_value_to_pod_counts
            .iter()
            .map(|m| {
                let mut v: Vec<_> = m.iter().map(|(k, c)| (k.clone(), *c)).collect();
                v.sort();
                v
            })
            .collect()
    }

    fn weights(sizes: &[usize]) -> Vec<f64> {
        sizes
            .iter()
            .map(|s| topology_normalizing_weight(*s))
            .collect()
    }

    fn zone_counts(vals: &[&str]) -> Vec<(String, i64)> {
        let mut v: Vec<_> = vals.iter().map(|s| ((*s).to_string(), 0)).collect();
        v.sort();
        v
    }

    /// TestPreScoreStateEmptyNodes.
    #[test]
    fn pre_score_state_empty_nodes() {
        let three = || {
            vec![
                node(
                    "node-a",
                    lbl(&[("zone", "zone1"), (LABEL_HOSTNAME, "node-a")]),
                    None,
                ),
                node(
                    "node-b",
                    lbl(&[("zone", "zone1"), (LABEL_HOSTNAME, "node-b")]),
                    None,
                ),
                node(
                    "node-x",
                    lbl(&[("zone", "zone2"), (LABEL_HOSTNAME, "node-x")]),
                    None,
                ),
            ]
        };
        // normal case
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![
                constraint(1, "zone", ANY, Some(foo()), json!({})),
                constraint(1, LABEL_HOSTNAME, ANY, Some(foo()), json!({})),
            ],
            json!({}),
        );
        let r = run(&p, &[], &three(), &[], &list_args(), None);
        let st = r.state.unwrap();
        assert_eq!(st.constraints.len(), 2);
        assert!(st.ignored_nodes.is_empty());
        assert_eq!(counts(&st), vec![zone_counts(&["zone1", "zone2"]), vec![]]);
        assert_eq!(st.topology_normalizing_weight, weights(&[2, 3]));
        assert!(st.constraints[0].node_affinity_honor && !st.constraints[0].node_taints_honor);

        // null selector
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(1, "zone", ANY, None, json!({}))],
            json!({}),
        );
        let st = run(&p, &[], &three(), &[], &list_args(), None)
            .state
            .unwrap();
        assert!(matches!(st.constraints[0].selector, Selector::Nothing));
        assert_eq!(counts(&st), vec![zone_counts(&["zone1", "zone2"])]);
        assert_eq!(st.topology_normalizing_weight, weights(&[2]));

        // node-x doesn't have label zone
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![
                constraint(1, "zone", ANY, Some(foo()), json!({})),
                constraint(1, LABEL_HOSTNAME, ANY, Some(bar()), json!({})),
            ],
            json!({}),
        );
        let nodes = vec![
            node(
                "node-a",
                lbl(&[("zone", "zone1"), (LABEL_HOSTNAME, "node-a")]),
                None,
            ),
            node(
                "node-b",
                lbl(&[("zone", "zone1"), (LABEL_HOSTNAME, "node-b")]),
                None,
            ),
            node("node-x", host("node-x"), None),
        ];
        let st = run(&p, &[], &nodes, &[], &list_args(), None).state.unwrap();
        assert_eq!(st.ignored_nodes, HashSet::from(["node-x".to_string()]));
        assert_eq!(counts(&st), vec![zone_counts(&["zone1"]), vec![]]);
        assert_eq!(st.topology_normalizing_weight, weights(&[1, 2]));

        // system default constraints and a replicaset
        let rs_inputs = |sel: Value| DefaultSelectorInputs {
            service_selectors: vec![],
            owner: Some(OwnerSelector::Selector(Some(
                serde_json::from_value(sel).unwrap(),
            ))),
        };
        let owned = |labels: Value, extra: Value| {
            let mut spec = json!({"containers": []});
            if let Value::Object(m) = extra {
                for (k, v) in m {
                    spec[k] = v;
                }
            }
            let mut p: Pod = serde_json::from_value(json!({"apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": "p", "namespace": "default", "labels": labels,
                    "ownerReferences": [{"apiVersion": "apps/v1", "kind": "ReplicaSet",
                        "name": "rs1", "uid": "u", "controller": true}]},
                "spec": spec}))
            .unwrap();
            p.metadata.uid = "p".into();
            p
        };
        let p = owned(lbl(&[("foo", "tar"), ("baz", "sup")]), json!({}));
        assert_eq!(
            default_selector_owner(&p),
            Some((OwnerKind::ReplicaSet, "rs1".to_string()))
        );
        let sel = default_selector(&p, &rs_inputs(foo()));
        let nodes = vec![
            node(
                "node-a",
                lbl(&[(LABEL_HOSTNAME, "node-a"), (LABEL_TOPOLOGY_ZONE, "mars")]),
                None,
            ),
            node(
                "node-b",
                lbl(&[(LABEL_HOSTNAME, "node-b"), (LABEL_TOPOLOGY_ZONE, "mars")]),
                None,
            ),
            node("node-c", host("node-c"), None),
            node("node-d", host("node-d"), None),
        ];
        let st = run(&p, &[], &nodes, &[], &Args::default(), Some(&sel))
            .state
            .unwrap();
        assert_eq!(st.constraints.len(), 2);
        assert_eq!(st.constraints[0].max_skew, 3);
        assert_eq!(st.constraints[1].max_skew, 5);
        assert!(st.ignored_nodes.is_empty());
        assert_eq!(counts(&st), vec![vec![], zone_counts(&["", "mars"])]);
        assert_eq!(st.topology_normalizing_weight, weights(&[4, 2]));

        // default constraints and a replicaset (List): DoNotSchedule dropped.
        let args = Args {
            default_constraints: vec![
                serde_json::from_value(constraint(1, LABEL_HOSTNAME, ANY, None, json!({})))
                    .unwrap(),
                serde_json::from_value(constraint(2, "rack", "DoNotSchedule", None, json!({})))
                    .unwrap(),
                serde_json::from_value(constraint(2, "planet", ANY, None, json!({}))).unwrap(),
            ],
            system_defaulted: false,
        };
        let nodes = vec![node(
            "node-a",
            lbl(&[
                ("rack", "rack1"),
                (LABEL_HOSTNAME, "node-a"),
                ("planet", "mars"),
            ]),
            None,
        )];
        let st = run(&p, &[], &nodes, &[], &args, Some(&sel)).state.unwrap();
        assert_eq!(
            st.constraints
                .iter()
                .map(|c| c.topology_key.as_str())
                .collect::<Vec<_>>(),
            vec![LABEL_HOSTNAME, "planet"]
        );
        assert_eq!(counts(&st), vec![vec![], zone_counts(&["mars"])]);
        assert_eq!(st.topology_normalizing_weight, weights(&[1, 1]));

        // default constraints and a replicaset, but pod has constraints
        let p2 = owned(
            lbl(&[("foo", "bar"), ("baz", "sup")]),
            json!({"topologySpreadConstraints": [
                constraint(1, "zone", "DoNotSchedule", Some(bar()), json!({})),
                constraint(2, "planet", ANY, Some(json!({"matchLabels": {"baz": "sup"}})), json!({})),
            ]}),
        );
        let args = Args {
            default_constraints: vec![serde_json::from_value(constraint(
                2,
                "galaxy",
                ANY,
                None,
                json!({}),
            ))
            .unwrap()],
            system_defaulted: false,
        };
        let nodes = vec![node(
            "node-a",
            lbl(&[("planet", "mars"), ("galaxy", "andromeda")]),
            None,
        )];
        let st = run(&p2, &[], &nodes, &[], &args, Some(&sel)).state.unwrap();
        assert_eq!(st.constraints.len(), 1);
        assert_eq!(st.constraints[0].topology_key, "planet");
        assert_eq!(counts(&st), vec![zone_counts(&["mars"])]);
        assert_eq!(st.topology_normalizing_weight, weights(&[1]));

        // NodeAffinityPolicy ignored / NodeTaintsPolicy honored keep the state
        // shape (zero counts for every value).
        let ignore = json!({"nodeAffinityPolicy": "Ignore"});
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(1, "zone", ANY, Some(bar()), ignore)],
            json!({"nodeSelector": {"foo": ""}}),
        );
        let st = run(&p, &[], &three(), &[], &list_args(), None)
            .state
            .unwrap();
        assert!(!st.constraints[0].node_affinity_honor);
        assert_eq!(counts(&st), vec![zone_counts(&["zone1", "zone2"])]);
        let honor = json!({"nodeTaintsPolicy": "Honor"});
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(1, "zone", ANY, Some(bar()), honor)],
            json!({}),
        );
        let st = run(&p, &[], &three(), &[], &list_args(), None)
            .state
            .unwrap();
        assert!(st.constraints[0].node_taints_honor);
    }

    fn names_scores(want: &[(&str, i64)]) -> Vec<(String, i64)> {
        want.iter().map(|(n, s)| ((*n).to_string(), *s)).collect()
    }

    fn two_nodes(a: Value, b: Value) -> Vec<Node> {
        vec![node("node-a", a, None), node("node-b", b, None)]
    }

    fn four_host_nodes() -> Vec<Node> {
        ["a", "b", "c", "d"]
            .iter()
            .map(|s| node(&format!("node-{s}"), host(&format!("node-{s}")), None))
            .collect()
    }

    fn host_pod(max_skew: i32) -> Pod {
        incoming(
            lbl(&[("foo", "")]),
            vec![constraint(
                max_skew,
                LABEL_HOSTNAME,
                ANY,
                Some(foo()),
                json!({}),
            )],
            json!({}),
        )
    }

    /// TestPodTopologySpreadScore, hostname-only and system-defaulting cases.
    #[test]
    fn score_hostname_cases() {
        let l = list_args();
        // no existing pods
        let r = run(
            &host_pod(1),
            &[],
            &two_nodes(host("node-a"), host("node-b")),
            &[],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 100), ("node-b", 100)]));

        // only one node is candidate
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
        ];
        let r = run(
            &host_pod(1),
            &ex,
            &[node("node-a", host("node-a"), None)],
            &[node("node-b", host("node-b"), None)],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 100)]));

        // all nodes have the same number of matching pods
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
        ];
        let r = run(
            &host_pod(1),
            &ex,
            &two_nodes(host("node-a"), host("node-b")),
            &[],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 100), ("node-b", 100)]));

        // 2/1/0/3
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-d1", "node-d", &["foo"]),
            existing("p-d2", "node-d", &["foo"]),
            existing("p-d3", "node-d", &["foo"]),
        ];
        let r = run(&host_pod(1), &ex, &four_host_nodes(), &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 20),
                ("node-b", 60),
                ("node-c", 100),
                ("node-d", 0)
            ])
        );
        // maxSkew=2
        let r = run(&host_pod(2), &ex, &four_host_nodes(), &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 33),
                ("node-b", 66),
                ("node-c", 100),
                ("node-d", 16)
            ])
        );
        // null selector
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(1, LABEL_HOSTNAME, ANY, None, json!({}))],
            json!({}),
        );
        let r = run(&p, &ex, &four_host_nodes(), &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 100),
                ("node-b", 100),
                ("node-c", 100),
                ("node-d", 100)
            ])
        );

        // maxSkew=3, spread 4/3/2/1
        let ex43 = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-a3", "node-a", &["foo"]),
            existing("p-a4", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-b2", "node-b", &["foo"]),
            existing("p-b3", "node-b", &["foo"]),
            existing("p-c1", "node-c", &["foo"]),
            existing("p-c2", "node-c", &["foo"]),
            existing("p-d1", "node-d", &["foo"]),
        ];
        let r = run(&host_pod(3), &ex43, &four_host_nodes(), &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 44),
                ("node-b", 66),
                ("node-c", 77),
                ("node-d", 100)
            ])
        );

        // system defaulting, nodes don't have zone, pods match service
        let p = incoming(lbl(&[("foo", "")]), vec![], json!({}));
        let sel = default_selector(
            &p,
            &DefaultSelectorInputs {
                service_selectors: vec![Some(HashMap::from([("foo".to_string(), String::new())]))],
                owner: None,
            },
        );
        let r = run(
            &p,
            &ex43,
            &four_host_nodes(),
            &[],
            &Args::default(),
            Some(&sel),
        );
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 44),
                ("node-b", 66),
                ("node-c", 77),
                ("node-d", 100)
            ])
        );

        // 3 out of 4 nodes are candidates (4/2/1/~3~)
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-a3", "node-a", &["foo"]),
            existing("p-a4", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-b2", "node-b", &["foo"]),
            existing("p-x1", "node-x", &["foo"]),
            existing("p-y1", "node-y", &["foo"]),
            existing("p-y2", "node-y", &["foo"]),
            existing("p-y3", "node-y", &["foo"]),
        ];
        let cands = vec![
            node("node-a", host("node-a"), None),
            node("node-b", host("node-b"), None),
            node("node-x", host("node-x"), None),
        ];
        let failed = vec![node("node-y", host("node-y"), None)];
        let r = run(&host_pod(1), &ex, &cands, &failed, &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[("node-a", 33), ("node-b", 83), ("node-x", 100)])
        );
        // ... and one node doesn't match the topology key
        let cands = vec![
            node("node-a", host("node-a"), None),
            node("node-b", lbl(&[("n", "node-b")]), None),
            node("node-x", host("node-x"), None),
        ];
        let r = run(&host_pod(1), &ex, &cands, &failed, &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[("node-a", 16), ("node-b", 0), ("node-x", 100)])
        );
    }

    fn zone_node(name: &str, zone: &str, with_host: bool) -> Node {
        let mut pairs = vec![("zone", zone)];
        let h = name.to_string();
        if with_host {
            pairs.push((LABEL_HOSTNAME, h.as_str()));
        }
        node(name, lbl(&pairs), None)
    }

    #[test]
    fn score_zone_cases() {
        let l = list_args();
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-a3", "node-a", &["foo"]),
            existing("p-a4", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-b2", "node-b", &["foo"]),
            existing("p-x1", "node-x", &["foo"]),
            existing("p-y1", "node-y", &["foo"]),
            existing("p-y2", "node-y", &["foo"]),
            existing("p-y3", "node-y", &["foo"]),
        ];
        // one constraint on zone, 3 out of 4 nodes are candidates
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(1, "zone", ANY, Some(foo()), json!({}))],
            json!({}),
        );
        let r = run(
            &p,
            &ex,
            &[
                zone_node("node-a", "zone1", true),
                zone_node("node-b", "zone1", true),
                zone_node("node-x", "zone2", true),
            ],
            &[zone_node("node-y", "zone2", true)],
            &l,
            None,
        );
        assert_eq!(
            r.scores,
            names_scores(&[("node-a", 75), ("node-b", 75), ("node-x", 100)])
        );

        // two constraints on zone and node, 2 out of 4 nodes are candidates
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-x1", "node-x", &["foo"]),
            existing("p-x2", "node-x", &["foo"]),
            existing("p-y1", "node-y", &["foo"]),
            existing("p-y2", "node-y", &["foo"]),
            existing("p-y3", "node-y", &["foo"]),
            existing("p-y4", "node-y", &["foo"]),
        ];
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![
                constraint(1, "zone", ANY, Some(foo()), json!({})),
                constraint(1, LABEL_HOSTNAME, ANY, Some(foo()), json!({})),
            ],
            json!({}),
        );
        let r = run(
            &p,
            &ex,
            &[
                zone_node("node-a", "zone1", true),
                zone_node("node-x", "zone2", true),
            ],
            &[
                zone_node("node-b", "zone1", true),
                zone_node("node-y", "zone2", true),
            ],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 100), ("node-x", 63)]));

        // two constraints on zone, 2 out of 4 nodes are candidates
        let ex = vec![
            existing("p-a1", "node-a", &["foo", "bar"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo", "bar"]),
            existing("p-y1", "node-y", &["foo"]),
            existing("p-y2", "node-y", &["foo", "bar"]),
            existing("p-y3", "node-y", &["foo"]),
        ];
        let p = incoming(
            lbl(&[("foo", ""), ("bar", "")]),
            vec![
                constraint(1, "zone", ANY, Some(foo()), json!({})),
                constraint(1, "zone", ANY, Some(bar()), json!({})),
            ],
            json!({}),
        );
        let r = run(
            &p,
            &ex,
            &[
                zone_node("node-a", "zone1", false),
                zone_node("node-x", "zone2", false),
            ],
            &[
                zone_node("node-b", "zone1", false),
                zone_node("node-y", "zone2", false),
            ],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 85), ("node-x", 100)]));

        // different labelSelectors
        let two_diff = || {
            incoming(
                lbl(&[("foo", ""), ("bar", "")]),
                vec![
                    constraint(1, "zone", ANY, Some(foo()), json!({})),
                    constraint(1, LABEL_HOSTNAME, ANY, Some(bar()), json!({})),
                ],
                json!({}),
            )
        };
        let four = || {
            vec![
                zone_node("node-a", "zone1", true),
                zone_node("node-b", "zone1", true),
                zone_node("node-x", "zone2", true),
                zone_node("node-y", "zone2", true),
            ]
        };
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo", "bar"]),
            existing("p-y1", "node-y", &["foo"]),
            existing("p-y2", "node-y", &["bar"]),
        ];
        let r = run(&two_diff(), &ex, &four(), &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 60),
                ("node-b", 20),
                ("node-x", 100),
                ("node-y", 60)
            ])
        );
        // some nodes have 0 pods
        let ex2 = vec![
            existing("p-b1", "node-b", &["bar"]),
            existing("p-x1", "node-x", &["foo"]),
            existing("p-y1", "node-y", &["foo", "bar"]),
        ];
        let r = run(&two_diff(), &ex2, &four(), &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[
                ("node-a", 100),
                ("node-b", 60),
                ("node-x", 40),
                ("node-y", 0)
            ])
        );
        // 3 out of 4 nodes are candidates
        let r = run(
            &two_diff(),
            &ex,
            &[
                zone_node("node-a", "zone1", true),
                zone_node("node-b", "zone1", true),
                zone_node("node-x", "zone2", true),
            ],
            &[zone_node("node-y", "zone2", true)],
            &l,
            None,
        );
        assert_eq!(
            r.scores,
            names_scores(&[("node-a", 50), ("node-b", 25), ("node-x", 100)])
        );
    }

    #[test]
    fn score_namespace_terminating_and_min_domains() {
        let l = list_args();
        // existing pods in a different namespace do not count
        let ex = vec![
            pod_with(
                "p-a1",
                "ns1",
                "node-a",
                lbl(&[("foo", "")]),
                json!({}),
                false,
            ),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-b2", "node-b", &["foo"]),
        ];
        let r = run(
            &host_pod(1),
            &ex,
            &two_nodes(host("node-a"), host("node-b")),
            &[],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 100), ("node-b", 33)]));

        // terminating pods should be excluded
        let ex = vec![
            pod_with("p-a", D, "node-a", lbl(&[("foo", "")]), json!({}), true),
            existing("p-b", "node-b", &["foo"]),
        ];
        let r = run(
            &host_pod(1),
            &ex,
            &two_nodes(host("node-a"), host("node-b")),
            &[],
            &l,
            None,
        );
        assert_eq!(r.scores, names_scores(&[("node-a", 100), ("node-b", 0)]));

        // minDomains has no effect when ScheduleAnyway
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(
                2,
                "node",
                ANY,
                Some(foo()),
                json!({"minDomains": 10}),
            )],
            json!({}),
        );
        let nodes: Vec<Node> = ["a", "b", "c"]
            .iter()
            .map(|s| {
                node(
                    &format!("node-{s}"),
                    lbl(&[("node", &format!("node-{s}"))]),
                    None,
                )
            })
            .collect();
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-b2", "node-b", &["foo"]),
            existing("p-c1", "node-c", &["foo"]),
        ];
        let r = run(&p, &ex, &nodes, &[], &l, None);
        assert_eq!(
            r.scores,
            names_scores(&[("node-a", 75), ("node-b", 75), ("node-c", 100)])
        );
    }

    fn node_label_pairs(name: &str, foo_label: bool) -> Node {
        let mut pairs = vec![("node", name)];
        if foo_label {
            pairs.push(("foo", ""));
        }
        node(name, lbl(&pairs), None)
    }

    #[test]
    fn score_node_inclusion_policies() {
        let l = list_args();
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-a2", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo"]),
            existing("p-c1", "node-c", &["foo"]),
        ];
        let nodes = || {
            vec![
                node_label_pairs("node-a", true),
                node_label_pairs("node-b", true),
                node_label_pairs("node-c", false),
            ]
        };
        let affinity = json!({"affinity": {"nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": "foo", "operator": "In", "values": [""]}]}]}}}});
        for (extra, ignore) in [
            (json!({"nodeSelector": {"foo": ""}}), false),
            (affinity.clone(), false),
        ] {
            let _ = ignore;
            // honored
            let p = incoming(
                lbl(&[("foo", "")]),
                vec![constraint(1, "node", ANY, Some(foo()), json!({}))],
                extra.clone(),
            );
            let r = run(&p, &ex, &nodes(), &[], &l, None);
            assert_eq!(
                got(&r),
                vec![("node-a", 0), ("node-b", 33), ("node-c", 100)]
            );
            // ignored
            let p = incoming(
                lbl(&[("foo", "")]),
                vec![constraint(
                    1,
                    "node",
                    ANY,
                    Some(foo()),
                    json!({"nodeAffinityPolicy": "Ignore"}),
                )],
                extra,
            );
            let r = run(&p, &ex, &nodes(), &[], &l, None);
            assert_eq!(
                got(&r),
                vec![("node-a", 66), ("node-b", 100), ("node-c", 100)]
            );
        }

        // NodeTaintsPolicy honored / ignored
        let taints = json!([{"key": "foo", "value": "bar", "effect": "NoSchedule"}]);
        let tn = || {
            vec![
                node("node-a", lbl(&[("node", "node-a")]), None),
                node("node-b", lbl(&[("node", "node-b")]), None),
                node("node-c", lbl(&[("node", "node-c")]), Some(taints.clone())),
            ]
        };
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(
                1,
                "node",
                ANY,
                Some(foo()),
                json!({"nodeTaintsPolicy": "Honor"}),
            )],
            json!({}),
        );
        let r = run(&p, &ex, &tn(), &[], &l, None);
        assert_eq!(
            got(&r),
            vec![("node-a", 0), ("node-b", 33), ("node-c", 100)]
        );
        let p = incoming(
            lbl(&[("foo", "")]),
            vec![constraint(1, "node", ANY, Some(foo()), json!({}))],
            json!({}),
        );
        let r = run(&p, &ex, &tn(), &[], &l, None);
        assert_eq!(
            got(&r),
            vec![("node-a", 66), ("node-b", 100), ("node-c", 100)]
        );
    }

    #[test]
    fn score_match_label_keys() {
        let l = list_args();
        let four = || {
            vec![
                zone_node("node-a", "zone1", true),
                zone_node("node-b", "zone1", true),
                zone_node("node-c", "zone2", true),
                zone_node("node-d", "zone2", true),
            ]
        };
        let want = vec![
            ("node-a", 60),
            ("node-b", 20),
            ("node-c", 60),
            ("node-d", 100),
        ];
        // ANDed with LabelSelector when LabelSelector is empty
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo", "bar"]),
            existing("p-y1", "node-c", &["foo"]),
            existing("p-y2", "node-c", &["bar"]),
        ];
        let p = incoming(
            lbl(&[("foo", ""), ("bar", "")]),
            vec![
                constraint(
                    1,
                    "zone",
                    ANY,
                    Some(json!({})),
                    json!({"matchLabelKeys": ["foo"]}),
                ),
                constraint(
                    1,
                    LABEL_HOSTNAME,
                    ANY,
                    Some(json!({})),
                    json!({"matchLabelKeys": ["bar"]}),
                ),
            ],
            json!({}),
        );
        assert_eq!(got(&run(&p, &ex, &four(), &[], &l, None)), want);

        // ANDed with LabelSelector when LabelSelector isn't empty
        let ex = vec![
            existing("p-a1", "node-a", &["foo"]),
            existing("p-b1", "node-b", &["foo", "bar", "baz"]),
            existing("p-c1", "node-c", &["foo"]),
            existing("p-c2", "node-c", &["bar"]),
            existing("p-d3", "node-c", &["bar", "baz"]),
        ];
        let p = incoming(
            lbl(&[("foo", ""), ("bar", ""), ("baz", "")]),
            vec![
                constraint(1, "zone", ANY, Some(foo()), json!({})),
                constraint(
                    1,
                    LABEL_HOSTNAME,
                    ANY,
                    Some(bar()),
                    json!({"matchLabelKeys": ["baz"]}),
                ),
            ],
            json!({}),
        );
        assert_eq!(got(&run(&p, &ex, &four(), &[], &l, None)), want);
    }

    #[test]
    fn default_selector_merges_services_and_controller() {
        // helper/spread_test.go TestDefaultSelector: service + RC labels merge,
        // an RS/StatefulSet adds its requirements, unmatched services are ignored.
        let p = pod_with(
            "p",
            D,
            "",
            lbl(&[("foo", "bar"), ("baz", "sup")]),
            json!({}),
            false,
        );
        let inputs = DefaultSelectorInputs {
            service_selectors: vec![
                Some(HashMap::from([("foo".to_string(), "bar".to_string())])),
                Some(HashMap::from([("foo".to_string(), "other".to_string())])),
                None,
            ],
            owner: Some(OwnerSelector::ReplicationController(Some(HashMap::from([
                ("baz".to_string(), "sup".to_string()),
            ])))),
        };
        let sel = default_selector(&p, &inputs);
        let m = |a: &[(&str, &str)]| {
            let h: HashMap<String, String> =
                a.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect();
            sel.matches(Some(&h))
        };
        assert!(m(&[("foo", "bar"), ("baz", "sup")]));
        assert!(!m(&[("foo", "bar")]));
        // no services, no owner: empty.
        let sel = default_selector(
            &p,
            &DefaultSelectorInputs {
                service_selectors: vec![None],
                owner: None,
            },
        );
        assert!(sel.is_everything());
        // an owner kind outside RC/RS/StatefulSet is ignored.
        assert_eq!(default_selector_owner(&p), None);
    }
}
