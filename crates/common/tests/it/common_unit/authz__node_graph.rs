//! Unit tests moved from `src/authz/node_graph.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use prometheus::{register, HistogramOpts, HistogramVec};
    pub use rusternetes_common::resources::podcertificaterequest::PodCertificateRequest;
    pub use rusternetes_common::resources::{PersistentVolume, Pod};
    pub use serde_json::Value;
    pub use std::collections::{BTreeSet, HashMap, HashSet};
    pub use std::sync::{LazyLock, RwLock};
    pub use std::time::Instant;
}

pub mod tests {
    //! Ported from `plugin/pkg/auth/authorizer/node/intset_test.go` (`TestIntSet`)
    //! and `graph_test.go` (`TestDeleteEdgesLocked`, `TestIndex`, `TestIndex2`),
    //! plus the visitor tables of `pkg/api/v1/pod/util.go` /
    //! `pkg/api/v1/persistentvolume/util.go` that feed `AddPod` / `AddPV`.

    use super::__parent::*;
    use rusternetes_common::authz::node_graph::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    pub(crate) fn pod_from(v: serde_json::Value) -> Pod {
        serde_json::from_value(v).expect("pod")
    }

    pub(crate) fn pv_from(v: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(v).expect("pv")
    }

    fn pcr(
        namespace: &str,
        name: &str,
        pod_name: &str,
        sa_name: &str,
        node_name: &str,
    ) -> PodCertificateRequest {
        serde_json::from_value(json!({
            "metadata": {"namespace": namespace, "name": name, "uid": format!("pcr{name}uid")},
            "spec": {"podName": pod_name, "serviceAccountName": sa_name, "nodeName": node_name},
        }))
        .expect("pcr")
    }

    fn test_graph() -> NodeGraph {
        let g = NodeGraph::new();
        g.set_destination_edge_threshold(3);
        g
    }

    /// `toString`/`expectGraph` of graph_test.go.
    fn graph_map(g: &NodeGraph) -> BTreeMap<String, Vec<String>> {
        let inner = g.read();
        let mut actual = BTreeMap::new();
        for (id, node) in &inner.nodes {
            let mut to: Vec<String> = inner
                .neighbors_from(*id)
                .into_iter()
                .map(|t| inner.nodes[&t].describe())
                .collect();
            to.sort();
            actual.insert(node.describe(), to);
        }
        actual
    }

    /// `expectIndex` of graph_test.go.
    fn index_map(g: &NodeGraph) -> BTreeMap<String, Vec<String>> {
        let inner = g.read();
        let mut actual = BTreeMap::new();
        for (from, set) in &inner.destination_edge_index {
            let mut values: Vec<String> = set
                .members
                .iter()
                .map(|(member, count)| format!("{}={}", inner.nodes[member].describe(), count))
                .collect();
            values.sort();
            actual.insert(inner.nodes[from].describe(), values);
        }
        actual
    }

    fn with(base: &[(&str, &[&str])], extra: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        let mut v = base.to_vec();
        v.extend_from_slice(extra);
        expected(&v)
    }

    fn expected(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn int_set() {
        let mut i = IntSet::default();

        assert!(!i.has(1));
        assert!(!i.has(2));
        assert!(!i.has(3));
        assert!(!i.has(4));

        i.reset();
        i.increment(1); // to 1
        i.increment(2); // to 1

        assert!(i.has(1));
        assert!(i.has(2));
        assert!(!i.has(3));
        assert!(!i.has(4));

        i.decrement(1); // to 0
        i.increment(3); // to 1

        assert!(!i.has(1)); // removed
        assert!(i.has(2)); // still present
        assert!(i.has(3)); // added
        assert!(!i.has(4)); // not yet present

        i.decrement(2); // to 0
        i.increment(3); // to 2
        i.decrement(3); // to 1
        i.increment(4); // to 1

        assert!(!i.has(1));
        assert!(!i.has(2));
        assert!(i.has(3));
        assert!(i.has(4));

        i.reset();
        assert!(!i.has(1));
        assert!(!i.has(2));
        assert!(!i.has(3));
        assert!(!i.has(4));
    }

    /// A comparable snapshot: `graph.Nodes()` and `graph.Edges()` sorted, and the
    /// `vertices` map (graph_test.go `TestDeleteEdgesLocked` assertions).
    type Snapshot = (
        Vec<(usize, String)>,
        Vec<(usize, usize, Option<usize>)>,
        String,
    );

    fn snapshot(g: &NodeGraph) -> Snapshot {
        let inner = g.read();
        let mut nodes: Vec<_> = inner
            .nodes
            .iter()
            .map(|(id, n)| (*id, n.describe()))
            .collect();
        nodes.sort();
        let mut edges: Vec<_> = inner
            .edges
            .values()
            .flat_map(|m| m.values())
            .map(|e| (e.from, e.to, e.destination))
            .collect();
        edges.sort();
        edges.dedup();
        let mut vertices: Vec<String> = inner
            .vertices
            .iter()
            .flat_map(|(t, by_ns)| {
                by_ns.iter().flat_map(move |(ns, by_name)| {
                    by_name
                        .iter()
                        .map(move |(name, id)| format!("{}|{}|{}|{}", t.name(), ns, name, id))
                })
            })
            .collect();
        vertices.sort();
        (nodes, edges, vertices.join(","))
    }

    #[test]
    fn delete_edges_locked() {
        use VertexType::{ConfigMap, Node};

        struct Case {
            desc: &'static str,
            to_name: &'static str,
            start: Box<dyn Fn(&mut GraphInner)>,
            expect: Box<dyn Fn(&mut GraphInner)>,
        }
        let cases = vec![
            Case {
                // single edge from a configmap to a node, will delete edge and orphaned configmap
                desc: "edges and source orphans are deleted, destination orphans are preserved",
                to_name: "node1",
                start: Box::new(|g| {
                    g.get_or_create_vertex(ConfigMap, "namespace1", "configmap2");
                    let node = g.get_or_create_vertex(Node, "", "node1");
                    let cm = g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                    g.add_edge(cm, node, Some(node));
                }),
                expect: Box::new(|g| {
                    g.get_or_create_vertex(ConfigMap, "namespace1", "configmap2");
                    g.get_or_create_vertex(Node, "", "node1");
                }),
            },
            Case {
                // two edges from the same configmap to distinct nodes, will delete one of the edges
                desc: "edges are deleted, non-orphans and destination orphans are preserved",
                to_name: "node2",
                start: Box::new(|g| {
                    let n1 = g.get_or_create_vertex(Node, "", "node1");
                    let n2 = g.get_or_create_vertex(Node, "", "node2");
                    let cm = g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                    g.add_edge(cm, n1, Some(n1));
                    g.add_edge(cm, n2, Some(n2));
                }),
                expect: Box::new(|g| {
                    let n1 = g.get_or_create_vertex(Node, "", "node1");
                    g.get_or_create_vertex(Node, "", "node2");
                    let cm = g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                    g.add_edge(cm, n1, Some(n1));
                }),
            },
            Case {
                desc: "no edges to delete",
                to_name: "node1",
                start: Box::new(|g| {
                    g.get_or_create_vertex(Node, "", "node1");
                    g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                }),
                expect: Box::new(|g| {
                    g.get_or_create_vertex(Node, "", "node1");
                    g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                }),
            },
            Case {
                desc: "destination vertex does not exist",
                to_name: "node1",
                start: Box::new(|g| {
                    g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                }),
                expect: Box::new(|g| {
                    g.get_or_create_vertex(ConfigMap, "namespace1", "configmap1");
                }),
            },
            Case {
                desc: "source vertex type doesn't exist",
                to_name: "node1",
                start: Box::new(|g| {
                    g.get_or_create_vertex(Node, "", "node1");
                }),
                expect: Box::new(|g| {
                    g.get_or_create_vertex(Node, "", "node1");
                }),
            },
        ];
        for c in cases {
            let start = NodeGraph::new();
            (c.start)(&mut start.write());
            let expect = NodeGraph::new();
            (c.expect)(&mut expect.write());

            start.write().delete_edges(ConfigMap, Node, "", c.to_name);

            assert_eq!(snapshot(&expect), snapshot(&start), "{}", c.desc);
        }
    }

    fn pod_with_volumes(
        name: &str,
        node: &str,
        sa: &str,
        volumes: serde_json::Value,
        resource_claims: serde_json::Value,
        extended_claim: &str,
    ) -> Pod {
        let mut spec = json!({"nodeName": node, "containers": []});
        if !sa.is_empty() {
            spec["serviceAccountName"] = json!(sa);
        }
        if !volumes.is_null() {
            spec["volumes"] = volumes;
        }
        if !resource_claims.is_null() {
            spec["resourceClaims"] = resource_claims;
        }
        let mut p = json!({
            "metadata": {"name": name, "namespace": "ns", "uid": format!("pod{name}uid")},
            "spec": spec,
        });
        if !extended_claim.is_empty() {
            p["status"] = json!({"extendedResourceClaimStatus": {"resourceClaimName": extended_claim, "requestMappings": []}});
        }
        pod_from(p)
    }

    fn pod_with_sa_and_cms(name: &str, node: &str) -> Pod {
        let cm = |n: &str| json!({"name": n, "configMap": {"name": n}});
        pod_with_volumes(
            name,
            node,
            "sa1",
            json!([cm("cm1"), cm("cm2"), cm("cm3")]),
            json!(null),
            "",
        )
    }

    fn pod_with_secrets(name: &str, node: &str) -> Pod {
        let s = |n: &str| json!({"name": n, "secret": {"secretName": n}});
        pod_with_volumes(
            name,
            node,
            "",
            json!([s("s1"), s("s2"), s("s3")]),
            json!(null),
            "",
        )
    }

    fn pod_with_pvcs(name: &str, node: &str) -> Pod {
        let p = |n: &str| json!({"name": n, "persistentVolumeClaim": {"claimName": n}});
        pod_with_volumes(
            name,
            node,
            "",
            json!([p("pvc1"), p("pvc2"), p("pvc3")]),
            json!(null),
            "",
        )
    }

    fn pod_with_resource_claims(name: &str, node: &str) -> Pod {
        let rc = |n: &str| json!({"name": n, "resourceClaimName": n});
        pod_with_volumes(
            name,
            node,
            "",
            json!(null),
            json!([rc("rc1"), rc("rc2"), rc("rc3")]),
            "rc4",
        )
    }

    fn pv(name: &str, pvc_name: &str, secret_name: &str) -> PersistentVolume {
        let mut v = json!({
            "metadata": {"name": name, "uid": format!("pv{name}uid")},
            "spec": {"claimRef": {"name": pvc_name, "namespace": "ns"}},
        });
        if !secret_name.is_empty() {
            v["spec"]["csi"] = json!({
                "driver": "d",
                "volumeHandle": "h",
                "nodePublishSecretRef": {"name": secret_name, "namespace": "ns"},
            });
        }
        pv_from(v)
    }

    #[test]
    fn index() {
        let g = NodeGraph::new();
        g.set_destination_edge_threshold(3);

        let pod = |n: usize, node: usize| {
            let cm = |c: &str| json!({"name": c, "configMap": {"name": c}});
            let p = pod_from(json!({
                "metadata": {"name": format!("pod{n}"), "namespace": "ns", "uid": format!("pod{n}uid1")},
                "spec": {
                    "nodeName": format!("node{node}"),
                    "serviceAccountName": "sa1",
                    "serviceAccount": "sa1",
                    "containers": [],
                    "volumes": [cm("cm1"), cm("cm2"), cm("cm3")],
                },
            }));
            g.add_pod(&p);
            let ok = g.has_path_from(&format!("node{node}"), VertexType::ConfigMap, "ns", "cm1");
            assert_eq!(ok, Ok(true), "expected path from node{node} to cm1");
        };

        let threshold = 3;
        for i in 1..=threshold {
            pod(i, i);
            if i < threshold {
                // if we're under the threshold, no index expected
                assert_eq!(index_map(&g), expected(&[]));
            }
        }
        let all3 = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3"];
        assert_eq!(
            graph_map(&g),
            expected(&[
                ("node:node1", &[]),
                ("node:node2", &[]),
                ("node:node3", &[]),
                ("pod:ns/pod1", &["node:node1"]),
                ("pod:ns/pod2", &["node:node2"]),
                ("pod:ns/pod3", &["node:node3"]),
                ("configmap:ns/cm1", &all3),
                ("configmap:ns/cm2", &all3),
                ("configmap:ns/cm3", &all3),
                ("serviceAccount:ns/sa1", &all3),
            ])
        );
        let idx3 = ["node:node1=1", "node:node2=1", "node:node3=1"];
        assert_eq!(
            index_map(&g),
            expected(&[
                ("configmap:ns/cm1", &idx3),
                ("configmap:ns/cm2", &idx3),
                ("configmap:ns/cm3", &idx3),
                ("serviceAccount:ns/sa1", &idx3),
            ])
        );

        // delete one to drop below the threshold
        g.delete_pod("pod1", "ns");
        let two = ["pod:ns/pod2", "pod:ns/pod3"];
        assert_eq!(
            graph_map(&g),
            expected(&[
                ("node:node2", &[]),
                ("node:node3", &[]),
                ("pod:ns/pod2", &["node:node2"]),
                ("pod:ns/pod3", &["node:node3"]),
                ("configmap:ns/cm1", &two),
                ("configmap:ns/cm2", &two),
                ("configmap:ns/cm3", &two),
                ("serviceAccount:ns/sa1", &two),
            ])
        );
        assert_eq!(index_map(&g), expected(&[]));

        // add two to get above the threshold
        pod(1, 1);
        pod(4, 1);
        let all4 = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"];
        assert_eq!(
            graph_map(&g),
            expected(&[
                ("node:node1", &[]),
                ("node:node2", &[]),
                ("node:node3", &[]),
                ("pod:ns/pod1", &["node:node1"]),
                ("pod:ns/pod2", &["node:node2"]),
                ("pod:ns/pod3", &["node:node3"]),
                ("pod:ns/pod4", &["node:node1"]),
                ("configmap:ns/cm1", &all4),
                ("configmap:ns/cm2", &all4),
                ("configmap:ns/cm3", &all4),
                ("serviceAccount:ns/sa1", &all4),
            ])
        );
        let idx4 = ["node:node1=2", "node:node2=1", "node:node3=1"];
        assert_eq!(
            index_map(&g),
            expected(&[
                ("configmap:ns/cm1", &idx4),
                ("configmap:ns/cm2", &idx4),
                ("configmap:ns/cm3", &idx4),
                ("serviceAccount:ns/sa1", &idx4),
            ])
        );

        // delete one to remain above the threshold
        g.delete_pod("pod1", "ns");
        let three = ["pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"];
        assert_eq!(
            graph_map(&g),
            expected(&[
                ("node:node1", &[]),
                ("node:node2", &[]),
                ("node:node3", &[]),
                ("pod:ns/pod2", &["node:node2"]),
                ("pod:ns/pod3", &["node:node3"]),
                ("pod:ns/pod4", &["node:node1"]),
                ("configmap:ns/cm1", &three),
                ("configmap:ns/cm2", &three),
                ("configmap:ns/cm3", &three),
                ("serviceAccount:ns/sa1", &three),
            ])
        );
        assert_eq!(
            index_map(&g),
            expected(&[
                ("configmap:ns/cm1", &idx3),
                ("configmap:ns/cm2", &idx3),
                ("configmap:ns/cm3", &idx3),
                ("serviceAccount:ns/sa1", &idx3),
            ])
        );
    }

    #[test]
    fn index2() {
        type Transform = Box<dyn Fn(&NodeGraph)>;
        struct Case {
            desc: &'static str,
            starting: Transform,
            transform: Transform,
            graph: BTreeMap<String, Vec<String>>,
            index: BTreeMap<String, Vec<String>>,
        }
        let nothing = || -> Transform { Box::new(|_| {}) };
        let all3 = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3"];
        let two = ["pod:ns/pod2", "pod:ns/pod3"];
        let all4 = ["pod:ns/pod1", "pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"];
        let _ = all4;
        let idx3 = ["node:node1=1", "node:node2=1", "node:node3=1"];
        let three_pods_base: Vec<(&str, &[&str])> = vec![
            ("node:node1", &[]),
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("pod:ns/pod1", &["node:node1"]),
            ("pod:ns/pod2", &["node:node2"]),
            ("pod:ns/pod3", &["node:node3"]),
        ];
        let two_pods_base: Vec<(&str, &[&str])> = vec![
            ("node:node2", &[]),
            ("node:node3", &[]),
            ("pod:ns/pod2", &["node:node2"]),
            ("pod:ns/pod3", &["node:node3"]),
        ];
        let add3 = |make: fn(&str, &str) -> Pod| -> Transform {
            Box::new(move |g| {
                g.add_pod(&make("pod1", "node1"));
                g.add_pod(&make("pod2", "node2"));
                g.add_pod(&make("pod3", "node3"));
            })
        };
        let add2 = |make: fn(&str, &str) -> Pod| -> Transform {
            Box::new(move |g| {
                g.add_pod(&make("pod1", "node1"));
                g.add_pod(&make("pod2", "node2"));
            })
        };
        let add_third = |make: fn(&str, &str) -> Pod| -> Transform {
            Box::new(move |g| g.add_pod(&make("pod3", "node3")))
        };
        let delete_pod1: fn() -> Transform = || Box::new(|g| g.delete_pod("pod1", "ns"));

        let cases = vec![
            Case {
                desc: "empty graph",
                starting: nothing(),
                transform: nothing(),
                graph: expected(&[]),
                index: expected(&[]),
            },
            Case {
                desc: "outdeg below destination edge index threshold",
                starting: nothing(),
                transform: add2(pod_with_sa_and_cms),
                graph: expected(&[
                    ("node:node1", &[]),
                    ("node:node2", &[]),
                    ("pod:ns/pod1", &["node:node1"]),
                    ("pod:ns/pod2", &["node:node2"]),
                    ("configmap:ns/cm1", &["pod:ns/pod1", "pod:ns/pod2"]),
                    ("configmap:ns/cm2", &["pod:ns/pod1", "pod:ns/pod2"]),
                    ("configmap:ns/cm3", &["pod:ns/pod1", "pod:ns/pod2"]),
                    ("serviceAccount:ns/sa1", &["pod:ns/pod1", "pod:ns/pod2"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "index built for configmaps and serviceaccounts",
                starting: add2(pod_with_sa_and_cms),
                transform: add_third(pod_with_sa_and_cms),
                graph: with(
                    &three_pods_base,
                    &[
                        ("configmap:ns/cm1", &all3),
                        ("configmap:ns/cm2", &all3),
                        ("configmap:ns/cm3", &all3),
                        ("serviceAccount:ns/sa1", &all3),
                    ],
                ),
                index: expected(&[
                    ("configmap:ns/cm1", &idx3),
                    ("configmap:ns/cm2", &idx3),
                    ("configmap:ns/cm3", &idx3),
                    ("serviceAccount:ns/sa1", &idx3),
                ]),
            },
            Case {
                desc: "no index for configmaps and serviceaccounts - dropping below threshold",
                starting: add3(pod_with_sa_and_cms),
                transform: delete_pod1(),
                graph: with(
                    &two_pods_base,
                    &[
                        ("configmap:ns/cm1", &two),
                        ("configmap:ns/cm2", &two),
                        ("configmap:ns/cm3", &two),
                        ("serviceAccount:ns/sa1", &two),
                    ],
                ),
                index: expected(&[]),
            },
            Case {
                desc: "removing pod but staying above threshold",
                starting: Box::new(|g| {
                    g.add_pod(&pod_with_sa_and_cms("pod1", "node1"));
                    g.add_pod(&pod_with_sa_and_cms("pod2", "node2"));
                    g.add_pod(&pod_with_sa_and_cms("pod3", "node3"));
                    g.add_pod(&pod_with_sa_and_cms("pod4", "node1"));
                }),
                transform: delete_pod1(),
                graph: expected(&[
                    ("node:node1", &[]),
                    ("node:node2", &[]),
                    ("node:node3", &[]),
                    ("pod:ns/pod2", &["node:node2"]),
                    ("pod:ns/pod3", &["node:node3"]),
                    ("pod:ns/pod4", &["node:node1"]),
                    (
                        "configmap:ns/cm1",
                        &["pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"],
                    ),
                    (
                        "configmap:ns/cm2",
                        &["pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"],
                    ),
                    (
                        "configmap:ns/cm3",
                        &["pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"],
                    ),
                    (
                        "serviceAccount:ns/sa1",
                        &["pod:ns/pod2", "pod:ns/pod3", "pod:ns/pod4"],
                    ),
                ]),
                index: expected(&[
                    ("configmap:ns/cm1", &idx3),
                    ("configmap:ns/cm2", &idx3),
                    ("configmap:ns/cm3", &idx3),
                    ("serviceAccount:ns/sa1", &idx3),
                ]),
            },
            Case {
                desc: "index built for secrets",
                starting: add2(pod_with_secrets),
                transform: add_third(pod_with_secrets),
                graph: with(
                    &three_pods_base,
                    &[
                        ("secret:ns/s1", &all3),
                        ("secret:ns/s2", &all3),
                        ("secret:ns/s3", &all3),
                    ],
                ),
                index: expected(&[
                    ("secret:ns/s1", &idx3),
                    ("secret:ns/s2", &idx3),
                    ("secret:ns/s3", &idx3),
                ]),
            },
            Case {
                desc: "no index for secrets - dropping below threshold",
                starting: add3(pod_with_secrets),
                transform: delete_pod1(),
                graph: with(
                    &two_pods_base,
                    &[
                        ("secret:ns/s1", &two),
                        ("secret:ns/s2", &two),
                        ("secret:ns/s3", &two),
                    ],
                ),
                index: expected(&[]),
            },
            Case {
                desc: "index built for pvcs",
                starting: add2(pod_with_pvcs),
                transform: add_third(pod_with_pvcs),
                graph: with(
                    &three_pods_base,
                    &[
                        ("pvc:ns/pvc1", &all3),
                        ("pvc:ns/pvc2", &all3),
                        ("pvc:ns/pvc3", &all3),
                    ],
                ),
                index: expected(&[
                    ("pvc:ns/pvc1", &idx3),
                    ("pvc:ns/pvc2", &idx3),
                    ("pvc:ns/pvc3", &idx3),
                ]),
            },
            Case {
                desc: "no index for pvcs - dropping below threshold",
                starting: add3(pod_with_pvcs),
                transform: delete_pod1(),
                graph: with(
                    &two_pods_base,
                    &[
                        ("pvc:ns/pvc1", &two),
                        ("pvc:ns/pvc2", &two),
                        ("pvc:ns/pvc3", &two),
                    ],
                ),
                index: expected(&[]),
            },
            Case {
                desc: "index built for resourceclaims",
                starting: nothing(),
                transform: add3(pod_with_resource_claims),
                graph: with(
                    &three_pods_base,
                    &[
                        ("resourceclaim:ns/rc1", &all3),
                        ("resourceclaim:ns/rc2", &all3),
                        ("resourceclaim:ns/rc3", &all3),
                        ("resourceclaim:ns/rc4", &all3),
                    ],
                ),
                index: expected(&[
                    ("resourceclaim:ns/rc1", &idx3),
                    ("resourceclaim:ns/rc2", &idx3),
                    ("resourceclaim:ns/rc3", &idx3),
                    ("resourceclaim:ns/rc4", &idx3),
                ]),
            },
            Case {
                desc: "no index for resourceclaims - dropping below threshold",
                starting: add3(pod_with_resource_claims),
                transform: delete_pod1(),
                graph: with(
                    &two_pods_base,
                    &[
                        ("resourceclaim:ns/rc1", &two),
                        ("resourceclaim:ns/rc2", &two),
                        ("resourceclaim:ns/rc3", &two),
                        ("resourceclaim:ns/rc4", &two),
                    ],
                ),
                index: expected(&[]),
            },
            Case {
                desc: "resourceslices adding",
                starting: nothing(),
                transform: Box::new(|g| {
                    g.add_resource_slice("s1", "node1");
                    g.add_resource_slice("s2", "node2");
                    g.add_resource_slice("s3", "node3");
                }),
                graph: expected(&[
                    ("node:node1", &[]),
                    ("node:node2", &[]),
                    ("node:node3", &[]),
                    ("resourceslice:s1", &["node:node1"]),
                    ("resourceslice:s2", &["node:node2"]),
                    ("resourceslice:s3", &["node:node3"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "resourceslices deleting",
                starting: Box::new(|g| {
                    g.add_resource_slice("s1", "node1");
                    g.add_resource_slice("s2", "node2");
                    g.add_resource_slice("s3", "node3");
                }),
                transform: Box::new(|g| g.delete_resource_slice("s1")),
                graph: expected(&[
                    ("node:node2", &[]),
                    ("node:node3", &[]),
                    ("resourceslice:s2", &["node:node2"]),
                    ("resourceslice:s3", &["node:node3"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "volumeattachments adding",
                starting: nothing(),
                transform: Box::new(|g| {
                    g.add_volume_attachment("va1", "node1");
                    g.add_volume_attachment("va2", "node2");
                    g.add_volume_attachment("va3", "node3");
                }),
                graph: expected(&[
                    ("node:node1", &[]),
                    ("node:node2", &[]),
                    ("node:node3", &[]),
                    ("volumeattachment:va1", &["node:node1"]),
                    ("volumeattachment:va2", &["node:node2"]),
                    ("volumeattachment:va3", &["node:node3"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "volumeattachments deleting",
                starting: Box::new(|g| {
                    g.add_volume_attachment("va1", "node1");
                    g.add_volume_attachment("va2", "node2");
                    g.add_volume_attachment("va3", "node3");
                }),
                transform: Box::new(|g| g.delete_volume_attachment("va1")),
                graph: expected(&[
                    ("node:node2", &[]),
                    ("node:node3", &[]),
                    ("volumeattachment:va2", &["node:node2"]),
                    ("volumeattachment:va3", &["node:node3"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "persistentvolumes adding",
                starting: nothing(),
                transform: Box::new(|g| {
                    g.add_pv(&pv("pv1", "pvc1", ""));
                    g.add_pv(&pv("pv2", "pvc2", ""));
                    g.add_pv(&pv("pv3", "pvc3", ""));
                }),
                graph: expected(&[
                    ("pv:pv1", &["pvc:ns/pvc1"]),
                    ("pv:pv2", &["pvc:ns/pvc2"]),
                    ("pv:pv3", &["pvc:ns/pvc3"]),
                    ("pvc:ns/pvc1", &[]),
                    ("pvc:ns/pvc2", &[]),
                    ("pvc:ns/pvc3", &[]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "persistentvolumes deleting",
                starting: Box::new(|g| {
                    g.add_pv(&pv("pv1", "pvc1", ""));
                    g.add_pv(&pv("pv2", "pvc2", ""));
                    g.add_pv(&pv("pv3", "pvc3", ""));
                }),
                transform: Box::new(|g| g.delete_pv("pv1")),
                graph: expected(&[
                    ("pv:pv2", &["pvc:ns/pvc2"]),
                    ("pv:pv3", &["pvc:ns/pvc3"]),
                    ("pvc:ns/pvc2", &[]),
                    ("pvc:ns/pvc3", &[]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "persistentvolumes with secrets",
                starting: nothing(),
                transform: Box::new(|g| {
                    g.add_pv(&pv("pv1", "pvc1", "s1"));
                    g.add_pv(&pv("pv2", "pvc2", "s2"));
                    g.add_pv(&pv("pv3", "pvc3", "s3"));
                }),
                graph: expected(&[
                    ("pv:pv1", &["pvc:ns/pvc1"]),
                    ("pv:pv2", &["pvc:ns/pvc2"]),
                    ("pv:pv3", &["pvc:ns/pvc3"]),
                    ("pvc:ns/pvc1", &[]),
                    ("pvc:ns/pvc2", &[]),
                    ("pvc:ns/pvc3", &[]),
                    ("secret:ns/s1", &["pv:pv1"]),
                    ("secret:ns/s2", &["pv:pv2"]),
                    ("secret:ns/s3", &["pv:pv3"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "podcertificaterequest adding",
                starting: nothing(),
                transform: Box::new(|g| {
                    g.add_pod_certificate_request(&pcr("foo", "pcr1", "pod1", "sa1", "node1"));
                    g.add_pod_certificate_request(&pcr("foo", "pcr2", "pod1", "sa1", "node1"));
                    g.add_pod_certificate_request(&pcr("foo", "pcr3", "pod2", "sa2", "node1"));
                    g.add_pod_certificate_request(&pcr("foo", "pcr4", "pod4", "sa4", "node2"));
                }),
                graph: expected(&[
                    ("node:node1", &[]),
                    ("node:node2", &[]),
                    ("podcertificaterequest:foo/pcr1", &["node:node1"]),
                    ("podcertificaterequest:foo/pcr2", &["node:node1"]),
                    ("podcertificaterequest:foo/pcr3", &["node:node1"]),
                    ("podcertificaterequest:foo/pcr4", &["node:node2"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "podcertificaterequest deleting",
                starting: Box::new(|g| {
                    g.add_pod_certificate_request(&pcr("foo", "pcr1", "pod1", "sa1", "node1"));
                    g.add_pod_certificate_request(&pcr("foo", "pcr2", "pod1", "sa1", "node1"));
                    g.add_pod_certificate_request(&pcr("foo", "pcr3", "pod2", "sa2", "node1"));
                    g.add_pod_certificate_request(&pcr("foo", "pcr4", "pod4", "sa4", "node2"));
                }),
                transform: Box::new(|g| {
                    g.delete_pod_certificate_request(&pcr("foo", "pcr3", "", "", ""));
                    g.delete_pod_certificate_request(&pcr("foo", "pcr4", "", "", ""));
                }),
                graph: expected(&[
                    ("node:node1", &[]),
                    ("podcertificaterequest:foo/pcr1", &["node:node1"]),
                    ("podcertificaterequest:foo/pcr2", &["node:node1"]),
                ]),
                index: expected(&[]),
            },
            Case {
                desc: "podcertificaterequest deleting (check namespace/name ordering)",
                starting: Box::new(|g| {
                    g.add_pod_certificate_request(&pcr("foo", "bar", "pod1", "sa1", "node1"));
                    g.add_pod_certificate_request(&pcr("bar", "foo", "pod2", "sa2", "node2"));
                }),
                transform: Box::new(|g| {
                    g.delete_pod_certificate_request(&pcr("foo", "bar", "", "", ""));
                }),
                graph: expected(&[
                    ("node:node2", &[]),
                    ("podcertificaterequest:bar/foo", &["node:node2"]),
                ]),
                index: expected(&[]),
            },
        ];

        for c in cases {
            let g = test_graph();
            (c.starting)(&g);
            (c.transform)(&g);
            assert_eq!(graph_map(&g), c.graph, "{}: graph", c.desc);
            assert_eq!(index_map(&g), c.index, "{}: index", c.desc);
        }
    }

    #[test]
    fn mirror_pod_gets_only_its_node_edge() {
        // graph.go AddPod: "A node must never be able to create a pod that grants
        // them permissions on other API objects."
        let g = NodeGraph::new();
        let mut p = json!({
            "metadata": {
                "name": "mirror", "namespace": "ns",
                "annotations": {"kubernetes.io/config.mirror": "hash"},
            },
            "spec": {
                "nodeName": "node1",
                "serviceAccountName": "sa",
                "containers": [],
                "volumes": [{"name": "s", "secret": {"secretName": "s1"}}],
            },
        });
        g.add_pod(&pod_from(p.clone()));
        assert_eq!(
            graph_map(&g),
            expected(&[("node:node1", &[]), ("pod:ns/mirror", &["node:node1"])])
        );
        p["metadata"]["annotations"] = json!({});
        g.add_pod(&pod_from(p));
        assert!(graph_map(&g).contains_key("secret:ns/s1"));
    }

    #[test]
    fn pod_reference_visitors() {
        let p = pod_from(json!({
            "metadata": {"name": "p", "namespace": "ns"},
            "spec": {
                "nodeName": "n",
                "imagePullSecrets": [{"name": "pull"}, {"name": ""}],
                "initContainers": [{"name": "i", "envFrom": [{"secretRef": {"name": "init-envfrom"}}]}],
                "containers": [{
                    "name": "c",
                    "env": [
                        {"name": "A", "valueFrom": {"secretKeyRef": {"name": "env-secret", "key": "k"}}},
                        {"name": "B", "valueFrom": {"configMapKeyRef": {"name": "env-cm", "key": "k"}}},
                    ],
                    "envFrom": [
                        {"secretRef": {"name": "envfrom-secret"}},
                        {"configMapRef": {"name": "envfrom-cm"}},
                    ],
                }],
                "ephemeralContainers": [{"name": "e", "envFrom": [{"configMapRef": {"name": "eph-cm"}}]}],
                "volumes": [
                    {"name": "v1", "secret": {"secretName": "vol-secret"}},
                    {"name": "v2", "configMap": {"name": "vol-cm"}},
                    {"name": "v3", "projected": {"sources": [
                        {"secret": {"name": "proj-secret"}}, {"configMap": {"name": "proj-cm"}}]}},
                    {"name": "v4", "csi": {"driver": "d", "nodePublishSecretRef": {"name": "csi-secret"}}},
                    {"name": "v5", "cephfs": {"monitors": [], "secretRef": {"name": "ceph-secret"}}},
                    {"name": "v6", "azureFile": {"secretName": "azure-secret", "shareName": "s"}},
                    {"name": "v7", "iscsi": {"targetPortal": "t", "iqn": "i", "lun": 0, "secretRef": {"name": "iscsi-secret"}}},
                ],
            },
        }));
        let mut secrets = visit_pod_secret_names(&p);
        secrets.sort();
        assert_eq!(
            secrets,
            vec![
                "azure-secret",
                "ceph-secret",
                "csi-secret",
                "env-secret",
                "envfrom-secret",
                "init-envfrom",
                "iscsi-secret",
                "proj-secret",
                "pull",
                "vol-secret",
            ]
        );
        let mut cms = visit_pod_configmap_names(&p);
        cms.sort();
        assert_eq!(
            cms,
            vec!["env-cm", "envfrom-cm", "eph-cm", "proj-cm", "vol-cm"]
        );
    }

    #[test]
    fn pv_secret_visitor() {
        // CSI: controller refs are not kubelet visible.
        let v = pv_from(json!({
            "metadata": {"name": "pv"},
            "spec": {"claimRef": {"name": "c", "namespace": "claimns"}, "csi": {
                "driver": "d", "volumeHandle": "h",
                "controllerPublishSecretRef": {"name": "cp", "namespace": "ns1"},
                "nodeStageSecretRef": {"name": "ns", "namespace": "ns2"},
            }},
        }));
        assert_eq!(
            visit_pv_secret_names(&v),
            vec![
                ("ns1".to_string(), "cp".to_string(), false),
                ("ns2".to_string(), "ns".to_string(), true)
            ]
        );
        // rbd without a secret namespace uses the claimRef namespace.
        let v = pv_from(json!({
            "metadata": {"name": "pv"},
            "spec": {"claimRef": {"name": "c", "namespace": "claimns"},
                     "rbd": {"monitors": [], "image": "i", "secretRef": {"name": "r"}}},
        }));
        assert_eq!(
            visit_pv_secret_names(&v),
            vec![("claimns".to_string(), "r".to_string(), true)]
        );
        // Only kubelet-visible secrets become graph edges.
        let g = NodeGraph::new();
        g.add_pv(&pv_from(json!({
            "metadata": {"name": "pv"},
            "spec": {"claimRef": {"name": "c", "namespace": "ns"}, "csi": {
                "driver": "d", "volumeHandle": "h",
                "controllerPublishSecretRef": {"name": "hidden", "namespace": "ns"},
            }},
        })));
        assert!(!graph_map(&g).contains_key("secret:ns/hidden"));
    }

    #[test]
    fn populator_skips_unchanged_pods_and_rebuilds_for_new_ephemeral_containers() {
        // graph_populator.go updatePod
        let g = std::sync::Arc::new(NodeGraph::new());
        let pop = GraphPopulator::new(g.clone());
        let base = json!({
            "metadata": {"name": "p", "namespace": "ns", "uid": "u"},
            "spec": {"nodeName": "n1", "containers": [],
                     "volumes": [{"name": "v", "secret": {"secretName": "s1"}}]},
        });
        let old = pod_from(base.clone());
        pop.add_pod(&old);
        assert!(graph_map(&g).contains_key("secret:ns/s1"));

        // no node assigned: ignored
        let mut unbound = base.clone();
        unbound["spec"]["nodeName"] = json!("");
        unbound["metadata"]["name"] = json!("unbound");
        pop.add_pod(&pod_from(unbound));
        assert!(!graph_map(&g).contains_key("pod:ns/unbound"));

        // unchanged: the graph is not touched even if the new object differs in
        // fields that cannot add references.
        let mut same = base.clone();
        same["spec"]["volumes"] = json!([{"name": "v", "secret": {"secretName": "s2"}}]);
        pop.update_pod(Some(&old), &pod_from(same));
        assert!(!graph_map(&g).contains_key("secret:ns/s2"));

        // a new ephemeral container re-adds the pod
        let mut eph = base;
        eph["spec"]["ephemeralContainers"] =
            json!([{"name": "e", "envFrom": [{"secretRef": {"name": "new-secret"}}]}]);
        pop.update_pod(Some(&old), &pod_from(eph));
        assert!(graph_map(&g).contains_key("secret:ns/new-secret"));
    }

    #[test]
    fn volume_attachment_update_skips_identical_node() {
        let g = std::sync::Arc::new(NodeGraph::new());
        let pop = GraphPopulator::new(g.clone());
        pop.update_volume_attachment(None, "va", "node1");
        assert!(graph_map(&g).contains_key("volumeattachment:va"));
        g.delete_volume_attachment("va");
        pop.update_volume_attachment(Some("node1"), "va", "node1");
        assert!(!graph_map(&g).contains_key("volumeattachment:va"));
        pop.update_volume_attachment(Some("node1"), "va", "node2");
        assert_eq!(
            graph_map(&g).get("volumeattachment:va"),
            Some(&vec!["node:node2".to_string()])
        );
    }
}
