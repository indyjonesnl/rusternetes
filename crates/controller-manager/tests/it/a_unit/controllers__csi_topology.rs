//! Unit tests moved from `src/controllers/csi_topology.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::csi::CSINode;
    pub use rusternetes_common::resources::volume::{
        NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, TopologySelectorTerm,
        VolumeNodeAffinity,
    };
    pub use rusternetes_common::resources::Node;
    pub use rusternetes_common::Error;
    pub use rusternetes_csi::proto::{Topology, TopologyRequirement};
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::cmp::Ordering;
    pub use std::collections::{BTreeSet, HashMap, HashSet};
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::csi::{CSINodeDriver, CSINodeSpec};
    use rusternetes_common::resources::volume::TopologySelectorLabelRequirement;
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_controller_manager::controllers::csi_topology::*;
    use rusternetes_storage::memory::MemoryStorage;

    const DRIVER: &str = "com.example.csi/driver";
    const ZONE: &str = "com.example.csi/zone";
    const RACK: &str = "com.example.csi/rack";

    fn seg(pairs: &[(&str, &str)]) -> Topology {
        Topology {
            segments: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn term(pairs: &[(&str, &str)]) -> TopologyTerm {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn allowed(exprs: &[(&str, &[&str])]) -> TopologySelectorTerm {
        TopologySelectorTerm {
            match_label_expressions: Some(
                exprs
                    .iter()
                    .map(|(k, vs)| TopologySelectorLabelRequirement {
                        key: k.to_string(),
                        values: vs.iter().map(|v| v.to_string()).collect(),
                    })
                    .collect(),
            ),
        }
    }

    // ---- TestGenerateVolumeNodeAffinity (topology_test.go:47) ----

    #[test]
    fn node_affinity_of_no_topology_is_nil() {
        assert!(generate_volume_node_affinity(&[]).is_none());
    }

    #[test]
    fn node_affinity_has_one_term_per_topology_and_one_in_expression_per_segment() {
        let a = generate_volume_node_affinity(&[
            seg(&[(ZONE, "zone1"), (RACK, "rack2")]),
            seg(&[(ZONE, "zone2")]),
        ])
        .unwrap();
        let terms = a.required.unwrap().node_selector_terms;
        assert_eq!(terms.len(), 2);
        let e = terms[0].match_expressions.as_ref().unwrap();
        assert_eq!(e.len(), 2);
        assert_eq!((e[0].key.as_str(), e[0].operator.as_str()), (RACK, "In"));
        assert_eq!(e[1].values.as_deref(), Some(&["zone1".to_string()][..]));
        assert_eq!(
            terms[1].match_expressions.as_ref().unwrap()[0]
                .values
                .as_deref(),
            Some(&["zone2".to_string()][..])
        );
    }

    #[test]
    fn node_affinity_skips_empty_segments() {
        let a = generate_volume_node_affinity(&[seg(&[])]).unwrap();
        assert!(a.required.unwrap().node_selector_terms.is_empty());
    }

    // ---- TestTopologyTermSort / Compare / Subset (topology_test.go:2464-2619) ----

    #[test]
    fn terms_compare_by_length_then_segments() {
        let a = term(&[("a", "1")]);
        let b = term(&[("a", "1"), ("b", "1")]);
        assert_eq!(compare(&a, &b), Ordering::Less);
        assert_eq!(compare(&b, &a), Ordering::Greater);
        assert_eq!(compare(&a, &a), Ordering::Equal);
        assert_eq!(
            compare(&term(&[("a", "1")]), &term(&[("a", "2")])),
            Ordering::Less
        );
        assert_eq!(
            compare(&term(&[("a", "9")]), &term(&[("b", "1")])),
            Ordering::Less
        );
    }

    #[test]
    fn subset_follows_topology_term_subset() {
        let sel = term(&[("rack", "r1"), ("zone", "z1")]);
        assert!(subset(&term(&[]), &sel));
        assert!(subset(&term(&[("zone", "z1")]), &sel));
        assert!(subset(&sel, &sel));
        assert!(!subset(&term(&[("zone", "z2")]), &sel));
        assert!(!subset(&term(&[("host", "h")]), &sel));
        assert!(!subset(&term(&[("rack", "r1"), ("host", "h")]), &sel));
    }

    // ---- TestAllowedTopologies (topology_test.go:465): flatten ----

    #[test]
    fn flatten_distributes_or_over_and() {
        let f = flatten(&[allowed(&[(ZONE, &["zone1"]), (RACK, &["rackA", "rackB"])])]);
        assert_eq!(
            f,
            vec![
                term(&[(RACK, "rackA"), (ZONE, "zone1")]),
                term(&[(RACK, "rackB"), (ZONE, "zone1")]),
            ]
        );
        let f = flatten(&[allowed(&[(ZONE, &["z1", "z2"]), (RACK, &["rA", "rB"])])]);
        assert_eq!(f.len(), 4);
        // several allowed terms are OR'd.
        let f = flatten(&[allowed(&[(ZONE, &["z1"])]), allowed(&[(ZONE, &["z2"])])]);
        assert_eq!(f, vec![term(&[(ZONE, "z1")]), term(&[(ZONE, "z2")])]);
    }

    // ---- TestStatefulSetSpreading (topology_test.go:163) ----

    #[test]
    fn fnv1_32_matches_go_hash_fnv() {
        // Go: fnv.New32() over "a" / "testset" / "foobar".
        let (h, i) = pvc_name_hash_and_index_offset("a");
        assert_eq!((h, i), (84696446, 0));
        let (h, i) = pvc_name_hash_and_index_offset("foobar");
        assert_eq!((h, i), (837857890, 0));
        // `ClaimName-StatefulSetName-Id`: only StatefulSetName is hashed.
        let (h, i) = pvc_name_hash_and_index_offset("testpvcA-testset-1");
        assert_eq!((h, i), (1633848515, 1));
        let (h2, _) = pvc_name_hash_and_index_offset("testpvcB-testset-7");
        assert_eq!(h2, 1633848515);
        // `Name-Id` hashes `Name`.
        let (h, i) = pvc_name_hash_and_index_offset("a-3");
        assert_eq!((h, i), (84696446, 3));
    }

    // ---- GenerateAccessibilityRequirements over a MemoryStorage ----

    struct Cluster {
        storage: MemoryStorage,
        store: TopologyStore,
    }

    impl Cluster {
        fn new() -> Self {
            Self {
                storage: MemoryStorage::new(),
                store: TopologyStore::new(),
            }
        }

        /// A node with `labels`; the driver is registered with `keys` on its
        /// CSINode when `keys` is `Some`.
        async fn node(&self, name: &str, labels: &[(&str, &str)], keys: Option<&[&str]>) {
            let mut n = Node::new(name);
            n.metadata.labels = Some(
                labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
            self.storage
                .create(&build_key("nodes", None, name), &n)
                .await
                .unwrap();
            if let Some(keys) = keys {
                let cn = CSINode {
                    type_meta: TypeMeta {
                        kind: "CSINode".into(),
                        api_version: "storage.k8s.io/v1".into(),
                    },
                    metadata: ObjectMeta::new(name),
                    spec: CSINodeSpec {
                        drivers: vec![CSINodeDriver {
                            name: DRIVER.into(),
                            node_id: name.into(),
                            topology_keys: Some(keys.iter().map(|k| k.to_string()).collect()),
                            allocatable: None,
                        }],
                    },
                };
                self.storage
                    .create(&build_key("csinodes", None, name), &cn)
                    .await
                    .unwrap();
            }
        }

        async fn generate(
            &self,
            pvc_name: &str,
            allowed: &[TopologySelectorTerm],
            selected: &str,
            strict: bool,
            immediate: bool,
        ) -> Result<Option<TopologyRequirement>, TopologyError> {
            TopologyInputs {
                storage: &self.storage,
                store: &self.store,
                driver_name: DRIVER,
                pvc_uid: "uid-1",
                pvc_name,
                allowed_topologies: allowed,
                selected_node_name: selected,
                strict_topology: strict,
                immediate_topology: immediate,
            }
            .generate_accessibility_requirements()
            .await
        }
    }

    const BOTH: &[&str] = &[ZONE, RACK];

    async fn four_nodes() -> Cluster {
        let c = Cluster::new();
        for (i, (z, r)) in [
            ("zone1", "rackA"),
            ("zone2", "rackB"),
            ("zone3", "rackC"),
            ("zone4", "rackD"),
        ]
        .iter()
        .enumerate()
        {
            c.node(&format!("node{i}"), &[(ZONE, z), (RACK, r)], Some(BOTH))
                .await;
        }
        c
    }

    #[tokio::test]
    async fn statefulset_pvcs_rotate_the_preferred_terms_by_their_ordinal() {
        let c = four_nodes().await;
        // hash("testset") = 1633848515, 1633848515 % 4 == 3.
        let r = c
            .generate("testpvcA-testset-0", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite.len(), 4);
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone4"), (RACK, "rackD")]));
        assert_eq!(r.preferred[1], seg(&[(ZONE, "zone1"), (RACK, "rackA")]));
        // ordinal 1 shifts by one: (3 + 1) % 4 == 0.
        c.store.delete("uid-1");
        let r = c
            .generate("testpvcA-testset-1", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.preferred, r.requisite);
        // The other claim of the same StatefulSet member lands in the same place.
        c.store.delete("uid-1");
        let r2 = c
            .generate("testpvcB-testset-1", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2.preferred, r.preferred);
    }

    #[tokio::test]
    async fn allowed_topologies_become_the_requisite_terms() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone2", "zone1"])])];
        let r = c
            .generate("p-0", &a, "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.requisite,
            vec![seg(&[(ZONE, "zone1")]), seg(&[(ZONE, "zone2")])]
        );
        assert_eq!(r.preferred.len(), 2);
    }

    #[tokio::test]
    async fn immediate_binding_without_immediate_topology_sends_nothing() {
        let c = four_nodes().await;
        assert!(c
            .generate("p-0", &[], "", false, false)
            .await
            .unwrap()
            .is_none());
        // allowedTopologies still apply.
        let a = [allowed(&[(ZONE, &["zone1"])])];
        assert!(c
            .generate("p-0", &a, "", false, false)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn aggregation_only_reports_nodes_whose_driver_registered_the_keys() {
        let c = Cluster::new();
        c.node("a", &[(ZONE, "z1")], Some(&[ZONE])).await;
        // Labelled but the driver is not registered there.
        c.node("b", &[(ZONE, "z2")], None).await;
        // Registered but without the label.
        c.node("c", &[], Some(&[ZONE])).await;
        let r = c
            .generate("p-0", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite, vec![seg(&[(ZONE, "z1")])]);
    }

    #[tokio::test]
    async fn aggregation_covers_every_distinct_key_set() {
        // TestTopologyAggregation: nodes may report different granularities.
        let c = Cluster::new();
        c.node("a", &[(ZONE, "z1")], Some(&[ZONE])).await;
        c.node("b", &[(ZONE, "z2"), (RACK, "r1")], Some(BOTH)).await;
        let r = c
            .generate("p-0", &[], "", false, true)
            .await
            .unwrap()
            .unwrap();
        // Sorted shortest first: the coarse term, then the finer one.
        assert_eq!(
            r.requisite,
            vec![
                seg(&[(ZONE, "z1")]),
                seg(&[(ZONE, "z2")]),
                seg(&[(ZONE, "z2"), (RACK, "r1")]),
            ]
        );
    }

    #[tokio::test]
    async fn no_registered_topology_keys_means_no_available_topology() {
        let c = Cluster::new();
        c.node("a", &[(ZONE, "z1")], None).await;
        let e = c.generate("p-0", &[], "", false, true).await.unwrap_err();
        assert_eq!(e.to_string(), "no available topology found");
    }

    #[tokio::test]
    async fn keys_registered_but_labels_absent_fail() {
        let c = Cluster::new();
        c.node("a", &[], Some(&[ZONE])).await;
        let e = c.generate("p-0", &[], "", false, true).await.unwrap_err();
        assert!(e.to_string().contains("were not found on any nodes"), "{e}");
    }

    // ---- TestPreferredTopologies (topology_test.go:1493) ----

    #[tokio::test]
    async fn delayed_binding_prefers_the_selected_node_and_wraps_around() {
        let c = four_nodes().await;
        let r = c
            .generate("p", &[], "node2", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite.len(), 4);
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone3"), (RACK, "rackC")]));
        assert_eq!(r.preferred[1], seg(&[(ZONE, "zone4"), (RACK, "rackD")]));
        assert_eq!(r.preferred[2], seg(&[(ZONE, "zone1"), (RACK, "rackA")]));
    }

    #[tokio::test]
    async fn strict_topology_passes_only_the_selected_node() {
        let c = four_nodes().await;
        let r = c
            .generate("p", &[], "node1", true, true)
            .await
            .unwrap()
            .unwrap();
        let only = vec![seg(&[(ZONE, "zone2"), (RACK, "rackB")])];
        assert_eq!(r.requisite, only);
        assert_eq!(r.preferred, only);
    }

    #[tokio::test]
    async fn strict_topology_rejects_a_node_outside_allowed_topologies() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone1"])])];
        let e = c.generate("p", &a, "node1", true, true).await.unwrap_err();
        assert!(
            e.to_string().contains("is not in allowed topologies"),
            "{e}"
        );
        // ... and accepts one inside it.
        // (a final error drops the PVC's cache entry in the provisioner)
        c.store.delete("uid-1");
        let r = c
            .generate("p", &a, "node0", true, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.requisite.len(), 1);
    }

    #[tokio::test]
    async fn non_strict_delayed_binding_with_allowed_topologies_uses_them() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone1", "zone2"])])];
        let r = c
            .generate("p", &a, "node1", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.requisite,
            vec![seg(&[(ZONE, "zone1")]), seg(&[(ZONE, "zone2")])]
        );
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone2")]));
    }

    #[tokio::test]
    async fn a_selected_node_outside_requisite_is_an_error() {
        let c = four_nodes().await;
        let a = [allowed(&[(ZONE, &["zone1"])])];
        let e = c.generate("p", &a, "node3", false, true).await.unwrap_err();
        assert!(e.to_string().contains("from selected node"), "{e}");
    }

    // ---- error cases of GenerateAccessibilityRequirements ----

    #[tokio::test]
    async fn a_missing_csinode_is_not_found_so_the_claim_reschedules() {
        let c = four_nodes().await;
        let e = c
            .generate("p", &[], "ghost", false, true)
            .await
            .unwrap_err();
        assert!(matches!(e, TopologyError::NotFound(_)), "{e}");
    }

    #[tokio::test]
    async fn a_csinode_without_topology_keys_is_an_error() {
        let c = Cluster::new();
        c.node("n", &[(ZONE, "z")], Some(&[])).await;
        let e = c.generate("p", &[], "n", false, true).await.unwrap_err();
        assert_eq!(e.to_string(), "no topology key found for node n");
    }

    #[tokio::test]
    async fn a_node_missing_a_registered_label_is_an_error() {
        // TestNodeTopologyLabelFallback.
        let c = Cluster::new();
        c.node("n", &[(ZONE, "z")], Some(BOTH)).await;
        let e = c.generate("p", &[], "n", false, true).await.unwrap_err();
        assert!(
            e.to_string()
                .contains("does not match topology keys from CSINode"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn a_deleted_node_is_served_from_the_cache_until_the_entry_is_dropped() {
        // TestProvisionWithDeletedNodeFromCache.
        let c = four_nodes().await;
        c.generate("p", &[], "node0", false, true).await.unwrap();
        c.storage
            .delete(&build_key("nodes", None, "node0"))
            .await
            .unwrap();
        let r = c
            .generate("p", &[], "node0", false, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.preferred[0], seg(&[(ZONE, "zone1"), (RACK, "rackA")]));
        c.store.delete("uid-1");
        assert!(c.generate("p", &[], "node0", false, true).await.is_err());
    }

    #[test]
    fn distinct_key_sets_are_sorted_and_deduplicated() {
        // TestDistinctTopologyKeySets.
        let mk = |name: &str, keys: Option<&[&str]>| CSINode {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new(name),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: DRIVER.into(),
                    node_id: String::new(),
                    topology_keys: keys.map(|k| k.iter().map(|s| s.to_string()).collect()),
                    allocatable: None,
                }],
            },
        };
        let nodes = [
            mk("a", Some(&["z", "r"])),
            mk("b", Some(&["r", "z"])),
            mk("c", Some(&["z"])),
            mk("d", None),
        ];
        assert_eq!(
            distinct_topology_key_sets(&nodes, DRIVER),
            vec![
                vec!["r".to_string(), "z".to_string()],
                vec!["z".to_string()]
            ]
        );
        let reg = registered_topology_keys(&nodes, DRIVER);
        assert_eq!(reg.len(), 3);
        assert!(!reg.contains_key("d"));
    }

    fn affinity(terms: Vec<NodeSelectorTerm>) -> VolumeNodeAffinity {
        VolumeNodeAffinity {
            required: Some(NodeSelector {
                node_selector_terms: terms,
            }),
        }
    }

    fn req_term(key: &str, op: &str, values: &[&str]) -> NodeSelectorTerm {
        NodeSelectorTerm {
            match_expressions: Some(vec![NodeSelectorRequirement {
                key: key.into(),
                operator: op.into(),
                values: Some(values.iter().map(|v| v.to_string()).collect()),
            }]),
            match_fields: None,
        }
    }

    /// `VolumeIsAccessible` (topology.go:85-99).
    #[test]
    fn volume_is_accessible_follows_match_node_selector_terms() {
        let topo: HashMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        // No topology / no affinity / no required => accessible.
        assert!(volume_is_accessible(
            Some(&affinity(vec![req_term("zone", "In", &["b"])])),
            None
        ));
        assert!(volume_is_accessible(None, Some(&topo)));
        assert!(volume_is_accessible(
            Some(&VolumeNodeAffinity { required: None }),
            Some(&topo)
        ));
        let acc = |terms| volume_is_accessible(Some(&affinity(terms)), Some(&topo));
        assert!(acc(vec![req_term("zone", "In", &["a"])]));
        assert!(!acc(vec![req_term("zone", "In", &["b"])]));
        // Terms are OR'd.
        assert!(acc(vec![
            req_term("zone", "In", &["b"]),
            req_term("zone", "Exists", &[])
        ]));
        assert!(acc(vec![req_term("zone", "NotIn", &["b"])]));
        assert!(!acc(vec![req_term("rack", "Exists", &[])]));
        assert!(acc(vec![req_term("rack", "DoesNotExist", &[])]));
        // An empty term matches nothing; no terms match nothing.
        assert!(!acc(vec![NodeSelectorTerm {
            match_expressions: None,
            match_fields: None
        }]));
        assert!(!acc(vec![]));
    }
}
