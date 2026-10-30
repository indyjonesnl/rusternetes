//! Unit tests moved from `src/csi_translation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::pod::Volume;
    pub use rusternetes_common::resources::volume::PersistentVolumeMode;
    pub use rusternetes_common::resources::volume::{
        AWSElasticBlockStoreVolumeSource, CSIVolumeSource, GCEPersistentDiskVolumeSource,
        NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, PersistentVolumeAccessMode,
        SecretReference, StorageClass, TopologySelectorLabelRequirement, TopologySelectorTerm,
        VolumeNodeAffinity,
    };
    pub use rusternetes_common::resources::PersistentVolume;
    pub use rusternetes_common::types::ObjectMeta;
    pub use std::collections::{BTreeSet, HashMap};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::csi_translation::*;
    use rusternetes_common::resources::volume::{
        AWSElasticBlockStoreVolumeSource, GCEPersistentDiskVolumeSource, NodeSelector,
        NodeSelectorRequirement, NodeSelectorTerm, PersistentVolumeAccessMode, VolumeNodeAffinity,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use std::collections::HashMap;

    const ZONE_BETA: &str = "failure-domain.beta.kubernetes.io/zone";
    const ZONE_GA: &str = "topology.kubernetes.io/zone";

    fn sc(params: &[(&str, &str)], topo: Option<Vec<TopologySelectorTerm>>) -> StorageClass {
        StorageClass {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("sc"),
            provisioner: String::new(),
            parameters: Some(
                params
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            reclaim_policy: None,
            volume_binding_mode: None,
            allowed_topologies: topo,
            allow_volume_expansion: None,
            mount_options: None,
        }
    }

    fn topo(key: &str, values: &[&str]) -> Option<Vec<TopologySelectorTerm>> {
        Some(vec![TopologySelectorTerm {
            match_label_expressions: Some(vec![TopologySelectorLabelRequirement {
                key: key.into(),
                values: values.iter().map(|v| v.to_string()).collect(),
            }]),
        }])
    }

    fn sc_eq(a: &StorageClass, b: &StorageClass) -> bool {
        a.parameters == b.parameters
            && serde_json::to_value(&a.allowed_topologies).unwrap()
                == serde_json::to_value(&b.allowed_topologies).unwrap()
    }

    /// `TestTranslatePDInTreeStorageClassToCSI` (`plugins/gce_pd_test.go:38`).
    #[test]
    fn gce_pd_storage_class_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        let tr = |c: &StorageClass| {
            t.translate_in_tree_storage_class_to_csi(GCE_PD_IN_TREE_PLUGIN_NAME, c)
        };
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = tr(&input).expect("translates");
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[("foo", "bar")], None), sc(&[("foo", "bar")], None));
        ok(
            sc(&[("fstype", "myfs")], None),
            sc(&[("csi.storage.k8s.io/fstype", "myfs")], None),
        );
        ok(sc(&[], None), sc(&[], None));
        ok(
            sc(&[("zone", "foo")], None),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
        );
        ok(
            sc(&[("zones", "foo,bar,baz")], None),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo", "bar", "baz"])),
        );
        ok(
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
        );
        ok(
            sc(&[], topo(LABEL_FAILURE_DOMAIN_BETA_ZONE, &["foo"])),
            sc(&[], topo(GCE_PD_TOPOLOGY_KEY, &["foo"])),
        );
        assert!(tr(&sc(&[("zone", "foo")], topo(GCE_PD_TOPOLOGY_KEY, &["foo"]))).is_err());
    }

    /// `TestTranslateEBSInTreeStorageClassToCSI` (`plugins/aws_ebs_test.go:83`).
    #[test]
    fn aws_ebs_storage_class_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        let tr = |c: &StorageClass| {
            t.translate_in_tree_storage_class_to_csi(AWS_EBS_IN_TREE_PLUGIN_NAME, c)
        };
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = tr(&input).expect("translates");
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[("foo", "bar")], None), sc(&[("foo", "bar")], None));
        ok(sc(&[], None), sc(&[], None));
        ok(
            sc(&[("fstype", "ext3")], None),
            sc(&[("csi.storage.k8s.io/fstype", "ext3")], None),
        );
        ok(
            sc(&[("iopsPerGB", "100")], None),
            sc(
                &[("iopsPerGB", "100"), ("allowautoiopspergbincrease", "true")],
                None,
            ),
        );
        ok(
            sc(&[("zones", "a,b")], None),
            sc(&[], topo(AWS_EBS_TOPOLOGY_KEY, &["a", "b"])),
        );
    }

    /// `TranslateInTreeStorageClassToCSI` (`translate.go:60-62`): an unknown
    /// plugin name is an error; the input class is never modified
    /// (`sc.DeepCopy()`, :55).
    #[test]
    fn storage_class_translation_rejects_unknown_plugin_and_keeps_input() {
        let t = CsiTranslator::new();
        let input = sc(&[("fstype", "x")], None);
        assert!(t
            .translate_in_tree_storage_class_to_csi("kubernetes.io/nope", &input)
            .unwrap_err()
            .contains("could not find in-tree storage class parameter translation logic"));
        let _ = t.translate_in_tree_storage_class_to_csi(GCE_PD_IN_TREE_PLUGIN_NAME, &input);
        assert_eq!(input.parameters.unwrap().get("fstype").unwrap(), "x");
    }

    fn blank_pv(name: &str) -> PersistentVolume {
        PersistentVolume {
            type_meta: TypeMeta {
                kind: "PersistentVolume".into(),
                api_version: "v1".into(),
            },
            metadata: ObjectMeta::new(name),
            spec: Default::default(),
            status: None,
        }
    }

    fn gce_pv(labels: &[(&str, &str)]) -> PersistentVolume {
        let mut pv = blank_pv("pv1");
        pv.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        );
        pv.spec.gce_persistent_disk = Some(GCEPersistentDiskVolumeSource {
            pd_name: "disk1".into(),
            fs_type: Some("ext4".into()),
            partition: Some(2),
            read_only: Some(true),
        });
        pv
    }

    fn ebs_pv(id: &str) -> PersistentVolume {
        let mut pv = blank_pv("pv2");
        pv.spec.aws_elastic_block_store = Some(AWSElasticBlockStoreVolumeSource {
            volume_id: id.into(),
            fs_type: None,
            partition: None,
            read_only: None,
        });
        pv
    }

    fn affinity(key: &str, values: &[&str]) -> Option<VolumeNodeAffinity> {
        Some(VolumeNodeAffinity {
            required: Some(NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(vec![NodeSelectorRequirement {
                        key: key.into(),
                        operator: "In".into(),
                        values: Some(values.iter().map(|s| s.to_string()).collect()),
                    }]),
                    match_fields: None,
                }],
            }),
        })
    }

    fn keys_and_values(pv: &PersistentVolume) -> Vec<(String, Vec<String>)> {
        pv.spec
            .node_affinity
            .as_ref()
            .and_then(|a| a.required.as_ref())
            .into_iter()
            .flat_map(|r| r.node_selector_terms.iter())
            .flat_map(|t| t.match_expressions.iter().flatten())
            .map(|r| (r.key.clone(), r.values.clone().unwrap_or_default()))
            .collect()
    }

    /// `TestTranslateInTreePVToCSIVolIDFmt` (`gce_pd_test.go`): zonal label.
    #[test]
    fn gce_zonal_pv_translates_to_csi() {
        let pv = gce_pv(&[(ZONE_GA, "us-central1-a")]);
        let t = CsiTranslator::new();
        assert!(t.is_pv_migratable(&pv));
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        assert!(out.spec.gce_persistent_disk.is_none());
        let csi = out.spec.csi.clone().unwrap();
        assert_eq!(csi.driver, "pd.csi.storage.gke.io");
        assert_eq!(
            csi.volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/zones/us-central1-a/disks/disk1")
        );
        assert_eq!(csi.fs_type.as_deref(), Some("ext4"));
        assert_eq!(csi.read_only, Some(true));
        assert_eq!(csi.volume_attributes.unwrap()["partition"], "2");
        // zone label -> node affinity on the CSI topology key
        assert_eq!(
            keys_and_values(&out),
            vec![(
                "topology.gke.io/zone".to_string(),
                vec!["us-central1-a".to_string()]
            )]
        );
    }

    /// Multi-zone label (`__`-delimited) is a regional disk.
    #[test]
    fn gce_regional_pv_uses_region_in_handle() {
        let pv = gce_pv(&[(ZONE_BETA, "us-central1-a__us-central1-b")]);
        let out = CsiTranslator::new()
            .translate_in_tree_pv_to_csi(&pv)
            .unwrap();
        assert_eq!(
            out.spec.csi.unwrap().volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/regions/us-central1/disks/disk1")
        );
    }

    #[test]
    fn gce_no_zone_is_unspecified_and_rwx_becomes_rwo() {
        let mut pv = gce_pv(&[]);
        pv.spec.access_modes = vec![
            PersistentVolumeAccessMode::ReadWriteMany,
            PersistentVolumeAccessMode::ReadOnlyMany,
        ];
        let out = CsiTranslator::new()
            .translate_in_tree_pv_to_csi(&pv)
            .unwrap();
        assert_eq!(
            out.spec.csi.as_ref().unwrap().volume_handle.as_deref(),
            Some("projects/UNSPECIFIED/zones/UNSPECIFIED/disks/disk1")
        );
        // `backwardCompatibleAccessModes` (`gce_pd.go:137-171`)
        assert_eq!(
            out.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadWriteOnce]
        );
    }

    /// An existing in-tree node affinity key is replaced (`replaceTopology`).
    #[test]
    fn gce_affinity_key_is_replaced() {
        let mut pv = gce_pv(&[]);
        pv.spec.node_affinity = affinity(ZONE_BETA, &["us-east1-b"]);
        let out = CsiTranslator::new()
            .translate_in_tree_pv_to_csi(&pv)
            .unwrap();
        assert_eq!(
            keys_and_values(&out),
            vec![("topology.gke.io/zone".into(), vec!["us-east1-b".into()])]
        );
    }

    #[test]
    fn ebs_pv_translates_and_strips_aws_url() {
        let t = CsiTranslator::new();
        let mut pv = ebs_pv("aws://us-east-1a/vol-0abc");
        pv.metadata.labels = Some([(ZONE_GA.to_string(), "us-east-1a".to_string())].into());
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        let csi = out.spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "ebs.csi.aws.com");
        assert_eq!(csi.volume_handle.as_deref(), Some("vol-0abc"));
        assert_eq!(csi.volume_attributes.as_ref().unwrap()["partition"], "0");
        assert_eq!(
            keys_and_values(&out),
            vec![(
                "topology.ebs.csi.aws.com/zone".into(),
                vec!["us-east-1a".into()]
            )]
        );
        assert!(t
            .translate_in_tree_pv_to_csi(&ebs_pv("aws://us-east-1a/notavol"))
            .is_err());
    }

    #[test]
    fn non_in_tree_pv_is_not_migratable_and_errors() {
        let pv = blank_pv("pv3");
        let t = CsiTranslator::new();
        assert!(!t.is_pv_migratable(&pv));
        assert!(t.translate_in_tree_pv_to_csi(&pv).is_err());
    }

    #[test]
    fn name_mappings() {
        let t = CsiTranslator::new();
        assert!(t.is_migratable_in_tree_plugin_by_name("kubernetes.io/gce-pd"));
        assert!(!t.is_migratable_in_tree_plugin_by_name("kubernetes.io/nfs"));
        assert!(t.is_migrated_csi_driver_by_name("ebs.csi.aws.com"));
        assert_eq!(
            t.get_csi_name_from_in_tree_name("kubernetes.io/aws-ebs")
                .unwrap(),
            "ebs.csi.aws.com"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("pd.csi.storage.gke.io")
                .unwrap(),
            "kubernetes.io/gce-pd"
        );
        assert_eq!(
            t.get_in_tree_plugin_name_from_pv(&gce_pv(&[])).unwrap(),
            "kubernetes.io/gce-pd"
        );
    }

    /// `TestRepairVolumeHandle` (`gce_pd_test.go`).
    #[test]
    fn gce_repair_volume_handle() {
        let t = CsiTranslator::new();
        let node = "projects/p1/zones/us-central1-c/instances/n1";
        assert_eq!(
            t.repair_volume_handle(
                "pd.csi.storage.gke.io",
                "projects/UNSPECIFIED/zones/UNSPECIFIED/disks/d",
                node
            )
            .unwrap(),
            "projects/p1/zones/us-central1-c/disks/d"
        );
        assert_eq!(
            t.repair_volume_handle(
                "pd.csi.storage.gke.io",
                "projects/UNSPECIFIED/regions/UNSPECIFIED/disks/d",
                node
            )
            .unwrap(),
            "projects/p1/regions/us-central1/disks/d"
        );
        // already specified: untouched
        assert_eq!(
            t.repair_volume_handle(
                "pd.csi.storage.gke.io",
                "projects/x/zones/z-z-z/disks/d",
                node
            )
            .unwrap(),
            "projects/x/zones/z-z-z/disks/d"
        );
        assert!(t
            .repair_volume_handle("pd.csi.storage.gke.io", "short", node)
            .is_err());
        // EBS: identity (`aws_ebs.go:227-229`)
        assert_eq!(
            t.repair_volume_handle("ebs.csi.aws.com", "vol-1", "i-1")
                .unwrap(),
            "vol-1"
        );
        assert!(t.repair_volume_handle("other", "x", "y").is_err());
    }

    // ---- TranslateCSIPVToInTree -------------------------------------------

    const REGION_GA: &str = "topology.kubernetes.io/region";
    const REGION_BETA: &str = "failure-domain.beta.kubernetes.io/region";

    fn req(key: &str, values: &[&str]) -> NodeSelectorRequirement {
        NodeSelectorRequirement {
            key: key.into(),
            operator: "In".into(),
            values: Some(values.iter().map(|s| s.to_string()).collect()),
        }
    }

    fn term(reqs: Vec<NodeSelectorRequirement>) -> NodeSelectorTerm {
        NodeSelectorTerm {
            match_expressions: Some(reqs),
            match_fields: None,
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Option<HashMap<String, String>> {
        Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn pv_with(
        labels: Option<HashMap<String, String>>,
        terms: Vec<NodeSelectorTerm>,
    ) -> PersistentVolume {
        let mut pv = blank_pv("gcepd");
        pv.metadata.labels = labels;
        pv.spec.node_affinity = Some(VolumeNodeAffinity {
            required: Some(NodeSelector {
                node_selector_terms: terms,
            }),
        });
        pv
    }

    fn terms_json(pv: &PersistentVolume) -> serde_json::Value {
        serde_json::to_value(
            &pv.spec
                .node_affinity
                .as_ref()
                .unwrap()
                .required
                .as_ref()
                .unwrap()
                .node_selector_terms,
        )
        .unwrap()
    }

    /// `TestTranslateTopologyFromCSIToInTree` (`in_tree_volume_test.go:106-485`),
    /// the GCE PD rows (the Cinder row waits for the Cinder plugin).
    #[test]
    fn translate_topology_from_csi_to_in_tree_matches_upstream_table() {
        let gce: Option<RegionParser> = Some(gce_get_region_from_zones);
        let ga_labels = labels(&[(ZONE_GA, "us-east1-a"), (REGION_GA, "us-east1")]);
        let beta_labels = labels(&[(ZONE_BETA, "us-west2-b"), (REGION_BETA, "us-west2")]);
        let ga_zone_first = vec![term(vec![
            req(ZONE_GA, &["us-east1-a"]),
            req(REGION_GA, &["us-east1"]),
        ])];
        let ga_region_first = vec![term(vec![
            req(REGION_GA, &["us-east1"]),
            req(ZONE_GA, &["us-east1-a"]),
        ])];
        let beta_zone_first = vec![term(vec![
            req(ZONE_BETA, &["us-west2-b"]),
            req(REGION_BETA, &["us-west2"]),
        ])];
        let beta_region_first = vec![term(vec![
            req(REGION_BETA, &["us-west2"]),
            req(ZONE_BETA, &["us-west2-b"]),
        ])];
        let existing = labels(&[(ZONE_GA, "existingZone"), (REGION_GA, "existingRegion")]);

        #[allow(clippy::type_complexity)]
        let cases: Vec<(
            &str,
            Option<RegionParser>,
            PersistentVolume,
            Vec<NodeSelectorTerm>,
            Option<HashMap<String, String>>,
        )> = vec![
            (
                "Remove CSI Topology Key and do not change existing GA Kubernetes topology",
                gce,
                pv_with(
                    ga_labels.clone(),
                    vec![term(vec![
                        req(REGION_GA, &["us-east1"]),
                        req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"]),
                    ])],
                ),
                ga_region_first.clone(),
                ga_labels.clone(),
            ),
            (
                "Remove CSI Topology Key and do not change existing Beta Kubernetes topology",
                gce,
                pv_with(
                    beta_labels.clone(),
                    vec![term(vec![
                        req(REGION_BETA, &["us-west2"]),
                        req(GCE_PD_TOPOLOGY_KEY, &["us-west2-b"]),
                    ])],
                ),
                beta_region_first,
                beta_labels.clone(),
            ),
            (
                "Remove CSI Topology Key and add Kubernetes topology from NodeAffinity, ignore labels",
                gce,
                pv_with(
                    existing.clone(),
                    vec![term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])])],
                ),
                ga_zone_first.clone(),
                existing.clone(),
            ),
            (
                "No CSI topology label exists and no change to the NodeAffinity",
                gce,
                pv_with(existing.clone(), vec![]),
                vec![],
                existing.clone(),
            ),
            (
                "Generate GA labels and kubernetes topology only from CSI topology",
                gce,
                pv_with(
                    None,
                    vec![term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])])],
                ),
                ga_zone_first,
                ga_labels,
            ),
            (
                "Generate Beta labels and kubernetes topology from Beta NodeAffinity",
                gce,
                pv_with(None, vec![term(vec![req(ZONE_BETA, &["us-west2-b"])])]),
                beta_zone_first,
                beta_labels.clone(),
            ),
            (
                "regionParser is missing and only zone labels get generated",
                None,
                pv_with(
                    None,
                    vec![term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])])],
                ),
                vec![term(vec![req(ZONE_GA, &["us-east1-a"])])],
                labels(&[(ZONE_GA, "us-east1-a")]),
            ),
            (
                "Replace multi-term CSI Topology Key and add Region Kubernetes topology for both",
                gce,
                pv_with(
                    None,
                    vec![
                        term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-a"])]),
                        term(vec![req(GCE_PD_TOPOLOGY_KEY, &["us-east1-c"])]),
                    ],
                ),
                vec![
                    term(vec![
                        req(ZONE_GA, &["us-east1-a"]),
                        req(REGION_GA, &["us-east1"]),
                    ]),
                    term(vec![
                        req(ZONE_GA, &["us-east1-c"]),
                        req(REGION_GA, &["us-east1"]),
                    ]),
                ],
                labels(&[(ZONE_GA, "us-east1-a__us-east1-c"), (REGION_GA, "us-east1")]),
            ),
        ];

        for (name, parser, mut pv, want_terms, want_labels) in cases {
            translate_topology_from_csi_to_in_tree(&mut pv, GCE_PD_TOPOLOGY_KEY, parser)
                .unwrap_or_else(|e| panic!("{name}: unexpected error {e}"));
            assert_eq!(
                terms_json(&pv),
                serde_json::to_value(&want_terms).unwrap(),
                "{name}"
            );
            assert_eq!(pv.metadata.labels, want_labels, "{name}");
        }
    }

    /// `regionTopologyHandler` (`in_tree_volume.go:356-361`): a term with no
    /// zone value reaches the region parser with no zones, and that error
    /// fails the translation.
    #[test]
    fn a_term_without_zones_fails_the_region_parser() {
        let mut pv = pv_with(None, vec![term(vec![req("other", &["x"])])]);
        let err = translate_topology_from_csi_to_in_tree(
            &mut pv,
            GCE_PD_TOPOLOGY_KEY,
            Some(gce_get_region_from_zones),
        )
        .unwrap_err();
        assert_eq!(
            err,
            "Failed to handle region topology. error: no zones specified"
        );
    }

    /// `TestGetAwsRegionFromZones` (`aws_ebs_test.go:213-283`).
    #[test]
    fn get_aws_region_from_zones_matches_upstream_table() {
        let ok: &[(&[&str], &str)] = &[
            (&["us-west-2a", "us-west-2b"], "us-west-2"),
            (&["us-gov-east-1a"], "us-gov-east-1"),
            (&["us-east-1-wl1-bos-wlz-1"], "us-east-1"),
            (&["us-west-2-lax-1a"], "us-west-2"),
        ];
        for (zones, want) in ok {
            assert_eq!(
                get_aws_region_from_zones(zones).unwrap(),
                *want,
                "{zones:?}"
            );
        }
        let bad: &[&[&str]] = &[
            &[],
            &["us-west-2a", "us-east-1a"],
            &["us-west-2"],
            &["us-west-2ab"],
            &["us-west"],
        ];
        for zones in bad {
            assert!(get_aws_region_from_zones(zones).is_err(), "{zones:?}");
        }
        assert_eq!(
            get_aws_region_from_zones(&["us-west"]).unwrap_err(),
            "Unexpected zone format: us-west is not a valid AWS zone"
        );
    }

    /// `TestTranslationStability` (`translate_test.go:47-102`): in-tree ->
    /// CSI -> in-tree is the identity.
    #[test]
    fn translation_round_trip_is_stable() {
        let t = CsiTranslator::new();
        let mut gce = blank_pv("");
        gce.metadata = ObjectMeta::default();
        gce.spec.gce_persistent_disk = Some(GCEPersistentDiskVolumeSource {
            pd_name: "test-disk".into(),
            fs_type: Some("ext4".into()),
            partition: None,
            read_only: None,
        });
        let mut ebs = blank_pv("");
        ebs.metadata = ObjectMeta::default();
        ebs.spec.aws_elastic_block_store = Some(AWSElasticBlockStoreVolumeSource {
            volume_id: "vol01".into(),
            fs_type: Some("ext3".into()),
            partition: Some(1),
            read_only: Some(true),
        });
        for pv in [gce, ebs] {
            let csi = t.translate_in_tree_pv_to_csi(&pv).unwrap();
            let back = t.translate_csi_pv_to_in_tree(&csi).unwrap();
            assert_eq!(
                serde_json::to_value(&back).unwrap(),
                serde_json::to_value(&pv).unwrap()
            );
        }
    }

    /// `TestTopologyTranslation` (`translate_test.go:104-258`), GCE PD and
    /// AWS EBS rows: the round trip removes the CSI key and leaves a
    /// Kubernetes zone key.
    #[test]
    fn topology_round_trip_restores_kubernetes_keys() {
        let t = CsiTranslator::new();
        let beta = labels(&[(ZONE_BETA, "us-east-1a"), (REGION_BETA, "us-east-1")]);
        let ga = labels(&[(ZONE_GA, "us-east-1a"), (REGION_GA, "us-east-1")]);
        let regional_beta = labels(&[(ZONE_BETA, "europe-west1-b__europe-west1-c")]);
        let regional_ga = labels(&[(ZONE_GA, "europe-west1-b__europe-west1-c")]);
        let topo = |key: &str, values: &[&str]| vec![term(vec![req(key, values)])];
        let gce = |labels: Option<HashMap<String, String>>, terms: Vec<NodeSelectorTerm>| {
            let mut pv = blank_pv("");
            pv.metadata.labels = labels;
            if !terms.is_empty() {
                pv.spec.node_affinity = Some(VolumeNodeAffinity {
                    required: Some(NodeSelector {
                        node_selector_terms: terms,
                    }),
                });
            }
            pv.spec.gce_persistent_disk = Some(GCEPersistentDiskVolumeSource {
                pd_name: "test-disk".into(),
                fs_type: Some("ext4".into()),
                ..Default::default()
            });
            pv
        };
        let ebs = |labels: Option<HashMap<String, String>>, terms: Vec<NodeSelectorTerm>| {
            let mut pv = gce(labels, terms);
            pv.spec.gce_persistent_disk = None;
            pv.spec.aws_elastic_block_store = Some(AWSElasticBlockStoreVolumeSource {
                volume_id: "vol01".into(),
                fs_type: Some("ext3".into()),
                partition: Some(1),
                read_only: Some(true),
            });
            pv
        };
        let cases: Vec<(&str, PersistentVolume)> = vec![
            (GCE_PD_TOPOLOGY_KEY, gce(beta.clone(), vec![])),
            (GCE_PD_TOPOLOGY_KEY, gce(ga.clone(), vec![])),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(None, topo(GCE_PD_TOPOLOGY_KEY, &["us-east-2a"])),
            ),
            (GCE_PD_TOPOLOGY_KEY, gce(regional_beta.clone(), vec![])),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(None, topo(ZONE_GA, &["europe-west1-b", "europe-west1-c"])),
            ),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(
                    regional_beta,
                    topo(ZONE_BETA, &["europe-west1-f", "europe-west1-g"]),
                ),
            ),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(
                    regional_ga,
                    topo(ZONE_GA, &["europe-west1-f", "europe-west1-g"]),
                ),
            ),
            (
                GCE_PD_TOPOLOGY_KEY,
                gce(
                    None,
                    vec![
                        term(vec![req(ZONE_GA, &["europe-west1-f"])]),
                        term(vec![req(ZONE_GA, &["europe-west1-g"])]),
                    ],
                ),
            ),
            (AWS_EBS_TOPOLOGY_KEY, ebs(beta.clone(), vec![])),
            (
                AWS_EBS_TOPOLOGY_KEY,
                ebs(beta, topo(ZONE_BETA, &["us-east-2a"])),
            ),
            (AWS_EBS_TOPOLOGY_KEY, ebs(ga.clone(), vec![])),
            (
                AWS_EBS_TOPOLOGY_KEY,
                ebs(ga, topo(ZONE_GA, &["us-east-2a"])),
            ),
        ];
        for (key, pv) in cases {
            let csi = t.translate_in_tree_pv_to_csi(&pv).unwrap();
            let back = t
                .translate_csi_pv_to_in_tree(&csi)
                .unwrap_or_else(|e| panic!("{pv:?}: {e}"));
            let affinity = back.spec.node_affinity.as_ref();
            assert!(!topology_key_exist(key, affinity), "{back:?}");
            assert!(
                topology_key_exist(ZONE_BETA, affinity) || topology_key_exist(ZONE_GA, affinity),
                "{back:?}"
            );
        }
    }

    /// `gcePersistentDiskCSITranslator.TranslateCSIPVToInTree`
    /// (`gce_pd.go:269-306`): the PD name is the handle's sixth element and a
    /// non-empty `partition` attribute is parsed.
    #[test]
    fn gce_csi_pv_translates_to_in_tree() {
        let t = CsiTranslator::new();
        let mut pv = blank_pv("pv1");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: GCE_PD_DRIVER_NAME.into(),
            volume_handle: Some("projects/p/zones/us-central1-a/disks/d1".into()),
            fs_type: Some("xfs".into()),
            read_only: Some(true),
            volume_attributes: Some(HashMap::from([("partition".into(), "3".into())])),
            ..Default::default()
        });
        let out = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        assert!(out.spec.csi.is_none());
        assert_eq!(
            out.spec.gce_persistent_disk,
            Some(GCEPersistentDiskVolumeSource {
                pd_name: "d1".into(),
                fs_type: Some("xfs".into()),
                partition: Some(3),
                read_only: Some(true),
            })
        );
        // the input is not modified
        assert!(pv.spec.csi.is_some());

        pv.spec.csi.as_mut().unwrap().volume_handle = Some("projects/p/disks/d1".into());
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "failed to get id components.Got: 4, wanted 6 components or more. "
        );
        let csi = pv.spec.csi.as_mut().unwrap();
        csi.volume_handle = Some("projects/p/zones/z/disks/d1".into());
        csi.volume_attributes = Some(HashMap::from([("partition".into(), "x".into())]));
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "Failed to convert partition x to integer: strconv.Atoi: parsing \"x\": invalid syntax"
        );
    }

    /// `awsElasticBlockStoreCSITranslator.TranslateCSIPVToInTree`
    /// (`aws_ebs.go:170-201`): the handle is the volume ID, and a present
    /// `partition` attribute must parse (an empty one too).
    #[test]
    fn ebs_csi_pv_translates_to_in_tree() {
        let t = CsiTranslator::new();
        let mut pv = blank_pv("pv2");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: AWS_EBS_DRIVER_NAME.into(),
            volume_handle: Some("vol-0abc".into()),
            volume_attributes: Some(HashMap::from([("partition".into(), "2".into())])),
            ..Default::default()
        });
        let out = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        assert_eq!(
            out.spec.aws_elastic_block_store,
            Some(AWSElasticBlockStoreVolumeSource {
                volume_id: "vol-0abc".into(),
                fs_type: None,
                partition: Some(2),
                read_only: None,
            })
        );
        pv.spec.csi.as_mut().unwrap().volume_attributes =
            Some(HashMap::from([("partition".into(), "".into())]));
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "failed to convert partition  to integer: strconv.Atoi: parsing \"\": invalid syntax"
        );
    }

    /// `CSITranslator.TranslateCSIPVToInTree` (`translate.go:116-128`).
    #[test]
    fn translate_csi_pv_to_in_tree_rejects_non_csi_and_unknown_drivers() {
        let t = CsiTranslator::new();
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&blank_pv("x")).unwrap_err(),
            "CSI persistent volume was nil"
        );
        let mut pv = blank_pv("x");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: "other.csi".into(),
            ..Default::default()
        });
        assert_eq!(
            t.translate_csi_pv_to_in_tree(&pv).unwrap_err(),
            "could not find in-tree plugin translation logic for other.csi"
        );
    }

    // ---- TranslateInTreeInlineVolumeToCSI (`translate.go:67-90`) ---------

    fn inline(src: serde_json::Value) -> Volume {
        serde_json::from_value(src).unwrap()
    }

    /// `TestInlineReadOnly` (`plugins/gce_pd_test.go:270-301`).
    #[test]
    fn gce_inline_read_only_is_read_only_many() {
        let vol = inline(serde_json::json!({
            "name": "", "gcePersistentDisk": {"pdName": "foo", "readOnly": true}
        }));
        let pv = GcePersistentDiskTranslator
            .translate_in_tree_inline_volume_to_csi(&vol, "")
            .unwrap();
        let csi = pv.spec.csi.as_ref().expect("CSI source");
        assert_eq!(csi.read_only, Some(true), "PV readonly value not true");
        assert_eq!(
            pv.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadOnlyMany]
        );
    }

    /// `gcePersistentDiskCSITranslator.TranslateInTreeInlineVolumeToCSI`
    /// (`plugins/gce_pd.go:166-202`): name, unspecified-zone handle, the
    /// partition attribute (empty for 0), RWO and Filesystem.
    #[test]
    fn gce_inline_translates_to_csi_pv() {
        let t = CsiTranslator::new();
        let vol = inline(serde_json::json!({
            "name": "v", "gcePersistentDisk": {"pdName": "disk1", "fsType": "ext4"}
        }));
        let pv = t
            .translate_in_tree_inline_volume_to_csi(&vol, "ns")
            .unwrap();
        assert_eq!(
            serde_json::to_value(&pv.spec).unwrap(),
            serde_json::json!({
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "csi": {
                    "driver": "pd.csi.storage.gke.io",
                    "volumeHandle": "projects/UNSPECIFIED/zones/UNSPECIFIED/disks/disk1",
                    "fsType": "ext4",
                    "volumeAttributes": {"partition": ""},
                },
            })
        );
        assert_eq!(pv.metadata.name, "pd.csi.storage.gke.io-disk1");

        let vol = inline(serde_json::json!({
            "name": "v", "gcePersistentDisk": {"pdName": "disk1", "partition": 3}
        }));
        let pv = t.translate_in_tree_inline_volume_to_csi(&vol, "").unwrap();
        let attrs = pv.spec.csi.unwrap().volume_attributes.unwrap();
        assert_eq!(attrs.get("partition").map(String::as_str), Some("3"));
    }

    /// `TestTranslateInTreeInlineVolumeToCSI` (`plugins/aws_ebs_test.go:134-212`).
    #[test]
    fn ebs_inline_translation_matches_upstream_table() {
        const NORMAL: &str = "vol-02399794d890f9375";
        let cases: [(&str, serde_json::Value, Option<&str>); 5] = [
            (
                "Normal ID format",
                serde_json::json!({"awsElasticBlockStore": {"volumeID": NORMAL}}),
                Some("ebs.csi.aws.com-vol-02399794d890f9375"),
            ),
            (
                "aws:///{volumeId} format",
                serde_json::json!({"awsElasticBlockStore":
                    {"volumeID": "aws:///vol-02399794d890f9375"}}),
                Some("ebs.csi.aws.com-vol-02399794d890f9375"),
            ),
            (
                "aws://{zone}/{volumeId} format",
                serde_json::json!({"awsElasticBlockStore":
                    {"volumeID": "aws://us-west-2a/vol-02399794d890f9375"}}),
                Some("ebs.csi.aws.com-vol-02399794d890f9375"),
            ),
            (
                "fails on invalid volume ID",
                serde_json::json!({"awsElasticBlockStore":
                    {"volumeID": "aws://us-west-2a/02399794d890f9375"}}),
                None,
            ),
            ("fails on empty volume source", serde_json::json!({}), None),
        ];
        for (name, mut src, exp_pv_name) in cases {
            src["name"] = "volume".into();
            let got = AwsElasticBlockStoreTranslator
                .translate_in_tree_inline_volume_to_csi(&inline(src), "");
            match (got, exp_pv_name) {
                (Ok(pv), Some(exp)) => {
                    assert_eq!(pv.metadata.name, exp, "{name}");
                    assert_eq!(
                        pv.spec.csi.unwrap().volume_handle.as_deref(),
                        Some(NORMAL),
                        "{name}"
                    );
                }
                (Err(_), None) => {}
                (got, _) => panic!("{name}: unexpected {got:?}"),
            }
        }
    }

    /// `awsElasticBlockStoreCSITranslator.TranslateInTreeInlineVolumeToCSI`
    /// (`plugins/aws_ebs.go:104-135`) leaves `VolumeMode` unset; the
    /// dispatcher (`translate.go:77-85`) fills in Filesystem. The partition
    /// attribute is always the decimal number, "0" included.
    #[test]
    fn ebs_inline_volume_mode_defaults_to_filesystem_in_dispatcher() {
        let vol = inline(serde_json::json!({
            "name": "v",
            "awsElasticBlockStore": {"volumeID": "vol-1", "fsType": "ext3", "readOnly": true}
        }));
        let direct = AwsElasticBlockStoreTranslator
            .translate_in_tree_inline_volume_to_csi(&vol, "")
            .unwrap();
        assert!(direct.spec.volume_mode.is_none());
        let pv = CsiTranslator::new()
            .translate_in_tree_inline_volume_to_csi(&vol, "")
            .unwrap();
        assert_eq!(
            serde_json::to_value(&pv.spec).unwrap(),
            serde_json::json!({
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "csi": {
                    "driver": "ebs.csi.aws.com",
                    "volumeHandle": "vol-1",
                    "readOnly": true,
                    "fsType": "ext3",
                    "volumeAttributes": {"partition": "0"},
                },
            })
        );
        assert_eq!(
            AwsElasticBlockStoreTranslator
                .translate_in_tree_inline_volume_to_csi(
                    &inline(serde_json::json!({"name": "v", "awsElasticBlockStore":
                        {"volumeID": "aws://us-west-2a/bad"}})),
                    ""
                )
                .unwrap_err(),
            "failed to translate Kubernetes ID to EBS Volume ID \
             Invalid format for AWS volume (aws://us-west-2a/bad)"
        );
        assert_eq!(
            GcePersistentDiskTranslator
                .translate_in_tree_inline_volume_to_csi(
                    &inline(serde_json::json!({"name": "v"})),
                    ""
                )
                .unwrap_err(),
            "volume is nil or GCE PD not defined on volume"
        );
        assert_eq!(
            AwsElasticBlockStoreTranslator
                .translate_in_tree_inline_volume_to_csi(
                    &inline(serde_json::json!({"name": "v"})),
                    ""
                )
                .unwrap_err(),
            "volume is nil or AWS EBS not defined on volume"
        );
    }

    /// `TestTranslateInTreeInlineVolumeToCSINameUniqueness`
    /// (`translate_test.go:369-403`), GCE PD and AWS EBS rows.
    #[test]
    fn inline_translation_names_are_unique_per_volume() {
        let t = CsiTranslator::new();
        for (field, key) in [
            ("gcePersistentDisk", "pdName"),
            ("awsElasticBlockStore", "volumeID"),
        ] {
            let mk = |id: &str| {
                let mut src = serde_json::json!({"name": ""});
                src[field] = serde_json::json!({ key: id });
                t.translate_in_tree_inline_volume_to_csi(&inline(src), "")
                    .unwrap()
            };
            let pv1 = mk("2a1f0c5e-0b7e-4d6a-9c1e-6f3a4b5c6d7e");
            let pv2 = mk("9b8c7d6e-5f4a-4b3c-8d2e-1f0a9b8c7d6e");
            assert_ne!(
                pv1.metadata.name, pv2.metadata.name,
                "PV name {} not sufficiently unique for different volumes",
                pv1.metadata.name
            );
        }
    }

    /// `TranslateInTreeInlineVolumeToCSI` with no supporting plugin, plus
    /// `IsInlineMigratable` (`translate.go:196-204`) and the volume branch of
    /// `GetInTreePluginNameFromSpec` (`translate.go:145-164`).
    #[test]
    fn inline_dispatch_migratable_and_plugin_name() {
        let t = CsiTranslator::new();
        let empty = inline(serde_json::json!({"name": "scratch", "emptyDir": {}}));
        assert_eq!(
            t.translate_in_tree_inline_volume_to_csi(&empty, "")
                .unwrap_err(),
            "could not find in-tree plugin translation logic for \"scratch\""
        );
        assert!(!t.is_inline_migratable(&empty));
        assert_eq!(
            t.get_in_tree_plugin_name_from_spec(None, Some(&empty))
                .unwrap_err(),
            "could not find in-tree plugin name from volume scratch"
        );

        let gce = inline(serde_json::json!({"name": "g", "gcePersistentDisk": {"pdName": "d"}}));
        let ebs =
            inline(serde_json::json!({"name": "e", "awsElasticBlockStore": {"volumeID": "vol-1"}}));
        assert!(t.is_inline_migratable(&gce));
        assert!(t.is_inline_migratable(&ebs));
        assert_eq!(
            t.get_in_tree_plugin_name_from_spec(None, Some(&gce))
                .unwrap(),
            GCE_PD_IN_TREE_PLUGIN_NAME
        );
        assert_eq!(
            t.get_in_tree_plugin_name_from_spec(None, Some(&ebs))
                .unwrap(),
            AWS_EBS_IN_TREE_PLUGIN_NAME
        );
        // The PV branch wins when both are given.
        assert_eq!(
            t.get_in_tree_plugin_name_from_spec(Some(&ebs_pv("vol-1")), Some(&gce))
                .unwrap(),
            AWS_EBS_IN_TREE_PLUGIN_NAME
        );
        assert_eq!(
            t.get_in_tree_plugin_name_from_spec(None, None).unwrap_err(),
            "both persistent volume and volume are nil"
        );
    }

    // ---- Portworx (`plugins/portworx_test.go`) ----------------------------

    fn px_pv(annotations: Option<&[(&str, &str)]>) -> PersistentVolume {
        let mut pv = blank_pv("pxd.portworx.com");
        pv.metadata.annotations = annotations.map(|a| {
            a.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteOnce];
        pv.spec.more_sources.portworx_volume = Some(
            rusternetes_common::resources::pv_sources::PortworxVolumeSource {
                volume_id: "ID1111".into(),
                fs_type: "type".into(),
                read_only: false,
            },
        );
        pv
    }

    /// `portworx.go:100-126`, `:160-176`, `:187-189`: inline translation,
    /// CSI->in-tree round trip, and `CanSupportInline`.
    #[test]
    fn portworx_inline_and_reverse_translation() {
        let t = CsiTranslator::new();
        let vol = inline(serde_json::json!({
            "name": "v", "portworxVolume": {"volumeID": "ID1", "fsType": "ext4", "readOnly": true}
        }));
        assert!(t.is_inline_migratable(&vol));
        let pv = t
            .translate_in_tree_inline_volume_to_csi(&vol, "ns")
            .unwrap();
        assert_eq!(pv.metadata.name, "pxd.portworx.com-ID1");
        assert_eq!(
            pv.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadOnlyMany]
        );
        let back = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        let px = back.spec.more_sources.portworx_volume.unwrap();
        assert_eq!(
            (px.volume_id.as_str(), px.fs_type.as_str()),
            ("ID1", "ext4")
        );
        assert!(back.spec.csi.is_none());
    }

    /// `TestTranslatePortworxInTreePVToCSI` "normal" / "with secret annotations".
    #[test]
    fn portworx_pv_translates() {
        let t = CsiTranslator::new();
        let out = t.translate_in_tree_pv_to_csi(&px_pv(None)).unwrap();
        assert!(out.spec.more_sources.portworx_volume.is_none());
        let csi = out.spec.csi.unwrap();
        assert_eq!(csi.driver, "pxd.portworx.com");
        assert_eq!(csi.volume_handle.as_deref(), Some("ID1111"));
        assert_eq!(csi.fs_type.as_deref(), Some("type"));
        assert_eq!(csi.volume_attributes, Some(HashMap::new()));
        assert!(csi.controller_publish_secret_ref.is_none());

        let ann = [
            ("openstorage.io/auth-secret-name", "test-secret"),
            ("openstorage.io/auth-secret-namespace", "test-namespace"),
        ];
        let csi = t
            .translate_in_tree_pv_to_csi(&px_pv(Some(&ann)))
            .unwrap()
            .spec
            .csi
            .unwrap();
        for r in [
            &csi.controller_publish_secret_ref,
            &csi.node_stage_secret_ref,
            &csi.node_publish_secret_ref,
            &csi.controller_expand_secret_ref,
            &csi.node_expand_secret_ref,
        ] {
            let r = r.as_ref().unwrap();
            assert_eq!(r.name.as_deref(), Some("test-secret"));
            assert_eq!(r.namespace.as_deref(), Some("test-namespace"));
        }
        // Both annotations are required (`HasAnnotation` x2).
        let one = [("openstorage.io/auth-secret-name", "test-secret")];
        let csi = t
            .translate_in_tree_pv_to_csi(&px_pv(Some(&one)))
            .unwrap()
            .spec
            .csi
            .unwrap();
        assert!(csi.node_stage_secret_ref.is_none());
    }

    #[test]
    fn portworx_names_and_support() {
        let t = CsiTranslator::new();
        let pv = px_pv(None);
        assert!(t.is_pv_migratable(&pv));
        assert_eq!(
            t.get_in_tree_plugin_name_from_pv(&pv).unwrap(),
            "kubernetes.io/portworx-volume"
        );
        assert_eq!(
            t.get_csi_name_from_in_tree_name("kubernetes.io/portworx-volume")
                .unwrap(),
            "pxd.portworx.com"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("pxd.portworx.com")
                .unwrap(),
            "kubernetes.io/portworx-volume"
        );
        // `RepairVolumeHandle` is the identity (`portworx.go`).
        assert_eq!(
            t.repair_volume_handle("pxd.portworx.com", "h", "n")
                .unwrap(),
            "h"
        );
        assert!(!t.is_pv_migratable(&blank_pv("x")));
    }

    /// `TestTranslatePortworxInTreeStorageClassToCSI`.
    #[test]
    fn portworx_storage_class_translates() {
        let t = CsiTranslator::new();
        let name = "kubernetes.io/portworx-volume";
        let out = t
            .translate_in_tree_storage_class_to_csi(
                name,
                &sc(
                    &[("repl", "1"), ("fs", "ext4"), ("priority_io", "high")],
                    None,
                ),
            )
            .unwrap();
        assert_eq!(out.provisioner, "pxd.portworx.com");
        let p = out.parameters.unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p["repl"], "1");

        // "empty": parameters stay as they were (`len(params) > 0` guard).
        let mut empty = sc(&[], None);
        empty.parameters = None;
        let out = t
            .translate_in_tree_storage_class_to_csi(name, &empty)
            .unwrap();
        assert_eq!(out.provisioner, "pxd.portworx.com");
        assert!(out.parameters.is_none());

        // "with secret params"
        let out = t
            .translate_in_tree_storage_class_to_csi(
                name,
                &sc(
                    &[
                        ("repl", "1"),
                        ("openstorage.io/auth-secret-name", "test-secret"),
                        ("openstorage.io/auth-secret-namespace", "test-namespace"),
                    ],
                    None,
                ),
            )
            .unwrap();
        let p = out.parameters.unwrap();
        assert_eq!(p.len(), 13);
        assert_eq!(p["repl"], "1");
        for k in [
            "provisioner",
            "controller-publish",
            "node-stage",
            "node-publish",
            "controller-expand",
            "node-expand",
        ] {
            assert_eq!(
                p[&format!("csi.storage.k8s.io/{k}-secret-name")],
                "test-secret"
            );
            assert_eq!(
                p[&format!("csi.storage.k8s.io/{k}-secret-namespace")],
                "test-namespace"
            );
        }
        assert!(!p.contains_key("openstorage.io/auth-secret-name"));
    }

    // ---- vSphere (`plugins/vsphere_volume_test.go`) -----------------------

    fn vsphere_pv(labels: &[(&str, &str)], policy: &str) -> PersistentVolume {
        let mut pv = blank_pv("pvc-d8b4475f");
        pv.metadata.labels = (!labels.is_empty()).then(|| {
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        pv.spec.access_modes = vec![PersistentVolumeAccessMode::ReadWriteOnce];
        pv.spec.more_sources.vsphere_volume = Some(
            rusternetes_common::resources::pv_sources::VsphereVirtualDiskVolumeSource {
                volume_path: "[vsanDatastore] 6785a85e/disk.vmdk".into(),
                fs_type: "ext4".into(),
                storage_policy_name: policy.into(),
                storage_policy_id: String::new(),
            },
        );
        pv
    }

    /// `TestTranslateVSphereInTreePVToCSI` ("translate valid vSphere in-tree
    /// PV", "...with beta topology labels").
    #[test]
    fn vsphere_pv_translates() {
        let t = CsiTranslator::new();
        let out = t
            .translate_in_tree_pv_to_csi(&vsphere_pv(&[], "vSAN Default Storage Policy"))
            .unwrap();
        assert!(out.spec.more_sources.vsphere_volume.is_none());
        let csi = out.spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "csi.vsphere.vmware.com");
        assert_eq!(
            csi.volume_handle.as_deref(),
            Some("[vsanDatastore] 6785a85e/disk.vmdk")
        );
        assert_eq!(csi.fs_type.as_deref(), Some("ext4"));
        assert_eq!(
            csi.volume_attributes.as_ref().unwrap(),
            &HashMap::from([(
                "storagepolicyname".to_string(),
                "vSAN Default Storage Policy".to_string()
            )])
        );
        assert!(out.spec.node_affinity.is_none());

        // No policy name: attributes stay an empty (non-nil) map.
        let out = t.translate_in_tree_pv_to_csi(&vsphere_pv(&[], "")).unwrap();
        assert_eq!(
            out.spec.csi.unwrap().volume_attributes,
            Some(HashMap::new())
        );

        // Beta topology labels become CSI zone + region node affinity.
        let out = t
            .translate_in_tree_pv_to_csi(&vsphere_pv(
                &[
                    ("failure-domain.beta.kubernetes.io/zone", "z1"),
                    ("failure-domain.beta.kubernetes.io/region", "r1"),
                ],
                "p",
            ))
            .unwrap();
        assert_eq!(
            keys_and_values(&out),
            vec![
                ("topology.csi.vmware.com/zone".into(), vec!["z1".into()]),
                ("topology.csi.vmware.com/region".into(), vec!["r1".into()]),
            ]
        );
        // Labels are kept.
        assert_eq!(out.metadata.labels.as_ref().unwrap().len(), 2);

        // Existing in-tree affinity keys are replaced in place.
        let mut pv = vsphere_pv(&[], "p");
        pv.spec.node_affinity = affinity("topology.kubernetes.io/zone", &["z9"]);
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        assert_eq!(
            keys_and_values(&out),
            vec![("topology.csi.vmware.com/zone".into(), vec!["z9".into()])]
        );
    }

    /// `TestTranslatevSphereInTreeStorageClassToCSI` (full parameter table).
    #[test]
    fn vsphere_storage_class_translates() {
        let t = CsiTranslator::new();
        let name = "kubernetes.io/vsphere-volume";
        let mig = ("csimigration", "true");
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = t
                .translate_in_tree_storage_class_to_csi(name, &input)
                .unwrap();
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[], None), sc(&[mig], None));
        ok(sc(&[("unknownparam", "value")], None), sc(&[mig], None));
        ok(
            sc(
                &[
                    ("storagepolicyname", "test-policy-name"),
                    ("datastore", "vsanDatastore"),
                ],
                None,
            ),
            sc(
                &[
                    ("storagepolicyname", "test-policy-name"),
                    ("datastore-migrationparam", "vsanDatastore"),
                    mig,
                ],
                None,
            ),
        );
        ok(
            sc(&[("fstype", "ext4")], None),
            sc(&[("csi.storage.k8s.io/fstype", "ext4"), mig], None),
        );
        ok(
            sc(&[], topo(LABEL_TOPOLOGY_ZONE, &["zone-a"])),
            sc(&[mig], topo("topology.csi.vmware.com/zone", &["zone-a"])),
        );
        ok(
            sc(
                &[("storagepolicyname", "p")],
                topo(LABEL_FAILURE_DOMAIN_BETA_ZONE, &["zone-a"]),
            ),
            sc(
                &[("storagepolicyname", "p"), mig],
                topo("topology.csi.vmware.com/zone", &["zone-a"]),
            ),
        );
        ok(
            sc(
                &[
                    ("storagepolicyname", "test-policy-name"),
                    ("datastore", "test-datastore-name"),
                    ("fstype", "ext4"),
                    ("diskformat", "thin"),
                    ("hostfailurestotolerate", "1"),
                    ("forceprovisioning", "yes"),
                    ("cachereservation", "25"),
                    ("diskstripes", "4"),
                    ("objectspacereservation", "10"),
                    ("iopslimit", "32"),
                ],
                topo(LABEL_TOPOLOGY_ZONE, &["zone-a"]),
            ),
            sc(
                &[
                    ("storagepolicyname", "test-policy-name"),
                    ("datastore-migrationparam", "test-datastore-name"),
                    ("csi.storage.k8s.io/fstype", "ext4"),
                    ("diskformat-migrationparam", "thin"),
                    ("hostfailurestotolerate-migrationparam", "1"),
                    ("forceprovisioning-migrationparam", "yes"),
                    ("cachereservation-migrationparam", "25"),
                    ("diskstripes-migrationparam", "4"),
                    ("objectspacereservation-migrationparam", "10"),
                    ("iopslimit-migrationparam", "32"),
                    mig,
                ],
                topo("topology.csi.vmware.com/zone", &["zone-a"]),
            ),
        );
    }

    #[test]
    fn vsphere_names_and_support() {
        let t = CsiTranslator::new();
        let pv = vsphere_pv(&[], "");
        assert!(t.is_pv_migratable(&pv));
        assert_eq!(
            t.get_in_tree_plugin_name_from_pv(&pv).unwrap(),
            "kubernetes.io/vsphere-volume"
        );
        assert_eq!(
            t.get_csi_name_from_in_tree_name("kubernetes.io/vsphere-volume")
                .unwrap(),
            "csi.vsphere.vmware.com"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("csi.vsphere.vmware.com")
                .unwrap(),
            "kubernetes.io/vsphere-volume"
        );
        assert_eq!(
            t.repair_volume_handle("csi.vsphere.vmware.com", "h", "n")
                .unwrap(),
            "h"
        );
    }

    // ---- Azure File (`plugins/azure_file_test.go`) ------------------------

    fn azf_pv(
        annotations: &[(&str, &str)],
        secret_ns: Option<&str>,
        claim_ns: Option<&str>,
    ) -> PersistentVolume {
        let mut pv = blank_pv("uuid");
        pv.metadata.annotations = (!annotations.is_empty()).then(|| {
            annotations
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        pv.spec.more_sources.azure_file = Some(
            rusternetes_common::resources::pv_sources::AzureFilePersistentVolumeSource {
                secret_name: "secretname".into(),
                share_name: "sharename".into(),
                read_only: true,
                secret_namespace: secret_ns.map(String::from),
            },
        );
        pv.spec.claim_ref =
            claim_ns.map(
                |ns| rusternetes_common::resources::service_account::ObjectReference {
                    namespace: Some(ns.into()),
                    ..Default::default()
                },
            );
        pv
    }

    /// `azure_file.go:62-110` / `:150-221` and `vsphere_volume.go:112-190`:
    /// inline translation and the CSI->in-tree round trip.
    #[test]
    fn azure_file_and_vsphere_inline_and_reverse_translation() {
        let t = CsiTranslator::new();
        let vol = inline(serde_json::json!({
            "name": "v",
            "azureFile": {"secretName": "azure-storage-account-acct-secret", "shareName": "sh"}
        }));
        let pv = t
            .translate_in_tree_inline_volume_to_csi(&vol, "ns")
            .unwrap();
        assert_eq!(pv.metadata.name, "#acct#sh#v#ns");
        assert_eq!(
            pv.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadWriteMany]
        );
        let back = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        let az = back.spec.more_sources.azure_file.unwrap();
        assert_eq!(az.share_name, "sh");
        assert_eq!(az.secret_name, "azure-storage-account-acct-secret");
        assert_eq!(az.secret_namespace.as_deref(), Some("ns"));

        let vol = inline(serde_json::json!({
            "name": "v", "vsphereVolume": {"volumePath": "[ds] vol.vmdk", "fsType": "ext4"}
        }));
        assert!(t.is_inline_migratable(&vol));
        let pv = t
            .translate_in_tree_inline_volume_to_csi(&vol, "ns")
            .unwrap();
        assert_eq!(pv.metadata.name, "csi.vsphere.vmware.com-[ds] vol.vmdk");
        let mut csi_pv = pv.clone();
        csi_pv
            .spec
            .csi
            .as_mut()
            .unwrap()
            .volume_attributes
            .as_mut()
            .unwrap()
            .insert("initialvolumefilepath".into(), "[ds] vol.vmdk".into());
        let back = t.translate_csi_pv_to_in_tree(&csi_pv).unwrap();
        let vs = back.spec.more_sources.vsphere_volume.unwrap();
        assert_eq!(vs.volume_path, "[ds] vol.vmdk");
        assert_eq!(vs.fs_type, "ext4");
    }

    /// `TestTranslateAzureFileInTreePVToCSI`.
    #[test]
    fn azure_file_pv_translates() {
        let t = CsiTranslator::new();
        let rg = [("kubernetes.io/azure-file-resource-group", "rg")];

        // "return error if secret namespace could not be found"
        assert!(t
            .translate_in_tree_pv_to_csi(&azf_pv(&rg, None, None))
            .unwrap_err()
            .contains("could not find a secret namespace"));

        // "azure file volume" / "with rg annotation"
        for (ann, handle) in [
            (&[][..], "#secretname#sharename#uuid#secretnamespace"),
            (&rg[..], "rg#secretname#sharename#uuid#secretnamespace"),
        ] {
            let out = t
                .translate_in_tree_pv_to_csi(&azf_pv(ann, Some("secretnamespace"), None))
                .unwrap();
            assert!(out.spec.more_sources.azure_file.is_none());
            let csi = out.spec.csi.unwrap();
            assert_eq!(csi.driver, "file.csi.azure.com");
            assert_eq!(csi.read_only, Some(true));
            assert_eq!(csi.volume_handle.as_deref(), Some(handle));
            let r = csi.node_stage_secret_ref.unwrap();
            assert_eq!(r.name.as_deref(), Some("secretname"));
            assert_eq!(r.namespace.as_deref(), Some("secretnamespace"));
            assert_eq!(
                csi.volume_attributes.unwrap(),
                HashMap::from([("sharename".to_string(), "sharename".to_string())])
            );
        }

        // "get secret namespace from ClaimRef when it's missing in pv spec source"
        let out = t
            .translate_in_tree_pv_to_csi(&azf_pv(&rg, None, Some("secretnamespace")))
            .unwrap();
        let csi = out.spec.csi.as_ref().unwrap();
        assert_eq!(
            csi.volume_handle.as_deref(),
            Some("rg#secretname#sharename#uuid#secretnamespace")
        );
        assert!(out.spec.claim_ref.is_some());

        // `getStorageAccountName`: a conforming secret name yields the account.
        let mut pv = azf_pv(&[], Some("ns"), None);
        pv.spec
            .more_sources
            .azure_file
            .as_mut()
            .unwrap()
            .secret_name = "azure-storage-account-accountname-dup-secret".into();
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        assert_eq!(
            out.spec.csi.unwrap().volume_handle.as_deref(),
            Some("#accountname-dup#sharename#uuid#ns")
        );
    }

    /// `TestGetStorageAccount`.
    #[test]
    fn azure_file_storage_account_name() {
        assert_eq!(
            get_storage_account_name("azure-storage-account-accountname-secret").unwrap(),
            "accountname"
        );
        assert_eq!(
            get_storage_account_name("azure-storage-account-accountname-dup-secret").unwrap(),
            "accountname-dup"
        );
        assert!(get_storage_account_name("invalid").is_err());
    }

    /// `TestTranslateAzureFileInTreeStorageClassToCSI`: the class is returned
    /// unchanged (`azure_file.go` `TranslateInTreeStorageClassToCSI`), and the
    /// provisioner is not rewritten.
    #[test]
    fn azure_file_storage_class_unchanged_and_names() {
        let t = CsiTranslator::new();
        let input = sc(&[("skuName", "Standard_LRS")], None);
        let out = t
            .translate_in_tree_storage_class_to_csi("kubernetes.io/azure-file", &input)
            .unwrap();
        assert!(sc_eq(&out, &input));
        assert_eq!(out.provisioner, input.provisioner);

        let pv = azf_pv(&[], Some("ns"), None);
        assert!(t.is_pv_migratable(&pv));
        assert_eq!(
            t.get_in_tree_plugin_name_from_pv(&pv).unwrap(),
            "kubernetes.io/azure-file"
        );
        assert_eq!(
            t.get_csi_name_from_in_tree_name("kubernetes.io/azure-file")
                .unwrap(),
            "file.csi.azure.com"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("file.csi.azure.com")
                .unwrap(),
            "kubernetes.io/azure-file"
        );
        assert_eq!(
            t.repair_volume_handle("file.csi.azure.com", "h", "n")
                .unwrap(),
            "h"
        );
    }

    // ---- Cinder (`plugins/openstack_cinder_test.go`) ----------------------

    const CINDER_NAME: &str = "kubernetes.io/cinder";
    const CINDER_KEY: &str = "topology.cinder.csi.openstack.org/zone";

    fn cinder_pv() -> PersistentVolume {
        let mut pv = blank_pv("pv1");
        pv.spec.more_sources.cinder = Some(
            rusternetes_common::resources::pv_sources::CinderPersistentVolumeSource {
                volume_id: "vol-1".into(),
                fs_type: "ext4".into(),
                read_only: true,
                secret_ref: None,
            },
        );
        pv
    }

    /// `TestTranslateCinderInTreeStorageClassToCSI`.
    #[test]
    fn cinder_storage_class_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = t
                .translate_in_tree_storage_class_to_csi(CINDER_NAME, &input)
                .expect("translates");
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[("foo", "bar")], None), sc(&[("foo", "bar")], None));
        ok(sc(&[], None), sc(&[], None));
        ok(
            sc(&[("fstype", "ext3")], None),
            sc(&[("csi.storage.k8s.io/fstype", "ext3")], None),
        );
        // "availability" is not translated to AllowedTopologies.
        ok(
            sc(&[("availability", "nova")], None),
            sc(&[("availability", "nova")], None),
        );
        ok(
            sc(&[], topo(ZONE_BETA, &["nova"])),
            sc(&[], topo(CINDER_KEY, &["nova"])),
        );
    }

    /// `openstack_cinder.go:78-104` / `:131-160`: inline translation and the
    /// CSI->in-tree round trip.
    #[test]
    fn cinder_inline_and_reverse_translation() {
        let t = CsiTranslator::new();
        let vol = inline(serde_json::json!({
            "name": "v", "cinder": {"volumeID": "vid", "fsType": "ext4", "readOnly": true}
        }));
        assert!(t.is_inline_migratable(&vol));
        let pv = t
            .translate_in_tree_inline_volume_to_csi(&vol, "ns")
            .unwrap();
        assert_eq!(pv.metadata.name, "cinder.csi.openstack.org-vid");
        let back = t.translate_csi_pv_to_in_tree(&pv).unwrap();
        let c = back.spec.more_sources.cinder.unwrap();
        assert_eq!(
            (c.volume_id.as_str(), c.fs_type.as_str(), c.read_only),
            ("vid", "ext4", true)
        );
        assert!(back.spec.csi.is_none());
    }

    /// `TranslateInTreePVToCSI` (`openstack_cinder.go`): the source moves to
    /// CSI, with empty VolumeAttributes, and topology is rewritten.
    #[test]
    fn cinder_pv_translates_to_csi() {
        let t = CsiTranslator::new();
        let mut pv = cinder_pv();
        pv.spec.node_affinity = affinity(ZONE_BETA, &["nova"]);
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        assert!(out.spec.more_sources.cinder.is_none());
        let csi = out.spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "cinder.csi.openstack.org");
        assert_eq!(csi.volume_handle.as_deref(), Some("vol-1"));
        assert_eq!(csi.fs_type.as_deref(), Some("ext4"));
        assert_eq!(csi.read_only, Some(true));
        assert_eq!(csi.volume_attributes, Some(HashMap::new()));
        assert_eq!(
            keys_and_values(&out),
            vec![(CINDER_KEY.to_string(), vec!["nova".to_string()])]
        );
        // The input is not modified.
        assert!(pv.spec.more_sources.cinder.is_some());
    }

    #[test]
    fn cinder_names_and_support() {
        let t = CsiTranslator::new();
        let pv = cinder_pv();
        assert!(t.is_pv_migratable(&pv));
        assert_eq!(t.get_in_tree_plugin_name_from_pv(&pv).unwrap(), CINDER_NAME);
        assert_eq!(
            t.get_csi_name_from_in_tree_name(CINDER_NAME).unwrap(),
            "cinder.csi.openstack.org"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("cinder.csi.openstack.org")
                .unwrap(),
            CINDER_NAME
        );
        // `RepairVolumeHandle` is the identity.
        assert_eq!(
            t.repair_volume_handle("cinder.csi.openstack.org", "h", "n")
                .unwrap(),
            "h"
        );
    }

    // ---- Azure Disk (`plugins/azure_disk_test.go`) ------------------------

    const AZD_NAME: &str = "kubernetes.io/azure-disk";
    const AZD_KEY: &str = "topology.disk.csi.azure.com/zone";
    const AZD_URI: &str =
        "/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name";

    fn azd_pv(
        src: rusternetes_common::resources::pv_sources::AzureDiskVolumeSource,
    ) -> PersistentVolume {
        let mut pv = blank_pv("pv1");
        pv.spec.more_sources.azure_disk = Some(src);
        pv
    }

    /// `TestIsManagedDisk`.
    #[test]
    fn azure_disk_is_managed_disk() {
        for (input, want) in [
            (
                "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name",
                true,
            ),
            ("test.com", true),
            ("HTTP://test.com", false),
            ("http://test.com/vhds/name", false),
        ] {
            assert_eq!(is_managed_disk(input), want, "input {input:?}");
        }
    }

    /// `TestGetDiskName`.
    #[test]
    fn azure_disk_get_disk_name() {
        assert_eq!(
            get_disk_name(
                "testurl/subscriptions/12/resourceGroups/23/providers/Microsoft.Compute/disks/name"
            ),
            Ok("name".to_string())
        );
        assert_eq!(
            get_disk_name("testurl/subscriptions/23/providers/Microsoft.Compute/disks/name"),
            Err("could not get disk name from testurl/subscriptions/23/providers/Microsoft.Compute/disks/name, correct format: .*/subscriptions/(?:.*)/resourceGroups/(?:.*)/providers/Microsoft.Compute/disks/(.+)".to_string())
        );
        assert_eq!(
            get_disk_name("http://test.com/vhds/name"),
            Ok("name".to_string())
        );
        assert_eq!(
            get_disk_name("http://test.io/name"),
            Err("could not get disk name from http://test.io/name, correct format: http(?:.*)://(?:.*)/vhds/(.+)".to_string())
        );
    }

    /// `TestTranslateAzureDiskInTreeInlineVolumeToCSI`.
    #[test]
    fn azure_disk_inline_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        // "no azure disk volume"
        let vol = inline(serde_json::json!({"name": "v"}));
        assert!(!t.is_inline_migratable(&vol));
        assert!(t.translate_in_tree_inline_volume_to_csi(&vol, "").is_err());
        // "azure disk volume"
        let vol = inline(serde_json::json!({
            "name": "v", "azureDisk": {"diskName": "diskname", "diskURI": "datadiskuri"}
        }));
        assert!(t.is_inline_migratable(&vol));
        let pv = t.translate_in_tree_inline_volume_to_csi(&vol, "").unwrap();
        assert_eq!(pv.metadata.name, "datadiskuri");
        let csi = pv.spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "disk.csi.azure.com");
        assert_eq!(csi.volume_handle.as_deref(), Some("datadiskuri"));
        assert_eq!(
            csi.volume_attributes,
            Some(HashMap::from([("kind".to_string(), "Managed".to_string())]))
        );
        assert_eq!(
            pv.spec.access_modes,
            vec![PersistentVolumeAccessMode::ReadWriteOnce]
        );
        // The remaining optional fields (`azure_disk.go:111-125`).
        let vol = inline(serde_json::json!({
            "name": "v",
            "azureDisk": {"diskName": "d", "diskURI": "u", "kind": "managed", "readOnly": true,
                          "cachingMode": "ReadOnly", "fsType": "ext4"}
        }));
        let pv = t.translate_in_tree_inline_volume_to_csi(&vol, "").unwrap();
        let csi = pv.spec.csi.as_ref().unwrap();
        assert_eq!(csi.read_only, Some(true));
        assert_eq!(csi.fs_type.as_deref(), Some("ext4"));
        assert_eq!(
            csi.volume_attributes,
            Some(HashMap::from([
                ("kind".to_string(), "Managed".to_string()),
                ("cachingmode".to_string(), "ReadOnly".to_string()),
                ("fstype".to_string(), "ext4".to_string()),
            ]))
        );
        // "azure disk volume with non-managed kind"
        let vol = inline(serde_json::json!({
            "name": "v",
            "azureDisk": {"diskName": "diskname", "diskURI": "datadiskuri", "kind": "Dedicated"}
        }));
        assert_eq!(
            t.translate_in_tree_inline_volume_to_csi(&vol, "")
                .unwrap_err(),
            "kind(Dedicated) is not supported in csi migration"
        );
    }

    /// `TestTranslateAzureDiskInTreePVToCSI`.
    #[test]
    fn azure_disk_pv_translates_matches_upstream_table() {
        use rusternetes_common::resources::pv_sources::AzureDiskVolumeSource;
        let t = CsiTranslator::new();
        // "no azure disk volume"
        assert!(t.translate_in_tree_pv_to_csi(&blank_pv("x")).is_err());
        // "azure disk volume"
        let pv = azd_pv(AzureDiskVolumeSource {
            caching_mode: Some("cachingmode".into()),
            data_disk_uri: AZD_URI.into(),
            fs_type: Some("fstype".into()),
            read_only: Some(true),
            ..Default::default()
        });
        let out = t.translate_in_tree_pv_to_csi(&pv).unwrap();
        assert!(out.spec.more_sources.azure_disk.is_none());
        let csi = out.spec.csi.as_ref().unwrap();
        assert_eq!(csi.driver, "disk.csi.azure.com");
        assert_eq!(csi.fs_type.as_deref(), Some("fstype"));
        assert_eq!(csi.read_only, Some(true));
        assert_eq!(csi.volume_handle.as_deref(), Some(AZD_URI));
        assert_eq!(
            csi.volume_attributes,
            Some(HashMap::from([
                ("cachingmode".to_string(), "cachingmode".to_string()),
                ("fstype".to_string(), "fstype".to_string()),
                ("kind".to_string(), "Managed".to_string()),
            ]))
        );
        // The input is not modified.
        assert!(pv.spec.more_sources.azure_disk.is_some());
        // "azure disk volume with non-managed kind"
        let pv = azd_pv(AzureDiskVolumeSource {
            data_disk_uri: AZD_URI.into(),
            kind: Some("Dedicated".into()),
            ..Default::default()
        });
        assert_eq!(
            t.translate_in_tree_pv_to_csi(&pv).unwrap_err(),
            "kind(Dedicated) is not supported in csi migration"
        );
    }

    /// `TestTranslateTranslateCSIPVToInTree`.
    #[test]
    fn azure_disk_csi_pv_translates_to_in_tree() {
        let t = CsiTranslator::new();
        let case = |attrs: &[(&str, &str)], mode: &str| {
            let mut pv = blank_pv("pv1");
            pv.spec.csi = Some(CSIVolumeSource {
                driver: "disk.csi.azure.com".into(),
                fs_type: Some("fstype".into()),
                read_only: Some(true),
                volume_handle: Some(AZD_URI.into()),
                volume_attributes: Some(
                    attrs
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                ..Default::default()
            });
            let out = t.translate_csi_pv_to_in_tree(&pv).unwrap();
            assert!(out.spec.csi.is_none());
            let az = out.spec.more_sources.azure_disk.unwrap();
            assert_eq!(az.caching_mode.as_deref(), Some(mode));
            assert_eq!(az.data_disk_uri, AZD_URI);
            assert_eq!(az.fs_type.as_deref(), Some("fstype"));
            assert_eq!(az.read_only, Some(true));
            assert_eq!(az.kind.as_deref(), Some("Managed"));
            assert_eq!(az.disk_name, "name");
        };
        case(
            &[
                ("cachingmode", "ReadOnly"),
                ("fstype", "fstype"),
                ("kind", "managed"),
            ],
            "ReadOnly",
        );
        case(
            &[
                ("cachingmode", "ReadOnly"),
                ("fsType", "fstype"),
                ("kind", "managed"),
            ],
            "ReadOnly",
        );
        // Keys are matched case-insensitively.
        case(
            &[
                ("cachingMode", "None"),
                ("fsType", "fstype"),
                ("kind", "managed"),
            ],
            "None",
        );
        case(
            &[
                ("cachingMode", "ReadWrite"),
                ("fsType", "fstype"),
                ("kind", "managed"),
            ],
            "ReadWrite",
        );
        // No attributes at all: FSType/ReadOnly come from the CSI source, and
        // an unparseable handle is an error.
        let mut pv = blank_pv("pv1");
        pv.spec.csi = Some(CSIVolumeSource {
            driver: "disk.csi.azure.com".into(),
            volume_handle: Some("nonsense".into()),
            ..Default::default()
        });
        assert!(t.translate_csi_pv_to_in_tree(&pv).is_err());
    }

    /// `TestTranslateInTreeStorageClassToCSI` (`azure_disk_test.go`).
    #[test]
    fn azure_disk_storage_class_translation_matches_upstream_table() {
        let t = CsiTranslator::new();
        let ok = |input: StorageClass, exp: StorageClass| {
            let got = t
                .translate_in_tree_storage_class_to_csi(AZD_NAME, &input)
                .expect("translates");
            assert!(sc_eq(&got, &exp), "got {got:?}, want {exp:?}");
        };
        ok(sc(&[("foo", "bar")], None), sc(&[("foo", "bar")], None));
        ok(sc(&[], None), sc(&[], None));
        ok(
            sc(&[("zone", "foo")], None),
            sc(&[], topo(AZD_KEY, &["foo"])),
        );
        ok(
            sc(&[("zones", "foo,bar,baz")], None),
            sc(&[], topo(AZD_KEY, &["foo", "bar", "baz"])),
        );
        ok(
            sc(&[], topo(AZD_KEY, &["foo"])),
            sc(&[], topo(AZD_KEY, &["foo"])),
        );
        ok(
            sc(&[], topo(LABEL_TOPOLOGY_ZONE, &["foo"])),
            sc(&[], topo(AZD_KEY, &["foo"])),
        );
        ok(
            sc(&[], topo(LABEL_FAILURE_DOMAIN_BETA_ZONE, &["foo"])),
            sc(&[], topo(AZD_KEY, &["foo"])),
        );
        // "zone and topology"
        assert!(t
            .translate_in_tree_storage_class_to_csi(
                AZD_NAME,
                &sc(&[("zone", "foo")], topo(AZD_KEY, &["foo"]))
            )
            .is_err());
        // "topology in regions without zones": numeric failure domains become "".
        ok(
            sc(&[], topo(LABEL_TOPOLOGY_ZONE, &["0"])),
            sc(&[], topo(AZD_KEY, &[""])),
        );
        ok(
            sc(&[], topo(LABEL_TOPOLOGY_ZONE, &["1234"])),
            sc(&[], topo(AZD_KEY, &[""])),
        );
        // "topology in regions with zones"
        ok(
            sc(&[], topo(LABEL_TOPOLOGY_ZONE, &["centralus-1"])),
            sc(&[], topo(AZD_KEY, &["centralus-1"])),
        );
    }

    #[test]
    fn azure_disk_names_and_support() {
        let t = CsiTranslator::new();
        let pv = azd_pv(Default::default());
        assert!(t.is_pv_migratable(&pv));
        assert_eq!(t.get_in_tree_plugin_name_from_pv(&pv).unwrap(), AZD_NAME);
        assert_eq!(
            t.get_csi_name_from_in_tree_name(AZD_NAME).unwrap(),
            "disk.csi.azure.com"
        );
        assert_eq!(
            t.get_in_tree_name_from_csi_name("disk.csi.azure.com")
                .unwrap(),
            AZD_NAME
        );
        // `RepairVolumeHandle` is the identity.
        assert_eq!(
            t.repair_volume_handle("disk.csi.azure.com", "h", "n")
                .unwrap(),
            "h"
        );
    }
}
