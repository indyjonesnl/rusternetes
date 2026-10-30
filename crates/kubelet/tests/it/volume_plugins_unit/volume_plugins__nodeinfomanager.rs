//! Unit tests moved from `src/volume_plugins/nodeinfomanager.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_common::resources::{
        CSINode, CSINodeDriver, CSINodeSpec, Node, VolumeNodeResources,
    };
    pub use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
    pub use rusternetes_storage::{build_key, Storage};
    pub use sha1::{Digest, Sha1};
    pub use std::collections::{BTreeMap, BTreeSet, HashMap};
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubelet::volume_plugins::nodeinfomanager::*;
    use rusternetes_storage::MemoryStorage;

    const DRIVER1: &str = "com.example.csi.driver1";

    type Annotation = Vec<(&'static str, &'static str)>;

    fn node(node_ids: Annotation, labels: Vec<(&str, &str)>, capacity: Vec<(&str, &str)>) -> Node {
        let mut n = Node::new("node1");
        if !node_ids.is_empty() {
            let m: std::collections::BTreeMap<_, _> = node_ids.into_iter().collect();
            n.metadata.annotations = Some(HashMap::from([(
                ANNOTATION_KEY_NODE_ID.to_string(),
                serde_json::to_string(&m).unwrap(),
            )]));
        }
        if !labels.is_empty() {
            n.metadata.labels = Some(
                labels
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        if !capacity.is_empty() {
            let c: HashMap<String, String> = capacity
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            n.status = Some(rusternetes_common::resources::NodeStatus {
                capacity: Some(c.clone()),
                allocatable: Some(c),
                ..Default::default()
            });
        }
        n
    }

    fn csi_node(drivers: Vec<(&str, &str, Option<i32>, Vec<&str>)>, owner_uid: &str) -> CSINode {
        CSINode {
            type_meta: TypeMeta {
                kind: "CSINode".into(),
                api_version: "storage.k8s.io/v1".into(),
            },
            metadata: ObjectMeta {
                name: "node1".into(),
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".into(),
                    kind: "Node".into(),
                    name: "node1".into(),
                    uid: owner_uid.into(),
                    block_owner_deletion: None,
                    controller: None,
                }]),
                ..Default::default()
            },
            spec: CSINodeSpec {
                drivers: drivers
                    .into_iter()
                    .map(|(name, id, count, keys)| CSINodeDriver {
                        name: name.into(),
                        node_id: id.into(),
                        topology_keys: if keys.is_empty() {
                            None
                        } else {
                            Some(keys.into_iter().map(String::from).collect())
                        },
                        allocatable: count.map(|c| VolumeNodeResources { count: Some(c) }),
                    })
                    .collect(),
            },
        }
    }

    async fn setup(
        n: Node,
        c: Option<CSINode>,
    ) -> (Arc<MemoryStorage>, NodeInfoManager<MemoryStorage>) {
        let st = Arc::new(MemoryStorage::new());
        st.create(&build_key("nodes", None, "node1"), &n)
            .await
            .unwrap();
        if let Some(c) = c {
            st.create(&build_key("csinodes", None, "node1"), &c)
                .await
                .unwrap();
        }
        let nim = NodeInfoManager::new("node1", st.clone());
        (st, nim)
    }

    type ErrLog = Arc<std::sync::Mutex<Vec<Option<String>>>>;

    fn recorder() -> (ErrLog, impl Fn(Option<String>)) {
        let log: ErrLog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let l = log.clone();
        (log, move |e| l.lock().unwrap().push(e))
    }

    /// csi_plugin.go:374,404: NotReady first, cleared only once the CSINode
    /// is installed and owned by the Node.
    #[tokio::test]
    async fn gate_sets_error_then_clears_once_csinode_initialized() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        let (log, set) = recorder();
        nim.initialize_csi_node_gating_ready(set).await.unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            vec![Some("CSINode is not yet initialized".to_string()), None]
        );
        assert!(got(&st).await.1.is_some());
    }

    /// csi_plugin.go:398,413: failures keep the kubelet NotReady with the
    /// cause and give up (upstream `klog.Fatalf`) after the backoff.
    #[tokio::test]
    async fn gate_stays_not_ready_and_errors_after_backoff() {
        let st = Arc::new(MemoryStorage::new()); // no Node: init always fails
        let nim = NodeInfoManager::new("node1", st);
        let (log, set) = recorder();
        let err = nim
            .initialize_csi_node_gating_ready_with(set, 2, std::time::Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(
            err.starts_with("Failed to initialize CSINode after retrying"),
            "{err}"
        );
        let log = log.lock().unwrap();
        assert_eq!(log[0].as_deref(), Some("CSINode is not yet initialized"));
        assert!(log[1]
            .as_ref()
            .unwrap()
            .starts_with("failed to initialize CSINode: "));
        assert!(log.last().unwrap().is_some(), "never cleared");
    }

    async fn got(st: &MemoryStorage) -> (Node, Option<CSINode>) {
        let n: Node = st.get(&build_key("nodes", None, "node1")).await.unwrap();
        let c: Option<CSINode> = st.get(&build_key("csinodes", None, "node1")).await.ok();
        (n, c)
    }

    fn nodeid_map(n: &Node) -> std::collections::BTreeMap<String, String> {
        n.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(ANNOTATION_KEY_NODE_ID))
            .map(|s| serde_json::from_str(s).unwrap())
            .unwrap_or_default()
    }

    fn topo(kv: &[(&str, &str)]) -> HashMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn no_nodeid_annotation(n: &Node) -> bool {
        n.metadata
            .annotations
            .as_ref()
            .is_none_or(|a| !a.contains_key(ANNOTATION_KEY_NODE_ID))
    }

    /// `TestInstallCSIDriver` "empty node": annotation, topology label, and a
    /// created CSINode owned by the node.
    #[tokio::test]
    async fn install_on_empty_node_creates_csinode_and_annotates_node() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        nim.install_csi_driver(
            DRIVER1,
            "com.example.csi/csi-node1",
            0,
            &topo(&[("com.example.csi/zone", "zoneA")]),
        )
        .await
        .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(nodeid_map(&n)[DRIVER1], "com.example.csi/csi-node1");
        assert_eq!(n.metadata.labels.unwrap()["com.example.csi/zone"], "zoneA");
        let c = c.expect("CSINode created");
        let o = &c.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (o.kind.as_str(), o.name.as_str(), o.api_version.as_str()),
            ("Node", "node1", "v1")
        );
        assert_eq!(c.spec.drivers.len(), 1);
        let d = &c.spec.drivers[0];
        assert_eq!(d.name, DRIVER1);
        assert_eq!(d.node_id, "com.example.csi/csi-node1");
        assert_eq!(
            d.topology_keys,
            Some(vec!["com.example.csi/zone".to_string()])
        );
        assert!(d.allocatable.is_none());
    }

    /// "pre-existing node info from different driver": the other driver's
    /// annotation entry, label and CSINode driver entry all survive; the new
    /// driver is appended.
    #[tokio::test]
    async fn install_keeps_other_drivers() {
        let (st, nim) = setup(
            node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/test-node",
                )],
                vec![("net.example.storage/rack", "rack1")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/test-node",
                    None,
                    vec!["net.example.storage/rack"],
                )],
                "",
            )),
        )
        .await;
        nim.install_csi_driver(
            DRIVER1,
            "com.example.csi/csi-node1",
            0,
            &topo(&[("com.example.csi/zone", "zoneA")]),
        )
        .await
        .unwrap();
        let (n, c) = got(&st).await;
        let ids = nodeid_map(&n);
        assert_eq!(ids.len(), 2);
        assert_eq!(
            ids["net.example.storage.other-driver"],
            "net.example.storage/test-node"
        );
        let labels = n.metadata.labels.unwrap();
        assert_eq!(labels["net.example.storage/rack"], "rack1");
        assert_eq!(labels["com.example.csi/zone"], "zoneA");
        let names: Vec<_> = c
            .unwrap()
            .spec
            .drivers
            .iter()
            .map(|d| d.name.clone())
            .collect();
        assert_eq!(names, vec!["net.example.storage.other-driver", DRIVER1]);
    }

    /// "...different node ID and topology values; labels should conflict".
    #[tokio::test]
    async fn install_fails_on_topology_label_collision() {
        let (_st, nim) = setup(
            node(
                vec![(DRIVER1, "com.example.csi/csi-node1")],
                vec![("com.example.csi/zone", "zoneA")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    DRIVER1,
                    "com.example.csi/csi-node1",
                    None,
                    vec!["com.example.csi/zone"],
                )],
                "",
            )),
        )
        .await;
        let err = nim
            .install_csi_driver(
                DRIVER1,
                "com.example.csi/csi-node1",
                0,
                &topo(&[("com.example.csi/zone", "other-zone")]),
            )
            .await
            .unwrap_err();
        assert!(err.contains("topology value collision"), "{err}");
    }

    /// "...different node ID and topology keys; new labels should be added":
    /// the driver's entry is replaced, old labels kept.
    #[tokio::test]
    async fn install_replaces_same_driver_entry_and_adds_labels() {
        let (st, nim) = setup(
            node(
                vec![(DRIVER1, "com.example.csi/csi-node1")],
                vec![("com.example.csi/zone", "zoneA")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    DRIVER1,
                    "com.example.csi/csi-node1",
                    None,
                    vec!["com.example.csi/zone"],
                )],
                "",
            )),
        )
        .await;
        nim.install_csi_driver(
            DRIVER1,
            "com.example.csi/other-node",
            0,
            &topo(&[("com.example.csi/rack", "rack1")]),
        )
        .await
        .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(nodeid_map(&n)[DRIVER1], "com.example.csi/other-node");
        let labels = n.metadata.labels.unwrap();
        assert_eq!(labels["com.example.csi/zone"], "zoneA");
        assert_eq!(labels["com.example.csi/rack"], "rack1");
        let c = c.unwrap();
        assert_eq!(c.spec.drivers.len(), 1);
        assert_eq!(c.spec.drivers[0].node_id, "com.example.csi/other-node");
        assert_eq!(
            c.spec.drivers[0].topology_keys,
            Some(vec!["com.example.csi/rack".to_string()])
        );
    }

    /// "empty node ID".
    #[tokio::test]
    async fn install_with_empty_node_id_fails() {
        let (_st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        let err = nim
            .install_csi_driver(DRIVER1, "", 0, &HashMap::new())
            .await
            .unwrap_err();
        assert!(err.contains("driverNodeID must not be empty"), "{err}");
    }

    /// "pre-existing node info with driver, but owned by previous node": the
    /// stale CSINode (another node's UID) is deleted and recreated, so only the
    /// new driver remains.
    #[tokio::test]
    async fn install_replaces_a_csinode_owned_by_a_previous_node() {
        let mut n = node(vec![], vec![], vec![]);
        n.metadata.uid = "node1-uid".into();
        let (st, nim) = setup(
            n,
            Some(csi_node(
                vec![(
                    "com.example.csi.old-driver",
                    "com.example.csi/csi-node2",
                    None,
                    vec![],
                )],
                "node2-uid",
            )),
        )
        .await;
        nim.initialize_csi_node().await.unwrap();
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 0, &HashMap::new())
            .await
            .unwrap();
        let (_n, c) = got(&st).await;
        let c = c.unwrap();
        assert_eq!(
            c.spec.drivers.len(),
            1,
            "only the new driver: the old CSINode represented a previous node"
        );
        assert_eq!(c.spec.drivers[0].name, DRIVER1);
        assert_eq!(c.metadata.owner_references.unwrap()[0].uid, "node1-uid");
    }

    /// "new node with valid/max/overflown max limit of volumes" and "without".
    #[tokio::test]
    async fn install_maps_max_volumes_to_allocatable_count() {
        for (limit, want) in [
            (10_i64, Some(10_i32)),
            (i32::MAX as i64, Some(i32::MAX)),
            (i32::MAX as i64 + 1, Some(i32::MAX)),
            (0, None),
        ] {
            let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
            nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", limit, &HashMap::new())
                .await
                .unwrap();
            let (_n, c) = got(&st).await;
            let a = c.unwrap().spec.drivers[0].allocatable.clone();
            assert_eq!(a.and_then(|a| a.count), want, "limit {limit}");
        }
    }

    /// "node with existing valid max limit of volumes": a changed limit updates
    /// the CSINode driver entry; unrelated Node capacity is untouched.
    #[tokio::test]
    async fn install_updates_a_changed_limit() {
        let (st, nim) = setup(
            node(vec![], vec![], vec![("cpu", "4m")]),
            Some(csi_node(
                vec![(DRIVER1, "com.example.csi/csi-node1", Some(10), vec![])],
                "",
            )),
        )
        .await;
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 20, &HashMap::new())
            .await
            .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(
            c.unwrap().spec.drivers[0]
                .allocatable
                .as_ref()
                .unwrap()
                .count,
            Some(20)
        );
        assert_eq!(n.status.unwrap().capacity.unwrap()["cpu"], "4m");
    }

    /// `removeMaxAttachLimit` (kept upstream for version skew): installing
    /// drops a legacy `attachable-volumes-csi-<driver>` capacity entry.
    #[tokio::test]
    async fn install_removes_the_legacy_attach_limit_capacity() {
        let key = "attachable-volumes-csi-com.example.csi.driver1";
        let (st, nim) = setup(node(vec![], vec![], vec![("cpu", "4m"), (key, "10")]), None).await;
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 0, &HashMap::new())
            .await
            .unwrap();
        let (n, _c) = got(&st).await;
        let status = n.status.unwrap();
        assert!(!status.capacity.as_ref().unwrap().contains_key(key));
        assert!(!status.allocatable.as_ref().unwrap().contains_key(key));
        assert_eq!(status.capacity.unwrap()["cpu"], "4m");
    }

    /// A conflicting write (here injected) is retried, as `updateNode` /
    /// `updateCSINode` do under `wait.ExponentialBackoff`.
    #[tokio::test]
    async fn install_retries_update_conflicts() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        st.inject_conflicts(2);
        nim.install_csi_driver(DRIVER1, "com.example.csi/csi-node1", 0, &HashMap::new())
            .await
            .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(nodeid_map(&n)[DRIVER1], "com.example.csi/csi-node1");
        assert_eq!(c.unwrap().spec.drivers.len(), 1);
    }

    /// `TestInstallCSIDriverExistingAnnotation`: a CSINode created up front and
    /// a Node already carrying (other) annotation entries.
    #[tokio::test]
    async fn install_after_create_csinode_with_existing_annotation() {
        let (st, nim) = setup(
            node(
                vec![("net.example.storage/other-driver", "x")],
                vec![],
                vec![],
            ),
            None,
        )
        .await;
        nim.create_csi_node().await.unwrap();
        nim.install_csi_driver(
            "com.example.csi/driver1",
            "com.example.csi/some-node",
            0,
            &HashMap::new(),
        )
        .await
        .unwrap();
        let (_n, c) = got(&st).await;
        let c = c.unwrap();
        let d = &c.spec.drivers[0];
        assert_eq!(
            (d.name.as_str(), d.node_id.as_str()),
            ("com.example.csi/driver1", "com.example.csi/some-node")
        );
    }

    /// `TestUninstallCSIDriver` "pre-existing node info from the same driver":
    /// annotation removed (and with it the annotation key), the CSINode driver
    /// removed, topology labels left alone.
    #[tokio::test]
    async fn uninstall_removes_driver_but_keeps_labels() {
        let (st, nim) = setup(
            node(
                vec![(DRIVER1, "com.example.csi/csi-node1")],
                vec![("com.example.csi/zone", "zoneA")],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    DRIVER1,
                    "com.example.csi/csi-node1",
                    None,
                    vec!["com.example.csi/zone"],
                )],
                "",
            )),
        )
        .await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, c) = got(&st).await;
        assert!(no_nodeid_annotation(&n));
        assert_eq!(n.metadata.labels.unwrap()["com.example.csi/zone"], "zoneA");
        assert!(c.unwrap().spec.drivers.is_empty());
    }

    /// "pre-existing node info from different driver": nothing changes.
    #[tokio::test]
    async fn uninstall_leaves_other_drivers() {
        let (st, nim) = setup(
            node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/csi-node1",
                )],
                vec![],
                vec![],
            ),
            Some(csi_node(
                vec![(
                    "net.example.storage.other-driver",
                    "net.example.storage/csi-node1",
                    None,
                    vec![],
                )],
                "",
            )),
        )
        .await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(
            nodeid_map(&n)["net.example.storage.other-driver"],
            "net.example.storage/csi-node1"
        );
        assert_eq!(c.unwrap().spec.drivers.len(), 1);
    }

    /// "pre-existing info about the same driver in node, but empty CSINode",
    /// and no CSINode at all (NotFound is not an error).
    #[tokio::test]
    async fn uninstall_cleans_the_annotation_without_a_csinode_entry() {
        let (st, nim) = setup(
            node(vec![(DRIVER1, "com.example.csi/csi-node1")], vec![], vec![]),
            None,
        )
        .await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, c) = got(&st).await;
        assert!(no_nodeid_annotation(&n));
        assert!(c.is_none());
    }

    /// "new node with valid max limit" (uninstall): an unrelated capacity entry
    /// stays; the driver's legacy attach-limit key is dropped.
    #[tokio::test]
    async fn uninstall_removes_the_legacy_attach_limit_capacity() {
        let key = "attachable-volumes-csi-com.example.csi.driver1";
        let (st, nim) = setup(node(vec![], vec![], vec![("cpu", "4m"), (key, "10")]), None).await;
        nim.uninstall_csi_driver(DRIVER1).await.unwrap();
        let (n, _c) = got(&st).await;
        let capacity = n.status.unwrap().capacity.unwrap();
        assert!(!capacity.contains_key(key));
        assert_eq!(capacity["cpu"], "4m");
    }

    /// `GetCSIAttachLimitKey` (`pkg/volume/util/attach_limit.go:36-48`): a name
    /// that would reach 63 chars is truncated to 23 chars plus a 16-hex sha1.
    #[test]
    fn attach_limit_key_truncates_long_driver_names() {
        assert_eq!(csi_attach_limit_key("a.b"), "attachable-volumes-csi-a.b");
        let long = "very-long-csi-driver-name.storage.example.com.xyz";
        let k = csi_attach_limit_key(long);
        assert_eq!(k.len(), "attachable-volumes-csi-".len() + 23 + 16, "{k}");
        assert!(k.starts_with("attachable-volumes-csi-very-long-csi-driver-"));
    }

    // ---- CSI migration annotation (`setMigrationAnnotation`) ----

    fn migrated(entries: &[(&str, bool)]) -> MigratedPlugins {
        let mut m = MigratedPlugins::new();
        for (name, on) in entries {
            m.insert(
                name.to_string(),
                if *on {
                    (|| true) as fn() -> bool
                } else {
                    (|| false) as fn() -> bool
                },
            );
        }
        m
    }

    fn csi_node_with_annotations(a: &[(&str, &str)]) -> CSINode {
        let mut c = csi_node(vec![], "");
        if !a.is_empty() {
            c.metadata.annotations = Some(
                a.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        c
    }

    fn annotations_of(c: &CSINode) -> Option<Vec<(String, String)>> {
        c.metadata.annotations.as_ref().map(|m| {
            let mut v: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            v.sort();
            v
        })
    }

    /// `TestSetMigrationAnnotation` (nodeinfomanager_test.go:853-998), every
    /// row, including the `Annotations: map[string]string{}` left behind by
    /// "remove plugin".
    #[test]
    fn set_migration_annotation_table() {
        let key = MIGRATED_PLUGINS_ANNOTATION_KEY;
        let kv = |k: &str, v: &str| (k.to_string(), v.to_string());
        struct Case {
            name: &'static str,
            plugins: Option<MigratedPlugins>,
            existing: Vec<(&'static str, &'static str)>,
            expected: Option<Vec<(String, String)>>,
            modified: bool,
        }
        let cases = vec![
            Case {
                name: "nil migrated plugins",
                plugins: None,
                existing: vec![],
                expected: None,
                modified: false,
            },
            Case {
                name: "one modified plugin",
                plugins: Some(migrated(&[("test", true)])),
                existing: vec![],
                expected: Some(vec![kv(key, "test")]),
                modified: true,
            },
            Case {
                name: "existing plugin",
                plugins: Some(migrated(&[("test", true)])),
                existing: vec![(key, "test")],
                expected: Some(vec![kv(key, "test")]),
                modified: false,
            },
            Case {
                name: "remove plugin",
                plugins: Some(migrated(&[])),
                existing: vec![(key, "test")],
                expected: Some(vec![]),
                modified: true,
            },
            Case {
                name: "one modified plugin, other annotations stable",
                plugins: Some(migrated(&[("test", true)])),
                existing: vec![("other", "annotation")],
                expected: Some(vec![kv("other", "annotation"), kv(key, "test")]),
                modified: true,
            },
            Case {
                name: "multiple plugins modified, other annotations stable",
                plugins: Some(migrated(&[("test", true), ("foo", false)])),
                existing: vec![("other", "annotation"), (key, "foo")],
                expected: Some(vec![kv("other", "annotation"), kv(key, "test")]),
                modified: true,
            },
            Case {
                name: "multiple plugins added, other annotations stable",
                plugins: Some(migrated(&[("test", true), ("foo", true)])),
                existing: vec![("other", "annotation")],
                expected: Some(vec![kv("other", "annotation"), kv(key, "foo,test")]),
                modified: true,
            },
        ];
        for c in cases {
            let mut node = csi_node_with_annotations(&c.existing);
            let modified = set_migration_annotation(c.plugins.as_ref(), &mut node);
            assert_eq!(modified, c.modified, "{}: modified", c.name);
            assert_eq!(annotations_of(&node), c.expected, "{}: annotations", c.name);
        }
    }

    /// The 1.35 map built in `csiPlugin.Init` (`csi_plugin.go:325-347`): every
    /// in-tree plugin that still has a CSI translation is migrated.
    #[test]
    fn default_migrated_plugins_lists_every_in_tree_plugin() {
        let mut node = csi_node_with_annotations(&[]);
        assert!(set_migration_annotation(
            Some(&default_migrated_plugins()),
            &mut node
        ));
        assert_eq!(
            node.metadata.annotations.unwrap()[MIGRATED_PLUGINS_ANNOTATION_KEY],
            "kubernetes.io/aws-ebs,kubernetes.io/azure-disk,kubernetes.io/azure-file,\
kubernetes.io/cinder,kubernetes.io/gce-pd,kubernetes.io/portworx-volume,\
kubernetes.io/vsphere-volume"
        );
    }

    /// nodeinfomanager_test.go:315-350 ("pre-existing node info ... owned by
    /// previous node" with `migratedPlugins`): the CSINode created by
    /// `CreateCSINode` carries the annotation.
    #[tokio::test]
    async fn create_csi_node_sets_the_migration_annotation() {
        let (st, nim) = setup(node(vec![], vec![], vec![]), None).await;
        let nim = nim.with_migrated_plugins(migrated(&[(DRIVER1, true)]));
        nim.initialize_csi_node().await.unwrap();
        let (_, c) = got(&st).await;
        assert_eq!(
            annotations_of(&c.unwrap()),
            Some(vec![(
                MIGRATED_PLUGINS_ANNOTATION_KEY.to_string(),
                DRIVER1.to_string()
            )])
        );
    }

    /// `tryInitializeCSINodeWithAnnotation` (`nodeinfomanager.go:457-462`): an
    /// existing, owned CSINode whose annotation is stale is updated.
    #[tokio::test]
    async fn initialize_updates_a_stale_migration_annotation_on_an_existing_csinode() {
        let mut n = node(vec![], vec![], vec![]);
        n.metadata.uid = "uid1".into();
        let (st, nim) = setup(n, Some(csi_node(vec![], "uid1"))).await;
        let nim = nim.with_migrated_plugins(migrated(&[("a", true), ("b", true)]));
        nim.initialize_csi_node().await.unwrap();
        let (_, c) = got(&st).await;
        assert_eq!(
            annotations_of(&c.unwrap()),
            Some(vec![(
                MIGRATED_PLUGINS_ANNOTATION_KEY.to_string(),
                "a,b".to_string()
            )])
        );
    }

    /// `installDriverToCSINode` (`nodeinfomanager.go:624-628`): a spec that is
    /// unchanged still gets written when only the annotation changed.
    #[tokio::test]
    async fn install_writes_when_only_the_migration_annotation_changed() {
        let (st, nim) = setup(
            node(vec![], vec![], vec![]),
            Some(csi_node(vec![(DRIVER1, "id1", None, vec![])], "")),
        )
        .await;
        let nim = nim.with_migrated_plugins(migrated(&[(DRIVER1, true)]));
        nim.install_csi_driver(DRIVER1, "id1", 0, &HashMap::new())
            .await
            .unwrap();
        let (_, c) = got(&st).await;
        let c = c.unwrap();
        assert_eq!(c.spec.drivers.len(), 1);
        assert_eq!(
            annotations_of(&c),
            Some(vec![(
                MIGRATED_PLUGINS_ANNOTATION_KEY.to_string(),
                DRIVER1.to_string()
            )])
        );
    }

    // ---- UpdateCSIDriver ----

    /// `UpdateCSIDriver` (`nodeinfomanager.go:138-144`) refreshes the CSINode
    /// entry only: the Node's annotation is not touched.
    #[tokio::test]
    async fn update_csi_driver_updates_csinode_but_not_node() {
        let (st, nim) = setup(
            node(vec![], vec![], vec![]),
            Some(csi_node(vec![(DRIVER1, "id1", Some(5), vec![])], "")),
        )
        .await;
        nim.update_csi_driver(DRIVER1, "id1", 9, &HashMap::new())
            .await
            .unwrap();
        let (n, c) = got(&st).await;
        assert_eq!(
            c.unwrap().spec.drivers[0]
                .allocatable
                .as_ref()
                .unwrap()
                .count,
            Some(9)
        );
        assert!(no_nodeid_annotation(&n));
    }
}
