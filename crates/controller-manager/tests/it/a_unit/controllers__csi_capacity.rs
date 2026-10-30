//! Unit tests moved from `src/controllers/csi_capacity.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::csi::{CSINode, CSIStorageCapacity};
    pub use rusternetes_common::resources::volume::LabelSelector;
    pub use rusternetes_common::resources::volume::{
        NodeSelectorTerm, StorageClass, VolumeBindingMode,
    };
    pub use rusternetes_common::resources::Node;
    pub use rusternetes_common::types::{ObjectMeta, OwnerReference, TypeMeta};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_controller_manager::controllers::csi_capacity_metrics::CapacityStats;
    pub use rusternetes_controller_manager::controllers::csi_capacity_refresh::{
        ItemKey, QueueKey, State,
    };
    pub use rusternetes_csi::controller_client::CsiControllerClient;
    pub use rusternetes_csi::proto::{GetCapacityRequest, GetCapacityResponse, Topology};
    pub use rusternetes_storage::{build_key, build_prefix, Storage};
    pub use std::cmp::Ordering;
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_client::ratelimit::RateLimiter;
    use rusternetes_common::resources::csi::{CSINodeDriver, CSINodeSpec};
    use rusternetes_common::resources::volume::VolumeNodeAffinity;
    use rusternetes_common::resources::volume::{
        NodeSelectorRequirement, StorageClass, VolumeBindingMode,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_controller_manager::controllers::csi_capacity::*;
    use rusternetes_controller_manager::controllers::csi_capacity_refresh::{
        CapacityRefresher, ItemKey, QueueKey,
    };
    use rusternetes_storage::build_key;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;

    const DRIVER: &str = "my-csi-driver";
    const NS: &str = "kube-system";
    const MANAGED: &str = "instance-1";

    fn seg(pairs: &[(&str, &str)]) -> Segment {
        Segment::from_entries(
            pairs
                .iter()
                .map(|(k, v)| SegmentEntry {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        )
    }

    fn req(key: &str, op: &str, vals: &[&str]) -> NodeSelectorRequirement {
        NodeSelectorRequirement {
            key: key.into(),
            operator: op.into(),
            values: Some(vals.iter().map(|v| v.to_string()).collect()),
        }
    }

    // ---- TestRefresh (capacity_test.go:1799-1957) ----

    fn vna(pairs: &[(&str, &str)]) -> VolumeNodeAffinity {
        VolumeNodeAffinity {
            required: Some(rusternetes_common::resources::volume::NodeSelector {
                node_selector_terms: vec![NodeSelectorTerm {
                    match_expressions: Some(
                        pairs.iter().map(|(k, v)| req(k, "In", &[v])).collect(),
                    ),
                    match_fields: None,
                }],
            }),
        }
    }

    /// `itemsAsSortedStringSlice`.
    fn queued_strings(c: &CapacityRefresher) -> Vec<String> {
        let mut v: Vec<String> = c
            .queued()
            .iter()
            .map(|i| {
                let e: Vec<String> = i
                    .segment
                    .entries()
                    .iter()
                    .map(|e| format!("{}: {}", e.key, e.value))
                    .collect();
                format!("{}, [{}]", i.storage_class_name, e.join(" "))
            })
            .collect();
        v.sort();
        v
    }

    const DEEP: [(&str, &str); 3] = [("layer0", "foo"), ("layer1", "X"), ("layer2", "A")];
    const DEEP_OTHER: [(&str, &str); 3] = [("layer0", "foo"), ("layer1", "X"), ("layer2", "B")];

    async fn refresh_fixture(
        segments: &[&[(&str, &str)]],
    ) -> (MemoryStorage, FakeClient, std::sync::Arc<CapacityRefresher>) {
        let st = MemoryStorage::new();
        put_sc(&st, sc("direct-sc", DRIVER, None)).await;
        put_sc(&st, sc("triple-sc", DRIVER, None)).await;
        let cl = FakeClient::default();
        let c = ctl(&st, &cl, false);
        let segs: Vec<Segment> = segments.iter().map(|s| seg(s)).collect();
        settle(&c, &st, &segs).await;
        // "Clear queue so that below we only get to see items scheduled for refresh."
        c.refresher.clear_queue();
        let r = c.refresher.clone();
        (st, cl, r)
    }

    #[tokio::test]
    async fn refresh_storageclass() {
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")], &[("layer0", "bar")]]).await;
        r.refresh_sc("direct-sc");
        assert_eq!(
            queued_strings(&r),
            vec!["direct-sc, [layer0: bar]", "direct-sc, [layer0: foo]"]
        );
    }

    #[tokio::test]
    async fn refresh_topology() {
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")], &[("layer0", "bar")]]).await;
        r.refresh_topology(&vna(&[("layer0", "bar")]));
        assert_eq!(
            queued_strings(&r),
            vec!["direct-sc, [layer0: bar]", "triple-sc, [layer0: bar]"]
        );
    }

    #[tokio::test]
    async fn refresh_deep_topology() {
        let (_s, _c, r) = refresh_fixture(&[&DEEP, &DEEP_OTHER]).await;
        r.refresh_topology(&vna(&DEEP));
        assert_eq!(
            queued_strings(&r),
            vec![
                "direct-sc, [layer0: foo layer1: X layer2: A]",
                "triple-sc, [layer0: foo layer1: X layer2: A]"
            ]
        );
    }

    #[tokio::test]
    async fn refresh_unknown_or_truncated_topology_and_class_queue_nothing() {
        let (_s, _c, r) = refresh_fixture(&[&DEEP, &DEEP_OTHER]).await;
        r.refresh_topology(&vna(&[
            ("layer0", "foo"),
            ("layer1", "X"),
            ("layer2", "BBBBBBBBBBB"),
        ]));
        r.refresh_sc("no-such-sc");
        r.refresh_topology(&vna(&[("layer0", "foo")]));
        assert!(queued_strings(&r).is_empty());
    }

    #[tokio::test]
    async fn refresh_topology_skips_unusable_terms_only() {
        let (_s, _c, r) = refresh_fixture(&[&DEEP]).await;
        r.refresh_topology(&VolumeNodeAffinity { required: None });
        assert!(queued_strings(&r).is_empty());
        // An unusable term (`MatchFields`) is skipped, a later usable one is not.
        let mut a = vna(&DEEP);
        let bad = NodeSelectorTerm {
            match_expressions: None,
            match_fields: Some(vec![req("metadata.name", "In", &["n"])]),
        };
        a.required
            .as_mut()
            .unwrap()
            .node_selector_terms
            .insert(0, bad);
        r.refresh_topology(&a);
        assert_eq!(queued_strings(&r).len(), 2);
    }

    #[tokio::test]
    async fn queue_does_not_deduplicate_fresh_adds() {
        // capacity.go:115-120: `QueueKey` holds pointers and every
        // `queue.Add(QueueKey{item: &item})` uses a fresh one, so the workqueue
        // keeps both entries.
        let (_s, _c, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        r.refresh_sc("direct-sc");
        r.refresh_sc("direct-sc");
        assert_eq!(queued_strings(&r).len(), 2);
    }

    #[tokio::test]
    async fn sync_queued_refreshes_only_the_queued_items() {
        let (st, cl, r) = refresh_fixture(&[&[("layer0", "foo")], &[("layer0", "bar")]]).await;
        *cl.capacity.lock().unwrap() = 99;
        cl.requests.lock().unwrap().clear();
        r.refresh_topology(&vna(&[("layer0", "bar")]));
        let c = ctl_with(&st, &cl, r.clone());
        c.sync_queued(Duration::from_secs(1), Duration::from_secs(300))
            .await;
        assert_eq!(cl.requests.lock().unwrap().len(), 2);
        let caps = list_caps(&st).await;
        let updated = caps
            .iter()
            .filter(|c| c.capacity.as_deref() == Some("99"))
            .count();
        assert_eq!(updated, 2);
        assert!(r.queued().is_empty());
    }

    #[tokio::test]
    async fn sync_queued_skips_items_that_are_no_longer_wanted() {
        let (st, cl, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        r.refresh_sc("direct-sc");
        // The StorageClass is gone from the lister while the item is still
        // queued: "Another indication that the value is no longer needed."
        r.lock().classes.remove("direct-sc");
        cl.requests.lock().unwrap().clear();
        let c = ctl_with(&st, &cl, r.clone());
        c.sync_queued(Duration::from_secs(1), Duration::from_secs(300))
            .await;
        assert!(cl.requests.lock().unwrap().is_empty());
        // The item left `capacities`: it "became obsolete".
        r.refresh_sc("triple-sc");
        r.set_capacities(HashSet::new());
        c.sync_queued(Duration::from_secs(1), Duration::from_secs(300))
            .await;
        assert!(cl.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_item_is_requeued_with_exponential_backoff_until_it_succeeds() {
        let (st, cl, r) = refresh_fixture(&[&[("layer0", "foo")]]).await;
        *cl.fail.lock().unwrap() = true;
        r.refresh_sc("direct-sc");
        let c = ctl_with(&st, &cl, r.clone());
        c.sync_queued(Duration::from_millis(10), Duration::from_secs(1))
            .await;
        // `AddRateLimited`: not queued yet, but scheduled.
        assert!(r.queued().is_empty());
        assert!(r.next_delayed().is_some());
        *cl.fail.lock().unwrap() = false;
        tokio::time::sleep(Duration::from_millis(30)).await;
        c.sync_queued(Duration::from_millis(10), Duration::from_secs(1))
            .await;
        // `Forget` after success: nothing pending.
        assert!(r.next_delayed().is_none());
        assert!(r.queued().is_empty());
    }

    #[test]
    fn rate_limiter_doubles_per_failure_and_caps_at_max() {
        let r = CapacityRefresher::new();
        let k = QueueKey::Item {
            id: 1,
            item: ItemKey {
                segment: seg(&[]),
                storage_class_name: "sc".into(),
            },
        };
        let (s, m) = (Duration::from_secs(1), Duration::from_secs(5));
        let d: Vec<_> = (0..5)
            .map(|_| r.add_rate_limited(k.clone(), s, m))
            .collect();
        assert_eq!(
            d,
            [1, 2, 4, 5, 5].map(Duration::from_secs).to_vec(),
            "ItemExponentialFailureRateLimiter.When"
        );
        r.forget(&k);
        assert_eq!(r.add_rate_limited(k, s, m), s);
    }

    // ---- TestTermToSegment (capacity_test.go:1686-1797) ----

    #[test]
    fn term_to_segment_rejects_match_fields() {
        let t = NodeSelectorTerm {
            match_expressions: None,
            match_fields: Some(vec![req("name", "In", &["worker-1"])]),
        };
        assert!(term_to_segment(&t).is_err());
    }

    #[test]
    fn term_to_segment_rejects_non_in_operator_and_multi_values() {
        let t = |r| NodeSelectorTerm {
            match_expressions: Some(vec![r]),
            match_fields: None,
        };
        assert!(term_to_segment(&t(req("segment", "NotIn", &["a"]))).is_err());
        assert!(term_to_segment(&t(req("segment", "In", &["a", "b"]))).is_err());
    }

    #[test]
    fn term_to_segment_sorts_entries() {
        let t = NodeSelectorTerm {
            match_expressions: Some(vec![
                req("zone", "In", &["X"]),
                req("segment", "In", &["a"]),
            ]),
            match_fields: None,
        };
        assert_eq!(
            term_to_segment(&t).unwrap(),
            seg(&[("segment", "a"), ("zone", "X")])
        );
    }

    // ---- Segment ----

    #[test]
    fn segment_compare_is_length_first_then_entrywise() {
        // topology.go:57-71: fewer keys is smaller before any value is looked at.
        let short = seg(&[("z", "9")]);
        let long = seg(&[("a", "1"), ("b", "1")]);
        assert_eq!(short.compare(&long), Ordering::Less);
        assert_eq!(long.compare(&short), Ordering::Greater);
        assert_eq!(long.compare(&long.clone()), Ordering::Equal);
        assert_eq!(
            seg(&[("a", "1")]).compare(&seg(&[("a", "2")])),
            Ordering::Less
        );
    }

    #[test]
    fn segment_label_selector_is_match_labels_only() {
        let s = seg(&[("a", "1"), ("b", "2")]);
        let sel = s.label_selector();
        assert_eq!(sel.match_labels.unwrap(), s.label_map());
        assert!(sel.match_expressions.is_none());
        assert_eq!(s.label_map().len(), 2);
    }

    // ---- nodeTopology.sync (nodes_test.go TestNodeTopology) ----

    fn csi_node(name: &str, driver: &str, keys: Option<Vec<&str>>) -> CSINode {
        CSINode {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new(name),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: driver.into(),
                    node_id: name.into(),
                    topology_keys: keys.map(|k| k.into_iter().map(String::from).collect()),
                    allocatable: None,
                }],
            },
        }
    }

    fn node(name: &str, labels: &[(&str, &str)]) -> Node {
        let mut n = Node::new(name);
        n.metadata.labels = Some(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        n
    }

    #[test]
    fn discover_segments_dedups_and_skips_other_drivers_and_missing_labels() {
        let csi_nodes = vec![
            csi_node("n1", DRIVER, Some(vec!["B", "A"])),
            csi_node("n2", DRIVER, Some(vec!["A", "B"])),
            csi_node("n3", "other-driver", Some(vec!["A", "B"])),
            // label B not on the Node yet: ignored until the node update.
            csi_node("n4", DRIVER, Some(vec!["A", "B"])),
            // CSINode without a Node: obsolete, ignored.
            csi_node("n5", DRIVER, Some(vec!["A", "B"])),
            // driver without topology keys: the empty segment (matches all nodes).
            csi_node("n6", DRIVER, None),
        ];
        let nodes = vec![
            node("n1", &[("A", "x"), ("B", "y")]),
            node("n2", &[("A", "x"), ("B", "y")]),
            node("n3", &[("A", "x"), ("B", "y")]),
            node("n4", &[("A", "x")]),
            node("n6", &[]),
        ];
        let mut got = discover_segments(&csi_nodes, &nodes, DRIVER);
        got.sort_by(|a, b| a.compare(b));
        assert_eq!(
            got,
            vec![Segment::default(), seg(&[("A", "x"), ("B", "y")])]
        );
    }

    // ---- capacity reconcile (capacity_test.go TestCapacityController) ----

    #[derive(Default)]
    struct FakeClient {
        capacity: Mutex<i64>,
        max: Mutex<Option<i64>>,
        requests: Mutex<Vec<GetCapacityRequest>>,
        fail: Mutex<bool>,
        /// How long a `GetCapacity` call takes.
        delay: Mutex<Duration>,
    }

    impl CapacityClient for FakeClient {
        async fn get_capacity(
            &self,
            req: GetCapacityRequest,
        ) -> std::result::Result<GetCapacityResponse, String> {
            self.requests.lock().unwrap().push(req);
            let delay = *self.delay.lock().unwrap();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if *self.fail.lock().unwrap() {
                return Err("boom".into());
            }
            Ok(GetCapacityResponse {
                available_capacity: *self.capacity.lock().unwrap(),
                maximum_volume_size: *self.max.lock().unwrap(),
                ..Default::default()
            })
        }
    }

    fn sc(name: &str, provisioner: &str, mode: Option<VolumeBindingMode>) -> StorageClass {
        StorageClass {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new(name),
            provisioner: provisioner.into(),
            parameters: Some([("foo".to_string(), "bar".to_string())].into()),
            reclaim_policy: None,
            volume_binding_mode: mode,
            allowed_topologies: None,
            allow_volume_expansion: None,
            mount_options: None,
        }
    }

    fn owner() -> OwnerReference {
        let mut o = OwnerReference::new("apps/v1", "Deployment", "csi-ctrl", "owner-uid");
        o.controller = Some(true);
        o
    }

    async fn put_sc(st: &MemoryStorage, s: StorageClass) {
        st.create(&build_key("storageclasses", None, &s.metadata.name), &s)
            .await
            .unwrap();
    }

    async fn list_caps(st: &MemoryStorage) -> Vec<CSIStorageCapacity> {
        st.list::<CSIStorageCapacity>(&format!("/registry/csistoragecapacities/{NS}/"))
            .await
            .unwrap()
    }

    fn ctl<'a>(
        st: &'a MemoryStorage,
        cl: &'a FakeClient,
        immediate: bool,
    ) -> CapacityController<'a, MemoryStorage, FakeClient> {
        CapacityController {
            storage: st,
            client: cl,
            driver_name: DRIVER.into(),
            owner: Some(owner()),
            managed_by_id: MANAGED.into(),
            owner_namespace: NS.into(),
            immediate_binding: immediate,
            refresher: Default::default(),
            limiter: std::sync::Arc::new(rusternetes_client::ratelimit::RateLimiter::unlimited()),
        }
    }

    fn ctl_with<'a>(
        st: &'a MemoryStorage,
        cl: &'a FakeClient,
        refresher: std::sync::Arc<CapacityRefresher>,
    ) -> CapacityController<'a, MemoryStorage, FakeClient> {
        CapacityController {
            refresher,
            ..ctl(st, cl, false)
        }
    }

    #[tokio::test]
    async fn publishes_one_object_per_segment_and_class_with_labels_owner_and_sizes() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        *cl.max.lock().unwrap() = Some(1 << 20);
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        put_sc(&st, sc("foreign-sc", "other-driver", None)).await;
        let segs = vec![seg(&[("zone", "a")]), seg(&[("zone", "b")])];
        settle(&ctl(&st, &cl, false), &st, &segs).await;

        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 2);
        for c in &caps {
            assert!(c.metadata.name.starts_with("csisc-"));
            assert_eq!(c.metadata.namespace.as_deref(), Some(NS));
            assert_eq!(c.storage_class_name, "late-sc");
            assert_eq!(c.capacity.as_deref(), Some("1Gi"));
            assert_eq!(c.maximum_volume_size.as_deref(), Some("1Mi"));
            let l = c.metadata.labels.as_ref().unwrap();
            assert_eq!(l[DRIVER_NAME_LABEL], DRIVER);
            assert_eq!(l[MANAGED_BY_LABEL], MANAGED);
            let o = &c.metadata.owner_references.as_ref().unwrap()[0];
            assert_eq!(o.uid, "owner-uid");
        }
        // capacity.go:599-604: the request carries the class parameters and
        // the segment as AccessibleTopology, and no VolumeCapabilities.
        let reqs = cl.requests.lock().unwrap();
        assert_eq!(reqs.len(), 2);
        assert!(reqs.iter().all(|r| r.parameters["foo"] == "bar"
            && r.volume_capabilities.is_empty()
            && r.accessible_topology.is_some()));
    }

    #[tokio::test]
    async fn stats_count_goal_current_and_obsolete() {
        // capacity.go:769-807: goal = work items, current = items with an
        // object, obsolete = managed objects matching no item.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &[seg(&[("zone", "a")]), seg(&[("zone", "b")])]).await;
        // Every object is created and has been seen, so each item has one.
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 2,
                current: 2,
                obsolete: 0
            }
        );

        // Zone b disappears: its object is deleted, so nothing obsolete remains.
        settle(&c, &st, &[seg(&[("zone", "a")])]).await;
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 1,
                current: 1,
                obsolete: 0
            }
        );
        assert_eq!(list_caps(&st).await.len(), 1);
    }

    #[tokio::test]
    async fn the_gauges_are_exported_per_driver_at_scrape_time() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        c.register_metrics();
        settle(&c, &st, &[seg(&[("zone", "a")])]).await;
        let out = rusternetes_controller_manager::controllers::csi_capacity_metrics::gather();
        for (n, v) in [
            ("csistoragecapacities_desired_goal", 1),
            ("csistoragecapacities_desired_current", 1),
            ("csistoragecapacities_obsolete", 0),
        ] {
            let want = format!("{n}{{driver_name=\"{DRIVER}\"}} {v}");
            assert!(out.contains(&want), "{want} missing in:\n{out}");
        }
    }

    #[tokio::test]
    async fn unchanged_object_is_not_rewritten_and_changed_capacity_is_updated() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 100;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[("zone", "a")])];
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &segs).await;
        let first = list_caps(&st).await.remove(0);

        settle(&c, &st, &segs).await;
        let same = list_caps(&st).await.remove(0);
        assert_eq!(
            first.metadata.resource_version,
            same.metadata.resource_version
        );

        *cl.capacity.lock().unwrap() = 200;
        settle(&c, &st, &segs).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1, "update, not a second object");
        assert_eq!(caps[0].metadata.name, first.metadata.name);
        assert_eq!(caps[0].capacity.as_deref(), Some("200"));
    }

    #[tokio::test]
    async fn update_keeps_the_stored_identity() {
        // The API server's Store turns a non-empty metadata.uid on a PUT into
        // a UID precondition (rest/update.go:188-203): the update must carry
        // the stored uid and creationTimestamp, as upstream's DeepCopy does.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[("zone", "a")])];
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &segs).await;
        let before = list_caps(&st).await.remove(0);
        *cl.capacity.lock().unwrap() = 7;
        settle(&c, &st, &segs).await;
        let after = list_caps(&st).await.remove(0);
        assert_eq!(after.capacity.as_deref(), Some("7"));
        assert_eq!(after.metadata.uid, before.metadata.uid);
        assert_eq!(
            after.metadata.creation_timestamp,
            before.metadata.creation_timestamp
        );
    }

    #[tokio::test]
    async fn max_volume_size_change_and_missing_owner_trigger_update() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 100;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[])];
        let c = ctl(&st, &cl, false);
        settle(&c, &st, &segs).await;

        // capacity.go:643-650: MaximumVolumeSize differs -> update.
        *cl.max.lock().unwrap() = Some(10);
        settle(&c, &st, &segs).await;
        assert_eq!(
            list_caps(&st).await[0].maximum_volume_size.as_deref(),
            Some("10")
        );

        // capacity.go:641 / 660-662: owner reference stripped -> re-added.
        let mut obj = list_caps(&st).await.remove(0);
        obj.metadata.owner_references = None;
        st.update(
            &build_key("csistoragecapacities", Some(NS), &obj.metadata.name),
            &obj,
        )
        .await
        .unwrap();
        settle(&c, &st, &segs).await;
        let o = list_caps(&st)
            .await
            .remove(0)
            .metadata
            .owner_references
            .unwrap();
        assert_eq!(o.len(), 1);
        assert_eq!(o[0].uid, "owner-uid");
    }

    #[tokio::test]
    async fn obsolete_duplicate_and_unmanaged_objects() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 5;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        let both = vec![seg(&[("zone", "a")]), seg(&[("zone", "b")])];
        settle(&c, &st, &both).await;
        assert_eq!(list_caps(&st).await.len(), 2);

        // A foreign object (other managed-by) must survive (capacity.go:782-786).
        let mut foreign = list_caps(&st).await.remove(0);
        foreign.metadata.name = "foreign".into();
        foreign.metadata.uid = "foreign-uid".into();
        foreign
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert(MANAGED_BY_LABEL.into(), "someone-else".into());
        st.create(
            &build_key("csistoragecapacities", Some(NS), "foreign"),
            &foreign,
        )
        .await
        .unwrap();
        // A duplicate of a managed one: "should never happen, but ... clean that up".
        let mut dup = list_caps(&st)
            .await
            .into_iter()
            .find(|x| x.metadata.name != "foreign")
            .unwrap();
        dup.metadata.name = "dup".into();
        dup.metadata.uid = "dup-uid".into();
        st.create(&build_key("csistoragecapacities", Some(NS), "dup"), &dup)
            .await
            .unwrap();

        // Segment b disappears -> its object goes; the duplicate goes too.
        settle(&c, &st, &both[..1]).await;
        let names: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.metadata.name)
            .collect();
        assert!(names.contains(&"foreign".to_string()));
        assert_eq!(
            names.len(),
            2,
            "one managed object + the foreign one: {names:?}"
        );

        // Storage class gone -> everything managed goes.
        st.delete(&build_key("storageclasses", None, "late-sc"))
            .await
            .unwrap();
        settle(&c, &st, &both[..1]).await;
        let names: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.metadata.name)
            .collect();
        assert_eq!(names, vec!["foreign".to_string()]);
    }

    #[tokio::test]
    async fn immediate_binding_classes_are_skipped_unless_enabled() {
        // capacity.go:379-381 (onTopologyChanges) and :412-415 (onSCAddOrUpdate).
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("imm", DRIVER, Some(VolumeBindingMode::Immediate))).await;
        put_sc(
            &st,
            sc(
                "late",
                DRIVER,
                Some(VolumeBindingMode::WaitForFirstConsumer),
            ),
        )
        .await;
        let segs = vec![seg(&[("zone", "a")])];
        settle(&ctl(&st, &cl, false), &st, &segs).await;
        let classes: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.storage_class_name)
            .collect();
        assert_eq!(classes, vec!["late".to_string()]);
        settle(&ctl(&st, &cl, true), &st, &segs).await;
        assert_eq!(list_caps(&st).await.len(), 2);
    }

    #[test]
    fn managed_by_id_matches_upstream_naming() {
        assert_eq!(managed_by_id(None), "external-provisioner");
        assert_eq!(managed_by_id(Some("node-1")), "external-provisioner-node-1");
        let long = "n".repeat(80);
        let id = managed_by_id(Some(&long));
        assert!(id.len() <= 63, "{id}");
    }

    #[tokio::test]
    async fn lookup_owner_walks_controller_chain() {
        use serde_json::json;
        let st = MemoryStorage::new();
        st.create(
            &build_key("pods", Some(NS), "p"),
            &json!({"metadata":{"name":"p","uid":"pu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs","uid":"rsu","controller":true}]}}),
        )
        .await
        .unwrap();
        st.create(
            &build_key("replicasets", Some(NS), "rs"),
            &json!({"metadata":{"name":"rs","uid":"rsu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"Deployment","name":"d","uid":"du","controller":true}]}}),
        )
        .await
        .unwrap();
        let ul = RateLimiter::unlimited();
        let o0 = lookup_owner(&st, NS, "p", "v1", "Pod", 0, &ul)
            .await
            .unwrap();
        assert_eq!((o0.kind.as_str(), o0.uid.as_str()), ("Pod", "pu"));
        let o1 = lookup_owner(&st, NS, "p", "v1", "Pod", 1, &ul)
            .await
            .unwrap();
        assert_eq!((o1.kind.as_str(), o1.uid.as_str()), ("ReplicaSet", "rsu"));
        let o2 = lookup_owner(&st, NS, "p", "v1", "Pod", 2, &ul)
            .await
            .unwrap();
        assert_eq!((o2.kind.as_str(), o2.uid.as_str()), ("Deployment", "du"));
        assert_eq!(o2.controller, Some(true));
        assert!(lookup_owner(&st, NS, "p", "v1", "Pod", 3, &ul)
            .await
            .is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn lookup_owner_gets_go_through_the_capacity_rate_limiter() {
        // csi-provisioner.go:456-474: `config.QPS/Burst` are set to the capacity
        // values BEFORE `owner.Lookup(config, ...)`, which builds its own client
        // (pkg/owner/owner.go:37-44) with a token bucket of QPS 1 / burst 1
        // here: the 2nd GET of a two-level walk waits 1s.
        use serde_json::json;
        let st = MemoryStorage::new();
        st.create(
            &build_key("pods", Some(NS), "p"),
            &json!({"metadata":{"name":"p","uid":"pu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"ReplicaSet","name":"rs","uid":"rsu","controller":true}]}}),
        )
        .await
        .unwrap();
        st.create(
            &build_key("replicasets", Some(NS), "rs"),
            &json!({"metadata":{"name":"rs","uid":"rsu","ownerReferences":[
                {"apiVersion":"apps/v1","kind":"Deployment","name":"d","uid":"du","controller":true}]}}),
        )
        .await
        .unwrap();
        let l = RateLimiter::new(1.0, 1.0);
        let start = tokio::time::Instant::now();
        lookup_owner(&st, NS, "p", "v1", "Pod", 2, &l)
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn topology_queue_coalesces_a_burst_into_one_sync() {
        // nodes.go:63-150 handlers only `queue.Add("")`; RunWorker (:226-243)
        // syncs once per Get, so n adds before the worker runs are one sync.
        let q = TopologyQueue::default();
        for _ in 0..1000 {
            q.add();
        }
        tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .expect("one pending sync");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), q.get())
                .await
                .is_err(),
            "the burst must have been coalesced into a single sync"
        );
    }

    #[tokio::test]
    async fn topology_queue_add_while_syncing_queues_one_more_sync() {
        // workqueue Add during processing marks the item dirty; Done queues it
        // again exactly once.
        let q = TopologyQueue::default();
        q.add();
        q.get().await;
        q.add();
        q.add();
        tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .expect("one re-queued sync");
        assert!(tokio::time::timeout(Duration::from_millis(100), q.get())
            .await
            .is_err());
    }

    #[test]
    fn fixed_segment_sorts_entries() {
        let m: HashMap<String, String> = [
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ]
        .into();
        assert_eq!(fixed_segment(Some(&m)), seg(&[("a", "1"), ("b", "2")]));
        assert_eq!(fixed_segment(None), Segment::default());
    }

    #[tokio::test]
    async fn capacity_tick_discovers_segments_from_csinodes() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let c = ctl(&st, &cl, false);
        // No CSINodes: no segments, nothing published.
        c.prepare(&TopologySource::Discovered).await.unwrap();
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
        // Fixed topology publishes exactly one object.
        c.prepare(&TopologySource::Fixed(seg(&[("zone", "a")])))
            .await
            .unwrap();
        pump(&c, &st).await;
        assert_eq!(list_caps(&st).await.len(), 1);
    }

    #[test]
    fn driver_topology_keys_is_none_without_the_driver_and_empty_without_keys() {
        // nodes.go:175-185
        let csi = |driver: &str, keys: Option<Vec<&str>>| CSINode {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("n"),
            spec: CSINodeSpec {
                drivers: vec![CSINodeDriver {
                    name: driver.into(),
                    node_id: "n".into(),
                    topology_keys: keys.map(|k| k.into_iter().map(String::from).collect()),
                    allocatable: None,
                }],
            },
        };
        assert_eq!(driver_topology_keys(&csi("other", None), DRIVER), None);
        assert_eq!(
            driver_topology_keys(&csi(DRIVER, None), DRIVER),
            Some(vec![])
        );
        assert_eq!(
            driver_topology_keys(&csi(DRIVER, Some(vec!["a"])), DRIVER),
            Some(vec!["a".to_string()])
        );
    }

    // ---- event-driven triggers (#3548): capacity.go onSCAddOrUpdate etc. ----

    async fn list_scs(st: &MemoryStorage) -> Vec<StorageClass> {
        st.list::<StorageClass>(&build_prefix("storageclasses", None))
            .await
            .unwrap()
    }

    /// `prepare` (capacity.go:262-337): classes and topology are known, then
    /// the existing objects are fed to `onCAddOrUpdate`.
    async fn boot<'a>(
        st: &'a MemoryStorage,
        cl: &'a FakeClient,
        immediate: bool,
        segs: &[Segment],
    ) -> CapacityController<'a, MemoryStorage, FakeClient> {
        let c = ctl(st, cl, immediate);
        c.apply_sc_list(list_scs(st).await);
        c.apply_topology(segs.to_vec());
        c.apply_c_list(list_caps(st).await);
        c
    }

    /// A full pass of the controller's loops over `segs`: `prepare`, one
    /// `pollCapacities` tick and the workers, with the informer feeding their
    /// writes back.
    async fn settle(
        c: &CapacityController<'_, MemoryStorage, FakeClient>,
        st: &MemoryStorage,
        segs: &[Segment],
    ) {
        c.apply_sc_list(list_scs(st).await);
        c.apply_topology(segs.to_vec());
        c.apply_c_list(list_caps(st).await);
        // The queue keeps every add (fresh `QueueKey` pointers, capacity.go:
        // 115-120), so the entries of `apply_topology` would be synced as well
        // as those of the poll. upstream's test queue (capacity_test.go:
        // 1383-1403) collapses them; do the same here to count one sync per item.
        c.refresher.clear_queue();
        c.poll_capacities();
        pump(c, st).await;
    }

    /// The informer delivering the effects of the workers' writes back to the
    /// handlers, until nothing is left to do.
    async fn pump(c: &CapacityController<'_, MemoryStorage, FakeClient>, st: &MemoryStorage) {
        for _ in 0..6 {
            c.sync_queued(Duration::from_millis(1), Duration::from_secs(1))
                .await;
            c.apply_c_list(list_caps(st).await);
        }
    }

    fn queued_classes(c: &CapacityRefresher) -> Vec<String> {
        let mut v: Vec<String> = c
            .queued()
            .iter()
            .map(|i| i.storage_class_name.clone())
            .collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn sc_added_enqueues_one_item_per_known_segment() {
        // onSCAddOrUpdate (capacity.go:370-387)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        let c = boot(
            &st,
            &cl,
            false,
            &[seg(&[("zone", "a")]), seg(&[("zone", "b")])],
        )
        .await;
        assert!(queued_classes(&c.refresher).is_empty());
        put_sc(&st, sc("late", DRIVER, None)).await;
        c.apply_sc_list(list_scs(&st).await);
        assert_eq!(queued_classes(&c.refresher), vec!["late", "late"]);
        pump(&c, &st).await;
        assert_eq!(list_caps(&st).await.len(), 2);
    }

    #[tokio::test]
    async fn sc_of_other_driver_or_immediate_binding_is_ignored_on_add() {
        // capacity.go:371-379
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        put_sc(&st, sc("foreign", "other-driver", None)).await;
        put_sc(&st, sc("imm", DRIVER, Some(VolumeBindingMode::Immediate))).await;
        c.apply_sc_list(list_scs(&st).await);
        assert!(queued_classes(&c.refresher).is_empty());
        let c = boot(&st, &cl, true, &[seg(&[("zone", "a")])]).await;
        assert_eq!(queued_classes(&c.refresher), vec!["imm"]);
    }

    #[tokio::test]
    async fn sc_deleted_removes_items_and_enqueues_object_removal() {
        // onSCDelete (capacity.go:390-403) + removeWorkItem (:488-512)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        assert_eq!(list_caps(&st).await.len(), 1);
        st.delete(&build_key("storageclasses", None, "late"))
            .await
            .unwrap();
        c.apply_sc_list(list_scs(&st).await);
        assert_eq!(c.stats().goal, 0);
        // The object itself is queued for removal, not left for a poll tick.
        assert_eq!(list_caps(&st).await.len(), 1);
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
    }

    #[tokio::test]
    async fn topology_added_creates_objects_for_all_matching_classes() {
        // "add storage topology segment" (capacity_test.go:927-1001)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        put_sc(&st, sc("imm", DRIVER, Some(VolumeBindingMode::Immediate))).await;
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[]).await;
        c.apply_topology(vec![seg(&[("zone", "a")])]);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].storage_class_name, "late");
        assert_eq!(c.stats().goal, 1);
        assert_eq!(c.stats().current, 1);
    }

    #[tokio::test]
    async fn topology_removed_deletes_objects_and_added_creates_new_ones() {
        // "add and remove storage topology segment" (capacity_test.go:1039-1085)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        c.apply_topology(vec![seg(&[("zone", "b")])]);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1);
        assert_eq!(
            caps[0].node_topology.as_ref().unwrap().match_labels,
            Some([("zone".to_string(), "b".to_string())].into())
        );
        c.apply_topology(vec![]);
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
    }

    #[tokio::test]
    async fn capacity_deleted_by_someone_is_recreated() {
        // "re-create capacity" (capacity_test.go:744-783); onCDelete (:728-741)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        let before = list_caps(&st).await.remove(0);
        st.delete(&build_key(
            "csistoragecapacities",
            Some(NS),
            &before.metadata.name,
        ))
        .await
        .unwrap();
        c.informer_c_delete(&before);
        pump(&c, &st).await;
        let after = list_caps(&st).await;
        assert_eq!(after.len(), 1);
        assert_ne!(after[0].metadata.uid, before.metadata.uid);
    }

    #[tokio::test]
    async fn capacity_that_lost_its_labels_is_left_alone_and_recreated() {
        // "ignore capacity after owner change" (capacity_test.go:793-844);
        // onCAddOrUpdate !isManaged branch (:688-701)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        let mut obj = list_caps(&st).await.remove(0);
        obj.metadata.labels = None;
        st.update(
            &build_key("csistoragecapacities", Some(NS), &obj.metadata.name),
            &obj,
        )
        .await
        .unwrap();
        c.apply_c_list(list_caps(&st).await);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 2, "foreign object kept, new one created");
        assert_eq!(
            caps.iter().filter(|c| c.metadata.labels.is_some()).count(),
            1
        );
    }

    #[tokio::test]
    async fn delete_and_recreate_by_someone_with_wrong_capacity_is_corrected() {
        // "delete and recreate by someone" (capacity_test.go:845-892)
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        pump(&c, &st).await;
        let old = list_caps(&st).await.remove(0);
        st.delete(&build_key(
            "csistoragecapacities",
            Some(NS),
            &old.metadata.name,
        ))
        .await
        .unwrap();
        let mut new = old.clone();
        new.metadata.uid = "uid-2".into();
        new.capacity = Some("1Mi".into());
        st.create(
            &build_key("csistoragecapacities", Some(NS), &new.metadata.name),
            &new,
        )
        .await
        .unwrap();
        // Watch order: Deleted(old), then Added(new).
        c.informer_c_delete(&old);
        let stored = list_caps(&st).await.remove(0);
        c.informer_c_upsert(&stored);
        pump(&c, &st).await;
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].capacity.as_deref(), Some("1Gi"));
    }

    #[tokio::test]
    async fn redundant_managed_capacity_is_deleted_on_add() {
        // "delete redundant capacity" (capacity_test.go:784-792); :723-725
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        let c = boot(&st, &cl, false, &[]).await;
        let mut dup = CSIStorageCapacity {
            type_meta: TypeMeta::default(),
            metadata: ObjectMeta::new("csisc-dup").with_namespace(NS),
            storage_class_name: "late".into(),
            capacity: Some("1Gi".into()),
            maximum_volume_size: None,
            node_topology: Some(seg(&[("zone", "a")]).label_selector()),
        };
        dup.metadata.uid = "dup-uid".into();
        dup.metadata.labels = Some(
            [
                (DRIVER_NAME_LABEL.to_string(), DRIVER.to_string()),
                (MANAGED_BY_LABEL.to_string(), MANAGED.to_string()),
            ]
            .into(),
        );
        st.create(
            &build_key("csistoragecapacities", Some(NS), "csisc-dup"),
            &dup,
        )
        .await
        .unwrap();
        c.informer_c_upsert(&dup);
        pump(&c, &st).await;
        assert!(list_caps(&st).await.is_empty());
    }

    #[tokio::test]
    async fn gauges_are_computed_lazily_from_current_state() {
        // CollectWithStability (capacity.go:751-767): no tick stores them; an
        // object created by a worker is not counted until its event arrives
        // (:642-645).
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &[seg(&[("zone", "a")])]).await;
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 1,
                current: 0,
                obsolete: 0
            }
        );
        c.sync_queued(Duration::from_millis(1), Duration::from_secs(1))
            .await;
        assert_eq!(c.stats().current, 0, "created, event not seen yet");
        c.apply_c_list(list_caps(&st).await);
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 1,
                current: 1,
                obsolete: 0
            }
        );
        // "obsolete object, missing SC" (capacity_test.go:440-459)
        st.delete(&build_key("storageclasses", None, "late"))
            .await
            .unwrap();
        c.apply_sc_list(list_scs(&st).await);
        assert_eq!(
            c.stats(),
            CapacityStats {
                goal: 0,
                current: 0,
                obsolete: 1
            }
        );
    }

    /// Poll `f` until it holds, for at most 5s.
    async fn eventually(what: &str, mut f: impl AsyncFnMut() -> bool) {
        for _ in 0..250 {
            if f().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    #[tokio::test]
    async fn run_loop_reacts_to_watch_events_long_before_the_poll_tick() {
        // The StorageClass, Node and CSINode informers drive the queue
        // (capacity.go:188-219 + nodes.go:63-150); the poll period is an hour.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 1 << 30;
        let c = ctl(&st, &cl, false);
        let run = run_capacity_loop(
            &c,
            &TopologySource::Discovered,
            Duration::from_secs(3600),
            Duration::from_millis(10),
            Duration::from_secs(1),
            2,
        );
        let scenario = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            put_sc(&st, sc("late-sc", DRIVER, None)).await;
            let n = node("n1", &[("zone", "a")]);
            st.create(&build_key("nodes", None, "n1"), &n)
                .await
                .unwrap();
            let csi = csi_node("n1", DRIVER, Some(vec!["zone"]));
            st.create(&build_key("csinodes", None, "n1"), &csi)
                .await
                .unwrap();
            eventually("object for the new segment", async || {
                list_caps(&st).await.len() == 1
            })
            .await;
            // A new label on the node changes the segment: new object, old gone.
            let mut n = node("n1", &[("zone", "b")]);
            n.metadata = st
                .get::<Node>(&build_key("nodes", None, "n1"))
                .await
                .unwrap()
                .metadata;
            n.metadata.labels = Some([("zone".to_string(), "b".to_string())].into());
            st.update(&build_key("nodes", None, "n1"), &n)
                .await
                .unwrap();
            eventually("object moved to zone b", async || {
                let caps = list_caps(&st).await;
                caps.len() == 1
                    && caps[0].node_topology.as_ref().unwrap().match_labels
                        == Some([("zone".to_string(), "b".to_string())].into())
            })
            .await;
            // The StorageClass goes: its object is removed by the delete event.
            st.delete(&build_key("storageclasses", None, "late-sc"))
                .await
                .unwrap();
            eventually("object removed with its class", async || {
                list_caps(&st).await.is_empty()
            })
            .await;
        };
        tokio::select! {
            _ = run => unreachable!("the loop never ends"),
            _ = scenario => {}
        }
    }

    // ---- #3547: capacity clientset rate limit and --capacity-threads ----

    fn zones(n: usize) -> Vec<Segment> {
        (0..n).map(|i| seg(&[("zone", &format!("z{i}"))])).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_creates_and_deletes_go_through_the_capacity_rate_limiter() {
        // csi-provisioner.go:456-458 builds the clientset that creates, updates
        // and deletes CSIStorageCapacity objects with its own QPS/Burst
        // (token bucket, rest/config.go:370-381): burst 2 at 1 QPS lets two
        // creates through at once, the 3rd and 4th wait 1s and 2s.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        put_sc(&st, sc("late", DRIVER, None)).await;
        let limited = |qps: f64, burst: f64| CapacityController {
            limiter: std::sync::Arc::new(rusternetes_client::ratelimit::RateLimiter::new(
                qps, burst,
            )),
            ..ctl(&st, &cl, false)
        };
        let c = limited(1.0, 2.0);
        let start = tokio::time::Instant::now();
        settle(&c, &st, &zones(4)).await;
        assert_eq!(list_caps(&st).await.len(), 4);
        assert_eq!(start.elapsed(), Duration::from_secs(2));

        // Deletes of obsolete objects are throttled by the same bucket kind:
        // burst 1 at 1 QPS, four deletes take 3s.
        let c = limited(1.0, 1.0);
        let start = tokio::time::Instant::now();
        settle(&c, &st, &[]).await;
        assert!(list_caps(&st).await.is_empty());
        assert_eq!(start.elapsed(), Duration::from_secs(3));
    }

    #[test]
    fn capacity_limiter_follows_client_go_defaulting() {
        // rest/config.go:370-381: 0 -> DefaultQPS/DefaultBurst, qps <= 0 -> none.
        let l = capacity_limiter(1.0, 5);
        assert_eq!((l.qps(), l.burst()), (1.0, 5.0));
        let l = capacity_limiter(0.0, 0);
        assert_eq!((l.qps(), l.burst()), (5.0, 10.0));
        assert!(capacity_limiter(-1.0, 5).qps() <= 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_threads_workers_process_the_queue_concurrently() {
        // Controller.Run (capacity.go:238-253) starts `threadiness` workers on
        // the one queue; four 100ms GetCapacity calls take 100ms with four.
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.delay.lock().unwrap() = Duration::from_millis(100);
        put_sc(&st, sc("late", DRIVER, None)).await;
        let c = boot(&st, &cl, false, &zones(4)).await;
        let start = tokio::time::Instant::now();
        let run = c.run_workers(4, Duration::from_millis(1), Duration::from_secs(1));
        tokio::select! {
            _ = run => unreachable!("workers never end"),
            _ = eventually("four objects", async || list_caps(&st).await.len() == 4) => {}
        }
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "took {:?}",
            start.elapsed()
        );
        // The same item is never processed by two workers at once, so no
        // duplicate objects appear (checked by the count above plus a tick).
        c.apply_c_list(list_caps(&st).await);
        c.poll_capacities();
        tokio::select! {
            _ = c.run_workers(4, Duration::from_millis(1), Duration::from_secs(1)) => unreachable!(),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
        assert_eq!(list_caps(&st).await.len(), 4);
    }

    #[test]
    fn workqueue_does_not_hand_out_a_key_that_is_being_processed() {
        // client-go workqueue Add/Get/Done: re-adding the same key in flight
        // marks it dirty; it is queued again by Done, never processed twice at
        // once. (Fresh adds of an item are fresh keys, see
        // `queue_does_not_deduplicate_fresh_adds`; the same key is added again
        // by `AddRateLimited` and by `Done`.)
        let r = CapacityRefresher::new();
        let k = ItemKey {
            segment: seg(&[("zone", "a")]),
            storage_class_name: "sc".into(),
        };
        r.set_capacities([k.clone()].into());
        r.refresh_sc("sc");
        let key = r.pop().expect("queued");
        r.lock().add(key.clone());
        r.lock().add(key.clone());
        assert_eq!(r.pop(), None, "in flight: must not be handed out again");
        r.done(&key);
        assert_eq!(r.pop(), Some(key.clone()));
        assert_eq!(r.pop(), None, "dirty twice, queued once");
    }
}
