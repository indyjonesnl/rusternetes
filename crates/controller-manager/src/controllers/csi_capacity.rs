//! CSIStorageCapacity publisher (#3214, overlaps #3201), a port of
//! kubernetes-csi/external-provisioner v6.4.0 `pkg/capacity/capacity.go`
//! (`Controller`) and `pkg/capacity/topology/{topology,nodes}.go`
//! (`Segment`, `nodeTopology.sync`).
//!
//! The Go controller is event/queue driven. This port keeps its *decisions*
//! (which `(topology segment, StorageClass)` pairs get an object, when an
//! object is created / updated / deleted, ownership and labels) as one
//! level-triggered `reconcile` pass over `Storage`; wiring it into a run loop
//! is tracked separately.

use rusternetes_common::resources::csi::{CSINode, CSIStorageCapacity};
use rusternetes_common::resources::volume::NodeSelectorTerm;
use rusternetes_common::resources::Node;
use rusternetes_common::types::{LabelSelector, OwnerReference};
use rusternetes_common::Result;
use rusternetes_csi::proto::{GetCapacityRequest, GetCapacityResponse};
use rusternetes_storage::Storage;
use std::cmp::Ordering;
use std::collections::HashMap;

/// `DriverNameLabel` (capacity.go:51).
pub const DRIVER_NAME_LABEL: &str = "csi.storage.k8s.io/drivername";
/// `ManagedByLabel` (capacity.go:52).
pub const MANAGED_BY_LABEL: &str = "csi.storage.k8s.io/managed-by";

/// `topology.SegmentEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentEntry {
    pub key: String,
    pub value: String,
}

/// `topology.Segment`: entries sorted by key, keys unique.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Segment(Vec<SegmentEntry>);

impl Segment {
    /// `sort.Sort(&segment)` after building from entries.
    pub fn from_entries(mut entries: Vec<SegmentEntry>) -> Self {
        entries.sort_by(|a, b| (&a.key, &a.value).cmp(&(&b.key, &b.value)));
        Segment(entries)
    }

    pub fn entries(&self) -> &[SegmentEntry] {
        &self.0
    }

    /// `Segment.Compare` (topology.go:57-71).
    pub fn compare(&self, _other: &Segment) -> Ordering {
        todo!()
    }

    /// `Segment.GetLabelMap` (topology.go:98-109).
    pub fn label_map(&self) -> HashMap<String, String> {
        todo!()
    }

    /// `Segment.GetLabelSelector` (topology.go:91-96).
    pub fn label_selector(&self) -> LabelSelector {
        todo!()
    }
}

/// `termToSegment` (capacity.go:458-478).
pub fn term_to_segment(_term: &NodeSelectorTerm) -> std::result::Result<Segment, String> {
    todo!()
}

/// `nodeTopology.sync` (nodes.go:268-341), minus the callback diffing.
pub fn discover_segments(
    _csi_nodes: &[CSINode],
    _nodes: &[Node],
    _driver_name: &str,
) -> Vec<Segment> {
    todo!()
}

/// `CSICapacityClient` (capacity.go:136-138).
#[allow(async_fn_in_trait)]
pub trait CapacityClient {
    async fn get_capacity(
        &self,
        req: GetCapacityRequest,
    ) -> std::result::Result<GetCapacityResponse, String>;
}

/// The `Controller` fields that decide what gets published.
pub struct CapacityController<'a, S: Storage, C: CapacityClient> {
    pub storage: &'a S,
    pub client: &'a C,
    pub driver_name: String,
    pub owner: Option<OwnerReference>,
    pub managed_by_id: String,
    pub owner_namespace: String,
    pub immediate_binding: bool,
}

impl<S: Storage, C: CapacityClient> CapacityController<'_, S, C> {
    /// One pass of `onTopologyChanges` + `onCAddOrUpdate` + `syncCapacity`:
    /// publish, update and delete objects until they match `segments` x the
    /// driver's StorageClasses.
    pub async fn reconcile(&self, _segments: &[Segment]) -> Result<()> {
        todo!()
    }
}

#[allow(dead_code)]
fn _unused(_: &CSIStorageCapacity) {}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::resources::csi::{CSINodeDriver, CSINodeSpec};
    use rusternetes_common::resources::volume::{
        NodeSelectorRequirement, StorageClass, VolumeBindingMode,
    };
    use rusternetes_common::types::{ObjectMeta, TypeMeta};
    use rusternetes_storage::build_key;
    use rusternetes_storage::memory::MemoryStorage;
    use std::sync::Mutex;

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
    }

    impl CapacityClient for FakeClient {
        async fn get_capacity(
            &self,
            req: GetCapacityRequest,
        ) -> std::result::Result<GetCapacityResponse, String> {
            self.requests.lock().unwrap().push(req);
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
        ctl(&st, &cl, false).reconcile(&segs).await.unwrap();

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
    async fn unchanged_object_is_not_rewritten_and_changed_capacity_is_updated() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 100;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[("zone", "a")])];
        let c = ctl(&st, &cl, false);
        c.reconcile(&segs).await.unwrap();
        let first = list_caps(&st).await.remove(0);

        c.reconcile(&segs).await.unwrap();
        let same = list_caps(&st).await.remove(0);
        assert_eq!(
            first.metadata.resource_version,
            same.metadata.resource_version
        );

        *cl.capacity.lock().unwrap() = 200;
        c.reconcile(&segs).await.unwrap();
        let caps = list_caps(&st).await;
        assert_eq!(caps.len(), 1, "update, not a second object");
        assert_eq!(caps[0].metadata.name, first.metadata.name);
        assert_eq!(caps[0].capacity.as_deref(), Some("200"));
    }

    #[tokio::test]
    async fn max_volume_size_change_and_missing_owner_trigger_update() {
        let st = MemoryStorage::new();
        let cl = FakeClient::default();
        *cl.capacity.lock().unwrap() = 100;
        put_sc(&st, sc("late-sc", DRIVER, None)).await;
        let segs = vec![seg(&[])];
        let c = ctl(&st, &cl, false);
        c.reconcile(&segs).await.unwrap();

        // capacity.go:643-650: MaximumVolumeSize differs -> update.
        *cl.max.lock().unwrap() = Some(10);
        c.reconcile(&segs).await.unwrap();
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
        c.reconcile(&segs).await.unwrap();
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
        c.reconcile(&both).await.unwrap();
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
        c.reconcile(&both[..1]).await.unwrap();
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
        c.reconcile(&both[..1]).await.unwrap();
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
        ctl(&st, &cl, false).reconcile(&segs).await.unwrap();
        let classes: Vec<_> = list_caps(&st)
            .await
            .into_iter()
            .map(|c| c.storage_class_name)
            .collect();
        assert_eq!(classes, vec!["late".to_string()]);
        ctl(&st, &cl, true).reconcile(&segs).await.unwrap();
        assert_eq!(list_caps(&st).await.len(), 2);
    }
}
