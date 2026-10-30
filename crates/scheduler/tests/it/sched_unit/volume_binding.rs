//! Unit tests moved from `src/volume_binding.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::affinity::matches_node_selector;
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::quantity::{Format, Quantity};
    pub use rusternetes_common::resources::volume::{
        PersistentVolumeClaimPhase, PersistentVolumeMode, PersistentVolumePhase,
        TopologySelectorTerm, VolumeBindingMode,
    };
    pub use rusternetes_common::resources::{
        CSIDriver, CSINode, CSIStorageCapacity, Node, PersistentVolume, PersistentVolumeClaim, Pod,
        StorageClass, Volume,
    };
    pub use rusternetes_common::types::{label_selector_as_selector, LabelSelector};
    pub use std::collections::HashMap;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::volume_binding::*;
    use serde_json::{json, Value};

    fn pod(volumes: Value) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p", "namespace": "ns", "uid": "pod-uid"},
            "spec": {"containers": [{"name": "c", "image": "i"}], "volumes": volumes}
        }))
        .unwrap()
    }
    fn pod_with_pvc(claim: &str) -> Pod {
        pod(json!([{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]))
    }
    fn node(name: &str, labels: Value) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "labels": labels}, "spec": {}
        }))
        .unwrap()
    }
    fn pvc(name: &str, extra_spec: Value, ann: Value, status: Value) -> PersistentVolumeClaim {
        let mut spec = json!({"resources": {"requests": {"storage": "1Gi"}}});
        for (k, v) in extra_spec.as_object().unwrap() {
            spec[k] = v.clone();
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "ns", "uid": format!("{name}-uid"), "annotations": ann},
            "spec": spec, "status": status
        }))
        .unwrap()
    }
    fn bound_pvc(name: &str, pv: &str) -> PersistentVolumeClaim {
        pvc(
            name,
            json!({"volumeName": pv, "storageClassName": "std"}),
            json!({"pv.kubernetes.io/bind-completed": "yes"}),
            json!({"phase": "Bound"}),
        )
    }
    fn wffc_pvc(name: &str, class: &str, ann: Value) -> PersistentVolumeClaim {
        pvc(
            name,
            json!({"storageClassName": class}),
            ann,
            json!({"phase": "Pending"}),
        )
    }
    fn pv(name: &str, class: &str, size: &str, aff: Option<Value>) -> PersistentVolume {
        let mut spec = json!({
            "capacity": {"storage": size}, "accessModes": ["ReadWriteOnce"],
            "storageClassName": class, "hostPath": {"path": "/x"}
        });
        if let Some(a) = aff {
            spec["nodeAffinity"] = a;
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name}, "spec": spec, "status": {"phase": "Available"}
        }))
        .unwrap()
    }
    fn zone_affinity(zone: &str) -> Value {
        json!({"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "zone", "operator": "In", "values": [zone]}]}]}})
    }
    fn class(name: &str, mode: &str, provisioner: &str, topo: Option<Value>) -> StorageClass {
        let mut v = json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": name}, "provisioner": provisioner, "volumeBindingMode": mode
        });
        if let Some(t) = topo {
            v["allowedTopologies"] = t;
        }
        serde_json::from_value(v).unwrap()
    }
    fn ready(p: &Pod, s: &VolumeSnapshot) -> PodVolumeClaims {
        match pre_filter(p, s) {
            PreFilterOutcome::Ready(c) => c,
            PreFilterOutcome::Skip => panic!("got Skip"),
            PreFilterOutcome::Unresolvable(m) | PreFilterOutcome::Error(m) => panic!("got {m}"),
        }
    }
    fn filter(p: &Pod, n: &Node, s: &VolumeSnapshot) -> (PodVolumes, Vec<&'static str>) {
        find_pod_volumes(p, &ready(p, s), n, s).unwrap()
    }

    #[test]
    fn pod_without_pvcs_is_skipped() {
        let p = pod(json!([{"name": "v", "emptyDir": {}}]));
        assert!(matches!(
            pre_filter(&p, &VolumeSnapshot::default()),
            PreFilterOutcome::Skip
        ));
    }

    #[test]
    fn missing_pvc_is_unresolvable() {
        match pre_filter(&pod_with_pvc("foo"), &VolumeSnapshot::default()) {
            PreFilterOutcome::Unresolvable(m) => {
                assert_eq!(m, "persistentvolumeclaim \"foo\" not found")
            }
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn lost_pvc_is_unresolvable() {
        let c = pvc(
            "foo",
            json!({"volumeName": "pv1"}),
            json!({}),
            json!({"phase": "Lost"}),
        );
        let s = VolumeSnapshot {
            pvcs: vec![c],
            ..Default::default()
        };
        match pre_filter(&pod_with_pvc("foo"), &s) {
            PreFilterOutcome::Unresolvable(m) => assert_eq!(
                m,
                "persistentvolumeclaim \"foo\" bound to non-existent persistentvolume \"pv1\""
            ),
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn deleting_pvc_is_unresolvable() {
        let mut c = bound_pvc("foo", "pv1");
        c.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let s = VolumeSnapshot {
            pvcs: vec![c],
            ..Default::default()
        };
        match pre_filter(&pod_with_pvc("foo"), &s) {
            PreFilterOutcome::Unresolvable(m) => {
                assert_eq!(m, "persistentvolumeclaim \"foo\" is being deleted")
            }
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn unbound_immediate_pvc_is_unresolvable() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "imm", json!({}))],
            classes: vec![class("imm", "Immediate", "p", None)],
            ..Default::default()
        };
        match pre_filter(&pod_with_pvc("foo"), &s) {
            PreFilterOutcome::Unresolvable(m) => {
                assert_eq!(m, "pod has unbound immediate PersistentVolumeClaims")
            }
            _ => panic!("expected Unresolvable"),
        }
    }

    #[test]
    fn bound_pvc_checks_pv_node_affinity() {
        let s = VolumeSnapshot {
            pvcs: vec![bound_pvc("foo", "pv1")],
            pvs: vec![pv("pv1", "std", "1Gi", Some(zone_affinity("a")))],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let (_, r) = filter(&p, &node("n1", json!({"zone": "a"})), &s);
        assert!(r.is_empty());
        let (_, r) = filter(&p, &node("n2", json!({"zone": "b"})), &s);
        assert_eq!(r, vec![ERR_REASON_NODE_CONFLICT]);
    }

    fn gce_bound_snapshot(csi_node_annotations: Option<Value>) -> VolumeSnapshot {
        let mut pv = pv(
            "pv1",
            "std",
            "1Gi",
            Some(json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "topology.kubernetes.io/zone", "operator": "In", "values": ["a"]}]}]}})),
        );
        pv.spec.host_path = None;
        pv.spec.gce_persistent_disk = serde_json::from_value(json!({"pdName": "d1"})).unwrap();
        let mut s = VolumeSnapshot {
            pvcs: vec![bound_pvc("foo", "pv1")],
            pvs: vec![pv],
            ..Default::default()
        };
        if let Some(ann) = csi_node_annotations {
            s.csi_nodes = vec![serde_json::from_value(json!({
                "apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
                "metadata": {"name": "n1", "annotations": ann}, "spec": {"drivers": []}
            }))
            .unwrap()];
        }
        s
    }

    // binder.go checkBoundClaims -> tryTranslatePVToCSI: the PV's zone
    // affinity is rewritten to the CSI topology key when the node's CSINode
    // lists the plugin as migrated, so a node carrying only the CSI key matches.
    #[test]
    fn migrated_in_tree_pv_affinity_is_translated_for_csi_node() {
        let s = gce_bound_snapshot(Some(json!({
            "storage.alpha.kubernetes.io/migrated-plugins": "kubernetes.io/gce-pd"
        })));
        let p = pod_with_pvc("foo");
        let (_, r) = filter(&p, &node("n1", json!({"topology.gke.io/zone": "a"})), &s);
        assert!(r.is_empty(), "{r:?}");
    }

    #[test]
    fn unmigrated_in_tree_pv_affinity_is_untouched() {
        let p = pod_with_pvc("foo");
        let n = node("n1", json!({"topology.gke.io/zone": "a"}));
        for s in [
            gce_bound_snapshot(None),
            gce_bound_snapshot(Some(json!({}))),
            gce_bound_snapshot(Some(json!({
                "storage.alpha.kubernetes.io/migrated-plugins": "kubernetes.io/aws-ebs"
            }))),
        ] {
            let (_, r) = filter(&p, &n, &s);
            assert_eq!(r, vec![ERR_REASON_NODE_CONFLICT]);
        }
    }

    #[test]
    fn bound_pvc_with_missing_pv_reports_pv_not_exist() {
        let s = VolumeSnapshot {
            pvcs: vec![bound_pvc("foo", "gone")],
            ..Default::default()
        };
        let (_, r) = filter(&pod_with_pvc("foo"), &node("n1", json!({})), &s);
        assert_eq!(r, vec![ERR_REASON_PV_NOT_EXIST]);
    }

    #[test]
    fn wffc_claim_finds_matching_pv_on_node() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            pvs: vec![pv("pv1", "wait", "2Gi", Some(zone_affinity("a")))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "kubernetes.io/no-provisioner",
                None,
            )],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let (vols, r) = filter(&p, &node("n1", json!({"zone": "a"})), &s);
        assert!(r.is_empty());
        assert_eq!(vols.static_bindings.len(), 1);
        assert_eq!(vols.static_bindings[0].pv.metadata.name, "pv1");
        let (vols, r) = filter(&p, &node("n2", json!({"zone": "b"})), &s);
        assert_eq!(r, vec![ERR_REASON_BIND_CONFLICT]);
        assert!(vols.static_bindings.is_empty());
    }

    #[test]
    fn smallest_fitting_pv_chosen_and_not_reused_across_claims() {
        let s = VolumeSnapshot {
            pvcs: vec![
                wffc_pvc("a", "wait", json!({})),
                wffc_pvc("b", "wait", json!({})),
            ],
            pvs: vec![
                pv("big", "wait", "10Gi", None),
                pv("small", "wait", "2Gi", None),
            ],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "kubernetes.io/no-provisioner",
                None,
            )],
            ..Default::default()
        };
        let p = pod(json!([
            {"name": "va", "persistentVolumeClaim": {"claimName": "a"}},
            {"name": "vb", "persistentVolumeClaim": {"claimName": "b"}}
        ]));
        let (vols, r) = filter(&p, &node("n1", json!({})), &s);
        assert!(r.is_empty());
        let mut names: Vec<_> = vols
            .static_bindings
            .iter()
            .map(|b| b.pv.metadata.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["big", "small"]);
    }

    #[test]
    fn no_provisioner_class_without_pv_is_bind_conflict() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "kubernetes.io/no-provisioner",
                None,
            )],
            ..Default::default()
        };
        let (_, r) = filter(&pod_with_pvc("foo"), &node("n1", json!({})), &s);
        assert_eq!(r, vec![ERR_REASON_BIND_CONFLICT]);
    }

    #[test]
    fn provisionable_class_without_pv_yields_dynamic_provision() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                None,
            )],
            ..Default::default()
        };
        let (vols, r) = filter(&pod_with_pvc("foo"), &node("n1", json!({})), &s);
        assert!(r.is_empty());
        assert_eq!(vols.dynamic_provisions.len(), 1);
        assert!(vols.static_bindings.is_empty());
    }

    #[test]
    fn selected_node_annotation_pins_provisioning_to_that_node() {
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc(
                "foo",
                "wait",
                json!({"volume.kubernetes.io/selected-node": "n1"}),
            )],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                None,
            )],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let (vols, r) = filter(&p, &node("n1", json!({})), &s);
        assert!(r.is_empty());
        assert_eq!(vols.dynamic_provisions.len(), 1);
        let (_, r) = filter(&p, &node("n2", json!({})), &s);
        assert_eq!(r, vec![ERR_REASON_BIND_CONFLICT]);
    }

    #[test]
    fn allowed_topologies_restrict_provisioning_nodes() {
        let topo = json!([{"matchLabelExpressions": [{"key": "zone", "values": ["a"]}]}]);
        let s = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                Some(topo),
            )],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        assert!(filter(&p, &node("n1", json!({"zone": "a"})), &s)
            .1
            .is_empty());
        assert_eq!(
            filter(&p, &node("n2", json!({"zone": "b"})), &s).1,
            vec![ERR_REASON_BIND_CONFLICT]
        );
    }

    fn csi_driver(storage_capacity: bool) -> CSIDriver {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIDriver",
            "metadata": {"name": "example.com/csi"},
            "spec": {"storageCapacity": storage_capacity}
        }))
        .unwrap()
    }
    fn capacity(zone: &str, size: &str) -> CSIStorageCapacity {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIStorageCapacity",
            "metadata": {"name": format!("cap-{zone}"), "namespace": "ns"},
            "storageClassName": "wait", "capacity": size,
            "nodeTopology": {"matchLabels": {"zone": zone}}
        }))
        .unwrap()
    }

    #[test]
    fn csi_capacity_gates_dynamic_provisioning() {
        let base = VolumeSnapshot {
            pvcs: vec![wffc_pvc("foo", "wait", json!({}))],
            classes: vec![class(
                "wait",
                "WaitForFirstConsumer",
                "example.com/csi",
                None,
            )],
            csi_drivers: vec![csi_driver(true)],
            ..Default::default()
        };
        let p = pod_with_pvc("foo");
        let n = node("n1", json!({"zone": "a"}));
        // Driver opts in but no capacity object: not enough space.
        assert_eq!(filter(&p, &n, &base).1, vec![ERR_REASON_NOT_ENOUGH_SPACE]);
        // Too small.
        let mut s = base.clone();
        s.csi_capacities = vec![capacity("a", "500Mi")];
        assert_eq!(filter(&p, &n, &s).1, vec![ERR_REASON_NOT_ENOUGH_SPACE]);
        // Wrong topology.
        let mut s = base.clone();
        s.csi_capacities = vec![capacity("b", "5Gi")];
        assert_eq!(filter(&p, &n, &s).1, vec![ERR_REASON_NOT_ENOUGH_SPACE]);
        // Sufficient and reachable.
        let mut s = base.clone();
        s.csi_capacities = vec![capacity("a", "5Gi")];
        let (vols, r) = filter(&p, &n, &s);
        assert!(r.is_empty());
        assert!(vols.dynamic_provisions[0].node_capacity.is_some());
        // Driver without storageCapacity: capacity not checked.
        let mut s = base;
        s.csi_drivers = vec![csi_driver(false)];
        assert!(filter(&p, &n, &s).1.is_empty());
    }

    #[test]
    fn ephemeral_volume_pvc_must_be_owned_by_pod() {
        let p =
            pod(json!([{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]));
        let mut c = bound_pvc("p-scratch", "pv1");
        let s = VolumeSnapshot {
            pvcs: vec![c.clone()],
            ..Default::default()
        };
        match pre_filter(&p, &s) {
            PreFilterOutcome::Error(m) | PreFilterOutcome::Unresolvable(m) => {
                assert!(m.contains("was not created for pod ns/p"), "{m}")
            }
            _ => panic!("expected failure for un-owned ephemeral PVC"),
        }
        c.metadata.owner_references = Some(vec![serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod", "name": "p", "uid": "pod-uid", "controller": true
        }))
        .unwrap()]);
        let s = VolumeSnapshot {
            pvcs: vec![c],
            ..Default::default()
        };
        assert!(matches!(pre_filter(&p, &s), PreFilterOutcome::Ready(_)));
    }

    #[test]
    fn missing_ephemeral_pvc_waits_for_controller() {
        let p =
            pod(json!([{"name": "scratch", "ephemeral": {"volumeClaimTemplate": {"spec": {}}}}]));
        match pre_filter(&p, &VolumeSnapshot::default()) {
            PreFilterOutcome::Unresolvable(m) => assert_eq!(
                m,
                "waiting for ephemeral volume controller to create the persistentvolumeclaim \"p-scratch\""
            ),
            _ => panic!("expected Unresolvable"),
        }
    }

    // ---- scorer_test.go TestScore ----

    fn scaled(points: &[(i64, i64)]) -> Vec<FunctionShapePoint> {
        points
            .iter()
            .map(|&(u, sc)| FunctionShapePoint {
                utilization: u,
                score: sc * (MAX_NODE_SCORE / MAX_CUSTOM_PRIORITY_SCORE),
            })
            .collect()
    }
    /// `defaultShapePoint` of volume_binding_test.go:80-89 (prefer high utilization).
    fn prefer_utilization() -> Vec<FunctionShapePoint> {
        scaled(&[(0, 0), (100, 10)])
    }
    fn res(requested: i64, capacity: i64) -> StorageResource {
        StorageResource {
            requested,
            capacity,
        }
    }
    fn classes(items: &[(&str, i64, i64)]) -> ClassResourceMap {
        items
            .iter()
            .map(|&(c, r, cap)| (c.to_string(), res(r, cap)))
            .collect()
    }

    #[test]
    fn scorer_default_shape_single_class() {
        let f = build_scorer_function(&prefer_utilization());
        for (req, want) in [(0, 0), (30, 30), (50, 50), (100, 100)] {
            assert_eq!(f(&classes(&[("hdd", req, 100)])), want, "req {req}");
        }
    }

    #[test]
    fn scorer_default_shape_multiple_classes() {
        let f = build_scorer_function(&prefer_utilization());
        for (hdd, ssd, want) in [
            (0, 0, 0),
            (0, 30, 15),
            (30, 30, 30),
            (30, 60, 45),
            (50, 50, 50),
            (50, 100, 75),
            (100, 100, 100),
        ] {
            assert_eq!(
                f(&classes(&[("hdd", hdd, 100), ("ssd", ssd, 100)])),
                want,
                "hdd {hdd} ssd {ssd}"
            );
        }
    }

    #[test]
    fn scorer_custom_shape_multiple_classes() {
        let f = build_scorer_function(&[
            FunctionShapePoint {
                utilization: 50,
                score: 0,
            },
            FunctionShapePoint {
                utilization: 80,
                score: 30,
            },
            FunctionShapePoint {
                utilization: 100,
                score: 50,
            },
        ]);
        for (hdd, ssd, want) in [
            (0, 0, 0),
            (0, 30, 0),
            (30, 30, 0),
            (30, 60, 5),
            (50, 100, 25),
            (90, 90, 40),
            (100, 100, 50),
        ] {
            assert_eq!(
                f(&classes(&[("hdd", hdd, 100), ("ssd", ssd, 100)])),
                want,
                "hdd {hdd} ssd {ssd}"
            );
        }
    }

    #[test]
    fn scorer_empty_map_zero_capacity_and_overcommit() {
        let f = build_scorer_function(&prefer_utilization());
        assert_eq!(f(&ClassResourceMap::new()), 0);
        // capacity == 0 || requested > capacity => rawScoringFunction(maxUtilization)
        assert_eq!(f(&classes(&[("hdd", 10, 0)])), 100);
        assert_eq!(f(&classes(&[("hdd", 200, 100)])), 100);
    }

    #[test]
    fn default_shape_is_max_score_at_zero_utilization() {
        // SetDefaults_VolumeBindingArgs (apis/config/v1/defaults.go:196-207)
        let f = build_scorer_function(&default_shape());
        assert_eq!(f(&classes(&[("c", 0, 100)])), 100);
        assert_eq!(f(&classes(&[("c", 100, 100)])), 0);
    }

    // ---- volume_binding_test.go score cases ----

    fn node_named(name: &str) -> Node {
        node(name, json!({"kubernetes.io/hostname": name, "node": name}))
    }
    fn sized_pvc(name: &str, class: &str, size: &str) -> PersistentVolumeClaim {
        pvc(
            name,
            json!({"storageClassName": class, "resources": {"requests": {"storage": size}}}),
            json!({}),
            json!({"phase": "Pending"}),
        )
    }
    fn host_affinity(node: &str) -> Value {
        json!({"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "In", "values": [node]}]}]}})
    }
    fn node_capacity(node: &str, class: &str, size: &str) -> CSIStorageCapacity {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSIStorageCapacity",
            "metadata": {"name": format!("cap-{node}"), "namespace": "ns"},
            "storageClassName": class, "capacity": size,
            "nodeTopology": {"matchLabels": {"node": node}}
        }))
        .unwrap()
    }
    fn pod_with_claims(claims: &[&str]) -> Pod {
        pod(serde_json::Value::Array(
            claims
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    json!({"name": format!("v{i}"), "persistentVolumeClaim": {"claimName": c}})
                })
                .collect(),
        ))
    }
    /// Filter every node (keeping `PodVolumes` per node like `podVolumesByNode`),
    /// then PreScore + Score. Filter-failed nodes score 0 (upstream's test
    /// expects 0 for them).
    fn run_scores(
        p: &Pod,
        nodes: &[Node],
        s: &VolumeSnapshot,
        shape: &[FunctionShapePoint],
    ) -> Vec<i64> {
        let claims = ready(p, s);
        let mut by_node = HashMap::new();
        for n in nodes {
            let (vols, reasons) = find_pod_volumes(p, &claims, n, s).unwrap();
            if reasons.is_empty() {
                by_node.insert(n.metadata.name.clone(), vols);
            }
        }
        assert!(!pre_score_skip(&by_node, true));
        let scorer = build_scorer_function(shape);
        nodes
            .iter()
            .map(|n| match by_node.get(&n.metadata.name) {
                Some(v) => score(Some(v), &scorer, true),
                None => 0,
            })
            .collect()
    }
    fn wait_class(name: &str, provisioner: &str) -> StorageClass {
        class(name, "WaitForFirstConsumer", provisioner, None)
    }
    fn local_pvs(size_a: &str, size_b: &str, class: &str, tag: &str) -> Vec<PersistentVolume> {
        vec![
            pv(
                &format!("pv-a-{tag}0"),
                class,
                size_a,
                Some(host_affinity("node-a")),
            ),
            pv(
                &format!("pv-a-{tag}1"),
                class,
                size_a,
                Some(host_affinity("node-a")),
            ),
            pv(
                &format!("pv-b-{tag}0"),
                class,
                size_b,
                Some(host_affinity("node-b")),
            ),
            pv(
                &format!("pv-b-{tag}1"),
                class,
                size_b,
                Some(host_affinity("node-b")),
            ),
        ]
    }
    const NOPROV: &str = "kubernetes.io/no-provisioner";

    fn three_nodes() -> [Node; 3] {
        [
            node_named("node-a"),
            node_named("node-b"),
            node_named("node-c"),
        ]
    }

    #[test]
    fn score_local_volumes_with_close_capacity_are_preferred() {
        let s = VolumeSnapshot {
            pvcs: vec![sized_pvc("pvc-a", "wait", "50Gi")],
            pvs: local_pvs("200Gi", "100Gi", "wait", ""),
            classes: vec![wait_class("wait", NOPROV)],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-a"]),
            &three_nodes(),
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![25, 50, 0]);
    }

    #[test]
    fn score_local_volumes_with_close_capacity_multiple_pvcs() {
        let mut pvs = local_pvs("200Gi", "100Gi", "wait", "");
        pvs.extend(local_pvs("200Gi", "100Gi", "wait-hdd", "h"));
        let s = VolumeSnapshot {
            pvcs: vec![
                sized_pvc("pvc-0", "wait", "50Gi"),
                sized_pvc("pvc-1", "wait-hdd", "100Gi"),
            ],
            pvs,
            classes: vec![wait_class("wait", NOPROV), wait_class("wait-hdd", NOPROV)],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-0", "pvc-1"]),
            &three_nodes(),
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![38, 75, 0]);
    }

    fn dynamic_snapshot(sizes: &[(&str, &str)], request: &str) -> (VolumeSnapshot, Vec<Node>) {
        let nodes: Vec<Node> = sizes.iter().map(|(n, _)| node_named(n)).collect();
        let s = VolumeSnapshot {
            pvcs: vec![sized_pvc("pvc-dynamic", "wait-cap", request)],
            classes: vec![wait_class("wait-cap", "example.com/csi")],
            csi_drivers: vec![csi_driver(true)],
            csi_capacities: sizes
                .iter()
                .map(|(n, sz)| node_capacity(n, "wait-cap", sz))
                .collect(),
            ..Default::default()
        };
        (s, nodes)
    }

    #[test]
    fn score_storage_capacity_prefers_least_free() {
        let (s, nodes) = dynamic_snapshot(
            &[("node-a", "100Gi"), ("node-b", "50Gi"), ("node-c", "10Gi")],
            "10Gi",
        );
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic"]),
            &nodes,
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![10, 20, 100]);
    }

    #[test]
    fn score_prefer_node_with_least_allocatable() {
        let (s, nodes) = dynamic_snapshot(
            &[("node-a", "100Gi"), ("node-b", "20Gi"), ("node-c", "10Gi")],
            "10Gi",
        );
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic"]),
            &nodes,
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![10, 50, 100]);
    }

    #[test]
    fn score_prefer_node_with_maximum_allocatable_custom_shape() {
        let (s, nodes) = dynamic_snapshot(
            &[("node-a", "100Gi"), ("node-b", "20Gi"), ("node-c", "10Gi")],
            "10Gi",
        );
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic"]),
            &nodes,
            &s,
            &default_shape(),
        );
        assert_eq!(got, vec![90, 50, 0]);
    }

    #[test]
    fn score_dynamic_provisioning_same_class_uses_capacity_once() {
        // Capacity is assigned (not summed) per class: 2 x 50Gi on 100Gi => 100.
        let s = VolumeSnapshot {
            pvcs: vec![
                sized_pvc("pvc-dynamic-0", "wait-cap", "50Gi"),
                sized_pvc("pvc-dynamic-1", "wait-cap", "50Gi"),
            ],
            classes: vec![wait_class("wait-cap", "example.com/csi")],
            csi_drivers: vec![csi_driver(true)],
            csi_capacities: vec![node_capacity("node-a", "wait-cap", "100Gi")],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic-0", "pvc-dynamic-1"]),
            &[node_named("node-a")],
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![100]);
    }

    #[test]
    fn score_with_static_binds_ignores_dynamic_provisions() {
        let s = VolumeSnapshot {
            pvcs: vec![
                sized_pvc("pvc-dynamic", "wait-cap", "10Gi"),
                sized_pvc("pvc-static", "wait", "50Gi"),
            ],
            pvs: ["a", "b", "c"]
                .iter()
                .map(|n| {
                    pv(
                        &format!("pv-static-{n}"),
                        "wait",
                        "100Gi",
                        Some(host_affinity(&format!("node-{n}"))),
                    )
                })
                .collect(),
            classes: vec![
                wait_class("wait", NOPROV),
                wait_class("wait-cap", "example.com/csi"),
            ],
            csi_drivers: vec![csi_driver(true)],
            csi_capacities: vec![
                node_capacity("node-a", "wait-cap", "100Gi"),
                node_capacity("node-b", "wait-cap", "50Gi"),
                node_capacity("node-c", "wait-cap", "10Gi"),
            ],
            ..Default::default()
        };
        let got = run_scores(
            &pod_with_claims(&["pvc-dynamic", "pvc-static"]),
            &three_nodes(),
            &s,
            &prefer_utilization(),
        );
        assert_eq!(got, vec![50, 50, 50]);
    }

    #[test]
    fn pre_score_skip_rules() {
        // volume_binding.go PreScore: skip unless static bindings exist or
        // StorageCapacityScoring is on.
        let mut by_node = HashMap::new();
        by_node.insert("n".to_string(), PodVolumes::default());
        assert!(pre_score_skip(&by_node, false));
        assert!(!pre_score_skip(&by_node, true));
        let s = VolumeSnapshot {
            pvcs: vec![sized_pvc("a", "wait", "1Gi")],
            pvs: vec![pv("pv", "wait", "2Gi", None)],
            classes: vec![wait_class("wait", NOPROV)],
            ..Default::default()
        };
        let (vols, _) = filter(&pod_with_claims(&["a"]), &node_named("n"), &s);
        let mut with_static = HashMap::new();
        with_static.insert("n".to_string(), vols);
        assert!(!pre_score_skip(&with_static, false));
    }

    #[test]
    fn score_without_pod_volumes_for_node_is_zero() {
        let scorer = build_scorer_function(&prefer_utilization());
        assert_eq!(score(None, &scorer, true), 0);
    }
}
