//! VolumeBinding scheduler plugin: PreFilter + Filter (`FindPodVolumes`).
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/volumebinding/{volume_binding.go,binder.go}`
//! and `staging/src/k8s.io/component-helpers/storage/volume/pv_helpers.go`.

use rusternetes_common::resources::{
    CSIDriver, CSIStorageCapacity, Node, PersistentVolume, PersistentVolumeClaim, Pod, StorageClass,
};
use std::collections::HashMap;

pub const ERR_REASON_BIND_CONFLICT: &str =
    "node(s) didn't find available persistent volumes to bind";
pub const ERR_REASON_NODE_CONFLICT: &str = "node(s) didn't match PersistentVolume's node affinity";
pub const ERR_REASON_NOT_ENOUGH_SPACE: &str = "node(s) did not have enough free storage";
pub const ERR_REASON_PV_NOT_EXIST: &str =
    "node(s) unavailable due to one or more pvc(s) bound to non-existent pv(s)";

/// Listers the plugin reads (upstream: PVC/PV/StorageClass/CSIDriver/
/// CSIStorageCapacity listers).
#[derive(Default, Clone)]
pub struct VolumeSnapshot {
    pub pvcs: Vec<PersistentVolumeClaim>,
    pub pvs: Vec<PersistentVolume>,
    pub classes: Vec<StorageClass>,
    pub csi_drivers: Vec<CSIDriver>,
    pub csi_capacities: Vec<CSIStorageCapacity>,
}

#[derive(Default, Clone)]
pub struct PodVolumeClaims {
    pub bound_claims: Vec<PersistentVolumeClaim>,
    pub unbound_claims_immediate: Vec<PersistentVolumeClaim>,
    pub unbound_claims_delay_binding: Vec<PersistentVolumeClaim>,
    pub unbound_volumes_delay_binding: HashMap<String, Vec<PersistentVolume>>,
}

pub enum PreFilterOutcome {
    Skip,
    Unresolvable(String),
    Error(String),
    Ready(PodVolumeClaims),
}

#[derive(Clone)]
pub struct BindingInfo {
    pub pv: PersistentVolume,
    pub pvc: PersistentVolumeClaim,
}

#[derive(Clone)]
pub struct DynamicProvision {
    pub pvc: PersistentVolumeClaim,
    pub node_capacity: Option<CSIStorageCapacity>,
}

#[derive(Default, Clone)]
pub struct PodVolumes {
    pub static_bindings: Vec<BindingInfo>,
    pub dynamic_provisions: Vec<DynamicProvision>,
}

pub fn pre_filter(_pod: &Pod, _snap: &VolumeSnapshot) -> PreFilterOutcome {
    PreFilterOutcome::Skip
}

pub fn find_pod_volumes(
    _pod: &Pod,
    _claims: &PodVolumeClaims,
    _node: &Node,
    _snap: &VolumeSnapshot,
) -> Result<(PodVolumes, Vec<&'static str>), String> {
    Ok((PodVolumes::default(), Vec::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
