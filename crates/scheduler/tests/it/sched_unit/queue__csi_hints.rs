//! Unit tests moved from `src/queue/csi_hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::csi_translation::CsiTranslator;
    pub use rusternetes_common::resources::{
        CSINode, CSINodeDriver, PersistentVolumeClaim, Pod, Volume, VolumeAttachment,
    };
    pub use rusternetes_scheduler::queue::events::*;
    pub use std::collections::HashMap;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::csi_hints::*;
    use serde_json::{json, Value};

    fn pod(ns: &str, name: &str, node: &str, vols: Value) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": ns},
            "spec": {"nodeName": node, "volumes": vols}
        }))
        .unwrap()
    }
    fn pvc_vol(claim: &str) -> Value {
        json!({"name": "v", "persistentVolumeClaim": {"claimName": claim}})
    }
    fn ebs_vol() -> Value {
        json!({"name": "i", "awsElasticBlockStore": {"volumeID": "test"}})
    }
    fn gce_vol() -> Value {
        json!({"name": "g", "gcePersistentDisk": {"pdName": "test"}})
    }
    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }
    fn pvc(ns: &str, name: &str) -> PersistentVolumeClaim {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": ns}, "spec": {}
        }))
        .unwrap()
    }
    fn va(attacher: &str) -> VolumeAttachment {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttachment",
            "metadata": {"name": "volumeattachment1"},
            "spec": {"attacher": attacher, "nodeName": "fake-node",
                     "source": {"persistentVolumeName": "pv1"}}
        }))
        .unwrap()
    }

    /// `TestCSILimitsQHint` (csi_test.go:668).
    #[test]
    fn pod_deleted() {
        let target = pod("", "t", "", json!([pvc_vol("csi-ebs.csi.aws.com-2")]));
        let cases = [
            (
                "deleted pod has a PVC",
                pod("", "d", "n", json!([pvc_vol("placeholder")])),
                QueueingHint::Queue,
            ),
            (
                "deleted pod has an inline migratable volume",
                pod("", "d", "n", json!([ebs_vol()])),
                QueueingHint::Queue,
            ),
            (
                "deleted pod has no volume",
                pod("", "d", "n", json!([])),
                QueueingHint::QueueSkip,
            ),
            (
                "deleted pod is not scheduled",
                pod("", "d", "", json!([pvc_vol("csi-ebs.csi.aws.com-0")])),
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, deleted, want) in cases {
            let o = obj(deleted);
            assert_eq!(
                is_schedulable_after_pod_deleted(&target, Some(&*o), None).unwrap(),
                want,
                "{name}"
            );
        }
    }

    /// `TestCSILimitsAddedPVCQHint` (csi_test.go:733).
    #[test]
    fn pvc_added() {
        let eph = json!([{"name": "ephemeral", "ephemeral": {}}]);
        let cases = [
            (
                "different namespace",
                pod("ns1", "p", "", json!([])),
                pvc("ns2", ""),
                QueueingHint::QueueSkip,
            ),
            (
                "same namespace, same PVC",
                pod("ns1", "p", "", json!([pvc_vol("pvc1")])),
                pvc("ns1", "pvc1"),
                QueueingHint::Queue,
            ),
            (
                "ephemeral volume related to the added PVC",
                pod("ns1", "pod1", "", eph),
                pvc("ns1", "pod1-ephemeral"),
                QueueingHint::Queue,
            ),
            (
                "pod doesn't have the same PVC",
                pod("ns1", "p", "", json!([pvc_vol("pvc1")])),
                pvc("ns1", "pvc2"),
                QueueingHint::QueueSkip,
            ),
            (
                "pod has no PVC",
                pod("ns1", "p", "", json!([])),
                pvc("ns1", "pvc2"),
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, p, added, want) in cases {
            let o = obj(added);
            assert_eq!(
                is_schedulable_after_pvc_added(&p, None, Some(&*o)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    /// `TestCSILimitsDeletedVolumeAttachmentQHint` (csi_test.go:810).
    #[test]
    fn volume_attachment_deleted() {
        let cases = [
            (
                "pod has PVC",
                pod("ns1", "p", "", json!([pvc_vol("pvc1")])),
                "test.storage.gke.io",
                QueueingHint::Queue,
            ),
            (
                "inline migratable (EBS) matching attacher",
                pod("ns1", "p", "", json!([ebs_vol()])),
                "ebs.csi.aws.com",
                QueueingHint::Queue,
            ),
            (
                "inline migratable (GCE PD) vs EBS attacher",
                pod("ns1", "p", "", json!([gce_vol()])),
                "ebs.csi.aws.com",
                QueueingHint::QueueSkip,
            ),
            (
                "inline migratable and PVC",
                pod("ns1", "p", "", json!([ebs_vol(), pvc_vol("pvc1")])),
                "test.storage.gke.io",
                QueueingHint::Queue,
            ),
            (
                "no PVC",
                pod("ns1", "p", "", json!([])),
                "test.storage.gke.io",
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, p, attacher, want) in cases {
            let o = obj(va(attacher));
            assert_eq!(
                is_schedulable_after_volume_attachment_deleted(&p, Some(&*o), None).unwrap(),
                want,
                "{name}"
            );
        }
    }

    fn csi_node(drivers: Value) -> CSINode {
        serde_json::from_value(json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "CSINode",
            "metadata": {"name": "test-node"}, "spec": {"drivers": drivers}
        }))
        .unwrap()
    }
    fn driver(name: &str, count: Option<i32>) -> Value {
        let mut d = json!({"name": name, "nodeID": "test-node"});
        if let Some(c) = count {
            d["allocatable"] = json!({"count": c});
        }
        d
    }

    /// `TestCSILimitsAfterCSINodeUpdatedQHint` (csi_test.go:983).
    #[test]
    fn csi_node_updated() {
        let p = pod(
            "ns1",
            "test-pod",
            "",
            json!([pvc_vol("csi-ebs.csi.aws.com-0")]),
        );
        let e = "ebs.csi.aws.com";
        let cases = [
            (
                "limit raised",
                vec![driver(e, Some(1))],
                vec![driver(e, Some(2))],
                QueueingHint::Queue,
            ),
            (
                "limit decreased",
                vec![driver(e, Some(2))],
                vec![driver(e, Some(1))],
                QueueingHint::QueueSkip,
            ),
            (
                "limit unchanged",
                vec![driver(e, Some(1))],
                vec![driver(e, Some(1))],
                QueueingHint::QueueSkip,
            ),
            (
                "driver added with limit",
                vec![],
                vec![driver(e, Some(1))],
                QueueingHint::Queue,
            ),
            (
                "driver added without limit",
                vec![],
                vec![driver(e, None)],
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, old, new, want) in cases {
            let (o, n) = (obj(csi_node(json!(old))), obj(csi_node(json!(new))));
            assert_eq!(
                is_schedulable_after_csi_node_updated(&p, Some(&*o), Some(&*n)).unwrap(),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn wrong_object_type_is_an_error() {
        let p = pod("ns1", "p", "", json!([]));
        let o = obj(p.clone());
        assert!(is_schedulable_after_pvc_added(&p, None, Some(&*o)).is_err());
    }
}
