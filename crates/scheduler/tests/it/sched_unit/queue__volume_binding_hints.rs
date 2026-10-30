//! Unit tests moved from `src/queue/volume_binding_hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::quantity::Quantity;
    pub use rusternetes_common::resources::{
        CSIDriver, CSINode, CSIStorageCapacity, PersistentVolumeClaim, Pod, StorageClass,
    };
    pub use rusternetes_scheduler::queue::events::*;
    pub use rusternetes_scheduler::volume_binding::{pod_volumes, volume_pvc_name};
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::volume_binding_hints::*;
    use serde_json::{json, Value};

    type HintFn =
        fn(&Pod, Option<&EventObject>, Option<&EventObject>) -> Result<QueueingHint, String>;

    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }
    fn pod(ns: Option<&str>, vols: Value) -> Pod {
        let mut meta = json!({"name": "pod-a"});
        if let Some(ns) = ns {
            meta["namespace"] = json!(ns);
        }
        serde_json::from_value(
            json!({"metadata": meta, "spec": {"containers": [], "volumes": vols}}),
        )
        .unwrap()
    }
    fn pvc_vol(c: &str) -> Value {
        json!({"name": c, "persistentVolumeClaim": {"claimName": c}})
    }
    fn eph_vol(n: &str) -> Value {
        json!({"name": n, "ephemeral": {}})
    }
    fn pvc(name: &str) -> PersistentVolumeClaim {
        serde_json::from_value(json!({"metadata": {"name": name}, "spec": {}})).unwrap()
    }
    fn run(
        f: HintFn,
        p: &Pod,
        old: Option<Box<EventObject>>,
        new: Option<Box<EventObject>>,
    ) -> Result<QueueingHint, String> {
        f(p, old.as_deref(), new.as_deref())
    }

    /// `TestIsSchedulableAfterCSINodeChange`.
    #[test]
    fn csi_node_change() {
        let node = |ann: Option<&str>| -> CSINode {
            let mut meta = json!({"name": "csinode-a"});
            if let Some(a) = ann {
                meta["annotations"] = json!({MIGRATED_PLUGINS_ANNOTATION_KEY: a});
            }
            serde_json::from_value(json!({"metadata": meta, "spec": {"drivers": []}})).unwrap()
        };
        let p = pod(None, json!([]));
        let f: HintFn = is_schedulable_after_csi_node_change;
        assert!(run(f, &p, Some(obj(())), Some(obj(()))).is_err());
        assert_eq!(
            run(f, &p, None, Some(obj(node(None)))).unwrap(),
            QueueingHint::Queue
        );
        for (name, o, n, want) in [
            ("added", "test1", "test1, test2", QueueingHint::Queue),
            ("updated", "test1", "test2", QueueingHint::Queue),
            ("unchanged", "test1", "test1", QueueingHint::QueueSkip),
        ] {
            assert_eq!(
                run(f, &p, Some(obj(node(Some(o)))), Some(obj(node(Some(n))))).unwrap(),
                want,
                "{name}"
            );
        }
    }

    /// `TestIsSchedulableAfterPersistentVolumeClaimChange`.
    #[test]
    fn pvc_change() {
        let f: HintFn = is_schedulable_after_pvc_change;
        let cases = [
            (
                "pod has no pvc or ephemeral volumes",
                pod(None, json!([{"name": "e", "emptyDir": {}}])),
                pvc("pvc-b"),
                QueueingHint::QueueSkip,
            ),
            (
                "pvc in a different namespace",
                pod(Some("ns-a"), json!([pvc_vol("pvc-a"), pvc_vol("pvc-b")])),
                pvc("pvc-b"),
                QueueingHint::QueueSkip,
            ),
            (
                "pod has no pvc that is being modified",
                pod(None, json!([pvc_vol("pvc-a"), pvc_vol("pvc-c")])),
                pvc("pvc-b"),
                QueueingHint::QueueSkip,
            ),
            (
                "pod has no generic ephemeral volume that is being modified",
                pod(
                    None,
                    json!([eph_vol("ephemeral-a"), eph_vol("ephemeral-c")]),
                ),
                pvc("pod-a-ephemeral-b"),
                QueueingHint::QueueSkip,
            ),
            (
                "pod has the pvc that is being modified",
                pod(None, json!([pvc_vol("pvc-a"), pvc_vol("pvc-b")])),
                pvc("pvc-b"),
                QueueingHint::Queue,
            ),
            (
                "pod has the generic ephemeral volume that is being modified",
                pod(
                    None,
                    json!([eph_vol("ephemeral-a"), eph_vol("ephemeral-b")]),
                ),
                pvc("pod-a-ephemeral-b"),
                QueueingHint::Queue,
            ),
        ];
        for (name, p, c, want) in cases {
            assert_eq!(
                run(f, &p, Some(obj(c.clone())), Some(obj(c))).unwrap(),
                want,
                "{name}"
            );
        }
        let p = pod(None, json!([]));
        assert!(run(f, &p, Some(obj(())), Some(obj(()))).is_err());
    }

    fn sc(topo: Option<Value>) -> StorageClass {
        let mut v = json!({"metadata": {"name": "sc-a"}, "provisioner": "p"});
        if let Some(t) = topo {
            v["allowedTopologies"] = t;
        }
        serde_json::from_value(v).unwrap()
    }

    /// `TestIsSchedulableAfterStorageClassChange`.
    #[test]
    fn storage_class_change() {
        let f: HintFn = is_schedulable_after_storage_class_change;
        let p = pod(None, json!([]));
        let topo = json!([{"matchLabelExpressions": [
            {"key": "kubernetes.io/hostname", "values": ["node-a"]}]}]);
        assert_eq!(
            run(f, &p, None, Some(obj(sc(None)))).unwrap(),
            QueueingHint::Queue
        );
        assert_eq!(
            run(f, &p, Some(obj(sc(None))), Some(obj(sc(Some(topo))))).unwrap(),
            QueueingHint::Queue
        );
        assert_eq!(
            run(f, &p, Some(obj(sc(None))), Some(obj(sc(None)))).unwrap(),
            QueueingHint::QueueSkip
        );
        // nil and empty AllowedTopologies are Semantic.DeepEqual.
        assert_eq!(
            run(f, &p, Some(obj(sc(None))), Some(obj(sc(Some(json!([])))))).unwrap(),
            QueueingHint::QueueSkip
        );
        assert!(run(f, &p, Some(obj(())), Some(obj(()))).is_err());
    }

    fn cap(capacity: Option<&str>, max: Option<&str>) -> CSIStorageCapacity {
        let mut v = json!({"metadata": {"name": "cap-a"}, "storageClassName": "sc"});
        if let Some(c) = capacity {
            v["capacity"] = json!(c);
        }
        if let Some(m) = max {
            v["maximumVolumeSize"] = json!(m);
        }
        serde_json::from_value(v).unwrap()
    }

    /// `TestIsSchedulableAfterCSIStorageCapacityChange`.
    #[test]
    fn csi_storage_capacity_change() {
        use QueueingHint::{Queue, QueueSkip};
        let f: HintFn = is_schedulable_after_csi_storage_capacity_change;
        let p = pod(None, json!([]));
        assert_eq!(run(f, &p, None, Some(obj(cap(None, None)))).unwrap(), Queue);
        type Q = (Option<&'static str>, Option<&'static str>);
        let cases: [(&str, Q, Q, QueueingHint); 10] = [
            ("Capacity set", (None, None), (Some("100"), None), Queue),
            (
                "MaximumVolumeSize set",
                (None, None),
                (None, Some("100")),
                Queue,
            ),
            (
                "Capacity increase",
                (Some("50"), None),
                (Some("100"), None),
                Queue,
            ),
            (
                "MaximumVolumeSize unset",
                (Some("100"), Some("50")),
                (Some("100"), None),
                Queue,
            ),
            (
                "MaximumVolumeSize increase",
                (Some("100"), Some("50")),
                (Some("100"), Some("60")),
                Queue,
            ),
            (
                "no changes",
                (Some("100"), Some("50")),
                (Some("100"), Some("50")),
                QueueSkip,
            ),
            (
                "equal (Capacity)",
                (Some("100"), None),
                (Some("100"), None),
                QueueSkip,
            ),
            (
                "equal (MaximumVolumeSize unset)",
                (Some("100"), Some("50")),
                (Some("50"), None),
                QueueSkip,
            ),
            (
                "decrease (Capacity)",
                (Some("100"), None),
                (Some("90"), None),
                QueueSkip,
            ),
            (
                "decrease (MaximumVolumeSize)",
                (None, Some("100")),
                (None, Some("90")),
                QueueSkip,
            ),
        ];
        for (name, (oc, om), (nc, nm), want) in cases {
            assert_eq!(
                run(f, &p, Some(obj(cap(oc, om))), Some(obj(cap(nc, nm)))).unwrap(),
                want,
                "{name}"
            );
        }
        assert!(run(f, &p, Some(obj(())), Some(obj(()))).is_err());
    }

    fn driver(name: &str, sc: Option<bool>) -> CSIDriver {
        let mut spec = json!({});
        if let Some(b) = sc {
            spec["storageCapacity"] = json!(b);
        }
        serde_json::from_value(json!({"metadata": {"name": name}, "spec": spec})).unwrap()
    }

    /// `TestIsSchedulableAfterCSIDriverChange`.
    #[test]
    fn csi_driver_change() {
        use QueueingHint::{Queue, QueueSkip};
        let f: HintFn = is_schedulable_after_csi_driver_change;
        let csi_pod = pod(None, json!([{"name": "c", "csi": {"driver": "test1"}}]));
        let none_pod = pod(None, json!([]));
        assert_eq!(
            run(
                f,
                &none_pod,
                Some(obj(driver("test1", None))),
                Some(obj(driver("test1", Some(true))))
            )
            .unwrap(),
            QueueSkip,
            "pod has no CSIDriver"
        );
        assert!(run(f, &none_pod, Some(obj(())), Some(obj(()))).is_err());
        type C = (
            &'static str,
            &'static str,
            Option<bool>,
            &'static str,
            Option<bool>,
            QueueingHint,
        );
        let cases: [C; 6] = [
            (
                "different names",
                "test2",
                Some(true),
                "test2",
                Some(false),
                QueueSkip,
            ),
            ("original nil", "test1", None, "test1", None, QueueSkip),
            (
                "original false",
                "test1",
                Some(false),
                "test1",
                Some(false),
                QueueSkip,
            ),
            ("modified nil", "test1", Some(true), "test1", None, Queue),
            (
                "modified true",
                "test1",
                Some(true),
                "test1",
                Some(true),
                QueueSkip,
            ),
            (
                "modified false",
                "test1",
                Some(true),
                "test1",
                Some(false),
                Queue,
            ),
        ];
        for (name, on, oc, nn, nc, want) in cases {
            assert_eq!(
                run(
                    f,
                    &csi_pod,
                    Some(obj(driver(on, oc))),
                    Some(obj(driver(nn, nc)))
                )
                .unwrap(),
                want,
                "{name}"
            );
        }
    }
}
