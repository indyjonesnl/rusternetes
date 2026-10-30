//! Unit tests moved from `src/queue/volume_hints.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::volume::{
        PersistentVolume, PersistentVolumeClaim, StorageClass, VolumeBindingMode,
    };
    pub use rusternetes_common::resources::{Pod, Volume};
    pub use rusternetes_scheduler::queue::events::{as_typed, EventObject, QueueingHint};
    pub use std::collections::BTreeSet;
    pub use tracing::debug;
}

mod tests {
    use super::__parent::*;
    use rusternetes_scheduler::queue::volume_hints::*;
    use serde_json::json;

    fn obj<T: Send + Sync + 'static>(t: T) -> Box<EventObject> {
        Box::new(t)
    }

    fn pod_with_volumes(name: &str, ns: &str, volumes: serde_json::Value) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": ns},
            "spec": {"containers": [], "volumes": volumes}
        }))
        .unwrap()
    }

    fn vol(source: serde_json::Value) -> serde_json::Value {
        let mut v = json!({"name": "v"});
        v.as_object_mut()
            .unwrap()
            .extend(source.as_object().unwrap().clone());
        v
    }

    fn pvc_pod(name: &str, ns: &str, claims: &[&str]) -> Pod {
        let vols: Vec<_> = claims
            .iter()
            .map(|c| json!({"name": c, "persistentVolumeClaim": {"claimName": c}}))
            .collect();
        pod_with_volumes(name, ns, json!(vols))
    }

    fn pvc(ns: &str, name: &str, vol_name: &str) -> PersistentVolumeClaim {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": ns},
            "spec": {"volumeName": vol_name}
        }))
        .unwrap()
    }

    fn pv(name: &str, labels: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json!({"metadata": {"name": name, "labels": labels}})).unwrap()
    }

    const BETA_ZONE: &str = "failure-domain.beta.kubernetes.io/zone";
    const TOPO_ZONE: &str = "topology.kubernetes.io/zone";

    /// `Test_isSchedulableAfterPodDeleted` (`volume_restrictions_test.go:489-691`).
    #[test]
    fn volume_restrictions_after_pod_deleted() {
        use volume_restrictions::is_schedulable_after_pod_deleted as f;
        let gce = |n: &str| vol(json!({"gcePersistentDisk": {"pdName": n}}));
        let aws = |n: &str| vol(json!({"awsElasticBlockStore": {"volumeID": n}}));
        let rbd = |m: &[&str]| {
            vol(json!({"rbd": {"monitors": m, "pool": "foo", "image": "bar", "fsType": "ext4"}}))
        };
        let iscsi = |iqn: &str| {
            vol(
                json!({"iscsi": {"targetPortal": "127.0.0.1:3260", "iqn": iqn, "fsType": "ext4", "lun": 0}}),
            )
        };
        let p = |v: serde_json::Value| pod_with_volumes("p", "", json!([v]));
        let cases = [
            (
                "queue-new-object-gcedisk-conflict",
                p(gce("foo")),
                p(gce("foo")),
                QueueingHint::Queue,
            ),
            (
                "skip-new-object-gcedisk-no-conflict",
                p(gce("foo")),
                p(gce("bar")),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-new-object-awsdisk-conflict",
                p(aws("foo")),
                p(aws("foo")),
                QueueingHint::Queue,
            ),
            (
                "skip-new-object-awsdisk-no-conflict",
                p(aws("foo")),
                p(aws("bar")),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-new-object-rbddisk-conflict",
                p(rbd(&["a", "b"])),
                p(rbd(&["a", "b"])),
                QueueingHint::Queue,
            ),
            (
                "skip-new-object-rbddisk-no-conflict",
                p(rbd(&["a", "b"])),
                p(rbd(&["c", "d"])),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-new-object-iscsidisk-conflict",
                p(iscsi("iqn.2016-12.server:storage.target01")),
                p(iscsi("iqn.2016-12.server:storage.target01")),
                QueueingHint::Queue,
            ),
            (
                "skip-new-object-iscsidisk-no-conflict",
                p(iscsi("iqn.2016-12.server:storage.target01")),
                p(iscsi("iqn.2017-12.server:storage.target01")),
                QueueingHint::QueueSkip,
            ),
            (
                "queue-has-same-claim",
                pvc_pod("pod1", "", &["claim-rwop"]),
                pvc_pod("pod2", "", &["claim-rwop"]),
                QueueingHint::Queue,
            ),
            (
                "skip-no-same-claim",
                pvc_pod("pod1", "", &["claim-1-rwop"]),
                pvc_pod("pod2", "", &["claim-2-rwop"]),
                QueueingHint::QueueSkip,
            ),
            (
                "skip-other-namespace",
                pvc_pod("pod1", "a", &["c"]),
                pvc_pod("pod2", "b", &["c"]),
                QueueingHint::QueueSkip,
            ),
        ];
        for (name, target, deleted, want) in cases {
            let old = obj(deleted);
            assert_eq!(f(&target, Some(&*old), None).unwrap(), want, "{name}");
        }
        // gce read-only on both sides doesn't conflict (isVolumeConflict).
        let ro = vol(json!({"gcePersistentDisk": {"pdName": "foo", "readOnly": true}}));
        let old = obj(p(ro.clone()));
        assert_eq!(
            f(&p(ro), Some(&*old), None).unwrap(),
            QueueingHint::QueueSkip
        );
        // wrong object type
        assert!(f(&p(gce("foo")), Some(&"not-a-pod"), None).is_err());
    }

    /// `Test_isSchedulableAfterPersistentVolumeClaimChange`
    /// (`volume_restrictions_test.go:693-`).
    #[test]
    fn volume_restrictions_after_pvc_added() {
        use volume_restrictions::is_schedulable_after_pvc_added as f;
        let one = pvc_pod("pod-with-one-pvc", "default", &["claim-with-rwx-1"]);
        let two = pvc_pod(
            "pod-with-two-pvcs",
            "default",
            &["claim-with-rwx-1", "claim-with-rwx-2"],
        );
        let other_ns = pvc_pod("pod-with-one-pvc", "kube-system", &["claim-with-rwx-1"]);
        let pvc1 = obj(pvc("default", "claim-with-rwx-1", ""));
        let pvc2 = obj(pvc("default", "claim-with-rwx-2", ""));
        assert_eq!(f(&two, None, Some(&*pvc1)).unwrap(), QueueingHint::Queue);
        assert_eq!(
            f(&one, None, Some(&*pvc2)).unwrap(),
            QueueingHint::QueueSkip
        );
        assert_eq!(
            f(&other_ns, None, Some(&*pvc1)).unwrap(),
            QueueingHint::QueueSkip
        );
        assert!(f(&one, None, Some(&"not-a-pvc")).is_err());
    }

    /// `TestIsSchedulableAfterPersistentVolumeClaimAdded` (`volume_zone_test.go:549-`).
    #[test]
    fn volume_zone_after_pvc_change() {
        use volume_zone::is_schedulable_after_pvc_change as f;
        let pod = pvc_pod("pod_1", "default", &["PVC_1"]);
        assert!(f(&pod, None, Some(&"not-a-pvc")).is_err());
        let no_pvc = pod_with_volumes("pod_1", "default", json!([]));
        let n = obj(pvc("default", "PVC_1", "Vol_1"));
        assert_eq!(
            f(&no_pvc, None, Some(&*n)).unwrap(),
            QueueingHint::QueueSkip
        );
        let other = pvc_pod("pod_1", "default", &["PVC_2"]);
        assert_eq!(f(&other, None, Some(&*n)).unwrap(), QueueingHint::QueueSkip);
        let ns1 = obj(pvc("ns1", "PVC_1", "Vol_1"));
        assert_eq!(f(&pod, None, Some(&*ns1)).unwrap(), QueueingHint::QueueSkip);
        assert_eq!(f(&pod, None, Some(&*n)).unwrap(), QueueingHint::Queue);
        let o = obj(pvc("default", "PVC_1", ""));
        assert_eq!(f(&pod, Some(&*o), Some(&*n)).unwrap(), QueueingHint::Queue);
        assert_eq!(
            f(&no_pvc, Some(&*o), Some(&*n)).unwrap(),
            QueueingHint::QueueSkip
        );
    }

    /// `TestIsSchedulableAfterStorageClassAdded` (`volume_zone_test.go:636-`).
    #[test]
    fn volume_zone_after_storage_class_added() {
        use volume_zone::is_schedulable_after_storage_class_added as f;
        let pod = pvc_pod("pod_1", "default", &["PVC_1"]);
        assert!(f(&pod, None, Some(&"not-a-storageclass")).is_err());
        let none: StorageClass =
            serde_json::from_value(json!({"metadata": {"name": "SC_1"}})).unwrap();
        assert_eq!(
            f(&pod, None, Some(&*obj(none))).unwrap(),
            QueueingHint::QueueSkip
        );
        let wait: StorageClass = serde_json::from_value(
            json!({"metadata": {"name": "SC_1"}, "volumeBindingMode": "WaitForFirstConsumer"}),
        )
        .unwrap();
        assert_eq!(
            f(&pod, None, Some(&*obj(wait))).unwrap(),
            QueueingHint::Queue
        );
        let imm: StorageClass = serde_json::from_value(
            json!({"metadata": {"name": "SC_1"}, "volumeBindingMode": "Immediate"}),
        )
        .unwrap();
        assert_eq!(
            f(&pod, None, Some(&*obj(imm))).unwrap(),
            QueueingHint::QueueSkip
        );
    }

    /// `TestIsSchedulableAfterPersistentVolumeChange` (`volume_zone_test.go:684-`).
    #[test]
    fn volume_zone_after_pv_change() {
        use volume_zone::is_schedulable_after_pv_change as f;
        let pod = pvc_pod("pod_1", "default", &["PVC_1"]);
        assert!(f(&pod, None, Some(&"not-a-pv")).is_err());
        let vol1 = obj(pv("Vol_1", json!({})));
        assert!(f(&pod, Some(&"not-a-pv"), Some(&*vol1)).is_err());
        let added = obj(pv("Vol_1", json!({BETA_ZONE: "us-west1-b"})));
        assert_eq!(f(&pod, None, Some(&*added)).unwrap(), QueueingHint::Queue);
        let a = obj(pv("Vol_1", json!({BETA_ZONE: "us-west1-a"})));
        assert_eq!(
            f(&pod, Some(&*a), Some(&*added)).unwrap(),
            QueueingHint::Queue
        );
        let both = obj(pv(
            "Vol_1",
            json!({BETA_ZONE: "us-west1-a", TOPO_ZONE: "zone"}),
        ));
        assert_eq!(
            f(&pod, Some(&*a), Some(&*both)).unwrap(),
            QueueingHint::Queue
        );
        let both2 = obj(pv(
            "Vol_1",
            json!({BETA_ZONE: "us-west1-a", TOPO_ZONE: "zone"}),
        ));
        assert_eq!(
            f(&pod, Some(&*both), Some(&*both2)).unwrap(),
            QueueingHint::QueueSkip
        );
        // A multi-zone value in a different order is the same set.
        let m1 = obj(pv("Vol_1", json!({BETA_ZONE: "a__b"})));
        let m2 = obj(pv("Vol_1", json!({BETA_ZONE: "b__a"})));
        assert_eq!(
            f(&pod, Some(&*m1), Some(&*m2)).unwrap(),
            QueueingHint::QueueSkip
        );
        // An unparsable label is ignored (getPVTopologies).
        let bad = obj(pv("Vol_1", json!({BETA_ZONE: "a____b"})));
        assert_eq!(
            f(&pod, Some(&*bad), Some(&*vol1)).unwrap(),
            QueueingHint::QueueSkip
        );
    }
}
