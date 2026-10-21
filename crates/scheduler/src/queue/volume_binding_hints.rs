//! VolumeBinding queueing hints.
//!
//! Ported from upstream (release-1.35)
//! `pkg/scheduler/framework/plugins/volumebinding/volume_binding.go`:
//! `EventsToRegister` (`:105-141`, wired in `hints.rs`),
//! `isSchedulableAfterCSINodeChange` (`:143-168`),
//! `isSchedulableAfterPersistentVolumeClaimChange` (`:170-210`),
//! `isSchedulableAfterStorageClassChange` (`:212-245`),
//! `isSchedulableAfterCSIStorageCapacityChange` (`:247-293`),
//! `isSchedulableAfterCSIDriverChange` (`:295-322`) and
//! `volumeLimit` (`binder.go:1029-1035`);
//! tests: `volume_binding_test.go` `TestIsSchedulableAfterCSINodeChange`
//! (`:1192`), `TestIsSchedulableAfterPersistentVolumeClaimChange` (`:1298`),
//! `TestIsSchedulableAfterStorageClassChange` (`:1395`),
//! `TestIsSchedulableAfterCSIStorageCapacityChange` (`:1483`),
//! `TestIsSchedulableAfterCSIDriverChange` (`:1712`).
//!
//! The PersistentVolume and Node events carry no hint upstream either.

use super::events::*;
use crate::volume_binding::{pod_volumes, volume_pvc_name};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::{
    CSIDriver, CSINode, CSIStorageCapacity, PersistentVolumeClaim, Pod, StorageClass,
};
use tracing::debug;

/// `v1.MigratedPluginsAnnotationKey`.
const MIGRATED_PLUGINS_ANNOTATION_KEY: &str = "storage.alpha.kubernetes.io/migrated-plugins";

fn pod_key(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `isSchedulableAfterCSINodeChange`.
pub fn is_schedulable_after_csi_node_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    if old_obj.is_none() {
        debug!("CSINode creation could make the pod schedulable");
        return Ok(QueueingHint::Queue);
    }
    let (old, modified) = as_typed::<CSINode>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterCSINodeChange: {e}"))?;
    let (Some(old), Some(modified)) = (old, modified) else {
        return Err("unexpected objects in isSchedulableAfterCSINodeChange: nil".to_string());
    };
    let annotation = |n: &CSINode| {
        n.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(MIGRATED_PLUGINS_ANNOTATION_KEY))
            .cloned()
            .unwrap_or_default()
    };
    if annotation(old) != annotation(modified) {
        debug!(pod = %pod_key(pod), csi_node = %modified.metadata.name, "CSINode's migrated plugins annotation is updated and that may make the pod schedulable");
        return Ok(QueueingHint::Queue);
    }
    debug!(pod = %pod_key(pod), csi_node = %modified.metadata.name, "CSINode was created or updated but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterPersistentVolumeClaimChange`.
pub fn is_schedulable_after_pvc_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (_, new_pvc) = as_typed::<PersistentVolumeClaim>(old_obj, new_obj).map_err(|e| {
        format!("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: {e}")
    })?;
    let new_pvc = new_pvc.ok_or_else(|| {
        "unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: nil".to_string()
    })?;
    if pod.metadata.namespace != new_pvc.metadata.namespace {
        debug!(pod = %pod_key(pod), pvc = %new_pvc.metadata.name, "PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable because the PVC belongs to a different namespace");
        return Ok(QueueingHint::QueueSkip);
    }
    for vol in pod_volumes(pod) {
        let Some((pvc_name, _)) = volume_pvc_name(pod, vol) else {
            continue;
        };
        if pvc_name == new_pvc.metadata.name {
            // Return Queue because, in this case, all PVC creations and
            // almost all PVC updates could make the Pod schedulable.
            debug!(pod = %pod_key(pod), pvc = %new_pvc.metadata.name, "PersistentVolumeClaim the pod requires was created or updated, potentially making the target Pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), pvc = %new_pvc.metadata.name, "PersistentVolumeClaim was created or updated, but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `apiequality.Semantic.DeepEqual` on `AllowedTopologies`: nil and empty
/// slices are equal.
fn allowed_topologies_equal(a: &StorageClass, b: &StorageClass) -> bool {
    type Terms = Vec<Vec<(String, Vec<String>)>>;
    fn norm(sc: &StorageClass) -> Terms {
        sc.allowed_topologies
            .iter()
            .flatten()
            .map(|t| {
                t.match_label_expressions
                    .iter()
                    .flatten()
                    .map(|r| (r.key.clone(), r.values.clone()))
                    .collect()
            })
            .collect()
    }
    norm(a) == norm(b)
}

/// `isSchedulableAfterStorageClassChange`: any StorageClass addition and a
/// StorageClass update to allowedTopologies might make a Pod schedulable.
pub fn is_schedulable_after_storage_class_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (old, new) = as_typed::<StorageClass>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterStorageClassChange: {e}"))?;
    let new = new.ok_or("unexpected objects in isSchedulableAfterStorageClassChange: nil")?;
    let Some(old) = old else {
        // No further filtering can be made for a creation event.
        debug!(pod = %pod_key(pod), storage_class = %new.metadata.name, "A new StorageClass was created, which could make a Pod schedulable");
        return Ok(QueueingHint::Queue);
    };
    if !allowed_topologies_equal(old, new) {
        debug!(pod = %pod_key(pod), storage_class = %new.metadata.name, "StorageClass got an update in AllowedTopologies");
        return Ok(QueueingHint::Queue);
    }
    debug!(pod = %pod_key(pod), storage_class = %new.metadata.name, "StorageClass was updated, but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `volumeLimit` (`binder.go:1029-1035`): prefer `maximumVolumeSize`, it is
/// more precise.
fn volume_limit(capacity: &CSIStorageCapacity) -> Option<Quantity> {
    capacity
        .maximum_volume_size
        .as_deref()
        .or(capacity.capacity.as_deref())
        .and_then(|q| Quantity::parse(q).ok())
}

/// `isSchedulableAfterCSIStorageCapacityChange`: any CSIStorageCapacity
/// addition and an update that raises the volume limit might make a Pod
/// schedulable.
pub fn is_schedulable_after_csi_storage_capacity_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (old, new) = as_typed::<CSIStorageCapacity>(old_obj, new_obj).map_err(|e| {
        format!("unexpected objects in isSchedulableAfterCSIStorageCapacityChange: {e}")
    })?;
    let new = new.ok_or("unexpected objects in isSchedulableAfterCSIStorageCapacityChange: nil")?;
    let Some(old) = old else {
        debug!(pod = %pod_key(pod), capacity = %new.metadata.name, "A new CSIStorageCapacity was created, which could make a Pod schedulable");
        return Ok(QueueingHint::Queue);
    };
    let old_limit = volume_limit(old);
    let new_limit = volume_limit(new);
    if let Some(new_limit) = new_limit {
        if old_limit.is_none_or(|o| new_limit.value() > o.value()) {
            debug!(pod = %pod_key(pod), capacity = %new.metadata.name, "VolumeLimit was increased, which could make a Pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), capacity = %new.metadata.name, "CSIStorageCapacity was updated, but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

/// `isSchedulableAfterCSIDriverChange`: disabling storage capacity tracking
/// on a driver the pod uses may make the pod schedulable.
pub fn is_schedulable_after_csi_driver_change(
    pod: &Pod,
    old_obj: Option<&EventObject>,
    new_obj: Option<&EventObject>,
) -> Result<QueueingHint, String> {
    let (original, modified) = as_typed::<CSIDriver>(old_obj, new_obj)
        .map_err(|e| format!("unexpected objects in isSchedulableAfterCSIDriverChange: {e}"))?;
    let (Some(original), Some(modified)) = (original, modified) else {
        return Err("unexpected objects in isSchedulableAfterCSIDriverChange: nil".to_string());
    };
    for vol in pod_volumes(pod) {
        let Some(csi) = vol.csi.as_ref() else {
            continue;
        };
        if csi.driver != modified.metadata.name {
            continue;
        }
        if original.spec.storage_capacity == Some(true)
            && modified.spec.storage_capacity != Some(true)
        {
            debug!(pod = %pod_key(pod), driver = %modified.metadata.name, "CSIDriver was updated and storage capacity got disabled, which may make the pod schedulable");
            return Ok(QueueingHint::Queue);
        }
    }
    debug!(pod = %pod_key(pod), driver = %modified.metadata.name, "CSIDriver was created or updated but it doesn't make this pod schedulable");
    Ok(QueueingHint::QueueSkip)
}

#[cfg(test)]
mod tests {
    use super::*;
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
