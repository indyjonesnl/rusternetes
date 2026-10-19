//! Queueing hints of the VolumeRestrictions and VolumeZone plugins.
//!
//! Ported from upstream (release-1.35):
//! - `pkg/scheduler/framework/plugins/volumerestrictions/volume_restrictions.go`:
//!   `isSchedulableAfterPodDeleted` (`:385-420`),
//!   `isSchedulableAfterPersistentVolumeClaimAdded` (`:359-381`),
//!   `satisfyVolumeConflicts` (`:276-289`), `isVolumeConflict` (`:111-151`),
//!   `haveOverlap` (`:154-166`), `needsRestrictionsCheck` (`:169-171`).
//! - `pkg/scheduler/framework/plugins/volumezone/volume_zone.go`:
//!   `isSchedulableAfterPersistentVolumeClaimChange`, `isPVCRequestedFromPod`,
//!   `isSchedulableAfterStorageClassAdded`,
//!   `isSchedulableAfterPersistentVolumeChange`, `getPVTopologies`,
//!   `topologyLabels` (`:84-89`).
//! - `staging/src/k8s.io/cloud-provider/volume/helpers/zones.go`:
//!   `LabelZonesToSet` / `stringToSet`.
//!
//! Tests port the upstream `Test_isSchedulableAfterPodDeleted`,
//! `Test_isSchedulableAfterPersistentVolumeClaimChange`,
//! `TestIsSchedulableAfterPersistentVolumeClaimAdded`,
//! `TestIsSchedulableAfterStorageClassAdded` and
//! `TestIsSchedulableAfterPersistentVolumeChange` tables.

use super::events::{as_typed, EventObject, QueueingHint};
use rusternetes_common::resources::volume::{
    PersistentVolume, PersistentVolumeClaim, StorageClass, VolumeBindingMode,
};
use rusternetes_common::resources::{Pod, Volume};
use std::collections::BTreeSet;
use tracing::debug;

fn pod_name(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

fn pod_volumes(pod: &Pod) -> &[Volume] {
    pod.spec
        .as_ref()
        .and_then(|s| s.volumes.as_deref())
        .unwrap_or(&[])
}

/// `volumerestrictions` queueing hints.
pub mod volume_restrictions {
    use super::*;
    use serde_json::Value;

    /// `needsRestrictionsCheck` (`volume_restrictions.go:169-171`).
    fn needs_restrictions_check(v: &Volume) -> bool {
        v.legacy_sources.gce_persistent_disk.is_some()
            || v.legacy_sources.aws_elastic_block_store.is_some()
            || v.legacy_sources.rbd.is_some()
            || v.iscsi.is_some()
    }

    /// `haveOverlap` (`volume_restrictions.go:154-166`).
    fn have_overlap(a1: &[String], a2: &[String]) -> bool {
        a1.iter().any(|a| a2.contains(a))
    }

    fn rbd_monitors(v: &Value) -> Vec<String> {
        v.get("monitors")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|m| m.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn rbd_str<'a>(v: &'a Value, key: &str) -> &'a str {
        v.get(key).and_then(Value::as_str).unwrap_or("")
    }

    fn rbd_read_only(v: &Value) -> bool {
        v.get("readOnly").and_then(Value::as_bool).unwrap_or(false)
    }

    /// `isVolumeConflict` (`volume_restrictions.go:111-151`).
    fn is_volume_conflict(volume: &Volume, pod: &Pod) -> bool {
        for existing in pod_volumes(pod) {
            // Same GCE disk mounted by multiple pods conflicts unless all pods mount it read-only.
            if let (Some(disk), Some(edisk)) = (
                &volume.legacy_sources.gce_persistent_disk,
                &existing.legacy_sources.gce_persistent_disk,
            ) {
                if disk.pd_name == edisk.pd_name
                    && !(disk.read_only.unwrap_or(false) && edisk.read_only.unwrap_or(false))
                {
                    return true;
                }
            }

            if let (Some(ebs), Some(eebs)) = (
                &volume.legacy_sources.aws_elastic_block_store,
                &existing.legacy_sources.aws_elastic_block_store,
            ) {
                if ebs.volume_id == eebs.volume_id {
                    return true;
                }
            }

            if let (Some(iscsi), Some(eiscsi)) = (&volume.iscsi, &existing.iscsi) {
                // two ISCSI volumes are same, if they share the same iqn.
                if iscsi.iqn == eiscsi.iqn
                    && !(iscsi.read_only.unwrap_or(false) && eiscsi.read_only.unwrap_or(false))
                {
                    return true;
                }
            }

            if let (Some(rbd), Some(erbd)) =
                (&volume.legacy_sources.rbd, &existing.legacy_sources.rbd)
            {
                // two RBDs images are the same if they share the same Ceph
                // monitor, are in the same RADOS Pool, and have the same image name.
                if have_overlap(&rbd_monitors(rbd), &rbd_monitors(erbd))
                    && rbd_str(rbd, "pool") == rbd_str(erbd, "pool")
                    && rbd_str(rbd, "image") == rbd_str(erbd, "image")
                    && !(rbd_read_only(rbd) && rbd_read_only(erbd))
                {
                    return true;
                }
            }
        }
        false
    }

    /// `satisfyVolumeConflicts` (`:276-289`) against a `NodeInfo` holding only
    /// `existing` (upstream: `framework.NewNodeInfo(deletedPod)`).
    fn satisfy_volume_conflicts(pod: &Pod, existing: &Pod) -> bool {
        for v in pod_volumes(pod) {
            if !needs_restrictions_check(v) {
                continue;
            }
            if is_volume_conflict(v, existing) {
                return false;
            }
        }
        true
    }

    /// `isSchedulableAfterPersistentVolumeClaimAdded`
    /// (`volume_restrictions.go:359-381`).
    pub fn is_schedulable_after_pvc_added(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (_, new_pvc) = as_typed::<PersistentVolumeClaim>(old_obj, new_obj).map_err(|e| {
            format!("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: {e}")
        })?;
        let new_pvc = new_pvc.ok_or_else(|| {
            "unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: nil object"
                .to_string()
        })?;

        if new_pvc.metadata.namespace != pod.metadata.namespace {
            return Ok(QueueingHint::QueueSkip);
        }
        for v in pod_volumes(pod) {
            let Some(pvc) = &v.persistent_volume_claim else {
                continue;
            };
            if pvc.claim_name == new_pvc.metadata.name {
                debug!(pod = %pod_name(pod), pvc = %new_pvc.metadata.name, "PVC that is referred from the pod was created, which might make this pod schedulable");
                return Ok(QueueingHint::Queue);
            }
        }
        debug!(pod = %pod_name(pod), pvc = %new_pvc.metadata.name, "PVC irrelevant to the Pod was created, which doesn't make this pod schedulable");
        Ok(QueueingHint::QueueSkip)
    }

    /// `isSchedulableAfterPodDeleted` (`volume_restrictions.go:385-420`).
    pub fn is_schedulable_after_pod_deleted(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (deleted_pod, _) = as_typed::<Pod>(old_obj, new_obj)
            .map_err(|e| format!("unexpected objects in isSchedulableAfterPodDeleted: {e}"))?;
        let deleted_pod = deleted_pod
            .ok_or_else(|| "unexpected objects in isSchedulableAfterPodDeleted: nil".to_string())?;

        if deleted_pod.metadata.namespace != pod.metadata.namespace {
            return Ok(QueueingHint::QueueSkip);
        }

        if !satisfy_volume_conflicts(pod, deleted_pod) {
            debug!(pod = %pod_name(pod), deleted_pod = %pod_name(deleted_pod), "Pod with the volume that the target pod requires was deleted, which might make this pod schedulable");
            return Ok(QueueingHint::Queue);
        }

        // Return Queue if a deleted pod uses the same PVC since the pod may be
        // unschedulable due to the ReadWriteOncePod access mode of the PVC.
        // (Upstream doesn't fetch the PVC either: kubernetes#124566.)
        let claims: BTreeSet<&str> = pod_volumes(pod)
            .iter()
            .filter_map(|v| v.persistent_volume_claim.as_ref())
            .map(|c| c.claim_name.as_str())
            .collect();
        for v in pod_volumes(deleted_pod) {
            if let Some(c) = &v.persistent_volume_claim {
                if claims.contains(c.claim_name.as_str()) {
                    debug!(pod = %pod_name(pod), deleted_pod = %pod_name(deleted_pod), "Pod with the same PVC that the target pod requires was deleted, which might make this pod schedulable");
                    return Ok(QueueingHint::Queue);
                }
            }
        }

        debug!(pod = %pod_name(pod), deleted_pod = %pod_name(deleted_pod), "An irrelevant Pod was deleted, which doesn't make this pod schedulable");
        Ok(QueueingHint::QueueSkip)
    }
}

/// `volumezone` queueing hints.
pub mod volume_zone {
    use super::*;

    /// `topologyLabels` (`volume_zone.go:84-89`).
    const TOPOLOGY_LABELS: [&str; 4] = [
        "failure-domain.beta.kubernetes.io/zone",
        "failure-domain.beta.kubernetes.io/region",
        "topology.kubernetes.io/zone",
        "topology.kubernetes.io/region",
    ];

    /// `LabelMultiZoneDelimiter` (`cloud-provider/volume/constants.go:25`).
    const LABEL_MULTI_ZONE_DELIMITER: &str = "__";

    #[derive(Debug, PartialEq, Eq)]
    struct PvTopology {
        pv_name: String,
        key: &'static str,
        values: BTreeSet<String>,
    }

    /// `LabelZonesToSet` / `stringToSet` (`zones.go:33-35`, `:52-67`): an empty
    /// element is an error.
    fn label_zones_to_set(value: &str) -> Result<BTreeSet<String>, String> {
        let mut set = BTreeSet::new();
        for zone in value.split(LABEL_MULTI_ZONE_DELIMITER) {
            let zone = zone.trim();
            if zone.is_empty() {
                return Err(format!(
                    "\"{LABEL_MULTI_ZONE_DELIMITER}\" separated list (\"{value}\") must not contain an empty string"
                ));
            }
            set.insert(zone.to_string());
        }
        Ok(set)
    }

    /// `getPVTopologies`. Doesn't check `spec.nodeAffinity` (immutable;
    /// handled by NodeAffinity).
    fn get_pv_topologies(pv: &PersistentVolume) -> Vec<PvTopology> {
        let mut out = Vec::new();
        for key in TOPOLOGY_LABELS {
            if let Some(value) = pv.metadata.labels.as_ref().and_then(|l| l.get(key)) {
                match label_zones_to_set(value) {
                    Ok(values) => out.push(PvTopology {
                        pv_name: pv.metadata.name.clone(),
                        key,
                        values,
                    }),
                    Err(err) => {
                        debug!(label = %format!("{key}:{value}"), %err, "failed to parse PV's topology label, ignoring the label");
                    }
                }
            }
        }
        out
    }

    /// `isPVCRequestedFromPod`.
    fn is_pvc_requested_from_pod(pvc: Option<&PersistentVolumeClaim>, pod: &Pod) -> bool {
        let Some(pvc) = pvc else {
            return false;
        };
        if pod.metadata.namespace != pvc.metadata.namespace {
            return false;
        }
        pod_volumes(pod)
            .iter()
            .filter_map(|v| v.persistent_volume_claim.as_ref())
            .any(|c| c.claim_name == pvc.metadata.name)
    }

    /// `isSchedulableAfterPersistentVolumeClaimChange`.
    pub fn is_schedulable_after_pvc_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (_, modified) = as_typed::<PersistentVolumeClaim>(old_obj, new_obj).map_err(|e| {
            format!("unexpected objects in isSchedulableAfterPersistentVolumeClaimChange: {e}")
        })?;
        if is_pvc_requested_from_pod(modified, pod) {
            debug!(pod = %pod_name(pod), "PVC that is referred from the pod was created or updated, which might make this pod schedulable");
            return Ok(QueueingHint::Queue);
        }
        debug!(pod = %pod_name(pod), "PVC irrelevant to the Pod was created or updated, which doesn't make this pod schedulable");
        Ok(QueueingHint::QueueSkip)
    }

    /// `isSchedulableAfterStorageClassAdded`: only a new StorageClass with
    /// WaitForFirstConsumer can make a pod schedulable. Upstream passes `nil`
    /// as the old object to `util.As`.
    pub fn is_schedulable_after_storage_class_added(
        pod: &Pod,
        _old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (_, added) = as_typed::<StorageClass>(None, new_obj).map_err(|e| {
            format!("unexpected objects in isSchedulableAfterStorageClassAdded: {e}")
        })?;
        let added = added.ok_or_else(|| {
            "unexpected objects in isSchedulableAfterStorageClassAdded: nil".to_string()
        })?;
        if !matches!(
            added.volume_binding_mode,
            Some(VolumeBindingMode::WaitForFirstConsumer)
        ) {
            debug!(pod = %pod_name(pod), storage_class = %added.metadata.name, "StorageClass is created, but its VolumeBindingMode is not waitForFirstConsumer, which doesn't make the pod schedulable");
            return Ok(QueueingHint::QueueSkip);
        }
        debug!(pod = %pod_name(pod), storage_class = %added.metadata.name, "StorageClass with waitForFirstConsumer mode was created and it might make this pod schedulable");
        Ok(QueueingHint::Queue)
    }

    /// `isSchedulableAfterPersistentVolumeChange`: a changed PV topology label
    /// may make a pod schedulable.
    pub fn is_schedulable_after_pv_change(
        pod: &Pod,
        old_obj: Option<&EventObject>,
        new_obj: Option<&EventObject>,
    ) -> Result<QueueingHint, String> {
        let (original, modified) = as_typed::<PersistentVolume>(old_obj, new_obj).map_err(|e| {
            format!("unexpected objects in isSchedulableAfterPersistentVolumeChange: {e}")
        })?;
        let modified = modified.ok_or_else(|| {
            "unexpected objects in isSchedulableAfterPersistentVolumeChange: nil".to_string()
        })?;
        let Some(original) = original else {
            debug!("PV is newly created, which might make the pod schedulable");
            return Ok(QueueingHint::Queue);
        };
        if get_pv_topologies(original) != get_pv_topologies(modified) {
            debug!(pod = %pod_name(pod), pv = %modified.metadata.name, "PV's topology was updated, which might make the pod schedulable.");
            return Ok(QueueingHint::Queue);
        }
        debug!(pod = %pod_name(pod), pv = %modified.metadata.name, "PV was updated, but the topology is unchanged, which it doesn't make the pod schedulable");
        Ok(QueueingHint::QueueSkip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
