//! VolumeBinding assume cache + `AssumePodVolumes` / `RevertAssumedPodVolumes`
//! (the state behind the plugin's Reserve / Unreserve).
//!
//! Ported from upstream Kubernetes release-1.35:
//! - `pkg/scheduler/util/assumecache/assume_cache.go` (`AssumeCache`)
//! - `pkg/scheduler/framework/plugins/volumebinding/assume_cache.go` (PV/PVC wrappers)
//! - `pkg/scheduler/framework/plugins/volumebinding/binder.go:401-477`
//!   (`AssumePodVolumes`, `RevertAssumedPodVolumes`, `revertAssumedPVs/PVCs`)
//! - `staging/src/k8s.io/component-helpers/storage/volume/pv_helpers.go:117-154`
//!   (`GetBindVolumeToClaim`)
//! - `volume_binding.go:531-553,603-615` (Reserve / Unreserve call these).
//!
//! Event handlers (`AddEventHandler`/`emitEvents`) are not ported: upstream
//! uses them only to requeue unschedulable pods, and this scheduler resyncs.

use crate::volume_binding::{
    ephemeral_volume_is_for_pod, is_pvc_fully_bound, is_volume_bound_to_claim, pod_volumes,
    volume_pvc_name, BindingInfo, DynamicProvision, PodVolumes, VolumeSnapshot, ANN_SELECTED_NODE,
};
use rusternetes_common::resources::service_account::ObjectReference;
use rusternetes_common::resources::{PersistentVolume, PersistentVolumeClaim, Pod, Volume};
use std::collections::HashMap;

/// `volume.AnnBoundByController` (pv_helpers.go).
pub const ANN_BOUND_BY_CONTROLLER: &str = "pv.kubernetes.io/bound-by-controller";

/// Errors of `assumecache.AssumeCache` (`NotFoundError`, version errors).
#[derive(Debug, Clone, PartialEq)]
pub enum AssumeError {
    NotFound {
        type_name: &'static str,
        key: String,
    },
    Other(String),
}

impl std::fmt::Display for AssumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssumeError::NotFound { type_name, key } => {
                write!(f, "{type_name} {key:?} does not exist")
            }
            AssumeError::Other(m) => f.write_str(m),
        }
    }
}

/// An object the assume cache can hold (`meta.Accessor` + `MetaNamespaceKeyFunc`).
pub trait Assumable: Clone {
    const DESCRIPTION: &'static str;
    fn cache_key(&self) -> String;
    fn resource_version(&self) -> &str;
}

impl Assumable for PersistentVolume {
    const DESCRIPTION: &'static str = "v1.PersistentVolume";
    fn cache_key(&self) -> String {
        self.metadata.name.clone()
    }
    fn resource_version(&self) -> &str {
        self.metadata.resource_version.as_deref().unwrap_or("")
    }
}

impl Assumable for PersistentVolumeClaim {
    const DESCRIPTION: &'static str = "v1.PersistentVolumeClaim";
    fn cache_key(&self) -> String {
        match self.metadata.namespace.as_deref() {
            Some(ns) if !ns.is_empty() => format!("{ns}/{}", self.metadata.name),
            _ => self.metadata.name.clone(),
        }
    }
    fn resource_version(&self) -> &str {
        self.metadata.resource_version.as_deref().unwrap_or("")
    }
}

struct ObjInfo<T> {
    /// Latest version of object could be cached-only or from informer.
    latest: T,
    /// Latest object from informer.
    api: T,
}

/// `assumecache.AssumeCache`.
pub struct AssumeCache<T: Assumable> {
    objs: HashMap<String, ObjInfo<T>>,
}

impl<T: Assumable> Default for AssumeCache<T> {
    fn default() -> Self {
        Self {
            objs: HashMap::new(),
        }
    }
}

impl<T: Assumable> AssumeCache<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// `getObjVersion`: `strconv.ParseInt(resourceVersion, 10, 64)`.
    fn obj_version(key: &str, obj: &T) -> Result<i64, AssumeError> {
        obj.resource_version().parse::<i64>().map_err(|_| {
            AssumeError::Other(format!(
                "error parsing ResourceVersion {:?} for {} {:?}",
                obj.resource_version(),
                T::DESCRIPTION,
                key
            ))
        })
    }

    /// `AssumeCache.add` (informer add/update), assume_cache.go:226-271.
    pub fn add(&mut self, obj: &T) {
        let key = obj.cache_key();
        if let Some(info) = self.objs.get(&key) {
            let (Ok(new_version), Ok(stored_version)) = (
                Self::obj_version(&key, obj),
                Self::obj_version(&key, &info.latest),
            ) else {
                return;
            };
            // Only update object if version is newer. This is so we don't
            // override assumed objects due to informer resync.
            if new_version <= stored_version {
                return;
            }
        }
        self.objs.insert(
            key,
            ObjInfo {
                latest: obj.clone(),
                api: obj.clone(),
            },
        );
    }

    /// `AssumeCache.delete`, assume_cache.go:277-313.
    pub fn delete(&mut self, key: &str) {
        self.objs.remove(key);
    }

    /// Feed a fresh lister listing (what the informer would deliver as
    /// add/update/delete events): add/update every object, delete the rest.
    pub fn sync(&mut self, objs: &[T]) {
        let present: std::collections::HashSet<String> =
            objs.iter().map(|o| o.cache_key()).collect();
        self.objs.retain(|k, _| present.contains(k));
        for o in objs {
            self.add(o);
        }
    }

    fn obj_info(&self, key: &str) -> Result<&ObjInfo<T>, AssumeError> {
        self.objs.get(key).ok_or_else(|| AssumeError::NotFound {
            type_name: T::DESCRIPTION,
            key: key.to_string(),
        })
    }

    /// `AssumeCache.Get`: the latest (possibly assumed) object.
    pub fn get(&self, key: &str) -> Result<&T, AssumeError> {
        Ok(&self.obj_info(key)?.latest)
    }

    /// `AssumeCache.GetAPIObj`: the informer's version.
    pub fn get_api_obj(&self, key: &str) -> Result<&T, AssumeError> {
        Ok(&self.obj_info(key)?.api)
    }

    /// `AssumeCache.List` without an index: latest objects.
    pub fn list(&self) -> Vec<&T> {
        self.objs.values().map(|i| &i.latest).collect()
    }

    /// `AssumeCache.Assume`, assume_cache.go:434-470: the version of the
    /// object must be greater or equal to the current object.
    pub fn assume(&mut self, obj: &T) -> Result<(), AssumeError> {
        let key = obj.cache_key();
        let info = self.obj_info(&key)?;
        let new_version = Self::obj_version(&key, obj)?;
        let stored_version = Self::obj_version(&key, &info.latest)?;
        if new_version < stored_version {
            return Err(AssumeError::Other(format!(
                "{} {:?} is out of sync (stored: {}, assume: {})",
                T::DESCRIPTION,
                key,
                stored_version,
                new_version
            )));
        }
        // Only update the cached object.
        if let Some(info) = self.objs.get_mut(&key) {
            info.latest = obj.clone();
        }
        Ok(())
    }

    /// `AssumeCache.Restore`, assume_cache.go:472-486. A missing object is
    /// expected (it may have been deleted) and is a no-op.
    pub fn restore(&mut self, key: &str) {
        if let Some(info) = self.objs.get_mut(key) {
            info.latest = info.api.clone();
        }
    }
}

/// `volume.GetBindVolumeToClaim` (pv_helpers.go:120-154).
pub fn get_bind_volume_to_claim(
    pv: &PersistentVolume,
    pvc: &PersistentVolumeClaim,
) -> Result<(PersistentVolume, bool), String> {
    let mut dirty = false;

    // Check if the volume was already bound (either by user or by controller).
    let should_set_bound_by_controller = !is_volume_bound_to_claim(pv, pvc);

    // The volume from method args can be pointing to watcher cache. We must
    // not modify these, therefore create a copy.
    let mut clone = pv.clone();

    // Bind the volume to the claim if it is not bound yet.
    let already = pv.spec.claim_ref.as_ref().is_some_and(|cr| {
        cr.name.as_deref() == Some(pvc.metadata.name.as_str())
            && cr.namespace.as_deref() == pvc.metadata.namespace.as_deref()
            && cr.uid.as_deref().unwrap_or("") == pvc.metadata.uid
    });
    if !already {
        // `reference.GetReference(scheme.Scheme, claim)`.
        clone.spec.claim_ref = Some(ObjectReference {
            kind: Some("PersistentVolumeClaim".to_string()),
            namespace: pvc.metadata.namespace.clone(),
            name: Some(pvc.metadata.name.clone()),
            uid: Some(pvc.metadata.uid.clone()),
            api_version: Some("v1".to_string()),
            resource_version: pvc.metadata.resource_version.clone(),
            field_path: None,
        });
        dirty = true;
    }

    // Set AnnBoundByController if it is not set yet.
    let has_ann = clone
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(ANN_BOUND_BY_CONTROLLER));
    if should_set_bound_by_controller && !has_ann {
        clone
            .metadata
            .annotations
            .get_or_insert_with(HashMap::new)
            .insert(ANN_BOUND_BY_CONTROLLER.to_string(), "yes".to_string());
        dirty = true;
    }

    Ok((clone, dirty))
}

/// The `pvCache` / `pvcCache` pair of `volumeBinder`.
#[derive(Default)]
pub struct VolumeBinder {
    pub pv_cache: AssumeCache<PersistentVolume>,
    pub pvc_cache: AssumeCache<PersistentVolumeClaim>,
}

impl VolumeBinder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Refresh both caches from the listers (the informer feed).
    pub fn sync(&mut self, snap: &VolumeSnapshot) {
        self.pv_cache.sync(&snap.pvs);
        self.pvc_cache.sync(&snap.pvcs);
    }

    /// `isVolumeBound` + `isPVCBound` + `isPVCFullyBound` (binder.go:724-778),
    /// reading the PVC through the assume cache. Errors count as "not bound".
    fn is_volume_bound(&self, pod: &Pod, vol: &Volume) -> bool {
        let Some((name, is_ephemeral)) = volume_pvc_name(pod, vol) else {
            return true;
        };
        let ns = pod.metadata.namespace.as_deref().unwrap_or("");
        let key = if ns.is_empty() {
            name
        } else {
            format!("{ns}/{name}")
        };
        let Ok(pvc) = self.pvc_cache.get(&key) else {
            return false;
        };
        if is_ephemeral && ephemeral_volume_is_for_pod(pod, pvc).is_err() {
            return false;
        }
        is_pvc_fully_bound(pvc)
    }

    /// `arePodVolumesBound` (binder.go:780-788).
    pub fn are_pod_volumes_bound(&self, pod: &Pod) -> bool {
        pod_volumes(pod)
            .iter()
            .all(|v| self.is_volume_bound(pod, v))
    }

    /// `AssumePodVolumes` (binder.go:401-460). Returns `allFullyBound`.
    pub fn assume_pod_volumes(
        &mut self,
        pod: &Pod,
        node_name: &str,
        vols: &mut PodVolumes,
    ) -> Result<bool, String> {
        if self.are_pod_volumes_bound(pod) {
            return Ok(true);
        }

        // Assume PV.
        let mut new_bindings: Vec<BindingInfo> = Vec::new();
        for binding in &vols.static_bindings {
            let (new_pv, dirty) = match get_bind_volume_to_claim(&binding.pv, &binding.pvc) {
                Ok(r) => r,
                Err(e) => {
                    self.revert_assumed_pvs(&new_bindings);
                    return Err(e);
                }
            };
            if dirty {
                if let Err(e) = self.pv_cache.assume(&new_pv) {
                    self.revert_assumed_pvs(&new_bindings);
                    return Err(e.to_string());
                }
            }
            new_bindings.push(BindingInfo {
                pv: new_pv,
                pvc: binding.pvc.clone(),
            });
        }

        // Assume PVCs.
        let mut new_provisioned: Vec<DynamicProvision> = Vec::new();
        for dp in &vols.dynamic_provisions {
            // The claims from method args can be pointing to watcher cache.
            // We must not modify these, therefore create a copy.
            let mut claim = dp.pvc.clone();
            claim
                .metadata
                .annotations
                .get_or_insert_with(HashMap::new)
                .insert(ANN_SELECTED_NODE.to_string(), node_name.to_string());
            if let Err(e) = self.pvc_cache.assume(&claim) {
                self.revert_assumed_pvs(&new_bindings);
                self.revert_assumed_pvcs(new_provisioned.iter().map(|d| &d.pvc));
                return Err(e.to_string());
            }
            new_provisioned.push(DynamicProvision {
                pvc: claim,
                node_capacity: None,
            });
        }

        vols.static_bindings = new_bindings;
        vols.dynamic_provisions = new_provisioned;
        Ok(false)
    }

    /// `RevertAssumedPodVolumes` (binder.go:463-467).
    pub fn revert_assumed_pod_volumes(&mut self, vols: &PodVolumes) {
        self.revert_assumed_pvs(&vols.static_bindings);
        self.revert_assumed_pvcs(vols.dynamic_provisions.iter().map(|d| &d.pvc));
    }

    /// `revertAssumedPVs` (binder.go:964-968).
    fn revert_assumed_pvs(&mut self, bindings: &[BindingInfo]) {
        for b in bindings {
            self.pv_cache.restore(&b.pv.cache_key());
        }
    }

    /// `revertAssumedPVCs` (binder.go:970-974).
    fn revert_assumed_pvcs<'a>(&mut self, claims: impl Iterator<Item = &'a PersistentVolumeClaim>) {
        for c in claims {
            self.pvc_cache.restore(&c.cache_key());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // The resource structs do not implement PartialEq; compare wire form.
    macro_rules! assert_same {
        ($a:expr, $b:expr $(, $($m:tt)+)?) => {
            assert_eq!(
                serde_json::to_value(&$a).unwrap(),
                serde_json::to_value(&$b).unwrap()
                $(, $($m)+)?
            )
        };
    }
    macro_rules! assert_differ {
        ($a:expr, $b:expr) => {
            assert_ne!(
                serde_json::to_value(&$a).unwrap(),
                serde_json::to_value(&$b).unwrap()
            )
        };
    }

    fn mk_pv(name: &str, rv: &str, claim_ref: Value) -> PersistentVolume {
        let mut spec = json!({
            "capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
            "storageClassName": "wait", "hostPath": {"path": "/x"}
        });
        if !claim_ref.is_null() {
            spec["claimRef"] = claim_ref;
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name, "resourceVersion": rv}, "spec": spec,
            "status": {"phase": "Available"}
        }))
        .unwrap()
    }

    fn mk_pvc(name: &str, rv: &str, volume_name: &str, ann: Value) -> PersistentVolumeClaim {
        let mut spec = json!({
            "resources": {"requests": {"storage": "1Gi"}}, "storageClassName": "wait"
        });
        if !volume_name.is_empty() {
            spec["volumeName"] = json!(volume_name);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "testns", "uid": format!("{name}-uid"),
                         "resourceVersion": rv, "annotations": ann},
            "spec": spec, "status": {"phase": "Pending"}
        }))
        .unwrap()
    }

    fn pod_with(pvcs: &[&PersistentVolumeClaim]) -> Pod {
        let vols: Vec<Value> = pvcs
            .iter()
            .map(|c| {
                json!({"name": format!("vol-{}", c.metadata.name),
                       "persistentVolumeClaim": {"claimName": c.metadata.name}})
            })
            .collect();
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "testns", "uid": "pod-uid"},
            "spec": {"containers": [{"name": "c", "image": "i"}], "volumes": vols}
        }))
        .unwrap()
    }

    fn bound_pvc(name: &str, pv: &str) -> PersistentVolumeClaim {
        mk_pvc(
            name,
            "5",
            pv,
            json!({"pv.kubernetes.io/bind-completed": "yes"}),
        )
    }

    fn claim_ref_for(pvc: &PersistentVolumeClaim) -> Value {
        json!({"kind": "PersistentVolumeClaim", "apiVersion": "v1",
               "namespace": "testns", "name": pvc.metadata.name, "uid": pvc.metadata.uid})
    }

    fn binder_with(pvcs: &[PersistentVolumeClaim], pvs: &[PersistentVolume]) -> VolumeBinder {
        let mut b = VolumeBinder::new();
        b.sync(&VolumeSnapshot {
            pvcs: pvcs.to_vec(),
            pvs: pvs.to_vec(),
            ..Default::default()
        });
        b
    }

    fn binding(pv: &PersistentVolume, pvc: &PersistentVolumeClaim) -> BindingInfo {
        BindingInfo {
            pv: pv.clone(),
            pvc: pvc.clone(),
        }
    }

    // ---- assume_cache_test.go TestAssume ----

    #[test]
    fn assume_cache_assume_scenarios() {
        // (old rv, new rv, expect ok)
        let cases = [
            ("5", "5", true),  // success-same-version
            ("5", "6", true),  // success-new-higher-version
            ("5", "4", false), // fail-new-lower-version
            ("5", "a", false), // fail-new-bad-version
            ("a", "5", false), // fail-old-bad-version
        ];
        for (old_rv, new_rv, ok) in cases {
            let mut c = AssumeCache::<PersistentVolumeClaim>::new();
            let old = mk_pvc("pvc1", old_rv, "", json!({}));
            let new = mk_pvc("pvc1", new_rv, "", json!({"x": "y"}));
            c.add(&old);
            assert_eq!(c.assume(&new).is_ok(), ok, "{old_rv}->{new_rv}");
            let expect = if ok { &new } else { &old };
            assert_same!(c.get("testns/pvc1").unwrap(), expect);
            assert_same!(c.get_api_obj("testns/pvc1").unwrap(), &old);
        }
    }

    #[test]
    fn assume_cache_assume_unknown_object_is_not_found() {
        let mut c = AssumeCache::<PersistentVolumeClaim>::new();
        c.add(&mk_pvc("pvc2", "5", "", json!({})));
        let err = c.assume(&mk_pvc("pvc1", "5", "", json!({}))).unwrap_err();
        assert!(matches!(err, AssumeError::NotFound { .. }), "{err:?}");
    }

    // ---- assume_cache_test.go TestRestore ----

    #[test]
    fn assume_cache_restore() {
        let mut c = AssumeCache::<PersistentVolumeClaim>::new();
        c.restore("nothing"); // no-op on a missing key
        let old = mk_pvc("pvc1", "5", "", json!({}));
        let new = mk_pvc("pvc1", "5", "", json!({"a": "b"}));
        c.add(&old);
        c.restore("testns/pvc1");
        assert_same!(c.get("testns/pvc1").unwrap(), &old);
        c.assume(&new).unwrap();
        assert_same!(c.get("testns/pvc1").unwrap(), &new);
        c.restore("testns/pvc1");
        assert_same!(c.get("testns/pvc1").unwrap(), &old);
    }

    // ---- assume_cache.go add(): "don't override assumed objects due to resync" ----

    #[test]
    fn assume_cache_resync_same_version_keeps_assumed_object() {
        let mut c = AssumeCache::<PersistentVolumeClaim>::new();
        let old = mk_pvc("pvc1", "5", "", json!({}));
        let assumed = mk_pvc("pvc1", "5", "", json!({"a": "b"}));
        c.add(&old);
        c.assume(&assumed).unwrap();
        c.add(&old); // informer resync, same version
        assert_same!(c.get("testns/pvc1").unwrap(), &assumed);
        let newer = mk_pvc("pvc1", "6", "", json!({}));
        c.add(&newer); // genuinely newer API object wins
        assert_same!(c.get("testns/pvc1").unwrap(), &newer);
        assert_same!(c.get_api_obj("testns/pvc1").unwrap(), &newer);
    }

    #[test]
    fn assume_cache_sync_deletes_absent_and_lists_latest() {
        let mut c = AssumeCache::<PersistentVolume>::new();
        c.sync(&[mk_pv("a", "1", Value::Null), mk_pv("b", "1", Value::Null)]);
        assert_eq!(c.list().len(), 2);
        c.sync(&[mk_pv("a", "1", Value::Null)]);
        assert_eq!(c.list().len(), 1);
        assert!(matches!(c.get("b"), Err(AssumeError::NotFound { .. })));
    }

    // ---- pv_helpers.go GetBindVolumeToClaim ----

    #[test]
    fn get_bind_volume_to_claim_sets_claim_ref_and_bound_by_controller() {
        let pvc = mk_pvc("pvc1", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let (new, dirty) = get_bind_volume_to_claim(&pv, &pvc).unwrap();
        assert!(dirty);
        let cr = new.spec.claim_ref.as_ref().unwrap();
        assert_eq!(cr.name.as_deref(), Some("pvc1"));
        assert_eq!(cr.namespace.as_deref(), Some("testns"));
        assert_eq!(cr.uid.as_deref(), Some("pvc1-uid"));
        assert_eq!(cr.kind.as_deref(), Some("PersistentVolumeClaim"));
        assert_eq!(cr.api_version.as_deref(), Some("v1"));
        assert_eq!(
            new.metadata.annotations.as_ref().unwrap()[ANN_BOUND_BY_CONTROLLER],
            "yes"
        );
        assert_eq!(pv.spec.claim_ref, None, "input must not be mutated");
        assert_eq!(new.metadata.resource_version.as_deref(), Some("7"));
    }

    #[test]
    fn get_bind_volume_to_claim_already_bound_is_not_dirty() {
        let pvc = mk_pvc("pvc1", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", claim_ref_for(&pvc));
        let (new, dirty) = get_bind_volume_to_claim(&pv, &pvc).unwrap();
        assert!(!dirty);
        assert_same!(new, pv);
    }

    // ---- binder_test.go TestAssumePodVolumes ----

    #[test]
    fn assume_pod_volumes_all_bound() {
        let pvc = bound_pvc("bound", "pv-bound");
        let pv = mk_pv("pv-bound", "1", claim_ref_for(&pvc));
        let mut b = binder_with(std::slice::from_ref(&pvc), std::slice::from_ref(&pv));
        let mut vols = PodVolumes::default();
        let all = b
            .assume_pod_volumes(&pod_with(&[&pvc]), "node1", &mut vols)
            .unwrap();
        assert!(all);
    }

    #[test]
    fn assume_pod_volumes_one_and_two_bindings() {
        let pvc1 = mk_pvc("pvc1", "5", "", json!({}));
        let pvc2 = mk_pvc("pvc2", "5", "", json!({}));
        let pv1 = mk_pv("pv1", "7", Value::Null);
        let pv2 = mk_pv("pv2", "7", Value::Null);
        let mut b = binder_with(&[pvc1.clone(), pvc2.clone()], &[pv1.clone(), pv2.clone()]);
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv1, &pvc1), binding(&pv2, &pvc2)],
            dynamic_provisions: vec![],
        };
        let all = b
            .assume_pod_volumes(&pod_with(&[&pvc1, &pvc2]), "node1", &mut vols)
            .unwrap();
        assert!(!all);
        // PodVolumes now carries the assumed (cache) objects.
        for (i, pv) in ["pv1", "pv2"].iter().enumerate() {
            let assumed = &vols.static_bindings[i].pv;
            assert_eq!(assumed.metadata.name, *pv);
            assert!(assumed.spec.claim_ref.is_some());
            assert_same!(b.pv_cache.get(pv).unwrap(), assumed);
            assert_differ!(b.pv_cache.get_api_obj(pv).unwrap(), assumed);
        }
    }

    #[test]
    fn assume_pod_volumes_pv_already_bound_not_dirty() {
        let pvc = mk_pvc("pvc1", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", claim_ref_for(&pvc));
        let mut b = binder_with(std::slice::from_ref(&pvc), std::slice::from_ref(&pv));
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &pvc)],
            dynamic_provisions: vec![],
        };
        assert!(!b
            .assume_pod_volumes(&pod_with(&[&pvc]), "node1", &mut vols)
            .unwrap());
        assert_same!(vols.static_bindings[0].pv, pv);
        assert_same!(
            b.pv_cache.get("pv1").unwrap(),
            b.pv_cache.get_api_obj("pv1").unwrap()
        );
    }

    #[test]
    fn assume_pod_volumes_tmpupdate_failed_restores_cache() {
        let pvc1 = mk_pvc("pvc1", "5", "", json!({}));
        let pvc2 = mk_pvc("pvc2", "5", "", json!({}));
        let pv1 = mk_pv("pv1", "7", Value::Null);
        let pv2 = mk_pv("pv2", "7", Value::Null);
        // pv2 is not in the cache -> second Assume fails; first is reverted.
        let mut b = binder_with(std::slice::from_ref(&pvc1), std::slice::from_ref(&pv1));
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv1, &pvc1), binding(&pv2, &pvc2)],
            dynamic_provisions: vec![],
        };
        let all = b.assume_pod_volumes(&pod_with(&[&pvc1]), "node1", &mut vols);
        assert!(all.is_err());
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv1);
        assert_eq!(
            vols.static_bindings.len(),
            2,
            "PodVolumes untouched on error"
        );
    }

    #[test]
    fn assume_pod_volumes_binding_and_provisioning() {
        let unbound = mk_pvc("unbound", "5", "", json!({}));
        let provisioned = mk_pvc("provisioned", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let mut b = binder_with(
            &[unbound.clone(), provisioned.clone()],
            std::slice::from_ref(&pv),
        );
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &unbound)],
            dynamic_provisions: vec![DynamicProvision {
                pvc: provisioned.clone(),
                node_capacity: None,
            }],
        };
        let all = b
            .assume_pod_volumes(&pod_with(&[&unbound, &provisioned]), "node1", &mut vols)
            .unwrap();
        assert!(!all);
        let sel = |c: &PersistentVolumeClaim| {
            c.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(ANN_SELECTED_NODE))
                .cloned()
        };
        assert_eq!(
            sel(&vols.dynamic_provisions[0].pvc).as_deref(),
            Some("node1")
        );
        assert_eq!(
            sel(b.pvc_cache.get("testns/provisioned").unwrap()).as_deref(),
            Some("node1")
        );
        // The lister's object must not be mutated.
        assert_eq!(sel(&provisioned), None);
        assert_eq!(
            sel(b.pvc_cache.get_api_obj("testns/provisioned").unwrap()),
            None
        );
    }

    #[test]
    fn assume_pod_volumes_provision_tmpupdate_failed_restores_cache() {
        let unbound = mk_pvc("unbound", "5", "", json!({}));
        let cached_higher = mk_pvc("provisioned", "6", "", json!({}));
        let stale = mk_pvc("provisioned", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let mut b = binder_with(
            &[unbound.clone(), cached_higher.clone()],
            std::slice::from_ref(&pv),
        );
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &unbound)],
            dynamic_provisions: vec![DynamicProvision {
                pvc: stale,
                node_capacity: None,
            }],
        };
        let r = b.assume_pod_volumes(&pod_with(&[&unbound, &cached_higher]), "node1", &mut vols);
        assert!(r.is_err());
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv, "PV assume reverted");
        assert_same!(
            b.pvc_cache.get("testns/provisioned").unwrap(),
            &cached_higher
        );
    }

    // ---- binder_test.go TestRevertAssumedPodVolumes ----

    #[test]
    fn revert_assumed_pod_volumes_restores_both_caches() {
        let unbound = mk_pvc("unbound", "5", "", json!({}));
        let provisioned = mk_pvc("provisioned", "5", "", json!({}));
        let pv = mk_pv("pv1", "7", Value::Null);
        let mut b = binder_with(
            &[unbound.clone(), provisioned.clone()],
            std::slice::from_ref(&pv),
        );
        let mut vols = PodVolumes {
            static_bindings: vec![binding(&pv, &unbound)],
            dynamic_provisions: vec![DynamicProvision {
                pvc: provisioned.clone(),
                node_capacity: None,
            }],
        };
        b.assume_pod_volumes(&pod_with(&[&unbound, &provisioned]), "node1", &mut vols)
            .unwrap();
        assert_differ!(b.pv_cache.get("pv1").unwrap(), &pv);
        b.revert_assumed_pod_volumes(&vols);
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv);
        assert_same!(b.pvc_cache.get("testns/provisioned").unwrap(), &provisioned);
        // Idempotent (volume_binding.go Unreserve doc).
        b.revert_assumed_pod_volumes(&vols);
        assert_same!(b.pv_cache.get("pv1").unwrap(), &pv);
    }
}
