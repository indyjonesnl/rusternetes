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
        self.sync_scoped(objs, |_| true);
    }

    /// `sync` for a listing that only covers part of the key space (the PVCs of
    /// one namespace): objects outside `in_scope` are left untouched, so an
    /// assumed PVC of another namespace survives the refresh.
    pub fn sync_scoped(&mut self, objs: &[T], in_scope: impl Fn(&str) -> bool) {
        let present: std::collections::HashSet<String> =
            objs.iter().map(|o| o.cache_key()).collect();
        self.objs.retain(|k, _| !in_scope(k) || present.contains(k));
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

    /// `sync` for a snapshot whose PVC list is limited to namespace `ns`
    /// (`DataPlane::list_volume_snapshot`): PVs are global, PVCs are scoped.
    pub fn sync_namespace(&mut self, ns: &str, snap: &VolumeSnapshot) {
        self.pv_cache.sync(&snap.pvs);
        let prefix = format!("{ns}/");
        self.pvc_cache
            .sync_scoped(&snap.pvcs, |k| k.starts_with(&prefix));
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
