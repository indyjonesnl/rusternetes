//! VolumeBinding PreBind: `BindPodVolumes` / `bindAPIUpdate` / `checkBindings`
//! and the plugin's `PreBindPreFlight` / `PreBind` hooks.
//!
//! Ported from upstream Kubernetes release-1.35
//! `pkg/scheduler/framework/plugins/volumebinding/binder.go`
//! (`BindPodVolumes` :479-514, `bindAPIUpdate` :521-597, `checkBindings`
//! :607-722) and `volume_binding.go` (`PreBindPreFlight` :555-568, `PreBind`
//! :570-601, `Unreserve` :603-615).
//!
//! The assume caches live in `volume_assume` (a separate library); this module
//! reaches them, the node/CSINode/pod listers and the API client through the
//! [`BinderEnv`] trait, mirroring the fields of Go's `volumeBinder`
//! (`kubeClient`, `pvCache`, `pvcCache`, `nodeLister`, `csiNodeLister`,
//! `podLister`). Go's nil-vs-empty slice distinction ("failed to get cached
//! bindings") has no equivalent with Rust `Vec`s and is not ported.

use crate::data_plane::DataPlane;
use crate::volume_assume::VolumeBinder;
use crate::volume_binding::{
    check_pv_node_affinity, is_pvc_fully_bound, pvc_key, try_translate_pv_to_csi, BindingInfo,
    DynamicProvision, PodVolumes, ANN_SELECTED_NODE,
};
use rusternetes_common::resources::{CSINode, Node, PersistentVolume, PersistentVolumeClaim, Pod};
use std::collections::HashMap;
use std::time::Duration;

/// `BindTimeoutSeconds` default, `pkg/scheduler/apis/config/v1/defaults.go:193-194`.
pub const DEFAULT_BIND_TIMEOUT: Duration = Duration::from_secs(600);

/// `wait.PollUntilContextTimeout(ctx, time.Second, ...)` (binder.go:497).
pub const BIND_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// `errNoPodVolumeForNode` (volume_binding.go:546).
pub const ERR_NO_POD_VOLUME_FOR_NODE: &str = "no pod volume found for node";

/// Cache lookup failure; `NotFound` mirrors `apierrors.IsNotFound`.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheError {
    NotFound(String),
    Other(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::NotFound(m) | CacheError::Other(m) => f.write_str(m),
        }
    }
}

/// The collaborators `volumeBinder` holds in Go.
#[async_trait::async_trait]
pub trait BinderEnv: Send + Sync {
    /// `kubeClient.CoreV1().PersistentVolumes().Update`.
    async fn update_pv(&self, pv: &PersistentVolume) -> Result<PersistentVolume, String>;
    /// `kubeClient.CoreV1().PersistentVolumeClaims(ns).Update`.
    async fn update_pvc(
        &self,
        pvc: &PersistentVolumeClaim,
    ) -> Result<PersistentVolumeClaim, String>;
    /// `pvCache.GetAPIObj(name)`.
    fn pv_api_obj(&self, name: &str) -> Result<PersistentVolume, CacheError>;
    /// `pvcCache.GetAPIObj("ns/name")`.
    fn pvc_api_obj(&self, key: &str) -> Result<PersistentVolumeClaim, CacheError>;
    /// `revertAssumedPVs`.
    fn revert_assumed_pvs(&self, bindings: &[BindingInfo]);
    /// `revertAssumedPVCs`.
    fn revert_assumed_pvcs(&self, claims: &[PersistentVolumeClaim]);
    /// `nodeLister.Get`.
    async fn node(&self, name: &str) -> Result<Node, String>;
    /// `csiNodeLister.Get`; absence is tolerated.
    async fn csi_node(&self, name: &str) -> Option<CSINode>;
    /// Feed the assume caches a fresh listing. Go's informers do this
    /// continuously; `bind_pod_volumes` calls it before every poll so the
    /// objects written by `bind_api_update` become visible to `check_bindings`.
    async fn refresh(&self) {}
    /// `podLister.Pods(ns).Get`: `Ok(false)` is NotFound, `Err` any other error.
    async fn pod_exists(&self, pod: &Pod) -> Result<bool, String>;
}

/// `stateData` (volume_binding.go) as far as PreBind needs it.
#[derive(Default, Clone)]
pub struct VolumeBindingState {
    pub all_bound: bool,
    pub pod_volumes_by_node: HashMap<String, PodVolumes>,
}

/// `PreBindPreFlight` result: `Skip` is `fwk.Skip`.
#[derive(Debug, PartialEq, Eq)]
pub enum PreBindPreFlight {
    Skip,
    Run,
}

fn pod_name(pod: &Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.as_deref().unwrap_or(""),
        pod.metadata.name
    )
}

/// `APIObjectVersioner.ObjectResourceVersion` (api_object_versioner.go:73-82):
/// empty is 0; a parse failure is a coder error upstream (it panics), surfaced
/// here as an `Err`.
fn object_resource_version(rv: Option<&str>) -> Result<u64, String> {
    match rv {
        None | Some("") => Ok(0),
        Some(v) => v
            .parse::<u64>()
            .map_err(|e| format!("invalid resource version {v:?}: {e}")),
    }
}

/// `bindAPIUpdate` (binder.go:521-597): write the assumed PV claimRefs and
/// the selected-node PVC annotations, replacing each slice entry with the
/// object the API server returned. Entries not yet written when an update
/// fails are reverted in the assume caches (the deferred func at :536-545).
pub async fn bind_api_update(
    env: &dyn BinderEnv,
    bindings: &mut [BindingInfo],
    claims_to_provision: &mut [PersistentVolumeClaim],
) -> Result<(), String> {
    let mut last_processed_binding = 0;
    let mut last_processed_provisioning = 0;
    let result = async {
        // Do the actual prebinding. Let the PV controller take care of the
        // rest. There is no API rollback if the actual binding fails.
        for binding in bindings.iter_mut() {
            let new_pv = env.update_pv(&binding.pv).await?;
            // Save updated object from apiserver for later checking.
            binding.pv = new_pv;
            last_processed_binding += 1;
        }
        // Update claims objects to trigger volume provisioning. The PV
        // controller signals back by removing annotations if provisioning fails.
        for claim in claims_to_provision.iter_mut() {
            let new_claim = env.update_pvc(claim).await?;
            *claim = new_claim;
            last_processed_provisioning += 1;
        }
        Ok(())
    }
    .await;
    // only revert assumed cached updates for volumes we haven't successfully
    // bound / claims we haven't updated
    if last_processed_binding < bindings.len() {
        env.revert_assumed_pvs(&bindings[last_processed_binding..]);
    }
    if last_processed_provisioning < claims_to_provision.len() {
        env.revert_assumed_pvcs(&claims_to_provision[last_processed_provisioning..]);
    }
    result
}

/// `volume.CheckNodeAffinity` failure text (helpers.go:79).
const ERR_NO_MATCHING_TERMS: &str = "no matching NodeSelectorTerms";

/// `checkBindings` (binder.go:607-722). `Ok(true)` when every PVC is fully
/// bound, `Ok(false)` to keep waiting, `Err` when binding must be retried.
/// Reads the API objects held by the assume caches, not the (possibly
/// re-assumed) cached ones.
pub async fn check_bindings(
    env: &dyn BinderEnv,
    pod: &Pod,
    bindings: &[BindingInfo],
    claims_to_provision: &[PersistentVolumeClaim],
) -> Result<bool, String> {
    let node_name = pod
        .spec
        .as_ref()
        .and_then(|s| s.node_name.as_deref())
        .unwrap_or("");
    let node = env
        .node(node_name)
        .await
        .map_err(|e| format!("failed to get node {node_name:?}: {e}"))?;
    // Upstream tolerates a missing CSINode ("TODO: return the error once
    // CSINode is created by default").
    let csi_node = env.csi_node(&node.metadata.name).await;

    // When pod is deleted, binding operation should be cancelled. There is no
    // need to check PV/PVC bindings any more.
    match env.pod_exists(pod).await {
        Ok(false) => {
            return Err(format!(
                "pod does not exist any more: pods {:?} not found",
                pod.metadata.name
            ))
        }
        Ok(true) => {}
        Err(e) => {
            tracing::error!(pod = %pod_name(pod), error = %e, "Failed to get pod from the lister")
        }
    }

    for binding in bindings {
        let pv = env
            .pv_api_obj(&binding.pv.metadata.name)
            .map_err(|e| format!("failed to check binding: {e}"))?;
        let pvc = env
            .pvc_api_obj(&pvc_key(&binding.pvc))
            .map_err(|e| format!("failed to check binding: {e}"))?;

        // Because we updated PV in apiserver, skip if API object is older and
        // wait for the new API object propagated from apiserver.
        if object_resource_version(binding.pv.metadata.resource_version.as_deref())?
            > object_resource_version(pv.metadata.resource_version.as_deref())?
        {
            return Ok(false);
        }

        let pv = try_translate_pv_to_csi(&pv, csi_node.as_ref())
            .map_err(|e| format!("failed to translate pv to csi: {e}"))?;

        // Check PV's node affinity (the node might not have the proper label)
        if !check_pv_node_affinity(&pv, &node) {
            return Err(format!(
                "pv {:?} node affinity doesn't match node {:?}: {ERR_NO_MATCHING_TERMS}",
                pv.metadata.name, node.metadata.name
            ));
        }

        // Check if pv.ClaimRef got dropped by unbindVolume()
        if pv
            .spec
            .claim_ref
            .as_ref()
            .is_none_or(|cr| cr.uid.as_deref().unwrap_or("").is_empty())
        {
            return Err(format!("ClaimRef got reset for pv {:?}", pv.metadata.name));
        }

        // Check if pvc is fully bound
        if !is_pvc_fully_bound(&pvc) {
            return Ok(false);
        }
    }

    for claim in claims_to_provision {
        let pvc = env
            .pvc_api_obj(&pvc_key(claim))
            .map_err(|e| format!("failed to check provisioning pvc: {e}"))?;

        if object_resource_version(claim.metadata.resource_version.as_deref())?
            > object_resource_version(pvc.metadata.resource_version.as_deref())?
        {
            return Ok(false);
        }

        // Check if selectedNode annotation is still set
        let Some(ann) = pvc.metadata.annotations.as_ref() else {
            return Err(format!(
                "selectedNode annotation reset for PVC {:?}",
                pvc.metadata.name
            ));
        };
        if ann.get(ANN_SELECTED_NODE).map(String::as_str).unwrap_or("") != node_name {
            // If provisioner fails to provision a volume, selectedNode
            // annotation will be removed to signal back to the scheduler to retry.
            return Err(format!(
                "provisioning failed for PVC {:?}",
                pvc.metadata.name
            ));
        }

        // If the PVC is bound to a PV, check its node affinity
        if let Some(pv_name) = pvc.spec.volume_name.as_deref().filter(|v| !v.is_empty()) {
            let pv = match env.pv_api_obj(pv_name) {
                Ok(pv) => pv,
                // We tolerate NotFound here: the PV is possibly not found
                // because of API delay, so check next time. If it was deleted
                // the PVC will be unbound eventually.
                Err(CacheError::NotFound(_)) => return Ok(false),
                Err(e) => return Err(format!("failed to get pv {pv_name:?} from cache: {e}")),
            };
            let pv = try_translate_pv_to_csi(&pv, csi_node.as_ref())?;
            if !check_pv_node_affinity(&pv, &node) {
                return Err(format!(
                    "pv {:?} node affinity doesn't match node {:?}: {ERR_NO_MATCHING_TERMS}",
                    pv.metadata.name, node.metadata.name
                ));
            }
        }

        // Check if pvc is fully bound
        if !is_pvc_fully_bound(&pvc) {
            return Ok(false);
        }
    }

    // All pvs and pvcs that we operated on are bound
    Ok(true)
}

/// `BindPodVolumes` (binder.go:479-514): make the API updates, then poll
/// `checkBindings` every `poll_interval` (first check after one interval,
/// `PollUntilContextTimeout` with `immediate=false`) until `bind_timeout`.
pub async fn bind_pod_volumes(
    env: &dyn BinderEnv,
    pod: &Pod,
    vols: &mut PodVolumes,
    bind_timeout: Duration,
    poll_interval: Duration,
) -> Result<(), String> {
    // convertDynamicProvisionsToPVCs (binder.go:469-476)
    let mut claims_to_provision: Vec<PersistentVolumeClaim> = vols
        .dynamic_provisions
        .iter()
        .map(|d| d.pvc.clone())
        .collect();

    // Start API operations
    let r = bind_api_update(env, &mut vols.static_bindings, &mut claims_to_provision).await;
    // Go shares these objects by pointer, so the API-returned copies stay in
    // PodVolumes; mirror that.
    for (d, c) in vols.dynamic_provisions.iter_mut().zip(&claims_to_provision) {
        d.pvc = c.clone();
    }
    r?;

    let bindings = &vols.static_bindings;
    let poll = async {
        loop {
            tokio::time::sleep(poll_interval).await;
            env.refresh().await;
            if check_bindings(env, pod, bindings, &claims_to_provision).await? {
                return Ok::<(), String>(());
            }
        }
    };
    match tokio::time::timeout(bind_timeout, poll).await {
        Ok(r) => r.map_err(|e| format!("binding volumes: {e}")),
        Err(_) => Err("binding volumes: context deadline exceeded".to_string()),
    }
}

/// `Reserve` (volume_binding.go:531-553): assume the chosen node's PodVolumes
/// and record `allBound` in the cycle state. A node without PodVolumes (pod
/// without PVCs) is trivially all-bound.
pub fn reserve(
    binder: &mut VolumeBinder,
    state: &mut VolumeBindingState,
    pod: &Pod,
    node_name: &str,
) -> Result<(), String> {
    match state.pod_volumes_by_node.get_mut(node_name) {
        Some(vols) => {
            state.all_bound = binder.assume_pod_volumes(pod, node_name, vols)?;
        }
        // may not exist if the pod does not reference any PVC
        None => state.all_bound = true,
    }
    Ok(())
}

/// [`BinderEnv`] over the scheduler's long-lived [`VolumeBinder`] caches and
/// [`DataPlane`] (storage or api-server).
pub struct DataPlaneBinderEnv<'a, S: rusternetes_storage::Storage + Send + Sync + 'static> {
    pub binder: &'a std::sync::Mutex<VolumeBinder>,
    pub data: &'a DataPlane<S>,
    /// Namespace of the pod being bound (scopes the PVC listing).
    pub namespace: String,
}

#[async_trait::async_trait]
impl<S: rusternetes_storage::Storage + Send + Sync + 'static> BinderEnv
    for DataPlaneBinderEnv<'_, S>
{
    async fn update_pv(&self, pv: &PersistentVolume) -> Result<PersistentVolume, String> {
        self.data.update_pv(pv).await.map_err(|e| e.to_string())
    }
    async fn update_pvc(
        &self,
        pvc: &PersistentVolumeClaim,
    ) -> Result<PersistentVolumeClaim, String> {
        self.data.update_pvc(pvc).await.map_err(|e| e.to_string())
    }
    fn pv_api_obj(&self, name: &str) -> Result<PersistentVolume, CacheError> {
        let b = self.binder.lock().unwrap_or_else(|e| e.into_inner());
        b.pv_cache.get_api_obj(name).cloned().map_err(cache_error)
    }
    fn pvc_api_obj(&self, key: &str) -> Result<PersistentVolumeClaim, CacheError> {
        let b = self.binder.lock().unwrap_or_else(|e| e.into_inner());
        b.pvc_cache.get_api_obj(key).cloned().map_err(cache_error)
    }
    fn revert_assumed_pvs(&self, bindings: &[BindingInfo]) {
        let mut b = self.binder.lock().unwrap_or_else(|e| e.into_inner());
        b.revert_assumed_pod_volumes(&PodVolumes {
            static_bindings: bindings.to_vec(),
            dynamic_provisions: vec![],
        });
    }
    fn revert_assumed_pvcs(&self, claims: &[PersistentVolumeClaim]) {
        let mut b = self.binder.lock().unwrap_or_else(|e| e.into_inner());
        b.revert_assumed_pod_volumes(&PodVolumes {
            static_bindings: vec![],
            dynamic_provisions: claims
                .iter()
                .map(|pvc| DynamicProvision {
                    pvc: pvc.clone(),
                    node_capacity: None,
                })
                .collect(),
        });
    }
    async fn node(&self, name: &str) -> Result<Node, String> {
        let nodes = self.data.list_nodes().await.map_err(|e| e.to_string())?;
        nodes
            .into_iter()
            .find(|n| n.metadata.name == name)
            .ok_or_else(|| format!("node {name:?} not found"))
    }
    async fn csi_node(&self, name: &str) -> Option<CSINode> {
        self.data.get_csi_node(name).await
    }
    async fn refresh(&self) {
        match self.data.list_volume_snapshot(&self.namespace).await {
            Ok(snap) => {
                let mut b = self.binder.lock().unwrap_or_else(|e| e.into_inner());
                b.sync_namespace(&self.namespace, &snap);
            }
            Err(e) => tracing::warn!("VolumeBinding: failed to refresh volume caches: {e}"),
        }
    }
    async fn pod_exists(&self, pod: &Pod) -> Result<bool, String> {
        let ns = pod.metadata.namespace.as_deref().unwrap_or("default");
        match self.data.get_pod(ns, &pod.metadata.name).await {
            Ok(_) => Ok(true),
            Err(rusternetes_common::Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e.to_string()),
        }
    }
}

fn cache_error(e: crate::volume_assume::AssumeError) -> CacheError {
    match e {
        crate::volume_assume::AssumeError::NotFound { .. } => CacheError::NotFound(e.to_string()),
        other => CacheError::Other(other.to_string()),
    }
}

/// `PreBindPreFlight` (volume_binding.go:555-568).
pub fn pre_bind_preflight(
    state: &VolumeBindingState,
    node_name: &str,
) -> Result<PreBindPreFlight, String> {
    if state.all_bound {
        // no need to bind volumes
        return Ok(PreBindPreFlight::Skip);
    }
    if !state.pod_volumes_by_node.contains_key(node_name) {
        return Err(format!("{ERR_NO_POD_VOLUME_FOR_NODE} {node_name:?}"));
    }
    Ok(PreBindPreFlight::Run)
}

/// `PreBind` (volume_binding.go:570-601). Upstream runs it on the assumed pod
/// (`spec.nodeName` already set), which `checkBindings` reads.
pub async fn pre_bind(
    env: &dyn BinderEnv,
    state: &mut VolumeBindingState,
    pod: &Pod,
    node_name: &str,
    bind_timeout: Duration,
    poll_interval: Duration,
) -> Result<(), String> {
    if state.all_bound {
        // no need to bind volumes
        return Ok(());
    }
    let Some(vols) = state.pod_volumes_by_node.get_mut(node_name) else {
        return Err(format!("{ERR_NO_POD_VOLUME_FOR_NODE} {node_name:?}"));
    };
    let mut assumed = pod.clone();
    if let Some(spec) = assumed.spec.as_mut() {
        spec.node_name = Some(node_name.to_string());
    }
    bind_pod_volumes(env, &assumed, vols, bind_timeout, poll_interval).await
}

/// `Unreserve` (volume_binding.go:603-615): idempotent, no-op when the node has
/// no PodVolumes.
pub fn unreserve(env: &dyn BinderEnv, state: &VolumeBindingState, node_name: &str) {
    let Some(vols) = state.pod_volumes_by_node.get(node_name) else {
        return;
    };
    env.revert_assumed_pvs(&vols.static_bindings);
    let pvcs: Vec<_> = vols
        .dynamic_provisions
        .iter()
        .map(|d| d.pvc.clone())
        .collect();
    env.revert_assumed_pvcs(&pvcs);
}
