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

use crate::volume_binding::{
    check_pv_node_affinity, is_pvc_fully_bound, pvc_key, try_translate_pv_to_csi, BindingInfo,
    PodVolumes, ANN_SELECTED_NODE,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_binding::DynamicProvision;
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    // ---- fixtures (binder_test.go: pvNode1a, pvNode1aBound, unboundPVC, ...) ----

    fn pod(node: &str) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "test-pod", "namespace": "testns", "uid": "pod-uid"},
            "spec": {"containers": [{"name": "c", "image": "i"}], "nodeName": node}
        }))
        .unwrap()
    }

    fn node(name: &str, labels: Value) -> Node {
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": name, "labels": labels}, "spec": {}
        }))
        .unwrap()
    }

    /// PV on `zone`, optionally pre-bound to the claim `claim` (uid `<claim>-uid`).
    fn pv(
        name: &str,
        rv: &str,
        zone: &str,
        claim: Option<&str>,
        claim_uid: &str,
    ) -> PersistentVolume {
        let mut spec = json!({
            "capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
            "storageClassName": "wait", "hostPath": {"path": "/x"},
            "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                {"key": "zone", "operator": "In", "values": [zone]}]}]}}
        });
        if let Some(c) = claim {
            spec["claimRef"] = json!({"kind": "PersistentVolumeClaim", "namespace": "testns",
                "name": c, "uid": claim_uid, "apiVersion": "v1"});
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": name, "resourceVersion": rv}, "spec": spec,
            "status": {"phase": "Available"}
        }))
        .unwrap()
    }

    fn pvc(name: &str, rv: &str, volume: &str, ann: Value) -> PersistentVolumeClaim {
        let mut spec = json!({"resources": {"requests": {"storage": "1Gi"}},
            "storageClassName": "wait"});
        if !volume.is_empty() {
            spec["volumeName"] = json!(volume);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": "testns", "uid": format!("{name}-uid"),
                "resourceVersion": rv, "annotations": ann},
            "spec": spec, "status": {"phase": "Pending"}
        }))
        .unwrap()
    }

    fn bound_ann() -> Value {
        json!({"pv.kubernetes.io/bind-completed": "yes"})
    }
    fn selected(node: &str) -> Value {
        json!({"volume.kubernetes.io/selected-node": node})
    }
    fn binding(pvc: &PersistentVolumeClaim, pv: &PersistentVolume) -> BindingInfo {
        BindingInfo {
            pv: pv.clone(),
            pvc: pvc.clone(),
        }
    }
    fn with_rv<T: serde::Serialize + serde::de::DeserializeOwned>(o: &T, rv: &str) -> T {
        let mut v = serde_json::to_value(o).unwrap();
        v["metadata"]["resourceVersion"] = json!(rv);
        serde_json::from_value(v).unwrap()
    }

    // ---- fake env: an API store with optimistic-concurrency Update ----

    #[derive(Default)]
    struct FakeEnv {
        pvs: Mutex<HashMap<String, PersistentVolume>>,
        pvcs: Mutex<HashMap<String, PersistentVolumeClaim>>,
        nodes: Mutex<HashMap<String, Node>>,
        pod_present: Mutex<bool>,
        reverted_pvs: Mutex<Vec<String>>,
        reverted_pvcs: Mutex<Vec<String>>,
        pv_updates: Mutex<usize>,
        pvc_updates: Mutex<usize>,
    }

    fn rv(o: &str) -> u64 {
        o.parse().unwrap_or(0)
    }

    impl FakeEnv {
        fn new() -> Arc<Self> {
            let e = Arc::new(Self::default());
            *e.pod_present.lock().unwrap() = true;
            e.nodes
                .lock()
                .unwrap()
                .insert("node1".into(), node("node1", json!({"zone": "a"})));
            e
        }
        fn put_pv(&self, pv: &PersistentVolume) {
            self.pvs
                .lock()
                .unwrap()
                .insert(pv.metadata.name.clone(), pv.clone());
        }
        fn put_pvc(&self, pvc: &PersistentVolumeClaim) {
            self.pvcs
                .lock()
                .unwrap()
                .insert(pvc_key_t(pvc), pvc.clone());
        }
    }

    fn pvc_key_t(p: &PersistentVolumeClaim) -> String {
        format!(
            "{}/{}",
            p.metadata.namespace.as_deref().unwrap_or(""),
            p.metadata.name
        )
    }

    #[async_trait::async_trait]
    impl BinderEnv for FakeEnv {
        async fn update_pv(&self, pv: &PersistentVolume) -> Result<PersistentVolume, String> {
            *self.pv_updates.lock().unwrap() += 1;
            let mut s = self.pvs.lock().unwrap();
            let cur = s.get(&pv.metadata.name).ok_or("not found")?;
            let cur_rv = cur.metadata.resource_version.clone().unwrap_or_default();
            if cur_rv != pv.metadata.resource_version.clone().unwrap_or_default() {
                return Err("Operation cannot be fulfilled: the object has been modified".into());
            }
            let new = with_rv(pv, &(rv(&cur_rv) + 1).to_string());
            s.insert(pv.metadata.name.clone(), new.clone());
            Ok(new)
        }
        async fn update_pvc(
            &self,
            pvc: &PersistentVolumeClaim,
        ) -> Result<PersistentVolumeClaim, String> {
            *self.pvc_updates.lock().unwrap() += 1;
            let mut s = self.pvcs.lock().unwrap();
            let k = pvc_key_t(pvc);
            let cur = s.get(&k).ok_or("not found")?;
            let cur_rv = cur.metadata.resource_version.clone().unwrap_or_default();
            if cur_rv != pvc.metadata.resource_version.clone().unwrap_or_default() {
                return Err("Operation cannot be fulfilled: the object has been modified".into());
            }
            let new = with_rv(pvc, &(rv(&cur_rv) + 1).to_string());
            s.insert(k, new.clone());
            Ok(new)
        }
        fn pv_api_obj(&self, name: &str) -> Result<PersistentVolume, CacheError> {
            self.pvs.lock().unwrap().get(name).cloned().ok_or_else(|| {
                CacheError::NotFound(format!("v1.PersistentVolume {name:?} does not exist"))
            })
        }
        fn pvc_api_obj(&self, key: &str) -> Result<PersistentVolumeClaim, CacheError> {
            self.pvcs.lock().unwrap().get(key).cloned().ok_or_else(|| {
                CacheError::NotFound(format!("v1.PersistentVolumeClaim {key:?} does not exist"))
            })
        }
        fn revert_assumed_pvs(&self, bindings: &[BindingInfo]) {
            let mut r = self.reverted_pvs.lock().unwrap();
            r.extend(bindings.iter().map(|b| b.pv.metadata.name.clone()));
        }
        fn revert_assumed_pvcs(&self, claims: &[PersistentVolumeClaim]) {
            let mut r = self.reverted_pvcs.lock().unwrap();
            r.extend(claims.iter().map(|c| c.metadata.name.clone()));
        }
        async fn node(&self, name: &str) -> Result<Node, String> {
            self.nodes
                .lock()
                .unwrap()
                .get(name)
                .cloned()
                .ok_or_else(|| format!("node {name:?} not found"))
        }
        async fn csi_node(&self, _name: &str) -> Option<CSINode> {
            None
        }
        async fn pod_exists(&self, _pod: &Pod) -> Result<bool, String> {
            Ok(*self.pod_present.lock().unwrap())
        }
    }

    const FAST: Duration = Duration::from_millis(10);

    fn unbound() -> PersistentVolumeClaim {
        pvc("unbound-pvc", "1", "", json!({}))
    }
    fn pv1a() -> PersistentVolume {
        pv("pv-node1a", "1", "a", None, "")
    }
    fn pv1a_bound() -> PersistentVolume {
        pv(
            "pv-node1a",
            "1",
            "a",
            Some("unbound-pvc"),
            "unbound-pvc-uid",
        )
    }
    fn provisioned() -> PersistentVolumeClaim {
        pvc("provisioned-pvc", "1", "", json!({}))
    }
    fn provisioned_ann() -> PersistentVolumeClaim {
        pvc("provisioned-pvc", "1", "", selected("node1"))
    }

    // ---- TestBindAPIUpdate ----

    #[tokio::test]
    async fn bind_api_update_nothing_to_bind() {
        let env = FakeEnv::new();
        let r = bind_api_update(&*env, &mut [], &mut []).await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[tokio::test]
    async fn bind_api_update_one_binding_writes_pv_and_saves_api_copy() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        let mut b = [binding(&unbound(), &pv1a_bound())];
        bind_api_update(&*env, &mut b, &mut []).await.unwrap();
        let api = env.pv_api_obj("pv-node1a").unwrap();
        assert_eq!(
            api.spec.claim_ref.as_ref().unwrap().name.as_deref(),
            Some("unbound-pvc")
        );
        // "Save updated object from apiserver for later checking."
        assert_eq!(b[0].pv.metadata.resource_version.as_deref(), Some("2"));
        assert!(env.reverted_pvs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bind_api_update_failure_reverts_only_unprocessed_pvs() {
        // "api-update-failed": second PV was modified in the API (higher version).
        let env = FakeEnv::new();
        let pv1b = pv("pv-node1b", "1", "a", None, "");
        env.put_pv(&pv1a());
        env.put_pv(&with_rv(&pv1b, "2"));
        let pv1b_bound = pv(
            "pv-node1b",
            "1",
            "a",
            Some("unbound-pvc2"),
            "unbound-pvc2-uid",
        );
        let pvc2 = pvc("unbound-pvc2", "1", "", json!({}));
        let mut b = [
            binding(&unbound(), &pv1a_bound()),
            binding(&pvc2, &pv1b_bound),
        ];
        let r = bind_api_update(&*env, &mut b, &mut []).await;
        assert!(r.is_err());
        assert_eq!(
            *env.reverted_pvs.lock().unwrap(),
            vec!["pv-node1b".to_string()]
        );
    }

    #[tokio::test]
    async fn bind_api_update_provision_updates_pvc() {
        let env = FakeEnv::new();
        env.put_pvc(&provisioned());
        let mut c = [provisioned_ann()];
        bind_api_update(&*env, &mut [], &mut c).await.unwrap();
        let api = env.pvc_api_obj("testns/provisioned-pvc").unwrap();
        assert_eq!(
            api.metadata
                .annotations
                .unwrap()
                .get("volume.kubernetes.io/selected-node")
                .map(String::as_str),
            Some("node1")
        );
        assert_eq!(c[0].metadata.resource_version.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn bind_api_update_provision_failure_reverts_unprocessed_pvcs_not_pvs() {
        // "binding-succeed, provision-api-update-failed"
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        env.put_pvc(&provisioned());
        let p2 = pvc("provisioned-pvc2", "1", "", json!({}));
        env.put_pvc(&with_rv(&p2, "2"));
        let p2_ann = pvc("provisioned-pvc2", "1", "", selected("node1"));
        let mut b = [binding(&unbound(), &pv1a_bound())];
        let mut c = [provisioned_ann(), p2_ann];
        let r = bind_api_update(&*env, &mut b, &mut c).await;
        assert!(r.is_err());
        assert!(env.reverted_pvs.lock().unwrap().is_empty());
        assert_eq!(
            *env.reverted_pvcs.lock().unwrap(),
            vec!["provisioned-pvc2".to_string()]
        );
    }

    // ---- TestCheckBindings ----

    async fn check(
        env: &FakeEnv,
        b: &[BindingInfo],
        c: &[PersistentVolumeClaim],
    ) -> Result<bool, String> {
        check_bindings(env, &pod("node1"), b, c).await
    }

    #[tokio::test]
    async fn check_bindings_nothing_to_bind_is_bound() {
        assert_eq!(check(&FakeEnv::new(), &[], &[]).await, Ok(true));
    }

    #[tokio::test]
    async fn check_bindings_binding_bound() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a_bound());
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        let b = [binding(&unbound(), &pv1a_bound())];
        assert_eq!(check(&env, &b, &[]).await, Ok(true));
    }

    #[tokio::test]
    async fn check_bindings_prebound_and_unbound_are_not_yet_bound() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a_bound());
        let b = [binding(&unbound(), &pv1a_bound())];
        // prebound: volumeName set, no bind-completed
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", json!({})));
        assert_eq!(check(&env, &b, &[]).await, Ok(false));
        env.put_pvc(&unbound());
        assert_eq!(check(&env, &b, &[]).await, Ok(false));
    }

    #[tokio::test]
    async fn check_bindings_missing_pvc_or_pv_fails() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a_bound());
        let b = [binding(&unbound(), &pv1a_bound())];
        let e = check(&env, &b, &[]).await.unwrap_err();
        assert!(e.starts_with("failed to check binding: "), "{e}");

        let env = FakeEnv::new();
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        assert!(check(&env, &b, &[]).await.is_err());
    }

    #[tokio::test]
    async fn check_bindings_claimref_reset_fails() {
        let env = FakeEnv::new();
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        // API PV (newer than the assumed one) lost its claimRef.
        env.put_pv(&with_rv(&pv1a(), "2"));
        let b = [binding(&unbound(), &pv1a_bound())];
        let e = check(&env, &b, &[]).await.unwrap_err();
        assert_eq!(e, "ClaimRef got reset for pv \"pv-node1a\"");

        // claimRef present but UID empty
        env.put_pv(&with_rv(
            &pv("pv-node1a", "1", "a", Some("unbound-pvc"), ""),
            "2",
        ));
        let e = check(&env, &b, &[]).await.unwrap_err();
        assert_eq!(e, "ClaimRef got reset for pv \"pv-node1a\"");
    }

    #[tokio::test]
    async fn check_bindings_older_api_object_waits() {
        // binding.pv has RV 2 (we updated it); the cache still serves RV 1.
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        let b = [binding(&unbound(), &with_rv(&pv1a_bound(), "2"))];
        assert_eq!(check(&env, &b, &[]).await, Ok(false));
    }

    #[tokio::test]
    async fn check_bindings_node_affinity_mismatch_fails() {
        let env = FakeEnv::new();
        env.nodes
            .lock()
            .unwrap()
            .insert("node1".into(), node("node1", json!({})));
        env.put_pv(&pv1a_bound());
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        let b = [binding(&unbound(), &pv1a_bound())];
        let e = check(&env, &b, &[]).await.unwrap_err();
        assert!(
            e.starts_with("pv \"pv-node1a\" node affinity doesn't match node \"node1\": "),
            "{e}"
        );
    }

    #[tokio::test]
    async fn check_bindings_pod_deleted_fails() {
        let env = FakeEnv::new();
        *env.pod_present.lock().unwrap() = false;
        let e = check(&env, &[], &[]).await.unwrap_err();
        assert!(e.starts_with("pod does not exist any more: "), "{e}");
    }

    #[tokio::test]
    async fn check_bindings_provisioning_states() {
        let c = [provisioned_ann()];
        // unbound but selected-node intact: keep waiting
        let env = FakeEnv::new();
        env.put_pvc(&provisioned_ann());
        assert_eq!(check(&env, &[], &c).await, Ok(false));

        // bound to a PV on the node: done
        let env = FakeEnv::new();
        env.put_pv(&pv("pv-bound", "1", "a", None, ""));
        let mut ann = selected("node1");
        ann["pv.kubernetes.io/bind-completed"] = json!("yes");
        env.put_pvc(&pvc("provisioned-pvc", "1", "pv-bound", ann.clone()));
        assert_eq!(check(&env, &[], &c).await, Ok(true));

        // bound, but the PV is not in the cache yet: tolerated
        let env = FakeEnv::new();
        env.put_pvc(&pvc("provisioned-pvc", "1", "pv-bound", ann));
        assert_eq!(check(&env, &[], &c).await, Ok(false));
    }

    #[tokio::test]
    async fn check_bindings_provisioning_failures() {
        let c = [provisioned_ann()];
        // pvc gone
        let e = check(&FakeEnv::new(), &[], &c).await.unwrap_err();
        assert!(e.starts_with("failed to check provisioning pvc: "), "{e}");

        // annotations nil
        let env = FakeEnv::new();
        env.put_pvc(&pvc("provisioned-pvc", "1", "", Value::Null));
        let e = check(&env, &[], &c).await.unwrap_err();
        assert_eq!(
            e,
            "selectedNode annotation reset for PVC \"provisioned-pvc\""
        );

        // selected-node dropped / wrong node => provisioner failed
        for ann in [json!({"x": "y"}), selected("wrong-node")] {
            let env = FakeEnv::new();
            env.put_pvc(&pvc("provisioned-pvc", "1", "", ann));
            let e = check(&env, &[], &c).await.unwrap_err();
            assert_eq!(e, "provisioning failed for PVC \"provisioned-pvc\"");
        }
    }

    // ---- TestBindPodVolumes ----

    fn vols(b: Vec<BindingInfo>, c: Vec<PersistentVolumeClaim>) -> PodVolumes {
        PodVolumes {
            static_bindings: b,
            dynamic_provisions: c
                .into_iter()
                .map(|pvc| DynamicProvision {
                    pvc,
                    node_capacity: None,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn bind_pod_volumes_nothing_to_bind() {
        let env = FakeEnv::new();
        let mut v = vols(vec![], vec![]);
        bind_pod_volumes(
            &*env,
            &pod("node1"),
            &mut v,
            Duration::from_millis(200),
            FAST,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bind_pod_volumes_already_bound() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a_bound());
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        let mut v = vols(vec![binding(&unbound(), &pv1a_bound())], vec![]);
        // Update bumps the PV to RV 2 and saves it; the check then sees it equal.
        bind_pod_volumes(
            &*env,
            &pod("node1"),
            &mut v,
            Duration::from_millis(500),
            FAST,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bind_pod_volumes_succeeds_after_pv_controller_binds() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        env.put_pvc(&unbound());
        let e2 = env.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            e2.put_pvc(&pvc("unbound-pvc", "2", "pv-node1a", bound_ann()));
        });
        let mut v = vols(vec![binding(&unbound(), &pv1a_bound())], vec![]);
        bind_pod_volumes(&*env, &pod("node1"), &mut v, Duration::from_secs(5), FAST)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn bind_pod_volumes_times_out() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        env.put_pvc(&unbound());
        let mut v = vols(vec![binding(&unbound(), &pv1a_bound())], vec![]);
        let e = bind_pod_volumes(
            &*env,
            &pod("node1"),
            &mut v,
            Duration::from_millis(150),
            FAST,
        )
        .await
        .unwrap_err();
        assert_eq!(e, "binding volumes: context deadline exceeded");
    }

    #[tokio::test]
    async fn bind_pod_volumes_api_conflict_fails_without_waiting() {
        // "bound-by-pv-controller-before-bind"
        let env = FakeEnv::new();
        env.put_pv(&with_rv(&pv1a_bound(), "2"));
        env.put_pvc(&pvc("unbound-pvc", "2", "pv-node1a", bound_ann()));
        let mut v = vols(vec![binding(&unbound(), &pv1a_bound())], vec![]);
        let e = bind_pod_volumes(&*env, &pod("node1"), &mut v, Duration::from_secs(30), FAST)
            .await
            .unwrap_err();
        assert!(!e.starts_with("binding volumes"), "{e}");
    }

    #[tokio::test]
    async fn bind_pod_volumes_pod_deleted_while_waiting() {
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        env.put_pvc(&unbound());
        let e2 = env.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(60)).await;
            *e2.pod_present.lock().unwrap() = false;
        });
        let mut v = vols(vec![binding(&unbound(), &pv1a_bound())], vec![]);
        let e = bind_pod_volumes(&*env, &pod("node1"), &mut v, Duration::from_secs(5), FAST)
            .await
            .unwrap_err();
        assert!(
            e.starts_with("binding volumes: pod does not exist any more"),
            "{e}"
        );
    }

    // ---- PreBindPreFlight / PreBind / Unreserve (volume_binding.go) ----

    #[test]
    fn preflight_skips_when_all_bound() {
        let s = VolumeBindingState {
            all_bound: true,
            ..Default::default()
        };
        assert_eq!(pre_bind_preflight(&s, "node1"), Ok(PreBindPreFlight::Skip));
    }

    #[test]
    fn preflight_errors_without_pod_volumes_for_node() {
        let s = VolumeBindingState::default();
        let e = pre_bind_preflight(&s, "node1").unwrap_err();
        assert_eq!(e, format!("{ERR_NO_POD_VOLUME_FOR_NODE} \"node1\""));
        let mut s = s;
        s.pod_volumes_by_node
            .insert("node1".into(), PodVolumes::default());
        assert_eq!(pre_bind_preflight(&s, "node1"), Ok(PreBindPreFlight::Run));
    }

    #[tokio::test]
    async fn pre_bind_noop_when_all_bound_and_errors_when_node_missing() {
        let env = FakeEnv::new();
        let mut s = VolumeBindingState {
            all_bound: true,
            ..Default::default()
        };
        pre_bind(
            &*env,
            &mut s,
            &pod(""),
            "node1",
            Duration::from_millis(50),
            FAST,
        )
        .await
        .unwrap();
        assert_eq!(*env.pv_updates.lock().unwrap(), 0);

        s.all_bound = false;
        let e = pre_bind(
            &*env,
            &mut s,
            &pod(""),
            "node1",
            Duration::from_millis(50),
            FAST,
        )
        .await
        .unwrap_err();
        assert!(e.starts_with(ERR_NO_POD_VOLUME_FOR_NODE), "{e}");
    }

    #[tokio::test]
    async fn pre_bind_binds_against_the_chosen_node_and_saves_updates_in_state() {
        // The pod handed to PreBind has no spec.nodeName yet; upstream runs it
        // on the assumed pod, so checkBindings must see nodeName == node1.
        let env = FakeEnv::new();
        env.put_pv(&pv1a());
        env.put_pvc(&pvc("unbound-pvc", "1", "pv-node1a", bound_ann()));
        let mut s = VolumeBindingState::default();
        s.pod_volumes_by_node.insert(
            "node1".into(),
            vols(vec![binding(&unbound(), &pv1a_bound())], vec![]),
        );
        pre_bind(
            &*env,
            &mut s,
            &pod(""),
            "node1",
            Duration::from_millis(500),
            FAST,
        )
        .await
        .unwrap();
        let saved = &s.pod_volumes_by_node["node1"].static_bindings[0].pv;
        assert_eq!(saved.metadata.resource_version.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn unreserve_reverts_node_volumes_and_is_idempotent_without_them() {
        let env = FakeEnv::new();
        let mut s = VolumeBindingState::default();
        unreserve(&*env, &s, "node1");
        assert!(env.reverted_pvs.lock().unwrap().is_empty());
        s.pod_volumes_by_node.insert(
            "node1".into(),
            vols(
                vec![binding(&unbound(), &pv1a_bound())],
                vec![provisioned_ann()],
            ),
        );
        unreserve(&*env, &s, "node1");
        assert_eq!(
            *env.reverted_pvs.lock().unwrap(),
            vec!["pv-node1a".to_string()]
        );
        assert_eq!(
            *env.reverted_pvcs.lock().unwrap(),
            vec!["provisioned-pvc".to_string()]
        );
    }
}
