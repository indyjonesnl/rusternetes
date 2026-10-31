//! Port of `staging/src/k8s.io/component-helpers/storage/ephemeral/ephemeral.go`
//! (`VolumeClaimName`, `VolumeIsForPod`), shared by the ephemeral volume
//! controller and the kubelet's desired-state populator.

use crate::resources::volume::PersistentVolumeClaim;
use crate::resources::{Pod, Volume};
use anyhow::{anyhow, Result};

/// `ephemeral.VolumeClaimName`.
pub fn volume_claim_name(pod: &Pod, volume: &Volume) -> String {
    format!("{}-{}", pod.metadata.name, volume.name)
}

/// `ephemeral.VolumeIsForPod`: the claim must be in the pod's namespace and
/// controlled by the pod (`metav1.IsControlledBy`: controller ref with the
/// pod's UID).
pub fn volume_is_for_pod(pod: &Pod, pvc: &PersistentVolumeClaim) -> Result<()> {
    let controlled = pvc
        .metadata
        .owner_references
        .iter()
        .flatten()
        .any(|o| o.controller == Some(true) && o.uid == pod.metadata.uid);
    if pvc.metadata.namespace != pod.metadata.namespace || !controlled {
        return Err(anyhow!(
            "PVC {}/{} was not created for pod {}/{} (pod is not owner)",
            pvc.metadata.namespace.as_deref().unwrap_or(""),
            pvc.metadata.name,
            pod.metadata.namespace.as_deref().unwrap_or(""),
            pod.metadata.name
        ));
    }
    Ok(())
}
