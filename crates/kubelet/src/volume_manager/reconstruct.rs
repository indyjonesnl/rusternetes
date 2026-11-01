//! Port of `pkg/kubelet/volumemanager/reconciler/reconstruct.go` and
//! `reconstruct_common.go`: rebuild the actual state of the world after a
//! kubelet restart from the volume directories the previous kubelet left under
//! the pods directory.
//!
//! Every volume found on disk that the actual state does not know is added to
//! it as uncertainly attached and uncertainly mounted
//! (`CheckAndMarkVolumeAsUncertainViaReconstruction`), with its device marked
//! uncertain when the plugin is device-mountable. A volume a pod still wants
//! is then mounted again by the reconciler (`!volMounted`), one nobody wants is
//! torn down. A volume whose plugin cannot rebuild it goes to
//! `volumesFailedReconstruction` and is force-unmounted by `cleanOrphanVolumes`
//! once the desired state is populated and does not want it.
//!
//! Not ported yet: block volumes (`volumeDevices`, `FindMapperPluginByName`,
//! `NewBlockVolumeMapper`; the kubelet does not support them) and
//! `updateReconstructedFromNodeStatus` (the devicePath and attachability from
//! `node.status.volumesAttached`; `readyToUnmount` only matters once the
//! reconciler has an unmount arm to gate, #1970).

use crate::pod_dirs::PodVolume;
use crate::volume_manager::reconciler::Reconciler;
use crate::volume_plugins::plugin::{DeviceMounter, Mounter, OwnedSpec};
use crate::volume_plugins::util::operation_executor::{MarkVolumeOpts, VolumeMountState};
use crate::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use anyhow::{anyhow, Context, Result};
use rusternetes_common::resources::Pod;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Port of `reconstructedVolume` (`reconstruct_common.go:62-72`), minus the
/// block `blockVolumeMapper`.
struct ReconstructedVolume {
    volume_name: UniqueVolumeName,
    pod_name: UniquePodName,
    volume_spec: Arc<OwnedSpec>,
    device_path: String,
    mounter: Arc<dyn Mounter>,
    device_mounter: Option<Box<dyn DeviceMounter>>,
    selinux_mount_context: String,
}

/// Port of `globalVolumeInfo` (`reconstruct_common.go:99-107`): a volume and
/// every pod that was using it.
struct GlobalVolumeInfo {
    volume_name: UniqueVolumeName,
    volume_spec: Arc<OwnedSpec>,
    device_path: String,
    device_mounter: Option<Box<dyn DeviceMounter>>,
    pod_volumes: BTreeMap<UniquePodName, ReconstructedVolume>,
}

impl Reconciler {
    /// Port of `reconstructVolumes` (`reconstruct.go:49-100`): scan every pod's
    /// volume directories and add what the actual state does not know to it.
    pub async fn reconstruct_volumes(&self) {
        // `getVolumesFromPodDir` over the whole pods dir (`:191-240`).
        let root = self.kubelet_pods_dir();
        let pods = match crate::pod_dirs::list_pods_from_disk(root) {
            Ok(pods) => pods,
            Err(e) => {
                error!(
                    "Cannot get volumes from disk, skip sync states for volume reconstruction: {e}"
                );
                return;
            }
        };
        let mut pod_volumes: Vec<PodVolume> = Vec::new();
        for uid in pods {
            match crate::pod_dirs::get_volumes_from_pod_dir(root, &uid) {
                Ok(volumes) => pod_volumes.extend(volumes),
                Err(e) => {
                    error!("Cannot get volumes from disk, skip sync states for volume reconstruction: {e}");
                    return;
                }
            }
        }

        let mut reconstructed_volumes: BTreeMap<UniqueVolumeName, GlobalVolumeInfo> =
            BTreeMap::new();
        for volume in pod_volumes {
            let pod_name = UniquePodName(volume.pod_uid.clone());
            if self
                .actual_state_of_world()
                .volume_exists_with_spec_name(&pod_name, &volume.volume_spec_name)
            {
                debug!(
                    "Volume exists in actual state, skip cleaning up mounts: pod {pod_name} volume {}",
                    volume.volume_spec_name
                );
                // There is nothing to reconstruct
                continue;
            }
            let mut reconstructed = match self.reconstruct_volume(&volume).await {
                Ok(r) => r,
                Err(e) => {
                    info!(
                        "Could not construct volume information: pod {pod_name} volume {}: {e:#}",
                        volume.volume_spec_name
                    );
                    // We can't reconstruct the volume. Remember to check DSW
                    // after it's fully populated and force unmount the volume
                    // when it's orphaned.
                    self.volumes_failed_reconstruction
                        .lock()
                        .expect("volumes failed reconstruction lock")
                        .push(volume);
                    continue;
                }
            };
            debug!(
                "Adding reconstructed volume to actual state: pod {pod_name} volume {}",
                volume.volume_spec_name
            );
            // A second pod reusing the volume joins the first one's entry
            // (`if cachedInfo, ok := reconstructedVolumes[...]`).
            let gvl = reconstructed_volumes
                .entry(reconstructed.volume_name.clone())
                .or_insert_with(|| GlobalVolumeInfo {
                    volume_name: reconstructed.volume_name.clone(),
                    volume_spec: reconstructed.volume_spec.clone(),
                    device_path: reconstructed.device_path.clone(),
                    device_mounter: None,
                    pod_volumes: BTreeMap::new(),
                });
            if gvl.device_mounter.is_none() {
                // The device mounter is the plugin's, not the pod's: keep
                // the first one built for the volume.
                gvl.device_mounter = reconstructed.device_mounter.take();
            }
            gvl.pod_volumes
                .insert(reconstructed.pod_name.clone(), reconstructed);
        }

        if !reconstructed_volumes.is_empty() {
            // Add the volumes to ASW
            self.update_states(reconstructed_volumes);
        }
        self.mounter().reconstruct_orphaned_devices();
        debug!("Volume reconstruction finished");
    }

    /// Port of `reconstructVolume` (`reconstruct_common.go:242-420`) for a
    /// filesystem volume.
    async fn reconstruct_volume(&self, volume: &PodVolume) -> Result<ReconstructedVolume> {
        let plugin_mgr = self.plugin_mgr();
        // plugin initializations
        let plugin = plugin_mgr
            .find_plugin_by_name(&volume.plugin_name)
            .map_err(|e| anyhow!("{e}"))?;
        let pod_name = UniquePodName(volume.pod_uid.clone());

        // Create pod object
        let mut pod = Pod::new("", Default::default());
        pod.spec = None;
        pod.metadata.uid = volume.pod_uid.clone();

        let reconstructed = plugin
            .construct_volume_spec(
                &volume.volume_spec_name,
                &volume.volume_path.to_string_lossy(),
            )
            .with_context(|| {
                format!(
                    "ReconstructVolumeOperation failed for plugin {:?} (spec.Name: {:?}) pod {:?}",
                    volume.plugin_name, volume.volume_spec_name, volume.pod_uid
                )
            })?;
        let Some(v1_volume) = reconstructed.volume else {
            return Err(anyhow!(
                "failed to reconstruct volume for plugin {:?} (spec.Name: {:?}) pod {:?} (UID: {:?}): got nil",
                volume.plugin_name,
                volume.volume_spec_name,
                pod_name,
                pod.metadata.uid
            ));
        };
        let volume_spec = Arc::new(OwnedSpec {
            volume: v1_volume,
            persistent_volume: reconstructed.persistent_volume,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        });
        let spec = volume_spec.as_spec();

        // We have to find the plugins by volume spec (NOT by plugin name) here
        // in order to correctly reconstruct ephemeral volume types.
        let device_mountable_plugin = plugin_mgr.find_device_mountable_plugin_by_spec(&spec);

        // The unique volume name used depends on whether the volume is
        // attachable/device-mountable (needsNameFromSpec = true) or not.
        // Attach-ability is checked only as a fallback, as upstream: all CSI
        // volumes are device-mountable and never reach it.
        let needs_name_from_spec = device_mountable_plugin.is_some()
            || plugin_mgr.find_attachable_plugin_by_spec(&spec).is_some();
        let volume_name = if needs_name_from_spec {
            crate::volume_plugins::util::get_unique_volume_name_from_spec(plugin, &spec)
                .map_err(|e| anyhow!("{e}"))?
        } else {
            crate::volume_plugins::util::get_unique_volume_name_from_spec_with_pod(
                &pod_name, plugin, &spec,
            )
        };

        let mounter: Arc<dyn Mounter> = Arc::from(
            plugin.new_mounter(&spec, &pod).await.map_err(|e| {
                anyhow!(
                    "reconstructVolume.NewMounter failed for volume {volume_name:?} (spec.Name: {:?}) pod {pod_name:?} (UID: {:?}) with: {e:#}",
                    volume_spec.name(),
                    pod.metadata.uid
                )
            })?,
        );
        let device_mounter = match device_mountable_plugin.and_then(|p| p.as_device_mountable_plugin())
        {
            Some(p) => Some(p.new_device_mounter().map_err(|e| {
                anyhow!(
                    "reconstructVolume.NewDeviceMounter failed for volume {volume_name:?} (spec.Name: {:?}) pod {pod_name:?} (UID: {:?}) with: {e:#}",
                    volume_spec.name(),
                    pod.metadata.uid
                )
            })?),
            None => None,
        };

        Ok(ReconstructedVolume {
            volume_name,
            pod_name,
            volume_spec,
            // devicePath is updated from node status's VolumesAttached data
            // upstream; not ported yet (module comment).
            device_path: String::new(),
            mounter,
            device_mounter,
            selinux_mount_context: reconstructed.selinux_mount_context,
        })
    }

    /// Port of `updateStates` (`reconstruct.go:102-157`).
    fn update_states(&self, reconstructed_volumes: BTreeMap<UniqueVolumeName, GlobalVolumeInfo>) {
        let asw = self.actual_state_of_world();
        for gvl in reconstructed_volumes.into_values() {
            if let Err(e) = asw.add_attach_uncertain_reconstructed_volume(
                Some(&gvl.volume_name),
                gvl.volume_spec.clone(),
                self.node_name(),
                &gvl.device_path,
            ) {
                error!(
                    "Could not add volume information to actual state of world: volume {}: {e}",
                    gvl.volume_name
                );
                continue;
            }
            let mut selinux_mount_context = String::new();
            for volume in gvl.pod_volumes.values() {
                let opts = MarkVolumeOpts {
                    pod_name: volume.pod_name.clone(),
                    pod_uid: volume.pod_name.0.clone(),
                    volume_name: volume.volume_name.clone(),
                    mounter: Some(volume.mounter.clone()),
                    block_volume_mapper: None,
                    volume_gid_volume: String::new(),
                    volume_spec: volume.volume_spec.clone(),
                    volume_mount_state: VolumeMountState::VolumeMountUncertain,
                    selinux_mount_context: volume.selinux_mount_context.clone(),
                };
                if let Err(e) = asw.check_and_mark_volume_as_uncertain_via_reconstruction(opts) {
                    error!(
                        "Could not add pod to volume information to actual state of world: pod {}: {e}",
                        volume.pod_name
                    );
                    continue;
                }
                selinux_mount_context = volume.selinux_mount_context.clone();
                debug!(
                    "Volume is marked as uncertain and added into the actual state: pod {} volume {}",
                    volume.pod_name, volume.volume_name
                );
            }
            // If the volume has device to mount, we mark its device as uncertain.
            if let Some(device_mounter) = gvl.device_mounter.as_ref() {
                let device_mount_path =
                    match device_mounter.get_device_mount_path(&gvl.volume_spec.as_spec()) {
                        Ok(p) => p,
                        Err(e) => {
                            error!(
                                "Could not find device mount path for volume {}: {e:#}",
                                gvl.volume_name
                            );
                            continue;
                        }
                    };
                if let Err(e) = asw.mark_device_as_uncertain(
                    &gvl.volume_name,
                    &gvl.device_path,
                    &device_mount_path,
                    &selinux_mount_context,
                ) {
                    error!(
                        "Could not mark device is uncertain to actual state of world: volume {} deviceMountPath {device_mount_path}: {e}",
                        gvl.volume_name
                    );
                    continue;
                }
                debug!(
                    "Volume is marked device as uncertain and added into the actual state: volume {} deviceMountPath {device_mount_path}",
                    gvl.volume_name
                );
            }
        }
    }

    /// `volumesFailedReconstruction`: the volumes found on disk that could not
    /// be reconstructed and are still waiting for `cleanOrphanVolumes`.
    pub fn volumes_failed_reconstruction(&self) -> Vec<PodVolume> {
        self.volumes_failed_reconstruction
            .lock()
            .expect("volumes failed reconstruction lock")
            .clone()
    }

    /// Replaces `volumesFailedReconstruction`, as upstream's tests assign the
    /// field.
    #[doc(hidden)]
    pub fn set_volumes_failed_reconstruction(&self, volumes: Vec<PodVolume>) {
        *self
            .volumes_failed_reconstruction
            .lock()
            .expect("volumes failed reconstruction lock") = volumes;
    }

    /// Port of `cleanOrphanVolumes` (`reconstruct.go:159-178`): force-unmount
    /// every volume that failed reconstruction and no pod wants. One shot: the
    /// list is emptied.
    pub async fn clean_orphan_volumes(&self) {
        let failed = std::mem::take(
            &mut *self
                .volumes_failed_reconstruction
                .lock()
                .expect("volumes failed reconstruction lock"),
        );
        if failed.is_empty() {
            return;
        }
        for volume in failed {
            let pod_name = UniquePodName(volume.pod_uid.clone());
            if self
                .desired_state_of_world()
                .volume_exists_with_spec_name(&pod_name, &volume.volume_spec_name)
            {
                // Some pod needs the volume, don't clean it up and hope that
                // reconcile() calls SetUp and reconstructs the volume in ASW.
                debug!(
                    "Volume exists in desired state, skip cleaning up mounts: pod {pod_name} volume {}",
                    volume.volume_spec_name
                );
                continue;
            }
            info!(
                "Cleaning up mounts for volume that could not be reconstructed: pod {pod_name} volume {}",
                volume.volume_spec_name
            );
            self.cleanup_mounts(&volume).await;
        }
        debug!("Orphan volume cleanup finished");
    }

    /// Port of `cleanupMounts` (`reconstruct_common.go:154-173`): the
    /// `UnmountVolume` operation on a volume the actual state never knew,
    /// i.e. `NewUnmounter` + `TearDown`.
    async fn cleanup_mounts(&self, volume: &PodVolume) {
        let plugin = match self.plugin_mgr().find_plugin_by_name(&volume.plugin_name) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    "UnmountVolume failed for volume {}: no plugin {:?}: {e}",
                    volume.volume_spec_name, volume.plugin_name
                );
                return;
            }
        };
        let result = async {
            plugin
                .new_unmounter(&volume.volume_spec_name, &volume.pod_uid)?
                .tear_down()
                .await
        }
        .await;
        if let Err(e) = result {
            warn!(
                "volumeHandler.UnmountVolumeHandler for UnmountVolume failed: pod {} volume {}: {e:#}",
                volume.pod_uid, volume.volume_spec_name
            );
        }
    }
}
