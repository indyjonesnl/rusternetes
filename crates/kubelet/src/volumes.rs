//! Runtime-agnostic volume provisioning.
//!
//! [`VolumeManager`] owns the logic that materialises pod volumes on the host
//! filesystem (configMap / secret / projected / downwardAPI / emptyDir /
//! hostPath) plus the periodic resync/refresh of mutable sources. It is
//! deliberately free of any container-runtime coupling so that the CRI runtime
//! ([`crate::cri_runtime::CriContainerRuntime`]) can reuse it; behavior is
//! identical to when this code lived inline in `runtime.rs`.

use anyhow::{Context, Result};
use rusternetes_common::resources::{
    ConfigMap, KeyToPath, PersistentVolume, PersistentVolumeClaim, Pod,
};
use rusternetes_storage::{build_key, Storage};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

// Free helpers that stay in `runtime.rs` (they are not runtime-specific but are
// shared with non-volume code paths there). Imported so the moved bodies keep
// calling them by their bare names, verbatim.
use crate::atomic_writer::FileProjection;
use crate::volume_plugins::VolumePlugin;

/// Build the projection payload (relative user-visible path -> bytes) for a
/// ConfigMap volume, honoring `items` (specific keys → mapped paths) or, when
/// absent, every key from `data` + `binaryData`. This is the SINGLE source of
/// truth for what a ConfigMap volume should contain, shared by the initial
/// mount and every re-projection so they all feed the same bytes to the
/// AtomicWriter (and therefore no-op identically when unchanged).
pub fn build_configmap_payload(
    configmap: &ConfigMap,
    items: Option<&Vec<KeyToPath>>,
    _configmap_name: &str,
    is_optional: bool,
    default_mode: u32,
) -> Result<std::collections::BTreeMap<String, FileProjection>> {
    let mut payload: std::collections::BTreeMap<String, FileProjection> =
        std::collections::BTreeMap::new();
    // `if len(mappings) == 0` (`pkg/volume/configmap/configmap.go:207`): an
    // empty `items` list projects every key, same as an absent one.
    if let Some(items) = items.filter(|i| !i.is_empty()) {
        for item in items {
            // `items[].mode` wins over the volume `defaultMode` for this key
            // alone — upstream carries it per file in `FileProjection.Mode`
            // (`pkg/volume/util/atomic_writer.go:64-68`), which is what the
            // "with mappings and Item mode set" Conformance spec asserts.
            let mode = item.mode.map(|m| m as u32).unwrap_or(default_mode);
            if let Some(v) = configmap.data.as_ref().and_then(|d| d.get(&item.key)) {
                payload.insert(
                    item.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone().into_bytes(),
                        mode,
                    },
                );
            } else if let Some(v) = configmap
                .binary_data
                .as_ref()
                .and_then(|d| d.get(&item.key))
            {
                payload.insert(
                    item.path.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone(),
                        mode,
                    },
                );
            } else if !is_optional {
                // `MakePayload` (`configmap.go:222-229`): a mapped key absent
                // from data and binaryData fails the mount unless optional.
                anyhow::bail!("configmap references non-existent config key: {}", item.key);
            }
        }
    } else {
        if let Some(data) = &configmap.data {
            for (k, v) in data {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone().into_bytes(),
                        mode: default_mode,
                    },
                );
            }
        }
        if let Some(bin) = &configmap.binary_data {
            for (k, v) in bin {
                payload.insert(
                    k.clone(),
                    FileProjection {
                        fs_user: None,
                        data: v.clone(),
                        mode: default_mode,
                    },
                );
            }
        }
    }
    Ok(payload)
}

/// PV annotation carrying a supplemental GID granted to pods that mount the
/// volume. Upstream `pkg/volume/util/util.go` `VolumeGidAnnotationKey`.
const VOLUME_GID_ANNOTATION_KEY: &str = "pv.beta.kubernetes.io/gid";

/// Provisions and maintains pod volumes on the host filesystem, independent of
/// any container runtime. See the module docs.
#[derive(Clone)]
pub struct VolumeManager {
    pub volumes_base_path: String,
    pub storage: Option<Arc<rusternetes_storage::StorageBackend>>,
    pub token_manager: rusternetes_common::auth::TokenManager,
    /// The node's `status.allocatable`, used to default an unset downwardAPI
    /// `limits.*` the way upstream `defaultPodLimitsForDownwardAPI`
    /// (`pkg/kubelet/kubelet_resources.go:43-47`) does. Seeded from the same
    /// [`crate::kubelet::node_allocatable_map`] the kubelet posts in NodeStatus,
    /// so a volume file and the NodeStatus can never disagree.
    pub node_allocatable: std::collections::HashMap<String, String>,
    /// The plugin registry. Built in `new` from the same host every plugin
    /// receives, so a plugin's directory can never disagree with the
    /// manager's. `Arc`-wrapped (rather than the bare `VolumePluginMgr` Task 3's
    /// brief showed) because `VolumeManager` is `#[derive(Clone)]` —
    /// `CriContainerRuntime` clones it per attach — and `VolumePluginMgr`'s
    /// `Vec<Box<dyn VolumePlugin>>` cannot itself derive `Clone`.
    pub plugin_mgr: Arc<crate::volume_plugins::VolumePluginMgr>,
    /// A CSI plugin over the same host, for the CSI-specific device methods
    /// (`unmount_device`, `list_staged_devices`) that are not part of
    /// `VolumePlugin`. `plugin_mgr` holds the plugin as a `dyn VolumePlugin`.
    pub csi_plugin: Arc<crate::volume_plugins::csi::CsiPlugin>,
    /// `operationExecutor.pendingOperations`
    /// (`NewNestedPendingOperations(true /* exponentialBackOffOnError */)`,
    /// `operation_executor.go:159`): what `IsOperationPending` and
    /// `UnmountDevice`'s backoff consult. Shared by every clone of the manager.
    pub pending_operations:
        crate::volume_plugins::util::nested_pending_operations::NestedPendingOperations,
    /// The mounts in flight, so the sweeps that detach an unmounted volume
    /// leave one alone while a pod is mounting it (`IsOperationPending`,
    /// `reconciler_common.go:275-277`).
    ///
    /// Not upstream's `pendingOperations` itself (that one also carries unmount
    /// and device operations), but the same mechanism: `MountVolume` is an
    /// operation on it, which backs failures off exponentially
    /// (`NewNestedPendingOperations(true)`). It does so once a volume manager is
    /// attached ([`Self::set_volume_manager`]), so the reconciler's 100ms loop
    /// does not hammer a mount that keeps failing; the inline mount, retried
    /// by the kubelet's own sync loop, never backs off.
    /// `blkUtil` of the operation generator: the host side of block-volume
    /// mapping (`volumepathhandler.NewBlockVolumePathHandler()`).
    pub block_util: Arc<dyn crate::volume_plugins::util::block_operations::BlockVolumePathHandler>,
    pub mount_operations:
        crate::volume_plugins::util::nested_pending_operations::NestedPendingOperations,
    /// An iscsi plugin over the same host, for the global-mount enumeration
    /// that stands in for reconstruction (`IscsiPlugin::list_global_mounts`).
    pub iscsi_plugin: Arc<crate::volume_plugins::iscsi::IscsiPlugin>,
    /// `csiMigratedPluginManager` of `desiredStateOfWorldPopulator`
    /// (`desired_state_of_world_populator.go:131`).
    pub csi_migrated_plugin_manager: rusternetes_common::csi_migration::PluginManager,
    /// `intreeToCSITranslator` of `desiredStateOfWorldPopulator` (`:132`).
    pub intree_to_csi_translator: rusternetes_common::csi_translation::CsiTranslator,
    /// The kubelet's one `ActualStateOfWorld` (`volumeManager.actualStateOfWorld`,
    /// `pkg/kubelet/volumemanager/volume_manager.go`), as far as the CSI
    /// device-unmount sweep uses it. Staged devices are reconstructed into it
    /// (`reconciler/reconstruct.go`) and it outlives a sweep, so a device left
    /// `DeviceMountUncertain` by a failed `UnmountDevice` is still there to be
    /// retried. `Arc`-shared because the manager is `Clone`d per runtime attach.
    pub asw: Arc<crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld>,
    /// `nodeName` of `NewVolumeManager` (`volume_manager.go:186`): the node the
    /// kubelet runs for. Empty until [`VolumeManager::with_node_name`].
    pub node_name: String,
    /// The cell behind `VolumeHost::get_node_name`, shared with the host the
    /// plugins were built with.
    node_name_cell: Arc<std::sync::RwLock<String>>,
    /// Each mounted pod volume's `Mounter.GetAttributes()` result plus the
    /// `SELinuxLabeled` bit, keyed by (pod UID, volume name). Stands in for the
    /// `VolumeInfo{Mounter, SELinuxLabeled}` map upstream's `makeMounts` reads
    /// (`pkg/kubelet/container/runtime.go` `VolumeInfo`;
    /// `kubelet_pods.go:296`, `:392`). `Arc`-shared because the manager is
    /// `Clone`d per runtime attach and every clone must see one record.
    pub mounted: Arc<std::sync::Mutex<HashMap<(String, String), MountedVolume>>>,
    /// The kubelet's pod certificate manager, the same one the volume host
    /// hands to the projected plugin (`kubeletVolumeHost.podCertificateManager`,
    /// `pkg/kubelet/volume_host.go:86`). Resync re-asks it for the bundle of
    /// each `podCertificate` source so a rotated credential reaches the mount.
    pub pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
    /// The volume manager whose reconciler mounts the pod's volumes
    /// (`kubelet.volumeManager`). Set once by [`Self::set_volume_manager`];
    /// `Weak` because the volume manager holds this manager (the spec factory
    /// and mounter its populator and reconciler use). Shared by every clone.
    volume_manager: Arc<std::sync::OnceLock<std::sync::Weak<crate::volume_manager::VolumeManager>>>,
}

/// `kubecontainer.VolumeInfo` as far as `makeMounts` reads it.
#[derive(Clone, Copy, Debug, Default)]
pub struct MountedVolume {
    pub attributes: crate::volume_plugins::plugin::Attributes,
    pub selinux_labeled: bool,
}

/// `pvcSource.ReadOnly` as `createVolumeSpec` passes it to
/// `NewSpecFromPersistentVolume(pv, pvcReadOnly)`
/// (`pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:461`,
/// `:588`). An `ephemeral` volume carries no such flag (`:432-441` builds the
/// claim source from the generated name alone), so it is false.
pub fn pvc_read_only(volume: &rusternetes_common::resources::Volume) -> bool {
    volume
        .persistent_volume_claim
        .as_ref()
        .and_then(|c| c.read_only)
        .unwrap_or(false)
}

/// `failed to get Plugin from volumeSpec for volume %q err=%v` wrapping
/// `ErrNoPluginMatched` (`desired_state_of_world.go:272-277`,
/// `plugins.go:72`). `lifecycle::is_volume_wait_error` classifies it as a wait
/// so the pod stays in ContainerCreating.
#[derive(Debug)]
pub struct NoPluginForVolume(pub String);

impl std::fmt::Display for NoPluginForVolume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to get Plugin from volumeSpec for volume {:?} err=no volume plugin matched",
            self.0
        )
    }
}

impl std::error::Error for NoPluginForVolume {}

/// `util.GetPodVolumeNames` (`pkg/volume/util/util.go:507-537`): a volume is
/// used when any container (init, regular, ephemeral) mounts or maps it.
fn pod_uses_volume(pod: &Pod, name: &str) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    spec.containers
        .iter()
        .chain(spec.init_containers.iter().flatten())
        .any(|c| {
            c.volume_mounts.iter().flatten().any(|v| v.name == name)
                || c.volume_devices.iter().flatten().any(|v| v.name == name)
        })
        || spec.ephemeral_containers.iter().flatten().any(|c| {
            c.volume_mounts.iter().flatten().any(|v| v.name == name)
                || c.volume_devices.iter().flatten().any(|v| v.name == name)
        })
}

impl VolumeManager {
    /// Construct a `VolumeManager` from the same three values the bollard
    /// `ContainerRuntime` already carries.
    pub fn new(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
    ) -> Self {
        Self::new_with_pod_certificate_manager(volumes_base_path, storage, token_manager, None)
    }

    /// [`VolumeManager::new`] plus the kubelet's pod certificate manager, which
    /// the volume host hands to the projected `podCertificate` source
    /// (`kubeletVolumeHost.podCertificateManager`, `pkg/kubelet/volume_host.go:86`).
    pub fn new_with_pod_certificate_manager(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
        pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
    ) -> Self {
        Self::new_inner(
            volumes_base_path,
            storage,
            token_manager,
            pod_certificate_manager,
            None,
        )
    }

    /// [`VolumeManager::new`] with the emptyDir mounter replaced by a recording
    /// fake (upstream's `FakeVolumeHost` injects `mount.NewFakeMounter`), for
    /// integration tests that run unprivileged and so cannot mount a tmpfs.
    #[doc(hidden)]
    pub fn new_with_recording_empty_dir_mounts(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
    ) -> Self {
        Self::new_inner(
            volumes_base_path,
            storage,
            token_manager,
            None,
            Some(crate::volume_plugins::empty_dir::fake_mount_deps().0),
        )
    }

    /// [`VolumeManager::new_with_pod_certificate_manager`] with the emptyDir
    /// mounter replaced by a recording fake, as
    /// [`VolumeManager::new_with_recording_empty_dir_mounts`] does.
    #[doc(hidden)]
    pub fn new_with_pod_certificate_manager_and_recording_empty_dir_mounts(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
        pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
    ) -> Self {
        Self::new_inner(
            volumes_base_path,
            storage,
            token_manager,
            pod_certificate_manager,
            Some(crate::volume_plugins::empty_dir::fake_mount_deps().0),
        )
    }

    fn new_inner(
        volumes_base_path: String,
        storage: Option<Arc<rusternetes_storage::StorageBackend>>,
        token_manager: rusternetes_common::auth::TokenManager,
        pod_certificate_manager: Option<Arc<dyn crate::podcertificate::Manager>>,
        empty_dir_mount_deps: Option<crate::volume_plugins::empty_dir::EmptyDirMountDeps>,
    ) -> Self {
        let node_allocatable = crate::kubelet::node_allocatable_map();
        let node_name_cell: Arc<std::sync::RwLock<String>> = Arc::default();
        let mut kubelet_host = crate::volume_plugins::KubeletVolumeHost::new(
            volumes_base_path.clone(),
            storage.clone(),
            token_manager.clone(),
            node_allocatable.clone(),
        )
        .with_node_name_cell(node_name_cell.clone());
        if let Some(m) = pod_certificate_manager.clone() {
            kubelet_host = kubelet_host.with_pod_certificate_manager(m);
        }
        if let Some(deps) = empty_dir_mount_deps {
            kubelet_host = kubelet_host.with_empty_dir_mount_deps(deps);
        }
        // The CSIDriver informer the CSI plugin's lister reads
        // (`pkg/kubelet/kubelet.go` builds it and hands it to the volume host).
        // Only a kubelet with an API to read from and a runtime to run it on
        // has one; otherwise the plugin sees upstream's nil lister.
        if let (Some(storage), Ok(rt)) = (storage.clone(), tokio::runtime::Handle::try_current()) {
            let lister = crate::volume_plugins::csi_driver_lister::CsiDriverLister::new();
            let l = lister.clone();
            rt.spawn(async move { l.run(storage).await });
            kubelet_host = kubelet_host.with_csi_driver_lister(lister);
        }
        let host: Arc<dyn crate::volume_plugins::VolumeHost> = Arc::new(kubelet_host);
        let plugin_mgr = Arc::new(crate::volume_plugins::VolumePluginMgr::new(vec![
            Box::new(crate::volume_plugins::empty_dir::EmptyDirPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::host_path::HostPathPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::local::LocalPlugin::new(host.clone())),
            // `nfs.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`).
            Box::new(crate::volume_plugins::nfs::NfsPlugin::new(host.clone())),
            // `git_repo.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go:61`).
            Box::new(crate::volume_plugins::git_repo::GitRepoPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::iscsi::IscsiPlugin::new(host.clone())),
            // `fc.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`).
            Box::new(crate::volume_plugins::fc::FcPlugin::new(host.clone())),
            // `image.ProbeVolumePlugins` (`cmd/kubelet/app/plugins.go`).
            Box::new(crate::volume_plugins::image::ImagePlugin::new()),
            Box::new(crate::volume_plugins::config_map::ConfigMapPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::csi::CsiPlugin::new(host.clone())),
            Box::new(crate::volume_plugins::secret::SecretPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::downward_api::DownwardApiPlugin::new(
                host.clone(),
            )),
            Box::new(crate::volume_plugins::projected::ProjectedPlugin::new(
                host.clone(),
            )),
        ]));
        Self {
            volumes_base_path,
            storage,
            token_manager,
            node_allocatable,
            iscsi_plugin: Arc::new(crate::volume_plugins::iscsi::IscsiPlugin::new(host.clone())),
            csi_plugin: Arc::new(crate::volume_plugins::csi::CsiPlugin::new(host)),
            pending_operations:
                crate::volume_plugins::util::nested_pending_operations::NestedPendingOperations::new(
                    true,
                ),
            block_util: Arc::new(crate::volume_plugins::util::volumepathhandler::VolumePathHandler),
            mount_operations:
                crate::volume_plugins::util::nested_pending_operations::NestedPendingOperations::new(
                    false,
                ),
            csi_migrated_plugin_manager: Default::default(),
            intree_to_csi_translator: rusternetes_common::csi_translation::CsiTranslator::new(),
            // `NewActualStateOfWorld(nodeName, volumePluginMgr)`; rebuilt with
            // the real node name by [`VolumeManager::with_node_name`].
            asw: Arc::new(
                crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld::new(
                    "",
                    Arc::clone(&plugin_mgr),
                ),
            ),
            plugin_mgr,
            node_name: String::new(),
            node_name_cell,
            mounted: Arc::default(),
            pod_certificate_manager,
            volume_manager: Arc::default(),
        }
    }

    /// Hand over the volume manager whose reconciler mounts pod volumes.
    /// From then on [`Self::create_pod_volumes`] is `WaitForAttachAndMount`
    /// (`kubelet.go:2204`): it waits for the reconciler instead of mounting
    /// inline.
    pub fn set_volume_manager(&self, volume_manager: &Arc<crate::volume_manager::VolumeManager>) {
        let _ = self.volume_manager.set(Arc::downgrade(volume_manager));
        self.mount_operations
            .inner
            .lock()
            .unwrap()
            .exponential_back_off_on_error = true;
    }

    /// Replace `blkUtil`, for tests that cannot bind-mount devices.
    #[doc(hidden)]
    pub fn with_block_util(
        mut self,
        block_util: Arc<dyn crate::volume_plugins::util::block_operations::BlockVolumePathHandler>,
    ) -> Self {
        self.block_util = block_util;
        self
    }

    /// Give the manager the kubelet's node name, the `nodeName` argument of
    /// `NewVolumeManager` (`volume_manager.go:186`) that it hands to
    /// `cache.NewActualStateOfWorld(nodeName, volumePluginMgr)` (`:202`).
    /// Builder-time only: the world is rebuilt (it is still empty), so call
    /// this before the manager is cloned or used.
    pub fn with_node_name(mut self, node_name: impl Into<String>) -> Self {
        self.node_name = node_name.into();
        *self
            .node_name_cell
            .write()
            .unwrap_or_else(|e| e.into_inner()) = self.node_name.clone();
        self.asw = Arc::new(
            crate::volume_manager::cache::actual_state_of_world::ActualStateOfWorld::new(
                self.node_name.clone(),
                Arc::clone(&self.plugin_mgr),
            ),
        );
        self
    }

    /// The migration tail of `createVolumeSpec`
    /// (`desired_state_of_world_populator.go:475-482` for a claim,
    /// `:511-518` for an inline volume):
    ///
    /// ```go
    /// migratable, err := dswp.csiMigratedPluginManager.IsMigratable(volumeSpec)
    /// if err != nil { return nil, nil, "", err }
    /// if migratable {
    ///     volumeSpec, err = csimigration.TranslateInTreeSpecToCSI(logger, volumeSpec, pod.Namespace, dswp.intreeToCSITranslator)
    ///     if err != nil { return nil, nil, "", err }
    /// }
    /// ```
    ///
    /// `pv` is the bound PV of a claim (`NewSpecFromPersistentVolume`, whose
    /// `Spec.Volume` is nil); without one the spec is `NewSpecFromVolume`.
    ///
    /// **Deviation, inherited from [`Spec`]:** the translated upstream spec has
    /// `Volume == nil`; ours keeps the pod's `Volume` (mandatory in `Spec`) next
    /// to the translated CSI PV. The migrated plugin still sees exactly one
    /// source, the PV's `csi`, because the retained in-tree volume carries none.
    pub fn create_volume_spec(
        &self,
        pod_namespace: &str,
        volume: &rusternetes_common::resources::Volume,
        pv: Option<&PersistentVolume>,
        read_only: bool,
    ) -> Result<crate::volume_plugins::OwnedSpec> {
        use rusternetes_common::csi_migration::{translate_in_tree_spec_to_csi, VolumeSpec};
        let spec = VolumeSpec {
            volume: pv.is_none().then(|| volume.clone()),
            persistent_volume: pv.cloned(),
            read_only,
            ..Default::default()
        };
        let migratable = self
            .csi_migrated_plugin_manager
            .is_migratable(Some(&spec))
            .map_err(anyhow::Error::msg)?;
        if !migratable {
            return Ok(crate::volume_plugins::OwnedSpec {
                volume: volume.clone(),
                persistent_volume: pv.cloned(),
                read_only,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            });
        }
        let translated =
            translate_in_tree_spec_to_csi(&spec, pod_namespace, &self.intree_to_csi_translator)
                .map_err(anyhow::Error::msg)?;
        Ok(crate::volume_plugins::OwnedSpec {
            volume: volume.clone(),
            persistent_volume: translated.persistent_volume,
            read_only: translated.read_only,
            migrated: translated.migrated,
            inline_volume_spec_for_csi_migration: translated.inline_volume_spec_for_csi_migration,
        })
    }

    /// The `makeMounts` read of each mounted volume's attributes for one
    /// container, keyed by volume name (`pkg/kubelet/kubelet_pods.go:296`,
    /// `:392`): `read_only` is `Attributes.ReadOnly` (`mustMountRO`);
    /// `selinux_relabel` is `Managed && SELinuxRelabel && !SELinuxLabeled`,
    /// and the volume is then marked labeled so a later container's mount of
    /// it is not relabelled again. A volume with no record (not mounted by this
    /// manager) is absent, as upstream errors for a missing `vol.Mounter`; the
    /// mount is then left exactly as the container spec declared it.
    pub fn mount_attributes(
        &self,
        pod: &Pod,
        container: &rusternetes_common::resources::Container,
    ) -> HashMap<String, crate::cri_runtime::translate::MountAttrs> {
        let mut out = HashMap::new();
        let mut mounted = self.mounted.lock().unwrap();
        for vm in container.volume_mounts.iter().flatten() {
            let Some(vol) = mounted.get_mut(&(pod.metadata.uid.clone(), vm.name.clone())) else {
                continue;
            };
            let a = vol.attributes;
            let mut relabel = false;
            if a.managed && a.selinux_relabel && !vol.selinux_labeled {
                vol.selinux_labeled = true;
                relabel = true;
            }
            let entry =
                out.entry(vm.name.clone())
                    .or_insert(crate::cri_runtime::translate::MountAttrs {
                        read_only: a.read_only,
                        selinux_relabel: false,
                    });
            entry.selinux_relabel |= relabel;
        }
        out
    }

    /// Rebuild the mount-attribute record of every pod volume that has none
    /// (after a kubelet restart, or when `start_container` runs without
    /// `create_pod_volumes` in this process).
    ///
    /// Port of the Mounter half of `reconstructVolume`
    /// (`pkg/kubelet/volumemanager/reconciler/reconstruct_common.go:352`,
    /// `volumeMounter, err = plugin.NewMounter(volumeSpec, pod)`): the mounter
    /// is built from the spec WITHOUT `SetUp`, and `makeMounts` then reads its
    /// `GetAttributes()` (`kubelet_pods.go:296`). An existing record is kept
    /// (upstream's `MarkVolumeAsMounted` refreshes the mounter, but never
    /// resets `SELinuxLabeled`). As upstream's `reconstructVolume` returns the
    /// error and the volume is simply not reconstructed, a volume whose
    /// mounter cannot be built is left without a record (logged).
    pub async fn reconstruct_mount_attributes(&self, pod: &Pod) {
        let Some(spec) = pod.spec.as_ref() else {
            return;
        };
        for volume in spec.volumes.iter().flatten() {
            let key = (pod.metadata.uid.clone(), volume.name.clone());
            if self.mounted.lock().unwrap().contains_key(&key) {
                continue;
            }
            let pv = match self.resolve_persistent_volume(pod, volume).await {
                Ok(pv) => pv,
                Err(e) => {
                    warn!("reconstruct mount attributes: volume {}: {e}", volume.name);
                    continue;
                }
            };
            let owned = match self.create_volume_spec(
                &pod.metadata.namespace.clone().unwrap_or_default(),
                volume,
                pv.as_ref(),
                pv.is_some() && pvc_read_only(volume),
            ) {
                Ok(o) => o,
                Err(e) => {
                    warn!("reconstruct mount attributes: volume {}: {e}", volume.name);
                    continue;
                }
            };
            let vspec = owned.as_spec();
            let Ok(plugin) = self.plugin_mgr.find_plugin_by_spec(&vspec) else {
                continue;
            };
            match plugin.new_mounter(&vspec, pod).await {
                Ok(mounter) => {
                    self.mounted
                        .lock()
                        .unwrap()
                        .entry(key)
                        .or_insert_with(|| MountedVolume {
                            attributes: mounter.get_attributes(),
                            selinux_labeled: false,
                        });
                }
                Err(e) => warn!(
                    "reconstructVolume.NewMounter failed for volume {} pod {}: {e}",
                    volume.name, pod.metadata.name
                ),
            }
        }
    }

    /// Whether a pod still has volumes mounted, in which case its directory
    /// must not be touched.
    ///
    /// Port of `podVolumesExist` (`pkg/kubelet/kubelet_volumes.go:78-96`) built
    /// on `getMountedVolumePathListFromDisk` (`kubelet_getters.go:381-402`).
    ///
    /// Any error answers **true**. That is upstream's rule, not caution added
    /// here: "If checking returns error, podVolumesExist will return true which
    /// means we consider volumes might exist and requires further checking."
    /// Deleting a directory whose volumes are still mounted can corrupt data,
    /// so an unreadable path must never be read as "safe to remove".
    ///
    /// Like upstream it first consults the volume manager's in-memory record
    /// (`HasPossiblyMountedVolumesForPod`, `volume_manager.go:333`), which the
    /// mounts and unmounts keep in the actual state of world. After a kubelet
    /// restart that record is empty until volumes are reconstructed
    /// (`reconstruct.go`, not ported), so the disk check still decides then.
    fn pod_volumes_exist(&self, pod_uid: &str) -> bool {
        if !self
            .asw
            .get_possibly_mounted_volumes_for_pod(
                &crate::volume_plugins::util::types::UniquePodName(pod_uid.to_string()),
            )
            .is_empty()
        {
            return true;
        }
        let volume_paths = match crate::pod_dirs::get_pod_volume_path_list_from_disk(
            &self.volumes_base_path,
            pod_uid,
        ) {
            Ok(paths) => paths,
            Err(e) => {
                warn!(
                    "Pod {} found, but error occurred during checking mounted volumes: {}",
                    pod_uid, e
                );
                return true;
            }
        };
        for path in &volume_paths {
            match crate::pod_dirs::is_likely_not_mount_point(path) {
                Ok(true) => {}
                Ok(false) => {
                    debug!(
                        "Pod {} found, but volumes are still mounted on disk: {}",
                        pod_uid,
                        path.display()
                    );
                    return true;
                }
                Err(e) => {
                    warn!("Fail to check mount point {}: {}", path.display(), e);
                    return true;
                }
            }
        }
        false
    }

    /// Remove a pod's volumes directory and its subdirectories.
    ///
    /// Port of `removeOrphanedPodVolumeDirs`
    /// (`pkg/kubelet/kubelet_volumes.go:119-165`). Returns every error it hit;
    /// an empty vec means the volumes dir is gone.
    ///
    /// Each individual volume path is removed with `rmdir`, never a recursive
    /// delete: under normal conditions these are empty mount points, and `rmdir`
    /// failing on a non-empty directory is what stops us deleting the contents
    /// of something still mounted.
    fn remove_orphaned_pod_volume_dirs(&self, pod_uid: &str) -> Vec<String> {
        let root = &self.volumes_base_path;
        let mut errors = Vec::new();

        // If there are still volume directories, attempt to rmdir them.
        match crate::pod_dirs::get_pod_volume_path_list_from_disk(root, pod_uid) {
            Ok(volume_paths) => {
                for volume_path in volume_paths {
                    if let Err(e) = std::fs::remove_dir(&volume_path) {
                        errors.push(format!(
                            "orphaned pod {} found, but failed to rmdir() volume at path {}: {}",
                            pod_uid,
                            volume_path.display(),
                            e
                        ));
                    } else {
                        info!(
                            "Cleaned up orphaned volume from pod {}: {}",
                            pod_uid,
                            volume_path.display()
                        );
                    }
                }
            }
            Err(e) => {
                errors.push(format!(
                    "orphaned pod {pod_uid} found, but error occurred during reading volume dir from disk: {e}"
                ));
                return errors;
            }
        }

        // If there are any volume-subpaths, attempt to remove them. These may be
        // bind mounts of a file OR a directory, so plain remove, not rmdir.
        match crate::pod_dirs::get_pod_volume_subpath_list_from_disk(root, pod_uid) {
            Ok(subpaths) => {
                for subpath in subpaths {
                    let removed =
                        std::fs::remove_file(&subpath).or_else(|_| std::fs::remove_dir(&subpath));
                    if let Err(e) = removed {
                        errors.push(format!(
                            "orphaned pod {} found, but failed to rmdir() subpath at path {}: {}",
                            pod_uid,
                            subpath.display(),
                            e
                        ));
                    } else {
                        info!(
                            "Cleaned up orphaned volume subpath from pod {}: {}",
                            pod_uid,
                            subpath.display()
                        );
                    }
                }
            }
            Err(e) => {
                errors.push(format!(
                    "orphaned pod {pod_uid} found, but error occurred during reading of volume-subpaths dir from disk: {e}"
                ));
                return errors;
            }
        }

        // Remove any remaining subdirectories along with the volumes directory
        // itself. Fails if any regular file is encountered.
        let pod_vol_dir = crate::pod_dirs::get_pod_volumes_dir(root, pod_uid);
        match crate::removeall::remove_dirs_one_filesystem(&pod_vol_dir) {
            Ok(()) => info!(
                "Cleaned up orphaned pod volumes dir for {}: {}",
                pod_uid,
                pod_vol_dir.display()
            ),
            Err(e) => errors.push(format!(
                "orphaned pod {pod_uid} found, but error occurred when trying to remove the volumes dir: {e}"
            )),
        }

        errors
    }

    /// `pendingOperations.IsOperationPending(volumeName, EmptyUniquePodName,
    /// EmptyNodeName)`: a device operation, or a mount, is running on the volume.
    pub fn volume_operation_pending(
        &self,
        volume_name: &crate::volume_plugins::util::types::UniqueVolumeName,
    ) -> bool {
        use crate::volume_plugins::util::nested_pending_operations::EMPTY;
        self.pending_operations
            .is_operation_pending(&volume_name.0, EMPTY, EMPTY)
            || self
                .mount_operations
                .is_operation_pending(&volume_name.0, EMPTY, EMPTY)
    }

    /// The stand-ins for the part of `reconstructVolumes` that finds devices
    /// with no pod directory left (a crash between a pod directory's removal
    /// and its device's): every staged CSI device and iscsi global mount the
    /// actual state does not know is added to it as globally mounted, so
    /// `unmountDetachDevices` releases it once nothing wants it.
    pub fn reconstruct_orphaned_devices(&self) {
        self.reconstruct_staged_devices();
        for path in self.iscsi_plugin.list_global_mounts() {
            self.reconstruct_iscsi_device(&path);
        }
    }

    /// Record one iscsi global mount in the actual state of world as attached
    /// and globally mounted (the stand-in for `reconciler/reconstruct.go`).
    pub fn reconstruct_iscsi_device(&self, device_mount_path: &str) {
        let plugin_name = crate::volume_plugins::iscsi::ISCSI_PLUGIN_NAME;
        let Ok(plugin) = self.plugin_mgr.find_plugin_by_name(plugin_name) else {
            return;
        };
        let Some(vol_name) = std::path::Path::new(device_mount_path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
        else {
            return;
        };
        let spec = match crate::volume_plugins::iscsi::get_volume_spec_from_global_map_path(
            &vol_name,
            device_mount_path,
        ) {
            Ok(s) => Arc::new(s),
            Err(e) => {
                warn!("Cannot reconstruct the spec of iscsi device {device_mount_path}: {e:#}");
                return;
            }
        };
        let volume_name = match crate::volume_plugins::util::get_unique_volume_name_from_spec(
            plugin,
            &spec.as_spec(),
        ) {
            Ok(n) => n,
            Err(e) => {
                warn!("Cannot name iscsi device {device_mount_path}: {e}");
                return;
            }
        };
        if self.asw.volume_exists(&volume_name) {
            return;
        }
        if let Err(e) = self
            .asw
            .mark_volume_as_attached(Some(&volume_name), spec, "", "")
            .and_then(|()| {
                self.asw
                    .mark_device_as_mounted(&volume_name, "", device_mount_path, "")
            })
        {
            warn!("Cannot record iscsi device {device_mount_path}: {e}");
        }
    }

    /// Put every staged CSI device that is not yet in the actual state of world
    /// into it as attached and globally mounted: the stand-in for
    /// `reconciler/reconstruct.go`, which does the same for each volume found on
    /// disk at startup. A device already known keeps its recorded state (an
    /// uncertain mount stays uncertain until `UnmountDevice` resolves it).
    pub(crate) fn reconstruct_staged_devices(&self) {
        use crate::volume_plugins::plugin::OwnedSpec;

        let plugin_name = crate::pod_dirs::plugin::CSI;
        let Ok(plugin) = self.plugin_mgr.find_plugin_by_name(plugin_name) else {
            return;
        };
        for (_, _, device_mount_path) in self.csi_plugin.list_staged_devices() {
            let device_mount_path_str = device_mount_path.to_string_lossy().to_string();
            // `ConstructVolumeSpec` over the volume directory holding `vol_data.json`.
            let Some(data_dir) = device_mount_path.parent() else {
                continue;
            };
            let reconstructed = match plugin.construct_volume_spec("", &data_dir.to_string_lossy())
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Cannot reconstruct the spec of staged device {device_mount_path_str}: {e:#}"
                    );
                    continue;
                }
            };
            // A plugin whose `ConstructVolumeSpec` returns a nil spec (the
            // `image` plugin, image.go:71-73) cannot be reconstructed
            // (reconstruct_common.go:292-295: "got nil").
            let Some(volume) = reconstructed.volume else {
                warn!("Cannot reconstruct the spec of staged device {device_mount_path_str}: got nil spec");
                continue;
            };
            // `reconstructedVolume.seLinuxMountContext` (`reconstruct_common.go:387`)
            // feeds `MarkDeviceAsMounted` (`reconstruct.go:120-131`).
            let reconstructed_selinux_mount_context = reconstructed.selinux_mount_context;
            let spec = Arc::new(OwnedSpec {
                volume,
                persistent_volume: reconstructed.persistent_volume,
                read_only: false,
                migrated: false,
                inline_volume_spec_for_csi_migration: false,
            });
            let volume_name = match crate::volume_plugins::util::get_unique_volume_name_from_spec(
                plugin,
                &spec.as_spec(),
            ) {
                Ok(n) => n,
                Err(e) => {
                    warn!("Cannot name staged device {device_mount_path_str}: {e}");
                    continue;
                }
            };
            if self.asw.volume_exists(&volume_name) {
                continue;
            }
            if let Err(e) = self
                .asw
                .mark_volume_as_attached(Some(&volume_name), spec, &self.node_name, "")
                .and_then(|()| {
                    self.asw.mark_device_as_mounted(
                        &volume_name,
                        "",
                        &device_mount_path_str,
                        &reconstructed_selinux_mount_context,
                    )
                })
            {
                warn!("Cannot record staged device {device_mount_path_str}: {e}");
            }
        }
    }

    /// `pendingOperations.IsOperationPending(volumeName, podName,
    /// EmptyNodeName)` (`reconciler_common.go:151`): an unmount or a mount is
    /// running on the volume for the pod.
    pub fn volume_pod_operation_pending(
        &self,
        volume_name: &crate::volume_plugins::util::types::UniqueVolumeName,
        pod_name: &crate::volume_plugins::util::types::UniquePodName,
    ) -> bool {
        use crate::volume_plugins::util::nested_pending_operations::EMPTY;
        self.pending_operations
            .is_operation_pending(&volume_name.0, &pod_name.0, EMPTY)
            || self
                .mount_operations
                .is_operation_pending(&volume_name.0, &pod_name.0, EMPTY)
    }

    /// `operationExecutor.UnmountVolume(mountedVolume, actualStateOfWorld,
    /// kubeletPodsDir)` (`operation_executor.go`): `GenerateUnmountVolumeFunc`
    /// run under `pendingOperations` keyed on the volume and pod, so it is
    /// refused while an operation is pending or backing off. A failure is
    /// logged and retried on the next reconcile.
    pub async fn unmount_volume(
        &self,
        mounted: &crate::volume_plugins::util::operation_executor::MountedVolume,
    ) {
        use crate::volume_plugins::util::nested_pending_operations::EMPTY;
        use crate::volume_plugins::util::operation_generator::generate_unmount_volume_func;

        let what = format!("{} pod {}", mounted.volume_name, mounted.pod_name);
        // `operationExecutor.UnmountVolume`: a Filesystem volume is unmounted,
        // a Block volume unmapped (`operation_executor.go:868-897`).
        let fs_volume =
            match crate::volume_plugins::util::operation_generator::check_volume_mode_filesystem(
                &mounted.volume_spec.as_spec(),
            ) {
                Ok(fs) => fs,
                Err(e) => {
                    warn!("UnmountVolume of {what}: {e:#}");
                    return;
                }
            };
        let op = if fs_volume {
            match generate_unmount_volume_func(&self.plugin_mgr, mounted, &self.asw) {
                Ok(op) => Some(op),
                Err(e) => {
                    warn!("UnmountVolume of {what}: {e:#}");
                    return;
                }
            }
        } else {
            None
        };
        let name = if fs_volume {
            "volume_unmount"
        } else {
            "volume_unmap"
        };
        let run = self.pending_operations.run_inline(
            &mounted.volume_name.0,
            &mounted.pod_name.0,
            EMPTY,
            name,
            async {
                let ctx = match &op {
                    Some(op) => op.run_operation().await,
                    None => crate::volume_plugins::util::block_operations::unmap_volume_with(
                        &self.plugin_mgr,
                        mounted,
                        &self.asw,
                        self.block_util.as_ref(),
                    )
                    .await
                    .map_err(|e| format!("{e:#}"))?,
                };
                match ctx.detailed_err {
                    Some(e) => Err(format!("{e:#}")),
                    None => Ok(()),
                }
            },
        );
        match run.await {
            Err(refused) => debug!("UnmountVolume of {what} not started: {refused}"),
            Ok(Err(e)) => debug!("UnmountVolume of {what} failed: {e}"),
            Ok(Ok(())) => {
                // The pod's volume records live as long as the actual state's
                // mounts do (upstream reads them from it).
                if self
                    .asw
                    .get_possibly_mounted_volumes_for_pod(&mounted.pod_name)
                    .is_empty()
                {
                    self.mounted
                        .lock()
                        .unwrap()
                        .retain(|(uid, _), _| *uid != mounted.pod_uid);
                }
            }
        }
    }

    /// `operationExecutor.UnmountDevice(attachedVolume.AttachedVolume,
    /// actualStateOfWorld, hostutil)` (`operation_executor.go`, called from
    /// `unmountDetachDevices`, `reconciler_common.go:296-306`):
    /// `GenerateUnmountDeviceFunc` then run its operation against the
    /// persistent actual state of world, so `MarkDeviceAsUncertain` /
    /// `MarkDeviceAsUnmounted` land on the entry the next sweep reads.
    /// A failure is logged and retried on the next sync, as the reconciler does.
    pub async fn unmount_device(
        &self,
        attached: &crate::volume_manager::cache::actual_state_of_world::AttachedVolume,
    ) {
        use crate::volume_plugins::util::operation_generator::generate_unmount_device_func;

        let device_to_detach: &crate::volume_plugins::util::operation_executor::AttachedVolume =
            attached;
        let what = format!(
            "{} ({})",
            device_to_detach.volume_name, device_to_detach.device_mount_path
        );
        let hostutil = crate::volume_plugins::util::hostutil::HostUtil;
        // `operationExecutor.UnmountDevice`: a Filesystem volume's device is
        // unmounted, a Block volume's unmapped (`operation_executor.go:899-930`).
        let fs_volume =
            match crate::volume_plugins::util::operation_generator::check_volume_mode_filesystem(
                &device_to_detach.volume_spec.as_spec(),
            ) {
                Ok(fs) => fs,
                Err(e) => {
                    warn!("UnmountDevice of {what}: {e:#}");
                    return;
                }
            };
        let op = if fs_volume {
            match generate_unmount_device_func(
                &self.plugin_mgr,
                device_to_detach,
                &self.asw,
                &hostutil,
            ) {
                Ok(op) => Some(op),
                Err(e) => {
                    warn!("UnmountDevice of {what}: {e:#}");
                    return;
                }
            }
        } else {
            None
        };
        // `oe.pendingOperations.Run(deviceToDetach.VolumeName, EmptyUniquePodName,
        // "" /* nodeName */, generatedOperations)` (`operation_executor.go:922-927`):
        // refused while one is pending or its failure's backoff has not elapsed.
        use crate::volume_plugins::util::nested_pending_operations::EMPTY;
        let name = if fs_volume {
            "unmount_device"
        } else {
            "unmap_device"
        };
        let run = self.pending_operations.run_inline(
            &device_to_detach.volume_name.0,
            EMPTY,
            EMPTY,
            name,
            async {
                let ctx = match &op {
                    Some(op) => op.run_operation().await,
                    None => crate::volume_plugins::util::block_operations::unmap_device_with(
                        &self.plugin_mgr,
                        device_to_detach,
                        &self.asw,
                        &hostutil,
                        self.block_util.as_ref(),
                    )
                    .await
                    .map_err(|e| format!("{e:#}"))?,
                };
                match ctx.detailed_err {
                    Some(e) => Err(format!("{e:#}")),
                    None => Ok(()),
                }
            },
        );
        match run.await {
            // `isExpectedError` (`reconciler_common.go`): AlreadyExists and
            // ExponentialBackoff are not logged.
            Err(refused) => debug!("UnmountDevice of {what} not started: {refused}"),
            // The map has already logged the failure (`operationComplete`).
            Ok(Err(e)) => debug!("UnmountDevice of {what} failed: {e}"),
            Ok(Ok(())) => {}
        }
    }

    /// Remove the directories of pods that should not be running and have no
    /// containers running.
    ///
    /// Port of `cleanupOrphanedPodDirs`
    /// (`pkg/kubelet/kubelet_volumes.go:169-260`). Upstream calls this from
    /// `HandlePodCleanups` (`kubelet_pods.go:1304`) on the sync loop, next to
    /// its container cleanup — which is why this sits beside
    /// `cleanup_orphaned_containers` rather than on a shutdown path. A periodic
    /// sweep is also the only form that survives a crash or a killed teardown.
    ///
    /// `live_pod_uids` must contain every pod the kubelet knows about *and*
    /// every pod with a running container, matching upstream's union of `pods`
    /// and `runningPods`.
    pub fn cleanup_orphaned_pod_dirs(&self, live_pod_uids: &std::collections::HashSet<String>) {
        let root = &self.volumes_base_path;
        let found = match crate::pod_dirs::list_pods_from_disk(root) {
            Ok(uids) => uids,
            Err(e) => {
                warn!("Could not list pods from disk: {}", e);
                return;
            }
        };

        for uid in found {
            if live_pod_uids.contains(&uid) {
                continue;
            }

            // If volumes have not been unmounted, do not delete the directory.
            // Doing so may result in corruption of data.
            if self.pod_volumes_exist(&uid) {
                debug!("Orphaned pod {} found, but volumes are not cleaned up", uid);
                continue;
            }

            let volume_errors = self.remove_orphaned_pod_volume_dirs(&uid);
            if !volume_errors.is_empty() {
                // Not all volumes were removed, so don't clean up the pod
                // directory yet — mountpoints or files left behind would make
                // the removal below fail anyway.
                for e in &volume_errors {
                    warn!("{}", e);
                }
                continue;
            }

            let pod_dir = crate::pod_dirs::get_pod_dir(root, &uid);
            let subdirs = match std::fs::read_dir(&pod_dir) {
                Ok(entries) => entries,
                Err(e) => {
                    warn!(
                        "Orphaned pod {} found, but error occurred during reading the pod dir from disk: {}",
                        uid, e
                    );
                    continue;
                }
            };

            let mut cleanup_failed = false;
            for subdir in subdirs {
                let subdir_path = match subdir {
                    Ok(entry) => entry.path(),
                    Err(e) => {
                        cleanup_failed = true;
                        warn!("Failed to read a subdir of pod dir {}: {}", uid, e);
                        continue;
                    }
                };
                // Never recursively delete the volumes directory: that could
                // lose data. It should already be gone, so finding it here is an
                // error to report, not something to force.
                if subdir_path
                    .file_name()
                    .map(|n| n == crate::pod_dirs::VOLUMES_DIR_NAME)
                    == Some(true)
                {
                    cleanup_failed = true;
                    warn!(
                        "Orphaned pod {} found, but failed to remove volumes subdir: volumes subdir was found after it was removed ({})",
                        uid,
                        subdir_path.display()
                    );
                    continue;
                }
                if let Err(e) = crate::removeall::remove_all_one_filesystem(&subdir_path) {
                    cleanup_failed = true;
                    warn!(
                        "Failed to remove orphaned pod subdir {}: {}",
                        subdir_path.display(),
                        e
                    );
                }
            }

            // Rmdir the pod dir, which should be empty if everything above
            // succeeded.
            if !cleanup_failed {
                debug!("Orphaned pod {} found, removing", uid);
                if let Err(e) = std::fs::remove_dir(&pod_dir) {
                    warn!(
                        "Failed to remove orphaned pod dir {}: {}",
                        pod_dir.display(),
                        e
                    );
                }
            }
        }
    }

    /// The plugin that owns a volume, and hence the plugin segment of its
    /// on-disk path. Replaces the ordered if-else chain this file used to
    /// dispatch `create_volume` through: upstream resolves this by asking
    /// every plugin `CanSupport` (`pkg/volume/plugins.go:634-666`), which is
    /// what the registry does.
    ///
    /// Both registry failures collapse to [`crate::pod_dirs::UNSUPPORTED_PLUGIN`]:
    ///
    /// - `NoPluginMatched` is exactly what the old if-else chain's
    ///   else-arm used to return, so the former resync and refresh paths and
    ///   the `kubelet.rs` init-container-restart path map keep looking in the
    ///   identical directory. This is the whole reason the mapping exists.
    /// - `MultipleMatched` would, under the old first-match-wins chain, have
    ///   picked a real plugin name here — but `create_volume` now **errors**
    ///   on `MultipleMatched` before any directory is created, so the pod
    ///   fails and nothing ever reads a path for that volume. The divergence
    ///   between this helper and `create_volume` is therefore unreachable.
    ///
    /// A `PersistentVolume` is never passed in: this helper answers "what
    /// plugin does the pod's directory belong to", which for a
    /// `persistentVolumeClaim`/`ephemeral` volume is moot — those never get a
    /// directory under the pod dir (they resolve to a path elsewhere on the
    /// host), so an unmatched PVC/ephemeral volume correctly falls back to
    /// `UNSUPPORTED_PLUGIN` here too.
    pub fn plugin_name_for_volume(
        &self,
        volume: &rusternetes_common::resources::Volume,
    ) -> &'static str {
        let spec = crate::volume_plugins::Spec {
            volume,
            persistent_volume: None,
            read_only: false,
            migrated: false,
            inline_volume_spec_for_csi_migration: false,
        };
        match self.plugin_mgr.find_plugin_by_spec(&spec) {
            Ok(plugin) => plugin.name(),
            Err(_) => crate::pod_dirs::UNSUPPORTED_PLUGIN,
        }
    }

    /// Create volumes for a pod and return volume bindings for containers
    pub async fn create_pod_volumes(&self, pod: &Pod) -> Result<HashMap<String, String>> {
        // With a volume manager running, the pod's volumes are mounted by its
        // reconciler: wait for them and read the paths from the actual state
        // (`WaitForAttachAndMount` + `GetMountedVolumesForPod`).
        if let Some(volume_manager) = self.volume_manager.get().and_then(|w| w.upgrade()) {
            volume_manager.wait_for_attach_and_mount(pod).await?;
            return Ok(volume_manager.get_mounted_volume_paths(pod));
        }

        let mut volume_paths = HashMap::new();

        if let Some(volumes) = &pod.spec.as_ref().unwrap().volumes {
            for volume in volumes {
                let volume_path = match self.create_volume(pod, volume).await {
                    Ok(p) => p,
                    // `desired_state_of_world_populator.go:293-298`: a volume
                    // no container mounts is "Skipping unused volume", so it
                    // never reaches `FindPluginBySpec`.
                    Err(e)
                        if e.downcast_ref::<NoPluginForVolume>().is_some()
                            && !pod_uses_volume(pod, &volume.name) =>
                    {
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                volume_paths.insert(volume.name.clone(), volume_path);
            }
        }

        // fsGroup is applied by each volume plugin's own SetUp
        // (`volume.NewVolumeOwnership(..).ChangePermissions()`: empty_dir.go:277,
        // configmap.go:246-252, secret.go:242-248, downwardapi.go:217-223,
        // projected.go:200-214). Upstream never chowns hostPath, and there is no
        // cross-volume owner->group mirror pass.
        Ok(volume_paths)
    }

    /// Collect the supplemental GIDs contributed by the pod's mounted volumes,
    /// read from the `pv.beta.kubernetes.io/gid` annotation on each PVC-bound PV
    /// (`VOLUME_GID_ANNOTATION_KEY`). This is the volume-GID half of upstream
    /// `GetExtraSupplementalGroupsForPod`; the CRI translation layer then applies
    /// `translate::extra_supplemental_gids` to drop GIDs already in the pod's
    /// `supplementalGroups` and de-duplicate, before appending them to the
    /// container/sandbox security contexts. Volumes with no PVC, unbound PVCs,
    /// missing PVs, a missing annotation, or a non-numeric annotation value are
    /// skipped.
    pub async fn volume_gids(&self, pod: &Pod) -> Vec<i64> {
        let Some(storage) = self.storage.as_ref() else {
            return Vec::new();
        };
        let Some(volumes) = pod.spec.as_ref().and_then(|s| s.volumes.as_ref()) else {
            return Vec::new();
        };
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");
        let mut gids = Vec::new();
        for volume in volumes {
            let Some(pvc_source) = volume.persistent_volume_claim.as_ref() else {
                continue;
            };
            let pvc_key = build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &pvc_source.claim_name,
            );
            let Ok(pvc) = storage.get::<PersistentVolumeClaim>(&pvc_key).await else {
                continue;
            };
            let Some(pv_name) = pvc.spec.volume_name.as_ref() else {
                continue;
            };
            let pv_key = build_key("persistentvolumes", None, pv_name);
            let Ok(pv) = storage.get::<PersistentVolume>(&pv_key).await else {
                continue;
            };
            if let Some(val) = pv
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(VOLUME_GID_ANNOTATION_KEY))
            {
                if let Ok(gid) = val.parse::<i64>() {
                    gids.push(gid);
                }
            }
        }
        gids
    }

    /// Resolve a claim-backed volume to the PersistentVolume that backs it.
    ///
    /// There is no `pvc` or `ephemeral` package under `pkg/volume`: upstream
    /// resolves the claim in the volume manager and then dispatches on the
    /// PV's SOURCE. `desiredStateOfWorldPopulator.createVolumeSpec`
    /// (`pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:426-471`)
    /// fetches the PVC, reads `pvc.Spec.VolumeName`, then `getPVSpec`
    /// (`:562-589`) fetches the PV and builds a `volume.Spec` from it via
    /// `NewSpecFromPersistentVolume` — the plugin lookup then runs against
    /// that `Spec`, exactly as `find_plugin_by_spec` does below. This is that
    /// resolution step. Returns `Ok(None)` for a volume that is neither a
    /// claim nor an ephemeral claim template.
    pub async fn resolve_persistent_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<Option<PersistentVolume>> {
        let pod_name = &pod.metadata.name;
        let namespace = pod.metadata.namespace.as_deref().unwrap_or("default");

        // PersistentVolumeClaim: find bound PV and use its path
        if let Some(pvc_source) = &volume.persistent_volume_claim {
            // ---- moved verbatim from create_volume's PersistentVolumeClaim branch
            //      (991a503d:crates/kubelet/src/volumes.rs:1328-1373) ----
            let storage = self
                .storage
                .as_ref()
                .context("Storage not available for PersistentVolumeClaim volumes")?;

            let pvc_key = build_key(
                "persistentvolumeclaims",
                Some(namespace),
                &pvc_source.claim_name,
            );
            let pvc: PersistentVolumeClaim = storage.get(&pvc_key).await.with_context(|| {
                crate::lifecycle::VolumeWaitError(format!(
                    "PersistentVolumeClaim {} not found in namespace {}",
                    pvc_source.claim_name, namespace
                ))
            })?;

            // Get the bound PV name
            let pv_name = pvc.spec.volume_name.as_ref().ok_or_else(|| {
                crate::lifecycle::VolumeWaitError(
                    "PersistentVolumeClaim is not bound to a volume".to_string(),
                )
            })?;

            // Get the PV
            let pv_key = build_key("persistentvolumes", None, pv_name);
            let pv: PersistentVolume = storage
                .get(&pv_key)
                .await
                .with_context(|| format!("PersistentVolume {} not found", pv_name))?;
            // ---- end moved body ----
            // The final mount path is not known here — the matched plugin
            // resolves and logs it separately — so log what this step has:
            // the pod's volume name and the PV it resolved to. Restores the
            // claim/PV pairing that used to be logged from this branch's
            // tail before that tail moved into the hostPath plugin
            // (`991a503d:volumes.rs:1364`).
            info!(
                "Using PersistentVolumeClaim volume {} backed by PV {}",
                volume.name, pv_name
            );
            return Ok(Some(pv));
        }

        // Ephemeral: generic ephemeral volume with PVC template
        if let Some(ephemeral) = &volume.ephemeral {
            if let Some(pvc_template) = &ephemeral.volume_claim_template {
                // ---- moved verbatim from create_volume's Ephemeral branch
                //      (991a503d:crates/kubelet/src/volumes.rs:1464-1551) ----
                let storage = self
                    .storage
                    .as_ref()
                    .context("Storage not available for ephemeral volumes")?;

                // Create a PVC name based on pod name and volume name
                let pvc_name = format!("{}-{}", pod_name, volume.name);

                // Check if PVC already exists
                let pvc_key = build_key("persistentvolumeclaims", Some(namespace), &pvc_name);
                let pvc_exists = storage.get::<PersistentVolumeClaim>(&pvc_key).await.is_ok();

                if !pvc_exists {
                    // Create the PVC from the template
                    let mut pvc = PersistentVolumeClaim {
                        type_meta: rusternetes_common::types::TypeMeta {
                            kind: "PersistentVolumeClaim".to_string(),
                            api_version: "v1".to_string(),
                        },
                        metadata: rusternetes_common::types::ObjectMeta::new(&pvc_name)
                            .with_namespace(namespace),
                        spec: pvc_template.spec.clone(),
                        status: None,
                    };

                    // Copy labels and annotations from template if provided
                    if let Some(template_meta) = &pvc_template.metadata {
                        if let Some(labels) = &template_meta.labels {
                            pvc.metadata.labels = Some(labels.clone());
                        }
                        if let Some(annotations) = &template_meta.annotations {
                            pvc.metadata.annotations = Some(annotations.clone());
                        }
                    }

                    // Store the PVC
                    storage
                        .create(&pvc_key, &pvc)
                        .await
                        .context("Failed to create ephemeral PVC")?;

                    info!(
                        "Created ephemeral PVC {} for volume {}",
                        pvc_name, volume.name
                    );

                    // Wait for PVC to be bound (simplified - in production would poll/watch)
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }

                // Now use the PVC like a regular PersistentVolumeClaim
                let pvc: PersistentVolumeClaim =
                    storage.get(&pvc_key).await.with_context(|| {
                        crate::lifecycle::VolumeWaitError(format!(
                            "PersistentVolumeClaim {} not found in namespace {}",
                            pvc_name, namespace
                        ))
                    })?;

                if let Some(pv_name) = &pvc.spec.volume_name {
                    let pv_key = build_key("persistentvolumes", None, pv_name);
                    let pv: PersistentVolume = storage.get(&pv_key).await.with_context(|| {
                        format!(
                            "PersistentVolume {} not found for ephemeral volume",
                            pv_name
                        )
                    })?;
                    // ---- end moved body ----
                    // Same rationale as the PersistentVolumeClaim branch
                    // above: no mount path here, so log the claim/PV pairing
                    // only. Restores the tail dropped from this branch
                    // (`991a503d:volumes.rs:1538`).
                    info!(
                        "Using ephemeral volume {} backed by PVC {} and PV {}",
                        volume.name, pvc_name, pv_name
                    );
                    return Ok(Some(pv));
                } else {
                    // Same wording as the PersistentVolumeClaim branch so
                    // `is_volume_wait_error` treats it as a retriable wait
                    // (upstream: an ephemeral volume is handled "the same way
                    // as a PVC reference", `desired_state_of_world_populator.go:432-441`).
                    return Err(crate::lifecycle::VolumeWaitError(format!(
                        "PersistentVolumeClaim {} is not bound to a volume",
                        pvc_name
                    ))
                    .into());
                }
            }
        }

        Ok(None)
    }

    /// Create a single volume and return its host path.
    ///
    /// Port of `pkg/volume/plugins.go:634-666`'s `FindPluginBySpec` used the
    /// way `desiredStateOfWorldPopulator.createVolumeSpec` uses it: resolve a
    /// `persistentVolumeClaim`/`ephemeral` volume to its bound PV first (see
    /// `resolve_persistent_volume` — upstream has no separate pvc plugin), then
    /// look the resulting `Spec` up in the registry exactly once. What used to
    /// be eight `if volume.<kind>.is_some()` blocks, each constructing its own
    /// plugin, is now the one dispatch the registry exists for.
    pub async fn create_volume(
        &self,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<String> {
        // persistentVolumeClaim / ephemeral resolve to a PersistentVolume
        // first; the PV's source then selects the plugin. Returns `None` for
        // every other volume kind, and for an ephemeral volume with no claim
        // template (which is not actionable and falls through like upstream's
        // empty spec would).
        let pv = self.resolve_persistent_volume(pod, volume).await?;
        // `NewSpecFromPersistentVolume(pv, pvcSource.ReadOnly)`; a spec
        // without a PV is `NewSpecFromVolume`, ReadOnly false. Then the
        // `IsMigratable` / `TranslateInTreeSpecToCSI` tail of
        // `createVolumeSpec` (`desired_state_of_world_populator.go:475`, `:511`).
        let owned = self.create_volume_spec(
            &pod.metadata.namespace.clone().unwrap_or_default(),
            volume,
            pv.as_ref(),
            pv.is_some() && pvc_read_only(volume),
        )?;
        let spec = owned.as_spec();

        match self.plugin_mgr.find_plugin_by_spec(&spec) {
            Ok(plugin) => {
                // The `VolumeToMount` the desired state would hand the
                // mount, and the volume's entry in the actual state
                // (`MarkVolumeAsAttached`, what
                // `GenerateVerifyControllerAttachedVolumeFunc` does for a
                // volume that needs no attach), so the mount can report to it.
                let volume_to_mount = self.volume_to_mount(plugin, owned.clone(), pod, volume)?;
                self.mark_volume_attached(&volume_to_mount)?;
                let (path, _) = self
                    .run_mount_operation(&spec, pod, &volume_to_mount, &volume.name)
                    .await?;
                Ok(path)
            }
            // Upstream: the populator's `AddPodToVolume` fails the volume and
            // the pod waits in ContainerCreating
            // (`desired_state_of_world.go:272-277`,
            // `desired_state_of_world_populator.go:310-315`); there is no
            // fallback directory (#3262). Covers a PV-resolved spec too (e.g.
            // a PersistentVolume whose source has no plugin).
            Err(crate::volume_plugins::PluginLookupError::NoPluginMatched) => {
                Err(NoPluginForVolume(volume.name.clone()).into())
            }
            // A volume declaring two sources (e.g. both emptyDir and
            // configMap) previously resolved silently, first-arm-wins, under
            // the ordered if-chain. The registry rejects it outright
            // (`pkg/volume/plugins.go:661-663`).
            Err(e) => Err(e.into()),
        }
    }

    /// The mode dispatch of `operationExecutor.MountVolume`
    /// (`operation_executor.go:827-866`): a Filesystem volume goes to
    /// `GenerateMountVolumeFunc`, a Block volume to `GenerateMapVolumeFunc`.
    /// A mapped volume's path is its pod device map symlink.
    async fn mount_or_map(
        &self,
        spec: &crate::volume_plugins::plugin::Spec<'_>,
        pod: &Pod,
        volume_to_mount: &crate::volume_plugins::util::operation_executor::VolumeToMount,
        mount_state: &crate::volume_plugins::util::operation_generator::MountState<'_>,
    ) -> Result<(String, crate::volume_plugins::plugin::Attributes)> {
        use crate::volume_plugins::util::{block_operations, operation_generator};
        if !operation_generator::check_volume_mode_filesystem(spec)? {
            let ctx = block_operations::map_volume_with(
                &self.plugin_mgr,
                volume_to_mount,
                mount_state.actual_state_of_world,
                self.block_util.as_ref(),
            )
            .await?;
            if let Some(e) = ctx.detailed_err {
                return Err(e);
            }
            let path = block_operations::pod_device_path(&self.plugin_mgr, spec, pod)?;
            return Ok((path, crate::volume_plugins::plugin::Attributes::default()));
        }
        operation_generator::mount_volume_with_attributes(
            &self.plugin_mgr,
            spec,
            pod,
            "",
            // `og.recorder`: the kubelet's event recorder over the same API
            // the volumes are read through.
            self.storage.as_ref().map(|s| {
                crate::volume_plugins::plugin::MounterRecorder(
                    rusternetes_storage::EventRecorder::new(s.clone()),
                )
            }),
            Some(mount_state),
        )
        .await
    }

    /// `operationExecutor.MountVolume` for one pod volume: the plugin `SetUp`
    /// (device mount first) under the mount operation guard, reporting to the
    /// actual state of the world. Shared by the inline mount
    /// ([`Self::create_volume`]) and the reconciler's remount.
    pub async fn run_mount_operation(
        &self,
        spec: &crate::volume_plugins::plugin::Spec<'_>,
        pod: &Pod,
        volume_to_mount: &crate::volume_plugins::util::operation_executor::VolumeToMount,
        volume_name: &str,
    ) -> Result<(String, crate::volume_plugins::plugin::Attributes)> {
        let mount_state = crate::volume_plugins::util::operation_generator::MountState {
            volume_to_mount,
            actual_state_of_world: &self.asw,
        };
        // The mount is an operation on the volume and pod
        // (`GenerateMountVolumeFunc` runs under `pendingOperations`).
        let mut mounted_volume = None;
        let run = self
            .mount_operations
            .run_inline(
                &volume_to_mount.volume_name.0,
                &volume_to_mount.pod_name.0,
                crate::volume_plugins::util::nested_pending_operations::EMPTY,
                "volume_mount",
                async {
                    let mounted = self
                        .mount_or_map(spec, pod, volume_to_mount, &mount_state)
                        .await;
                    match mounted {
                        Ok(ok) => {
                            mounted_volume = Some(Ok(ok));
                            Ok(())
                        }
                        Err(e) => {
                            let msg = format!("{e:#}");
                            mounted_volume = Some(Err(e));
                            Err(msg)
                        }
                    }
                },
            )
            .await;
        let (path, attributes) = match (run, mounted_volume) {
            (Ok(_), Some(mounted)) => mounted?,
            (Err(refused), _) => {
                // Another mount of this pod's volume is in flight: wait.
                return Err(crate::lifecycle::VolumeWaitError(format!(
                    "MountVolume for volume {} not started: {refused}",
                    volume_name
                ))
                .into());
            }
            (Ok(_), None) => {
                anyhow::bail!("MountVolume for volume {} did not run", volume_name)
            }
        };
        // Keep an existing `SELinuxLabeled` across a re-mount of the same pod
        // volume (upstream keeps it on the pod's VolumeInfo).
        self.mounted
            .lock()
            .unwrap()
            .entry((pod.metadata.uid.clone(), volume_name.to_string()))
            .or_default()
            .attributes = attributes;
        Ok((path, attributes))
    }

    /// Port of `GenerateVerifyControllerAttachedVolumeFunc`
    /// (`operation_generator.go:1396-1446`): a volume an attach/detach
    /// controller attaches enters the actual state once its attachment is
    /// reported attached; one that needs no attach enters at once. Under
    /// `pendingOperations` keyed on the volume, so a volume still waiting for
    /// its attachment backs off. Only the CSI plugin's attachments are
    /// consulted; `node.status.volumesAttached` / `VolumesInUse` are not.
    pub async fn verify_controller_attached_volume(
        &self,
        volume_to_mount: &crate::volume_plugins::util::operation_executor::VolumeToMount,
    ) -> Result<()> {
        if self.asw.volume_exists(&volume_to_mount.volume_name) {
            return Ok(());
        }
        let run = self
            .pending_operations
            .run_inline(
                &volume_to_mount.volume_name.0,
                "",
                "",
                "verify_controller_attached_volume",
                async {
                    let spec = volume_to_mount.volume_spec.as_spec();
                    if volume_to_mount.plugin_is_attachable && self.csi_plugin.can_support(&spec) {
                        match self
                            .csi_plugin
                            .is_volume_attached(&spec, &self.node_name)
                            .await
                        {
                            Ok(true) => {}
                            Ok(false) => {
                                return Err(format!(
                                    "volume {} is not yet attached according to node status",
                                    volume_to_mount.volume_name
                                ))
                            }
                            Err(e) => return Err(format!("{e:#}")),
                        }
                    }
                    self.mark_volume_attached(volume_to_mount)
                        .map_err(|e| format!("{e:#}"))
                },
            )
            .await;
        match run {
            Err(refused) => Err(anyhow::anyhow!("{refused}")),
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(anyhow::anyhow!(msg)),
        }
    }

    /// `MarkVolumeAsAttached` for a volume the kubelet need not attach: what
    /// `GenerateVerifyControllerAttachedVolumeFunc` does for a non-attachable
    /// plugin (`operation_generator.go:1431-1446`). The volume enters the
    /// actual state so the mount can report to it.
    pub fn mark_volume_attached(
        &self,
        volume_to_mount: &crate::volume_plugins::util::operation_executor::VolumeToMount,
    ) -> Result<()> {
        if self.asw.volume_exists(&volume_to_mount.volume_name) {
            return Ok(());
        }
        self.asw
            .mark_volume_as_attached(
                Some(&volume_to_mount.volume_name),
                volume_to_mount.volume_spec.clone(),
                &self.node_name,
                "",
            )
            .map_err(|e| anyhow::anyhow!("MountVolume.MarkVolumeAsAttached failed: {e}"))
    }

    /// Port of `operationExecutor.ExpandInUseVolume`
    /// (`operation_executor.go:925-931`): expand the in-use volume from
    /// `current_size` to the desired PVC size under `pendingOperations`
    /// (keyed on the volume alone, so it backs off after a failure), recording
    /// a failure as the generated operation's event recorder does.
    pub async fn expand_in_use_volume(
        &self,
        volume_to_mount: &crate::volume_plugins::util::operation_executor::VolumeToMount,
        current_size: rusternetes_common::quantity::Quantity,
    ) -> Result<()> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        let recorder = rusternetes_storage::EventRecorder::new(storage.clone());
        let og = crate::volume_plugins::util::operation_generator::OperationGenerator {
            volume_plugin_mgr: &self.plugin_mgr,
            kube_client: storage.as_ref(),
            recorder: &recorder,
        };
        let op = og.generate_expand_in_use_volume_func(volume_to_mount, &self.asw, current_size)?;
        let mut outcome = None;
        let run = self
            .pending_operations
            .run_inline(
                &volume_to_mount.volume_name.0,
                "",
                "",
                op.operation_name,
                async {
                    let ctx = op.run().await;
                    match (ctx.event_err, ctx.detailed_err) {
                        (None, None) => Ok(()),
                        (event_err, detailed_err) => {
                            let msg = detailed_err
                                .as_ref()
                                .map(|e| format!("{e:#}"))
                                .unwrap_or_default();
                            outcome = Some(event_err);
                            Err(msg)
                        }
                    }
                },
            )
            .await;
        match run {
            Err(refused) => Err(anyhow::anyhow!("{refused}")),
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => {
                if let Some(event_err) = outcome.flatten() {
                    op.record_event_on_error(Some(&event_err)).await;
                }
                Err(anyhow::anyhow!(msg))
            }
        }
    }

    /// The `VolumeToMount` `DesiredStateOfWorld.GetVolumesToMount` would
    /// return for this pod volume (`desired_state_of_world.go:586-616`): the
    /// unique volume name the desired state gives it
    /// ([`get_unique_volume_name_for_pod`]), the effective SELinux label and
    /// size limit `AddPodToVolume` computes, and the PV's GID annotation.
    fn volume_to_mount(
        &self,
        plugin: &dyn crate::volume_plugins::plugin::VolumePlugin,
        owned: crate::volume_plugins::plugin::OwnedSpec,
        pod: &Pod,
        volume: &rusternetes_common::resources::Volume,
    ) -> Result<crate::volume_plugins::util::operation_executor::VolumeToMount> {
        use crate::volume_manager::cache::desired_state_of_world::get_unique_volume_name_for_pod;
        let pod_name = crate::volume_plugins::util::get_unique_pod_name(pod);
        let spec = owned.as_spec();
        let (volume_name, plugin_is_attachable, plugin_is_device_mountable) =
            get_unique_volume_name_for_pod(&self.plugin_mgr, &pod_name, plugin, &spec)?;
        let mounter_args = crate::volume_plugins::util::operation_generator::mounter_args_for(
            &self.plugin_mgr,
            &spec,
            pod,
            &crate::volume_plugins::util::selinux::Translator,
        )?;
        let volume_gid_value = owned
            .persistent_volume
            .as_ref()
            .and_then(|pv| pv.metadata.annotations.as_ref())
            .and_then(|a| a.get(VOLUME_GID_ANNOTATION_KEY))
            .cloned()
            .unwrap_or_default();
        Ok(
            crate::volume_plugins::util::operation_executor::VolumeToMount {
                volume_name,
                pod_name,
                outer_volume_spec_names: vec![volume.name.clone()],
                plugin_is_attachable,
                plugin_is_device_mountable,
                volume_gid_value,
                device_path: String::new(),
                reported_in_use: true,
                desired_size_limit:
                    crate::volume_manager::cache::desired_state_of_world::desired_size_limit(
                        pod,
                        &owned.volume,
                    ),
                mount_request_time: std::time::SystemTime::now(),
                desired_persistent_volume_size: None,
                selinux_label: mounter_args.selinux_label,
                volume_spec: Arc::new(owned),
                pod: Arc::new(pod.clone()),
            },
        )
    }
}
