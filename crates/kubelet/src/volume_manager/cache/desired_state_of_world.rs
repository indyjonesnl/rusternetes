//! Port of `pkg/kubelet/volumemanager/cache/desired_state_of_world.go`.

use super::desired_state_of_world_selinux_metrics::{
    register_selinux_metrics, GaugeVec, SELINUX_CONTAINER_CONTEXT_ERRORS,
    SELINUX_CONTAINER_CONTEXT_WARNINGS, SELINUX_POD_CONTEXT_MISMATCH_ERRORS,
    SELINUX_POD_CONTEXT_MISMATCH_WARNINGS, SELINUX_VOLUMES_ADMITTED,
    SELINUX_VOLUME_CONTEXT_MISMATCH_ERRORS, SELINUX_VOLUME_CONTEXT_MISMATCH_WARNINGS,
};
use crate::volume_plugins::plugin::{OwnedSpec, Spec, VolumePlugin};
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::operation_executor;
use crate::volume_plugins::util::selinux::{
    get_mount_selinux_label, volume_supports_selinux_mount, SELinuxLabelError,
    SELinuxLabelTranslator,
};
use crate::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use crate::volume_plugins::util::{
    contains_access_mode, get_unique_volume_name_from_spec,
    get_unique_volume_name_from_spec_with_pod, is_attachable_volume, is_device_mountable_volume,
    is_local_ephemeral_volume,
};
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::quantity::{Format, Quantity};
use rusternetes_common::quota::pod_limits;
use rusternetes_common::resources::pod::SELinuxOptions;
use rusternetes_common::resources::volume::PersistentVolumeAccessMode;
use rusternetes_common::resources::Pod;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tracing::debug;

/// Port of `VolumeToMount` (`desired_state_of_world.go:144-148`):
///
/// ```go
/// type VolumeToMount struct {
///     operationexecutor.VolumeToMount
/// }
/// ```
///
/// Go's struct embedding promotes the inner fields; `Deref`/`DerefMut` is the
/// Rust equivalent, so `vmt.volume_name` still resolves.
#[derive(Clone)]
pub struct VolumeToMount(pub operation_executor::VolumeToMount);

impl Deref for VolumeToMount {
    type Target = operation_executor::VolumeToMount;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VolumeToMount {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The errors `AddPodToVolume` returns. Upstream builds them with
/// `fmt.Errorf`; the messages here are those format strings verbatim, with
/// `{:?}` standing in for Go's `%q`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DesiredStateOfWorldError {
    /// `desired_state_of_world.go:273-278`.
    #[error("failed to get Plugin from volumeSpec for volume {volume_name:?} err={err}")]
    NoPlugin { volume_name: String, err: String },

    /// `desired_state_of_world.go:293-299`.
    #[error(
        "failed to GetUniqueVolumeNameFromSpec for volumeSpec {volume_name:?} \
         using volume plugin {plugin_name:?} err={err}"
    )]
    UniqueVolumeName {
        volume_name: String,
        plugin_name: String,
        err: String,
    },

    /// `desired_state_of_world.go:377`.
    #[error("conflicting SELinux labels of volume {volume_name}: {existing:?} and {new:?}")]
    ConflictingSELinuxLabels {
        volume_name: String,
        existing: String,
        new: String,
    },

    /// Any SELinux error `getSELinuxLabel` chose to propagate rather than
    /// consume (`desired_state_of_world.go:405-437`).
    #[error(transparent)]
    SELinux(#[from] SELinuxLabelError),
}

/// Maximum errors to be stored per pod in `pod_errors` to prevent unbound
/// growth. Port of `maxPodErrors` (`desired_state_of_world.go:255-259`).
const MAX_POD_ERRORS: usize = 10;

/// Port of `desiredStateOfWorld` (`desired_state_of_world.go:163-178`).
///
/// This cache contains volumes->pods, i.e. a set of all volumes that should be
/// attached to this node and the pods that reference them and should mount the
/// volume. Distinct from the `DesiredStateOfWorld` of the attach/detach
/// controller: they track different objects.
///
/// **Interface.** Upstream splits this into a `DesiredStateOfWorld` interface
/// (`:53-142`) and one unexported implementation. There is exactly one
/// implementation and no fake, so the interface would be pure ceremony in
/// Rust; the 14 interface methods are the 14 public methods below, in
/// declaration order, and their doc comments are upstream's interface
/// comments.
///
/// **Locking.** Upstream embeds one `sync.RWMutex` and takes it across every
/// method, so `volumesToMount` and `podErrors` move together — `AddPodToVolume`
/// reads one and writes the other under a single hold, and `DeletePodFromVolume`
/// writes both. Two independent `RwLock`s would let a reader observe a pod
/// deleted from one and not the other. So the two maps live in one inner struct
/// behind one `RwLock`; `volume_plugin_mgr` and `selinux_translator` are
/// immutable after construction and sit outside it, as upstream's never-written
/// fields effectively do.
pub struct DesiredStateOfWorld {
    state: RwLock<State>,
    /// The volume plugin manager used to create volume plugin objects.
    volume_plugin_mgr: Arc<VolumePluginMgr>,
    /// Translates `v1.SELinuxOptions` to a file SELinux label.
    selinux_translator: Arc<dyn SELinuxLabelTranslator>,
}

/// The lock-guarded half of [`DesiredStateOfWorld`].
struct State {
    /// The set of volumes that should be attached to this node and mounted to
    /// the pods referencing it, keyed by unique volume name.
    volumes_to_mount: HashMap<UniqueVolumeName, VolumeToMountEntry>,
    /// Errors caught by the desired-state-of-world populator about volumes for
    /// a given pod.
    ///
    /// Upstream's `sets.Set[string]` is unordered but `PopPodErrors` returns
    /// `sets.List`, which sorts; a `BTreeSet` gives that sort for free.
    pod_errors: HashMap<UniquePodName, BTreeSet<String>>,
}

/// Port of `volumeToMount` (`desired_state_of_world.go:180-232`) — the value in
/// `volumes_to_mount`, one per volume. Named `VolumeToMountEntry` because the
/// public `VolumeToMount` above already has upstream's exported name.
struct VolumeToMountEntry {
    /// The unique identifier for this volume.
    #[allow(
        dead_code,
        reason = "upstream field; its reader arrives with the reconciler (#1970)"
    )]
    volume_name: UniqueVolumeName,

    /// The set of pods that reference this volume and should mount it once it
    /// is attached, keyed by unique pod name.
    pods_to_mount: HashMap<UniquePodName, PodToMount>,

    /// Indicates that the plugin for this volume implements the
    /// `volume.Attacher` interface.
    plugin_is_attachable: bool,

    /// Indicates that the plugin for this volume implements the
    /// `volume.DeviceMounter` interface.
    plugin_is_device_mountable: bool,

    /// The value of the GID annotation, if present.
    volume_gid_value: String,

    /// Indicates that the volume was successfully added to the `VolumesInUse`
    /// field in the node's status.
    reported_in_use: bool,

    /// The desired upper bound on the size of the volume (if so implemented).
    desired_size_limit: Option<Quantity>,

    /// Desired size of a persistent volume. Usually reflects the size recorded
    /// in `pv.Spec.Capacity`. `None` is upstream's zero `resource.Quantity`,
    /// which its only reader tests with `IsZero()`.
    persistent_volume_size: Option<Quantity>,

    /// The SELinux label that will be applied to the volume using mount
    /// options. If empty, then:
    /// - either the context+label is unknown (assigned randomly by the
    ///   container runtime),
    /// - or the volume plugin responsible for this volume does not support
    ///   mounting with -o context,
    /// - or the volume is not ReadWriteOncePod,
    /// - or the OS does not support SELinux.
    ///
    /// In all cases, the SELinux context does not matter when mounting the
    /// volume.
    effective_selinux_mount_file_label: String,

    /// The SELinux label that would be used if SELinux mount was supported for
    /// all access modes. For RWOP volumes it's the same as
    /// `effective_selinux_mount_file_label`. It is used only to report
    /// potential SELinux mismatch metrics. If empty, then:
    /// - either the context+label is unknown (assigned randomly by the
    ///   container runtime),
    /// - or the volume plugin responsible for this volume does not support
    ///   mounting with -o context,
    /// - or the OS does not support SELinux.
    original_selinux_label: String,
}

/// Port of `podToMount` (`desired_state_of_world.go:234-253`) — the innermost
/// value, one per (volume, pod).
struct PodToMount {
    /// The name of this pod.
    #[allow(
        dead_code,
        reason = "upstream field; its reader arrives with the reconciler (#1970)"
    )]
    pod_name: UniquePodName,

    /// Pod to mount the volume to. Used to create NewMounter.
    pod: Arc<Pod>,

    /// Volume spec containing the specification for this volume. Used to
    /// generate the volume plugin object, and passed to plugin methods. For
    /// non-PVC volumes this is the same as defined in the pod object. For PVC
    /// volumes it is from the dereferenced PV object.
    volume_spec: Arc<OwnedSpec>,

    /// The `podSpec.Volume[x].Name`s of the volume. Plural: one unique volume
    /// can be reached through several entries of the same pod's `volumes`
    /// list, and both names must survive
    /// (`Test_AddPodToVolume_Positive_MultiOuterNames`).
    outer_volume_spec_names: Vec<String>,

    /// Time at which mount was requested.
    mount_request_time: SystemTime,
}

impl DesiredStateOfWorld {
    /// Port of `NewDesiredStateOfWorld` (`desired_state_of_world.go:150-161`).
    pub fn new(
        volume_plugin_mgr: Arc<VolumePluginMgr>,
        selinux_translator: Arc<dyn SELinuxLabelTranslator>,
    ) -> Self {
        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
            register_selinux_metrics();
        }
        Self {
            state: RwLock::new(State {
                volumes_to_mount: HashMap::new(),
                pod_errors: HashMap::new(),
            }),
            volume_plugin_mgr,
            selinux_translator,
        }
    }

    /// Adds the given pod to the given volume in the cache indicating the
    /// specified pod should mount the specified volume.
    ///
    /// A unique `volume_name` is generated from the `volume_spec` and returned
    /// on success. If no volume plugin can support the given `volume_spec` or
    /// more than one plugin can support it, an error is returned. If a volume
    /// with the name `volume_name` does not exist in the list of volumes that
    /// should be attached to this node, the volume is implicitly added. If a
    /// pod with the same unique name already exists under the specified
    /// volume, this is a no-op.
    ///
    /// Port of `desiredStateOfWorld.AddPodToVolume`
    /// (`desired_state_of_world.go:261-403`).
    ///
    /// The "no-op" in upstream's interface comment is loose: the `podToMount`
    /// is rebuilt on every call so the pod object and spec are refreshed (that
    /// is what remount-on-update volumes need). What is preserved is
    /// `mount_request_time`, and only when the plugin does not require remount
    /// (`:361-366`).
    ///
    /// The `klog.Logger` first argument is dropped: this crate logs through
    /// `tracing`, whose macros take the subscriber from the ambient context.
    pub fn add_pod_to_volume(
        &self,
        pod_name: &UniquePodName,
        pod: Arc<Pod>,
        volume_spec: Arc<OwnedSpec>,
        outer_volume_spec_name: &str,
        volume_gid_value: &str,
        selinux_container_contexts: &[Option<SELinuxOptions>],
    ) -> Result<UniqueVolumeName, DesiredStateOfWorldError> {
        let mut state = self.state.write().expect("desired state of world lock");

        let spec = volume_spec.as_spec();
        let volume_plugin = self
            .volume_plugin_mgr
            .find_plugin_by_spec(&spec)
            .map_err(|err| DesiredStateOfWorldError::NoPlugin {
                volume_name: spec.name().to_string(),
                err: err.to_string(),
            })?;
        let volume_plugin_name = get_volume_plugin_name_with_driver(volume_plugin, &spec);
        let access_mode = get_volume_access_mode(&spec);

        // The unique volume name used depends on whether the volume is
        // attachable/device-mountable or not.
        let attachable = is_attachable_volume(&spec, &self.volume_plugin_mgr);
        let device_mountable = is_device_mountable_volume(&spec, &self.volume_plugin_mgr);
        let volume_name = if attachable || device_mountable {
            // For attachable/device-mountable volumes, use the unique volume
            // name as reported by the plugin.
            get_unique_volume_name_from_spec(volume_plugin, &spec).map_err(|err| {
                DesiredStateOfWorldError::UniqueVolumeName {
                    volume_name: spec.name().to_string(),
                    plugin_name: volume_plugin.name().to_string(),
                    err,
                }
            })?
        } else {
            // For non-attachable and non-device-mountable volumes, generate a
            // unique name based on the unique pod name (refer to podUID) and
            // the name of the volume within the pod.
            get_unique_volume_name_from_spec_with_pod(pod_name, volume_plugin, &spec)
        };

        let (selinux_file_label, plugin_supports_selinux_context_mount) = self.get_selinux_label(
            &spec,
            selinux_container_contexts,
            pod.spec.as_ref().and_then(|s| s.security_context.as_ref()),
        )?;
        debug!(
            volume = spec.name(),
            label = selinux_file_label,
            "expected volume SELinux label context"
        );

        if !state.volumes_to_mount.contains_key(&volume_name) {
            let mut size_limit: Option<Quantity> = None;
            if is_local_ephemeral_volume(&volume_spec.volume) {
                let limits = pod_limits(&pod);
                let ephemeral_storage_limit = limits
                    .get(EPHEMERAL_STORAGE)
                    .copied()
                    .unwrap_or_else(|| Quantity::from_value(0, Format::DecimalSI));
                let mut chosen = Quantity::from_value(
                    saturating_i64(ephemeral_storage_limit.value()),
                    Format::BinarySI,
                );
                if let Some(empty_dir_limit) = volume_spec
                    .volume
                    .empty_dir
                    .as_ref()
                    .and_then(|ed| ed.size_limit.as_deref())
                    .and_then(|s| Quantity::parse(s).ok())
                {
                    if empty_dir_limit.value() > 0
                        && (chosen.value() == 0 || empty_dir_limit.value() < chosen.value())
                    {
                        chosen = Quantity::from_value(
                            saturating_i64(empty_dir_limit.value()),
                            Format::BinarySI,
                        );
                    }
                }
                size_limit = Some(chosen);
            }
            let mut effective_selinux_mount_label = selinux_file_label.clone();
            if !volume_supports_selinux_mount(&spec) {
                // Clear SELinux label for the volume with unsupported access modes.
                debug!(
                    volume = spec.name(),
                    "volume does not support SELinux context mount, clearing the expected label"
                );
                effective_selinux_mount_label = String::new();
            }
            if !selinux_file_label.is_empty() {
                SELINUX_VOLUMES_ADMITTED.add(&[&volume_plugin_name, access_mode], 1.0);
            }
            let vmt = VolumeToMountEntry {
                volume_name: volume_name.clone(),
                pods_to_mount: HashMap::new(),
                plugin_is_attachable: attachable,
                plugin_is_device_mountable: device_mountable,
                volume_gid_value: volume_gid_value.to_string(),
                reported_in_use: false,
                desired_size_limit: size_limit,
                // Record desired size of the volume. Upstream reads
                // `spec.PersistentVolume.Spec.Capacity.Storage()`, which is a
                // zero quantity when the key is absent — `None` here.
                persistent_volume_size: spec
                    .persistent_volume
                    .and_then(|pv| pv.spec.capacity.get(STORAGE))
                    .and_then(|s| Quantity::parse(s).ok()),
                effective_selinux_mount_file_label: effective_selinux_mount_label,
                original_selinux_label: selinux_file_label.clone(),
            };
            state.volumes_to_mount.insert(volume_name.clone(), vmt);
        }

        let volume_obj = state
            .volumes_to_mount
            .get(&volume_name)
            .expect("volume was just inserted if absent");
        let old_pod_mount = volume_obj.pods_to_mount.get(pod_name);
        let mut mount_request_time = SystemTime::now();
        let mut outer_volume_spec_names: Vec<String> = Vec::new();
        let existed = old_pod_mount.is_some();
        if let Some(old_pod_mount) = old_pod_mount {
            if !volume_plugin.requires_remount(&spec) {
                mount_request_time = old_pod_mount.mount_request_time;
            }
            outer_volume_spec_names = old_pod_mount.outer_volume_spec_names.clone();
        }
        if !outer_volume_spec_names
            .iter()
            .any(|n| n == outer_volume_spec_name)
        {
            outer_volume_spec_names.push(outer_volume_spec_name.to_string());
        }

        if !existed {
            // The volume exists, but not with this pod.
            // It will be added below as podToMount, now just report SELinux metric.
            if plugin_supports_selinux_context_mount {
                let existing_volume = &state.volumes_to_mount[&volume_name];
                if selinux_file_label != existing_volume.original_selinux_label {
                    let full_err = DesiredStateOfWorldError::ConflictingSELinuxLabels {
                        volume_name: spec.name().to_string(),
                        existing: existing_volume.original_selinux_label.clone(),
                        new: selinux_file_label.clone(),
                    };
                    let supported = volume_supports_selinux_mount(&spec);
                    if let Some(err) = handle_selinux_metric_error(
                        full_err,
                        supported,
                        &SELINUX_VOLUME_CONTEXT_MISMATCH_WARNINGS,
                        &SELINUX_VOLUME_CONTEXT_MISMATCH_ERRORS,
                        &[&volume_plugin_name, access_mode],
                    ) {
                        return Err(err);
                    }
                }
            }
        }

        // Create new podToMount object. If it already exists, it is refreshed
        // with updated values (this is required for volumes that require
        // remounting on pod update, like Downward API volumes).
        state
            .volumes_to_mount
            .get_mut(&volume_name)
            .expect("volume was just inserted if absent")
            .pods_to_mount
            .insert(
                pod_name.clone(),
                PodToMount {
                    pod_name: pod_name.clone(),
                    pod,
                    volume_spec: Arc::clone(&volume_spec),
                    outer_volume_spec_names,
                    mount_request_time,
                },
            );
        Ok(volume_name)
    }

    /// Port of `getSELinuxLabel` (`desired_state_of_world.go:405-437`).
    ///
    /// Returns the SELinux label for a given volume and combination of SELinux
    /// labels, and a bool indicating if the plugin supports mounting the volume
    /// with an SELinux context. It returns an error if the SELinux label cannot
    /// be constructed or when the volume is used with multiple SELinux labels —
    /// unless `handle_selinux_metric_error` consumes it, in which case the
    /// label comes back empty and the caller proceeds.
    fn get_selinux_label(
        &self,
        volume_spec: &Spec<'_>,
        selinux_container_contexts: &[Option<SELinuxOptions>],
        pod_security_context: Option<&rusternetes_common::resources::pod::PodSecurityContext>,
    ) -> Result<(String, bool), DesiredStateOfWorldError> {
        let (label_info, err) = get_mount_selinux_label(
            volume_spec,
            selinux_container_contexts,
            pod_security_context,
            &self.volume_plugin_mgr,
            self.selinux_translator.as_ref(),
        );
        if let Some(err) = err {
            let access_mode = get_volume_access_mode(volume_spec);
            let selinux_supported = volume_supports_selinux_mount(volume_spec);

            match err {
                SELinuxLabelError::Translation(_) => {
                    return match handle_selinux_metric_error(
                        err.into(),
                        selinux_supported,
                        &SELINUX_CONTAINER_CONTEXT_WARNINGS,
                        &SELINUX_CONTAINER_CONTEXT_ERRORS,
                        &[access_mode],
                    ) {
                        Some(err) => Err(err),
                        None => Ok((
                            String::new(),
                            label_info.plugin_supports_selinux_context_mount,
                        )),
                    };
                }
                SELinuxLabelError::MultipleLabels(_) => {
                    // Note the `false`: upstream does NOT propagate
                    // `labelInfo.PluginSupportsSELinuxContextMount` on this arm
                    // (`:431`), unlike the other two.
                    return match handle_selinux_metric_error(
                        err.into(),
                        selinux_supported,
                        &SELINUX_POD_CONTEXT_MISMATCH_WARNINGS,
                        &SELINUX_POD_CONTEXT_MISMATCH_ERRORS,
                        &[access_mode],
                    ) {
                        Some(err) => Err(err),
                        None => Ok((String::new(), false)),
                    };
                }
                SELinuxLabelError::Other(_) => return Err(err.into()),
            }
        }

        Ok((
            label_info.selinux_mount_label,
            label_info.plugin_supports_selinux_context_mount,
        ))
    }

    /// Sets the `reported_in_use` value to true for `reported_volumes`. For
    /// volumes not in `reported_volumes`, the value is reset to false — this
    /// is a full resync, not an additive update. The default value for a newly
    /// created volume is false.
    ///
    /// When true this value indicates that the volume was successfully added
    /// to the `VolumesInUse` field in the node's status. The mount operation
    /// needs to check this value before issuing the operation.
    ///
    /// If a volume in `reported_volumes` does not exist in the list of volumes
    /// that should be attached to this node, it is skipped without error.
    ///
    /// Port of `MarkVolumesReportedInUse` (`desired_state_of_world.go:439-456`).
    pub fn mark_volumes_reported_in_use(&self, reported_volumes: &[UniqueVolumeName]) {
        let mut state = self.state.write().expect("desired state of world lock");

        let reported: HashSet<&UniqueVolumeName> = reported_volumes.iter().collect();

        for (volume_name, volume_obj) in state.volumes_to_mount.iter_mut() {
            volume_obj.reported_in_use = reported.contains(volume_name);
        }
    }

    /// Removes the given pod from the given volume in the cache indicating the
    /// specified pod no longer requires the specified volume.
    ///
    /// If a pod with the same unique name does not exist under the specified
    /// volume, this is a no-op. If a volume with the name `volume_name` does
    /// not exist in the list of attached volumes, this is a no-op. If after
    /// deleting the pod the specified volume contains no other child pods, the
    /// volume is also deleted.
    ///
    /// Port of `DeletePodFromVolume` (`desired_state_of_world.go:458-481`).
    pub fn delete_pod_from_volume(&self, pod_name: &UniquePodName, volume_name: &UniqueVolumeName) {
        let mut state = self.state.write().expect("desired state of world lock");

        // Unconditional, and before the volume lookup: a pod's errors are
        // dropped even when the volume was never in the cache (`:463`).
        state.pod_errors.remove(pod_name);

        let Some(volume_obj) = state.volumes_to_mount.get_mut(volume_name) else {
            return;
        };

        if volume_obj.pods_to_mount.remove(pod_name).is_none() {
            return;
        }

        if volume_obj.pods_to_mount.is_empty() {
            // Delete volume if no child pods left
            state.volumes_to_mount.remove(volume_name);
        }
    }

    /// Updates the last known PV size. This is used for volume expansion and
    /// should only be used for persistent volumes.
    ///
    /// Port of `UpdatePersistentVolumeSize` (`desired_state_of_world.go:483-494`).
    pub fn update_persistent_volume_size(&self, volume_name: &UniqueVolumeName, size: Quantity) {
        let mut state = self.state.write().expect("desired state of world lock");

        if let Some(vol) = state.volumes_to_mount.get_mut(volume_name) {
            vol.persistent_volume_size = Some(size);
        }
    }

    /// Returns true if the given volume exists in the list of volumes that
    /// should be attached to this node.
    ///
    /// Port of `VolumeExists` (`desired_state_of_world.go:496-522`).
    pub fn volume_exists(
        &self,
        volume_name: &UniqueVolumeName,
        selinux_mount_context: &str,
    ) -> bool {
        let state = self.state.read().expect("desired state of world lock");

        let Some(vol) = state.volumes_to_mount.get(volume_name) else {
            return false;
        };
        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
            // Handling two volumes with the same name and different SELinux
            // context as two *different* volumes here. Because if a volume is
            // mounted with an old SELinux context, it must be unmounted first
            // and then mounted again with the new context.
            //
            // This will happen when a pod A with context alpha_t runs and is
            // being terminated by kubelet and its volumes are being torn down,
            // while a pod B with context beta_t is already scheduled on the
            // same node, using the same volumes. The volumes from Pod A must be
            // fully unmounted (incl. UnmountDevice) and mounted with new SELinux
            // mount options for pod B. Without SELinux, kubelet can (and often
            // does) reuse the device mounted for A.
            return vol.effective_selinux_mount_file_label == selinux_mount_context;
        }
        true
    }

    /// Returns true if the given pod exists in the list of `pods_to_mount` for
    /// the given volume in the cache. False if the pod does not exist under
    /// the specified volume, or if the volume does not exist at all.
    ///
    /// Port of `PodExistsInVolume` (`desired_state_of_world.go:524-545`).
    pub fn pod_exists_in_volume(
        &self,
        pod_name: &UniquePodName,
        volume_name: &UniqueVolumeName,
        selinux_mount_option: &str,
    ) -> bool {
        let state = self.state.read().expect("desired state of world lock");

        let Some(volume_obj) = state.volumes_to_mount.get(volume_name) else {
            return false;
        };

        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod)
            && volume_obj.effective_selinux_mount_file_label != selinux_mount_option
        {
            // The volume is in DSW, but with a different SELinux mount option.
            // Report it as unused, so the volume is unmounted and mounted back
            // with the right SELinux option.
            return false;
        }

        volume_obj.pods_to_mount.contains_key(pod_name)
    }

    /// Returns true if the given volume, specified by its volume spec name
    /// (a.k.a. `InnerVolumeSpecName`), exists in the list of volumes that
    /// should be attached to this node under the given pod.
    ///
    /// Port of `VolumeExistsWithSpecName` (`desired_state_of_world.go:547-558`).
    pub fn volume_exists_with_spec_name(
        &self,
        pod_name: &UniquePodName,
        volume_spec_name: &str,
    ) -> bool {
        let state = self.state.read().expect("desired state of world lock");
        for volume_obj in state.volumes_to_mount.values() {
            if let Some(pod_obj) = volume_obj.pods_to_mount.get(pod_name) {
                if pod_obj.volume_spec.name() == volume_spec_name {
                    return true;
                }
            }
        }
        false
    }

    /// Returns the set of pods currently in the desired state of world.
    ///
    /// Port of `GetPods` (`desired_state_of_world.go:560-571`). Upstream's
    /// return type is `map[types.UniquePodName]bool` with every value `true` —
    /// Go's set idiom, and its callers only ever range over the keys or test
    /// membership. `HashSet` says the same thing.
    pub fn get_pods(&self) -> HashSet<UniquePodName> {
        let state = self.state.read().expect("desired state of world lock");

        let mut pod_list = HashSet::new();
        for volume_obj in state.volumes_to_mount.values() {
            for pod_name in volume_obj.pods_to_mount.keys() {
                pod_list.insert(pod_name.clone());
            }
        }
        pod_list
    }

    /// Returns the `UniqueVolumeName`s for the given pod, indexed by
    /// `outerVolumeSpecName`.
    ///
    /// Port of `GetVolumeNamesForPod` (`desired_state_of_world.go:573-584`).
    /// Note it iterates ALL of a pod's outer names per volume, not one — a
    /// single unique volume reachable through two `podSpec.volumes` entries
    /// yields two map entries pointing at it.
    pub fn get_volume_names_for_pod(
        &self,
        pod_name: &UniquePodName,
    ) -> HashMap<String, UniqueVolumeName> {
        let state = self.state.read().expect("desired state of world lock");

        let mut volume_names = HashMap::new();
        for (volume_name, volume_obj) in state.volumes_to_mount.iter() {
            let Some(pod_obj) = volume_obj.pods_to_mount.get(pod_name) else {
                // Go indexes a missing key to the zero `podToMount`, whose
                // `outerVolumeSpecNames` is a nil slice, so the inner loop does
                // not run. Skipping is the same thing.
                continue;
            };
            for outer_volume_spec_name in &pod_obj.outer_volume_spec_names {
                volume_names.insert(outer_volume_spec_name.clone(), volume_name.clone());
            }
        }
        volume_names
    }

    /// Generates and returns a list of volumes that should be attached to this
    /// node and the pods they should be mounted to based on the current desired
    /// state of the world.
    ///
    /// Port of `GetVolumesToMount` (`desired_state_of_world.go:586-616`).
    pub fn get_volumes_to_mount(&self) -> Vec<VolumeToMount> {
        let state = self.state.read().expect("desired state of world lock");

        let mut volumes_to_mount = Vec::with_capacity(state.volumes_to_mount.len());
        for (volume_name, volume_obj) in state.volumes_to_mount.iter() {
            for (pod_name, pod_obj) in volume_obj.pods_to_mount.iter() {
                let vmt = VolumeToMount(operation_executor::VolumeToMount {
                    volume_name: volume_name.clone(),
                    pod_name: pod_name.clone(),
                    pod: Arc::clone(&pod_obj.pod),
                    volume_spec: Arc::clone(&pod_obj.volume_spec),
                    plugin_is_attachable: volume_obj.plugin_is_attachable,
                    plugin_is_device_mountable: volume_obj.plugin_is_device_mountable,
                    outer_volume_spec_names: pod_obj.outer_volume_spec_names.clone(),
                    volume_gid_value: volume_obj.volume_gid_value.clone(),
                    reported_in_use: volume_obj.reported_in_use,
                    mount_request_time: pod_obj.mount_request_time,
                    desired_size_limit: volume_obj.desired_size_limit,
                    selinux_label: volume_obj.effective_selinux_mount_file_label.clone(),
                    // Upstream leaves `DevicePath` at its zero value here; DSW
                    // does not know it (`:593-607` sets no such field).
                    device_path: String::new(),
                    // `if !volumeObj.persistentVolumeSize.IsZero()` (`:609`).
                    desired_persistent_volume_size: volume_obj
                        .persistent_volume_size
                        .filter(|q| !q.is_zero()),
                });
                volumes_to_mount.push(vmt);
            }
        }
        volumes_to_mount
    }

    /// Adds the given error to the given pod in the cache. It will be returned
    /// by a subsequent `pop_pod_errors`. Each error string is stored only once.
    ///
    /// Port of `AddErrorToPod` (`desired_state_of_world.go:618-629`).
    ///
    /// The bound is `<=`, not `<`, so a pod can hold `MAX_POD_ERRORS + 1`
    /// distinct errors: the insert that takes the set from 10 to 11 passes the
    /// check. Ported as written — the cap exists to stop unbounded growth, not
    /// to be exactly 10, and a `<` here would be a silent divergence.
    pub fn add_error_to_pod(&self, pod_name: &UniquePodName, err: &str) {
        let mut state = self.state.write().expect("desired state of world lock");

        if let Some(errs) = state.pod_errors.get_mut(pod_name) {
            if errs.len() <= MAX_POD_ERRORS {
                errs.insert(err.to_string());
            }
            return;
        }
        state
            .pod_errors
            .insert(pod_name.clone(), BTreeSet::from([err.to_string()]));
    }

    /// Returns accumulated errors on a given pod and clears them.
    ///
    /// Port of `PopPodErrors` (`desired_state_of_world.go:631-640`). Upstream
    /// returns `sets.List(errs)`, which is sorted; so is this.
    pub fn pop_pod_errors(&self, pod_name: &UniquePodName) -> Vec<String> {
        let mut state = self.state.write().expect("desired state of world lock");

        match state.pod_errors.remove(pod_name) {
            Some(errs) => errs.into_iter().collect(),
            None => Vec::new(),
        }
    }

    /// Returns the names of pods that have stored errors. Non-destructive.
    ///
    /// Port of `GetPodsWithErrors` (`desired_state_of_world.go:642-651`).
    pub fn get_pods_with_errors(&self) -> Vec<UniquePodName> {
        let state = self.state.read().expect("desired state of world lock");

        state.pod_errors.keys().cloned().collect()
    }

    /// Updates the volume's attachability for a given volume. No-op if the
    /// volume is not in the cache.
    ///
    /// Port of `MarkVolumeAttachability` (`desired_state_of_world.go:653-662`).
    pub fn mark_volume_attachability(&self, volume_name: &UniqueVolumeName, attachable: bool) {
        let mut state = self.state.write().expect("desired state of world lock");
        let Some(volume_obj) = state.volumes_to_mount.get_mut(volume_name) else {
            return;
        };
        volume_obj.plugin_is_attachable = attachable;
    }
}

/// `v1.ResourceEphemeralStorage`
/// (`staging/src/k8s.io/api/core/v1/types.go`).
const EPHEMERAL_STORAGE: &str = "ephemeral-storage";

/// `v1.ResourceStorage` — the key `ResourceList.Storage()` reads.
const STORAGE: &str = "storage";

/// `resource.Quantity.Value()` is an `int64` upstream; ours is an `i128`
/// because the parser keeps a wider mantissa. Clamping is the closest thing to
/// Go's behaviour at the boundary and cannot be reached by any quantity the
/// API accepts.
fn saturating_i64(v: i128) -> i64 {
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Based on `is_rwop`, bump the right warning / error metric and either consume
/// the error or return it.
///
/// Port of `handleSELinuxMetricError` (`desired_state_of_world.go:664-675`).
/// Returning `Option` rather than `error` makes Go's "nil means consumed"
/// explicit.
fn handle_selinux_metric_error(
    err: DesiredStateOfWorldError,
    selinux_supported: bool,
    warning_metric: &GaugeVec,
    error_metric: &GaugeVec,
    label_values: &[&str],
) -> Option<DesiredStateOfWorldError> {
    if selinux_supported {
        error_metric.add(label_values, 1.0);
        return Some(err);
    }

    // This is not an error yet, but it will be when support for other access
    // modes is added.
    warning_metric.add(label_values, 1.0);
    debug!(
        %err,
        "Please report this error in https://github.com/kubernetes/enhancements/issues/1710, \
         together with full Pod yaml file"
    );
    None
}

/// Return the volume plugin name, together with the CSI driver name if it's a
/// CSI volume.
///
/// Port of `getVolumePluginNameWithDriver` (`desired_state_of_world.go:677-693`).
fn get_volume_plugin_name_with_driver(plugin: &dyn VolumePlugin, spec: &Spec<'_>) -> String {
    let plugin_name = plugin.name();
    if plugin_name != crate::pod_dirs::plugin::CSI {
        return plugin_name.to_string();
    }

    // It's a CSI volume
    let driver_name = get_csi_driver_name(spec).unwrap_or_else(|err| {
        // In theory this is unreachable - such volume would not pass validation.
        debug!(%err, "failed to get CSI driver name from volume spec");
        "unknown".to_string()
    });
    // `/` is used to separate plugin + CSI driver in util::get_unique_volume_name() too
    format!("{plugin_name}/{driver_name}")
}

/// Port of `csi.GetCSIDriverName` (`pkg/volume/csi/csi_util.go:182-196`):
/// the inline source's driver if present, else the PV source's, else an error.
fn get_csi_driver_name(spec: &Spec<'_>) -> Result<String, String> {
    if let Some(vol_src) = &spec.volume.csi {
        return Ok(vol_src.driver.clone());
    }
    if let Some(pv_src) = spec.persistent_volume.and_then(|pv| pv.spec.csi.as_ref()) {
        return Ok(pv_src.driver.clone());
    }
    Err("volume source not found in volume.Spec".to_string())
}

/// Port of `getVolumeAccessMode` (`desired_state_of_world.go:695-716`).
///
/// A metric label, not a decision: it reports only the "highest" access mode in
/// the order RWX > ROX > RWO > RWOP even when several are set, and `""` when
/// none matches — which API validation should make unreachable.
fn get_volume_access_mode(spec: &Spec<'_>) -> &'static str {
    let Some(pv) = spec.persistent_volume else {
        // In-line volumes in pod do not have a specific access mode, using "inline".
        return "inline";
    };
    if contains_access_mode(
        &pv.spec.access_modes,
        &PersistentVolumeAccessMode::ReadWriteMany,
    ) {
        return "RWX";
    }
    if contains_access_mode(
        &pv.spec.access_modes,
        &PersistentVolumeAccessMode::ReadOnlyMany,
    ) {
        return "ROX";
    }
    if contains_access_mode(
        &pv.spec.access_modes,
        &PersistentVolumeAccessMode::ReadWriteOnce,
    ) {
        return "RWO";
    }
    if contains_access_mode(
        &pv.spec.access_modes,
        &PersistentVolumeAccessMode::ReadWriteOncePod,
    ) {
        return "RWOP";
    }
    // This should not happen, validation does not allow empty or unknown AccessModes.
    ""
}

#[cfg(test)]
mod tests {
    //! Ports of `pkg/kubelet/volumemanager/cache/desired_state_of_world_test.go`.
    //! Each test keeps the name of the Go test it came from.
    //!
    //! **Two substitutions run through every test here.**
    //!
    //! 1. *GCE PersistentDisk -> NFS.* Upstream's fixtures use
    //!    `v1.GCEPersistentDiskVolumeSource{PDName: "fake-device1"}` purely as
    //!    "a volume source whose identity the fake plugin can read". This
    //!    project's `Volume` has no `gcePersistentDisk` field, so `nfs` plays
    //!    that role and `nfs.path` carries the `fake-deviceN` identity.
    //!    Nothing about the volume kind matters to `DesiredStateOfWorld`.
    //!
    //! 2. *PV-only specs.* The SELinux tests build
    //!    `&volume.Spec{PersistentVolume: ...}` with a nil `Volume`, which
    //!    [`Spec`] cannot express (see [`Spec::name`]). Those tests pass the
    //!    pod's own PVC volume entry alongside the PV, so `spec.name()` is the
    //!    pod-side name (`"volume-name"`) rather than upstream's PV name
    //!    (`"basicPV"`). Only the generated string differs; every assertion is
    //!    on behaviour that does not read it.

    use super::*;
    use crate::volume_plugins::plugin::Mounter;
    use crate::volume_plugins::util::get_unique_pod_name;
    use crate::volume_plugins::util::selinux::FakeSELinuxLabelTranslator;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use rusternetes_common::feature_gates::with_feature;
    use rusternetes_common::resources::{PersistentVolume, Volume};
    use serde_json::json;
    use std::collections::HashSet;

    /// Port of `volumetesting.FakeVolumePlugin`
    /// (`pkg/volume/testing/testing.go:~250-300`), trimmed to what the
    /// `DesiredStateOfWorld` tests exercise, with
    /// `FakeBasicVolumePlugin.CanSupport` (`testing.go:547-552`) folded in as
    /// `can_support_prefix`.
    struct FakeVolumePlugin {
        plugin_name: &'static str,
        /// `None` is upstream's nil `CanSupportFn`, i.e. support everything
        /// (`testing.go:279-285`). `Some(prefix)` is
        /// `FakeBasicVolumePlugin.CanSupport`: `strings.HasPrefix(spec.Name(), prefix)`.
        can_support_prefix: Option<&'static str>,
        /// `FakeVolumePlugin.SupportsRemount` (`testing.go:287-289`).
        supports_remount: bool,
        /// `FakeVolumePlugin.SupportsSELinux` (`testing.go:295-297`).
        supports_selinux: bool,
        /// Stands for registering the plugin as a
        /// `FakeAttachableVolumePlugin` (`testing.go:609-...`).
        attachable: bool,
        /// Stands for registering the plugin as a
        /// `FakeDeviceMountableVolumePlugin`.
        device_mountable: bool,
    }

    impl FakeVolumePlugin {
        fn basic(plugin_name: &'static str) -> Self {
            Self {
                plugin_name,
                can_support_prefix: Some(plugin_name),
                supports_remount: false,
                supports_selinux: false,
                attachable: false,
                device_mountable: false,
            }
        }
    }

    #[async_trait]
    impl VolumePlugin for FakeVolumePlugin {
        fn name(&self) -> &'static str {
            self.plugin_name
        }

        /// `FakeVolumePlugin.GetVolumeName` (`testing.go:258-277`), with NFS in
        /// GCE PD's place and the `if volumeName == "" { volumeName = spec.Name() }`
        /// fallback that makes the empty-`VolumeSource` fixtures work.
        fn get_volume_name(&self, spec: &Spec<'_>) -> Result<String> {
            let mut volume_name = String::new();
            if let Some(nfs) = &spec.volume.nfs {
                volume_name = nfs.path.clone();
            } else if let Some(nfs) = spec.persistent_volume.and_then(|pv| pv.spec.nfs.as_ref()) {
                volume_name = nfs.path.clone();
            } else if let Some(csi) = &spec.volume.csi {
                volume_name = csi.driver.clone();
            }
            if volume_name.is_empty() {
                volume_name = spec.name().to_string();
            }
            Ok(volume_name)
        }

        fn can_support(&self, spec: &Spec<'_>) -> bool {
            match self.can_support_prefix {
                Some(prefix) => spec.name().starts_with(prefix),
                None => true,
            }
        }

        fn requires_remount(&self, _spec: &Spec<'_>) -> bool {
            self.supports_remount
        }

        fn supports_selinux_context_mount(&self, _spec: &Spec<'_>) -> Result<bool> {
            Ok(self.supports_selinux)
        }

        fn can_attach(&self, _spec: &Spec<'_>) -> bool {
            self.attachable
        }

        fn can_device_mount(&self, _spec: &Spec<'_>) -> bool {
            self.device_mountable
        }

        async fn new_mounter(&self, _spec: &Spec<'_>, _pod: &Pod) -> Result<Box<dyn Mounter>> {
            Err(anyhow!("DSW tests never mount"))
        }
    }

    /// Port of `volumetesting.GetTestKubeletVolumePluginMgr`
    /// (`pkg/volume/testing/testing.go:1671-1680`): one `FakeVolumePlugin`
    /// named `fake-plugin` that supports every spec and is both attachable and
    /// device-mountable, so every volume gets the plugin-scoped unique name.
    fn get_test_kubelet_volume_plugin_mgr() -> Arc<VolumePluginMgr> {
        Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "fake-plugin",
            can_support_prefix: None,
            supports_remount: false,
            supports_selinux: false,
            attachable: true,
            device_mountable: true,
        })]))
    }

    fn new_dsw(mgr: Arc<VolumePluginMgr>) -> DesiredStateOfWorld {
        DesiredStateOfWorld::new(mgr, Arc::new(FakeSELinuxLabelTranslator))
    }

    fn pod(value: serde_json::Value) -> Arc<Pod> {
        Arc::new(serde_json::from_value(value).expect("pod fixture"))
    }

    fn spec_of(pod: &Pod, index: usize) -> Arc<OwnedSpec> {
        Arc::new(OwnedSpec {
            volume: pod
                .spec
                .as_ref()
                .expect("spec")
                .volumes
                .as_ref()
                .expect("volumes")[index]
                .clone(),
            persistent_volume: None,
        })
    }

    /// `nfs` stands in for upstream's `gcePersistentDisk`; `path` carries the
    /// `fake-deviceN` identity the fake plugin reads.
    fn nfs_volume(name: &str, device: &str) -> serde_json::Value {
        json!({ "name": name, "nfs": { "server": "fake-server", "path": device } })
    }

    // ---- verify helpers, ports of `desired_state_of_world_test.go:1429-1576` ----

    fn verify_volume_exists_dsw(
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            dsw.volume_exists(expected_volume_name, expected_selinux_context),
            "VolumeExists({expected_volume_name}) failed. Expected: <true> Actual: <false>"
        );
    }

    fn verify_volume_doesnt_exist(
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.volume_exists(expected_volume_name, expected_selinux_context),
            "VolumeExists({expected_volume_name}) returned incorrect value. \
             Expected: <false> Actual: <true>"
        );
    }

    fn verify_volume_exists_in_volumes_to_mount(
        expected_volume_name: &UniqueVolumeName,
        expected_outer_names: &[&str],
        expect_reported_in_use: bool,
        dsw: &DesiredStateOfWorld,
    ) {
        let volumes_to_mount = dsw.get_volumes_to_mount();
        for volume in &volumes_to_mount {
            if &volume.volume_name == expected_volume_name {
                assert_eq!(
                    volume.reported_in_use, expect_reported_in_use,
                    "Found volume {expected_volume_name} in the list of VolumesToMount, \
                     but ReportedInUse incorrect"
                );
                let mut names = volume.outer_volume_spec_names.clone();
                names.sort();
                assert_eq!(
                    names, expected_outer_names,
                    "Expected outer volume spec names to be {expected_outer_names:?}"
                );
                return;
            }
        }
        panic!(
            "Could not find volume {expected_volume_name} in the list of desired state of \
             world volumes to mount"
        );
    }

    fn verify_volume_doesnt_exist_in_volumes_to_mount(
        volume_to_check: &UniqueVolumeName,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.get_volumes_to_mount()
                .iter()
                .any(|v| &v.volume_name == volume_to_check),
            "Found volume {volume_to_check} in the list of desired state of world volumes \
             to mount. Expected it not to exist."
        );
    }

    fn verify_pod_exists_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            dsw.pod_exists_in_volume(
                expected_pod_name,
                expected_volume_name,
                expected_selinux_context
            ),
            "DSW PodExistsInVolume returned incorrect value. Expected: <true> Actual: <false>"
        );
    }

    fn verify_pod_doesnt_exist_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_name: &UniqueVolumeName,
        expected_selinux_context: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.pod_exists_in_volume(
                expected_pod_name,
                expected_volume_name,
                expected_selinux_context
            ),
            "DSW PodExistsInVolume returned incorrect value. Expected: <false> Actual: <true>"
        );
    }

    fn verify_volume_exists_with_spec_name_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_spec_name: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            dsw.volume_exists_with_spec_name(expected_pod_name, expected_volume_spec_name),
            "DSW VolumeExistsWithSpecName returned incorrect value. \
             Expected: <true> Actual: <false>"
        );
    }

    fn verify_volume_doesnt_exist_with_spec_name_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_volume_spec_name: &str,
        dsw: &DesiredStateOfWorld,
    ) {
        assert!(
            !dsw.volume_exists_with_spec_name(expected_pod_name, expected_volume_spec_name),
            "DSW VolumeExistsWithSpecName returned incorrect value. \
             Expected: <false> Actual: <true>"
        );
    }

    /// Port of `verifyDesiredSizeLimitInVolumeDsw` (`:1550-1576`).
    fn verify_desired_size_limit_in_volume_dsw(
        expected_pod_name: &UniquePodName,
        expected_desired_size_map: &[(&str, i128)],
        dsw: &DesiredStateOfWorld,
    ) {
        let volumes_to_mount = dsw.get_volumes_to_mount();
        for (volume_name, expected_desired_size) in expected_desired_size_map {
            assert!(
                dsw.volume_exists_with_spec_name(expected_pod_name, volume_name),
                "DSW VolumeExistsWithSpecName returned incorrect value for {volume_name}"
            );
            for v in &volumes_to_mount {
                if v.volume_spec.name() == *volume_name && &v.pod_name == expected_pod_name {
                    let actual = v.desired_size_limit.map(|q| q.value());
                    assert_eq!(
                        actual,
                        Some(*expected_desired_size),
                        "Found volume {volume_name} in the list of VolumesToMount, but \
                         DesiredSizeLimit incorrect"
                    );
                }
            }
        }
    }

    // ---------------------------------------------------------------- tests

    /// <- `Test_AddPodToVolume_Positive_NewPodNewVolume` (`:42`).
    #[test]
    fn test_add_pod_to_volume_positive_new_pod_new_volume() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod3", "uid": "pod3uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));

        let volume_spec = spec_of(&pod, 0);
        let pod_name = get_unique_pod_name(&pod);

        let generated_volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        verify_volume_exists_dsw(&generated_volume_name, "", &dsw);
        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name"],
            false,
            &dsw,
        );
        verify_pod_exists_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);
        verify_volume_exists_with_spec_name_in_volume_dsw(&pod_name, volume_spec.name(), &dsw);
    }

    /// <- `Test_AddPodToVolume_Positive_ExistingPodExistingVolume` (`:89`).
    ///
    /// Pins that a re-add does not create a duplicate entry and does not change
    /// the generated unique name.
    #[test]
    fn test_add_pod_to_volume_positive_existing_pod_existing_volume() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod4", "uid": "pod4uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));

        let volume_spec = spec_of(&pod, 0);
        let pod_name = get_unique_pod_name(&pod);

        let add = || {
            dsw.add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed")
        };
        let generated_volume_name = add();
        let generated_volume_name2 = add();

        assert_eq!(
            generated_volume_name, generated_volume_name2,
            "AddPodToVolume should generate same names"
        );
        verify_volume_exists_dsw(&generated_volume_name, "", &dsw);
        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name"],
            false,
            &dsw,
        );
        verify_pod_exists_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);
        verify_volume_exists_with_spec_name_in_volume_dsw(&pod_name, volume_spec.name(), &dsw);
    }

    /// <- `Test_AddPodToVolume_Positive_NamesForDifferentPodsAndDifferentVolumes` (`:143`).
    ///
    /// The attachable / device-mountable / neither branch of the unique-name
    /// construction: the first two make two pods share one cache entry, the
    /// third gives each pod its own.
    #[test]
    fn test_add_pod_to_volume_positive_names_for_different_pods_and_different_volumes() {
        let mgr = Arc::new(VolumePluginMgr::new(vec![
            Box::new(FakeVolumePlugin::basic("basic")),
            Box::new(FakeVolumePlugin {
                device_mountable: true,
                ..FakeVolumePlugin::basic("device-mountable")
            }),
            Box::new(FakeVolumePlugin {
                attachable: true,
                device_mountable: true,
                ..FakeVolumePlugin::basic("attachable")
            }),
        ]));
        let dsw = new_dsw(mgr);

        // (volume name, same unique name across the two pods?)
        let testcases = [
            ("basic", false),
            ("device-mountable", true),
            ("attachable", true),
        ];

        for (volume_name, same) in testcases {
            let pod1 = pod(json!({
                "metadata": { "name": "pod1", "uid": "pod1uid" },
                "spec": { "volumes": [{ "name": volume_name }] },
            }));
            let pod2 = pod(json!({
                "metadata": { "name": "pod2", "uid": "pod2uid" },
                "spec": { "volumes": [{ "name": volume_name }] },
            }));
            let spec1 = spec_of(&pod1, 0);
            let spec2 = spec_of(&pod2, 0);

            let name1 = dsw
                .add_pod_to_volume(
                    &get_unique_pod_name(&pod1),
                    Arc::clone(&pod1),
                    Arc::clone(&spec1),
                    spec1.name(),
                    "",
                    &[],
                )
                .unwrap_or_else(|e| panic!("test {volume_name:?}: AddPodToVolume failed: {e}"));
            let name2 = dsw
                .add_pod_to_volume(
                    &get_unique_pod_name(&pod2),
                    Arc::clone(&pod2),
                    Arc::clone(&spec2),
                    spec2.name(),
                    "",
                    &[],
                )
                .unwrap_or_else(|e| panic!("test {volume_name:?}: AddPodToVolume failed: {e}"));

            if same {
                assert_eq!(
                    name1, name2,
                    "test {volume_name:?}: AddPodToVolume should generate same names"
                );
            } else {
                assert_ne!(
                    name1, name2,
                    "test {volume_name:?}: AddPodToVolume should generate different names"
                );
            }
        }
    }

    /// <- `Test_AddPodToVolume_Positive_MultiOuterNames` (`:304`).
    ///
    /// Two `podSpec.volumes` entries of ONE pod resolving to the same unique
    /// volume: both outer names must survive against the single entry. A port
    /// that stores one `outer_volume_spec_name` string fails here.
    #[test]
    fn test_add_pod_to_volume_positive_multi_outer_names() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod5", "uid": "pod5uid" },
            "spec": { "volumes": [
                nfs_volume("volume-name-1", "fake-device1"),
                nfs_volume("volume-name-2", "fake-device1"),
            ] },
        }));

        let spec1 = spec_of(&pod, 0);
        let spec2 = spec_of(&pod, 1);
        let pod_name = get_unique_pod_name(&pod);

        dsw.add_pod_to_volume(
            &pod_name,
            Arc::clone(&pod),
            Arc::clone(&spec1),
            spec1.name(),
            "",
            &[],
        )
        .expect("AddPodToVolume failed");
        let generated_volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&spec2),
                spec2.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name-1", "volume-name-2"],
            false,
            &dsw,
        );
    }

    /// <- `Test_DeletePodFromVolume_Positive_PodExistsVolumeExists` (`:360`).
    ///
    /// Pins the cascading delete: removing the last pod removes the volume.
    #[test]
    fn test_delete_pod_from_volume_positive_pod_exists_volume_exists() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod = pod(json!({
            "metadata": { "name": "pod3", "uid": "pod3uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));

        let volume_spec = spec_of(&pod, 0);
        let pod_name = get_unique_pod_name(&pod);
        let generated_volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&pod),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");
        verify_volume_exists_dsw(&generated_volume_name, "", &dsw);
        verify_volume_exists_in_volumes_to_mount(
            &generated_volume_name,
            &["volume-name"],
            false,
            &dsw,
        );
        verify_pod_exists_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);

        dsw.delete_pod_from_volume(&pod_name, &generated_volume_name);

        verify_volume_doesnt_exist(&generated_volume_name, "", &dsw);
        verify_volume_doesnt_exist_in_volumes_to_mount(&generated_volume_name, &dsw);
        verify_pod_doesnt_exist_in_volume_dsw(&pod_name, &generated_volume_name, "", &dsw);
        verify_volume_doesnt_exist_with_spec_name_in_volume_dsw(
            &pod_name,
            volume_spec.name(),
            &dsw,
        );
    }

    /// <- `Test_MarkVolumesReportedInUse_Positive_NewPodNewVolume` (`:414`).
    ///
    /// Pins the full-resync semantics: a volume left out of the list is reset
    /// to `false`, not left alone.
    #[test]
    fn test_mark_volumes_reported_in_use_positive_new_pod_new_volume() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());

        let mut pods = Vec::new();
        let mut specs = Vec::new();
        let mut pod_names = Vec::new();
        let mut generated = Vec::new();
        for i in 1..=3 {
            let p = pod(json!({
                "metadata": { "name": format!("pod{i}"), "uid": format!("pod{i}uid") },
                "spec": { "volumes": [
                    nfs_volume(&format!("volume{i}-name"), &format!("fake-device{i}")),
                ] },
            }));
            let spec = spec_of(&p, 0);
            let pod_name = get_unique_pod_name(&p);
            let name = dsw
                .add_pod_to_volume(
                    &pod_name,
                    Arc::clone(&p),
                    Arc::clone(&spec),
                    spec.name(),
                    "",
                    &[],
                )
                .expect("AddPodToVolume failed");
            pods.push(p);
            specs.push(spec);
            pod_names.push(pod_name);
            generated.push(name);
        }

        let assert_only = |reported_index: usize, dsw: &DesiredStateOfWorld| {
            for i in 0..3 {
                verify_volume_exists_dsw(&generated[i], "", dsw);
                verify_volume_exists_in_volumes_to_mount(
                    &generated[i],
                    &[&format!("volume{}-name", i + 1)],
                    i == reported_index,
                    dsw,
                );
                verify_pod_exists_in_volume_dsw(&pod_names[i], &generated[i], "", dsw);
            }
        };

        dsw.mark_volumes_reported_in_use(&[generated[1].clone()]);
        assert_only(1, &dsw);

        dsw.mark_volumes_reported_in_use(&[generated[2].clone()]);
        assert_only(2, &dsw);
    }

    /// <- `Test_AddPodToVolume_WithEmptyDirSizeLimit` (`:542`).
    ///
    /// The desired size limit is the pod's ephemeral-storage limit, lowered by
    /// the emptyDir's own `sizeLimit` only when that is smaller (or when the
    /// pod has no limit at all).
    #[test]
    fn test_add_pod_to_volume_with_empty_dir_size_limit() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        const GI: i128 = 1024 * 1024 * 1024;

        let pod1 = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1uid" },
            "spec": {
                "containers": [
                    { "name": "c1", "resources": { "limits": { "ephemeral-storage": "1Gi" } } },
                    { "name": "c2", "resources": { "limits": { "ephemeral-storage": "1Gi" } } },
                ],
                "volumes": [
                    { "name": "emptyDir1", "emptyDir": { "sizeLimit": "1Gi" } },
                    { "name": "emptyDir2", "emptyDir": { "sizeLimit": "2Gi" } },
                    { "name": "emptyDir3", "emptyDir": { "sizeLimit": "3Gi" } },
                    { "name": "emptyDir4", "emptyDir": {} },
                ],
            },
        }));
        let pod1_name = get_unique_pod_name(&pod1);
        let pod1_expected = [
            ("emptyDir1", GI),
            ("emptyDir2", 2 * GI),
            ("emptyDir3", 2 * GI),
            ("emptyDir4", 2 * GI),
        ];

        let pod2 = pod(json!({
            "metadata": { "name": "pod2", "uid": "pod2uid" },
            "spec": {
                "volumes": [
                    { "name": "emptyDir5", "emptyDir": { "sizeLimit": "1Gi" } },
                    { "name": "emptyDir6", "emptyDir": { "sizeLimit": "2Gi" } },
                    { "name": "emptyDir7", "emptyDir": { "sizeLimit": "3Gi" } },
                    { "name": "emptyDir8", "emptyDir": {} },
                ],
            },
        }));
        let pod2_name = get_unique_pod_name(&pod2);
        let pod2_expected = [
            ("emptyDir5", GI),
            ("emptyDir6", 2 * GI),
            ("emptyDir7", 3 * GI),
            ("emptyDir8", 0),
        ];

        for (p, name) in [(&pod1, &pod1_name), (&pod2, &pod2_name)] {
            let count = p.spec.as_ref().unwrap().volumes.as_ref().unwrap().len();
            for i in 0..count {
                let spec = spec_of(p, i);
                dsw.add_pod_to_volume(name, Arc::clone(p), Arc::clone(&spec), spec.name(), "", &[])
                    .expect("AddPodToVolume failed");
            }
        }

        verify_desired_size_limit_in_volume_dsw(&pod1_name, &pod1_expected, &dsw);
        verify_desired_size_limit_in_volume_dsw(&pod2_name, &pod2_expected, &dsw);
    }

    /// <- `TestGetVolumeNamesForPod` (`:1333`).
    ///
    /// The returned map is keyed by OUTER name, so one unique volume reached
    /// through two of a pod's `volumes` entries appears twice.
    #[test]
    fn test_get_volume_names_for_pod() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());

        let pod1 = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1-uid" },
            "spec": { "volumes": [
                nfs_volume("volume1-name", "fake-device1"),
                nfs_volume("volume1-dup", "fake-device1"),
                nfs_volume("volume3-name", "fake-device3"),
            ] },
        }));
        let pod2 = pod(json!({
            "metadata": { "name": "pod2", "uid": "pod2-uid" },
            "spec": { "volumes": [nfs_volume("volume2-name", "fake-device2")] },
        }));

        let add_volume = |p: &Arc<Pod>, index: usize| {
            let spec = spec_of(p, index);
            dsw.add_pod_to_volume(
                &get_unique_pod_name(p),
                Arc::clone(p),
                Arc::clone(&spec),
                spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");
        };
        add_volume(&pod1, 0);
        add_volume(&pod1, 1);
        add_volume(&pod1, 2);
        add_volume(&pod2, 0);

        let expect = |p: &Arc<Pod>, expected: &[(&str, &str)]| {
            let actual = dsw.get_volume_names_for_pod(&get_unique_pod_name(p));
            let expected: HashMap<String, UniqueVolumeName> = expected
                .iter()
                .map(|(k, v)| ((*k).to_string(), UniqueVolumeName((*v).to_string())))
                .collect();
            assert_eq!(
                actual, expected,
                "GetVolumeNamesForPod returned incorrect value"
            );
        };
        expect(
            &pod1,
            &[
                ("volume1-name", "fake-plugin/fake-device1"),
                ("volume1-dup", "fake-plugin/fake-device1"),
                ("volume3-name", "fake-plugin/fake-device3"),
            ],
        );
        expect(&pod2, &[("volume2-name", "fake-plugin/fake-device2")]);
    }

    // ------------------------------------------------------------- SELinux

    /// The gates a Go SELinux test case names, resolved to the value the Go
    /// test binary actually runs with.
    ///
    /// `featuregatetesting.SetFeatureGateDuringTest` only forces the NAMED
    /// gates to `true`; the rest keep their upstream defaults, which for
    /// v1.35 are `SELinuxMountReadWriteOncePod = true`,
    /// `SELinuxChangePolicy = true`, `SELinuxMount = false`
    /// (`pkg/features/kube_features.go:1733-1747`). So the only gate the
    /// `featureGates` column actually varies is `SELinuxMount`; the other two
    /// are on in every case either way. Set here explicitly so the test does
    /// not depend on whatever a sibling test left in the process-wide registry.
    fn selinux_gates(selinux_mount: bool) -> (impl Drop, impl Drop, impl Drop) {
        (
            with_feature(Feature::SELinuxMountReadWriteOncePod, true),
            with_feature(Feature::SELinuxChangePolicy, true),
            with_feature(Feature::SELinuxMount, selinux_mount),
        )
    }

    const COMPLETE_SELINUX_LABEL: &str = "system_u:object_r:container_file_t:s0:c1,c2";

    fn complete_selinux_opts() -> SELinuxOptions {
        serde_json::from_value(json!({
            "user": "system_u", "role": "object_r", "type": "container_t", "level": "s0:c1,c2",
        }))
        .unwrap()
    }

    fn incomplete_selinux_opts() -> SELinuxOptions {
        serde_json::from_value(json!({ "level": "s0:c1,c2" })).unwrap()
    }

    fn conflicting_selinux_opts() -> SELinuxOptions {
        serde_json::from_value(json!({
            "user": "system_u", "role": "object_r", "type": "container_t", "level": "s0:c98,c99",
        }))
        .unwrap()
    }

    /// The pod + PV pair the SELinux tests share. See the module doc for why
    /// the inline PVC volume is carried alongside the PV.
    fn selinux_fixture(
        pod_name: &str,
        pod_uid: &str,
        change_policy: Option<&str>,
        access_mode: &str,
    ) -> (Arc<Pod>, Arc<OwnedSpec>) {
        let p = pod(json!({
            "metadata": { "name": pod_name, "uid": pod_uid },
            "spec": {
                "securityContext": { "seLinuxChangePolicy": change_policy },
                "volumes": [{
                    "name": "volume-name",
                    "persistentVolumeClaim": { "claimName": "myClaim" },
                }],
            },
        }));
        let volume: Volume = p.spec.as_ref().unwrap().volumes.as_ref().unwrap()[0].clone();
        let pv: PersistentVolume = serde_json::from_value(json!({
            "metadata": { "name": "basicPV" },
            "spec": { "accessModes": [access_mode] },
        }))
        .unwrap();
        (
            p,
            Arc::new(OwnedSpec {
                volume,
                persistent_volume: Some(pv),
            }),
        )
    }

    fn selinux_plugin_mgr(supports_selinux: bool) -> Arc<VolumePluginMgr> {
        Arc::new(VolumePluginMgr::new(vec![Box::new(FakeVolumePlugin {
            plugin_name: "basic",
            // Upstream uses `FakeBasicVolumePlugin`, whose CanSupport is a
            // prefix match on `spec.Name()` == "basicPV". Our spec name is the
            // pod-side "volume-name" (module doc, substitution 2), and this is
            // the only registered plugin, so "support everything" is the same
            // registry behaviour.
            can_support_prefix: None,
            supports_remount: false,
            supports_selinux,
            attachable: false,
            device_mountable: true,
        })]))
    }

    /// <- `Test_AddPodToVolume_SELinuxSinglePod` (`:680`).
    #[test]
    #[serial_test::serial]
    fn test_add_pod_to_volume_selinux_single_pod() {
        // (name, selinux_mount gate, plugin supports SELinux, access mode,
        //  pod SELinuxOptions, change policy, expect error, expected label)
        type Case = (
            &'static str,
            bool,
            bool,
            &'static str,
            Option<SELinuxOptions>,
            Option<&'static str>,
            bool,
            &'static str,
        );
        let tests: Vec<Case> = vec![
            (
                "RWOP: ReadWriteOncePod with plugin that supports SELinux mount",
                false,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                None,
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            // kubelet fills the missing SELinuxOptions fields
            (
                "RWOP: ReadWriteOncePod incomplete SELinuxOptions",
                false,
                true,
                "ReadWriteOncePod",
                Some(incomplete_selinux_opts()),
                None,
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            (
                "RWOP: ReadWriteOncePod no SELinuxOptions",
                false,
                true,
                "ReadWriteOncePod",
                None,
                None,
                false,
                "",
            ),
            // The plugin does not support SELinux
            (
                "RWOP: ReadWriteOncePod with plugin that does not support SELinux mount",
                false,
                false,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                None,
                false,
                "",
            ),
            (
                "RWOP: ReadWriteMany with plugin that supports SELinux mount",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                None,
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy: ReadWriteMany with the default policy",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                None,
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy: ReadWriteMany with Recursive policy",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            // still not supported for RWX
            (
                "RWOP+ChangePolicy: ReadWriteMany with MountOption policy",
                false,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                "",
            ),
            // "Recursive" is applied to RWOP volumes too
            (
                "RWOP+ChangePolicy: ReadWriteOncePod with Recursive policy",
                false,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            // the policy is ignored, but mounting with SELinux is the default
            (
                "RWOP+ChangePolicy: ReadWriteOncePod with MountOption policy",
                false,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteMany with the default policy",
                true,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                None,
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteMany with Recursive policy",
                true,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteMany with MountOption policy",
                true,
                true,
                "ReadWriteMany",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                COMPLETE_SELINUX_LABEL,
            ),
            // "Recursive" is applied to RWOP volumes too
            (
                "RWOP+ChangePolicy+Mount: ReadWriteOncePod with Recursive policy",
                true,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("Recursive"),
                false,
                "",
            ),
            (
                "RWOP+ChangePolicy+Mount: ReadWriteOncePod with MountOption policy",
                true,
                true,
                "ReadWriteOncePod",
                Some(complete_selinux_opts()),
                Some("MountOption"),
                false,
                COMPLETE_SELINUX_LABEL,
            ),
        ];

        for (
            name,
            selinux_mount,
            plugin_supports,
            access_mode,
            opts,
            policy,
            expect_error,
            expected_label,
        ) in tests
        {
            let _gates = selinux_gates(selinux_mount);
            let dsw = new_dsw(selinux_plugin_mgr(plugin_supports));
            let (p, volume_spec) = selinux_fixture("pod1", "pod1uid", policy, access_mode);
            let pod_name = get_unique_pod_name(&p);
            let contexts = [opts];

            let result = dsw.add_pod_to_volume(
                &pod_name,
                Arc::clone(&p),
                Arc::clone(&volume_spec),
                "volume-name",
                "",
                &contexts,
            );

            if expect_error {
                assert!(result.is_err(), "{name}: expected an error, got Ok");
                continue;
            }
            let generated_volume_name =
                result.unwrap_or_else(|e| panic!("{name}: AddPodToVolume failed: {e}"));

            verify_volume_exists_dsw(&generated_volume_name, expected_label, &dsw);
            verify_volume_exists_in_volumes_to_mount(
                &generated_volume_name,
                &["volume-name"],
                false,
                &dsw,
            );
            verify_pod_exists_in_volume_dsw(
                &pod_name,
                &generated_volume_name,
                expected_label,
                &dsw,
            );
            verify_volume_exists_with_spec_name_in_volume_dsw(&pod_name, volume_spec.name(), &dsw);
        }
    }

    /// <- `Test_AddPodToVolume_SELinux_MultiplePods` (`:961`).
    ///
    /// Pins that the conflicting-label check fires only when a SECOND pod
    /// joins an existing volume (`desired_state_of_world.go:371-390`), and that
    /// the first pod's entry survives the rejection.
    #[test]
    #[serial_test::serial]
    fn test_add_pod_to_volume_selinux_multiple_pods() {
        // (name, selinux_mount gate, access mode, first opts, second opts,
        //  first policy, second policy, expect error, expected label)
        type Case = (
            &'static str,
            bool,
            &'static str,
            Option<SELinuxOptions>,
            Option<SELinuxOptions>,
            Option<&'static str>,
            Option<&'static str>,
            bool,
            &'static str,
        );
        let c = complete_selinux_opts;
        let x = conflicting_selinux_opts;
        let tests: Vec<Case> = vec![
            ("RWOP: ReadWriteOncePod with the same SELinux options", false, "ReadWriteOncePod", Some(c()), Some(c()), None, None, false, COMPLETE_SELINUX_LABEL),
            ("RWOP: ReadWriteOncePod with conflicting SELinux options", false, "ReadWriteOncePod", Some(c()), Some(x()), None, None, true, COMPLETE_SELINUX_LABEL),
            // RWX does not support SELinux mount (yet)
            ("RWOP: ReadWriteMany with the same SELinux options", false, "ReadWriteMany", Some(c()), Some(c()), None, None, false, ""),
            ("RWOP+ChangePolicy: ReadWriteOncePod with the same SELinux options", false, "ReadWriteOncePod", Some(c()), Some(c()), None, None, false, COMPLETE_SELINUX_LABEL),
            // Recursive is applied to RWOP volumes
            ("RWOP+ChangePolicy: ReadWriteOncePod with the same SELinux options and same Recursive policy", false, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            // Conflicting policies with RWOP are an error
            ("RWOP+ChangePolicy: ReadWriteOncePod with the same SELinux options and conflicting policies", false, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), None, true, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with the same SELinux options with Recursive policy", false, "ReadWriteMany", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with the same SELinux options with MountOption policy", false, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("MountOption"), false, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with the same SELinux options with conflicting policies", false, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("Recursive"), false, ""),
            // Conflicting SELinux options are allowed with recursive policy
            ("RWOP+ChangePolicy: ReadWriteMany with conflicting SELinux options and Recursive policy", false, "ReadWriteMany", Some(c()), Some(x()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy: ReadWriteMany with conflicting SELinux options and MountOption policy", false, "ReadWriteMany", Some(c()), Some(x()), Some("MountOption"), Some("MountOption"), false, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteOncePod with the same SELinux options", true, "ReadWriteOncePod", Some(c()), Some(c()), None, None, false, COMPLETE_SELINUX_LABEL),
            ("RWOP+ChangePolicy+Mount: ReadWriteOncePod with the same SELinux options and same Recursive policy", true, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteOncePod with the same SELinux options and conflicting policies", true, "ReadWriteOncePod", Some(c()), Some(c()), Some("Recursive"), None, true, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with Recursive policy", true, "ReadWriteMany", Some(c()), Some(c()), Some("Recursive"), Some("Recursive"), false, ""),
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with MountOption policy", true, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("MountOption"), false, COMPLETE_SELINUX_LABEL),
            // nil should default to MountOption
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with default and MountOption policy", true, "ReadWriteMany", Some(c()), Some(c()), None, Some("MountOption"), false, COMPLETE_SELINUX_LABEL),
            // MountOption policy is applied to the first volume
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with the same SELinux options with conflicting policies", true, "ReadWriteMany", Some(c()), Some(c()), Some("MountOption"), Some("Recursive"), true, COMPLETE_SELINUX_LABEL),
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with conflicting SELinux options and Recursive policy", true, "ReadWriteMany", Some(c()), Some(x()), Some("Recursive"), Some("Recursive"), false, ""),
            // The SELinux label of the first pod is used
            ("RWOP+ChangePolicy+Mount: ReadWriteMany with conflicting SELinux options and MountOption policy", true, "ReadWriteMany", Some(c()), Some(x()), Some("MountOption"), Some("MountOption"), true, COMPLETE_SELINUX_LABEL),
        ];

        for (
            name,
            selinux_mount,
            access_mode,
            first_opts,
            second_opts,
            first_policy,
            second_policy,
            expect_error,
            expected_label,
        ) in tests
        {
            let _gates = selinux_gates(selinux_mount);
            let dsw = new_dsw(selinux_plugin_mgr(true));

            let (pod1, volume_spec) = selinux_fixture("pod1", "pod1uid", first_policy, access_mode);
            let pod1_name = get_unique_pod_name(&pod1);
            let generated_volume_name = dsw
                .add_pod_to_volume(
                    &pod1_name,
                    Arc::clone(&pod1),
                    Arc::clone(&volume_spec),
                    "volume-name",
                    "",
                    &[first_opts],
                )
                .unwrap_or_else(|e| panic!("{name}: AddPodToVolume failed: {e}"));

            verify_volume_exists_dsw(&generated_volume_name, expected_label, &dsw);
            verify_volume_exists_in_volumes_to_mount(
                &generated_volume_name,
                &["volume-name"],
                false,
                &dsw,
            );
            verify_pod_exists_in_volume_dsw(
                &pod1_name,
                &generated_volume_name,
                expected_label,
                &dsw,
            );
            verify_volume_exists_with_spec_name_in_volume_dsw(&pod1_name, volume_spec.name(), &dsw);

            // A different pod with the same volume.
            let (pod2, _) = selinux_fixture("pod2", "pod2uid", second_policy, access_mode);
            let pod2_name = get_unique_pod_name(&pod2);
            let result = dsw.add_pod_to_volume(
                &pod2_name,
                Arc::clone(&pod2),
                Arc::clone(&volume_spec),
                "volume-name",
                "",
                &[second_opts],
            );

            if expect_error {
                assert!(result.is_err(), "{name}: expected an error, got Ok");
                // Verify the original SELinux context is still in DSW.
                verify_pod_exists_in_volume_dsw(
                    &pod1_name,
                    &generated_volume_name,
                    expected_label,
                    &dsw,
                );
                continue;
            }
            let generated_volume_name2 =
                result.unwrap_or_else(|e| panic!("{name}: second AddPodToVolume failed: {e}"));
            assert_eq!(
                generated_volume_name2, generated_volume_name,
                "{name}: expected the same generated volume name"
            );
            verify_pod_exists_in_volume_dsw(
                &pod2_name,
                &generated_volume_name,
                expected_label,
                &dsw,
            );
        }
    }

    // --------------------------------------------- no upstream test exists

    /// `desired_state_of_world_test.go` has no coverage for the pod-errors
    /// methods; this pins them against `desired_state_of_world.go:618-651` as
    /// read. Not a port.
    #[test]
    fn pod_errors_dedupe_are_popped_destructively_and_are_listed() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod_name = UniquePodName("poduid".to_string());

        assert!(dsw.get_pods_with_errors().is_empty());
        assert!(dsw.pop_pod_errors(&pod_name).is_empty());

        dsw.add_error_to_pod(&pod_name, "b");
        dsw.add_error_to_pod(&pod_name, "a");
        dsw.add_error_to_pod(&pod_name, "b"); // stored only once
        assert_eq!(dsw.get_pods_with_errors(), vec![pod_name.clone()]);

        // `sets.List` sorts.
        assert_eq!(dsw.pop_pod_errors(&pod_name), vec!["a", "b"]);
        // ...and clears.
        assert!(dsw.pop_pod_errors(&pod_name).is_empty());
        assert!(dsw.get_pods_with_errors().is_empty());
    }

    /// The `<=` bound at `:623` lets the set reach `MAX_POD_ERRORS + 1`. Pinned
    /// so a later "tidy-up" to `<` is caught as the divergence it would be.
    #[test]
    fn pod_errors_cap_admits_one_more_than_max_pod_errors() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let pod_name = UniquePodName("poduid".to_string());
        for i in 0..50 {
            dsw.add_error_to_pod(&pod_name, &format!("err{i:02}"));
        }
        assert_eq!(dsw.pop_pod_errors(&pod_name).len(), MAX_POD_ERRORS + 1);
    }

    /// `DeletePodFromVolume` is a no-op for an unknown volume and for a known
    /// volume that does not hold the pod — but it drops the pod's errors
    /// either way, because that happens before the volume lookup (`:463`).
    #[test]
    fn delete_pod_from_volume_is_a_no_op_but_still_drops_pod_errors() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let p = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));
        let volume_spec = spec_of(&p, 0);
        let pod_name = get_unique_pod_name(&p);
        let volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&p),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        let other_pod = UniquePodName("otheruid".to_string());
        dsw.add_error_to_pod(&other_pod, "boom");

        // Unknown volume: the volume and its pod survive.
        dsw.delete_pod_from_volume(&other_pod, &UniqueVolumeName("nope".to_string()));
        verify_volume_exists_dsw(&volume_name, "", &dsw);
        // ...but the pod's errors are gone.
        assert!(dsw.get_pods_with_errors().is_empty());

        // Known volume, pod not under it: still a no-op.
        dsw.delete_pod_from_volume(&other_pod, &volume_name);
        verify_pod_exists_in_volume_dsw(&pod_name, &volume_name, "", &dsw);
        assert_eq!(dsw.get_pods(), HashSet::from([pod_name]));
    }

    /// `MarkVolumeAttachability` and `UpdatePersistentVolumeSize` are both
    /// untested upstream. Both mutate in place and both no-op on an unknown
    /// volume (`:653-662`, `:483-494`).
    #[test]
    fn mark_volume_attachability_and_update_persistent_volume_size() {
        let dsw = new_dsw(get_test_kubelet_volume_plugin_mgr());
        let p = pod(json!({
            "metadata": { "name": "pod1", "uid": "pod1uid" },
            "spec": { "volumes": [nfs_volume("volume-name", "fake-device1")] },
        }));
        let volume_spec = spec_of(&p, 0);
        let pod_name = get_unique_pod_name(&p);
        let volume_name = dsw
            .add_pod_to_volume(
                &pod_name,
                Arc::clone(&p),
                Arc::clone(&volume_spec),
                volume_spec.name(),
                "",
                &[],
            )
            .expect("AddPodToVolume failed");

        // The fake plugin is attachable, so it starts true.
        assert!(dsw.get_volumes_to_mount()[0].plugin_is_attachable);
        dsw.mark_volume_attachability(&volume_name, false);
        assert!(!dsw.get_volumes_to_mount()[0].plugin_is_attachable);

        // No PV, so no size is recorded until one is pushed in.
        assert_eq!(
            dsw.get_volumes_to_mount()[0].desired_persistent_volume_size,
            None
        );
        dsw.update_persistent_volume_size(&volume_name, Quantity::parse("5Gi").unwrap());
        assert_eq!(
            dsw.get_volumes_to_mount()[0].desired_persistent_volume_size,
            Some(Quantity::parse("5Gi").unwrap())
        );

        // Unknown volume: both are silent no-ops.
        let missing = UniqueVolumeName("nope".to_string());
        dsw.mark_volume_attachability(&missing, true);
        dsw.update_persistent_volume_size(&missing, Quantity::parse("1Gi").unwrap());
        assert!(!dsw.get_volumes_to_mount()[0].plugin_is_attachable);
    }
}
