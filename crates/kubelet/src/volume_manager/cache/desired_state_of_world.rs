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

/// The unique volume name `AddPodToVolume` gives a pod's volume
/// (`desired_state_of_world.go:272-299`), with whether the plugin is
/// attachable and device-mountable. The name depends on those: an
/// attachable/device-mountable volume is named by what the plugin reports
/// (shared between pods), any other by the pod and the volume within it.
///
/// Shared by the desired state (when the populator adds a volume) and by
/// whoever records the same volume in the actual state, so both caches key a
/// volume by one name.
pub fn get_unique_volume_name_for_pod(
    volume_plugin_mgr: &VolumePluginMgr,
    pod_name: &UniquePodName,
    volume_plugin: &dyn VolumePlugin,
    spec: &Spec<'_>,
) -> Result<(UniqueVolumeName, bool, bool), DesiredStateOfWorldError> {
    // The unique volume name used depends on whether the volume is
    // attachable/device-mountable or not.
    let attachable = is_attachable_volume(spec, volume_plugin_mgr);
    let device_mountable = is_device_mountable_volume(spec, volume_plugin_mgr);
    let volume_name = if attachable || device_mountable {
        // For attachable/device-mountable volumes, use the unique volume
        // name as reported by the plugin.
        get_unique_volume_name_from_spec(volume_plugin, spec).map_err(|err| {
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
        get_unique_volume_name_from_spec_with_pod(pod_name, volume_plugin, spec)
    };
    Ok((volume_name, attachable, device_mountable))
}

/// Maximum errors to be stored per pod in `pod_errors` to prevent unbound
/// growth. Port of `maxPodErrors` (`desired_state_of_world.go:255-259`).
pub const MAX_POD_ERRORS: usize = 10;

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

        let (volume_name, attachable, device_mountable) = get_unique_volume_name_for_pod(
            &self.volume_plugin_mgr,
            pod_name,
            volume_plugin,
            &spec,
        )?;

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
            let size_limit = desired_size_limit(&pod, &volume_spec.volume);
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
        get_selinux_label(
            &self.volume_plugin_mgr,
            self.selinux_translator.as_ref(),
            volume_spec,
            selinux_container_contexts,
            pod_security_context,
        )
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

/// Port of `getSELinuxLabel` (`desired_state_of_world.go:405-437`) with the
/// plugin manager and translator passed in, so the kubelet's `create_volume`
/// (which has no `DesiredStateOfWorld` yet) computes `SELinuxLabel` with the
/// same code `AddPodToVolume` does.
pub fn get_selinux_label(
    volume_plugin_mgr: &VolumePluginMgr,
    selinux_translator: &dyn SELinuxLabelTranslator,
    volume_spec: &Spec<'_>,
    selinux_container_contexts: &[Option<SELinuxOptions>],
    pod_security_context: Option<&rusternetes_common::resources::pod::PodSecurityContext>,
) -> Result<(String, bool), DesiredStateOfWorldError> {
    let (label_info, err) = get_mount_selinux_label(
        volume_spec,
        selinux_container_contexts,
        pod_security_context,
        volume_plugin_mgr,
        selinux_translator,
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

/// The `effectiveSELinuxMountFileLabel` `AddPodToVolume` stores
/// (`desired_state_of_world.go:333-336`): the label, cleared for a volume
/// whose access mode cannot carry `-o context=`.
pub fn effective_selinux_mount_label(volume_spec: &Spec<'_>, selinux_file_label: &str) -> String {
    if volume_supports_selinux_mount(volume_spec) {
        selinux_file_label.to_string()
    } else {
        String::new()
    }
}

/// The `DesiredSizeLimit` `AddPodToVolume` records for a volume
/// (`desired_state_of_world.go:297-322`): for a local ephemeral volume, the
/// pod's ephemeral-storage limit, lowered to the emptyDir `sizeLimit` when
/// that is smaller; `None` for any other volume.
pub fn desired_size_limit(
    pod: &Pod,
    volume: &rusternetes_common::resources::Volume,
) -> Option<Quantity> {
    let mut size_limit: Option<Quantity> = None;
    if is_local_ephemeral_volume(volume) {
        let limits = pod_limits(pod);
        let ephemeral_storage_limit = limits
            .get(EPHEMERAL_STORAGE)
            .copied()
            .unwrap_or_else(|| Quantity::from_value(0, Format::DecimalSI));
        let mut chosen = Quantity::from_value(
            saturating_i64(ephemeral_storage_limit.value()),
            Format::BinarySI,
        );
        if let Some(empty_dir_limit) = volume
            .empty_dir
            .as_ref()
            .and_then(|ed| ed.size_limit.as_deref())
            .and_then(|s| Quantity::parse(s).ok())
        {
            if empty_dir_limit.value() > 0
                && (chosen.value() == 0 || empty_dir_limit.value() < chosen.value())
            {
                chosen =
                    Quantity::from_value(saturating_i64(empty_dir_limit.value()), Format::BinarySI);
            }
        }
        size_limit = Some(chosen);
    }
    size_limit
}

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
    let driver_name = crate::volume_plugins::csi::get_csi_driver_name(spec).unwrap_or_else(|err| {
        // In theory this is unreachable - such volume would not pass validation.
        debug!(%err, "failed to get CSI driver name from volume spec");
        "unknown".to_string()
    });
    // `/` is used to separate plugin + CSI driver in util::get_unique_volume_name() too
    format!("{plugin_name}/{driver_name}")
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
