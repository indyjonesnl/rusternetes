//! Port of `pkg/kubelet/volumemanager/cache/actual_state_of_world.go`.

use crate::volume_plugins::plugin::{BlockVolumeMapper, Mounter, OwnedSpec};
use crate::volume_plugins::registry::VolumePluginMgr;
use crate::volume_plugins::util::get_unique_volume_name_from_spec;
use crate::volume_plugins::util::operation_executor::{
    self, DeviceMountState, MarkVolumeOpts, VolumeMountState,
};
use crate::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::quantity::Quantity;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use tracing::{debug, error};

/// Port of `MountedVolume` (`actual_state_of_world.go:198-200`):
///
/// ```go
/// type MountedVolume struct {
///     operationexecutor.MountedVolume
/// }
/// ```
///
/// A volume that has successfully been mounted to a pod. Go's struct embedding
/// promotes the inner fields; `Deref`/`DerefMut` is the Rust equivalent, so
/// `mv.volume_name` still resolves.
#[derive(Clone)]
pub struct MountedVolume(pub operation_executor::MountedVolume);

impl Deref for MountedVolume {
    type Target = operation_executor::MountedVolume;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MountedVolume {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Port of `AttachedVolume` (`actual_state_of_world.go:203-213`) — a volume
/// that is attached to a node.
///
/// Upstream embeds `operationexecutor.AttachedVolume` and adds two fields. The
/// embedded value is [`AttachedVolume::attached_volume`] and `Deref` promotes
/// its fields, exactly as Go's embedding does. Note that
/// `SELinuxMountContext` exists on *both*: Go's shadowing rule makes
/// `av.SELinuxMountContext` the outer one, and an inherent field winning over
/// a `Deref` target reproduces that. `newAttachedVolume` writes the same
/// string into both (`:1198-1207`), so they never actually differ.
#[derive(Clone)]
pub struct AttachedVolume {
    pub attached_volume: operation_executor::AttachedVolume,

    /// Indicates if the device has been globally mounted or not.
    pub device_mount_state: DeviceMountState,

    /// The context with which the volume is globally mounted (via the
    /// `-o context=XYZ` mount option). If empty, the volume is not mounted
    /// with `-o context=`.
    pub selinux_mount_context: String,
}

impl Deref for AttachedVolume {
    type Target = operation_executor::AttachedVolume;
    fn deref(&self) -> &Self::Target {
        &self.attached_volume
    }
}

impl AttachedVolume {
    /// Returns true if the device is mounted in the global path or is in an
    /// uncertain state. Port of `AttachedVolume.DeviceMayBeMounted`
    /// (`actual_state_of_world.go:217-220`).
    pub fn device_may_be_mounted(&self) -> bool {
        self.device_mount_state == DeviceMountState::DeviceGloballyMounted
            || self.device_mount_state == DeviceMountState::DeviceMountUncertain
    }
}

/// The errors this cache returns.
///
/// Upstream splits these across four named error types with `IsXxxError`
/// predicates (`actual_state_of_world.go:1212-1334`) and four bare
/// `fmt.Errorf` sites (`:682`, `:691`, `:733`/`:828`/`:879`, `:908`). One enum
/// carries all of them, matching [`super::DesiredStateOfWorldError`]; the
/// predicates are ported as the `is_*_error` free functions below so callers
/// that only need "which kind" keep reading the same way.
///
/// Messages are upstream's format strings verbatim, with `{:?}` standing in
/// for Go's `%q`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActualStateOfWorldError {
    /// `actual_state_of_world.go:682-685`.
    #[error("failed to get Plugin from volumeSpec for volume {volume_name:?} err={err}")]
    NoPlugin { volume_name: String, err: String },

    /// `actual_state_of_world.go:691-696`.
    #[error(
        "failed to GetUniqueVolumeNameFromSpec for volumeSpec {volume_name:?} \
         using volume plugin {plugin_name:?} err={err}"
    )]
    UniqueVolumeName {
        volume_name: String,
        plugin_name: String,
        err: String,
    },

    /// The shared message of `AddPodToVolume` (`:733-735`),
    /// `SetDeviceMountState` (`:828-831`) and `DeletePodFromVolume`
    /// (`:879-882`) — one `fmt.Errorf` text repeated three times upstream.
    #[error("no volume with the name {volume_name:?} exists in the list of attached volumes")]
    VolumeNotInAttachedVolumes { volume_name: String },

    /// `DeleteVolume` (`actual_state_of_world.go:908-911`).
    #[error("failed to DeleteVolume {volume_name:?}, it still has {mounted_pods} mountedPods")]
    VolumeStillHasMountedPods {
        volume_name: String,
        mounted_pods: usize,
    },

    /// Port of `volumeNotAttachedError` (`actual_state_of_world.go:1214-1228`)
    /// — `PodExistsInVolume` could not find the volume in the list of attached
    /// volumes.
    #[error("volumeName {volume_name:?} does not exist in the list of attached volumes")]
    VolumeNotAttached { volume_name: String },

    /// Port of `remountRequiredError` (`actual_state_of_world.go:1237-1256`) —
    /// `PodExistsInVolume` found the volume/pod attached and mounted, but
    /// `remountRequired` was true.
    #[error("volumeName {volume_name:?} is mounted to {pod_name:?} but should be remounted")]
    RemountRequired {
        volume_name: String,
        pod_name: String,
    },

    /// Port of `FsResizeRequiredError` (`actual_state_of_world.go:1259-1280`)
    /// — `PodExistsInVolume` found the volume/pod attached and mounted, but a
    /// resize request arrived after the mount.
    ///
    /// `current_size` is exported upstream and read by the reconciler; `None`
    /// is upstream's zero `resource.Quantity`.
    #[error("volumeName {volume_name:?} mounted to {pod_name:?} needs to resize file system")]
    FsResizeRequired {
        current_size: Option<Quantity>,
        volume_name: String,
        pod_name: String,
    },

    /// Port of `seLinuxMountMismatchError`
    /// (`actual_state_of_world.go:1313-1327`) — `PodExistsInVolume` found a
    /// volume mounted with a different SELinux label than expected.
    #[error(
        "waiting for unmount of volume {volume_name:?}, because it is already mounted \
         to a different pod with a different SELinux label"
    )]
    SELinuxMountMismatch { volume_name: String },
}

/// Port of `IsVolumeNotAttachedError` (`actual_state_of_world.go:237-241`).
///
/// Takes an `Option` because upstream's argument is a nilable `error` and a
/// type assertion on `nil` is false.
pub fn is_volume_not_attached_error(err: Option<&ActualStateOfWorldError>) -> bool {
    matches!(err, Some(ActualStateOfWorldError::VolumeNotAttached { .. }))
}

/// Port of `IsRemountRequiredError` (`actual_state_of_world.go:244-247`).
pub fn is_remount_required_error(err: Option<&ActualStateOfWorldError>) -> bool {
    matches!(err, Some(ActualStateOfWorldError::RemountRequired { .. }))
}

/// Port of `IsFSResizeRequiredError` (`actual_state_of_world.go:1282-1285`).
pub fn is_fs_resize_required_error(err: Option<&ActualStateOfWorldError>) -> bool {
    matches!(err, Some(ActualStateOfWorldError::FsResizeRequired { .. }))
}

/// Port of `IsSELinuxMountMismatchError`
/// (`actual_state_of_world.go:1330-1334`).
pub fn is_selinux_mount_mismatch_error(err: Option<&ActualStateOfWorldError>) -> bool {
    matches!(
        err,
        Some(ActualStateOfWorldError::SELinuxMountMismatch { .. })
    )
}

/// Port of `volumeAttachability` (`actual_state_of_world.go:272-278`):
///
/// ```go
/// type volumeAttachability string
/// const (
///     volumeAttachabilityTrue      volumeAttachability = "True"
///     volumeAttachabilityFalse     volumeAttachability = "False"
///     volumeAttachabilityUncertain volumeAttachability = "Uncertain"
/// )
/// ```
///
/// Unexported upstream, and it stays private here: the cache never exposes the
/// third state. `newAttachedVolume` collapses it to a plain bool at the
/// `operationexecutor.AttachedVolume` boundary with
/// `pluginIsAttachable == volumeAttachabilityTrue` (`:1204`), so `False` and
/// `Uncertain` both read as "not attachable" to every consumer.
/// `UpdateReconstructedVolumeAttachability` is the only thing that can tell
/// them apart, and only to resolve `Uncertain` once.
/// `verifyVolumeAttachability` in upstream's test says the same:
/// "ASW does not have any special difference between False and Uncertain."
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VolumeAttachability {
    True,
    False,
    Uncertain,
}

/// Port of `actualStateOfWorld` (`actual_state_of_world.go:249-270`).
///
/// This cache contains volumes->pods, i.e. the set of all volumes attached to
/// this node and the pods the manager believes have successfully mounted them.
/// Distinct from the `ActualStateOfWorld` of the attach/detach controller:
/// they track different objects.
///
/// **Interface.** Upstream splits this into an `ActualStateOfWorld` interface
/// (`:48-194`) embedding `operationexecutor.ActualStateOfWorldMounterUpdater`
/// (`operation_executor.go:178-246`) and
/// `operationexecutor.ActualStateOfWorldAttacherUpdater`
/// (`operation_executor.go:248-280`), plus one unexported implementation.
/// There is exactly one implementation and no fake, so the interfaces would be
/// pure ceremony in Rust; their 22 + 18 + 7 methods are the 47 public methods
/// below and their doc comments are upstream's interface comments.
///
/// **Locking.** Upstream embeds one `sync.RWMutex` and takes it across every
/// method, so `attachedVolumes`, `foundDuringReconstruction` and
/// `volumesWithFinalExpansionErrors` move together — `AddPodToVolume` writes
/// the first two under a single hold and `DeletePodFromVolume` writes both.
/// Three independent `RwLock`s would let a reader observe a pod deleted from
/// one and not the other. So the three live in one inner struct behind one
/// `RwLock`; `node_name` and `volume_plugin_mgr` are immutable after
/// construction and sit outside it, as upstream's never-written fields
/// effectively do.
pub struct ActualStateOfWorld {
    /// The name of this node. This value is passed to Attach/Detach.
    node_name: String,
    state: RwLock<State>,
    /// The volume plugin manager used to create volume plugin objects.
    volume_plugin_mgr: Arc<VolumePluginMgr>,
}

/// The lock-guarded half of [`ActualStateOfWorld`].
struct State {
    /// The set of volumes the kubelet volume manager believes to be
    /// successfully attached to this node. Volume types that do not implement
    /// an attacher interface are assumed to be in this state by default.
    attached_volumes: HashMap<UniqueVolumeName, AttachedVolumeEntry>,

    /// Volumes which were discovered from the kubelet root directory when
    /// kubelet was restarted. The inner value is the pod UID.
    found_during_reconstruction: HashMap<UniqueVolumeName, HashMap<UniquePodName, String>>,

    volumes_with_final_expansion_errors: HashSet<UniqueVolumeName>,
}

/// Port of `attachedVolume` (`actual_state_of_world.go:283-333`) — a volume
/// the kubelet volume manager believes to be successfully attached to a node
/// it is managing. Volume types that do not implement an attacher are assumed
/// to be in this state.
///
/// Named `AttachedVolumeEntry` because the public [`AttachedVolume`] above
/// already has upstream's exported name.
struct AttachedVolumeEntry {
    /// The unique identifier for this volume.
    volume_name: UniqueVolumeName,

    /// The set of pods that this volume has been successfully mounted to,
    /// keyed by unique pod name.
    mounted_pods: HashMap<UniquePodName, MountedPod>,

    /// The volume spec containing the specification for this volume. Used to
    /// generate the volume plugin object, and passed to plugin methods. In
    /// particular, the Unmount method uses `spec.name()` as the
    /// volumeSpecName in the mount path
    /// `/var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{volumeSpecName}/`.
    spec: Arc<OwnedSpec>,

    /// The Unescaped Qualified name of the volume plugin used to attach and
    /// mount this volume. Stored separately in case the full volume spec
    /// (everything except the name) cannot be reconstructed for a volume that
    /// should be unmounted.
    plugin_name: String,

    /// Indicates whether the volume plugin used to attach and mount this
    /// volume implements the `volume.Attacher` interface.
    plugin_is_attachable: VolumeAttachability,

    /// Stores information that tells us if the device is mounted globally or
    /// not.
    device_mount_state: DeviceMountState,

    /// The path on the node where the volume is attached, for attachable
    /// volumes.
    device_path: String,

    /// The path on the node where the device should be mounted after it is
    /// attached.
    device_mount_path: String,

    /// Indicates the volume driver has previously returned a volume-in-use
    /// error for this volume and volume expansion on this node should not be
    /// retried.
    volume_in_use_error_for_expansion: bool,

    /// Records the size of the volume when the pod was started, or the size
    /// after successful completion of a volume expansion operation. `None` is
    /// upstream's zero `resource.Quantity`, which every reader tests with
    /// `IsZero()`.
    persistent_volume_size: Option<Quantity>,

    /// The context with which the volume is mounted to the global directory
    /// (via the `-o context=XYZ` mount option). If `None`, the volume is not
    /// mounted. If `Some("")`, the volume is mounted without `-o context=`.
    ///
    /// The tri-state is upstream's `*string`, and all three cases are load
    /// bearing: `PodExistsInVolume` compares only when non-nil (`:931-935`)
    /// and `AddPodToVolume` writes only when nil (`:776-782`).
    selinux_mount_context: Option<String>,
}

/// Port of `mountedPod` (`actual_state_of_world.go:339-380`) — a pod for which
/// the kubelet volume manager believes the underlying volume has been
/// successfully mounted.
struct MountedPod {
    /// The name of the pod.
    pod_name: UniquePodName,

    /// The UID of the pod.
    pod_uid: String,

    /// Mounter used to mount.
    mounter: Option<Arc<dyn Mounter>>,

    /// Mapper used for block volume support.
    block_volume_mapper: Option<Arc<dyn BlockVolumeMapper>>,

    /// The volume spec containing the specification for this volume.
    volume_spec: Arc<OwnedSpec>,

    /// Indicates the underlying volume has been successfully mounted to this
    /// pod but it should be remounted to reflect changes in the referencing
    /// pod. Atomically updating volumes depend on this to update the contents
    /// of the volume. All volume mounting calls should be idempotent so a
    /// second mount call for volumes that do not need to update contents
    /// should not fail.
    remount_required: bool,

    /// The value of the GID annotation, if present.
    volume_gid_value: String,

    /// State of the volume mount for the pod. If it is:
    /// - `VolumeMounted`: the volume for the pod has been successfully mounted,
    /// - `VolumeMountUncertain`: the volume for the pod may not be mounted,
    ///   but it must be unmounted.
    volume_mount_state_for_pod: VolumeMountState,

    /// The context with which the volume is mounted to the pod directory (via
    /// the `-o context=XYZ` mount option).
    ///
    /// **Shape deviation, inherited from upstream's own comment.** The doc
    /// comment on this field (`:375-377`) describes a `*string` tri-state, but
    /// the field is declared a plain `string`. Nothing reads it, so the
    /// comment is stale rather than the type; a plain `String` is what is
    /// actually there.
    #[allow(
        dead_code,
        reason = "upstream field; unread upstream too — see the field comment"
    )]
    selinux_mount_context: String,
}

impl ActualStateOfWorld {
    /// Port of `NewActualStateOfWorld` (`actual_state_of_world.go:223-232`).
    pub fn new(node_name: impl Into<String>, volume_plugin_mgr: Arc<VolumePluginMgr>) -> Self {
        Self {
            node_name: node_name.into(),
            state: RwLock::new(State {
                attached_volumes: HashMap::new(),
                found_during_reconstruction: HashMap::new(),
                volumes_with_final_expansion_errors: HashSet::new(),
            }),
            volume_plugin_mgr,
        }
    }

    // ---------------------------------------------------------------------
    // operationexecutor.ActualStateOfWorldAttacherUpdater
    // (`operation_executor.go:248-280`)
    // ---------------------------------------------------------------------

    /// Marks the specified volume as attached to the specified node. If the
    /// volume name is supplied, that volume name will be used; if not, the
    /// volume name is computed using the result from querying the plugin.
    ///
    /// Port of `MarkVolumeAsAttached` (`actual_state_of_world.go:382-391`).
    ///
    /// The `nodeName` argument is `_` upstream too — the kubelet's ASW only
    /// ever tracks its own node.
    pub fn mark_volume_as_attached(
        &self,
        volume_name: Option<&UniqueVolumeName>,
        volume_spec: Arc<OwnedSpec>,
        _node_name: &str,
        device_path: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        let mut plugin_is_attachable = VolumeAttachability::False;
        if self
            .volume_plugin_mgr
            .find_attachable_plugin_by_spec(&volume_spec.as_spec())
            .is_some()
        {
            plugin_is_attachable = VolumeAttachability::True;
        }

        self.add_volume(volume_name, volume_spec, device_path, plugin_is_attachable)
    }

    /// Adds the specified volume to the ASW as uncertainly attached.
    ///
    /// Port of `AddAttachUncertainReconstructedVolume`
    /// (`actual_state_of_world.go:393-398`). Declared on the
    /// `ActualStateOfWorld` interface (`:186`) rather than the attacher
    /// updater, but sits next to its sibling here.
    pub fn add_attach_uncertain_reconstructed_volume(
        &self,
        volume_name: Option<&UniqueVolumeName>,
        volume_spec: Arc<OwnedSpec>,
        _node_name: &str,
        device_path: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        self.add_volume(
            volume_name,
            volume_spec,
            device_path,
            VolumeAttachability::Uncertain,
        )
    }

    /// Marks the specified volume as *possibly* attached to the specified
    /// node.
    ///
    /// Port of `MarkVolumeAsUncertain` (`actual_state_of_world.go:400-403`),
    /// whose whole body is `return nil` — the kubelet's ASW has nothing to
    /// record for an uncertain *attach* (uncertainty about the global device
    /// mount lives in [`Self::mark_device_as_uncertain`], and about the pod
    /// mount in [`Self::mark_volume_mount_as_uncertain`]). The `Result` is
    /// kept because the interface declares one and callers check it.
    pub fn mark_volume_as_uncertain(
        &self,
        _volume_name: &UniqueVolumeName,
        _volume_spec: Arc<OwnedSpec>,
        _node_name: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        Ok(())
    }

    /// Marks the specified volume as detached from the specified node.
    ///
    /// Port of `MarkVolumeAsDetached` (`actual_state_of_world.go:405-408`):
    ///
    /// ```go
    /// func (asw *actualStateOfWorld) MarkVolumeAsDetached(
    ///     volumeName v1.UniqueVolumeName, nodeName types.NodeName) {
    ///     asw.DeleteVolume(volumeName)
    /// }
    /// ```
    ///
    /// **The discarded error is the contract, not an oversight.** If the
    /// volume still has mounted pods, `DeleteVolume` refuses and the volume
    /// stays — and because the interface signature
    /// (`operation_executor.go:266`) has no error return, the caller is told
    /// nothing. `Test_MarkVolumeAsDetached_Negative_PodInVolume` pins exactly
    /// that: it calls this on a volume with a pod and then asserts the pod is
    /// still there. Returning a `Result` here would change the interface and
    /// tempt a caller into treating a refusal as a failure.
    pub fn mark_volume_as_detached(&self, volume_name: &UniqueVolumeName, _node_name: &str) {
        let _ = self.delete_volume(volume_name);
    }

    /// Marks the desire to detach the specified volume (removes the volume
    /// from the node's `volumesToReportAsAttached` list).
    ///
    /// Port of `RemoveVolumeFromReportAsAttached`
    /// (`actual_state_of_world.go:540-543`): a no-op on the kubelet side —
    /// the attach/detach controller owns `Node.Status.VolumesAttached`.
    pub fn remove_volume_from_report_as_attached(
        &self,
        _volume_name: &UniqueVolumeName,
        _node_name: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        // no operation for kubelet side
        Ok(())
    }

    /// Unmarks the desire to detach for the specified volume (adds the volume
    /// back to the node's `volumesToReportAsAttached` list).
    ///
    /// Port of `AddVolumeToReportAsAttached`
    /// (`actual_state_of_world.go:536-538`): a no-op on the kubelet side.
    pub fn add_volume_to_report_as_attached(
        &self,
        _volume_name: &UniqueVolumeName,
        _node_name: &str,
    ) {
        // no operation for kubelet side
    }

    /// Sets the PVC claim size by reading `pvc.Status.Capacity`, but only if
    /// the recorded size is still zero — which happens when the volume was
    /// rebuilt after kubelet startup.
    ///
    /// Port of `InitializeClaimSize` (`actual_state_of_world.go:850-861`).
    pub fn initialize_claim_size(&self, volume_name: &UniqueVolumeName, claim_size: Quantity) {
        let mut state = self.state.write().expect("actual state of world lock");

        if let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) {
            // only set volume claim size if claimStatusSize is zero
            // this can happen when volume was rebuilt after kubelet startup
            if is_zero_quantity(volume_obj.persistent_volume_size) {
                volume_obj.persistent_volume_size = Some(claim_size);
            }
        }
    }

    /// Port of `GetClaimSize` (`actual_state_of_world.go:863-872`). `None` is
    /// upstream's zero `resource.Quantity` return for an unknown volume.
    pub fn get_claim_size(&self, volume_name: &UniqueVolumeName) -> Option<Quantity> {
        let state = self.state.read().expect("actual state of world lock");

        state
            .attached_volumes
            .get(volume_name)
            .and_then(|volume_obj| volume_obj.persistent_volume_size)
    }

    // ---------------------------------------------------------------------
    // operationexecutor.ActualStateOfWorldMounterUpdater
    // (`operation_executor.go:178-246`)
    // ---------------------------------------------------------------------

    /// Marks the specified volume as mounted to the specified pod.
    ///
    /// Port of `MarkVolumeAsMounted` (`actual_state_of_world.go:532-534`).
    pub fn mark_volume_as_mounted(
        &self,
        mark_volume_opts: MarkVolumeOpts,
    ) -> Result<(), ActualStateOfWorldError> {
        self.add_pod_to_volume(mark_volume_opts)
    }

    /// Marks the specified volume as unmounted from the specified pod.
    ///
    /// Port of `MarkVolumeAsUnmounted` (`actual_state_of_world.go:545-548`).
    pub fn mark_volume_as_unmounted(
        &self,
        pod_name: &UniquePodName,
        volume_name: &UniqueVolumeName,
    ) -> Result<(), ActualStateOfWorldError> {
        self.delete_pod_from_volume(pod_name, volume_name)
    }

    /// Marks the state of the volume mount for the pod uncertain.
    ///
    /// Port of `MarkVolumeMountAsUncertain` (`actual_state_of_world.go:560-563`).
    pub fn mark_volume_mount_as_uncertain(
        &self,
        mut mark_volume_opts: MarkVolumeOpts,
    ) -> Result<(), ActualStateOfWorldError> {
        mark_volume_opts.volume_mount_state = VolumeMountState::VolumeMountUncertain;
        self.add_pod_to_volume(mark_volume_opts)
    }

    /// Marks the specified volume as having been globally mounted.
    ///
    /// Port of `MarkDeviceAsMounted` (`actual_state_of_world.go:550-553`).
    pub fn mark_device_as_mounted(
        &self,
        volume_name: &UniqueVolumeName,
        device_path: &str,
        device_mount_path: &str,
        selinux_mount_context: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        self.set_device_mount_state(
            volume_name,
            DeviceMountState::DeviceGloballyMounted,
            device_path,
            device_mount_path,
            selinux_mount_context,
        )
    }

    /// Marks the device state in the global mount path as uncertain.
    ///
    /// Port of `MarkDeviceAsUncertain` (`actual_state_of_world.go:555-558`).
    pub fn mark_device_as_uncertain(
        &self,
        volume_name: &UniqueVolumeName,
        device_path: &str,
        device_mount_path: &str,
        selinux_mount_context: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        self.set_device_mount_state(
            volume_name,
            DeviceMountState::DeviceMountUncertain,
            device_path,
            device_mount_path,
            selinux_mount_context,
        )
    }

    /// Marks the specified volume as having its global mount unmounted.
    ///
    /// Port of `MarkDeviceAsUnmounted` (`actual_state_of_world.go:565-568`).
    /// Upstream passes empty strings for the device path, the device mount
    /// path and the SELinux context — and because `SetDeviceMountState` only
    /// *writes* the device path and the SELinux context when the argument is
    /// non-empty (`:838-846`), this clears the mount path and the state but
    /// deliberately leaves the other two alone.
    pub fn mark_device_as_unmounted(
        &self,
        volume_name: &UniqueVolumeName,
    ) -> Result<(), ActualStateOfWorldError> {
        self.set_device_mount_state(volume_name, DeviceMountState::DeviceNotMounted, "", "", "")
    }

    /// Marks the specified volume's file system resize request as finished.
    ///
    /// Port of `MarkVolumeAsResized` (`actual_state_of_world.go:787-798`).
    pub fn mark_volume_as_resized(
        &self,
        volume_name: &UniqueVolumeName,
        claim_size: Quantity,
    ) -> bool {
        let mut state = self.state.write().expect("actual state of world lock");

        if let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) {
            volume_obj.persistent_volume_size = Some(claim_size);
            return true;
        }
        false
    }

    /// Returns the mount state of the device in the global path.
    ///
    /// Port of `GetDeviceMountState` (`actual_state_of_world.go:610-620`).
    pub fn get_device_mount_state(&self, volume_name: &UniqueVolumeName) -> DeviceMountState {
        let state = self.state.read().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get(volume_name) else {
            return DeviceMountState::DeviceNotMounted;
        };

        volume_obj.device_mount_state
    }

    /// Returns the mount state of the volume for the pod.
    ///
    /// Port of `GetVolumeMountState` (`actual_state_of_world.go:633-647`).
    /// `VolumeNotMounted` is never stored — it is what this returns when the
    /// volume, or the pod entry under it, is absent.
    pub fn get_volume_mount_state(
        &self,
        volume_name: &UniqueVolumeName,
        pod_name: &UniquePodName,
    ) -> VolumeMountState {
        let state = self.state.read().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get(volume_name) else {
            return VolumeMountState::VolumeNotMounted;
        };

        let Some(pod_obj) = volume_obj.mounted_pods.get(pod_name) else {
            return VolumeMountState::VolumeNotMounted;
        };
        pod_obj.volume_mount_state_for_pod
    }

    /// Returns whether the supplied volume is mounted in a pod other than the
    /// supplied one.
    ///
    /// Port of `IsVolumeMountedElsewhere` (`actual_state_of_world.go:649-667`).
    pub fn is_volume_mounted_elsewhere(
        &self,
        volume_name: &UniqueVolumeName,
        pod_name: &UniquePodName,
    ) -> bool {
        let state = self.state.read().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get(volume_name) else {
            return false;
        };

        for pod_obj in volume_obj.mounted_pods.values() {
            if *pod_name != pod_obj.pod_name {
                // Treat uncertain mount state as mounted until certain.
                if pod_obj.volume_mount_state_for_pod != VolumeMountState::VolumeNotMounted {
                    return true;
                }
            }
        }
        false
    }

    /// Marks the volume as having had an in-use error during expansion. Volume
    /// expansion must not be retried for this volume.
    ///
    /// Port of `MarkForInUseExpansionError` (`actual_state_of_world.go:622-631`).
    pub fn mark_for_in_use_expansion_error(&self, volume_name: &UniqueVolumeName) {
        let mut state = self.state.write().expect("actual state of world lock");

        if let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) {
            volume_obj.volume_in_use_error_for_expansion = true;
        }
    }

    /// Only adds the volume to the actual state of the world if it was not
    /// already there, so any previously stored state is not overwritten.
    /// Returns true if this operation resulted in the volume being added.
    ///
    /// Port of `CheckAndMarkVolumeAsUncertainViaReconstruction`
    /// (`actual_state_of_world.go:456-510`). The `Result` is upstream's error
    /// return, which its body never populates.
    pub fn check_and_mark_volume_as_uncertain_via_reconstruction(
        &self,
        opts: MarkVolumeOpts,
    ) -> Result<bool, ActualStateOfWorldError> {
        let mut guard = self.state.write().expect("actual state of world lock");
        let state = &mut *guard;

        let Some(volume_obj) = state.attached_volumes.get_mut(&opts.volume_name) else {
            return Ok(false);
        };

        if let Some(pod_obj) = volume_obj.mounted_pods.get(&opts.pod_name) {
            // if volume mount was uncertain we should keep trying to unmount the volume
            if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMountUncertain {
                return Ok(false);
            }
            if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted {
                return Ok(false);
            }
        }

        let pod_obj = MountedPod {
            pod_name: opts.pod_name.clone(),
            pod_uid: opts.pod_uid.clone(),
            // The mounter stored in the object may have old information, use
            // the newest one. (Upstream assigns `mounter` twice — once in the
            // literal and once behind `if mounter != nil` at `:497-501` —
            // which is the same value both times.)
            mounter: opts.mounter.clone(),
            block_volume_mapper: opts.block_volume_mapper.clone(),
            volume_gid_value: opts.volume_gid_volume.clone(),
            volume_spec: opts.volume_spec.clone(),
            remount_required: false,
            volume_mount_state_for_pod: VolumeMountState::VolumeMountUncertain,
            selinux_mount_context: String::new(),
        };
        volume_obj
            .mounted_pods
            .insert(opts.pod_name.clone(), pod_obj);

        state
            .found_during_reconstruction
            .entry(opts.volume_name.clone())
            .or_default()
            .insert(opts.pod_name.clone(), opts.pod_uid.clone());
        Ok(true)
    }

    /// Only adds the device to the actual state of the world if it was not
    /// already there, so any previously stored state is not overwritten. Only
    /// `device_mount_path` is supplied because `device_path` is already
    /// determined by `VerifyControllerAttachedVolume`.
    ///
    /// Port of `CheckAndMarkDeviceUncertainViaReconstruction`
    /// (`actual_state_of_world.go:513-530`).
    pub fn check_and_mark_device_uncertain_via_reconstruction(
        &self,
        volume_name: &UniqueVolumeName,
        device_mount_path: &str,
    ) -> bool {
        let mut state = self.state.write().expect("actual state of world lock");

        // CheckAndMarkDeviceUncertainViaReconstruction requires the volume to
        // be marked as attached, so if the volume does not exist in the ASW or
        // is in any state other than DeviceNotMounted we should return.
        let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) else {
            return false;
        };
        if volume_obj.device_mount_state != DeviceMountState::DeviceNotMounted {
            return false;
        }

        volume_obj.device_mount_state = DeviceMountState::DeviceMountUncertain;
        // we are only changing deviceMountPath because devicePath at this
        // stage is determined from the node object.
        volume_obj.device_mount_path = device_mount_path.to_string();
        true
    }

    /// Returns true if the volume currently added to the actual state of the
    /// world was found during reconstruction.
    ///
    /// Port of `IsVolumeReconstructed` (`actual_state_of_world.go:431-447`).
    pub fn is_volume_reconstructed(
        &self,
        volume_name: &UniqueVolumeName,
        pod_name: &UniquePodName,
    ) -> bool {
        // Taken before the lock, exactly as upstream does — `RWMutex` is not
        // reentrant, so `GetVolumeMountState` must release before `RLock`.
        let volume_state = self.get_volume_mount_state(volume_name, pod_name);

        // only uncertain volumes are reconstructed
        if volume_state != VolumeMountState::VolumeMountUncertain {
            return false;
        }

        let state = self.state.read().expect("actual state of world lock");
        let Some(pod_map) = state.found_during_reconstruction.get(volume_name) else {
            return false;
        };
        pod_map.contains_key(pod_name)
    }

    /// Returns true if the volume device identified by `volume_name` was found
    /// during reconstruction.
    ///
    /// Port of `IsVolumeDeviceReconstructed` (`actual_state_of_world.go:449-454`).
    pub fn is_volume_device_reconstructed(&self, volume_name: &UniqueVolumeName) -> bool {
        let state = self.state.read().expect("actual state of world lock");
        state.found_during_reconstruction.contains_key(volume_name)
    }

    /// Marks the volume as failed with a final error, so this state does not
    /// have to be recorded in the API server.
    ///
    /// Port of `MarkVolumeExpansionFailedWithFinalError`
    /// (`actual_state_of_world.go:410-415`).
    pub fn mark_volume_expansion_failed_with_final_error(&self, volume_name: &UniqueVolumeName) {
        let mut state = self.state.write().expect("actual state of world lock");

        state
            .volumes_with_final_expansion_errors
            .insert(volume_name.clone());
    }

    /// Removes the volume from the list indicating that the volume has failed
    /// expansion with a final error.
    ///
    /// Port of `RemoveVolumeFromFailedWithFinalErrors`
    /// (`actual_state_of_world.go:417-422`).
    pub fn remove_volume_from_failed_with_final_errors(&self, volume_name: &UniqueVolumeName) {
        let mut state = self.state.write().expect("actual state of world lock");

        state
            .volumes_with_final_expansion_errors
            .remove(volume_name);
    }

    /// Verifies if volume expansion has failed with a final error.
    ///
    /// Port of `CheckVolumeInFailedExpansionWithFinalErrors`
    /// (`actual_state_of_world.go:424-429`).
    pub fn check_volume_in_failed_expansion_with_final_errors(
        &self,
        volume_name: &UniqueVolumeName,
    ) -> bool {
        let state = self.state.read().expect("actual state of world lock");

        state
            .volumes_with_final_expansion_errors
            .contains(volume_name)
    }

    // ---------------------------------------------------------------------
    // cache.ActualStateOfWorld (`actual_state_of_world.go:47-194`)
    // ---------------------------------------------------------------------

    /// Adds the given pod to the given volume in the cache, indicating the
    /// specified volume has been successfully mounted to the specified pod. If
    /// a pod with the same unique name already exists under the specified
    /// volume, the pod's `remountRequired` value is reset. If a volume with
    /// the name `volume_name` does not exist in the list of attached volumes,
    /// an error is returned.
    ///
    /// Port of `AddPodToVolume` (`actual_state_of_world.go:720-785`).
    ///
    /// **The refresh is partial, deliberately.** On an existing *and certain*
    /// pod entry only `remount_required`, `volume_mount_state_for_pod` and —
    /// when non-`None` — `mounter` are overwritten. `block_volume_mapper`,
    /// `volume_spec`, `volume_gid_value`, `pod_uid` and the pod-level SELinux
    /// context are rebuilt only when the entry is new or was
    /// `VolumeMountUncertain` (`:738-763`); a reconstructed volume is marked
    /// uncertain precisely so its first real mount replaces the guessed
    /// fields. Always-overwrite and never-overwrite both diverge.
    ///
    /// **It errors where the DSW's implicitly creates.** A volume must already
    /// be attached before a pod can be recorded against it
    /// (`Test_AddPodToVolume_Negative_VolumeDoesntExist`); the desired state
    /// has no such precondition. The two caches differ here on purpose.
    pub fn add_pod_to_volume(
        &self,
        mark_volume_opts: MarkVolumeOpts,
    ) -> Result<(), ActualStateOfWorldError> {
        let MarkVolumeOpts {
            pod_name,
            pod_uid,
            volume_name,
            mounter,
            block_volume_mapper,
            volume_gid_volume: volume_gid_value,
            volume_spec,
            volume_mount_state,
            selinux_mount_context,
        } = mark_volume_opts;
        let mut guard = self.state.write().expect("actual state of world lock");
        // Reborrow so `attached_volumes` and `found_during_reconstruction` can
        // be borrowed independently; through the guard's `DerefMut` they would
        // both borrow the whole `State`.
        let state = &mut *guard;

        let Some(volume_obj) = state.attached_volumes.get_mut(&volume_name) else {
            return Err(ActualStateOfWorldError::VolumeNotInAttachedVolumes {
                volume_name: volume_name.to_string(),
            });
        };

        let existing_pod_obj = volume_obj.mounted_pods.get(&pod_name);
        let pod_exists = existing_pod_obj.is_some();

        // Update uncertain volumes — the new markVolumeOpts may have updated
        // information. Especially reconstructed volumes (marked as uncertain
        // during reconstruction) need an update.
        let update_uncertain_volume = existing_pod_obj.is_some_and(|pod_obj| {
            pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMountUncertain
        });

        let mut pod_obj = if !pod_exists || update_uncertain_volume {
            // Add new mountedPod or update existing one.
            MountedPod {
                pod_name: pod_name.clone(),
                pod_uid,
                mounter: mounter.clone(),
                block_volume_mapper,
                volume_gid_value,
                volume_spec,
                remount_required: false,
                volume_mount_state_for_pod: volume_mount_state,
                selinux_mount_context: selinux_mount_context.clone(),
            }
        } else {
            volume_obj
                .mounted_pods
                .remove(&pod_name)
                .expect("checked present just above")
        };

        // If pod exists, reset remountRequired value
        pod_obj.remount_required = false;
        pod_obj.volume_mount_state_for_pod = volume_mount_state;

        // if volume is mounted successfully, then it should be removed from
        // the foundDuringReconstruction map
        if volume_mount_state == VolumeMountState::VolumeMounted {
            if let Some(pod_map) = state.found_during_reconstruction.get_mut(&volume_name) {
                pod_map.remove(&pod_name);
            }
        }
        if mounter.is_some() {
            // The mounter stored in the object may have old information, use
            // the newest one.
            pod_obj.mounter = mounter;
        }
        volume_obj.mounted_pods.insert(pod_name, pod_obj);

        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
            // Store the mount context also in the AttachedVolume to have a
            // global volume context for a quick comparison in
            // PodExistsInVolume.
            if volume_obj.selinux_mount_context.is_none() {
                volume_obj.selinux_mount_context = Some(selinux_mount_context);
            }
        }

        Ok(())
    }

    /// Marks each volume that is successfully attached and mounted for the
    /// specified pod as requiring remount, if the plugin for the volume
    /// indicates it requires remounting on pod updates. Atomically updating
    /// volumes depend on this to update the contents of the volume on pod
    /// update.
    ///
    /// Port of `MarkRemountRequired` (`actual_state_of_world.go:800-821`).
    pub fn mark_remount_required(&self, pod_name: &UniquePodName) {
        let mut state = self.state.write().expect("actual state of world lock");
        for (volume_name, volume_obj) in state.attached_volumes.iter_mut() {
            let Some(pod_obj) = volume_obj.mounted_pods.get_mut(pod_name) else {
                continue;
            };
            let volume_spec = pod_obj.volume_spec.clone();
            let spec = volume_spec.as_spec();
            let volume_plugin = match self.volume_plugin_mgr.find_plugin_by_spec(&spec) {
                Ok(volume_plugin) => volume_plugin,
                Err(err) => {
                    // Log and continue processing
                    error!(
                        unique_pod_name = %pod_obj.pod_name,
                        pod_uid = pod_obj.pod_uid,
                        volume_name = %volume_name,
                        volume_spec_name = spec.name(),
                        %err,
                        "MarkRemountRequired failed to FindPluginBySpec for volume"
                    );
                    continue;
                }
            };

            if volume_plugin.requires_remount(&spec) {
                pod_obj.remount_required = true;
            }
        }
    }

    /// Sets the device mount state for the given volume. When it is
    /// `DeviceGloballyMounted` the device is mounted at a global mount point;
    /// when it is `DeviceMountUncertain` the volume MAY be globally mounted at
    /// a global mount point. In both cases the volume must be unmounted from
    /// the global mount point prior to detach. If a volume with the name
    /// `volume_name` does not exist in the list of attached volumes, an error
    /// is returned.
    ///
    /// Port of `SetDeviceMountState` (`actual_state_of_world.go:823-848`).
    pub fn set_device_mount_state(
        &self,
        volume_name: &UniqueVolumeName,
        device_mount_state: DeviceMountState,
        device_path: &str,
        device_mount_path: &str,
        selinux_mount_context: &str,
    ) -> Result<(), ActualStateOfWorldError> {
        let mut state = self.state.write().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) else {
            return Err(ActualStateOfWorldError::VolumeNotInAttachedVolumes {
                volume_name: volume_name.to_string(),
            });
        };

        volume_obj.device_mount_state = device_mount_state;
        volume_obj.device_mount_path = device_mount_path.to_string();
        if !device_path.is_empty() {
            volume_obj.device_path = device_path.to_string();
        }
        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod)
            && !selinux_mount_context.is_empty()
        {
            volume_obj.selinux_mount_context = Some(selinux_mount_context.to_string());
        }

        Ok(())
    }

    /// Removes the given pod from the given volume in the cache, indicating
    /// the volume has been successfully unmounted from the pod. If a pod with
    /// the same unique name does not exist under the specified volume, this is
    /// a no-op. If a volume with the name `volume_name` does not exist in the
    /// list of attached volumes, an error is returned.
    ///
    /// Port of `DeletePodFromVolume` (`actual_state_of_world.go:874-898`).
    pub fn delete_pod_from_volume(
        &self,
        pod_name: &UniquePodName,
        volume_name: &UniqueVolumeName,
    ) -> Result<(), ActualStateOfWorldError> {
        let mut guard = self.state.write().expect("actual state of world lock");
        let state = &mut *guard;

        let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) else {
            return Err(ActualStateOfWorldError::VolumeNotInAttachedVolumes {
                volume_name: volume_name.to_string(),
            });
        };

        volume_obj.mounted_pods.remove(pod_name);

        // if there were reconstructed volumes, we should remove them
        if let Some(pod_map) = state.found_during_reconstruction.get_mut(volume_name) {
            pod_map.remove(pod_name);
        }

        Ok(())
    }

    /// Removes the given volume from the list of attached volumes in the
    /// cache, indicating the volume has been successfully detached from this
    /// node. If a volume with the name `volume_name` does not exist in the
    /// list of attached volumes, this is a no-op. If it exists and its list of
    /// `mounted_pods` is not empty, an error is returned.
    ///
    /// Port of `DeleteVolume` (`actual_state_of_world.go:900-919`).
    pub fn delete_volume(
        &self,
        volume_name: &UniqueVolumeName,
    ) -> Result<(), ActualStateOfWorldError> {
        let mut state = self.state.write().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get(volume_name) else {
            return Ok(());
        };

        if !volume_obj.mounted_pods.is_empty() {
            return Err(ActualStateOfWorldError::VolumeStillHasMountedPods {
                volume_name: volume_name.to_string(),
                mounted_pods: volume_obj.mounted_pods.len(),
            });
        }

        state.attached_volumes.remove(volume_name);
        state.found_during_reconstruction.remove(volume_name);
        Ok(())
    }

    /// Returns true if the given pod exists in the list of `mounted_pods` for
    /// the given volume in the cache, indicating that the volume is attached
    /// to this node and the pod has successfully mounted it.
    ///
    /// If a pod with the same unique name does not exist under the specified
    /// volume, false is returned. If a volume with the name `volume_name` does
    /// not exist in the list of attached volumes,
    /// [`ActualStateOfWorldError::VolumeNotAttached`] is returned, indicating
    /// the given volume is not yet attached. If the given volume/pod combo
    /// exists but the value of `remount_required` is true,
    /// [`ActualStateOfWorldError::RemountRequired`] is returned, indicating
    /// the given volume has been successfully mounted to this pod but should
    /// be remounted to reflect changes in the referencing pod. All volume
    /// mounting calls should be idempotent, so a second mount call for volumes
    /// that do not need to update contents should not fail.
    ///
    /// Port of `PodExistsInVolume` (`actual_state_of_world.go:921-953`).
    ///
    /// **Not a `Result`.** Upstream returns `(bool, string, error)` and the
    /// reconciler reads the `bool` and the device path *alongside* a non-nil
    /// error — `RemountRequired` and `FsResizeRequired` both come back with
    /// `true` and a real device path. A `Result` would discard exactly the
    /// values the caller needs, so the error is the third element and
    /// `None` is upstream's nil.
    pub fn pod_exists_in_volume(
        &self,
        pod_name: &UniquePodName,
        volume_name: &UniqueVolumeName,
        desired_volume_size: Option<Quantity>,
        selinux_label: &str,
    ) -> (bool, String, Option<ActualStateOfWorldError>) {
        let state = self.state.read().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get(volume_name) else {
            return (
                false,
                String::new(),
                Some(ActualStateOfWorldError::VolumeNotAttached {
                    volume_name: volume_name.to_string(),
                }),
            );
        };

        // The volume exists, check its SELinux context mount option
        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
            if let Some(context) = &volume_obj.selinux_mount_context {
                if context != selinux_label {
                    return (
                        false,
                        volume_obj.device_path.clone(),
                        Some(ActualStateOfWorldError::SELinuxMountMismatch {
                            volume_name: volume_name.to_string(),
                        }),
                    );
                }
            }
        }

        let pod_obj = volume_obj.mounted_pods.get(pod_name);
        if let Some(pod_obj) = pod_obj {
            // if volume mount was uncertain we should keep trying to mount the volume
            if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMountUncertain {
                return (false, volume_obj.device_path.clone(), None);
            }
            if pod_obj.remount_required {
                return (
                    true,
                    volume_obj.device_path.clone(),
                    Some(ActualStateOfWorldError::RemountRequired {
                        volume_name: volume_obj.volume_name.to_string(),
                        pod_name: pod_obj.pod_name.to_string(),
                    }),
                );
            }
            let (current_size, expand_volume) =
                self.volume_needs_expansion(volume_obj, desired_volume_size);
            if expand_volume {
                return (
                    true,
                    volume_obj.device_path.clone(),
                    Some(ActualStateOfWorldError::FsResizeRequired {
                        current_size,
                        volume_name: volume_obj.volume_name.to_string(),
                        pod_name: pod_obj.pod_name.to_string(),
                    }),
                );
            }
        }

        (pod_obj.is_some(), volume_obj.device_path.clone(), None)
    }

    /// Returns true if the given pod does not exist in the list of
    /// `mounted_pods` for the given volume, indicating that the pod has fully
    /// unmounted it or never mounted the volume. If the volume is fully
    /// mounted or is in an uncertain mount state for the pod, the pod is
    /// considered to still exist in the volume manager's actual state of the
    /// world and false is returned.
    ///
    /// Port of `PodRemovedFromVolume` (`actual_state_of_world.go:996-1018`).
    ///
    /// **Not the negation of [`Self::pod_exists_in_volume`].** For a
    /// `VolumeMountUncertain` entry both return false: the volume does *not*
    /// exist (so do not use it) and is *not* removed (so keep unmounting it).
    /// `TestUncertainVolumeMounts` asserts both in the same breath. Each is
    /// written from upstream's own body; deriving either from the other loses
    /// that middle state.
    pub fn pod_removed_from_volume(
        &self,
        pod_name: &UniquePodName,
        volume_name: &UniqueVolumeName,
    ) -> bool {
        let state = self.state.read().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get(volume_name) else {
            return true;
        };

        if let Some(pod_obj) = volume_obj.mounted_pods.get(pod_name) {
            // if volume mount was uncertain we should keep trying to unmount the volume
            if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMountUncertain {
                return false;
            }
            if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted {
                return false;
            }
        }
        true
    }

    /// Returns true if any volume is mounted on the given pod.
    ///
    /// Port of `PodHasMountedVolumes` (`actual_state_of_world.go:955-967`).
    pub fn pod_has_mounted_volumes(&self, pod_name: &UniquePodName) -> bool {
        let state = self.state.read().expect("actual state of world lock");
        for volume_obj in state.attached_volumes.values() {
            if let Some(pod_obj) = volume_obj.mounted_pods.get(pod_name) {
                if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted {
                    return true;
                }
            }
        }

        false
    }

    /// Returns true if the given volume, specified with the volume spec name
    /// (a.k.a. `InnerVolumeSpecName`), exists in the list of volumes that
    /// should be attached to this node.
    ///
    /// Port of `VolumeExistsWithSpecName` (`actual_state_of_world.go:1020-1031`).
    pub fn volume_exists_with_spec_name(
        &self,
        pod_name: &UniquePodName,
        volume_spec_name: &str,
    ) -> bool {
        let state = self.state.read().expect("actual state of world lock");
        for volume_obj in state.attached_volumes.values() {
            if let Some(pod_obj) = volume_obj.mounted_pods.get(pod_name) {
                if pod_obj.volume_spec.name() == volume_spec_name {
                    return true;
                }
            }
        }
        false
    }

    /// Returns true if the given volume exists in the list of attached volumes
    /// in the cache, indicating the volume is attached to this node.
    ///
    /// Port of `VolumeExists` (`actual_state_of_world.go:1033-1040`).
    pub fn volume_exists(&self, volume_name: &UniqueVolumeName) -> bool {
        let state = self.state.read().expect("actual state of world lock");

        state.attached_volumes.contains_key(volume_name)
    }

    /// Generates and returns a list of volumes and the pods they are
    /// successfully attached and mounted for, based on the current actual
    /// state of the world.
    ///
    /// Port of `GetMountedVolumes` (`actual_state_of_world.go:1042-1057`).
    pub fn get_mounted_volumes(&self) -> Vec<MountedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut mounted_volume = Vec::with_capacity(state.attached_volumes.len());
        for volume_obj in state.attached_volumes.values() {
            for pod_obj in volume_obj.mounted_pods.values() {
                if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted {
                    mounted_volume.push(get_mounted_volume(pod_obj, volume_obj));
                }
            }
        }
        mounted_volume
    }

    /// Returns the list of all possibly mounted volumes, including those in
    /// the `VolumeMounted` state and the `VolumeMountUncertain` state.
    ///
    /// Port of `GetAllMountedVolumes` (`actual_state_of_world.go:1059-1075`).
    pub fn get_all_mounted_volumes(&self) -> Vec<MountedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut mounted_volume = Vec::with_capacity(state.attached_volumes.len());
        for volume_obj in state.attached_volumes.values() {
            for pod_obj in volume_obj.mounted_pods.values() {
                if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted
                    || pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMountUncertain
                {
                    mounted_volume.push(get_mounted_volume(pod_obj, volume_obj));
                }
            }
        }

        mounted_volume
    }

    /// Generates and returns a list of volumes that are successfully attached
    /// and mounted for the specified pod, based on the current actual state of
    /// the world.
    ///
    /// Port of `GetMountedVolumesForPod` (`actual_state_of_world.go:1077-1093`).
    pub fn get_mounted_volumes_for_pod(&self, pod_name: &UniquePodName) -> Vec<MountedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut mounted_volume = Vec::new();
        for volume_obj in state.attached_volumes.values() {
            for (mounted_pod_name, pod_obj) in &volume_obj.mounted_pods {
                if mounted_pod_name == pod_name
                    && pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted
                {
                    mounted_volume.push(get_mounted_volume(pod_obj, volume_obj));
                }
            }
        }

        mounted_volume
    }

    /// Returns the volume and true if the given name is mounted on the given
    /// pod.
    ///
    /// Port of `GetMountedVolumeForPod` (`actual_state_of_world.go:1095-1107`).
    /// Upstream returns `(MountedVolume{}, false)` when absent; `Option` is
    /// the Rust spelling of the same pair.
    pub fn get_mounted_volume_for_pod(
        &self,
        pod_name: &UniquePodName,
        volume_name: &UniqueVolumeName,
    ) -> Option<MountedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        // Upstream indexes the map without the comma-ok form, so a missing
        // volume yields a zero `attachedVolume` whose nil `mountedPods` map
        // then misses every lookup. `?` is the same "not found" outcome.
        let volume_obj = state.attached_volumes.get(volume_name)?;
        let pod_obj = volume_obj.mounted_pods.get(pod_name)?;
        if pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted {
            return Some(get_mounted_volume(pod_obj, volume_obj));
        }

        None
    }

    /// Generates and returns a list of volumes for the specified pod that
    /// either are attached and mounted or are "uncertain", i.e. a volume
    /// plugin may be mounting the volume right now.
    ///
    /// Port of `GetPossiblyMountedVolumesForPod`
    /// (`actual_state_of_world.go:1109-1127`).
    pub fn get_possibly_mounted_volumes_for_pod(
        &self,
        pod_name: &UniquePodName,
    ) -> Vec<MountedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut mounted_volume = Vec::with_capacity(state.attached_volumes.len());
        for volume_obj in state.attached_volumes.values() {
            for (mounted_pod_name, pod_obj) in &volume_obj.mounted_pods {
                if mounted_pod_name == pod_name
                    && (pod_obj.volume_mount_state_for_pod == VolumeMountState::VolumeMounted
                        || pod_obj.volume_mount_state_for_pod
                            == VolumeMountState::VolumeMountUncertain)
                {
                    mounted_volume.push(get_mounted_volume(pod_obj, volume_obj));
                }
            }
        }

        mounted_volume
    }

    /// Generates and returns a list of all attached volumes that are globally
    /// mounted. This list can be used to determine which volumes should be
    /// reported as "in use" in the node's `VolumesInUse` status field.
    /// Globally mounted here refers to the shared plugin mount point for the
    /// attachable volume from which the pod-specific mount points are created
    /// (via bind mount).
    ///
    /// Port of `GetGloballyMountedVolumes` (`actual_state_of_world.go:1129-1143`).
    pub fn get_globally_mounted_volumes(&self) -> Vec<AttachedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut globally_mounted_volumes = Vec::with_capacity(state.attached_volumes.len());
        for volume_obj in state.attached_volumes.values() {
            if volume_obj.device_mount_state == DeviceMountState::DeviceGloballyMounted {
                globally_mounted_volumes.push(self.new_attached_volume(volume_obj));
            }
        }

        globally_mounted_volumes
    }

    /// Returns a list of volumes that are known to be attached to the node.
    /// This list can be used to determine volumes that are either in use or
    /// have a mount/unmount operation pending.
    ///
    /// Port of `GetAttachedVolumes` (`actual_state_of_world.go:1145-1157`).
    pub fn get_attached_volumes(&self) -> Vec<AttachedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut all_attached_volumes = Vec::with_capacity(state.attached_volumes.len());
        for volume_obj in state.attached_volumes.values() {
            all_attached_volumes.push(self.new_attached_volume(volume_obj));
        }

        all_attached_volumes
    }

    /// Returns the volume that is known to be attached to the node with the
    /// given volume name, or `None` if it is not found.
    ///
    /// Port of `GetAttachedVolume` (`actual_state_of_world.go:1159-1169`).
    pub fn get_attached_volume(&self, volume_name: &UniqueVolumeName) -> Option<AttachedVolume> {
        let state = self.state.read().expect("actual state of world lock");

        state
            .attached_volumes
            .get(volume_name)
            .map(|volume_obj| self.new_attached_volume(volume_obj))
    }

    /// Generates and returns a list of attached volumes that have no
    /// `mounted_pods`. This list can be used to determine which volumes are no
    /// longer referenced and may be globally unmounted and detached.
    ///
    /// Port of `GetUnmountedVolumes` (`actual_state_of_world.go:1171-1184`).
    pub fn get_unmounted_volumes(&self) -> Vec<AttachedVolume> {
        let state = self.state.read().expect("actual state of world lock");
        let mut unmounted_volumes = Vec::with_capacity(state.attached_volumes.len());
        for volume_obj in state.attached_volumes.values() {
            if volume_obj.mounted_pods.is_empty() {
                unmounted_volumes.push(self.new_attached_volume(volume_obj));
            }
        }

        unmounted_volumes
    }

    /// Updates the `device_path` of a reconstructed volume from
    /// `Node.Status.VolumesAttached`. The ASW is updated only when the volume
    /// is still uncertain; if the volume got mounted in the meantime, its
    /// device path must have been fixed by such an update.
    ///
    /// Port of `UpdateReconstructedDevicePath`
    /// (`actual_state_of_world.go:570-586`).
    pub fn update_reconstructed_device_path(
        &self,
        volume_name: &UniqueVolumeName,
        device_path: &str,
    ) {
        let mut state = self.state.write().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) else {
            return;
        };
        if volume_obj.device_mount_state != DeviceMountState::DeviceMountUncertain {
            // Reconciler must have updated volume state, i.e. when a pod uses
            // the volume and succeeded mounting the volume. Such an update has
            // fixed the device path.
            return;
        }

        volume_obj.device_path = device_path.to_string();
    }

    /// Updates volume attachability from the API server.
    ///
    /// Port of `UpdateReconstructedVolumeAttachability`
    /// (`actual_state_of_world.go:588-608`). Uncertain attachability resolves
    /// exactly once — a volume that is already certainly `True` or `False`
    /// cannot be changed, which upstream's
    /// `TestActualStateOfWorld_FoundDuringReconstruction` pins with its
    /// "certain (true)/(false) attachability cannot be changed" cases.
    pub fn update_reconstructed_volume_attachability(
        &self,
        volume_name: &UniqueVolumeName,
        attachable: bool,
    ) {
        let mut state = self.state.write().expect("actual state of world lock");

        let Some(volume_obj) = state.attached_volumes.get_mut(volume_name) else {
            return;
        };
        if volume_obj.plugin_is_attachable != VolumeAttachability::Uncertain {
            // Reconciler must have updated volume state, i.e. when a pod uses
            // the volume and succeeded mounting the volume. Such an update has
            // fixed the device path.
            return;
        }

        volume_obj.plugin_is_attachable = if attachable {
            VolumeAttachability::True
        } else {
            VolumeAttachability::False
        };
    }

    // ---------------------------------------------------------------------
    // Unexported helpers
    // ---------------------------------------------------------------------

    /// Adds the given volume to the cache, indicating the specified volume is
    /// attached to this node. If no volume name is supplied, a unique volume
    /// name is generated from the `volume_spec`. If a volume with the same
    /// generated name already exists, only its device path is refreshed. If no
    /// volume plugin can support the given `volume_spec`, or more than one
    /// plugin can support it, an error is returned.
    ///
    /// Port of `addVolume` (`actual_state_of_world.go:675-715`).
    fn add_volume(
        &self,
        volume_name: Option<&UniqueVolumeName>,
        volume_spec: Arc<OwnedSpec>,
        device_path: &str,
        attachability: VolumeAttachability,
    ) -> Result<(), ActualStateOfWorldError> {
        let mut state = self.state.write().expect("actual state of world lock");

        let spec = volume_spec.as_spec();
        let volume_plugin = self
            .volume_plugin_mgr
            .find_plugin_by_spec(&spec)
            .map_err(|err| ActualStateOfWorldError::NoPlugin {
                volume_name: spec.name().to_string(),
                err: err.to_string(),
            })?;

        // `len(volumeName) == 0` upstream: the empty `v1.UniqueVolumeName` is
        // the caller's "you pick one". `None` says the same without a magic
        // empty string.
        let volume_name = match volume_name {
            Some(volume_name) => volume_name.clone(),
            None => get_unique_volume_name_from_spec(volume_plugin, &spec).map_err(|err| {
                ActualStateOfWorldError::UniqueVolumeName {
                    volume_name: spec.name().to_string(),
                    plugin_name: volume_plugin.name().to_string(),
                    err,
                }
            })?,
        };

        match state.attached_volumes.get_mut(&volume_name) {
            Some(volume_obj) => {
                // If volume object already exists, update the fields such as device path
                volume_obj.device_path = device_path.to_string();
                debug!(
                    volume_name = %volume_name,
                    path = device_path,
                    "Volume is already added to attachedVolume list, update device path"
                );
            }
            None => {
                state.attached_volumes.insert(
                    volume_name.clone(),
                    AttachedVolumeEntry {
                        volume_name,
                        spec: volume_spec.clone(),
                        mounted_pods: HashMap::new(),
                        plugin_name: volume_plugin.name().to_string(),
                        plugin_is_attachable: attachability,
                        device_mount_state: DeviceMountState::DeviceNotMounted,
                        device_path: device_path.to_string(),
                        // Go's zero values for the fields the literal omits.
                        device_mount_path: String::new(),
                        volume_in_use_error_for_expansion: false,
                        persistent_volume_size: None,
                        selinux_mount_context: None,
                    },
                );
            }
        }

        Ok(())
    }

    /// Port of `volumeNeedsExpansion` (`actual_state_of_world.go:969-994`).
    /// Returns the current size and whether a file-system resize is required.
    fn volume_needs_expansion(
        &self,
        volume_obj: &AttachedVolumeEntry,
        desired_volume_size: Option<Quantity>,
    ) -> (Option<Quantity>, bool) {
        let current_size = volume_obj.persistent_volume_size;
        if volume_obj.volume_in_use_error_for_expansion {
            return (current_size, false);
        }
        let (Some(persistent_volume_size), Some(desired_volume_size)) =
            (volume_obj.persistent_volume_size, desired_volume_size)
        else {
            // Either is upstream's zero `resource.Quantity`, i.e. `IsZero()`.
            return (current_size, false);
        };
        if persistent_volume_size.is_zero() || desired_volume_size.is_zero() {
            return (current_size, false);
        }

        debug!(
            actual_size = %persistent_volume_size,
            desired_size = %desired_volume_size,
            volume = %volume_obj.volume_name,
            "NodeExpandVolume checking size"
        );

        if desired_volume_size.cmp_value(&persistent_volume_size) == std::cmp::Ordering::Greater {
            let spec = volume_obj.spec.as_spec();
            let Some(volume_plugin) = self
                .volume_plugin_mgr
                .find_node_expandable_plugin_by_spec(&spec)
            else {
                // Log and continue processing
                debug!(
                    volume = %volume_obj.volume_name,
                    volume_spec_name = spec.name(),
                    "PodExistsInVolume failed to find expandable plugin"
                );
                return (current_size, false);
            };
            if volume_plugin.requires_fs_resize(&spec) {
                return (current_size, true);
            }
        }
        (current_size, false)
    }

    /// Port of `newAttachedVolume` (`actual_state_of_world.go:1186-1210`).
    ///
    /// **Where attachability collapses.** `plugin_is_attachable` on the wire
    /// is `attachedVolume.pluginIsAttachable == volumeAttachabilityTrue`, so
    /// `False` and `Uncertain` are indistinguishable to every consumer.
    fn new_attached_volume(&self, attached_volume: &AttachedVolumeEntry) -> AttachedVolume {
        let mut selinux_mount_context = String::new();
        if feature_gates::enabled(Feature::SELinuxMountReadWriteOncePod) {
            if let Some(context) = &attached_volume.selinux_mount_context {
                selinux_mount_context = context.clone();
            }
        }
        AttachedVolume {
            attached_volume: operation_executor::AttachedVolume {
                volume_name: attached_volume.volume_name.clone(),
                volume_spec: attached_volume.spec.clone(),
                node_name: self.node_name.clone(),
                plugin_is_attachable: attached_volume.plugin_is_attachable
                    == VolumeAttachability::True,
                device_path: attached_volume.device_path.clone(),
                device_mount_path: attached_volume.device_mount_path.clone(),
                plugin_name: attached_volume.plugin_name.clone(),
                selinux_mount_context: selinux_mount_context.clone(),
            },
            device_mount_state: attached_volume.device_mount_state,
            selinux_mount_context,
        }
    }
}

/// True for upstream's zero `resource.Quantity`, i.e. `q.IsZero()` where a
/// missing value is itself the zero quantity.
fn is_zero_quantity(quantity: Option<Quantity>) -> bool {
    quantity.is_none_or(|quantity| quantity.is_zero())
}

/// Constructs and returns a [`MountedVolume`] from the given [`MountedPod`]
/// and [`AttachedVolumeEntry`].
///
/// Port of `getMountedVolume` (`actual_state_of_world.go:1289-1311`). Note it
/// reads `attachedVolume.seLinuxMountContext` *without* the
/// `SELinuxMountReadWriteOncePod` gate that `newAttachedVolume` applies —
/// upstream asymmetry, preserved.
fn get_mounted_volume(
    mounted_pod: &MountedPod,
    attached_volume: &AttachedVolumeEntry,
) -> MountedVolume {
    let selinux_mount_context = attached_volume
        .selinux_mount_context
        .clone()
        .unwrap_or_default();
    MountedVolume(operation_executor::MountedVolume {
        pod_name: mounted_pod.pod_name.clone(),
        volume_name: attached_volume.volume_name.clone(),
        inner_volume_spec_name: mounted_pod.volume_spec.name().to_string(),
        plugin_name: attached_volume.plugin_name.clone(),
        pod_uid: mounted_pod.pod_uid.clone(),
        mounter: mounted_pod.mounter.clone(),
        block_volume_mapper: mounted_pod.block_volume_mapper.clone(),
        volume_gid_value: mounted_pod.volume_gid_value.clone(),
        volume_spec: mounted_pod.volume_spec.clone(),
        device_mount_path: attached_volume.device_mount_path.clone(),
        selinux_mount_context,
    })
}
