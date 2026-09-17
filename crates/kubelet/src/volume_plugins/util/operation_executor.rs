//! Port of the shared value types in
//! `pkg/volume/util/operationexecutor/operation_executor.go`.
//!
//! `VolumeToMount` is what `DesiredStateOfWorld` materialises;
//! `MarkVolumeOpts`, the two mount-state enums and
//! `AttachedVolume`/`MountedVolume` are what `ActualStateOfWorld` consumes and
//! produces. The operation executor itself arrives with the sub-project that
//! runs the mount operations (#1970).

use crate::volume_plugins::plugin::{BlockVolumeMapper, Mounter, OwnedSpec};
use crate::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::Pod;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

/// Port of `operationexecutor.VolumeToMount`
/// (`pkg/volume/util/operationexecutor/operation_executor.go:410-465`).
///
/// One entry per (volume, pod) pair.
///
/// **Ownership.** Upstream's `Pod *v1.Pod` and `VolumeSpec *volume.Spec` are
/// raw pointers copied out from under the cache's read lock, which the caller
/// could mutate afterwards; nothing upstream does. `Arc` reproduces the
/// sharing (one pod object, many readers, no deep copy) while making the
/// immutability the invariant already relies on explicit.
#[derive(Clone)]
pub struct VolumeToMount {
    /// The unique identifier for the volume that should be mounted.
    pub volume_name: UniqueVolumeName,

    /// The unique identifier for the pod that the volume should be mounted to
    /// after it is attached.
    pub pod_name: UniquePodName,

    /// A volume spec containing the specification for the volume that should
    /// be mounted. Used to create NewMounter. Used to generate
    /// InnerVolumeSpecName.
    pub volume_spec: Arc<OwnedSpec>,

    /// The `podSpec.Volume[x].Name`s of the volume.
    pub outer_volume_spec_names: Vec<String>,

    /// Pod to mount the volume to. Used to create NewMounter.
    pub pod: Arc<Pod>,

    /// Indicates that the plugin for this volume implements the
    /// `volume.Attacher` interface.
    pub plugin_is_attachable: bool,

    /// Indicates that the plugin for this volume implements the
    /// `volume.DeviceMounter` interface.
    pub plugin_is_device_mountable: bool,

    /// The value of the GID annotation, if present.
    pub volume_gid_value: String,

    /// The path on the node where the volume is attached. For non-attachable
    /// volumes this is empty.
    pub device_path: String,

    /// Indicates that the volume was successfully added to the `VolumesInUse`
    /// field in the node's status.
    pub reported_in_use: bool,

    /// The desired upper bound on the size of the volume (if so implemented).
    pub desired_size_limit: Option<Quantity>,

    /// Time at which the volume was requested to be mounted.
    pub mount_request_time: SystemTime,

    /// Desired size of the volume, usually `pv.Spec.Capacity`.
    ///
    /// **Shape deviation:** upstream's field is a value `resource.Quantity`
    /// whose zero value means "not recorded", and the only writer guards on
    /// `!persistentVolumeSize.IsZero()` (`desired_state_of_world.go:609-611`).
    /// `Option` says that directly; `None` is upstream's zero value.
    pub desired_persistent_volume_size: Option<Quantity>,

    /// SELinux label that should be used to mount. The label is set when:
    /// * the `SELinuxMountReadWriteOncePod` feature gate is enabled and the
    ///   volume is RWOP and kubelet knows the SELinux label,
    /// * or the `SELinuxMount` feature gate is enabled and kubelet knows the
    ///   SELinux label.
    pub selinux_label: String,
}

/// Port of `DeviceMountState` (`operation_executor.go:467-480`):
///
/// ```go
/// type DeviceMountState string
/// const (
///     DeviceGloballyMounted DeviceMountState = "DeviceGloballyMounted"
///     DeviceMountUncertain  DeviceMountState = "DeviceMountUncertain"
///     DeviceNotMounted      DeviceMountState = "DeviceNotMounted"
/// )
/// ```
///
/// The device mount state in a global path. `DeviceNotMounted` is the default
/// because `addVolume` writes it explicitly into every new `attachedVolume`
/// (`actual_state_of_world.go:707`), so Go's zero value for this field is
/// never observable — unlike [`VolumeMountState`], where it is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "these are upstream's constant names verbatim; renaming them would \
              break the one-to-one mapping reviewers check the port against"
)]
pub enum DeviceMountState {
    /// Device has been globally mounted successfully.
    DeviceGloballyMounted,
    /// Device may not be mounted but a mount operation may be in-progress
    /// which can cause device mount to succeed.
    DeviceMountUncertain,
    /// Device has not been mounted globally.
    #[default]
    DeviceNotMounted,
}

impl fmt::Display for DeviceMountState {
    /// The Go constants are their own names as strings.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DeviceGloballyMounted => "DeviceGloballyMounted",
            Self::DeviceMountUncertain => "DeviceMountUncertain",
            Self::DeviceNotMounted => "DeviceNotMounted",
        })
    }
}

/// Port of `VolumeMountState` (`operation_executor.go:482-494`):
///
/// ```go
/// type VolumeMountState string
/// const (
///     VolumeMounted        VolumeMountState = "VolumeMounted"
///     VolumeMountUncertain VolumeMountState = "VolumeMountUncertain"
///     VolumeNotMounted     VolumeMountState = "VolumeNotMounted"
/// )
/// ```
///
/// The volume mount state in a path local to the pod.
///
/// **Four variants for three Go constants.** Go's type is string-kinded, so
/// the zero value of a `MarkVolumeOpts` field is `""` — a fourth state,
/// distinct from all three constants, that reaches
/// `mountedPod.volumeMountStateForPod` whenever a caller leaves
/// `MarkVolumeOpts.VolumeMountState` unset. It is not dead: upstream's
/// `Test_AddTwoPodsToVolume_Positive` builds `MarkVolumeOpts` without the
/// field and then asserts `IsVolumeMountedElsewhere` is **true**, which only
/// holds because `IsVolumeMountedElsewhere` tests
/// `!= VolumeNotMounted` (`actual_state_of_world.go:660`) and `""` is not
/// `"VolumeNotMounted"`. Collapsing the zero value onto `VolumeNotMounted`
/// would flip that assertion, so it is modelled as [`Self::Unspecified`].
/// Production never sets it: every `MarkVolumeOpts` literal outside tests
/// passes `VolumeMounted` or `VolumeMountUncertain`
/// (`operation_generator.go:598`, `:766`, `:1031`, `:1203`,
/// `reconstruct.go:119`).
///
/// `VolumeNotMounted` is never *stored*: it is what
/// `ActualStateOfWorld::get_volume_mount_state` returns when the volume or the
/// pod entry is absent (`actual_state_of_world.go:633-647`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VolumeMountState {
    /// Volume has been mounted in the pod's local path.
    VolumeMounted,
    /// Volume may or may not be mounted in the pod's local path.
    VolumeMountUncertain,
    /// Volume has not been mounted in the pod's local path.
    VolumeNotMounted,
    /// Go's zero value for the string-kinded type — see the type comment.
    #[default]
    Unspecified,
}

impl fmt::Display for VolumeMountState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::VolumeMounted => "VolumeMounted",
            Self::VolumeMountUncertain => "VolumeMountUncertain",
            Self::VolumeNotMounted => "VolumeNotMounted",
            Self::Unspecified => "",
        })
    }
}

/// Port of `MarkVolumeOpts` (`operation_executor.go:165-175`) — the argument
/// struct of `MarkVolumeAsMounted`, `MarkVolumeMountAsUncertain`,
/// `AddPodToVolume` and `CheckAndMarkVolumeAsUncertainViaReconstruction`.
///
/// **Ownership.** `Mounter` and `BlockVolumeMapper` are `Option<Arc<…>>`:
/// upstream's fields are interface values that are legitimately nil — `nil`
/// is what makes `AddPodToVolume`'s `if mounter != nil` guard
/// (`actual_state_of_world.go:769-772`) meaningful, and
/// `TestUncertainVolumeMounts` builds opts with no mapper at all. `Arc`
/// reproduces Go's shared-pointer semantics.
///
/// `VolumeSpec` is not optional. Upstream's is a nilable `*volume.Spec`, but
/// `VolumeExistsWithSpecName` dereferences `podObj.volumeSpec.Name()` without
/// a nil check (`actual_state_of_world.go:1025`), so a nil spec is already a
/// panic there; every caller supplies one.
#[derive(Clone)]
pub struct MarkVolumeOpts {
    pub pod_name: UniquePodName,
    pub pod_uid: String,
    pub volume_name: UniqueVolumeName,
    pub mounter: Option<Arc<dyn Mounter>>,
    pub block_volume_mapper: Option<Arc<dyn BlockVolumeMapper>>,
    pub volume_gid_volume: String,
    pub volume_spec: Arc<OwnedSpec>,
    pub volume_mount_state: VolumeMountState,
    pub selinux_mount_context: String,
}

/// Port of `operationexecutor.AttachedVolume`
/// (`operation_executor.go:545-573`) — a volume that is attached to a node.
///
/// The cache's own richer `cache::AttachedVolume` embeds this and adds the
/// device mount state; see that type.
#[derive(Clone)]
pub struct AttachedVolume {
    /// The unique identifier for the volume that is attached.
    pub volume_name: UniqueVolumeName,

    /// The volume spec containing the specification for the volume that is
    /// attached.
    pub volume_spec: Arc<OwnedSpec>,

    /// The identifier for the node that the volume is attached to.
    pub node_name: String,

    /// Indicates that the plugin for this volume implements the
    /// `volume.Attacher` interface.
    pub plugin_is_attachable: bool,

    /// The path on the node where the volume is attached. For non-attachable
    /// volumes this is empty.
    pub device_path: String,

    /// The path on the node where the device should be mounted after it is
    /// attached.
    pub device_mount_path: String,

    /// The Unescaped Qualified name of the volume plugin used to attach and
    /// mount this volume.
    pub plugin_name: String,

    pub selinux_mount_context: String,
}

/// Port of `operationexecutor.MountedVolume`
/// (`operation_executor.go:634-715`) — a volume that has been mounted to a
/// pod.
#[derive(Clone)]
pub struct MountedVolume {
    /// The unique identifier of the pod mounted to.
    pub pod_name: UniquePodName,

    /// The unique identifier of the volume mounted to the pod.
    pub volume_name: UniqueVolumeName,

    /// The `volume.Spec.Name()` of the volume. If the volume was referenced
    /// through a persistent volume claim, this contains the name of the bound
    /// persistent volume object. It is the name that plugins use in their pod
    /// mount path, i.e.
    /// `/var/lib/kubelet/pods/{podUID}/volumes/{escapeQualifiedPluginName}/{innerVolumeSpecName}/`.
    pub inner_volume_spec_name: String,

    /// The "Unescaped Qualified" name of the volume plugin used to mount and
    /// unmount this volume.
    pub plugin_name: String,

    /// The UID of the pod mounted to.
    pub pod_uid: String,

    /// The volume mounter used to mount this volume. Required by kubelet to
    /// create `container.VolumeMap`. Only required for file system volumes,
    /// not for block volumes — hence `Option`, which is upstream's nil.
    pub mounter: Option<Arc<dyn Mounter>>,

    /// The volume mapper used to map this volume. Required by kubelet to
    /// create `container.VolumeMap`. Only required for block volumes, not for
    /// file system volumes.
    pub block_volume_mapper: Option<Arc<dyn BlockVolumeMapper>>,

    /// The value of the GID annotation, if present.
    pub volume_gid_value: String,

    /// A volume spec containing the specification for the volume that should
    /// be mounted.
    pub volume_spec: Arc<OwnedSpec>,

    /// The path on the node where the device should be mounted after it is
    /// attached.
    pub device_mount_path: String,

    /// The value of the mount option `mount -o context=XYZ`. If empty, no such
    /// mount option was used.
    pub selinux_mount_context: String,
}
