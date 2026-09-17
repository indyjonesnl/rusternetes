//! Port of the shared value types in
//! `pkg/volume/util/operationexecutor/operation_executor.go`.
//!
//! Only `VolumeToMount` is ported here — it is what `DesiredStateOfWorld`
//! materialises. The operation executor itself, the mount-state enums and
//! `MountedVolume`/`AttachedVolume` arrive with the sub-projects that need
//! them (#1970).

use crate::volume_plugins::plugin::OwnedSpec;
use crate::volume_plugins::util::types::{UniquePodName, UniqueVolumeName};
use rusternetes_common::quantity::Quantity;
use rusternetes_common::resources::Pod;
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
