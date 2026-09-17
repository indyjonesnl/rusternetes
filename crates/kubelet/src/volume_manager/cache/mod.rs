//! Port of `pkg/kubelet/volumemanager/cache` — the data structures the kubelet
//! volume manager uses to track attached volumes and the pods that mounted
//! them.

pub mod actual_state_of_world;
pub mod desired_state_of_world;
pub mod desired_state_of_world_selinux_metrics;

pub use actual_state_of_world::{
    is_fs_resize_required_error, is_remount_required_error, is_selinux_mount_mismatch_error,
    is_volume_not_attached_error, ActualStateOfWorld, ActualStateOfWorldError, AttachedVolume,
    MountedVolume,
};
pub use desired_state_of_world::{DesiredStateOfWorld, DesiredStateOfWorldError, VolumeToMount};
