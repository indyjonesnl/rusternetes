//! Port of `pkg/kubelet/volumemanager/cache` — the data structures the kubelet
//! volume manager uses to track attached volumes and the pods that mounted
//! them.
//!
//! `ActualStateOfWorld` arrives with its own sub-project (#1970).

pub mod desired_state_of_world;
pub mod desired_state_of_world_selinux_metrics;

pub use desired_state_of_world::{DesiredStateOfWorld, DesiredStateOfWorldError, VolumeToMount};
