//! Port of `pkg/volume` — the volume plugin interface and its registry.
//!
//! Upstream's kubelet does not dispatch on volume kind; it asks a registry of
//! plugins which one supports a given `Spec`. Every part of the volume manager
//! (caches, reconciler, reconstruction) is written against that interface, so
//! it is the seam the rest of epic #1970 needs.

pub mod config_map;
#[cfg(test)]
mod create_volume_tests;
pub mod csi;
pub mod csi_client;
pub mod csi_drivers_store;
pub mod csi_node_updater;
pub mod downward_api;
pub mod empty_dir;
pub mod host;
pub mod host_path;
pub mod nodeinfomanager;
pub mod plugin;
pub mod projected;
pub mod registry;
pub mod secret;
pub mod util;

pub use host::{KubeletVolumeHost, VolumeHost};
pub use plugin::{
    DeviceMounter, DeviceMounterArgs, Mounter, OwnedSpec, ReconstructedVolume, Spec, Unmounter,
    VolumePlugin,
};
pub use registry::{PluginLookupError, VolumePluginMgr};
