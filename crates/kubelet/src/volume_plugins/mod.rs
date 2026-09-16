//! Port of `pkg/volume` — the volume plugin interface and its registry.
//!
//! Upstream's kubelet does not dispatch on volume kind; it asks a registry of
//! plugins which one supports a given `Spec`. Every part of the volume manager
//! (caches, reconciler, reconstruction) is written against that interface, so
//! it is the seam the rest of epic #1970 needs.

pub mod config_map;
pub mod empty_dir;
pub mod host;
pub mod host_path;
pub mod plugin;
pub mod registry;
pub mod secret;

pub use host::{KubeletVolumeHost, VolumeHost};
pub use plugin::{Mounter, Spec, VolumePlugin};
pub use registry::{PluginLookupError, VolumePluginMgr};
