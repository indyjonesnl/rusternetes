//! Port of `pkg/kubelet/volumemanager` — the component that keeps the volumes
//! a node's pods need attached and mounted.
//!
//! Only the caches are here so far; the populator, the reconciler and the
//! `VolumeManager` itself arrive with their own sub-projects (#1970). Nothing
//! in the running kubelet calls this yet.

pub mod cache;
