//! Shared CSI v1 wire layer: the generated `csi.v1` proto and the Controller
//! service client, usable by both the kubelet (Node service) and the
//! controller-manager (external-provisioner role).

pub mod controller_client;

/// Generated from `proto/csi/v1/csi.proto` (package `csi.v1`).
#[allow(clippy::result_large_err, clippy::large_enum_variant)]
pub mod proto {
    tonic::include_proto!("csi.v1");
}

use std::path::PathBuf;

/// Strip a `unix://` (or `unix:`) scheme, leaving the socket path. The
/// registered endpoint is a path (`csi_plugin.go:118`) but plugin-registration
/// hands over `unix://` forms in some deployments.
pub fn normalize_endpoint(endpoint: &std::path::Path) -> PathBuf {
    let s = endpoint.to_string_lossy();
    let trimmed = s
        .strip_prefix("unix://")
        .or_else(|| s.strip_prefix("unix:"))
        .unwrap_or(&s);
    PathBuf::from(trimmed)
}
