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

#[cfg(test)]
mod tests {
    use super::proto::GetCapacityResponse;
    use prost::Message;

    /// csi.proto GetCapacityResponse fields 2/3 are `google.protobuf.Int64Value`
    /// (vendored csi.pb.go:3044,3060). external-provisioner's capacity
    /// controller reads maximum_volume_size; unset must stay None.
    #[test]
    fn get_capacity_response_decodes_min_max_volume_size() {
        // field1 varint 5; field2 = Int64Value{7}; field3 = Int64Value{3}
        let wire = [0x08, 0x05, 0x12, 0x02, 0x08, 0x07, 0x1a, 0x02, 0x08, 0x03];
        let r = GetCapacityResponse::decode(&wire[..]).unwrap();
        assert_eq!(r.available_capacity, 5);
        assert_eq!(r.maximum_volume_size, Some(7));
        assert_eq!(r.minimum_volume_size, Some(3));
        let r = GetCapacityResponse::decode(&wire[..2]).unwrap();
        assert_eq!(r.maximum_volume_size, None);
    }
}
