/// Resolve the kubelet connection parameters from a `Node` resource.
///
/// Extracted from `proxy.rs` so that Tasks 6 and 7 (exec/attach/logs routing)
/// can reuse the same InternalIP→ExternalIP fallback, advertised-port lookup,
/// and scheme selection without duplicating the logic.
use rusternetes_common::resources::Node;

/// Resolved kubelet connection parameters for a node.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeConn {
    /// The hostname or IP address to connect to (InternalIP preferred, ExternalIP fallback).
    pub host: String,
    /// The TCP port of the kubelet API.
    pub port: u16,
    /// URL scheme. Always `"https"`: the kubelet serves its `:10250` API over
    /// TLS (#1644), matching upstream, and a Kubernetes api-server always
    /// proxies logs/exec/attach/metrics to `https://<nodeIP>:10250`. The
    /// kubelet's serving cert is self-signed, so the proxy client skips
    /// verification (see `streamproxy`'s TLS connector and the reqwest
    /// `danger_accept_invalid_certs` log client).
    pub scheme: &'static str,
}

/// Resolve kubelet connection parameters from a node's status.
///
/// Port resolution order (mirrors `pkg/registry/core/node/strategy.go::ResourceLocation`):
/// 1. `port_override` — explicit port from the URL id (`<name>:<port>`).
/// 2. `status.daemonEndpoints.kubeletEndpoint.port` — advertised by the kubelet.
/// 3. 10250 — the Kubernetes default.
///
/// Returns `Err(rusternetes_common::Error::NotFound)` when the node has no
/// usable address.
pub fn node_conn(
    node: &Node,
    port_override: Option<u16>,
) -> Result<NodeConn, rusternetes_common::Error> {
    let host = node
        .status
        .as_ref()
        .and_then(|s| s.addresses.as_ref())
        .and_then(|addrs| {
            addrs
                .iter()
                .find(|a| a.address_type == "InternalIP")
                .or_else(|| addrs.iter().find(|a| a.address_type == "ExternalIP"))
        })
        .map(|a| a.address.clone())
        .ok_or_else(|| {
            rusternetes_common::Error::NotFound(format!(
                "No address found for node {}",
                node.metadata.name,
            ))
        })?;

    let port = port_override.unwrap_or_else(|| {
        node.status
            .as_ref()
            .and_then(|s| s.daemon_endpoints.as_ref())
            .and_then(|d| d.kubelet_endpoint.as_ref())
            .map(|e| e.port as u16)
            .unwrap_or(10250)
    });

    Ok(NodeConn {
        host,
        port,
        scheme: "https",
    })
}
