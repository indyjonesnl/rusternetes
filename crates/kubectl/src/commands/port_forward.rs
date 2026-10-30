use crate::websocket;
use anyhow::Result;
use rusternetes_client::http::ApiClient;

/// Forward one or more local ports to a pod
pub async fn execute(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    ports: &[String],
    address: &str,
) -> Result<()> {
    if ports.is_empty() {
        anyhow::bail!("At least one port must be specified");
    }

    // Parse port mapping (format: "local:remote" or "port")
    let (local_port, remote_port) = parse_port_mapping(&ports[0])?;

    // Build WebSocket URL
    let url_path = format!(
        "/api/v1/namespaces/{}/pods/{}/portforward?ports={}",
        namespace, pod_name, remote_port
    );

    let ws_url = client.get_ws_url(&url_path)?;

    // Start port forwarding
    websocket::port_forward_stream(ws_url, local_port, remote_port, address).await
}

pub fn parse_port_mapping(port_spec: &str) -> Result<(u16, u16)> {
    if let Some((local, remote)) = port_spec.split_once(':') {
        let local_port = local.parse::<u16>()?;
        let remote_port = remote.parse::<u16>()?;
        Ok((local_port, remote_port))
    } else {
        // If only one port specified, use same for both local and remote
        let port = port_spec.parse::<u16>()?;
        Ok((port, port))
    }
}
