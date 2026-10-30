use crate::websocket;
use anyhow::Result;
use rusternetes_client::http::ApiClient;

/// Attach to a running container in a pod.
///
/// This is similar to exec but attaches to the main process (PID 1)
/// of the container rather than running a new command.
pub async fn execute(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    container: Option<&str>,
    tty: bool,
    stdin: bool,
) -> Result<()> {
    // Build the attach URL path
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/attach", namespace, pod_name);

    // Add query parameters
    let mut query_params = vec![];

    if let Some(cont) = container {
        query_params.push(format!("container={}", cont));
    }

    query_params.push("stdout=true".to_string());
    query_params.push("stderr=true".to_string());

    if stdin {
        query_params.push("stdin=true".to_string());
    }

    if tty {
        query_params.push("tty=true".to_string());
    }

    if !query_params.is_empty() {
        url_path.push('?');
        url_path.push_str(&query_params.join("&"));
    }

    // Convert HTTP(S) URL to WebSocket URL
    let ws_url = client.get_ws_url(&url_path)?;

    // Execute with WebSocket streaming (same mechanism as exec)
    websocket::exec_stream(ws_url, stdin, tty).await
}
