use crate::websocket;
use anyhow::Result;
use rusternetes_client::http::ApiClient;

/// Execute a command in a container
pub async fn execute(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    container: Option<&str>,
    command: &[String],
    tty: bool,
    stdin: bool,
) -> Result<()> {
    // Build the exec URL path
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec", namespace, pod_name);

    // Add query parameters
    let mut query_params = vec![];

    for cmd in command {
        query_params.push(format!("command={}", urlencoding::encode(cmd)));
    }

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

    // Execute with WebSocket streaming
    websocket::exec_stream(ws_url, stdin, tty).await
}

pub mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}
