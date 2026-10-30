use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use std::fs;
use std::path::Path;

/// Copy files between local filesystem and containers
pub async fn execute(
    client: &ApiClient,
    source: &str,
    destination: &str,
    namespace: &str,
    container: Option<&str>,
) -> Result<()> {
    // Parse source and destination to determine direction
    let (is_upload, pod_name, pod_path, local_path) = parse_copy_spec(source, destination)?;

    if is_upload {
        copy_to_pod(
            client,
            namespace,
            &pod_name,
            &pod_path,
            &local_path,
            container,
        )
        .await
    } else {
        copy_from_pod(
            client,
            namespace,
            &pod_name,
            &pod_path,
            &local_path,
            container,
        )
        .await
    }
}

pub fn parse_copy_spec(source: &str, dest: &str) -> Result<(bool, String, String, String)> {
    // pod:path format
    if source.contains(':') && !dest.contains(':') {
        // Copy from pod to local
        let parts: Vec<&str> = source.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid source format: {}", source);
        }
        Ok((
            false,
            parts[0].to_string(),
            parts[1].to_string(),
            dest.to_string(),
        ))
    } else if dest.contains(':') && !source.contains(':') {
        // Copy from local to pod
        let parts: Vec<&str> = dest.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid destination format: {}", dest);
        }
        Ok((
            true,
            parts[0].to_string(),
            parts[1].to_string(),
            source.to_string(),
        ))
    } else {
        anyhow::bail!("One of source or destination must be in pod:path format");
    }
}

pub async fn copy_to_pod(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    pod_path: &str,
    local_path: &str,
    container: Option<&str>,
) -> Result<()> {
    // Check if local path exists
    let local_metadata =
        fs::metadata(local_path).context(format!("Local path not found: {}", local_path))?;

    // Create tar archive of local file/directory
    let tar_data = if local_metadata.is_dir() {
        create_tar_from_dir(local_path)?
    } else {
        create_tar_from_file(local_path)?
    };

    // Build exec command to extract tar in pod
    let extract_cmd = vec![
        "tar".to_string(),
        "-xf".to_string(),
        "-".to_string(),
        "-C".to_string(),
        pod_path.to_string(),
    ];

    // Execute tar extraction via exec API
    exec_with_stdin(
        client,
        namespace,
        pod_name,
        container,
        &extract_cmd,
        &tar_data,
    )
    .await?;

    println!("Copied {} to {}:{}", local_path, pod_name, pod_path);
    Ok(())
}

pub async fn copy_from_pod(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    pod_path: &str,
    local_path: &str,
    container: Option<&str>,
) -> Result<()> {
    // Build exec command to create tar in pod
    let tar_cmd = vec![
        "tar".to_string(),
        "-cf".to_string(),
        "-".to_string(),
        "-C".to_string(),
        "/".to_string(),
        pod_path.trim_start_matches('/').to_string(),
    ];

    // Execute tar creation and capture output
    let tar_data = exec_capture_output(client, namespace, pod_name, container, &tar_cmd).await?;

    // Extract tar to local filesystem
    extract_tar_to_local(local_path, &tar_data)?;

    println!("Copied {}:{} to {}", pod_name, pod_path, local_path);
    Ok(())
}

pub fn create_tar_from_file(path: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut output);
        let file_path = Path::new(path);
        let file_name = file_path.file_name().context("Invalid file name")?;
        tar.append_path_with_name(path, file_name)?;
        tar.finish()?;
    }
    Ok(output)
}

pub fn create_tar_from_dir(path: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut output);
        tar.append_dir_all(".", path)?;
        tar.finish()?;
    }
    Ok(output)
}

pub fn extract_tar_to_local(dest_path: &str, tar_data: &[u8]) -> Result<()> {
    let mut archive = tar::Archive::new(tar_data);
    archive.unpack(dest_path)?;
    Ok(())
}

async fn exec_with_stdin(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    container: Option<&str>,
    command: &[String],
    stdin_data: &[u8],
) -> Result<()> {
    use crate::websocket::{StreamChannel, StreamMessage};
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    // Build the exec URL with query parameters
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec?", namespace, pod_name);

    for cmd in command {
        url_path.push_str(&format!("command={}&", urlencoding::encode(cmd)));
    }

    if let Some(cont) = container {
        url_path.push_str(&format!("container={}&", cont));
    }

    url_path.push_str("stdin=true&stdout=true&stderr=true");

    // Get WebSocket URL
    let ws_url = client.get_ws_url(&url_path)?;
    let url = url::Url::parse(&ws_url)?;

    // Connect WebSocket
    let (ws_stream, _) = connect_async(url.as_str())
        .await
        .context("Failed to connect to exec WebSocket")?;

    let (mut write, mut read) = ws_stream.split();

    // Send stdin data
    let stdin_msg = StreamMessage::new(StreamChannel::Stdin, stdin_data.to_vec());
    write
        .send(Message::Binary(stdin_msg.encode()))
        .await
        .context("Failed to send stdin data")?;

    // Close stdin to signal end of input
    write.close().await.ok();

    // Read output (for error handling)
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok(stream_msg) = StreamMessage::decode(&data) {
                    match stream_msg.channel {
                        StreamChannel::Stdout
                            // Print any stdout for debugging
                            if !stream_msg.data.is_empty() => {
                                print!("{}", String::from_utf8_lossy(&stream_msg.data));
                            }
                        StreamChannel::Stderr
                            // Print stderr
                            if !stream_msg.data.is_empty() => {
                                eprint!("{}", String::from_utf8_lossy(&stream_msg.data));
                            }
                        StreamChannel::Error => {
                            return Err(anyhow::anyhow!(
                                "Exec error: {}",
                                String::from_utf8_lossy(&stream_msg.data)
                            ));
                        }
                        _ => {}
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => return Err(anyhow::anyhow!("WebSocket error: {}", e)),
            _ => {}
        }
    }

    Ok(())
}

async fn exec_capture_output(
    client: &ApiClient,
    namespace: &str,
    pod_name: &str,
    container: Option<&str>,
    command: &[String],
) -> Result<Vec<u8>> {
    use crate::websocket::{StreamChannel, StreamMessage};
    use futures::StreamExt;
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    // Build the exec URL with query parameters
    let mut url_path = format!("/api/v1/namespaces/{}/pods/{}/exec?", namespace, pod_name);

    for cmd in command {
        url_path.push_str(&format!("command={}&", urlencoding::encode(cmd)));
    }

    if let Some(cont) = container {
        url_path.push_str(&format!("container={}&", cont));
    }

    url_path.push_str("stdout=true&stderr=true");

    // Get WebSocket URL
    let ws_url = client.get_ws_url(&url_path)?;
    let url = url::Url::parse(&ws_url)?;

    // Connect WebSocket
    let (ws_stream, _) = connect_async(url.as_str())
        .await
        .context("Failed to connect to exec WebSocket")?;

    let (_, mut read) = ws_stream.split();

    // Capture stdout data
    let mut output = Vec::new();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                if let Ok(stream_msg) = StreamMessage::decode(&data) {
                    match stream_msg.channel {
                        StreamChannel::Stdout => {
                            // Append stdout data to output buffer
                            output.extend_from_slice(&stream_msg.data);
                        }
                        StreamChannel::Stderr => {
                            // Log stderr but don't include in output
                            eprintln!("{}", String::from_utf8_lossy(&stream_msg.data));
                        }
                        StreamChannel::Error => {
                            return Err(anyhow::anyhow!(
                                "Exec error: {}",
                                String::from_utf8_lossy(&stream_msg.data)
                            ));
                        }
                        _ => {}
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => return Err(anyhow::anyhow!("WebSocket error: {}", e)),
            _ => {}
        }
    }

    Ok(output)
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
