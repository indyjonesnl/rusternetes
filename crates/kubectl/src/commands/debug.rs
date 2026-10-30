use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Create debugging sessions for troubleshooting workloads and nodes.
///
/// Adds an ephemeral container to a running pod for debugging.
/// Equivalent to `kubectl debug pod/nginx -it --image=busybox`
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    target: &str,
    namespace: &str,
    image: &str,
    container_name: Option<&str>,
    interactive: bool,
    tty: bool,
    target_container: Option<&str>,
    command: &[String],
) -> Result<()> {
    // Parse target: could be "pod/name" or just "name" (defaults to pod)
    let (resource_type, pod_name) = parse_target(target)?;

    match resource_type.as_str() {
        "pod" | "pods" | "po" => {
            debug_pod(
                client,
                pod_name,
                namespace,
                image,
                container_name,
                interactive,
                tty,
                target_container,
                command,
            )
            .await
        }
        "node" | "nodes" | "no" => {
            debug_node(client, pod_name, image, container_name, interactive, tty).await
        }
        _ => anyhow::bail!(
            "unsupported resource type for debug: {}. Supported: pod, node",
            resource_type
        ),
    }
}

pub fn parse_target(target: &str) -> Result<(String, &str)> {
    if let Some((rtype, name)) = target.split_once('/') {
        Ok((rtype.to_lowercase(), name))
    } else {
        // Default to pod
        Ok(("pod".to_string(), target))
    }
}

#[allow(clippy::too_many_arguments)]
async fn debug_pod(
    client: &ApiClient,
    pod_name: &str,
    namespace: &str,
    image: &str,
    container_name: Option<&str>,
    _interactive: bool,
    _tty: bool,
    target_container: Option<&str>,
    command: &[String],
) -> Result<()> {
    // Verify the pod exists
    let pod_path = format!("/api/v1/namespaces/{}/pods/{}", namespace, pod_name);
    let pod: Value = client
        .get(&pod_path)
        .await
        .map_err(|e| anyhow::anyhow!("pod \"{}\" not found: {}", pod_name, e))?;

    // Generate a container name if not provided
    let debug_container_name = container_name
        .map(|s| s.to_string())
        .unwrap_or_else(|| generate_debug_container_name(&pod));

    // Build the ephemeral container spec
    let mut ephemeral_container = json!({
        "name": debug_container_name,
        "image": image,
        "stdin": true,
        "tty": true,
    });

    if let Some(target) = target_container {
        ephemeral_container["targetContainerName"] = json!(target);
    }

    if !command.is_empty() {
        ephemeral_container["command"] = json!(command);
    }

    // Get existing ephemeral containers
    let existing_ephemeral = pod
        .get("spec")
        .and_then(|s| s.get("ephemeralContainers"))
        .cloned()
        .unwrap_or_else(|| json!([]));

    let mut ephemeral_list = if let Some(arr) = existing_ephemeral.as_array() {
        arr.clone()
    } else {
        vec![]
    };
    ephemeral_list.push(ephemeral_container);

    // PATCH the pod's ephemeralcontainers subresource
    let patch = json!({
        "spec": {
            "ephemeralContainers": ephemeral_list,
        }
    });

    let patch_path = format!(
        "/api/v1/namespaces/{}/pods/{}/ephemeralcontainers",
        namespace, pod_name
    );

    let _result: Value = client
        .patch(
            &patch_path,
            &patch,
            "application/strategic-merge-patch+json",
        )
        .await
        .context("Failed to add ephemeral container to pod")?;

    println!(
        "Defaulting debug container name to {}.",
        debug_container_name
    );

    // Note: In a real implementation, we would attach to the container here
    // using websocket exec. For now, we just inform the user.
    eprintln!(
        "If you don't see a command prompt, try pressing enter.\n\
         Use 'kubectl attach {} -c {} -i -t' to connect to the debug container.",
        pod_name, debug_container_name
    );

    Ok(())
}

async fn debug_node(
    client: &ApiClient,
    node_name: &str,
    image: &str,
    container_name: Option<&str>,
    _interactive: bool,
    _tty: bool,
) -> Result<()> {
    // Verify the node exists
    let node_path = format!("/api/v1/nodes/{}", node_name);
    let _node: Value = client
        .get(&node_path)
        .await
        .map_err(|e| anyhow::anyhow!("node \"{}\" not found: {}", node_name, e))?;

    let debug_pod_name = format!("node-debugger-{}-{}", node_name, generate_suffix());
    let cname = container_name.unwrap_or("debugger");

    // Create a pod that runs in the node's host namespaces
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": debug_pod_name,
            "namespace": "default",
        },
        "spec": {
            "nodeName": node_name,
            "hostPID": true,
            "hostNetwork": true,
            "containers": [{
                "name": cname,
                "image": image,
                "stdin": true,
                "tty": true,
                "securityContext": {
                    "privileged": true,
                },
                "volumeMounts": [{
                    "name": "host-root",
                    "mountPath": "/host",
                }],
            }],
            "volumes": [{
                "name": "host-root",
                "hostPath": {
                    "path": "/",
                },
            }],
            "tolerations": [{
                "operator": "Exists",
            }],
            "restartPolicy": "Never",
        }
    });

    let pod_path = "/api/v1/namespaces/default/pods";
    let _result: Value = client
        .post(pod_path, &pod)
        .await
        .context("Failed to create node debug pod")?;

    println!(
        "Creating debugging pod {} with container {} on node {}.",
        debug_pod_name, cname, node_name
    );
    eprintln!(
        "If you don't see a command prompt, try pressing enter.\n\
         Use 'kubectl attach {} -c {} -i -t' to connect.",
        debug_pod_name, cname
    );

    Ok(())
}

pub fn generate_debug_container_name(pod: &Value) -> String {
    let existing: Vec<String> = pod
        .get("spec")
        .and_then(|s| s.get("ephemeralContainers"))
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    c.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .unwrap_or_default();

    for i in 0u32.. {
        let name = if i == 0 {
            "debugger".to_string()
        } else {
            format!("debugger-{}", i)
        };
        if !existing.contains(&name) {
            return name;
        }
    }
    "debugger".to_string()
}

pub fn generate_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{}", ts % 100000)
}
