use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Build the API path to GET a resource by type and name.
pub fn resource_path(resource_type: &str, namespace: &str, name: &str) -> Result<String> {
    match resource_type {
        "pod" | "pods" | "po" => Ok(format!(
            "/api/v1/namespaces/{}/pods/{}",
            namespace, name
        )),
        "service" | "services" | "svc" => Ok(format!(
            "/api/v1/namespaces/{}/services/{}",
            namespace, name
        )),
        "replicationcontroller" | "replicationcontrollers" | "rc" => Ok(format!(
            "/api/v1/namespaces/{}/replicationcontrollers/{}",
            namespace, name
        )),
        "deployment" | "deployments" | "deploy" => Ok(format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}",
            namespace, name
        )),
        "replicaset" | "replicasets" | "rs" => Ok(format!(
            "/apis/apps/v1/namespaces/{}/replicasets/{}",
            namespace, name
        )),
        _ => anyhow::bail!(
            "cannot expose a {}; supported resource types: pod, service, replicationcontroller, deployment, replicaset",
            resource_type
        ),
    }
}

/// Extract selector labels from a resource JSON object.
///
/// - For Pods: uses `.metadata.labels`
/// - For Services: uses `.spec.selector`
/// - For Deployments/ReplicaSets: uses `.spec.selector.matchLabels`
/// - For ReplicationControllers: uses `.spec.selector`
pub fn extract_selector(resource_type: &str, resource: &Value) -> Result<BTreeMap<String, String>> {
    let selector_value = match resource_type {
        "pod" | "pods" | "po" => resource
            .pointer("/metadata/labels")
            .ok_or_else(|| anyhow::anyhow!("pod has no labels to use as selector"))?,
        "service" | "services" | "svc" => resource
            .pointer("/spec/selector")
            .ok_or_else(|| anyhow::anyhow!("service has no selector"))?,
        "replicationcontroller" | "replicationcontrollers" | "rc" => resource
            .pointer("/spec/selector")
            .ok_or_else(|| anyhow::anyhow!("replication controller has no selector"))?,
        "deployment" | "deployments" | "deploy" | "replicaset" | "replicasets" | "rs" => resource
            .pointer("/spec/selector/matchLabels")
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "resource selector is not convertible to a map-based selector (matchLabels required)"
                )
            })?,
        _ => anyhow::bail!("unsupported resource type for expose: {}", resource_type),
    };

    let map = selector_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("selector is not a JSON object"))?;

    let mut selector = BTreeMap::new();
    for (k, v) in map {
        if let Some(s) = v.as_str() {
            selector.insert(k.clone(), s.to_string());
        }
    }

    if selector.is_empty() {
        anyhow::bail!("couldn't retrieve selectors from the resource");
    }

    Ok(selector)
}

/// Extract container ports from a resource to auto-detect --port when not specified.
pub fn extract_ports(resource: &Value) -> Vec<i32> {
    let containers = resource
        .pointer("/spec/containers")
        .or_else(|| resource.pointer("/spec/template/spec/containers"));

    let mut ports = Vec::new();
    if let Some(Value::Array(containers)) = containers {
        for container in containers {
            if let Some(Value::Array(cports)) = container.get("ports") {
                for p in cports {
                    if let Some(port) = p.get("containerPort").and_then(|v| v.as_i64()) {
                        ports.push(port as i32);
                    }
                }
            }
        }
    }
    ports
}

/// Build a Service JSON object from the given parameters.
pub fn build_service(
    name: &str,
    namespace: &str,
    selector: &BTreeMap<String, String>,
    port: i32,
    target_port: Option<i32>,
    protocol: &str,
    service_type: Option<&str>,
) -> Value {
    let tp = target_port.unwrap_or(port);

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": namespace
        },
        "spec": {
            "selector": selector,
            "ports": [
                {
                    "protocol": protocol,
                    "port": port,
                    "targetPort": tp
                }
            ]
        }
    });

    if let Some(svc_type) = service_type {
        service["spec"]["type"] = json!(svc_type);
    }

    service
}

/// Execute the `kubectl expose` command.
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    resource_name: &str,
    namespace: &str,
    port: Option<i32>,
    target_port: Option<i32>,
    protocol: &str,
    name: Option<&str>,
    service_type: Option<&str>,
) -> Result<()> {
    // 1. GET the resource from the API server
    let path = resource_path(resource_type, namespace, resource_name)?;
    let resource: Value = client.get(&path).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to get resource {}/{}: {}",
            resource_type,
            resource_name,
            e
        )
    })?;

    // 2. Extract selector labels
    let selector = extract_selector(resource_type, &resource)?;

    // 3. Determine the port
    let port = match port {
        Some(p) => p,
        None => {
            let ports = extract_ports(&resource);
            match ports.len() {
                0 => anyhow::bail!(
                    "couldn't find port via --port flag or introspection; use --port to specify"
                ),
                1 => ports[0],
                _ => anyhow::bail!(
                    "resource has multiple ports ({}); use --port to specify which one",
                    ports
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            }
        }
    };

    // 4. Service name defaults to the resource name
    let service_name = name.unwrap_or(resource_name);

    // 5. Build the Service
    let service = build_service(
        service_name,
        namespace,
        &selector,
        port,
        target_port,
        protocol,
        service_type,
    );

    // 6. POST the Service to the API server
    let svc_path = format!("/api/v1/namespaces/{}/services", namespace);
    let result: Value = client
        .post(&svc_path, &service)
        .await
        .context("Failed to create service")?;

    let created_name = result
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or(service_name);
    println!("service/{} exposed", created_name);

    Ok(())
}
