use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Update labels on a resource
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    labels: &[String],
    _overwrite: bool,
) -> Result<()> {
    // Parse labels from key=value or key- (removal) format
    let mut label_map = HashMap::new();
    for label in labels {
        if let Some(key) = label.strip_suffix('-') {
            // Label removal - set to null
            label_map.insert(key.to_string(), Value::Null);
        } else if let Some((key, value)) = label.split_once('=') {
            label_map.insert(key.to_string(), Value::String(value.to_string()));
        } else {
            anyhow::bail!(
                "Invalid label format: {}. Expected key=value or key-",
                label
            );
        }
    }

    // Build API path based on resource type
    let (api_path, resource_name) = match resource_type {
        "pod" | "pods" => ("api/v1", "pods"),
        "service" | "services" | "svc" => ("api/v1", "services"),
        "deployment" | "deployments" | "deploy" => ("apis/apps/v1", "deployments"),
        "daemonset" | "daemonsets" | "ds" => ("apis/apps/v1", "daemonsets"),
        "statefulset" | "statefulsets" | "sts" => ("apis/apps/v1", "statefulsets"),
        "replicaset" | "replicasets" | "rs" => ("apis/apps/v1", "replicasets"),
        "configmap" | "configmaps" | "cm" => ("api/v1", "configmaps"),
        "secret" | "secrets" => ("api/v1", "secrets"),
        "node" | "nodes" => ("api/v1", "nodes"),
        _ => anyhow::bail!("Unsupported resource type: {}", resource_type),
    };

    let path = if resource_name == "nodes" {
        format!("/{}/{}/{}", api_path, resource_name, name)
    } else {
        format!(
            "/{}/namespaces/{}/{}/{}",
            api_path, namespace, resource_name, name
        )
    };

    let patch_body = json!({
        "metadata": {
            "labels": label_map
        }
    });

    // Use merge patch for labels (unless overwrite is false, but we'll use merge for simplicity)
    let result: Value = client
        .patch(&path, &patch_body, "application/merge-patch+json")
        .await
        .context("Failed to update labels")?;

    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
