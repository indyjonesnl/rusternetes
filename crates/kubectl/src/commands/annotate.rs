use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};
use std::collections::HashMap;

/// Update annotations on a resource
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    annotations: &[String],
    _overwrite: bool,
) -> Result<()> {
    // Parse annotations from key=value or key- (removal) format
    let mut annotation_map = HashMap::new();
    for annotation in annotations {
        if let Some(key) = annotation.strip_suffix('-') {
            // Annotation removal - set to null
            annotation_map.insert(key.to_string(), Value::Null);
        } else if let Some((key, value)) = annotation.split_once('=') {
            annotation_map.insert(key.to_string(), Value::String(value.to_string()));
        } else {
            anyhow::bail!(
                "Invalid annotation format: {}. Expected key=value or key-",
                annotation
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
            "annotations": annotation_map
        }
    });

    // Use merge patch for annotations
    let result: Value = client
        .patch(&path, &patch_body, "application/merge-patch+json")
        .await
        .context("Failed to update annotations")?;

    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
