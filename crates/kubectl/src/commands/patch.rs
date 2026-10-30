use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::Value;
use std::fs;

/// Patch a resource
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    patch: Option<&str>,
    patch_file: Option<&str>,
    patch_type: &str,
) -> Result<()> {
    let patch_content = if let Some(p) = patch {
        p.to_string()
    } else if let Some(file) = patch_file {
        fs::read_to_string(file).context("Failed to read patch file")?
    } else {
        anyhow::bail!("Either --patch or --patch-file must be provided");
    };

    // Parse the patch to validate JSON
    let patch_value: Value =
        serde_json::from_str(&patch_content).context("Failed to parse patch as JSON")?;

    // Determine content type based on patch type
    let content_type = match patch_type {
        "strategic" => "application/strategic-merge-patch+json",
        "merge" => "application/merge-patch+json",
        "json" => "application/json-patch+json",
        _ => anyhow::bail!(
            "Invalid patch type: {}. Must be one of: strategic, merge, json",
            patch_type
        ),
    };

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

    // Send PATCH request
    let result: Value = client
        .patch(&path, &patch_value, content_type)
        .await
        .context("Failed to patch resource")?;

    // Pretty print the result
    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
