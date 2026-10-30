use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Scale a resource (Deployment, ReplicaSet, StatefulSet, etc.)
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    replicas: i32,
) -> Result<()> {
    let path = match resource_type {
        "deployment" | "deployments" | "deploy" => {
            format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}/scale",
                namespace, name
            )
        }
        "replicaset" | "replicasets" | "rs" => {
            format!(
                "/apis/apps/v1/namespaces/{}/replicasets/{}/scale",
                namespace, name
            )
        }
        "statefulset" | "statefulsets" | "sts" => {
            format!(
                "/apis/apps/v1/namespaces/{}/statefulsets/{}/scale",
                namespace, name
            )
        }
        "replicationcontroller" | "rc" => {
            format!(
                "/api/v1/namespaces/{}/replicationcontrollers/{}/scale",
                namespace, name
            )
        }
        _ => anyhow::bail!("Resource type {} does not support scaling", resource_type),
    };

    let scale_body = json!({
        "spec": {
            "replicas": replicas
        }
    });

    // Use strategic merge patch for the scale subresource
    let result: Value = client
        .patch(&path, &scale_body, "application/merge-patch+json")
        .await
        .context("Failed to scale resource")?;

    println!("{}", serde_json::to_string_pretty(&result)?);

    Ok(())
}
