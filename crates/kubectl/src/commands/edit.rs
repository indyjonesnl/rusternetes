use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use std::env;
use std::fs;
use std::process::Command;

/// Edit a resource in an editor
pub async fn execute(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    output_format: &str,
) -> Result<()> {
    // Determine editor
    let editor = env::var("EDITOR")
        .or_else(|_| env::var("VISUAL"))
        .unwrap_or_else(|_| {
            if cfg!(target_os = "windows") {
                "notepad".to_string()
            } else {
                "vi".to_string()
            }
        });

    // Get API path for resource
    let api_path = get_resource_api_path(resource_type, namespace, name)?;

    // Fetch current resource
    let resource: serde_json::Value = client
        .get(&api_path)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get {} {}: {}", resource_type, name, e))?;

    // Convert to desired format
    let content = if output_format == "json" {
        serde_json::to_string_pretty(&resource)?
    } else {
        serde_yaml::to_string(&resource)?
    };

    // Create temporary file
    let temp_file = tempfile::Builder::new()
        .suffix(if output_format == "json" {
            ".json"
        } else {
            ".yaml"
        })
        .tempfile()
        .context("Failed to create temporary file")?;

    let temp_path = temp_file.path().to_path_buf();

    // Write content to temp file
    fs::write(&temp_path, &content).context("Failed to write to temporary file")?;

    // Open editor
    let status = Command::new(&editor)
        .arg(&temp_path)
        .status()
        .context(format!("Failed to launch editor: {}", editor))?;

    if !status.success() {
        anyhow::bail!("Editor exited with non-zero status");
    }

    // Read edited content
    let edited_content = fs::read_to_string(&temp_path).context("Failed to read edited file")?;

    // Check if content changed
    if edited_content.trim() == content.trim() {
        println!("Edit cancelled, no changes made.");
        return Ok(());
    }

    // Parse edited content
    let edited_resource: serde_json::Value = if output_format == "json" {
        serde_json::from_str(&edited_content).context("Failed to parse edited JSON")?
    } else {
        serde_yaml::from_str(&edited_content).context("Failed to parse edited YAML")?
    };

    // Update resource
    let _updated: serde_json::Value = client
        .put(&api_path, &edited_resource)
        .await
        .context("Failed to update resource")?;

    println!("{}/{} edited", resource_type, name);

    Ok(())
}

pub fn get_resource_api_path(resource_type: &str, namespace: &str, name: &str) -> Result<String> {
    Ok(match resource_type {
        "pod" | "pods" | "po" => format!("/api/v1/namespaces/{}/pods/{}", namespace, name),
        "service" | "services" | "svc" => {
            format!("/api/v1/namespaces/{}/services/{}", namespace, name)
        }
        "deployment" | "deployments" | "deploy" => format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}",
            namespace, name
        ),
        "statefulset" | "statefulsets" | "sts" => format!(
            "/apis/apps/v1/namespaces/{}/statefulsets/{}",
            namespace, name
        ),
        "daemonset" | "daemonsets" | "ds" => {
            format!("/apis/apps/v1/namespaces/{}/daemonsets/{}", namespace, name)
        }
        "replicaset" | "replicasets" | "rs" => format!(
            "/apis/apps/v1/namespaces/{}/replicasets/{}",
            namespace, name
        ),
        "job" | "jobs" => format!("/apis/batch/v1/namespaces/{}/jobs/{}", namespace, name),
        "cronjob" | "cronjobs" | "cj" => {
            format!("/apis/batch/v1/namespaces/{}/cronjobs/{}", namespace, name)
        }
        "configmap" | "configmaps" | "cm" => {
            format!("/api/v1/namespaces/{}/configmaps/{}", namespace, name)
        }
        "secret" | "secrets" => format!("/api/v1/namespaces/{}/secrets/{}", namespace, name),
        "serviceaccount" | "serviceaccounts" | "sa" => {
            format!("/api/v1/namespaces/{}/serviceaccounts/{}", namespace, name)
        }
        "ingress" | "ingresses" | "ing" => format!(
            "/apis/networking.k8s.io/v1/namespaces/{}/ingresses/{}",
            namespace, name
        ),
        "persistentvolumeclaim" | "persistentvolumeclaims" | "pvc" => format!(
            "/api/v1/namespaces/{}/persistentvolumeclaims/{}",
            namespace, name
        ),
        "persistentvolume" | "persistentvolumes" | "pv" => {
            format!("/api/v1/persistentvolumes/{}", name)
        }
        "storageclass" | "storageclasses" | "sc" => {
            format!("/apis/storage.k8s.io/v1/storageclasses/{}", name)
        }
        "namespace" | "namespaces" | "ns" => format!("/api/v1/namespaces/{}", name),
        "node" | "nodes" | "no" => format!("/api/v1/nodes/{}", name),
        "role" | "roles" => format!(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/roles/{}",
            namespace, name
        ),
        "rolebinding" | "rolebindings" => format!(
            "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/rolebindings/{}",
            namespace, name
        ),
        "clusterrole" | "clusterroles" => {
            format!("/apis/rbac.authorization.k8s.io/v1/clusterroles/{}", name)
        }
        "clusterrolebinding" | "clusterrolebindings" => format!(
            "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
            name
        ),
        _ => anyhow::bail!("Unsupported resource type: {}", resource_type),
    })
}
