use anyhow::{Context, Result};
use rusternetes_client::http::{ApiClient, GetError};
use serde_json::Value;
use std::time::{Duration, Instant};

/// Wait for a specific condition on resources
pub async fn execute(
    client: &ApiClient,
    resources: &[String],
    for_condition: Option<&str>,
    for_delete: bool,
    namespace: &str,
    timeout: &str,
    selector: Option<&str>,
) -> Result<()> {
    // Parse timeout (e.g., "30s", "5m")
    let timeout_duration = parse_duration(timeout)?;
    let start = Instant::now();

    for resource in resources {
        // Parse resource type and name
        let (resource_type, name) = if let Some((rt, n)) = resource.split_once('/') {
            (rt, Some(n))
        } else {
            (resource.as_str(), None)
        };

        if for_delete {
            // Wait for deletion
            wait_for_deletion(
                client,
                resource_type,
                name,
                namespace,
                selector,
                start,
                timeout_duration,
            )
            .await?;
        } else if let Some(condition) = for_condition {
            // Wait for specific condition
            wait_for_condition(
                client,
                resource_type,
                name,
                namespace,
                selector,
                condition,
                start,
                timeout_duration,
            )
            .await?;
        } else {
            anyhow::bail!("Must specify --for=condition or --for=delete");
        }
    }

    Ok(())
}

async fn wait_for_deletion(
    client: &ApiClient,
    resource_type: &str,
    name: Option<&str>,
    namespace: &str,
    _selector: Option<&str>,
    start: Instant,
    timeout: Duration,
) -> Result<()> {
    let (api_path, resource_name) = parse_resource_type(resource_type)?;

    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for deletion");
        }

        let path = if let Some(n) = name {
            format!(
                "/{}/namespaces/{}/{}/{}",
                api_path, namespace, resource_name, n
            )
        } else {
            format!("/{}/namespaces/{}/{}", api_path, namespace, resource_name)
        };

        match client.get::<Value>(&path).await {
            Err(GetError::NotFound) => {
                println!("{}/{} deleted", resource_type, name.unwrap_or("*"));
                return Ok(());
            }
            Err(GetError::Other(e)) => return Err(e),
            Ok(_) => {
                // Still exists, wait and retry
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_condition(
    client: &ApiClient,
    resource_type: &str,
    name: Option<&str>,
    namespace: &str,
    _selector: Option<&str>,
    condition: &str,
    start: Instant,
    timeout: Duration,
) -> Result<()> {
    let (api_path, resource_name) = parse_resource_type(resource_type)?;

    // Parse condition (e.g., "Ready", "condition=Ready", "condition=Ready=true")
    let (condition_type, expected_status) = parse_condition(condition);

    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for condition {}", condition);
        }

        let path = if let Some(n) = name {
            format!(
                "/{}/namespaces/{}/{}/{}",
                api_path, namespace, resource_name, n
            )
        } else {
            format!("/{}/namespaces/{}/{}", api_path, namespace, resource_name)
        };

        let resource: Value = client.get(&path).await.map_err(|e| match e {
            GetError::NotFound => anyhow::anyhow!("Resource not found"),
            GetError::Other(e) => e,
        })?;

        // Check if condition is met
        if check_condition(&resource, condition_type, expected_status)? {
            println!(
                "{}/{} condition met: {}",
                resource_type,
                name.unwrap_or("*"),
                condition
            );
            return Ok(());
        }

        // Wait and retry
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub fn parse_resource_type(resource_type: &str) -> Result<(&str, &str)> {
    Ok(match resource_type {
        "pod" | "pods" => ("api/v1", "pods"),
        "service" | "services" | "svc" => ("api/v1", "services"),
        "deployment" | "deployments" | "deploy" => ("apis/apps/v1", "deployments"),
        "daemonset" | "daemonsets" | "ds" => ("apis/apps/v1", "daemonsets"),
        "statefulset" | "statefulsets" | "sts" => ("apis/apps/v1", "statefulsets"),
        "replicaset" | "replicasets" | "rs" => ("apis/apps/v1", "replicasets"),
        "job" | "jobs" => ("apis/batch/v1", "jobs"),
        _ => anyhow::bail!("Unsupported resource type: {}", resource_type),
    })
}

pub fn parse_condition(condition: &str) -> (&str, &str) {
    // Parse "condition=Type=Status" or "condition=Type" or just "Type"
    if let Some(rest) = condition.strip_prefix("condition=") {
        if let Some((typ, status)) = rest.split_once('=') {
            (typ, status)
        } else {
            (rest, "True")
        }
    } else {
        (condition, "True")
    }
}

pub fn check_condition(
    resource: &Value,
    condition_type: &str,
    expected_status: &str,
) -> Result<bool> {
    // Check status.conditions array
    if let Some(conditions) = resource
        .get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
    {
        for cond in conditions {
            if let Some(typ) = cond.get("type").and_then(|t| t.as_str()) {
                if typ == condition_type {
                    if let Some(status) = cond.get("status").and_then(|s| s.as_str()) {
                        return Ok(status == expected_status);
                    }
                }
            }
        }
    }
    Ok(false)
}

pub fn parse_duration(duration: &str) -> Result<Duration> {
    let duration = duration.trim();
    if duration.is_empty() {
        return Ok(Duration::from_secs(30)); // default
    }

    // Parse "30s", "5m", "1h", etc.
    if let Some(s) = duration.strip_suffix('s') {
        let secs: u64 = s.parse().context("Invalid seconds")?;
        Ok(Duration::from_secs(secs))
    } else if let Some(m) = duration.strip_suffix('m') {
        let mins: u64 = m.parse().context("Invalid minutes")?;
        Ok(Duration::from_secs(mins * 60))
    } else if let Some(h) = duration.strip_suffix('h') {
        let hours: u64 = h.parse().context("Invalid hours")?;
        Ok(Duration::from_secs(hours * 3600))
    } else {
        // Try parsing as raw seconds
        let secs: u64 = duration.parse().context("Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    }
}
