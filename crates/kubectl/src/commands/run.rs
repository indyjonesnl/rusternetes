use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::json;
use std::collections::HashMap;

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    name: &str,
    namespace: &str,
    image: &str,
    port: Option<u16>,
    env_vars: &[String],
    labels: Option<&str>,
    restart_policy: &str,
    command: &[String],
    args: &[String],
    dry_run: Option<&str>,
) -> Result<()> {
    // Build labels map
    let mut label_map: HashMap<String, String> = HashMap::new();
    label_map.insert("run".to_string(), name.to_string());

    if let Some(label_str) = labels {
        for pair in label_str.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((k, v)) = pair.split_once('=') {
                label_map.insert(k.to_string(), v.to_string());
            } else {
                anyhow::bail!("Invalid label format: '{}'. Expected key=value", pair);
            }
        }
    }

    // Build container
    let mut container = json!({
        "name": name,
        "image": image,
    });

    // Add port if specified
    if let Some(p) = port {
        container["ports"] = json!([{"containerPort": p}]);
    }

    // Add environment variables
    if !env_vars.is_empty() {
        let mut env_list = Vec::new();
        for env in env_vars {
            if let Some((k, v)) = env.split_once('=') {
                env_list.push(json!({"name": k, "value": v}));
            } else {
                anyhow::bail!(
                    "Invalid environment variable format: '{}'. Expected KEY=VALUE",
                    env
                );
            }
        }
        container["env"] = json!(env_list);
    }

    // Add command if specified
    if !command.is_empty() {
        container["command"] = json!(command);
    }

    // Add args if specified
    if !args.is_empty() {
        container["args"] = json!(args);
    }

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": label_map,
        },
        "spec": {
            "containers": [container],
            "restartPolicy": restart_policy,
        }
    });

    // Handle dry-run
    if let Some(dr) = dry_run {
        if dr == "client" {
            // Just print the resource
            let yaml = serde_yaml::to_string(&pod)?;
            print!("{}", yaml);
            return Ok(());
        }
    }

    let path = format!("/api/v1/namespaces/{}/pods", namespace);
    let query = if let Some(dr) = dry_run {
        if dr == "server" {
            format!("{}?dryRun=All", path)
        } else {
            path
        }
    } else {
        path
    };

    let _result: serde_json::Value = client
        .post(&query, &pod)
        .await
        .context("Failed to create pod")?;

    println!("pod/{} created", name);
    Ok(())
}
