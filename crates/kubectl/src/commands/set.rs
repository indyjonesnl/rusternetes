use crate::discovery::ResourceMapping;
use crate::types::SetCommands;
use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};

/// Dispatch set subcommands.
pub async fn execute_set(
    client: &ApiClient,
    command: SetCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        SetCommands::Image {
            resource,
            container_images,
            namespace,
        } => {
            execute_image(
                client,
                &resource,
                &container_images,
                namespace.as_deref().unwrap_or(default_namespace),
            )
            .await
        }
        SetCommands::Env {
            resource,
            env_vars,
            namespace,
            container,
            list,
        } => {
            execute_env(
                client,
                &resource,
                &env_vars,
                namespace.as_deref().unwrap_or(default_namespace),
                container.as_deref(),
                list,
            )
            .await
        }
        SetCommands::Resources {
            resource,
            namespace,
            container,
            limits,
            requests,
        } => {
            execute_resources(
                client,
                &resource,
                namespace.as_deref().unwrap_or(default_namespace),
                container.as_deref(),
                limits.as_deref(),
                requests.as_deref(),
            )
            .await
        }
        SetCommands::Selector {
            resource,
            expressions,
            namespace,
            all: _,
            resource_version: _,
        } => {
            execute_selector(
                client,
                &resource,
                &expressions,
                namespace.as_deref().unwrap_or(default_namespace),
            )
            .await
        }
        SetCommands::ServiceAccount {
            resource,
            service_account_name,
            namespace,
        } => {
            execute_serviceaccount(
                client,
                &resource,
                &service_account_name,
                namespace.as_deref().unwrap_or(default_namespace),
            )
            .await
        }
        SetCommands::Subject {
            resource,
            namespace,
            serviceaccount,
            user,
            group,
        } => {
            execute_subject(
                client,
                &resource,
                namespace.as_deref().unwrap_or(default_namespace),
                serviceaccount.as_deref(),
                user.as_deref(),
                group.as_deref(),
            )
            .await
        }
    }
}

/// Resolve the API path and mapping for a resource type/name via server
/// discovery.
///
/// Uses `RestMapper` so any kind the server exposes (plural, singular, short
/// name, or Kind) is addressable, rather than a hardcoded alias table. Returns
/// the item path plus the resolved `ResourceMapping`, so callers can build a
/// group-qualified label (via `ops::resource_label`) and apply kind guards
/// without a second `RestMapper::from_server` round-trip. This is the single
/// discovery call site shared by every `set` subcommand.
async fn resolve_resource(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<(String, ResourceMapping)> {
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(resource_type).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{resource_type}\"")
    })?;
    let ns = if mapping.namespaced {
        Some(namespace)
    } else {
        None
    };
    let path = crate::ops::build_path(mapping, ns, Some(name));
    Ok((path, mapping.clone()))
}

/// Parse "resource_type/name" format into (type, name).
pub fn parse_resource_arg(arg: &str) -> Result<(String, String)> {
    if let Some((rt, name)) = arg.split_once('/') {
        Ok((rt.to_string(), name.to_string()))
    } else {
        anyhow::bail!(
            "Invalid resource format: '{}'. Expected TYPE/NAME (e.g., deployment/nginx)",
            arg
        );
    }
}

/// Update container images in a resource.
///
/// Usage: `kubectl set image deployment/nginx nginx=nginx:1.9.1`
pub async fn execute_image(
    client: &ApiClient,
    resource: &str,
    container_images: &[String],
    namespace: &str,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Parse container=image pairs
    let mut image_updates: Vec<(String, String)> = Vec::new();
    for ci in container_images {
        if let Some((container, image)) = ci.split_once('=') {
            image_updates.push((container.to_string(), image.to_string()));
        } else {
            anyhow::bail!("Invalid image format: '{}'. Expected CONTAINER=IMAGE", ci);
        }
    }

    if image_updates.is_empty() {
        anyhow::bail!("At least one CONTAINER=IMAGE argument is required");
    }

    // Get the current resource to find container indices
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    // Find the containers array path (handles both pod and pod template specs)
    let containers = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        // Deployment/StatefulSet/DaemonSet/ReplicaSet/Job/CronJob
        current["spec"]["template"]["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    } else {
        // Pod
        current["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    // Build the strategic merge patch for containers
    let mut patch_containers: Vec<Value> = Vec::new();
    for (container_name, image) in &image_updates {
        if container_name == "*" {
            // Update all containers
            for c in &containers {
                if let Some(cname) = c.get("name").and_then(|n| n.as_str()) {
                    patch_containers.push(json!({
                        "name": cname,
                        "image": image,
                    }));
                }
            }
        } else {
            // Check container exists
            let found = containers
                .iter()
                .any(|c| c.get("name").and_then(|n| n.as_str()) == Some(container_name));
            if !found {
                anyhow::bail!(
                    "Container '{}' not found in resource {}/{}",
                    container_name,
                    resource_type,
                    name
                );
            }
            patch_containers.push(json!({
                "name": container_name,
                "image": image,
            }));
        }
    }

    // Build patch based on resource type
    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": patch_containers,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "containers": patch_containers,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update image")?;

    let label = crate::ops::resource_label(&mapping, &name);
    for (container_name, image) in &image_updates {
        if container_name == "*" {
            println!("{label} image updated to {image} (all containers)");
        } else {
            println!("{label} container {container_name} image updated to {image}");
        }
    }

    Ok(())
}

/// Update environment variables on a resource.
///
/// Usage: `kubectl set env deployment/registry STORAGE_DIR=/local`
pub async fn execute_env(
    client: &ApiClient,
    resource: &str,
    env_vars: &[String],
    namespace: &str,
    container_name: Option<&str>,
    list: bool,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Get current resource
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let containers = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        current["spec"]["template"]["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    } else {
        current["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    // If --list, just print current env vars
    if list {
        for c in &containers {
            let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
            if let Some(target) = container_name {
                if cname != target {
                    continue;
                }
            }
            println!("# Container: {}", cname);
            if let Some(envs) = c.get("env").and_then(|e| e.as_array()) {
                for env in envs {
                    let key = env.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let val = env.get("value").and_then(|v| v.as_str()).unwrap_or("");
                    println!("{}={}", key, val);
                }
            }
        }
        return Ok(());
    }

    // Parse env vars: KEY=VALUE to add, KEY- to remove
    let mut to_add: Vec<(String, String)> = Vec::new();
    let mut to_remove: Vec<String> = Vec::new();

    for ev in env_vars {
        if let Some(key) = ev.strip_suffix('-') {
            to_remove.push(key.to_string());
        } else if let Some((key, value)) = ev.split_once('=') {
            to_add.push((key.to_string(), value.to_string()));
        } else {
            anyhow::bail!("Invalid env format: '{}'. Expected KEY=VALUE or KEY-", ev);
        }
    }

    // Build updated containers with env changes
    let mut updated_containers: Vec<Value> = Vec::new();

    for c in &containers {
        let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if let Some(target) = container_name {
            if cname != target {
                continue;
            }
        }

        let mut env_list: Vec<Value> = c
            .get("env")
            .and_then(|e| e.as_array())
            .cloned()
            .unwrap_or_default();

        // Remove specified vars
        env_list.retain(|e| {
            let key = e.get("name").and_then(|n| n.as_str()).unwrap_or("");
            !to_remove.contains(&key.to_string())
        });

        // Add/update vars
        for (key, value) in &to_add {
            if let Some(existing) = env_list
                .iter_mut()
                .find(|e| e.get("name").and_then(|n| n.as_str()) == Some(key))
            {
                existing["value"] = json!(value);
            } else {
                env_list.push(json!({"name": key, "value": value}));
            }
        }

        updated_containers.push(json!({
            "name": cname,
            "env": env_list,
        }));
    }

    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": updated_containers,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "containers": updated_containers,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update environment variables")?;

    println!(
        "{} env updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Update resource requests/limits on a resource.
///
/// Usage: `kubectl set resources deployment nginx --limits=cpu=200m,memory=512Mi`
pub async fn execute_resources(
    client: &ApiClient,
    resource: &str,
    namespace: &str,
    container_name: Option<&str>,
    limits: Option<&str>,
    requests: Option<&str>,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Get current resource
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let containers = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        current["spec"]["template"]["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    } else {
        current["spec"]["containers"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    };

    // Parse limits and requests
    let limits_map = parse_resource_spec(limits)?;
    let requests_map = parse_resource_spec(requests)?;

    // Build updated containers
    let mut updated_containers: Vec<Value> = Vec::new();

    for c in &containers {
        let cname = c.get("name").and_then(|n| n.as_str()).unwrap_or("");

        if let Some(target) = container_name {
            if cname != target {
                continue;
            }
        }

        let mut resources = json!({});

        if !limits_map.is_empty() {
            resources["limits"] = json!(limits_map);
        }
        if !requests_map.is_empty() {
            resources["requests"] = json!(requests_map);
        }

        updated_containers.push(json!({
            "name": cname,
            "resources": resources,
        }));
    }

    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": updated_containers,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "containers": updated_containers,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update resource requirements")?;

    println!(
        "{} resource requirements updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Set the selector on a resource (e.g., a Service).
///
/// Usage: `kubectl set selector service/myapp app=myapp`
pub async fn execute_selector(
    client: &ApiClient,
    resource: &str,
    expressions: &[String],
    namespace: &str,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;

    // Build selector map from key=value expressions
    let mut selector = serde_json::Map::new();
    for expr in expressions {
        if let Some((key, value)) = expr.split_once('=') {
            selector.insert(key.to_string(), json!(value));
        } else {
            anyhow::bail!(
                "Invalid selector expression: '{}'. Expected key=value",
                expr
            );
        }
    }

    if selector.is_empty() {
        anyhow::bail!("At least one selector expression (key=value) is required");
    }

    // Resolve the API path via server discovery (handles services and any
    // other selector-bearing kind).
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    let patch_body = json!({
        "spec": {
            "selector": selector,
        }
    });

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update selector")?;

    println!(
        "{} selector updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Set serviceAccountName on pod templates in a resource.
///
/// Usage: `kubectl set serviceaccount deployment/nginx my-service-account`
pub async fn execute_serviceaccount(
    client: &ApiClient,
    resource: &str,
    service_account_name: &str,
    namespace: &str,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;

    // Get current resource to determine if it has a template
    let current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let patch_body = if current
        .get("spec")
        .and_then(|s| s.get("template"))
        .is_some()
    {
        json!({
            "spec": {
                "template": {
                    "spec": {
                        "serviceAccountName": service_account_name,
                    }
                }
            }
        })
    } else {
        json!({
            "spec": {
                "serviceAccountName": service_account_name,
            }
        })
    };

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update serviceAccountName")?;

    println!(
        "{} serviceaccount updated to {}",
        crate::ops::resource_label(&mapping, &name),
        service_account_name
    );

    Ok(())
}

/// Update subjects in a RoleBinding or ClusterRoleBinding.
///
/// Usage: `kubectl set subject clusterrolebinding/admin --user=admin --group=devs`
pub async fn execute_subject(
    client: &ApiClient,
    resource: &str,
    namespace: &str,
    serviceaccount: Option<&str>,
    user: Option<&str>,
    group: Option<&str>,
) -> Result<()> {
    let (resource_type, name) = parse_resource_arg(resource)?;

    // set subject only operates on (Cluster)RoleBindings. Resolve the type via
    // the shared discovery resolver, then guard on the resolved Kind so the
    // path is built by the shared RestMapper rather than a hardcoded
    // group/version literal.
    let (path, mapping) = resolve_resource(client, &resource_type, &name, namespace).await?;
    // Guard runs after discovery (we need the resolved Kind); a wrong kind costs one discovery round-trip before rejection.
    if mapping.kind != "RoleBinding" && mapping.kind != "ClusterRoleBinding" {
        anyhow::bail!(
            "set subject only supports rolebinding and clusterrolebinding, got: {}",
            resource_type
        );
    }

    // Get current binding
    let mut current: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))
        .context("Failed to get resource")?;

    let subjects = current
        .get_mut("subjects")
        .and_then(|s| s.as_array_mut())
        .map(|a| a.clone())
        .unwrap_or_default();

    let mut new_subjects = subjects;

    if let Some(sa) = serviceaccount {
        let (sa_namespace, sa_name) = if let Some((ns, n)) = sa.split_once(':') {
            (ns.to_string(), n.to_string())
        } else {
            (namespace.to_string(), sa.to_string())
        };
        // Check if subject already exists
        let exists = new_subjects.iter().any(|s| {
            s.get("kind").and_then(|k| k.as_str()) == Some("ServiceAccount")
                && s.get("name").and_then(|n| n.as_str()) == Some(&sa_name)
                && s.get("namespace").and_then(|n| n.as_str()) == Some(&sa_namespace)
        });
        if !exists {
            new_subjects.push(json!({
                "kind": "ServiceAccount",
                "name": sa_name,
                "namespace": sa_namespace,
            }));
        }
    }

    if let Some(u) = user {
        let exists = new_subjects.iter().any(|s| {
            s.get("kind").and_then(|k| k.as_str()) == Some("User")
                && s.get("name").and_then(|n| n.as_str()) == Some(u)
        });
        if !exists {
            new_subjects.push(json!({
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "User",
                "name": u,
            }));
        }
    }

    if let Some(g) = group {
        let exists = new_subjects.iter().any(|s| {
            s.get("kind").and_then(|k| k.as_str()) == Some("Group")
                && s.get("name").and_then(|n| n.as_str()) == Some(g)
        });
        if !exists {
            new_subjects.push(json!({
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Group",
                "name": g,
            }));
        }
    }

    let patch_body = json!({
        "subjects": new_subjects,
    });

    let _result: Value = client
        .patch(&path, &patch_body, "application/strategic-merge-patch+json")
        .await
        .context("Failed to update subjects")?;

    println!(
        "{} subjects updated",
        crate::ops::resource_label(&mapping, &name)
    );

    Ok(())
}

/// Parse a resource spec string like "cpu=200m,memory=512Mi" into a map.
pub fn parse_resource_spec(
    spec: Option<&str>,
) -> Result<std::collections::HashMap<String, String>> {
    let mut map = std::collections::HashMap::new();
    if let Some(s) = spec {
        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((key, value)) = pair.split_once('=') {
                map.insert(key.to_string(), value.to_string());
            } else {
                anyhow::bail!("Invalid resource spec: '{}'. Expected key=value", pair);
            }
        }
    }
    Ok(map)
}
