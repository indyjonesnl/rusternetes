use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;

/// Cascade strategy for delete operations, matching Kubernetes propagation policies.
#[derive(Debug, Clone, PartialEq)]
pub enum CascadeStrategy {
    /// Delete dependents in the background (default).
    Background,
    /// Block until all dependents are deleted.
    Foreground,
    /// Do not delete dependents.
    Orphan,
}

impl CascadeStrategy {
    /// Parse from CLI string value.
    pub fn from_str_value(s: &str) -> Result<Self> {
        match s {
            "background" => Ok(CascadeStrategy::Background),
            "foreground" => Ok(CascadeStrategy::Foreground),
            "orphan" => Ok(CascadeStrategy::Orphan),
            _ => anyhow::bail!(
                "Invalid cascade value '{}'. Must be one of: background, foreground, orphan",
                s
            ),
        }
    }

    /// Return the Kubernetes propagation policy string for the API.
    pub fn propagation_policy(&self) -> &str {
        match self {
            CascadeStrategy::Background => "Background",
            CascadeStrategy::Foreground => "Foreground",
            CascadeStrategy::Orphan => "Orphan",
        }
    }
}

/// Options controlling delete behavior, passed through to the API server.
#[derive(Debug, Clone)]
pub struct DeleteOptions {
    /// Grace period in seconds. None means use the resource default.
    pub grace_period: Option<i64>,
    /// Force deletion (sets grace_period=0, cascade=Background).
    pub force: bool,
    /// Cascade strategy (foreground, background, orphan).
    pub cascade: CascadeStrategy,
    /// Server-side dry run — no changes are persisted.
    pub dry_run: bool,
    /// Wait for resources to be fully deleted before returning.
    pub wait: bool,
    /// Output format. When "name", prints `resource/name` only.
    pub output: Option<String>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            grace_period: None,
            force: false,
            cascade: CascadeStrategy::Background,
            dry_run: false,
            wait: false,
            output: None,
        }
    }
}

impl DeleteOptions {
    /// Resolve force flag: when --force, grace_period becomes 0 and cascade becomes Background.
    pub fn resolve(&mut self) {
        if self.force {
            self.grace_period = Some(0);
            self.cascade = CascadeStrategy::Background;
        }
    }

    /// Build query parameters for the DELETE request.
    pub fn query_params(&self) -> Vec<(String, String)> {
        let mut params = Vec::new();

        if let Some(gp) = self.grace_period {
            params.push(("gracePeriodSeconds".to_string(), gp.to_string()));
        }

        if self.dry_run {
            params.push(("dryRun".to_string(), "All".to_string()));
        }

        params
    }

    /// Build the JSON body containing propagationPolicy for the DELETE request.
    pub fn delete_body(&self) -> Option<serde_json::Value> {
        let policy = self.cascade.propagation_policy();
        let mut body = serde_json::json!({
            "kind": "DeleteOptions",
            "apiVersion": "v1",
            "propagationPolicy": policy,
        });

        if let Some(gp) = self.grace_period {
            body["gracePeriodSeconds"] = serde_json::json!(gp);
        }

        Some(body)
    }

    /// Format the output message for a deleted resource.
    pub fn format_output(&self, resource_type: &str, name: &str) -> String {
        if self.output.as_deref() == Some("name") {
            format!("{}/{}", resource_type_to_kind(resource_type), name)
        } else {
            let operation = if self.force {
                "force deleted"
            } else {
                "deleted"
            };
            let dry_run_suffix = if self.dry_run {
                " (server dry run)"
            } else {
                ""
            };
            format!(
                "{} \"{}\" {}{}",
                resource_type_to_kind(resource_type),
                name,
                operation,
                dry_run_suffix,
            )
        }
    }
}

/// Map CLI resource type aliases to canonical kind strings for output.
pub fn resource_type_to_kind(resource_type: &str) -> &str {
    match resource_type {
        "pod" | "pods" => "pod",
        "service" | "services" | "svc" => "service",
        "deployment" | "deployments" | "deploy" => "deployment.apps",
        "statefulset" | "statefulsets" | "sts" => "statefulset.apps",
        "daemonset" | "daemonsets" | "ds" => "daemonset.apps",
        "replicaset" | "replicasets" | "rs" => "replicaset.apps",
        "job" | "jobs" => "job.batch",
        "cronjob" | "cronjobs" | "cj" => "cronjob.batch",
        "configmap" | "configmaps" | "cm" => "configmap",
        "secret" | "secrets" => "secret",
        "serviceaccount" | "serviceaccounts" | "sa" => "serviceaccount",
        "ingress" | "ingresses" | "ing" => "ingress.networking.k8s.io",
        "persistentvolumeclaim" | "persistentvolumeclaims" | "pvc" => "persistentvolumeclaim",
        "persistentvolume" | "persistentvolumes" | "pv" => "persistentvolume",
        "storageclass" | "storageclasses" | "sc" => "storageclass.storage.k8s.io",
        "namespace" | "namespaces" | "ns" => "namespace",
        "node" | "nodes" => "node",
        "role" | "roles" => "role.rbac.authorization.k8s.io",
        "rolebinding" | "rolebindings" => "rolebinding.rbac.authorization.k8s.io",
        "clusterrole" | "clusterroles" => "clusterrole.rbac.authorization.k8s.io",
        "clusterrolebinding" | "clusterrolebindings" => {
            "clusterrolebinding.rbac.authorization.k8s.io"
        }
        other => other,
    }
}

pub async fn execute_from_file(client: &ApiClient, file: &str, opts: &DeleteOptions) -> Result<()> {
    // Read + split the source into its YAML documents. Empty documents (e.g. a
    // trailing `---`) are skipped by the shared helper.
    let documents = crate::manifest::read_documents(file)?;

    let mut deleted_count = 0;
    for value in &documents {
        delete_resource(client, value, opts).await?;
        deleted_count += 1;
    }

    println!("Deleted {} resource(s) from file", deleted_count);
    Ok(())
}

pub async fn delete_resource(
    client: &ApiClient,
    value: &serde_yaml::Value,
    opts: &DeleteOptions,
) -> Result<()> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?;

    let metadata = value.get("metadata").context("Missing 'metadata' field")?;
    let name = metadata
        .get("name")
        .and_then(|n| n.as_str())
        .context("Missing 'name' in metadata")?;

    let namespace = metadata.get("namespace").and_then(|n| n.as_str());

    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(kind).ok_or_else(|| {
        anyhow::anyhow!(
            "error: the server doesn't have a resource type \"{}\"",
            kind
        )
    })?;

    // Reuse the shared opts-aware delete core so grace-period/force/cascade/
    // dry-run/wait all apply, exactly like the named-resource path.
    delete_single_resource_with_mapping(client, mapping, name, namespace, opts).await
}

pub async fn execute_with_selector(
    client: &ApiClient,
    resource_type: &str,
    selector: &str,
    namespace: &str,
    opts: &DeleteOptions,
) -> Result<()> {
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(resource_type).ok_or_else(|| {
        anyhow::anyhow!(
            "error: the server doesn't have a resource type \"{}\"",
            resource_type
        )
    })?;

    let selector_query = format!("?labelSelector={}", urlencoding::encode(selector));
    let items =
        crate::ops::list_value(client, mapping, Some(namespace), false, &selector_query).await?;

    if items.is_empty() {
        println!("No resources found matching selector: {}", selector);
        return Ok(());
    }

    // Delete each resource
    for item in &items {
        let name = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .context("Missing resource name")?;

        delete_single_resource_with_mapping(client, mapping, name, Some(namespace), opts).await?;
    }

    Ok(())
}

/// Execute --all: delete all resources of the given type in the namespace.
pub async fn execute_delete_all(
    client: &ApiClient,
    resource_type: &str,
    namespace: &str,
    opts: &DeleteOptions,
) -> Result<()> {
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(resource_type).ok_or_else(|| {
        anyhow::anyhow!(
            "error: the server doesn't have a resource type \"{}\"",
            resource_type
        )
    })?;

    // List all resources first so we can print their names
    let items = crate::ops::list_value(client, mapping, Some(namespace), false, "").await?;

    if items.is_empty() {
        println!("No resources found");
        return Ok(());
    }

    // Delete each individually (to get per-resource output and wait behavior)
    for item in &items {
        let name = item
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .context("Missing resource name")?;

        delete_single_resource_with_mapping(client, mapping, name, Some(namespace), opts).await?;
    }

    Ok(())
}

pub mod urlencoding {
    pub fn encode(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' | '=' | ',' | '!' => {
                    c.to_string()
                }
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}

/// Build a query string from DeleteOptions query params (e.g. `?gracePeriodSeconds=0&dryRun=All`).
fn build_query_string(opts: &DeleteOptions) -> String {
    let params = opts.query_params();
    if params.is_empty() {
        String::new()
    } else {
        let qs: Vec<String> = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        format!("?{}", qs.join("&"))
    }
}

/// Core single-resource delete with full options support (RestMapper-based).
async fn delete_single_resource_with_mapping(
    client: &ApiClient,
    mapping: &crate::discovery::ResourceMapping,
    name: &str,
    namespace: Option<&str>,
    opts: &DeleteOptions,
) -> Result<()> {
    let query = build_query_string(opts);
    let body = opts.delete_body();

    let status = crate::ops::delete_value(client, mapping, namespace, name, &query, body.as_ref())
        .await
        .with_context(|| format!("Failed to delete {} {}", mapping.singular, name))?;

    if status == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("{} \"{}\" not found", mapping.singular, name);
    }

    // Use mapping.singular as the resource_type for format_output so the
    // display is consistent with the RestMapper-resolved kind.
    println!("{}", opts.format_output(&mapping.singular, name));

    // --wait: poll until the resource is gone
    if opts.wait && !opts.dry_run {
        let ns = if mapping.namespaced {
            Some(namespace.unwrap_or("default").to_string())
        } else {
            None
        };
        let api_path = crate::ops::build_path(mapping, ns.as_deref(), Some(name));
        wait_for_deletion(client, &api_path).await?;
    }

    Ok(())
}

/// Core single-resource delete with full options support (legacy CLI-alias-based).
/// Used by `execute_enhanced` and `execute` which receive a string resource type alias.
pub async fn delete_single_resource(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: Option<&str>,
    opts: &DeleteOptions,
) -> Result<()> {
    let mapper = crate::discovery::RestMapper::from_server(client).await?;
    let mapping = mapper.resolve(resource_type).ok_or_else(|| {
        anyhow::anyhow!(
            "error: the server doesn't have a resource type \"{}\"",
            resource_type
        )
    })?;
    delete_single_resource_with_mapping(client, mapping, name, namespace, opts).await
}

/// Poll until the resource returns 404 (deleted).
async fn wait_for_deletion(client: &ApiClient, api_path: &str) -> Result<()> {
    let timeout = std::time::Duration::from_secs(60);
    let poll_interval = std::time::Duration::from_millis(500);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            anyhow::bail!("Timed out waiting for resource to be deleted");
        }
        let exists = client
            .resource_exists(api_path)
            .await
            .context("Failed polling resource for deletion")?;
        if !exists {
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn execute_enhanced(
    client: &ApiClient,
    resource_type: &str,
    name: &str,
    namespace: &str,
    opts: &DeleteOptions,
) -> Result<()> {
    delete_single_resource(client, resource_type, name, Some(namespace), opts).await
}
