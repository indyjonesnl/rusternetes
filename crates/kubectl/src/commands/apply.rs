use crate::discovery::RestMapper;
use crate::types::ApplyCommands;
use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

/// Options controlling how `kubectl apply` behaves.
#[derive(Debug, Clone)]
pub struct ApplyOptions {
    /// One or more file paths or directories.
    pub files: Vec<String>,
    /// Optional namespace override.
    pub namespace: Option<String>,
    /// Dry-run mode string (e.g. "client", "server", "none").
    pub dry_run: Option<String>,
    /// Use server-side apply.
    pub server_side: bool,
    /// Force apply (resolve conflicts).
    pub force: bool,
    /// Recurse into directories.
    pub recursive: bool,
    /// Field manager name for server-side apply.
    pub field_manager: String,
    /// Output format: json, yaml, or name.
    pub output: Option<String>,
    /// Validation directive: true/strict, false/ignore, warn.
    pub validate: Option<String>,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            namespace: None,
            dry_run: None,
            server_side: false,
            force: false,
            recursive: false,
            field_manager: "kubectl-client-side-apply".to_string(),
            output: None,
            validate: None,
        }
    }
}

/// Main entry point that honours all ApplyOptions.
pub async fn execute_with_options(client: &ApiClient, options: &ApplyOptions) -> Result<()> {
    // Build the list of file paths to process.
    let file_paths = collect_files(&options.files, options.recursive)?;

    if file_paths.is_empty() {
        anyhow::bail!("no files to apply — specify at least one file with -f");
    }

    // Build query-parameter suffix.
    let query = build_query_string(options);

    // Build the REST mapper once, up front — discovery is a pair of HTTP
    // round-trips, so re-fetching it per document would be wasteful for
    // multi-document manifests.
    let mapper = RestMapper::from_server(client).await?;

    for path in &file_paths {
        // Read + split the single source into its YAML documents. Empty
        // documents (e.g. a trailing `---`) are skipped by the shared helper.
        for value in crate::manifest::read_documents(path)? {
            let result = apply_resource(client, &mapper, &value, &query, options).await?;

            // Format output based on --output flag
            format_output(&result, options);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// File collection
// ---------------------------------------------------------------------------

/// Collect file paths from the supplied list, expanding directories.
pub fn collect_files(inputs: &[String], recursive: bool) -> Result<Vec<String>> {
    let mut result = Vec::new();

    for input in inputs {
        if input == "-" {
            result.push("-".to_string());
            continue;
        }

        let path = Path::new(input);
        if path.is_dir() {
            if recursive {
                for entry in WalkDir::new(path).sort_by_file_name() {
                    let entry = entry.with_context(|| {
                        format!("Failed to read directory entry under {}", input)
                    })?;
                    if entry.file_type().is_file() && is_manifest_file(entry.path()) {
                        result.push(entry.path().to_string_lossy().to_string());
                    }
                }
            } else {
                // Non-recursive: only direct children.
                let mut entries: Vec<_> = fs::read_dir(path)
                    .with_context(|| format!("Failed to read directory: {}", input))?
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.file_type().map(|ft| ft.is_file()).unwrap_or(false)
                            && is_manifest_file(&e.path())
                    })
                    .collect();
                entries.sort_by_key(|e| e.file_name());
                for entry in entries {
                    result.push(entry.path().to_string_lossy().to_string());
                }
            }
        } else if path.is_file() || input == "-" {
            result.push(input.to_string());
        } else {
            anyhow::bail!("path does not exist: {}", input);
        }
    }

    Ok(result)
}

/// Return true if the file extension indicates a Kubernetes manifest.
pub fn is_manifest_file(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("yaml") | Some("yml") | Some("json")
    )
}

// ---------------------------------------------------------------------------
// Query string building
// ---------------------------------------------------------------------------

pub fn build_query_string(options: &ApplyOptions) -> String {
    let mut params: Vec<String> = Vec::new();

    if let Some(ref dr) = options.dry_run {
        if dr == "server" || dr == "client" || dr == "none" {
            // For server-side dry run we pass the query param.
            if dr == "server" {
                params.push("dryRun=All".to_string());
            }
        }
    }

    if options.server_side {
        params.push(format!("fieldManager={}", options.field_manager));
        if options.force {
            params.push("force=true".to_string());
        }
    }

    if let Some(ref v) = options.validate {
        let directive = match v.as_str() {
            "true" | "strict" | "Strict" => "Strict",
            "warn" | "Warn" => "Warn",
            "false" | "ignore" | "Ignore" => "Ignore",
            other => other,
        };
        params.push(format!("fieldValidation={}", directive));
    }

    if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

/// Information returned from a single apply operation for output formatting.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub kind: String,
    pub api_group: String,
    pub name: String,
    pub action: ApplyAction,
    pub response: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ApplyAction {
    Created,
    Configured,
}

impl ApplyResult {
    /// Return the resource label like "deployment.apps/nginx".
    pub fn resource_label(&self) -> String {
        let kind_lower = self.kind.to_lowercase();
        if self.api_group.is_empty() {
            format!("{}/{}", kind_lower, self.name)
        } else {
            format!("{}.{}/{}", kind_lower, self.api_group, self.name)
        }
    }
}

pub fn format_output(result: &ApplyResult, options: &ApplyOptions) {
    match options.output.as_deref() {
        Some("json") => {
            if let Ok(pretty) = serde_json::to_string_pretty(&result.response) {
                println!("{}", pretty);
            }
        }
        Some("yaml") => {
            if let Ok(yaml) = serde_yaml::to_string(&result.response) {
                print!("{}", yaml);
            }
        }
        Some("name") => {
            println!("{}", result.resource_label());
        }
        _ => {
            // Default: standard kubectl output
            let action = match result.action {
                ApplyAction::Created => "created",
                ApplyAction::Configured => "configured",
            };
            println!("{} {}", result.resource_label(), action);
        }
    }
}

// ---------------------------------------------------------------------------
// last-applied-configuration annotation
// ---------------------------------------------------------------------------

pub const LAST_APPLIED_ANNOTATION: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// Inject the `last-applied-configuration` annotation into a JSON Value
/// representing the resource being applied.  The annotation stores the
/// *original* input config (before the annotation itself was added) serialised
/// as compact JSON, matching the behaviour of real kubectl.
pub fn set_last_applied_annotation(value: &mut Value) {
    let clean = strip_last_applied(value);
    let annotation_value = serde_json::to_string(&clean).unwrap_or_default();

    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let metadata = obj.entry("metadata").or_insert_with(|| json!({}));
    let Some(meta_obj) = metadata.as_object_mut() else {
        return;
    };
    let annotations = meta_obj.entry("annotations").or_insert_with(|| json!({}));
    if let Some(ann) = annotations.as_object_mut() {
        ann.insert(
            LAST_APPLIED_ANNOTATION.to_string(),
            Value::String(annotation_value),
        );
    }
}

/// Return a copy of the value with the last-applied annotation removed
/// (used to build the annotation value without recursion).
pub fn strip_last_applied(value: &Value) -> Value {
    let mut clean = value.clone();
    if let Some(metadata) = clean.get_mut("metadata") {
        if let Some(annotations) = metadata.get_mut("annotations") {
            if let Some(ann) = annotations.as_object_mut() {
                ann.remove(LAST_APPLIED_ANNOTATION);
                if ann.is_empty() {
                    // Remove empty annotations map entirely.
                    if let Some(m) = metadata.as_object_mut() {
                        m.remove("annotations");
                    }
                }
            }
        }
    }
    clean
}

// ---------------------------------------------------------------------------
// Core apply logic
// ---------------------------------------------------------------------------

async fn apply_resource(
    client: &ApiClient,
    mapper: &RestMapper,
    value: &serde_yaml::Value,
    query: &str,
    options: &ApplyOptions,
) -> Result<ApplyResult> {
    // YAML doc -> JSON Value.
    let mut json: Value = serde_json::to_value(value)?;
    let kind = json
        .get("kind")
        .and_then(|k| k.as_str())
        .context("Missing 'kind' field")?
        .to_string();

    let mapping = mapper.resolve(&kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;

    // last-applied-configuration annotation (helper already takes &mut Value).
    set_last_applied_annotation(&mut json);

    let ns = options.namespace.clone();

    let (action_str, response) =
        crate::ops::apply_value(client, mapping, ns.as_deref(), &json, query).await?;
    let action = if action_str == "created" {
        ApplyAction::Created
    } else {
        ApplyAction::Configured
    };

    let name = crate::ops::value_name(&json).unwrap_or_default();

    Ok(ApplyResult {
        kind,
        api_group: mapping.group.clone(),
        name,
        action,
        response,
    })
}

// ---------------------------------------------------------------------------
// Subcommand support (view-last-applied, set-last-applied, edit-last-applied)
// ---------------------------------------------------------------------------

/// Resolve an API path for a resource type and name.
pub fn resolve_api_path(resource_type: &str, name: &str, namespace: &str) -> Result<String> {
    let path = match resource_type {
        "pod" | "pods" | "po" => {
            format!("/api/v1/namespaces/{}/pods/{}", namespace, name)
        }
        "service" | "services" | "svc" => {
            format!("/api/v1/namespaces/{}/services/{}", namespace, name)
        }
        "deployment" | "deployments" | "deploy" => {
            format!(
                "/apis/apps/v1/namespaces/{}/deployments/{}",
                namespace, name
            )
        }
        "statefulset" | "statefulsets" | "sts" => {
            format!(
                "/apis/apps/v1/namespaces/{}/statefulsets/{}",
                namespace, name
            )
        }
        "daemonset" | "daemonsets" | "ds" => {
            format!("/apis/apps/v1/namespaces/{}/daemonsets/{}", namespace, name)
        }
        "configmap" | "configmaps" | "cm" => {
            format!("/api/v1/namespaces/{}/configmaps/{}", namespace, name)
        }
        "secret" | "secrets" => {
            format!("/api/v1/namespaces/{}/secrets/{}", namespace, name)
        }
        "serviceaccount" | "serviceaccounts" | "sa" => {
            format!("/api/v1/namespaces/{}/serviceaccounts/{}", namespace, name)
        }
        "job" | "jobs" => {
            format!("/apis/batch/v1/namespaces/{}/jobs/{}", namespace, name)
        }
        "cronjob" | "cronjobs" | "cj" => {
            format!("/apis/batch/v1/namespaces/{}/cronjobs/{}", namespace, name)
        }
        "role" | "roles" => {
            format!(
                "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/roles/{}",
                namespace, name
            )
        }
        "rolebinding" | "rolebindings" => {
            format!(
                "/apis/rbac.authorization.k8s.io/v1/namespaces/{}/rolebindings/{}",
                namespace, name
            )
        }
        "clusterrole" | "clusterroles" => {
            format!("/apis/rbac.authorization.k8s.io/v1/clusterroles/{}", name)
        }
        "clusterrolebinding" | "clusterrolebindings" => {
            format!(
                "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings/{}",
                name
            )
        }
        "namespace" | "namespaces" | "ns" => {
            format!("/api/v1/namespaces/{}", name)
        }
        "node" | "nodes" => {
            format!("/api/v1/nodes/{}", name)
        }
        _ => anyhow::bail!(
            "Unsupported resource type for apply subcommands: {}",
            resource_type
        ),
    };
    Ok(path)
}

/// Execute apply subcommands (edit-last-applied, set-last-applied, view-last-applied)
pub async fn execute_subcommand(
    client: &ApiClient,
    command: ApplyCommands,
    default_namespace: &str,
) -> Result<()> {
    match command {
        ApplyCommands::ViewLastApplied {
            resource_type,
            name,
            namespace,
            output,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let path = resolve_api_path(&resource_type, &name, ns)?;

            let resource: Value = client
                .get(&path)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
                .context("Failed to get resource")?;

            let annotation = resource
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(LAST_APPLIED_ANNOTATION))
                .and_then(|v| v.as_str());

            match annotation {
                Some(config) => {
                    let fmt = output.as_deref().unwrap_or("yaml");
                    match fmt {
                        "json" => {
                            // Parse and re-pretty-print
                            let parsed: Value = serde_json::from_str(config)
                                .unwrap_or_else(|_| Value::String(config.to_string()));
                            println!("{}", serde_json::to_string_pretty(&parsed)?);
                        }
                        _ => {
                            // Convert JSON annotation to YAML
                            let parsed: Value = serde_json::from_str(config)
                                .unwrap_or_else(|_| Value::String(config.to_string()));
                            println!("{}", serde_yaml::to_string(&parsed)?);
                        }
                    }
                }
                None => {
                    anyhow::bail!(
                        "no last-applied-configuration annotation found on {}/{}",
                        resource_type,
                        name
                    );
                }
            }
        }
        ApplyCommands::SetLastApplied {
            filename,
            create_annotation,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);

            // Read the file
            let contents = fs::read_to_string(&filename).context("Failed to read file")?;

            // Parse to get kind/name
            let value: serde_yaml::Value = serde_yaml::from_str(&contents)?;
            let kind = value
                .get("kind")
                .and_then(|k| k.as_str())
                .context("Missing 'kind' field")?;
            let name = value
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .context("Missing 'metadata.name' field")?;

            let resource_type = kind.to_lowercase();
            let path = resolve_api_path(&resource_type, name, ns)?;

            // Convert the YAML to JSON for the annotation value
            let json_value: Value = serde_yaml::from_str(&contents)?;
            let annotation_value = serde_json::to_string(&json_value)?;

            // Check if resource exists
            let exists: Result<Value, _> = client.get(&path).await;
            match exists {
                Ok(_) => {
                    // Patch the annotation
                    let patch = json!({
                        "metadata": {
                            "annotations": {
                                LAST_APPLIED_ANNOTATION: annotation_value,
                            }
                        }
                    });
                    let _: Value = client
                        .patch(&path, &patch, "application/strategic-merge-patch+json")
                        .await
                        .context("Failed to set last-applied-configuration")?;
                    println!("{}/{} last-applied-configuration set", resource_type, name);
                }
                Err(_) if create_annotation => {
                    anyhow::bail!(
                        "resource {}/{} not found; --create-annotation requires the resource to exist",
                        resource_type,
                        name
                    );
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("{}", e)).context("Failed to get resource");
                }
            }
        }
        ApplyCommands::EditLastApplied {
            resource_type,
            name,
            namespace,
        } => {
            let ns = namespace.as_deref().unwrap_or(default_namespace);
            let path = resolve_api_path(&resource_type, &name, ns)?;

            let resource: Value = client
                .get(&path)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
                .context("Failed to get resource")?;

            let annotation = resource
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(LAST_APPLIED_ANNOTATION))
                .and_then(|v| v.as_str());

            let current_config = match annotation {
                Some(config) => config.to_string(),
                None => {
                    anyhow::bail!(
                        "no last-applied-configuration annotation found on {}/{}",
                        resource_type,
                        name
                    );
                }
            };

            // Write to temp file for editing
            let parsed: Value = serde_json::from_str(&current_config)?;
            let yaml_str = serde_yaml::to_string(&parsed)?;

            let tmp_dir = std::env::temp_dir();
            let tmp_path = tmp_dir.join(format!("{}-{}-last-applied.yaml", resource_type, name));
            fs::write(&tmp_path, &yaml_str)?;

            // Open editor
            let editor = std::env::var("KUBE_EDITOR")
                .or_else(|_| std::env::var("EDITOR"))
                .unwrap_or_else(|_| "vi".to_string());

            let status = std::process::Command::new(&editor)
                .arg(&tmp_path)
                .status()
                .context("Failed to open editor")?;

            if !status.success() {
                anyhow::bail!("Editor exited with non-zero status");
            }

            // Read back the edited file
            let edited = fs::read_to_string(&tmp_path)?;
            let edited_value: Value = serde_yaml::from_str(&edited)?;
            let new_annotation = serde_json::to_string(&edited_value)?;

            // Update the annotation
            let patch = json!({
                "metadata": {
                    "annotations": {
                        LAST_APPLIED_ANNOTATION: new_annotation,
                    }
                }
            });
            let _: Value = client
                .patch(&path, &patch, "application/strategic-merge-patch+json")
                .await
                .context("Failed to update last-applied-configuration")?;

            // Clean up temp file
            let _ = fs::remove_file(&tmp_path);

            println!(
                "{}/{} last-applied-configuration edited",
                resource_type, name
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
