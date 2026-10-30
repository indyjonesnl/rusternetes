use anyhow::{Context, Result};
use chrono::Utc;
use futures::StreamExt;
use rusternetes_client::http::{ApiClient, GetError};
use rusternetes_common::resources::{
    CronJob, DaemonSet, Deployment, Job, Namespace, Node, PersistentVolume, PersistentVolumeClaim,
    Pod, Service, StatefulSet,
};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Table,
    Json,
    Yaml,
    Wide,
    Name,
    JsonPath(String),
}

impl OutputFormat {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "json" => Ok(Self::Json),
            "yaml" => Ok(Self::Yaml),
            "wide" => Ok(Self::Wide),
            "name" => Ok(Self::Name),
            _ if s.starts_with("jsonpath=") => Ok(Self::JsonPath(s[9..].to_string())),
            _ if s.starts_with("jsonpath-file=") => {
                let path = &s[14..];
                let expr = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read jsonpath file: {}", path))?;
                Ok(Self::JsonPath(expr.trim().to_string()))
            }
            _ => anyhow::bail!(
                "Unknown output format: {}. Supported formats: json, yaml, wide, name, jsonpath=<expr>",
                s
            ),
        }
    }
}

pub fn format_output<T: Serialize>(resource: &T, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(resource)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(resource)?);
        }
        OutputFormat::JsonPath(expr) => {
            let value = serde_json::to_value(resource)?;
            let result = evaluate_jsonpath(&value, expr)?;
            print!("{}", result);
        }
        OutputFormat::Name => {
            // Print kind/name format
            let value = serde_json::to_value(resource)?;
            let kind = value
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let name = value
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            println!("{}/{}", kind.to_lowercase(), name);
        }
        _ => {
            // For table and wide, just use JSON for now
            println!("{}", serde_json::to_string_pretty(resource)?);
        }
    }
    Ok(())
}

/// Evaluate a jsonpath expression against a JSON value.
/// Supports basic dotted paths, array indexing, and filter expressions.
pub fn evaluate_jsonpath(value: &serde_json::Value, expr: &str) -> Result<String> {
    // Strip leading { and trailing } if present (kubectl jsonpath format)
    let expr = expr.trim();
    let expr = expr.strip_prefix('{').unwrap_or(expr);
    let expr = expr.strip_suffix('}').unwrap_or(expr);

    // Handle multiple expressions joined by whitespace (e.g., {.foo}{"\n"}{.bar})
    // For simplicity, evaluate the full expr as a single path
    let result = resolve_path(value, expr)?;
    Ok(result)
}

pub fn resolve_path(value: &serde_json::Value, path: &str) -> Result<String> {
    // Handle string literals like "\n", "\t"
    if path.starts_with('"') && path.ends_with('"') {
        let s = &path[1..path.len() - 1];
        return Ok(s.replace("\\n", "\n").replace("\\t", "\t"));
    }

    // Strip leading dot
    let path = path.strip_prefix('.').unwrap_or(path);

    if path.is_empty() {
        return format_value(value);
    }

    // Split on first dot or bracket
    let mut current = value;
    let mut remaining = path;

    while !remaining.is_empty() {
        if remaining.starts_with('[') {
            // Array index or filter
            let end = remaining.find(']').unwrap_or(remaining.len());
            let idx_str = &remaining[1..end];
            remaining = remaining
                .get(end + 1..)
                .unwrap_or("")
                .trim_start_matches('.');

            if let Ok(idx) = idx_str.parse::<usize>() {
                match current.get(idx) {
                    Some(v) => current = v,
                    None => return Ok(String::new()),
                }
            } else if idx_str.starts_with('?') {
                // Filter expression like [?(@.type=="Ready")]
                // For now just return empty string for unsupported filters
                return Ok(String::new());
            } else {
                return Ok(String::new());
            }
        } else {
            // Field access
            let (key, rest) = match remaining.find(['.', '[']) {
                Some(i) => {
                    let (k, r) = remaining.split_at(i);
                    (k, r.trim_start_matches('.'))
                }
                None => (remaining, ""),
            };
            remaining = rest;
            match current.get(key) {
                Some(v) => current = v,
                None => return Ok(String::new()),
            }
        }
    }

    format_value(current)
}

pub fn format_value(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Null => Ok(String::new()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Array(arr) => {
            let parts: Result<Vec<_>> = arr.iter().map(format_value).collect();
            Ok(parts?.join(" "))
        }
        serde_json::Value::Object(_) => Ok(serde_json::to_string(value)?),
    }
}

/// Parse resource_type for slash syntax (e.g. "pod/nginx") and return (type, optional_name)
pub fn parse_resource_slash(resource_type: &str) -> (&str, Option<&str>) {
    if let Some(idx) = resource_type.find('/') {
        let rtype = &resource_type[..idx];
        let rname = &resource_type[idx + 1..];
        if rname.is_empty() {
            (rtype, None)
        } else {
            (rtype, Some(rname))
        }
    } else {
        (resource_type, None)
    }
}

/// Resolve a JSONPath expression to a sortable value from a serde_json::Value
pub fn resolve_sort_key(value: &serde_json::Value, sort_expr: &str) -> String {
    let path = sort_expr
        .trim()
        .strip_prefix('{')
        .unwrap_or(sort_expr.trim());
    let path = path.strip_suffix('}').unwrap_or(path);
    let path = path.strip_prefix('.').unwrap_or(path);

    let mut current = value;
    for key in path.split('.') {
        if key.is_empty() {
            continue;
        }
        match current.get(key) {
            Some(v) => current = v,
            None => return String::new(),
        }
    }
    match current {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => format!("{:020}", n.as_f64().unwrap_or(0.0) as i64),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Sort a vector of serializable items by a JSONPath expression
pub fn sort_by_jsonpath<T: Serialize>(items: &mut [T], sort_expr: &str) {
    items.sort_by(|a, b| {
        let va = serde_json::to_value(a).unwrap_or(serde_json::Value::Null);
        let vb = serde_json::to_value(b).unwrap_or(serde_json::Value::Null);
        let ka = resolve_sort_key(&va, sort_expr);
        let kb = resolve_sort_key(&vb, sort_expr);
        ka.cmp(&kb)
    });
}

/// Format labels as a comma-separated string for --show-labels output
pub fn format_labels(labels: &Option<std::collections::HashMap<String, String>>) -> String {
    match labels {
        Some(l) if !l.is_empty() => {
            let mut pairs: Vec<_> = l.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
            pairs.sort();
            pairs.join(",")
        }
        _ => "<none>".to_string(),
    }
}

/// Watch resources by streaming newline-delimited JSON from the API
async fn watch_resources(client: &ApiClient, api_path: &str, query: &str) -> Result<()> {
    // If query already starts with ?, append &watch=true; else ?watch=true
    let watch_path = if query.is_empty() {
        format!("{}?watch=true", api_path)
    } else {
        format!("{}{}&watch=true", api_path, query)
    };

    let response = client.get_stream(&watch_path).await.map_err(|e| match e {
        GetError::NotFound => anyhow::anyhow!("Resource not found"),
        GetError::Other(e) => e,
    })?;

    let mut stream = response.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("Error reading watch stream")?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        // Process complete lines
        while let Some(newline_pos) = buffer.find('\n') {
            let line = buffer[..newline_pos].trim().to_string();
            buffer = buffer[newline_pos + 1..].to_string();

            if line.is_empty() {
                continue;
            }

            // Parse watch event: {"type": "ADDED/MODIFIED/DELETED", "object": {...}}
            if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
                let event_type = event
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("UNKNOWN");
                let object = event.get("object").unwrap_or(&event);
                let kind = object
                    .get("kind")
                    .and_then(|k| k.as_str())
                    .unwrap_or("unknown");
                let name = object
                    .pointer("/metadata/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown");
                let ns = object
                    .pointer("/metadata/namespace")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");

                let ns_prefix = if ns.is_empty() {
                    String::new()
                } else {
                    format!("{}/", ns)
                };
                println!(
                    "{:<10} {}/{}{}",
                    event_type,
                    kind.to_lowercase(),
                    ns_prefix,
                    name
                );
            }
        }
    }

    Ok(())
}

/// The curated set of kinds expanded by `get all`. Mirrors the legacy behavior
/// (pods, services, deployments, statefulsets, daemonsets, jobs, cronjobs).
/// Kinds the server doesn't serve are skipped at resolve time.
const ALL_RESOURCE_KINDS: &[&str] = &[
    "pods",
    "services",
    "deployments",
    "statefulsets",
    "daemonsets",
    "jobs",
    "cronjobs",
];

/// Enhanced execute with all new parameters — now backed by RestMapper.
#[allow(clippy::too_many_arguments)]
pub async fn execute_enhanced(
    client: &ApiClient,
    resource_type: &str,
    name: Option<&str>,
    namespace: Option<&str>,
    all_namespaces: bool,
    output: Option<&str>,
    no_headers: bool,
    selector: Option<&str>,
    field_selector: Option<&str>,
    watch: bool,
    show_labels: bool,
    sort_by: Option<&str>,
) -> Result<()> {
    // Build query string for label selector and field selector
    let mut query_params = Vec::new();
    if let Some(sel) = selector {
        query_params.push(format!("labelSelector={}", urlencoding::encode(sel)));
    }
    if let Some(fs) = field_selector {
        query_params.push(format!("fieldSelector={}", urlencoding::encode(fs)));
    }

    let query_string = if query_params.is_empty() {
        String::new()
    } else {
        format!("?{}", query_params.join("&"))
    };

    // Build the REST mapper once per invocation (one discovery round-trip).
    let mapper = crate::discovery::RestMapper::from_server(client).await?;

    // `get all` expands to a curated set of common kinds (mirrors the legacy
    // behavior). Each is resolved via the mapper; kinds the server doesn't
    // serve are silently skipped rather than erroring out.
    let is_all = resource_type.eq_ignore_ascii_case("all");

    // Support comma-separated resource types: "pods,services"
    let resource_types: Vec<&str> = if is_all {
        ALL_RESOURCE_KINDS.to_vec()
    } else {
        resource_type.split(',').collect()
    };

    // Tracks whether any kind under `get all` produced output (drives the
    // blank-line separators between non-empty kinds).
    let mut printed_any = false;

    for (i, rtype) in resource_types.iter().enumerate() {
        // Support type/name syntax: "pod/nginx"
        let (resolved_type, slash_name) = parse_resource_slash(rtype.trim());
        let effective_name = slash_name.or(if resource_types.len() == 1 {
            name
        } else {
            None
        });

        // Resolve the resource type via the discovery mapper. Under `get all`
        // an unservable kind is skipped instead of failing the whole command.
        let mapping = match mapper.resolve(resolved_type) {
            Some(m) => m,
            None if is_all => continue,
            None => {
                return Err(anyhow::anyhow!(
                    "error: the server doesn't have a resource type \"{}\"",
                    resolved_type
                ));
            }
        };

        if watch {
            // Build the watch collection path from the mapping. ops::list_path
            // applies the all-namespaces cluster-wide override (no namespace
            // segment), so `get pods -A --watch` hits `/api/v1/pods`.
            let base_path = crate::ops::list_path(mapping, namespace, all_namespaces);
            watch_resources(client, &base_path, &query_string).await?;
            return Ok(());
        }

        // For `get all`, suppress per-kind "No resources found" noise and only
        // separate kinds that actually produced output. The separator is
        // emitted inside execute_with_mapping right before it prints, so an
        // empty kind between two non-empty kinds never leaves a stray blank
        // line. For a normal single / comma-list invocation, keep the legacy
        // blank-line-before-subsequent behavior and the empty-collection
        // message.
        if is_all {
            // Ask the callee to lead with a blank line only if a prior kind
            // already printed; it will only honor this when it actually prints.
            let printed = execute_with_mapping(
                client,
                mapping,
                effective_name,
                namespace,
                all_namespaces,
                output,
                no_headers,
                &query_string,
                show_labels,
                sort_by,
                true,
                printed_any,
            )
            .await?;
            printed_any = printed_any || printed;
        } else {
            // Legacy comma-list behavior: a blank line before every kind after
            // the first, printed unconditionally here (not inside the callee).
            if i > 0 {
                println!();
            }
            execute_with_mapping(
                client,
                mapping,
                effective_name,
                namespace,
                all_namespaces,
                output,
                no_headers,
                &query_string,
                show_labels,
                sort_by,
                false,
                false,
            )
            .await?;
        }
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

/// Inner dispatcher: fetch and render a single resolved resource kind.
///
/// For the well-known kinds that have typed table printers (Pod, Service,
/// Deployment, Node, Namespace, Job, CronJob, PersistentVolume,
/// PersistentVolumeClaim) the fetched JSON is deserialized into the concrete
/// type so the existing column-aware printers are preserved. For every other
/// kind — and for all non-table output formats — the raw `serde_json::Value`
/// is passed directly to `format_output`.
///
/// When `is_all` is true (invoked via `get all`), empty collections are
/// silently skipped (no "No resources found" message). `leading_separator`
/// requests a blank line before this kind's output; it is honored only when
/// the kind actually prints, so an empty kind never leaves a stray blank line.
/// Returns whether any output was produced.
#[allow(clippy::too_many_arguments)]
async fn execute_with_mapping(
    client: &ApiClient,
    mapping: &crate::discovery::ResourceMapping,
    name: Option<&str>,
    namespace: Option<&str>,
    all_namespaces: bool,
    output: Option<&str>,
    no_headers: bool,
    query: &str,
    show_labels: bool,
    sort_by: Option<&str>,
    is_all: bool,
    leading_separator: bool,
) -> Result<bool> {
    use crate::ops::{get_value, list_value};

    let format = output
        .map(OutputFormat::from_str)
        .transpose()?
        .unwrap_or(OutputFormat::Table);

    if let Some(n) = name {
        // Single-resource GET
        let value = get_value(client, mapping, namespace, n)
            .await
            .map_err(|e| {
                // Mirror kubectl's "Error from server (NotFound)" message when the
                // underlying error string contains "not found".
                let s = e.to_string();
                if s.to_lowercase().contains("not found") {
                    anyhow::anyhow!(
                        "Error from server (NotFound): {} \"{}\" not found",
                        mapping.kind.to_lowercase() + "s",
                        n
                    )
                } else {
                    e
                }
            })?;
        // selectors are not forwarded to single-item by-name GETs (they don't
        // apply).
        if leading_separator {
            println!();
        }
        format_output(&value, &format)?;
    } else {
        // Collection GET — all path logic (including the all-namespaces
        // cluster-wide override) lives in ops::list_value / ops::list_path.
        let mut items = list_value(client, mapping, namespace, all_namespaces, query).await?;

        if items.is_empty() {
            // Under `get all` an empty kind is skipped silently.
            if is_all {
                return Ok(false);
            }
            if all_namespaces {
                eprintln!("No resources found.");
            } else {
                eprintln!(
                    "No resources found in {} namespace.",
                    namespace.unwrap_or("default")
                );
            }
            return Ok(false);
        }

        if let Some(sort_expr) = sort_by {
            sort_by_jsonpath(&mut items, sort_expr);
        }

        // Emit the inter-kind separator only now that we know this kind prints.
        if leading_separator {
            println!();
        }

        // For well-known kinds with typed printers, attempt to deserialize and
        // use the column-aware printer. Fall back to generic Value output if
        // deserialization fails or the kind isn't recognized.
        match (mapping.kind.as_str(), &format) {
            ("Pod", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Pod> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_pods(&typed, no_headers, show_labels);
            }
            ("Service", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Service> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_services(&typed, no_headers, show_labels);
            }
            ("Deployment", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Deployment> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_deployments(&typed, no_headers, show_labels);
            }
            ("Node", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Node> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_nodes(&typed, no_headers, show_labels);
            }
            ("Namespace", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Namespace> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_namespaces(&typed, no_headers, show_labels);
            }
            ("StatefulSet", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<StatefulSet> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_statefulsets(&typed, no_headers, show_labels);
            }
            ("DaemonSet", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<DaemonSet> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_daemonsets(&typed, no_headers, show_labels);
            }
            ("Job", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<Job> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_jobs(&typed, no_headers, show_labels);
            }
            ("CronJob", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<CronJob> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_cronjobs(&typed, no_headers, show_labels);
            }
            ("PersistentVolume", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<PersistentVolume> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_pvs(&typed, no_headers, show_labels);
            }
            ("PersistentVolumeClaim", OutputFormat::Table | OutputFormat::Wide) => {
                let typed: Vec<PersistentVolumeClaim> = items
                    .iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect();
                print_pvcs(&typed, no_headers, show_labels);
            }
            _ => {
                // Generic Value output: works for all formats and any kind.
                format_output(&items, &format)?;
            }
        }
    }

    Ok(true)
}

pub fn print_pods(pods: &[Pod], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<15} {:<15} LABELS", "NAME", "STATUS", "NODE");
        } else {
            println!("{:<30} {:<15} {:<15}", "NAME", "STATUS", "NODE");
        }
    }
    for pod in pods {
        let status = pod
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        let node = pod
            .spec
            .as_ref()
            .and_then(|s| s.node_name.as_ref())
            .map(|n| n.as_str())
            .unwrap_or("<none>");
        if show_labels {
            println!(
                "{:<30} {:<15} {:<15} {}",
                pod.metadata.name,
                status,
                node,
                format_labels(&pod.metadata.labels)
            );
        } else {
            println!("{:<30} {:<15} {:<15}", pod.metadata.name, status, node);
        }
    }
}

pub fn print_services(services: &[Service], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<20} {:<10} LABELS", "NAME", "CLUSTER-IP", "PORTS");
        } else {
            println!("{:<30} {:<20} {:<10}", "NAME", "CLUSTER-IP", "PORTS");
        }
    }
    for service in services {
        let cluster_ip = service.spec.cluster_ip.as_deref().unwrap_or("<none>");
        let ports = service
            .spec
            .ports
            .iter()
            .map(|p| p.port.to_string())
            .collect::<Vec<_>>()
            .join(",");
        if show_labels {
            println!(
                "{:<30} {:<20} {:<10} {}",
                service.metadata.name,
                cluster_ip,
                ports,
                format_labels(&service.metadata.labels)
            );
        } else {
            println!(
                "{:<30} {:<20} {:<10}",
                service.metadata.name, cluster_ip, ports
            );
        }
    }
}

pub fn format_duration(duration: chrono::Duration) -> String {
    let days = duration.num_days();
    if days > 0 {
        return format!("{}d", days);
    }
    let hours = duration.num_hours();
    if hours > 0 {
        return format!("{}h", hours);
    }
    let minutes = duration.num_minutes();
    if minutes > 0 {
        return format!("{}m", minutes);
    }
    let seconds = duration.num_seconds();
    format!("{}s", seconds)
}

pub fn print_deployments(deployments: &[Deployment], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10} LABELS",
                "NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE"
            );
        } else {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10}",
                "NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE"
            );
        }
    }
    for deployment in deployments {
        let desired = deployment.spec.replicas.unwrap_or(1);
        let ready = deployment
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);
        let updated = deployment
            .status
            .as_ref()
            .and_then(|s| s.updated_replicas)
            .unwrap_or(0);
        let available = deployment
            .status
            .as_ref()
            .and_then(|s| s.available_replicas)
            .unwrap_or(0);
        let age = deployment
            .metadata
            .creation_timestamp
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<unknown>".to_string());
        if show_labels {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10} {}",
                deployment.metadata.name,
                format!("{}/{}", ready, desired),
                updated,
                available,
                age,
                format_labels(&deployment.metadata.labels)
            );
        } else {
            println!(
                "{:<30} {:<15} {:<15} {:<15} {:<10}",
                deployment.metadata.name,
                format!("{}/{}", ready, desired),
                updated,
                available,
                age
            );
        }
    }
}

pub fn print_statefulsets(statefulsets: &[StatefulSet], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<10} {:<10} LABELS", "NAME", "READY", "AGE");
        } else {
            println!("{:<30} {:<10} {:<10}", "NAME", "READY", "AGE");
        }
    }
    for sts in statefulsets {
        let desired = sts.spec.replicas.unwrap_or(0);
        let ready = sts
            .status
            .as_ref()
            .and_then(|s| s.ready_replicas)
            .unwrap_or(0);
        let age = sts
            .metadata
            .creation_timestamp
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<unknown>".to_string());
        if show_labels {
            println!(
                "{:<30} {:<10} {:<10} {}",
                sts.metadata.name,
                format!("{}/{}", ready, desired),
                age,
                format_labels(&sts.metadata.labels)
            );
        } else {
            println!(
                "{:<30} {:<10} {:<10}",
                sts.metadata.name,
                format!("{}/{}", ready, desired),
                age
            );
        }
    }
}

pub fn print_daemonsets(daemonsets: &[DaemonSet], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!(
                "{:<30} {:<9} {:<9} {:<7} {:<12} {:<11} {:<20} {:<10} LABELS",
                "NAME",
                "DESIRED",
                "CURRENT",
                "READY",
                "UP-TO-DATE",
                "AVAILABLE",
                "NODE SELECTOR",
                "AGE"
            );
        } else {
            println!(
                "{:<30} {:<9} {:<9} {:<7} {:<12} {:<11} {:<20} {:<10}",
                "NAME",
                "DESIRED",
                "CURRENT",
                "READY",
                "UP-TO-DATE",
                "AVAILABLE",
                "NODE SELECTOR",
                "AGE"
            );
        }
    }
    for ds in daemonsets {
        let desired = ds
            .status
            .as_ref()
            .map(|s| s.desired_number_scheduled)
            .unwrap_or(0);
        let current = ds
            .status
            .as_ref()
            .map(|s| s.current_number_scheduled)
            .unwrap_or(0);
        let ready = ds.status.as_ref().map(|s| s.number_ready).unwrap_or(0);
        let updated = ds
            .status
            .as_ref()
            .and_then(|s| s.updated_number_scheduled)
            .unwrap_or(0);
        let available = ds
            .status
            .as_ref()
            .and_then(|s| s.number_available)
            .unwrap_or(0);
        let node_selector = match &ds.spec.template.spec.node_selector {
            Some(sel) if !sel.is_empty() => {
                let mut pairs: Vec<_> = sel.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                pairs.sort();
                pairs.join(",")
            }
            _ => "<none>".to_string(),
        };
        let age = ds
            .metadata
            .creation_timestamp
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<unknown>".to_string());
        if show_labels {
            println!(
                "{:<30} {:<9} {:<9} {:<7} {:<12} {:<11} {:<20} {:<10} {}",
                ds.metadata.name,
                desired,
                current,
                ready,
                updated,
                available,
                node_selector,
                age,
                format_labels(&ds.metadata.labels)
            );
        } else {
            println!(
                "{:<30} {:<9} {:<9} {:<7} {:<12} {:<11} {:<20} {:<10}",
                ds.metadata.name, desired, current, ready, updated, available, node_selector, age
            );
        }
    }
}

pub fn print_nodes(nodes: &[Node], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<15} LABELS", "NAME", "STATUS");
        } else {
            println!("{:<30} {:<15}", "NAME", "STATUS");
        }
    }
    for node in nodes {
        let status = node
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|c| c.iter().find(|cond| cond.condition_type == "Ready"))
            .map(|c| c.status.as_str())
            .unwrap_or("Unknown");
        if show_labels {
            println!(
                "{:<30} {:<15} {}",
                node.metadata.name,
                status,
                format_labels(&node.metadata.labels)
            );
        } else {
            println!("{:<30} {:<15}", node.metadata.name, status);
        }
    }
}

pub fn print_namespaces(namespaces: &[Namespace], no_headers: bool, show_labels: bool) {
    if !no_headers {
        if show_labels {
            println!("{:<30} {:<15} LABELS", "NAME", "STATUS");
        } else {
            println!("{:<30} {:<15}", "NAME", "STATUS");
        }
    }
    for namespace in namespaces {
        let status = namespace
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        if show_labels {
            println!(
                "{:<30} {:<15} {}",
                namespace.metadata.name,
                status,
                format_labels(&namespace.metadata.labels)
            );
        } else {
            println!("{:<30} {:<15}", namespace.metadata.name, status);
        }
    }
}

pub fn print_pvs(pvs: &[PersistentVolume], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!(
            "{:<30} {:<15} {:<20} {:<15}",
            "NAME", "CAPACITY", "ACCESS MODES", "STATUS"
        );
    }
    for pv in pvs {
        let capacity = pv
            .spec
            .capacity
            .get("storage")
            .map(|s| s.as_str())
            .unwrap_or("<none>");
        let access_modes = pv
            .spec
            .access_modes
            .iter()
            .map(|m| format!("{:?}", m))
            .collect::<Vec<_>>()
            .join(",");
        let status = pv
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        println!(
            "{:<30} {:<15} {:<20} {:<15}",
            pv.metadata.name, capacity, access_modes, status
        );
    }
}

pub fn print_pvcs(pvcs: &[PersistentVolumeClaim], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!(
            "{:<30} {:<15} {:<20} {:<20} {:<15}",
            "NAME", "STATUS", "VOLUME", "CAPACITY", "ACCESS MODES"
        );
    }
    for pvc in pvcs {
        let status = pvc
            .status
            .as_ref()
            .map(|s| format!("{:?}", s.phase))
            .unwrap_or_else(|| "Unknown".to_string());
        let volume = pvc.spec.volume_name.as_deref().unwrap_or("<none>");
        let capacity = pvc
            .status
            .as_ref()
            .and_then(|s| s.capacity.as_ref())
            .and_then(|c| c.get("storage"))
            .map(|s| s.as_str())
            .unwrap_or("<none>");
        let access_modes = pvc
            .spec
            .access_modes
            .iter()
            .map(|m| format!("{:?}", m))
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<30} {:<15} {:<20} {:<20} {:<15}",
            pvc.metadata.name, status, volume, capacity, access_modes
        );
    }
}

pub fn print_jobs(jobs: &[Job], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!("{:<30} {:<15} {:<10}", "NAME", "COMPLETIONS", "AGE");
    }
    for job in jobs {
        let completions = job.spec.completions.unwrap_or(1);
        let succeeded = job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);
        let age = job
            .metadata
            .creation_timestamp
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<unknown>".to_string());
        println!(
            "{:<30} {:<15} {:<10}",
            job.metadata.name,
            format!("{}/{}", succeeded, completions),
            age
        );
    }
}

pub fn print_cronjobs(cronjobs: &[CronJob], no_headers: bool, _show_labels: bool) {
    if !no_headers {
        println!(
            "{:<30} {:<20} {:<10} {:<20} {:<10}",
            "NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "LAST SCHEDULE"
        );
    }
    for cronjob in cronjobs {
        let schedule = &cronjob.spec.schedule;
        let suspend = cronjob.spec.suspend.unwrap_or(false);
        let active = cronjob.status.as_ref().map(|s| s.active.len()).unwrap_or(0);
        let last_schedule = cronjob
            .status
            .as_ref()
            .and_then(|s| s.last_schedule_time)
            .map(|ts| format_duration(Utc::now().signed_duration_since(ts)))
            .unwrap_or_else(|| "<none>".to_string());
        println!(
            "{:<30} {:<20} {:<10} {:<20} {:<10}",
            cronjob.metadata.name, schedule, suspend, active, last_schedule
        );
    }
}
