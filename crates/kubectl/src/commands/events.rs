use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusternetes_client::http::ApiClient;
use serde_json::Value;

/// List events in a namespace, optionally filtered by resource.
///
/// Equivalent to:
///   kubectl events
///   kubectl events --for pod/nginx
///   kubectl events -A
///   kubectl events --types=Warning
#[allow(clippy::too_many_arguments)]
pub async fn execute(
    client: &ApiClient,
    namespace: &str,
    all_namespaces: bool,
    for_object: Option<&str>,
    types: Option<&str>,
    watch: bool,
    no_headers: bool,
    output: Option<&str>,
) -> Result<()> {
    let ns_path = if all_namespaces {
        "/api/v1/events".to_string()
    } else {
        format!("/api/v1/namespaces/{}/events", namespace)
    };

    // Build field selector for --for filtering
    let path = if let Some(for_obj) = for_object {
        let (kind, obj_name) = parse_for_object(for_obj)?;
        let field_selector = format!(
            "involvedObject.kind={},involvedObject.name={}",
            kind, obj_name
        );
        format!("{}?fieldSelector={}", ns_path, urlencoding(&field_selector))
    } else {
        ns_path
    };

    // Parse type filters
    let type_filters: Vec<String> = types
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    // Validate type filters
    for t in &type_filters {
        if !t.eq_ignore_ascii_case("Normal") && !t.eq_ignore_ascii_case("Warning") {
            anyhow::bail!("valid --types are Normal or Warning");
        }
    }

    if watch {
        return execute_watch(client, &path, &type_filters, all_namespaces, no_headers).await;
    }

    let events: Value = client
        .get(&path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list events: {}", e))?;

    let items = events
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();

    // Filter by type if specified
    let filtered: Vec<&Value> = items
        .iter()
        .filter(|event| {
            if type_filters.is_empty() {
                return true;
            }
            let event_type = event
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("Normal");
            type_filters
                .iter()
                .any(|f| f.eq_ignore_ascii_case(event_type))
        })
        .collect();

    if filtered.is_empty() {
        if all_namespaces {
            eprintln!("No events found.");
        } else {
            eprintln!("No events found in {} namespace.", namespace);
        }
        return Ok(());
    }

    // Sort events by lastTimestamp (or eventTime)
    let mut sorted: Vec<&Value> = filtered;
    sorted.sort_by(|a, b| {
        let time_a = get_event_time(a);
        let time_b = get_event_time(b);
        time_a.cmp(&time_b)
    });

    match output {
        Some("json") => {
            let output_list = serde_json::json!({
                "apiVersion": "v1",
                "kind": "EventList",
                "items": sorted,
            });
            println!("{}", serde_json::to_string_pretty(&output_list)?);
        }
        Some("yaml") => {
            let output_list = serde_json::json!({
                "apiVersion": "v1",
                "kind": "EventList",
                "items": sorted,
            });
            println!("{}", serde_yaml::to_string(&output_list)?);
        }
        _ => {
            // Table output
            if !no_headers {
                if all_namespaces {
                    println!(
                        "{:<12} {:<8} {:<6} {:<40} {:<20} {:<}",
                        "NAMESPACE", "LAST SEEN", "TYPE", "REASON", "OBJECT", "MESSAGE"
                    );
                } else {
                    println!(
                        "{:<8} {:<8} {:<6} {:<40} {:<20} {:<}",
                        "LAST SEEN", "TYPE", "COUNT", "REASON", "OBJECT", "MESSAGE"
                    );
                }
            }

            for event in &sorted {
                print_event_row(event, all_namespaces);
            }
        }
    }

    Ok(())
}

async fn execute_watch(
    client: &ApiClient,
    path: &str,
    type_filters: &[String],
    all_namespaces: bool,
    no_headers: bool,
) -> Result<()> {
    // First list existing events
    let events: Value = client
        .get(path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to list events: {}", e))?;

    let items = events
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();

    if !no_headers {
        if all_namespaces {
            println!(
                "{:<12} {:<8} {:<6} {:<40} {:<20} {:<}",
                "NAMESPACE", "LAST SEEN", "TYPE", "REASON", "OBJECT", "MESSAGE"
            );
        } else {
            println!(
                "{:<8} {:<8} {:<6} {:<40} {:<20} {:<}",
                "LAST SEEN", "TYPE", "COUNT", "REASON", "OBJECT", "MESSAGE"
            );
        }
    }

    for event in &items {
        if !type_filters.is_empty() {
            let event_type = event
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("Normal");
            if !type_filters
                .iter()
                .any(|f| f.eq_ignore_ascii_case(event_type))
            {
                continue;
            }
        }
        print_event_row(event, all_namespaces);
    }

    // Get resource version for watch
    let resource_version = events
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|r| r.as_str())
        .unwrap_or("0");

    let watch_path = if path.contains('?') {
        format!("{}&watch=true&resourceVersion={}", path, resource_version)
    } else {
        format!("{}?watch=true&resourceVersion={}", path, resource_version)
    };

    // Stream watch events
    let text = client
        .get_text(&watch_path)
        .await
        .context("Failed to watch events")?;

    for line in text.lines() {
        if let Ok(watch_event) = serde_json::from_str::<Value>(line) {
            if let Some(event) = watch_event.get("object") {
                if !type_filters.is_empty() {
                    let event_type = event
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("Normal");
                    if !type_filters
                        .iter()
                        .any(|f| f.eq_ignore_ascii_case(event_type))
                    {
                        continue;
                    }
                }
                print_event_row(event, all_namespaces);
            }
        }
    }

    Ok(())
}

pub fn parse_for_object(for_obj: &str) -> Result<(String, &str)> {
    let (kind_str, name) = for_obj
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("--for must be in resource/name form (e.g., pod/nginx)"))?;

    // Capitalize and singularize the kind
    let kind = match kind_str.to_lowercase().as_str() {
        "pod" | "pods" | "po" => "Pod",
        "service" | "services" | "svc" => "Service",
        "deployment" | "deployments" | "deploy" => "Deployment",
        "replicaset" | "replicasets" | "rs" => "ReplicaSet",
        "statefulset" | "statefulsets" | "sts" => "StatefulSet",
        "daemonset" | "daemonsets" | "ds" => "DaemonSet",
        "job" | "jobs" => "Job",
        "cronjob" | "cronjobs" | "cj" => "CronJob",
        "node" | "nodes" | "no" => "Node",
        "namespace" | "namespaces" | "ns" => "Namespace",
        "configmap" | "configmaps" | "cm" => "ConfigMap",
        "secret" | "secrets" => "Secret",
        "persistentvolumeclaim" | "persistentvolumeclaims" | "pvc" => "PersistentVolumeClaim",
        "persistentvolume" | "persistentvolumes" | "pv" => "PersistentVolume",
        "ingress" | "ingresses" | "ing" => "Ingress",
        "horizontalpodautoscaler" | "horizontalpodautoscalers" | "hpa" => "HorizontalPodAutoscaler",
        other => {
            // Try to capitalize first letter
            let mut chars = other.chars();
            match chars.next() {
                None => return Err(anyhow::anyhow!("empty resource type")),
                Some(first) => {
                    let capitalized: String = first.to_uppercase().chain(chars).collect();
                    return Ok((capitalized, name));
                }
            }
        }
    };

    Ok((kind.to_string(), name))
}

pub fn get_event_time(event: &Value) -> String {
    // Prefer eventTime, then lastTimestamp, then firstTimestamp
    event
        .get("eventTime")
        .or_else(|| event.get("lastTimestamp"))
        .or_else(|| event.get("firstTimestamp"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string()
}

pub fn format_age(timestamp: &str) -> String {
    if timestamp.is_empty() {
        return "<unknown>".to_string();
    }

    let parsed = DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.with_timezone(&Utc))
        .ok();

    match parsed {
        Some(dt) => {
            let now = Utc::now();
            let duration = now.signed_duration_since(dt);
            let seconds = duration.num_seconds();
            if seconds < 0 {
                return "<future>".to_string();
            }
            if seconds < 60 {
                format!("{}s", seconds)
            } else if seconds < 3600 {
                format!("{}m", seconds / 60)
            } else if seconds < 86400 {
                format!("{}h", seconds / 3600)
            } else {
                format!("{}d", seconds / 86400)
            }
        }
        None => timestamp.to_string(),
    }
}

pub fn print_event_row(event: &Value, all_namespaces: bool) {
    let last_seen = get_event_time(event);
    let age = format_age(&last_seen);
    let event_type = event
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("Normal");
    let reason = event.get("reason").and_then(|r| r.as_str()).unwrap_or("");
    let message = event.get("message").and_then(|m| m.as_str()).unwrap_or("");
    let count = event.get("count").and_then(|c| c.as_i64()).unwrap_or(1);

    // Build object reference
    let involved = event.get("involvedObject");
    let obj_kind = involved
        .and_then(|o| o.get("kind"))
        .and_then(|k| k.as_str())
        .unwrap_or("");
    let obj_name = involved
        .and_then(|o| o.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("");
    let object = format!("{}/{}", obj_kind.to_lowercase(), obj_name);

    if all_namespaces {
        let ns = event
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .unwrap_or("");
        println!(
            "{:<12} {:<8} {:<6} {:<40} {:<20} {}",
            ns, age, event_type, reason, object, message
        );
    } else {
        println!(
            "{:<8} {:<8} {:<6} {:<40} {:<20} {}",
            age, event_type, count, reason, object, message
        );
    }
}

pub fn urlencoding(s: &str) -> String {
    s.replace('=', "%3D").replace(',', "%2C")
}
