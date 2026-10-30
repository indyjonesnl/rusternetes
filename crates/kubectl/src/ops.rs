use crate::discovery::ResourceMapping;
use anyhow::{Context, Result};
use rusternetes_client::http::ApiClient;
use serde_json::Value;

/// Build the API path for a resource. When `name` is `None` the collection
/// path is returned; otherwise the item path. For cluster-scoped resources the
/// namespace is ignored.
pub fn build_path(m: &ResourceMapping, namespace: Option<&str>, name: Option<&str>) -> String {
    let base = if m.group.is_empty() {
        format!("/api/{}", m.version)
    } else {
        format!("/apis/{}/{}", m.group, m.version)
    };
    let mut path = if m.namespaced {
        let ns = namespace.unwrap_or("default");
        format!("{base}/namespaces/{ns}/{}", m.plural)
    } else {
        format!("{base}/{}", m.plural)
    };
    if let Some(n) = name {
        path.push('/');
        path.push_str(n);
    }
    path
}

/// Read `metadata.name` from a resource Value.
pub fn value_name(v: &Value) -> Option<String> {
    v.pointer("/metadata/name")
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Read `metadata.namespace` from a resource Value.
pub fn value_namespace(v: &Value) -> Option<String> {
    v.pointer("/metadata/namespace")
        .and_then(|n| n.as_str())
        .map(String::from)
}

/// Apply a resource Value (create-or-replace). Returns the action taken
/// ("created" or "configured") and the server response body.
///
/// `namespace`: the explicit `-n` flag value, or `None` when unset (falls back
/// to the body's metadata.namespace, then "default").
///
/// `query`: an optional query-string suffix (e.g. `?dryRun=All&fieldManager=...`)
/// appended to the PUT/POST URLs. The existence check is performed on the bare
/// item path so a query string never changes how we detect create-vs-replace.
pub async fn apply_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    body: &Value,
    query: &str,
) -> Result<(&'static str, Value)> {
    let name = value_name(body).context("resource is missing metadata.name")?;
    let ns = if m.namespaced {
        Some(
            namespace
                .map(String::from)
                .or_else(|| value_namespace(body))
                .unwrap_or_else(|| "default".to_string()),
        )
    } else {
        None
    };
    let item = build_path(m, ns.as_deref(), Some(&name));
    let collection = build_path(m, ns.as_deref(), None);
    if client.resource_exists(&item).await? {
        let resp: Value = client.put(&format!("{item}{query}"), body).await?;
        Ok(("configured", resp))
    } else {
        let resp: Value = client.post(&format!("{collection}{query}"), body).await?;
        Ok(("created", resp))
    }
}

/// GET a single resource as a Value.
pub async fn get_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    name: &str,
) -> Result<Value> {
    let ns = if m.namespaced {
        Some(namespace.unwrap_or("default").to_string())
    } else {
        None
    };
    let path = build_path(m, ns.as_deref(), Some(name));
    client
        .get::<Value>(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Build the collection path for a LIST. When `all_namespaces` is true for a
/// namespaced resource the cluster-wide path is used (no `/namespaces/{ns}`
/// segment), e.g. `/api/v1/pods` rather than `/api/v1/namespaces/default/pods`.
pub fn list_path(m: &ResourceMapping, namespace: Option<&str>, all_namespaces: bool) -> String {
    if m.namespaced && all_namespaces {
        // Cluster-wide path: no /namespaces/{ns} segment
        let base = if m.group.is_empty() {
            format!("/api/{}", m.version)
        } else {
            format!("/apis/{}/{}", m.group, m.version)
        };
        format!("{}/{}", base, m.plural)
    } else {
        let ns = if m.namespaced {
            Some(namespace.unwrap_or("default").to_string())
        } else {
            None
        };
        build_path(m, ns.as_deref(), None)
    }
}

/// Build the `?labelSelector=<encoded>` query suffix for a LIST, or an empty
/// string when no selector is given. Percent-encodes the selector value so
/// set-based expressions (which may contain spaces / parens) survive transport.
pub fn label_selector_query(selector: Option<&str>) -> String {
    match selector.filter(|s| !s.is_empty()) {
        None => String::new(),
        Some(sel) => {
            let encoded: String = sel
                .chars()
                .map(|c| match c {
                    'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' | '=' | ',' | '!' => {
                        c.to_string()
                    }
                    ' ' => "+".to_string(),
                    _ => format!("%{:02X}", c as u8),
                })
                .collect();
            format!("?labelSelector={encoded}")
        }
    }
}

/// GET a resource collection; returns the `.items` array as Values. When
/// `all_namespaces` is true for a namespaced resource the cluster-wide
/// collection path is used (no namespace segment). `query` is an optional
/// query-string suffix (e.g. `?labelSelector=...`) appended to the path.
pub async fn list_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    all_namespaces: bool,
    query: &str,
) -> Result<Vec<Value>> {
    let path = format!("{}{}", list_path(m, namespace, all_namespaces), query);
    let list: Value = client
        .get::<Value>(&path)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(list
        .get("items")
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default())
}

/// DELETE a single resource by name.
///
/// `query` is an optional query-string suffix (e.g. `?gracePeriodSeconds=0&dryRun=All`)
/// appended to the path, consistent with the pattern used by `apply_value` and
/// `list_value`.  Callers that need propagationPolicy in the request body should
/// build it separately and call `client.delete_with_options` directly.
pub async fn delete_value(
    client: &ApiClient,
    m: &ResourceMapping,
    namespace: Option<&str>,
    name: &str,
    query: &str,
    body: Option<&serde_json::Value>,
) -> Result<reqwest::StatusCode> {
    let ns = if m.namespaced {
        Some(namespace.unwrap_or("default").to_string())
    } else {
        None
    };
    let path = build_path(m, ns.as_deref(), Some(name));
    let full_path = format!("{path}{query}");
    // query is already encoded into `full_path`; pass no extra query_params
    client.delete_with_options(&full_path, &[], body).await
}

/// Format a resource label the way kubectl does: "pod/name" for core-group
/// resources, "deployment.apps/name" for grouped resources.
pub fn resource_label(m: &ResourceMapping, name: &str) -> String {
    if m.group.is_empty() {
        format!("{}/{}", m.singular, name)
    } else {
        format!("{}.{}/{}", m.singular, m.group, name)
    }
}
