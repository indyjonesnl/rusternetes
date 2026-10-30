use crate::discovery::RestMapper;
use crate::ops::build_path;
use anyhow::{Context, Result};
use rusternetes_client::http::{ApiClient, GetError};
use serde_json::Value;

/// Show diff between current and applied configuration
pub async fn execute(client: &ApiClient, file: &str, namespace: &str) -> Result<()> {
    // Read + split the source into its YAML documents. Empty documents (e.g. a
    // trailing `---`) are skipped by the shared helper.
    let documents = crate::manifest::read_documents(file)?;

    // Build the REST mapper once for all documents in this file.
    let mapper = RestMapper::from_server(client).await?;

    for value in &documents {
        diff_resource(client, &mapper, value, namespace).await?;
    }

    Ok(())
}

async fn diff_resource(
    client: &ApiClient,
    mapper: &RestMapper,
    value: &serde_yaml::Value,
    default_namespace: &str,
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

    // Resolve the kind via the server-side REST mapper.
    let mapping = mapper.resolve(kind).ok_or_else(|| {
        anyhow::anyhow!("error: the server doesn't have a resource type \"{kind}\"")
    })?;

    // Determine the effective namespace.
    let ns: Option<String> = if mapping.namespaced {
        let from_body = metadata.get("namespace").and_then(|n| n.as_str());
        Some(from_body.unwrap_or(default_namespace).to_string())
    } else {
        None
    };

    // Build the item path and fetch the live object.
    let api_path = build_path(mapping, ns.as_deref(), Some(name));

    // Fetch the live object. We match on GetError so we can cleanly distinguish
    // a genuine 404 (resource doesn't exist yet → show a creation diff) from
    // any other error (connection failure, permission denied, etc.) which we
    // propagate to the caller.
    // Can't use ops::get_value here — it erases GetError::NotFound, which we need
    // for the creation-diff branch.
    let current_yaml = match client.get::<Value>(&api_path).await {
        Ok(mut current) => {
            // Strip server-managed fields the manifest never carries so the diff
            // shows only meaningful changes.
            strip_server_managed(&mut current);
            // Convert to YAML for diffing
            serde_yaml::to_string(&current)?
        }
        Err(GetError::NotFound) => {
            println!("--- /dev/null");
            println!("+++ {}/{} (create)", kind, name);
            // Round-trip through serde_json::Value so keys sort the same way the
            // live side does (see the changed-resource branch below).
            let mut new_json: serde_json::Value = serde_yaml::from_value(value.clone())?;
            strip_server_managed(&mut new_json);
            let new_yaml = serde_yaml::to_string(&new_json)?;
            for line in new_yaml.lines() {
                println!("+{}", line);
            }
            println!();
            return Ok(());
        }
        Err(GetError::Other(e)) => {
            return Err(e);
        }
    };

    // Prepare new resource YAML. Round-trip the desired object through
    // serde_json::Value so its keys sort identically to the live side (which is
    // a serde_json::Value → sorted via BTreeMap). Without this, manifests whose
    // keys aren't already alphabetical would show spurious diffs every run.
    let mut new_json: serde_json::Value = serde_yaml::from_value(value.clone())?;
    // Strip the same server-managed fields from the desired side so a field that
    // exists only on the live side never renders as a deletion.
    strip_server_managed(&mut new_json);
    let new_yaml = serde_yaml::to_string(&new_json)?;

    // Calculate and display diff
    if current_yaml.trim() == new_yaml.trim() {
        println!("No changes for {}/{}", kind, name);
        println!();
        return Ok(());
    }

    println!("--- {}/{} (current)", kind, name);
    println!("+++ {}/{} (new)", kind, name);

    // Simple line-by-line diff
    let current_lines: Vec<&str> = current_yaml.lines().collect();
    let new_lines: Vec<&str> = new_yaml.lines().collect();

    let max_len = current_lines.len().max(new_lines.len());
    for i in 0..max_len {
        let current_line = current_lines.get(i).copied();
        let new_line = new_lines.get(i).copied();

        match (current_line, new_line) {
            (Some(c), Some(n)) if c == n => println!(" {}", c),
            (Some(c), Some(n)) => {
                println!("-{}", c);
                println!("+{}", n);
            }
            (Some(c), None) => println!("-{}", c),
            (None, Some(n)) => println!("+{}", n),
            (None, None) => unreachable!(),
        }
    }
    println!();

    Ok(())
}

/// Remove server-managed fields that the user's manifest never sets, so `diff`
/// shows only meaningful changes (approximation of kubectl's server-side-apply
/// dry-run diff).
pub fn strip_server_managed(v: &mut serde_json::Value) {
    if let Some(meta) = v.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        for k in [
            "uid",
            "resourceVersion",
            "generation",
            "creationTimestamp",
            "managedFields",
            "selfLink",
            "ownerReferences",
        ] {
            meta.remove(k);
        }
        if let Some(ann) = meta.get_mut("annotations").and_then(|a| a.as_object_mut()) {
            ann.remove("kubectl.kubernetes.io/last-applied-configuration");
            // drop now-empty annotations to avoid an empty-map diff
            let empty = ann.is_empty();
            if empty {
                meta.remove("annotations");
            }
        }
    }
    if let Some(obj) = v.as_object_mut() {
        obj.remove("status");
    }
}
