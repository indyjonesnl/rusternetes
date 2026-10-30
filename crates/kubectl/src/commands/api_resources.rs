use anyhow::Result;
use rusternetes_client::http::ApiClient;
use serde::{Deserialize, Serialize};
use tabled::{settings::Style, Table, Tabled};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGroupList {
    groups: Vec<ApiGroup>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGroup {
    name: String,
    preferred_version: GroupVersionForDiscovery,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GroupVersionForDiscovery {
    pub group_version: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResourceList {
    pub group_version: String,
    pub resources: Vec<ApiResource>,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ApiResource {
    pub name: String,
    pub namespaced: bool,
    pub kind: String,
    #[serde(default)]
    pub short_names: Vec<String>,
}

#[derive(Tabled, Serialize)]
pub struct ApiResourceRow {
    #[tabled(rename = "NAME")]
    pub name: String,
    #[tabled(rename = "SHORTNAMES")]
    pub short_names: String,
    #[tabled(rename = "APIVERSION")]
    pub api_version: String,
    #[tabled(rename = "NAMESPACED")]
    pub namespaced: String,
    #[tabled(rename = "KIND")]
    pub kind: String,
}

/// Display available API resources
pub async fn execute(
    client: &ApiClient,
    namespaced: Option<bool>,
    api_group: Option<&str>,
    no_headers: bool,
    output: Option<&str>,
) -> Result<()> {
    let mut resources = Vec::new();

    // Get core API resources (v1)
    let core_resources: ApiResourceList = client.get("/api/v1").await.map_err(|e| match e {
        rusternetes_client::http::GetError::NotFound => anyhow::anyhow!("Core API not found"),
        rusternetes_client::http::GetError::Other(e) => e,
    })?;

    for resource in core_resources.resources {
        // Skip subresources (contain /)
        if !resource.name.contains('/') {
            resources.push((resource, core_resources.group_version.clone()));
        }
    }

    // Get API groups
    let api_groups: ApiGroupList = client.get("/apis").await.map_err(|e| match e {
        rusternetes_client::http::GetError::NotFound => anyhow::anyhow!("API groups not found"),
        rusternetes_client::http::GetError::Other(e) => e,
    })?;

    for group in api_groups.groups {
        // Filter by API group if specified
        if let Some(filter_group) = api_group {
            if group.name != filter_group {
                continue;
            }
        }

        // Use preferred version
        let group_version = &group.preferred_version.group_version;
        let path = format!("/apis/{}", group_version);

        if let Ok(resource_list) = client.get::<ApiResourceList>(&path).await {
            for resource in resource_list.resources {
                // Skip subresources
                if !resource.name.contains('/') {
                    resources.push((resource, resource_list.group_version.clone()));
                }
            }
        }
    }

    // Filter by namespaced if specified
    if let Some(ns) = namespaced {
        resources.retain(|(r, _)| r.namespaced == ns);
    }

    // Sort by name
    resources.sort_by(|a, b| a.0.name.cmp(&b.0.name));

    // Build table rows
    let rows: Vec<ApiResourceRow> = resources
        .into_iter()
        .map(|(resource, api_version)| ApiResourceRow {
            name: resource.name,
            short_names: resource.short_names.join(","),
            api_version,
            namespaced: resource.namespaced.to_string(),
            kind: resource.kind,
        })
        .collect();

    if rows.is_empty() {
        println!("No resources found");
        return Ok(());
    }

    match output {
        Some("json") => {
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        Some("yaml") => {
            println!("{}", serde_yaml::to_string(&rows)?);
        }
        _ => {
            let mut table = Table::new(&rows);
            table.with(Style::blank());
            if no_headers {
                // Remove headers - tabled doesn't have a simple way to do this,
                // so we'll just print without table
                for row in rows {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        row.name, row.short_names, row.api_version, row.namespaced, row.kind
                    );
                }
            } else {
                println!("{}", table);
            }
        }
    }

    Ok(())
}
