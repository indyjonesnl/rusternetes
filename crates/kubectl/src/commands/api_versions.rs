use anyhow::Result;
use rusternetes_client::http::ApiClient;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGroupList {
    pub groups: Vec<ApiGroup>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiGroup {
    pub versions: Vec<GroupVersionForDiscovery>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionForDiscovery {
    pub group_version: String,
}

/// Display supported API versions
pub async fn execute(client: &ApiClient) -> Result<()> {
    let mut versions = Vec::new();

    // Add core API version
    versions.push("v1".to_string());

    // Get API groups
    let api_groups: ApiGroupList = client.get("/apis").await.map_err(|e| match e {
        rusternetes_client::http::GetError::NotFound => anyhow::anyhow!("API groups not found"),
        rusternetes_client::http::GetError::Other(e) => e,
    })?;

    for group in api_groups.groups {
        for version in group.versions {
            versions.push(version.group_version);
        }
    }

    // Sort versions
    versions.sort();

    // Print each version
    for version in versions {
        println!("{}", version);
    }

    Ok(())
}
