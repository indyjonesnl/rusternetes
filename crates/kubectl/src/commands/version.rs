use anyhow::Result;
use rusternetes_client::http::ApiClient;
use serde::Deserialize;
use serde_json::json;

pub const CLIENT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerVersion {
    pub major: String,
    pub minor: String,
    pub git_version: String,
    #[serde(default)]
    pub git_commit: String,
    #[serde(default)]
    pub git_tree_state: String,
    #[serde(default)]
    pub build_date: String,
    #[serde(default)]
    pub go_version: String,
    #[serde(default)]
    pub compiler: String,
    #[serde(default)]
    pub platform: String,
}

/// Display kubectl and Kubernetes version information
pub async fn execute(client: &ApiClient, client_only: bool, output: Option<&str>) -> Result<()> {
    let client_version = json!({
        "major": "1",
        "minor": "35",
        "gitVersion": format!("v{}", CLIENT_VERSION),
        "gitCommit": "unknown",
        "gitTreeState": "clean",
        "buildDate": "unknown",
        "goVersion": "rust",
        "compiler": "rustc",
        "platform": std::env::consts::OS
    });

    if client_only {
        match output {
            Some("json") => {
                let version_info = json!({
                    "clientVersion": client_version
                });
                println!("{}", serde_json::to_string_pretty(&version_info)?);
            }
            Some("yaml") => {
                let version_info = json!({
                    "clientVersion": client_version
                });
                println!("{}", serde_yaml::to_string(&version_info)?);
            }
            _ => {
                println!("Client Version: v{}", CLIENT_VERSION);
                println!("Kustomize Version: v5.0.0");
            }
        }
    } else {
        // Get server version
        let server_version: ServerVersion = client.get("/version").await.map_err(|e| match e {
            rusternetes_client::http::GetError::NotFound => {
                anyhow::anyhow!("Server version endpoint not found")
            }
            rusternetes_client::http::GetError::Other(e) => e,
        })?;

        match output {
            Some("json") => {
                let version_info = json!({
                    "clientVersion": client_version,
                    "serverVersion": {
                        "major": server_version.major,
                        "minor": server_version.minor,
                        "gitVersion": server_version.git_version,
                        "gitCommit": server_version.git_commit,
                        "gitTreeState": server_version.git_tree_state,
                        "buildDate": server_version.build_date,
                        "goVersion": server_version.go_version,
                        "compiler": server_version.compiler,
                        "platform": server_version.platform,
                    }
                });
                println!("{}", serde_json::to_string_pretty(&version_info)?);
            }
            Some("yaml") => {
                let version_info = json!({
                    "clientVersion": client_version,
                    "serverVersion": {
                        "major": server_version.major,
                        "minor": server_version.minor,
                        "gitVersion": server_version.git_version,
                    }
                });
                println!("{}", serde_yaml::to_string(&version_info)?);
            }
            _ => {
                println!("Client Version: v{}", CLIENT_VERSION);
                println!("Kustomize Version: v5.0.0");
                println!("Server Version: {}", server_version.git_version);
            }
        }
    }

    Ok(())
}
