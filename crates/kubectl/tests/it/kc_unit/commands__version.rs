//! Unit tests moved from `src/commands/version.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use serde::Deserialize;
    pub use serde_json::json;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::version::*;

    #[test]
    fn test_client_version_constant() {
        // CLIENT_VERSION should be a valid semver string from Cargo.toml
        assert!(!CLIENT_VERSION.is_empty());
    }

    #[test]
    fn test_client_version_json_structure() {
        let client_version = serde_json::json!({
            "major": "1",
            "minor": "35",
            "gitVersion": format!("v{}", CLIENT_VERSION),
            "goVersion": "rust",
            "compiler": "rustc",
            "platform": std::env::consts::OS
        });
        assert_eq!(client_version["major"], "1");
        assert_eq!(client_version["minor"], "35");
        assert!(client_version["gitVersion"]
            .as_str()
            .unwrap()
            .starts_with("v"));
        assert_eq!(client_version["goVersion"], "rust");
        assert_eq!(client_version["compiler"], "rustc");
    }

    #[test]
    fn test_server_version_deserialization() {
        let json = r#"{
            "major": "1",
            "minor": "29",
            "gitVersion": "v1.29.0",
            "gitCommit": "abc123",
            "gitTreeState": "clean",
            "buildDate": "2024-01-01",
            "goVersion": "go1.21",
            "compiler": "gc",
            "platform": "linux/amd64"
        }"#;
        let sv: ServerVersion = serde_json::from_str(json).unwrap();
        assert_eq!(sv.major, "1");
        assert_eq!(sv.minor, "29");
        assert_eq!(sv.git_version, "v1.29.0");
        assert_eq!(sv.platform, "linux/amd64");
    }

    #[test]
    fn test_server_version_deserialization_minimal() {
        // Only major, minor, gitVersion required; rest have defaults
        let json = r#"{
            "major": "1",
            "minor": "35",
            "gitVersion": "v1.35.0"
        }"#;
        let sv: ServerVersion = serde_json::from_str(json).unwrap();
        assert_eq!(sv.major, "1");
        assert_eq!(sv.minor, "35");
        assert_eq!(sv.git_version, "v1.35.0");
        assert_eq!(sv.git_commit, "");
        assert_eq!(sv.platform, "");
    }
}
