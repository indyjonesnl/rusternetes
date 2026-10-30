//! Unit tests moved from `src/commands/api_versions.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use serde::Deserialize;
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::api_versions::*;

    #[test]
    fn test_api_group_list_deserialization() {
        let json = r#"{
            "groups": [
                {
                    "name": "apps",
                    "versions": [
                        {"groupVersion": "apps/v1"}
                    ]
                },
                {
                    "name": "batch",
                    "versions": [
                        {"groupVersion": "batch/v1"},
                        {"groupVersion": "batch/v1beta1"}
                    ]
                }
            ]
        }"#;
        let group_list: ApiGroupList = serde_json::from_str(json).unwrap();
        assert_eq!(group_list.groups.len(), 2);
        assert_eq!(group_list.groups[1].versions.len(), 2);
        assert_eq!(group_list.groups[1].versions[0].group_version, "batch/v1");
    }

    #[test]
    fn test_versions_sorting() {
        let mut versions = vec![
            "batch/v1".to_string(),
            "apps/v1".to_string(),
            "v1".to_string(),
        ];
        versions.sort();
        assert_eq!(versions, vec!["apps/v1", "batch/v1", "v1"]);
    }

    #[test]
    fn test_api_group_list_empty_groups() {
        let json = r#"{"groups": []}"#;
        let group_list: ApiGroupList = serde_json::from_str(json).unwrap();
        assert!(group_list.groups.is_empty());
    }
}
