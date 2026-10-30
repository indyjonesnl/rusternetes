//! Unit tests moved from `src/commands/api_resources.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use anyhow::Result;
    pub use rusternetes_client::http::ApiClient;
    pub use serde::{Deserialize, Serialize};
    pub use tabled::{settings::Style, Table, Tabled};
}

mod tests {
    use super::__parent::*;
    use rusternetes_kubectl::commands::api_resources::*;

    #[test]
    fn test_api_resource_deserialization() {
        let json = r#"{
            "name": "pods",
            "singularName": "pod",
            "namespaced": true,
            "kind": "Pod",
            "shortNames": ["po"],
            "verbs": ["get", "list", "create", "delete"]
        }"#;
        let resource: ApiResource = serde_json::from_str(json).unwrap();
        assert_eq!(resource.name, "pods");
        assert_eq!(resource.kind, "Pod");
        assert!(resource.namespaced);
        assert_eq!(resource.short_names, vec!["po"]);
    }

    #[test]
    fn test_api_resource_row_construction() {
        let row = ApiResourceRow {
            name: "deployments".to_string(),
            short_names: "deploy".to_string(),
            api_version: "apps/v1".to_string(),
            namespaced: "true".to_string(),
            kind: "Deployment".to_string(),
        };
        assert_eq!(row.name, "deployments");
        assert_eq!(row.api_version, "apps/v1");
    }

    #[test]
    fn test_api_resource_list_deserialization() {
        let json = r#"{
            "groupVersion": "v1",
            "resources": [
                {"name": "pods", "namespaced": true, "kind": "Pod", "verbs": ["get"]},
                {"name": "pods/log", "namespaced": true, "kind": "Pod", "verbs": ["get"]}
            ]
        }"#;
        let list: ApiResourceList = serde_json::from_str(json).unwrap();
        assert_eq!(list.group_version, "v1");
        assert_eq!(list.resources.len(), 2);
        // Subresources (containing /) should be filtered in the execute function
        assert!(list.resources[1].name.contains('/'));
    }

    #[test]
    fn test_api_resource_subresource_filtering() {
        let resources = [
            ("pods", false),
            ("pods/log", true),
            ("pods/status", true),
            ("deployments", false),
            ("deployments/scale", true),
        ];
        let filtered: Vec<_> = resources
            .iter()
            .filter(|(name, _)| !name.contains('/'))
            .collect();
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].0, "pods");
        assert_eq!(filtered[1].0, "deployments");
    }
}
