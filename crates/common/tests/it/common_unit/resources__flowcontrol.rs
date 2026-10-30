//! Unit tests moved from `src/resources/flowcontrol.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::flowcontrol::*;

    /// FlowSchema nonResourceRules without nonResourceURLs must decode.
    /// Reproduces sig-api-machinery FlowControl 422 "missing field `nonResourceURLs`"
    /// from the 2026-05-31 conformance run (flowcontrol.go:379).
    /// Go: []string json:"nonResourceURLs" — absent → empty slice via #[serde(default)].
    #[test]
    fn test_non_resource_policy_rule_without_urls_decodes() {
        let json = r#"{"verbs": ["get"]}"#;
        let rule: NonResourcePolicyRule =
            serde_json::from_str(json).expect("NonResourcePolicyRule without urls must decode");
        assert_eq!(rule.verbs, vec!["get"]);
        assert!(rule.non_resource_urls.is_empty());
    }

    /// FlowSchema with missing required scalar fields must decode to zero values.
    #[test]
    fn test_flow_schema_spec_without_matching_precedence_decodes() {
        let json = r#"{
            "priorityLevelConfiguration": {"name": "exempt"},
            "rules": []
        }"#;
        let spec: FlowSchemaSpec = serde_json::from_str(json)
            .expect("FlowSchemaSpec without matchingPrecedence must decode");
        assert_eq!(spec.matching_precedence, 0);
    }

    /// ResourcePolicyRule without verbs/apiGroups/resources must decode to empty slices.
    #[test]
    fn test_resource_policy_rule_without_required_fields_decodes() {
        let json = r#"{}"#;
        let rule: ResourcePolicyRule = serde_json::from_str(json)
            .expect("ResourcePolicyRule without required fields must decode");
        assert!(rule.verbs.is_empty());
        assert!(rule.api_groups.is_empty());
        assert!(rule.resources.is_empty());
    }
}
