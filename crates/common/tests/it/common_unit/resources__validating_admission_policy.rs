//! Unit tests moved from `src/resources/validating_admission_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::serde_helpers::empty_string_as_none;
    pub use rusternetes_common::resources::{LabelSelector, MatchCondition};
    pub use rusternetes_common::types::ObjectMeta;
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::validating_admission_policy::*;

    #[test]
    fn test_validating_admission_policy_creation() {
        let policy = ValidatingAdmissionPolicy::new("test-policy");
        assert_eq!(policy.metadata.name, "test-policy");
        assert_eq!(policy.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(policy.kind, "ValidatingAdmissionPolicy");
    }

    #[test]
    fn test_validating_admission_policy_binding_creation() {
        let binding = ValidatingAdmissionPolicyBinding::new("test-binding");
        assert_eq!(binding.metadata.name, "test-binding");
        assert_eq!(binding.api_version, "admissionregistration.k8s.io/v1");
        assert_eq!(binding.kind, "ValidatingAdmissionPolicyBinding");
    }

    #[test]
    fn test_validation_action_serialization() {
        let deny = serde_json::to_string(&ValidationAction::Deny).unwrap();
        assert_eq!(deny, r#""Deny""#);

        let warn = serde_json::to_string(&ValidationAction::Warn).unwrap();
        assert_eq!(warn, r#""Warn""#);

        let audit = serde_json::to_string(&ValidationAction::Audit).unwrap();
        assert_eq!(audit, r#""Audit""#);
    }

    #[test]
    fn test_validation_spec() {
        let validation = Validation {
            expression: "object.spec.replicas <= 5".to_string(),
            message: Some("Too many replicas".to_string()),
            message_expression: None,
            reason: Some(StatusReason::Invalid),
            validation_actions: Some(vec![ValidationAction::Deny]),
        };

        assert_eq!(validation.expression, "object.spec.replicas <= 5");
        assert_eq!(validation.message, Some("Too many replicas".to_string()));
    }

    #[test]
    fn test_param_kind() {
        let param_kind = ParamKind {
            api_version: Some("v1".to_string()),
            kind: "ConfigMap".to_string(),
        };

        let json = serde_json::to_string(&param_kind).unwrap();
        let deserialized: ParamKind = serde_json::from_str(&json).unwrap();
        assert_eq!(param_kind, deserialized);
    }

    #[test]
    fn test_match_resources() {
        let match_resources = MatchResources {
            namespace_selector: None,
            object_selector: None,
            resource_rules: Some(vec![NamedRuleWithOperations {
                resource_names: None,
                rule: RuleWithOperations {
                    operations: Some(vec![OperationType::Create, OperationType::Update]),
                    api_groups: Some(vec!["apps".to_string()]),
                    api_versions: Some(vec!["v1".to_string()]),
                    resources: Some(vec!["deployments".to_string()]),
                    scope: None,
                },
            }]),
            exclude_resource_rules: None,
            match_policy: Some(MatchPolicyType::Exact),
        };

        let json = serde_json::to_string(&match_resources).unwrap();
        let deserialized: MatchResources = serde_json::from_str(&json).unwrap();
        assert_eq!(match_resources, deserialized);
    }
}
