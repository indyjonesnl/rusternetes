//! Unit tests moved from `src/validation/flowschema.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::flowcontrol::{
        FlowDistinguisherMethodType, FlowSchema, FlowSchemaCondition, FlowSchemaStatus,
        FlowSchemaSubject, NonResourcePolicyRule, PolicyRulesWithSubjects, ResourcePolicyRule,
        SubjectKind,
    };
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::flowcontrol_bootstrap::{
        mandatory_flow_schema, semantic_equal,
    };
    pub use rusternetes_common::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_common::validation::flowschema::*;

    fn cond(type_: &str) -> FlowSchemaCondition {
        FlowSchemaCondition {
            type_: type_.to_string(),
            status: "True".to_string(),
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    #[test]
    fn non_resource_url_path_accepts_valid() {
        let p = Path::new("p");
        assert!(validate_non_resource_url_path("/", &p).is_none());
        assert!(validate_non_resource_url_path("/healthz", &p).is_none());
        assert!(validate_non_resource_url_path("/api/*", &p).is_none());
    }

    #[test]
    fn non_resource_url_path_rejects_empty() {
        let e = validate_non_resource_url_path("", &Path::new("p")).expect("empty rejected");
        assert_eq!(e.error_type, ErrorType::Invalid);
        assert_eq!(e.detail, "must not be empty");
    }

    #[test]
    fn non_resource_url_path_rejects_no_leading_slash() {
        let e = validate_non_resource_url_path("healthz", &Path::new("p")).expect("rejected");
        assert_eq!(e.detail, "must start with slash");
    }

    #[test]
    fn non_resource_url_path_rejects_whitespace() {
        let e = validate_non_resource_url_path("/health z", &Path::new("p")).expect("rejected");
        assert_eq!(e.detail, "must not contain white-space");
    }

    #[test]
    fn non_resource_url_path_rejects_double_slash() {
        let e = validate_non_resource_url_path("/api//v1", &Path::new("p")).expect("rejected");
        assert_eq!(e.detail, "must not contain double slash");
    }

    #[test]
    fn non_resource_url_path_rejects_non_suffix_wildcard() {
        // wildcard not at suffix
        let e = validate_non_resource_url_path("/*/v1", &Path::new("p")).expect("rejected");
        assert_eq!(e.detail, "wildcard can only do suffix matching");
        // more than one wildcard
        let e2 = validate_non_resource_url_path("/a/*/*", &Path::new("p")).expect("rejected");
        assert_eq!(e2.detail, "wildcard can only do suffix matching");
    }

    #[test]
    fn status_condition_type_required() {
        let status = FlowSchemaStatus {
            conditions: Some(vec![cond("")]),
        };
        let errs = validate_flow_schema_status(&status, &Path::new("status"));
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, ErrorType::Required);
        assert_eq!(errs[0].field, "status.conditions[0].type");
    }

    #[test]
    fn status_condition_duplicate_type() {
        let status = FlowSchemaStatus {
            conditions: Some(vec![cond("Dangling"), cond("Dangling")]),
        };
        let errs = validate_flow_schema_status(&status, &Path::new("status"));
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, ErrorType::Duplicate);
        assert_eq!(errs[0].field, "status.conditions[1].type");
    }

    #[test]
    fn status_conditions_unique_ok() {
        let status = FlowSchemaStatus {
            conditions: Some(vec![cond("Dangling"), cond("Ready")]),
        };
        assert!(validate_flow_schema_status(&status, &Path::new("status")).is_empty());
    }
}
