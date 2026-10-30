//! Unit tests moved from `src/validation/prioritylevelconfiguration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::flowcontrol::{
        ExemptPriorityLevelConfiguration, LimitResponse, LimitResponseType,
        LimitedPriorityLevelConfiguration, PriorityLevelConfiguration,
        PriorityLevelConfigurationCondition, PriorityLevelConfigurationStatus, PriorityLevelType,
        QueuingConfiguration,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::flowcontrol_bootstrap::{
        mandatory_priority_level_configuration, semantic_equal,
    };
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_common::validation::prioritylevelconfiguration::*;

    fn cond(type_: &str) -> PriorityLevelConfigurationCondition {
        PriorityLevelConfigurationCondition {
            type_: type_.to_string(),
            status: "True".to_string(),
            last_transition_time: None,
            reason: None,
            message: None,
        }
    }

    #[test]
    fn status_condition_type_required() {
        let status = PriorityLevelConfigurationStatus {
            conditions: Some(vec![cond("")]),
        };
        let errs = validate_status(&status, &Path::new("status"));
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, ErrorType::Required);
        assert_eq!(errs[0].field, "status.conditions[0].type");
    }

    #[test]
    fn status_condition_duplicate_type() {
        let status = PriorityLevelConfigurationStatus {
            conditions: Some(vec![cond("ConcurrencyShared"), cond("ConcurrencyShared")]),
        };
        let errs = validate_status(&status, &Path::new("status"));
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].error_type, ErrorType::Duplicate);
        assert_eq!(errs[0].field, "status.conditions[1].type");
    }

    #[test]
    fn status_conditions_unique_ok() {
        let status = PriorityLevelConfigurationStatus {
            conditions: Some(vec![cond("ConcurrencyShared"), cond("Ready")]),
        };
        assert!(validate_status(&status, &Path::new("status")).is_empty());
    }
}
