//! Unit tests moved from `src/validation/validating_admission_policy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::admission_webhook::MatchCondition;
    pub use rusternetes_common::resources::validating_admission_policy::{
        AuditAnnotation, ExpressionWarning, MatchPolicyType, MatchResources,
        NamedRuleWithOperations, ParamKind, ParamRef, ValidatingAdmissionPolicy,
        ValidatingAdmissionPolicyBinding, ValidatingAdmissionPolicyBindingSpec,
        ValidatingAdmissionPolicySpec, Validation, ValidationAction, Variable,
    };
    pub use rusternetes_common::resources::LabelSelector;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        is_dns1035_label, is_dns1123_subdomain, is_qualified_name, validate_label_selector,
        LabelSelectorValidationOptions,
    };
    pub use rusternetes_common::validation::webhookconfiguration::{
        to_metav1_selector, validate_rule_parts,
    };
    pub use std::collections::HashSet;
}

mod field_ref_tests {
    use super::__parent::*;
    use rusternetes_common::validation::validating_admission_policy::*;

    /// Upstream `TestValidateValidatingAdmissionPolicyStatus`
    /// (`validation_test.go:4150-4199`): "type checking bad json path" expects
    /// `invalid JSONPath: invalid array index foo`; the good path and the two
    /// `Required value` cases accept/reject as before.
    #[test]
    fn field_ref_bad_json_path_is_invalid() {
        let path = Path::new("fieldRef");
        let errs = validate_field_ref("spec[foo]", &path);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(
            errs[0].to_string(),
            "fieldRef: Invalid value: \"spec[foo]\": invalid JSONPath: invalid array index foo"
        );
    }

    #[test]
    fn field_ref_good_and_missing() {
        let path = Path::new("fieldRef");
        assert!(validate_field_ref("spec.validations[0].expression", &path).is_empty());
        assert_eq!(
            validate_field_ref("  ", &path)[0].to_string(),
            "fieldRef: Required value"
        );
    }

    /// The bad value is the TRIMMED fieldRef (`validation.go:1288`).
    #[test]
    fn field_ref_invalid_reports_trimmed_value() {
        let path = Path::new("fieldRef");
        let errs = validate_field_ref("  spec[foo]  ", &path);
        assert_eq!(
            errs[0].to_string(),
            "fieldRef: Invalid value: \"spec[foo]\": invalid JSONPath: invalid array index foo"
        );
    }
}
