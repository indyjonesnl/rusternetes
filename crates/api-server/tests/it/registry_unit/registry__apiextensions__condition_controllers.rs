//! Unit tests moved from `src/registry/apiextensions/condition_controllers.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::apiextensions::controllers::{
        aggregate, condition, find_crd_condition, set_crd_condition,
    };
    pub use rusternetes_common::resources::{
        CustomResourceDefinition, CustomResourceDefinitionCondition,
    };
    pub use rusternetes_common::validation::crd::{
        api_approval_state, is_protected_community_group, ApiApprovalState,
        KUBE_API_APPROVED_ANNOTATION,
    };
    pub use rusternetes_common::validation::field::{Error, Path};
    pub use rusternetes_common::validation::structural::{new_structural, validate_structural};
    pub use std::collections::HashMap;
    pub use std::sync::Mutex;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::condition_controllers::*;
    use rusternetes_common::resources::CustomResourceDefinitionStatus;

    fn crd(group: &str, annotation: Option<&str>, preserve: bool) -> CustomResourceDefinition {
        let mut c: CustomResourceDefinition = serde_json::from_value(serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foo", "generation": 1},
            "spec": {"group": group, "scope": "Namespaced", "versions": [],
                     "names": {"plural": "foos", "kind": "Foo"}}
        }))
        .unwrap();
        c.spec.preserve_unknown_fields = Some(preserve);
        if let Some(a) = annotation {
            c.metadata.annotations =
                Some([(KUBE_API_APPROVED_ANNOTATION.to_string(), a.to_string())].into());
        }
        c
    }

    /// `TestCalculateCondition` (apiapproval_controller_test.go:26-103).
    #[test]
    fn api_approval_calculate_condition() {
        let cases: [(&str, &str, &str, &str, &str); 5] = [
            ("other.io", "", "", "", ""),
            (
                "sigs.k8s.io",
                "",
                "False",
                "MissingAnnotation",
                r#"protected groups must have approval annotation "api-approved.kubernetes.io", see https://github.com/kubernetes/enhancements/pull/1111"#,
            ),
            (
                "sigs.k8s.io",
                "bad value",
                "False",
                "InvalidAnnotation",
                r#"protected groups must have approval annotation "api-approved.kubernetes.io" with either a URL or a reason starting with "unapproved", see https://github.com/kubernetes/enhancements/pull/1111"#,
            ),
            (
                "sigs.k8s.io",
                "https://github.com/kubernetes/kubernetes/pull/79724",
                "True",
                "ApprovedAnnotation",
                "approved in https://github.com/kubernetes/kubernetes/pull/79724",
            ),
            (
                "sigs.k8s.io",
                "unapproved for reasons",
                "False",
                "UnapprovedAnnotation",
                r#"not approved: "unapproved for reasons""#,
            ),
        ];
        for (group, annotation, status, reason, message) in cases {
            let got = calculate_api_approval_condition(&crd(group, Some(annotation), false));
            if status.is_empty() {
                assert!(got.is_none(), "{group}: {got:?}");
                continue;
            }
            let got = got.unwrap_or_else(|| panic!("missing condition for {annotation:?}"));
            assert_eq!(got.type_, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT);
            assert_eq!(got.status, status, "{annotation:?}");
            assert_eq!(got.reason.as_deref(), Some(reason), "{annotation:?}");
            assert_eq!(got.message.as_deref(), Some(message), "{annotation:?}");
        }
    }

    /// `Test_calculateCondition` (nonstructuralschema_controller_test.go:28-66).
    #[test]
    fn non_structural_calculate_condition() {
        assert_eq!(
            calculate_non_structural_condition(&crd("a.io", None, false)),
            None
        );
        let got = calculate_non_structural_condition(&crd("a.io", None, true)).unwrap();
        assert_eq!(got.type_, NON_STRUCTURAL_SCHEMA);
        assert_eq!(got.status, "True");
        assert_eq!(got.reason.as_deref(), Some("Violations"));
        assert_eq!(
            got.message.as_deref(),
            Some("spec.preserveUnknownFields: Invalid value: true: must be false")
        );
    }

    fn crd_with_schema(schema: serde_json::Value) -> CustomResourceDefinition {
        let mut c = crd("a.io", None, false);
        c.spec.versions = serde_json::from_value(serde_json::json!([
            {"name": "v1", "served": true, "storage": true,
             "schema": {"openAPIV3Schema": schema}}
        ]))
        .unwrap();
        c
    }

    /// `ValidateStructural` feeds the `Violations` message
    /// (nonstructuralschema_controller.go:102-122).
    #[test]
    fn non_structural_schema_is_reported_per_version() {
        let structural = crd_with_schema(serde_json::json!({
            "type": "object",
            "properties": {"spec": {"type": "object"}}
        }));
        assert_eq!(calculate_non_structural_condition(&structural), None);

        let bad = crd_with_schema(serde_json::json!({
            "type": "object",
            "properties": {"a": {"type": "array"}, "b": {"description": "no type"}}
        }));
        let got = calculate_non_structural_condition(&bad).unwrap();
        assert_eq!(got.type_, NON_STRUCTURAL_SCHEMA);
        assert_eq!(got.status, "True");
        assert_eq!(got.reason.as_deref(), Some("Violations"));
        let p = "spec.versions[0].schema.openAPIV3Schema.properties";
        assert_eq!(
            got.message.as_deref(),
            Some(
                format!(
                    "[{p}[a].items: Required value: must be specified, \
                     {p}[b].type: Required value: must not be empty for specified object fields]"
                )
                .as_str()
            )
        );
    }

    /// Violations of the spec and of the schema are reported together (:96-122).
    #[test]
    fn non_structural_reports_preserve_unknown_fields_and_schema_together() {
        let mut c = crd_with_schema(serde_json::json!({"type": "string"}));
        c.spec.preserve_unknown_fields = Some(true);
        let got = calculate_non_structural_condition(&c).unwrap();
        let m = got.message.unwrap();
        assert!(m.contains("spec.preserveUnknownFields: Invalid value: true: must be false"));
        assert!(m.contains(
            "openAPIV3Schema.type: Invalid value: \"string\": must be object at the root"
        ));
    }

    /// `NewStructural` failing yields `StructuralError`, status Unknown, and
    /// drops the earlier errors (:111-116).
    #[test]
    fn non_structural_new_structural_error_is_structural_error() {
        let mut c = crd_with_schema(serde_json::json!({"type": "object", "$ref": "#/x"}));
        c.spec.preserve_unknown_fields = Some(true);
        let got = calculate_non_structural_condition(&c).unwrap();
        assert_eq!(got.status, "Unknown");
        assert_eq!(got.reason.as_deref(), Some("StructuralError"));
        assert_eq!(
            got.message.as_deref(),
            Some("failed to check validation schema for version v1: OpenAPIV3Schema '$ref' is not supported")
        );
    }

    #[test]
    fn non_structural_sync_sets_then_skips_same_generation_then_removes() {
        let c = ConditionControllers::default();
        let bad = crd("a.io", None, true);
        let written = c.sync_non_structural(&bad).expect("condition is written");
        assert!(find_crd_condition(&written, NON_STRUCTURAL_SCHEMA).is_some());
        // Same details already on the object: no write (:159-161).
        assert!(c.sync_non_structural(&written).is_none());

        // The memo skips the same generation even if the object differs (:148).
        c.record_non_structural(&written);
        let mut fixed = written.clone();
        fixed.spec.preserve_unknown_fields = Some(false);
        assert!(c.sync_non_structural(&fixed).is_none());

        // A newer generation recalculates, and the stale condition is removed (:165-166).
        fixed.metadata.generation = Some(2);
        let out = c.sync_non_structural(&fixed).expect("condition removed");
        assert!(find_crd_condition(&out, NON_STRUCTURAL_SCHEMA).is_none());

        // A deleted CRD forgets its generation (:278-280).
        c.forget("foo");
        let mut again = written.clone();
        again.metadata.generation = Some(1);
        again.spec.preserve_unknown_fields = Some(false);
        assert!(c.sync_non_structural(&again).is_some());
    }

    #[test]
    fn non_structural_sync_without_condition_or_violation_is_a_noop() {
        let c = ConditionControllers::default();
        let mut ok = crd("a.io", None, false);
        assert!(c.sync_non_structural(&ok).is_none());
        ok.status = Some(CustomResourceDefinitionStatus::default());
        assert!(c.sync_non_structural(&ok).is_none());
    }

    #[test]
    fn api_approval_sync_writes_once_and_never_removes() {
        let c = ConditionControllers::default();
        assert!(c.sync_api_approval(&crd("other.io", None, false)).is_none());

        let missing = crd("sigs.k8s.io", None, false);
        let written = c.sync_api_approval(&missing).expect("condition is written");
        let cond = find_crd_condition(&written, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT).unwrap();
        assert_eq!(cond.reason.as_deref(), Some("MissingAnnotation"));
        assert!(c.sync_api_approval(&written).is_none());

        // The memo skips an unchanged annotation (:144-146) ...
        c.record_api_approval(&written);
        assert!(c.sync_api_approval(&missing).is_none());
        // ... and a changed one rewrites the condition.
        let mut approved = written.clone();
        approved.metadata.annotations = Some(
            [(
                KUBE_API_APPROVED_ANNOTATION.to_string(),
                "https://github.com/kubernetes/kubernetes/pull/1".to_string(),
            )]
            .into(),
        );
        let out = c.sync_api_approval(&approved).expect("condition updated");
        let cond = find_crd_condition(&out, KUBERNETES_API_APPROVAL_POLICY_CONFORMANT).unwrap();
        assert_eq!(
            (cond.status.as_str(), cond.reason.as_deref()),
            ("True", Some("ApprovedAnnotation"))
        );
    }
}
