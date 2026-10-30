//! Unit tests moved from `src/validation/authorization.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::authorization::{
        FieldSelectorAttributes, FieldSelectorRequirement, LabelSelectorAttributes,
        LabelSelectorRequirement, ResourceAttributes,
    };
    pub use rusternetes_common::resources::{
        LocalSubjectAccessReview, SelfSubjectAccessReview, SelfSubjectAccessReviewSpec,
        SubjectAccessReview, SubjectAccessReviewSpec,
    };
    pub use rusternetes_common::types::ObjectMeta;
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{
        validate_label_selector_requirement, LabelSelectorValidationOptions,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::authorization::*;

    /// #2190: `metadata` renders in Go struct-field order
    /// (`ObjectMeta`: name, resourceVersion, generation, ..., labels), with
    /// map keys sorted, not as an alphabetically sorted object. Upstream:
    /// `field/errors.go:92-97` marshals the value with `json.Marshal`.
    #[test]
    fn metadata_must_be_empty_renders_in_declaration_order() {
        let meta = ObjectMeta {
            name: "n".into(),
            resource_version: Some("7".into()),
            generation: Some(2),
            labels: Some(
                [
                    ("b".to_string(), "1".to_string()),
                    ("a".to_string(), "2".to_string()),
                ]
                .into(),
            ),
            ..Default::default()
        };
        let e = metadata_must_be_empty(&meta, &meta, "must be empty").unwrap();
        assert_eq!(
            e.error_body(),
            r#"Invalid value: {"name":"n","resourceVersion":"7","generation":2,"labels":{"a":"2","b":"1"}}: must be empty"#
        );
    }

    /// #2375: the kubelet's webhook authorizer posts its SubjectAccessReview as
    /// protobuf (client-go's default for core clients). Go's generated marshaller
    /// writes every scalar `ObjectMeta` field, so the decoded metadata is
    /// `{"name":"","generation":0}`. Upstream's `DeepEqual(metav1.ObjectMeta{}, ..)`
    /// treats Go zero values as empty; `generation: Some(0)` must too, or every
    /// kubelet authz check answers 422 and the node never goes Ready.
    #[test]
    fn metadata_with_only_go_zero_scalars_is_empty() {
        // `generation` arrives through the serde fold (#2955), the strings
        // through `metadata_must_be_empty`.
        let meta = ObjectMeta {
            generate_name: Some(String::new()),
            namespace: Some(String::new()),
            resource_version: Some(String::new()),
            ..serde_json::from_value(serde_json::json!({"name": "", "generation": 0})).unwrap()
        };
        assert_eq!(meta.generation, None);
        assert!(metadata_must_be_empty(&meta, &meta, "must be empty").is_none());
        let review = SubjectAccessReview {
            metadata: meta,
            ..serde_json::from_value(serde_json::json!({
                "spec": {"user": "u", "resourceAttributes": {"verb": "get"}}
            }))
            .unwrap()
        };
        assert!(validate_subject_access_review(&review).is_empty());
    }
}
