//! Unit tests moved from `src/validation/structural/listtype.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::validation::field::{Error, Path};
    pub use rusternetes_common::validation::structural::Structural;
    pub use serde_json::{Map, Value};
    pub use std::collections::HashMap;
}

mod tests {
    //! Ports of `schema/listtype/validation_test.go` `TestValidateListSetsAndMaps`
    //! (release-1.35), asserting error type and field path as upstream's
    //! `validationMatch` does, plus the offending values `field.Duplicate`
    //! carries.

    use serde_json::{json, Value};

    use super::__parent::*;
    use rusternetes_common::resources::crd::JSONSchemaProps;
    use rusternetes_common::validation::field::ErrorType;
    use rusternetes_common::validation::structural::listtype::*;
    use rusternetes_common::validation::structural::new_structural;

    fn structural(v: Value) -> Structural {
        let props: JSONSchemaProps = serde_json::from_value(v).unwrap();
        new_structural(&props).expect("structural")
    }

    fn run(schema: Value, obj: Value) -> Vec<(ErrorType, String)> {
        let s = structural(schema);
        validate_list_sets_and_maps(&Path::new("root"), Some(&s), obj.as_object())
            .into_iter()
            .map(|e| (e.error_type, e.field))
            .collect()
    }

    fn dup(path: &str) -> (ErrorType, String) {
        (ErrorType::Duplicate, path.to_string())
    }

    fn set_of_array() -> Value {
        json!({"type": "object", "properties": {
            "array": {"type": "array", "x-kubernetes-list-type": "set",
                      "items": {"type": "string"}}}})
    }

    #[test]
    fn nil_schema_or_object_is_clean() {
        assert!(validate_list_sets_and_maps(&Path::default(), None, None).is_empty());
        let s = structural(json!({"type": "object"}));
        assert!(validate_list_sets_and_maps(&Path::default(), Some(&s), None).is_empty());
        assert!(
            validate_list_sets_and_maps(&Path::default(), None, json!({}).as_object()).is_empty()
        );
    }

    #[test]
    fn set_with_two_equal_items() {
        assert_eq!(
            run(set_of_array(), json!({"array": ["a", "a"]})),
            vec![dup("root.array[1]")]
        );
    }

    #[test]
    fn set_with_two_different_items_or_one_item() {
        assert!(run(set_of_array(), json!({"array": ["a", "b"]})).is_empty());
        assert!(run(set_of_array(), json!({"array": ["a"]})).is_empty());
    }

    #[test]
    fn set_with_multiple_duplicated_items_reports_the_second_appearance_once() {
        assert_eq!(
            run(
                set_of_array(),
                json!({"array": ["a", "a", "b", "c", "d", "c", "c"]})
            ),
            vec![dup("root.array[1]"), dup("root.array[5]")]
        );
    }

    #[test]
    fn normal_list_is_not_checked() {
        let schema = json!({"type": "object", "properties": {
            "array": {"type": "array", "items": {"type": "string"}}}});
        assert!(run(schema, json!({"array": ["a", "a"]})).is_empty());
    }

    #[test]
    fn set_under_additional_properties_uses_a_key_path() {
        let schema = json!({"type": "object", "additionalProperties": {
            "type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string"}}});
        assert_eq!(
            run(schema, json!({"k": ["a", "a"]})),
            vec![dup("root[k][1]")]
        );
    }

    #[test]
    fn set_under_items_and_nested_sets() {
        let schema = json!({"type": "object", "properties": {
            "outer": {"type": "array", "items": {
                "type": "array", "x-kubernetes-list-type": "set",
                "items": {"type": "string"}}}}});
        assert_eq!(
            run(schema, json!({"outer": [["a"], ["b", "b"]]})),
            vec![dup("root.outer[1][1]")]
        );
    }

    #[test]
    fn set_of_compound_items_compares_marshalled_json() {
        let schema = json!({"type": "object", "properties": {
            "array": {"type": "array", "x-kubernetes-list-type": "set",
                      "items": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}});
        // key order does not matter: json.Marshal sorts keys.
        assert_eq!(
            run(
                schema.clone(),
                json!({"array": [{"a": 1, "b": 2}, {"b": 2, "a": 1}, {"a": 1}]})
            ),
            vec![dup("root.array[1]")]
        );
        assert!(run(schema, json!({"array": [{"a": 1}, {"a": 2}]})).is_empty());
    }

    #[test]
    fn set_distinguishes_null_from_a_string() {
        assert_eq!(
            run(set_of_array(), json!({"array": [null, "null", null]})),
            vec![dup("root.array[2]")]
        );
    }

    fn map_schema(keys: &[&str]) -> Value {
        json!({"type": "object", "properties": {
            "array": {"type": "array", "x-kubernetes-list-type": "map",
                      "x-kubernetes-list-map-keys": keys,
                      "items": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}})
    }

    #[test]
    fn map_single_key_duplicate_carries_the_key_value() {
        let s = structural(map_schema(&["k"]));
        let obj = json!({"array": [{"k": "a", "v": 1}, {"k": "b"}, {"k": "a", "v": 2}]});
        let errs = validate_list_sets_and_maps(&Path::new("root"), Some(&s), obj.as_object());
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].error_type, ErrorType::Duplicate);
        assert_eq!(errs[0].field, "root.array[2]");
        assert_eq!(
            errs[0].bad_value,
            rusternetes_common::validation::field::BadValue::Json(json!({"k": "a"}))
        );
    }

    #[test]
    fn map_single_key_unspecified_keys_collide() {
        assert_eq!(
            run(
                map_schema(&["k"]),
                json!({"array": [{"v": 1}, null, {"k": "a"}, {"v": 2}]})
            ),
            vec![dup("root.array[1]")]
        );
    }

    #[test]
    fn map_multiple_keys() {
        let schema = map_schema(&["a", "b"]);
        assert_eq!(
            run(
                schema.clone(),
                json!({"array": [{"a": 1, "b": 2}, {"a": 1, "b": 3}, {"a": 1, "b": 2, "c": 9}]})
            ),
            vec![dup("root.array[2]")]
        );
        // a missing key field is part of the identity.
        assert!(run(schema, json!({"array": [{"a": 1}, {"a": 1, "b": 2}]})).is_empty());
    }

    #[test]
    fn map_rejects_non_object_elements() {
        let errs = run(map_schema(&["k"]), json!({"array": [{"k": "a"}, "x"]}));
        assert_eq!(
            errs,
            vec![(ErrorType::Invalid, "root.array[1]".to_string())]
        );
    }

    #[test]
    fn map_of_zero_or_one_element_is_clean() {
        assert!(run(map_schema(&["k"]), json!({"array": []})).is_empty());
        assert!(run(map_schema(&["k"]), json!({"array": [{"k": "a"}]})).is_empty());
    }
}
