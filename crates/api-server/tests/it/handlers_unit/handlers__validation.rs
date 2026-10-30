//! Unit tests moved from `src/handlers/validation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::Error;
    pub use std::collections::{HashMap, HashSet};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::validation::*;

    /// A CRD whose validation schema sets standard JSON-Schema fields to their
    /// zero value (`exclusiveMaximum: false`, `nullable: false`,
    /// `x-kubernetes-int-or-string: false`, `uniqueItems: false`) and uses an
    /// array property with `items` must pass strict field validation — those
    /// fields ARE modelled on JSONSchemaProps. They were falsely reported as
    /// `unknown field` because the diff-based detector re-serialises the parsed
    /// CRD (which drops `false`/empty values via `skip_serializing_if`) and
    /// flagged every dropped-but-known key.
    ///
    /// Reproduces the CustomResourcePublishOpenAPI / CustomResourceDefinition /
    /// FieldValidation conformance cluster, all of which failed at CRD creation
    /// with `strict decoding error: unknown field
    /// "spec.versions[0].schema.openAPIV3Schema.exclusiveMaximum", ...`.
    #[test]
    fn crd_with_zero_valued_schema_fields_passes_strict() {
        use rusternetes_common::resources::CustomResourceDefinition;

        let body = br#"{
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "foos.stable.example.com"},
            "spec": {
                "group": "stable.example.com",
                "names": {"plural": "foos", "singular": "foo", "kind": "Foo", "listKind": "FooList"},
                "scope": "Namespaced",
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "nullable": false,
                            "exclusiveMaximum": false,
                            "exclusiveMinimum": false,
                            "uniqueItems": false,
                            "x-kubernetes-int-or-string": false,
                            "x-kubernetes-embedded-resource": false,
                            "properties": {
                                "spec": {
                                    "type": "object",
                                    "nullable": false,
                                    "exclusiveMaximum": false,
                                    "properties": {
                                        "cronSpec": {"type": "string", "nullable": false, "exclusiveMaximum": false},
                                        "bars": {
                                            "description": "List of Bars and their specs.",
                                            "type": "array",
                                            "nullable": false,
                                            "items": {
                                                "type": "object",
                                                "nullable": false,
                                                "uniqueItems": false,
                                                "x-kubernetes-int-or-string": false,
                                                "required": ["name"],
                                                "properties": {
                                                    "name": {"type": "string", "nullable": false},
                                                    "feeling": {"type": "string", "enum": ["Great", "Down"]},
                                                    "bazs": {
                                                        "description": "List of Bazs.",
                                                        "type": "array",
                                                        "nullable": false,
                                                        "items": {"type": "string", "nullable": false}
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }]
            }
        }"#;

        let parsed: CustomResourceDefinition =
            serde_json::from_slice(body).expect("CRD must deserialize");
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(
            result.is_ok() && result.as_ref().unwrap().is_empty(),
            "CRD with zero-valued JSONSchemaProps fields must pass strict validation, got: {:?}",
            result
        );
    }

    // --- Duplicate JSON key detection tests ---

    #[test]
    fn test_duplicate_key_top_level() {
        let json = r#"{"name": "a", "name": "b"}"#;
        assert_eq!(find_duplicate_json_key(json), Some("name".to_string()));
    }

    #[test]
    fn test_no_duplicate_keys() {
        let json = r#"{"name": "a", "value": "b"}"#;
        assert_eq!(find_duplicate_json_key(json), None);
    }

    #[test]
    fn test_duplicate_key_nested() {
        // Duplicate "replicas" inside "spec" — should be detected with dotted path
        let json = r#"{"metadata": {"name": "test"}, "spec": {"replicas": 1, "replicas": 2}}"#;
        assert_eq!(
            find_duplicate_json_key(json),
            Some("spec.replicas".to_string())
        );
    }

    #[test]
    fn test_duplicate_key_deeply_nested() {
        let json = r#"{"a": {"b": {"c": 1, "c": 2}}}"#;
        assert_eq!(find_duplicate_json_key(json), Some("a.b.c".to_string()));
    }

    #[test]
    fn test_duplicate_key_in_array_element() {
        let json = r#"{"items": [{"x": 1, "x": 2}]}"#;
        assert_eq!(
            find_duplicate_json_key(json),
            Some("items[0].x".to_string())
        );
    }

    #[test]
    fn test_no_duplicate_same_key_different_objects() {
        // "name" appears in both objects but each object has it once — no duplicate
        let json = r#"{"a": {"name": "x"}, "b": {"name": "y"}}"#;
        assert_eq!(find_duplicate_json_key(json), None);
    }

    #[test]
    fn test_empty_object() {
        assert_eq!(find_duplicate_json_key("{}"), None);
    }

    #[test]
    fn test_non_object() {
        assert_eq!(find_duplicate_json_key("[]"), None);
        assert_eq!(find_duplicate_json_key("42"), None);
    }

    // --- Strict field validation tests ---

    #[test]
    fn test_strict_validation_no_unknown_fields() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
            value: i32,
        }

        let body = br#"{"name": "test", "value": 42}"#;
        let parsed = Simple {
            name: "test".to_string(),
            value: 42,
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
        assert!(warnings.is_empty(), "no warnings on clean body");
    }

    #[test]
    fn test_strict_validation_unknown_field() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("unknown field"),
            "Expected 'unknown field' in error: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_duplicate_field() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "a", "name": "b"}"#;
        let parsed = Simple {
            name: "b".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("duplicate field"),
            "Expected 'duplicate field' in error: {}",
            err_msg
        );
        assert!(
            err_msg.contains("name"),
            "Expected field name in error: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_default_is_warn() {
        // Upstream fieldValidation("") returns Warn
        // (staging/src/k8s.io/apiserver/pkg/endpoints/handlers/rest.go:409-413):
        // a missing ?fieldValidation= must not reject, only warn. (kubectl
        // sends Strict itself; that is a client default, not a server one.)
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let params = HashMap::new(); // no fieldValidation param

        let warnings = validate_strict_fields(&params, body, &parsed)
            .expect("default mode must not reject unknown fields");
        assert_eq!(warnings.len(), 1, "expected one warning: {:?}", warnings);
        assert!(
            warnings[0].contains("unknown field"),
            "warning should name the unknown field: {:?}",
            warnings
        );
    }

    #[test]
    fn test_strict_validation_ignore_mode_allows_unknown() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Ignore".to_string());

        let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
        assert!(warnings.is_empty(), "Ignore mode must not surface warnings");
    }

    #[test]
    fn test_strict_validation_warn_mode_returns_warnings() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "test", "extra": "field"}"#;
        let parsed = Simple {
            name: "test".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Warn".to_string());

        // Warn mode must surface unknown fields as warning strings (no error).
        let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("unknown field") && warnings[0].contains("extra"),
            "warning must identify the unknown field: {:?}",
            warnings
        );
    }

    /// Upstream `addStrictDecodingWarnings` (rest.go:439-446) turns every
    /// strict decoding violation, duplicates included, into a warning; see
    /// field_validation_test.go "post-warn-validation" / "post-no-validation".
    #[test]
    fn test_warn_mode_surfaces_duplicate_fields() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }
        let body = br#"{"name": "a", "name": "b", "extra": 1}"#;
        let parsed = Simple { name: "b".into() };
        for directive in [Some("Warn"), None] {
            let mut params = HashMap::new();
            if let Some(d) = directive {
                params.insert("fieldValidation".to_string(), d.to_string());
            }
            let warnings = validate_strict_fields(&params, body, &parsed).unwrap();
            assert!(
                warnings.contains(&r#"duplicate field "name""#.to_string()),
                "{directive:?}: {warnings:?}"
            );
            assert!(warnings.contains(&r#"unknown field "extra""#.to_string()));
        }
    }

    #[test]
    fn test_strict_validation_nested_duplicate() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Outer {
            spec: Inner,
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Inner {
            replicas: i32,
        }

        let body = br#"{"spec": {"replicas": 1, "replicas": 2}}"#;
        let parsed = Outer {
            spec: Inner { replicas: 2 },
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("duplicate field"),
            "Expected 'duplicate field' in error: {}",
            err_msg
        );
        assert!(
            err_msg.contains("spec.replicas"),
            "Expected 'spec.replicas' dotted path in error: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_error_format_matches_k8s() {
        // K8s returns: strict decoding error: json: unknown field "fieldName"
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Simple {
            name: String,
        }

        let body = br#"{"name": "a", "name": "b"}"#;
        let parsed = Simple {
            name: "b".to_string(),
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains(r#"strict decoding error:"#)
                && err_msg.contains(r#"duplicate field "name""#),
            "Error format must match K8s duplicate field detection: {}",
            err_msg
        );
    }

    #[test]
    fn test_strict_validation_combined_unknown_and_duplicate() {
        // K8s returns both unknown and duplicate field errors in a single message
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Outer {
            spec: Inner,
        }
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Inner {
            replicas: i32,
        }

        // Body has unknown field "spec.unknownField" AND duplicate "spec.replicas"
        let body = br#"{"spec": {"unknownField": "foo", "replicas": 1, "replicas": 2}}"#;
        let parsed = Outer {
            spec: Inner { replicas: 2 },
        };
        let mut params = HashMap::new();
        params.insert("fieldValidation".to_string(), "Strict".to_string());

        let result = validate_strict_fields(&params, body, &parsed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        // Should contain both errors
        assert!(
            err_msg.contains(r#"unknown field "spec.unknownField""#),
            "Expected unknown field error: {}",
            err_msg
        );
        assert!(
            err_msg.contains(r#"duplicate field "spec.replicas""#),
            "Expected duplicate field error: {}",
            err_msg
        );
        // Should be combined in a single strict decoding error
        assert!(
            err_msg.contains("strict decoding error:"),
            "Expected strict decoding error prefix: {}",
            err_msg
        );
    }

    #[test]
    fn test_find_all_duplicate_json_keys_multiple() {
        // Test that we find ALL duplicate keys, not just the first
        let json = r#"{"a": 1, "a": 2, "b": {"c": 1, "c": 2}}"#;
        let dups = find_all_duplicate_json_keys(json);
        assert_eq!(dups.len(), 2, "Expected 2 duplicates, got: {:?}", dups);
        assert!(dups.contains(&"a".to_string()));
        assert!(dups.contains(&"b.c".to_string()));
    }

    #[test]
    fn test_find_all_duplicate_json_keys_dotted_paths() {
        let json = r#"{"spec": {"replicas": 1, "replicas": 2}}"#;
        let dups = find_all_duplicate_json_keys(json);
        assert_eq!(dups, vec!["spec.replicas".to_string()]);
    }

    #[test]
    fn test_format_warning_header_shape() {
        // Upstream emits `Warning: 299 - "..."` per RFC 7234.
        let header = format_warning_header(r#"unknown field "spec.bogus""#);
        assert!(
            header.starts_with("299 "),
            "header must start with warn-code 299: {}",
            header
        );
        assert!(
            header.contains("spec.bogus"),
            "header must mention the unknown field: {}",
            header
        );
        assert!(
            header.contains(r#"\"spec.bogus\""#),
            "field name should be quoted/escaped: {}",
            header
        );
    }

    #[test]
    fn test_field_validation_mode_from_query_defaults_to_warn() {
        let empty = HashMap::new();
        assert_eq!(
            FieldValidationMode::from_query(&empty),
            FieldValidationMode::Warn
        );

        let mut blank = HashMap::new();
        blank.insert("fieldValidation".to_string(), String::new());
        assert_eq!(
            FieldValidationMode::from_query(&blank),
            FieldValidationMode::Warn
        );

        let mut warn = HashMap::new();
        warn.insert("fieldValidation".to_string(), "Warn".to_string());
        assert_eq!(
            FieldValidationMode::from_query(&warn),
            FieldValidationMode::Warn
        );

        let mut ignore = HashMap::new();
        ignore.insert("fieldValidation".to_string(), "Ignore".to_string());
        assert_eq!(
            FieldValidationMode::from_query(&ignore),
            FieldValidationMode::Ignore
        );

        let mut strict = HashMap::new();
        strict.insert("fieldValidation".to_string(), "Strict".to_string());
        assert_eq!(
            FieldValidationMode::from_query(&strict),
            FieldValidationMode::Strict
        );
    }
}
