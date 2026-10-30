//! Unit tests moved from `src/schema_validation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use regex::Regex;
    pub use rusternetes_common::error::Error;
    pub use rusternetes_common::resources::crd::JSONSchemaProps;
    pub use serde_json::Value;
    pub use std::collections::HashMap;
    pub use std::sync::{LazyLock, RwLock};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::schema_validation::*;
    use serde_json::json;
    use std::collections::HashMap;

    /// `structuraldefaulting.Default` (algorithm.go:45,:53,:61): a non-nullable
    /// null is replaced by the default, for properties, additionalProperties
    /// and array items alike; a nullable null is kept.
    #[test]
    fn test_apply_defaults_replaces_non_nullable_nulls() {
        let schema: JSONSchemaProps = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "a": {"type": "string", "default": "A"},
                "n": {"type": "string", "nullable": true, "default": "N"},
                "m": {"type": "object", "additionalProperties": {"type": "string", "default": "X"}},
                "l": {"type": "array", "items": {"type": "string", "default": "I"}}
            }
        }))
        .unwrap();
        let mut v = json!({"a": null, "n": null, "m": {"k": null}, "l": [null, "z"]});
        SchemaValidator::apply_defaults(&schema, &mut v);
        assert_eq!(
            v,
            json!({"a": "A", "n": null, "m": {"k": "X"}, "l": ["I", "z"]})
        );
    }

    #[test]
    fn test_nested_required_in_array_items_reports_rooted_path() {
        // CustomResourcePublishOpenAPI #239: a CR with spec.bars[0] missing the
        // required `name` must be rejected with the upstream-format path
        // `spec.bars[0].name: Required value` (the conformance test matches that
        // exact substring). Previously the path was rooted at the spec subtree
        // (`bars[0].name`), so the test saw an error but not the expected one.
        let item_schema: JSONSchemaProps = serde_json::from_value(json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": {"type": "string"}, "age": {"type": "string"} }
        }))
        .unwrap();
        let spec_schema: JSONSchemaProps = serde_json::from_value(json!({
            "type": "object",
            "properties": { "bars": { "type": "array", "items": item_schema } }
        }))
        .unwrap();

        let spec = json!({ "bars": [ { "age": "10" } ] });
        let err = SchemaValidator::validate_no_unknown_check_with_root(&spec_schema, &spec, "spec")
            .expect_err("missing required name must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("spec.bars[0].name: Required value"),
            "error must carry the rooted path, got: {msg}"
        );

        // Sanity: a valid CR passes.
        let ok = json!({ "bars": [ { "name": "bar0", "age": "10" } ] });
        assert!(
            SchemaValidator::validate_no_unknown_check_with_root(&spec_schema, &ok, "spec").is_ok()
        );

        // The kubectl-create path goes through validate_strict (fieldValidation
        // =Strict). It must produce the same rooted path.
        let strict_err = SchemaValidator::validate_strict(&spec_schema, &spec, "spec")
            .expect_err("validate_strict must reject missing required name");
        let smsg = strict_err.to_string();
        assert!(
            smsg.contains("spec.bars[0].name: Required value"),
            "validate_strict must root the path at spec, got: {smsg}"
        );
        assert!(
            SchemaValidator::validate_strict(&spec_schema, &ok, "spec").is_ok(),
            "valid CR must pass validate_strict"
        );
    }

    #[test]
    fn test_validate_type_object() {
        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            ..Default::default()
        };

        let valid = json!({"key": "value"});
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!("string");
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_required_fields() {
        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            required: Some(vec!["name".to_string()]),
            properties: Some(properties),
            ..Default::default()
        };

        let valid = json!({"name": "test"});
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!({});
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_string_length() {
        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            min_length: Some(3),
            max_length: Some(10),
            ..Default::default()
        };

        let valid = json!("hello");
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let too_short = json!("hi");
        assert!(SchemaValidator::validate(&schema, &too_short).is_err());

        let too_long = json!("hello world!");
        assert!(SchemaValidator::validate(&schema, &too_long).is_err());
    }

    #[test]
    fn test_validate_number_range() {
        let schema = JSONSchemaProps {
            type_: Some("number".to_string()),
            minimum: Some(0.0),
            maximum: Some(100.0),
            ..Default::default()
        };

        let valid = json!(50);
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let too_small = json!(-1);
        assert!(SchemaValidator::validate(&schema, &too_small).is_err());

        let too_large = json!(101);
        assert!(SchemaValidator::validate(&schema, &too_large).is_err());
    }

    #[test]
    fn test_validate_array_items() {
        let schema = JSONSchemaProps {
            type_: Some("array".to_string()),
            items: Some(Box::new(
                rusternetes_common::resources::crd::JSONSchemaPropsOrArray::Schema(
                    JSONSchemaProps {
                        type_: Some("string".to_string()),
                        ..Default::default()
                    },
                ),
            )),
            ..Default::default()
        };

        let valid = json!(["a", "b", "c"]);
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!(["a", 1, "c"]);
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_enum() {
        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            enum_: Some(vec![json!("red"), json!("green"), json!("blue")]),
            ..Default::default()
        };

        let valid = json!("red");
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!("yellow");
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    #[test]
    fn test_validate_pattern() {
        let schema = JSONSchemaProps {
            type_: Some("string".to_string()),
            pattern: Some("^[a-z]+$".to_string()),
            ..Default::default()
        };

        let valid = json!("hello");
        assert!(SchemaValidator::validate(&schema, &valid).is_ok());

        let invalid = json!("Hello123");
        assert!(SchemaValidator::validate(&schema, &invalid).is_err());
    }

    /// Test that unknown fields are collected (not returned on first error)
    /// matching K8s PruneWithOptions behavior.
    #[test]
    fn test_collects_all_unknown_fields() {
        let mut properties = HashMap::new();
        properties.insert(
            "known".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(properties),
            ..Default::default()
        };

        // Two unknown fields — both should appear in the error
        let value = json!({"known": "ok", "unknown1": "bad", "unknown2": "also bad"});
        let err = SchemaValidator::validate(&schema, &value).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(".unknown1") && msg.contains(".unknown2"),
            "Error should contain ALL unknown fields, got: {}",
            msg
        );
    }

    /// Test that embedded resource allows apiVersion/kind/metadata (case-sensitive)
    /// but rejects other unknown fields like 'apiversion' (lowercase).
    /// K8s ref: pruning/algorithm.go:51-55, 77
    #[test]
    fn test_embedded_resource_meta_fields() {
        let mut template_properties = HashMap::new();
        template_properties.insert(
            "name".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let mut properties = HashMap::new();
        properties.insert(
            "template".to_string(),
            JSONSchemaProps {
                type_: Some("object".to_string()),
                x_kubernetes_embedded_resource: Some(true),
                properties: Some(template_properties),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(properties),
            ..Default::default()
        };

        // apiVersion (camelCase) should be allowed on embedded resource
        let valid = json!({"template": {"apiVersion": "v1", "kind": "Pod", "name": "test"}});
        assert!(
            SchemaValidator::validate(&schema, &valid).is_ok(),
            "apiVersion/kind should be allowed on embedded resource"
        );

        // apiversion (lowercase) is NOT a meta field — should be rejected
        let invalid = json!({"template": {"apiversion": "v1", "name": "test"}});
        let err = SchemaValidator::validate(&schema, &invalid).unwrap_err();
        assert!(
            err.to_string().contains("apiversion"),
            "lowercase 'apiversion' should be rejected: {}",
            err
        );
    }

    /// Test enum validation inside array items (conformance test scenario).
    /// Schema: spec.bars[].feeling has enum: ["Great", "Down"]
    /// Invalid value "NonExistentValue" should be rejected.
    #[test]
    fn test_validate_enum_in_array_items() {
        use rusternetes_common::resources::crd::JSONSchemaPropsOrArray;

        // Build the schema matching the conformance test:
        // spec.bars[].feeling with enum: ["Great", "Down"]
        let mut feeling_props = HashMap::new();
        feeling_props.insert(
            "name".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );
        feeling_props.insert(
            "feeling".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                enum_: Some(vec![json!("Great"), json!("Down")]),
                ..Default::default()
            },
        );

        let item_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            required: Some(vec!["name".to_string()]),
            properties: Some(feeling_props),
            ..Default::default()
        };

        let mut spec_properties = HashMap::new();
        spec_properties.insert(
            "bars".to_string(),
            JSONSchemaProps {
                type_: Some("array".to_string()),
                items: Some(Box::new(JSONSchemaPropsOrArray::Schema(item_schema))),
                ..Default::default()
            },
        );

        let spec_schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(spec_properties),
            ..Default::default()
        };

        // Valid value: feeling = "Great"
        let valid_cr = json!({"bars": [{"name": "test-bar", "feeling": "Great"}]});
        assert!(
            SchemaValidator::validate_no_unknown_check(&spec_schema, &valid_cr).is_ok(),
            "Valid enum value 'Great' should be accepted"
        );

        // Invalid value: feeling = "NonExistentValue"
        let invalid_cr = json!({"bars": [{"name": "test-bar", "feeling": "NonExistentValue"}]});
        let result = SchemaValidator::validate_no_unknown_check(&spec_schema, &invalid_cr);
        assert!(
            result.is_err(),
            "Invalid enum value 'NonExistentValue' should be rejected"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unsupported value") && err_msg.contains("NonExistentValue"),
            "Error should mention unsupported value: {}",
            err_msg
        );
    }

    /// Test that x-kubernetes-preserve-unknown-fields allows arbitrary fields
    #[test]
    fn test_preserve_unknown_fields() {
        let mut properties = HashMap::new();
        properties.insert(
            "known".to_string(),
            JSONSchemaProps {
                type_: Some("string".to_string()),
                ..Default::default()
            },
        );

        let schema = JSONSchemaProps {
            type_: Some("object".to_string()),
            properties: Some(properties),
            x_kubernetes_preserve_unknown_fields: Some(true),
            ..Default::default()
        };

        // Unknown fields should be allowed when preserve-unknown-fields is true
        let value = json!({"known": "ok", "arbitrary": "allowed", "anything": 123});
        assert!(
            SchemaValidator::validate(&schema, &value).is_ok(),
            "preserve-unknown-fields should allow arbitrary fields"
        );
    }

    // ── Regex cache tests ────────────────────────────────────────────────

    /// First call should populate the cache; second call should hit it
    /// without recompiling. Verified by inspecting the static cache
    /// directly — the cached entry must be present after a single call.
    #[test]
    fn regex_cache_populates_on_first_call() {
        // Use a pattern unique to this test so other tests / runs don't
        // pollute the assertion.
        let pattern = "^cache-populate-test-[a-z0-9]+$";

        // Ensure the cache starts without this entry.
        REGEX_CACHE.write().unwrap().remove(pattern);
        assert!(
            !REGEX_CACHE.read().unwrap().contains_key(pattern),
            "precondition: cache must not contain our test pattern"
        );

        // First call compiles + inserts into cache.
        let re1 = compile_regex_cached(pattern).expect("compile");
        assert!(re1.is_match("cache-populate-test-abc123"));
        assert!(
            REGEX_CACHE.read().unwrap().contains_key(pattern),
            "cache must contain pattern after first call"
        );

        // Second call returns a regex that still matches — proving the cached
        // copy is functionally equivalent. We can't compare Regex by identity
        // (no PartialEq) so functional check is the strongest assertion.
        let re2 = compile_regex_cached(pattern).expect("compile (cached)");
        assert!(re2.is_match("cache-populate-test-xyz789"));
    }

    /// Bad regex still surfaces an error and does NOT get cached.
    #[test]
    fn regex_cache_does_not_store_invalid_pattern() {
        let bad = "[unclosed";
        REGEX_CACHE.write().unwrap().remove(bad);
        let result = compile_regex_cached(bad);
        assert!(result.is_err(), "invalid regex should error");
        assert!(
            !REGEX_CACHE.read().unwrap().contains_key(bad),
            "invalid regex must not be cached"
        );
    }

    // Note: the `regex_cache_speedup` perf microbenchmark that used to live
    // here was moved to `crates/common/benches/regex_cache.rs` (criterion).
    // Run with: `cargo bench -p rusternetes-common --bench regex_cache`.
}
