//! Unit tests moved from `src/handlers/openapi.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use axum::{
        body::Body,
        extract::State,
        http::{header, HeaderMap, StatusCode},
        response::Response,
    };
    pub use rusternetes_api_server::gnostic;
    pub use rusternetes_api_server::openapi::generate_openapi_spec;
    pub use rusternetes_api_server::state::ApiServerState;
    pub use rusternetes_storage::{build_prefix, Storage};
    pub use std::sync::Arc;
    pub use tracing::info;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::handlers::openapi::*;

    /// Refs #3057: admissionregistration.k8s.io/v1 kinds must be published in
    /// /openapi/v2 under their `io.k8s.api.admissionregistration.v1.<Kind>` key
    /// with the right group/version/kind extension.
    #[test]
    fn swagger_defines_admissionregistration_v1_kinds() {
        let spec = build_swagger_spec_for_crds(&[]);
        let defs = spec["definitions"].as_object().unwrap();
        for kind in [
            "MutatingWebhookConfiguration",
            "ValidatingWebhookConfiguration",
            "ValidatingAdmissionPolicy",
            "ValidatingAdmissionPolicyBinding",
        ] {
            let def = defs
                .get(&format!("io.k8s.api.admissionregistration.v1.{kind}"))
                .unwrap_or_else(|| panic!("{kind} missing from v2 definitions"));
            assert_eq!(
                def["x-kubernetes-group-version-kind"][0]["group"],
                "admissionregistration.k8s.io"
            );
            assert_eq!(def["x-kubernetes-group-version-kind"][0]["version"], "v1");
        }
    }

    #[test]
    fn test_strip_false_extensions_removes_defaults() {
        // Test v2 conversion behavior matching K8s:
        // - False booleans stripped (Go omitempty)
        // - Empty strings stripped (Go omitempty)
        // - x-kubernetes-* false values stripped, true values KEPT as vendor extensions
        // - When x-kubernetes-preserve-unknown-fields=true, items/properties cleared
        // - When nullable=true, type/items/properties cleared
        let mut schema = serde_json::json!({
            "description": "Foo",
            "type": "object",
            "$schema": "",
            "id": "",
            "format": "",
            "pattern": "",
            "title": "",
            "exclusiveMaximum": false,
            "exclusiveMinimum": false,
            "nullable": false,
            "uniqueItems": false,
            "x-kubernetes-embedded-resource": false,
            "x-kubernetes-int-or-string": false,
            "properties": {
                "spec": {
                    "description": "Spec",
                    "type": "object",
                    "$schema": "",
                    "id": "",
                    "title": "",
                    "format": "",
                    "nullable": false,
                    "uniqueItems": false,
                    "exclusiveMaximum": false,
                    "x-kubernetes-preserve-unknown-fields": false,
                    "x-kubernetes-embedded-resource": true,
                    "properties": {
                        "bars": {
                            "description": "List of bars",
                            "type": "array",
                            "$schema": "",
                            "nullable": false
                        }
                    }
                },
                "nested_preserve": {
                    "description": "Has preserve-unknown-fields",
                    "type": "object",
                    "x-kubernetes-preserve-unknown-fields": true,
                    "properties": {
                        "should_be_removed": {
                            "type": "string"
                        }
                    }
                }
            }
        });

        strip_false_extensions(&mut schema);

        let obj = schema.as_object().unwrap();
        assert!(obj.contains_key("description"));
        assert!(obj.contains_key("type"));
        assert!(obj.contains_key("properties"));

        // Removed: empty strings and false booleans
        assert!(!obj.contains_key("$schema"), "$schema should be removed");
        assert!(!obj.contains_key("id"), "id should be removed");
        assert!(!obj.contains_key("format"), "format should be removed");
        assert!(!obj.contains_key("pattern"), "pattern should be removed");
        assert!(!obj.contains_key("exclusiveMaximum"));
        assert!(!obj.contains_key("exclusiveMinimum"));
        assert!(!obj.contains_key("nullable"), "nullable should be removed");
        assert!(!obj.contains_key("title"), "title should be removed");
        assert!(!obj.contains_key("uniqueItems"));
        // false x-kubernetes-* stripped
        assert!(!obj.contains_key("x-kubernetes-embedded-resource"));
        assert!(!obj.contains_key("x-kubernetes-int-or-string"));

        // Nested spec: false x-kubernetes-* stripped, true KEPT
        let spec = obj["properties"]["spec"].as_object().unwrap();
        assert!(spec.contains_key("description"));
        assert!(spec.contains_key("properties"));
        assert!(!spec.contains_key("$schema"));
        assert!(!spec.contains_key("id"));
        assert!(!spec.contains_key("nullable"));
        assert!(
            !spec.contains_key("x-kubernetes-preserve-unknown-fields"),
            "false preserve-unknown-fields should be stripped"
        );
        // x-kubernetes-embedded-resource=true should be KEPT
        assert!(
            spec.contains_key("x-kubernetes-embedded-resource"),
            "true x-kubernetes-embedded-resource should be KEPT as vendor extension"
        );

        // Nested with preserve-unknown-fields=true: properties and type cleared
        let nested = obj["properties"]["nested_preserve"].as_object().unwrap();
        assert!(nested.contains_key("description"), "description kept");
        assert!(
            nested.contains_key("x-kubernetes-preserve-unknown-fields"),
            "true preserve-unknown-fields KEPT as vendor extension"
        );
        assert!(
            !nested.contains_key("properties"),
            "properties cleared when preserve-unknown-fields=true"
        );
        assert!(
            !nested.contains_key("type"),
            "type=object cleared when preserve-unknown-fields=true"
        );

        // 3 levels deep: spec.properties.bars
        let bars = spec
            .get("properties")
            .unwrap()
            .get("bars")
            .unwrap()
            .as_object()
            .unwrap();
        assert!(bars.contains_key("description"), "deep description kept");
        assert!(!bars.contains_key("$schema"), "deep $schema removed");
        assert!(
            !bars.contains_key("nullable"),
            "nested nullable should be removed"
        );
    }

    #[test]
    fn test_wrap_in_k8s_protobuf_uses_correct_field_numbers() {
        let data = b"{\"test\": true}";
        let wrapped = wrap_in_k8s_protobuf("ignored-content-type", data);

        // Verify magic prefix
        assert_eq!(&wrapped[0..4], b"k8s\0");

        // After magic, first byte should be field 2 tag (0x12)
        // field 2, wire type 2 = (2 << 3) | 2 = 0x12
        assert_eq!(
            wrapped[4], 0x12,
            "first field tag should be 0x12 (field 2, raw bytes)"
        );

        // Parse past the varint length to find the raw data
        let mut pos = 5;
        let mut raw_len: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = wrapped[pos];
            raw_len |= ((byte & 0x7f) as u64) << shift;
            pos += 1;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        // Verify raw data matches input
        assert_eq!(raw_len as usize, data.len());
        assert_eq!(&wrapped[pos..pos + data.len()], data);

        // After raw data, next byte should be field 4 tag (0x22)
        let after_raw = pos + data.len();
        assert_eq!(
            wrapped[after_raw], 0x22,
            "second field tag should be 0x22 (field 4, contentType)"
        );

        // Verify contentType value is "application/json"
        let ct_len_pos = after_raw + 1;
        let ct_len = wrapped[ct_len_pos] as usize;
        let ct_start = ct_len_pos + 1;
        let ct_bytes = &wrapped[ct_start..ct_start + ct_len];
        assert_eq!(ct_bytes, b"application/json");
    }

    #[test]
    fn test_openapi_spec_has_definitions_key() {
        // Even with no CRDs, the swagger spec JSON should have a definitions key
        let spec = serde_json::json!({
            "swagger": "2.0",
            "info": {
                "title": "Rusternetes Kubernetes API",
                "version": "v1.35.0"
            },
            "paths": {},
            "definitions": {}
        });
        let val: serde_json::Value = spec;
        assert!(
            val.get("definitions").is_some(),
            "spec must include definitions"
        );
        assert!(val.get("paths").is_some(), "spec must include paths");
        assert!(
            val["definitions"].is_object(),
            "definitions must be an object"
        );
    }

    /// Test that the CRD schema processing (strip_false_extensions + add GVK +
    /// add standard properties) preserves the schema correctly.
    /// Simulates the Go conformance test's dropDefaults to verify roundtrip.
    #[test]
    fn test_crd_schema_roundtrip_matches_expected() {
        // Schema matching Go's JSONSchemaProps.MarshalJSON for the schemaFoo YAML
        let original_schema = serde_json::json!({
            "description": "Foo CRD for Testing",
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "description": "Specification of Foo",
                    "properties": {
                        "bars": {
                            "description": "List of Bars and their specs.",
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["name"],
                                "properties": {
                                    "name": {"description": "Name of Bar.", "type": "string"},
                                    "age": {"description": "Age of Bar.", "type": "string"},
                                    "feeling": {
                                        "description": "Whether Bar is feeling great.",
                                        "type": "string",
                                        "enum": ["Great", "Down"]
                                    },
                                    "bazs": {
                                        "description": "List of Bazs.",
                                        "items": {"type": "string"},
                                        "type": "array"
                                    }
                                }
                            }
                        }
                    }
                },
                "status": {
                    "description": "Status of Foo",
                    "type": "object",
                    "properties": {
                        "bars": {
                            "description": "List of Bars and their statuses.",
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"description": "Name of Bar.", "type": "string"},
                                    "available": {"description": "Whether the Bar is installed.", "type": "boolean"},
                                    "quxType": {
                                        "description": "Indicates to external qux type.",
                                        "pattern": "in-tree|out-of-tree",
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let mut cleaned = original_schema.clone();
        strip_false_extensions(&mut cleaned);
        if let Some(obj) = cleaned.as_object_mut() {
            obj.insert(
                "x-kubernetes-group-version-kind".to_string(),
                serde_json::json!([{"group": "test.example.com", "kind": "Foo", "version": "v1"}]),
            );
        }
        add_standard_crd_properties(&mut cleaned);

        // Simulate Go test's dropDefaults
        if let Some(obj) = cleaned.as_object_mut() {
            if let Some(props) = obj.get_mut("properties").and_then(|p| p.as_object_mut()) {
                props.remove("metadata");
                props.remove("apiVersion");
                props.remove("kind");
            }
            obj.remove("x-kubernetes-group-version-kind");
        }

        assert_eq!(
            cleaned,
            original_schema,
            "Schema should match after roundtrip.\nExpected:\n{}\n\nActual:\n{}",
            serde_json::to_string_pretty(&original_schema).unwrap(),
            serde_json::to_string_pretty(&cleaned).unwrap()
        );
    }

    /// K8s CRD schemas reference io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
    /// via $ref. kubectl validates CRs against the OpenAPI spec before sending
    /// them to the API server. If ObjectMeta is missing from definitions,
    /// kubectl fails with "unknown model in reference".
    /// K8s ref: staging/src/k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder/builder.go
    #[test]
    fn test_strip_empty_objects_and_arrays() {
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false,
            "default": null,
            "example": null,
            "required": [],
            "enum": [],
            "x-kubernetes-list-type": "",
            "x-kubernetes-map-type": "",
            "x-kubernetes-list-map-keys": [],
            "definitions": {},
            "allOf": [],
        });
        strip_false_extensions(&mut schema);

        // All empty/null/false values should be stripped (Go omitempty)
        let obj = schema.as_object().unwrap();
        assert!(
            !obj.contains_key("properties"),
            "empty properties should be stripped"
        );
        assert!(
            !obj.contains_key("additionalProperties"),
            "false additionalProperties should be stripped"
        );
        assert!(
            !obj.contains_key("default"),
            "null default should be stripped"
        );
        assert!(
            !obj.contains_key("example"),
            "null example should be stripped"
        );
        assert!(
            !obj.contains_key("required"),
            "empty required should be stripped"
        );
        assert!(!obj.contains_key("enum"), "empty enum should be stripped");
        assert!(
            !obj.contains_key("x-kubernetes-list-type"),
            "empty x-kubernetes-list-type should be stripped"
        );
        assert!(
            !obj.contains_key("x-kubernetes-map-type"),
            "empty x-kubernetes-map-type should be stripped"
        );
        assert!(
            !obj.contains_key("x-kubernetes-list-map-keys"),
            "empty x-kubernetes-list-map-keys should be stripped"
        );
        assert!(
            !obj.contains_key("definitions"),
            "empty definitions should be stripped"
        );
        assert!(!obj.contains_key("allOf"), "empty allOf should be stripped");
        // type: "object" should be kept (non-empty)
        assert!(obj.contains_key("type"), "non-empty type should be kept");
    }

    // -- CRD OpenAPI publish-on-update tests ------------------------------
    //
    // These cover upstream Kubernetes v1.35 conformance expectations:
    //   [Conformance] CustomResourceDefinition resources publish openAPI ...
    //   - works for CRD with validation schema
    //   - works for CRD without validation schema
    //   - removes definition from spec when one version gets changed to not be served
    //   - updates the published spec when one version gets renamed
    //   - works for multiple CRDs of same group but different versions
    //
    // We exercise `build_swagger_spec_for_crds` directly (the helper that
    // the live /openapi/v2 handler delegates to). That guarantees the spec
    // reflects the latest CRD set whenever it's regenerated, which the
    // production handler does on every request.

    fn crd_with_schema(
        name: &str,
        group: &str,
        plural: &str,
        kind: &str,
        versions: Vec<(&str, bool, Option<serde_json::Value>)>,
    ) -> serde_json::Value {
        let versions_json: Vec<serde_json::Value> = versions
            .into_iter()
            .map(|(ver, served, schema)| {
                let mut v = serde_json::json!({
                    "name": ver,
                    "served": served,
                    "storage": false,
                });
                if let Some(s) = schema {
                    v["schema"] = serde_json::json!({ "openAPIV3Schema": s });
                }
                v
            })
            .collect();
        serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": { "name": name },
            "spec": {
                "group": group,
                "names": { "plural": plural, "kind": kind },
                "scope": "Namespaced",
                "versions": versions_json,
            }
        })
    }

    fn def_key(group: &str, ver: &str, kind: &str) -> String {
        let group_parts: Vec<&str> = group.rsplitn(10, '.').collect();
        format!("{}.{}.{}", group_parts.join("."), ver, kind)
    }

    #[test]
    fn test_publish_crd_with_validation_schema() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "spec": {
                    "type": "object",
                    "properties": {
                        "bars": {
                            "type": "array",
                            "items": { "type": "object" }
                        }
                    }
                }
            }
        });
        let crd = crd_with_schema(
            "foos.test.example.com",
            "test.example.com",
            "foos",
            "Foo",
            vec![("v1", true, Some(schema.clone()))],
        );

        let spec = build_swagger_spec_for_crds(&[crd]);
        let key = def_key("test.example.com", "v1", "Foo");
        let def = spec
            .pointer(&format!("/definitions/{}", key))
            .expect("CRD definition must be published");

        // GVK extension must be present with the right values.
        let gvk = def
            .get("x-kubernetes-group-version-kind")
            .and_then(|v| v.as_array())
            .expect("GVK extension must be an array");
        assert_eq!(gvk.len(), 1);
        assert_eq!(gvk[0]["group"], "test.example.com");
        assert_eq!(gvk[0]["kind"], "Foo");
        assert_eq!(gvk[0]["version"], "v1");

        // Standard K8s metadata/apiVersion/kind properties must be auto-added.
        let props = def
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("properties must exist");
        assert!(props.contains_key("metadata"));
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
        // The user-supplied spec property must still be present.
        assert!(props.contains_key("spec"));
    }

    #[test]
    fn test_publish_crd_without_validation_schema() {
        // Upstream conformance: a CRD with no openAPIV3Schema must still get
        // a definition stub so kubectl explain & discovery clients see the
        // GVK. We emit {type: object} + GVK + the standard properties.
        let crd = crd_with_schema(
            "bars.test.example.com",
            "test.example.com",
            "bars",
            "Bar",
            vec![("v1", true, None)],
        );

        let spec = build_swagger_spec_for_crds(&[crd]);
        let key = def_key("test.example.com", "v1", "Bar");
        let def = spec
            .pointer(&format!("/definitions/{}", key))
            .expect("a definition must be published even without a schema");

        assert_eq!(def["type"], "object");
        let gvk = def
            .get("x-kubernetes-group-version-kind")
            .and_then(|v| v.as_array())
            .unwrap();
        assert_eq!(gvk[0]["kind"], "Bar");
        let props = def["properties"].as_object().unwrap();
        assert!(props.contains_key("metadata"));
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
    }

    #[test]
    fn test_published_spec_refreshes_on_crd_update() {
        // First publish with schema A — then with schema B. The spec must
        // reflect the latest schema. This is the core of the upstream
        // "[Conformance] updates an existing CustomResourceDefinition's
        // published OpenAPI schema" check.
        let schema_a = serde_json::json!({
            "type": "object",
            "properties": { "color": { "type": "string" } }
        });
        let schema_b = serde_json::json!({
            "type": "object",
            "properties": { "size": { "type": "integer" } }
        });

        let crd_v1 = crd_with_schema(
            "widgets.test.example.com",
            "test.example.com",
            "widgets",
            "Widget",
            vec![("v1", true, Some(schema_a))],
        );
        let spec1 = build_swagger_spec_for_crds(&[crd_v1]);
        let key = def_key("test.example.com", "v1", "Widget");
        let props1 = spec1
            .pointer(&format!("/definitions/{}/properties", key))
            .and_then(|v| v.as_object())
            .unwrap();
        assert!(props1.contains_key("color"));
        assert!(!props1.contains_key("size"));

        let crd_v2 = crd_with_schema(
            "widgets.test.example.com",
            "test.example.com",
            "widgets",
            "Widget",
            vec![("v1", true, Some(schema_b))],
        );
        let spec2 = build_swagger_spec_for_crds(&[crd_v2]);
        let props2 = spec2
            .pointer(&format!("/definitions/{}/properties", key))
            .and_then(|v| v.as_object())
            .unwrap();
        assert!(
            props2.contains_key("size"),
            "updated schema must publish new property"
        );
        assert!(
            !props2.contains_key("color"),
            "stale schema property must be dropped after update"
        );
    }

    #[test]
    fn test_removes_definition_when_version_no_longer_served() {
        // Upstream: "removes definition from spec when one version gets
        // changed to not be served". Toggling served=false must drop the
        // corresponding definition from the published spec.
        let schema = serde_json::json!({"type": "object"});
        let initial = crd_with_schema(
            "zwidgets.test.example.com",
            "test.example.com",
            "zwidgets",
            "Zwidget",
            vec![
                ("v1beta1", true, Some(schema.clone())),
                ("v1", true, Some(schema.clone())),
            ],
        );
        let spec_initial = build_swagger_spec_for_crds(&[initial]);
        let beta_key = def_key("test.example.com", "v1beta1", "Zwidget");
        let v1_key = def_key("test.example.com", "v1", "Zwidget");
        assert!(spec_initial
            .pointer(&format!("/definitions/{}", beta_key))
            .is_some());
        assert!(spec_initial
            .pointer(&format!("/definitions/{}", v1_key))
            .is_some());

        // Now v1beta1 is no longer served.
        let updated = crd_with_schema(
            "zwidgets.test.example.com",
            "test.example.com",
            "zwidgets",
            "Zwidget",
            vec![
                ("v1beta1", false, Some(schema.clone())),
                ("v1", true, Some(schema)),
            ],
        );
        let spec_updated = build_swagger_spec_for_crds(&[updated]);
        assert!(
            spec_updated
                .pointer(&format!("/definitions/{}", beta_key))
                .is_none(),
            "unserved version must be removed from the published spec"
        );
        assert!(
            spec_updated
                .pointer(&format!("/definitions/{}", v1_key))
                .is_some(),
            "served version must remain in the published spec"
        );
    }

    #[test]
    fn test_updates_published_spec_when_version_gets_renamed() {
        // Upstream: "updates the published spec when one version gets
        // renamed". Replacing v1beta1 with v2 must drop the old definition
        // key and add the new one.
        let schema = serde_json::json!({"type": "object"});
        let before = crd_with_schema(
            "gadgets.test.example.com",
            "test.example.com",
            "gadgets",
            "Gadget",
            vec![("v1beta1", true, Some(schema.clone()))],
        );
        let key_before = def_key("test.example.com", "v1beta1", "Gadget");
        let spec_before = build_swagger_spec_for_crds(&[before]);
        assert!(spec_before
            .pointer(&format!("/definitions/{}", key_before))
            .is_some());

        let after = crd_with_schema(
            "gadgets.test.example.com",
            "test.example.com",
            "gadgets",
            "Gadget",
            vec![("v2", true, Some(schema))],
        );
        let key_after = def_key("test.example.com", "v2", "Gadget");
        let spec_after = build_swagger_spec_for_crds(&[after]);
        assert!(
            spec_after
                .pointer(&format!("/definitions/{}", key_after))
                .is_some(),
            "renamed version must appear in the spec"
        );
        assert!(
            spec_after
                .pointer(&format!("/definitions/{}", key_before))
                .is_none(),
            "stale version key must be removed when CRD is renamed"
        );
    }

    #[test]
    fn test_deletion_removes_published_definition() {
        // Deleting a CRD must drop its definitions from the spec.
        let crd = crd_with_schema(
            "doomeds.test.example.com",
            "test.example.com",
            "doomeds",
            "Doomed",
            vec![("v1", true, Some(serde_json::json!({"type": "object"})))],
        );
        let key = def_key("test.example.com", "v1", "Doomed");
        let spec_with = build_swagger_spec_for_crds(&[crd]);
        assert!(spec_with
            .pointer(&format!("/definitions/{}", key))
            .is_some());

        // Simulate post-deletion (no CRDs).
        let spec_without: serde_json::Value = build_swagger_spec_for_crds(&[]);
        assert!(
            spec_without
                .pointer(&format!("/definitions/{}", key))
                .is_none(),
            "deleted CRD must not leak into the published spec"
        );
        // Baseline definitions are still present.
        assert!(spec_without
            .pointer("/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta")
            .is_some());
    }

    #[test]
    fn test_publishes_multiple_crds_same_group_different_versions() {
        // Upstream: "works for multiple CRDs of same group but different
        // versions". Each served version yields its own definition keyed by
        // group/version/kind.
        let schema = serde_json::json!({"type": "object"});
        let crd_one = crd_with_schema(
            "alphas.example.com",
            "example.com",
            "alphas",
            "Alpha",
            vec![("v1", true, Some(schema.clone()))],
        );
        let crd_two = crd_with_schema(
            "betas.example.com",
            "example.com",
            "betas",
            "Beta",
            vec![("v2", true, Some(schema))],
        );
        let spec = build_swagger_spec_for_crds(&[crd_one, crd_two]);
        assert!(spec
            .pointer(&format!(
                "/definitions/{}",
                def_key("example.com", "v1", "Alpha")
            ))
            .is_some());
        assert!(spec
            .pointer(&format!(
                "/definitions/{}",
                def_key("example.com", "v2", "Beta")
            ))
            .is_some());
    }

    #[test]
    fn test_builtin_core_v1_definitions_are_published() {
        // Upstream conformance and kubectl discovery both expect canonical
        // built-in GVK keys (`io.k8s.api.core.v1.Pod`,
        // `io.k8s.api.apps.v1.Deployment`, ...) in /openapi/v2's
        // `definitions` map. Build a spec with no CRDs and verify the
        // baselines are present.
        let spec = build_swagger_spec_for_crds(&[]);
        let defs = spec
            .get("definitions")
            .and_then(|d| d.as_object())
            .expect("definitions object");

        let pod = defs
            .get("io.k8s.api.core.v1.Pod")
            .expect("io.k8s.api.core.v1.Pod must be published");
        // GVK extension carries the right kind so discovery clients can
        // resolve the resource to a schema.
        let gvk = pod
            .get("x-kubernetes-group-version-kind")
            .and_then(|v| v.as_array())
            .expect("GVK extension must be an array");
        assert_eq!(gvk.len(), 1);
        assert_eq!(gvk[0]["group"], "");
        assert_eq!(gvk[0]["version"], "v1");
        assert_eq!(gvk[0]["kind"], "Pod");

        // Spot-check that the standard wrapper properties are present and
        // metadata $refs the existing ObjectMeta definition (no broken refs).
        let props = pod["properties"].as_object().unwrap();
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
        assert!(props.contains_key("metadata"));
        assert_eq!(
            props["metadata"]["$ref"],
            "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
        );
        assert!(props.contains_key("spec"));
        assert!(props.contains_key("status"));

        // Sibling built-ins kubectl discovery cares about must also be
        // present under their canonical group keys.
        assert!(defs.contains_key("io.k8s.api.apps.v1.Deployment"));
        assert!(defs.contains_key("io.k8s.api.batch.v1.Job"));
        assert!(defs.contains_key("io.k8s.api.core.v1.Service"));

        // ConfigMap/Secret carry no spec/status — verify we don't suggest
        // fields the resource doesn't have.
        let cm = defs.get("io.k8s.api.core.v1.ConfigMap").unwrap();
        let cm_props = cm["properties"].as_object().unwrap();
        assert!(!cm_props.contains_key("spec"));
        assert!(!cm_props.contains_key("status"));
    }

    #[test]
    fn test_preserve_unknown_fields_collapses_definition() {
        // CRD-level preserveUnknownFields=true must collapse the schema to a
        // bare {type: object} (with GVK + standard properties), preventing
        // kubectl from rejecting unknown fields during client-side validation.
        let mut crd = crd_with_schema(
            "freeforms.example.com",
            "example.com",
            "freeforms",
            "FreeForm",
            vec![(
                "v1",
                true,
                Some(serde_json::json!({
                    "type": "object",
                    "properties": { "anything": { "type": "string" } }
                })),
            )],
        );
        crd["spec"]["preserveUnknownFields"] = serde_json::Value::Bool(true);

        let spec = build_swagger_spec_for_crds(&[crd]);
        let key = def_key("example.com", "v1", "FreeForm");
        let def = spec.pointer(&format!("/definitions/{}", key)).unwrap();
        // preserveUnknownFields collapses to bare object; the user "anything"
        // property must NOT appear.
        let props = def["properties"].as_object().unwrap();
        assert!(props.contains_key("metadata"));
        assert!(props.contains_key("apiVersion"));
        assert!(props.contains_key("kind"));
        assert!(
            !props.contains_key("anything"),
            "preserveUnknownFields must drop user-defined properties to bare object"
        );
    }
}
