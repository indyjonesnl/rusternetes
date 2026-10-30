//! Unit tests moved from `src/openapi.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use indexmap::IndexMap;
    pub use openapiv3::{
        ArrayType, BooleanType, Info, IntegerType, MediaType, ObjectType, OpenAPI, Operation,
        Parameter, ParameterData, ParameterSchemaOrContent, PathItem, Paths, ReferenceOr,
        RequestBody, Response, Responses, Schema, SchemaData, SchemaKind, Server, StringType, Tag,
        Type,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::openapi::*;

    #[test]
    fn test_generate_openapi_spec() {
        let spec = generate_openapi_spec();
        assert_eq!(spec.openapi, "3.0.0");
        assert_eq!(spec.info.title, "Rusternetes Kubernetes API");
        assert!(!spec.paths.paths.is_empty());
    }

    #[test]
    fn test_spec_has_core_paths() {
        let spec = generate_openapi_spec();
        assert!(spec
            .paths
            .paths
            .contains_key("/api/v1/namespaces/{namespace}/pods"));
        assert!(spec
            .paths
            .paths
            .contains_key("/api/v1/namespaces/{namespace}/pods/{name}"));
    }

    #[test]
    fn test_spec_has_apps_paths() {
        let spec = generate_openapi_spec();
        assert!(spec
            .paths
            .paths
            .contains_key("/apis/apps/v1/namespaces/{namespace}/deployments"));
    }

    /// Refs #3057: v3 component schemas for admissionregistration.k8s.io/v1
    /// kinds carry the real group/version (not core/v1).
    #[test]
    fn spec_defines_admissionregistration_v1_kinds() {
        let spec = serde_json::to_value(generate_openapi_spec()).unwrap();
        let schemas = spec["components"]["schemas"].as_object().unwrap();
        for kind in [
            "MutatingWebhookConfiguration",
            "ValidatingWebhookConfiguration",
            "ValidatingAdmissionPolicy",
            "ValidatingAdmissionPolicyBinding",
        ] {
            let s = schemas
                .get(&format!("io.k8s.api.admissionregistration.v1.{kind}"))
                .unwrap_or_else(|| panic!("{kind} missing from v3 schemas"));
            let g = &s["x-kubernetes-group-version-kind"][0];
            assert_eq!(g["group"], "admissionregistration.k8s.io");
            assert_eq!(g["version"], "v1");
            assert_eq!(g["kind"], kind);
        }
    }

    #[test]
    fn test_spec_serialization() {
        let spec = generate_openapi_spec();
        let json = serde_json::to_string_pretty(&spec).unwrap();
        assert!(json.contains("Rusternetes"));
        assert!(json.contains("openapi"));
    }
}
