//! Unit tests moved from `src/validation/crd.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::{
        ConversionStrategyType, CustomResourceColumnDefinition, CustomResourceDefinition,
        CustomResourceDefinitionNames, CustomResourceDefinitionSpec, CustomResourceSubresources,
        JSONSchemaProps, JSONSchemaPropsOrBool, ResourceScope, SelectableField,
    };
    pub use rusternetes_common::validation::crd_openapi_schema::OpenApiSchemaOptions;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_dns1035_label, is_dns1123_subdomain};
    pub use rusternetes_common::validation::structural::Structural;
    pub use rusternetes_common::validation::webhookconfiguration::{
        validate_webhook_service, validate_webhook_url,
    };
    pub use std::collections::HashSet;
}

mod whole_object_tests {
    use super::__parent::*;
    use rusternetes_common::validation::crd::*;

    fn crd(group: &str) -> CustomResourceDefinition {
        let mut crd = CustomResourceDefinition::new("widget", group, "Widget", "widgets");
        crd.spec.versions = vec![serde_json::from_value(serde_json::json!({
            "name": "v1", "served": true, "storage": true
        }))
        .unwrap()];
        set_defaults_custom_resource_definition(&mut crd);
        crd
    }

    /// `validateEnumStrings` (validation.go:502-515): an unknown `spec.scope`
    /// is a Go string, so it decodes and is `NotSupported` with the accepted
    /// list in declaration order (`Cluster`, `Namespaced`, :364).
    #[test]
    fn unknown_scope_is_not_supported() {
        let spec: rusternetes_common::resources::CustomResourceDefinitionSpec =
            serde_json::from_value(serde_json::json!({
                "group": "example.com",
                "names": {"kind": "Widget", "plural": "widgets"},
                "scope": "Galactic",
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }))
            .expect("an unknown scope must decode");
        let errs = validate_custom_resource_definition_spec(&spec, &Path::new("spec"));
        let e = errs
            .iter()
            .find(|e| e.field == "spec.scope")
            .unwrap_or_else(|| panic!("{errs:?}"));
        assert_eq!(
            e.error_type,
            rusternetes_common::validation::field::ErrorType::NotSupported,
            "{errs:?}"
        );
        assert!(
            e.to_string()
                .contains("supported values: \"Cluster\", \"Namespaced\""),
            "{e}"
        );
    }

    /// validation.go:617 + 618-640: an unknown strategy is `NotSupported`
    /// (`None`, `Webhook`) and, not being Webhook, takes the else branch that
    /// forbids `webhook` config.
    #[test]
    fn unknown_conversion_strategy_is_not_supported() {
        let mut c = crd("example.com");
        c.spec.conversion = Some(
            serde_json::from_value(serde_json::json!({"strategy": "Carrier"}))
                .expect("an unknown strategy must decode"),
        );
        let errs = validate_custom_resource_definition_spec(&c.spec, &Path::new("spec"));
        let e = errs
            .iter()
            .find(|e| e.field == "spec.conversion.strategy")
            .unwrap_or_else(|| panic!("{errs:?}"));
        assert_eq!(
            e.error_type,
            rusternetes_common::validation::field::ErrorType::NotSupported,
            "{errs:?}"
        );
        assert!(
            e.to_string()
                .contains("supported values: \"None\", \"Webhook\""),
            "{e}"
        );
    }

    /// `TestValidateCustomResourceDefinitionStoredVersions`
    /// (validation_test.go): the storage version must be stored, and a stored
    /// version must stay in `spec.versions`.
    #[test]
    fn stored_versions_rules() {
        let c = crd("example.com");
        let path = Path::new("status").child("storedVersions");
        assert!(validate_custom_resource_definition_stored_versions(
            &["v1".into()],
            &c.spec.versions,
            &path
        )
        .is_empty());
        let errs =
            validate_custom_resource_definition_stored_versions(&[], &c.spec.versions, &path);
        assert_eq!(errs.len(), 1);
        let errs = validate_custom_resource_definition_stored_versions(
            &["v0".into()],
            &c.spec.versions,
            &path,
        );
        assert_eq!(errs.len(), 2, "{errs:?}");
    }

    #[test]
    fn protected_groups() {
        for g in ["k8s.io", "a.k8s.io", "kubernetes.io", "a.kubernetes.io"] {
            assert!(is_protected_community_group(g), "{g}");
        }
        assert!(!is_protected_community_group("example.com"));
        assert!(!is_protected_community_group("notk8s.io.example.com"));
    }

    #[test]
    fn approval_annotation_states() {
        let mut c = crd("a.k8s.io");
        let errs = validate_api_approval(&c, None);
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("Required value")),
            "{errs:?}"
        );
        for (value, ok) in [
            ("https://github.com/kubernetes/kubernetes/pull/1", true),
            ("unapproved, experimental-only", true),
            ("not a url", false),
        ] {
            c.metadata.annotations =
                Some([(KUBE_API_APPROVED_ANNOTATION.to_string(), value.to_string())].into());
            let errs = validate_api_approval(&c, None);
            assert_eq!(errs.is_empty(), ok, "{value}: {errs:?}");
        }
    }

    /// An unchanged approval state is never rejected on update
    /// (`validation.go:1873-1877`).
    #[test]
    fn unchanged_approval_state_passes_update() {
        let c = crd("a.k8s.io");
        assert!(validate_api_approval(&c, Some(&c)).is_empty());
    }

    #[test]
    fn preserve_unknown_fields_true_is_rejected_unless_already_set() {
        let mut c = crd("example.com");
        c.spec.preserve_unknown_fields = Some(true);
        assert_eq!(validate_preserve_unknown_fields(&c, None).len(), 1);
        assert!(validate_preserve_unknown_fields(&c, Some(&c)).is_empty());
    }

    #[test]
    fn defaults_seed_stored_versions_once() {
        let mut c = crd("example.com");
        assert_eq!(
            c.status.as_ref().unwrap().stored_versions,
            Some(vec!["v1".to_string()])
        );
        c.status.as_mut().unwrap().stored_versions = Some(vec!["v0".to_string()]);
        set_defaults_custom_resource_definition(&mut c);
        assert_eq!(
            c.status.as_ref().unwrap().stored_versions,
            Some(vec!["v0".to_string()])
        );
    }

    #[test]
    fn name_must_be_plural_dot_group() {
        let mut c = crd("example.com");
        assert!(required_name_errors(&c).is_empty());
        c.metadata.name = "other.example.com".into();
        assert_eq!(required_name_errors(&c).len(), 1);
    }
}

mod ca_bundle_tests {
    use super::__parent::*;
    use base64::Engine;
    use rusternetes_common::validation::crd::*;

    const CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIBbzCCARWgAwIBAgIUNlw6NuSK77Vukti6ud7ET7GhqoEwCgYIKoZIzj0EAwIw
DDEKMAgGA1UEAwwBdDAgFw0yNjEwMDYxMDAwMjZaGA8yMTI2MDkxMjEwMDAyNlow
DDEKMAgGA1UEAwwBdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABAyxwlFXhKv2
VrRugTxQF05QRdxMCwi9imsVkTzmSbFhY08oQcY7rE0p32un+8/2/RlclBghXdsk
JoOVsn1LB16jUzBRMB0GA1UdDgQWBBT0iCEcLVCXeTKArrq89VZNNHkTqjAfBgNV
HSMEGDAWgBT0iCEcLVCXeTKArrq89VZNNHkTqjAPBgNVHRMBAf8EBTADAQH/MAoG
CCqGSM49BAMCA0gAMEUCIGaiWFbeacTzpGuCBN2CeZq9GsKKNp4ii/lsWwWDxF7s
AiEAhWF2/JiVNXisqXic1Dfy761y8wW/Io2IJoBMvcYxo4E=
-----END CERTIFICATE-----"#;

    fn b64(b: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(b)
    }

    fn crd_with_bundle(bundle: Option<String>, established: bool) -> CustomResourceDefinition {
        let mut c = CustomResourceDefinition::new("widget", "example.com", "Widget", "widgets");
        c.spec.versions = vec![serde_json::from_value(serde_json::json!({
            "name": "v1", "served": true, "storage": true,
            "schema": {"openAPIV3Schema": {"type": "object"}}
        }))
        .unwrap()];
        c.spec.conversion = Some(rusternetes_common::resources::CustomResourceConversion {
            strategy: Some(ConversionStrategyType::Webhook),
            webhook: Some(rusternetes_common::resources::WebhookConversion {
                client_config: rusternetes_common::resources::WebhookClientConfig {
                    url: Some("https://example.com/convert".into()),
                    service: None,
                    ca_bundle: bundle,
                },
                conversion_review_versions: vec!["v1".into()],
            }),
        });
        set_defaults_custom_resource_definition(&mut c);
        c.metadata.resource_version = Some("42".into());
        if established {
            let status = c.status.get_or_insert_with(Default::default);
            status.conditions = Some(vec![serde_json::from_value(serde_json::json!({
                "type": "Established", "status": "True"
            }))
            .unwrap()]);
        }
        c
    }

    /// `TestValidateCABundle` (apiserver/pkg/util/webhook/validation_test.go).
    #[test]
    fn validate_ca_bundle_cases() {
        let p = Path::new("caBundle");
        assert!(validate_ca_bundle(&p, b"").is_empty());
        assert!(validate_ca_bundle(&p, CERT.as_bytes()).is_empty());
        let errs = validate_ca_bundle(&p, b"bogus");
        assert_eq!(errs.len(), 1);
        assert!(
            errs[0]
                .error_body()
                .contains("unable to load root certificates: unable to parse bytes as PEM block"),
            "{errs:?}"
        );
        assert_eq!(validate_ca_bundle(&p, b"Cg==").len(), 1);
    }

    /// "invalid CABundle should be allowed on Create" (validation_test.go:260).
    #[test]
    fn invalid_ca_bundle_allowed_on_create() {
        let c = crd_with_bundle(Some(b64(b"Cg==")), false);
        assert!(validate_custom_resource_definition(&c).is_empty());
    }

    /// "update to invalid CABundle should fail if existing is valid"
    /// (validation_test.go:5958) -- error at
    /// `spec.conversion.webhookClientConfig.caBundle`.
    #[test]
    fn update_to_invalid_ca_bundle_fails_if_existing_valid_and_established() {
        let old = crd_with_bundle(Some(b64(CERT.as_bytes())), true);
        let mut new = old.clone();
        new.spec
            .conversion
            .as_mut()
            .unwrap()
            .webhook
            .as_mut()
            .unwrap()
            .client_config
            .ca_bundle = Some(b64(b"Cg=="));
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0]
                .to_string()
                .starts_with("spec.conversion.webhookClientConfig.caBundle: Invalid value"),
            "{errs:?}"
        );
    }

    /// "existing valid CABundle should be able to transition to invalid
    /// pre-serving" (validation_test.go:5844): not Established.
    #[test]
    fn invalid_ca_bundle_allowed_before_established() {
        let old = crd_with_bundle(Some(b64(CERT.as_bytes())), false);
        let mut new = old.clone();
        new.spec
            .conversion
            .as_mut()
            .unwrap()
            .webhook
            .as_mut()
            .unwrap()
            .client_config
            .ca_bundle = Some(b64(b"Cg=="));
        assert!(validate_custom_resource_definition_update(&new, &old).is_empty());
    }

    /// "existing invalid CABundle update should pass" (validation_test.go:5724).
    #[test]
    fn existing_invalid_ca_bundle_update_passes() {
        let old = crd_with_bundle(Some(b64(b"Cg==")), true);
        let new = old.clone();
        assert!(validate_custom_resource_definition_update(&new, &old).is_empty());
    }
}

#[path = "crd_structural_tests.rs"]
mod structural_schema_tests {
    //! Ports of the structural-schema cases of upstream
    //! `TestValidateCustomResourceDefinition` (`validation_test.go:1430-1700`,
    //! release-1.35) and the `requireStructuralSchema` ratchet
    //! (`validation.go:1748-1777`).

    use super::__parent::*;
    use rusternetes_common::validation::crd::*;
    use serde_json::json;

    /// `validValidationSchema` (`validation_test.go:11487`), trimmed to the
    /// fields that matter: structural.
    fn valid_schema() -> serde_json::Value {
        json!({
            "type": "object",
            "description": "This is a description",
            "required": ["spec", "status"],
            "properties": {
                "spec": {"type": "object"},
                "status": {"type": "object"}
            }
        })
    }

    /// `validUnstructuralValidationSchema` (`validation_test.go:11523`): `type`
    /// missing under `properties` and `items`.
    fn unstructural_schema() -> serde_json::Value {
        json!({
            "type": "object",
            "required": ["spec", "status"],
            "items": {"description": "This is a schema nested under Items"},
            "properties": {"spec": {}, "status": {}}
        })
    }

    fn crd_with(schemas: &[serde_json::Value]) -> CustomResourceDefinition {
        let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
        crd.spec.versions = schemas
            .iter()
            .enumerate()
            .map(|(i, s)| {
                serde_json::from_value(json!({
                    "name": format!("v{}", i + 1),
                    "served": true,
                    "storage": i == 0,
                    "schema": {"openAPIV3Schema": s}
                }))
                .unwrap()
            })
            .collect();
        crd.spec.preserve_unknown_fields = Some(false);
        crd.metadata.resource_version = Some("1".to_string());
        set_defaults_custom_resource_definition(&mut crd);
        crd
    }

    fn schema_errs(errs: &ErrorList) -> Vec<String> {
        errs.iter()
            .filter(|e| e.field.contains("openAPIV3Schema"))
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn structural_schema_is_accepted() {
        let errs = validate_custom_resource_definition(&crd_with(&[valid_schema()]));
        assert!(schema_errs(&errs).is_empty(), "{errs:?}");
    }

    /// "preserveUnknownFields with unstructural schema in one version"
    /// (`validation_test.go:1511`): only versions[1] is reported, with the
    /// `Required` errors for the missing types.
    #[test]
    fn unstructural_schema_in_one_version_is_required_type() {
        let errs = validate_custom_resource_definition(&crd_with(&[
            valid_schema(),
            unstructural_schema(),
        ]));
        let got = schema_errs(&errs);
        for want in [
            "spec.versions[1].schema.openAPIV3Schema.properties[spec].type: Required value",
            "spec.versions[1].schema.openAPIV3Schema.properties[status].type: Required value",
            "spec.versions[1].schema.openAPIV3Schema.items.type: Required value",
        ] {
            assert!(got.iter().any(|g| g.starts_with(want)), "{want}\n{got:#?}");
        }
        assert!(got.iter().all(|g| g.contains("versions[1]")), "{got:#?}");
    }

    /// An unsupported field is reported by the `ValidateCustomResourceDefinitionOpenAPISchema`
    /// walk (`Forbidden`); the overlapping `NewStructural` init error is then
    /// suppressed (`validation.go:975-979`).
    #[test]
    fn unsupported_field_is_forbidden_by_the_walk() {
        let mut s = valid_schema();
        s["properties"]["spec"]["id"] = json!("foo");
        let errs = validate_custom_resource_definition(&crd_with(&[s]));
        let got = schema_errs(&errs);
        assert_eq!(got.len(), 1, "init error must be hidden: {got:#?}");
        assert!(
            got[0].starts_with(
                "spec.versions[0].schema.openAPIV3Schema.properties[spec].id: Forbidden"
            ),
            "{got:#?}"
        );
    }

    /// `specHasDefaults` (`validation.go:391-396`): defaults require
    /// `preserveUnknownFields: false` — on update, where an old `true` is
    /// tolerated.
    #[test]
    fn defaults_need_preserve_unknown_fields_false() {
        let mut s = valid_schema();
        s["properties"]["spec"]["default"] = json!({});
        let mut crd = crd_with(&[s]);
        let old = crd.clone();
        crd.spec.preserve_unknown_fields = Some(true);
        let errs = validate_custom_resource_definition_update(&crd, &old);
        assert!(
            errs.iter().any(|e| e.field == "spec.preserveUnknownFields"
                && e.to_string()
                    .contains("must be false in order to use defaults in the schema")),
            "{errs:?}"
        );
    }

    /// `requireStructuralSchema` (`validation.go:1748`): an object whose stored
    /// schema is already non-structural is not tightened on update. Only a CRD
    /// with `preserveUnknownFields: true` can be in that state, because
    /// `preserveUnknownFields` false forces the requirement (`validation.go:366`).
    #[test]
    fn update_does_not_tighten_already_unstructural_crd() {
        let mut old = crd_with(&[unstructural_schema()]);
        old.spec.preserve_unknown_fields = Some(true);
        let new = old.clone();
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert!(schema_errs(&errs).is_empty(), "{errs:?}");
    }

    /// An update from a structural schema to a non-structural one is rejected.
    #[test]
    fn update_to_unstructural_is_rejected() {
        let old = crd_with(&[valid_schema()]);
        let mut new = crd_with(&[unstructural_schema()]);
        new.metadata = old.metadata.clone();
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert!(!schema_errs(&errs).is_empty(), "{errs:?}");
    }

    /// `openAPIV3Schema: {}` is a non-nil pointer upstream, so unlike an absent
    /// one (`validation.go:914-916`) it is run through `NewStructural` and
    /// `ValidateStructural`, which require a root type.
    #[test]
    fn empty_schema_is_validated_not_skipped() {
        let errs = validate_custom_resource_definition(&crd_with(&[json!({})]));
        assert!(
            schema_errs(&errs)
                .iter()
                .any(|e| e
                    .starts_with("spec.versions[0].schema.openAPIV3Schema.type: Required value")),
            "{errs:?}"
        );
    }

    /// A CRD whose versions carry the given raw `schema` values (`None` omits the
    /// key, as a manifest without `schema` does).
    fn crd_with_raw_schemas(schemas: &[Option<serde_json::Value>]) -> CustomResourceDefinition {
        let mut crd = crd_with(&[]);
        crd.spec.versions = schemas
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let mut v =
                    json!({"name": format!("v{}", i + 1), "served": true, "storage": i == 0});
                if let Some(s) = s {
                    v["schema"] = s.clone();
                }
                serde_json::from_value(v).unwrap()
            })
            .collect();
        crd.spec.preserve_unknown_fields = Some(false);
        crd
    }

    fn required_schema_errs(errs: &ErrorList) -> Vec<String> {
        errs.iter()
            .filter(|e| {
                e.to_string().starts_with("spec.versions[")
                    && e.to_string()
                        .contains("schema.openAPIV3Schema: Required value")
            })
            .map(|e| e.field.clone())
            .collect()
    }

    /// "preserveUnknownFields with no schema in one version"
    /// (`validation_test.go:1557`): `schema: {openAPIV3Schema: nil}`.
    #[test]
    fn create_requires_schema_in_every_version_one_missing() {
        let crd = crd_with_raw_schemas(&[
            Some(json!({"openAPIV3Schema": valid_schema()})),
            Some(json!({})),
        ]);
        let errs = validate_custom_resource_definition(&crd);
        assert_eq!(
            required_schema_errs(&errs),
            vec!["spec.versions[1].schema.openAPIV3Schema"],
            "{errs:?}"
        );
    }

    /// "preserveUnknownFields with no schema at all" (`validation_test.go:1599`).
    #[test]
    fn create_requires_schema_when_none_at_all() {
        let crd = crd_with_raw_schemas(&[None, Some(json!({}))]);
        let errs = validate_custom_resource_definition(&crd);
        assert_eq!(
            required_schema_errs(&errs),
            vec![
                "spec.versions[0].schema.openAPIV3Schema",
                "spec.versions[1].schema.openAPIV3Schema"
            ],
            "{errs:?}"
        );
    }

    /// `preserveUnknownFields: true` does not excuse a missing schema on create:
    /// `requireOpenAPISchema` is true for every create (`validation.go:371-377`).
    #[test]
    fn create_requires_schema_even_with_preserve_unknown_fields() {
        let mut crd = crd_with_raw_schemas(&[None]);
        crd.spec.preserve_unknown_fields = Some(true);
        let errs = validate_custom_resource_definition(&crd);
        assert_eq!(
            required_schema_errs(&errs),
            vec!["spec.versions[0].schema.openAPIV3Schema"],
            "{errs:?}"
        );
    }

    /// "schema not required if old object is missing schema"
    /// (`validation_test.go:6742`): `requireOpenAPISchema` ratchet.
    #[test]
    fn update_does_not_require_schema_if_old_is_missing_it() {
        let mut old = crd_with_raw_schemas(&[None]);
        old.spec.preserve_unknown_fields = Some(true);
        let new = old.clone();
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert!(required_schema_errs(&errs).is_empty(), "{errs:?}");
    }

    /// "schema not required if old object is missing schema for some versions"
    /// (`validation_test.go:6770`).
    #[test]
    fn update_does_not_require_schema_if_old_is_missing_some() {
        let mut old =
            crd_with_raw_schemas(&[Some(json!({"openAPIV3Schema": {"type": "object"}})), None]);
        old.spec.preserve_unknown_fields = Some(true);
        let mut new = crd_with_raw_schemas(&[None, None]);
        new.spec.preserve_unknown_fields = Some(true);
        new.metadata = old.metadata.clone();
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert!(required_schema_errs(&errs).is_empty(), "{errs:?}");
    }

    /// "schema required if all versions of old object have schema"
    /// (`validation_test.go:6831`/`:6867`).
    #[test]
    fn update_requires_schema_if_all_old_versions_have_one() {
        let mut old = crd_with_raw_schemas(&[
            Some(json!({"openAPIV3Schema": {"type": "object", "description": "1"}})),
            Some(json!({"openAPIV3Schema": {"type": "object", "description": "2"}})),
        ]);
        old.spec.preserve_unknown_fields = Some(true);
        let mut new = crd_with_raw_schemas(&[None, None]);
        new.spec.preserve_unknown_fields = Some(true);
        new.metadata = old.metadata.clone();
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert_eq!(
            required_schema_errs(&errs),
            vec![
                "spec.versions[0].schema.openAPIV3Schema",
                "spec.versions[1].schema.openAPIV3Schema"
            ],
            "{errs:?}"
        );
    }

    /// The `else if` arm (`validation.go:378-387`): the old object lacked a schema
    /// somewhere so `requireOpenAPISchema` is false, and `preserveUnknownFields:
    /// false` makes a served version without a schema prune every field.
    #[test]
    fn update_served_version_without_schema_is_pruned_required() {
        let mut old = crd_with_raw_schemas(&[None]);
        old.spec.preserve_unknown_fields = Some(true);
        let mut new = old.clone();
        new.spec.preserve_unknown_fields = Some(false);
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert!(
            errs.iter().any(|e| e.to_string().starts_with(
                "spec.versions[0].schema.openAPIV3Schema: Required value: because otherwise all fields are pruned"
            )),
            "{errs:?}"
        );
    }
}

#[path = "crd_selectable_tests.rs"]
mod selectable_fields_tests {
    //! Ports of upstream `TestSelectableFields` (`validation_test.go:4544`,
    //! release-1.35) plus the structural-schema edge cases of
    //! `ValidateCustomResourceSelectableFields` / `cel.ValidFieldPath`
    //! (`validation.go:296-320`, `:847-890`; `schema/cel/validation.go:557-676`).

    use super::__parent::*;
    use rusternetes_common::validation::crd::*;
    use serde_json::json;

    fn crd_with_version(
        schema: Option<serde_json::Value>,
        paths: &[&str],
    ) -> CustomResourceDefinition {
        let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
        let mut version = json!({
            "name": "version",
            "served": true,
            "storage": true,
            "selectableFields": paths.iter().map(|p| json!({"jsonPath": p})).collect::<Vec<_>>(),
        });
        if let Some(s) = schema {
            version["schema"] = json!({"openAPIV3Schema": s});
        }
        crd.spec.versions = vec![serde_json::from_value(version).unwrap()];
        crd.spec.preserve_unknown_fields = Some(false);
        crd.metadata.resource_version = Some("1".to_string());
        set_defaults_custom_resource_definition(&mut crd);
        crd
    }

    fn selectable_errs(crd: &CustomResourceDefinition) -> Vec<String> {
        validate_custom_resource_definition_spec(&crd.spec, &Path::new("spec"))
            .iter()
            .filter(|e| e.field.contains("selectableFields"))
            .map(|e| e.to_string())
            .collect()
    }

    /// `validation.go:311-312`.
    #[test]
    fn selectable_fields_without_a_schema_are_invalid() {
        let errs = selectable_errs(&crd_with_version(None, &[".foo"]));
        assert_eq!(
            errs,
            vec![
                "spec.versions[0].selectableFields: Invalid value: \"\": may only be set when `version.schema.openAPIV3Schema` is not included"
                    .to_string()
            ]
        );
    }

    /// `validation.go:314-317`: a schema `NewStructural` rejects is reported at the
    /// version path plus the literal single element `schema.openAPIV3Schema`, in
    /// addition to the structural-schema error of `validateCustomResourceDefinitionValidation`.
    #[test]
    fn unconvertible_schema_reports_at_schema_dot_openapi_path() {
        let bad = json!({
            "type": "object",
            "properties": {"foo": {"type": "string"}},
            "items": [{"type": "string"}]
        });
        // `foo` (no leading delimiter) takes the `default:` arm of the token
        // switch (`schema/cel/validation.go:670-671`) and never dereferences the
        // nil schema, so only the errors are produced.
        let c = crd_with_version(Some(bad), &["foo"]);
        let errs: Vec<String> =
            validate_custom_resource_definition_spec(&c.spec, &Path::new("spec"))
                .iter()
                .map(|e| e.to_string())
                .collect();
        assert_eq!(errs.len(), 3, "{errs:?}");
        assert!(
            errs.iter().any(|e| e.starts_with(
                "spec.versions[0].schema.openAPIV3Schema: Invalid value: \"\": OpenAPIV3Schema 'items' must be a schema, but is an array"
            )),
            "{errs:?}"
        );
    }

    /// `NewStructural` leaves `Properties` nil when `len(s.Properties) == 0`
    /// (`convert.go:93`), so `ValidFieldPath` falls through to
    /// `AdditionalProperties` (`schema/cel/validation.go:660-664`).
    #[test]
    fn empty_properties_fall_through_to_additional_properties() {
        let errs = selectable_errs(&crd_with_version(
            Some(json!({
                "type": "object",
                "properties": {},
                "additionalProperties": {"type": "string"}
            })),
            &[".anything"],
        ));
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// `additionalProperties: true` is a non-nil `AdditionalProperties` whose
    /// `Structural` is nil, so the leaf schema is nil and
    /// `allowedSelectableFieldSchema(nil)` is false (`validation.go:880-883`).
    #[test]
    fn bool_additional_properties_leaf_is_not_selectable() {
        let errs = selectable_errs(&crd_with_version(
            Some(json!({"type": "object", "additionalProperties": true})),
            &[".anything"],
        ));
        assert_eq!(
            errs,
            vec![
                "spec.versions[0].selectableFields[0].jsonPath: Invalid value: \".anything\": must point to a field of type string, boolean or integer. Enum string fields and strings with formats are allowed."
                    .to_string()
            ]
        );
    }

    /// A field reachable only through `allOf` is not in the structural schema's
    /// `Properties` either, so upstream rejects it too.
    #[test]
    fn all_of_only_field_is_rejected_like_upstream() {
        let errs = selectable_errs(&crd_with_version(
            Some(json!({
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "allOf": [{"properties": {"b": {"type": "string"}}}]
            })),
            &[".b"],
        ));
        assert_eq!(
            errs,
            vec![
                "spec.versions[0].selectableFields[0].jsonPath: Invalid value: \".b\": is an invalid path: does not refer to a valid field"
                    .to_string()
            ]
        );
    }

    /// `TestSelectableFields` "in top level schema ...": a path under an
    /// existing object resolves; an absent sibling is invalid; `.spec.foo` twice
    /// is a duplicate on the second only.
    #[test]
    fn nested_resolution_and_duplicates() {
        let errs = selectable_errs(&crd_with_version(
            Some(json!({
                "type": "object",
                "properties": {"spec": {"type": "object", "properties": {"foo": {"type": "string"}}}}
            })),
            &[".spec.foo", ".spec.xyz", ".spec.foo"],
        ));
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(errs[0].starts_with("spec.versions[0].selectableFields[1].jsonPath: Invalid value"));
        assert!(
            errs[1].starts_with("spec.versions[0].selectableFields[2].jsonPath: Duplicate value")
        );
    }

    /// `NewStructural` failed, so `schema` is a nil `*Structural`
    /// (`validation.go:314-316` appends the error and carries on), and
    /// `ValidFieldPath` evaluates `schema.Properties` (`schema/cel/validation.go:660`:
    /// `if schema.Properties != nil {`) on the first `.` token: a Go nil-pointer
    /// panic, which the apiserver's panic-recovery filter turns into a 500.
    #[test]
    #[should_panic]
    fn nil_structural_after_new_structural_failure_panics() {
        let bad = json!({
            "type": "object",
            "properties": {"foo": {"type": "string"}},
            "items": [{"type": "string"}]
        });
        let c = crd_with_version(Some(bad), &[".foo"]);
        let _ = validate_custom_resource_definition_spec(&c.spec, &Path::new("spec"));
    }

    /// `additionalProperties: true` makes `schema = schema.AdditionalProperties.Structural`
    /// nil (`schema/cel/validation.go:566`); the next `.` token evaluates
    /// `schema.Properties` (`:660`) on it: a Go nil-pointer panic.
    #[test]
    #[should_panic]
    fn stepping_through_bool_additional_properties_panics() {
        let c = crd_with_version(
            Some(json!({"type": "object", "additionalProperties": true})),
            &[".a.b"],
        );
        let _ = validate_custom_resource_definition_spec(&c.spec, &Path::new("spec"));
    }
}

#[path = "crd_defaults_tests.rs"]
mod defaults_tests {
    //! Ports of the default cases of upstream `TestValidateCustomResourceDefinition`
    //! (`apis/apiextensions/validation/validation_test.go:1780-2260`,
    //! `2311-2604`, `2608-3980`, release-1.35) and the `requirePrunedDefaults`
    //! ratchet (`validation.go:1779`; update cases at `:6872-7000`).
    //!
    //! Like upstream's `validationMatch`, expected errors are (type, field path
    //! below `openAPIV3Schema`) pairs matched as a multiset.

    use super::__parent::*;
    use rusternetes_common::validation::crd::*;
    use rusternetes_common::validation::field::ErrorType;
    use serde_json::{json, Value};

    fn crd_with_schema(schema: Value) -> CustomResourceDefinition {
        let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
        crd.spec.versions = vec![serde_json::from_value(json!({
            "name": "v1", "served": true, "storage": true,
            "schema": {"openAPIV3Schema": schema}
        }))
        .unwrap()];
        crd.spec.preserve_unknown_fields = Some(false);
        crd.metadata.resource_version = Some("1".to_string());
        set_defaults_custom_resource_definition(&mut crd);
        crd
    }

    const PREFIX: &str = "spec.versions[0].schema.openAPIV3Schema";

    fn got(crd: &CustomResourceDefinition) -> Vec<(ErrorType, String)> {
        let mut v: Vec<_> = validate_custom_resource_definition(crd)
            .into_iter()
            .filter(|e| e.field.starts_with(PREFIX))
            .map(|e| {
                (
                    e.error_type,
                    e.field.strip_prefix(PREFIX).unwrap().to_string(),
                )
            })
            .collect();
        v.sort_by(|a, b| a.1.cmp(&b.1));
        v
    }

    fn check(schema: Value, want: &[(ErrorType, &str)]) {
        let mut want: Vec<_> = want.iter().map(|(t, p)| (*t, p.to_string())).collect();
        want.sort_by(|a, b| a.1.cmp(&b.1));
        let g = got(&crd_with_schema(schema));
        assert_eq!(g, want);
    }

    fn obj(props: Value) -> Value {
        json!({"type": "object", "properties": props})
    }

    /// "embedded metadata defaults": the allowed shapes — unknown fields inside
    /// `metadata`, and under `x-kubernetes-preserve-unknown-fields`.
    #[test]
    fn allowed_embedded_defaults() {
        check(
            obj(json!({
                "allowed-in-object-defaults": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {
                        "apiVersion": {"type": "string", "default": "v1"},
                        "kind": {"type": "string", "default": "Pod"},
                        "metadata": {
                            "type": "object",
                            "properties": {"name": {"type": "string", "default": "foo"}},
                            "default": {"unknown": 42}
                        }
                    }
                },
                "allowed-object-defaults": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"something": {"type": "string"}},
                    "default": {"apiVersion": "v1", "kind": "Pod",
                                "metadata": {"name": "foo", "unknown": 42}}
                },
                "allowed-spanning-object-defaults": {
                    "type": "object",
                    "properties": {"embedded": {
                        "type": "object", "x-kubernetes-embedded-resource": true,
                        "properties": {"something": {"type": "string"}}}},
                    "default": {"embedded": {"apiVersion": "v1", "kind": "Pod",
                                "metadata": {"name": "foo", "unknown": 42}}}
                },
                "x-preserve-unknown-fields-unknown-field-object-defaults": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "x-kubernetes-preserve-unknown-fields": true, "properties": {},
                    "default": {"apiVersion": "v1", "kind": "Pod",
                                "metadata": {"name": "foo", "unknown": 42}, "unknown": 42}
                }
            })),
            &[],
        );
    }

    /// "unknown-field-object-defaults" / "...-spanning-object-defaults".
    #[test]
    fn unknown_fields_in_defaults_are_invalid() {
        check(
            obj(json!({
                "unknown-field-object-defaults": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"something": {"type": "string"}},
                    "default": {"apiVersion": "v1", "kind": "Pod",
                                "metadata": {"name": "foo", "unknown": 42}, "unknown": 42}
                },
                "unknown-field-spanning-object-defaults": {
                    "type": "object",
                    "properties": {"embedded": {
                        "type": "object", "x-kubernetes-embedded-resource": true,
                        "properties": {"something": {"type": "string"}}}},
                    "default": {"embedded": {"apiVersion": "v1", "kind": "Pod",
                                "metadata": {"name": "foo", "unknown": 42}, "unknown": 42},
                                "unknown": 42}
                }
            })),
            &[
                (
                    ErrorType::Invalid,
                    ".properties[unknown-field-object-defaults].default",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[unknown-field-spanning-object-defaults].default",
                ),
            ],
        );
    }

    /// "wrongly-typed-in-object-defaults": defaults of the `apiVersion`, `kind`
    /// and `metadata` properties of an embedded resource (the `insideMeta` branch).
    #[test]
    fn wrongly_typed_in_object_defaults() {
        check(
            obj(json!({
                "wrongly-typed-in-object-defaults": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {
                        "apiVersion": {"type": "string", "default": 42},
                        "kind": {"type": "string", "default": 42},
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "default": 42},
                                "annotations": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"},
                                    "default": 42
                                }
                            }
                        }
                    }
                }
            })),
            &[
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-in-object-defaults].properties[kind].default",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-in-object-defaults].properties[apiVersion].default",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-in-object-defaults].properties[metadata].properties[name].default",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-in-object-defaults].properties[metadata].properties[annotations].default",
                ),
            ],
        );
    }

    /// "wrongly-typed-object-defaults-*" and the `invalid-*` / `missing-typemeta`
    /// object defaults.
    #[test]
    fn embedded_object_defaults_are_validated() {
        let embedded = |default: Value| {
            json!({"type": "object", "x-kubernetes-embedded-resource": true,
                   "properties": {"something": {"type": "string"}}, "default": default})
        };
        check(
            obj(json!({
                "wrongly-typed-object-defaults-metadata": embedded(json!({"metadata": 42})),
                "wrongly-typed-object-defaults-apiVersion":
                    embedded(json!({"apiVersion": 42, "kind": "Foo"})),
                "wrongly-typed-object-defaults-kind":
                    embedded(json!({"apiVersion": "foo/v1", "kind": 42})),
                "wrongly-typed-object-defaults-labels":
                    embedded(json!({"metadata": {"labels": 42}})),
                "invalid-object-defaults-kind":
                    embedded(json!({"apiVersion": "foo/v1", "kind": "%"})),
                "invalid-object-defaults-name":
                    embedded(json!({"apiVersion": "foo/v1", "kind": "Foo",
                                    "metadata": {"name": "%"}})),
                "invalid-object-defaults-labels":
                    embedded(json!({"apiVersion": "foo/v1", "kind": "Foo",
                                    "metadata": {"labels": {"foo": "x y"}}})),
                "object-defaults-with-missing-typemeta":
                    embedded(json!({"metadata": {"name": "foo"}})),
                "valid-object-defaults":
                    embedded(json!({"apiVersion": "foo/v1", "kind": "Foo",
                                    "metadata": {"name": "foo"}})),
            })),
            &[
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-object-defaults-metadata].default.metadata",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-object-defaults-apiVersion].default.apiVersion",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-object-defaults-kind].default.kind",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[wrongly-typed-object-defaults-labels].default.metadata",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[invalid-object-defaults-kind].default.kind",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[invalid-object-defaults-name].default.metadata.name",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[invalid-object-defaults-labels].default.metadata.labels",
                ),
                (
                    ErrorType::Required,
                    ".properties[object-defaults-with-missing-typemeta].default.apiVersion",
                ),
                (
                    ErrorType::Required,
                    ".properties[object-defaults-with-missing-typemeta].default.kind",
                ),
            ],
        );
    }

    /// "spanning" defaults: the embedded resource sits below the object holding
    /// the default.
    #[test]
    fn spanning_object_defaults_are_validated() {
        let spanning = |default: Value| {
            json!({"type": "object",
                   "properties": {"embedded": {
                       "type": "object", "x-kubernetes-embedded-resource": true,
                       "properties": {"something": {"type": "string"}}}},
                   "default": default})
        };
        check(
            obj(json!({
                "invalid-spanning-object-defaults-kind":
                    spanning(json!({"embedded": {"apiVersion": "foo/v1", "kind": "%"}})),
                "invalid-spanning-object-defaults-name":
                    spanning(json!({"embedded": {"apiVersion": "foo/v1", "kind": "Foo",
                                                 "metadata": {"name": "%"}}})),
                "spanning-defaults-with-missing-typemeta":
                    spanning(json!({"embedded": {"metadata": {"name": "foo"}}})),
            })),
            &[
                (
                    ErrorType::Invalid,
                    ".properties[invalid-spanning-object-defaults-kind].default.embedded.kind",
                ),
                (
                    ErrorType::Invalid,
                    ".properties[invalid-spanning-object-defaults-name].default.embedded.metadata.name",
                ),
                (
                    ErrorType::Required,
                    ".properties[spanning-defaults-with-missing-typemeta].default.embedded.apiVersion",
                ),
                (
                    ErrorType::Required,
                    ".properties[spanning-defaults-with-missing-typemeta].default.embedded.kind",
                ),
            ],
        );
    }

    /// "metadata-defaults-with-invalid-constraints-*": a default in `metadata`
    /// that violates the constraints of the metadata sub-schema is `Unsupported`.
    #[test]
    fn metadata_defaults_violating_constraints_are_unsupported() {
        check(
            obj(json!({
                "metadata-defaults-with-invalid-constraints-name": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"metadata": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "enum": ["foo"]},
                            "labels": {"type": "object",
                                       "additionalProperties": {"type": "string", "enum": ["foo"]}}
                        },
                        "default": {"name": "bar"}
                    }}
                },
                "metadata-defaults-with-invalid-constraints-labels": {
                    "type": "object", "x-kubernetes-embedded-resource": true,
                    "properties": {"metadata": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string", "enum": ["foo"]},
                            "labels": {"type": "object",
                                       "additionalProperties": {"type": "string", "enum": ["foo"]}}
                        },
                        "default": {"name": "foo", "labels": {"foo": "bar"}}
                    }}
                }
            })),
            &[
                (
                    ErrorType::NotSupported,
                    ".properties[metadata-defaults-with-invalid-constraints-name].properties[metadata].default.name",
                ),
                (
                    ErrorType::NotSupported,
                    ".properties[metadata-defaults-with-invalid-constraints-labels].properties[metadata].default.labels.foo",
                ),
            ],
        );
    }

    /// A plain default that does not validate against its own schema, and a
    /// default under `items`.
    #[test]
    fn plain_defaults_are_validated_against_their_schema() {
        check(
            obj(json!({
                "ok": {"type": "integer", "default": 1},
                "bad": {"type": "integer", "default": "x"},
                "list": {"type": "array", "items": {"type": "string", "default": 5}},
                "enumd": {"type": "string", "enum": ["a"], "default": "b"},
                "nested": {"type": "object",
                           "properties": {"x": {"type": "string"}},
                           "required": ["x"], "default": {}}
            })),
            &[
                (ErrorType::Invalid, ".properties[bad].default"),
                (ErrorType::Invalid, ".properties[list].items.default"),
                (ErrorType::NotSupported, ".properties[enumd].default"),
                (ErrorType::Required, ".properties[nested].default.x"),
            ],
        );
    }

    fn unpruned_default_schema() -> Value {
        obj(json!({"spec": {
            "type": "object",
            "properties": {"a": {"type": "string"}},
            "default": {"a": "x", "unknown": 1}
        }}))
    }

    /// "add default ... structural schema, without pruning": a new unpruned
    /// default is rejected on create.
    #[test]
    fn unpruned_default_rejected_on_create() {
        check(
            unpruned_default_schema(),
            &[(ErrorType::Invalid, ".properties[spec].default")],
        );
    }

    /// `requirePrunedDefaults` (`validation.go:1779`): an old object that already
    /// holds an unpruned default keeps `requirePrunedDefaults=false`, so the
    /// update passes; once the old object is pruned, a new unpruned default fails.
    #[test]
    fn require_pruned_defaults_ratchet_on_update() {
        let old = crd_with_schema(unpruned_default_schema());
        assert!(
            !rusternetes_common::validation::structural::defaulting::require_pruned_defaults(
                &old.spec
            )
        );
        let new = old.clone();
        let errs: Vec<_> = validate_custom_resource_definition_update(&new, &old)
            .into_iter()
            .filter(|e| e.field.starts_with(PREFIX))
            .collect();
        assert!(errs.is_empty(), "{errs:?}");

        let clean = crd_with_schema(obj(json!({"spec": {
            "type": "object", "properties": {"a": {"type": "string"}},
            "default": {"a": "x"}}})));
        assert!(
            rusternetes_common::validation::structural::defaulting::require_pruned_defaults(
                &clean.spec
            )
        );
        let mut bad = clean.clone();
        bad.spec = old.spec.clone();
        let errs: Vec<_> = validate_custom_resource_definition_update(&bad, &clean)
            .into_iter()
            .filter(|e| e.field.starts_with(PREFIX))
            .collect();
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].error_type, ErrorType::Invalid);
    }

    /// `PruneDefaults` (`prune.go`): prunes a default against the root schema.
    #[test]
    fn prune_defaults_prunes_unknown_fields() {
        use rusternetes_common::validation::structural::{defaulting, new_structural};
        let schema: rusternetes_common::resources::crd::JSONSchemaProps =
            serde_json::from_value(unpruned_default_schema()).unwrap();
        let mut ss = new_structural(&schema).unwrap();
        defaulting::prune_defaults(&mut ss).unwrap();
        assert_eq!(
            ss.properties["spec"].generic.default,
            Some(json!({"a": "x"}))
        );
    }
}

#[path = "crd_openapi_schema_tests.rs"]
mod openapi_schema_tests {
    //! Ports of the schema-error cases of upstream
    //! `TestValidateCustomResourceDefinitionValidation` (`validation_test.go:8321`,
    //! release-1.35): the `ValidateCustomResourceDefinitionOpenAPISchema` walk, the
    //! status-subresource root restrictions and the `requireAtomicSetType` /
    //! `requireMapListKeysMapSetValidation` / `requireValidPropertyType` ratchets.
    //! The CEL cases of that test belong to the api-server crate.

    use super::__parent::*;
    use rusternetes_common::validation::crd::*;
    use serde_json::json;

    fn run(
        schema: serde_json::Value,
        status_enabled: bool,
        structural: bool,
        opts: OpenApiSchemaOptions,
    ) -> Vec<String> {
        let schema: JSONSchemaProps = serde_json::from_value(schema).unwrap();
        validate_custom_resource_definition_validation(
            &schema,
            status_enabled,
            structural,
            true,
            &opts,
            &Path::new("spec").child("validation"),
        )
        .iter()
        .map(|e| e.to_string())
        .collect()
    }

    fn ok(schema: serde_json::Value) -> Vec<String> {
        run(schema, false, false, OpenApiSchemaOptions::default())
    }

    fn assert_errs(got: &[String], want: &[&str]) {
        assert_eq!(got.len(), want.len(), "{got:#?}");
        for w in want {
            assert!(
                got.iter().any(|g| g.starts_with(w)),
                "missing {w}\n{got:#?}"
            );
        }
    }

    const P: &str = "spec.validation.openAPIV3Schema";

    #[test]
    fn root_type_with_status_must_be_object() {
        let got = run(json!({"type": "string"}), true, false, Default::default());
        assert_errs(&got, &[&format!("{P}.type: Invalid value: \"string\"")]);
        assert!(run(json!({"type": "string"}), false, false, Default::default()).is_empty());
    }

    #[test]
    fn non_allowed_root_field_with_status() {
        let got = run(
            json!({"type": "object", "default": {}}),
            true,
            false,
            Default::default(),
        );
        assert_eq!(got.len(), 1, "{got:#?}");
        assert!(
            got[0].starts_with(&format!("{P}: Invalid value:")),
            "{got:#?}"
        );
        assert!(
            got[0].ends_with("only [Description Type Format Title Maximum ExclusiveMaximum Minimum ExclusiveMinimum MaxLength MinLength Pattern MaxItems MinItems UniqueItems MultipleOf Required Items Properties ExternalDocs Example XPreserveUnknownFields XValidations] fields are allowed at the root of the schema if the status subresource is enabled"),
            "{got:#?}"
        );
    }

    #[test]
    fn allowed_root_fields_with_status() {
        let got = run(
            json!({
                "description": "d", "type": "object", "format": "f", "title": "t",
                "maximum": 1.0, "exclusiveMaximum": true, "minimum": 0.0,
                "exclusiveMinimum": true, "maxLength": 3, "minLength": 1, "pattern": "p",
                "maxItems": 4, "minItems": 1, "uniqueItems": false, "multipleOf": 2.0,
                "required": ["a"], "properties": {"a": {"type": "string"}},
                "example": {}, "x-kubernetes-preserve-unknown-fields": true
            }),
            true,
            false,
            Default::default(),
        );
        assert!(got.is_empty(), "{got:#?}");
    }

    #[test]
    fn null_type_and_nullable_root() {
        // upstream's "null type" case runs without `requireValidPropertyType`
        let opts = OpenApiSchemaOptions {
            require_valid_property_type: false,
            ..Default::default()
        };
        let got = run(
            json!({"type": "object", "properties": {"null": {"type": "null"}}}),
            false,
            false,
            opts,
        );
        assert_errs(&got, &[&format!("{P}.properties[null].type: Forbidden")]);
        let got = ok(json!({"type": "object", "nullable": true}));
        assert_errs(&got, &[&format!("{P}.nullable: Forbidden")]);
        assert!(ok(json!({"type": "object", "properties": {"a": {"nullable": true}}})).is_empty());
    }

    #[test]
    fn unsupported_fields_are_forbidden() {
        let got = ok(json!({
            "type": "object",
            "id": "x", "$ref": "#/x", "dependencies": {}, "additionalItems": true,
            "patternProperties": {"a": {}}, "definitions": {"a": {}},
            "items": [{"type": "string"}]
        }));
        for w in [
            "id",
            "$ref",
            "dependencies",
            "additionalItems",
            "patternProperties",
            "definitions",
            "items",
        ] {
            assert!(
                got.iter()
                    .any(|g| g.starts_with(&format!("{P}.{w}: Forbidden"))),
                "{w}\n{got:#?}"
            );
        }
        // definitions are walked and keyed
        let got = ok(json!({"definitions": {"a": {"id": "x"}}}));
        assert!(got
            .iter()
            .any(|g| g.starts_with(&format!("{P}.definitions[a].id: Forbidden"))));
    }

    #[test]
    fn unique_items_and_preserve_unknown_false() {
        let got = ok(json!({"type": "array", "uniqueItems": true, "items": {"type": "string"}}));
        assert_errs(&got, &[&format!("{P}.uniqueItems: Forbidden")]);
        let got = ok(json!({"type": "object", "x-kubernetes-preserve-unknown-fields": false}));
        assert_errs(
            &got,
            &[&format!(
                "{P}.x-kubernetes-preserve-unknown-fields: Invalid value: false: must be true or undefined"
            )],
        );
    }

    #[test]
    fn additional_properties_and_properties_are_mutually_exclusive() {
        let got = ok(json!({
            "type": "object", "properties": {"a": {"type": "string"}},
            "additionalProperties": {"type": "string"}
        }));
        assert_errs(
            &got,
            &[&format!(
                "{P}.additionalProperties: Forbidden: additionalProperties and properties are mutual exclusive"
            )],
        );
        assert!(ok(json!({"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": true})).is_empty());
        assert!(
            ok(json!({"type": "object", "additionalProperties": {"type": "string"}})).is_empty()
        );
    }

    #[test]
    fn require_valid_property_type() {
        let schema = json!({"type": "object", "properties": {"a": {"type": "bogus"}}});
        let got = ok(schema.clone());
        assert_errs(
            &got,
            &[&format!(
                "{P}.properties[a].type: Unsupported value: \"bogus\""
            )],
        );
        let opts = OpenApiSchemaOptions {
            require_valid_property_type: false,
            ..Default::default()
        };
        assert!(run(schema, false, false, opts).is_empty());
    }

    #[test]
    fn defaults_forbidden_inside_top_level_meta() {
        let got = ok(json!({"type": "object", "properties": {
            "metadata": {"type": "object", "default": {}, "properties": {"name": {"type": "string", "default": "x"}}},
            "kind": {"type": "string", "default": "k"},
            "spec": {"type": "object", "default": {}}
        }}));
        assert_errs(
            &got,
            &[
                &format!("{P}.properties[metadata].default: Forbidden: must not be set in top-level metadata"),
                &format!("{P}.properties[metadata].properties[name].default: Forbidden: must not be set in top-level metadata"),
                &format!("{P}.properties[kind].default: Forbidden: must not be set in top-level kind"),
            ],
        );
    }

    #[test]
    fn embedded_resource_meta_and_additional_properties() {
        let got = ok(json!({"type": "object", "properties": {"emb": {
            "type": "object", "x-kubernetes-embedded-resource": true,
            "properties": {"metadata": {"type": "object", "properties": {
                "labels": {"type": "object", "additionalProperties": {"type": "string", "default": "x"}},
                "inner": {"type": "object", "x-kubernetes-embedded-resource": true}
            }}}
        }}}));
        assert_errs(
            &got,
            &[
                &format!("{P}.properties[emb].properties[metadata].properties[labels].additionalProperties.default: Forbidden: must not be set inside additionalProperties applying to object metadata"),
                &format!("{P}.properties[emb].properties[metadata].properties[inner].x-kubernetes-embedded-resource: Forbidden: must not be used inside of resource meta"),
            ],
        );
    }

    #[test]
    fn list_type_extension_checks() {
        let got = ok(json!({"type": "string", "x-kubernetes-list-type": "set"}));
        assert_errs(
            &got,
            &[&format!(
                "{P}.type: Invalid value: \"string\": must be array if x-kubernetes-list-type is specified"
            )],
        );
        let got = ok(json!({"x-kubernetes-list-type": "set"}));
        assert_errs(
            &got,
            &[&format!(
                "{P}.type: Required value: must be array if x-kubernetes-list-type is specified"
            )],
        );
        let got = ok(
            json!({"type": "array", "items": {"type": "string"}, "x-kubernetes-list-type": "invalid"}),
        );
        assert_errs(
            &got,
            &[&format!(
                "{P}.x-kubernetes-list-type: Unsupported value: \"invalid\""
            )],
        );
        let got = ok(
            json!({"type": "array", "items": {"type": "string"}, "x-kubernetes-list-map-keys": ["a"], "x-kubernetes-list-type": "invalid"}),
        );
        assert_eq!(got.len(), 2, "{got:#?}");
        assert!(got.iter().any(|g| g.starts_with(&format!(
            "{P}.x-kubernetes-list-type: Invalid value: \"invalid\": must be map if x-kubernetes-list-map-keys is non-empty"
        ))));
        let got = ok(
            json!({"type": "array", "items": {"type": "string"}, "x-kubernetes-list-map-keys": ["a"]}),
        );
        assert_errs(
            &got,
            &[&format!(
                "{P}.x-kubernetes-list-type: Required value: must be map if x-kubernetes-list-map-keys is non-empty"
            )],
        );
    }

    #[test]
    fn map_type_extension_checks() {
        let got = ok(json!({"type": "string", "x-kubernetes-map-type": "granular"}));
        assert_errs(
            &got,
            &[&format!(
                "{P}.type: Invalid value: \"string\": must be object if x-kubernetes-map-type is specified"
            )],
        );
        let got = ok(json!({"x-kubernetes-map-type": "atomic"}));
        assert_errs(
            &got,
            &[&format!(
                "{P}.type: Required value: must be object if x-kubernetes-map-type is specified"
            )],
        );
        let got = ok(json!({"type": "object", "x-kubernetes-map-type": "invalid"}));
        assert_errs(
            &got,
            &[&format!(
                "{P}.x-kubernetes-map-type: Unsupported value: \"invalid\""
            )],
        );
        assert!(ok(json!({"type": "object", "x-kubernetes-map-type": "granular"})).is_empty());
    }

    fn map_list(items: serde_json::Value, keys: serde_json::Value) -> serde_json::Value {
        json!({"type": "array", "x-kubernetes-list-type": "map", "x-kubernetes-list-map-keys": keys, "items": items})
    }

    #[test]
    fn list_type_map_checks() {
        let item = |props: serde_json::Value| json!({"type": "object", "required": ["key"], "properties": props});
        // empty keys + missing items
        let got = ok(json!({"type": "array", "x-kubernetes-list-type": "map"}));
        assert_errs(
            &got,
            &[
                &format!("{P}.x-kubernetes-list-map-keys: Required value"),
                &format!("{P}.items: Required value"),
            ],
        );
        // array of schemas
        let got = ok(
            json!({"type": "array", "x-kubernetes-list-type": "map", "x-kubernetes-list-map-keys": ["a"], "items": [{"type": "object"}]}),
        );
        assert_errs(
            &got,
            &[
                &format!("{P}.items: Forbidden"),
                &format!("{P}.items: Invalid value:"),
            ],
        );
        // non object item
        let got = ok(map_list(json!({"type": "string"}), json!(["key"])));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.type: Invalid value: \"string\": must be object if parent array's x-kubernetes-list-type is map"
            )],
        );
        // key missing from properties
        let got = ok(map_list(
            item(json!({"other": {"type": "string"}})),
            json!(["key"]),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.x-kubernetes-list-map-keys: Invalid value: [\"key\"]: entries must all be names of item properties"
            )],
        );
        // non scalar key; upstream reports the parent items' type
        let got = ok(map_list(
            item(json!({"key": {"type": "object"}})),
            json!(["key"]),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.properties[key].type: Invalid value: \"object\": must be a scalar type if parent array's x-kubernetes-list-type is map"
            )],
        );
        // duplicate keys
        let got = ok(map_list(
            item(json!({"key": {"type": "string"}})),
            json!(["key", "key"]),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.x-kubernetes-list-map-keys: Invalid value: [\"key\",\"key\"]: must not contain duplicate entries"
            )],
        );
        // allowed
        assert!(ok(map_list(
            item(json!({"key": {"type": "string"}})),
            json!(["key"])
        ))
        .is_empty());
    }

    #[test]
    fn map_list_keys_must_be_required_or_defaulted_and_not_nullable() {
        let schema = |item: serde_json::Value| map_list(item, json!(["key"]));
        let got = ok(schema(
            json!({"type": "object", "properties": {"key": {"type": "string"}}}),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.properties[key].default: Required value: this property is in x-kubernetes-list-map-keys, so it must have a default or be a required property"
            )],
        );
        assert!(ok(schema(
            json!({"type": "object", "properties": {"key": {"type": "string", "default": "a"}}})
        ))
        .is_empty());
        let got = ok(schema(
            json!({"type": "object", "required": ["key"], "properties": {"key": {"type": "string", "nullable": true}}}),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.properties[key].nullable: Forbidden: this property is in x-kubernetes-list-map-keys, so it cannot be nullable"
            )],
        );
        let got = ok(schema(
            json!({"type": "object", "nullable": true, "required": ["key"], "properties": {"key": {"type": "string"}}}),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.nullable: Forbidden: cannot be nullable when x-kubernetes-list-type is map"
            )],
        );
        let nullable_set = json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string", "nullable": true}});
        let got = ok(nullable_set.clone());
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.nullable: Forbidden: cannot be nullable when x-kubernetes-list-type is set"
            )],
        );
        // the ratchet turns the check off
        let opts = OpenApiSchemaOptions {
            require_map_list_keys_map_set_validation: false,
            ..Default::default()
        };
        assert!(run(nullable_set, false, false, opts).is_empty());
    }

    #[test]
    fn list_type_set_requires_atomic_items() {
        let set = |items: serde_json::Value| json!({"type": "array", "x-kubernetes-list-type": "set", "items": items});
        assert!(ok(set(json!({"type": "string"}))).is_empty());
        assert!(ok(set(
            json!({"type": "object", "x-kubernetes-map-type": "atomic"})
        ))
        .is_empty());
        assert!(ok(set(json!({"type": "array", "items": {"type": "string"}}))).is_empty());
        assert!(ok(set(
            json!({"type": "array", "x-kubernetes-list-type": "atomic", "items": {"type": "string"}})
        ))
        .is_empty());
        for items in [
            json!({"type": "object", "x-kubernetes-map-type": "granular"}),
            json!({"type": "object"}),
        ] {
            let got = ok(set(items));
            assert_errs(
                &got,
                &[&format!(
                    "{P}.items.x-kubernetes-map-type: Invalid value: null: must be atomic as item of a list with x-kubernetes-list-type=set"
                )],
            );
        }
        let got = ok(set(
            json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string"}}),
        ));
        assert_errs(
            &got,
            &[&format!(
                "{P}.items.x-kubernetes-list-type: Invalid value: \"set\": must be atomic as item of a list with x-kubernetes-list-type=set"
            )],
        );
        // the ratchet turns the check off
        let opts = OpenApiSchemaOptions {
            require_atomic_set_type: false,
            ..Default::default()
        };
        assert!(run(set(json!({"type": "object"})), false, false, opts).is_empty());
    }

    #[test]
    fn update_options_ratchet_on_persisted_violations() {
        let spec_with = |s: serde_json::Value| {
            let mut crd = CustomResourceDefinition::new("widget", "group.com", "Plural", "plural");
            crd.spec.versions = vec![serde_json::from_value(json!({
                "name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": s}
            }))
            .unwrap()];
            crd.spec
        };
        let clean = OpenApiSchemaOptions::for_update(&spec_with(json!({"type": "object"})));
        assert!(
            clean.require_valid_property_type
                && clean.require_atomic_set_type
                && clean.require_map_list_keys_map_set_validation
        );
        let o = OpenApiSchemaOptions::for_update(&spec_with(json!({"type": "bogus"})));
        assert!(!o.require_valid_property_type && o.require_atomic_set_type);
        let o = OpenApiSchemaOptions::for_update(&spec_with(
            json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "object"}}),
        ));
        assert!(!o.require_atomic_set_type && o.require_map_list_keys_map_set_validation);
        let o = OpenApiSchemaOptions::for_update(&spec_with(
            json!({"type": "array", "x-kubernetes-list-type": "set", "items": {"type": "string", "nullable": true}}),
        ));
        assert!(!o.require_map_list_keys_map_set_validation && o.require_atomic_set_type);
    }

    /// `validation.go:975-979`: `NewStructural` init errors overlap the walk and are
    /// shown only when the walk (and `ValidateStructural`) reported nothing.
    #[test]
    fn structural_init_errors_hidden_when_walk_reports() {
        let s = json!({"type": "object", "properties": {"a": {"type": "string", "id": "x"}}});
        let got = run(s, false, true, Default::default());
        assert_errs(
            &got,
            &[&format!(
                "{P}.properties[a].id: Forbidden: id is not supported"
            )],
        );
    }

    #[test]
    fn structural_init_errors_shown_when_walk_is_clean() {
        // `NewStructural` rejects a non-nil `$schema` (`convert.go`) which the walk
        // does not look at.
        let s = json!({"type": "object", "$schema": "http://json-schema.org/draft-04/schema#"});
        let got = run(s, false, true, Default::default());
        assert_eq!(got.len(), 1, "{got:#?}");
        assert!(
            got[0].starts_with(&format!("{P}: Invalid value: \"\": ")),
            "{got:#?}"
        );
    }
}
