//! Unit tests moved from `src/registry/apiextensions/customresource.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use rusternetes_api_server::handlers::custom_resource::{
        apply_schema_defaults, has_cel_rules, validate_custom_resource_phase,
        validate_field_selector_paths, CrPhase,
    };
    pub use rusternetes_api_server::registry::generic::store::{BeginUpdate, Finish};
    pub use rusternetes_api_server::registry::generic::{
        CreateOptions, Deleted, Store, UpdateOptions,
    };
    pub use rusternetes_api_server::registry::rest::{
        with_group_kind, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestStorage, RestUpdateStrategy, UpdatedObjectInfo,
        ValidateObject, ValidateObjectUpdate,
    };
    pub use rusternetes_api_server::ssa::ResetFields;
    pub use rusternetes_common::deletion::{DeleteOptions, Preconditions};
    pub use rusternetes_common::fieldmanager::schema::Type as FieldType;
    pub use rusternetes_common::resources::{
        prune_custom_resource, CustomResource, CustomResourceDefinition,
        CustomResourceSubresourceScale, ResourceScope, Scale, ScaleSpec, ScaleStatus,
    };
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use rusternetes_common::validation::field::{
        Error as FieldError, ErrorList, ErrorType, Path,
    };
    pub use rusternetes_common::validation::metav1::is_qualified_name;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_common::validation::structural::listtype::validate_list_sets_and_maps;
    pub use rusternetes_common::validation::structural::objectmeta::{
        coerce_with_options, CoerceOptions,
    };
    pub use rusternetes_common::validation::structural::{new_structural, Structural};
    pub use rusternetes_common::{Error, Result};
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod prune_nulls_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::customresource::*;
    use serde_json::json;

    fn strategy(preserve_unknown_fields: bool) -> Strategy {
        let crd: CustomResourceDefinition = serde_json::from_value(json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.stable.example.com"},
            "spec": {
                "group": "stable.example.com", "scope": "Namespaced",
                "preserveUnknownFields": preserve_unknown_fields,
                "names": {"plural": "widgets", "kind": "Widget", "listKind": "WidgetList"},
                "versions": [{"name": "v1", "served": true, "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object", "properties": {
                        "spec": {"type": "object", "properties": {
                            "plain": {"type": "string"},
                            "nullable": {"type": "string", "nullable": true},
                            "defaulted": {"type": "string", "default": "d"},
                            "nested": {"type": "object", "properties": {
                                "inner": {"type": "integer"}}}
                        }}
                    }}}}]
            }
        }))
        .unwrap();
        Strategy::new(Arc::new(crd), "v1")
    }

    fn cr(spec: serde_json::Value) -> CustomResource {
        serde_json::from_value(json!({
            "apiVersion": "stable.example.com/v1", "kind": "Widget",
            "metadata": {"name": "w"}, "spec": spec
        }))
        .unwrap()
    }

    /// customresource_handler.go:1439: non-nullable nulls without a default
    /// are removed at decode; nullable ones are kept; absent defaulted ones get
    /// their default.
    #[test]
    fn decode_prunes_non_nullable_nulls_without_defaults() {
        let mut obj = cr(json!({
            "plain": null, "nullable": null,
            "nested": {"inner": null}
        }));
        strategy(false).coerce(&mut obj);
        assert_eq!(
            obj.spec.unwrap(),
            json!({"nullable": null, "defaulted": "d", "nested": {}})
        );
    }

    /// `schemaCoercingConverter` (customresource_handler.go:1348-1384) runs the
    /// coercer but not the defaulter: nulls and unknown fields go, an absent
    /// defaulted field stays absent.
    #[test]
    fn converter_coerces_without_defaulting() {
        let mut obj = cr(json!({"plain": null, "nullable": null, "unknown": 1}));
        strategy(false).coerce_converted(&mut obj);
        assert_eq!(obj.spec.unwrap(), json!({"nullable": null}));
    }

    /// customresource_handler.go:1432: only when `!preserveUnknownFields`.
    #[test]
    fn preserve_unknown_fields_skips_null_pruning() {
        let mut obj = cr(json!({"plain": null}));
        strategy(true).coerce(&mut obj);
        assert_eq!(obj.spec.unwrap(), json!({"plain": null, "defaulted": "d"}));
    }

    /// `BeforeCreate` returns `errors.NewInvalid(kind.GroupKind(), name, errs)`
    /// (rest/create.go:122-123), so a kind that does not match the CRD reads
    /// `Other.stable.example.com "w" is invalid: kind: Invalid value: ...`
    /// (errors.go:284-312), not the bare `Invalid resource: ...` list.
    #[test]
    fn decode_strict_kind_mismatch_is_new_invalid() {
        use rusternetes_storage::MemoryStorage;
        let strat = strategy(false);
        let rest = CustomResourceRest::new(
            Arc::new(StorageBackend::Memory(Arc::new(MemoryStorage::new()))),
            strat.crd.clone(),
            "v1",
            false,
            StrictMode::Off,
        );
        let mut obj = cr(json!({}));
        obj.kind = "Other".into();
        let err = rest.decode_strict(&obj).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Other.stable.example.com \"w\" is invalid: kind: Invalid value: \"Other\": must be Widget"
        );
    }
}

mod embedded_and_blocking_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::customresource::*;
    use serde_json::json;

    const BLOCKING: &str = "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation";

    fn strategy(with_rule: bool) -> Strategy {
        let mut spec = json!({"type": "object",
        "required": ["need"],
        "properties": {
            "need": {"type": "string"},
            "n": {"type": "integer"},
            "embedded": {"type": "object",
                "x-kubernetes-embedded-resource": true,
                "x-kubernetes-preserve-unknown-fields": true}
        }});
        if with_rule {
            spec["x-kubernetes-validations"] =
                json!([{"rule": "!has(self.n) || self.n < 10", "message": "n too big"}]);
        }
        let crd: CustomResourceDefinition = serde_json::from_value(json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.stable.example.com"},
            "spec": {
                "group": "stable.example.com", "scope": "Namespaced",
                "names": {"plural": "widgets", "kind": "Widget", "listKind": "WidgetList"},
                "versions": [{"name": "v1", "served": true, "storage": true,
                    "subresources": {"status": {}},
                    "schema": {"openAPIV3Schema": {"type": "object", "properties": {
                        "spec": spec,
                        "status": {"type": "object", "properties": {
                            "s": {"type": "string"}}}
                    }}}}]
            }
        }))
        .unwrap();
        Strategy::new(Arc::new(crd), "v1")
    }

    fn cr(spec: serde_json::Value) -> CustomResource {
        serde_json::from_value(json!({
            "apiVersion": "stable.example.com/v1", "kind": "Widget",
            "metadata": {"name": "w", "namespace": "ns"}, "spec": spec
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(Some("ns"))
    }

    fn has(errs: &ErrorList, ty: ErrorType, field: &str) -> bool {
        errs.iter().any(|e| e.error_type == ty && e.field == field)
    }

    fn blocking_count(errs: &ErrorList) -> usize {
        errs.iter().filter(|e| e.detail == BLOCKING).count()
    }

    /// strategy.go:211 + objectmeta/validation_test.go `TestValidate`
    /// ("embedded"): an embedded resource needs apiVersion and kind.
    #[test]
    fn create_requires_api_version_and_kind_on_embedded_resource() {
        let s = strategy(false);
        let errs = s.validate(&ctx(), &cr(json!({"need": "x", "embedded": {}})));
        assert!(
            has(&errs, ErrorType::Required, "spec.embedded.apiVersion"),
            "{errs:?}"
        );
        assert!(
            has(&errs, ErrorType::Required, "spec.embedded.kind"),
            "{errs:?}"
        );
        assert_eq!(errs.len(), 2, "{errs:?}");
    }

    /// An embedded resource's `metadata` is validated with
    /// `ValidateObjectMeta` and a path-segment name check
    /// (validation_test.go `TestValidateEmbeddedResource` "invalid").
    #[test]
    fn create_validates_embedded_metadata() {
        let s = strategy(false);
        let errs = s.validate(
            &ctx(),
            &cr(json!({"need": "x", "embedded": {
                "apiVersion": "foo/v1", "kind": "Foo",
                "metadata": {"name": "..", "namespace": "$$$"}}})),
        );
        assert!(
            has(&errs, ErrorType::Invalid, "spec.embedded.metadata.name"),
            "{errs:?}"
        );
        assert!(
            has(
                &errs,
                ErrorType::Invalid,
                "spec.embedded.metadata.namespace"
            ),
            "{errs:?}"
        );
        let ok = s.validate(
            &ctx(),
            &cr(json!({"need": "x", "embedded": {
                "apiVersion": "foo/v1", "kind": "Foo", "metadata": {"name": "foo"}}})),
        );
        assert!(ok.is_empty(), "{ok:?}");
    }

    /// strategy.go:296-297: update checks the embedded objects too, with no
    /// ratcheting against the old object.
    #[test]
    fn update_validates_embedded_resource_without_ratcheting() {
        let s = strategy(false);
        let old = cr(json!({"need": "x", "embedded": {}}));
        let errs = s.validate_update(&ctx(), &old, &old);
        assert!(
            has(&errs, ErrorType::Required, "spec.embedded.apiVersion"),
            "{errs:?}"
        );
    }

    /// status_strategy.go:88-135 does not run `schemaobjectmeta.Validate`.
    #[test]
    fn status_update_does_not_validate_embedded_resource() {
        let st = StatusStrategy(Arc::new(strategy(false)));
        let old = cr(json!({"need": "x", "embedded": {}}));
        let errs = st.validate_update(&ctx(), &old, &old);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// strategy.go:216-224: a blocking error (here `required`) suppresses the
    /// CEL rules and adds the "not checked" error instead.
    #[test]
    fn create_blocking_error_skips_cel() {
        let s = strategy(true);
        let errs = s.validate(&ctx(), &cr(json!({"n": 50})));
        assert!(has(&errs, ErrorType::Required, "spec.need"), "{errs:?}");
        assert_eq!(blocking_count(&errs), 1, "{errs:?}");
        assert!(
            !errs.iter().any(|e| e.to_string().contains("n too big")),
            "{errs:?}"
        );
        let blocking = errs.iter().find(|e| e.detail == BLOCKING).unwrap();
        assert_eq!(blocking.error_type, ErrorType::Invalid);
        assert_eq!(blocking.field, "");
    }

    /// A wrong type is a blocking error too (`ErrorTypeTypeInvalid`).
    #[test]
    fn create_wrong_type_is_blocking() {
        let s = strategy(true);
        let errs = s.validate(&ctx(), &cr(json!({"need": "x", "n": "ten"})));
        assert_eq!(blocking_count(&errs), 1, "{errs:?}");
    }

    /// Without a blocking error the CEL rules run.
    #[test]
    fn create_without_blocking_error_runs_cel() {
        let s = strategy(true);
        let errs = s.validate(&ctx(), &cr(json!({"need": "x", "n": 50})));
        assert_eq!(blocking_count(&errs), 0, "{errs:?}");
        assert!(
            errs.iter().any(|e| e.to_string().contains("n too big")),
            "{errs:?}"
        );
    }

    /// `celValidator == nil` when the schema has no rules: no gate message.
    #[test]
    fn no_cel_rules_means_no_blocking_message() {
        let s = strategy(false);
        let errs = s.validate(&ctx(), &cr(json!({"n": 50})));
        assert!(has(&errs, ErrorType::Required, "spec.need"), "{errs:?}");
        assert_eq!(blocking_count(&errs), 0, "{errs:?}");
    }

    /// strategy.go:304-312 and status_strategy.go:123-131 gate update the same
    /// way.
    #[test]
    fn update_and_status_update_gate_cel() {
        let s = Arc::new(strategy(true));
        let old = cr(json!({"need": "x"}));
        let new = cr(json!({"n": 50}));
        let errs = s.validate_update(&ctx(), &new, &old);
        assert_eq!(blocking_count(&errs), 1, "{errs:?}");
        let errs = StatusStrategy(s.clone()).validate_update(&ctx(), &new, &old);
        assert_eq!(blocking_count(&errs), 1, "{errs:?}");
    }
}

mod list_type_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::customresource::*;
    use serde_json::json;

    fn strategy() -> Strategy {
        let crd: CustomResourceDefinition = serde_json::from_value(json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.stable.example.com"},
            "spec": {
                "group": "stable.example.com", "scope": "Namespaced",
                "names": {"plural": "widgets", "kind": "Widget", "listKind": "WidgetList"},
                "versions": [{"name": "v1", "served": true, "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object", "properties": {
                        "spec": {"type": "object", "properties": {
                            "tags": {"type": "array", "x-kubernetes-list-type": "set",
                                     "items": {"type": "string"}}
                        }}
                    }}}}]
            }
        }))
        .unwrap();
        Strategy::new(Arc::new(crd), "v1")
    }

    fn cr(tags: serde_json::Value) -> CustomResource {
        serde_json::from_value(json!({
            "apiVersion": "stable.example.com/v1", "kind": "Widget",
            "metadata": {"name": "w"}, "spec": {"tags": tags}
        }))
        .unwrap()
    }

    /// strategy.go:300-302: an update is checked only when the old object
    /// was clean, so an object stored with duplicates stays updatable.
    #[test]
    fn update_ratchets_when_the_old_object_already_had_duplicates() {
        let s = strategy();
        let old = cr(json!(["a", "a"]));
        let new = cr(json!(["a", "a", "b"]));
        assert!(s.list_type_errors(&new, Some(&old)).is_empty());
    }

    #[test]
    fn update_checks_when_the_old_object_was_clean() {
        let s = strategy();
        let old = cr(json!(["a"]));
        let new = cr(json!(["a", "a"]));
        let errs = s.list_type_errors(&new, Some(&old));
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert_eq!(errs[0].field, "spec.tags[1]");
    }

    #[test]
    fn create_always_checks() {
        let errs = strategy().list_type_errors(&cr(json!(["a", "a"])), None);
        assert_eq!(errs.len(), 1, "{errs:?}");
    }
}

mod field_converter_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::apiextensions::customresource::*;
    use serde_json::json;

    fn strategy(versions: &[&str], conversion: Option<serde_json::Value>) -> Strategy {
        let versions: Vec<_> = versions
            .iter()
            .enumerate()
            .map(|(i, v)| json!({"name": v, "served": true, "storage": i == 0}))
            .collect();
        let mut spec = json!({
            "group": "stable.example.com", "scope": "Namespaced",
            "names": {"plural": "widgets", "kind": "Widget", "listKind": "WidgetList"},
            "versions": versions
        });
        if let Some(c) = conversion {
            spec["conversion"] = c;
        }
        let crd: CustomResourceDefinition = serde_json::from_value(json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.stable.example.com"},
            "spec": spec
        }))
        .unwrap();
        Strategy::new(Arc::new(crd), "v1")
    }

    /// customresource_handler.go:1133-1142: a multi-version CRD with
    /// `strategy: None` converts with `nopConverter` through the storage
    /// version.
    #[test]
    fn a_multi_version_crd_with_no_conversion_gets_the_nop_converter() {
        let s = strategy(&["v1", "v2"], Some(json!({"strategy": "None"})));
        let c = s.field_converter().expect("a converter");
        let out = c
            .convert(
                &json!({"apiVersion": "stable.example.com/v2", "spec": {"a": 1}}),
                "stable.example.com/v1",
            )
            .unwrap();
        assert_eq!(out["apiVersion"], "stable.example.com/v1");
        assert_eq!(out["spec"]["a"], 1);
    }

    #[test]
    fn a_single_version_or_webhook_crd_has_none() {
        assert!(strategy(&["v1"], None).field_converter().is_none());
        let webhook = json!({"strategy": "Webhook", "webhook": {
            "conversionReviewVersions": ["v1"], "clientConfig": {"url": "https://x"}}});
        assert!(strategy(&["v1", "v2"], Some(webhook))
            .field_converter()
            .is_none());
    }
}
