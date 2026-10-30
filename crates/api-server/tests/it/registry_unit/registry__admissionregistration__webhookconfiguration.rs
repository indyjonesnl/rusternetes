//! Unit tests moved from `src/registry/admissionregistration/webhookconfiguration.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::cel::compile_failure;
    pub use rusternetes_common::cel_env::{output_type_failure, ExpectedOutput};
    pub use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
    pub use rusternetes_common::resources::admission_webhook::MatchCondition;
    pub use rusternetes_common::resources::{
        MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
    };
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta,
    };
    pub use rusternetes_common::validation::webhookconfiguration::{
        find_mutating_preexisting_expressions, find_validating_preexisting_expressions,
        ignore_mutating_webhook_match_conditions, ignore_validating_webhook_match_conditions,
        set_defaults_mutating_webhook_configuration, set_defaults_validating_webhook_configuration,
        validate_mutating_webhook_configuration, validate_mutating_webhook_configuration_update,
        validate_validating_webhook_configuration,
        validate_validating_webhook_configuration_update, PreexistingExpressions,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod preexisting_expression_tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::admissionregistration::webhookconfiguration::*;
    use rusternetes_common::feature_gates::Version;
    use serde_json::json;

    const SEMVER: &str = "semver('1.2.3').major() > 0";

    fn config(expression: &str) -> ValidatingWebhookConfiguration {
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfiguration", "metadata": {"name": "foo"}, "webhooks": [{
            "name": "a.example.com", "sideEffects": "None", "admissionReviewVersions": ["v1"],
            "clientConfig": {"url": "https://example.com"},
            "matchConditions": [{"name": "c", "expression": expression}]}]})).unwrap()
    }

    fn errors(
        new: &ValidatingWebhookConfiguration,
        old: Option<&ValidatingWebhookConfiguration>,
    ) -> String {
        let preexisting = old
            .map(find_validating_preexisting_expressions)
            .unwrap_or_default();
        let env = EnvSet::must_base_env_set(Version::major_minor(1, 32));
        match_condition_compile_errors_in(
            new.webhooks
                .iter()
                .flatten()
                .map(|h| h.match_conditions.as_ref()),
            &preexisting,
            &env,
        )
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
    }

    /// `validateMatchConditionsExpression` (validation.go:1100-1112) via
    /// `findValidatingPreexistingExpressions` (:297-306): new expressions are
    /// version gated, stored ones are not.
    #[test]
    fn stored_match_condition_compiles_in_the_stored_environment() {
        let c = config(SEMVER);
        let got = errors(&c, None);
        assert!(
            got.contains("webhooks[0].matchConditions[0].expression")
                && got.contains("undeclared reference to 'semver'"),
            "{got}"
        );
        assert_eq!(errors(&c, Some(&c)), "");
        let other = config("semver('1.2.3').minor() > 0");
        assert!(errors(&other, Some(&c)).contains("undeclared reference to 'semver'"));
    }

    /// The mutating finder collects the same set (:288-295).
    #[test]
    fn mutating_finder_collects_every_webhooks_expressions() {
        let m: MutatingWebhookConfiguration = serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "MutatingWebhookConfiguration", "metadata": {"name": "foo"}, "webhooks": [
            {"name": "a.example.com", "sideEffects": "None", "admissionReviewVersions": ["v1"], "clientConfig": {"url": "https://example.com"}, "matchConditions": [{"name": "c", "expression": "true"}]},
            {"name": "b.example.com", "sideEffects": "None", "admissionReviewVersions": ["v1"], "clientConfig": {"url": "https://example.com"}, "matchConditions": [{"name": "c", "expression": "false"}]}]})).unwrap();
        let pre = find_mutating_preexisting_expressions(&m);
        assert!(pre.match_condition_expressions.contains("true"));
        assert!(pre.match_condition_expressions.contains("false"));
    }
}
