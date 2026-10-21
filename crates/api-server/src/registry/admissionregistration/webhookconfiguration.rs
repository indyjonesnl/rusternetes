//! ValidatingWebhookConfiguration and MutatingWebhookConfiguration strategies
//! and storage — port of
//! `pkg/registry/admissionregistration/{validating,mutating}webhookconfiguration/strategy.go`
//! and their `storage/storage.go`. The two files are identical but for the
//! type, so one macro stamps out both. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::cel_versions::{self, EnvSet, EnvType};
use rusternetes_common::resources::admission_webhook::MatchCondition;
use rusternetes_common::resources::{MutatingWebhookConfiguration, ValidatingWebhookConfiguration};
use rusternetes_common::validation::field::{Error, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::webhookconfiguration::{
    find_mutating_preexisting_expressions, find_validating_preexisting_expressions,
    ignore_mutating_webhook_match_conditions, ignore_validating_webhook_match_conditions,
    set_defaults_mutating_webhook_configuration, set_defaults_validating_webhook_configuration,
    validate_mutating_webhook_configuration, validate_mutating_webhook_configuration_update,
    validate_validating_webhook_configuration, validate_validating_webhook_configuration_update,
    PreexistingExpressions,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

pub(super) use rusternetes_common::cel::compile_failure;
use rusternetes_common::cel_env::{output_type_failure, ExpectedOutput};

/// The compile half of `validateMatchCondition` (validation.go:984-997): each
/// non-blank `matchConditions[j].expression` of each webhook, as a
/// `field.Invalid` on `webhooks[i].matchConditions[j].expression`. The shape
/// rules ran in the ported `validate_match_conditions`.
///
/// `envType` is `StoredExpressions` when the trimmed expression is one the old
/// configuration stores, else `NewExpressions` (`validateMatchConditionsExpression`,
/// validation.go:1100-1112), compiled against
/// `MustBaseEnvSet(DefaultCompatibilityVersion())` (`getStrictStatelessCELCompiler`,
/// :1309-1314).
fn match_condition_compile_errors<'a>(
    hooks: impl Iterator<Item = Option<&'a Vec<MatchCondition>>>,
    preexisting: &PreexistingExpressions,
) -> ErrorList {
    match_condition_compile_errors_in(
        hooks,
        preexisting,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

fn match_condition_compile_errors_in<'a>(
    hooks: impl Iterator<Item = Option<&'a Vec<MatchCondition>>>,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs = ErrorList::new();
    for (i, conditions) in hooks.enumerate() {
        for (j, condition) in conditions.into_iter().flatten().enumerate() {
            let expression = condition.expression.trim();
            if expression.is_empty() {
                continue;
            }
            let env_type = if preexisting.match_condition_expressions.contains(expression) {
                EnvType::StoredExpressions
            } else {
                EnvType::NewExpressions
            };
            if let Some(detail) = compile_failure(expression)
                .or_else(|| output_type_failure(expression, ExpectedOutput::Bool))
                .or_else(|| cel_versions::compile_failure(expression, env_set, env_type))
            {
                errs.push(Error::invalid(
                    &Path::new("webhooks")
                        .index(i)
                        .child("matchConditions")
                        .index(j)
                        .child("expression"),
                    expression.to_string(),
                    detail,
                ));
            }
        }
    }
    errs
}

macro_rules! webhook_configuration {
    (
        $module:ident, $ty:ident, $resource:literal,
        defaults: $defaults:ident,
        validate: $validate:ident,
        validate_update: $validate_update:ident,
        ignore_match_conditions: $ignore:ident,
        preexisting: $preexisting:ident
    ) => {
        pub mod $module {
            use super::*;

            /// `SetObjectDefaults_*WebhookConfiguration`
            /// (`pkg/apis/admissionregistration/v1/zz_generated.defaults.go`).
            pub fn convert_to_internal(cfg: &mut $ty) {
                $defaults(cfg);
            }

            fn webhooks_json(cfg: &$ty) -> Option<serde_json::Value> {
                serde_json::to_value(&cfg.webhooks).ok()
            }

            fn object_meta_errors(cfg: &$ty) -> ErrorList {
                validate_object_meta(
                    &cfg.metadata,
                    false,
                    name_is_dns_subdomain,
                    &Path::new("metadata"),
                )
            }

            /// The strategy (strategy.go `Strategy`).
            pub struct Strategy;

            impl NamespaceScopedStrategy for Strategy {
                fn namespace_scoped(&self) -> bool {
                    false
                }
            }

            impl RestCreateStrategy<$ty> for Strategy {
                /// `PrepareForCreate`: the generation starts at 1.
                fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut $ty) {
                    obj.metadata.generation = Some(1);
                }

                /// `Validate`: ObjectMeta with `NameIsDNSSubdomain`, then the
                /// webhooks and their `matchConditions` expressions.
                fn validate(&self, _ctx: &RequestContext, obj: &$ty) -> ErrorList {
                    let mut errs = object_meta_errors(obj);
                    errs.extend($validate(obj));
                    errs.extend(match_condition_compile_errors(
                        obj.webhooks
                            .iter()
                            .flatten()
                            .map(|h| h.match_conditions.as_ref()),
                        &PreexistingExpressions::default(),
                    ));
                    errs
                }
            }

            impl RestUpdateStrategy<$ty> for Strategy {
                fn allow_create_on_update(&self) -> bool {
                    false
                }

                /// `PrepareForUpdate`: a change to `webhooks` increments the
                /// generation.
                fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut $ty, old: &$ty) {
                    if webhooks_json(obj) != webhooks_json(old) {
                        obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
                    }
                }

                /// `Validate*WebhookConfigurationUpdate` (validation.go:728-753).
                /// An unchanged `matchConditions` list is not compiled again,
                /// as `ignoreMatchConditions` spares it.
                fn validate_update(
                    &self,
                    _ctx: &RequestContext,
                    obj: &$ty,
                    old: &$ty,
                ) -> ErrorList {
                    let mut errs = object_meta_errors(obj);
                    errs.extend($validate_update(obj, old));
                    if !$ignore(obj, old) {
                        errs.extend(match_condition_compile_errors(
                            obj.webhooks
                                .iter()
                                .flatten()
                                .map(|h| h.match_conditions.as_ref()),
                            &$preexisting(old),
                        ));
                    }
                    errs
                }

                fn allow_unconditional_update(&self) -> bool {
                    false
                }
            }

            impl RestDeleteStrategy<$ty> for Strategy {}

            /// `NewREST` (storage/storage.go).
            pub fn new_store(storage: Arc<StorageBackend>) -> Store<$ty, StorageBackend> {
                Store::new(
                    storage,
                    GroupResource::new("admissionregistration.k8s.io", $resource),
                    Arc::new(Strategy),
                )
                .with_decode_defaulter(convert_to_internal)
            }
        }
    };
}

webhook_configuration!(
    validating,
    ValidatingWebhookConfiguration,
    "validatingwebhookconfigurations",
    defaults: set_defaults_validating_webhook_configuration,
    validate: validate_validating_webhook_configuration,
    validate_update: validate_validating_webhook_configuration_update,
    ignore_match_conditions: ignore_validating_webhook_match_conditions,
    preexisting: find_validating_preexisting_expressions
);

webhook_configuration!(
    mutating,
    MutatingWebhookConfiguration,
    "mutatingwebhookconfigurations",
    defaults: set_defaults_mutating_webhook_configuration,
    validate: validate_mutating_webhook_configuration,
    validate_update: validate_mutating_webhook_configuration_update,
    ignore_match_conditions: ignore_mutating_webhook_match_conditions,
    preexisting: find_mutating_preexisting_expressions
);

#[cfg(test)]
mod preexisting_expression_tests {
    use super::*;
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
