//! ValidatingWebhookConfiguration and MutatingWebhookConfiguration strategies
//! and storage — port of
//! `pkg/registry/admissionregistration/{validating,mutating}webhookconfiguration/strategy.go`
//! and their `storage/storage.go`. The two files are identical but for the
//! type, so one macro stamps out both. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::resources::admission_webhook::MatchCondition;
use rusternetes_common::resources::{MutatingWebhookConfiguration, ValidatingWebhookConfiguration};
use rusternetes_common::validation::field::{Error, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::webhookconfiguration::{
    ignore_mutating_webhook_match_conditions, ignore_validating_webhook_match_conditions,
    set_defaults_mutating_webhook_configuration, set_defaults_validating_webhook_configuration,
    validate_mutating_webhook_configuration, validate_mutating_webhook_configuration_update,
    validate_validating_webhook_configuration, validate_validating_webhook_configuration_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// A CEL error that comes from a declaration this server does not supply,
/// rather than from the expression being malformed.
fn is_missing_declaration(err: &str) -> bool {
    let err = err.to_lowercase();
    err.contains("no such key")
        || err.contains("not found")
        || err.contains("undeclared")
        || err.contains("undefined")
        || err.contains("no matching overload")
}

/// Why `expression` does not compile, if it does not.
///
/// Upstream compiles against a typed CEL environment
/// (`validateMatchConditionsExpression`, validation.go:1100). Rusternetes has
/// no typed environment, so this compiles with the plain `cel` crate and
/// tolerates the errors that come from the missing declarations rather than
/// from the expression itself.
fn compile_failure(expression: &str) -> Option<String> {
    // The antlr4rust parser panics on some invalid expressions instead of
    // returning `Err`.
    let source = expression.to_string();
    let program = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cel::Program::compile(&source)
    })) {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            // The CEL crate type-checks at compile time and rejects references
            // to declarations we do not supply (`object.metadata`), which are
            // valid at admission time.
            return (!is_missing_declaration(&e.to_string()))
                .then(|| format!("compilation failed: {e}"));
        }
        Err(_) => {
            return Some(format!(
                "compilation failed: invalid CEL expression '{expression}'"
            ))
        }
    };

    // The CEL crate's parser accepts some expressions Kubernetes rejects.
    // Executing with an empty context catches the genuinely invalid ones.
    let ctx = cel::Context::default();
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| program.execute(&ctx))) {
        Ok(Ok(_)) => None,
        Ok(Err(e)) => {
            (!is_missing_declaration(&e.to_string())).then(|| format!("compilation failed: {e}"))
        }
        Err(_) => Some(format!(
            "compilation failed: invalid CEL expression '{expression}'"
        )),
    }
}

/// The compile half of `validateMatchCondition` (validation.go:984-997): each
/// non-blank `matchConditions[j].expression` of each webhook, as a
/// `field.Invalid` on `webhooks[i].matchConditions[j].expression`. The shape
/// rules ran in the ported `validate_match_conditions`.
fn match_condition_compile_errors<'a>(
    hooks: impl Iterator<Item = Option<&'a Vec<MatchCondition>>>,
) -> ErrorList {
    let mut errs = ErrorList::new();
    for (i, conditions) in hooks.enumerate() {
        for (j, condition) in conditions.into_iter().flatten().enumerate() {
            let expression = condition.expression.trim();
            if expression.is_empty() {
                continue;
            }
            if let Some(detail) = compile_failure(expression) {
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
        ignore_match_conditions: $ignore:ident
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
    ignore_match_conditions: ignore_validating_webhook_match_conditions
);

webhook_configuration!(
    mutating,
    MutatingWebhookConfiguration,
    "mutatingwebhookconfigurations",
    defaults: set_defaults_mutating_webhook_configuration,
    validate: validate_mutating_webhook_configuration,
    validate_update: validate_mutating_webhook_configuration_update,
    ignore_match_conditions: ignore_mutating_webhook_match_conditions
);
