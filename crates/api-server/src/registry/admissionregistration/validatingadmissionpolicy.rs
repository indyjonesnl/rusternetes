//! ValidatingAdmissionPolicy strategy and storage — port of
//! `pkg/registry/admissionregistration/validatingadmissionpolicy/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};
use rusternetes_common::cel_versions::{self, EnvSet, EnvType};

use rusternetes_common::resources::validating_admission_policy::ValidatingAdmissionPolicyStatus;
use rusternetes_common::resources::ValidatingAdmissionPolicy;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::validating_admission_policy::{
    find_validating_policy_preexisting_expressions,
    ignore_validating_admission_policy_match_conditions, set_defaults_validating_admission_policy,
    validate_validating_admission_policy, validate_validating_admission_policy_status_update,
    PreexistingExpressions, MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH,
};
use rusternetes_storage::StorageBackend;

use super::authz::{authorize_param_kind, ParamAuthz};
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
};

/// `SetObjectDefaults_ValidatingAdmissionPolicy`
/// (`pkg/apis/admissionregistration/v1/zz_generated.defaults.go`).
pub fn convert_to_internal(policy: &mut ValidatingAdmissionPolicy) {
    set_defaults_validating_admission_policy(policy);
}

/// A Go `ValidatingAdmissionPolicyStatus{}`: the status is not a pointer.
fn empty_status() -> ValidatingAdmissionPolicyStatus {
    ValidatingAdmissionPolicyStatus {
        observed_generation: None,
        type_checking: None,
        conditions: None,
    }
}

fn spec_json(obj: &ValidatingAdmissionPolicy) -> Option<serde_json::Value> {
    serde_json::to_value(&obj.spec).ok()
}

/// The CEL compile of `validateValidatingAdmissionPolicySpec`
/// (validation.go:803-828) that needs the composited compiler, in upstream's
/// order: `validateVariable` (:999-1032), then per validation
/// `validateValidation` (:1034-1061: `validateValidationExpression` and
/// `validateMessageExpression`) and `validateAuditAnnotation` (:1128-1166).
///
/// `getCompiler` (:773-779) builds the composited compiler only when the
/// policy has `variables` and the stateless one otherwise (`createCompiler`,
/// :1316-1327). `CompileAndStoreVariable` (composition.go:95-100) adds each
/// variable as a field of `variables` only after compiling it, so a variable
/// sees just the ones before it; validations and audit annotations (compiled
/// after the whole variables loop) see all of them. `HasParams` is
/// `paramKind != nil` (`opts.allowParamsInMatchConditions` for
/// `messageExpression`, set at :790-792); `HasAuthorizer` is true except for
/// `messageExpression` (:1124) (`compilePolicy`, validating/plugin.go:118-119).
/// Output types: `Variable` any (interface.go:74-76), `ValidationCondition`
/// bool (:46-48), `MessageExpressionCondition` string (message.go:34-36),
/// `AuditAnnotationCondition` string or null (:60-62); inference-dependent
/// checks are #3277. Each expression is also compiled in the CEL library
/// environment of its `envType` (`StoredExpressions` when the old policy's
/// `preexistingExpressions` hold it, else `NewExpressions`; `:1012`, `:1088`,
/// `:1114`, `:1143`), keyed by the untrimmed expression. Upstream replicated:
/// `validateVariable` keys on `validationExpressions` (:1012), there being no
/// set of variable expressions.
fn message_expression_errors(
    obj: &ValidatingAdmissionPolicy,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    #[allow(clippy::too_many_arguments)]
    fn push(
        errs: &mut ErrorList,
        path: Path,
        value: &str,
        compiled: &str,
        env: &MutationEnv,
        expected: ExpectedOutput,
        stored: &std::collections::HashSet<String>,
        env_set: &EnvSet,
    ) {
        if let Some(detail) = mutation_env_failure(compiled, env, expected).or_else(|| {
            cel_versions::compile_failure(compiled, env_set, env_type_of(stored, value))
        }) {
            errs.push(FieldError::invalid(&path, value.to_string(), detail));
        }
    }
    let mut errs = ErrorList::new();
    let Some(spec) = &obj.spec else {
        return errs;
    };
    let has_params = spec.param_kind.is_some();
    let has_variables = spec.variables.as_ref().is_some_and(|v| !v.is_empty());
    let all_names: std::collections::BTreeSet<String> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| v.name.clone())
        .filter(|n| !n.trim().is_empty())
        .collect();
    // validateVariable (:999-1032).
    let mut seen = std::collections::BTreeSet::new();
    for (i, v) in spec.variables.iter().flatten().enumerate() {
        if !v.expression.trim().is_empty() {
            let env = MutationEnv {
                has_params,
                has_authorizer: true,
                has_patch_types: false,
                variables: Some(seen.clone()),
            };
            push(
                &mut errs,
                Path::new("spec")
                    .child("variables")
                    .index(i)
                    .child("expression"),
                &v.expression,
                &v.expression,
                &env,
                ExpectedOutput::Any,
                &preexisting.validation_expressions,
                env_set,
            );
        }
        if !v.name.trim().is_empty() {
            seen.insert(v.name.clone());
        }
    }
    let env_for = |has_authorizer: bool| MutationEnv {
        has_params,
        has_authorizer,
        has_patch_types: false,
        variables: has_variables.then(|| all_names.clone()),
    };
    // validateValidation (:1034-1061).
    for (i, v) in spec.validations.iter().flatten().enumerate() {
        let base = Path::new("spec").child("validations").index(i);
        if !v.expression.trim().is_empty() {
            push(
                &mut errs,
                base.child("expression"),
                &v.expression,
                &v.expression,
                &env_for(true),
                ExpectedOutput::Bool,
                &preexisting.validation_expressions,
                env_set,
            );
        }
        if let Some(expression) = v.message_expression.as_deref() {
            if !expression.trim().is_empty() {
                push(
                    &mut errs,
                    base.child("messageExpression"),
                    expression,
                    expression,
                    &env_for(false),
                    ExpectedOutput::String,
                    &preexisting.validation_message_expressions,
                    env_set,
                );
            }
        }
    }
    // validateAuditAnnotation (:1128-1166): a blank or over-long expression is
    // reported by the structural validation instead.
    for (i, a) in spec.audit_annotations.iter().flatten().enumerate() {
        let trimmed = a.value_expression.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_AUDIT_ANNOTATION_VALUE_EXPRESSION_LENGTH {
            continue;
        }
        push(
            &mut errs,
            Path::new("spec")
                .child("auditAnnotations")
                .index(i)
                .child("valueExpression"),
            &a.value_expression,
            trimmed,
            &env_for(true),
            ExpectedOutput::StringOrNull,
            &preexisting.audit_annotation_values_expressions,
            env_set,
        );
    }
    errs
}

/// The `envType` selection every `validate*Expression` makes: the expression
/// is `StoredExpressions` when the old policy's `preexistingExpressions` hold
/// it, else `NewExpressions` (validation.go:1012-1015, :1088-1092, :1100-1104,
/// :1114-1118, :1143-1146).
fn env_type_of(stored: &std::collections::HashSet<String>, expression: &str) -> EnvType {
    if stored.contains(expression) {
        EnvType::StoredExpressions
    } else {
        EnvType::NewExpressions
    }
}

/// `validateValidatingAdmissionPolicy` (validation.go:762-770): ObjectMeta
/// with `NameIsDNSSubdomain`, then the spec.
fn validate(obj: &ValidatingAdmissionPolicy, old: Option<&ValidatingAdmissionPolicy>) -> ErrorList {
    let preexisting = old
        .map(find_validating_policy_preexisting_expressions)
        .unwrap_or_default();
    validate_in(
        obj,
        &preexisting,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

fn validate_in(
    obj: &ValidatingAdmissionPolicy,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_validating_admission_policy(obj));
    errs.extend(message_expression_errors(obj, preexisting, env_set));
    errs
}

/// The CEL compile of `spec.matchConditions` in
/// `validateValidatingAdmissionPolicySpec` (validation.go:800-802): each
/// non-blank expression goes through `validateMatchConditionsExpression`
/// (:1100-1112) and `convertCELErrorToValidationError` (:1073-1085) unless
/// `ignore_match_conditions` (`ignoreValidatingAdmissionPolicyMatchConditions`,
/// :619-628). The stateless compiler has `params` only with a `paramKind`
/// (`allowParamsInMatchConditions`, :788) and no `variables`; the output must
/// be `bool` (`MatchCondition.ReturnTypes`, matcher.go:49-51;
/// compile.go:189-203). Inference-dependent output types are #3277.
fn compile_errors(
    obj: &ValidatingAdmissionPolicy,
    ignore_match_conditions: bool,
    old: Option<&ValidatingAdmissionPolicy>,
) -> ErrorList {
    compile_errors_in(
        obj,
        ignore_match_conditions,
        &old.map(find_validating_policy_preexisting_expressions)
            .unwrap_or_default(),
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

fn compile_errors_in(
    obj: &ValidatingAdmissionPolicy,
    ignore_match_conditions: bool,
    preexisting: &PreexistingExpressions,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(spec) = &obj.spec else {
        return errs;
    };
    if ignore_match_conditions {
        return errs;
    }
    let env = MutationEnv {
        has_params: spec.param_kind.is_some(),
        has_patch_types: false,
        has_authorizer: true,
        variables: None,
    };
    for (i, c) in spec.match_conditions.iter().flatten().enumerate() {
        let expression = c.expression.trim();
        if expression.is_empty() {
            continue;
        }
        // `validateMatchConditionsExpression` (:1100-1104), keyed by the trimmed
        // expression `validateMatchCondition` passes (:988-991).
        let env_type = env_type_of(&preexisting.match_condition_expressions, expression);
        if let Some(detail) = mutation_env_failure(expression, &env, ExpectedOutput::Bool)
            .or_else(|| cel_versions::compile_failure(expression, env_set, env_type))
        {
            let path = Path::new("spec")
                .child("matchConditions")
                .index(i)
                .child("expression");
            errs.push(FieldError::invalid(&path, expression.to_string(), detail));
        }
    }
    errs
}

/// `validatingAdmissionPolicyStrategy` (strategy.go:36-45).
#[derive(Default)]
pub struct Strategy {
    /// `authorizer` / `resourceResolver`; `None` skips the `paramKind` check.
    pub authz: Option<ParamAuthz>,
}

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ValidatingAdmissionPolicy> for Strategy {
    /// `PrepareForCreate` (strategy.go:56-60): clears the status and starts
    /// the generation at 1.
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ValidatingAdmissionPolicy) {
        obj.status = Some(empty_status());
        obj.metadata.generation = Some(1);
    }

    fn validate(&self, _ctx: &RequestContext, obj: &ValidatingAdmissionPolicy) -> ErrorList {
        let mut errs = validate(obj, None);
        errs.extend(compile_errors(obj, false, None));
        errs
    }

    /// The `paramKind` authorization of `Validate` (strategy.go:83-92), run
    /// only when the rest of `Validate` found nothing (`len(errs) == 0`).
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ValidatingAdmissionPolicy,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestCreateStrategy::validate(self, ctx, obj).is_empty() {
                return Vec::new();
            }
            authorize_param_kind(ctx, authz.authorizer.as_ref(), &authz.storage, obj, None).await
        })
    }
}

impl RestUpdateStrategy<ValidatingAdmissionPolicy> for Strategy {
    /// strategy.go:91-93.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// `PrepareForUpdate` (strategy.go:63-78): the status is not writable
    /// here, and a spec change increments the generation.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicy,
        old: &ValidatingAdmissionPolicy,
    ) {
        obj.status = old.status.clone();
        if spec_json(obj) != spec_json(old) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }

    /// `ValidateValidatingAdmissionPolicyUpdate` (validation.go:1239-1244).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ValidatingAdmissionPolicy,
        old: &ValidatingAdmissionPolicy,
    ) -> ErrorList {
        let mut errs = validate(obj, Some(old));
        errs.extend(compile_errors(
            obj,
            ignore_validating_admission_policy_match_conditions(obj, old),
            Some(old),
        ));
        errs
    }

    /// The `paramKind` authorization of `ValidateUpdate` (strategy.go:109-119).
    fn validate_update_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ValidatingAdmissionPolicy,
        old: &'a ValidatingAdmissionPolicy,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let Some(authz) = &self.authz else {
                return Vec::new();
            };
            if !RestUpdateStrategy::validate_update(self, ctx, obj, old).is_empty() {
                return Vec::new();
            }
            authorize_param_kind(
                ctx,
                authz.authorizer.as_ref(),
                &authz.storage,
                obj,
                Some(old),
            )
            .await
        })
    }

    /// strategy.go:111-113.
    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

impl RestDeleteStrategy<ValidatingAdmissionPolicy> for Strategy {}

/// `validatingAdmissionPolicyStatusStrategy` (strategy.go:147-180).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<ValidatingAdmissionPolicy> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Setting the spec is not allowed, setting the status is; the generation
    /// does not change (strategy.go:164-175).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ValidatingAdmissionPolicy,
        old: &ValidatingAdmissionPolicy,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// `ValidateValidatingAdmissionPolicyStatusUpdate` (validation.go:1247).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ValidatingAdmissionPolicy,
        _old: &ValidatingAdmissionPolicy,
    ) -> ErrorList {
        validate_validating_admission_policy_status_update(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        false
    }
}

/// `NewREST` (storage/storage.go:49-80): the main store and the status store.
pub fn new_stores(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> (
    Store<ValidatingAdmissionPolicy, StorageBackend>,
    Store<ValidatingAdmissionPolicy, StorageBackend>,
) {
    let store = Store::new(
        storage.clone(),
        GroupResource::new(
            "admissionregistration.k8s.io",
            "validatingadmissionpolicies",
        ),
        Arc::new(Strategy {
            authz: Some(ParamAuthz {
                authorizer,
                storage,
            }),
        }),
    )
    .with_decode_defaulter(convert_to_internal);
    // The status strategy has no authorization (strategy.go:147-180).
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}

#[cfg(test)]
mod invalid_status_tests {
    use super::*;
    use crate::registry::generic::store::{CreateOptions, UpdateOptions};
    use crate::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;

    /// `ValidatingAdmissionPolicy` strategy `Validate` appends the authz error as a
    /// `field.Forbidden`, and `BeforeCreate` / `BeforeUpdate` wrap the list in
    /// `errors.NewInvalid(kind.GroupKind(), name, errs)`
    /// (rest/create.go:122-131, rest/update.go:149-158; errors.go:284-312), so
    /// the status is `<Kind>.<group> "<name>" is invalid: ...` with details.
    #[tokio::test]
    async fn authz_denial_is_a_new_invalid_status() {
        let obj: ValidatingAdmissionPolicy = serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "paramKind": {"kind": "ReplicaLimit", "apiVersion": "rules.example.com/v1"},
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "validations": [{"expression": "true"}],
            "failurePolicy": "Fail"}})).unwrap();
        assert!(
            validate(&obj, None).is_empty(),
            "{:?}",
            validate(&obj, None)
        );
        let storage = Arc::new(StorageBackend::Memory(Arc::new(
            rusternetes_storage::MemoryStorage::new(),
        )));
        let (store, _status) = new_stores(storage, Arc::new(AlwaysDenyAuthorizer));
        let user = UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        };
        let ctx = RequestContext::new(None).with_user(&user);
        let Err(Error::Status(st)) = store
            .create(&ctx, obj.clone(), None, &CreateOptions::default())
            .await
        else {
            panic!("expected a Status error from create")
        };
        assert_eq!(st.code, Some(422));
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
        let msg = st.message.clone().unwrap_or_default();
        assert!(
            msg.starts_with(r#"ValidatingAdmissionPolicy.admissionregistration.k8s.io "foo" is invalid: spec.paramKind: Forbidden: "#),
            "{msg}"
        );
        let d = st.details.unwrap();
        assert_eq!(d.group.as_deref(), Some("admissionregistration.k8s.io"));
        assert_eq!(d.kind.as_deref(), Some("ValidatingAdmissionPolicy"));
        assert_eq!(d.name.as_deref(), Some("foo"));

        let mut plain = obj.clone();
        plain.spec.as_mut().unwrap().param_kind = None;
        let created = store
            .create(&ctx, plain, None, &CreateOptions::default())
            .await
            .unwrap();
        let mut update = obj.clone();
        update.metadata.resource_version = created.metadata.resource_version.clone();
        let info = DefaultUpdatedObjectInfo::new(Some(update), Vec::new());
        let Err(Error::Status(st)) = store
            .update(
                &ctx,
                "foo",
                &info,
                None,
                None,
                false,
                &UpdateOptions::default(),
            )
            .await
        else {
            panic!("expected a Status error from update")
        };
        assert_eq!(st.reason.as_deref(), Some("Invalid"));
    }
}

#[cfg(test)]
mod param_kind_authz_ordering_tests {
    use super::*;
    use crate::registry::generic::store::{CreateOptions, UpdateOptions};
    use crate::registry::rest::DefaultUpdatedObjectInfo;
    use rusternetes_common::auth::UserInfo;
    use rusternetes_common::authz::AlwaysDenyAuthorizer;
    use rusternetes_common::Error;
    use rusternetes_storage::Storage;

    fn policy(param_kind: bool) -> ValidatingAdmissionPolicy {
        let mut spec = serde_json::json!({
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "validations": [{"expression": "true"}],
            "failurePolicy": "Fail"});
        if param_kind {
            spec["paramKind"] =
                serde_json::json!({"kind": "ReplicaLimit", "apiVersion": "rules.example.com/v1"});
        }
        serde_json::from_value(serde_json::json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": spec})).unwrap()
    }

    /// Upstream authorizes `paramKind` INSIDE `Validate` / `ValidateUpdate`
    /// (validatingadmissionpolicy/strategy.go:83-92, :109-119), which
    /// `BeforeUpdate` (rest/update.go:149-158) calls after
    /// `validateCommonFields`; both lists are returned in ONE `Invalid`. An
    /// update that adds a finalizer to an object being deleted
    /// (`ValidateObjectMetaUpdate`) and an unauthorized paramKind therefore
    /// reports both causes.
    #[tokio::test]
    async fn an_unauthorized_param_kind_is_reported_with_the_metadata_errors() {
        let storage = Arc::new(StorageBackend::Memory(Arc::new(
            rusternetes_storage::MemoryStorage::new(),
        )));
        let (store, _status) = new_stores(storage.clone(), Arc::new(AlwaysDenyAuthorizer));
        let user = UserInfo {
            username: "alice".into(),
            uid: String::new(),
            groups: vec![],
            extra: Default::default(),
        };
        let ctx = RequestContext::new(None).with_user(&user);
        let mut plain = policy(false);
        plain.metadata.finalizers = Some(vec!["a".to_string()]);
        store
            .create(&ctx, plain, None, &CreateOptions::default())
            .await
            .unwrap();
        // The object is now being deleted.
        let key = rusternetes_storage::build_key("validatingadmissionpolicies", None, "foo");
        let mut stored: ValidatingAdmissionPolicy = storage.get(&key).await.unwrap();
        stored.metadata.deletion_timestamp = Some(chrono::Utc::now());
        let stored = storage.update(&key, &stored).await.unwrap();

        let mut next = policy(true);
        next.metadata.finalizers = Some(vec!["a".to_string(), "b".to_string()]);
        next.metadata.resource_version = stored.metadata.resource_version.clone();
        let info = DefaultUpdatedObjectInfo::new(Some(next), Vec::new());
        let err = store
            .update(
                &ctx,
                "foo",
                &info,
                None,
                None,
                false,
                &UpdateOptions::default(),
            )
            .await
            .unwrap_err();
        let Error::Status(st) = err else {
            panic!("expected Status, got {err:?}")
        };
        let fields: Vec<_> = st
            .details
            .unwrap()
            .causes
            .unwrap()
            .into_iter()
            .filter_map(|c| c.field)
            .collect();
        assert!(fields.contains(&"spec.paramKind".to_string()), "{fields:?}");
        assert!(
            fields.contains(&"metadata.finalizers".to_string()),
            "{fields:?}"
        );
    }
}

#[cfg(test)]
mod match_condition_compile_tests {
    use super::*;
    use serde_json::json;

    fn policy(match_conditions: serde_json::Value) -> ValidatingAdmissionPolicy {
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "validations": [{"expression": "true"}],
            "matchConditions": match_conditions,
            "failurePolicy": "Fail"}})).unwrap()
    }

    fn messages(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// `validateMatchConditionsExpression` (validation.go:1100-1112) compiles
    /// with `MatchCondition.ReturnTypes = [bool]` (matcher.go:49-51), and
    /// `CompileCELExpression` rejects another output type (compile.go:189-203)
    /// (#3265).
    #[test]
    fn create_rejects_a_non_bool_match_condition() {
        let p = policy(json!([{"name": "c", "expression": "6"}]));
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(
            messages(&errs).contains(
                r#"spec.matchConditions[0].expression: Invalid value: "6": must evaluate to bool but got int"#
            ),
            "{}",
            messages(&errs)
        );
    }

    /// `validateMatchCondition` (validation.go:984-997).
    #[test]
    fn create_rejects_a_match_condition_that_does_not_compile() {
        let p = policy(json!([{"name": "c", "expression": "///"}]));
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(
            messages(&errs).contains(
                r#"spec.matchConditions[0].expression: Invalid value: "///": compilation failed"#
            ),
            "{}",
            messages(&errs)
        );
    }

    #[test]
    fn create_accepts_a_bool_match_condition() {
        let p = policy(json!([{"name": "c", "expression": "object.metadata.name == 'x'"}]));
        let errs =
            RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), &p);
        assert!(errs.is_empty(), "{}", messages(&errs));
    }

    /// `ignoreValidatingAdmissionPolicyMatchConditions` (validation.go:619-628):
    /// unchanged `matchConditions` / `paramKind` are not compiled again; a
    /// change is.
    #[test]
    fn update_skips_unchanged_match_conditions_only() {
        let old = policy(json!([{"name": "c", "expression": "6"}]));
        let unchanged = old.clone();
        let ctx = RequestContext::new(None);
        let errs =
            RestUpdateStrategy::validate_update(&Strategy::default(), &ctx, &unchanged, &old);
        assert!(errs.is_empty(), "{}", messages(&errs));
        let changed = policy(json!([{"name": "c", "expression": "7"}]));
        let errs = RestUpdateStrategy::validate_update(&Strategy::default(), &ctx, &changed, &old);
        assert!(
            messages(&errs).contains("spec.matchConditions[0].expression"),
            "{}",
            messages(&errs)
        );
    }
}

#[cfg(test)]
mod expression_compile_tests {
    use super::*;
    use serde_json::json;

    fn policy(spec_extra: serde_json::Value) -> ValidatingAdmissionPolicy {
        let mut spec = json!({
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "failurePolicy": "Fail"});
        for (k, v) in spec_extra.as_object().unwrap() {
            spec[k] = v.clone();
        }
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": spec})).unwrap()
    }

    fn msgs(p: &ValidatingAdmissionPolicy) -> String {
        RestCreateStrategy::validate(&Strategy::default(), &RequestContext::new(None), p)
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// validation_test.go:2618 "validation expression that does not compile".
    #[test]
    fn rejects_a_validation_expression_that_does_not_compile() {
        let m = msgs(&policy(
            json!({"validations": [{"expression": "object.x in [1, 2, "}]}),
        ));
        assert!(
            m.contains(r#"spec.validations[0].expression: Invalid value: "object.x in [1, 2, ": compilation failed"#),
            "{m}"
        );
    }

    /// `ValidationCondition.ReturnTypes` is `[bool]` (interface.go:46-48).
    #[test]
    fn rejects_a_non_bool_validation_expression() {
        let m = msgs(&policy(json!({"validations": [{"expression": "6"}]})));
        assert!(
            m.contains(r#"spec.validations[0].expression: Invalid value: "6": must evaluate to bool but got int"#),
            "{m}"
        );
    }

    /// validation_test.go:2954: `variables` has no field `replicas`.
    #[test]
    fn rejects_an_undefined_variable_in_a_validation() {
        let m = msgs(&policy(json!({
            "variables": [{"name": "foo", "expression": "1 + 1"}],
            "validations": [{"expression": "true"}, {"expression": "variables.replicas == 2"}]})));
        assert!(
            m.contains(r#"spec.validations[1].expression: Invalid value: "variables.replicas == 2": compilation failed"#) && m.contains("undefined field 'replicas'"),
            "{m}"
        );
    }

    /// validation_test.go:2983: a variable only sees the ones before it.
    #[test]
    fn variables_only_see_earlier_variables() {
        let m = msgs(&policy(json!({
            "variables": [
                {"name": "correct", "expression": "object"},
                {"name": "bar", "expression": "variables.foo + 1"},
                {"name": "foo", "expression": "1 + 1"}],
            "validations": [{"expression": "variables.foo > 1"}]})));
        assert!(
            m.contains(r#"spec.variables[1].expression: Invalid value: "variables.foo + 1": compilation failed"#) && m.contains("undefined field 'foo'"),
            "{m}"
        );
        assert!(!m.contains("spec.validations"), "{m}");
    }

    /// validation_test.go:4415.
    #[test]
    fn rejects_a_variable_expression_that_does_not_compile() {
        let m = msgs(&policy(json!({
            "variables": [{"name": "a", "expression": "///"}],
            "validations": [{"expression": "true"}]})));
        assert!(
            m.contains(r#"spec.variables[0].expression: Invalid value: "///": compilation failed"#),
            "{m}"
        );
    }

    /// validation_test.go:2731-2757.
    #[test]
    fn audit_annotation_value_expression_is_compiled() {
        let m = msgs(&policy(json!({
            "validations": [{"expression": "true"}],
            "auditAnnotations": [{"key": "a", "valueExpression": "object.x in [1, 2, "}]})));
        assert!(
            m.contains(r#"spec.auditAnnotations[0].valueExpression: Invalid value: "object.x in [1, 2, ": compilation failed"#),
            "{m}"
        );
        let m = msgs(&policy(json!({
            "validations": [{"expression": "true"}],
            "auditAnnotations": [{"key": "a", "valueExpression": "true"}]})));
        assert!(
            m.contains(r#"spec.auditAnnotations[0].valueExpression: Invalid value: "true": must evaluate to one of [string null_type]"#),
            "{m}"
        );
    }

    #[test]
    fn accepts_valid_expressions() {
        let m = msgs(&policy(json!({
            "variables": [{"name": "foo", "expression": "object.metadata.name"}],
            "validations": [{"expression": "variables.foo == 'x' && authorizer.group('a').check('get').allowed()"}],
            "auditAnnotations": [{"key": "a", "valueExpression": "variables.foo"}]})));
        assert!(m.is_empty(), "{m}");
    }
}

#[cfg(test)]
mod preexisting_expression_tests {
    use super::*;
    use serde_json::json;

    const SEMVER: &str = "semver('1.2.3').major() > 0";

    fn env_1_32() -> EnvSet {
        EnvSet::must_base_env_set(rusternetes_common::feature_gates::Version::major_minor(
            1, 32,
        ))
    }

    fn policy(
        match_condition: &str,
        variable: &str,
        validation: &str,
        message: &str,
        audit: &str,
    ) -> ValidatingAdmissionPolicy {
        serde_json::from_value(json!({"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy", "metadata": {"name": "foo"}, "spec": {
            "matchConstraints": {"matchPolicy": "Exact", "objectSelector": {}, "namespaceSelector": {}, "resourceRules": [{"operations": ["CREATE"], "apiGroups": ["a"], "apiVersions": ["a"], "resources": ["a"]}]},
            "matchConditions": [{"name": "c", "expression": match_condition}],
            "variables": [{"name": "v", "expression": variable}],
            "validations": [{"expression": validation, "messageExpression": message}],
            "auditAnnotations": [{"key": "k", "valueExpression": audit}],
            "failurePolicy": "Fail"}})).unwrap()
    }

    fn all_errors(
        new: &ValidatingAdmissionPolicy,
        old: Option<&ValidatingAdmissionPolicy>,
    ) -> String {
        let preexisting = old
            .map(find_validating_policy_preexisting_expressions)
            .unwrap_or_default();
        let env = env_1_32();
        let mut errs = validate_in(new, &preexisting, &env);
        errs.extend(compile_errors_in(new, false, &preexisting, &env));
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Each of the five expression sites compiles in `NewExpressions` when
    /// the old policy does not store it (validateMatchConditionsExpression
    /// :1100, validateVariable :1012, validateValidationExpression :1088,
    /// validateMessageExpression :1114, validateAuditAnnotation :1143).
    #[test]
    fn new_expressions_are_version_gated_at_every_site() {
        let s = format!("{SEMVER} ? 'a' : 'b'");
        let p = policy(SEMVER, SEMVER, SEMVER, &s, &s);
        let got = all_errors(&p, None);
        for path in [
            "spec.matchConditions[0].expression",
            "spec.variables[0].expression",
            "spec.validations[0].expression",
            "spec.validations[0].messageExpression",
            "spec.auditAnnotations[0].valueExpression",
        ] {
            assert!(
                got.contains(path) && got.contains("undeclared reference to 'semver'"),
                "{path}: {got}"
            );
        }
    }

    /// An old policy that stores the expressions compiles them in
    /// `StoredExpressions`, where every library is enabled.
    #[test]
    fn stored_expressions_are_not_version_gated() {
        let s = format!("{SEMVER} ? 'a' : 'b'");
        let p = policy(SEMVER, SEMVER, SEMVER, &s, &s);
        assert_eq!(all_errors(&p, Some(&p)), "");
        // A different expression is new again.
        let other = policy("semver('1.2.3').minor() > 0", SEMVER, SEMVER, &s, &s);
        assert!(all_errors(&other, Some(&p)).contains("spec.matchConditions[0].expression"));
    }

    /// Upstream replicated: `validateVariable` consults
    /// `validationExpressions` (validation.go:1012), so a variable is stored
    /// only when it equals a stored `validations[].expression`.
    #[test]
    fn variable_consults_the_validation_expression_set() {
        let ok = "true";
        let old = policy(ok, ok, SEMVER, "'m'", "'a'");
        let new = policy(ok, SEMVER, ok, "'m'", "'a'");
        assert_eq!(all_errors(&new, Some(&old)), "");
        assert!(all_errors(&new, None).contains("spec.variables[0].expression"));
    }

    /// `findValidatingPolicyPreexistingExpressions` (:308-322): a blank
    /// `messageExpression` is not recorded.
    #[test]
    fn blank_message_expression_is_not_preexisting() {
        let p = policy("true", "true", "true", "", "true");
        let pre = find_validating_policy_preexisting_expressions(&p);
        assert!(pre.validation_message_expressions.is_empty());
        assert!(pre.validation_expressions.contains("true"));
    }
}
