//! ValidatingAdmissionPolicy strategy and storage — port of
//! `pkg/registry/admissionregistration/validatingadmissionpolicy/strategy.go`
//! and `.../storage/storage.go`. See [`super`] for what is not modelled.

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::cel_env::{mutation_env_failure, ExpectedOutput, MutationEnv};

use rusternetes_common::resources::validating_admission_policy::ValidatingAdmissionPolicyStatus;
use rusternetes_common::resources::ValidatingAdmissionPolicy;
use rusternetes_common::validation::field::{Error as FieldError, ErrorList, Path};
use rusternetes_common::validation::objectmeta::{name_is_dns_subdomain, validate_object_meta};
use rusternetes_common::validation::validating_admission_policy::{
    ignore_validating_admission_policy_match_conditions, set_defaults_validating_admission_policy,
    validate_validating_admission_policy, validate_validating_admission_policy_status_update,
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

/// `validateMessageExpression` (validation.go:1114-1126) for every
/// `validations[i].messageExpression` that is not blank (:1041-1045): compiled
/// with `HasAuthorizer: false` (also `compilePolicy`, validating/plugin.go:119),
/// so a reference to `authorizer` is `undeclared reference`, and expected to
/// evaluate to a string (`MessageExpressionCondition.ReturnTypes`,
/// message.go:34-36). `HasParams` is `opts.allowParamsInMatchConditions`, which
/// `validateValidatingAdmissionPolicySpec` sets when `paramKind` is present
/// (:790-792). The `variables` are declared only when the policy has some:
/// `getCompiler` (:773-779) builds the composited compiler only then and the
/// stateless one otherwise (`createCompiler`, :1316-1327).
///
/// The rest of the spec's CEL compile (`validations[].expression`,
/// `variables`, `auditAnnotations`) is not modelled here, see [`super`].
fn message_expression_errors(obj: &ValidatingAdmissionPolicy) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(spec) = &obj.spec else {
        return errs;
    };
    let has_variables = spec.variables.as_ref().is_some_and(|v| !v.is_empty());
    let names: std::collections::BTreeSet<String> = spec
        .variables
        .iter()
        .flatten()
        .map(|v| v.name.clone())
        .filter(|n| !n.trim().is_empty())
        .collect();
    let env = MutationEnv {
        has_params: spec.param_kind.is_some(),
        has_authorizer: false,
        has_patch_types: false,
        variables: has_variables.then_some(names),
    };
    for (i, v) in spec.validations.iter().flatten().enumerate() {
        let Some(expression) = v.message_expression.as_deref() else {
            continue;
        };
        if expression.trim().is_empty() {
            continue;
        }
        if let Some(detail) = mutation_env_failure(expression, &env, ExpectedOutput::String) {
            errs.push(FieldError::invalid(
                &Path::new("spec")
                    .child("validations")
                    .index(i)
                    .child("messageExpression"),
                expression.to_string(),
                detail,
            ));
        }
    }
    errs
}

/// `validateValidatingAdmissionPolicy` (validation.go:762-770): ObjectMeta
/// with `NameIsDNSSubdomain`, then the spec.
fn validate(obj: &ValidatingAdmissionPolicy) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_validating_admission_policy(obj));
    errs.extend(message_expression_errors(obj));
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
fn compile_errors(obj: &ValidatingAdmissionPolicy, ignore_match_conditions: bool) -> ErrorList {
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
        if let Some(detail) = mutation_env_failure(expression, &env, ExpectedOutput::Bool) {
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
        let mut errs = validate(obj);
        errs.extend(compile_errors(obj, false));
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
        let mut errs = validate(obj);
        errs.extend(compile_errors(
            obj,
            ignore_validating_admission_policy_match_conditions(obj, old),
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
        assert!(validate(&obj).is_empty(), "{:?}", validate(&obj));
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
