//! RoleBinding strategy and storage — port of
//! `pkg/registry/rbac/rolebinding/strategy.go`,
//! `pkg/registry/rbac/rolebinding/storage/storage.go` and the policybased
//! wrapper (`pkg/registry/rbac/rest/storage_rbac.go:113-116`).
//!
//! `ValidateDeclarativelyWithMigrationChecks` (strategy.go:84, 107) adds the
//! declarative `+k8s:` validation of `roleRef`; its `required` rule on
//! `roleRef.name` is the hand-written one (`MarkCoveredByDeclarative`), so
//! nothing is modelled for it separately.

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::RoleBinding;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_path_segment, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::rbac::{
    default_role_binding, validate_role_binding, validate_role_binding_update,
};
use rusternetes_storage::StorageBackend;

use super::policybased::{PolicyBased, RoleBindingPolicyBased};
use super::rule::DefaultRuleResolver;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// The v1 defaulting a decoded RoleBinding goes through:
/// `SetDefaults_RoleBinding` (pkg/apis/rbac/v1/defaults.go). Runs on the
/// request body and on every object read from storage.
pub fn convert_to_internal(rb: &mut RoleBinding) {
    default_role_binding(rb);
}

/// `ValidateRoleBinding` (validation.go:128-159): ObjectMeta with
/// `ValidateRBACName`, then roleRef and subjects.
fn validate(obj: &RoleBinding) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        true,
        name_is_path_segment,
        &Path::new("metadata"),
    );
    errs.extend(validate_role_binding(obj));
    errs
}

/// `strategy` (strategy.go:30-36).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<RoleBinding> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut RoleBinding) {}

    fn validate(&self, _ctx: &RequestContext, obj: &RoleBinding) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<RoleBinding> for Strategy {
    /// `AllowCreateOnUpdate` is true for RoleBindings (strategy.go:50-52).
    fn allow_create_on_update(&self) -> bool {
        true
    }

    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut RoleBinding,
        _old: &RoleBinding,
    ) {
    }

    /// `ValidateRoleBindingUpdate` (validation.go:161-170): `ValidateRoleBinding`,
    /// `ValidateObjectMetaUpdate`, and an immutable roleRef.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &RoleBinding,
        old: &RoleBinding,
    ) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            true,
            name_is_path_segment,
            &Path::new("metadata"),
        );
        errs.extend(validate_object_meta_update(
            &obj.metadata,
            &old.metadata,
            &Path::new("metadata"),
        ));
        errs.extend(validate_role_binding_update(obj, old));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<RoleBinding> for Strategy {}

/// `NewREST` (storage/storage.go) wrapped by `rolebindingpolicybased.NewStorage`.
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<RoleBinding, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("rbac.authorization.k8s.io", "rolebindings"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let hook = Arc::new(RoleBindingPolicyBased(PolicyBased {
        authorizer,
        resolver: Arc::new(DefaultRuleResolver::new(storage)),
    }));
    store.begin_create = Some(hook.clone());
    store.update_transformers = vec![hook];
    store
}
