//! Role strategy and storage — port of `pkg/registry/rbac/role/strategy.go`,
//! `pkg/registry/rbac/role/storage/storage.go` and the policybased wrapper
//! (`pkg/registry/rbac/rest/storage_rbac.go:108-111`).

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::Role;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_path_segment, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::rbac::validate_role;
use rusternetes_storage::StorageBackend;

use super::policybased::{PolicyBased, RolePolicyBased};
use super::rule::DefaultRuleResolver;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `ValidateRole` (validation.go:36-49): ObjectMeta with `ValidateRBACName`,
/// then the rules.
fn validate(obj: &Role) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        true,
        name_is_path_segment,
        &Path::new("metadata"),
    );
    errs.extend(validate_role(obj));
    errs
}

/// `strategy` (strategy.go:31-37).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<Role> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut Role) {}

    fn validate(&self, _ctx: &RequestContext, obj: &Role) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<Role> for Strategy {
    /// `AllowCreateOnUpdate` is true for Roles (strategy.go:52-54).
    fn allow_create_on_update(&self) -> bool {
        true
    }

    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut Role, _old: &Role) {}

    /// `ValidateRoleUpdate` (validation.go:51-56): `ValidateRole`, then
    /// `ValidateObjectMetaUpdate`.
    fn validate_update(&self, _ctx: &RequestContext, obj: &Role, old: &Role) -> ErrorList {
        let mut errs = validate(obj);
        errs.extend(validate_object_meta_update(
            &obj.metadata,
            &old.metadata,
            &Path::new("metadata"),
        ));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<Role> for Strategy {}

/// `NewREST` (storage/storage.go:35-60) wrapped by `rolepolicybased.NewStorage`.
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<Role, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("rbac.authorization.k8s.io", "roles"),
        Arc::new(Strategy),
    );
    let hook = Arc::new(RolePolicyBased(PolicyBased {
        authorizer,
        resolver: Arc::new(DefaultRuleResolver::new(storage)),
    }));
    store.begin_create = Some(hook.clone());
    store.begin_update = Some(hook);
    store
}
