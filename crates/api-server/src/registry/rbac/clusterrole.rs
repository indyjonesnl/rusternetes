//! ClusterRole strategy and storage — port of
//! `pkg/registry/rbac/clusterrole/strategy.go`,
//! `pkg/registry/rbac/clusterrole/storage/storage.go` and the policybased
//! wrapper (`pkg/registry/rbac/rest/storage_rbac.go:118-121`).
//!
//! `aggregationRule` is materialised on write; see [`super::aggregation`].

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::ClusterRole;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_path_segment, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::rbac::{
    has_invalid_label_value_in_label_selector, validate_cluster_role_with_options,
};
use rusternetes_storage::StorageBackend;

use super::policybased::{ClusterRolePolicyBased, PolicyBased};
use super::rule::DefaultRuleResolver;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `ValidateClusterRole` (validation.go:62-93): ObjectMeta with
/// `ValidateRBACName`, then the rules and the aggregation rule.
fn validate(obj: &ClusterRole, allow_invalid_label_value_in_selector: bool) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_path_segment,
        &Path::new("metadata"),
    );
    errs.extend(validate_cluster_role_with_options(
        obj,
        allow_invalid_label_value_in_selector,
    ));
    errs
}

/// `strategy` (strategy.go).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ClusterRole> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut ClusterRole) {}

    fn validate(&self, _ctx: &RequestContext, obj: &ClusterRole) -> ErrorList {
        validate(obj, false)
    }
}

impl RestUpdateStrategy<ClusterRole> for Strategy {
    /// `AllowCreateOnUpdate` is true for ClusterRoles.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut ClusterRole,
        _old: &ClusterRole,
    ) {
    }

    /// `ValidateClusterRoleUpdate` (validation.go:95-100): `ValidateClusterRole`
    /// — allowing invalid label values in a selector the stored object already
    /// had — then `ValidateObjectMetaUpdate`.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ClusterRole,
        old: &ClusterRole,
    ) -> ErrorList {
        let mut errs = validate(obj, has_invalid_label_value_in_label_selector(old));
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

impl RestDeleteStrategy<ClusterRole> for Strategy {}

/// `NewREST` (storage/storage.go) wrapped by `clusterrolepolicybased.NewStorage`.
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<ClusterRole, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("rbac.authorization.k8s.io", "clusterroles"),
        Arc::new(Strategy),
    );
    let hook = Arc::new(ClusterRolePolicyBased {
        base: PolicyBased {
            authorizer,
            resolver: Arc::new(DefaultRuleResolver::new(storage.clone())),
        },
        storage,
    });
    store.begin_create = Some(hook.clone());
    store.begin_update = Some(hook.clone());
    store.update_transformers = vec![hook];
    store
}
