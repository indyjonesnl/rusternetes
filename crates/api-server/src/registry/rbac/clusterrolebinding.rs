//! ClusterRoleBinding strategy and storage — port of
//! `pkg/registry/rbac/clusterrolebinding/strategy.go`,
//! `pkg/registry/rbac/clusterrolebinding/storage/storage.go` and the
//! policybased wrapper (`pkg/registry/rbac/rest/storage_rbac.go:123-126`).

use std::sync::Arc;

use rusternetes_common::authz::Authorizer;
use rusternetes_common::resources::ClusterRoleBinding;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_path_segment, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::rbac::{
    default_cluster_role_binding, validate_cluster_role_binding,
    validate_cluster_role_binding_update,
};
use rusternetes_storage::StorageBackend;

use super::policybased::{ClusterRoleBindingPolicyBased, PolicyBased};
use super::rule::DefaultRuleResolver;
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// The v1 defaulting a decoded ClusterRoleBinding goes through:
/// `SetDefaults_ClusterRoleBinding` (pkg/apis/rbac/v1/defaults.go).
pub fn convert_to_internal(crb: &mut ClusterRoleBinding) {
    default_cluster_role_binding(crb);
}

/// `ValidateClusterRoleBinding` (validation.go:172-203): ObjectMeta with
/// `ValidateRBACName`, then roleRef and subjects.
fn validate(obj: &ClusterRoleBinding) -> ErrorList {
    let mut errs = validate_object_meta(
        &obj.metadata,
        false,
        name_is_path_segment,
        &Path::new("metadata"),
    );
    errs.extend(validate_cluster_role_binding(obj));
    errs
}

/// `strategy` (strategy.go).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ClusterRoleBinding> for Strategy {
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut ClusterRoleBinding) {}

    fn validate(&self, _ctx: &RequestContext, obj: &ClusterRoleBinding) -> ErrorList {
        validate(obj)
    }
}

impl RestUpdateStrategy<ClusterRoleBinding> for Strategy {
    /// `AllowCreateOnUpdate` is true for ClusterRoleBindings.
    fn allow_create_on_update(&self) -> bool {
        true
    }

    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut ClusterRoleBinding,
        _old: &ClusterRoleBinding,
    ) {
    }

    /// `ValidateClusterRoleBindingUpdate` (validation.go:205-214):
    /// `ValidateClusterRoleBinding`, `ValidateObjectMetaUpdate`, and an
    /// immutable roleRef.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ClusterRoleBinding,
        old: &ClusterRoleBinding,
    ) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            false,
            name_is_path_segment,
            &Path::new("metadata"),
        );
        errs.extend(validate_object_meta_update(
            &obj.metadata,
            &old.metadata,
            &Path::new("metadata"),
        ));
        errs.extend(validate_cluster_role_binding_update(obj, old));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<ClusterRoleBinding> for Strategy {}

/// `NewREST` (storage/storage.go) wrapped by
/// `clusterrolebindingpolicybased.NewStorage`.
pub fn new_store(
    storage: Arc<StorageBackend>,
    authorizer: Arc<dyn Authorizer>,
) -> Store<ClusterRoleBinding, StorageBackend> {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("rbac.authorization.k8s.io", "clusterrolebindings"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal);
    let hook = Arc::new(ClusterRoleBindingPolicyBased(PolicyBased {
        authorizer,
        resolver: Arc::new(DefaultRuleResolver::new(storage)),
    }));
    store.begin_create = Some(hook.clone());
    store.update_transformers = vec![hook];
    store
}
