//! Port of pkg/registry/networking/ingressclass/strategy.go and storage/storage.go.
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::IngressClass;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::ingressclass::{
    validate_ingress_class, validate_ingress_class_update,
};
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
/// SetDefaults_IngressClass (pkg/apis/networking/v1/defaults.go:48-51).
pub fn convert_to_internal(obj: &mut IngressClass) {
    if let Some(params) = obj.spec.as_mut().and_then(|s| s.parameters.as_mut()) {
        if params.scope.is_none() {
            params.scope = Some("Cluster".to_string());
        }
    }
}
pub struct Strategy;
impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}
impl RestCreateStrategy<IngressClass> for Strategy {
    /// PrepareForCreate (pkg/registry/networking/ingressclass/strategy.go:49-52).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut IngressClass) {
        obj.metadata.generation = Some(1);
    }
    fn validate(&self, _ctx: &RequestContext, obj: &IngressClass) -> ErrorList {
        validate_ingress_class(obj)
    }
}
impl RestUpdateStrategy<IngressClass> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }
    /// PrepareForUpdate (pkg/registry/networking/ingressclass/strategy.go:56-65).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut IngressClass,
        old: &IngressClass,
    ) {
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &IngressClass,
        old: &IngressClass,
    ) -> ErrorList {
        validate_ingress_class_update(obj, old)
    }
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}
impl RestDeleteStrategy<IngressClass> for Strategy {}
/// NewREST (pkg/registry/networking/ingressclass/storage/storage.go:36-55).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<IngressClass, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("networking.k8s.io", "ingressclasses"),
        Arc::new(Strategy),
    )
    .with_decode_defaulter(convert_to_internal)
}
