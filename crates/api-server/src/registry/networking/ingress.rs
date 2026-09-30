//! Port of pkg/registry/networking/ingress/strategy.go and storage/storage.go.
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use rusternetes_common::equality::semantic_equal;
use rusternetes_common::resources::Ingress;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::ingress::{validate_ingress_create, validate_ingress_update};
use rusternetes_storage::StorageBackend;
use std::sync::Arc;
pub struct Strategy;
impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}
impl RestCreateStrategy<Ingress> for Strategy {
    /// PrepareForCreate (pkg/registry/networking/ingress/strategy.go:71-77).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut Ingress) {
        obj.status = Some(rusternetes_common::resources::IngressStatus {
            load_balancer: Some(rusternetes_common::resources::IngressLoadBalancerStatus {
                ingress: None,
            }),
        });
        obj.metadata.generation = Some(1);
    }
    fn validate(&self, _ctx: &RequestContext, obj: &Ingress) -> ErrorList {
        validate_ingress_create(obj)
    }
    /// WarningsOnCreate (pkg/registry/networking/ingress/strategy.go:102-109).
    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &Ingress) -> Vec<String> {
        if obj
            .metadata
            .annotations
            .as_ref()
            .is_some_and(|a| a.contains_key("kubernetes.io/ingress.class"))
            && obj
                .spec
                .as_ref()
                .is_none_or(|s| s.ingress_class_name.is_none())
        {
            vec!["annotation \"kubernetes.io/ingress.class\" is deprecated, please use 'spec.ingressClassName' instead".to_string()]
        } else {
            Vec::new()
        }
    }
}
impl RestUpdateStrategy<Ingress> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }
    /// PrepareForUpdate (pkg/registry/networking/ingress/strategy.go:80-93).
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Ingress, old: &Ingress) {
        obj.status = old.status.clone();
        if !semantic_equal(&obj.spec, &old.spec) {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
    }
    fn validate_update(&self, _ctx: &RequestContext, obj: &Ingress, old: &Ingress) -> ErrorList {
        validate_ingress_update(obj, old)
    }
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}
impl RestDeleteStrategy<Ingress> for Strategy {}
/// NewREST (pkg/registry/networking/ingress/storage/storage.go:41-63).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<Ingress, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("networking.k8s.io", "ingresses"),
        Arc::new(Strategy),
    )
}
/// ingressStatusStrategy (pkg/registry/networking/ingress/strategy.go:136-189).
pub struct StatusStrategy;
impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}
impl RestUpdateStrategy<Ingress> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }
    /// PrepareForUpdate (strategy.go:162-167) restores spec.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut Ingress, old: &Ingress) {
        obj.spec = old.spec.clone();
    }
    fn validate_update(&self, _ctx: &RequestContext, obj: &Ingress, old: &Ingress) -> ErrorList {
        rusternetes_common::validation::ingress::validate_ingress_status_update(obj, old)
    }
    fn allow_unconditional_update(&self) -> bool {
        true
    }
    /// WarningsOnUpdate (strategy.go:175-188).
    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &Ingress,
        _old: &Ingress,
    ) -> Vec<String> {
        let mut warnings = Vec::new();
        if let Some(points) = obj
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .and_then(|s| s.ingress.as_ref())
        {
            for (i, point) in points.iter().enumerate() {
                if let Some(ip) = point.ip.as_deref().filter(|ip| !ip.is_empty()) {
                    warnings.extend(rusternetes_common::validation::metav1::get_warnings_for_ip(
                        &format!("status.loadBalancer.ingress[{i}]"),
                        ip,
                    ));
                }
            }
        }
        warnings
    }
}
/// StatusREST (storage/storage.go:60-63,90-99).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<Ingress, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
