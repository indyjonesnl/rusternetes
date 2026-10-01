//! Port of pkg/registry/networking/servicecidr/strategy.go and storage/storage.go.
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};
use rusternetes_common::resources::ServiceCIDR;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::servicecidr::{
    validate_service_cidr, validate_service_cidr_status_update, validate_service_cidr_update,
};
use rusternetes_storage::StorageBackend;
use std::sync::Arc;

pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:49-51: all ServiceCIDRs are cluster scoped.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ServiceCIDR> for Strategy {
    /// PrepareForCreate (strategy.go:68-71) changes nothing: despite its doc
    /// comment it does not clear the status. (`GetResetFields`, :55, only
    /// feeds server-side-apply's managed fields.)
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut ServiceCIDR) {}

    /// Validate (strategy.go:82-86).
    fn validate(&self, _ctx: &RequestContext, obj: &ServiceCIDR) -> ErrorList {
        validate_service_cidr(obj)
    }
}

impl RestUpdateStrategy<ServiceCIDR> for Strategy {
    /// strategy.go:93-95: POST is needed to create one.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// PrepareForUpdate (strategy.go:74-80) changes nothing.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        _obj: &mut ServiceCIDR,
        _old: &ServiceCIDR,
    ) {
    }

    /// ValidateUpdate (strategy.go:103-108).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ServiceCIDR,
        old: &ServiceCIDR,
    ) -> ErrorList {
        validate_service_cidr_update(obj, old)
    }

    /// strategy.go:111-113.
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<ServiceCIDR> for Strategy {}

/// `serviceCIDRStatusStrategy` (strategy.go:120-158): the update strategy of
/// `/status`.
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestUpdateStrategy<ServiceCIDR> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// strategy.go:142-149: status changes are not allowed to update spec, and
    /// `ResetObjectMetaForStatus` keeps the stored metadata.
    fn prepare_for_update(&self, _ctx: &RequestContext, obj: &mut ServiceCIDR, old: &ServiceCIDR) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
    }

    /// strategy.go:151-153.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ServiceCIDR,
        old: &ServiceCIDR,
    ) -> ErrorList {
        validate_service_cidr_status_update(obj, old)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// NewREST (storage/storage.go:41-65): the ServiceCIDR store.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ServiceCIDR, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("networking.k8s.io", "servicecidrs"),
        Arc::new(Strategy),
    )
}

/// The `/status` store: the ServiceCIDR store updating with [`StatusStrategy`]
/// (storage.go:61-63).
pub fn new_status_store(storage: Arc<StorageBackend>) -> Store<ServiceCIDR, StorageBackend> {
    new_store(storage).with_update_strategy(Arc::new(StatusStrategy))
}
