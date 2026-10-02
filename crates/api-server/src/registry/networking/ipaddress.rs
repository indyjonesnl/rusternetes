//! Port of pkg/registry/networking/ipaddress/strategy.go and storage/storage.go.
use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};
use rusternetes_common::resources::IPAddress;
use rusternetes_common::validation::field::ErrorList;
use rusternetes_common::validation::ipaddress::{validate_ip_address, validate_ip_address_update};
use rusternetes_storage::StorageBackend;
use std::sync::Arc;

pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    /// strategy.go:54-56: all IPAddresses are cluster scoped.
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<IPAddress> for Strategy {
    /// `noopNameGenerator` (strategy.go:38-42): IPAddress does not generate
    /// names, it returns the base.
    fn generate_name(&self, base: &str) -> String {
        base.to_string()
    }

    /// PrepareForCreate (strategy.go:59-62) changes nothing.
    fn prepare_for_create(&self, _ctx: &RequestContext, _obj: &mut IPAddress) {}

    /// Validate (strategy.go:73-77).
    fn validate(&self, _ctx: &RequestContext, obj: &IPAddress) -> ErrorList {
        validate_ip_address(obj)
    }
}

impl RestUpdateStrategy<IPAddress> for Strategy {
    /// strategy.go:84-86: POST is needed to create one.
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// PrepareForUpdate (strategy.go:65-70) changes nothing.
    fn prepare_for_update(&self, _ctx: &RequestContext, _obj: &mut IPAddress, _old: &IPAddress) {}

    /// ValidateUpdate (strategy.go:89-95): `ValidateIPAddress` then
    /// `ValidateIPAddressUpdate`.
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &IPAddress,
        old: &IPAddress,
    ) -> ErrorList {
        let mut errs = validate_ip_address(obj);
        errs.extend(validate_ip_address_update(obj, old));
        errs
    }

    /// strategy.go:98-100.
    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<IPAddress> for Strategy {}

/// NewREST (storage/storage.go:37-55).
pub fn new_store(storage: Arc<StorageBackend>) -> Store<IPAddress, StorageBackend> {
    Store::new(
        storage,
        GroupResource::new("networking.k8s.io", "ipaddresses"),
        Arc::new(Strategy),
    )
}
