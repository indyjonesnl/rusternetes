//! ResourceClaim strategies and storage — port of
//! `pkg/registry/resource/resourceclaim/strategy.go` and
//! `pkg/registry/resource/resourceclaim/storage/storage.go`.
//!
//! The declarative validation check that follows each validator is not
//! modelled. The admin-access namespace check is described in [`super::admin`].

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use rusternetes_common::resources::dra::ResourceClaimStatus;
use rusternetes_common::resources::ResourceClaim;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::resourceclaim::{
    validate_resource_claim, validate_resource_claim_status_update, validate_resource_claim_update,
};
use rusternetes_common::Result;
use rusternetes_storage::StorageBackend;

use super::admin::authorized_for_admin;
use super::spec::drop_disabled_fields as drop_disabled_spec_fields;
use crate::registry::generic::store::{BeginCreate, CreateOptions, Finish};
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy,
};

/// A Go `resource.ResourceClaimStatus{}`: the status is a value, so it always
/// serializes.
fn empty_status() -> Option<ResourceClaimStatus> {
    Some(ResourceClaimStatus::default())
}

/// `dropDeallocatedStatusDevices` (strategy.go:290-345): a `status.devices`
/// entry for a device the old allocation held and the new one no longer does
/// is removed. `DRAResourceClaimDeviceStatus` is on in 1.35, so the early
/// return on a disabled gate never applies. `shareID` is not modelled.
fn drop_deallocated_status_devices(new: &mut ResourceClaim, old: &ResourceClaim) {
    type Id = (String, String, String);
    let ids = |c: &ResourceClaim| -> HashSet<Id> {
        c.status
            .as_ref()
            .and_then(|s| s.allocation.as_ref())
            .map(|a| {
                a.devices
                    .results
                    .iter()
                    .map(|r| (r.driver.clone(), r.pool.clone(), r.device.clone()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let still_allocated = ids(new);
    let deallocated: HashSet<Id> = ids(old).difference(&still_allocated).cloned().collect();
    if let Some(status) = new.status.as_mut() {
        status.devices.retain(|d| {
            !deallocated.contains(&(d.driver.clone(), d.pool.clone(), d.device.clone()))
        });
    }
}

/// Hook running the `AuthorizedForAdmin` check before the create strategy.
struct AdminCheck {
    storage: Arc<StorageBackend>,
}

struct Noop;

#[async_trait]
impl Finish for Noop {
    async fn finish(self: Box<Self>, _success: bool) {}
}

#[async_trait]
impl BeginCreate<ResourceClaim> for AdminCheck {
    async fn begin_create(
        &self,
        ctx: &RequestContext,
        obj: &mut ResourceClaim,
        _options: &CreateOptions,
    ) -> Result<Box<dyn Finish>> {
        let ns = ctx
            .namespace
            .clone()
            .or_else(|| obj.metadata.namespace.clone())
            .unwrap_or_default();
        authorized_for_admin(&self.storage, &obj.spec.devices.requests, &ns).await?;
        Ok(Box::new(Noop))
    }
}

/// `resourceclaimStrategy` (strategy.go:47-67).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestCreateStrategy<ResourceClaim> for Strategy {
    /// Status must not be set by the user on create (strategy.go:93-99).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ResourceClaim) {
        obj.status = empty_status();
        drop_disabled_spec_fields(&mut obj.spec, None);
    }

    /// `ValidateResourceClaim` (validation.go:109-113): ObjectMeta with
    /// `ValidateResourceClaimName` (`NameIsDNSSubdomain`), then the spec.
    fn validate(&self, _ctx: &RequestContext, obj: &ResourceClaim) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            true,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_resource_claim(obj));
        errs
    }
}

impl RestUpdateStrategy<ResourceClaim> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceClaim,
        old: &ResourceClaim,
    ) {
        obj.status = old.status.clone().or_else(empty_status);
        drop_disabled_spec_fields(&mut obj.spec, Some(&old.spec));
    }

    /// `ValidateResourceClaimUpdate` (validation.go:116-124).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ResourceClaim,
        old: &ResourceClaim,
    ) -> ErrorList {
        let meta = Path::new("metadata");
        let mut errs = validate_object_meta(&obj.metadata, true, name_is_dns_subdomain, &meta);
        errs.extend(validate_object_meta_update(
            &obj.metadata,
            &old.metadata,
            &meta,
        ));
        errs.extend(validate_resource_claim_update(obj, old));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<ResourceClaim> for Strategy {}

/// `resourceclaimStatusStrategy` (strategy.go:140-217).
pub struct StatusStrategy;

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<ResourceClaim> for StatusStrategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Only status may change. The status-only gated fields are not modelled
    /// (see [`super::spec`]), so `dropDisabledStatusFields` has nothing to do.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceClaim,
        old: &ResourceClaim,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
        drop_deallocated_status_devices(obj, old);
    }

    /// `ValidateResourceClaimStatusUpdate` (validation.go:127-132).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ResourceClaim,
        old: &ResourceClaim,
    ) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        errs.extend(validate_resource_claim_status_update(
            &obj.status.clone().unwrap_or_default(),
            &old.status.clone().unwrap_or_default(),
            &obj.spec.devices,
            obj.metadata.deletion_timestamp.is_some(),
            &Path::new("status"),
        ));
        errs
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

/// `NewREST` (storage/storage.go): the main store and the status store;
/// `ReturnDeletedObject: true`.
pub fn new_stores(
    storage: Arc<StorageBackend>,
) -> (
    Store<ResourceClaim, StorageBackend>,
    Store<ResourceClaim, StorageBackend>,
) {
    let mut store = Store::new(
        storage.clone(),
        GroupResource::new("resource.k8s.io", "resourceclaims"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store.begin_create = Some(Arc::new(AdminCheck { storage }));
    let status = store.with_update_strategy(Arc::new(StatusStrategy));
    (store, status)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim(results: &[&str], devices: &[&str]) -> ResourceClaim {
        let res: Vec<_> = results
            .iter()
            .map(|d| serde_json::json!({"request": "r", "driver": "drv", "pool": "p", "device": d}))
            .collect();
        let devs: Vec<_> = devices
            .iter()
            .map(|d| serde_json::json!({"driver": "drv", "pool": "p", "device": d}))
            .collect();
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "c"},
            "status": {"allocation": {"devices": {"results": res}}, "devices": devs}
        }))
        .unwrap()
    }

    /// `dropDeallocatedStatusDevices` (strategy.go:290-345): a status device
    /// the old allocation held and the new one dropped is removed; others stay.
    #[test]
    fn a_deallocated_device_loses_its_status_entry() {
        let old = claim(&["a", "b"], &["a", "b"]);
        let mut new = claim(&["a"], &["a", "b"]);
        drop_deallocated_status_devices(&mut new, &old);
        let devices: Vec<_> = new
            .status
            .unwrap()
            .devices
            .into_iter()
            .map(|d| d.device)
            .collect();
        assert_eq!(devices, vec!["a".to_string()]);
    }
}
