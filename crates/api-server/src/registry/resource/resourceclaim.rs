//! ResourceClaim strategies and storage — port of
//! `pkg/registry/resource/resourceclaim/strategy.go` and
//! `pkg/registry/resource/resourceclaim/storage/storage.go`.
//!
//! The declarative validation check that follows each validator is not
//! modelled. The admin-access namespace check is described in [`super::admin`].

use std::collections::HashSet;
use std::sync::Arc;

use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::dra::ResourceClaimStatus;
use rusternetes_common::resources::ResourceClaim;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::resourceclaim::{
    validate_resource_claim, validate_resource_claim_status_update, validate_resource_claim_update,
};
use rusternetes_storage::StorageBackend;

use super::admin::{authorized_for_admin, authorized_for_admin_status};
use super::spec::drop_disabled_fields as drop_disabled_spec_fields;
use crate::registry::generic::Store;
use crate::registry::rest::{
    reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
    RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
};

/// A Go `resource.ResourceClaimStatus{}`: the status is a value, so it always
/// serializes.
fn empty_status() -> Option<ResourceClaimStatus> {
    Some(ResourceClaimStatus::default())
}

/// `dropDisabledStatusFields` (strategy.go:240-245) minus the
/// `DRAAdminAccess` part, which is on in 1.35 so never drops anything.
pub fn drop_disabled_status_fields(new: &mut ResourceClaim, old: &ResourceClaim) {
    // dropDisabledDRAResourceClaimDeviceStatusFields (:295-300)
    if !feature_gates::enabled(Feature::DRAResourceClaimDeviceStatus)
        && old.status.as_ref().is_none_or(|s| s.devices.is_empty())
    {
        if let Some(s) = new.status.as_mut() {
            s.devices.clear();
        }
    }
    let old_results = |c: &ResourceClaim| {
        c.status
            .as_ref()
            .and_then(|s| s.allocation.as_ref())
            .map(|a| a.devices.results.clone())
            .unwrap_or_default()
    };
    let old_res = old_results(old);
    // draConsumableCapacityFeatureInUse (:354-379); the spec half is shared
    // with the template/claim spec drop.
    let capacity_in_use = super::spec::consumable_capacity_in_use(Some(&old.spec))
        || old_res
            .iter()
            .any(|r| r.share_id.is_some() || r.consumed_capacity.is_some())
        || old
            .status
            .as_ref()
            .is_some_and(|s| s.devices.iter().any(|d| d.share_id.is_some()));
    // dropDisabledDRAResourceClaimConsumableCapacityStatusFields (:397-416)
    if !feature_gates::enabled(Feature::DRAConsumableCapacity) && !capacity_in_use {
        if let Some(status) = new.status.as_mut() {
            if let Some(a) = status.allocation.as_mut() {
                for r in &mut a.devices.results {
                    r.share_id = None;
                    r.consumed_capacity = None;
                }
            }
            for d in &mut status.devices {
                d.share_id = None;
            }
        }
    }
    // draDeviceBindingConditionsInUse (:381-393) and
    // dropDeviceBindingConditionsFields (:420-433). Upstream tests `!= nil`;
    // an empty slice is omitted on the wire, so `is_empty` is the equivalent.
    let binding_in_use = old_res
        .iter()
        .any(|r| !r.binding_conditions.is_empty() || !r.binding_failure_conditions.is_empty());
    if !feature_gates::enabled(Feature::DRADeviceBindingConditions) && !binding_in_use {
        if let Some(a) = new.status.as_mut().and_then(|s| s.allocation.as_mut()) {
            for r in &mut a.devices.results {
                r.binding_conditions.clear();
                r.binding_failure_conditions.clear();
            }
        }
    }
}

/// `dropDeallocatedStatusDevices` (strategy.go:302-352): a `status.devices`
/// entry for a device share the old allocation held and the new one no longer
/// does is removed. The identity is `SharedDeviceID`: driver, pool, device and
/// shareID.
pub fn drop_deallocated_status_devices(new: &mut ResourceClaim, old: &ResourceClaim) {
    // strategy.go:309-311
    if !feature_gates::enabled(Feature::DRAResourceClaimDeviceStatus)
        && old.status.as_ref().is_none_or(|s| s.devices.is_empty())
    {
        return;
    }
    type Id = (String, String, String, Option<String>);
    let ids = |c: &ResourceClaim| -> HashSet<Id> {
        c.status
            .as_ref()
            .and_then(|s| s.allocation.as_ref())
            .map(|a| {
                a.devices
                    .results
                    .iter()
                    .map(|r| {
                        (
                            r.driver.clone(),
                            r.pool.clone(),
                            r.device.clone(),
                            r.share_id.clone(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let still_allocated = ids(new);
    let deallocated: HashSet<Id> = ids(old).difference(&still_allocated).cloned().collect();
    if let Some(status) = new.status.as_mut() {
        status.devices.retain(|d| {
            !deallocated.contains(&(
                d.driver.clone(),
                d.pool.clone(),
                d.device.clone(),
                d.share_id.clone(),
            ))
        });
    }
}

/// `resourceclaimStrategy` (strategy.go:47-67).
pub struct Strategy {
    storage: Arc<StorageBackend>,
}

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

    /// `AuthorizedForAdmin` (strategy.go:102), ahead of the validation errors.
    fn validate_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ResourceClaim,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let ns = ctx
                .namespace
                .clone()
                .or_else(|| obj.metadata.namespace.clone())
                .unwrap_or_default();
            authorized_for_admin(&self.storage, &obj.spec.devices.requests, &ns).await
        })
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
    /// `GetResetFields` (pkg/registry/resource/resourceclaim/strategy.go:72): status.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new()
            .with("resource.k8s.io/v1alpha3", &[&["status"]])
            .with("resource.k8s.io/v1beta1", &[&["status"]])
            .with("resource.k8s.io/v1beta2", &[&["status"]])
            .with("resource.k8s.io/v1", &[&["status"]])
    }

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
pub struct StatusStrategy {
    storage: Arc<StorageBackend>,
}

impl NamespaceScopedStrategy for StatusStrategy {
    fn namespace_scoped(&self) -> bool {
        true
    }
}

impl RestUpdateStrategy<ResourceClaim> for StatusStrategy {
    /// `GetResetFields` (pkg/registry/resource/resourceclaim/strategy.go:153): metadata, spec.
    fn get_reset_fields(&self) -> crate::ssa::ResetFields {
        crate::ssa::ResetFields::new()
            .with("resource.k8s.io/v1alpha3", &[&["metadata"], &["spec"]])
            .with("resource.k8s.io/v1beta1", &[&["metadata"], &["spec"]])
            .with("resource.k8s.io/v1beta2", &[&["metadata"], &["spec"]])
            .with("resource.k8s.io/v1", &[&["metadata"], &["spec"]])
    }

    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// Only status may change (strategy.go:177-184). `DRAAdminAccess` is on
    /// in 1.35, so nothing is dropped for `adminAccess`.
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceClaim,
        old: &ResourceClaim,
    ) {
        obj.spec = old.spec.clone();
        reset_object_meta_for_status(&mut obj.metadata, &old.metadata);
        drop_disabled_status_fields(obj, old);
        drop_deallocated_status_devices(obj, old); // NOP if fields got dropped, so last.
    }

    /// `AuthorizedForAdminStatus` (strategy.go:190-196), ahead of the
    /// validation errors.
    fn validate_update_async<'a>(
        &'a self,
        ctx: &'a RequestContext,
        obj: &'a ResourceClaim,
        old: &'a ResourceClaim,
    ) -> ValidateFuture<'a> {
        Box::pin(async move {
            let results = |c: &ResourceClaim| {
                c.status
                    .as_ref()
                    .and_then(|s| s.allocation.as_ref())
                    .map(|a| a.devices.results.clone())
                    .unwrap_or_default()
            };
            let ns = ctx
                .namespace
                .clone()
                .or_else(|| obj.metadata.namespace.clone())
                .unwrap_or_default();
            authorized_for_admin_status(&self.storage, &results(obj), &results(old), &ns).await
        })
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
        Arc::new(Strategy {
            storage: storage.clone(),
        }),
    );
    store.return_deleted_object = true;
    let status = store.with_update_strategy(Arc::new(StatusStrategy { storage }));
    (store, status)
}
