//! ResourceClaim strategies and storage — port of
//! `pkg/registry/resource/resourceclaim/strategy.go` and
//! `pkg/registry/resource/resourceclaim/storage/storage.go`.
//!
//! The declarative validation check that follows each validator is not
//! modelled. The admin-access namespace check is described in [`super::admin`].

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
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
use rusternetes_common::Result;
use rusternetes_storage::StorageBackend;

use super::admin::{authorized_for_admin, authorized_for_admin_status};
use super::spec::drop_disabled_fields as drop_disabled_spec_fields;
use crate::registry::generic::store::{
    BeginCreate, BeginUpdate, CreateOptions, Finish, UpdateOptions,
};
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

/// `dropDisabledStatusFields` (strategy.go:240-245) minus the
/// `DRAAdminAccess` part, which is on in 1.35 so never drops anything.
fn drop_disabled_status_fields(new: &mut ResourceClaim, old: &ResourceClaim) {
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
fn drop_deallocated_status_devices(new: &mut ResourceClaim, old: &ResourceClaim) {
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

/// Hook running `AuthorizedForAdminStatus` before the status update strategy
/// (strategy.go:190-196).
struct AdminStatusCheck {
    storage: Arc<StorageBackend>,
}

#[async_trait]
impl BeginUpdate<ResourceClaim> for AdminStatusCheck {
    async fn begin_update(
        &self,
        ctx: &RequestContext,
        obj: &mut ResourceClaim,
        old: &mut ResourceClaim,
        _options: &UpdateOptions,
    ) -> Result<Box<dyn Finish>> {
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
        authorized_for_admin_status(&self.storage, &results(obj), &results(old), &ns).await?;
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
    store.begin_create = Some(Arc::new(AdminCheck {
        storage: storage.clone(),
    }));
    let mut status = store.with_update_strategy(Arc::new(StatusStrategy));
    status.begin_update = Some(Arc::new(AdminStatusCheck { storage }));
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

    fn gated_claim() -> ResourceClaim {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name": "c"},
            "status": {"allocation": {"devices": {"results": [{
                "request": "r", "driver": "drv", "pool": "p", "device": "a",
                "shareID": "u1", "consumedCapacity": {"memory": "1Gi"},
                "bindingConditions": ["x"], "bindingFailureConditions": ["y"]
            }]}}, "devices": [{"driver": "drv", "pool": "p", "device": "a", "shareID": "u1"}]}
        }))
        .unwrap()
    }

    fn first_result(
        c: &ResourceClaim,
    ) -> &rusternetes_common::resources::dra::DeviceRequestAllocationResult {
        &c.status
            .as_ref()
            .unwrap()
            .allocation
            .as_ref()
            .unwrap()
            .devices
            .results[0]
    }

    /// strategy.go:397-416: with `DRAConsumableCapacity` off (the 1.35
    /// default) and no old use, shareID / consumedCapacity are dropped.
    #[test]
    #[serial_test::serial]
    fn consumable_capacity_status_fields_are_dropped_when_gate_off() {
        let mut new = gated_claim();
        drop_disabled_status_fields(&mut new, &claim(&[], &[]));
        let r = first_result(&new);
        assert!(r.share_id.is_none() && r.consumed_capacity.is_none());
        assert!(new.status.unwrap().devices[0].share_id.is_none());
    }

    /// strategy.go:398-402: fields already in use on the old claim survive.
    #[test]
    #[serial_test::serial]
    fn consumable_capacity_status_fields_survive_when_in_use() {
        let mut new = gated_claim();
        drop_disabled_status_fields(&mut new, &gated_claim());
        let r = first_result(&new);
        assert!(r.share_id.is_some() && r.consumed_capacity.is_some());
        assert!(new.status.unwrap().devices[0].share_id.is_some());
    }

    /// strategy.go:421-432: binding conditions are dropped with the gate off.
    #[test]
    #[serial_test::serial]
    fn binding_conditions_are_dropped_when_gate_off_and_kept_when_on() {
        let mut new = gated_claim();
        drop_disabled_status_fields(&mut new, &claim(&[], &[]));
        let r = first_result(&new);
        assert!(r.binding_conditions.is_empty() && r.binding_failure_conditions.is_empty());

        let _g = rusternetes_common::feature_gates::with_feature(
            rusternetes_common::feature_gates::Feature::DRADeviceBindingConditions,
            true,
        );
        let mut new = gated_claim();
        drop_disabled_status_fields(&mut new, &claim(&[], &[]));
        assert_eq!(first_result(&new).binding_conditions, vec!["x"]);
    }

    /// strategy.go:319-341: the share is part of the device identity, so a
    /// status entry for a deallocated share goes while another share stays.
    #[test]
    #[serial_test::serial]
    fn deallocation_is_per_share() {
        let mk = |shares: &[&str], dev: &[&str]| -> ResourceClaim {
            let res: Vec<_> = shares.iter().map(|s| serde_json::json!({"request": "r", "driver": "drv", "pool": "p", "device": "a", "shareID": s})).collect();
            let devs: Vec<_> = dev.iter().map(|s| serde_json::json!({"driver": "drv", "pool": "p", "device": "a", "shareID": s})).collect();
            serde_json::from_value(serde_json::json!({"metadata": {"name": "c"}, "status": {"allocation": {"devices": {"results": res}}, "devices": devs}})).unwrap()
        };
        let old = mk(&["s1", "s2"], &["s1", "s2"]);
        let mut new = mk(&["s1"], &["s1", "s2"]);
        drop_deallocated_status_devices(&mut new, &old);
        let shares: Vec<_> = new
            .status
            .unwrap()
            .devices
            .into_iter()
            .map(|d| d.share_id)
            .collect();
        assert_eq!(shares, vec![Some("s1".to_string())]);
    }
}
