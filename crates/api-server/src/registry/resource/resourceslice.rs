//! ResourceSlice strategy and storage — port of
//! `pkg/registry/resource/resourceslice/strategy.go` and
//! `pkg/registry/resource/resourceslice/storage/storage.go`.
//!
//! The declarative validation check that follows `ValidateResourceSlice` is
//! not modelled.

use std::sync::Arc;

use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::ResourceSlice;
use rusternetes_common::validation::field::{ErrorList, Path};
use rusternetes_common::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use rusternetes_common::validation::resourceslice::{
    validate_resource_slice, validate_resource_slice_update,
};
use rusternetes_storage::StorageBackend;

use crate::registry::generic::Store;
use crate::registry::rest::{
    GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy, RestDeleteStrategy,
    RestUpdateStrategy,
};

/// `draDeviceTaintsFeatureInUse` (strategy.go:205-220).
fn device_taints_in_use(slice: Option<&ResourceSlice>) -> bool {
    slice.is_some_and(|s| s.spec.devices.iter().any(|d| !d.taints.is_empty()))
}

/// `draPartitionableDevicesFeatureInUse` (strategy.go:262-283).
fn partitionable_devices_in_use(slice: Option<&ResourceSlice>) -> bool {
    let Some(s) = slice else { return false };
    if !s.spec.shared_counters.is_empty() || s.spec.per_device_node_selection.is_some() {
        return true;
    }
    s.spec.devices.iter().any(|d| {
        !d.consumes_counters.is_empty()
            || d.node_name.is_some()
            || d.node_selector.is_some()
            || d.all_nodes.is_some()
    })
}

/// `draBindingConditionsFeatureInUse` (strategy.go:285-299).
fn binding_conditions_in_use(slice: Option<&ResourceSlice>) -> bool {
    slice.is_some_and(|s| {
        s.spec.devices.iter().any(|d| {
            !d.binding_conditions.is_empty()
                || !d.binding_failure_conditions.is_empty()
                || d.binds_to_node.is_some()
        })
    })
}

/// `draConsumableCapacityFeatureInUse` (strategy.go:301-319).
fn consumable_capacity_in_use(slice: Option<&ResourceSlice>) -> bool {
    slice.is_some_and(|s| {
        s.spec.devices.iter().any(|d| {
            d.allow_multiple_allocations.is_some()
                || d.capacity
                    .iter()
                    .flatten()
                    .any(|(_, c)| c.request_policy.is_some())
        })
    })
}

/// `dropDisabledFields` (strategy.go:190-195): a field behind a disabled gate
/// is cleared unless the old object already uses it.
fn drop_disabled_fields(new: &mut ResourceSlice, old: Option<&ResourceSlice>) {
    if !feature_gates::enabled(Feature::DRADeviceTaints) && !device_taints_in_use(old) {
        for d in &mut new.spec.devices {
            d.taints.clear();
        }
    }

    if !feature_gates::enabled(Feature::DRAPartitionableDevices)
        && !partitionable_devices_in_use(old)
    {
        new.spec.shared_counters.clear();
        new.spec.per_device_node_selection = None;
        for d in &mut new.spec.devices {
            d.consumes_counters.clear();
            d.node_name = None;
            d.node_selector = None;
            d.all_nodes = None;
        }
    }

    let binding_on = feature_gates::enabled(Feature::DRADeviceBindingConditions)
        && feature_gates::enabled(Feature::DRAResourceClaimDeviceStatus);
    if !binding_on && !binding_conditions_in_use(old) {
        for d in &mut new.spec.devices {
            d.binding_conditions.clear();
            d.binding_failure_conditions.clear();
            d.binds_to_node = None;
        }
    }

    if !feature_gates::enabled(Feature::DRAConsumableCapacity) && !consumable_capacity_in_use(old) {
        for d in &mut new.spec.devices {
            d.allow_multiple_allocations = None;
            for c in d.capacity.iter_mut().flat_map(|m| m.values_mut()) {
                c.request_policy = None;
            }
        }
    }
}

/// The lowercase-driver warning shared by `WarningsOnCreate` and
/// `WarningsOnUpdate` (strategy.go:66-74, :103-111).
fn driver_warnings(slice: &ResourceSlice) -> Vec<String> {
    let driver = &slice.spec.driver;
    if *driver != driver.to_lowercase() {
        vec![format!(
            "spec.driver: driver names should be lowercase; {driver:?} contains uppercase characters"
        )]
    } else {
        Vec::new()
    }
}

/// `resourceSliceStrategy` (strategy.go:36-45).
pub struct Strategy;

impl NamespaceScopedStrategy for Strategy {
    fn namespace_scoped(&self) -> bool {
        false
    }
}

impl RestCreateStrategy<ResourceSlice> for Strategy {
    /// `Generation = 1`, then `dropDisabledFields` (strategy.go:47-52).
    fn prepare_for_create(&self, _ctx: &RequestContext, obj: &mut ResourceSlice) {
        obj.metadata.generation = Some(1);
        drop_disabled_fields(obj, None);
    }

    /// `ValidateResourceSlice` (validation.go:636-640).
    fn validate(&self, _ctx: &RequestContext, obj: &ResourceSlice) -> ErrorList {
        let mut errs = validate_object_meta(
            &obj.metadata,
            false,
            name_is_dns_subdomain,
            &Path::new("metadata"),
        );
        errs.extend(validate_resource_slice(obj));
        errs
    }

    fn warnings_on_create(&self, _ctx: &RequestContext, obj: &ResourceSlice) -> Vec<String> {
        driver_warnings(obj)
    }
}

impl RestUpdateStrategy<ResourceSlice> for Strategy {
    fn allow_create_on_update(&self) -> bool {
        false
    }

    /// A spec change increments the generation, then `dropDisabledFields`
    /// (strategy.go:83-92).
    fn prepare_for_update(
        &self,
        _ctx: &RequestContext,
        obj: &mut ResourceSlice,
        old: &ResourceSlice,
    ) {
        if serde_json::to_value(&obj.spec).ok() != serde_json::to_value(&old.spec).ok() {
            obj.metadata.generation = Some(old.metadata.generation.unwrap_or(0) + 1);
        }
        drop_disabled_fields(obj, Some(old));
    }

    /// `ValidateResourceSliceUpdate` (validation.go:643-647).
    fn validate_update(
        &self,
        _ctx: &RequestContext,
        obj: &ResourceSlice,
        old: &ResourceSlice,
    ) -> ErrorList {
        let mut errs =
            validate_object_meta_update(&obj.metadata, &old.metadata, &Path::new("metadata"));
        errs.extend(validate_resource_slice_update(obj, old));
        errs
    }

    fn warnings_on_update(
        &self,
        _ctx: &RequestContext,
        obj: &ResourceSlice,
        _old: &ResourceSlice,
    ) -> Vec<String> {
        driver_warnings(obj)
    }

    fn allow_unconditional_update(&self) -> bool {
        true
    }
}

impl RestDeleteStrategy<ResourceSlice> for Strategy {}

/// `NewREST` (storage/storage.go:36-50): `ReturnDeletedObject: true`.
pub fn new_store(storage: Arc<StorageBackend>) -> Store<ResourceSlice, StorageBackend> {
    let mut store = Store::new(
        storage,
        GroupResource::new("resource.k8s.io", "resourceslices"),
        Arc::new(Strategy),
    );
    store.return_deleted_object = true;
    store
}
