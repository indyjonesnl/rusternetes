//! `pkg/api/resourceclaimspec/util.go`: the gate-driven field dropping shared
//! by ResourceClaim and ResourceClaimTemplate.
//!
//! `DRAPrioritizedList` and `DRAAdminAccess` are on in 1.35, so nothing is
//! dropped for them. The `DRADeviceTaints` (tolerations) and
//! `DRAConsumableCapacity` (request capacity) gates are off. The other gated
//! fields (`distinctAttribute`, sub-request capacity, the status
//! `adminAccess` / `shareID` / `consumedCapacity` / binding conditions) are not
//! in our types, so a client-supplied value is already discarded on decode.

use rusternetes_common::feature_gates::{self, Feature};
use rusternetes_common::resources::dra::ResourceClaimSpec;

/// `draDeviceTaintsInUse` (util.go:57-72).
fn device_taints_in_use(spec: Option<&ResourceClaimSpec>) -> bool {
    spec.is_some_and(|s| {
        s.devices.requests.iter().any(|r| {
            r.exactly
                .as_ref()
                .is_some_and(|e| !e.tolerations.is_empty())
                || r.first_available.iter().any(|f| !f.tolerations.is_empty())
        })
    })
}

/// `DRAConsumableCapacityFeatureInUse` (util.go:150-170), for the fields we
/// model.
fn consumable_capacity_in_use(spec: Option<&ResourceClaimSpec>) -> bool {
    spec.is_some_and(|s| {
        s.devices
            .requests
            .iter()
            .any(|r| r.exactly.as_ref().is_some_and(|e| e.capacity.is_some()))
    })
}

/// `DropDisabledFields` (util.go:28-34).
pub fn drop_disabled_fields(new: &mut ResourceClaimSpec, old: Option<&ResourceClaimSpec>) {
    if !feature_gates::enabled(Feature::DRADeviceTaints) && !device_taints_in_use(old) {
        for r in &mut new.devices.requests {
            if let Some(e) = r.exactly.as_mut() {
                e.tolerations.clear();
            }
            for f in &mut r.first_available {
                f.tolerations.clear();
            }
        }
    }
    if !feature_gates::enabled(Feature::DRAConsumableCapacity) && !consumable_capacity_in_use(old) {
        for r in &mut new.devices.requests {
            if let Some(e) = r.exactly.as_mut() {
                e.capacity = None;
            }
        }
    }
}
