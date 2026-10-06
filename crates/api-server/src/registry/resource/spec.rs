//! `pkg/api/resourceclaimspec/util.go`: the gate-driven field dropping shared
//! by ResourceClaim and ResourceClaimTemplate.
//!
//! `DRAPrioritizedList` and `DRAAdminAccess` are on in 1.35, so nothing is
//! dropped for them. The `DRADeviceTaints` (tolerations) and
//! `DRAConsumableCapacity` (request capacity, `distinctAttribute`) gates are
//! off.

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

/// `DRAConsumableCapacityFeatureInUse` (util.go:150-170).
pub fn consumable_capacity_in_use(spec: Option<&ResourceClaimSpec>) -> bool {
    spec.is_some_and(|s| {
        s.devices
            .constraints
            .iter()
            .any(|c| c.distinct_attribute.is_some())
            || s.devices.requests.iter().any(|r| {
                r.exactly.as_ref().is_some_and(|e| e.capacity.is_some())
                    || r.first_available.iter().any(|f| f.capacity.is_some())
            })
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
            for f in &mut r.first_available {
                f.capacity = None;
            }
        }
        // util.go:138-140 clears the constraint attribute too.
        for c in &mut new.devices.constraints {
            c.distinct_attribute = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ResourceClaimSpec {
        serde_json::from_value(serde_json::json!({"devices": {
            "constraints": [{"distinctAttribute": "a.example.com/b"}],
            "requests": [{"name": "r", "firstAvailable": [
                {"name": "s", "deviceClassName": "c", "capacity": {"m": {"value": "1"}}}]}]
        }}))
        .unwrap()
    }

    /// util.go:132-149: with `DRAConsumableCapacity` off, `distinctAttribute`
    /// and sub-request capacity are dropped unless the old spec used them.
    #[test]
    #[serial_test::serial]
    fn distinct_attribute_and_subrequest_capacity_are_dropped() {
        let mut new = spec();
        drop_disabled_fields(&mut new, None);
        assert!(new.devices.constraints[0].distinct_attribute.is_none());
        assert!(new.devices.requests[0].first_available[0]
            .capacity
            .is_none());

        let mut kept = spec();
        drop_disabled_fields(&mut kept, Some(&spec()));
        assert!(kept.devices.constraints[0].distinct_attribute.is_some());
        assert!(kept.devices.requests[0].first_available[0]
            .capacity
            .is_some());
    }
}
