//! Unit tests moved from `src/registry/resource/resourceclaim.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::resource::admin::{
        authorized_for_admin, authorized_for_admin_status,
    };
    pub use rusternetes_api_server::registry::resource::spec::drop_disabled_fields as drop_disabled_spec_fields;
    pub use rusternetes_api_server::registry::rest::{
        reset_object_meta_for_status, GroupResource, NamespaceScopedStrategy, RequestContext,
        RestCreateStrategy, RestDeleteStrategy, RestUpdateStrategy, ValidateFuture,
    };
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::resources::dra::ResourceClaimStatus;
    pub use rusternetes_common::resources::ResourceClaim;
    pub use rusternetes_common::validation::field::{ErrorList, Path};
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use rusternetes_common::validation::resourceclaim::{
        validate_resource_claim, validate_resource_claim_status_update,
        validate_resource_claim_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::collections::HashSet;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::resource::resourceclaim::*;

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
