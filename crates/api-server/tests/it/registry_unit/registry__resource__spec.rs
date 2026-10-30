//! Unit tests moved from `src/registry/resource/spec.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::feature_gates::{self, Feature};
    pub use rusternetes_common::resources::dra::ResourceClaimSpec;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::resource::spec::*;

    fn spec() -> ResourceClaimSpec {
        serde_json::from_value(serde_json::json!({"devices": {
            "constraints": [{"distinctAttribute": "a.example.com/b"}],
            "requests": [{"name": "r", "firstAvailable": [
                {"name": "s", "deviceClassName": "c", "capacity": {"requests": {"m": "1"}}}]}]
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
