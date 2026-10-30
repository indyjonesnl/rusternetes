//! Unit tests moved from `src/registry/storage/volumeattributesclass.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_api_server::registry::generic::Store;
    pub use rusternetes_api_server::registry::rest::{
        GroupResource, NamespaceScopedStrategy, RequestContext, RestCreateStrategy,
        RestDeleteStrategy, RestUpdateStrategy,
    };
    pub use rusternetes_common::resources::csi::VolumeAttributesClass;
    pub use rusternetes_common::validation::field::ErrorList;
    pub use rusternetes_common::validation::volumeattributesclass::{
        validate_volume_attributes_class, validate_volume_attributes_class_update,
    };
    pub use rusternetes_storage::StorageBackend;
    pub use std::sync::Arc;
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::registry::storage::volumeattributesclass::*;

    fn vac(driver: &str, param: &str) -> VolumeAttributesClass {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttributesClass",
            "metadata": {"name": "gold"}, "driverName": driver,
            "parameters": {"iops": param}
        }))
        .unwrap()
    }

    fn ctx() -> RequestContext {
        RequestContext::new(None)
    }

    #[test]
    fn flags_and_update_immutability_match_upstream() {
        assert!(!Strategy.namespace_scoped());
        assert!(!Strategy.allow_create_on_update());
        assert!(Strategy.allow_unconditional_update());
        let old = vac("csi.example.com", "100");
        assert!(Strategy.validate(&ctx(), &old).is_empty());
        let errs = Strategy.validate_update(&ctx(), &vac("csi.example.com", "200"), &old);
        assert!(errs.iter().any(|e| e.field == "parameters"), "{errs:?}");
    }
}
