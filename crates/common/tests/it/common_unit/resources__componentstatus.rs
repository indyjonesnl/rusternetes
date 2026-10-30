//! Unit tests moved from `src/resources/componentstatus.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{ObjectMeta, TypeMeta};
    pub use serde::{Deserialize, Serialize};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::componentstatus::*;

    #[test]
    fn test_component_status_healthy() {
        let cs = ComponentStatus::healthy("scheduler");

        assert_eq!(cs.metadata.name, "scheduler");
        assert_eq!(cs.type_meta.kind, "ComponentStatus");
        assert_eq!(cs.type_meta.api_version, "v1");

        let conditions = cs.conditions.unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].condition_type, "Healthy");
        assert_eq!(conditions[0].status, "True");
    }

    #[test]
    fn test_component_status_unhealthy() {
        let cs = ComponentStatus::unhealthy("controller-manager", "Connection refused");

        assert_eq!(cs.metadata.name, "controller-manager");

        let conditions = cs.conditions.unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].condition_type, "Healthy");
        assert_eq!(conditions[0].status, "False");
        assert_eq!(conditions[0].error, Some("Connection refused".to_string()));
    }
}
