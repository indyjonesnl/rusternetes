//! Unit tests moved from `src/validation/csinode.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::csi::{CSINode, CSINodeDriver};
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::{is_dns1123_subdomain, is_qualified_name};
    pub use rusternetes_common::validation::objectmeta::{
        validate_immutable_field, validate_nonnegative_field,
    };
    pub use std::collections::HashSet;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::csi::{CSINodeSpec, VolumeNodeResources};
    use rusternetes_common::validation::csinode::*;

    const LONG: CsiNodeValidationOptions = CsiNodeValidationOptions {
        allow_long_node_id: true,
    };

    fn node(drivers: Vec<CSINodeDriver>) -> CSINode {
        CSINode {
            type_meta: Default::default(),
            metadata: Default::default(),
            spec: CSINodeSpec { drivers },
        }
    }

    fn driver(name: &str, node_id: &str) -> CSINodeDriver {
        CSINodeDriver {
            name: name.to_string(),
            node_id: node_id.to_string(),
            topology_keys: None,
            allocatable: None,
        }
    }

    #[test]
    fn unchanged_driver_passes() {
        let old = node(vec![driver("csi.example.com", "node-1")]);
        let new = node(vec![driver("csi.example.com", "node-1")]);
        assert!(validate_csi_node_update(&new, &old, LONG).is_empty());
    }

    #[test]
    fn mutating_existing_driver_node_id_is_immutable() {
        let old = node(vec![driver("csi.example.com", "node-1")]);
        let new = node(vec![driver("csi.example.com", "node-2")]);
        let errs = validate_csi_node_update(&new, &old, LONG);
        assert!(
            errs.iter().any(|e| e.detail == "field is immutable"),
            "expected immutability error, got {errs:?}"
        );
    }

    /// `MutableCSINodeAllocatableCount` (default on): `allocatable` may change.
    #[test]
    fn mutating_existing_driver_allocatable_is_allowed() {
        let old = node(vec![driver("csi.example.com", "node-1")]);
        let mut d = driver("csi.example.com", "node-1");
        d.allocatable = Some(VolumeNodeResources { count: Some(10) });
        let new = node(vec![d]);
        assert!(validate_csi_node_update(&new, &old, LONG).is_empty());
    }

    #[test]
    fn adding_a_new_driver_is_allowed() {
        let old = node(vec![driver("csi.example.com", "node-1")]);
        let new = node(vec![
            driver("csi.example.com", "node-1"),
            driver("other.example.com", "node-1"),
        ]);
        assert!(validate_csi_node_update(&new, &old, LONG).is_empty());
    }
}
