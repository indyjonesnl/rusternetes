//! CSINode validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSINode` (release-1.35).
//!
//! Covers each `spec.drivers[]`: CSI driver name format, required nodeID
//! (length-bounded), non-negative `allocatable.count`, topologyKeys
//! (non-empty + unique + qualified names), and duplicate driver names across
//! the list. ObjectMeta is validated separately. CSI is a non-negotiable
//! project contract.

use crate::resources::csi::{CSINode, CSINodeDriver};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_subdomain, is_qualified_name};
use crate::validation::objectmeta::{validate_immutable_field, validate_nonnegative_field};
use std::collections::HashSet;

const CSI_DRIVER_NAME_MAX_LENGTH: usize = 63;
/// `csiNodeIDMaxLength` / `csiNodeIDLongerMaxLength`
/// (pkg/apis/storage/validation/validation.go).
const CSI_NODE_ID_MAX_LENGTH: usize = 192;
const CSI_NODE_ID_LONGER_MAX_LENGTH: usize = 256;

/// `CSINodeValidationOptions` (validation.go:51-54). The CSINode strategy
/// always sets `AllowLongNodeID`.
#[derive(Debug, Clone, Copy, Default)]
pub struct CsiNodeValidationOptions {
    pub allow_long_node_id: bool,
}

/// Port of upstream `ValidateCSIDriverName`: required, ≤63 chars, and a
/// DNS-1123 subdomain when lowercased (caseless). Shared with ResourceSlice
/// (`spec.driver`), which uses the same upstream validator.
pub fn validate_csi_driver_name(name: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if name.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    if name.len() > CSI_DRIVER_NAME_MAX_LENGTH {
        errs.push(Error::too_long(fld_path, CSI_DRIVER_NAME_MAX_LENGTH));
    }
    for msg in is_dns1123_subdomain(&name.to_lowercase()) {
        errs.push(Error::invalid(fld_path, name.to_string(), msg));
    }
    errs
}

/// Port of upstream `validateCSINodeDriver`.
fn validate_csi_node_driver(
    driver: &CSINodeDriver,
    seen_names: &mut HashSet<String>,
    fld_path: &Path,
    opts: CsiNodeValidationOptions,
) -> ErrorList {
    let mut errs = validate_csi_driver_name(&driver.name, &fld_path.child("name"));

    // nodeID — always required, length-bounded.
    let node_id_path = fld_path.child("nodeID");
    if driver.node_id.is_empty() {
        errs.push(Error::required(&node_id_path, ""));
    }
    let max_length = if opts.allow_long_node_id {
        CSI_NODE_ID_LONGER_MAX_LENGTH
    } else {
        CSI_NODE_ID_MAX_LENGTH
    };
    if driver.node_id.len() > max_length {
        errs.push(Error::invalid(
            &node_id_path,
            driver.node_id.clone(),
            format!("must be {max_length} characters or less"),
        ));
    }

    // allocatable.count — non-negative when present.
    if let Some(alloc) = &driver.allocatable {
        if let Some(count) = alloc.count {
            errs.extend(validate_nonnegative_field(
                count as i64,
                &fld_path.child("allocatable").child("count"),
            ));
        }
    }

    // topologyKeys — non-empty, unique, qualified names. Upstream attaches these
    // to the driver path (not a topologyKeys child).
    if let Some(keys) = &driver.topology_keys {
        let mut topo_keys: HashSet<&str> = HashSet::new();
        for key in keys {
            if key.is_empty() {
                errs.push(Error::required(fld_path, ""));
            }
            if !topo_keys.insert(key.as_str()) {
                errs.push(Error::duplicate(fld_path, key.clone()));
            }
            for msg in is_qualified_name(key) {
                errs.push(Error::invalid(fld_path, key.clone(), msg));
            }
        }
    }

    // duplicate driver name across the spec.
    if !seen_names.insert(driver.name.clone()) {
        errs.push(Error::duplicate(
            &fld_path.child("name"),
            driver.name.clone(),
        ));
    }

    errs
}

/// Validate a `CSINode` on create. Mirrors upstream `ValidateCSINode` minus
/// ObjectMeta.
pub fn validate_csi_node(node: &CSINode, opts: CsiNodeValidationOptions) -> ErrorList {
    let drivers_path = Path::new("spec").child("drivers");
    let mut errs: ErrorList = Vec::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    for (i, driver) in node.spec.drivers.iter().enumerate() {
        errs.extend(validate_csi_node_driver(
            driver,
            &mut seen_names,
            &drivers_path.index(i),
            opts,
        ));
    }
    errs
}

/// `ValidateCSINodeUpdate` (validation.go:313-339): the create validation,
/// then any driver present in both old and new (matched by name) is
/// immutable. `MutableCSINodeAllocatableCount` is on by default in 1.35
/// (pkg/features/kube_features.go), so `allocatable` is excluded from the
/// comparison; the bad value is the driver itself.
pub fn validate_csi_node_update(
    new: &CSINode,
    old: &CSINode,
    opts: CsiNodeValidationOptions,
) -> ErrorList {
    let mut errs = validate_csi_node(new, opts);
    let drivers_path = Path::new("spec").child("drivers");

    for old_driver in &old.spec.drivers {
        for new_driver in &new.spec.drivers {
            if old_driver.name != new_driver.name {
                continue;
            }
            let mut old_copy = old_driver.clone();
            let mut new_copy = new_driver.clone();
            old_copy.allocatable = None;
            new_copy.allocatable = None;
            errs.extend(validate_immutable_field(
                &new_copy,
                &old_copy,
                &drivers_path,
            ));
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::csi::{CSINodeSpec, VolumeNodeResources};

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
