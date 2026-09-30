//! CSIStorageCapacity validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSIStorageCapacity`
//! (release-1.35).
//!
//! Covers `nodeTopology` (a label selector), `storageClassName` (DNS-subdomain
//! class name), and `capacity` (a non-negative quantity). ObjectMeta is
//! validated separately (#1087 / #1277). CSI is a non-negotiable contract.

use crate::quantity::Quantity;
use crate::resources::csi::CSIStorageCapacity;
use crate::resources::volume::LabelSelector as VolumeLabelSelector;
use crate::types::{LabelSelector, LabelSelectorRequirement};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_subdomain, validate_label_selector, LabelSelectorValidationOptions,
};
use crate::validation::runtimeclass::get_node_label_deprecated_message;

/// The `nodeTopology` field uses the `volume` crate's structurally-identical
/// `LabelSelector`; convert it to the `types` one the metav1 validator expects.
fn to_types_selector(sel: &VolumeLabelSelector) -> LabelSelector {
    LabelSelector {
        match_labels: sel.match_labels.clone(),
        match_expressions: sel.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| LabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: r.operator.clone(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    }
}

/// `CSIStorageCapacityValidateOptions` (validation.go:590-592).
#[derive(Debug, Clone, Copy, Default)]
pub struct CsiStorageCapacityValidateOptions {
    pub allow_invalid_label_value_in_selector: bool,
}

/// `hasInvalidLabelValueInLabelSelector`
/// (pkg/registry/storage/csistoragecapacity/strategy.go:95-98).
pub fn has_invalid_label_value_in_label_selector(csc: &CSIStorageCapacity) -> bool {
    csc.node_topology.as_ref().is_some_and(|t| {
        !validate_label_selector(
            &to_types_selector(t),
            LabelSelectorValidationOptions::default(),
            &Path::new("nodeTopology"),
        )
        .is_empty()
    })
}

/// `GetWarningsForNodeSelector` (pkg/api/node/util.go:64-92): a warning per
/// deprecated node label in `matchExpressions` keys and `matchLabels` keys.
pub fn get_warnings_for_node_selector(
    selector: &VolumeLabelSelector,
    fld_path: &Path,
) -> Vec<String> {
    let mut warnings = Vec::new();
    for (i, expr) in selector.match_expressions.iter().flatten().enumerate() {
        if let Some(msg) = get_node_label_deprecated_message(&expr.key) {
            warnings.push(format!(
                "{}: {} is {}",
                fld_path.child("matchExpressions").index(i).child("key"),
                expr.key,
                msg
            ));
        }
    }
    for label in selector.match_labels.iter().flat_map(|m| m.keys()) {
        if let Some(msg) = get_node_label_deprecated_message(label) {
            warnings.push(format!(
                "{}: {}",
                fld_path.child("matchLabels").child(label),
                msg
            ));
        }
    }
    warnings
}

/// `GetWarningsForCSIStorageCapacity` (pkg/api/storage/util.go:44-49).
pub fn get_warnings_for_csi_storage_capacity(csc: &CSIStorageCapacity) -> Vec<String> {
    csc.node_topology
        .as_ref()
        .map(|t| get_warnings_for_node_selector(t, &Path::new("nodeTopology")))
        .unwrap_or_default()
}

/// Validate a `CSIStorageCapacity`. Mirrors upstream
/// `ValidateCSIStorageCapacity` (validation.go:595-606) minus ObjectMeta.
pub fn validate_csi_storage_capacity(
    csc: &CSIStorageCapacity,
    opts: CsiStorageCapacityValidateOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if let Some(topology) = &csc.node_topology {
        errs.extend(validate_label_selector(
            &to_types_selector(topology),
            LabelSelectorValidationOptions {
                allow_invalid_label_value_in_selector: opts.allow_invalid_label_value_in_selector,
                ..Default::default()
            },
            &Path::new("nodeTopology"),
        ));
    }

    // storageClassName — a DNS-subdomain class name (also rejects empty).
    for msg in is_dns1123_subdomain(&csc.storage_class_name) {
        errs.push(Error::invalid(
            &Path::new("storageClassName"),
            csc.storage_class_name.clone(),
            msg,
        ));
    }

    // capacity — a non-negative quantity when present.
    if let Some(capacity) = &csc.capacity {
        let cap_path = Path::new("capacity");
        match Quantity::parse(capacity) {
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &cap_path,
                        capacity.clone(),
                        "must be greater than or equal to 0",
                    ));
                }
            }
            Err(e) => errs.push(Error::invalid(&cap_path, capacity.clone(), e.to_string())),
        }
    }

    errs
}

/// Validate a CSIStorageCapacity update — upstream
/// `ValidateCSIStorageCapacityUpdate` (pkg/apis/storage/validation). The CSI
/// `GetCapacity` input fields `nodeTopology` and `storageClassName` are
/// immutable.
pub fn validate_csi_storage_capacity_update(
    new_csc: &CSIStorageCapacity,
    old_csc: &CSIStorageCapacity,
) -> ErrorList {
    let mut errs = ErrorList::new();
    if serde_json::to_value(&new_csc.node_topology).ok()
        != serde_json::to_value(&old_csc.node_topology).ok()
    {
        errs.push(Error::invalid(
            &Path::new("nodeTopology"),
            serde_json::to_value(&new_csc.node_topology).unwrap_or_default(),
            "field is immutable",
        ));
    }
    if new_csc.storage_class_name != old_csc.storage_class_name {
        errs.push(Error::invalid(
            &Path::new("storageClassName"),
            new_csc.storage_class_name.clone(),
            "field is immutable",
        ));
    }
    errs
}
