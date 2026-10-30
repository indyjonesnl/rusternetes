//! DeviceTaintRule validation — port of upstream
//! `pkg/apis/resource/validation/validation.go` (release-1.35):
//! `ValidateDeviceTaintRule` :1339-1344, `ValidateDeviceTaintRuleUpdate`
//! :1346-1351, `validateDeviceTaintRuleSpec` :1353-1367,
//! `validateDeviceTaintSelector` :1369-1386 and
//! `ValidateDeviceTaintRuleStatusUpdate` :1488-1500. `validateDeviceTaint` is
//! shared with ResourceSlice (see `resourceslice.rs`).

use crate::resources::devicetaintrule::{
    DeviceTaintRule, DeviceTaintRuleSpec, DeviceTaintSelector,
    DEVICE_TAINT_RULE_STATUS_MAX_CONDITIONS,
};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::validate_conditions;
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
};
use crate::validation::resourceslice::{
    validate_device_name, validate_device_taint, validate_pool_name,
};

/// `ValidateDeviceTaintRule` (validation.go:1340-1344).
pub fn validate_device_taint_rule(rule: &DeviceTaintRule) -> ErrorList {
    let mut errs = validate_object_meta(
        &rule.metadata,
        false,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_spec(&rule.spec, None, &Path::new("spec")));
    errs
}

/// `ValidateDeviceTaintRuleUpdate` (validation.go:1347-1351).
pub fn validate_device_taint_rule_update(
    rule: &DeviceTaintRule,
    old: &DeviceTaintRule,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&rule.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_spec(
        &rule.spec,
        Some(&old.spec),
        &Path::new("spec"),
    ));
    errs
}

/// `validateDeviceTaintRuleSpec` (validation.go:1353-1367).
fn validate_spec(
    spec: &DeviceTaintRuleSpec,
    old_spec: Option<&DeviceTaintRuleSpec>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = validate_selector(
        spec.device_selector.as_ref(),
        &fld_path.child("deviceSelector"),
    );
    errs.extend(validate_device_taint(
        &spec.taint,
        old_spec.map(|o| &o.taint),
        &fld_path.child("taint"),
    ));
    errs
}

/// `validateDeviceTaintSelector` (validation.go:1369-1386). The driver goes
/// through `validateDriverName` (= `ValidateCSIDriverName`), as ResourceSlice's.
fn validate_selector(filter: Option<&DeviceTaintSelector>, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();
    let Some(filter) = filter else {
        return errs;
    };
    if let Some(driver) = &filter.driver {
        errs.extend(validate_csi_driver_name(driver, &fld_path.child("driver")));
    }
    if let Some(pool) = &filter.pool {
        errs.extend(validate_pool_name(pool, &fld_path.child("pool")));
    }
    if let Some(device) = &filter.device {
        errs.extend(validate_device_name(device, &fld_path.child("device")));
    }
    errs
}

/// `ValidateDeviceTaintRuleStatusUpdate` (validation.go:1489-1500).
pub fn validate_device_taint_rule_status_update(
    rule: &DeviceTaintRule,
    old: &DeviceTaintRule,
) -> ErrorList {
    let fld_path = Path::new("status");
    // Covers invalid name changes.
    let mut errs =
        validate_object_meta_update(&rule.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_conditions(
        &rule.status.conditions,
        &fld_path.child("conditions"),
    ));
    if rule.status.conditions.len() > DEVICE_TAINT_RULE_STATUS_MAX_CONDITIONS {
        errs.push(Error::too_many(
            &fld_path.child("conditions"),
            rule.status.conditions.len(),
            DEVICE_TAINT_RULE_STATUS_MAX_CONDITIONS,
        ));
    }
    errs
}
