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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::dra::{DeviceTaint, DeviceTaintEffect};
    use crate::types::Condition;

    fn agg(errs: &ErrorList) -> String {
        errs.iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn rule() -> DeviceTaintRule {
        let mut r = DeviceTaintRule::default();
        r.metadata.name = "valid-patch".to_string();
        r.spec.taint = DeviceTaint {
            key: "example.com/tainted".to_string(),
            effect: Some(DeviceTaintEffect::NoExecute),
            ..Default::default()
        };
        r
    }

    #[test]
    fn valid_rule() {
        assert_eq!(agg(&validate_device_taint_rule(&rule())), "");
        let mut r = rule();
        r.spec.device_selector = Some(DeviceTaintSelector {
            driver: Some("example.com".into()),
            pool: Some("pool/a".into()),
            device: Some("dev-1".into()),
        });
        r.spec.taint.value = Some("v".into());
        assert_eq!(agg(&validate_device_taint_rule(&r)), "");
    }

    #[test]
    fn invalid_name_key_effect_value() {
        let mut r = rule();
        r.metadata.name = "%#@$%$".into();
        let out = agg(&validate_device_taint_rule(&r));
        assert!(
            out.contains(
                "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters"
            ),
            "{out}"
        );

        let mut r = rule();
        r.spec.taint.key.clear();
        let out = agg(&validate_device_taint_rule(&r));
        assert!(out.contains("spec.taint.key"), "{out}");

        let mut r = rule();
        r.spec.taint.effect = None;
        assert_eq!(
            agg(&validate_device_taint_rule(&r)),
            "spec.taint.effect: Required value"
        );

        let mut r = rule();
        r.spec.taint.effect = Some(DeviceTaintEffect::Unknown("Bogus".into()));
        let out = agg(&validate_device_taint_rule(&r));
        assert!(
            out.starts_with("spec.taint.effect: Unsupported value: \"Bogus\""),
            "{out}"
        );

        let mut r = rule();
        r.spec.taint.value = Some("not valid!".into());
        let out = agg(&validate_device_taint_rule(&r));
        assert!(out.contains("spec.taint.value: Invalid value"), "{out}");
    }

    #[test]
    fn invalid_selector() {
        let mut r = rule();
        r.spec.device_selector = Some(DeviceTaintSelector {
            driver: Some("".into()),
            pool: Some("".into()),
            device: Some("Bad_Name".into()),
        });
        let out = agg(&validate_device_taint_rule(&r));
        assert!(out.contains("spec.deviceSelector.driver"), "{out}");
        assert!(
            out.contains("spec.deviceSelector.pool: Required value"),
            "{out}"
        );
        assert!(
            out.contains("spec.deviceSelector.device: Invalid value"),
            "{out}"
        );
    }

    /// An unchanged effect is not re-validated on update (grandfathering,
    /// validation.go:1398).
    #[test]
    fn update_effect_only_validated_when_changed() {
        let mut old = rule();
        old.spec.taint.effect = Some(DeviceTaintEffect::Unknown("Legacy".into()));
        old.metadata.resource_version = Some("1".into());
        let new = old.clone();
        assert_eq!(agg(&validate_device_taint_rule_update(&new, &old)), "");

        let mut changed = old.clone();
        changed.spec.taint.effect = Some(DeviceTaintEffect::Unknown("Other".into()));
        assert!(agg(&validate_device_taint_rule_update(&changed, &old))
            .contains("spec.taint.effect: Unsupported value"));
    }

    #[test]
    fn name_change_is_immutable() {
        let mut old = rule();
        old.metadata.resource_version = Some("1".into());
        let mut new = old.clone();
        new.metadata.name.push_str("-2");
        assert!(agg(&validate_device_taint_rule_update(&new, &old)).contains("field is immutable"));
        assert!(agg(&validate_device_taint_rule_status_update(&new, &old))
            .contains("field is immutable"));
    }

    fn cond(i: usize) -> Condition {
        Condition {
            condition_type: format!("T{i}"),
            status: "True".into(),
            observed_generation: None,
            last_transition_time: Some(chrono::Utc::now()),
            reason: Some("R".into()),
            message: Some("m".into()),
        }
    }

    #[test]
    fn status_conditions_capped_at_eight() {
        let mut old = rule();
        old.metadata.resource_version = Some("1".into());
        let mut new = old.clone();
        new.status.conditions = (0..8).map(cond).collect();
        assert_eq!(
            agg(&validate_device_taint_rule_status_update(&new, &old)),
            ""
        );
        new.status.conditions = (0..9).map(cond).collect();
        assert_eq!(
            agg(&validate_device_taint_rule_status_update(&new, &old)),
            "status.conditions: Too many: 9: must have at most 8 items"
        );
    }
}
