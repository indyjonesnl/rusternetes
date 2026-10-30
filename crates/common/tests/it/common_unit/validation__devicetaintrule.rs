//! Unit tests moved from `src/validation/devicetaintrule.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::devicetaintrule::{
        DeviceTaintRule, DeviceTaintRuleSpec, DeviceTaintSelector,
        DEVICE_TAINT_RULE_STATUS_MAX_CONDITIONS,
    };
    pub use rusternetes_common::validation::csinode::validate_csi_driver_name;
    pub use rusternetes_common::validation::field::{Error, ErrorList, Path};
    pub use rusternetes_common::validation::metav1::validate_conditions;
    pub use rusternetes_common::validation::objectmeta::{
        name_is_dns_subdomain, validate_object_meta, validate_object_meta_update,
    };
    pub use rusternetes_common::validation::resourceslice::{
        validate_device_name, validate_device_taint, validate_pool_name,
    };
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::resources::dra::{DeviceTaint, DeviceTaintEffect};
    use rusternetes_common::types::Condition;
    use rusternetes_common::validation::devicetaintrule::*;

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
