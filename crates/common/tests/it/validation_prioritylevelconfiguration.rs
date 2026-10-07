//! Tests for PriorityLevelConfiguration validation (upstream APF).

use rusternetes_common::resources::flowcontrol::{
    ExemptPriorityLevelConfiguration, LimitResponse, LimitResponseType,
    LimitedPriorityLevelConfiguration, PriorityLevelConfiguration, PriorityLevelConfigurationSpec,
    PriorityLevelType, QueuingConfiguration,
};
use rusternetes_common::validation::field::ErrorType;
use rusternetes_common::validation::prioritylevelconfiguration::validate_priority_level_configuration;

fn plc(name: &str, spec: PriorityLevelConfigurationSpec) -> PriorityLevelConfiguration {
    let mut p = PriorityLevelConfiguration {
        api_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
        kind: "PriorityLevelConfiguration".to_string(),
        metadata: Default::default(),
        spec,
        status: None,
    };
    p.metadata.name = name.to_string();
    p
}

fn limited_reject() -> LimitedPriorityLevelConfiguration {
    LimitedPriorityLevelConfiguration {
        nominal_concurrency_shares: Some(30),
        lending_concurrency_limit: None,
        lendable_percent: None,
        borrowing_limit_percent: None,
        limit_response: Some(LimitResponse {
            type_: LimitResponseType::Reject,
            queuing: None,
        }),
    }
}

fn has(errs: &[rusternetes_common::validation::field::Error], field: &str) -> bool {
    errs.iter().any(|e| e.field.contains(field))
}

#[test]
fn valid_limited_passes() {
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(limited_reject()),
        exempt: None,
    };
    assert!(validate_priority_level_configuration(&plc("workload", spec)).is_empty());
}

#[test]
fn exempt_name_requires_exempt_type() {
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(limited_reject()),
        exempt: None,
    };
    let errs = validate_priority_level_configuration(&plc("exempt", spec));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.type" && e.detail.contains("if and only if")));
}

#[test]
fn limited_type_without_limited_rejected() {
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: None,
        exempt: None,
    };
    let errs = validate_priority_level_configuration(&plc("x", spec));
    assert!(errs
        .iter()
        .any(|e| e.field == "spec.limited" && e.error_type == ErrorType::Required));
}

#[test]
fn exempt_type_with_limited_rejected() {
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Exempt,
        limited: Some(limited_reject()),
        exempt: None,
    };
    assert!(has(
        &validate_priority_level_configuration(&plc("exempt", spec)),
        "spec.limited"
    ));
}

#[test]
fn negative_nominal_shares_rejected() {
    let mut l = limited_reject();
    l.nominal_concurrency_shares = Some(-1);
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(l),
        exempt: None,
    };
    assert!(has(
        &validate_priority_level_configuration(&plc("x", spec)),
        "spec.limited.nominalConcurrencyShares"
    ));
}

#[test]
fn queue_type_requires_queuing() {
    let mut l = limited_reject();
    l.limit_response = Some(LimitResponse {
        type_: LimitResponseType::Queue,
        queuing: None,
    });
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(l),
        exempt: None,
    };
    assert!(has(
        &validate_priority_level_configuration(&plc("x", spec)),
        "spec.limited.limitResponse.queuing"
    ));
}

#[test]
fn reject_type_with_queuing_forbidden() {
    let mut l = limited_reject();
    l.limit_response = Some(LimitResponse {
        type_: LimitResponseType::Reject,
        queuing: Some(QueuingConfiguration {
            queues: 64,
            hand_size: 8,
            queue_length_limit: 50,
        }),
    });
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(l),
        exempt: None,
    };
    let errs = validate_priority_level_configuration(&plc("x", spec));
    assert!(errs
        .iter()
        .any(|e| e.field.contains("queuing") && e.error_type == ErrorType::Forbidden));
}

#[test]
fn handsize_greater_than_queues_rejected() {
    let mut l = limited_reject();
    l.limit_response = Some(LimitResponse {
        type_: LimitResponseType::Queue,
        queuing: Some(QueuingConfiguration {
            queues: 4,
            hand_size: 8,
            queue_length_limit: 50,
        }),
    });
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(l),
        exempt: None,
    };
    assert!(has(
        &validate_priority_level_configuration(&plc("x", spec)),
        "handSize"
    ));
}

#[test]
fn valid_queuing_passes() {
    let mut l = limited_reject();
    l.limit_response = Some(LimitResponse {
        type_: LimitResponseType::Queue,
        queuing: Some(QueuingConfiguration {
            queues: 64,
            hand_size: 8,
            queue_length_limit: 50,
        }),
    });
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Limited,
        limited: Some(l),
        exempt: None,
    };
    assert!(validate_priority_level_configuration(&plc("x", spec)).is_empty());
}

#[test]
fn valid_exempt_passes() {
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Exempt,
        limited: None,
        exempt: Some(ExemptPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(0),
            lending_concurrency_limit: None,
            lendable_percent: None,
        }),
    };
    assert!(validate_priority_level_configuration(&plc("exempt", spec)).is_empty());
}

#[test]
fn limited_lendable_percent_out_of_range_rejected() {
    for bad in [-1, 101, 200] {
        let mut l = limited_reject();
        l.lendable_percent = Some(bad);
        let spec = PriorityLevelConfigurationSpec {
            type_: PriorityLevelType::Limited,
            limited: Some(l),
            exempt: None,
        };
        let errs = validate_priority_level_configuration(&plc("lim", spec));
        assert!(
            errs.iter().any(|e| e.field.contains("lendablePercent")
                && e.to_string()
                    .contains("must be between 0 and 100, inclusive")),
            "bad={bad} errs={errs:?}"
        );
    }
}

#[test]
fn limited_lendable_percent_in_range_passes() {
    for ok in [0, 50, 100] {
        let mut l = limited_reject();
        l.lendable_percent = Some(ok);
        let spec = PriorityLevelConfigurationSpec {
            type_: PriorityLevelType::Limited,
            limited: Some(l),
            exempt: None,
        };
        let errs = validate_priority_level_configuration(&plc("lim", spec));
        assert!(
            !errs.iter().any(|e| e.field.contains("lendablePercent")),
            "ok={ok} errs={errs:?}"
        );
    }
}

#[test]
fn exempt_lendable_percent_out_of_range_rejected() {
    let spec = PriorityLevelConfigurationSpec {
        type_: PriorityLevelType::Exempt,
        limited: None,
        exempt: Some(ExemptPriorityLevelConfiguration {
            nominal_concurrency_shares: Some(0),
            lending_concurrency_limit: None,
            lendable_percent: Some(101),
        }),
    };
    let errs = validate_priority_level_configuration(&plc("exempt", spec));
    assert!(
        errs.iter().any(|e| e.field.contains("lendablePercent")
            && e.to_string()
                .contains("must be between 0 and 100, inclusive")),
        "errs={errs:?}"
    );
}

// ---- mandatory-object spec equality (validation.go:364-390) ----

use rusternetes_common::validation::flowcontrol_bootstrap::{
    mandatory_priority_level_configuration, MANDATORY_PRIORITY_LEVEL_NAMES,
};

#[test]
fn mandatory_plcs_validate_clean() {
    for name in MANDATORY_PRIORITY_LEVEL_NAMES {
        let p = mandatory_priority_level_configuration(name).expect("mandatory");
        let errs = validate_priority_level_configuration(&p);
        assert!(errs.is_empty(), "{name}: {errs:?}");
    }
}

#[test]
fn mandatory_catch_all_with_changed_spec_rejected() {
    let mut p = mandatory_priority_level_configuration("catch-all").unwrap();
    p.spec.limited.as_mut().unwrap().nominal_concurrency_shares = Some(6);
    let errs = validate_priority_level_configuration(&p);
    assert!(
        errs.iter().any(|e| e.field == "spec"
            && e.error_type == ErrorType::Invalid
            && e.detail == "spec of 'catch-all' must equal the fixed value"),
        "{errs:?}"
    );
}

#[test]
fn mandatory_exempt_allows_changing_only_the_exempt_field() {
    let mut p = mandatory_priority_level_configuration("exempt").unwrap();
    p.spec.exempt = Some(ExemptPriorityLevelConfiguration {
        nominal_concurrency_shares: Some(7),
        lending_concurrency_limit: None,
        lendable_percent: Some(10),
    });
    assert!(validate_priority_level_configuration(&p).is_empty());
}

#[test]
fn mandatory_exempt_rejects_other_spec_changes() {
    let mut p = mandatory_priority_level_configuration("exempt").unwrap();
    p.spec.limited = Some(limited_reject());
    let errs = validate_priority_level_configuration(&p);
    assert!(
        errs.iter().any(|e| e.field == "spec"
            && e.detail
                == "spec of 'exempt' except the 'spec.exempt' field must equal the fixed value"),
        "{errs:?}"
    );
}

// #2497: unknown enum strings decode and validate to NotSupported.
fn plc_from_json(name: &str, spec: serde_json::Value) -> PriorityLevelConfiguration {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "flowcontrol.apiserver.k8s.io/v1",
        "kind": "PriorityLevelConfiguration",
        "metadata": {"name": name},
        "spec": spec,
    }))
    .expect("an unknown enum string must decode")
}

#[test]
fn unknown_priority_level_type_is_not_supported() {
    // validation.go:419 `default:` arm of `switch spec.Type`.
    let p = plc_from_json("workload", serde_json::json!({"type": "Bogus"}));
    let errs = validate_priority_level_configuration(&p);
    assert!(
        errs.iter()
            .any(|e| e.error_type == ErrorType::NotSupported && e.field == "spec.type"),
        "{errs:?}"
    );
}

#[test]
fn unknown_limit_response_type_is_not_supported() {
    // validation.go:477-479.
    let p = plc_from_json(
        "workload",
        serde_json::json!({"type": "Limited", "limited": {
            "nominalConcurrencyShares": 30, "limitResponse": {"type": "Drop"}}}),
    );
    let errs = validate_priority_level_configuration(&p);
    assert!(
        errs.iter().any(|e| e.error_type == ErrorType::NotSupported
            && e.field == "spec.limited.limitResponse.type"),
        "{errs:?}"
    );
}
