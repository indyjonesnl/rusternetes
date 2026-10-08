//! Port of upstream `TestValidationOptionsForPersistentVolumeClaimTemplate`
//! (pkg/apis/core/validation/validation_test.go:3196-3226, release-1.35) plus
//! the selector branch of `ValidationOptionsForPersistentVolumeClaimTemplate`
//! (validation.go:2375-2381), which the upstream test does not exercise.

use rusternetes_common::resources::pod::PersistentVolumeClaimTemplate;
use rusternetes_common::resources::volume::{LabelSelector, PersistentVolumeClaimSpec};
use rusternetes_common::validation::pvc::validation_options_for_persistent_volume_claim_template;
use std::collections::HashMap;

fn template_with_selector(value: &str) -> PersistentVolumeClaimTemplate {
    PersistentVolumeClaimTemplate {
        metadata: None,
        spec: PersistentVolumeClaimSpec {
            selector: Some(LabelSelector {
                match_labels: Some(HashMap::from([("k".to_string(), value.to_string())])),
                match_expressions: None,
            }),
            ..Default::default()
        },
    }
}

#[test]
fn nil_old_template_uses_defaults() {
    let o = validation_options_for_persistent_volume_claim_template(None);
    assert!(!o.allow_invalid_label_value_in_selector);
}

#[test]
fn valid_old_selector_stays_strict() {
    let old = template_with_selector("ok");
    let o = validation_options_for_persistent_volume_claim_template(Some(&old));
    assert!(!o.allow_invalid_label_value_in_selector);
}

#[test]
fn invalid_old_selector_label_value_is_tolerated() {
    let old = template_with_selector("not a valid value!");
    let o = validation_options_for_persistent_volume_claim_template(Some(&old));
    assert!(o.allow_invalid_label_value_in_selector);
}
