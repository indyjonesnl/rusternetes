//! `autoscaling/v1` Scale: `ValidateScale` (pkg/apis/autoscaling/validation/
//! validation.go:40-49), the wire shape, and the selector string a scale
//! status carries (`LabelSelectorAsSelector(..).String()`).

use rusternetes_common::resources::{Scale, ScaleSpec};
use rusternetes_common::types::{LabelSelector, LabelSelectorRequirement, ObjectMeta};
use rusternetes_common::validation::hpa::validate_scale;
use std::collections::HashMap;

fn scale(replicas: i32) -> Scale {
    Scale {
        metadata: ObjectMeta {
            name: "s".to_string(),
            namespace: Some("default".to_string()),
            ..ObjectMeta::default()
        },
        spec: ScaleSpec { replicas },
        ..Scale::default()
    }
}

#[test]
fn validate_scale_rejects_negative_replicas_and_bad_metadata() {
    assert!(validate_scale(&scale(0)).is_empty());
    assert!(validate_scale(&scale(3)).is_empty());

    let errs = validate_scale(&scale(-1));
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(errs[0].to_string().contains("spec.replicas"), "{errs:?}");

    let mut unnamed = scale(1);
    unnamed.metadata.name.clear();
    assert!(validate_scale(&unnamed)
        .iter()
        .any(|e| e.to_string().contains("metadata.name")));
}

/// `ScaleSpec.Replicas` is `omitempty`; `ScaleStatus.Replicas` is not.
#[test]
fn scale_wire_shape() {
    let v = serde_json::to_value(scale(0)).unwrap();
    assert_eq!(v["spec"], serde_json::json!({}));
    assert_eq!(v["status"], serde_json::json!({"replicas": 0}));
}

fn requirement(key: &str, op: &str, values: &[&str]) -> LabelSelectorRequirement {
    LabelSelectorRequirement {
        key: key.to_string(),
        operator: op.to_string(),
        values: if values.is_empty() {
            None
        } else {
            Some(values.iter().map(|v| v.to_string()).collect())
        },
    }
}

/// The cases of upstream `TestLabelSelectorAsSelector`
/// (apimachinery/pkg/apis/meta/v1/helpers_test.go), rendered as strings.
#[test]
fn label_selector_as_selector_string() {
    assert_eq!(LabelSelector::default().as_selector_string().unwrap(), "");

    let sel = LabelSelector {
        match_labels: Some(HashMap::from([
            ("foo".to_string(), "bar".to_string()),
            ("baz".to_string(), "qux".to_string()),
        ])),
        match_expressions: Some(vec![
            requirement("zone", "In", &["b", "a"]),
            requirement("tier", "NotIn", &["x"]),
            requirement("gpu", "Exists", &[]),
            requirement("legacy", "DoesNotExist", &[]),
        ]),
    };
    assert_eq!(
        sel.as_selector_string().unwrap(),
        "baz=qux,foo=bar,gpu,!legacy,tier notin (x),zone in (a,b)"
    );

    let bad = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![requirement("k", "Near", &["v"])]),
    };
    assert_eq!(
        bad.as_selector_string().unwrap_err(),
        "\"Near\" is not a valid label selector operator"
    );

    let empty_in = LabelSelector {
        match_labels: None,
        match_expressions: Some(vec![requirement("k", "In", &[])]),
    };
    assert!(empty_in
        .as_selector_string()
        .unwrap_err()
        .contains("values set can't be empty"));
}
