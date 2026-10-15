//! Port of `schema/objectmeta/validation_test.go` (`TestValidateEmbeddedResource`,
//! `TestValidate`), release-1.35. Like upstream, the expected errors are
//! matched as a multiset of (type, field) pairs: Go iterates a map, so the
//! order is not part of the contract.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::*;
use crate::validation::field::ErrorType;
use crate::validation::structural::{Extensions, StructuralOrBool};

fn embedded() -> Structural {
    Structural {
        extensions: Extensions {
            x_embedded_resource: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn required(p: &str) -> (ErrorType, String) {
    (ErrorType::Required, p.to_string())
}

fn invalid(p: &str) -> (ErrorType, String) {
    (ErrorType::Invalid, p.to_string())
}

/// `validationMatch.matches` over the whole list: every expected error is
/// found exactly once and nothing else is reported.
fn assert_matches(name: &str, errs: &[Error], expected: &[(ErrorType, String)]) {
    let mut got: Vec<(ErrorType, String)> = errs
        .iter()
        .map(|e| (e.error_type.clone(), e.field.clone()))
        .collect();
    let mut want = expected.to_vec();
    let key = |e: &(ErrorType, String)| format!("{:?} {}", e.0, e.1);
    got.sort_by_key(key);
    want.sort_by_key(key);
    assert_eq!(got, want, "{name}: errors {errs:?}");
}

/// `TestValidateEmbeddedResource`.
#[test]
fn validate_embedded_resource_cases() {
    let cases: Vec<(&str, Value, Vec<(ErrorType, String)>)> = vec![
        (
            "empty",
            json!({}),
            vec![required("apiVersion"), required("kind")],
        ),
        (
            "version and kind",
            json!({"apiVersion": "foo/v1", "kind": "Foo"}),
            vec![],
        ),
        (
            "invalid kind",
            json!({"apiVersion": "foo/v1", "kind": "foo.bar-com"}),
            vec![invalid("kind")],
        ),
        (
            "no name",
            json!({"apiVersion": "foo/v1", "kind": "Foo",
                   "metadata": {"namespace": "kube-system"}}),
            vec![],
        ),
        (
            "no namespace",
            json!({"apiVersion": "foo/v1", "kind": "Foo",
                   "metadata": {"name": "foo"}}),
            vec![],
        ),
        (
            "invalid",
            json!({"apiVersion": "foo/v1", "kind": "Foo", "metadata": {
                "name": "..",
                "namespace": "$$$",
                "labels": {"#": "#"},
                "annotations": {"#": "#"},
            }}),
            vec![
                invalid("metadata.name"),
                invalid("metadata.namespace"),
                invalid("metadata.labels"),      // key
                invalid("metadata.labels"),      // value
                invalid("metadata.annotations"), // key
            ],
        ),
    ];
    let schema = embedded();
    for (name, object, expected) in cases {
        let errs = validate_embedded_resource(None, object.as_object().unwrap(), &schema);
        assert_matches(name, &errs, &expected);
    }
}

/// `TestValidate`.
#[test]
fn validate_cases() {
    let mut nested = embedded();
    nested.properties = BTreeMap::from([("embedded".to_string(), embedded())]);
    let schema = Structural {
        properties: BTreeMap::from([
            ("embedded".to_string(), embedded()),
            ("nested".to_string(), nested),
            (
                "items".to_string(),
                Structural {
                    items: Some(Box::new(embedded())),
                    ..Default::default()
                },
            ),
            (
                "additionalProperties".to_string(),
                Structural {
                    additional_properties: Some(Box::new(StructuralOrBool {
                        structural: Some(embedded()),
                        bool_: false,
                    })),
                    ..Default::default()
                },
            ),
        ]),
        ..Default::default()
    };

    let cases: Vec<(&str, Value, bool, Vec<(ErrorType, String)>)> = vec![
        ("empty", json!({}), false, vec![]),
        (
            "include root",
            json!({}),
            true,
            vec![required("apiVersion"), required("kind")],
        ),
        (
            "embedded",
            json!({"embedded": {}}),
            false,
            vec![required("embedded.apiVersion"), required("embedded.kind")],
        ),
        (
            "nested",
            json!({"nested": {"embedded": {}}}),
            false,
            vec![
                required("nested.apiVersion"),
                required("nested.kind"),
                required("nested.embedded.apiVersion"),
                required("nested.embedded.kind"),
            ],
        ),
        (
            "items",
            json!({"items": [{}]}),
            false,
            vec![required("items[0].apiVersion"), required("items[0].kind")],
        ),
        (
            "additionalProperties",
            json!({"additionalProperties": {"foo": {}}}),
            false,
            vec![
                required("additionalProperties[foo].apiVersion"),
                required("additionalProperties[foo].kind"),
            ],
        ),
    ];
    for (name, object, include_root, expected) in cases {
        let errs = validate(None, &object, Some(&schema), include_root);
        assert_matches(name, &errs, &expected);
    }
}

/// The wording of each `validateEmbeddedResource` branch
/// (validation.go:96-121), which the match-by-type tests above do not pin.
#[test]
fn validate_embedded_resource_messages() {
    let schema = embedded();
    let errs = validate_embedded_resource(
        None,
        json!({"apiVersion": 1, "kind": ""}).as_object().unwrap(),
        &schema,
    );
    let details: Vec<(&str, &str)> = errs
        .iter()
        .map(|e| (e.field.as_str(), e.detail.as_str()))
        .collect();
    assert_eq!(
        details,
        vec![
            ("apiVersion", "must be a string"),
            ("kind", "must not be empty")
        ]
    );

    let errs = validate_embedded_resource(
        None,
        json!({"apiVersion": "a/b/c", "kind": 2})
            .as_object()
            .unwrap(),
        &schema,
    );
    let details: Vec<(&str, &str)> = errs
        .iter()
        .map(|e| (e.field.as_str(), e.detail.as_str()))
        .collect();
    assert_eq!(
        details,
        vec![
            ("apiVersion", "unexpected GroupVersion string: a/b/c"),
            ("kind", "must be a string"),
        ]
    );

    let errs = validate_embedded_resource(
        None,
        json!({"apiVersion": "", "kind": "foo.bar"})
            .as_object()
            .unwrap(),
        &schema,
    );
    let details: Vec<(&str, String)> = errs
        .iter()
        .map(|e| (e.field.as_str(), e.detail.clone()))
        .collect();
    assert_eq!(details[0], ("apiVersion", "must not be empty".to_string()));
    assert_eq!(details[1].0, "kind");
    assert!(
        details[1]
            .1
            .starts_with("may have mixed case, but should otherwise match: "),
        "{details:?}"
    );

    // A malformed `metadata` reports GetObjectMeta's error.
    let errs = validate_embedded_resource(
        None,
        json!({"apiVersion": "v1", "kind": "Foo", "metadata": {"name": 1}})
            .as_object()
            .unwrap(),
        &schema,
    );
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert_eq!(errs[0].field, "metadata");
    assert_eq!(
        errs[0].detail,
        "json: cannot unmarshal number into Go struct field ObjectMeta.name of type string"
    );
}
