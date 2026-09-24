use std::collections::HashMap;

use rusternetes_common::resources::ConfigMap;
use rusternetes_common::types::{DeletionPropagation, ObjectMeta};
use rusternetes_common::Error;

use super::delete::decode_delete_options;
use super::rest::check_name;

fn cm(name: &str, namespace: Option<&str>) -> ConfigMap {
    ConfigMap {
        type_meta: Default::default(),
        metadata: ObjectMeta {
            name: name.to_string(),
            namespace: namespace.map(str::to_string),
            ..Default::default()
        },
        data: None,
        binary_data: None,
        immutable: None,
    }
}

fn bad_request(r: rusternetes_common::Result<()>) -> String {
    match r {
        Err(Error::BadRequest(msg)) => msg,
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

/// The three `checkName` outcomes (rest.go:272-290, namer.go:74-85).
#[test]
fn check_name_matches_upstream() {
    assert!(check_name(&cm("a", Some("ns")), "a", Some("ns")).is_ok());
    // An object without a namespace is fine: the handler fills it.
    assert!(check_name(&cm("a", None), "a", Some("ns")).is_ok());

    assert_eq!(
        bad_request(check_name(&cm("", None), "a", Some("ns"))),
        "the name of the object (a based on URL) was undeterminable: name must be provided"
    );
    assert_eq!(
        bad_request(check_name(&cm("b", None), "a", Some("ns"))),
        "the name of the object (b) does not match the name on the URL (a)"
    );
    assert_eq!(
        bad_request(check_name(&cm("a", Some("other")), "a", Some("ns"))),
        "the namespace of the object (other) does not match the namespace on the request (ns)"
    );
}

fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// delete.go:95-126: a body wins outright; the query applies only without one.
#[test]
fn delete_options_come_from_the_body_or_else_the_query() {
    let from_query = decode_delete_options(
        &q(&[
            ("propagationPolicy", "Foreground"),
            ("gracePeriodSeconds", "5"),
            ("dryRun", "All"),
        ]),
        b"",
    )
    .unwrap();
    assert_eq!(
        from_query.propagation_policy,
        Some(DeletionPropagation::Foreground)
    );
    assert_eq!(from_query.grace_period_seconds, Some(5));
    assert_eq!(from_query.dry_run, Some(vec!["All".to_string()]));

    let from_body = decode_delete_options(
        &q(&[("propagationPolicy", "Foreground")]),
        br#"{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"u"}}"#,
    )
    .unwrap();
    assert_eq!(from_body.propagation_policy, None);
    assert_eq!(
        from_body.preconditions.and_then(|p| p.uid),
        Some("u".to_string())
    );
}

/// Undecodable options are a BadRequest; decodable-but-invalid ones fail
/// `ValidateDeleteOptions` (delete.go:103-107, 130-134).
#[test]
fn delete_options_errors() {
    assert!(matches!(
        decode_delete_options(&q(&[]), b"not json"),
        Err(Error::BadRequest(_))
    ));
    assert!(matches!(
        decode_delete_options(&q(&[("gracePeriodSeconds", "soon")]), b""),
        Err(Error::BadRequest(_))
    ));
    assert!(matches!(
        decode_delete_options(&q(&[("dryRun", "Maybe")]), b""),
        Err(Error::Invalid(_))
    ));
}
