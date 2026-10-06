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

/// get.go:90-110: `export` is refused unless it converts to false
/// (runtime.Convert_Slice_string_To_bool, conversion.go:79-95); the
/// `resourceVersion` reaches the Store.
#[test]
fn decode_get_options_matches_get_resource() {
    use super::get::decode_get_options;

    assert_eq!(decode_get_options(&q(&[])).unwrap().resource_version, "");
    assert_eq!(
        decode_get_options(&q(&[("resourceVersion", "12")]))
            .unwrap()
            .resource_version,
        "12"
    );
    for ok in ["0", "false", "FALSE"] {
        assert!(decode_get_options(&q(&[("export", ok)])).is_ok(), "{ok}");
    }
    for refused in ["true", "1", "", "yes"] {
        assert_eq!(
            bad_request(decode_get_options(&q(&[("export", refused)])).map(|_| ())),
            "the export parameter, deprecated since v1.14, is no longer supported",
            "{refused:?}"
        );
    }
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

fn owner(uid: &str, controller: Option<bool>) -> rusternetes_common::types::OwnerReference {
    rusternetes_common::types::OwnerReference {
        api_version: "v1".into(),
        kind: "Kind".into(),
        name: "name".into(),
        uid: uid.into(),
        block_owner_deletion: controller,
        controller,
    }
}

/// rest_test.go `TestDedupOwnerReferences`: an entry is dropped only when it
/// is wholly equal to an earlier one; same UID with other fields differing
/// is kept.
#[test]
fn dedup_owner_references_matches_upstream() {
    use super::rest::dedup_owner_references;

    let refs = vec![owner("1", None), owner("2", None), owner("1", None)];
    let (deduped, dups) = dedup_owner_references(&refs);
    assert_eq!(deduped, vec![owner("1", None), owner("2", None)]);
    assert_eq!(dups, vec!["1".to_string()]);

    let refs = vec![owner("1", Some(false)), owner("1", None)];
    let (deduped, dups) = dedup_owner_references(&refs);
    assert_eq!(deduped, refs, "semantic-different entries are kept");
    assert!(dups.is_empty());
}

/// rest.go:332-353: duplicates are removed from the object and a warning is
/// recorded; the text differs after mutating admission.
#[test]
fn dedup_owner_references_and_add_warning_matches_upstream() {
    use super::rest::dedup_owner_references_and_add_warning;
    use crate::registry::rest::RequestContext;

    for (after, needle) in [
        (
            false,
            ".metadata.ownerReferences contains duplicate entries; API server dedups",
        ),
        (
            true,
            ".metadata.ownerReferences contains duplicate entries after mutating admission happens; API server dedups",
        ),
    ] {
        let mut c = cm("a", Some("ns"));
        c.metadata.owner_references = Some(vec![owner("u1", None), owner("u1", None)]);
        let ctx = RequestContext::new(Some("ns"));
        dedup_owner_references_and_add_warning(&mut c, &ctx, after);
        assert_eq!(c.metadata.owner_references, Some(vec![owner("u1", None)]));
        let w = ctx.warnings();
        assert_eq!(w.len(), 1);
        assert!(w[0].starts_with(needle), "{}", w[0]);
        assert!(w[0].ends_with("please fix your requests; duplicate UID(s) observed: u1"));
    }

    // No duplicates: nothing recorded, object untouched.
    let mut c = cm("a", Some("ns"));
    c.metadata.owner_references = Some(vec![owner("u1", None), owner("u2", None)]);
    let ctx = RequestContext::new(Some("ns"));
    dedup_owner_references_and_add_warning(&mut c, &ctx, false);
    assert!(ctx.warnings().is_empty());
}
