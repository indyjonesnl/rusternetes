//! Ports of `schema/objectmeta/coerce_test.go` (`TestGetObjectMetaWithOptions`,
//! `TestGetObjectMetaNils`, `TestGetObjectMeta`, the malformed-field cases of
//! `TestMalformedObjectMetaFields`) and `algorithm_test.go` (`TestCoerce`),
//! release-1.35. The Go fuzz tests (`TestRoundTripObjectMeta`, the fuzzed half
//! of `TestMalformedObjectMetaFields`) are replaced by enumerating the
//! malformed shapes per field.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::*;
use crate::validation::structural::{Extensions, Generic};

fn obj(v: Value) -> Map<String, Value> {
    v.as_object().unwrap().clone()
}

fn get(v: Value, drop: bool, unknown: bool) -> Result<ObjectMetaResult, String> {
    get_object_meta_with_options(
        &obj(v),
        &ObjectMetaOptions {
            drop_malformed_fields: drop,
            return_unknown_field_paths: unknown,
            parent_path: None,
        },
    )
}

fn unknown_and_malformed() -> Value {
    json!({"kind": "Pod", "apiVersion": "v1", "metadata": {
        "name": "my-meta", "unknownField": "foo", "generateName": null,
        "generation": null, "labels": {"foo": "bar"}, "annotations": 11}})
}

fn unknown_only() -> Value {
    json!({"kind": "Pod", "apiVersion": "v1", "metadata": {
        "name": "my-meta", "unknownField": "foo", "generateName": null,
        "generation": null, "labels": {"foo": "bar"}}})
}

fn malformed_only() -> Value {
    json!({"kind": "Pod", "apiVersion": "v1", "metadata": {
        "name": "my-meta", "generateName": null, "generation": null,
        "labels": {"foo": "bar"}, "annotations": 11}})
}

const ANNOTATIONS_ERR: &str = "json: cannot unmarshal number into Go struct field ObjectMeta.annotations of type map[string]string";

fn expected_meta() -> Option<Map<String, Value>> {
    Some(obj(json!({"name": "my-meta", "labels": {"foo": "bar"}})))
}

/// coerce_test.go `TestGetObjectMetaWithOptions`, all 12 cases.
#[test]
fn get_object_meta_with_options_cases() {
    struct Case {
        obj: Value,
        drop: bool,
        unknown: bool,
        err: Option<&'static str>,
        paths: &'static [&'static str],
    }
    let c = |obj: Value, drop, unknown, err, paths| Case {
        obj,
        drop,
        unknown,
        err,
        paths,
    };
    let cases = vec![
        c(
            unknown_and_malformed(),
            false,
            false,
            Some(ANNOTATIONS_ERR),
            &[],
        ),
        c(unknown_and_malformed(), true, false, None, &[]),
        c(
            unknown_and_malformed(),
            false,
            true,
            Some(ANNOTATIONS_ERR),
            &[],
        ),
        c(
            unknown_and_malformed(),
            true,
            true,
            None,
            &["metadata.unknownField"],
        ),
        c(unknown_only(), false, false, None, &[]),
        c(unknown_only(), true, false, None, &[]),
        c(
            unknown_only(),
            false,
            true,
            None,
            &["metadata.unknownField"],
        ),
        c(unknown_only(), true, true, None, &["metadata.unknownField"]),
        c(malformed_only(), false, false, Some(ANNOTATIONS_ERR), &[]),
        c(malformed_only(), true, false, None, &[]),
        c(malformed_only(), false, true, Some(ANNOTATIONS_ERR), &[]),
        c(malformed_only(), true, true, None, &[]),
    ];
    for (i, tc) in cases.iter().enumerate() {
        let got = get(tc.obj.clone(), tc.drop, tc.unknown);
        match tc.err {
            Some(e) => assert_eq!(got.unwrap_err(), e, "case {i}"),
            None => {
                let got = got.unwrap_or_else(|e| panic!("case {i}: {e}"));
                assert_eq!(got.meta, expected_meta(), "case {i}");
                assert!(got.found, "case {i}");
                assert_eq!(got.unknown_field_paths, tc.paths, "case {i}");
            }
        }
    }
}

/// coerce_test.go `TestGetObjectMetaNils`: null reads as the zero value.
#[test]
fn get_object_meta_nils() {
    let got = get(
        json!({"metadata": {"generateName": null, "generation": null, "labels": {"foo": null}}}),
        true,
        false,
    )
    .unwrap();
    assert_eq!(got.meta, Some(obj(json!({"labels": {"foo": ""}}))));
}

/// coerce_test.go `TestGetObjectMeta`: field names match case-sensitively and
/// a malformed time is dropped.
#[test]
fn get_object_meta_case_sensitive_and_drops_bad_time() {
    let got = get(
        json!({"metadata": {
            "name": "good", "Name": "bad1", "nAme": "bad2",
            "namespace": "good", "Namespace": "bad1",
            "creationTimestamp": "a"}}),
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        got.meta,
        Some(obj(json!({"name": "good", "namespace": "good"})))
    );
}

/// The case-sensitive near-misses are unknown paths under strict decoding.
#[test]
fn case_variants_are_unknown_fields() {
    let got = get(json!({"metadata": {"name": "n", "Name": "x"}}), false, true).unwrap();
    assert_eq!(got.unknown_field_paths, vec!["metadata.Name"]);
}

/// kjson strict paths descend into slices of structs
/// (`appendStrictFieldStackIndex`, kubernetes_patch.go:118-123).
#[test]
fn unknown_paths_nest_through_slices() {
    let got = get(
        json!({"metadata": {
            "name": "n",
            "ownerReferences": [{"apiVersion": "v1", "kind": "K", "name": "o", "uid": "u"},
                                {"name": "o2", "bogus": 1}],
            "managedFields": [{"manager": "m", "extra": true}]}}),
        false,
        true,
    )
    .unwrap();
    assert_eq!(
        got.unknown_field_paths,
        vec![
            "metadata.managedFields[0].extra",
            "metadata.ownerReferences[1].bogus"
        ]
    );
}

/// With a parent path the prefix is `parent.metadata` (coerce.go:77,
/// `opts.ParentPath.Child("metadata").String()`).
#[test]
fn parent_path_prefixes_unknown_paths() {
    let got = get_object_meta_with_options(
        &obj(json!({"metadata": {"zzz": 1}})),
        &ObjectMetaOptions {
            drop_malformed_fields: false,
            return_unknown_field_paths: true,
            parent_path: Some(Path::new("spec").child("embedded")),
        },
    )
    .unwrap();
    assert_eq!(got.unknown_field_paths, vec!["spec.embedded.metadata.zzz"]);
}

/// Go's `UnmarshalTypeError` wording for the malformed shapes of
/// `TestMalformedObjectMetaFields`: nested struct fields name the innermost
/// struct and the full field stack.
#[test]
fn malformed_field_errors_use_go_wording() {
    let cases: Vec<(Value, &str)> = vec![
        (
            json!({"name": 1}),
            "json: cannot unmarshal number into Go struct field ObjectMeta.name of type string",
        ),
        (
            json!({"uid": true}),
            "json: cannot unmarshal bool into Go struct field ObjectMeta.uid of type types.UID",
        ),
        (
            json!({"generation": 1.5}),
            "json: cannot unmarshal number 1.5 into Go struct field ObjectMeta.generation of type int64",
        ),
        (
            json!({"generation": "a"}),
            "json: cannot unmarshal string into Go struct field ObjectMeta.generation of type int64",
        ),
        (
            json!({"deletionGracePeriodSeconds": []}),
            "json: cannot unmarshal array into Go struct field ObjectMeta.deletionGracePeriodSeconds of type int64",
        ),
        (
            json!({"labels": {"a": 1}}),
            "json: cannot unmarshal number into Go struct field ObjectMeta.labels of type string",
        ),
        (
            json!({"labels": ["a"]}),
            "json: cannot unmarshal array into Go struct field ObjectMeta.labels of type map[string]string",
        ),
        (
            json!({"finalizers": {"a": "b"}}),
            "json: cannot unmarshal object into Go struct field ObjectMeta.finalizers of type []string",
        ),
        (
            json!({"finalizers": ["a", 1]}),
            "json: cannot unmarshal number into Go struct field ObjectMeta.finalizers of type string",
        ),
        (
            json!({"ownerReferences": ["x"]}),
            "json: cannot unmarshal string into Go struct field ObjectMeta.ownerReferences of type v1.OwnerReference",
        ),
        (
            json!({"ownerReferences": [{"controller": "yes"}]}),
            "json: cannot unmarshal string into Go struct field OwnerReference.ownerReferences.controller of type bool",
        ),
        (
            json!({"managedFields": [{"operation": 3}]}),
            "json: cannot unmarshal number into Go struct field ManagedFieldsEntry.managedFields.operation of type v1.ManagedFieldsOperationType",
        ),
        (
            json!({"creationTimestamp": 4}),
            "json: cannot unmarshal number into Go struct field ObjectMeta.creationTimestamp of type string",
        ),
        (
            json!({"creationTimestamp": "a"}),
            "parsing time \"a\" as \"2006-01-02T15:04:05Z07:00\": cannot parse \"a\" as \"2006\"",
        ),
        (
            json!({"creationTimestamp": "2026-13-01T00:00:00Z"}),
            "parsing time \"2026-13-01T00:00:00Z\": month out of range",
        ),
        (
            json!({"deletionTimestamp": "2026-01-01T00:00:00Zjunk"}),
            "parsing time \"2026-01-01T00:00:00Zjunk\": extra text: \"junk\"",
        ),
    ];
    for (meta, want) in cases {
        let err = get(json!({"metadata": meta.clone()}), false, false).unwrap_err();
        assert_eq!(err, want, "{meta}");
        // dropMalformedFields drops the field and keeps the rest.
        let mut with_name = meta.clone();
        with_name["name"] = json!("kept");
        let got = get(json!({"metadata": with_name}), true, false).unwrap();
        assert_eq!(
            got.meta.unwrap().get("name"),
            Some(&json!("kept")),
            "{meta}"
        );
    }
}

#[test]
fn metadata_not_an_object() {
    assert_eq!(
        get(json!({"metadata": "x"}), false, false).unwrap_err(),
        "json: cannot unmarshal string into Go value of type v1.ObjectMeta"
    );
    assert_eq!(
        get(json!({"metadata": [42]}), true, false).unwrap_err(),
        "invalid metadata: expected object, got []interface {}"
    );
    // null decodes to an empty ObjectMeta.
    let got = get(json!({"metadata": null}), false, false).unwrap();
    assert!(got.found);
    assert_eq!(got.meta, Some(Map::new()));
    // no metadata: not found.
    let got = get(json!({"a": 1}), false, false).unwrap();
    assert!(!got.found && got.meta.is_none());
}

/// `ToUnstructured(ObjectMeta)`: times are normalized to UTC seconds,
/// `creationTimestamp` is `omitzero`, owner references keep their required
/// fields, non-nil pointers are kept even when zero.
#[test]
fn set_form_normalizes_like_to_unstructured() {
    let got = get(
        json!({"metadata": {
            "name": "n", "generation": 3, "selfLink": "/x",
            "creationTimestamp": "2026-01-02T03:04:05.678+01:00",
            "deletionTimestamp": "2026-01-02T03:04:05Z",
            "deletionGracePeriodSeconds": 0,
            "labels": {}, "finalizers": [],
            "ownerReferences": [{"name": "o", "controller": false}, null],
            "managedFields": [{"manager": "m", "time": "2026-01-02T03:04:05Z",
                               "fieldsV1": {"f:a": {}}}]}}),
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        Value::Object(got.meta.unwrap()),
        json!({
            "name": "n", "generation": 3, "selfLink": "/x",
            "creationTimestamp": "2026-01-02T02:04:05Z",
            "deletionTimestamp": "2026-01-02T03:04:05Z",
            "deletionGracePeriodSeconds": 0,
            "ownerReferences": [
                {"apiVersion": "", "kind": "", "name": "o", "uid": "", "controller": false},
                {"apiVersion": "", "kind": "", "name": "", "uid": ""}],
            "managedFields": [{"manager": "m", "time": "2026-01-02T03:04:05Z",
                               "fieldsV1": {"f:a": {}}}]
        })
    );
    // a zero creationTimestamp is omitted.
    let got = get(
        json!({"metadata": {"creationTimestamp": "0001-01-01T00:00:00Z"}}),
        false,
        false,
    )
    .unwrap();
    assert_eq!(got.meta, Some(Map::new()));
}

fn embedded() -> Structural {
    Structural {
        generic: Generic {
            type_: "object".into(),
            ..Default::default()
        },
        extensions: Extensions {
            x_embedded_resource: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn object(props: Vec<(&str, Structural)>) -> Structural {
    Structural {
        generic: Generic {
            type_: "object".into(),
            ..Default::default()
        },
        properties: props
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<BTreeMap<_, _>>(),
        ..Default::default()
    }
}

fn coerce(
    mut v: Value,
    s: Option<&Structural>,
    root: bool,
    drop: bool,
) -> (Value, Option<Error>, Vec<String>) {
    let (err, paths) = coerce_with_options(
        None,
        &mut v,
        s,
        root,
        CoerceOptions {
            drop_invalid_fields: drop,
            return_unknown_field_paths: true,
        },
    );
    (v, err, paths)
}

/// algorithm_test.go `TestCoerce` "empty" and "scalar".
#[test]
fn coerce_trivial() {
    assert_eq!(coerce(Value::Null, None, false, false).0, Value::Null);
    let s = Structural::default();
    assert_eq!(coerce(json!(4), Some(&s), false, false).0, json!(4));
    let arr = Structural {
        items: Some(Box::new(Structural::default())),
        ..Default::default()
    };
    assert_eq!(
        coerce(json!([1, 2]), Some(&arr), false, false).0,
        json!([1, 2])
    );
}

fn foo_instance() -> Value {
    json!({"apiVersion": "foo/v1", "kind": "Foo",
           "unspecified": "bar",
           "metadata": {"name": "instance", "unspecified": "bar"},
           "spec": {"unspecified": "bar"}})
}

/// algorithm_test.go `TestCoerce` "x-kubernetes-embedded-resource" and the
/// `includeRoot=true` variant.
#[test]
fn coerce_embedded_resource() {
    let mut preserving = embedded();
    preserving.extensions.x_preserve_unknown_fields = true;
    let mut nested_inner = embedded();
    nested_inner
        .properties
        .insert("spec".into(), object(vec![]));
    let mut pruned = embedded();
    pruned.properties.insert("spec".into(), object(vec![]));
    let mut nested = embedded();
    nested
        .properties
        .insert("spec".into(), object(vec![("embedded", nested_inner)]));
    let schema = object(vec![
        ("pruned", pruned),
        ("preserving", preserving),
        ("nested", nested),
    ]);

    let mut nested_val = foo_instance();
    nested_val["spec"]["embedded"] = foo_instance();
    let input = json!({
        "apiVersion": "foo/v1", "kind": "Foo", "unspecified": "bar",
        "metadata": {"name": "instance", "unspecified": "bar"},
        "pruned": foo_instance(), "preserving": foo_instance(),
        "nested": nested_val});
    let strip = |mut v: Value| {
        v["metadata"] = json!({"name": "instance"});
        v
    };
    let mut nested_out = strip(foo_instance());
    nested_out["spec"]["embedded"] = strip(foo_instance());
    let expected = json!({
        "apiVersion": "foo/v1", "kind": "Foo", "unspecified": "bar",
        "metadata": {"name": "instance", "unspecified": "bar"},
        "pruned": strip(foo_instance()), "preserving": strip(foo_instance()),
        "nested": nested_out});

    let (got, err, paths) = coerce(input.clone(), Some(&schema), false, false);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(got, expected);
    assert_eq!(
        paths,
        vec![
            "nested.metadata.unspecified",
            "nested.spec.embedded.metadata.unspecified",
            "preserving.metadata.unspecified",
            "pruned.metadata.unspecified",
        ]
    );

    let (got, err, paths) = coerce(input, Some(&schema), true, false);
    assert!(err.is_none(), "{err:?}");
    let mut expected_root = expected;
    expected_root["metadata"] = json!({"name": "instance"});
    assert_eq!(got, expected_root);
    assert_eq!(
        paths,
        vec![
            "metadata.unspecified",
            "nested.metadata.unspecified",
            "nested.spec.embedded.metadata.unspecified",
            "preserving.metadata.unspecified",
            "pruned.metadata.unspecified",
        ]
    );
}

/// algorithm_test.go `TestCoerce` "without name".
#[test]
fn coerce_without_name() {
    let schema = object(vec![("pruned", embedded())]);
    let input = json!({"apiVersion": "foo/v1", "kind": "Foo",
        "metadata": {"name": "instance"},
        "pruned": {"apiVersion": "foo/v1", "kind": "Foo",
                   "metadata": {"namespace": "kube-system"}}});
    let (got, err, paths) = coerce(input.clone(), Some(&schema), false, false);
    assert!(err.is_none());
    assert_eq!(got, input);
    assert!(paths.is_empty());
}

/// algorithm_test.go `TestCoerce` "x-kubernetes-embedded-resource, with
/// dropInvalidFields=true" and "invalid metadata type".
#[test]
fn coerce_drop_invalid_fields() {
    let mut pruned = embedded();
    pruned.properties.insert("spec".into(), object(vec![]));
    let schema = object(vec![("pruned", pruned)]);
    let input = json!({"apiVersion": "foo/v1", "kind": "Foo",
        "metadata": {"name": "instance"},
        "pruned": {"apiVersion": 42, "kind": 42,
                   "metadata": {"name": "instance", "namespace": ["abc"],
                                "labels": {"foo": 42}}}});
    let (got, err, _) = coerce(input, Some(&schema), false, true);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(
        got,
        json!({"apiVersion": "foo/v1", "kind": "Foo",
               "metadata": {"name": "instance"},
               "pruned": {"metadata": {"name": "instance"}}})
    );

    let schema = object(vec![("pruned", embedded())]);
    let input = json!({"apiVersion": "foo/v1", "kind": "Foo",
        "metadata": {"name": "instance"},
        "pruned": {"apiVersion": 42, "kind": 42, "metadata": [42]}});
    let (got, err, _) = coerce(input, Some(&schema), false, true);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(
        got,
        json!({"apiVersion": "foo/v1", "kind": "Foo",
               "metadata": {"name": "instance"},
               "pruned": {"metadata": [42]}})
    );
}

/// Without dropInvalidFields a malformed embedded object is a field error
/// (algorithm.go:91-96, 98-108).
#[test]
fn coerce_invalid_fields_error() {
    let schema = object(vec![("pruned", embedded())]);
    let (_, err, _) = coerce(
        json!({"pruned": {"apiVersion": 42, "kind": "K"}}),
        Some(&schema),
        false,
        false,
    );
    assert_eq!(
        err.unwrap().to_string(),
        "pruned.apiVersion: Invalid value: 42: must be a string"
    );
    let (_, err, _) = coerce(
        json!({"pruned": {"metadata": {"labels": 3}}}),
        Some(&schema),
        false,
        false,
    );
    assert_eq!(
        err.unwrap().to_string(),
        "pruned.metadata: Invalid value: {\"labels\":3}: json: cannot unmarshal number into Go struct field ObjectMeta.labels of type map[string]string"
    );
}
