//! Strategic-merge-patch error text parity (#3713): the plain errors the
//! apiserver returns as a reason-less 500 (`interpretStrategicMergePatchError`
//! default arm, apiserver `endpoints/handlers/patch.go:805-806`) carry
//! upstream's exact `mergepatch` / `strategicpatch` wording.

use super::{apply_patch, PatchError, PatchType};
use serde_json::{json, Value};

fn smp(original: Value, patch: Value) -> Result<Value, PatchError> {
    apply_patch(&original, &patch, PatchType::StrategicMergePatch)
}

fn uninterpreted(r: Result<Value, PatchError>) -> String {
    match r {
        Err(PatchError::Uninterpreted(m)) => m,
        other => panic!("expected PatchError::Uninterpreted, got {other:?}"),
    }
}

/// `handleDirectiveInMergeMap` (strategicpatch/patch.go:906-923): any
/// `$patch` on a map other than `replace` / `delete` is `ErrBadPatchType` —
/// including `merge`.
#[test]
fn unknown_map_directive_is_err_bad_patch_type() {
    let orig = json!({"metadata": {"labels": {"a": "b"}}});
    assert_eq!(
        uninterpreted(smp(
            orig.clone(),
            json!({"metadata": {"labels": {"$patch": "foo", "x": "y"}}})
        )),
        "unknown patch type: foo in map: map[$patch:foo x:y]"
    );
    assert_eq!(
        uninterpreted(smp(
            orig.clone(),
            json!({"metadata": {"labels": {"$patch": "merge"}}})
        )),
        "unknown patch type: merge in map: map[$patch:merge]"
    );
    assert_eq!(
        uninterpreted(smp(orig, json!({"metadata": {"$patch": 5}}))),
        "unknown patch type: %!s(int64=5) in map: map[$patch:5]"
    );
}

/// `mergeSliceWithSpecialElements` (patch.go:1556-1585): an unknown list
/// element directive is `ErrBadPatchType` over the element; `merge` has its
/// own message.
#[test]
fn unknown_list_directive_is_err_bad_patch_type() {
    let orig = json!({"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [{"name": "a", "image": "x"}]}});
    assert_eq!(
        uninterpreted(smp(
            orig.clone(),
            json!({"spec": {"containers": [{"name": "b", "$patch": "foo"}]}})
        )),
        "unknown patch type: foo in map: map[$patch:foo name:b]"
    );
    assert_eq!(
        uninterpreted(smp(
            orig,
            json!({"spec": {"containers": [{"name": "a", "$patch": "merge"}]}})
        )),
        "merging lists cannot yet be specified in the patch"
    );
}

/// `mergePatchIntoOriginal` (patch.go:1141-1164): the order list, the live
/// field and the patch field must each be a `[]interface{}`, else
/// `ErrBadArgType`.
#[test]
fn set_element_order_non_list_is_err_bad_arg_type() {
    let orig = json!({"metadata": {"finalizers": ["a"]}, "spec": {"foo": {"a": 1}}});
    assert_eq!(
        uninterpreted(smp(
            orig.clone(),
            json!({"metadata": {"$setElementOrder/finalizers": "a"}})
        )),
        "expected a []interface {}, but received a string"
    );
    assert_eq!(
        uninterpreted(smp(
            orig.clone(),
            json!({"spec": {"$setElementOrder/foo": ["a"]}})
        )),
        "expected a []interface {}, but received a map[string]interface {}"
    );
    assert_eq!(
        uninterpreted(smp(
            orig,
            json!({"metadata": {"$setElementOrder/finalizers": ["a"], "finalizers": null}})
        )),
        "expected a []interface {}, but received a %!s(<nil>)"
    );
}

/// `validatePatchWithSetOrderList` (patch.go:1017-1020) renders both lists
/// with `%v`; mirrors patch_test.go "$setElementOrder contains item that is
/// not present in the int list to be merged".
#[test]
fn set_element_order_mismatch_renders_lists_with_percent_v() {
    assert_eq!(
        uninterpreted(super::smp_schema_tests::merge_item(
            json!({"mergingIntList": [1, 3, 2]}),
            json!({
                "$setElementOrder/mergingIntList": [1, 2, 3],
                "mergingIntList": [3, 1]
            })
        )),
        "The order in patch list:\n[3 1]\n doesn't match $setElementOrder list:\n[1 2 3]\n"
    );
    // patch_test.go "should check if order in $setElementOrder and patch
    // list match".
    assert_eq!(
        uninterpreted(super::smp_schema_tests::merge_item(
            json!({"mergingList": [{"name": "1"}, {"name": "3"}, {"name": "2"}]}),
            json!({
                "$setElementOrder/mergingList": [{"name": "1"}, {"name": "2"}, {"name": "3"}],
                "mergingList": [{"name": "3", "value": 3}, {"name": "1", "value": 1}]
            })
        )),
        "The order in patch list:\n[map[name:3 value:3] map[name:1 value:1]]\n doesn't match \
         $setElementOrder list:\n[map[name:1] map[name:2] map[name:3]]\n"
    );
}

/// `mergeKeyValueEqual` (patch.go:947-954) and
/// `partitionMapsByPresentInList` (patch.go:1262-1265): `ErrNoMergeKey`
/// naming the offending map and the key.
#[test]
fn missing_merge_key_is_err_no_merge_key() {
    assert_eq!(
        uninterpreted(smp(
            json!({"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [{"name": "a"}]}}),
            json!({"spec": {
                "$setElementOrder/containers": [{"name": "a"}],
                "containers": [{"image": "x"}]
            }})
        )),
        "map: map[image:x] does not contain declared merge key: name"
    );
    assert_eq!(
        uninterpreted(smp(
            json!({"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [{"image": "x"}]}}),
            json!({"spec": {
                "$setElementOrder/containers": [{"name": "a"}],
                "containers": [{"name": "a"}]
            }})
        )),
        "map: map[image:x] does not contain declared merge key: name"
    );
}

/// `mergeMap` (patch.go:1322-1426) + `preprocessDeletionListForMerging`
/// (patch.go:1026-1040)
/// with `MergeParallelList`: a `$deleteFromPrimitiveList/<f>` value of a
/// different type than the live field is skipped (no error); `null` deletes
/// the field.
#[test]
fn delete_from_primitive_list_non_list_follows_merge_map() {
    let orig = json!({"metadata": {"finalizers": ["a", "b"]}});
    assert_eq!(
        smp(
            orig.clone(),
            json!({"metadata": {"$deleteFromPrimitiveList/finalizers": "a"}})
        )
        .unwrap(),
        orig
    );
    assert_eq!(
        smp(
            orig,
            json!({"metadata": {"$deleteFromPrimitiveList/finalizers": null}})
        )
        .unwrap(),
        json!({"metadata": {}})
    );
}
