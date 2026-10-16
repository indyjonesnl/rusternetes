//! Schema-driven strategic merge patch (#3731): the merge key and strategy of
//! a list come from the field's struct tags (`PatchMetaFromStruct`,
//! strategicpatch/meta.go), not from what the patch items happen to carry.

use super::{apply_patch, apply_patch_with_schema, PatchError, PatchSchema, PatchType};
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

/// strategicpatch/patch_test.go:100-123 `MergeItem`, as a schema.
fn merge_item_schema() -> PatchSchema {
    PatchSchema::for_test(
        &json!({"gvk": {}, "defs": {"MergeItem": {
            "name": ["P:string", "", ""],
            "value": ["P:string", "", ""],
            "mergingList": ["L:S:MergeItem", "merge", "name"],
            "nonMergingList": ["L:S:MergeItem", "", ""],
            "mergingIntList": ["L:P:int", "merge", ""],
            "nonMergingIntList": ["L:P:int", "", ""],
            "simpleMap": ["M:P:string", "", ""],
            "replacingItem": ["S:RawExtension", "replace", ""],
        }, "RawExtension": {}}})
        .to_string(),
        "MergeItem",
    )
}

fn merge_item(original: Value, patch: Value) -> Result<Value, PatchError> {
    apply_patch_with_schema(
        &original,
        &patch,
        PatchType::StrategicMergePatch,
        Some(&merge_item_schema()),
    )
}

/// patch_test.go:666-680 "missing merge key should error out": the key comes
/// from the `mergingList` tag, so a patch item without it fails with
/// `ErrNoMergeKey` (`mergeSliceWithoutSpecialElements`, patch.go:1627-1634)
/// instead of replacing the list.
#[test]
fn missing_merge_key_should_error_out() {
    let err = uninterpreted(merge_item(
        json!({"mergingList": [{"name": "1", "value": "a"}]}),
        json!({"mergingList": [{"value": "b"}]}),
    ));
    assert_eq!(
        err,
        "map: map[value:b] does not contain declared merge key: name"
    );
}

/// `mergeSlice` (patch.go:1537-1540): a list of maps merged without a merge
/// key. Reachable once the strategy says `merge` and the tag has no key.
#[test]
fn merging_list_of_maps_without_merge_key_errors() {
    let schema = PatchSchema::for_test(
        &json!({"gvk": {}, "defs": {"Item": {
            "list": ["L:S:Item", "merge", ""],
            "name": ["P:string", "", ""],
        }}})
        .to_string(),
        "Item",
    );
    let err = uninterpreted(apply_patch_with_schema(
        &json!({"list": [{"name": "a"}]}),
        &json!({"list": [{"name": "b"}]}),
        PatchType::StrategicMergePatch,
        Some(&schema),
    ));
    assert_eq!(err, "cannot merge lists without merge key for struct");
}

/// A list without `patchStrategy:"merge"` is replaced by the patch list
/// (`mergeSliceHandler`, patch.go:1481-1487), whatever its items carry.
#[test]
fn list_without_merge_strategy_is_replaced() {
    let r = merge_item(
        json!({"nonMergingList": [{"name": "a", "value": "x"}, {"name": "b"}]}),
        json!({"nonMergingList": [{"name": "a"}]}),
    )
    .unwrap();
    assert_eq!(r, json!({"nonMergingList": [{"name": "a"}]}));
}

/// `patchStrategy:"merge"` on a list of scalars unions it (`mergeSlice`,
/// patch.go:1513-1523), where the patch items sort by patch order.
#[test]
fn scalar_list_with_merge_strategy_is_unioned() {
    let r = merge_item(
        json!({"mergingIntList": [1, 2]}),
        json!({"mergingIntList": [2, 3]}),
    )
    .unwrap();
    assert_eq!(r, json!({"mergingIntList": [1, 2, 3]}));
    let r = merge_item(
        json!({"nonMergingIntList": [1, 2]}),
        json!({"nonMergingIntList": [3]}),
    )
    .unwrap();
    assert_eq!(r, json!({"nonMergingIntList": [3]}));
}

/// `patchStrategy:"replace"` on a map replaces it (`mergeMapHandler`,
/// patch.go:1451-1472).
#[test]
fn map_with_replace_strategy_is_replaced() {
    let r = merge_item(
        json!({"replacingItem": {"a": 1, "b": 2}}),
        json!({"replacingItem": {"b": 3}}),
    )
    .unwrap();
    assert_eq!(r, json!({"replacingItem": {"b": 3}}));
}

/// Upstream finds a field with `strings.EqualFold` when the exact name is
/// absent (fields.go:44-53) and fails on an unknown one (fields.go:68).
#[test]
fn unknown_field_is_a_lookup_error() {
    let err = uninterpreted(merge_item(
        json!({"simpleMap": {"a": {"x": 1}}}),
        json!({"simpleMap": {"a": {"x": 2}}}),
    ));
    assert_eq!(
        err,
        "merging an object in json but data type is not struct, instead is: map"
    );
    let err = uninterpreted(merge_item(
        json!({"mergingList": [{"name": "a", "bogus": {"x": 1}}]}),
        json!({"mergingList": [{"name": "a", "bogus": {"x": 2}}]}),
    ));
    assert_eq!(
        err,
        "unable to find api field in struct MergeItem for the json field \"bogus\""
    );
}

/// The kind's own `apiVersion`/`kind` selects the schema: `containers` is
/// keyed by `name` (PodSpec tag), so a container without one is
/// `ErrNoMergeKey`, not a wholesale replacement.
#[test]
fn pod_container_without_name_errors() {
    let err = uninterpreted(smp(
        json!({"apiVersion": "v1", "kind": "Pod",
               "spec": {"containers": [{"name": "a", "image": "x"}]}}),
        json!({"spec": {"containers": [{"image": "y"}]}}),
    ));
    assert_eq!(
        err,
        "map: map[image:y] does not contain declared merge key: name"
    );
}

/// `volumeMounts` is keyed by `mountPath` (Container tag), not by `name`.
#[test]
fn volume_mounts_merge_by_mount_path() {
    let r = smp(
        json!({"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [{
            "name": "c",
            "volumeMounts": [
                {"name": "v", "mountPath": "/x"},
                {"name": "v", "mountPath": "/y"}]}]}}),
        json!({"spec": {"containers": [{
            "name": "c",
            "volumeMounts": [{"mountPath": "/y", "readOnly": true}]}]}}),
    )
    .unwrap();
    assert_eq!(
        r["spec"]["containers"][0]["volumeMounts"],
        json!([
            {"name": "v", "mountPath": "/x"},
            {"name": "v", "mountPath": "/y", "readOnly": true}
        ])
    );
}

/// `ServicePort` is keyed by `port` (ServiceSpec tag); a patch item that
/// names only the port merges into the live entry.
#[test]
fn service_ports_merge_by_port() {
    let r = smp(
        json!({"apiVersion": "v1", "kind": "Service", "spec": {"ports": [
            {"name": "http", "port": 80, "protocol": "TCP", "targetPort": 80}]}}),
        json!({"spec": {"ports": [{"port": 80, "targetPort": 8080}]}}),
    )
    .unwrap();
    assert_eq!(
        r["spec"]["ports"],
        json!([{"name": "http", "port": 80, "protocol": "TCP", "targetPort": 8080}])
    );
}

/// A list keyed by `$setElementOrder` takes its key from the field too.
#[test]
fn set_element_order_uses_field_merge_key() {
    let r = smp(
        json!({"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [
            {"name": "a"}, {"name": "b"}, {"name": "c"}]}}),
        json!({"spec": {
            "$setElementOrder/containers": [{"name": "c"}, {"name": "a"}],
            "containers": [{"name": "a", "image": "x"}]}}),
    )
    .unwrap();
    assert_eq!(
        r["spec"]["containers"],
        json!([{"name": "b"}, {"name": "c"}, {"name": "a", "image": "x"}])
    );
}
