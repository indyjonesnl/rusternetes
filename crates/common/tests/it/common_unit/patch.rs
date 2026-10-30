//! Unit tests moved from `src/patch.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::patch::mergepatch::{
        err_bad_arg_type, err_bad_patch_type, err_no_merge_key, go_fmt_v, GO_SLICE,
    };
    pub use rusternetes_common::patch::meta::PatchSchema;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json::{json, Value};
}

mod tests {
    use super::__parent::*;
    use crate::common_unit::patch::smp_schema_tests;
    use rusternetes_common::patch::*;

    fn apply_strategic_merge_patch(original: &Value, patch: &Value) -> Result<Value, PatchError> {
        apply_patch(original, patch, PatchType::StrategicMergePatch)
    }

    fn merge_item(original: &Value, patch: &Value) -> Result<Value, PatchError> {
        crate::common_unit::patch::smp_schema_tests::merge_item(original.clone(), patch.clone())
    }

    #[test]
    fn test_json_merge_patch_simple() {
        let original = json!({
            "name": "test",
            "value": 1
        });

        let patch = json!({
            "value": 2,
            "new_field": "hello"
        });

        let result = apply_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["value"], 2);
        assert_eq!(result["new_field"], "hello");
    }

    #[test]
    fn test_json_merge_patch_delete() {
        let original = json!({
            "name": "test",
            "value": 1
        });

        let patch = json!({
            "value": null
        });

        let result = apply_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["name"], "test");
        assert!(result.get("value").is_none());
    }

    #[test]
    fn test_json_patch_add() {
        let original = json!({
            "name": "test"
        });

        let patch = json!([
            {"op": "add", "path": "/value", "value": 42}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert_eq!(result["value"], 42);
    }

    #[test]
    fn test_json_patch_remove() {
        let original = json!({
            "name": "test",
            "value": 42
        });

        let patch = json!([
            {"op": "remove", "path": "/value"}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert!(result.get("value").is_none());
    }

    #[test]
    fn test_json_patch_replace() {
        let original = json!({
            "name": "test",
            "value": 42
        });

        let patch = json!([
            {"op": "replace", "path": "/value", "value": 100}
        ]);

        let result = apply_json_patch(&original, &patch).unwrap();
        assert_eq!(result["value"], 100);
    }

    #[test]
    fn test_strategic_merge_patch_simple() {
        let original = json!({
            "metadata": {
                "name": "test",
                "labels": {
                    "app": "nginx"
                }
            },
            "spec": {
                "replicas": 1
            }
        });

        let patch = json!({
            "metadata": {
                "labels": {
                    "version": "1.0"
                }
            },
            "spec": {
                "replicas": 3
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["metadata"]["name"], "test");
        assert_eq!(result["metadata"]["labels"]["app"], "nginx");
        assert_eq!(result["metadata"]["labels"]["version"], "1.0");
        assert_eq!(result["spec"]["replicas"], 3);
    }

    /// `containers` is `patchStrategy:"merge" patchMergeKey:"name"` (core/v1
    /// `PodSpec.Containers`): the served kind's schema, not the patch items,
    /// names the key. K8s SMP order: patch items first, then server-only.
    #[test]
    fn test_strategic_merge_containers_by_name_via_schema() {
        let original = json!({"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [
            {"name": "container1", "image": "nginx:1.0"},
            {"name": "container2", "image": "redis:5"}
        ]}});
        let patch = json!({"spec": {"containers": [
            {"name": "container1", "image": "nginx:1.1"},
            {"name": "container3", "image": "postgres:12"}
        ]}});
        let result = apply_patch(&original, &patch, PatchType::StrategicMergePatch).unwrap();
        let c = result["spec"]["containers"].as_array().unwrap();
        assert_eq!(c.len(), 3);
        assert_eq!(c[0]["image"], "nginx:1.1");
        assert_eq!(c[1]["name"], "container3");
        assert_eq!(c[2]["image"], "redis:5");
    }

    /// Without a kind there is no struct to look up, so a list takes the
    /// default replace strategy (no merge key is guessed from the items).
    #[test]
    fn test_strategic_merge_unknown_kind_replaces_lists() {
        let original = json!({"spec": {"containers": [
            {"name": "a", "image": "x"}, {"name": "b", "image": "y"}]}});
        let patch = json!({"spec": {"containers": [{"name": "a", "image": "z"}]}});
        let result = apply_patch(&original, &patch, PatchType::StrategicMergePatch).unwrap();
        assert_eq!(
            result["spec"]["containers"],
            json!([{"name": "a", "image": "z"}])
        );
    }

    #[test]
    fn test_patch_type_from_content_type() {
        assert_eq!(
            PatchType::from_content_type("application/strategic-merge-patch+json").unwrap(),
            PatchType::StrategicMergePatch
        );
        assert_eq!(
            PatchType::from_content_type("application/merge-patch+json").unwrap(),
            PatchType::JsonMergePatch
        );
        assert_eq!(
            PatchType::from_content_type("application/json-patch+json").unwrap(),
            PatchType::JsonPatch
        );
    }

    #[test]
    fn test_strategic_merge_patch_directive() {
        let original = json!({
            "metadata": {
                "name": "test",
                "labels": {
                    "app": "nginx",
                    "version": "1.0"
                }
            },
            "spec": {
                "replicas": 1
            }
        });

        let patch = json!({
            "metadata": {
                "labels": {
                    "$patch": "replace",
                    "app": "apache"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // With $patch: replace, the labels should be replaced entirely
        assert_eq!(result["metadata"]["labels"]["app"], "apache");
        assert!(result["metadata"]["labels"].get("version").is_none());
    }

    #[test]
    fn test_strategic_merge_patch_retain_keys() {
        let original = json!({
            "metadata": {
                "name": "test",
                "uid": "abc-123",
                "labels": {
                    "app": "nginx",
                    "version": "1.0"
                }
            }
        });

        let patch = json!({
            "metadata": {
                "$patch": "replace",
                "$retainKeys": ["name", "uid"],
                "labels": {
                    "app": "apache"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // Should retain name and uid, but replace labels
        assert_eq!(result["metadata"]["name"], "test");
        assert_eq!(result["metadata"]["uid"], "abc-123");
        assert_eq!(result["metadata"]["labels"]["app"], "apache");
    }

    #[test]
    fn test_strategic_merge_delete_from_primitive_list() {
        let original = json!({
            "spec": {
                "finalizers": ["kubernetes.io/pv-protection", "example.com/my-finalizer"]
            }
        });

        let patch = json!({
            "spec": {
                "$deleteFromPrimitiveList/finalizers": ["example.com/my-finalizer"]
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        let finalizers = result["spec"]["finalizers"].as_array().unwrap();
        assert_eq!(finalizers.len(), 1);
        assert_eq!(finalizers[0], "kubernetes.io/pv-protection");
    }

    #[test]
    fn test_smp_delete_from_primitive_list_parallel_key() {
        // Wire form used by kube-controller-manager (servicecidrs controller).
        let original = json!({"metadata": {"finalizers": ["a", "b", "c"]}});
        let patch = json!({"metadata": {"$deleteFromPrimitiveList/finalizers": ["b"]}});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(r, json!({"metadata": {"finalizers": ["a", "c"]}}));
    }

    #[test]
    fn test_smp_delete_from_primitive_list_nonexistent_item_and_list() {
        // patch_test.go:681-731
        let r = apply_strategic_merge_patch(
            &json!({"l": [1, 2]}),
            &json!({"$deleteFromPrimitiveList/l": [3]}),
        )
        .unwrap();
        assert_eq!(r, json!({"l": [1, 2]}));
        let r = apply_strategic_merge_patch(
            &json!({"foo": ["bar"]}),
            &json!({"$deleteFromPrimitiveList/l": [3]}),
        )
        .unwrap();
        assert_eq!(r, json!({"foo": ["bar"]}));
        let r = apply_strategic_merge_patch(
            &json!({"l": null}),
            &json!({"$deleteFromPrimitiveList/l": [3]}),
        )
        .unwrap();
        assert_eq!(r, json!({"l": null}));
    }

    #[test]
    fn test_smp_delete_all_duplicates_with_set_element_order() {
        // patch_test.go:1360-1395 "delete all duplicate items in lists of scalars"
        let original = json!({"l": [1, 2, 3, 3, 4]});
        let patch = json!({
            "$setElementOrder/l": [1, 2],
            "$deleteFromPrimitiveList/l": [3]
        });
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(r, json!({"l": [1, 2, 4]}));
    }

    #[test]
    fn test_smp_add_and_delete_with_set_element_order() {
        // patch_test.go:1397-1440 "add and delete items in lists of scalars"
        let original = json!({"mergingIntList": [1, 2, 3]});
        let patch = json!({
            "$setElementOrder/mergingIntList": [1, 2, 4],
            "$deleteFromPrimitiveList/mergingIntList": [3],
            "mergingIntList": [4]
        });
        let r = merge_item(&original, &patch).unwrap();
        assert_eq!(r, json!({"mergingIntList": [1, 2, 4]}));
    }

    #[test]
    fn test_smp_set_element_order_reorders_primitives_keeping_server_only() {
        let original = json!({"mergingIntList": [1, 2, 9, 3]});
        let patch = json!({"$setElementOrder/mergingIntList": [3, 1, 2], "mergingIntList": [3]});
        let r = merge_item(&original, &patch).unwrap();
        // patch items follow the order list; server-only 9 is kept.
        let l = r["mergingIntList"].as_array().unwrap();
        let pos = |n: i64| l.iter().position(|v| *v == n).unwrap();
        assert!(pos(3) < pos(1) && pos(1) < pos(2));
        assert_eq!(l.len(), 4);
        assert!(r.get("$setElementOrder/mergingIntList").is_none());
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_merge_key_name() {
        // patch_test.go:1455-1522 "merge lists of maps" (ThreeWay + Result).
        let original = json!({"mergingList": [
            {"name": "1", "other": "a"},
            {"name": "2", "value": "2", "other": "b"}
        ]});
        let patch = json!({
            "$setElementOrder/mergingList": [
                {"name": "4"}, {"name": "1"}, {"name": "2"}, {"name": "3"}
            ],
            "mergingList": [
                {"name": "4", "value": "4"},
                {"name": "3", "value": "3"}
            ]
        });
        let r = merge_item(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"mergingList": [
                {"name": "4", "value": "4"},
                {"name": "1", "other": "a"},
                {"name": "2", "value": "2", "other": "b"},
                {"name": "3", "value": "3"}
            ]})
        );
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_keeps_server_only() {
        // Server-only item "x" is kept; patch items follow the order list.
        let original = json!({"mergingList": [
            {"name": "a"}, {"name": "x"}, {"name": "b"}
        ]});
        let patch = json!({
            "$setElementOrder/mergingList": [{"name": "b"}, {"name": "a"}],
            "mergingList": [{"name": "b", "image": "i"}]
        });
        let r = merge_item(&original, &patch).unwrap();
        let names: Vec<&str> = r["mergingList"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 3);
        let pos = |n: &str| names.iter().position(|x| *x == n).unwrap();
        assert!(pos("b") < pos("a"));
        assert_eq!(r["mergingList"][pos("b")]["image"], "i");
        assert!(r.get("$setElementOrder/mergingList").is_none());
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_delete_item() {
        // patch_test.go:1275-1315: `$patch: delete` item listed in the order.
        let original = json!({"mergingList": [{"name": "hello2"}]});
        let patch = json!({
            "$setElementOrder/mergingList": [{"name": "hello"}, {"name": "doesntexist"}],
            "mergingList": [{"name": "hello"}, {"$patch": "delete", "name": "doesntexist"}]
        });
        let r = merge_item(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"mergingList": [{"name": "hello"}, {"name": "hello2"}]})
        );
    }

    #[test]
    fn test_smp_replace_directive_deletes_all_items_from_merging_list() {
        // patch_test.go:370-377 "delete all items from merging list":
        // mergeSliceWithSpecialElements (patch.go:1548-1587) returns the
        // patch minus its `$patch: replace` element.
        let original = json!({"mergingList": [{"name": "1"}, {"name": "2"}]});
        let patch = json!({"mergingList": [{"$patch": "replace"}]});
        let r = merge_item(&original, &patch).unwrap();
        assert_eq!(r, json!({"mergingList": []}));
    }

    #[test]
    fn test_smp_replace_directive_replaces_merging_list_with_patch_items() {
        // Same function: the items beside `$patch: replace` become the list
        // (what fixupPatchForNodeStatusAddresses emits for status.addresses,
        // component-helpers/node/util/status.go:84-130).
        let original = json!({"apiVersion": "v1", "kind": "Node", "status": {"addresses": [
            {"type": "InternalIP", "address": "9.9.9.9"}]}});
        let patch = json!({"status": {"addresses": [
            {"type": "InternalIP", "address": "10.1.1.1"},
            {"type": "Hostname", "address": "n"},
            {"$patch": "replace"}]}});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"apiVersion": "v1", "kind": "Node", "status": {"addresses": [
                {"type": "InternalIP", "address": "10.1.1.1"},
                {"type": "Hostname", "address": "n"}]}})
        );
    }

    #[test]
    fn test_smp_set_element_order_lists_of_maps_mismatch_errors() {
        // validatePatchWithSetOrderList (patch.go:1018).
        let r = merge_item(
            &json!({"mergingList": [{"name": "1"}]}),
            &json!({
                "$setElementOrder/mergingList": [{"name": "1"}, {"name": "2"}],
                "mergingList": [{"name": "3"}]
            }),
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_smp_retain_keys_clears_and_accepts_listed() {
        // patch_test.go:4412-4440 discriminated union.
        let original = json!({"retainKeysMap": {"name": "type1", "value": "foo"}});
        let patch = json!({"retainKeysMap": {
            "$retainKeys": ["name", "other"], "name": "type2", "other": "bar"
        }});
        let r = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(
            r,
            json!({"retainKeysMap": {"name": "type2", "other": "bar"}})
        );
    }

    #[test]
    fn test_smp_retain_keys_rejects_patch_key_not_listed() {
        // applyRetainKeysDirective (patch.go:1081-1090).
        let err = apply_strategic_merge_patch(
            &json!({"m": {"a": 1}}),
            &json!({"m": {"$retainKeys": ["a"], "b": 2}}),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("invalid patch format of retainKeys"),
            "{err}"
        );
        // null values are exempt (patch.go:1082).
        let r = apply_strategic_merge_patch(
            &json!({"m": {"a": 1, "b": 2}}),
            &json!({"m": {"$retainKeys": ["a"], "b": null}}),
        )
        .unwrap();
        assert_eq!(r, json!({"m": {"a": 1}}));
    }

    #[test]
    fn test_smp_retain_keys_not_a_list_errors() {
        // patch.go:1070-1073.
        let err = apply_strategic_merge_patch(
            &json!({"m": {"a": 1}}),
            &json!({"m": {"$retainKeys": "a", "a": 2}}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("retainKeys"), "{err}");
    }

    #[test]
    fn test_smp_array_element_delete_shim_removed() {
        // Not an upstream wire form (only the parallel `$deleteFromPrimitiveList/<f>`
        // key is); must not delete.
        let r = apply_strategic_merge_patch(
            &json!({"f": ["a", "b"]}),
            &json!({"f": [{"$deleteFromPrimitiveList": ["a"]}]}),
        )
        .unwrap();
        assert_ne!(r, json!({"f": ["b"]}));
    }

    #[test]
    fn test_smp_set_element_order_mismatch_errors() {
        let r = merge_item(
            &json!({"mergingIntList": [1, 2]}),
            &json!({"$setElementOrder/mergingIntList": [1, 2], "mergingIntList": [3]}),
        );
        assert!(r.is_err());
    }

    #[test]
    fn test_strategic_merge_delete_directive() {
        let original = json!({
            "metadata": {
                "name": "test",
                "annotations": {
                    "foo": "bar"
                }
            }
        });

        let patch = json!({
            "metadata": {
                "annotations": {
                    "$patch": "delete"
                }
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // `handleDirectiveInMergeMap` (strategicpatch/patch.go:916-920)
        // returns an empty map, and `mergeMap` stores it under the key.
        assert_eq!(result["metadata"]["annotations"], json!({}));
    }

    /// `$patch: delete` ignores every sibling key (patch.go:916-920), unlike
    /// a "clear then merge siblings" reading.
    #[test]
    fn test_strategic_merge_delete_directive_ignores_siblings() {
        let original = json!({"metadata": {"annotations": {"old": "1"}}});
        let patch = json!({
            "metadata": {"annotations": {"$patch": "delete", "new": "2"}}
        });
        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        assert_eq!(result["metadata"]["annotations"], json!({}));
    }

    // Regression: the garbage collector orphans a dependent by strategic-merge
    // `$patch: delete` on ownerReferences, keyed by the owner's `uid`. Without a
    // uid merge-key the array was replaced wholesale, leaking the `$patch`
    // directive object into the list and breaking OwnerReference decode
    // ("missing field `apiVersion`") — blocking GC orphan deletion.
    #[test]
    fn test_strategic_merge_owner_reference_delete_by_uid() {
        let original = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "rs",
                "uid": "rs-uid",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": "d",
                    "uid": "owner-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            }
        });
        // Exactly what pkg/controller/garbagecollector sends.
        let patch = json!({
            "metadata": {
                "ownerReferences": [{"$patch": "delete", "uid": "owner-uid"}],
                "uid": "rs-uid"
            }
        });

        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        // Top-level TypeMeta must survive.
        assert_eq!(result["apiVersion"], "apps/v1");
        assert_eq!(result["kind"], "ReplicaSet");
        // The ownerReference must be gone and no `$patch` directive must leak.
        let owners = result["metadata"]["ownerReferences"].as_array().unwrap();
        assert!(
            owners.is_empty(),
            "ownerReferences must be empty after $patch:delete, got {owners:?}"
        );
    }

    // A second owner must be preserved when only one is deleted by uid.
    #[test]
    fn test_strategic_merge_owner_reference_delete_keeps_others() {
        let original = json!({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"ownerReferences": [
                {"apiVersion":"apps/v1","kind":"Deployment","name":"d","uid":"a","controller":true},
                {"apiVersion":"apps/v1","kind":"Deployment","name":"e","uid":"b"}
            ]}
        });
        let patch = json!({"metadata": {"ownerReferences": [{"$patch":"delete","uid":"a"}]}});
        let result = apply_strategic_merge_patch(&original, &patch).unwrap();
        let owners = result["metadata"]["ownerReferences"].as_array().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0]["uid"], "b");
    }

    #[test]
    fn test_json_patch_add_missing_value_returns_err() {
        let original = json!({"a": 1});
        let patch = json!([{ "op": "add", "path": "/b" }]);
        let result = apply_patch(&original, &patch, PatchType::JsonPatch);
        match result {
            Err(PatchError::InvalidPatch(msg)) => assert!(msg.contains("add")),
            other => panic!(
                "expected InvalidPatch for add without value, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_json_patch_replace_missing_value_returns_err() {
        let original = json!({"a": 1});
        let patch = json!([{ "op": "replace", "path": "/a" }]);
        let result = apply_patch(&original, &patch, PatchType::JsonPatch);
        match result {
            Err(PatchError::InvalidPatch(msg)) => assert!(msg.contains("replace")),
            other => panic!(
                "expected InvalidPatch for replace without value, got {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_json_patch_test_missing_value_returns_err() {
        let original = json!({"a": 1});
        let patch = json!([{ "op": "test", "path": "/a" }]);
        let result = apply_patch(&original, &patch, PatchType::JsonPatch);
        match result {
            Err(PatchError::InvalidPatch(msg)) => assert!(msg.contains("test")),
            other => panic!(
                "expected InvalidPatch for test without value, got {:?}",
                other
            ),
        }
    }
}

mod smp_error_tests {
    //! Strategic-merge-patch error text parity (#3713): the plain errors the
    //! apiserver returns as a reason-less 500 (`interpretStrategicMergePatchError`
    //! default arm, apiserver `endpoints/handlers/patch.go:805-806`) carry
    //! upstream's exact `mergepatch` / `strategicpatch` wording.

    use rusternetes_common::patch::{apply_patch, PatchError, PatchType};
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
            uninterpreted(crate::common_unit::patch::smp_schema_tests::merge_item(
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
            uninterpreted(crate::common_unit::patch::smp_schema_tests::merge_item(
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
}

pub mod smp_schema_tests {
    //! Schema-driven strategic merge patch (#3731): the merge key and strategy of
    //! a list come from the field's struct tags (`PatchMetaFromStruct`,
    //! strategicpatch/meta.go), not from what the patch items happen to carry.

    use rusternetes_common::patch::{
        apply_patch, apply_patch_with_schema, PatchError, PatchSchema, PatchType,
    };
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

    pub(super) fn merge_item(original: Value, patch: Value) -> Result<Value, PatchError> {
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

    /// `mergeSliceWithSpecialElements` (patch.go:1583-1585): on `$patch: replace`
    /// the patch items become the live list and the patch is empty, so items that
    /// share a merge key all survive (nothing is merged into the first one).
    #[test]
    fn replace_directive_keeps_duplicate_merge_keys() {
        let r = smp(
            json!({"apiVersion": "v1", "kind": "Node",
                   "status": {"addresses": [{"type": "Hostname", "address": "old"}]}}),
            json!({"status": {"addresses": [
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "InternalIP", "address": "fd00::1"},
                {"type": "Hostname", "address": "n"},
                {"$patch": "replace"}]}}),
        )
        .unwrap();
        assert_eq!(
            r["status"]["addresses"],
            json!([
                {"type": "InternalIP", "address": "10.0.0.1"},
                {"type": "InternalIP", "address": "fd00::1"},
                {"type": "Hostname", "address": "n"}
            ])
        );
    }

    /// `ErrNoListOfLists` is its own error so the handler can answer 422
    /// (`interpretStrategicMergePatchError`, apiserver patch.go:802-803).
    #[test]
    fn list_of_lists_is_unprocessable() {
        let r = smp(
            json!({"apiVersion": "v1", "kind": "Pod", "metadata": {"finalizers": []}}),
            json!({"metadata": {"finalizers": [[1]]}}),
        );
        match r {
            Err(PatchError::UnprocessableEntity(m)) => {
                assert_eq!(m, "lists of lists are not supported")
            }
            other => panic!("expected UnprocessableEntity, got {other:?}"),
        }
    }
}

mod deep_merge_tests {
    use rusternetes_common::patch::deep_merge_objects;

    #[test]
    fn test_deep_merge_preserves_existing_map_entries() {
        let mut target = serde_json::json!({
            "capacity": {
                "cpu": "4",
                "memory": "8Gi",
                "pods": "110"
            },
            "conditions": [{"type": "Ready"}]
        });
        let patch = serde_json::json!({
            "capacity": {
                "scheduling.k8s.io/foo": "5"
            }
        });
        deep_merge_objects(&mut target, &patch);

        let cap = target.get("capacity").unwrap().as_object().unwrap();
        assert_eq!(cap.get("cpu").unwrap(), "4", "existing cpu preserved");
        assert_eq!(
            cap.get("memory").unwrap(),
            "8Gi",
            "existing memory preserved"
        );
        assert_eq!(
            cap.get("scheduling.k8s.io/foo").unwrap(),
            "5",
            "new extended resource added"
        );
        assert!(target.get("conditions").is_some(), "conditions preserved");
    }
}
