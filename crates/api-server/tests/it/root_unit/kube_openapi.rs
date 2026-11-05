//! Tests for `src/kube_openapi.rs` (`k8s.io/kube-openapi/pkg/aggregator`).
//!
//! The vendored kube-openapi has no `_test.go` files; these pin the behaviour
//! of the ported functions against the Go source they cite
//! (`FilterSpecByPathsWithoutSideEffects`, `mergeSpecs`, `mergedGVKs`,
//! `walkOnAllReferences`).

use rusternetes_api_server::kube_openapi::{
    filter_spec_by_paths_without_side_effects, merge_specs,
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters, walk_on_all_references,
};
use serde_json::{json, Value};

fn op_with_ref(r: &str) -> Value {
    json!({"get": {"responses": {"200": {"description": "OK", "schema": {"$ref": r}}}}})
}

#[test]
fn filter_removes_paths_and_the_definitions_only_they_used() {
    let sp = json!({
        "paths": {
            "/apis/a/v1/x": op_with_ref("#/definitions/A"),
            "/apis/b/v1/y": op_with_ref("#/definitions/B"),
            "x-ext": 1
        },
        "definitions": {
            "A": {"properties": {"c": {"$ref": "#/definitions/C"}}},
            "B": {"type": "object"},
            "C": {"type": "string"},
            "Unused": {"type": "object"}
        }
    });
    let out = filter_spec_by_paths_without_side_effects(&sp, &["/apis/a/v1/".to_string()]);
    assert!(out["paths"].get("/apis/a/v1/x").is_some());
    assert!(out["paths"].get("/apis/b/v1/y").is_none());
    // extensions of the paths object stay
    assert_eq!(out["paths"]["x-ext"], 1);
    let defs = out["definitions"].as_object().unwrap();
    assert!(defs.contains_key("A") && defs.contains_key("C"));
    // used before, unused now: removed
    assert!(!defs.contains_key("B"));
    // "a definition will be removed only if it has been used before"
    assert!(defs.contains_key("Unused"));
    // the input is not modified
    assert!(sp["paths"].get("/apis/b/v1/y").is_some());
}

#[test]
fn filter_without_paths_returns_the_spec() {
    let sp = json!({"definitions": {"A": {}}});
    assert_eq!(filter_spec_by_paths_without_side_effects(&sp, &[]), sp);
}

#[test]
fn walk_follows_parameter_and_definition_references() {
    let sp = json!({
        "paths": {"/p": {
            "parameters": [{"$ref": "#/parameters/P"}],
            "get": {"responses": {"default": {"schema": {"$ref": "#/definitions/D"}}}}
        }},
        "parameters": {"P": {"in": "body", "schema": {"$ref": "#/definitions/E"}}},
        "definitions": {
            "D": {"items": {"$ref": "#/definitions/F"}},
            "E": {}, "F": {}, "G": {}
        }
    });
    let mut seen = Vec::new();
    walk_on_all_references(&mut |r| seen.push(r.to_string()), &sp);
    seen.sort();
    assert_eq!(
        seen,
        [
            "#/definitions/D",
            "#/definitions/E",
            "#/definitions/F",
            "#/parameters/P"
        ]
    );
}

#[test]
fn merge_copies_paths_definitions_and_parameters() {
    let mut dest = json!({"paths": {"/a": {}}, "definitions": {"A": {"type": "object"}}});
    let src = json!({
        "paths": {"/b": op_with_ref("#/definitions/B")},
        "definitions": {"B": {"type": "string"}},
        "parameters": {"P": {"name": "p"}}
    });
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    assert!(dest["paths"].get("/a").is_some() && dest["paths"].get("/b").is_some());
    assert!(dest["definitions"].get("B").is_some());
    assert_eq!(dest["parameters"]["P"]["name"], "p");
}

#[test]
fn merge_ignores_conflicting_paths_keeping_the_destination() {
    let mut dest = json!({"paths": {"/a": {"get": {"operationId": "dest"}}}});
    let src = json!({
        "paths": {
            "/a": {"get": {"operationId": "src"}},
            "/b": op_with_ref("#/definitions/B")
        },
        "definitions": {"B": {}, "OnlyA": {}}
    });
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    assert_eq!(dest["paths"]["/a"]["get"]["operationId"], "dest");
    assert!(dest["paths"].get("/b").is_some());
    assert!(dest["definitions"].get("B").is_some());

    // "There is nothing to merge. All paths are conflicting."
    let mut dest = json!({"paths": {"/a": {}}});
    let src = json!({"paths": {"/a": {}}, "definitions": {"X": {}}});
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    assert!(dest.get("definitions").is_none());
}

#[test]
fn merge_without_ignore_fails_on_a_duplicate_path() {
    let mut dest = json!({"paths": {"/a": {}}});
    let src = json!({"paths": {"/a": {}}});
    assert_eq!(
        merge_specs(&mut dest, &src, true, true, false).unwrap_err(),
        "unable to merge: duplicated path /a"
    );
}

#[test]
fn a_source_without_paths_is_ignored() {
    let mut dest = json!({"paths": {}});
    let src = json!({"definitions": {"A": {}}});
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    assert!(dest.get("definitions").is_none());
}

#[test]
fn conflicting_definitions_are_renamed_and_references_follow() {
    let mut dest = json!({
        "paths": {"/a": {}},
        "definitions": {"Foo": {"type": "object"}}
    });
    let src = json!({
        "paths": {"/b": op_with_ref("#/definitions/Foo")},
        "definitions": {"Foo": {"type": "string"}}
    });
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    assert_eq!(dest["definitions"]["Foo"]["type"], "object");
    assert_eq!(dest["definitions"]["Foo_v2"]["type"], "string");
    assert_eq!(
        dest["paths"]["/b"]["get"]["responses"]["200"]["schema"]["$ref"],
        "#/definitions/Foo_v2"
    );

    // "Reuse previously renamed model if one exists"
    let src2 = json!({
        "paths": {"/c": op_with_ref("#/definitions/Foo")},
        "definitions": {"Foo": {"type": "string"}}
    });
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src2).unwrap();
    assert!(dest["definitions"].get("Foo_v3").is_none());
    assert_eq!(
        dest["paths"]["/c"]["get"]["responses"]["200"]["schema"]["$ref"],
        "#/definitions/Foo_v2"
    );
}

#[test]
fn a_definition_conflict_fails_when_renaming_is_off() {
    let mut dest = json!({"paths": {}, "definitions": {"Foo": {"type": "object"}}});
    let src = json!({"paths": {"/b": {}}, "definitions": {"Foo": {"type": "string"}}});
    assert_eq!(
        merge_specs(&mut dest, &src, false, false, false).unwrap_err(),
        "model name conflict in merging OpenAPI spec: Foo"
    );
}

#[test]
fn conflicting_parameters_are_renamed() {
    let mut dest = json!({"paths": {}, "parameters": {"P": {"name": "a"}}});
    let src = json!({
        "paths": {"/b": {"parameters": [{"$ref": "#/parameters/P"}]}},
        "parameters": {"P": {"name": "b"}}
    });
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    assert_eq!(dest["parameters"]["P"]["name"], "a");
    assert_eq!(dest["parameters"]["P_v2"]["name"], "b");
    assert_eq!(
        dest["paths"]["/b"]["parameters"][0]["$ref"],
        "#/parameters/P_v2"
    );
}

#[test]
fn definitions_equal_modulo_gvks_merge_their_group_version_kinds() {
    let gvk = |v: &str| json!({"group": "g", "version": v, "kind": "K"});
    let mut dest = json!({
        "paths": {},
        "definitions": {"K": {"type": "object", "x-kubernetes-group-version-kind": [gvk("v2")]}}
    });
    let src = json!({
        "paths": {"/b": {}},
        "definitions": {"K": {"type": "object", "x-kubernetes-group-version-kind": [gvk("v1")]}}
    });
    merge_specs_ignore_path_conflict_renaming_definitions_and_parameters(&mut dest, &src).unwrap();
    // no rename, the slices are merged and sorted by "group/version.kind"
    assert!(dest["definitions"].get("K_v2").is_none());
    assert_eq!(
        dest["definitions"]["K"]["x-kubernetes-group-version-kind"],
        json!([gvk("v1"), gvk("v2")])
    );
}
