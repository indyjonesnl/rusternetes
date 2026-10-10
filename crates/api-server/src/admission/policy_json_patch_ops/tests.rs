//! evanphx `patch.go` semantics the `jsonPatch` patcher relies on.

use super::*;
use serde_json::json;

fn op(op: &str, path: &str, value: Option<Value>) -> Operation {
    Operation {
        op: op.into(),
        path: Some(path.into()),
        from: None,
        value,
    }
}

fn err(doc: &Value, o: Operation) -> String {
    apply(doc, &[o]).unwrap_err().to_string()
}

#[test]
fn failed_test_is_the_sentinel() {
    let doc = json!({"a": 1});
    assert_eq!(
        apply(&doc, &[op("test", "/a", Some(json!(2)))]),
        Err(ApplyError::TestFailed)
    );
    assert_eq!(apply(&doc, &[op("test", "/a", Some(json!(1)))]), Ok(doc));
}

#[test]
fn negative_indices_count_from_the_end() {
    let doc = json!({"a": [1, 2, 3]});
    assert_eq!(
        apply(&doc, &[op("remove", "/a/-1", None)]).unwrap(),
        json!({"a": [1, 2]})
    );
    assert_eq!(
        apply(&doc, &[op("add", "/a/-1", Some(json!(9)))]).unwrap(),
        json!({"a": [1, 2, 3, 9]})
    );
}

#[test]
fn error_strings_match_the_library() {
    let doc = json!({"a": {}});
    assert_eq!(
        err(&doc, op("add", "/x/y", Some(json!(1)))),
        "add operation does not apply: doc is missing path: \"/x/y\": missing value"
    );
    assert_eq!(
        err(&doc, op("remove", "/a/b", None)),
        "error in remove for path: '/a/b': Unable to remove nonexistent key: b: missing value"
    );
    assert_eq!(err(&doc, op("bogus", "/a", None)), "Unexpected kind: bogus");
    assert_eq!(
        err(&json!({"a": [1]}), op("add", "/a/5", Some(json!(1)))),
        "error in add for path: '/a/5': Unable to access invalid index: 5: invalid index referenced"
    );
}

#[test]
fn key_unescaping_and_input_is_not_mutated() {
    let doc = json!({"m": {}});
    let out = apply(&doc, &[op("add", "/m/k8s.io~1x~0y", Some(json!("t")))]).unwrap();
    assert_eq!(out, json!({"m": {"k8s.io/x~y": "t"}}));
    assert_eq!(doc, json!({"m": {}}));
}
