//! Filters, recursive descent and `range`/`end` in a printer-column JSONPath.
//! Cases ported from `staging/src/k8s.io/client-go/util/jsonpath/jsonpath_test.go`
//! (`TestStructInput`'s storeTests, `TestKubernetes`, `TestRangeOverEmptyList`)
//! against the evaluator of `registry/apiextensions/tableconvertor.rs`, which
//! mirrors `FindResults`. `run` is `Execute`: each result set printed by
//! `PrintResults` (values separated by a space) and concatenated.

use rusternetes_api_server::registry::apiextensions::tableconvertor::find_results;
use rusternetes_common::jsonpath;
use serde_json::{json, Value};

fn run(template: &str, data: &Value) -> Result<String, String> {
    let parser = jsonpath::parse("t", template).map_err(|e| e.to_string())?;
    let full = find_results(&parser.root, data)?;
    let mut out = String::new();
    for set in full {
        let parts: Vec<String> = set
            .iter()
            .map(|v| match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect();
        out.push_str(&parts.join(" "));
    }
    Ok(out)
}

fn nodes() -> Value {
    json!({
        "kind": "List",
        "items": [
            {"kind": "None", "metadata": {"name": "127.0.0.1",
                "labels": {"kubernetes.io/hostname": "127.0.0.1"}},
             "status": {"capacity": {"cpu": "4"}, "ready": true,
                "addresses": [{"type": "LegacyHostIP", "address": "127.0.0.1"}]}},
            {"kind": "None", "metadata": {"name": "127.0.0.2"},
             "status": {"capacity": {"cpu": "8"},
                "addresses": [{"type": "LegacyHostIP", "address": "127.0.0.2"},
                              {"type": "another", "address": "127.0.0.3"}]}}
        ]
    })
}

#[test]
fn filter_over_scalars() {
    assert_eq!(run("{[?(@<5)]}", &json!([2, 6, 3, 7])).unwrap(), "2 3");
}

#[test]
fn filter_string_and_bool_equality() {
    let d = json!({"users": [{"name": "e2e", "user": {"password": "secret"}},
                             {"name": "other", "user": {"password": "x"}}]});
    assert_eq!(
        run(r#"{.users[?(@.name=="e2e")].user.password}"#, &d).unwrap(),
        "secret"
    );
    assert_eq!(
        run("{.items[?(@..ready==true)].metadata.name}", &nodes()).unwrap(),
        "127.0.0.1"
    );
    assert_eq!(
        run(
            r#"{.items[?(@.metadata.labels.kubernetes\.io/hostname=="127.0.0.1")].kind}"#,
            &nodes()
        )
        .unwrap(),
        "None"
    );
}

#[test]
fn exists_filter() {
    let d = json!([{"id": "i1", "x": 4}, {"id": "i2", "z": 1}, {"id": "i3"}, {"id": "i5", "z": 2}]);
    assert_eq!(run("{[?(@.z)].id}", &d).unwrap(), "i2 i5");
}

#[test]
fn filter_errors_follow_upstream() {
    let d = json!({"Book": [{"Price": 8}]});
    assert!(run("{.Book[?(@.Price<>10)]}", &d).is_err());
    // A non-array input cannot be filtered.
    assert!(run("{[?(@.a==1)]}", &json!({"a": 1}))
        .unwrap_err()
        .contains("cannot be filtered"));
    // int vs float is errBadComparison.
    assert_eq!(
        run("{[?(@.a==1.5)]}", &json!([{"a": 1}])).unwrap_err(),
        "incompatible types for comparison"
    );
}

#[test]
fn recursive_descent() {
    assert_eq!(run("{..}", &json!([1, 2, 3])).unwrap(), "[1,2,3]");
    assert_eq!(
        run(
            "{..name}",
            &json!({"items": [{"metadata": {"name": "a"}}, {"name": "b"}]})
        )
        .unwrap(),
        "a b"
    );
    let store = json!({"A": [{"Price": 8.95}, {"Price": 12.99}], "B": [{"Price": 19.95}]});
    assert_eq!(run("{..Price}", &store).unwrap(), "8.95 12.99 19.95");
    assert_eq!(run("{...Price}", &store).unwrap(), "8.95 12.99 19.95");
}

#[test]
fn range_and_end() {
    assert_eq!(
        run("{range .items[*]}{.metadata.name}, {end}{.kind}", &nodes()).unwrap(),
        "127.0.0.1, 127.0.0.2, List"
    );
    assert_eq!(
        run(
            "{range .items[*]}{range .status.addresses[*]}{.address}, {end}{end}",
            &nodes()
        )
        .unwrap(),
        "127.0.0.1, 127.0.0.2, 127.0.0.3, "
    );
    assert_eq!(
        run(
            "{range .items[*]}{.metadata.name}{end}",
            &json!({"items": []})
        )
        .unwrap(),
        ""
    );
    assert_eq!(
        run(
            r#"{range .items[*]}{.metadata.name}{":"}{range @.spec.containers[*]}{.name}{","}{end}{"+"}{end}"#,
            &json!({"items": []})
        )
        .unwrap(),
        ""
    );
    assert_eq!(
        run("{range .items[*]}{.metadata.name}{end}{end}", &nodes()).unwrap_err(),
        "not in range, nothing to end"
    );
}
