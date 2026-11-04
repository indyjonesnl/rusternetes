//! Port of `tableconvertor_test.go`
//! (`Test_cellForJSONValue`, `Test_convertor_ConvertToTable`) for
//! `src/registry/apiextensions/tableconvertor.rs`.

use rusternetes_api_server::registry::apiextensions::tableconvertor::{
    cell_for_json_value, columns_for_version, TableConvertor,
};
use rusternetes_common::resources::{CustomResourceColumnDefinition, CustomResourceDefinition};
use serde_json::{json, Value};

fn col(name: &str, ty: &str, path: &str) -> CustomResourceColumnDefinition {
    CustomResourceColumnDefinition {
        name: name.to_string(),
        type_: ty.to_string(),
        format: None,
        description: None,
        priority: None,
        json_path: path.to_string(),
    }
}

fn rfc3339_ago(d: chrono::Duration) -> String {
    (chrono::Utc::now() - d).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[test]
fn cell_for_json_value_matches_upstream_table() {
    let cases: Vec<(&str, Value, Value)> = vec![
        ("integer", json!(42), json!(42)),
        ("integer", json!(2.5), json!(2)),
        ("integer", json!(true), Value::Null),
        ("integer", json!("foo"), Value::Null),
        ("number", json!(42), json!(42.0)),
        ("number", json!(2.5), json!(2.5)),
        ("number", json!(true), Value::Null),
        ("number", json!("foo"), Value::Null),
        ("boolean", json!(42), Value::Null),
        ("boolean", json!(2.5), Value::Null),
        ("boolean", json!(true), json!(true)),
        ("boolean", json!("foo"), Value::Null),
        ("string", json!(42), Value::Null),
        ("string", json!(2.5), Value::Null),
        ("string", json!(true), Value::Null),
        ("string", json!("foo"), json!("foo")),
        ("date", json!(42), Value::Null),
        ("date", json!(2.5), Value::Null),
        ("date", json!(true), Value::Null),
        (
            "date",
            json!(rfc3339_ago(
                chrono::Duration::hours(12) + chrono::Duration::minutes(30)
            )),
            json!("12h"),
        ),
        (
            "date",
            json!(rfc3339_ago(
                -(chrono::Duration::hours(12) + chrono::Duration::minutes(30))
            )),
            json!("<invalid>"),
        ),
        ("date", json!(""), json!("<unknown>")),
        ("date", json!("not a time"), json!("<invalid>")),
        ("unknown", json!("foo"), Value::Null),
    ];
    for (ty, value, want) in cases {
        assert_eq!(cell_for_json_value(ty, &value), want, "{ty} {value}");
    }
}

fn blah(spec: Value) -> Value {
    json!({
        "apiVersion": "example.istio.io/v1alpha1", "kind": "Blah",
        "metadata": {"name": "blah"}, "spec": spec
    })
}

#[test]
fn multiple_string_values_follow_upstream() {
    let (c, err) = TableConvertor::new(&[
        col("valueOnly", "string", ".spec.servers[0].hosts[0]"),
        col("single1", "string", ".spec.servers[0].hosts"),
        col("single2", "string", ".spec.servers[1].hosts"),
        col("multi", "string", ".spec.servers[*].hosts"),
    ]);
    assert!(err.is_none());
    let obj = blah(json!({"servers": [{"hosts": ["foo"]}, {"hosts": ["bar", "baz"]}]}));
    let table = c.convert_to_table(&obj, false);
    assert_eq!(table.rows.len(), 1);
    assert_eq!(
        table.rows[0].cells,
        vec![
            json!("blah"),
            json!("foo"),
            json!(r#"["foo"]"#),
            json!(r#"["bar","baz"]"#),
            // upstream TODO: only the first of the wildcard's results
            json!(r#"["foo"]"#),
        ]
    );
    assert_eq!(table.rows[0].object.as_ref(), Some(&obj));
}

#[test]
fn integer_values_as_string_and_as_integer() {
    let obj = blah(json!({"foo": [{"bar": [1]}, {"bar": [2, 3]}]}));
    let paths = [
        ".spec.foo[0].bar[0]",
        ".spec.foo[0].bar",
        ".spec.foo[1].bar",
        ".spec.foo[*].bar",
    ];
    let as_string: Vec<_> = paths.iter().map(|p| col("c", "string", p)).collect();
    let (c, _) = TableConvertor::new(&as_string);
    assert_eq!(
        c.convert_to_table(&obj, false).rows[0].cells,
        vec![
            json!("blah"),
            json!("1"),
            json!("[1]"),
            json!("[2,3]"),
            json!("[1]")
        ]
    );
    let as_int: Vec<_> = paths.iter().map(|p| col("c", "integer", p)).collect();
    let (c, _) = TableConvertor::new(&as_int);
    assert_eq!(
        c.convert_to_table(&obj, false).rows[0].cells,
        vec![
            json!("blah"),
            json!(1),
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );
}

#[test]
fn list_headers_resource_version_and_no_headers() {
    let (c, _) = TableConvertor::new(&[]);
    let list = json!({
        "apiVersion": "v1", "kind": "BlahList",
        "metadata": {"resourceVersion": "7", "continue": "abc", "remainingItemCount": 3},
        "items": [{"metadata": {"name": "a"}}, {"metadata": {"name": "b"}}]
    });
    let t = c.convert_to_table(&list, false);
    assert_eq!(t.column_definitions.len(), 1);
    assert_eq!(t.column_definitions[0].name, "Name");
    assert_eq!(t.column_definitions[0].format, "name");
    assert_eq!(t.metadata.resource_version.as_deref(), Some("7"));
    assert_eq!(t.metadata.continue_token.as_deref(), Some("abc"));
    assert_eq!(t.metadata.remaining_item_count, Some(3));
    assert_eq!(t.rows.len(), 2);
    let t = c.convert_to_table(&list, true);
    assert!(t.column_definitions.is_empty());
    assert_eq!(t.rows.len(), 2);
}

#[test]
fn missing_keys_and_bad_paths_give_null_cells() {
    let (c, err) = TableConvertor::new(&[
        col("missing", "string", ".spec.nope"),
        col("oob", "string", ".spec.list[9]"),
        col("notarray", "string", ".spec.s[0]"),
    ]);
    assert!(err.is_none());
    let obj = blah(json!({"list": [1], "s": "x"}));
    assert_eq!(
        c.convert_to_table(&obj, false).rows[0].cells,
        vec![json!("blah"), Value::Null, Value::Null, Value::Null]
    );
}

#[test]
fn unparsable_column_stops_the_build_and_keeps_earlier_columns() {
    let (c, err) = TableConvertor::new(&[
        col("ok", "string", ".metadata.name"),
        col("bad", "string", ".spec.[["),
        col("after", "string", ".metadata.name"),
    ]);
    assert_eq!(
        err.as_deref(),
        Some("unrecognized column definition \".spec.[[\"")
    );
    let names: Vec<_> = c.headers().iter().map(|h| h.name.as_str()).collect();
    assert_eq!(names, ["Name", "ok"]);
}

#[test]
fn header_description_priority_and_format() {
    let mut c1 = col("Replicas", "integer", ".spec.replicas");
    c1.priority = Some(1);
    c1.format = Some("int32".to_string());
    let mut c2 = col("Desc", "string", ".spec.d");
    c2.description = Some("mine".to_string());
    let (c, _) = TableConvertor::new(&[c1, c2]);
    let h = c.headers();
    assert_eq!(
        h[1].description,
        "Custom resource definition column (in JSONPath format): .spec.replicas"
    );
    assert_eq!((h[1].priority, h[1].format.as_str()), (1, "int32"));
    assert_eq!(h[2].description, "mine");
}

#[test]
fn columns_default_to_age_when_a_version_declares_none() {
    let crd: CustomResourceDefinition = serde_json::from_value(json!({
        "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
        "metadata": {"name": "foos.x.io"},
        "spec": {"group": "x.io", "scope": "Namespaced",
            "names": {"plural": "foos", "kind": "Foo"},
            "versions": [
              {"name": "v1", "served": true, "storage": true},
              {"name": "v2", "served": true, "storage": false,
               "additionalPrinterColumns": [{"name": "C", "type": "string", "jsonPath": ".spec.c"}]}
            ]}
    }))
    .unwrap();
    let v1 = columns_for_version(&crd, "v1").unwrap();
    assert_eq!(v1.len(), 1);
    assert_eq!(
        (
            v1[0].name.as_str(),
            v1[0].type_.as_str(),
            v1[0].json_path.as_str()
        ),
        ("Age", "date", ".metadata.creationTimestamp")
    );
    assert_eq!(columns_for_version(&crd, "v2").unwrap()[0].name, "C");
    assert!(columns_for_version(&crd, "v3").is_none());
}
