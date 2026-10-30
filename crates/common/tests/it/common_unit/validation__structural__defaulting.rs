//! Unit tests moved from `src/validation/structural/defaulting.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::resources::crd::{
        CustomResourceDefinitionSpec, JSONSchemaProps, JSONSchemaPropsOrArray,
        JSONSchemaPropsOrBool,
    };
    pub use rusternetes_common::schema_validation::{PathSeg, SchemaValidator};
    pub use rusternetes_common::validation::field::{BadValue, Error, ErrorList, Path};
    pub use rusternetes_common::validation::structural::objectmeta::{
        coerce_without_paths, validate as object_meta_validate,
    };
    pub use rusternetes_common::validation::structural::prune::prune_with_options;
    pub use rusternetes_common::validation::structural::{
        new_structural, validate_structural, Structural,
    };
    pub use serde_json::{Map, Value};
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::validation::structural::defaulting::*;
    use rusternetes_common::validation::structural::{Generic, StructuralOrBool};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn d(default: Value) -> Structural {
        Structural {
            generic: Generic {
                default: Some(default),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn gen(default: Option<Value>, nullable: bool) -> Structural {
        Structural {
            generic: Generic {
                default,
                nullable,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn props(v: Vec<(&str, Structural)>) -> BTreeMap<String, Structural> {
        v.into_iter().map(|(k, s)| (k.to_string(), s)).collect()
    }

    fn items(s: Structural) -> Option<Box<Structural>> {
        Some(Box::new(s))
    }

    fn ap(s: Structural) -> Option<Box<StructuralOrBool>> {
        Some(Box::new(StructuralOrBool {
            structural: Some(s),
            bool_: false,
        }))
    }

    fn run(mut x: Value, s: Option<Structural>, expected: Value) {
        default(&mut x, s.as_ref());
        assert_eq!(x, expected);
    }

    /// algorithm_test.go `TestDefault` :30-247.
    #[test]
    fn upstream_cases() {
        // "empty"
        run(json!(null), None, json!(null));
        // "scalar"
        run(json!(4), Some(d(json!("foo"))), json!(4));
        // "scalar array"
        run(
            json!([1, 2]),
            Some(Structural {
                items: items(d(json!("foo"))),
                ..Default::default()
            }),
            json!([1, 2]),
        );
        // "object array"
        run(
            json!([{"a":1},{"b":1},{"c":1}]),
            Some(Structural {
                items: items(Structural {
                    properties: props(vec![
                        ("a", d(json!("A"))),
                        ("b", d(json!("B"))),
                        ("c", d(json!("C"))),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            json!([{"a":1,"b":"B","c":"C"},{"a":"A","b":1,"c":"C"},{"a":"A","b":"B","c":1}]),
        );
        // "object array object"
        run(
            json!({"array":[{"a":1},{"b":2}],"object":{"a":1},"additionalProperties":{"x":{"a":1},"y":{"b":2}}}),
            Some(Structural {
                properties: props(vec![
                    (
                        "array",
                        Structural {
                            items: items(Structural {
                                properties: props(vec![("a", d(json!("A"))), ("b", d(json!("B")))]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    (
                        "object",
                        Structural {
                            properties: props(vec![("a", d(json!("N"))), ("b", d(json!("O")))]),
                            ..Default::default()
                        },
                    ),
                    (
                        "additionalProperties",
                        Structural {
                            additional_properties: ap(Structural {
                                properties: props(vec![
                                    ("a", d(json!("alpha"))),
                                    ("b", d(json!("beta"))),
                                ]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ),
                    ("foo", d(json!("bar"))),
                ]),
                ..Default::default()
            }),
            json!({"array":[{"a":1,"b":"B"},{"a":"A","b":2}],"object":{"a":1,"b":"O"},"additionalProperties":{"x":{"a":1,"b":"beta"},"y":{"a":"alpha","b":2}},"foo":"bar"}),
        );
        // "empty and null"
        run(
            json!([{},{"a":1},{"a":0},{"a":0.0},{"a":""},{"a":null},{"a":[]},{"a":{}}]),
            Some(Structural {
                items: items(Structural {
                    properties: props(vec![("a", d(json!("A")))]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            json!([{"a":"A"},{"a":1},{"a":0},{"a":0.0},{"a":""},{"a":"A"},{"a":[]},{"a":{}}]),
        );
        // "null in nullable list"
        run(
            json!([null]),
            Some(Structural {
                generic: Generic {
                    nullable: true,
                    ..Default::default()
                },
                items: items(Structural {
                    properties: props(vec![("a", d(json!("A")))]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            json!([null]),
        );
        // "null in non-nullable list"
        run(
            json!([null]),
            Some(Structural {
                items: items(d(json!("A"))),
                ..Default::default()
            }),
            json!(["A"]),
        );
        // "null in nullable object"
        run(
            json!({"a": null}),
            Some(Structural {
                properties: props(vec![("a", gen(Some(json!("A")), true))]),
                ..Default::default()
            }),
            json!({"a": null}),
        );
        // "null in non-nullable object"
        run(
            json!({"a": null}),
            Some(Structural {
                properties: props(vec![("a", gen(Some(json!("A")), false))]),
                ..Default::default()
            }),
            json!({"a": "A"}),
        );
        // "null in nullable object with additionalProperties"
        run(
            json!({"a": null}),
            Some(Structural {
                additional_properties: ap(gen(Some(json!("A")), true)),
                ..Default::default()
            }),
            json!({"a": null}),
        );
        // "null in non-nullable object with additionalProperties"
        run(
            json!({"a": null}),
            Some(Structural {
                additional_properties: ap(gen(Some(json!("A")), false)),
                ..Default::default()
            }),
            json!({"a": "A"}),
        );
        // "null unknown field"
        run(
            json!({"a": null}),
            Some(Structural {
                additional_properties: Some(Box::new(StructuralOrBool {
                    structural: None,
                    bool_: true,
                })),
                ..Default::default()
            }),
            json!({"a": null}),
        );
    }
}
