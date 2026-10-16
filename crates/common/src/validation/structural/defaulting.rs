//! `structuraldefaulting.Default`.
//!
//! Port of
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting/algorithm.go`
//! (:25-72), release-1.35.

use serde_json::Value;

use super::Structural;

/// `Default` (:30-72): defaults `x` depending on the default values in `s`.
pub fn default(x: &mut Value, s: Option<&Structural>) {
    let Some(s) = s else {
        return;
    };

    match x {
        Value::Object(map) => {
            for (k, prop) in &s.properties {
                let Some(def) = default_value(prop) else {
                    continue;
                };
                if map
                    .get(k)
                    .is_none_or(|v| is_non_nullable_null(v, Some(prop)))
                {
                    map.insert(k.clone(), def.clone());
                }
            }
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let Some(v) = map.get_mut(&k) else { continue };
                if let Some(prop) = s.properties.get(&k) {
                    default(v, Some(prop));
                } else if let Some(ap) = s.additional_properties.as_deref() {
                    let ap_s = ap.structural.as_ref();
                    if is_non_nullable_null(v, ap_s) {
                        // `DeepCopyJSONValue(nil)` is nil: a missing default leaves the null.
                        if let Some(def) = ap_s.and_then(default_value) {
                            *v = def.clone();
                        }
                    }
                    default(v, ap_s);
                }
            }
        }
        Value::Array(items) => {
            let item_s = s.items.as_deref();
            for item in items {
                if is_non_nullable_null(item, item_s) {
                    if let Some(def) = item_s.and_then(default_value) {
                        *item = def.clone();
                    }
                }
                default(item, item_s);
            }
        }
        // scalars, do nothing
        _ => {}
    }
}

/// `prop.Default.Object` when it is non-nil. A JSON `null` default is the
/// nil `JSON{Object: nil}` upstream.
fn default_value(s: &Structural) -> Option<&Value> {
    s.generic.default.as_ref().filter(|v| !v.is_null())
}

/// `isNonNullableNull` (:25-28): returns true if the item is nil AND it is
/// not nullable.
fn is_non_nullable_null(x: &Value, s: Option<&Structural>) -> bool {
    x.is_null() && s.is_some_and(|s| !s.generic.nullable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::structural::{Generic, StructuralOrBool};
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
