//! The schema the field manager's walkers are driven by (#2734).
//!
//! Port of the part of `sigs.k8s.io/structured-merge-diff/v6/schema` that the
//! walkers consult -- `schema.Atom`'s `Scalar` / `List` / `Map` split, a map's
//! `ElementRelationship` (`Separable` or `Atomic`), a list's `ElementRelationship`
//! plus `Keys` -- and of how `managedfields/internal/typeconverter.go`
//! (`NewTypeConverter`) obtains it: from the OpenAPI schema, where
//! `x-kubernetes-list-type`, `x-kubernetes-list-map-keys` and
//! `x-kubernetes-map-type` carry the relationships
//! (`kube-openapi/pkg/schemaconv/smd.go`, `convert.go`:
//! `makeUnion`, `ToSchemaFromOpenAPI`, `toSchema`), and, for a custom resource,
//! from the CRD's structural schema (`apiextensions-apiserver/pkg/apiserver/
//! schema/convert` -> `schemaconv.ToSchemaFromOpenAPI` through
//! `managedfields.NewTypeConverter(openapiModels, preserveUnknownFields)`).
//!
//! Where the upstream converter says "no annotation means atomic list / granular
//! map", so does [`Type`]. Where a path has no schema at all ([`Type::Legacy`])
//! the name-driven table of [`super::fieldset::field_kind`] still answers; that
//! is the remaining deviation, confined to kinds whose root type this tree does
//! not vendor an OpenAPI definition for.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::validation::structural::{Structural, StructuralOrBool};

/// `schema.ElementRelationship` of a list, with the associative keys.
#[derive(Debug, Clone, PartialEq)]
pub enum ListKind {
    /// `x-kubernetes-list-type: atomic`, or no annotation: the list is one leaf.
    Atomic,
    /// `x-kubernetes-list-type: set`: scalars, each a `v:` element.
    Set,
    /// `x-kubernetes-list-type: map`: items keyed by these fields, each with
    /// the default the schema gives a key an item leaves out (`typed.
    /// listItemToPathElement` fills a missing key from the schema default).
    Map(Vec<(String, Option<Value>)>),
}

/// A struct (`schema.Map` with named `Fields`) or, with no fields, an untyped
/// object.
#[derive(Debug, Clone, PartialEq)]
pub struct StructType {
    pub fields: BTreeMap<String, Type>,
    /// `x-kubernetes-map-type: atomic` (`ElementRelationship: Atomic`).
    pub atomic: bool,
    /// What a field the schema does not declare is: [`Type::Legacy`] for the
    /// root of a kind (its `metadata` may not be declared), [`Type::Deduced`]
    /// below.
    pub unknown_legacy: bool,
}

/// One node of the schema.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// `schema.Scalar`: one leaf, whatever the JSON value.
    Scalar,
    /// `typed.DeducedTypeName`: no schema (`x-kubernetes-preserve-unknown-fields`,
    /// `runtime.RawExtension`). Objects are granular, lists atomic.
    Deduced,
    /// No schema was available: [`super::fieldset::field_kind`] decides.
    Legacy,
    Struct(StructType),
    /// A map with `additionalProperties` (`Fields` empty, `ElementType` set).
    Map {
        elem: Box<Type>,
        atomic: bool,
    },
    List {
        elem: Box<Type>,
        kind: ListKind,
    },
}

impl Type {
    /// The type of the field `name` of a struct, `None` for a legacy field.
    pub fn field(&self, name: &str) -> Option<&Type> {
        match self {
            Type::Struct(s) => match s.fields.get(name) {
                Some(t) => Some(t),
                None if s.unknown_legacy => None,
                None => Some(&Type::Deduced),
            },
            Type::Map { elem, .. } => Some(elem),
            Type::Deduced => Some(&Type::Deduced),
            _ => None,
        }
    }

    /// The root of a kind from the `components.schemas` of an OpenAPI v3
    /// document: `definition` is e.g. `io.k8s.api.apps.v1.Deployment`.
    pub fn from_openapi(schemas: &Map<String, Value>, definition: &str) -> Option<Type> {
        let node = schemas.get(definition)?;
        let mut stack = vec![definition.to_string()];
        let mut t = convert(schemas, node, &mut stack);
        if let Type::Struct(s) = &mut t {
            s.unknown_legacy = true;
        }
        Some(t)
    }

    /// A root struct built from named OpenAPI definitions, for a kind whose own
    /// definition is not vendored (`Pod`: `metadata` and `spec` are).
    /// Everything else of the root stays [`Type::Legacy`].
    pub fn root_of(schemas: &Map<String, Value>, fields: &[(&str, &str)]) -> Option<Type> {
        let mut out = BTreeMap::new();
        for (name, definition) in fields {
            let node = schemas.get(*definition)?;
            let mut stack = vec![definition.to_string()];
            out.insert(name.to_string(), convert(schemas, node, &mut stack));
        }
        Some(Type::Struct(StructType {
            fields: out,
            atomic: false,
            unknown_legacy: true,
        }))
    }

    /// The root of a custom resource from the served version's structural
    /// schema (`NewTypeConverter(..., preserveUnknownFields)`).
    pub fn from_structural(s: &Structural) -> Type {
        let mut t = structural(s);
        match &mut t {
            Type::Struct(st) => st.unknown_legacy = true,
            // A schema that does not describe an object still has a root that
            // carries `metadata`.
            _ => {
                t = Type::Struct(StructType {
                    fields: BTreeMap::new(),
                    atomic: false,
                    unknown_legacy: true,
                })
            }
        }
        t
    }
}

fn ref_name(node: &Value) -> Option<&str> {
    let r = node.get("$ref").and_then(Value::as_str).or_else(|| {
        node.get("allOf")
            .and_then(Value::as_array)
            .filter(|a| a.len() == 1)
            .and_then(|a| a[0].get("$ref"))
            .and_then(Value::as_str)
    })?;
    r.rsplit('/').next()
}

/// `schemaconv.ToSchemaFromOpenAPI`'s `toSchema` for one node.
fn convert(schemas: &Map<String, Value>, node: &Value, stack: &mut Vec<String>) -> Type {
    let mut outer = node;
    let mut def = node;
    let mut pushed = false;
    if let Some(name) = ref_name(node) {
        if stack.iter().any(|s| s == name) {
            return Type::Deduced;
        }
        match schemas.get(name) {
            Some(d) => {
                def = d;
                stack.push(name.to_string());
                pushed = true;
            }
            None => return Type::Deduced,
        }
        outer = node;
    }
    let ext = |key: &str| outer.get(key).or_else(|| def.get(key));
    let t = match def.get("type").and_then(Value::as_str) {
        Some("object") => {
            let atomic = ext("x-kubernetes-map-type").and_then(Value::as_str) == Some("atomic");
            if let Some(props) = def.get("properties").and_then(Value::as_object) {
                Type::Struct(StructType {
                    fields: props
                        .iter()
                        .map(|(k, v)| (k.clone(), convert(schemas, v, stack)))
                        .collect(),
                    atomic,
                    unknown_legacy: false,
                })
            } else if let Some(add) = def.get("additionalProperties").filter(|a| a.is_object()) {
                Type::Map {
                    elem: Box::new(convert(schemas, add, stack)),
                    atomic,
                }
            } else if atomic {
                Type::Struct(StructType {
                    fields: BTreeMap::new(),
                    atomic: true,
                    unknown_legacy: false,
                })
            } else {
                Type::Deduced
            }
        }
        Some("array") => {
            let items = def.get("items").cloned().unwrap_or(Value::Null);
            let elem = convert(schemas, &items, stack);
            let kind = match ext("x-kubernetes-list-type").and_then(Value::as_str) {
                Some("set") => ListKind::Set,
                Some("map") => {
                    let keys: Vec<(String, Option<Value>)> = ext("x-kubernetes-list-map-keys")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .filter_map(Value::as_str)
                                .map(|k| (k.to_string(), openapi_key_default(schemas, &items, k)))
                                .collect()
                        })
                        .unwrap_or_default();
                    if keys.is_empty() {
                        ListKind::Atomic
                    } else {
                        ListKind::Map(keys)
                    }
                }
                _ => ListKind::Atomic,
            };
            Type::List {
                elem: Box::new(elem),
                kind,
            }
        }
        // string, integer, number, boolean, IntOrString, Quantity, Time.
        Some(_) => Type::Scalar,
        None => {
            if def.get("properties").is_some() {
                // A node with properties and no `type`.
                let mut with_type = def.clone();
                if let Some(o) = with_type.as_object_mut() {
                    o.insert("type".into(), Value::String("object".into()));
                }
                convert(schemas, &with_type, stack)
            } else {
                Type::Scalar
            }
        }
    };
    if pushed {
        stack.pop();
    }
    t
}

/// The `default` of the key property `key` of the list item schema `items`.
fn openapi_key_default(schemas: &Map<String, Value>, items: &Value, key: &str) -> Option<Value> {
    let def = match ref_name(items) {
        Some(n) => schemas.get(n)?,
        None => items,
    };
    let prop = def.get("properties")?.get(key)?;
    prop.get("default").cloned()
}

/// A CRD's structural schema as a [`Type`] (`schemaconv` over
/// `structuralschema.ToKubeOpenAPI`).
fn structural(s: &Structural) -> Type {
    let ext = &s.extensions;
    if ext.x_int_or_string {
        return Type::Scalar;
    }
    let atomic = ext.x_map_type.as_deref() == Some("atomic");
    match s.generic.type_.as_str() {
        "object" => {
            if !s.properties.is_empty() {
                Type::Struct(StructType {
                    fields: s
                        .properties
                        .iter()
                        .map(|(k, v)| (k.clone(), structural(v)))
                        .collect(),
                    atomic,
                    unknown_legacy: false,
                })
            } else if let Some(StructuralOrBool {
                structural: Some(add),
                ..
            }) = s.additional_properties.as_deref()
            {
                Type::Map {
                    elem: Box::new(structural(add)),
                    atomic,
                }
            } else if atomic {
                Type::Struct(StructType {
                    fields: BTreeMap::new(),
                    atomic: true,
                    unknown_legacy: false,
                })
            } else {
                Type::Deduced
            }
        }
        "array" => {
            let elem = s.items.as_deref().map(structural).unwrap_or(Type::Deduced);
            let kind = match ext.x_list_type.as_deref() {
                Some("set") => ListKind::Set,
                Some("map") if !ext.x_list_map_keys.is_empty() => ListKind::Map(
                    ext.x_list_map_keys
                        .iter()
                        .map(|k| {
                            let default = s
                                .items
                                .as_deref()
                                .and_then(|i| i.properties.get(k))
                                .and_then(|p| p.generic.default.clone());
                            (k.clone(), default)
                        })
                        .collect(),
                ),
                _ => ListKind::Atomic,
            };
            Type::List {
                elem: Box::new(elem),
                kind,
            }
        }
        "" if ext.x_preserve_unknown_fields => Type::Deduced,
        "" => Type::Deduced,
        _ => Type::Scalar,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schemas() -> Map<String, Value> {
        json!({
            "Port": {"type": "object", "properties": {
                "containerPort": {"type": "integer", "default": 0},
                "protocol": {"type": "string", "default": "TCP"}}},
            "Sel": {"type": "object", "x-kubernetes-map-type": "atomic",
                    "properties": {"a": {"type": "string"}}},
            "Spec": {"type": "object", "properties": {
                "ports": {"type": "array", "items": {"allOf": [{"$ref": "#/components/schemas/Port"}]},
                          "x-kubernetes-list-type": "map",
                          "x-kubernetes-list-map-keys": ["containerPort", "protocol"]},
                "sel": {"allOf": [{"$ref": "#/components/schemas/Sel"}]},
                "plain": {"type": "array", "items": {"type": "string"}},
                "self": {"$ref": "#/components/schemas/Spec"}}}
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn openapi_annotations_become_relationships() {
        let t = Type::from_openapi(&schemas(), "Spec").unwrap();
        let Type::Struct(s) = &t else { panic!() };
        match &s.fields["ports"] {
            Type::List {
                kind: ListKind::Map(keys),
                ..
            } => {
                assert_eq!(keys[0], ("containerPort".to_string(), Some(json!(0))));
                assert_eq!(keys[1], ("protocol".to_string(), Some(json!("TCP"))));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(&s.fields["sel"], Type::Struct(st) if st.atomic));
        // No list-type annotation: atomic.
        assert!(matches!(
            &s.fields["plain"],
            Type::List {
                kind: ListKind::Atomic,
                ..
            }
        ));
        // A recursive reference ends in an untyped node, not a loop.
        assert_eq!(s.fields["self"], Type::Deduced);
    }
}
