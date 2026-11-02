//! The CEL declaration type of a CRD schema node: `common.SchemaDeclType` of
//! `k8s.io/apiserver/pkg/cel/common/schemas.go` (release-1.35), over
//! [`JSONSchemaProps`].
//!
//! The type model and its provider are `rusternetes_cel_checker::decl`. The
//! size estimates `SchemaDeclType` also computes (`MinSerializedSize`,
//! `MaxElements`) feed only the cost estimator and are not computed here.

use crate::resources::crd::{JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool};
use rusternetes_cel_checker::decl::{escape, DeclField, DeclType};
use rusternetes_cel_checker::DType;
use std::borrow::Cow;
use std::collections::BTreeMap;

fn string_schema() -> JSONSchemaProps {
    JSONSchemaProps {
        type_: Some("string".into()),
        ..Default::default()
    }
}

/// `WithTypeAndObjectMeta` (`schemas.go`): ensures `kind`, `apiVersion` and
/// `metadata.name` / `metadata.generateName` are specified.
pub fn with_type_and_object_meta(s: &JSONSchemaProps) -> Cow<'_, JSONSchemaProps> {
    let is =
        |p: Option<&JSONSchemaProps>, ty: &str| p.is_some_and(|p| p.type_.as_deref() == Some(ty));
    if let Some(props) = &s.properties {
        let meta = props.get("metadata");
        if is(props.get("kind"), "string")
            && is(props.get("apiVersion"), "string")
            && is(meta, "object")
            && meta.and_then(|m| m.properties.as_ref()).is_some_and(|mp| {
                is(mp.get("name"), "string") && is(mp.get("generateName"), "string")
            })
        {
            return Cow::Borrowed(s);
        }
    }
    let mut result = s.clone();
    let props = result.properties.get_or_insert_with(Default::default);
    props.insert("kind".into(), string_schema());
    props.insert("apiVersion".into(), string_schema());
    props.insert(
        "metadata".into(),
        JSONSchemaProps {
            type_: Some("object".into()),
            properties: Some(
                [
                    ("name".to_string(), string_schema()),
                    ("generateName".to_string(), string_schema()),
                ]
                .into_iter()
                .collect(),
            ),
            ..Default::default()
        },
    );
    Cow::Owned(result)
}

/// `SchemaDeclType`: the declaration of `s`, or `None` when the schema must
/// not be exposed in CEL expressions. `is_resource_root` is set for the root
/// of a custom resource and for `x-kubernetes-embedded-resource` objects.
///
/// `x-kubernetes-preserve-unknown-fields` is ignored (no rules on data without
/// a schema); arrays and maps are exposed only if their element schema is;
/// object properties only if their schema is.
pub fn schema_decl_type(s: &JSONSchemaProps, is_resource_root: bool) -> Option<DeclType> {
    if s.x_kubernetes_int_or_string == Some(true) {
        // `x-kubernetes-int-or-string` is `dyn`: all type checking is deferred
        // to runtime.
        return Some(DeclType::Simple(DType::Dyn));
    }
    let s = if is_resource_root {
        with_type_and_object_meta(s)
    } else {
        Cow::Borrowed(s)
    };
    let embedded = |p: &JSONSchemaProps| p.x_kubernetes_embedded_resource == Some(true);
    match s.type_.as_deref().unwrap_or("") {
        "array" => {
            let Some(JSONSchemaPropsOrArray::Schema(items)) = s.items.as_deref() else {
                return None;
            };
            let items_type = schema_decl_type(items, embedded(items))?;
            Some(DeclType::List(Box::new(items_type)))
        }
        "object" => {
            if let Some(JSONSchemaPropsOrBool::Schema(extra)) = s.additional_properties.as_deref() {
                let props_type = schema_decl_type(extra, embedded(extra))?;
                return Some(DeclType::Map(Box::new(props_type)));
            }
            let required: Vec<&str> = s.required.iter().flatten().map(String::as_str).collect();
            let mut fields = BTreeMap::new();
            for (name, prop) in s.properties.iter().flatten() {
                if let Some(field_type) = schema_decl_type(prop, embedded(prop)) {
                    if let Some(prop_name) = escape(name) {
                        fields.insert(
                            prop_name.clone(),
                            DeclField {
                                name: prop_name,
                                ty: field_type,
                                required: required.contains(&name.as_str()),
                            },
                        );
                    }
                }
            }
            Some(DeclType::object("object", fields))
        }
        "string" => Some(DeclType::Simple(match s.format.as_deref().unwrap_or("") {
            "byte" => DType::Bytes,
            "duration" => DType::Duration,
            "date" | "date-time" => DType::Timestamp,
            _ => DType::Str,
        })),
        "boolean" => Some(DeclType::Simple(DType::Bool)),
        "number" => Some(DeclType::Simple(DType::Double)),
        "integer" => Some(DeclType::Simple(DType::Int)),
        _ => None,
    }
}
