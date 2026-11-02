//! The CEL declaration type of a CRD schema node: `common.SchemaDeclType` of
//! `k8s.io/apiserver/pkg/cel/common/schemas.go` (release-1.35), over
//! [`JSONSchemaProps`].
//!
//! The type model and its provider are `rusternetes_cel_checker::decl`. The
//! size estimates `SchemaDeclType` also computes (`MinSerializedSize`,
//! `MaxElements`) feed the cost estimator.

use crate::resources::crd::{JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool};
use rusternetes_cel_checker::decl::{escape, limits, DeclField, DeclType};
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

/// `maxRequestSizeBytes` (`schemas.go`), `celconfig.MaxRequestSizeBytes`.
const MAX_REQUEST_SIZE_BYTES: i64 = limits::MAX_REQUEST_SIZE_BYTES;

fn zero_if_negative(v: i64) -> i64 {
    v.max(0)
}

/// `estimateMaxStringLengthPerRequest`: the longest string (in characters)
/// compatible with the format of `s`. Only for `string` and
/// `x-kubernetes-int-or-string` schemas.
fn estimate_max_string_length_per_request(s: &JSONSchemaProps) -> i64 {
    if s.x_kubernetes_int_or_string == Some(true) {
        return MAX_REQUEST_SIZE_BYTES - 2;
    }
    match s.format.as_deref().unwrap_or("") {
        "duration" => limits::MAX_DURATION_SIZE_JSON,
        "date" => limits::JSON_DATE_SIZE,
        "date-time" => limits::MAX_DATETIME_SIZE_JSON,
        // Subtract 2 to account for "".
        _ => MAX_REQUEST_SIZE_BYTES - 2,
    }
}

/// `estimateMaxStringEnumLength`: the length of the longest enum value.
fn estimate_max_string_enum_length(s: &JSONSchemaProps) -> i64 {
    s.enum_
        .iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .map(|v| v.len() as i64)
        .max()
        .unwrap_or(0)
}

/// `estimateMaxArrayItemsFromMinSize`.
fn estimate_max_array_items_from_min_size(min_size: i64) -> i64 {
    // Subtract 2 to account for [ and ].
    (MAX_REQUEST_SIZE_BYTES - 2) / (min_size + 1)
}

/// `estimateMaxAdditionalPropertiesFromMinSize`.
fn estimate_max_additional_properties_from_min_size(min_size: i64) -> i64 {
    // 2 bytes for key + "" + colon + comma + smallest possible value.
    let key_value_pair_size = min_size + 6;
    // Subtract 2 to account for { and }.
    (MAX_REQUEST_SIZE_BYTES - 2) / key_value_pair_size
}

/// `estimateMaxElementsFromMaxLength`: `maxLength` is in code points, and one
/// can take up to 4 bytes. Only called with `max_length` set.
fn estimate_max_elements_from_max_length(s: &JSONSchemaProps) -> i64 {
    zero_if_negative(s.max_length.unwrap_or(0)).saturating_mul(4)
}

/// `SchemaDeclType`: the declaration of `s`, or `None` when the schema must
/// not be exposed in CEL expressions. `is_resource_root` is set for the root
/// of a custom resource and for `x-kubernetes-embedded-resource` objects.
///
/// `x-kubernetes-preserve-unknown-fields` is ignored (no rules on data without
/// a schema); arrays and maps are exposed only if their element schema is;
/// object properties only if their schema is.
///
/// The returned types carry `MinSerializedSize` and `MaxElements`.
pub fn schema_decl_type(s: &JSONSchemaProps, is_resource_root: bool) -> Option<DeclType> {
    if s.x_kubernetes_int_or_string == Some(true) {
        // `x-kubernetes-int-or-string` is `dyn`: all type checking is deferred
        // to runtime. The smallest serialized value is 0 (1 byte).
        let mut dyn_ = DeclType::simple_with_min_size(DType::Dyn, 1);
        // With a maxLength, bound the max elements by it; else by the request.
        dyn_.max_elements = if s.max_length.is_some() {
            estimate_max_elements_from_max_length(s)
        } else {
            estimate_max_string_length_per_request(s)
        };
        return Some(dyn_);
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
            let max_items = match s.max_items {
                Some(m) => zero_if_negative(m),
                None => estimate_max_array_items_from_min_size(items_type.min_serialized_size),
            };
            Some(DeclType::list(items_type, max_items))
        }
        "object" => {
            if let Some(JSONSchemaPropsOrBool::Schema(extra)) = s.additional_properties.as_deref() {
                let props_type = schema_decl_type(extra, embedded(extra))?;
                let max_properties = match s.max_properties {
                    Some(m) => zero_if_negative(m),
                    None => estimate_max_additional_properties_from_min_size(
                        props_type.min_serialized_size,
                    ),
                };
                return Some(DeclType::map(props_type, max_properties));
            }
            let required: Vec<&str> = s.required.iter().flatten().map(String::as_str).collect();
            let mut fields = BTreeMap::new();
            // An object is always serialized as at least `{}`.
            let mut min_serialized_size = 2i64;
            for (name, prop) in s.properties.iter().flatten() {
                let Some(field_type) = schema_decl_type(prop, embedded(prop)) else {
                    continue;
                };
                let is_required = required.contains(&name.as_str());
                let min_field_size = field_type.min_serialized_size;
                if let Some(prop_name) = escape(name) {
                    fields.insert(
                        prop_name.clone(),
                        DeclField {
                            name: prop_name,
                            ty: field_type,
                            required: is_required,
                        },
                    );
                }
                // Only required properties without a default count: defaults
                // are filled in server-side.
                let has_default = prop.default.as_ref().is_some_and(|d| !d.is_null());
                if is_required && !has_default {
                    min_serialized_size += name.len() as i64 + min_field_size + 4;
                }
            }
            let mut obj = DeclType::object("object", fields);
            obj.min_serialized_size = min_serialized_size;
            Some(obj)
        }
        "string" => {
            let format = s.format.as_deref().unwrap_or("");
            let (t, min) = match format {
                "byte" => {
                    let mut t =
                        DeclType::simple_with_min_size(DType::Bytes, limits::MIN_STRING_SIZE);
                    t.max_elements = match s.max_length {
                        Some(m) => zero_if_negative(m),
                        None => estimate_max_string_length_per_request(&s),
                    };
                    return Some(t);
                }
                "duration" => (DType::Duration, limits::MIN_DURATION_SIZE_JSON),
                "date" => (DType::Timestamp, limits::JSON_DATE_SIZE),
                "date-time" => (DType::Timestamp, limits::MIN_DATETIME_SIZE_JSON),
                _ => (DType::Str, limits::MIN_STRING_SIZE),
            };
            let mut t = DeclType::simple_with_min_size(t, min);
            t.max_elements = if matches!(format, "duration" | "date" | "date-time") {
                estimate_max_string_length_per_request(&s)
            } else if s.max_length.is_some() {
                estimate_max_elements_from_max_length(&s)
            } else if s.enum_.as_ref().is_some_and(|e| !e.is_empty()) {
                estimate_max_string_enum_length(&s)
            } else {
                estimate_max_string_length_per_request(&s)
            };
            Some(t)
        }
        "boolean" => Some(DeclType::simple(DType::Bool)),
        "number" => Some(DeclType::simple(DType::Double)),
        "integer" => Some(DeclType::simple(DType::Int)),
        _ => None,
    }
}
