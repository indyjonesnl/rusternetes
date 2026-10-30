// OpenAPI v3 Schema Validation for Custom Resources
//
// This module provides JSON Schema validation for custom resources based on
// OpenAPI v3 schemas defined in CustomResourceDefinitions.
//
// K8s architecture (customresource_handler.go):
// 1. GetObjectMetaWithOptions — validates top-level metadata, collects unknown meta fields
// 2. PruneWithOptions — walks structural schema, collects unknown field paths
// 3. CoerceWithOptions — validates embedded resource metadata
// All unknown field paths are collected and returned together.
// See: staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning/algorithm.go

use crate::error::Error;
use crate::resources::crd::JSONSchemaProps;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

/// Cache of compiled regexes keyed by pattern string. CRD validation can hit the
/// same pattern repeatedly on every admission call; recompiling each time is
/// expensive enough to show up in profiles for cluster-wide reconciles.
pub static REGEX_CACHE: LazyLock<RwLock<HashMap<String, Regex>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Free helper: does `value` satisfy a JSONSchema `type` declaration?
fn type_matches(type_: &str, value: &Value) -> bool {
    matches!(
        (type_, value),
        ("object", Value::Object(_))
            | ("array", Value::Array(_))
            | ("string", Value::String(_))
            | ("number", Value::Number(_))
            | ("boolean", Value::Bool(_))
            | ("null", Value::Null)
    ) || (type_ == "integer" && matches!(value, Value::Number(n) if n.is_i64() || n.is_u64()))
}

pub fn compile_regex_cached(pattern: &str) -> Result<Regex, regex::Error> {
    if let Some(re) = REGEX_CACHE
        .read()
        .ok()
        .and_then(|g| g.get(pattern).cloned())
    {
        return Ok(re);
    }
    let re = Regex::new(pattern)?;
    if let Ok(mut g) = REGEX_CACHE.write() {
        g.insert(pattern.to_string(), re.clone());
    }
    Ok(re)
}

/// A single validation issue with its location inside the validated value.
///
/// `path` is the sequence of structural segments from the root of the
/// validated value to the offending node. Object segments are property
/// names; array segments are indices. Empty path means the issue applies
/// to the root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub path: Vec<PathSeg>,
    pub message: String,
}

/// A single segment of a JSON path used by ratcheting.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathSeg {
    /// Object property name (or `additionalProperties` key).
    Key(String),
    /// Array index.
    Index(usize),
}

impl PathSeg {
    /// Format a path as a K8s-style dotted/indexed string. The path
    /// `[Key("list"), Index(0), Key("field")]` becomes `list[0].field`.
    pub fn render_path(segs: &[PathSeg]) -> String {
        let mut out = String::new();
        for s in segs {
            match s {
                PathSeg::Key(k) => {
                    if !out.is_empty() {
                        out.push('.');
                    }
                    out.push_str(k);
                }
                PathSeg::Index(i) => {
                    out.push_str(&format!("[{}]", i));
                }
            }
        }
        out
    }
}

/// Validator validates JSON values against JSONSchemaProps
pub struct SchemaValidator;

impl SchemaValidator {
    /// Validate a JSON value against a schema.
    /// Collects ALL unknown field paths and returns them as a single error,
    /// matching K8s behavior where PruneWithOptions collects all paths.
    pub fn validate(schema: &JSONSchemaProps, value: &Value) -> Result<(), Error> {
        let mut unknown_fields = Vec::new();
        Self::validate_with_path(schema, value, "", &mut unknown_fields)?;
        if !unknown_fields.is_empty() {
            unknown_fields.sort();
            let msg = unknown_fields
                .iter()
                .map(|p| format!("{}: field not declared in schema", p))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::InvalidResource(msg));
        }
        Ok(())
    }

    /// Validate a JSON value against a schema WITHOUT checking for unknown fields.
    /// Used for normal (non-strict) validation where unknown fields are pruned
    /// rather than rejected. Still validates types, required fields, enums, etc.
    pub fn validate_no_unknown_check(schema: &JSONSchemaProps, value: &Value) -> Result<(), Error> {
        Self::validate_no_unknown_check_with_root(schema, value, "")
    }

    /// Like [`Self::validate_no_unknown_check`] but starts error paths at
    /// `root` (e.g. `"spec"`), so a missing nested required field reports the
    /// upstream-format path `spec.bars[0].name: Required value` rather than
    /// `bars[0].name: ...`. Callers validating a sub-document (spec/status/etc.)
    /// must pass the field's name so the path is rooted at the whole CR.
    pub fn validate_no_unknown_check_with_root(
        schema: &JSONSchemaProps,
        value: &Value,
        root: &str,
    ) -> Result<(), Error> {
        let mut dummy = Vec::new();
        Self::validate_with_path_skip_unknown(schema, value, root, &mut dummy)
    }

    /// Walk a JSON value against a schema and collect every validation issue
    /// (with structural path segments) instead of returning at the first one.
    /// Unknown-field detection is skipped — callers wanting strict behaviour
    /// should still call [`Self::validate_strict`]. This is the entry point
    /// used by CRD ratcheting (KEP-4008): the caller filters issues whose path
    /// resolves to an unchanged correlatable sub-tree before reporting.
    pub fn collect_issues(schema: &JSONSchemaProps, value: &Value) -> Vec<ValidationIssue> {
        let mut out = Vec::new();
        Self::collect_issues_recursive(schema, value, &mut Vec::new(), &mut out);
        out
    }

    fn collect_issues_recursive(
        schema: &JSONSchemaProps,
        value: &Value,
        path: &mut Vec<PathSeg>,
        out: &mut Vec<ValidationIssue>,
    ) {
        // Type check
        if let Some(ref type_) = schema.type_ {
            if !type_matches(type_, value) {
                out.push(ValidationIssue {
                    path: path.clone(),
                    message: format!(
                        "{}: must be of type {}, got {}",
                        PathSeg::render_path(path),
                        type_,
                        Self::value_type(value)
                    ),
                });
                return;
            }
        }

        // Enum
        if let Some(ref enum_values) = schema.enum_ {
            if !enum_values.contains(value) {
                let val_str = match value {
                    Value::String(s) => format!("\"{}\"", s),
                    other => other.to_string(),
                };
                out.push(ValidationIssue {
                    path: path.clone(),
                    message: format!(
                        "{}: Unsupported value: {}",
                        PathSeg::render_path(path),
                        val_str
                    ),
                });
            }
        }

        match value {
            Value::Object(obj) => Self::collect_object(schema, obj, path, out),
            Value::Array(arr) => Self::collect_array(schema, arr, path, out),
            Value::String(s) => {
                if let Err(e) = Self::validate_string(schema, s, &PathSeg::render_path(path)) {
                    out.push(ValidationIssue {
                        path: path.clone(),
                        message: e.to_string(),
                    });
                }
            }
            Value::Number(n) => {
                if let Err(e) = Self::validate_number(schema, n, &PathSeg::render_path(path)) {
                    out.push(ValidationIssue {
                        path: path.clone(),
                        message: e.to_string(),
                    });
                }
            }
            Value::Bool(_) => {}
            Value::Null => {
                if let Some(false) = schema.nullable {
                    out.push(ValidationIssue {
                        path: path.clone(),
                        message: format!("{}: cannot be null", PathSeg::render_path(path)),
                    });
                }
            }
        }
    }

    fn collect_object(
        schema: &JSONSchemaProps,
        obj: &serde_json::Map<String, Value>,
        path: &mut Vec<PathSeg>,
        out: &mut Vec<ValidationIssue>,
    ) {
        if let Some(ref required) = schema.required {
            for field in required {
                if !obj.contains_key(field) {
                    let mut p = path.clone();
                    p.push(PathSeg::Key(field.clone()));
                    out.push(ValidationIssue {
                        path: p,
                        message: format!("{}: Required value", {
                            let mut s = PathSeg::render_path(path);
                            if !s.is_empty() {
                                s.push('.');
                            }
                            s.push_str(field);
                            s
                        }),
                    });
                }
            }
        }

        if let Some(min) = schema.min_properties {
            if (obj.len() as i64) < min {
                out.push(ValidationIssue {
                    path: path.clone(),
                    message: format!(
                        "{}: must have at least {} properties",
                        PathSeg::render_path(path),
                        min
                    ),
                });
            }
        }
        if let Some(max) = schema.max_properties {
            if (obj.len() as i64) > max {
                out.push(ValidationIssue {
                    path: path.clone(),
                    message: format!(
                        "{}: must have at most {} properties",
                        PathSeg::render_path(path),
                        max
                    ),
                });
            }
        }

        if let Some(ref properties) = schema.properties {
            for (key, value) in obj {
                if let Some(prop_schema) = properties.get(key) {
                    path.push(PathSeg::Key(key.clone()));
                    Self::collect_issues_recursive(prop_schema, value, path, out);
                    path.pop();
                } else if let Some(additional) = schema.additional_properties.as_deref() {
                    use crate::resources::crd::JSONSchemaPropsOrBool;
                    if let JSONSchemaPropsOrBool::Schema(addl_schema) = additional {
                        path.push(PathSeg::Key(key.clone()));
                        Self::collect_issues_recursive(addl_schema, value, path, out);
                        path.pop();
                    }
                }
            }
        } else if let Some(additional) = schema.additional_properties.as_deref() {
            use crate::resources::crd::JSONSchemaPropsOrBool;
            if let JSONSchemaPropsOrBool::Schema(addl_schema) = additional {
                for (key, value) in obj {
                    path.push(PathSeg::Key(key.clone()));
                    Self::collect_issues_recursive(addl_schema, value, path, out);
                    path.pop();
                }
            }
        }
    }

    fn collect_array(
        schema: &JSONSchemaProps,
        arr: &[Value],
        path: &mut Vec<PathSeg>,
        out: &mut Vec<ValidationIssue>,
    ) {
        if let Some(min) = schema.min_items {
            if (arr.len() as i64) < min {
                out.push(ValidationIssue {
                    path: path.clone(),
                    message: format!(
                        "{}: must have at least {} items",
                        PathSeg::render_path(path),
                        min
                    ),
                });
            }
        }
        if let Some(max) = schema.max_items {
            if (arr.len() as i64) > max {
                out.push(ValidationIssue {
                    path: path.clone(),
                    message: format!(
                        "{}: must have at most {} items",
                        PathSeg::render_path(path),
                        max
                    ),
                });
            }
        }
        if let Some(true) = schema.unique_items {
            let mut seen: Vec<&Value> = Vec::new();
            for item in arr {
                if seen.contains(&item) {
                    out.push(ValidationIssue {
                        path: path.clone(),
                        message: format!("{}: must have unique items", PathSeg::render_path(path)),
                    });
                    break;
                }
                seen.push(item);
            }
        }

        if let Some(ref items) = schema.items {
            use crate::resources::crd::JSONSchemaPropsOrArray;
            match items.as_ref() {
                JSONSchemaPropsOrArray::Schema(item_schema) => {
                    for (i, item) in arr.iter().enumerate() {
                        path.push(PathSeg::Index(i));
                        Self::collect_issues_recursive(item_schema, item, path, out);
                        path.pop();
                    }
                }
                JSONSchemaPropsOrArray::Schemas(schemas) => {
                    for (i, item) in arr.iter().enumerate() {
                        if let Some(s) = schemas.get(i) {
                            path.push(PathSeg::Index(i));
                            Self::collect_issues_recursive(s, item, path, out);
                            path.pop();
                        }
                    }
                }
            }
        }
    }

    /// Validate with strict mode — returns unknown fields as K8s-formatted errors.
    /// Returns `strict decoding error: unknown field "spec.foo"` format.
    pub fn validate_strict(
        schema: &JSONSchemaProps,
        value: &Value,
        base_path: &str,
    ) -> Result<(), Error> {
        let mut unknown_fields = Vec::new();
        // Root all paths at `base_path` (e.g. "spec") so non-unknown errors —
        // required, type, enum — that `validate_with_path` returns via `?` carry
        // the upstream-format path (`spec.bars[0].name: Required value`), not a
        // spec-relative one. Unknown-field paths are already rooted here too, so
        // they no longer need the base_path re-prepended below.
        Self::validate_with_path(schema, value, base_path, &mut unknown_fields)?;
        if !unknown_fields.is_empty() {
            unknown_fields.sort();
            let msg = unknown_fields
                .iter()
                .map(|p| {
                    let field = p.trim_start_matches('.');
                    format!("strict decoding error: unknown field \"{}\"", field)
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::InvalidResource(msg));
        }
        Ok(())
    }

    /// Validate with path but skip unknown field detection.
    /// Validates types, required fields, enums, patterns, etc.
    fn validate_with_path_skip_unknown(
        schema: &JSONSchemaProps,
        value: &Value,
        path: &str,
        _unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        // kube-openapi `SchemaValidator.Validate`
        // (pkg/validation/validate/schema.go:131-135): a nil value runs only
        // the type and common validators, and `typeValidator.Validate`
        // (type.go:137) accepts nil when the schema is `Nullable`.
        if value.is_null() && schema.nullable == Some(true) {
            return Ok(());
        }
        // Validate type
        if let Some(ref type_) = schema.type_ {
            Self::validate_type(type_, value, path)?;
        }

        // Validate based on value type
        match value {
            Value::Object(obj) => Self::validate_object_skip_unknown(schema, obj, path)?,
            Value::Array(arr) => {
                let mut dummy = Vec::new();
                Self::validate_array(schema, arr, path, &mut dummy)?;
            }
            Value::String(s) => Self::validate_string(schema, s, path)?,
            Value::Number(n) => Self::validate_number(schema, n, path)?,
            Value::Bool(_) => {}
            Value::Null => {
                if let Some(false) = schema.nullable {
                    return Err(Error::InvalidResource(format!(
                        "Field at {} cannot be null",
                        path
                    )));
                }
            }
        }

        // Validate enum
        if let Some(ref enum_values) = schema.enum_ {
            if !enum_values.contains(value) {
                let val_str = match value {
                    Value::String(s) => format!("\"{}\"", s),
                    other => other.to_string(),
                };
                return Err(Error::InvalidResource(format!(
                    "Unsupported value: {}",
                    val_str
                )));
            }
        }

        Ok(())
    }

    /// Validate object properties without checking for unknown fields.
    fn validate_object_skip_unknown(
        schema: &JSONSchemaProps,
        obj: &serde_json::Map<String, Value>,
        path: &str,
    ) -> Result<(), Error> {
        // Validate required fields
        if let Some(ref required) = schema.required {
            for field in required {
                if !obj.contains_key(field) {
                    let field_path = if path.is_empty() {
                        field.clone()
                    } else {
                        format!("{}.{}", path, field)
                    };
                    return Err(Error::InvalidResource(format!(
                        "{}: Required value",
                        field_path
                    )));
                }
            }
        }

        // Validate known properties
        if let Some(ref properties) = schema.properties {
            for (key, value) in obj {
                if let Some(prop_schema) = properties.get(key) {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    let mut dummy = Vec::new();
                    Self::validate_with_path_skip_unknown(
                        prop_schema,
                        value,
                        &new_path,
                        &mut dummy,
                    )?;
                }
                // Unknown fields are silently ignored (they'll be pruned)
            }
        }

        Ok(())
    }

    /// Validate with a path for error reporting.
    /// Unknown fields are collected into `unknown_fields` instead of returning
    /// immediately, matching K8s PruneWithOptions behavior.
    fn validate_with_path(
        schema: &JSONSchemaProps,
        value: &Value,
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        // kube-openapi `SchemaValidator.Validate`
        // (pkg/validation/validate/schema.go:131-135): a nil value runs only
        // the type and common validators, and `typeValidator.Validate`
        // (type.go:137) accepts nil when the schema is `Nullable`.
        if value.is_null() && schema.nullable == Some(true) {
            return Ok(());
        }
        // Validate type
        if let Some(ref type_) = schema.type_ {
            Self::validate_type(type_, value, path)?;
        }

        // Validate based on value type
        match value {
            Value::Object(obj) => Self::validate_object(schema, obj, path, unknown_fields)?,
            Value::Array(arr) => Self::validate_array(schema, arr, path, unknown_fields)?,
            Value::String(s) => Self::validate_string(schema, s, path)?,
            Value::Number(n) => Self::validate_number(schema, n, path)?,
            Value::Bool(_) => {}
            Value::Null => {
                if let Some(false) = schema.nullable {
                    return Err(Error::InvalidResource(format!(
                        "Field at {} cannot be null",
                        path
                    )));
                }
            }
        }

        // Validate enum
        if let Some(ref enum_values) = schema.enum_ {
            if !enum_values.contains(value) {
                // K8s format: Unsupported value: "NonExistentValue": supported values: "Great", "Down"
                let val_str = match value {
                    Value::String(s) => format!("\"{}\"", s),
                    other => other.to_string(),
                };
                return Err(Error::InvalidResource(format!(
                    "Unsupported value: {}",
                    val_str
                )));
            }
        }

        // Validate oneOf
        if let Some(ref one_of) = schema.one_of {
            let mut dummy = Vec::new();
            let matches: Vec<_> = one_of
                .iter()
                .filter(|s| Self::validate_with_path(s, value, path, &mut dummy).is_ok())
                .collect();

            if matches.len() != 1 {
                return Err(Error::InvalidResource(format!(
                    "Field at {} must match exactly one schema (matched {})",
                    path,
                    matches.len()
                )));
            }
        }

        // Validate anyOf
        if let Some(ref any_of) = schema.any_of {
            let mut dummy = Vec::new();
            let matches = any_of
                .iter()
                .any(|s| Self::validate_with_path(s, value, path, &mut dummy).is_ok());

            if !matches {
                return Err(Error::InvalidResource(format!(
                    "Field at {} must match at least one schema",
                    path
                )));
            }
        }

        // Validate allOf
        if let Some(ref all_of) = schema.all_of {
            for sub_schema in all_of {
                Self::validate_with_path(sub_schema, value, path, unknown_fields)?;
            }
        }

        // Validate not
        if let Some(ref not) = schema.not {
            let mut dummy = Vec::new();
            if Self::validate_with_path(not, value, path, &mut dummy).is_ok() {
                return Err(Error::InvalidResource(format!(
                    "Field at {} must not match the schema",
                    path
                )));
            }
        }

        Ok(())
    }

    fn validate_type(type_: &str, value: &Value, path: &str) -> Result<(), Error> {
        let matches = type_matches(type_, value);

        if !matches {
            return Err(Error::InvalidResource(format!(
                "Field at {} must be of type {}, got {:?}",
                path,
                type_,
                Self::value_type(value)
            )));
        }

        Ok(())
    }

    fn validate_object(
        schema: &JSONSchemaProps,
        obj: &serde_json::Map<String, Value>,
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        // Validate required fields
        if let Some(ref required) = schema.required {
            for field in required {
                if !obj.contains_key(field) {
                    // K8s format: spec.bars[0].name: Required value
                    // Also accepted: missing required field "name"
                    let field_path = if path.is_empty() {
                        field.clone()
                    } else {
                        format!("{}.{}", path, field)
                    };
                    return Err(Error::InvalidResource(format!(
                        "{}: Required value",
                        field_path
                    )));
                }
            }
        }

        // Validate min/max properties
        if let Some(min) = schema.min_properties {
            if (obj.len() as i64) < min {
                return Err(Error::InvalidResource(format!(
                    "Object at {} must have at least {} properties",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.max_properties {
            if (obj.len() as i64) > max {
                return Err(Error::InvalidResource(format!(
                    "Object at {} must have at most {} properties",
                    path, max
                )));
            }
        }

        // K8s embedded resource meta fields are always allowed:
        // apiVersion, kind, metadata (case-sensitive).
        // See: pruning/algorithm.go:51-55, 77
        let is_embedded = schema.x_kubernetes_embedded_resource == Some(true);

        // Validate properties
        if let Some(ref properties) = schema.properties {
            for (key, value) in obj {
                if let Some(prop_schema) = properties.get(key) {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    Self::validate_with_path(prop_schema, value, &new_path, unknown_fields)?;
                } else if is_embedded && (key == "apiVersion" || key == "kind" || key == "metadata")
                {
                    // Embedded resource meta fields are implicitly allowed
                    continue;
                } else if let Some(ref additional) = schema.additional_properties {
                    let new_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };
                    Self::validate_additional_properties(
                        additional,
                        value,
                        &new_path,
                        unknown_fields,
                    )?;
                } else if schema.x_kubernetes_preserve_unknown_fields != Some(true) {
                    // Unknown field — collect path instead of returning immediately.
                    // K8s PruneWithOptions collects all unknown field paths.
                    let field_path = if path.is_empty() {
                        format!(".{}", key)
                    } else {
                        format!(".{}.{}", path, key)
                    };
                    unknown_fields.push(field_path);
                }
            }
        } else if let Some(ref additional) = schema.additional_properties {
            for (key, value) in obj {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                Self::validate_additional_properties(additional, value, &new_path, unknown_fields)?;
            }
        }

        Ok(())
    }

    fn validate_additional_properties(
        additional: &crate::resources::crd::JSONSchemaPropsOrBool,
        value: &Value,
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        use crate::resources::crd::JSONSchemaPropsOrBool;

        match additional {
            JSONSchemaPropsOrBool::Schema(schema) => {
                Self::validate_with_path(schema, value, path, unknown_fields)?;
            }
            JSONSchemaPropsOrBool::Bool(false) => {
                return Err(Error::InvalidResource(format!(
                    "Additional property at {} not allowed",
                    path
                )));
            }
            JSONSchemaPropsOrBool::Bool(true) => {}
        }

        Ok(())
    }

    fn validate_array(
        schema: &JSONSchemaProps,
        arr: &[Value],
        path: &str,
        unknown_fields: &mut Vec<String>,
    ) -> Result<(), Error> {
        if let Some(min) = schema.min_items {
            if (arr.len() as i64) < min {
                return Err(Error::InvalidResource(format!(
                    "Array at {} must have at least {} items",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.max_items {
            if (arr.len() as i64) > max {
                return Err(Error::InvalidResource(format!(
                    "Array at {} must have at most {} items",
                    path, max
                )));
            }
        }

        if let Some(true) = schema.unique_items {
            let mut seen = Vec::new();
            for item in arr {
                if seen.contains(item) {
                    return Err(Error::InvalidResource(format!(
                        "Array at {} must have unique items",
                        path
                    )));
                }
                seen.push(item.clone());
            }
        }

        if let Some(ref items) = schema.items {
            use crate::resources::crd::JSONSchemaPropsOrArray;

            match items.as_ref() {
                JSONSchemaPropsOrArray::Schema(item_schema) => {
                    for (i, item) in arr.iter().enumerate() {
                        let new_path = format!("{}[{}]", path, i);
                        Self::validate_with_path(item_schema, item, &new_path, unknown_fields)?;
                    }
                }
                JSONSchemaPropsOrArray::Schemas(schemas) => {
                    for (i, item) in arr.iter().enumerate() {
                        if i < schemas.len() {
                            let new_path = format!("{}[{}]", path, i);
                            Self::validate_with_path(&schemas[i], item, &new_path, unknown_fields)?;
                        } else if let Some(ref additional) = schema.additional_items {
                            let new_path = format!("{}[{}]", path, i);
                            Self::validate_additional_items(additional, item, &new_path)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_additional_items(
        additional: &crate::resources::crd::JSONSchemaPropsOrBool,
        value: &Value,
        path: &str,
    ) -> Result<(), Error> {
        use crate::resources::crd::JSONSchemaPropsOrBool;

        match additional {
            JSONSchemaPropsOrBool::Schema(schema) => {
                let mut dummy = Vec::new();
                Self::validate_with_path(schema, value, path, &mut dummy)?;
            }
            JSONSchemaPropsOrBool::Bool(false) => {
                return Err(Error::InvalidResource(format!(
                    "Additional item at {} not allowed",
                    path
                )));
            }
            JSONSchemaPropsOrBool::Bool(true) => {}
        }

        Ok(())
    }

    fn validate_string(schema: &JSONSchemaProps, s: &str, path: &str) -> Result<(), Error> {
        if let Some(min) = schema.min_length {
            if (s.len() as i64) < min {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be at least {} characters",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.max_length {
            if (s.len() as i64) > max {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be at most {} characters",
                    path, max
                )));
            }
        }

        if let Some(ref pattern) = schema.pattern {
            let re = compile_regex_cached(pattern)
                .map_err(|e| Error::InvalidResource(format!("Invalid regex pattern: {}", e)))?;

            if !re.is_match(s) {
                return Err(Error::InvalidResource(format!(
                    "String at {} must match pattern '{}'",
                    path, pattern
                )));
            }
        }

        if let Some(ref format) = schema.format {
            Self::validate_format(format, s, path)?;
        }

        Ok(())
    }

    fn validate_number(
        schema: &JSONSchemaProps,
        n: &serde_json::Number,
        path: &str,
    ) -> Result<(), Error> {
        let value = n.as_f64().unwrap_or(0.0);

        if let Some(min) = schema.minimum {
            let exclusive = schema.exclusive_minimum.unwrap_or(false);
            if exclusive {
                if value <= min {
                    return Err(Error::InvalidResource(format!(
                        "Number at {} must be greater than {}",
                        path, min
                    )));
                }
            } else if value < min {
                return Err(Error::InvalidResource(format!(
                    "Number at {} must be at least {}",
                    path, min
                )));
            }
        }

        if let Some(max) = schema.maximum {
            let exclusive = schema.exclusive_maximum.unwrap_or(false);
            if exclusive {
                if value >= max {
                    return Err(Error::InvalidResource(format!(
                        "Number at {} must be less than {}",
                        path, max
                    )));
                }
            } else if value > max {
                return Err(Error::InvalidResource(format!(
                    "Number at {} must be at most {}",
                    path, max
                )));
            }
        }

        Ok(())
    }

    fn validate_format(format: &str, value: &str, path: &str) -> Result<(), Error> {
        match format {
            "date-time" if (!value.contains('T') || !value.contains(':')) => {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be a valid date-time",
                    path
                )));
            }
            "email" if (!value.contains('@') || !value.contains('.')) => {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be a valid email",
                    path
                )));
            }
            "uri" | "url" if !value.starts_with("http://") && !value.starts_with("https://") => {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be a valid URL",
                    path
                )));
            }
            "uuid" if (value.len() != 36 || value.chars().filter(|c| *c == '-').count() != 4) => {
                return Err(Error::InvalidResource(format!(
                    "String at {} must be a valid UUID",
                    path
                )));
            }
            _ => {}
        }

        Ok(())
    }

    fn value_type(value: &Value) -> &str {
        match value {
            Value::Object(_) => "object",
            Value::Array(_) => "array",
            Value::String(_) => "string",
            Value::Number(_) => "number",
            Value::Bool(_) => "boolean",
            Value::Null => "null",
        }
    }

    /// Apply default values from a JSONSchemaProps to a JSON value:
    /// `structuraldefaulting.Default(x, structural)`
    /// (schema/defaulting/algorithm.go:30-72), which operates on the
    /// structural form of the schema (`schema.NewStructural`).
    pub fn apply_defaults(schema: &JSONSchemaProps, value: &mut Value) {
        if let Ok(structural) = crate::validation::structural::new_structural(schema) {
            crate::validation::structural::defaulting::default(value, Some(&structural));
        }
    }
}
