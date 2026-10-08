//! Structural schemas for CustomResourceDefinitions: the `Structural` type,
//! `NewStructural` and `ValidateStructural`.
//!
//! Ports of, under
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/`
//! (release-1.35):
//!
//! - `structural.go` (`Structural`, `Generic`, `Extensions`, `ValueValidation`,
//!   `NestedValueValidation`, ...),
//! - `convert.go` (`NewStructural` and the `new*` helpers),
//! - `validation.go` (`ValidateStructural`, `validateStructuralInvariants`,
//!   the IntOrString patterns, the metadata invariants),
//! - `complete.go` (`validateStructuralCompleteness`).
//!
//! The consumer is the `NonStructuralSchema` condition
//! (`nonstructuralschema_controller.go:102-122`).
//!
//! Deviations, stated deliberately:
//!
//! - `properties` are a `BTreeMap` (upstream: a Go map walked in random order,
//!   with the resulting errors then sorted by message). The final sort is
//!   ported, so output order is the same.
//! - `pattern` is compiled with the Rust `regex` crate (RE2-like, as Go's
//!   `regexp` is); the *wording* of the compile error after
//!   `must be a valid regular expression, but isn't:` is the Rust crate's, not
//!   Go's.
//! - Not ported: `ToKubeOpenAPI`/`kubeopenapi.go`, `unfold.go`, `skeleton.go`
//!   and the visitor helpers; nothing in Rusternetes consumes them yet.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::resources::crd::{JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool};
use crate::validation::field::{Error, Path};

/// `schema.Structural` (structural.go:27-37).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Structural {
    pub items: Option<Box<Structural>>,
    pub properties: BTreeMap<String, Structural>,
    pub additional_properties: Option<Box<StructuralOrBool>>,
    pub generic: Generic,
    pub extensions: Extensions,
    pub validation_extensions: ValidationExtensions,
    pub value_validation: Option<ValueValidation>,
}

/// `schema.StructuralOrBool` (structural.go:41-44).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StructuralOrBool {
    pub structural: Option<Structural>,
    pub bool_: bool,
}

/// `schema.Generic`: the generic schema fields not allowed in value
/// validation (structural.go:49-60). `type_` is `""` when unset.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Generic {
    pub description: String,
    pub type_: String,
    pub title: String,
    pub default: Option<Value>,
    pub nullable: bool,
}

/// `schema.Extensions`: the Kubernetes OpenAPI v3 vendor extensions
/// (structural.go:64-130).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Extensions {
    pub x_preserve_unknown_fields: bool,
    pub x_embedded_resource: bool,
    pub x_int_or_string: bool,
    pub x_list_map_keys: Vec<String>,
    pub x_list_type: Option<String>,
    pub x_map_type: Option<String>,
}

/// `schema.ValidationExtensions` (structural.go:134-139).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValidationExtensions {
    pub x_validations: Vec<Value>,
}

/// `schema.ValueValidation`: all schema fields not contributing to the
/// structure of the schema (structural.go:143-163).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValueValidation {
    pub format: String,
    pub maximum: Option<f64>,
    pub exclusive_maximum: bool,
    pub minimum: Option<f64>,
    pub exclusive_minimum: bool,
    pub max_length: Option<i64>,
    pub min_length: Option<i64>,
    pub pattern: String,
    pub max_items: Option<i64>,
    pub min_items: Option<i64>,
    pub unique_items: bool,
    pub multiple_of: Option<f64>,
    pub enum_: Vec<Value>,
    pub max_properties: Option<i64>,
    pub min_properties: Option<i64>,
    pub required: Vec<String>,
    pub all_of: Vec<NestedValueValidation>,
    pub one_of: Vec<NestedValueValidation>,
    pub any_of: Vec<NestedValueValidation>,
    pub not: Option<Box<NestedValueValidation>>,
}

/// `schema.NestedValueValidation` (structural.go:167-189).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NestedValueValidation {
    pub value_validation: ValueValidation,
    pub validation_extensions: ValidationExtensions,
    pub items: Option<Box<NestedValueValidation>>,
    pub properties: BTreeMap<String, NestedValueValidation>,
    pub additional_properties: Option<Box<NestedValueValidation>>,
    pub forbidden_generics: Generic,
    pub forbidden_extensions: Extensions,
}

// ---------------------------------------------------------------------------
// convert.go
// ---------------------------------------------------------------------------

/// `schema.NewStructural` (convert.go:41-109). The error text is upstream's.
pub fn new_structural(s: &JSONSchemaProps) -> Result<Structural, String> {
    validate_unsupported_fields(s)?;
    let vv = new_value_validation(s)?;
    let mut ss = Structural {
        generic: new_generics(s),
        extensions: new_extensions(s)?,
        validation_extensions: new_validation_extensions(s),
        value_validation: Some(vv),
        ..Default::default()
    };

    if let Some(items) = &s.items {
        match &**items {
            // "we validate that it is not an array" (:80-83)
            JSONSchemaPropsOrArray::Schemas(v) if !v.is_empty() => {
                return Err("OpenAPIV3Schema 'items' must be a schema, but is an array".into());
            }
            // an empty array is `Items.Schema == nil`: NewStructural(nil) is nil
            JSONSchemaPropsOrArray::Schemas(_) => {}
            JSONSchemaPropsOrArray::Schema(item) => {
                ss.items = Some(Box::new(new_structural(item)?));
            }
        }
    }

    if let Some(props) = &s.properties {
        for (k, x) in props {
            ss.properties.insert(k.clone(), new_structural(x)?);
        }
    }

    if let Some(ap) = &s.additional_properties {
        ss.additional_properties = Some(Box::new(match &**ap {
            JSONSchemaPropsOrBool::Schema(schema) => StructuralOrBool {
                structural: Some(new_structural(schema)?),
                bool_: true,
            },
            JSONSchemaPropsOrBool::Bool(b) => StructuralOrBool {
                structural: None,
                bool_: *b,
            },
        }));
    }

    Ok(ss)
}

/// `newGenerics` (convert.go:111-127).
fn new_generics(s: &JSONSchemaProps) -> Generic {
    Generic {
        type_: s.type_.clone().unwrap_or_default(),
        description: s.description.clone().unwrap_or_default(),
        title: s.title.clone().unwrap_or_default(),
        nullable: s.nullable.unwrap_or(false),
        default: s.default.clone(),
    }
}

/// `newValueValidation` (convert.go:129-186).
fn new_value_validation(s: &JSONSchemaProps) -> Result<ValueValidation, String> {
    let nested = |v: &Option<Vec<JSONSchemaProps>>| -> Result<Vec<NestedValueValidation>, String> {
        v.iter()
            .flatten()
            .map(new_nested_value_validation)
            .collect()
    };
    Ok(ValueValidation {
        format: s.format.clone().unwrap_or_default(),
        maximum: s.maximum,
        exclusive_maximum: s.exclusive_maximum.unwrap_or(false),
        minimum: s.minimum,
        exclusive_minimum: s.exclusive_minimum.unwrap_or(false),
        max_length: s.max_length,
        min_length: s.min_length,
        pattern: s.pattern.clone().unwrap_or_default(),
        max_items: s.max_items,
        min_items: s.min_items,
        unique_items: s.unique_items.unwrap_or(false),
        multiple_of: s.multiple_of,
        max_properties: s.max_properties,
        min_properties: s.min_properties,
        required: s.required.clone().unwrap_or_default(),
        not: match &s.not {
            Some(n) => Some(Box::new(new_nested_value_validation(n)?)),
            None => None,
        },
        enum_: s.enum_.clone().unwrap_or_default(),
        all_of: nested(&s.all_of)?,
        any_of: nested(&s.any_of)?,
        one_of: nested(&s.one_of)?,
    })
}

/// `newNestedValueValidation` (convert.go:188-259).
fn new_nested_value_validation(s: &JSONSchemaProps) -> Result<NestedValueValidation, String> {
    validate_unsupported_fields(s)?;
    let mut v = NestedValueValidation {
        value_validation: new_value_validation(s)?,
        validation_extensions: new_validation_extensions(s),
        forbidden_generics: new_generics(s),
        forbidden_extensions: new_extensions(s)?,
        ..Default::default()
    };

    if let Some(items) = &s.items {
        match &**items {
            JSONSchemaPropsOrArray::Schemas(a) if !a.is_empty() => {
                return Err("OpenAPIV3Schema 'items' must be a schema, but is an array".into());
            }
            JSONSchemaPropsOrArray::Schemas(_) => {}
            JSONSchemaPropsOrArray::Schema(item) => {
                v.items = Some(Box::new(new_nested_value_validation(item)?));
            }
        }
    }
    if let Some(props) = &s.properties {
        for (k, x) in props {
            v.properties
                .insert(k.clone(), new_nested_value_validation(x)?);
        }
    }
    if let Some(ap) = &s.additional_properties {
        match &**ap {
            JSONSchemaPropsOrBool::Schema(schema) => {
                v.additional_properties = Some(Box::new(new_nested_value_validation(schema)?));
            }
            JSONSchemaPropsOrBool::Bool(true) => {
                v.additional_properties = Some(Box::default());
            }
            JSONSchemaPropsOrBool::Bool(false) => {}
        }
    }
    Ok(v)
}

/// `newExtensions` (convert.go:261-283).
fn new_extensions(s: &JSONSchemaProps) -> Result<Extensions, String> {
    let mut ret = Extensions {
        x_embedded_resource: s.x_kubernetes_embedded_resource.unwrap_or(false),
        x_int_or_string: s.x_kubernetes_int_or_string.unwrap_or(false),
        x_list_map_keys: s.x_kubernetes_list_map_keys.clone().unwrap_or_default(),
        x_list_type: s.x_kubernetes_list_type.clone(),
        x_map_type: s.x_kubernetes_map_type.clone(),
        x_preserve_unknown_fields: false,
    };
    if let Some(p) = s.x_kubernetes_preserve_unknown_fields {
        if !p {
            return Err(
                "internal error: 'x-kubernetes-preserve-unknown-fields' must be true or undefined"
                    .into(),
            );
        }
        ret.x_preserve_unknown_fields = true;
    }
    Ok(ret)
}

/// `newValidationExtensions` (convert.go:285-297).
fn new_validation_extensions(s: &JSONSchemaProps) -> ValidationExtensions {
    ValidationExtensions {
        x_validations: s.x_kubernetes_validations.clone().unwrap_or_default(),
    }
}

/// `validateUnsupportedFields` (convert.go:299-325).
fn validate_unsupported_fields(s: &JSONSchemaProps) -> Result<(), String> {
    let set = |o: &Option<String>| o.as_deref().is_some_and(|v| !v.is_empty());
    if set(&s.id) {
        return Err("OpenAPIV3Schema 'id' is not supported".into());
    }
    if set(&s.schema) {
        return Err("OpenAPIV3Schema 'schema' is not supported".into());
    }
    if set(&s.ref_path) {
        return Err("OpenAPIV3Schema '$ref' is not supported".into());
    }
    if s.pattern_properties.as_ref().is_some_and(|m| !m.is_empty()) {
        return Err("OpenAPIV3Schema 'patternProperties' is not supported".into());
    }
    if s.dependencies.as_ref().is_some_and(|m| !m.is_empty()) {
        return Err("OpenAPIV3Schema 'dependencies' is not supported".into());
    }
    if s.additional_items.is_some() {
        return Err("OpenAPIV3Schema 'additionalItems' is not supported".into());
    }
    if s.definitions.as_ref().is_some_and(|m| !m.is_empty()) {
        return Err("OpenAPIV3Schema 'definitions' is not supported".into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// validation.go
// ---------------------------------------------------------------------------

/// `schema.level` (validation.go:34-40).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Root,
    Item,
    Field,
}

/// `schema.ValidationOptions` (validation.go:42-57).
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidationOptions {
    pub allow_nested_additional_properties: bool,
    pub allow_nested_x_validations: bool,
    pub allow_validation_properties_with_additional_properties: bool,
}

fn int_or_string_any_of() -> Vec<NestedValueValidation> {
    ["integer", "string"]
        .iter()
        .map(|t| NestedValueValidation {
            forbidden_generics: Generic {
                type_: (*t).to_string(),
                ..Default::default()
            },
            ..Default::default()
        })
        .collect()
}

/// `schema.ValidateStructural` (validation.go:76-85).
pub fn validate_structural(fld_path: &Path, s: &Structural) -> Vec<Error> {
    validate_structural_with_options(fld_path, s, ValidationOptions::default())
}

/// `schema.ValidateStructuralWithOptions` (validation.go:87-99).
pub fn validate_structural_with_options(
    fld_path: &Path,
    s: &Structural,
    opts: ValidationOptions,
) -> Vec<Error> {
    let mut all_errs = Vec::new();
    all_errs.extend(validate_structural_invariants(
        Some(s),
        Level::Root,
        fld_path,
        opts,
    ));
    all_errs.extend(validate_structural_completeness(Some(s), fld_path, opts));
    // sort error messages (:93-97)
    all_errs.sort_by_key(|e| e.to_string());
    all_errs
}

/// `validateStructuralInvariants` (validation.go:102-197).
fn validate_structural_invariants(
    s: Option<&Structural>,
    lvl: Level,
    fld_path: &Path,
    opts: ValidationOptions,
) -> Vec<Error> {
    let Some(s) = s else {
        return Vec::new();
    };
    let mut all_errs = Vec::new();

    if s.generic.type_ == "array" && s.items.is_none() {
        all_errs.push(Error::required(
            &fld_path.child("items"),
            "must be specified",
        ));
    }
    all_errs.extend(validate_structural_invariants(
        s.items.as_deref(),
        Level::Item,
        &fld_path.child("items"),
        opts,
    ));

    for (k, v) in &s.properties {
        all_errs.extend(validate_structural_invariants(
            Some(v),
            Level::Field,
            &fld_path.child("properties").key(k),
            opts,
        ));
    }

    if let Some(ap) = &s.additional_properties {
        if lvl == Level::Root {
            all_errs.push(Error::forbidden(
                &fld_path.child("additionalProperties"),
                "must not be used at the root",
            ));
        }
        if let Some(st) = &ap.structural {
            all_errs.extend(validate_structural_invariants(
                Some(st),
                Level::Field,
                &fld_path.child("additionalProperties"),
                opts,
            ));
        }
    }

    // validateGeneric is a no-op upstream (:226-232).
    all_errs.extend(validate_extensions(&s.extensions, fld_path));

    // detect the two IntOrString exceptions (:140-149)
    let skip_any_of = is_int_or_string_any_of_pattern(s);
    let skip_first_all_of_any_of = is_int_or_string_all_of_pattern(s);

    all_errs.extend(validate_value_validation(
        s.value_validation.as_ref(),
        skip_any_of,
        skip_first_all_of_any_of,
        lvl,
        fld_path,
        opts,
    ));

    let check_metadata = lvl == Level::Root || s.extensions.x_embedded_resource;
    let ty = s.generic.type_.as_str();

    if s.extensions.x_embedded_resource && ty != "object" {
        if ty.is_empty() {
            all_errs.push(Error::required(
                &fld_path.child("type"),
                "must be object if x-kubernetes-embedded-resource is true",
            ));
        } else {
            all_errs.push(Error::invalid(
                &fld_path.child("type"),
                ty,
                "must be object if x-kubernetes-embedded-resource is true",
            ));
        }
    } else if ty.is_empty()
        && !s.extensions.x_int_or_string
        && !s.extensions.x_preserve_unknown_fields
    {
        let detail = match lvl {
            Level::Root => "must not be empty at the root",
            Level::Item => "must not be empty for specified array items",
            Level::Field => "must not be empty for specified object fields",
        };
        all_errs.push(Error::required(&fld_path.child("type"), detail));
    }
    if s.extensions.x_embedded_resource && s.additional_properties.is_some() {
        all_errs.push(Error::forbidden(
            &fld_path.child("additionalProperties"),
            "must not be used if x-kubernetes-embedded-resource is set",
        ));
    }

    if lvl == Level::Root && !ty.is_empty() && ty != "object" {
        all_errs.push(Error::invalid(
            &fld_path.child("type"),
            ty,
            "must be object at the root",
        ));
    }

    // restrict metadata schemas to name and generateName only (:179-190)
    if let Some(kind) = s.properties.get("kind") {
        if check_metadata && kind.generic.type_ != "string" {
            all_errs.push(Error::invalid(
                &fld_path.child("properties").key("kind").child("type"),
                kind.generic.type_.as_str(),
                "must be string",
            ));
        }
    }
    if let Some(api_version) = s.properties.get("apiVersion") {
        if check_metadata && api_version.generic.type_ != "string" {
            all_errs.push(Error::invalid(
                &fld_path.child("properties").key("apiVersion").child("type"),
                api_version.generic.type_.as_str(),
                "must be string",
            ));
        }
    }

    if let Some(metadata) = s.properties.get("metadata") {
        all_errs.extend(validate_structural_metadata_invariants(
            metadata,
            check_metadata,
            lvl,
            &fld_path.child("properties").key("metadata"),
        ));
    }

    if s.extensions.x_embedded_resource
        && !s.extensions.x_preserve_unknown_fields
        && s.properties.is_empty()
    {
        all_errs.push(Error::required(
            &fld_path.child("properties"),
            "must not be empty if x-kubernetes-embedded-resource is true without x-kubernetes-preserve-unknown-fields",
        ));
    }

    all_errs
}

/// `validateStructuralMetadataInvariants` (validation.go:199-231). Upstream
/// mutates a shallow copy; here the copy is explicit.
pub fn validate_structural_metadata_invariants(
    s: &Structural,
    check_metadata: bool,
    lvl: Level,
    fld_path: &Path,
) -> Vec<Error> {
    let mut all_errs = Vec::new();

    if check_metadata && s.generic.type_ != "object" {
        all_errs.push(Error::invalid(
            &fld_path.child("type"),
            s.generic.type_.as_str(),
            "must be object",
        ));
    }

    if lvl == Level::Root {
        let mut s = s.clone();
        let found_name = s.properties.contains_key("name");
        let found_generate_name = s.properties.contains_key("generateName");
        if found_name && found_generate_name && s.properties.len() == 2 {
            s.properties.clear();
        } else if (found_name || found_generate_name) && s.properties.len() == 1 {
            s.properties.clear();
        }
        s.generic.type_ = String::new();
        // "this is checked in API validation (and also tested)"
        s.generic.default = None;
        if s.value_validation.is_none() {
            s.value_validation = Some(ValueValidation::default());
        }
        let want = Structural {
            value_validation: Some(ValueValidation::default()),
            ..Default::default()
        };
        if s != want {
            // upstream TODO: really a field.Invalid, but no JSON serialization here
            all_errs.push(Error::forbidden(
                fld_path,
                "must not specify anything other than name and generateName, but metadata is implicitly specified",
            ));
        }
    }

    all_errs
}

/// `isIntOrStringAnyOfPattern` (validation.go:233-238).
fn is_int_or_string_any_of_pattern(s: &Structural) -> bool {
    s.value_validation
        .as_ref()
        .is_some_and(|vv| vv.any_of.len() == 2 && vv.any_of == int_or_string_any_of())
}

/// `isIntOrStringAllOfPattern` (validation.go:240-245).
fn is_int_or_string_all_of_pattern(s: &Structural) -> bool {
    s.value_validation.as_ref().is_some_and(|vv| {
        vv.all_of.first().is_some_and(|f| {
            f.value_validation.any_of.len() == 2
                && f.value_validation.any_of == int_or_string_any_of()
        })
    })
}

/// `validateExtensions` (validation.go:258-270).
fn validate_extensions(x: &Extensions, fld_path: &Path) -> Vec<Error> {
    let mut all_errs = Vec::new();
    if x.x_int_or_string && x.x_preserve_unknown_fields {
        all_errs.push(Error::invalid(
            &fld_path.child("x-kubernetes-preserve-unknown-fields"),
            x.x_preserve_unknown_fields,
            "must be false if x-kubernetes-int-or-string is true",
        ));
    }
    if x.x_int_or_string && x.x_embedded_resource {
        all_errs.push(Error::invalid(
            &fld_path.child("x-kubernetes-embedded-resource"),
            x.x_embedded_resource,
            "must be false if x-kubernetes-int-or-string is true",
        ));
    }
    all_errs
}

/// `validateValueValidation` (validation.go:272-325).
fn validate_value_validation(
    v: Option<&ValueValidation>,
    skip_any_of: bool,
    skip_first_all_of_any_of: bool,
    lvl: Level,
    fld_path: &Path,
    opts: ValidationOptions,
) -> Vec<Error> {
    let Some(v) = v else {
        return Vec::new();
    };
    let mut all_errs = Vec::new();

    // x-kubernetes-validations stays forbidden under every quantifier but
    // allOf (:284-289)
    let opts_cel_disabled = ValidationOptions {
        allow_nested_x_validations: false,
        ..opts
    };

    if !skip_any_of {
        for (i, a) in v.any_of.iter().enumerate() {
            all_errs.extend(validate_nested_value_validation(
                Some(a),
                false,
                false,
                lvl,
                &fld_path.child("anyOf").index(i),
                opts_cel_disabled,
            ));
        }
    }

    for (i, a) in v.all_of.iter().enumerate() {
        let skip_any_of = skip_first_all_of_any_of && i == 0;
        all_errs.extend(validate_nested_value_validation(
            Some(a),
            skip_any_of,
            false,
            lvl,
            &fld_path.child("allOf").index(i),
            opts,
        ));
    }

    for (i, a) in v.one_of.iter().enumerate() {
        all_errs.extend(validate_nested_value_validation(
            Some(a),
            false,
            false,
            lvl,
            &fld_path.child("oneOf").index(i),
            opts_cel_disabled,
        ));
    }

    all_errs.extend(validate_nested_value_validation(
        v.not.as_deref(),
        false,
        false,
        lvl,
        &fld_path.child("not"),
        opts_cel_disabled,
    ));

    if !v.pattern.is_empty() {
        if let Err(e) = regex::Regex::new(&v.pattern) {
            all_errs.push(Error::invalid(
                &fld_path.child("pattern"),
                v.pattern.as_str(),
                format!("must be a valid regular expression, but isn't: {e}"),
            ));
        }
    }

    all_errs
}

/// `validateNestedValueValidation` (validation.go:327-372).
pub fn validate_nested_value_validation(
    v: Option<&NestedValueValidation>,
    skip_any_of: bool,
    skip_all_of_any_of: bool,
    lvl: Level,
    fld_path: &Path,
    opts: ValidationOptions,
) -> Vec<Error> {
    let Some(v) = v else {
        return Vec::new();
    };
    let mut all_errs = Vec::new();

    all_errs.extend(validate_value_validation(
        Some(&v.value_validation),
        skip_any_of,
        skip_all_of_any_of,
        lvl,
        fld_path,
        opts,
    ));
    all_errs.extend(validate_nested_value_validation(
        v.items.as_deref(),
        false,
        false,
        lvl,
        &fld_path.child("items"),
        opts,
    ));

    for (k, fld) in &v.properties {
        all_errs.extend(validate_nested_value_validation(
            Some(fld),
            false,
            false,
            Level::Field,
            &fld_path.child("properties").key(k),
            opts,
        ));
    }

    let g = &v.forbidden_generics;
    if !g.type_.is_empty() {
        all_errs.push(Error::forbidden(
            &fld_path.child("type"),
            "must be empty to be structural",
        ));
    }
    if v.additional_properties.is_some() && !opts.allow_nested_additional_properties {
        all_errs.push(Error::forbidden(
            &fld_path.child("additionalProperties"),
            "must be undefined to be structural",
        ));
    } else {
        all_errs.extend(validate_nested_value_validation(
            v.additional_properties.as_deref(),
            false,
            false,
            lvl,
            &fld_path.child("additionalProperties"),
            opts,
        ));
    }
    if g.default.is_some() {
        all_errs.push(Error::forbidden(
            &fld_path.child("default"),
            "must be undefined to be structural",
        ));
    }
    if !g.title.is_empty() {
        all_errs.push(Error::forbidden(
            &fld_path.child("title"),
            "must be empty to be structural",
        ));
    }
    if !g.description.is_empty() {
        all_errs.push(Error::forbidden(
            &fld_path.child("description"),
            "must be empty to be structural",
        ));
    }
    if g.nullable {
        all_errs.push(Error::forbidden(
            &fld_path.child("nullable"),
            "must be false to be structural",
        ));
    }

    let x = &v.forbidden_extensions;
    if x.x_preserve_unknown_fields {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-preserve-unknown-fields"),
            "must be false to be structural",
        ));
    }
    if x.x_embedded_resource {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-embedded-resource"),
            "must be false to be structural",
        ));
    }
    if x.x_int_or_string {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-int-or-string"),
            "must be false to be structural",
        ));
    }
    if !x.x_list_map_keys.is_empty() {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-list-map-keys"),
            "must be empty to be structural",
        ));
    }
    if x.x_list_type.is_some() {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-list-type"),
            "must be undefined to be structural",
        ));
    }
    if x.x_map_type.is_some() {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-map-type"),
            "must be undefined to be structural",
        ));
    }
    if !v.validation_extensions.x_validations.is_empty() && !opts.allow_nested_x_validations {
        all_errs.push(Error::forbidden(
            &fld_path.child("x-kubernetes-validations"),
            "must be empty to be structural",
        ));
    }

    // forbid reasoning about metadata because it can lead to metadata
    // restriction we don't want
    if v.properties.contains_key("metadata") {
        all_errs.push(Error::forbidden(
            &fld_path.child("properties").key("metadata"),
            "must not be specified in a nested context",
        ));
    }

    all_errs
}

// ---------------------------------------------------------------------------
// complete.go
// ---------------------------------------------------------------------------

/// `validateStructuralCompleteness` (complete.go:30-37).
pub fn validate_structural_completeness(
    s: Option<&Structural>,
    fld_path: &Path,
    opts: ValidationOptions,
) -> Vec<Error> {
    match s {
        None => Vec::new(),
        Some(s) => validate_value_validation_completeness(
            s.value_validation.as_ref(),
            Some(s),
            fld_path,
            fld_path,
            opts,
        ),
    }
}

/// `validateValueValidationCompleteness` (complete.go:39-62).
fn validate_value_validation_completeness(
    v: Option<&ValueValidation>,
    s: Option<&Structural>,
    s_path: &Path,
    v_path: &Path,
    opts: ValidationOptions,
) -> Vec<Error> {
    let Some(v) = v else {
        return Vec::new();
    };
    let Some(s) = s else {
        return vec![Error::required(
            s_path,
            format!("because it is defined in {v_path}"),
        )];
    };
    let mut all_errs = Vec::new();

    all_errs.extend(validate_nested_value_validation_completeness(
        v.not.as_deref(),
        Some(s),
        s_path,
        &v_path.child("not"),
        opts,
    ));
    for (name, list) in [
        ("allOf", &v.all_of),
        ("anyOf", &v.any_of),
        ("oneOf", &v.one_of),
    ] {
        for (i, n) in list.iter().enumerate() {
            all_errs.extend(validate_nested_value_validation_completeness(
                Some(n),
                Some(s),
                s_path,
                &v_path.child(name).index(i),
                opts,
            ));
        }
    }
    all_errs
}

/// `validateNestedValueValidationCompleteness` (complete.go:64-106).
fn validate_nested_value_validation_completeness(
    v: Option<&NestedValueValidation>,
    s: Option<&Structural>,
    s_path: &Path,
    v_path: &Path,
    opts: ValidationOptions,
) -> Vec<Error> {
    let Some(v) = v else {
        return Vec::new();
    };
    let Some(s) = s else {
        return vec![Error::required(
            s_path,
            format!("because it is defined in {v_path}"),
        )];
    };
    let mut all_errs = Vec::new();

    all_errs.extend(validate_value_validation_completeness(
        Some(&v.value_validation),
        Some(s),
        s_path,
        v_path,
        opts,
    ));
    all_errs.extend(validate_nested_value_validation_completeness(
        v.items.as_deref(),
        s.items.as_deref(),
        &s_path.child("items"),
        &v_path.child("items"),
        opts,
    ));

    let s_additional_properties_schema = s
        .additional_properties
        .as_ref()
        .and_then(|ap| ap.structural.as_ref());

    for (k, v_fld) in &v.properties {
        match s.properties.get(k) {
            None => match s_additional_properties_schema {
                Some(ap) if opts.allow_validation_properties_with_additional_properties => {
                    // NOTE (upstream): `additionalProperties: true` cannot be
                    // combined with specific property validations.
                    all_errs.extend(validate_nested_value_validation_completeness(
                        Some(v_fld),
                        Some(ap),
                        &s_path.child("additionalProperties"),
                        &v_path.child("properties").key(k),
                        opts,
                    ));
                }
                _ => all_errs.push(Error::required(
                    &s_path.child("properties").key(k),
                    format!(
                        "because it is defined in {}",
                        v_path.child("properties").key(k)
                    ),
                )),
            },
            Some(s_fld) => all_errs.extend(validate_nested_value_validation_completeness(
                Some(v_fld),
                Some(s_fld),
                &s_path.child("properties").key(k),
                &v_path.child("properties").key(k),
                opts,
            )),
        }
    }

    if v.additional_properties.is_some() && opts.allow_nested_additional_properties {
        all_errs.extend(validate_nested_value_validation_completeness(
            v.additional_properties.as_deref(),
            s_additional_properties_schema,
            &s_path.child("additionalProperties"),
            &v_path.child("additionalProperties"),
            opts,
        ));
    }

    all_errs
}

#[cfg(test)]
mod tests;
