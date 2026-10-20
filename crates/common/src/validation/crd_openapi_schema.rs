//! The schema half of upstream's `ValidateCustomResourceDefinitionOpenAPISchema`
//! walk and the `specStandardValidatorV3` it carries.
//!
//! Ported from
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation/validation.go`:
//! `ValidateCustomResourceDefinitionOpenAPISchema` (`:1031-1322`),
//! `specStandardValidatorV3.validate` (`:1462-1523`), `validateMapListKeysMapSet`
//! (`:1373-1416`) and the root checks of `validateCustomResourceDefinitionValidation`
//! (`:918-946`).
//!
//! Upstream's walk returns an `OpenAPISchemaErrorList` with `SchemaErrors` and
//! `CELErrors` kept apart. Only `SchemaErrors` are produced here; the `CELErrors`
//! half (the `x-kubernetes-validations` rule/message/fieldPath/reason field
//! checks, compilation and cost) lives in the api-server crate
//! (`handlers::cel_validation::validate_crd_schema_rules`), because it needs the
//! CEL environment. `uncorrelatableOldSelfValidationPath` is likewise only read
//! by that CEL half, so it is not tracked here.

use crate::resources::{
    JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool, JSONSchemaPropsOrStringArray,
};
use crate::validation::crd::schema_has;
use crate::validation::field::{BadValue, Error, ErrorList, Path};

/// `openapiV3Types` (`validation.go:46`), in `sets.List()` (sorted) order.
const OPENAPI_V3_TYPES: [&str; 6] = ["array", "boolean", "integer", "number", "object", "string"];

/// `allowedFieldsAtRootSchema` (`validation.go:1585`).
const ALLOWED_FIELDS_AT_ROOT_SCHEMA: [&str; 22] = [
    "Description",
    "Type",
    "Format",
    "Title",
    "Maximum",
    "ExclusiveMaximum",
    "Minimum",
    "ExclusiveMinimum",
    "MaxLength",
    "MinLength",
    "Pattern",
    "MaxItems",
    "MinItems",
    "UniqueItems",
    "MultipleOf",
    "Required",
    "Items",
    "Properties",
    "ExternalDocs",
    "Example",
    "XPreserveUnknownFields",
    "XValidations",
];

/// `metaFields` (`validation.go:1009`).
const META_FIELDS: [&str; 3] = ["metadata", "kind", "apiVersion"];

/// The `validationOptions` members the schema walk reads (`validation.go:122-150`):
/// `allowDefaults`, `requireValidPropertyType`, `requireAtomicSetType` and
/// `requireMapListKeysMapSetValidation`.
#[derive(Debug, Clone, Copy)]
pub struct OpenApiSchemaOptions {
    pub allow_defaults: bool,
    pub require_valid_property_type: bool,
    pub require_atomic_set_type: bool,
    pub require_map_list_keys_map_set_validation: bool,
}

impl Default for OpenApiSchemaOptions {
    /// `ValidateCustomResourceDefinition` (`validation.go:75-90`): create
    /// tightens everything.
    fn default() -> Self {
        Self {
            allow_defaults: true,
            require_valid_property_type: true,
            require_atomic_set_type: true,
            require_map_list_keys_map_set_validation: true,
        }
    }
}

impl OpenApiSchemaOptions {
    /// `ValidateCustomResourceDefinitionUpdate` (`validation.go:230-248`): each
    /// requirement is dropped when the persisted spec already violates it, so
    /// existing data is not rejected.
    pub fn for_update(old: &crate::resources::CustomResourceDefinitionSpec) -> Self {
        Self {
            allow_defaults: true,
            // `requireValidPropertyType` (`:1848-1855`).
            require_valid_property_type: !spec_has_invalid_types(old),
            // `requireAtomicSetType` (`:1815-1818`).
            require_atomic_set_type: !has_schema_with(old, &has_non_atomic_set_type),
            // `requireMapListKeysMapSetValidation` (`:1837-1839`).
            require_map_list_keys_map_set_validation: !has_schema_with(
                old,
                &has_invalid_map_list_keys_map_set,
            ),
        }
    }
}

fn has_schema_with(
    spec: &crate::resources::CustomResourceDefinitionSpec,
    pred: &dyn Fn(&JSONSchemaProps) -> bool,
) -> bool {
    spec.versions.iter().any(|v| {
        v.schema
            .as_ref()
            .and_then(|s| s.open_apiv3_schema.as_ref())
            .is_some_and(|s| schema_has(s, pred))
    })
}

fn type_of(s: &JSONSchemaProps) -> &str {
    s.type_.as_deref().unwrap_or("")
}

/// `specHasInvalidTypes` / `SchemaHasInvalidTypes` (`validation.go:1906-1923`).
fn spec_has_invalid_types(spec: &crate::resources::CustomResourceDefinitionSpec) -> bool {
    has_schema_with(spec, &|s| {
        !type_of(s).is_empty() && !OPENAPI_V3_TYPES.contains(&type_of(s))
    })
}

/// `hasNonAtomicSetType` (`validation.go:1820-1835`).
fn has_non_atomic_set_type(s: &JSONSchemaProps) -> bool {
    if s.x_kubernetes_list_type.as_deref() != Some("set") {
        return false;
    }
    // "we don't support schema.Items.JSONSchemas"
    let Some(JSONSchemaPropsOrArray::Schema(is)) = s.items.as_deref() else {
        return false;
    };
    match type_of(is) {
        // atomic is the implicit default if unset, hence != atomic is wrong
        "array" => is
            .x_kubernetes_list_type
            .as_deref()
            .is_some_and(|t| t != "atomic"),
        // granular is the implicit default if unset, hence nil and != atomic are wrong
        "object" => is.x_kubernetes_map_type.as_deref() != Some("atomic"),
        _ => false,
    }
}

/// `hasInvalidMapListKeysMapSet` (`validation.go:1841-1846`).
fn has_invalid_map_list_keys_map_set(s: &JSONSchemaProps) -> bool {
    !validate_map_list_keys_map_set(s, &Path::new("")).is_empty()
}

/// `specStandardValidatorV3` (`validation.go:1420-1427`), minus
/// `uncorrelatableOldSelfValidationPath` (CEL only).
#[derive(Debug, Clone)]
struct SpecStandardValidatorV3 {
    allow_defaults: bool,
    disallow_defaults_reason: String,
    is_inside_resource_meta: bool,
    require_valid_property_type: bool,
}

impl SpecStandardValidatorV3 {
    /// `withForbiddenDefaults` (`:1429`).
    fn with_forbidden_defaults(&self, reason: String) -> Self {
        Self {
            disallow_defaults_reason: reason,
            allow_defaults: false,
            ..self.clone()
        }
    }

    /// `withInsideResourceMeta` (`:1436`).
    fn with_inside_resource_meta(&self) -> Self {
        Self {
            is_inside_resource_meta: true,
            ..self.clone()
        }
    }

    /// `validate` (`:1462-1523`): validates against OpenAPI Schema v3.
    fn validate(&self, schema: &JSONSchemaProps, fld_path: &Path) -> ErrorList {
        let mut errs = ErrorList::new();

        if self.require_valid_property_type
            && !type_of(schema).is_empty()
            && !OPENAPI_V3_TYPES.contains(&type_of(schema))
        {
            errs.push(Error::not_supported(
                &fld_path.child("type"),
                type_of(schema),
                &OPENAPI_V3_TYPES,
            ));
        }

        if schema.default.is_some() && !self.allow_defaults {
            let mut detail = "must not be set".to_string();
            if !self.disallow_defaults_reason.is_empty() {
                detail.push(' ');
                detail.push_str(&self.disallow_defaults_reason);
            }
            errs.push(Error::forbidden(&fld_path.child("default"), detail));
        }

        if schema.id.as_deref().is_some_and(|id| !id.is_empty()) {
            errs.push(Error::forbidden(
                &fld_path.child("id"),
                "id is not supported",
            ));
        }

        if schema.additional_items.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("additionalItems"),
                "additionalItems is not supported",
            ));
        }

        if schema
            .pattern_properties
            .as_ref()
            .is_some_and(|m| !m.is_empty())
        {
            errs.push(Error::forbidden(
                &fld_path.child("patternProperties"),
                "patternProperties is not supported",
            ));
        }

        if schema.definitions.as_ref().is_some_and(|m| !m.is_empty()) {
            errs.push(Error::forbidden(
                &fld_path.child("definitions"),
                "definitions is not supported",
            ));
        }

        if schema.dependencies.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("dependencies"),
                "dependencies is not supported",
            ));
        }

        if schema.ref_path.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("$ref"),
                "$ref is not supported",
            ));
        }

        if type_of(schema) == "null" {
            errs.push(Error::forbidden(
                &fld_path.child("type"),
                "type cannot be set to null, use nullable as an alternative",
            ));
        }

        if let Some(JSONSchemaPropsOrArray::Schemas(l)) = schema.items.as_deref() {
            if !l.is_empty() {
                errs.push(Error::forbidden(
                    &fld_path.child("items"),
                    "items must be a schema object and not an array",
                ));
            }
        }

        if self.is_inside_resource_meta && schema.x_kubernetes_embedded_resource == Some(true) {
            errs.push(Error::forbidden(
                &fld_path.child("x-kubernetes-embedded-resource"),
                "must not be used inside of resource meta",
            ));
        }

        errs
    }
}

/// The root checks of `validateCustomResourceDefinitionValidation`
/// (`validation.go:918-946`): the status subresource root restrictions and
/// `nullable` at the root. `fld_path` is the path
/// of the enclosing `schema` (`validation` upstream) field.
pub fn validate_root_restrictions(
    schema: &JSONSchemaProps,
    status_subresource_enabled: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let schema_path = fld_path.child("openAPIV3Schema");

    // if the status subresource is enabled, only certain fields are allowed
    // inside the root schema. these fields are chosen such that, if status is
    // extracted as properties["status"], it's validation is not lost.
    if status_subresource_enabled {
        for (name, non_zero) in root_fields(schema) {
            // skip zero values
            if !non_zero {
                continue;
            }
            // only "object" type is valid at root of the schema since validation
            // schema for status is extracted as properties["status"]
            if name == "Type" {
                if type_of(schema) != "object" {
                    errs.push(Error::invalid(
                        &fld_path.child("openAPIV3Schema.type"),
                        type_of(schema),
                        r#"only "object" is allowed as the type at the root of the schema if the status subresource is enabled"#,
                    ));
                    break;
                }
                continue;
            }
            if !ALLOWED_FIELDS_AT_ROOT_SCHEMA.contains(&name) {
                errs.push(Error::invalid(
                    &schema_path,
                    BadValue::marshal(schema),
                    format!(
                        "only [{}] fields are allowed at the root of the schema if the status subresource is enabled",
                        ALLOWED_FIELDS_AT_ROOT_SCHEMA.join(" ")
                    ),
                ));
                break;
            }
        }
    }

    if schema.nullable == Some(true) {
        errs.push(Error::forbidden(
            &fld_path.child("openAPIV3Schema.nullable"),
            "nullable cannot be true at the root",
        ));
    }

    errs
}

/// `ValidateCustomResourceDefinitionOpenAPISchema(schema, fldPath.Child("openAPIV3Schema"),
/// openAPIV3Schema, true, &opts, celContext)` (`validation.go:973`), schema errors only.
pub fn validate_openapi_schema_walk(
    schema: &JSONSchemaProps,
    fld_path: &Path,
    opts: &OpenApiSchemaOptions,
) -> ErrorList {
    let ssv = SpecStandardValidatorV3 {
        allow_defaults: opts.allow_defaults,
        disallow_defaults_reason: String::new(),
        is_inside_resource_meta: false,
        require_valid_property_type: opts.require_valid_property_type,
    };
    validate_custom_resource_definition_openapi_schema(
        schema,
        &fld_path.child("openAPIV3Schema"),
        &ssv,
        true,
        opts,
    )
}

/// The fields of `JSONSchemaProps` in Go declaration order
/// (`types_jsonschema.go:40-115`) with whether each is non-zero, which is what
/// upstream's `reflect` loop (`validation.go:922-942`) inspects.
fn root_fields(s: &JSONSchemaProps) -> Vec<(&'static str, bool)> {
    let str_set = |v: &Option<String>| v.as_deref().is_some_and(|x| !x.is_empty());
    vec![
        ("ID", str_set(&s.id)),
        ("Schema", str_set(&s.schema)),
        ("Ref", s.ref_path.is_some()),
        ("Description", str_set(&s.description)),
        ("Type", str_set(&s.type_)),
        ("Nullable", s.nullable == Some(true)),
        ("Format", str_set(&s.format)),
        ("Title", str_set(&s.title)),
        ("Default", s.default.is_some()),
        ("Maximum", s.maximum.is_some()),
        ("ExclusiveMaximum", s.exclusive_maximum == Some(true)),
        ("Minimum", s.minimum.is_some()),
        ("ExclusiveMinimum", s.exclusive_minimum == Some(true)),
        ("MaxLength", s.max_length.is_some()),
        ("MinLength", s.min_length.is_some()),
        ("Pattern", str_set(&s.pattern)),
        ("MaxItems", s.max_items.is_some()),
        ("MinItems", s.min_items.is_some()),
        ("UniqueItems", s.unique_items == Some(true)),
        ("MultipleOf", s.multiple_of.is_some()),
        ("Enum", s.enum_.is_some()),
        ("MaxProperties", s.max_properties.is_some()),
        ("MinProperties", s.min_properties.is_some()),
        ("Required", s.required.is_some()),
        ("Items", s.items.is_some()),
        ("AllOf", s.all_of.is_some()),
        ("OneOf", s.one_of.is_some()),
        ("AnyOf", s.any_of.is_some()),
        ("Not", s.not.is_some()),
        ("Properties", s.properties.is_some()),
        ("AdditionalProperties", s.additional_properties.is_some()),
        ("PatternProperties", s.pattern_properties.is_some()),
        ("Dependencies", s.dependencies.is_some()),
        ("AdditionalItems", s.additional_items.is_some()),
        ("Definitions", s.definitions.is_some()),
        ("ExternalDocs", s.external_docs.is_some()),
        ("Example", s.example.is_some()),
        (
            "XPreserveUnknownFields",
            s.x_kubernetes_preserve_unknown_fields.is_some(),
        ),
        (
            "XEmbeddedResource",
            s.x_kubernetes_embedded_resource == Some(true),
        ),
        ("XIntOrString", s.x_kubernetes_int_or_string == Some(true)),
        ("XListMapKeys", s.x_kubernetes_list_map_keys.is_some()),
        ("XListType", str_set(&s.x_kubernetes_list_type)),
        ("XMapType", str_set(&s.x_kubernetes_map_type)),
        ("XValidations", s.x_kubernetes_validations.is_some()),
    ]
}

/// `ValidateCustomResourceDefinitionOpenAPISchema` (`validation.go:1031`),
/// `SchemaErrors` only.
fn validate_custom_resource_definition_openapi_schema(
    schema: &JSONSchemaProps,
    fld_path: &Path,
    ssv: &SpecStandardValidatorV3,
    is_root: bool,
    opts: &OpenApiSchemaOptions,
) -> ErrorList {
    let mut errs = ssv.validate(schema, fld_path);

    if schema.unique_items == Some(true) {
        errs.push(Error::forbidden(
            &fld_path.child("uniqueItems"),
            "uniqueItems cannot be set to true since the runtime complexity becomes quadratic",
        ));
    }

    // additionalProperties and properties are mutual exclusive because otherwise
    // they contradict Kubernetes' API convention to ignore unknown fields.
    //
    // In other words:
    // - properties are for structs,
    // - additionalProperties are for map[string]interface{}
    if let Some(ap) = schema.additional_properties.as_deref() {
        if schema.properties.as_ref().is_some_and(|m| !m.is_empty())
            && !matches!(ap, JSONSchemaPropsOrBool::Bool(true))
        {
            errs.push(Error::forbidden(
                &fld_path.child("additionalProperties"),
                "additionalProperties and properties are mutual exclusive",
            ));
        }
        // Note: we forbid additionalProperties at resource root, both embedded
        // and top-level. But further inside, additionalProperties is possible,
        // e.g. for labels or annotations.
        let sub_ssv = if ssv.is_inside_resource_meta {
            // we have to forbid defaults inside additionalProperties because
            // pruning without actual value is ambiguous
            ssv.with_forbidden_defaults(
                "inside additionalProperties applying to object metadata".to_string(),
            )
        } else {
            ssv.clone()
        };
        if let JSONSchemaPropsOrBool::Schema(s) = ap {
            errs.extend(validate_custom_resource_definition_openapi_schema(
                s,
                &fld_path.child("additionalProperties"),
                &sub_ssv,
                false,
                opts,
            ));
        }
    }

    if let Some(props) = &schema.properties {
        // Go ranges a map in random order; sort for stable output.
        let mut names: Vec<&String> = props.keys().collect();
        names.sort();
        for property in names {
            let mut sub_ssv = ssv.clone();
            if (is_root || schema.x_kubernetes_embedded_resource == Some(true))
                && META_FIELDS.contains(&property.as_str())
            {
                // we recurse into the schema that applies to ObjectMeta.
                sub_ssv = sub_ssv.with_inside_resource_meta();
                if is_root {
                    sub_ssv = sub_ssv.with_forbidden_defaults(format!("in top-level {property}"));
                }
            }
            errs.extend(validate_custom_resource_definition_openapi_schema(
                &props[property],
                &fld_path.child("properties").key(property.clone()),
                &sub_ssv,
                false,
                opts,
            ));
        }
    }

    if let Some(not) = schema.not.as_deref() {
        errs.extend(validate_custom_resource_definition_openapi_schema(
            not,
            &fld_path.child("not"),
            ssv,
            false,
            opts,
        ));
    }

    for (name, list) in [
        ("allOf", &schema.all_of),
        ("oneOf", &schema.one_of),
        ("anyOf", &schema.any_of),
    ] {
        for (i, s) in list.iter().flatten().enumerate() {
            errs.extend(validate_custom_resource_definition_openapi_schema(
                s,
                &fld_path.child(name).index(i),
                ssv,
                false,
                opts,
            ));
        }
    }

    if let Some(defs) = &schema.definitions {
        let mut names: Vec<&String> = defs.keys().collect();
        names.sort();
        for definition in names {
            errs.extend(validate_custom_resource_definition_openapi_schema(
                &defs[definition],
                &fld_path.child("definitions").key(definition.clone()),
                ssv,
                false,
                opts,
            ));
        }
    }

    if let Some(items) = schema.items.as_deref() {
        match items {
            JSONSchemaPropsOrArray::Schema(s) => {
                errs.extend(validate_custom_resource_definition_openapi_schema(
                    s,
                    &fld_path.child("items"),
                    ssv,
                    false,
                    opts,
                ));
            }
            JSONSchemaPropsOrArray::Schemas(l) => {
                for (i, s) in l.iter().enumerate() {
                    errs.extend(validate_custom_resource_definition_openapi_schema(
                        s,
                        &fld_path.child("items").index(i),
                        ssv,
                        false,
                        opts,
                    ));
                }
            }
        }
    }

    if let Some(deps) = &schema.dependencies {
        let mut names: Vec<&String> = deps.keys().collect();
        names.sort();
        for dependency in names {
            if let JSONSchemaPropsOrStringArray::Schema(s) = &deps[dependency] {
                errs.extend(validate_custom_resource_definition_openapi_schema(
                    s,
                    &fld_path.child("dependencies").key(dependency.clone()),
                    ssv,
                    false,
                    opts,
                ));
            }
        }
    }

    if schema.x_kubernetes_preserve_unknown_fields == Some(false) {
        errs.push(Error::invalid(
            &fld_path.child("x-kubernetes-preserve-unknown-fields"),
            false,
            "must be true or undefined",
        ));
    }

    let ty = type_of(schema);
    let map_type = schema
        .x_kubernetes_map_type
        .as_deref()
        .filter(|t| !t.is_empty());
    let list_type = schema
        .x_kubernetes_list_type
        .as_deref()
        .filter(|t| !t.is_empty());

    if map_type.is_some() && ty != "object" {
        if ty.is_empty() {
            errs.push(Error::required(
                &fld_path.child("type"),
                "must be object if x-kubernetes-map-type is specified",
            ));
        } else {
            errs.push(Error::invalid(
                &fld_path.child("type"),
                ty,
                "must be object if x-kubernetes-map-type is specified",
            ));
        }
    }

    if let Some(mt) = map_type {
        if mt != "atomic" && mt != "granular" {
            errs.push(Error::not_supported(
                &fld_path.child("x-kubernetes-map-type"),
                mt,
                &["atomic", "granular"],
            ));
        }
    }

    let single_items = match schema.items.as_deref() {
        Some(JSONSchemaPropsOrArray::Schema(s)) => Some(s),
        _ => None,
    };

    if list_type.is_some() && ty != "array" {
        if ty.is_empty() {
            errs.push(Error::required(
                &fld_path.child("type"),
                "must be array if x-kubernetes-list-type is specified",
            ));
        } else {
            errs.push(Error::invalid(
                &fld_path.child("type"),
                ty,
                "must be array if x-kubernetes-list-type is specified",
            ));
        }
    } else if opts.require_atomic_set_type && list_type == Some("set") {
        // by structural schema items are present
        if let Some(is) = single_items {
            match type_of(is) {
                "array" => {
                    // atomic is the implicit default behaviour if unset, hence != atomic is wrong
                    if let Some(t) = is.x_kubernetes_list_type.as_deref() {
                        if t != "atomic" {
                            errs.push(Error::invalid(
                                &fld_path.child("items").child("x-kubernetes-list-type"),
                                BadValue::Json(serde_json::Value::String(t.to_string())),
                                "must be atomic as item of a list with x-kubernetes-list-type=set",
                            ));
                        }
                    }
                }
                // granular is the implicit default behaviour if unset, hence nil and != atomic are wrong
                "object" if is.x_kubernetes_map_type.as_deref() != Some("atomic") => {
                    // upstream passes `is.XListType` as the bad value here
                    // (`validation.go:1209`), faithfully reproduced.
                    let bad = match is.x_kubernetes_list_type.as_deref() {
                        Some(t) => serde_json::Value::String(t.to_string()),
                        None => serde_json::Value::Null,
                    };
                    errs.push(Error::invalid(
                        &fld_path.child("items").child("x-kubernetes-map-type"),
                        BadValue::Json(bad),
                        "must be atomic as item of a list with x-kubernetes-list-type=set",
                    ));
                }
                _ => {}
            }
        }
    }

    if let Some(lt) = list_type {
        if lt != "atomic" && lt != "set" && lt != "map" {
            errs.push(Error::not_supported(
                &fld_path.child("x-kubernetes-list-type"),
                lt,
                &["atomic", "set", "map"],
            ));
        }
    }

    let map_keys: &[String] = schema.x_kubernetes_list_map_keys.as_deref().unwrap_or(&[]);
    if !map_keys.is_empty() {
        match list_type {
            None => errs.push(Error::required(
                &fld_path.child("x-kubernetes-list-type"),
                "must be map if x-kubernetes-list-map-keys is non-empty",
            )),
            Some(lt) if lt != "map" => errs.push(Error::invalid(
                &fld_path.child("x-kubernetes-list-type"),
                lt,
                "must be map if x-kubernetes-list-map-keys is non-empty",
            )),
            Some(_) => {}
        }
    }

    if list_type == Some("map") {
        if map_keys.is_empty() {
            errs.push(Error::required(
                &fld_path.child("x-kubernetes-list-map-keys"),
                "must not be empty if x-kubernetes-list-type is map",
            ));
        }

        match schema.items.as_deref() {
            None => errs.push(Error::required(
                &fld_path.child("items"),
                "must have a schema if x-kubernetes-list-type is map",
            )),
            Some(items @ JSONSchemaPropsOrArray::Schemas(_)) => errs.push(Error::invalid(
                &fld_path.child("items"),
                BadValue::marshal(items),
                "must only have a single schema if x-kubernetes-list-type is map",
            )),
            Some(JSONSchemaPropsOrArray::Schema(is)) => {
                if type_of(is) != "object" {
                    errs.push(Error::invalid(
                        &fld_path.child("items").child("type"),
                        type_of(is),
                        "must be object if parent array's x-kubernetes-list-type is map",
                    ));
                } else {
                    let mut keys: std::collections::HashSet<&str> = Default::default();
                    for k in map_keys {
                        match is.properties.as_ref().and_then(|p| p.get(k)) {
                            Some(s) => {
                                if matches!(type_of(s), "array" | "object") {
                                    // upstream reports the parent items' type here
                                    // (`validation.go:1300`), faithfully reproduced.
                                    errs.push(Error::invalid(
                                        &fld_path
                                            .child("items")
                                            .child("properties")
                                            .key(k.clone())
                                            .child("type"),
                                        type_of(is),
                                        "must be a scalar type if parent array's x-kubernetes-list-type is map",
                                    ));
                                }
                            }
                            None => errs.push(Error::invalid(
                                &fld_path.child("x-kubernetes-list-map-keys"),
                                map_keys.to_vec(),
                                "entries must all be names of item properties",
                            )),
                        }
                        if !keys.insert(k.as_str()) {
                            errs.push(Error::invalid(
                                &fld_path.child("x-kubernetes-list-map-keys"),
                                map_keys.to_vec(),
                                "must not contain duplicate entries",
                            ));
                        }
                    }
                }
            }
        }
    }

    if opts.require_map_list_keys_map_set_validation {
        errs.extend(validate_map_list_keys_map_set(schema, fld_path));
    }

    // `len(schema.XValidations) > 0` (`:1306-1389`): the per-rule field checks and
    // CEL compilation are the `CELErrors` half, ported in the api-server crate.
    errs
}

/// `validateMapListKeysMapSet` (`validation.go:1373-1416`).
pub fn validate_map_list_keys_map_set(schema: &JSONSchemaProps, fld_path: &Path) -> ErrorList {
    let mut errs = ErrorList::new();

    let Some(JSONSchemaPropsOrArray::Schema(is)) = schema.items.as_deref() else {
        return errs;
    };
    let Some(list_type) = schema.x_kubernetes_list_type.as_deref() else {
        return errs;
    };
    if list_type != "set" && list_type != "map" {
        return errs;
    }

    // set and map list items cannot be nullable
    if is.nullable == Some(true) {
        errs.push(Error::forbidden(
            &fld_path.child("items").child("nullable"),
            format!("cannot be nullable when x-kubernetes-list-type is {list_type}"),
        ));
    }

    if list_type == "map" {
        // ensure all map keys are required or have a default
        let required: std::collections::HashSet<&str> =
            is.required.iter().flatten().map(String::as_str).collect();
        for k in schema.x_kubernetes_list_map_keys.iter().flatten() {
            let Some(obj) = is.properties.as_ref().and_then(|p| p.get(k)) else {
                // we validate that all XListMapKeys are existing properties in
                // ValidateCustomResourceDefinitionOpenAPISchema, so skipping here is ok
                continue;
            };
            if !required.contains(k.as_str()) && obj.default.is_none() {
                errs.push(Error::required(
                    &fld_path
                        .child("items")
                        .child("properties")
                        .key(k.clone())
                        .child("default"),
                    "this property is in x-kubernetes-list-map-keys, so it must have a default or be a required property",
                ));
            }
            if obj.nullable == Some(true) {
                errs.push(Error::forbidden(
                    &fld_path
                        .child("items")
                        .child("properties")
                        .key(k.clone())
                        .child("nullable"),
                    "this property is in x-kubernetes-list-map-keys, so it cannot be nullable",
                ));
            }
        }
    }
    errs
}
