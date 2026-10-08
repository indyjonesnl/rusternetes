//! CustomResourceDefinition validation and defaulting — port of upstream
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation/validation.go`
//! and `.../apiextensions/v1/defaults.go` (release-1.35).
//!
//! Scope: the structural half of `validateCustomResourceDefinitionSpec`
//! (`:353`) — group, scope, the version set (unique DNS-1035 names, exactly one
//! storage version), `ValidateCustomResourceDefinitionNames` (`:785`),
//! `ValidateCustomResourceColumnDefinition` (`:821`),
//! `ValidateCustomResourceSelectableFields` (`:847`),
//! `ValidateCustomResourceDefinitionSubresources` (`:1525`) and
//! `validateCustomResourceConversion` (`:612`).
//!
//! Also here: the whole-object checks (`ValidateCustomResourceDefinition`,
//! `ValidateCustomResourceDefinitionUpdate`, `...UpdateStatus`) -- required
//! names, api-approval annotation, `preserveUnknownFields`, status conditions,
//! `storedVersions` -- and `SetDefaults_CustomResourceDefinition`.
//!
//! Out of scope here: the structural-schema and CEL rule checks, which
//! `handlers::cel_validation` runs (wrapped by the registry strategy).
//!
//! Field paths follow upstream, which validates the *internal* type after
//! converting from `v1` — so a conversion error is reported under
//! `spec.conversion.webhookClientConfig` (internal) even though a v1 client
//! sends `spec.conversion.webhook.clientConfig`, and a printer column's path is
//! `JSONPath`, not `jsonPath`. Diverging would make our messages disagree with
//! every other Kubernetes cluster.

use crate::resources::{
    ConversionStrategyType, CustomResourceColumnDefinition, CustomResourceDefinition,
    CustomResourceDefinitionNames, CustomResourceDefinitionSpec, CustomResourceSubresources,
    JSONSchemaProps, JSONSchemaPropsOrBool, ResourceScope, SelectableField,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1035_label, is_dns1123_subdomain};
use crate::validation::webhookconfiguration::{validate_webhook_service, validate_webhook_url};
use std::collections::HashSet;

/// `printerColumnDatatypes` (`validation.go:52`).
const PRINTER_COLUMN_DATATYPES: &[&str] = &["boolean", "date", "integer", "number", "string"];
/// `customResourceColumnDefinitionFormats` (`validation.go:53`).
const COLUMN_FORMATS: &[&str] = &[
    "byte",
    "date",
    "date-time",
    "double",
    "float",
    "int32",
    "int64",
    "password",
];
/// `MaxSelectableFields` (`validation.go:63`).
const MAX_SELECTABLE_FIELDS: usize = 8;
/// `acceptedConversionReviewVersions` (`validation.go:560`).
const ACCEPTED_CONVERSION_REVIEW_VERSIONS: &[&str] = &["v1", "v1beta1"];

/// `SetDefaults_CustomResourceDefinition` (`apiextensions/v1/defaults.go:30-40`):
/// the spec defaults of `SetDefaults_CustomResourceDefinitionSpec` (`:42-55`),
/// and `status.storedVersions` seeded with the first storage version when it
/// is empty. Must run *before* validation: the `names.singular` /
/// `names.listKind` / `conversion.strategy` rules below are all satisfied by
/// these defaults, exactly as upstream's are.
pub fn set_defaults_custom_resource_definition(crd: &mut CustomResourceDefinition) {
    let seeded = crd
        .status
        .as_ref()
        .and_then(|s| s.stored_versions.as_ref())
        .is_some_and(|v| !v.is_empty());
    if !seeded {
        if let Some(v) = crd.spec.versions.iter().find(|v| v.storage) {
            let status = crd.status.get_or_insert_with(Default::default);
            status.stored_versions = Some(vec![v.name.clone()]);
        }
    }
    let names = &mut crd.spec.names;
    if names.singular.as_deref().unwrap_or("").is_empty() {
        names.singular = Some(names.kind.to_lowercase());
    }
    if names.list_kind.as_deref().unwrap_or("").is_empty() && !names.kind.is_empty() {
        names.list_kind = Some(format!("{}List", names.kind));
    }
    if crd.spec.conversion.is_none() {
        crd.spec.conversion = Some(crate::resources::CustomResourceConversion {
            strategy: Some(ConversionStrategyType::None),
            webhook: None,
        });
    }
}

/// `validateCustomResourceDefinitionSpecUpdate` (`validation.go:648-664`), the
/// immutability half — the spec half is
/// [`validate_custom_resource_definition_spec`], which the caller runs.
///
/// ```go
/// if opts.requireImmutableNames {
///     allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Scope, oldSpec.Scope, fldPath.Child("scope"))...)
///     allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Names.Kind, oldSpec.Names.Kind, fldPath.Child("names", "kind"))...)
/// }
/// allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Group, oldSpec.Group, fldPath.Child("group"))...)
/// allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Names.Plural, oldSpec.Names.Plural, fldPath.Child("names", "plural"))...)
/// ```
///
/// `require_immutable_names` is `IsCRDConditionTrue(oldObj, Established)`
/// (`validation.go:234`): only a CRD the apiserver has already established is
/// held to its `scope` and `names.kind`, because those are what the storage
/// layout was built from.
pub fn validate_custom_resource_definition_spec_update(
    spec: &CustomResourceDefinitionSpec,
    old_spec: &CustomResourceDefinitionSpec,
    require_immutable_names: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if require_immutable_names {
        if spec.scope != old_spec.scope {
            errs.push(immutable(&fld_path.child("scope"), &spec.scope));
        }
        if spec.names.kind != old_spec.names.kind {
            errs.push(immutable(
                &fld_path.child("names").child("kind"),
                &spec.names.kind,
            ));
        }
    }
    if spec.group != old_spec.group {
        errs.push(immutable(&fld_path.child("group"), &spec.group));
    }
    if spec.names.plural != old_spec.names.plural {
        errs.push(immutable(
            &fld_path.child("names").child("plural"),
            &spec.names.plural,
        ));
    }

    errs
}

/// `ValidateImmutableField`
/// (`staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta.go`): the
/// message is always `field is immutable`.
fn immutable<T: serde::Serialize>(path: &Path, value: &T) -> Error {
    Error::invalid(
        path,
        crate::validation::field::BadValue::marshal(value),
        "field is immutable",
    )
}

/// `ValidateCustomResourceSelectableFields` (`validation.go:847-878`).
///
/// Upstream resolves each `jsonPath` against the version's **structural**
/// schema with `cel.ValidFieldPath(..., WithFieldPathAllowArrayNotation(false))`
/// and then checks three things the required-and-unique half cannot: that the
/// path exists, that it does not point into `metadata`, and that its leaf is a
/// scalar. Uniqueness is on the *resolved* path, so two spellings of the same
/// field collide.
///
/// Deviation, stated deliberately: Rusternetes has no structural-schema
/// builder, so the path is resolved against the declared `properties` /
/// `additionalProperties` of the OpenAPI schema as written. A field reachable
/// only through `allOf`/`anyOf`/`oneOf` — which a structural schema would have
/// flattened — is reported as invalid here. The rest of the contract, including
/// every message, is upstream's.
pub fn validate_custom_resource_selectable_fields(
    selectable_fields: &[SelectableField],
    schema: Option<&JSONSchemaProps>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let mut unique: HashSet<String> = HashSet::new();

    for (i, selectable) in selectable_fields.iter().enumerate() {
        let index_path = fld_path.index(i);
        let sp = index_path.child("jsonPath");
        if selectable.json_path.is_empty() {
            errs.push(Error::required(&sp, ""));
            continue;
        }
        // Without a schema there is nothing to resolve against; upstream always
        // has one here because `requireOpenAPISchema` has already rejected a
        // version without it.
        let Some(schema) = schema else {
            if !unique.insert(selectable.json_path.clone()) {
                errs.push(Error::duplicate(&sp, selectable.json_path.clone()));
            }
            continue;
        };

        let (path, found) = match valid_field_path(&selectable.json_path, schema) {
            Ok(resolved) => resolved,
            Err(e) => {
                errs.push(Error::invalid(
                    &sp,
                    selectable.json_path.clone(),
                    format!("is an invalid path: {e}"),
                ));
                continue;
            }
        };

        if path.first().map(String::as_str) == Some("metadata") {
            errs.push(Error::invalid(
                &sp,
                selectable.json_path.clone(),
                "must not point to fields in metadata",
            ));
        }
        if !allowed_selectable_field_schema(found) {
            errs.push(Error::invalid(
                &sp,
                selectable.json_path.clone(),
                "must point to a field of type string, boolean or integer. Enum string fields and strings with formats are allowed.",
            ));
        }
        if !unique.insert(path.join(".")) {
            errs.push(Error::duplicate(&sp, selectable.json_path.clone()));
        }
    }

    if unique.len() > MAX_SELECTABLE_FIELDS {
        errs.push(Error::too_many(fld_path, MAX_SELECTABLE_FIELDS));
    }

    errs
}

/// `allowedSelectableFieldSchema` (`validation.go:880-890`).
fn allowed_selectable_field_schema(schema: &JSONSchemaProps) -> bool {
    matches!(
        schema.type_.as_deref(),
        Some("string") | Some("boolean") | Some("integer")
    )
}

/// `cel.ValidFieldPath`
/// (`staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/validation.go:557-676`)
/// with `allowArrayNotation` false, which is how
/// `ValidateCustomResourceSelectableFields` calls it.
///
/// Returns the resolved path segments and the schema of the leaf. Every error
/// string is upstream's, because a client reads them.
fn valid_field_path<'a>(
    json_path: &str,
    schema: &'a JSONSchemaProps,
) -> Result<(Vec<String>, &'a JSONSchemaProps), String> {
    let mut path: Vec<String> = Vec::new();
    let mut schema = schema;
    let mut tokens = tokenize_field_path(json_path).into_iter();

    while let Some(token) = tokens.next() {
        match token.as_str() {
            // `WithFieldPathAllowArrayNotation(false)` (`:622-624`).
            "[" => return Err("array notation is not allowed".to_string()),
            "." => {
                let Some(name) = tokens.next() else {
                    return Err("unexpected end of JSON path".to_string());
                };
                if schema.properties.is_some() {
                    let Some(next) = schema.properties.as_ref().and_then(|p| p.get(&name)) else {
                        return Err("does not refer to a valid field".to_string());
                    };
                    path.push(name);
                    schema = next;
                } else if let Some(additional) = schema.additional_properties.as_deref() {
                    // An unnamed key: the leaf is the map's value schema.
                    let JSONSchemaPropsOrBool::Schema(next) = additional else {
                        return Err("does not refer to a valid field".to_string());
                    };
                    path.push(name);
                    schema = next;
                } else {
                    return Err("does not refer to a valid field".to_string());
                }
            }
            other => return Err(format!("expected [ or . but got: {other}")),
        }
    }

    Ok((path, schema))
}

/// The scanner in `ValidFieldPath` (`validation.go:580-615`): `.`, `[` and `]`
/// come back as single-character tokens, everything between them as one token.
/// A single-quoted string is returned whole, delimiters included — which only
/// matters for the array notation this caller rejects anyway.
fn tokenize_field_path(json_path: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut escaped = false;

    for ch in json_path.chars() {
        if in_quote {
            current.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                in_quote = false;
                tokens.push(std::mem::take(&mut current));
            }
            continue;
        }
        match ch {
            '.' | '[' | ']' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
            }
            '\'' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                in_quote = true;
                current.push(ch);
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// `validateCustomResourceDefinitionSpec` (`validation.go:353`), minus the
/// schema-structural half (see the module doc).
///
/// The create-time form: `allowInvalidCABundle: true` (`validation.go:96-97`).
pub fn validate_custom_resource_definition_spec(
    spec: &CustomResourceDefinitionSpec,
    fld_path: &Path,
) -> ErrorList {
    validate_custom_resource_definition_spec_opts(spec, true, fld_path)
}

/// [`validate_custom_resource_definition_spec`] with
/// `validationOptions.allowInvalidCABundle` (`validation.go:146`) supplied.
fn validate_custom_resource_definition_spec_opts(
    spec: &CustomResourceDefinitionSpec,
    allow_invalid_ca_bundle: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // group (`:356-362`).
    let group_path = fld_path.child("group");
    if spec.group.is_empty() {
        errs.push(Error::required(&group_path, ""));
    } else {
        let msgs = is_dns1123_subdomain(&spec.group);
        if !msgs.is_empty() {
            errs.push(Error::invalid(
                &group_path,
                spec.group.clone(),
                msgs.join(","),
            ));
        } else if spec.group.split('.').count() < 2 {
            errs.push(Error::invalid(
                &group_path,
                spec.group.clone(),
                "should be a domain with at least one dot",
            ));
        }
    }

    // scope — `validateEnumStrings(..., required=true)` (`:364`, `:502-515`).
    match &spec.scope {
        ResourceScope::Unspecified => errs.push(Error::required(&fld_path.child("scope"), "")),
        ResourceScope::Unknown(v) => errs.push(Error::not_supported(
            &fld_path.child("scope"),
            v.clone(),
            &["Cluster", "Namespaced"],
        )),
        ResourceScope::Cluster | ResourceScope::Namespaced => {}
    }

    // versions (`:398-414`).
    let versions_path = fld_path.child("versions");
    let mut storage_flag_count = 0usize;
    let mut seen: HashSet<&str> = HashSet::new();
    let mut unique_names = true;
    for (i, version) in spec.versions.iter().enumerate() {
        let vp = versions_path.index(i);
        if version.storage {
            storage_flag_count += 1;
        }
        if !seen.insert(version.name.as_str()) {
            unique_names = false;
        }
        let msgs = is_dns1035_label(&version.name);
        if !msgs.is_empty() {
            errs.push(Error::invalid(
                &vp.child("name"),
                version.name.clone(),
                msgs.join(","),
            ));
        }

        if let Some(columns) = &version.additional_printer_columns {
            let columns_path = vp.child("additionalPrinterColumns");
            for (j, column) in columns.iter().enumerate() {
                errs.extend(validate_custom_resource_column_definition(
                    column,
                    &columns_path.index(j),
                ));
            }
        }
        if let Some(fields) = &version.selectable_fields {
            errs.extend(validate_custom_resource_selectable_fields(
                fields,
                version.schema.as_ref().map(|s| &s.open_apiv3_schema),
                &vp.child("selectableFields"),
            ));
        }
        errs.extend(validate_subresources(
            version.subresources.as_ref(),
            &vp.child("subresources"),
        ));
    }

    if !unique_names {
        errs.push(Error::invalid(
            &versions_path,
            String::new(),
            "must contain unique version names",
        ));
    }
    if storage_flag_count != 1 {
        errs.push(Error::invalid(
            &versions_path,
            String::new(),
            "must have exactly one version marked as storage version",
        ));
    }

    errs.extend(validate_names(&spec.names, &fld_path.child("names")));
    errs.extend(validate_conversion(
        spec,
        allow_invalid_ca_bundle,
        &fld_path.child("conversion"),
    ));

    errs
}

/// The spec-level required names (`validation.go:456-470`) on top of
/// `ValidateCustomResourceDefinitionNames` (`:785`). `singular` and `listKind`
/// are required here because defaulting has already filled them.
fn validate_names(names: &CustomResourceDefinitionNames, fld_path: &Path) -> ErrorList {
    let mut errs = validate_custom_resource_definition_names(names, fld_path);
    let singular = names.singular.clone().unwrap_or_default();
    let list_kind = names.list_kind.clone().unwrap_or_default();
    if names.plural.is_empty() {
        errs.push(Error::required(&fld_path.child("plural"), ""));
    }
    if singular.is_empty() {
        errs.push(Error::required(&fld_path.child("singular"), ""));
    }
    if names.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    }
    if list_kind.is_empty() {
        errs.push(Error::required(&fld_path.child("listKind"), ""));
    }
    errs
}

/// `ValidateCustomResourceDefinitionNames` (`validation.go:785-818`): the
/// format rules, which hold for `spec.names` and `status.acceptedNames` alike.
/// The "required" half is spec-only and stays in [`validate_names`].
pub fn validate_custom_resource_definition_names(
    names: &CustomResourceDefinitionNames,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let singular = names.singular.clone().unwrap_or_default();
    let list_kind = names.list_kind.clone().unwrap_or_default();

    let mut label_rule = |value: &str, child: &str, mixed_case: bool| {
        if value.is_empty() {
            return;
        }
        let msgs = is_dns1035_label(&value.to_lowercase());
        if msgs.is_empty() {
            return;
        }
        let detail = if mixed_case {
            format!(
                "may have mixed case, but should otherwise match: {}",
                msgs.join(",")
            )
        } else {
            msgs.join(",")
        };
        errs.push(Error::invalid(
            &fld_path.child(child),
            value.to_string(),
            detail,
        ));
    };
    label_rule(&names.plural, "plural", false);
    label_rule(&singular, "singular", false);
    label_rule(&names.kind, "kind", true);
    label_rule(&list_kind, "listKind", true);

    if let Some(short_names) = &names.short_names {
        for (i, short_name) in short_names.iter().enumerate() {
            let msgs = is_dns1035_label(short_name);
            if !msgs.is_empty() {
                errs.push(Error::invalid(
                    &fld_path.child("shortNames").index(i),
                    short_name.clone(),
                    msgs.join(","),
                ));
            }
        }
    }
    if !names.kind.is_empty() && names.kind == list_kind {
        errs.push(Error::invalid(
            &fld_path.child("listKind"),
            list_kind.clone(),
            "kind and listKind may not be the same",
        ));
    }
    if let Some(categories) = &names.categories {
        for (i, category) in categories.iter().enumerate() {
            let msgs = is_dns1035_label(category);
            if !msgs.is_empty() {
                errs.push(Error::invalid(
                    &fld_path.child("categories").index(i),
                    category.clone(),
                    msgs.join(","),
                ));
            }
        }
    }

    errs
}

/// `ValidateCustomResourceColumnDefinition` (`validation.go:821-845`).
fn validate_custom_resource_column_definition(
    column: &CustomResourceColumnDefinition,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if column.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    }
    let types = PRINTER_COLUMN_DATATYPES.join(",");
    if column.type_.is_empty() {
        errs.push(Error::required(
            &fld_path.child("type"),
            format!("must be one of {types}"),
        ));
    } else if !PRINTER_COLUMN_DATATYPES.contains(&column.type_.as_str()) {
        errs.push(Error::invalid(
            &fld_path.child("type"),
            column.type_.clone(),
            format!("must be one of {types}"),
        ));
    }
    if let Some(format) = &column.format {
        if !format.is_empty() && !COLUMN_FORMATS.contains(&format.as_str()) {
            errs.push(Error::invalid(
                &fld_path.child("format"),
                format.clone(),
                format!("must be one of {}", COLUMN_FORMATS.join(",")),
            ));
        }
    }
    // Upstream's path is the internal field name, `JSONPath`.
    let json_path = fld_path.child("JSONPath");
    if column.json_path.is_empty() {
        errs.push(Error::required(&json_path, ""));
    } else {
        errs.extend(validate_simple_json_path(&column.json_path, &json_path));
    }
    errs
}

/// `ValidateCustomResourceDefinitionSubresources` (`validation.go:1525-1566`).
fn validate_subresources(
    subresources: Option<&CustomResourceSubresources>,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(scale) = subresources.and_then(|s| s.scale.as_ref()) else {
        return errs;
    };

    let spec_path = fld_path.child("scale.specReplicasPath");
    if scale.spec_replicas_path.is_empty() {
        errs.push(Error::required(&spec_path, ""));
    } else {
        let path_errs = validate_simple_json_path(&scale.spec_replicas_path, &spec_path);
        if !path_errs.is_empty() {
            errs.extend(path_errs);
        } else if !scale.spec_replicas_path.starts_with(".spec.") {
            errs.push(Error::invalid(
                &spec_path,
                scale.spec_replicas_path.clone(),
                "should be a json path under .spec",
            ));
        }
    }

    let status_path = fld_path.child("scale.statusReplicasPath");
    if scale.status_replicas_path.is_empty() {
        errs.push(Error::required(&status_path, ""));
    } else {
        let path_errs = validate_simple_json_path(&scale.status_replicas_path, &status_path);
        if !path_errs.is_empty() {
            errs.extend(path_errs);
        } else if !scale.status_replicas_path.starts_with(".status.") {
            errs.push(Error::invalid(
                &status_path,
                scale.status_replicas_path.clone(),
                "should be a json path under .status",
            ));
        }
    }

    if let Some(selector) = scale.label_selector_path.as_ref().filter(|p| !p.is_empty()) {
        let selector_path = fld_path.child("scale.labelSelectorPath");
        let path_errs = validate_simple_json_path(selector, &selector_path);
        if !path_errs.is_empty() {
            errs.extend(path_errs);
        } else if !selector.starts_with(".spec.") && !selector.starts_with(".status.") {
            errs.push(Error::invalid(
                &selector_path,
                selector.clone(),
                "should be a json path under either .spec or .status",
            ));
        }
    }
    errs
}

/// `validateSimpleJSONPath` (`validation.go:1568-1582`).
fn validate_simple_json_path(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if value.is_empty() {
        errs.push(Error::invalid(fld_path, String::new(), "must not be empty"));
    } else if !value.starts_with('.') {
        errs.push(Error::invalid(
            fld_path,
            value.to_string(),
            "must be a simple json path starting with .",
        ));
    }
    errs
}

/// `validateCustomResourceConversion` (`validation.go:612-644`), with
/// `requireRecognizedVersion = true` (create).
///
/// The paths are upstream's internal ones — a v1 client's
/// `spec.conversion.webhook.clientConfig` is `spec.conversion.webhookClientConfig`
/// after conversion, and that is what upstream reports.
fn validate_conversion(
    spec: &CustomResourceDefinitionSpec,
    allow_invalid_ca_bundle: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    let Some(conversion) = &spec.conversion else {
        return errs;
    };
    let client_config_path = fld_path.child("webhookClientConfig");
    let review_versions_path = fld_path.child("conversionReviewVersions");

    match &conversion.strategy {
        None => errs.push(Error::required(&fld_path.child("strategy"), "")),
        Some(ConversionStrategyType::Webhook) => {
            match conversion.webhook.as_ref() {
                None => errs.push(Error::required(
                    &client_config_path,
                    "required when strategy is set to Webhook",
                )),
                Some(webhook) => {
                    let cc = &webhook.client_config;
                    match (cc.url.as_ref(), cc.service.as_ref()) {
                        (Some(url), None) => {
                            errs.extend(validate_webhook_url(
                                url,
                                &client_config_path.child("url"),
                            ));
                        }
                        (None, Some(service)) => {
                            errs.extend(validate_webhook_service(
                                &service.name,
                                &service.namespace,
                                service.path.as_deref(),
                                service.port,
                                &client_config_path.child("service"),
                            ));
                        }
                        // Both set or neither set.
                        _ => errs.push(Error::required(
                            &client_config_path,
                            "exactly one of url or service is required",
                        )),
                    }
                    // `validation.go:631-633`.
                    if let Some(bundle) = cc.ca_bundle.as_deref().filter(|b| !b.is_empty()) {
                        if !allow_invalid_ca_bundle {
                            errs.extend(validate_ca_bundle_field(
                                bundle,
                                &client_config_path.child("caBundle"),
                            ));
                        }
                    }
                    errs.extend(validate_conversion_review_versions(
                        &webhook.conversion_review_versions,
                        &review_versions_path,
                    ));
                }
            }
            if conversion.webhook.is_none() {
                errs.extend(validate_conversion_review_versions(
                    &[],
                    &review_versions_path,
                ));
            }
        }
        // `validateEnumStrings` (:617) answers an unknown strategy
        // `NotSupported`; it is then not Webhook, so the else branch (:638-645)
        // applies exactly as for `None`.
        Some(ConversionStrategyType::None) | Some(ConversionStrategyType::Unknown(_)) => {
            if let Some(ConversionStrategyType::Unknown(v)) = &conversion.strategy {
                errs.push(Error::not_supported(
                    &fld_path.child("strategy"),
                    v.clone(),
                    &["None", "Webhook"],
                ));
            }
            if let Some(webhook) = &conversion.webhook {
                errs.push(Error::forbidden(
                    &client_config_path,
                    "should not be set when strategy is not set to Webhook",
                ));
                if !webhook.conversion_review_versions.is_empty() {
                    errs.push(Error::forbidden(
                        &review_versions_path,
                        "should not be set when strategy is not set to Webhook",
                    ));
                }
            }
        }
    }
    errs
}

/// `allowInvalidCABundle` (`validation.go:557-569`): an invalid CA bundle may
/// be written only while the CRD is not yet Established, or when the stored
/// bundle is itself already invalid.
fn allow_invalid_ca_bundle(old: &CustomResourceDefinition) -> bool {
    if !is_crd_condition_true(old, "Established") {
        return true;
    }
    let Some(bundle) = old
        .spec
        .conversion
        .as_ref()
        .and_then(|c| c.webhook.as_ref())
        .and_then(|w| w.client_config.ca_bundle.as_deref())
        .filter(|b| !b.is_empty())
    else {
        return false;
    };
    !validate_ca_bundle_field(bundle, &Path::new("caBundle")).is_empty()
}

/// `caBundle` is a Go `[]byte`, so it is base64 on the wire. A value that is
/// not valid base64 is taken as the raw bytes (the same leniency as
/// `api-server/src/conversion.rs`); it then fails the PEM check below.
fn validate_ca_bundle_field(bundle: &str, path: &Path) -> ErrorList {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(bundle)
        .unwrap_or_else(|_| bundle.as_bytes().to_vec());
    validate_ca_bundle_with_value(path, &bytes, bundle)
}

/// `webhook.ValidateCABundle` (`apiserver/pkg/util/webhook/validation.go:29-36`).
/// The bad value is the `[]byte`, which upstream marshals to JSON as base64.
#[cfg(test)]
fn validate_ca_bundle(path: &Path, ca_bundle: &[u8]) -> ErrorList {
    use base64::Engine;
    let shown = base64::engine::general_purpose::STANDARD.encode(ca_bundle);
    validate_ca_bundle_with_value(path, ca_bundle, &shown)
}

fn validate_ca_bundle_with_value(path: &Path, ca_bundle: &[u8], shown: &str) -> ErrorList {
    match root_cert_pool(ca_bundle) {
        Ok(()) => Vec::new(),
        Err(msg) => vec![Error::invalid(
            path,
            serde_json::Value::String(shown.to_string()),
            format!("unable to load root certificates: {msg}"),
        )],
    }
}

/// `rootCertPool` + `createErrorParsingCAData`
/// (`client-go/transport/transport.go:252-286`): `AppendCertsFromPEM` succeeds
/// when at least one `CERTIFICATE` block (no headers) parses; otherwise the
/// error names the first failure.
fn root_cert_pool(ca_data: &[u8]) -> Result<(), String> {
    if ca_data.is_empty() {
        return Ok(());
    }
    let blocks = pem::parse_many(ca_data).unwrap_or_default();
    let is_cert = |b: &pem::Pem| b.tag() == "CERTIFICATE" && b.headers().iter().next().is_none();
    if blocks
        .iter()
        .filter(|b| is_cert(b))
        .any(|b| x509_parser::parse_x509_certificate(b.contents()).is_ok())
    {
        return Ok(());
    }
    if blocks.is_empty() {
        return Err("unable to parse bytes as PEM block".to_string());
    }
    for b in blocks.iter().filter(|b| is_cert(b)) {
        if let Err(e) = x509_parser::parse_x509_certificate(b.contents()) {
            return Err(format!("failed to parse certificate: {e}"));
        }
    }
    Err("no valid certificate authority data seen".to_string())
}

/// `validateConversionReviewVersions` (`validation.go:527-555`).
fn validate_conversion_review_versions(versions: &[String], fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if versions.is_empty() {
        errs.push(Error::required(fld_path, ""));
        return errs;
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut has_accepted = false;
    for (i, version) in versions.iter().enumerate() {
        if !seen.insert(version.as_str()) {
            errs.push(Error::invalid(
                &fld_path.index(i),
                version.clone(),
                "duplicate version",
            ));
            continue;
        }
        for msg in is_dns1035_label(version) {
            errs.push(Error::invalid(&fld_path.index(i), version.clone(), msg));
        }
        if ACCEPTED_CONVERSION_REVIEW_VERSIONS.contains(&version.as_str()) {
            has_accepted = true;
        }
    }
    if !has_accepted {
        errs.push(Error::invalid(
            fld_path,
            versions.join(","),
            format!(
                "must include at least one of {}",
                ACCEPTED_CONVERSION_REVIEW_VERSIONS.join(", ")
            ),
        ));
    }
    errs
}

// ---------------------------------------------------------------------------
// Whole-object validation: `ValidateCustomResourceDefinition` and friends.
// ---------------------------------------------------------------------------

/// `apiextensionsv1.CustomResourceCleanupFinalizer`
/// (`apiextensions/v1/types.go:392`).
pub const CUSTOM_RESOURCE_CLEANUP_FINALIZER: &str = "customresourcecleanup.apiextensions.k8s.io";

/// `apiextensionsv1beta1.KubeAPIApprovedAnnotation`
/// (`apiextensions/v1beta1/types.go:32`).
pub const KUBE_API_APPROVED_ANNOTATION: &str = "api-approved.kubernetes.io";

/// `IsCRDConditionTrue` (`apiextensions/helpers.go:70`): the condition is
/// present and strictly `True`.
pub fn is_crd_condition_true(crd: &CustomResourceDefinition, condition_type: &str) -> bool {
    crd.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|cs| cs.iter().find(|c| c.type_ == condition_type))
        .is_some_and(|c| c.status == "True")
}

/// `IsProtectedCommunityGroup` (`apihelpers/helpers.go:31-41`).
pub fn is_protected_community_group(group: &str) -> bool {
    group == "k8s.io"
        || group.ends_with(".k8s.io")
        || group == "kubernetes.io"
        || group.ends_with(".kubernetes.io")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiApprovalState {
    Invalid,
    Approved,
    Bypassed,
    Missing,
}

/// `GetAPIApprovalState` (`apihelpers/helpers.go:55-70`).
pub fn api_approval_state(
    annotations: Option<&std::collections::HashMap<String, String>>,
) -> (ApiApprovalState, String) {
    let annotation = annotations
        .and_then(|a| a.get(KUBE_API_APPROVED_ANNOTATION))
        .map(String::as_str)
        .unwrap_or("");
    if annotation.is_empty() {
        return (
            ApiApprovalState::Missing,
            format!("protected groups must have approval annotation {KUBE_API_APPROVED_ANNOTATION:?}, see https://github.com/kubernetes/enhancements/pull/1111"),
        );
    }
    if annotation.starts_with("unapproved") {
        return (
            ApiApprovalState::Bypassed,
            format!("not approved: {annotation:?}"),
        );
    }
    // `url.ParseRequestURI` with a non-empty host and scheme.
    let approved = url::Url::parse(annotation)
        .ok()
        .is_some_and(|u| u.host().is_some() && !u.scheme().is_empty());
    if approved {
        return (
            ApiApprovalState::Approved,
            format!("approved in {annotation}"),
        );
    }
    (
        ApiApprovalState::Invalid,
        format!("protected groups must have approval annotation {KUBE_API_APPROVED_ANNOTATION:?} with either a URL or a reason starting with \"unapproved\", see https://github.com/kubernetes/enhancements/pull/1111"),
    )
}

/// `validateAPIApproval` (`validation.go:1857-1890`).
fn validate_api_approval(
    new_crd: &CustomResourceDefinition,
    old_crd: Option<&CustomResourceDefinition>,
) -> ErrorList {
    if !is_protected_community_group(&new_crd.spec.group) {
        return Vec::new();
    }
    let old_state = old_crd.map(|o| api_approval_state(o.metadata.annotations.as_ref()).0);
    let (new_state, reason) = api_approval_state(new_crd.metadata.annotations.as_ref());
    // A v1 client that only updates the spec must not be rejected over an
    // approval it never touched.
    if old_state == Some(new_state) {
        return Vec::new();
    }
    let path = Path::new("metadata")
        .child("annotations")
        .key(KUBE_API_APPROVED_ANNOTATION);
    match new_state {
        ApiApprovalState::Approved | ApiApprovalState::Bypassed => Vec::new(),
        ApiApprovalState::Missing => vec![Error::required(&path, reason)],
        ApiApprovalState::Invalid => vec![Error::invalid(
            &path,
            new_crd
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(KUBE_API_APPROVED_ANNOTATION))
                .cloned()
                .unwrap_or_default(),
            reason,
        )],
    }
}

/// `validatePreserveUnknownFields` (`validation.go:1892-1904`).
fn validate_preserve_unknown_fields(
    crd: &CustomResourceDefinition,
    old_crd: Option<&CustomResourceDefinition>,
) -> ErrorList {
    if old_crd.is_some_and(|o| o.spec.preserve_unknown_fields == Some(true)) {
        // No-op for compatibility with existing data.
        return Vec::new();
    }
    if crd.spec.preserve_unknown_fields == Some(true) {
        return vec![Error::invalid(
            &Path::new("spec").child("preserveUnknownFields"),
            true,
            "cannot set to true, set x-kubernetes-preserve-unknown-fields to true in spec.versions[*].schema instead",
        )];
    }
    Vec::new()
}

/// `ValidateCustomResourceDefinitionStatus` (`validation.go:778-783`).
pub fn validate_custom_resource_definition_status(
    status: Option<&crate::resources::CustomResourceDefinitionStatus>,
    fld_path: &Path,
) -> ErrorList {
    // `AcceptedNames` is a struct upstream, so an absent one is the zero value,
    // which has nothing to reject.
    match status.and_then(|s| s.accepted_names.as_ref()) {
        Some(names) => {
            validate_custom_resource_definition_names(names, &fld_path.child("acceptedNames"))
        }
        None => Vec::new(),
    }
}

/// `ValidateCustomResourceDefinitionStoredVersions` (`validation.go:261-285`).
pub fn validate_custom_resource_definition_stored_versions(
    stored_versions: &[String],
    versions: &[crate::resources::CustomResourceDefinitionVersion],
    fld_path: &Path,
) -> ErrorList {
    if stored_versions.is_empty() {
        return vec![Error::invalid(
            fld_path,
            serde_json::json!(stored_versions),
            "must have at least one stored version",
        )];
    }
    let mut errs: ErrorList = Vec::new();
    // `storedVersionsMap[v] = i` keeps the last index of a duplicate.
    let mut remaining: Vec<(usize, &String)> = stored_versions.iter().enumerate().collect();
    for v in versions {
        let found = remaining.iter().any(|(_, s)| **s == v.name);
        if v.storage && !found {
            errs.push(Error::invalid(
                fld_path,
                serde_json::json!(stored_versions),
                format!("must have the storage version {}", v.name),
            ));
        }
        remaining.retain(|(_, s)| **s != v.name);
    }
    for (i, v) in remaining {
        errs.push(Error::invalid(
            &fld_path.index(i),
            v.clone(),
            format!("missing from spec.versions; {v} was previously a storage version, and must remain in spec.versions until a storage migration ensures no data remains persisted in {v} and removes {v} from status.storedVersions"),
        ));
    }
    errs
}

/// The name function `ValidateCustomResourceDefinition` hands
/// `ValidateObjectMeta` (`validation.go:76-83`) beyond `NameIsDNSSubdomain`:
/// the name is `<plural>.<group>`. Upstream appends the message for the name
/// and for a `generateName` prefix alike, because the prefix never equals the
/// required name.
fn required_name_errors(crd: &CustomResourceDefinition) -> ErrorList {
    let required = format!("{}.{}", crd.spec.names.plural, crd.spec.group);
    let message = "must be spec.names.plural+\".\"+spec.group";
    let mut errs: ErrorList = Vec::new();
    let meta_path = Path::new("metadata");
    if let Some(gn) = crd
        .metadata
        .generate_name
        .as_deref()
        .filter(|g| !g.is_empty())
    {
        errs.push(Error::invalid(
            &meta_path.child("generateName"),
            gn.to_string(),
            message,
        ));
    }
    if !crd.metadata.name.is_empty() && crd.metadata.name != required {
        errs.push(Error::invalid(
            &meta_path.child("name"),
            crd.metadata.name.clone(),
            message,
        ));
    }
    errs
}

fn stored_versions_of(crd: &CustomResourceDefinition) -> &[String] {
    crd.status
        .as_ref()
        .and_then(|s| s.stored_versions.as_deref())
        .unwrap_or_default()
}

/// `ValidateCustomResourceDefinition` (`validation.go:75-107`), without the
/// structural-schema and CEL halves of the spec (see the module doc).
pub fn validate_custom_resource_definition(crd: &CustomResourceDefinition) -> ErrorList {
    let mut errs = crate::validation::objectmeta::validate_object_meta(
        &crd.metadata,
        false,
        crate::validation::objectmeta::name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(required_name_errors(crd));
    errs.extend(validate_custom_resource_definition_spec(
        &crd.spec,
        &Path::new("spec"),
    ));
    let status_path = Path::new("status");
    errs.extend(validate_custom_resource_definition_status(
        crd.status.as_ref(),
        &status_path,
    ));
    errs.extend(validate_custom_resource_definition_stored_versions(
        stored_versions_of(crd),
        &crd.spec.versions,
        &status_path.child("storedVersions"),
    ));
    errs.extend(validate_api_approval(crd, None));
    errs.extend(validate_preserve_unknown_fields(crd, None));
    errs
}

/// `ValidateCustomResourceDefinitionUpdate` (`validation.go:230-259`).
pub fn validate_custom_resource_definition_update(
    crd: &CustomResourceDefinition,
    old: &CustomResourceDefinition,
) -> ErrorList {
    let mut errs = crate::validation::objectmeta::validate_object_meta_update(
        &crd.metadata,
        &old.metadata,
        &Path::new("metadata"),
    );
    let spec_path = Path::new("spec");
    errs.extend(validate_custom_resource_definition_spec_opts(
        &crd.spec,
        allow_invalid_ca_bundle(old),
        &spec_path,
    ));
    errs.extend(validate_custom_resource_definition_spec_update(
        &crd.spec,
        &old.spec,
        is_crd_condition_true(old, "Established"),
        &spec_path,
    ));
    let status_path = Path::new("status");
    errs.extend(validate_custom_resource_definition_status(
        crd.status.as_ref(),
        &status_path,
    ));
    errs.extend(validate_custom_resource_definition_stored_versions(
        stored_versions_of(crd),
        &crd.spec.versions,
        &status_path.child("storedVersions"),
    ));
    errs.extend(validate_api_approval(crd, Some(old)));
    errs.extend(validate_preserve_unknown_fields(crd, Some(old)));
    errs
}

/// `ValidateUpdateCustomResourceDefinitionStatus` (`validation.go:288-292`).
pub fn validate_update_custom_resource_definition_status(
    crd: &CustomResourceDefinition,
    old: &CustomResourceDefinition,
) -> ErrorList {
    let mut errs = crate::validation::objectmeta::validate_object_meta_update(
        &crd.metadata,
        &old.metadata,
        &Path::new("metadata"),
    );
    errs.extend(validate_custom_resource_definition_status(
        crd.status.as_ref(),
        &Path::new("status"),
    ));
    errs
}

#[cfg(test)]
mod whole_object_tests {
    use super::*;

    fn crd(group: &str) -> CustomResourceDefinition {
        let mut crd = CustomResourceDefinition::new("widget", group, "Widget", "widgets");
        crd.spec.versions = vec![serde_json::from_value(serde_json::json!({
            "name": "v1", "served": true, "storage": true
        }))
        .unwrap()];
        set_defaults_custom_resource_definition(&mut crd);
        crd
    }

    /// `validateEnumStrings` (validation.go:502-515): an unknown `spec.scope`
    /// is a Go string, so it decodes and is `NotSupported` with the accepted
    /// list in declaration order (`Cluster`, `Namespaced`, :364).
    #[test]
    fn unknown_scope_is_not_supported() {
        let spec: crate::resources::CustomResourceDefinitionSpec =
            serde_json::from_value(serde_json::json!({
                "group": "example.com",
                "names": {"kind": "Widget", "plural": "widgets"},
                "scope": "Galactic",
                "versions": [{"name": "v1", "served": true, "storage": true}]
            }))
            .expect("an unknown scope must decode");
        let errs = validate_custom_resource_definition_spec(&spec, &Path::new("spec"));
        let e = errs
            .iter()
            .find(|e| e.field == "spec.scope")
            .unwrap_or_else(|| panic!("{errs:?}"));
        assert_eq!(
            e.error_type,
            crate::validation::field::ErrorType::NotSupported,
            "{errs:?}"
        );
        assert!(
            e.to_string()
                .contains("supported values: \"Cluster\", \"Namespaced\""),
            "{e}"
        );
    }

    /// validation.go:617 + 618-640: an unknown strategy is `NotSupported`
    /// (`None`, `Webhook`) and, not being Webhook, takes the else branch that
    /// forbids `webhook` config.
    #[test]
    fn unknown_conversion_strategy_is_not_supported() {
        let mut c = crd("example.com");
        c.spec.conversion = Some(
            serde_json::from_value(serde_json::json!({"strategy": "Carrier"}))
                .expect("an unknown strategy must decode"),
        );
        let errs = validate_custom_resource_definition_spec(&c.spec, &Path::new("spec"));
        let e = errs
            .iter()
            .find(|e| e.field == "spec.conversion.strategy")
            .unwrap_or_else(|| panic!("{errs:?}"));
        assert_eq!(
            e.error_type,
            crate::validation::field::ErrorType::NotSupported,
            "{errs:?}"
        );
        assert!(
            e.to_string()
                .contains("supported values: \"None\", \"Webhook\""),
            "{e}"
        );
    }

    /// `TestValidateCustomResourceDefinitionStoredVersions`
    /// (validation_test.go): the storage version must be stored, and a stored
    /// version must stay in `spec.versions`.
    #[test]
    fn stored_versions_rules() {
        let c = crd("example.com");
        let path = Path::new("status").child("storedVersions");
        assert!(validate_custom_resource_definition_stored_versions(
            &["v1".into()],
            &c.spec.versions,
            &path
        )
        .is_empty());
        let errs =
            validate_custom_resource_definition_stored_versions(&[], &c.spec.versions, &path);
        assert_eq!(errs.len(), 1);
        let errs = validate_custom_resource_definition_stored_versions(
            &["v0".into()],
            &c.spec.versions,
            &path,
        );
        assert_eq!(errs.len(), 2, "{errs:?}");
    }

    #[test]
    fn protected_groups() {
        for g in ["k8s.io", "a.k8s.io", "kubernetes.io", "a.kubernetes.io"] {
            assert!(is_protected_community_group(g), "{g}");
        }
        assert!(!is_protected_community_group("example.com"));
        assert!(!is_protected_community_group("notk8s.io.example.com"));
    }

    #[test]
    fn approval_annotation_states() {
        let mut c = crd("a.k8s.io");
        let errs = validate_api_approval(&c, None);
        assert!(
            errs.iter()
                .any(|e| e.error_body().contains("Required value")),
            "{errs:?}"
        );
        for (value, ok) in [
            ("https://github.com/kubernetes/kubernetes/pull/1", true),
            ("unapproved, experimental-only", true),
            ("not a url", false),
        ] {
            c.metadata.annotations =
                Some([(KUBE_API_APPROVED_ANNOTATION.to_string(), value.to_string())].into());
            let errs = validate_api_approval(&c, None);
            assert_eq!(errs.is_empty(), ok, "{value}: {errs:?}");
        }
    }

    /// An unchanged approval state is never rejected on update
    /// (`validation.go:1873-1877`).
    #[test]
    fn unchanged_approval_state_passes_update() {
        let c = crd("a.k8s.io");
        assert!(validate_api_approval(&c, Some(&c)).is_empty());
    }

    #[test]
    fn preserve_unknown_fields_true_is_rejected_unless_already_set() {
        let mut c = crd("example.com");
        c.spec.preserve_unknown_fields = Some(true);
        assert_eq!(validate_preserve_unknown_fields(&c, None).len(), 1);
        assert!(validate_preserve_unknown_fields(&c, Some(&c)).is_empty());
    }

    #[test]
    fn defaults_seed_stored_versions_once() {
        let mut c = crd("example.com");
        assert_eq!(
            c.status.as_ref().unwrap().stored_versions,
            Some(vec!["v1".to_string()])
        );
        c.status.as_mut().unwrap().stored_versions = Some(vec!["v0".to_string()]);
        set_defaults_custom_resource_definition(&mut c);
        assert_eq!(
            c.status.as_ref().unwrap().stored_versions,
            Some(vec!["v0".to_string()])
        );
    }

    #[test]
    fn name_must_be_plural_dot_group() {
        let mut c = crd("example.com");
        assert!(required_name_errors(&c).is_empty());
        c.metadata.name = "other.example.com".into();
        assert_eq!(required_name_errors(&c).len(), 1);
    }
}

#[cfg(test)]
mod ca_bundle_tests {
    use super::*;
    use base64::Engine;

    const CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIBbzCCARWgAwIBAgIUNlw6NuSK77Vukti6ud7ET7GhqoEwCgYIKoZIzj0EAwIw
DDEKMAgGA1UEAwwBdDAgFw0yNjEwMDYxMDAwMjZaGA8yMTI2MDkxMjEwMDAyNlow
DDEKMAgGA1UEAwwBdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABAyxwlFXhKv2
VrRugTxQF05QRdxMCwi9imsVkTzmSbFhY08oQcY7rE0p32un+8/2/RlclBghXdsk
JoOVsn1LB16jUzBRMB0GA1UdDgQWBBT0iCEcLVCXeTKArrq89VZNNHkTqjAfBgNV
HSMEGDAWgBT0iCEcLVCXeTKArrq89VZNNHkTqjAPBgNVHRMBAf8EBTADAQH/MAoG
CCqGSM49BAMCA0gAMEUCIGaiWFbeacTzpGuCBN2CeZq9GsKKNp4ii/lsWwWDxF7s
AiEAhWF2/JiVNXisqXic1Dfy761y8wW/Io2IJoBMvcYxo4E=
-----END CERTIFICATE-----"#;

    fn b64(b: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(b)
    }

    fn crd_with_bundle(bundle: Option<String>, established: bool) -> CustomResourceDefinition {
        let mut c = CustomResourceDefinition::new("widget", "example.com", "Widget", "widgets");
        c.spec.versions = vec![serde_json::from_value(serde_json::json!({
            "name": "v1", "served": true, "storage": true
        }))
        .unwrap()];
        c.spec.conversion = Some(crate::resources::CustomResourceConversion {
            strategy: Some(ConversionStrategyType::Webhook),
            webhook: Some(crate::resources::WebhookConversion {
                client_config: crate::resources::WebhookClientConfig {
                    url: Some("https://example.com/convert".into()),
                    service: None,
                    ca_bundle: bundle,
                },
                conversion_review_versions: vec!["v1".into()],
            }),
        });
        set_defaults_custom_resource_definition(&mut c);
        c.metadata.resource_version = Some("42".into());
        if established {
            let status = c.status.get_or_insert_with(Default::default);
            status.conditions = Some(vec![serde_json::from_value(serde_json::json!({
                "type": "Established", "status": "True"
            }))
            .unwrap()]);
        }
        c
    }

    /// `TestValidateCABundle` (apiserver/pkg/util/webhook/validation_test.go).
    #[test]
    fn validate_ca_bundle_cases() {
        let p = Path::new("caBundle");
        assert!(validate_ca_bundle(&p, b"").is_empty());
        assert!(validate_ca_bundle(&p, CERT.as_bytes()).is_empty());
        let errs = validate_ca_bundle(&p, b"bogus");
        assert_eq!(errs.len(), 1);
        assert!(
            errs[0]
                .error_body()
                .contains("unable to load root certificates: unable to parse bytes as PEM block"),
            "{errs:?}"
        );
        assert_eq!(validate_ca_bundle(&p, b"Cg==").len(), 1);
    }

    /// "invalid CABundle should be allowed on Create" (validation_test.go:260).
    #[test]
    fn invalid_ca_bundle_allowed_on_create() {
        let c = crd_with_bundle(Some(b64(b"Cg==")), false);
        assert!(validate_custom_resource_definition(&c).is_empty());
    }

    /// "update to invalid CABundle should fail if existing is valid"
    /// (validation_test.go:5958) -- error at
    /// `spec.conversion.webhookClientConfig.caBundle`.
    #[test]
    fn update_to_invalid_ca_bundle_fails_if_existing_valid_and_established() {
        let old = crd_with_bundle(Some(b64(CERT.as_bytes())), true);
        let mut new = old.clone();
        new.spec
            .conversion
            .as_mut()
            .unwrap()
            .webhook
            .as_mut()
            .unwrap()
            .client_config
            .ca_bundle = Some(b64(b"Cg=="));
        let errs = validate_custom_resource_definition_update(&new, &old);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0]
                .to_string()
                .starts_with("spec.conversion.webhookClientConfig.caBundle: Invalid value"),
            "{errs:?}"
        );
    }

    /// "existing valid CABundle should be able to transition to invalid
    /// pre-serving" (validation_test.go:5844): not Established.
    #[test]
    fn invalid_ca_bundle_allowed_before_established() {
        let old = crd_with_bundle(Some(b64(CERT.as_bytes())), false);
        let mut new = old.clone();
        new.spec
            .conversion
            .as_mut()
            .unwrap()
            .webhook
            .as_mut()
            .unwrap()
            .client_config
            .ca_bundle = Some(b64(b"Cg=="));
        assert!(validate_custom_resource_definition_update(&new, &old).is_empty());
    }

    /// "existing invalid CABundle update should pass" (validation_test.go:5724).
    #[test]
    fn existing_invalid_ca_bundle_update_passes() {
        let old = crd_with_bundle(Some(b64(b"Cg==")), true);
        let new = old.clone();
        assert!(validate_custom_resource_definition_update(&new, &old).is_empty());
    }
}

#[cfg(test)]
#[path = "crd_structural_tests.rs"]
mod structural_schema_tests;
