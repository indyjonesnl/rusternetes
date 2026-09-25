//! CustomResourceDefinition validation and defaulting — port of upstream
//! `staging/src/k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation/validation.go`
//! and `.../apiextensions/v1/defaults.go` (release-1.35).
//!
//! Scope: the structural half of `validateCustomResourceDefinitionSpec`
//! (`:353`) — group, scope, the version set (unique DNS-1035 names, exactly one
//! storage version), `ValidateCustomResourceDefinitionNames` (`:785`),
//! `ValidateCustomResourceColumnDefinition` (`:821`), the `jsonPath`-required
//! half of `ValidateCustomResourceSelectableFields` (`:856`),
//! `ValidateCustomResourceDefinitionSubresources` (`:1525`) and
//! `validateCustomResourceConversion` (`:612`).
//!
//! Out of scope here: the structural-schema and CEL rule checks, which
//! `handlers::cel_validation` already runs, and `ValidFieldPath` resolution of
//! a selectable field against that schema (it needs the structural schema).
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
    ResourceScope,
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
/// `acceptedConversionReviewVersions` (`validation.go:560`).
const ACCEPTED_CONVERSION_REVIEW_VERSIONS: &[&str] = &["v1", "v1beta1"];

/// `SetDefaults_CustomResourceDefinitionSpec`
/// (`apiextensions/v1/defaults.go:41-53`). The `status.storedVersions` half of
/// `SetDefaults_CustomResourceDefinition` (`:29-38`) is already done by the
/// create handler, which seeds the whole status block. Must run *before* validation: the
/// `names.singular` / `names.listKind` / `conversion.strategy` rules below are
/// all satisfied by these defaults, exactly as upstream's are.
pub fn set_defaults_custom_resource_definition(crd: &mut CustomResourceDefinition) {
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

/// `validateCustomResourceDefinitionSpec` (`validation.go:353`), minus the
/// schema-structural half (see the module doc).
pub fn validate_custom_resource_definition_spec(
    spec: &CustomResourceDefinitionSpec,
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
    if spec.scope == ResourceScope::Unspecified {
        errs.push(Error::required(&fld_path.child("scope"), ""));
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
            let fields_path = vp.child("selectableFields");
            let mut seen_paths: HashSet<&str> = HashSet::new();
            for (j, selectable) in fields.iter().enumerate() {
                let sp = fields_path.index(j).child("jsonPath");
                if selectable.json_path.is_empty() {
                    errs.push(Error::required(&sp, ""));
                    continue;
                }
                if !seen_paths.insert(selectable.json_path.as_str()) {
                    errs.push(Error::duplicate(&sp, selectable.json_path.clone()));
                }
            }
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
    errs.extend(validate_conversion(spec, &fld_path.child("conversion")));

    errs
}

/// The spec-level required names (`validation.go:456-470`) on top of
/// `ValidateCustomResourceDefinitionNames` (`:785`). `singular` and `listKind`
/// are required here because defaulting has already filled them.
fn validate_names(names: &CustomResourceDefinitionNames, fld_path: &Path) -> ErrorList {
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
fn validate_conversion(spec: &CustomResourceDefinitionSpec, fld_path: &Path) -> ErrorList {
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
        Some(ConversionStrategyType::None) => {
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
