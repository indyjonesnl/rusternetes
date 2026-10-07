//! PersistentVolumeClaim validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolumeClaim`,
//! `ValidatePersistentVolumeClaimSpec`, and `ValidatePersistentVolumeClaimUpdate`
//! (release-1.35).
//!
//! Covers the field-level checks that don't need cluster state: access modes,
//! the storage request, `storageClassName`, the `selector`, `dataSource` /
//! `dataSourceRef` consistency, and `volumeAttributesClassName`. The
//! `accessModes` and `volumeMode` enums are closed Rust enums, so an
//! out-of-range value is rejected at deserialization (matching upstream's
//! `NotSupported` set membership check).

use crate::quantity::{Format, Quantity};
use crate::resources::volume::{
    LabelSelector as VolumeLabelSelector, PersistentVolumeAccessMode, PersistentVolumeClaim,
    PersistentVolumeClaimPhase, PersistentVolumeClaimSpec, TypedLocalObjectReference,
    TypedObjectReference,
};
use crate::types::{
    LabelSelector as MetaLabelSelector, LabelSelectorRequirement as MetaLabelSelectorRequirement,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{
    is_dns1123_subdomain, is_qualified_name, validate_label_selector,
    LabelSelectorValidationOptions,
};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_immutable_field, validate_namespace_name, validate_object_meta,
    validate_object_meta_update, FIELD_IMMUTABLE_ERROR_MSG,
};
use std::collections::HashMap;

/// `core.BetaStorageClassAnnotation` (pkg/apis/core/types.go:345).
const BETA_STORAGE_CLASS_ANNOTATION: &str = "volume.beta.kubernetes.io/storage-class";

/// Convert the PVC-spec `volume::LabelSelector` to the structurally-identical
/// `types::LabelSelector` that `validate_label_selector` consumes.
fn to_meta_label_selector(sel: &VolumeLabelSelector) -> MetaLabelSelector {
    MetaLabelSelector {
        match_labels: sel.match_labels.clone(),
        match_expressions: sel.match_expressions.as_ref().map(|reqs| {
            reqs.iter()
                .map(|r| MetaLabelSelectorRequirement {
                    key: r.key.clone(),
                    operator: r.operator.clone(),
                    values: r.values.clone(),
                })
                .collect()
        }),
    }
}

/// Mirrors upstream `validateDataSource`. `dataSource` is a
/// `TypedLocalObjectReference` (no namespace).
fn validate_data_source(
    ds: &TypedLocalObjectReference,
    fld_path: &Path,
    allow_invalid_api_group_in_data_source_or_ref: bool,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if ds.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    }
    if ds.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    }
    let api_group = ds.api_group.as_deref().unwrap_or("");
    if api_group.is_empty() && ds.kind != "PersistentVolumeClaim" {
        errs.push(Error::invalid(
            fld_path,
            ds.kind.clone(),
            "must be 'PersistentVolumeClaim' when referencing the default apiGroup",
        ));
    }
    if !api_group.is_empty() && !allow_invalid_api_group_in_data_source_or_ref {
        for msg in is_dns1123_subdomain(api_group) {
            errs.push(Error::invalid(
                &fld_path.child("apiGroup"),
                api_group.to_string(),
                msg,
            ));
        }
    }
    errs
}

/// Mirrors upstream `validateDataSourceRef`. `dataSourceRef` is a
/// `TypedObjectReference` (may carry a cross-namespace reference).
fn validate_data_source_ref(
    dsr: &TypedObjectReference,
    fld_path: &Path,
    allow_invalid_api_group_in_data_source_or_ref: bool,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    if dsr.name.is_empty() {
        errs.push(Error::required(&fld_path.child("name"), ""));
    }
    if dsr.kind.is_empty() {
        errs.push(Error::required(&fld_path.child("kind"), ""));
    }
    let api_group = dsr.api_group.as_deref().unwrap_or("");
    if api_group.is_empty() && dsr.kind != "PersistentVolumeClaim" {
        errs.push(Error::invalid(
            fld_path,
            dsr.kind.clone(),
            "must be 'PersistentVolumeClaim' when referencing the default apiGroup",
        ));
    }
    if !api_group.is_empty() && !allow_invalid_api_group_in_data_source_or_ref {
        for msg in is_dns1123_subdomain(api_group) {
            errs.push(Error::invalid(
                &fld_path.child("apiGroup"),
                api_group.to_string(),
                msg,
            ));
        }
    }
    if let Some(ns) = &dsr.namespace {
        if !ns.is_empty() {
            for msg in validate_namespace_name(ns, false) {
                errs.push(Error::invalid(
                    &fld_path.child("namespace"),
                    ns.clone(),
                    msg,
                ));
            }
        }
    }
    errs
}

/// Upstream `isDataSourceEqualDataSourceRef`: a `dataSource` and `dataSourceRef`
/// are equivalent when apiGroup, kind, and name all match.
fn is_data_source_equal_data_source_ref(
    ds: &TypedLocalObjectReference,
    dsr: &TypedObjectReference,
) -> bool {
    ds.api_group == dsr.api_group && ds.kind == dsr.kind && ds.name == dsr.name
}

/// Validate a `PersistentVolumeClaimSpec`. Mirrors upstream
/// `ValidatePersistentVolumeClaimSpec`.
pub fn validate_persistent_volume_claim_spec(
    spec: &PersistentVolumeClaimSpec,
    fld_path: &Path,
) -> ErrorList {
    validate_persistent_volume_claim_spec_with_opts(
        spec,
        fld_path,
        &PersistentVolumeClaimSpecValidationOptions::default(),
    )
}

/// Upstream `PersistentVolumeClaimSpecValidationOptions` (validation.go:2317-2326).
/// `EnableRecoverFromExpansionFailure` and `EnableVolumeAttributesClass` are
/// GA and locked on in 1.35 (`RecoverVolumeExpansionFailure`,
/// pkg/features/kube_features.go:1680-1684; `VolumeAttributesClass` GA at 1.34,
/// :1900-1903), so they are not fields here: they are always true.
#[derive(Debug, Clone, Copy, Default)]
pub struct PersistentVolumeClaimSpecValidationOptions {
    /// Allow an invalid label value in the selector.
    pub allow_invalid_label_value_in_selector: bool,
    /// Allow an invalid API group in the data source or data source ref.
    pub allow_invalid_api_group_in_data_source_or_ref: bool,
}

/// Upstream `allowInvalidAPIGroupInDataSourceOrRef` (validation.go:2386-2394).
fn allow_invalid_api_group_in_data_source_or_ref(spec: &PersistentVolumeClaimSpec) -> bool {
    spec.data_source
        .as_ref()
        .is_some_and(|d| d.api_group.is_some())
        || spec
            .data_source_ref
            .as_ref()
            .is_some_and(|d| d.api_group.is_some())
}

/// Upstream `ValidationOptionsForPersistentVolumeClaim` (validation.go:2336-2364)
/// for an update: a claim already holding an invalid API group or label
/// selector keeps passing validation.
pub fn validation_options_for_persistent_volume_claim(
    old_pvc: &PersistentVolumeClaim,
) -> PersistentVolumeClaimSpecValidationOptions {
    let mut opts = PersistentVolumeClaimSpecValidationOptions {
        allow_invalid_api_group_in_data_source_or_ref:
            allow_invalid_api_group_in_data_source_or_ref(&old_pvc.spec),
        ..Default::default()
    };
    if let Some(sel) = &old_pvc.spec.selector {
        if !validate_label_selector(
            &to_meta_label_selector(sel),
            LabelSelectorValidationOptions::default(),
            &Path::new(""),
        )
        .is_empty()
        {
            opts.allow_invalid_label_value_in_selector = true;
        }
    }
    opts
}

/// `ValidatePersistentVolumeClaimSpec` (validation.go:2456-2535) with options.
pub fn validate_persistent_volume_claim_spec_with_opts(
    spec: &PersistentVolumeClaimSpec,
    fld_path: &Path,
    opts: &PersistentVolumeClaimSpecValidationOptions,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // accessModes: at least one is required. (Individual values are an enum, so
    // their validity is enforced at deserialization.)
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }

    // selector: when present, validate as a label selector (upstream
    // `ValidateLabelSelector`).
    if let Some(selector) = &spec.selector {
        errs.extend(validate_label_selector(
            &to_meta_label_selector(selector),
            LabelSelectorValidationOptions {
                allow_invalid_label_value_in_selector: opts.allow_invalid_label_value_in_selector,
                ..Default::default()
            },
            &fld_path.child("selector"),
        ));
    }

    // ReadWriteOncePod may not be combined with any other access mode.
    // validation.go:2475-2486: an unsupported mode is NotSupported and is not
    // counted as "another" mode next to ReadWriteOncePod.
    for mode in &spec.access_modes {
        if let PersistentVolumeAccessMode::Unknown(v) = mode {
            errs.push(Error::not_supported(
                &fld_path.child("accessModes"),
                v.clone(),
                crate::validation::persistentvolume::SUPPORTED_ACCESS_MODES,
            ));
        }
    }
    let has_rwop = spec
        .access_modes
        .iter()
        .any(|m| matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    let has_other = spec.access_modes.iter().any(|m| {
        !matches!(
            m,
            PersistentVolumeAccessMode::ReadWriteOncePod | PersistentVolumeAccessMode::Unknown(_)
        )
    });
    if has_rwop && has_other {
        errs.push(Error::forbidden(
            &fld_path.child("accessModes"),
            "may not use ReadWriteOncePod with other access modes",
        ));
    }

    // resources.requests[storage] is required and must be a positive quantity.
    let storage_path = fld_path
        .child("resources")
        .child("requests")
        .child("storage");
    match spec
        .resources
        .requests
        .as_ref()
        .and_then(|r| r.get("storage"))
    {
        None => errs.push(Error::required(&storage_path, "")),
        Some(val) => match Quantity::parse(val) {
            Err(_) => errs.push(Error::invalid(
                &storage_path,
                val.clone(),
                "must be a valid resource quantity",
            )),
            Ok(q) => {
                if q.is_negative() || q.is_zero() {
                    errs.push(Error::invalid(
                        &storage_path,
                        val.clone(),
                        "must be greater than 0",
                    ));
                }
            }
        },
    }

    // storageClassName, when set, must be a DNS-1123 subdomain (upstream
    // `ValidateClassName`).
    if let Some(scn) = &spec.storage_class_name {
        if !scn.is_empty() {
            for msg in is_dns1123_subdomain(scn) {
                errs.push(Error::invalid(
                    &fld_path.child("storageClassName"),
                    scn.clone(),
                    msg,
                ));
            }
        }
    }

    // validation.go:2504-2506 (`supportedVolumeModes`, sorted).
    if let Some(crate::resources::volume::PersistentVolumeMode::Unknown(v)) = &spec.volume_mode {
        errs.push(Error::not_supported(
            &fld_path.child("volumeMode"),
            v.clone(),
            &["Block", "Filesystem"],
        ));
    }

    // dataSource / dataSourceRef field-level validation.
    if let Some(ds) = &spec.data_source {
        errs.extend(validate_data_source(
            ds,
            &fld_path.child("dataSource"),
            opts.allow_invalid_api_group_in_data_source_or_ref,
        ));
    }
    if let Some(dsr) = &spec.data_source_ref {
        errs.extend(validate_data_source_ref(
            dsr,
            &fld_path.child("dataSourceRef"),
            opts.allow_invalid_api_group_in_data_source_or_ref,
        ));
    }

    // dataSource / dataSourceRef interaction (upstream block at validation.go
    // ~2514): if dataSourceRef carries a namespace, dataSource may not also be
    // set; otherwise if both are set they must be equal.
    let dsr_has_namespace = spec
        .data_source_ref
        .as_ref()
        .and_then(|r| r.namespace.as_ref())
        .is_some_and(|ns| !ns.is_empty());
    if dsr_has_namespace {
        if spec.data_source.is_some() {
            errs.push(Error::invalid(
                fld_path,
                fld_path.child("dataSource").to_string(),
                "may not be specified when dataSourceRef.namespace is specified",
            ));
        }
    } else if let (Some(ds), Some(dsr)) = (&spec.data_source, &spec.data_source_ref) {
        if !is_data_source_equal_data_source_ref(ds, dsr) {
            errs.push(Error::invalid(
                fld_path,
                fld_path.child("dataSource").to_string(),
                "must match dataSourceRef",
            ));
        }
    }

    // volumeAttributesClassName, when set, must be a DNS-1123 subdomain
    // (upstream `ValidateClassName`). The upstream feature-gate guard is always
    // open here.
    if let Some(vacn) = &spec.volume_attributes_class_name {
        if !vacn.is_empty() {
            for msg in is_dns1123_subdomain(vacn) {
                errs.push(Error::invalid(
                    &fld_path.child("volumeAttributesClassName"),
                    vacn.clone(),
                    msg,
                ));
            }
        }
    }

    errs
}

/// Upstream `ValidatePersistentVolumeClaim` (validation.go:2397-2401):
/// ObjectMeta (`ValidatePersistentVolumeName`, `NameIsDNSSubdomain`) and the
/// spec.
pub fn validate_persistent_volume_claim(pvc: &PersistentVolumeClaim) -> ErrorList {
    validate_persistent_volume_claim_with_opts(
        pvc,
        &PersistentVolumeClaimSpecValidationOptions::default(),
    )
}

/// `ValidatePersistentVolumeClaim` with explicit options.
pub fn validate_persistent_volume_claim_with_opts(
    pvc: &PersistentVolumeClaim,
    opts: &PersistentVolumeClaimSpecValidationOptions,
) -> ErrorList {
    let mut errs = validate_object_meta(
        &pvc.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_persistent_volume_claim_spec_with_opts(
        &pvc.spec,
        &Path::new("spec"),
        opts,
    ));
    errs
}

/// `resizeStatusSet` (validation.go:2666-2670).
const RESIZE_STATUSES: &[&str] = &[
    "ControllerResizeInProgress",
    "ControllerResizeInfeasible",
    "NodeResizePending",
    "NodeResizeInProgress",
    "NodeResizeInfeasible",
];

/// `validatePersistentVolumeClaimResourceKey` (validation.go:2648-2664): a
/// qualified name, and a native resource name must be `storage`.
fn validate_persistent_volume_claim_resource_key(value: &str, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = is_qualified_name(value)
        .into_iter()
        .map(|msg| Error::invalid(fld_path, value.to_string(), msg))
        .collect();
    if !errs.is_empty() {
        return errs;
    }
    // `helper.IsNativeResource` (pkg/apis/core/helper/helpers.go:198-201).
    let native = !value.contains('/') || value.contains("kubernetes.io/");
    if native && value != "storage" {
        errs.push(Error::not_supported(
            fld_path,
            value.to_string(),
            &["storage"],
        ));
    }
    errs
}

/// `validateBasicResource` (validation.go:7810-7815).
fn validate_basic_resource(quantity: &str, fld_path: &Path) -> ErrorList {
    match Quantity::parse(quantity) {
        Ok(q) if q.is_negative() => vec![Error::invalid(
            fld_path,
            q.value() as i64,
            "must be a valid resource quantity",
        )],
        _ => Vec::new(),
    }
}

/// Upstream `ValidatePersistentVolumeClaimStatusUpdate`
/// (validation.go:2673-2714), with `EnableRecoverFromExpansionFailure` on:
/// RecoverVolumeExpansionFailure is GA and locked on in 1.35
/// (pkg/features/kube_features.go:1680-1684).
pub fn validate_persistent_volume_claim_status_update(
    new_pvc: &PersistentVolumeClaim,
    old_pvc: &PersistentVolumeClaim,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new_pvc.metadata, &old_pvc.metadata, &Path::new("metadata"));
    if new_pvc
        .metadata
        .resource_version
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        errs.push(Error::required(&Path::new("resourceVersion"), ""));
    }
    if new_pvc.spec.access_modes.is_empty() {
        // Upstream spells this path `Spec.accessModes`, capitalised.
        errs.push(Error::required(&Path::new("Spec").child("accessModes"), ""));
    }
    let Some(status) = &new_pvc.status else {
        return errs;
    };
    let cap_path = Path::new("status").child("capacity");
    for (r, qty) in status.capacity.iter().flatten() {
        errs.extend(validate_basic_resource(qty, &cap_path.key(r.clone())));
    }
    let resize_path = Path::new("status").child("allocatedResourceStatuses");
    for (k, v) in status.allocated_resource_statuses.iter().flatten() {
        errs.extend(validate_persistent_volume_claim_resource_key(
            k,
            &resize_path,
        ));
        if !RESIZE_STATUSES.contains(&v.as_str()) {
            // Upstream reports the key, not the value, as the bad value.
            errs.push(Error::not_supported(
                &resize_path,
                k.clone(),
                RESIZE_STATUSES,
            ));
        }
    }
    let alloc_path = Path::new("status").child("allocatedResources");
    for (r, qty) in status.allocated_resources.iter().flatten() {
        let key_errs = validate_persistent_volume_claim_resource_key(r, &alloc_path);
        if !key_errs.is_empty() {
            errs.extend(key_errs);
            continue;
        }
        // `ValidateResourceQuantityValue(storage, …)` after it only adds the
        // non-negative check `validateBasicResource` already made.
        errs.extend(validate_basic_resource(qty, &alloc_path.key(r.clone())));
    }
    errs
}

/// Upstream `validateStorageClassUpgradeFromAnnotation`
/// (validation.go:2627-2634).
fn validate_storage_class_upgrade_from_annotation(
    old_annotations: Option<&HashMap<String, String>>,
    new_annotations: Option<&HashMap<String, String>>,
    old_sc_name: Option<&str>,
    new_sc_name: Option<&str>,
) -> bool {
    let old_sc = old_annotations.and_then(|a| a.get(BETA_STORAGE_CLASS_ANNOTATION));
    let new_sc_in_annotation = new_annotations.and_then(|a| a.get(BETA_STORAGE_CLASS_ANNOTATION));
    match old_sc {
        None => false, // condition 1
        Some(old_sc) => {
            old_sc_name.is_none() // condition 2
                && new_sc_name == Some(old_sc.as_str()) // condition 3
                && new_sc_in_annotation.is_none_or(|n| n == old_sc) // condition 4
        }
    }
}

/// Upstream `validateStorageClassUpgradeFromNil` (validation.go:2641-2646).
fn validate_storage_class_upgrade_from_nil(
    old_annotations: Option<&HashMap<String, String>>,
    old_sc_name: Option<&str>,
    new_sc_name: Option<&str>,
) -> bool {
    let old_annotation = old_annotations.and_then(|a| a.get(BETA_STORAGE_CLASS_ANNOTATION));
    match new_sc_name {
        None => false, // condition 1
        Some(new) => {
            old_sc_name.is_none() // condition 2
                && old_annotation.is_none_or(|a| a == new) // condition 3
        }
    }
}

/// Compare two `resources` quantity maps the way `apiequality.Semantic.DeepEqual`
/// does: nil and empty are equal, and quantities compare by value (`1Gi` ==
/// `1024Mi`).
fn quantity_maps_semantically_equal(
    a: &Option<HashMap<String, String>>,
    b: &Option<HashMap<String, String>>,
) -> bool {
    let empty = HashMap::new();
    let (a, b) = (a.as_ref().unwrap_or(&empty), b.as_ref().unwrap_or(&empty));
    a.len() == b.len()
        && a.iter().all(|(k, av)| match b.get(k) {
            None => false,
            Some(bv) => match (Quantity::parse(av), Quantity::parse(bv)) {
                (Ok(aq), Ok(bq)) => aq.value_eq(&bq),
                _ => av == bv,
            },
        })
}

/// Semantic equality of two claim specs (the `DeepEqual` at validation.go:2582).
fn specs_semantically_equal(a: &PersistentVolumeClaimSpec, b: &PersistentVolumeClaimSpec) -> bool {
    let strip = |s: &PersistentVolumeClaimSpec| {
        let mut s = s.clone();
        s.resources.requests = None;
        s.resources.limits = None;
        serde_json::to_value(&s).unwrap_or(serde_json::Value::Null)
    };
    quantity_maps_semantically_equal(&a.resources.requests, &b.resources.requests)
        && quantity_maps_semantically_equal(&a.resources.limits, &b.resources.limits)
        && strip(a) == strip(b)
}

/// Stand-in for upstream's `diff.Diff(old, new)` in the spec-immutable error:
/// the top-level spec fields that differ, old then new.
fn spec_diff(old: &PersistentVolumeClaimSpec, new: &PersistentVolumeClaimSpec) -> String {
    let o = serde_json::to_value(old).unwrap_or_default();
    let n = serde_json::to_value(new).unwrap_or_default();
    let empty = serde_json::Map::new();
    let (o, n) = (
        o.as_object().unwrap_or(&empty),
        n.as_object().unwrap_or(&empty),
    );
    let mut keys: Vec<&String> = o.keys().chain(n.keys()).collect();
    keys.sort();
    keys.dedup();
    let mut out = String::new();
    for k in keys {
        if o.get(k) != n.get(k) {
            out.push_str(&format!(
                "  {k}:\n-   {}\n+   {}\n",
                o.get(k).map_or("<absent>".into(), |v| v.to_string()),
                n.get(k).map_or("<absent>".into(), |v| v.to_string())
            ));
        }
    }
    out
}

fn storage_quantity(spec: &PersistentVolumeClaimSpec) -> Quantity {
    // A missing key is the zero Quantity in Go.
    spec.resources
        .requests
        .as_ref()
        .and_then(|m| m.get("storage"))
        .and_then(|v| Quantity::parse(v).ok())
        .unwrap_or_else(|| Quantity::from_value(0, Format::DecimalSI))
}

/// Upstream `ValidatePersistentVolumeClaimUpdate` (validation.go:2539-2618,
/// release-1.35) with the options the strategy derives
/// (`ValidationOptionsForPersistentVolumeClaim`, :2336-2364).
/// `EnableRecoverFromExpansionFailure` and `EnableVolumeAttributesClass` are
/// GA/locked on, so the "feature gate disabled" branch at :2602-2604 can not
/// fire. Deviation: the `spec is immutable` message carries a top-level-field
/// diff instead of Go's `diff.Diff` text.
pub fn validate_persistent_volume_claim_update(
    new_pvc: &PersistentVolumeClaim,
    old_pvc: &PersistentVolumeClaim,
) -> ErrorList {
    let opts = validation_options_for_persistent_volume_claim(old_pvc);
    let mut errs =
        validate_object_meta_update(&new_pvc.metadata, &old_pvc.metadata, &Path::new("metadata"));
    errs.extend(validate_persistent_volume_claim_with_opts(new_pvc, &opts));
    let mut new_clone = new_pvc.clone();
    let mut old_clone = old_pvc.clone();

    // PVController needs to update PVC.Spec w/ VolumeName; volumeName changes
    // are allowed once.
    if old_pvc.spec.volume_name.as_deref().unwrap_or("").is_empty() {
        old_clone.spec.volume_name = new_clone.spec.volume_name.clone();
    }

    if validate_storage_class_upgrade_from_annotation(
        old_clone.metadata.annotations.as_ref(),
        new_clone.metadata.annotations.as_ref(),
        old_clone.spec.storage_class_name.as_deref(),
        new_clone.spec.storage_class_name.as_deref(),
    ) {
        new_clone.spec.storage_class_name = None;
        let old_sc = old_clone
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(BETA_STORAGE_CLASS_ANNOTATION))
            .cloned()
            .unwrap_or_default();
        new_clone
            .metadata
            .annotations
            .get_or_insert_with(HashMap::new)
            .insert(BETA_STORAGE_CLASS_ANNOTATION.to_string(), old_sc);
    } else {
        // storageclass annotation should be immutable after creation
        let ann = |p: &PersistentVolumeClaim| {
            p.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(BETA_STORAGE_CLASS_ANNOTATION))
                .cloned()
                .unwrap_or_default()
        };
        let (new_val, old_val) = (ann(new_pvc), ann(old_pvc));
        if new_val != old_val {
            // ValidateImmutableAnnotation (validation.go:410-417)
            errs.push(Error::invalid(
                &Path::new("metadata")
                    .child("annotations")
                    .child(BETA_STORAGE_CLASS_ANNOTATION),
                new_val,
                FIELD_IMMUTABLE_ERROR_MSG,
            ));
        }
        // If update from annotation to attribute failed we can attempt try to
        // validate update from nil value.
        if validate_storage_class_upgrade_from_nil(
            old_pvc.metadata.annotations.as_ref(),
            old_pvc.spec.storage_class_name.as_deref(),
            new_pvc.spec.storage_class_name.as_deref(),
        ) {
            new_clone.spec.storage_class_name = old_clone.spec.storage_class_name.clone();
        }
    }

    let bound = matches!(
        new_pvc.status.as_ref().map(|s| &s.phase),
        Some(PersistentVolumeClaimPhase::Bound)
    );
    // lets make sure storage values are same.
    if bound {
        if let Some(reqs) = new_clone.spec.resources.requests.as_mut() {
            let old_storage = old_pvc
                .spec
                .resources
                .requests
                .as_ref()
                .and_then(|m| m.get("storage"))
                .cloned()
                .unwrap_or_else(|| "0".to_string());
            reqs.insert("storage".to_string(), old_storage);
        }
        // lets make sure volume attributes class name is same.
        new_clone.spec.volume_attributes_class_name =
            old_clone.spec.volume_attributes_class_name.clone();
    }

    let old_size = storage_quantity(&old_pvc.spec);
    let new_size = storage_quantity(&new_pvc.spec);
    let status_size = old_pvc
        .status
        .as_ref()
        .and_then(|s| s.capacity.as_ref())
        .and_then(|c| c.get("storage"))
        .and_then(|v| Quantity::parse(v).ok())
        .unwrap_or_else(|| Quantity::from_value(0, Format::DecimalSI));

    if !specs_semantically_equal(&new_clone.spec, &old_clone.spec) {
        errs.push(Error::forbidden(
            &Path::new("spec"),
            format!(
                "spec is immutable after creation except resources.requests and volumeAttributesClassName for bound claims\n{}",
                spec_diff(&old_clone.spec, &new_clone.spec)
            ),
        ));
    }
    if new_size.cmp_value(&old_size) == std::cmp::Ordering::Less {
        // This validation permits reducing pvc requested size up to capacity
        // recorded in pvc.status so that users can recover from volume
        // expansion failure (EnableRecoverFromExpansionFailure is on).
        if new_size.cmp_value(&status_size) != std::cmp::Ordering::Greater {
            errs.push(Error::forbidden(
                &Path::new("spec")
                    .child("resources")
                    .child("requests")
                    .child("storage"),
                "field can not be less than status.capacity",
            ));
        }
    }

    errs.extend(validate_immutable_field(
        &new_pvc.spec.volume_mode,
        &old_pvc.spec.volume_mode,
        &Path::new("volumeMode"),
    ));

    if old_pvc.spec.volume_attributes_class_name != new_pvc.spec.volume_attributes_class_name {
        // Forbid removing VAC once one is successfully applied.
        let vac_path = Path::new("spec").child("volumeAttributesClassName");
        if old_pvc
            .status
            .as_ref()
            .is_some_and(|s| s.current_volume_attributes_class_name.is_some())
        {
            match &new_pvc.spec.volume_attributes_class_name {
                None => errs.push(Error::forbidden(
                    &vac_path,
                    "update to nil is forbidden when status.currentVolumeAttributesClassName is not nil",
                )),
                Some(v) if v.is_empty() => errs.push(Error::forbidden(
                    &vac_path,
                    "update to empty string is forbidden when status.currentVolumeAttributesClassName is not nil",
                )),
                Some(_) => {}
            }
        }
    }

    errs
}
