//! PersistentVolume validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolume` (release-1.35).
//!
//! Scope: capacity (storage required, storage-only, non-negative), access modes
//! (≥1 + ReadWriteOncePod exclusivity), exactly one volume source,
//! nodeAffinity-required-for-Local, the hostPath-'/'-with-Recycle prohibition,
//! and storageClassName, plus the per-source field validation each
//! `numVolumes++` carries upstream and `validateVolumeNodeAffinity`.
//!
//! reclaimPolicy / volumeMode / accessModes enum-membership (upstream
//! `supportedReclaimPolicy` / `supportedVolumeModes` / `supportedAccessModes`)
//! is enforced upstream-of-validation by Rusternetes' typed enums: an unknown
//! string fails to deserialize before this validator runs, so no explicit
//! `NotSupported` check is reproduced here.

use crate::quantity::Quantity;
use crate::resources::volume::{
    CSIVolumeSource, HostPathVolumeSource, ISCSIVolumeSource, LocalVolumeSource, NodeSelector,
    NodeSelectorRequirement, NodeSelectorTerm, PersistentVolume, PersistentVolumeAccessMode,
    PersistentVolumeMode, PersistentVolumeReclaimPolicy, PersistentVolumeSpec, SecretReference,
    VolumeNodeAffinity,
};
use crate::validation::csinode::validate_csi_driver_name;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::{is_dns1123_label, is_dns1123_subdomain};
use crate::validation::pod::{validate_nfs_volume_source, validate_node_selector};
use once_cell::sync::Lazy;
use regex::Regex;

/// Upstream `iscsiInitiatorIqnRegex` / `iscsiInitiatorEuiRegex` /
/// `iscsiInitiatorNaaRegex` (`pkg/apis/core/validation/validation.go:89-91`).
/// Go's `[[:alnum:]]` is `[0-9A-Za-z]`; the Rust `regex` crate spells POSIX
/// classes the same way inside a character class, so the patterns carry over
/// verbatim apart from that.
static ISCSI_IQN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"iqn\.\d{4}-\d{2}\.([[:alnum:]\-.]+)(:[^,;*&$|\s]+)$").expect("iscsi iqn regex")
});
static ISCSI_EUI_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^eui.[[:alnum:]]{16}$").expect("iscsi eui regex"));
static ISCSI_NAA_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^naa.[[:alnum:]]{32}$").expect("iscsi naa regex"));

/// Lexically normalise a path, mirroring Go's `path.Clean` for the cases that
/// matter to the hostPath-root check: collapse repeated slashes and resolve
/// `.` / `..` elements, returning "/" for any path that reduces to the root.
fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if let Some(&last) = out.last() {
                    if last != ".." {
                        out.pop();
                        continue;
                    }
                }
                if !rooted {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

/// Port of upstream `validatePathNoBacksteps`
/// (`pkg/apis/core/validation/validation.go:754-764`): one error is enough,
/// even for `../../..`.
fn validate_path_no_backsteps(target_path: &str, fld_path: &Path) -> ErrorList {
    if target_path.split('/').any(|part| part == "..") {
        return vec![Error::invalid(
            fld_path,
            target_path.to_string(),
            "must not contain '..'",
        )];
    }
    Vec::new()
}

/// Port of upstream `validateHostPathVolumeSource`
/// (`pkg/apis/core/validation/validation.go:816-826`). The `type` enum is a
/// closed Rust enum, so upstream's `validateHostPathType` membership check is
/// answered at decode time.
fn validate_host_path_volume_source(hp: &HostPathVolumeSource, fld_path: &Path) -> ErrorList {
    if hp.path.is_empty() {
        return vec![Error::required(&fld_path.child("path"), "")];
    }
    validate_path_no_backsteps(&hp.path, &fld_path.child("path"))
}

/// Port of upstream `validateLocalVolumeSource`
/// (`pkg/apis/core/validation/validation.go:1735-1744`).
fn validate_local_volume_source(ls: &LocalVolumeSource, fld_path: &Path) -> ErrorList {
    if ls.path.is_empty() {
        return vec![Error::required(&fld_path.child("path"), "")];
    }
    validate_path_no_backsteps(&ls.path, &fld_path.child("path"))
}

/// Port of upstream `validatePVSecretReference`
/// (`pkg/apis/core/validation/validation.go:1829-1843`): unlike the pod-side
/// `LocalObjectReference`, a PV secret reference carries a namespace and both
/// halves are required.
fn validate_pv_secret_reference(secret_ref: &SecretReference, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();
    match secret_ref.name.as_deref().unwrap_or("") {
        "" => errs.push(Error::required(&fld_path.child("name"), "")),
        name => {
            for msg in is_dns1123_subdomain(name) {
                errs.push(Error::invalid(
                    &fld_path.child("name"),
                    name.to_string(),
                    msg,
                ));
            }
        }
    }
    match secret_ref.namespace.as_deref().unwrap_or("") {
        "" => errs.push(Error::required(&fld_path.child("namespace"), "")),
        ns => {
            for msg in is_dns1123_label(ns) {
                errs.push(Error::invalid(
                    &fld_path.child("namespace"),
                    ns.to_string(),
                    msg,
                ));
            }
        }
    }
    errs
}

/// Port of upstream `validateCSIPersistentVolumeSource`
/// (`pkg/apis/core/validation/validation.go:1848-1869`). Note this is the
/// *persistent* variant: `volumeHandle` is required and every secret reference
/// is a namespaced `SecretReference`, where the pod-inline `CSIVolumeSource`
/// (`:1871-1887`) has neither.
fn validate_csi_persistent_volume_source(csi: &CSIVolumeSource, fld_path: &Path) -> ErrorList {
    let mut errs = validate_csi_driver_name(&csi.driver, &fld_path.child("driver"));

    if csi.volume_handle.as_deref().unwrap_or("").is_empty() {
        errs.push(Error::required(&fld_path.child("volumeHandle"), ""));
    }
    for (secret_ref, child) in [
        (
            &csi.controller_publish_secret_ref,
            "controllerPublishSecretRef",
        ),
        (
            &csi.controller_expand_secret_ref,
            "controllerExpandSecretRef",
        ),
        (&csi.node_publish_secret_ref, "nodePublishSecretRef"),
        (&csi.node_expand_secret_ref, "nodeExpandSecretRef"),
    ] {
        if let Some(r) = secret_ref {
            errs.extend(validate_pv_secret_reference(r, &fld_path.child(child)));
        }
    }
    errs
}

/// Port of upstream `validateISCSIPersistentVolumeSource`
/// (`pkg/apis/core/validation/validation.go:879-928`). The differences from the
/// pod-inline `validateISCSIVolumeSource` are the `<pv name>:<targetPortal>`
/// length bound that `initiatorName` imposes and the namespaced `secretRef`.
fn validate_iscsi_persistent_volume_source(
    iscsi: &ISCSIVolumeSource,
    pv_name: &str,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    if iscsi.target_portal.is_empty() {
        errs.push(Error::required(&fld_path.child("targetPortal"), ""));
    }
    if iscsi.initiator_name.is_some() && pv_name.len() + 1 + iscsi.target_portal.len() > 64 {
        errs.push(Error::invalid(
            &fld_path.child("targetportal"),
            iscsi.target_portal.clone(),
            "Total length of <volume name>:<iscsi.targetPortal> must be under 64 characters if              iscsi.initiatorName is specified.",
        ));
    }
    if iscsi.iqn.is_empty() {
        errs.push(Error::required(&fld_path.child("iqn"), ""));
    } else {
        errs.extend(validate_iscsi_name(&iscsi.iqn, &fld_path.child("iqn")));
    }
    if !(0..=255).contains(&iscsi.lun) {
        errs.push(Error::invalid(
            &fld_path.child("lun"),
            iscsi.lun,
            "must be between 0 and 255, inclusive",
        ));
    }
    let chap =
        iscsi.chap_auth_discovery.unwrap_or(false) || iscsi.chap_auth_session.unwrap_or(false);
    match (&iscsi.secret_ref, chap) {
        (None, true) => errs.push(Error::required(&fld_path.child("secretRef"), "")),
        (Some(r), _) => {
            if r.name.as_deref().unwrap_or("").is_empty() {
                errs.push(Error::required(
                    &fld_path.child("secretRef").child("name"),
                    "",
                ));
            }
        }
        (None, false) => {}
    }
    if let Some(initiator) = &iscsi.initiator_name {
        errs.extend(validate_iscsi_name(
            initiator,
            &fld_path.child("initiatorname"),
        ));
    }
    errs
}

/// The iqn / initiatorName format check shared by both halves of
/// `validateISCSIPersistentVolumeSource`
/// (`pkg/apis/core/validation/validation.go:888-898`, `:918-927`): a recognised
/// prefix, then the prefix's own regex.
fn validate_iscsi_name(name: &str, fld_path: &Path) -> ErrorList {
    let ok = if name.starts_with("iqn") {
        ISCSI_IQN_RE.is_match(name)
    } else if name.starts_with("eui") {
        ISCSI_EUI_RE.is_match(name)
    } else if name.starts_with("naa") {
        ISCSI_NAA_RE.is_match(name)
    } else {
        false
    };
    if ok {
        Vec::new()
    } else {
        vec![Error::invalid(
            fld_path,
            name.to_string(),
            "must be valid format",
        )]
    }
}

/// Port of upstream `validateVolumeNodeAffinity`
/// (`pkg/apis/core/validation/validation.go:8701-8714`): `required` is not
/// optional once `nodeAffinity` is set.
fn validate_volume_node_affinity(na: &VolumeNodeAffinity, fld_path: &Path) -> ErrorList {
    let Some(required) = &na.required else {
        return vec![Error::required(
            &fld_path.child("required"),
            "must specify required node constraints",
        )];
    };
    validate_node_selector(&to_pod_node_selector(required), &fld_path.child("required"))
}

/// `volume.rs` carries its own structurally-identical copy of the node-selector
/// types, so convert to the `resources::pod` shapes that
/// `validation::pod::validate_node_selector` consumes — the same approach
/// `pvc.rs::to_meta_label_selector` takes for label selectors.
fn to_pod_node_selector(sel: &NodeSelector) -> crate::resources::pod::NodeSelector {
    crate::resources::pod::NodeSelector {
        node_selector_terms: sel.node_selector_terms.iter().map(to_pod_term).collect(),
    }
}

fn to_pod_term(term: &NodeSelectorTerm) -> crate::resources::pod::NodeSelectorTerm {
    let convert = |reqs: &Option<Vec<NodeSelectorRequirement>>| {
        reqs.as_ref().map(|rs| {
            rs.iter()
                .map(|r| crate::resources::pod::NodeSelectorRequirement {
                    key: r.key.clone(),
                    operator: r.operator.clone(),
                    values: r.values.clone(),
                })
                .collect()
        })
    };
    crate::resources::pod::NodeSelectorTerm {
        match_expressions: convert(&term.match_expressions),
        match_fields: convert(&term.match_fields),
    }
}

/// Validate a `PersistentVolumeSpec`. Mirrors the core of upstream
/// `ValidatePersistentVolume`.
pub fn validate_persistent_volume_spec(
    spec: &PersistentVolumeSpec,
    pv_name: &str,
    inline: bool,
    fld_path: &Path,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    let capacity_path = fld_path.child("capacity");

    // Inline-volume-only deltas (upstream `validateInlinePersistentVolumeSpec`,
    // core validation.go:1968-1978): claimRef + capacity are forbidden and a CSI
    // source is required, because an inline PV (VolumeAttachment.inlineVolumeSpec)
    // is not a standalone object.
    if inline {
        if spec.claim_ref.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("claimRef"),
                "may not be specified in the context of inline volumes",
            ));
        }
        if !spec.capacity.is_empty() {
            errs.push(Error::forbidden(
                &capacity_path,
                "may not be specified in the context of inline volumes",
            ));
        }
        if spec.csi.is_none() {
            errs.push(Error::required(
                &fld_path.child("csi"),
                "has to be specified in the context of inline volumes",
            ));
        }
    }

    // capacity is required (upstream line ~2002). Then it must hold exactly the
    // `storage` resource and nothing else (upstream line ~2005-2007).
    // Upstream uses two independent `if`s (not else-if): an empty capacity is
    // both Required AND NotSupported (storage absent). Match that exactly.
    // These run only for standalone PVs — inline volumes forbid capacity above.
    if !inline {
        if spec.capacity.is_empty() {
            errs.push(Error::required(&capacity_path, ""));
        }
        if !spec.capacity.contains_key("storage") || spec.capacity.len() > 1 {
            errs.push(Error::not_supported(
                &capacity_path,
                "<capacity>",
                &["storage"],
            ));
        }

        // Every capacity quantity must parse and be a non-negative value
        // (upstream validateBasicResource + ValidatePositiveQuantityValue, ~2009-2012).
        for (resource, value) in &spec.capacity {
            let key_path = capacity_path.key(resource.clone());
            match Quantity::parse(value) {
                Err(_) => errs.push(Error::invalid(
                    &key_path,
                    value.clone(),
                    "must be a valid resource quantity",
                )),
                Ok(q) => {
                    if q.is_negative() {
                        errs.push(Error::invalid(
                            &key_path,
                            value.clone(),
                            "must be greater than or equal to 0",
                        ));
                    }
                }
            }
        }
    }

    // accessModes: at least one; ReadWriteOncePod may not combine with others.
    if spec.access_modes.is_empty() {
        errs.push(Error::required(
            &fld_path.child("accessModes"),
            "at least 1 access mode is required",
        ));
    }
    let has_rwop = spec
        .access_modes
        .iter()
        .any(|m| matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    let has_other = spec
        .access_modes
        .iter()
        .any(|m| !matches!(m, PersistentVolumeAccessMode::ReadWriteOncePod));
    if has_rwop && has_other {
        errs.push(Error::forbidden(
            &fld_path.child("accessModes"),
            "may not use ReadWriteOncePod with other access modes",
        ));
    }

    // nodeAffinity, when set, must carry `required` and a valid node selector
    // (upstream `validateVolumeNodeAffinity`, validation.go:8701-8714). Upstream
    // runs this *before* the source block, so a PV that is wrong in both places
    // reports the affinity error first.
    if let Some(na) = &spec.node_affinity {
        if !inline {
            errs.extend(validate_volume_node_affinity(
                na,
                &fld_path.child("nodeAffinity"),
            ));
        }
    }

    // Exactly one volume source must be specified, and that source's own fields
    // are validated next to its `numVolumes++` (upstream
    // `ValidatePersistentVolumeSpec`, validation.go:2037-2135). Counting the
    // sources without validating them was the gap this closes: a PV whose only
    // source was `{"nfs": {"server": "x"}}` was written with no path at all.
    let mut num_volumes = 0usize;
    macro_rules! source {
        ($opt:expr, $child:expr, $validate:expr) => {
            if let Some(src) = $opt {
                let child_path = fld_path.child($child);
                if num_volumes > 0 {
                    errs.push(Error::forbidden(
                        &child_path,
                        "may not specify more than 1 volume type",
                    ));
                } else {
                    num_volumes += 1;
                    #[allow(clippy::redundant_closure_call)]
                    errs.extend($validate(src, &child_path));
                }
            }
        };
    }
    source!(
        &spec.host_path,
        "hostPath",
        validate_host_path_volume_source
    );
    source!(&spec.nfs, "nfs", validate_nfs_volume_source);
    source!(&spec.iscsi, "iscsi", |iscsi, p| {
        validate_iscsi_persistent_volume_source(iscsi, pv_name, p)
    });
    source!(&spec.local, "local", validate_local_volume_source);
    source!(&spec.csi, "csi", validate_csi_persistent_volume_source);
    if num_volumes == 0 {
        errs.push(Error::required(fld_path, "must specify a volume type"));
    }

    // A Local volume requires node affinity (upstream line ~2194-2197).
    if spec.local.is_some() && spec.node_affinity.is_none() {
        errs.push(Error::required(
            &fld_path.child("nodeAffinity"),
            "Local volume requires node affinity",
        ));
    }

    // A hostPath mount of '/' may not use the Recycle reclaim policy
    // (upstream line ~2222-2225).
    if let Some(hp) = &spec.host_path {
        if clean_path(&hp.path) == "/"
            && spec.persistent_volume_reclaim_policy == Some(PersistentVolumeReclaimPolicy::Recycle)
        {
            errs.push(Error::forbidden(
                &fld_path.child("persistentVolumeReclaimPolicy"),
                "may not be 'recycle' for a hostPath mount of '/'",
            ));
        }
    }

    // reclaimPolicy: inline volumes may only use Retain (upstream
    // validation.go:~2018). Standalone PVs accept the full supported set
    // (enum validation handled elsewhere); the hostPath '/' Recycle case above
    // is independent.
    if inline {
        if let Some(policy) = &spec.persistent_volume_reclaim_policy {
            if *policy != PersistentVolumeReclaimPolicy::Retain {
                errs.push(Error::forbidden(
                    &fld_path.child("persistentVolumeReclaimPolicy"),
                    "may only be Retain in the context of inline volumes",
                ));
            }
        }
        // nodeAffinity may not be specified for inline volumes (validation.go:~2228).
        if spec.node_affinity.is_some() {
            errs.push(Error::forbidden(
                &fld_path.child("nodeAffinity"),
                "may not be specified in the context of inline volumes",
            ));
        }
        // volumeMode, when set, must be Filesystem for inline volumes
        // (validation.go:~2237).
        if let Some(mode) = &spec.volume_mode {
            if *mode != PersistentVolumeMode::Filesystem {
                errs.push(Error::forbidden(
                    &fld_path.child("volumeMode"),
                    "may not specify volumeMode other than Filesystem in the context of inline volumes",
                ));
            }
        }
    }

    // storageClassName: forbidden for inline volumes; otherwise, when set, must
    // be a DNS-1123 subdomain (upstream validation.go:~2228-2233).
    if let Some(scn) = &spec.storage_class_name {
        if inline {
            if !scn.is_empty() {
                errs.push(Error::forbidden(
                    &fld_path.child("storageClassName"),
                    "may not be specified in the context of inline volumes",
                ));
            }
        } else if !scn.is_empty() {
            for msg in is_dns1123_subdomain(scn) {
                errs.push(Error::invalid(
                    &fld_path.child("storageClassName"),
                    scn.clone(),
                    msg,
                ));
            }
        }
    }

    errs
}

/// Validate a new `PersistentVolume`. Mirrors upstream `ValidatePersistentVolume`.
pub fn validate_persistent_volume(pv: &PersistentVolume) -> ErrorList {
    validate_persistent_volume_spec(&pv.spec, &pv.metadata.name, false, &Path::new("spec"))
}

/// JSON view of just the volume-source union of a `PersistentVolumeSpec`
/// (`PersistentVolumeSource` upstream) — the fields that are immutable after
/// creation. Capacity / accessModes / reclaimPolicy etc. are intentionally
/// excluded.
fn persistent_volume_source(spec: &PersistentVolumeSpec) -> serde_json::Value {
    serde_json::json!({
        "hostPath": spec.host_path,
        "nfs": spec.nfs,
        "iscsi": spec.iscsi,
        "local": spec.local,
        "csi": spec.csi,
    })
}

/// Validate a `PersistentVolume` on update. Mirrors upstream
/// `ValidatePersistentVolumeUpdate`: re-run create validation, then enforce that
/// the volume source and `volumeMode` are immutable. The CSI
/// `controllerExpandSecretRef` may be set when it was previously unset (allowed
/// for volume expansion), so it is excluded from the source-immutability diff in
/// that case.
pub fn validate_persistent_volume_update(
    new: &PersistentVolume,
    old: &PersistentVolume,
) -> ErrorList {
    let mut errs = validate_persistent_volume(new);

    // Allow first-time setting of csi.controllerExpandSecretRef: normalise the
    // new spec to drop it before the source diff when old had none.
    let mut new_spec = new.spec.clone();
    let old_had_expand_ref = old
        .spec
        .csi
        .as_ref()
        .map(|c| c.controller_expand_secret_ref.is_some())
        .unwrap_or(false);
    if !old_had_expand_ref {
        if let Some(csi) = new_spec.csi.as_mut() {
            csi.controller_expand_secret_ref = None;
        }
    }

    if persistent_volume_source(&new_spec) != persistent_volume_source(&old.spec) {
        errs.push(Error::forbidden(
            &Path::new("spec").child("persistentvolumesource"),
            "spec.persistentvolumesource is immutable after creation",
        ));
    }

    if new.spec.volume_mode != old.spec.volume_mode {
        errs.push(Error::invalid(
            &Path::new("spec").child("volumeMode"),
            format!("{:?}", new.spec.volume_mode),
            "field is immutable",
        ));
    }

    // nodeAffinity: immutable once set (upstream validatePvNodeAffinity, with the
    // default-off MutablePVNodeAffinity gate). A nil → set transition is allowed.
    // The beta→GA topology-label masking carve-out is not modelled.
    if old.spec.node_affinity.is_some() {
        let na_eq = serde_json::to_value(&new.spec.node_affinity).ok()
            == serde_json::to_value(&old.spec.node_affinity).ok();
        if !na_eq {
            errs.push(Error::invalid(
                &Path::new("spec").child("nodeAffinity"),
                "<nodeAffinity>".to_string(),
                "field is immutable",
            ));
        }
    }

    // volumeAttributesClassName: with the VolumeAttributesClass feature enabled
    // (beta-on by 1.35), an existing class may be changed but not cleared.
    if old.spec.volume_attributes_class_name.is_some()
        && new.spec.volume_attributes_class_name.is_none()
    {
        errs.push(Error::forbidden(
            &Path::new("spec").child("volumeAttributesClassName"),
            "update from non-nil value to nil is forbidden",
        ));
    }

    errs
}

#[cfg(test)]
mod create_tests {
    use super::*;

    fn pv(json: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json).unwrap()
    }

    fn valid_hostpath() -> serde_json::Value {
        serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "hostPath": {"path": "/data"}
            }
        })
    }

    #[test]
    fn clean_path_resolves_root() {
        assert_eq!(clean_path("/"), "/");
        assert_eq!(clean_path("//"), "/");
        assert_eq!(clean_path("/."), "/");
        assert_eq!(clean_path("/foo/.."), "/");
        assert_eq!(clean_path("/foo/../bar"), "/bar");
        assert_eq!(clean_path("/data"), "/data");
    }

    #[test]
    fn valid_pv_passes() {
        assert!(validate_persistent_volume(&pv(valid_hostpath())).is_empty());
    }

    #[test]
    fn capacity_required() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("capacity")),
            "{errs:?}"
        );
    }

    #[test]
    fn capacity_must_be_storage_only() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"storage": "1Gi", "cpu": "1"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("capacity") && e.detail.contains("supported values")),
            "{errs:?}"
        );
    }

    #[test]
    fn capacity_missing_storage_key_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"cpu": "1"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("capacity")),
            "{errs:?}"
        );
    }

    #[test]
    fn negative_capacity_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["capacity"] = serde_json::json!({"storage": "-1Gi"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.detail.contains("greater than or equal to 0")),
            "{errs:?}"
        );
    }

    #[test]
    fn access_modes_required() {
        let mut v = valid_hostpath();
        v["spec"]["accessModes"] = serde_json::json!([]);
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("accessModes")),
            "{errs:?}"
        );
    }

    #[test]
    fn rwop_with_other_modes_forbidden() {
        let mut v = valid_hostpath();
        v["spec"]["accessModes"] = serde_json::json!(["ReadWriteOncePod", "ReadWriteOnce"]);
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not use ReadWriteOncePod with other access modes")),
            "{errs:?}"
        );
    }

    #[test]
    fn no_volume_source_rejected() {
        let mut v = valid_hostpath();
        v["spec"].as_object_mut().unwrap().remove("hostPath");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("must specify a volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn more_than_one_volume_source_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["nfs"] = serde_json::json!({"server": "1.2.3.4", "path": "/exports"});
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not specify more than 1 volume type")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_requires_node_affinity() {
        let v = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "local": {"path": "/mnt/disk"}
            }
        }));
        let errs = validate_persistent_volume(&v);
        assert!(
            errs.iter().any(|e| e.field.ends_with("nodeAffinity")
                && e.to_string()
                    .contains("Local volume requires node affinity")),
            "{errs:?}"
        );
    }

    #[test]
    fn local_with_node_affinity_passes() {
        let v = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "local": {"path": "/mnt/disk"},
                "nodeAffinity": {"required": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}
                ]}}
            }
        }));
        assert!(validate_persistent_volume(&v).is_empty());
    }

    #[test]
    fn hostpath_root_with_recycle_forbidden() {
        let mut v = valid_hostpath();
        v["spec"]["hostPath"]["path"] = serde_json::json!("/");
        v["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Recycle");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("may not be 'recycle' for a hostPath mount of '/'")),
            "{errs:?}"
        );
    }

    #[test]
    fn hostpath_nonroot_with_recycle_ok() {
        let mut v = valid_hostpath();
        v["spec"]["persistentVolumeReclaimPolicy"] = serde_json::json!("Recycle");
        // path is /data, not '/', so Recycle is allowed
        assert!(validate_persistent_volume(&pv(v)).is_empty());
    }

    #[test]
    fn invalid_storage_class_name_rejected() {
        let mut v = valid_hostpath();
        v["spec"]["storageClassName"] = serde_json::json!("Bad_Name");
        let errs = validate_persistent_volume(&pv(v));
        assert!(
            errs.iter().any(|e| e.field.ends_with("storageClassName")),
            "{errs:?}"
        );
    }
}

#[cfg(test)]
mod update_tests {
    use super::*;

    fn pv(json: serde_json::Value) -> PersistentVolume {
        serde_json::from_value(json).unwrap()
    }

    fn hostpath_pv(path: &str) -> PersistentVolume {
        pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {
                "capacity": {"storage": "1Gi"},
                "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Retain",
                "hostPath": {"path": path}
            }
        }))
    }

    #[test]
    fn unchanged_passes() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/data");
        assert!(validate_persistent_volume_update(&new, &old).is_empty());
    }

    #[test]
    fn changed_source_rejected() {
        let old = hostpath_pv("/data");
        let new = hostpath_pv("/other");
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("persistentvolumesource is immutable")),
            "{errs:?}"
        );
    }

    #[test]
    fn changed_volume_mode_rejected() {
        let mut old = hostpath_pv("/data");
        old.spec.volume_mode = Some(crate::resources::volume::PersistentVolumeMode::Filesystem);
        let mut new = hostpath_pv("/data");
        new.spec.volume_mode = Some(crate::resources::volume::PersistentVolumeMode::Block);
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("volumeMode") && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    #[test]
    fn first_time_csi_expand_secret_ref_allowed() {
        let old = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Delete",
                "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1"}}
        }));
        let new = pv(serde_json::json!({
            "metadata": {"name": "pv"},
            "spec": {"capacity": {"storage": "1Gi"}, "accessModes": ["ReadWriteOnce"],
                "persistentVolumeReclaimPolicy": "Delete",
                "csi": {"driver": "csi.example.com", "volumeHandle": "vol-1",
                    "controllerExpandSecretRef": {"name": "s", "namespace": "ns"}}}
        }));
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            !errs
                .iter()
                .any(|e| e.to_string().contains("persistentvolumesource")),
            "{errs:?}"
        );
    }

    #[test]
    fn node_affinity_immutable_once_set() {
        let na = serde_json::json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n1"]}]}
        ]}});
        let mut old = hostpath_pv("/data");
        old.spec.node_affinity = serde_json::from_value(na.clone()).unwrap();
        // unchanged -> ok
        let mut same = hostpath_pv("/data");
        same.spec.node_affinity = serde_json::from_value(na).unwrap();
        assert!(validate_persistent_volume_update(&same, &old).is_empty());
        // changed -> immutable
        let mut changed = hostpath_pv("/data");
        changed.spec.node_affinity = serde_json::from_value(serde_json::json!({"required": {"nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": ["n2"]}]}
        ]}})).unwrap();
        let errs = validate_persistent_volume_update(&changed, &old);
        assert!(
            errs.iter()
                .any(|e| e.field.ends_with("nodeAffinity") && e.detail == "field is immutable"),
            "{errs:?}"
        );
    }

    #[test]
    fn node_affinity_may_be_set_when_old_nil() {
        let old = hostpath_pv("/data"); // no nodeAffinity
        let mut new = hostpath_pv("/data");
        new.spec.node_affinity =
            serde_json::from_value(serde_json::json!({"required": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": "k", "operator": "Exists"}]}
            ]}}))
            .unwrap();
        assert!(validate_persistent_volume_update(&new, &old).is_empty());
    }

    #[test]
    fn vac_name_may_not_be_cleared() {
        let mut old = hostpath_pv("/data");
        old.spec.volume_attributes_class_name = Some("gold".to_string());
        let new = hostpath_pv("/data"); // VAC cleared
        let errs = validate_persistent_volume_update(&new, &old);
        assert!(
            errs.iter()
                .any(|e| e.to_string().contains("non-nil value to nil is forbidden")),
            "{errs:?}"
        );
        // changing to another class is allowed
        let mut changed = hostpath_pv("/data");
        changed.spec.volume_attributes_class_name = Some("silver".to_string());
        assert!(!validate_persistent_volume_update(&changed, &old)
            .iter()
            .any(|e| e.field.ends_with("volumeAttributesClassName")));
    }
}
