//! PersistentVolume validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go::ValidatePersistentVolume` (release-1.35).
//!
//! Scope: capacity (storage required + non-negative), access modes (≥1 +
//! ReadWriteOncePod exclusivity), exactly one volume source, and
//! storageClassName. The per-source field validation, nodeAffinity-for-Local
//! requirement, and reclaim-policy/source compatibility are left as a follow-up.

use crate::quantity::Quantity;
use crate::resources::volume::{
    PersistentVolume, PersistentVolumeAccessMode, PersistentVolumeSpec,
};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_dns1123_subdomain;

/// Validate a `PersistentVolumeSpec`. Mirrors the core of upstream
/// `ValidatePersistentVolume`.
pub fn validate_persistent_volume_spec(spec: &PersistentVolumeSpec, fld_path: &Path) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // capacity.storage is required and must be a non-negative quantity.
    let cap_path = fld_path.child("capacity").child("storage");
    match spec.capacity.get("storage") {
        None => errs.push(Error::required(&cap_path, "")),
        Some(v) => match Quantity::parse(v) {
            Err(_) => errs.push(Error::invalid(
                &cap_path,
                v.clone(),
                "must be a valid resource quantity",
            )),
            Ok(q) => {
                if q.is_negative() {
                    errs.push(Error::invalid(
                        &cap_path,
                        v.clone(),
                        "must be greater than or equal to 0",
                    ));
                }
            }
        },
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

    // Exactly one volume source must be specified.
    let num_volumes = [
        spec.host_path.is_some(),
        spec.nfs.is_some(),
        spec.iscsi.is_some(),
        spec.local.is_some(),
        spec.csi.is_some(),
    ]
    .iter()
    .filter(|x| **x)
    .count();
    if num_volumes == 0 {
        errs.push(Error::required(fld_path, "must specify a volume type"));
    } else if num_volumes > 1 {
        errs.push(Error::forbidden(
            fld_path,
            "may not specify more than 1 volume type",
        ));
    }

    // storageClassName, when set, must be a DNS-1123 subdomain.
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

    errs
}

/// Validate a new `PersistentVolume`. Mirrors upstream `ValidatePersistentVolume`.
pub fn validate_persistent_volume(pv: &PersistentVolume) -> ErrorList {
    validate_persistent_volume_spec(&pv.spec, &Path::new("spec"))
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

    errs
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
}
