//! DeviceClass (resource.k8s.io / DRA) validation — port of upstream
//! Kubernetes `pkg/apis/resource/validation/validation.go::ValidateDeviceClass`
//! (release-1.35).
//!
//! Scope: the CEL-free structural checks of `spec` — the `selectors` set
//! (≤32, each must carry a `cel` selector whose `expression` is non-empty and
//! ≤10Ki) and the `config` set (≤32). Parse-checking the CEL expressions; compiling against the
//! DRA CEL environment is tracked in #1442. ObjectMeta is validated separately.

use crate::cel_versions::{self, EnvSet};
use crate::resources::{DeviceClass, DeviceClassSpec};
use crate::validation::field::{ErrorList, Path};
use crate::validation::resourceclaim::{too_many_max_items, validate_selector_slice_in};

const DEVICE_CONFIG_MAX_SIZE: usize = 32;

/// Validate a `DeviceClass` on create. Mirrors the structural part of upstream
/// `ValidateDeviceClass` (minus CEL compilation — see #1442).
pub fn validate_device_class(class: &DeviceClass) -> ErrorList {
    validate_device_class_spec(&class.spec, &Path::new("spec"))
}

/// `validateDeviceClassSpec(spec, oldSpec, fldPath)` as called from
/// `ValidateDeviceClassUpdate` (`validation.go:545-567`): the selectors are
/// "stored" (compiled in `StoredExpressions`) iff exactly as before; any
/// change, including a reorder, compiles them as new expressions.
pub fn validate_device_class_update(class: &DeviceClass, old: &DeviceClass) -> ErrorList {
    validate_device_class_update_in(
        class,
        old,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

pub fn validate_device_class_update_in(
    class: &DeviceClass,
    old: &DeviceClass,
    env_set: &EnvSet,
) -> ErrorList {
    let stored = class.spec.selectors == old.spec.selectors;
    validate_device_class_spec_in(&class.spec, &Path::new("spec"), stored, env_set)
}

fn validate_device_class_spec(spec: &DeviceClassSpec, fld_path: &Path) -> ErrorList {
    validate_device_class_spec_in(
        spec,
        fld_path,
        false,
        &EnvSet::must_base_env_set(cel_versions::default_compatibility_version()),
    )
}

fn validate_device_class_spec_in(
    spec: &DeviceClassSpec,
    fld_path: &Path,
    stored: bool,
    env_set: &EnvSet,
) -> ErrorList {
    let mut errs: ErrorList = Vec::new();

    // Upstream reaches the identical `validateSelectorSlice` from both a
    // DeviceClass and a claim's requests (`validation.go:298`), so this calls
    // the one port of it rather than keeping a second copy.
    errs.extend(validate_selector_slice_in(
        &spec.selectors,
        &fld_path.child("selectors"),
        stored,
        env_set,
    ));

    if spec.config.len() > DEVICE_CONFIG_MAX_SIZE {
        errs.push(too_many_max_items(
            &fld_path.child("config"),
            spec.config.len(),
            DEVICE_CONFIG_MAX_SIZE,
        ));
    }

    errs
}
