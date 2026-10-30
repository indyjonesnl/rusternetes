//! CSIDriver validation — port of upstream Kubernetes
//! `pkg/apis/storage/validation/validation.go::ValidateCSIDriver` (release-1.35).
//!
//! `FSGroupPolicy` / `VolumeLifecycleMode` carry an `Unknown(String)` arm (#2496)
//! so an unsupported value decodes and is answered `NotSupported`, as upstream
//! does. The other create-time checks ported here are the required-field presence checks (`attachRequired`,
//! `podInfoOnMount`, `storageCapacity`), the `nodeAllocatableUpdatePeriodSeconds`
//! lower bound, the `serviceAccountTokenInSecrets`/`tokenRequests` cross-field
//! check, and `tokenRequests` (duplicate audience + expiration bounds).
//! ObjectMeta is validated separately (#1087 / #1277). CSI conformance is
//! non-negotiable for this project.

use crate::resources::csi::{CSIDriver, FSGroupPolicy, VolumeLifecycleMode};
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::validate_immutable_field;
use std::collections::HashSet;

// Upstream `validateTokenRequests` bounds.
const TOKEN_EXPIRATION_MIN_SECONDS: i64 = 600; // 10 minutes
const TOKEN_EXPIRATION_MAX_SECONDS: i64 = 1 << 32;

/// Validate a `CSIDriver` on create. Mirrors upstream `ValidateCSIDriver` /
/// `validateCSIDriverSpec` minus ObjectMeta and the enum/bool checks the Rust
/// type system already enforces.
pub fn validate_csi_driver(driver: &CSIDriver) -> ErrorList {
    let spec = &driver.spec;
    let spec_path = Path::new("spec");
    let mut errs: ErrorList = Vec::new();

    // attachRequired is required (defaulted to true by the handler; this guards
    // raw/storage-seeded objects). Upstream uses the `attachedRequired` path.
    if spec.attach_required.is_none() {
        errs.push(Error::required(&spec_path.child("attachedRequired"), ""));
    }

    // podInfoOnMount required — validateCSIDriverSpec/validatePodInfoOnMount
    // (validation.go:452 / 483-490).
    if spec.pod_info_on_mount.is_none() {
        errs.push(Error::required(&spec_path.child("podInfoOnMount"), ""));
    }

    // storageCapacity required — validateCSIDriverSpec/validateStorageCapacity
    // (validation.go:453 / 493-500).
    if spec.storage_capacity.is_none() {
        errs.push(Error::required(&spec_path.child("storageCapacity"), ""));
    }

    // validateFSGroupPolicy (validation.go:502-516): `supportedFSGroupPolicy.List()`
    // is sorted.
    if let Some(FSGroupPolicy::Unknown(v)) = &spec.fs_group_policy {
        errs.push(Error::not_supported(
            &spec_path.child("fsGroupPolicy"),
            v.clone(),
            &["File", "None", "ReadWriteOnceWithFSType"],
        ));
    }

    // validateVolumeLifecycleModes (validation.go:549-563): one error per bad
    // entry, all on the list path, values in declaration order.
    for mode in spec.volume_lifecycle_modes.as_deref().unwrap_or(&[]) {
        if let VolumeLifecycleMode::Unknown(v) = mode {
            errs.push(Error::not_supported(
                &spec_path.child("volumeLifecycleModes"),
                v.clone(),
                &["Persistent", "Ephemeral"],
            ));
        }
    }

    // seLinuxMount is required while SELinuxMountReadWriteOncePod is on
    // (GA, default on in 1.35) — validateSELinuxMount (validation.go:566-573).
    if spec.se_linux_mount.is_none() {
        errs.push(Error::required(&spec_path.child("seLinuxMount"), ""));
    }

    // nodeAllocatableUpdatePeriodSeconds must be >= 10 when set —
    // validateNodeAllocatableUpdatePeriodSeconds (validation.go:458 / 464-470).
    if let Some(period) = spec.node_allocatable_update_period_seconds {
        if period < 10 {
            errs.push(Error::invalid(
                &spec_path.child("nodeAllocatableUpdatePeriodSeconds"),
                period,
                "must be greater than or equal to 10 seconds",
            ));
        }
    }

    if let Some(token_requests) = &spec.token_requests {
        let tr_path = spec_path.child("tokenRequests");
        let mut audiences: HashSet<&str> = HashSet::new();
        for (i, tr) in token_requests.iter().enumerate() {
            let p = tr_path.index(i);
            if !audiences.insert(tr.audience.as_str()) {
                errs.push(Error::duplicate(&p.child("audience"), tr.audience.clone()));
                continue;
            }
            if let Some(exp) = tr.expiration_seconds {
                if exp < TOKEN_EXPIRATION_MIN_SECONDS {
                    errs.push(Error::invalid(
                        &p.child("expirationSeconds"),
                        exp,
                        "may not specify a duration less than 10 minutes",
                    ));
                }
                if exp > TOKEN_EXPIRATION_MAX_SECONDS {
                    errs.push(Error::invalid(
                        &p.child("expirationSeconds"),
                        exp,
                        "may not specify a duration larger than 2^32 seconds",
                    ));
                }
            }
        }
    }

    // serviceAccountTokenInSecrets set but tokenRequests empty → Invalid —
    // validateServiceAccountTokenInSecrets (validation.go:459 / 577-584).
    // Upstream gates on `len(tokenRequests) == 0`, which is true for both a
    // nil slice and an empty one.
    if let Some(in_secrets) = spec.service_account_token_in_secrets {
        let token_requests_empty = spec.token_requests.as_ref().is_none_or(|t| t.is_empty());
        if token_requests_empty {
            errs.push(Error::invalid(
                &spec_path.child("serviceAccountTokenInSecrets"),
                in_secrets,
                "serviceAccountTokenInSecrets is set but no tokenRequests are specified",
            ));
        }
    }

    errs
}

/// `ValidateCSIDriverUpdate` (validation.go:435-444): the spec is validated
/// again, and `attachRequired` and `volumeLifecycleModes` are immutable.
/// The immutable path for `attachRequired` is upstream's
/// `spec.attachedRequired` (sic).
pub fn validate_csi_driver_update(new_d: &CSIDriver, old_d: &CSIDriver) -> ErrorList {
    let mut errs = validate_csi_driver(new_d);
    let spec_path = Path::new("spec");
    errs.extend(validate_immutable_field(
        &new_d.spec.attach_required,
        &old_d.spec.attach_required,
        &spec_path.child("attachedRequired"),
    ));
    errs.extend(validate_immutable_field(
        &new_d.spec.volume_lifecycle_modes,
        &old_d.spec.volume_lifecycle_modes,
        &spec_path.child("volumeLifecycleModes"),
    ));
    errs
}
