//! ControllerRevision validation — port of upstream Kubernetes
//! `pkg/apis/apps/validation/validation.go:328-367` (release-1.35):
//! `validateControllerRevision`, `ValidateControllerRevisionCreate` and
//! `ValidateControllerRevisionUpdate`.

use crate::resources::ControllerRevision;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::objectmeta::{
    name_is_dns_subdomain, validate_immutable_field, validate_nonnegative_field,
    validate_object_meta, validate_object_meta_update,
};

/// Upstream `validateControllerRevision` (validation.go:333-338), with
/// `ValidateControllerRevisionName = NameIsDNSSubdomain` (:328).
fn validate_controller_revision_common(cr: &ControllerRevision) -> ErrorList {
    let mut errs = validate_object_meta(
        &cr.metadata,
        true,
        name_is_dns_subdomain,
        &Path::new("metadata"),
    );
    errs.extend(validate_nonnegative_field(
        cr.revision,
        &Path::new("revision"),
    ));
    errs
}

/// Validate a `ControllerRevision` on create: upstream
/// `ValidateControllerRevisionCreate` (validation.go:340-358). `data` is
/// mandatory and must be a JSON object.
///
/// The `error parsing data` branch has no equivalent: a body whose `data` is
/// not JSON fails to decode before it gets here.
pub fn validate_controller_revision(cr: &ControllerRevision) -> ErrorList {
    let mut errs = validate_controller_revision_common(cr);

    let data_path = Path::new("data");
    match &cr.data {
        // `"data": null` leaves `RawExtension.Raw` nil, as absence does.
        None | Some(serde_json::Value::Null) => {
            errs.push(Error::required(&data_path, "data is mandatory"));
        }
        Some(v) if !v.is_object() => {
            errs.push(Error::required(
                &data_path,
                "data must be a valid JSON object",
            ));
        }
        Some(_) => {}
    }
    errs
}

/// Validate a `ControllerRevision` on update: upstream
/// `ValidateControllerRevisionUpdate` (validation.go:361-367) — the metadata
/// update, the common checks and an immutable `data`. The create-only `data`
/// shape check does not re-run.
pub fn validate_controller_revision_update(
    new: &ControllerRevision,
    old: &ControllerRevision,
) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    errs.extend(validate_controller_revision_common(new));
    errs.extend(validate_immutable_field(
        &new.data,
        &old.data,
        &Path::new("data"),
    ));
    errs
}
