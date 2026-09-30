//! Namespace validation — port of upstream Kubernetes
//! `pkg/apis/core/validation/validation.go` (release-1.35):
//! `ValidateNamespace`, `ValidateNamespaceUpdate`,
//! `ValidateNamespaceStatusUpdate` and `ValidateNamespaceFinalizeUpdate`.

use crate::resources::Namespace;
use crate::types::Phase;
use crate::validation::field::{Error, ErrorList, Path};
use crate::validation::metav1::is_qualified_name;
use crate::validation::objectmeta::{
    validate_namespace_name, validate_object_meta, validate_object_meta_update,
};

/// Standard finalizer names (`pkg/apis/core/helper.standardFinalizers`):
/// `kubernetes` + the metav1 orphan / foreground-deletion finalizers.
const STANDARD_FINALIZERS: [&str; 3] = ["kubernetes", "orphan", "foregroundDeletion"];

/// `validateFinalizerName` (validation.go:8177-8181): apimachinery's
/// `ValidateFinalizerName` (a qualified name) and `validateKubeFinalizerName`
/// (:8184-8193: an unqualified name must be a standard finalizer).
fn validate_finalizer_name(f: &str, path: &Path) -> ErrorList {
    let mut errs: ErrorList = is_qualified_name(f)
        .into_iter()
        .map(|msg| Error::invalid(path, f.to_string(), msg))
        .collect();
    if f.split('/').count() == 1 && !STANDARD_FINALIZERS.contains(&f) {
        errs.push(Error::invalid(
            path,
            f.to_string(),
            "name is neither a standard finalizer name nor is it fully qualified",
        ));
    }
    errs
}

fn spec_finalizers(ns: &Namespace) -> &[String] {
    ns.spec
        .as_ref()
        .and_then(|s| s.finalizers.as_deref())
        .unwrap_or_default()
}

/// `ValidateNamespace` (validation.go:8168-8174). Upstream passes the
/// unindexed `spec.finalizers` path here.
pub fn validate_namespace(ns: &Namespace) -> ErrorList {
    let mut errs = validate_object_meta(
        &ns.metadata,
        false,
        validate_namespace_name,
        &Path::new("metadata"),
    );
    let path = Path::new("spec").child("finalizers");
    for f in spec_finalizers(ns) {
        errs.extend(validate_finalizer_name(f, &path));
    }
    errs
}

/// `ValidateNamespaceUpdate` (validation.go:8196-8199).
pub fn validate_namespace_update(new: &Namespace, old: &Namespace) -> ErrorList {
    validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"))
}

/// `ValidateNamespaceFinalizeUpdate` (validation.go:8217-8226): the
/// `/finalize` subresource may change `spec.finalizers`, each of which must
/// be a valid finalizer name.
pub fn validate_namespace_finalize_update(new: &Namespace, old: &Namespace) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    let path = Path::new("spec").child("finalizers");
    for (i, f) in spec_finalizers(new).iter().enumerate() {
        errs.extend(validate_finalizer_name(f, &path.index(i)));
    }
    errs
}

/// Render a `Phase` as the bare wire string (e.g. `Active`, `Terminating`) for
/// inclusion in a validation error's bad-value, matching how upstream reports
/// the offending `status.Phase` value. `None` renders as the empty string,
/// mirroring Go's zero-valued `core.NamespacePhase`.
fn phase_str(phase: Option<&Phase>) -> String {
    match phase {
        Some(p) => serde_json::to_value(p)
            .ok()
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default(),
        None => String::new(),
    }
}

/// `ValidateNamespaceStatusUpdate` (validation.go:8202-8215): ObjectMeta
/// update, and a phase consistent with the deletion timestamp — `Active`
/// while `deletionTimestamp` is empty, `Terminating` once it is set.
pub fn validate_namespace_status_update(new: &Namespace, old: &Namespace) -> ErrorList {
    let mut errs =
        validate_object_meta_update(&new.metadata, &old.metadata, &Path::new("metadata"));
    // Upstream uses field.NewPath("status", "Phase") — note the capitalised
    // "Phase" segment; preserve it verbatim for error-wording parity.
    let path = Path::new("status").child("Phase");

    let phase = new.status.as_ref().and_then(|s| s.phase.as_ref());

    if new.metadata.deletion_timestamp.is_none() {
        if phase != Some(&Phase::Active) {
            errs.push(Error::invalid(
                &path,
                phase_str(phase),
                "may only be 'Active' if `deletionTimestamp` is empty",
            ));
        }
    } else if phase != Some(&Phase::Terminating) {
        errs.push(Error::invalid(
            &path,
            phase_str(phase),
            "may only be 'Terminating' if `deletionTimestamp` is not empty",
        ));
    }

    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::NamespaceStatus;
    use chrono::Utc;

    fn ns_with(phase: Option<Phase>, deleting: bool) -> Namespace {
        let mut ns = Namespace::new("test");
        ns.metadata.resource_version = Some("1".to_string());
        ns.status = Some(NamespaceStatus {
            phase,
            conditions: None,
        });
        ns.metadata.deletion_timestamp = if deleting { Some(Utc::now()) } else { None };
        ns
    }

    #[test]
    fn active_with_no_deletion_timestamp_is_valid() {
        let ns = ns_with(Some(Phase::Active), false);
        assert!(validate_namespace_status_update(&ns, &ns).is_empty());
    }

    #[test]
    fn terminating_with_deletion_timestamp_is_valid() {
        let ns = ns_with(Some(Phase::Terminating), true);
        assert!(validate_namespace_status_update(&ns, &ns).is_empty());
    }

    #[test]
    fn non_active_without_deletion_timestamp_is_invalid() {
        let ns = ns_with(Some(Phase::Terminating), false);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "status.Phase");
        assert_eq!(
            errs[0].detail,
            "may only be 'Active' if `deletionTimestamp` is empty"
        );
    }

    #[test]
    fn missing_phase_without_deletion_timestamp_is_invalid() {
        let ns = ns_with(None, false);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(
            errs[0].detail,
            "may only be 'Active' if `deletionTimestamp` is empty"
        );
    }

    #[test]
    fn non_terminating_with_deletion_timestamp_is_invalid() {
        let ns = ns_with(Some(Phase::Active), true);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "status.Phase");
        assert_eq!(
            errs[0].detail,
            "may only be 'Terminating' if `deletionTimestamp` is not empty"
        );
    }

    #[test]
    fn missing_phase_with_deletion_timestamp_is_invalid() {
        let ns = ns_with(None, true);
        let errs = validate_namespace_status_update(&ns, &ns);
        assert_eq!(errs.len(), 1);
        assert_eq!(
            errs[0].detail,
            "may only be 'Terminating' if `deletionTimestamp` is not empty"
        );
    }
}
