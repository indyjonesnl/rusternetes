//! Shadow type check of a ValidatingAdmissionPolicy against the types of the
//! resources it matches (`RUSTERNETES_CEL_CHECKER=shadow`, default off).
//!
//! The checker itself (`typechecking.go` port) lives in
//! `rusternetes_discovery::vap_typechecking`, shared with the
//! `validatingadmissionpolicy-status` controller, which writes its results as
//! `status.typeChecking`. Here they are only compared with the untyped compile
//! probe and logged (`cel_checker_shadow`); nothing is rejected or written.

use rusternetes_common::cel_checker_shadow::Mode;
use rusternetes_common::cel_versions::EnvSet;
use rusternetes_common::resources::ValidatingAdmissionPolicy;

pub use rusternetes_discovery::vap_typechecking::{
    typed_divergences, types_to_check, TypedDivergence, MAX_TYPES_TO_CHECK,
};

/// The hook called by the policy validation: in `shadow` mode logs the typed
/// check's disagreements with the probe. Never affects validation.
pub fn observe(obj: &ValidatingAdmissionPolicy, set: &EnvSet) {
    if Mode::current() != Mode::Shadow {
        return;
    }
    for d in typed_divergences(obj, set) {
        tracing::warn!(
            target: "cel_checker_shadow",
            policy = %obj.metadata.name,
            gvk = ?(&d.gvk.group, &d.gvk.version, &d.gvk.kind),
            field = %d.field,
            probe = ?d.probe,
            checker = ?d.checker,
            checker_display = %d.display,
            "typed cel checker disagrees with the compile probe"
        );
    }
}
