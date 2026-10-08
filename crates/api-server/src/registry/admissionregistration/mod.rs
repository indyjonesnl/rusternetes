//! `admissionregistration.k8s.io/v1` — port of
//! `pkg/registry/admissionregistration/`.
//!
//! Not modelled, each tracked on the issue named:
//!
//! - CEL compilation of a policy's `validations`, `variables`,
//!   `auditAnnotations` and `messageExpression`, and the type-checking
//!   controller that fills `status.typeChecking` (#1442). The
//!   `matchConditions` of the two webhook configurations are compiled, as
//!   `validateMatchConditionsExpression` does.
//! - `MutatingAdmissionPolicy` (`admissionregistration.k8s.io/v1beta1`,
//!   served only under the `MutatingAdmissionPolicy` gate): the CEL compile of
//!   `applyConfiguration` / `jsonPatch` / `matchConditions` / `variables`
//!   (`validateApplyConfiguration`, `validateJSONPatch`, #2798) with the
//!   update-time `ignoreMutatingAdmissionPolicyMatchConditions` /
//!   `preexistingExpressions` options that only gate it, and the admission
//!   plugin (#2731).
//! - `GetResetFields` (managed-fields reset sets) and declarative validation.

pub mod authz;
pub mod mutatingadmissionpolicy;
pub mod mutatingadmissionpolicybinding;
pub mod validatingadmissionpolicy;
pub mod validatingadmissionpolicybinding;
pub mod webhookconfiguration;
