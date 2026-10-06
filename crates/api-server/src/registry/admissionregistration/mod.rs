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
//! - `MutatingAdmissionPolicy` and its binding, which Rusternetes does not
//!   implement.
//! - `GetResetFields` (managed-fields reset sets) and declarative validation.

pub mod authz;
pub mod validatingadmissionpolicy;
pub mod validatingadmissionpolicybinding;
pub mod webhookconfiguration;
