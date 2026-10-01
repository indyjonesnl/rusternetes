//! RBAC (`rbac.authorization.k8s.io/v1`) on the generic Store — port of
//! `pkg/registry/rbac/` (kubernetes release-1.35).
//!
//! | here | upstream |
//! |---|---|
//! | [`role`], [`rolebinding`], [`clusterrole`], [`clusterrolebinding`] | `<kind>/strategy.go`, `<kind>/storage/storage.go` |
//! | [`policybased`] | `<kind>/policybased/storage.go` |
//! | [`escalation_check`] | `escalation_check.go`, `helpers.go` |
//! | [`rule`] | `validation/rule.go`, `validation/internal_version_adapter.go` |
//! | [`policy_compact`] | `validation/policy_compact.go`, `CompactString` |
//! | [`policy_comparator`] | `staging/src/k8s.io/component-helpers/auth/rbac/validation/policy_comparator.go` |
//!
//! Not ported:
//!
//! * `ValidateDeclarativelyWithMigrationChecks` (declarative validation of
//!   RoleBinding and ClusterRole): the hand-written validators already cover
//!   what the `+k8s:` tags declare.
//! * The `clusterroleaggregation` controller: [`aggregation`] recomputes an
//!   aggregated ClusterRole's rules when that ClusterRole is written instead.
//! * Upstream wraps `Create`/`Update` of the standard storage; here the
//!   policybased checks are the Store's `BeginCreate`/`BeginUpdate` hooks (see
//!   [`policybased`] for the one ordering difference that causes).

pub mod aggregation;
pub mod clusterrole;
pub mod clusterrolebinding;
pub mod escalation_check;
pub mod policy_compact;
pub mod policy_comparator;
pub mod policybased;
pub mod role;
pub mod rolebinding;
pub mod rule;
