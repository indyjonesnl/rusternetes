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
//! | [`reconciliation`] | `staging/src/k8s.io/component-helpers/auth/rbac/reconciliation/` |
//! | [`policy_comparator`] | `staging/src/k8s.io/component-helpers/auth/rbac/validation/policy_comparator.go` |
//!
//! Not ported:
//!
//! * `ValidateDeclarativelyWithMigrationChecks` (declarative validation of
//!   RoleBinding and ClusterRole): the hand-written validators already cover
//!   what the `+k8s:` tags declare.
//! * The `clusterroleaggregation` controller lives in the controller-manager
//!   (`controllers/clusterrole_aggregation.rs`); a ClusterRole's `rules` are
//!   never recomputed at write time, as upstream.
//! * Upstream wraps `Create`/`Update` of the standard storage; here the
//!   policybased checks are the Store's `BeginCreate` hook and its
//!   `update_transformers` (`rest.WrapUpdatedObjectInfo`), see [`policybased`].

pub mod clusterrole;
pub mod clusterrolebinding;
pub mod escalation_check;
pub mod policy_compact;
pub mod policy_comparator;
pub mod policybased;
pub mod reconciliation;
pub mod role;
pub mod rolebinding;
pub mod rule;
