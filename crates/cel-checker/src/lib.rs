//! A port of the cel-go checker (`vendor/github.com/google/cel-go/checker`,
//! `common/types`, `common/decls`, `common/stdlib`) over the AST of the `cel`
//! crate, which has no checker of its own (tracking issue #3440).
//!
//! The `cel` parser already is a port of cel-go's: every `IdedExpr` node has a
//! unique id, macros are expanded, and the operator names are cel-go's, so the
//! checker's `typeMap` / `referenceMap` can key on node ids. This crate
//! therefore depends only on `cel`, never on `rusternetes-common`, so editing
//! the checker does not rebuild `common` and its dependants.
//!
//! Layout (one module per cel-go concern):
//!
//! * [`types`]  : `common/types/types.go`, the type model and its formatting
//!   (`checker/format.go`).
//! * [`unify`]  : `checker/types.go` + `checker/mapping.go`, assignability with
//!   type-parameter substitution, and `checker.resolveOverload`.
//! * [`env`]    : `checker/env.go`, `scopes.go`, `options.go` and the container
//!   resolution of `common/containers`.
//! * [`errors`] : `checker/errors.go` messages and `common/errors.go`.
//! * [`checker`]: `checker/checker.go`, the type-checking pass.
//! * [`cost`]   : `checker/cost.go`, the static cost estimator, with
//!   [`overload_ids`] mapping the table's overloads to the cel-go ids it keys on.
//! * [`library_cost`]: the Kubernetes call-cost estimator
//!   (`apiserver/pkg/cel/library/cost.go`).
//! * [`table`]  : `common/stdlib/standard.go` plus the extension and Kubernetes
//!   library overload declarations of the DRA environment.

pub mod checker;
pub mod cost;
pub mod decl;
pub mod env;
pub mod environment;
pub mod errors;
pub mod ext_cost;
pub mod library_cost;
pub mod overload_ids;
pub mod table;
pub mod types;
pub mod unify;

pub use checker::{check, CheckedAst, Reference};
pub use env::{
    Container, EmptyProvider, Env, FunctionDecl, MapProvider, Options, PatchTypeProvider, Provider,
    VariableDecl,
};
pub use environment::{EnvSet, EnvType, Features, Version};
pub use errors::{CheckError, Errors, Location, Locator, NoLocations, SourceLocator};
pub use table::{is_macro, lookup, table};
pub use types::{DType, Overload};
pub use unify::{format_signature, is_assignable, resolve, Resolution};
