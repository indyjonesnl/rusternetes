//! The generic registry: one write pipeline for every resource.
//!
//! This is a port of upstream's registry layer, kept in the same shape so each
//! piece can be checked against the Go it came from:
//!
//! | here | upstream (`staging/src/k8s.io/apiserver/pkg/`) |
//! |---|---|
//! | [`rest`] | `registry/rest/{create,update,delete,meta}.go` — the strategy interfaces and `BeforeCreate` / `BeforeUpdate` / `BeforeDelete` |
//! | [`names`] | `storage/names/generate.go` — `SimpleNameGenerator` |
//! | [`generic::store`] | `registry/generic/registry/{store,dryrun}.go` — `Store` |
//! | [`core`] | `pkg/registry/core/<resource>/` (kubernetes) — per-resource strategies |
//!
//! Upstream has exactly one create/update/delete path: every built-in
//! resource is a `genericregistry.Store` configured with a per-resource
//! strategy (`pkg/registry/<group>/<resource>/strategy.go`). A resource opts
//! into behaviour — clearing status on create, bumping generation, allowing
//! create-on-update — by what its strategy returns, never by carrying its own
//! copy of the pipeline. Rusternetes grew one hand-rolled pipeline per handler
//! instead, and they drifted (#1990). Resources move onto this Store one at a
//! time; each migration deletes its bespoke handler code.

pub mod apps;
pub mod core;
pub mod generic;
pub mod names;
pub mod rest;
pub mod scale;
