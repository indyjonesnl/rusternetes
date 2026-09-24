//! Port of `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/`.
//!
//! | here | upstream |
//! |---|---|
//! | [`rest`] | `rest.go` — `RequestScope`, `checkName`, `fieldValidation`, `transformResponseObject` |
//! | [`get`] | `get.go` — `GetResource` |
//! | [`create`] | `create.go` — `createHandler` |
//! | [`update`] | `update.go` — `UpdateResource` |
//! | [`patch`] | `patch.go` — `PatchResource` and its patchers |
//! | [`delete`] | `delete.go` — `DeleteResource`, `DeleteCollection` |
//! | [`admission`] | `registry/rest/rest.go` `AdmissionToValidateObject*Func`, and the plugin chain (`pkg/kubeapiserver/options/plugins.go:106-110`) |
//!
//! Authorization runs at the top of each handler; upstream runs it in the
//! `WithAuthorization` filter, which also precedes the handler.

pub mod admission;
pub mod create;
pub mod delete;
pub mod get;
pub mod patch;
pub mod rest;
pub mod update;

pub use create::create_resource;
pub use delete::{delete_collection, delete_resource};
pub use get::get_resource;
pub use patch::patch_resource;
pub use rest::{negotiate, RequestScope};
pub use update::update_resource;

#[cfg(test)]
mod tests;
