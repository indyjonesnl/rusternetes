//! Port of `staging/src/k8s.io/apiserver/pkg/registry/generic/`.

pub mod store;

pub(crate) use store::{page_for_delete_collection, reject_compacted_continue};
pub use store::{CreateOptions, Deleted, GetOptions, Store, UpdateOptions};
