//! Port of `staging/src/k8s.io/apiserver/pkg/registry/generic/`.

pub mod store;

pub(crate) use store::page_for_delete_collection;
pub use store::{CreateOptions, Deleted, GetOptions, Store, UpdateOptions};
