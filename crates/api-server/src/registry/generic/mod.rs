//! Port of `staging/src/k8s.io/apiserver/pkg/registry/generic/`.

pub mod store;

pub use store::{CreateOptions, Deleted, Store, UpdateOptions};
