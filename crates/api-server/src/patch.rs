//! PATCH applier. The implementation moved to `k3rs_common::patch` so
//! crates below the api-server (storage) can apply patches; re-exported here so
//! existing `crate::patch::*` paths keep working.

pub use k3rs_common::patch::*;
