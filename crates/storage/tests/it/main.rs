//! Single integration-test binary for this crate.
//!
//! Cargo compiles and links every `tests/*.rs` as its own binary, each one
//! statically linking this crate plus its dependency graph. Keeping the tests
//! as modules of one target collapses those links into one.
//!
//! Add a new test file as `tests/it/<name>.rs` plus a `mod <name>;` line
//! below. A stray `tests/<name>.rs` still compiles -- it just silently
//! reintroduces a separate binary, which is what this layout avoids.
mod apply_direct_backend_fieldmanager;
mod etcd_unix_socket;
mod event_bus_integration;
mod patch_status_direct_backend;
mod rhino_bus_test;
mod rhino_bus_ttl_test;
mod rhino_busy_contention_test;
mod stor_unit;
mod strategic_merge_applier_reachable;
mod update_status_test;
