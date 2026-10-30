//! Single integration-test binary for this crate.
//!
//! Cargo compiles and links every `tests/*.rs` as its own binary, each one
//! statically linking this crate plus its dependency graph. Keeping the tests
//! as modules of one target collapses those links into one.
//!
//! Add a new test file as `tests/it/<name>.rs` plus a `mod <name>;` line
//! below. A stray `tests/<name>.rs` still compiles -- it just silently
//! reintroduces a separate binary, which is what this layout avoids.
mod admissionregistration_mutating_policy;
mod authorization_selector_attributes;
mod controllerrevision_decode;
mod crd_items_roundtrip;
mod empty_pointer_strings;
mod flowcontrol_wire;
mod go_zero_field_emission;
mod ingress_inline_and_apigroup;
mod list_kinds_registered;
mod objectmeta_self_link;
mod plain_zero_scalars;
mod plain_zero_scalars_models;
mod plain_zero_scalars_table;
mod podcertificaterequest_wire;
mod tokenrequest_roundtrip;
mod upstream_testdata_present;
