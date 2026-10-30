//! Single integration-test binary for this crate.
//!
//! Cargo compiles and links every `tests/*.rs` as its own binary, each one
//! statically linking this crate plus its dependency graph. Keeping the tests
//! as modules of one target collapses those links into one.
//!
//! Add a new test file as `tests/it/<name>.rs` plus a `mod <name>;` line
//! below. A stray `tests/<name>.rs` still compiles -- it just silently
//! reintroduces a separate binary, which is what this layout avoids.
mod apply_tests;
mod auth_tests;
mod command_tests;
mod describe_tests;
mod exec_attach_portforward_tests;
mod info_commands_tests;
mod kc_unit;
mod mock_client_example;
mod scale_patch_label_tests;
mod top_tests;
mod wait_tests;
