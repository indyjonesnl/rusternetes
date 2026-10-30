//! Single integration-test binary for this crate.
//!
//! Cargo compiles and links every `tests/*.rs` as its own binary, each one
//! statically linking this crate plus its dependency graph. Keeping the tests
//! as modules of one target collapses those links into one.
//!
//! Add a new test file as `tests/it/<name>.rs` plus a `mod <name>;` line
//! below. A stray `tests/<name>.rs` still compiles -- it just silently
//! reintroduces a separate binary, which is what this layout avoids.
mod configz_registry;
mod conformance_network_dns;
mod conformance_network_endpointslices_dns_headless;
mod conformance_network_hostport_protocols;
mod conformance_network_services_proxy;
mod hostport_test;
mod metrics_encoding;
mod node_net_cni_routes;
mod proxy_health_handlers;
mod serve_metrics_handlers;
