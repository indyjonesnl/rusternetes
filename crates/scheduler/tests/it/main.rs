//! Single integration-test binary for this crate.
//!
//! Cargo compiles and links every `tests/*.rs` as its own binary, each one
//! statically linking this crate plus its dependency graph. Keeping the tests
//! as modules of one target collapses those links into one.
//!
//! Add a new test file as `tests/it/<name>.rs` plus a `mod <name>;` line
//! below. A stray `tests/<name>.rs` still compiles -- it just silently
//! reintroduces a separate binary, which is what this layout avoids.
mod api_mode_test;
mod conformance_scheduling_node_affinity;
mod conformance_scheduling_predicates_affinity;
mod conformance_scheduling_preemption_conditions;
mod conformance_scheduling_priority_preemption_hostport;
mod coverage_affinity_taints;
mod coverage_topology_preempt_score;
mod predicates_test;
mod preemption_test;
mod preemption_victim_identity_test;
mod profile_node_scoring;
mod profile_plugin_sets;
mod scheduler_preemption_test;
mod scheduler_test;
mod score_plugins;
