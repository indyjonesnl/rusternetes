//! `WithAuthorization` filter tests (#3386, #3444, #3649).
//!
//! SEPARATE-TEST-BINARY: these tests read before/after deltas of the
//! process-global `authorization_attempts_total` / `authorization_duration_seconds`
//! metrics (upstream resets a registry per test, `filters/metrics_test.go`;
//! the Rust metrics sit on the default prometheus registry, so there is no
//! reset seam). Inside the shared `tests/it` binary every test that drives the
//! real router (`TestApiServer`) passes through `with_authorization` and bumps
//! those counters, so no lock local to these modules could exclude them, and
//! the deltas flaked. A process of their own plus the single `LOCK` in
//! `metrics_test` makes the counters deterministic. `cargo nextest` gives every
//! test its own process and would hide the problem; plain `cargo test` does not.
mod filter_test;
mod metrics_test;
