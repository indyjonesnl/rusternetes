//! Shared storage contract suite, ported from upstream Kubernetes'
//! `staging/src/k8s.io/apiserver/pkg/storage/testing/`.
//!
//! Upstream runs one suite against several `storage.Interface` implementations;
//! kine compiles that same suite and runs it against itself. Every backend we
//! ship must satisfy the same contract, so the tests live here once and each
//! backend instantiates them via `contract_suite!`.

pub mod fixtures;
pub mod store;

/// Instantiate the whole contract suite for one backend.
///
/// `$setup` is an async expression yielding `Option<Fixture<S>>`; `None` means
/// the backend is unavailable (no Docker) and every test soft-skips.
/// `revisions:` declares whether the backend has a real resourceVersion
/// concept (see `store::run_test_create`'s doc comment) — `false` for
/// `MemoryStorage`.
///
/// Written out explicitly rather than via a nested `macro_rules!` per test:
/// a `macro_rules!` defined inside this macro's expansion would need to bind
/// `$test_name`/`$run` for *its own* invocations, but on stable Rust those
/// metavariables are still captured by the outer `contract_suite!` expansion,
/// so it doesn't compile.
#[macro_export]
macro_rules! contract_suite {
    ($name:ident, $setup:expr, revisions: $revisions:expr) => {
        mod $name {
            use $crate::contract::store;

            #[tokio::test]
            async fn create() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                store::run_test_create(&fixture.storage, $revisions).await;
            }

            #[tokio::test]
            async fn create_with_key_exist() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                store::run_test_create_with_key_exist(&fixture.storage).await;
            }
        }
    };
}
