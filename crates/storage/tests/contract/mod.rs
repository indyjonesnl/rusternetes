//! Shared storage contract suite, ported from upstream Kubernetes'
//! `staging/src/k8s.io/apiserver/pkg/storage/testing/`.
//!
//! Upstream runs one suite against several `storage.Interface` implementations;
//! kine compiles that same suite and runs it against itself. Every backend we
//! ship must satisfy the same contract, so the tests live here once and each
//! backend instantiates them via `contract_suite!`.

pub mod fixtures;
pub mod store;
pub mod watcher;

/// Instantiate the whole contract suite for one backend.
///
/// `$setup` is an async expression yielding `Option<Fixture<S>>`; `None` means
/// the backend is unavailable (no Docker) and every test soft-skips.
/// `revisions:` declares whether the backend has a real resourceVersion
/// concept (see `store::run_test_create`'s doc comment) — `false` for
/// `MemoryStorage`.
/// `snapshot_paging:` says whether the backend can serve a paged list as a
/// snapshot at a past revision — `false` for stores that keep only current
/// state.
///
/// Written out explicitly rather than via a nested `macro_rules!` per test:
/// a `macro_rules!` defined inside this macro's expansion would need to bind
/// `$test_name`/`$run` for *its own* invocations, but on stable Rust those
/// metavariables are still captured by the outer `contract_suite!` expansion,
/// so it doesn't compile.
#[macro_export]
macro_rules! contract_suite {
    ($name:ident, $setup:expr, revisions: $revisions:expr, snapshot_paging: $snapshot:expr) => {
        mod $name {
            use $crate::contract::{store, watcher};

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

            #[tokio::test]
            async fn list_recursive_prefix() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                store::run_test_list_recursive_prefix(&fixture.storage).await;
            }

            #[tokio::test]
            async fn unconditional_delete() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                store::run_test_unconditional_delete(&fixture.storage).await;
            }

            #[tokio::test]
            async fn list_continuation() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                store::run_test_list_continuation(&fixture.storage).await;
            }

            #[tokio::test]
            async fn delete_trigger_watch() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                watcher::run_test_delete_trigger_watch(&fixture.storage, $revisions).await;
            }

            #[tokio::test]
            async fn list_paging() {
                let Some(fixture) = $setup.await else {
                    return;
                };
                store::run_test_list_paging(&fixture.storage, $snapshot).await;
            }
        }
    };
}
