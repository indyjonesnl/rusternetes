//! Bounded conflict-retry for read-modify-write handler paths.
//!
//! Optimistic concurrency is **caller-opt-in** in Kubernetes: a client that
//! supplies `metadata.resourceVersion` (on a PUT) or
//! `preconditions.resourceVersion` (on a DELETE) asks for a CAS and must be told
//! when it fails. A client that supplies neither has asked for none, and the
//! request must succeed regardless of concurrent writers.
//!
//! The api-server nonetheless performs read-modify-write internally all over the
//! place — applying a patch, stamping `deletionTimestamp`, adding or draining a
//! finalizer, merging a status. Each of those reads the stored object (inheriting
//! its resourceVersion) and writes it back, so a concurrent writer turns the
//! *server's own* CAS into a 409 that the client never asked for. Upstream
//! absorbs exactly this by retrying the read-modify-write:
//!
//! ```text
//! staging/src/k8s.io/apiserver/pkg/registry/rest/patch.go
//!   // MaxRetryWhenPatchConflicts is the number of times we retry when a patch conflicts
//!   const MaxRetryWhenPatchConflicts = 5
//! ```
//!
//! and `registry/generic/registry/store.go::GuaranteedUpdate` loops on conflict
//! for the graceful-deletion and finalizer paths.
//!
//! Measured symptoms this exists to fix (#1776):
//!
//! ```text
//! Failed to patch PV "pv-1238-f53f7": resourceVersion mismatch:
//!   resource was modified (expected: 469, current: 471)
//! deleting Pod: resourceVersion mismatch:
//!   resource was modified (expected: 21312, current: 21313)
//! ```
//!
//! The critical rule is that a retry must **re-apply the mutation to the freshly
//! read object**, never re-write the bytes computed from the stale read — the
//! latter silently discards the concurrent writer's change.

use rusternetes_common::{Error, Result};

/// Upstream's bound: `MaxRetryWhenPatchConflicts = 5`
/// (`staging/src/k8s.io/apiserver/pkg/registry/rest/patch.go`).
pub const MAX_CONFLICT_RETRIES: usize = 5;

/// Run a read-modify-write, retrying on storage `Conflict` up to
/// [`MAX_CONFLICT_RETRIES`] times.
///
/// `attempt` is called once per try and must do the whole cycle: re-read the
/// current object, apply the mutation to *that* object, and write it back. It
/// receives the 0-based attempt number, which is useful for logging.
///
/// Only `Error::Conflict` is retried; every other error is returned immediately.
/// When the bound is exhausted the final conflict is returned with the retry
/// count named, so the client can tell a genuine precondition failure from a
/// pathological write-contention case.
pub async fn with_conflict_retry<T, F, Fut>(what: &str, mut attempt: F) -> Result<T>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last: Option<Error> = None;

    for i in 0..MAX_CONFLICT_RETRIES {
        match attempt(i).await {
            Ok(value) => return Ok(value),
            Err(Error::Conflict(msg)) => {
                tracing::debug!(
                    "{what}: conflict on attempt {}/{MAX_CONFLICT_RETRIES}, re-reading and retrying: {msg}",
                    i + 1,
                );
                last = Some(Error::Conflict(msg));
            }
            Err(other) => return Err(other),
        }
    }

    Err(match last {
        Some(Error::Conflict(msg)) => Error::Conflict(format!(
            "{what}: giving up after {MAX_CONFLICT_RETRIES} conflicting attempts: {msg}"
        )),
        Some(other) => other,
        None => Error::Internal(format!("{what}: retry loop exited without an outcome")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn succeeds_without_retrying_when_there_is_no_conflict() {
        let calls = Cell::new(0);
        let out: i32 = with_conflict_retry("test", |_| {
            calls.set(calls.get() + 1);
            async { Ok(7) }
        })
        .await
        .expect("no conflict means no error");

        assert_eq!(out, 7);
        assert_eq!(calls.get(), 1, "a successful attempt must not be repeated");
    }

    /// #1776: a conflict on an internal read-modify-write is absorbed, not
    /// surfaced — the client never asked for a CAS.
    #[tokio::test]
    async fn retries_a_conflict_and_then_succeeds() {
        let calls = Cell::new(0);
        let out: &str = with_conflict_retry("test", |attempt| {
            calls.set(calls.get() + 1);
            async move {
                if attempt < 2 {
                    Err(Error::Conflict("resourceVersion mismatch".to_string()))
                } else {
                    Ok("written")
                }
            }
        })
        .await
        .expect("the third attempt succeeds");

        assert_eq!(out, "written");
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn gives_up_after_the_upstream_bound_and_says_so() {
        let calls = Cell::new(0);
        let err = with_conflict_retry::<(), _, _>("patching PV", |_| {
            calls.set(calls.get() + 1);
            async { Err(Error::Conflict("resource was modified".to_string())) }
        })
        .await
        .expect_err("a permanent conflict must still surface");

        assert_eq!(
            calls.get(),
            MAX_CONFLICT_RETRIES,
            "the bound is upstream's MaxRetryWhenPatchConflicts"
        );
        match err {
            Error::Conflict(msg) => {
                assert!(
                    msg.contains("giving up after 5"),
                    "the message must say the retries were exhausted, got: {msg}"
                );
                assert!(msg.contains("patching PV"), "and name the operation");
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    /// A non-conflict error is a real failure: return it at once rather than
    /// hammering storage five times.
    #[tokio::test]
    async fn does_not_retry_other_errors() {
        let calls = Cell::new(0);
        let err = with_conflict_retry::<(), _, _>("test", |_| {
            calls.set(calls.get() + 1);
            async { Err(Error::NotFound("gone".to_string())) }
        })
        .await
        .expect_err("NotFound must propagate");

        assert_eq!(calls.get(), 1, "only conflicts are retried");
        assert!(matches!(err, Error::NotFound(_)));
    }
}
