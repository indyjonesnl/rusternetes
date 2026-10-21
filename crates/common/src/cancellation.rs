//! The request `context.Context`: cancellation (client went away) plus a
//! deadline, carried as a task-local so everything a request touches can
//! observe it without a `ctx` parameter on every call.
//!
//! Go threads `ctx` explicitly: `WithRequestDeadline`
//! (endpoints/filters/request_deadline.go:91-96) puts a deadline on
//! `req.Context()`, net/http cancels that context when the client's connection
//! goes away (`connReader.backgroundRead` -> `cancelCtx`, net/http/server.go),
//! and every etcd client call selects on `ctx.Done()`
//! (storage/etcd3/store.go). Here `StorageBackend` selects on [`done`] at its
//! single dispatch point and the admission webhook dispatchers read
//! [`current_request_cancellation`] (validating/dispatcher.go:118-124,
//! :179-204; mutating/dispatcher.go:174-223).
//!
//! The deadline is NOT a cancel: like Go's `ctx.Err()`,
//! [`RequestCancellation::err`] reports [`ContextError::Canceled`] after
//! [`RequestCanceller::cancel`] and [`ContextError::DeadlineExceeded`] once
//! the deadline passes (webhook metrics tell the two apart: only
//! `context.Canceled` is kept out of them).
//!
//! Task-locals do not cross `tokio::spawn`: capture
//! [`current_request_cancellation`] before spawning and re-scope it with
//! [`with_request_cancellation`] inside.
//!
//! [`done`]: RequestCancellation::done

use std::future::Future;
use tokio::sync::watch;
use tokio::time::Instant;

tokio::task_local! {
    static REQUEST_CANCELLATION: RequestCancellation;
}

/// `ctx.Err()` of a done context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextError {
    /// `context.Canceled`: the client went away.
    Canceled,
    /// `context.DeadlineExceeded`: the request deadline passed.
    DeadlineExceeded,
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ContextError::Canceled => "context canceled",
            ContextError::DeadlineExceeded => "context deadline exceeded",
        })
    }
}

impl std::error::Error for ContextError {}

/// A done request context fails the operation with a 504 `Timeout` Status,
/// `NewTimeoutError` (apimachinery/pkg/api/errors/errors.go:401-411) with the
/// context's error as the reason text. The response is normally discarded:
/// the timeout filter has already answered (or the client is gone).
impl From<ContextError> for crate::Error {
    fn from(e: ContextError) -> Self {
        let details = crate::types::StatusDetails {
            name: None,
            group: None,
            kind: None,
            uid: None,
            causes: None,
            retry_after_seconds: Some(0),
        };
        crate::Error::Status(Box::new(crate::types::Status::failure_with_details(
            format!("Timeout: request did not complete: {e}"),
            "Timeout",
            504,
            details,
        )))
    }
}

/// The read side: the request's "context".
#[derive(Clone, Debug)]
pub struct RequestCancellation {
    cancelled: watch::Receiver<bool>,
    deadline: Option<Instant>,
}

/// The write side: `cancel()` is Go's `cancel()` of the request context.
/// Dropping it without cancelling leaves the context live (the deadline, if
/// any, still applies).
#[derive(Debug)]
pub struct RequestCanceller(watch::Sender<bool>);

/// A fresh, not yet cancelled request context without a deadline.
pub fn request_cancellation() -> (RequestCanceller, RequestCancellation) {
    request_cancellation_with_deadline(None)
}

/// A fresh request context with `context.WithDeadline(ctx, deadline)`.
pub fn request_cancellation_with_deadline(
    deadline: Option<Instant>,
) -> (RequestCanceller, RequestCancellation) {
    let (tx, rx) = watch::channel(false);
    (
        RequestCanceller(tx),
        RequestCancellation {
            cancelled: rx,
            deadline,
        },
    )
}

impl RequestCanceller {
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
}

impl RequestCancellation {
    /// `ctx.Deadline()`.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// `ctx.Err() == context.Canceled`.
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    /// `ctx.Err()`: `None` while the context is live.
    pub fn err(&self) -> Option<ContextError> {
        if self.is_cancelled() {
            Some(ContextError::Canceled)
        } else if self.deadline.is_some_and(|d| Instant::now() >= d) {
            Some(ContextError::DeadlineExceeded)
        } else {
            None
        }
    }

    /// `ctx.Err() != nil`, i.e. the `select { case <-ctx.Done(): .. default: }`
    /// poll.
    pub fn is_done(&self) -> bool {
        self.err().is_some()
    }

    /// Resolves once the request is cancelled (the cancel half of
    /// `<-ctx.Done()`). Never resolves if the canceller is dropped without
    /// cancelling.
    pub async fn cancelled(&self) {
        let mut rx = self.cancelled.clone();
        if rx.wait_for(|c| *c).await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    /// `<-ctx.Done()`: resolves with `ctx.Err()` once the request is
    /// cancelled or its deadline passes, whichever comes first. Never
    /// resolves for a context with neither a live canceller nor a deadline.
    pub async fn done(&self) -> ContextError {
        let deadline = async {
            match self.deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            _ = self.cancelled() => ContextError::Canceled,
            _ = deadline => self.err().unwrap_or(ContextError::DeadlineExceeded),
        }
    }
}

/// Run `fut` with `cancellation` as the request context of everything inside
/// it.
pub async fn with_request_cancellation<F: Future>(
    cancellation: RequestCancellation,
    fut: F,
) -> F::Output {
    REQUEST_CANCELLATION.scope(cancellation, fut).await
}

/// The request context of the current task, if one was set. Task-locals do not
/// cross `tokio::spawn`: capture this before spawning and carry it in.
pub fn current_request_cancellation() -> Option<RequestCancellation> {
    REQUEST_CANCELLATION.try_with(Clone::clone).ok()
}

/// Run `op` unless the current request context is done first
/// (`select { case <-ctx.Done(): ... }` around a blocking call). With no
/// request context in scope, or one that is never done, this is just `op`.
///
/// Cancelling drops `op` at its next await point; a statement already handed
/// to the backend may still complete, as with a cancelled etcd client call.
pub async fn run_until_done<F, T>(op: F) -> Result<T, ContextError>
where
    F: Future<Output = T>,
{
    match current_request_cancellation() {
        None => Ok(op.await),
        Some(ctx) => {
            if let Some(e) = ctx.err() {
                return Err(e);
            }
            tokio::select! {
                biased;
                r = op => Ok(r),
                e = ctx.done() => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn no_context_runs_op() {
        assert_eq!(run_until_done(async { 7 }).await, Ok(7));
    }

    #[tokio::test]
    async fn cancel_ends_pending_op() {
        let (canceller, ctx) = request_cancellation();
        let r = with_request_cancellation(ctx, async {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                canceller.cancel();
            });
            run_until_done(std::future::pending::<()>()).await
        })
        .await;
        assert_eq!(r, Err(ContextError::Canceled));
    }

    #[tokio::test]
    async fn deadline_is_not_a_cancel() {
        let (_c, ctx) =
            request_cancellation_with_deadline(Some(Instant::now() + Duration::from_millis(20)));
        let probe = ctx.clone();
        let r = with_request_cancellation(ctx, run_until_done(std::future::pending::<()>())).await;
        assert_eq!(r, Err(ContextError::DeadlineExceeded));
        assert!(!probe.is_cancelled());
        assert!(probe.is_done());
    }

    #[tokio::test]
    async fn cancel_takes_precedence_in_err() {
        let (c, ctx) = request_cancellation_with_deadline(Some(Instant::now()));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(ctx.err(), Some(ContextError::DeadlineExceeded));
        c.cancel();
        assert_eq!(ctx.err(), Some(ContextError::Canceled));
    }

    #[tokio::test]
    async fn precancelled_op_is_not_polled() {
        let (c, ctx) = request_cancellation();
        c.cancel();
        let polled = std::sync::atomic::AtomicBool::new(false);
        let r = with_request_cancellation(
            ctx,
            run_until_done(async {
                polled.store(true, std::sync::atomic::Ordering::SeqCst);
            }),
        )
        .await;
        assert_eq!(r, Err(ContextError::Canceled));
        assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn live_context_passes_result_through() {
        let (_c, ctx) = request_cancellation();
        let r = with_request_cancellation(ctx, run_until_done(async { "ok" })).await;
        assert_eq!(r, Ok("ok"));
    }

    #[test]
    fn context_error_is_a_504_timeout_status() {
        let e: crate::Error = ContextError::DeadlineExceeded.into();
        match e {
            crate::Error::Status(s) => {
                assert_eq!(s.code, Some(504));
                assert_eq!(s.reason.as_deref(), Some("Timeout"));
            }
            other => panic!("{other:?}"),
        }
    }
}
