//! Unit tests moved from `src/cancellation.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::future::Future;
    pub use tokio::sync::watch;
    pub use tokio::time::Instant;
}

mod tests {
    use super::__parent::*;
    use rusternetes_common::cancellation::*;
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
        let e: rusternetes_common::Error = ContextError::DeadlineExceeded.into();
        match e {
            rusternetes_common::Error::Status(s) => {
                assert_eq!(s.code, Some(504));
                assert_eq!(s.reason.as_deref(), Some("Timeout"));
            }
            other => panic!("{other:?}"),
        }
    }
}
