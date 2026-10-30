//! Unit tests moved from `src/secure_serving/webhook.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use async_trait::async_trait;
    pub use serde_json::Value;
    pub use std::future::Future;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_middleware::secure_serving::webhook::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn err(code: u16, reason: &str) -> ReviewError {
        ReviewError {
            status_code: code,
            reason: reason.into(),
            message: format!("{code} {reason}"),
            connection_reset: false,
            suggests_client_delay: false,
        }
    }

    #[test]
    fn default_backoff_matches_upstream() {
        let b = Backoff::default_auth_webhook();
        assert_eq!(b.duration, Duration::from_millis(500));
        assert_eq!((b.factor, b.jitter, b.steps), (1.5, 0.2, 5));
    }

    // backoff.go `delay`: the wait is Duration (+ up to Jitter*Duration), the
    // stored Duration grows by Factor, Steps counts down.
    #[test]
    fn step_grows_by_factor_with_bounded_jitter() {
        let mut b = Backoff::default_auth_webhook();
        let w1 = b.step();
        assert!(
            w1 >= Duration::from_millis(500) && w1 <= Duration::from_millis(600),
            "{w1:?}"
        );
        assert_eq!(b.duration, Duration::from_millis(750));
        assert_eq!(b.steps, 4);
        let w2 = b.step();
        assert!(
            w2 >= Duration::from_millis(750) && w2 <= Duration::from_millis(900),
            "{w2:?}"
        );
    }

    // webhook.go:63-73
    #[test]
    fn should_retry_matches_default_should_retry() {
        assert!(default_should_retry(&err(500, "InternalError")));
        assert!(default_should_retry(&err(504, "Timeout")));
        assert!(default_should_retry(&err(429, "TooManyRequests")));
        let mut reset = err(0, "");
        reset.connection_reset = true;
        assert!(default_should_retry(&reset));
        let mut delay = err(503, "ServiceUnavailable");
        delay.suggests_client_delay = true;
        assert!(default_should_retry(&delay));
        assert!(!default_should_retry(&err(403, "Forbidden")));
        assert!(!default_should_retry(&err(401, "Unauthorized")));
        assert!(!default_should_retry(&err(0, "")));
    }

    fn fast(steps: i32) -> Backoff {
        Backoff {
            duration: Duration::from_millis(1),
            factor: 1.0,
            jitter: 0.0,
            steps,
        }
    }

    // webhook.go:121-145: a retriable error is retried `Steps` times, then the
    // LAST webhook error is returned.
    #[tokio::test]
    async fn retries_retriable_errors_then_returns_the_last_error() {
        let calls = AtomicUsize::new(0);
        let r = with_exponential_backoff(
            fast(3),
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move { Err(err(500, &format!("attempt-{n}"))) }
            },
            default_should_retry,
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(r, Err("500 attempt-2".to_string()));
    }

    // A non-retriable error returns at once.
    #[tokio::test]
    async fn non_retriable_error_returns_immediately() {
        let calls = AtomicUsize::new(0);
        let r = with_exponential_backoff(
            fast(5),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err(err(403, "Forbidden")) }
            },
            default_should_retry,
        )
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(r, Err("403 Forbidden".to_string()));
    }

    #[tokio::test]
    async fn success_after_a_retry() {
        let calls = AtomicUsize::new(0);
        let r = with_exponential_backoff(
            fast(5),
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        Err(err(500, "InternalError"))
                    } else {
                        Ok(())
                    }
                }
            },
            default_should_retry,
        )
        .await;
        assert_eq!(r, Ok(()));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
