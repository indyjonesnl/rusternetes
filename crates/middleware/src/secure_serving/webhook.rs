//! The client side of the delegated TokenReview / SubjectAccessReview calls.
//!
//! Upstream sources (release-1.35):
//! * `staging/src/k8s.io/apimachinery/pkg/util/wait/backoff.go:58-65,493-518`
//!   (`Backoff.Step`, `ExponentialBackoffWithContext`) and `wait.go:92-98` (`Jitter`)
//! * `staging/src/k8s.io/apiserver/pkg/util/webhook/webhook.go:47-73,121-145`
//!   (`DefaultShouldRetry`, `WithExponentialBackoff`)
//! * `staging/src/k8s.io/apiserver/pkg/server/options/authentication.go:46-53`
//!   (`DefaultAuthWebhookRetryBackoff`)

use async_trait::async_trait;
use serde_json::Value;
use std::future::Future;
use std::time::Duration;

/// `wait.Backoff` (the fields the webhook retry uses; `Cap` is unset by every
/// delegating caller).
#[derive(Debug, Clone, PartialEq)]
pub struct Backoff {
    pub duration: Duration,
    pub factor: f64,
    pub jitter: f64,
    pub steps: i32,
}

impl Backoff {
    /// `DefaultAuthWebhookRetryBackoff` (authentication.go:46-53).
    pub fn default_auth_webhook() -> Self {
        Backoff {
            duration: Duration::from_millis(500),
            factor: 1.5,
            jitter: 0.2,
            steps: 5,
        }
    }

    /// `Backoff.Step` (backoff.go:58-65, `delay`): the wait before the next
    /// attempt; mutates the backoff.
    pub fn step(&mut self) -> Duration {
        // delay(): when steps is non-positive, do not alter the base duration.
        if self.steps < 1 {
            if self.jitter > 0.0 {
                return jitter(self.duration, self.jitter);
            }
            return self.duration;
        }
        self.steps -= 1;
        let next = if self.factor != 0.0 {
            self.duration.mul_f64(self.factor)
        } else {
            self.duration
        };
        let wait = if self.jitter > 0.0 {
            jitter(self.duration, self.jitter)
        } else {
            self.duration
        };
        self.duration = next;
        wait
    }
}

/// `wait.Jitter` (wait.go:92-98): `d + rand[0,1) * maxFactor * d`.
fn jitter(d: Duration, max_factor: f64) -> Duration {
    let r = (uuid::Uuid::new_v4().as_u128() >> 64) as f64 / (u64::MAX as f64);
    d + d.mul_f64(r * max_factor)
}

/// The failure of one `Create` call: what `DefaultShouldRetry` looks at.
#[derive(Debug, Clone, PartialEq)]
pub struct ReviewError {
    /// HTTP status of the response, 0 when there was none (`statusCode != 0`
    /// is how upstream tells a response from a transport failure).
    pub status_code: u16,
    /// `StatusReason` of the response (`apierrors.ReasonForError`).
    pub reason: String,
    pub message: String,
    /// `utilnet.IsConnectionReset`.
    pub connection_reset: bool,
    /// `apierrors.SuggestsClientDelay`.
    pub suggests_client_delay: bool,
}

impl std::fmt::Display for ReviewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ReviewError {}

/// `DefaultShouldRetry` (webhook.go:63-73).
pub fn default_should_retry(err: &ReviewError) -> bool {
    // IsConnectionReset || IsHTTP2ConnectionLost || IsInternalError ||
    // IsTimeout || IsTooManyRequests, then SuggestsClientDelay.
    let unknown = !rusternetes_client::api_status::KNOWN_REASONS.contains(&err.reason.as_str());
    err.connection_reset
        // IsInternalError (errors.go:711-720)
        || err.reason == "InternalError"
        || (unknown && err.status_code == 500)
        // IsTimeout (errors.go:689-698)
        || err.reason == "Timeout"
        || (unknown && err.status_code == 504)
        // IsTooManyRequests (errors.go:725-738): the code check ignores the reason
        || err.reason == "TooManyRequests"
        || err.status_code == 429
        || err.suggests_client_delay
}

/// `WithExponentialBackoff` (webhook.go:121-145) over `ExponentialBackoffWithContext`.
/// Returns the last webhook error if there was one, else `webhook call failed: ...`.
pub async fn with_exponential_backoff<F, Fut>(
    mut backoff: Backoff,
    mut call: F,
    should_retry: fn(&ReviewError) -> bool,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), ReviewError>>,
{
    // The last actual webhook error wins over the loop's own error.
    let mut webhook_err: Option<ReviewError> = None;
    // ExponentialBackoffWithContext (backoff.go:493-518)
    let mut done = false;
    while backoff.steps > 0 {
        match call().await {
            Ok(()) => {
                webhook_err = None;
                done = true;
                break;
            }
            Err(e) => {
                let retry = should_retry(&e);
                webhook_err = Some(e);
                if !retry {
                    break;
                }
            }
        }
        if backoff.steps == 1 {
            break;
        }
        let wait = backoff.step();
        tokio::time::sleep(wait).await;
    }
    match webhook_err {
        Some(e) => Err(e.message),
        None if done => Ok(()),
        // ErrWaitTimeout
        None => Err("webhook call failed: timed out waiting for the condition".to_string()),
    }
}

/// POSTs a review object to the delegated API server. `Create` in
/// `tokenReviewer` / `subjectAccessReviewer`
/// (`plugin/pkg/authenticator/token/webhook/webhook.go:75-79`,
/// `plugin/pkg/authorizer/webhook/webhook.go:70-72`).
#[async_trait]
pub trait ReviewClient: Send + Sync {
    /// Returns the review the server answered with.
    async fn create(&self, path: &str, review: &Value) -> Result<Value, ReviewError>;
}

/// [`ReviewClient`] over the repo's API client.
pub struct ApiClientReviewClient(pub std::sync::Arc<rusternetes_client::http::ApiClient>);

#[async_trait]
impl ReviewClient for ApiClientReviewClient {
    async fn create(&self, path: &str, review: &Value) -> Result<Value, ReviewError> {
        match self.0.post::<Value, Value>(path, review).await {
            Ok(v) => Ok(v),
            Err(e) => {
                let (reason, code) = rusternetes_client::api_status::reason_and_code_for_error(&e);
                let connection_reset = e.chain().any(|c| {
                    c.downcast_ref::<std::io::Error>()
                        .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionReset)
                        || c.to_string().contains("connection reset")
                });
                Err(ReviewError {
                    status_code: code,
                    reason,
                    message: format!("{e:#}"),
                    connection_reset,
                    suggests_client_delay: false,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
