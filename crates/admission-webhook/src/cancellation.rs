//! The request cancellation signal webhook dispatch observes, the Rust
//! counterpart of the request `context.Context` being `context.Canceled`.
//!
//! Upstream's dispatchers tell a call that failed because the *request* was
//! cancelled (the client went away) from a webhook that failed: the former is
//! kept out of the rejection metric and out of fail-open
//! (validating/dispatcher.go:179-182, :201-204; mutating/dispatcher.go:174-177,
//! :206-209), and the mutating dispatcher then stops with a timeout error
//! once the parent context is done (mutating/dispatcher.go:216-223;
//! validating/dispatcher.go:118-124 checks before spawning the calls).
//!
//! A caller that can observe the client going away wraps the admission call in
//! [`with_request_cancellation`] and trips the [`RequestCanceller`].

use std::future::Future;
use tokio::sync::watch;

tokio::task_local! {
    static REQUEST_CANCELLATION: RequestCancellation;
}

/// The read side: the request's "context", cancelled or not.
#[derive(Clone, Debug)]
pub struct RequestCancellation(watch::Receiver<bool>);

/// The write side: `cancel()` is Go's `cancel()` of the request context.
#[derive(Debug)]
pub struct RequestCanceller(watch::Sender<bool>);

/// A fresh, not yet cancelled request context.
pub fn request_cancellation() -> (RequestCanceller, RequestCancellation) {
    let (tx, rx) = watch::channel(false);
    (RequestCanceller(tx), RequestCancellation(rx))
}

impl RequestCanceller {
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
}

impl RequestCancellation {
    /// `ctx.Err() == context.Canceled`.
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    /// Resolves once the request is cancelled (`<-ctx.Done()`). Never
    /// resolves if the canceller is dropped without cancelling.
    pub async fn cancelled(&self) {
        let mut rx = self.0.clone();
        if rx.wait_for(|c| *c).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// Run `fut` with `cancellation` as the request context of any webhook
/// dispatch inside it.
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
