//! Port of `pkg/util/goroutinemap/exponentialbackoff/exponential_backoff.go`.
//!
//! Shared by the pending-operation maps that upstream layers on it:
//! `goroutinemap` (the plugin manager's operation executor) and
//! `nestedpendingoperations` (the volume operation executor).

use std::time::{Duration, Instant};

/// `initialDurationBeforeRetry`: the wait after the first error; each
/// successive error doubles it.
pub const INITIAL_DURATION_BEFORE_RETRY: Duration = Duration::from_millis(500);

/// `maxDurationBeforeRetry`: the cap, "slightly offset from 2 minutes to make
/// timeouts due to this constant recognizable".
pub const MAX_DURATION_BEFORE_RETRY: Duration = Duration::from_secs(2 * 60 + 2);

/// `ExponentialBackoff`: the last error and the period retries are refused.
#[derive(Clone, Debug, Default)]
pub struct ExponentialBackoff {
    last_error: Option<String>,
    last_error_time: Option<Instant>,
    duration_before_retry: Duration,
}

impl ExponentialBackoff {
    /// `SafeToRetry`: `Err` while `time.Since(lastErrorTime) <=
    /// durationBeforeRetry` (`exponentialBackoffError`). A zero value is safe:
    /// upstream's zero `lastErrorTime` is the year 1, so `Since` is huge.
    pub fn safe_to_retry(&self, operation_name: &str) -> Result<(), ExponentialBackoffError> {
        match self.last_error_time {
            Some(t) if t.elapsed() <= self.duration_before_retry => Err(ExponentialBackoffError {
                operation_name: operation_name.to_string(),
                last_error: self.last_error.clone().unwrap_or_default(),
                duration_before_retry: self.duration_before_retry,
            }),
            _ => Ok(()),
        }
    }

    /// `Update`: double the backoff (500ms first, capped), record the error.
    pub fn update(&mut self, err: &str) {
        self.duration_before_retry = if self.duration_before_retry.is_zero() {
            INITIAL_DURATION_BEFORE_RETRY
        } else {
            (self.duration_before_retry * 2).min(MAX_DURATION_BEFORE_RETRY)
        };
        self.last_error = Some(err.to_string());
        self.last_error_time = Some(Instant::now());
    }

    /// `GenerateNoRetriesPermittedMsg`.
    pub fn no_retries_permitted_msg(&self, operation_name: &str) -> String {
        format!(
            "Operation for {operation_name:?} failed. No retries permitted for {:?} (durationBeforeRetry {:?}). Error: {}",
            self.duration_before_retry,
            self.duration_before_retry,
            self.last_error.as_deref().unwrap_or_default()
        )
    }

    pub fn duration_before_retry(&self) -> Duration {
        self.duration_before_retry
    }
}

/// `exponentialBackoffError`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExponentialBackoffError {
    operation_name: String,
    last_error: String,
    duration_before_retry: Duration,
}

impl std::fmt::Display for ExponentialBackoffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Failed to create operation with name {:?}. An operation with that name failed. No retries permitted for {:?}. Last error: {:?}.",
            self.operation_name, self.duration_before_retry, self.last_error
        )
    }
}

impl std::error::Error for ExponentialBackoffError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_from_500ms_and_caps() {
        let mut b = ExponentialBackoff::default();
        assert!(b.safe_to_retry("x").is_ok());
        b.update("e");
        assert_eq!(b.duration_before_retry(), Duration::from_millis(500));
        assert!(b.safe_to_retry("x").is_err());
        b.update("e");
        assert_eq!(b.duration_before_retry(), Duration::from_secs(1));
        for _ in 0..20 {
            b.update("e");
        }
        assert_eq!(b.duration_before_retry(), MAX_DURATION_BEFORE_RETRY);
    }
}
