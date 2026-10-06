//! Bounded retry for SQLite lock contention, and the retriable status an
//! exhausted retry surfaces as (#1621).
//!
//! Ported from:
//! - kine `pkg/drivers/generic/generic.go` `(*Generic).execute`: a bounded loop
//!   that re-runs a statement while the driver's `Retry(err)` classifier says
//!   so (the SQLite driver classifies `SQLITE_BUSY` / `SQLITE_LOCKED`), with a
//!   linear backoff between attempts. (kine is not checked out on this
//!   machine; cited from the k3s-io/kine source as recalled, not verbatim.)
//! - apiserver `storage/errors/storage.go` `InterpretListError` / `Get` /
//!   `Create` / `Update` / `Delete`: `storage.IsUnreachable` becomes
//!   `errors.NewServerTimeout(resource, op, 2)`.
//! - apimachinery `api/errors/errors.go:365` `NewServerTimeout`: reason
//!   `ServerTimeout`, `details.retryAfterSeconds`, and the message
//!   `The %s operation against %s could not be completed at this time, please
//!   try again.`
//!
//! Deliberate deviations: kine retries 20 times; we retry fewer because the
//! driver already waited out a 30s `busy_timeout` per attempt. Upstream's
//! `NewServerTimeout` is HTTP 500 with a `Retry-After` header; we keep that
//! exact status (client-go retries a 5xx that carries `Retry-After`) rather
//! than the 409/503 the issue sketched, since 409 means "re-read and retry"
//! and 503 has no apiserver storage mapping.

use rusternetes_common::types::{Status, StatusDetails};
use rusternetes_common::Error;
use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

/// `retryAfterSeconds` upstream passes from `InterpretXxxError`.
pub const RETRY_AFTER_SECONDS: i32 = 2;

/// Bounded linear-backoff policy.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts, including the first.
    pub attempts: u32,
    /// Sleep before retry `n` (n >= 1) is `step * n`.
    pub step: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 5,
            step: Duration::from_millis(100),
        }
    }
}

/// True when `msg` is SQLite reporting lock contention
/// (`SQLITE_BUSY` "database is locked", `SQLITE_LOCKED` "database table is
/// locked").
pub fn is_busy(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("database is locked")
        || m.contains("database table is locked")
        || m.contains("sqlite_busy")
}

/// Run `op`, re-running it while it fails with lock contention, up to
/// `policy.attempts` times. Any other error is returned immediately.
pub async fn retry_busy<T, E, F, Fut>(policy: RetryPolicy, mut op: F) -> Result<T, E>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Err(e) if is_busy(&e.to_string()) && attempt + 1 < policy.attempts => {
                attempt += 1;
                tokio::time::sleep(policy.step * attempt).await;
            }
            other => return other,
        }
    }
}

/// `NewServerTimeout(resource, op, 2)` for the resource named by a
/// `/registry/<resource>/...` key.
pub fn server_timeout(key: &str, operation: &str) -> Error {
    let resource = key
        .trim_start_matches("/registry/")
        .split('/')
        .next()
        .unwrap_or("")
        .to_string();
    let status = Status::failure_with_details(
        format!(
            "The {operation} operation against {resource} could not be completed at this time, please try again."
        ),
        "ServerTimeout",
        500,
        StatusDetails {
            name: Some(operation.to_string()),
            group: None,
            kind: Some(resource),
            uid: None,
            causes: None,
            retry_after_seconds: Some(RETRY_AFTER_SECONDS),
        },
    );
    Error::Status(Box::new(status))
}

/// Map a backend error from `operation` on `key`: exhausted lock contention
/// becomes the retriable `ServerTimeout`, anything else `Error::Storage`.
pub fn map_backend_error(key: &str, operation: &str, what: &str, err: &impl Display) -> Error {
    if is_busy(&err.to_string()) {
        server_timeout(key, operation)
    } else {
        Error::Storage(format!("Failed to {what}: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const FAST: RetryPolicy = RetryPolicy {
        attempts: 4,
        step: Duration::from_millis(1),
    };

    #[test]
    fn classifies_sqlite_lock_errors() {
        assert!(is_busy("database is locked"));
        assert!(is_busy(
            "error returned from database: (code: 5) database is locked"
        ));
        assert!(is_busy("database table is locked"));
        assert!(!is_busy("UNIQUE constraint failed"));
    }

    #[tokio::test]
    async fn retries_busy_until_success() {
        let n = AtomicU32::new(0);
        let r: Result<u32, String> = retry_busy(FAST, || async {
            if n.fetch_add(1, Ordering::SeqCst) < 2 {
                Err("database is locked".to_string())
            } else {
                Ok(7)
            }
        })
        .await;
        assert_eq!(r, Ok(7));
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn gives_up_after_bounded_attempts() {
        let n = AtomicU32::new(0);
        let r: Result<(), String> = retry_busy(FAST, || async {
            n.fetch_add(1, Ordering::SeqCst);
            Err("database is locked".to_string())
        })
        .await;
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn does_not_retry_other_errors() {
        let n = AtomicU32::new(0);
        let r: Result<(), String> = retry_busy(FAST, || async {
            n.fetch_add(1, Ordering::SeqCst);
            Err("disk I/O error".to_string())
        })
        .await;
        assert!(r.is_err());
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exhausted_lock_maps_to_retriable_server_timeout() {
        let e = map_backend_error(
            "/registry/pods/ns/p",
            "create",
            "create resource",
            &"database is locked",
        );
        let Error::Status(s) = e else {
            panic!("want Status, got {e:?}")
        };
        assert_eq!(s.reason.as_deref(), Some("ServerTimeout"));
        assert_eq!(s.details.as_ref().unwrap().retry_after_seconds, Some(2));
        assert_eq!(s.details.as_ref().unwrap().kind.as_deref(), Some("pods"));
    }

    #[test]
    fn other_errors_stay_storage() {
        let e = map_backend_error("/registry/pods/ns/p", "create", "create resource", &"boom");
        assert!(matches!(e, Error::Storage(_)));
    }
}
