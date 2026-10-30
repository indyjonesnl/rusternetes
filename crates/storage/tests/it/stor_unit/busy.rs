//! Unit tests moved from `src/busy.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use rusternetes_common::types::{Status, StatusDetails};
    pub use rusternetes_common::Error;
    pub use std::fmt::Display;
    pub use std::future::Future;
    pub use std::time::Duration;
}

mod tests {
    use super::__parent::*;
    use rusternetes_storage::busy::*;
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
