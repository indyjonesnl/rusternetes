//! Tests for the log backend's batch options: `defaultLogBatchConfig` and
//! `AuditBatchOptions.wrapBackend` over the log delegate
//! (`pkg/server/options/audit.go`, `plugin/pkg/audit/log/backend.go`).

use async_trait::async_trait;
use rusternetes_api_server::audit_webhook::*;
use rusternetes_common::audit::{AuditBackend, AuditEvent, AuditLevel, AuditStage, UserInfo};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn event() -> AuditEvent {
    AuditEvent {
        api_version: "audit.k8s.io/v1".into(),
        kind: "Event".into(),
        level: AuditLevel::Metadata,
        audit_id: "id".into(),
        stage: AuditStage::ResponseComplete,
        request_uri: "/api".into(),
        verb: "get".into(),
        user: UserInfo {
            username: "u".into(),
            uid: String::new(),
            groups: vec![],
            extra: None,
        },
        user_agent: None,
        source_ips: vec![],
        object_ref: None,
        response_status: None,
        response_object: None,
        request_object: None,
        impersonated_user: None,
        request_received_timestamp: chrono::Utc::now(),
        stage_timestamp: chrono::Utc::now(),
        annotations: None,
    }
}

struct Log {
    fail: bool,
    got: Mutex<usize>,
}

#[async_trait]
impl AuditBackend for Log {
    async fn log(&self, _: AuditEvent) -> Result<(), String> {
        *self.got.lock().unwrap() += 1;
        if self.fail {
            Err("disk full".into())
        } else {
            Ok(())
        }
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

fn opts(mode: &str) -> BatchOptions {
    BatchOptions {
        mode: mode.into(),
        batch_config: BatchConfig::default_log(),
    }
}

#[test]
fn default_log_config_matches_upstream() {
    let c = BatchConfig::default_log();
    assert_eq!(c.buffer_size, 10000);
    assert_eq!(c.max_batch_size, 1);
    assert_eq!(c.max_batch_wait, Duration::ZERO);
    assert!(!c.throttle_enable);
    assert!(!c.async_delegate);
    assert!(opts("batch").validate("log").is_ok());
    assert_eq!(
        opts("bogus").validate("log").unwrap_err(),
        "invalid audit log mode bogus, allowed modes are \"batch,blocking,blocking-strict\""
    );
}

#[tokio::test]
async fn blocking_ignores_log_errors_and_strict_does_not() {
    let log = Arc::new(Log {
        fail: true,
        got: Mutex::new(0),
    });
    let (b, buffered) = opts("blocking").wrap_backend(Arc::new(LogDelegate(log.clone())));
    assert!(buffered.is_none());
    assert!(b.log(event()).await.is_ok());
    let (b, _) = opts("blocking-strict").wrap_backend(Arc::new(LogDelegate(log.clone())));
    assert!(b.log(event()).await.is_err());
    assert_eq!(*log.got.lock().unwrap(), 2);
}

#[tokio::test]
async fn batch_mode_buffers_and_flushes_on_shutdown() {
    let log = Arc::new(Log {
        fail: false,
        got: Mutex::new(0),
    });
    let (b, buffered) = opts("batch").wrap_backend(Arc::new(LogDelegate(log.clone())));
    for _ in 0..3 {
        b.log(event()).await.unwrap();
    }
    buffered.unwrap().shutdown().await;
    assert_eq!(*log.got.lock().unwrap(), 3);
}
