//! Tests for `src/audit_truncate.rs`, mirroring
//! `plugin/pkg/audit/truncate/truncate_test.go` (`TestTruncatingEvents`)
//! and `options/audit.go` `AuditTruncateOptions`.

use async_trait::async_trait;
use rusternetes_api_server::audit_truncate::*;
use rusternetes_common::audit::{AuditBackend, AuditEvent, AuditLevel, AuditStage, UserInfo};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const MAX_EVENT_SIZE: i64 = 100 * 1024;

fn event(level: AuditLevel) -> AuditEvent {
    AuditEvent {
        api_version: "audit.k8s.io/v1".into(),
        kind: "Event".into(),
        level,
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
        request_received_timestamp: chrono::Utc::now(),
        stage_timestamp: chrono::Utc::now(),
        annotations: None,
    }
}

#[derive(Default)]
struct Fake(Mutex<Vec<AuditEvent>>);

#[async_trait]
impl AuditBackend for Fake {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        self.0.lock().unwrap().push(event);
        Ok(())
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

fn backend(fake: &Arc<Fake>) -> TruncateBackend {
    TruncateBackend::new(fake.clone(), TruncateOptions::default().config)
}

fn big() -> serde_json::Value {
    serde_json::Value::String("A".repeat(MAX_EVENT_SIZE as usize))
}

#[tokio::test]
async fn empty_event_is_not_truncated() {
    let fake = Arc::new(Fake::default());
    backend(&fake)
        .log(event(AuditLevel::Metadata))
        .await
        .unwrap();
    let got = fake.0.lock().unwrap();
    assert_eq!(got.len(), 1);
    assert!(got[0].annotations.is_none());
}

#[tokio::test]
async fn too_large_body_is_truncated() {
    let fake = Arc::new(Fake::default());
    let mut e = event(AuditLevel::RequestResponse);
    e.response_object = Some(big());
    backend(&fake).log(e).await.unwrap();
    let got = fake.0.lock().unwrap();
    assert_eq!(got.len(), 1);
    assert!(got[0].response_object.is_none());
    assert_eq!(
        got[0].annotations.as_ref().unwrap()["audit.k8s.io/truncated"],
        "true"
    );
}

#[tokio::test]
async fn too_large_metadata_is_dropped_and_still_succeeds() {
    let fake = Arc::new(Fake::default());
    let mut e = event(AuditLevel::Metadata);
    e.annotations = Some(HashMap::from([(
        "key".to_string(),
        "A".repeat(MAX_EVENT_SIZE as usize),
    )]));
    assert!(backend(&fake).log(e).await.is_ok());
    assert!(fake.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn too_large_body_below_request_level_is_dropped_untruncated() {
    // `event.Level.GreaterOrEqual(LevelRequest)` gates the truncation.
    let fake = Arc::new(Fake::default());
    let mut e = event(AuditLevel::Metadata);
    e.response_object = Some(big());
    backend(&fake).log(e).await.unwrap();
    assert!(fake.0.lock().unwrap().is_empty());
}

#[test]
fn defaults_and_validate() {
    let o = TruncateOptions::default();
    assert!(!o.enabled);
    assert_eq!(o.config.max_batch_size, 10 * 1024 * 1024);
    assert_eq!(o.config.max_event_size, 100 * 1024);
    assert!(o.validate("webhook").is_ok());

    let mut bad = o.clone();
    bad.config.max_event_size = 0;
    assert_eq!(
        bad.validate("webhook").unwrap_err(),
        "invalid audit truncate webhook max event size 0, must be a positive number"
    );
    let mut bad = o;
    bad.config.max_batch_size = 10;
    assert_eq!(
        bad.validate("webhook").unwrap_err(),
        "invalid audit truncate webhook max batch size 10, must be greater than max event size (102400)"
    );
}

#[tokio::test]
async fn wrap_backend_only_when_enabled() {
    let fake = Arc::new(Fake::default());
    let mut o = TruncateOptions::default();
    let mut e = event(AuditLevel::Metadata);
    e.response_object = Some(big());
    // Disabled: the delegate itself, so an oversized event passes through.
    o.wrap_backend(fake.clone()).log(e.clone()).await.unwrap();
    assert_eq!(fake.0.lock().unwrap().len(), 1);
    o.enabled = true;
    o.wrap_backend(fake.clone()).log(e).await.unwrap();
    assert_eq!(fake.0.lock().unwrap().len(), 1);
}
