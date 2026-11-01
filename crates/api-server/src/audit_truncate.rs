//! The audit truncate backend.
//!
//! Ported from `staging/src/k8s.io/apiserver` (release-1.35):
//! - `plugin/pkg/audit/truncate/truncate.go` (`Config`, `backend.ProcessEvents`,
//!   `truncate`, `calcSize`);
//! - `pkg/server/options/audit.go` (`AuditTruncateOptions`,
//!   `NewAuditTruncateOptions`, `Validate`, `wrapBackend`).
//!
//! Deviations: `AuditBackend::log` delivers one event at a time, so the
//! batch-splitting half of `ProcessEvents` (`MaxBatchSize`) has nothing to
//! split; the option is kept and validated, as upstream's callers
//! (`WithAudit`) also hand the backend one event per call.

use crate::audit_webhook::handle_plugin_error;
use async_trait::async_trait;
use rusternetes_common::audit::{AuditBackend, AuditEvent, AuditLevel};
use std::sync::Arc;

/// `truncate.PluginName`.
pub const TRUNCATE_PLUGIN_NAME: &str = "truncate";
/// `truncate.annotationKey` / `annotationValue`.
const ANNOTATION_KEY: &str = "audit.k8s.io/truncated";
const ANNOTATION_VALUE: &str = "true";

/// `truncate.Config` (truncate.go:41-49).
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateConfig {
    pub max_event_size: i64,
    pub max_batch_size: i64,
}

/// `AuditTruncateOptions` (options/audit.go:109-115).
#[derive(Debug, Clone, PartialEq)]
pub struct TruncateOptions {
    pub enabled: bool,
    pub config: TruncateConfig,
}

impl Default for TruncateOptions {
    /// `NewAuditTruncateOptions` (options/audit.go:168-177).
    fn default() -> Self {
        Self {
            enabled: false,
            config: TruncateConfig {
                max_batch_size: 10 * 1024 * 1024,
                max_event_size: 100 * 1024,
            },
        }
    }
}

impl TruncateOptions {
    /// `AuditTruncateOptions.Validate` (options/audit.go:403-413).
    pub fn validate(&self, plugin_name: &str) -> Result<(), String> {
        let c = &self.config;
        if c.max_event_size <= 0 {
            return Err(format!(
                "invalid audit truncate {plugin_name} max event size {}, must be a positive number",
                c.max_event_size
            ));
        }
        if c.max_batch_size < c.max_event_size {
            return Err(format!(
                "invalid audit truncate {plugin_name} max batch size {}, must be greater than max event size ({})",
                c.max_batch_size, c.max_event_size
            ));
        }
        Ok(())
    }

    /// `AuditTruncateOptions.wrapBackend` (options/audit.go:428-433).
    pub fn wrap_backend(&self, delegate: Arc<dyn AuditBackend>) -> Arc<dyn AuditBackend> {
        if !self.enabled {
            return delegate;
        }
        Arc::new(TruncateBackend::new(delegate, self.config.clone()))
    }
}

/// `truncate.backend` (truncate.go:52-61).
pub struct TruncateBackend {
    delegate: Arc<dyn AuditBackend>,
    config: TruncateConfig,
}

impl TruncateBackend {
    /// `truncate.NewBackend`.
    pub fn new(delegate: Arc<dyn AuditBackend>, config: TruncateConfig) -> Self {
        Self { delegate, config }
    }
}

/// `calcSize`: the length of the encoded event.
fn calc_size(event: &AuditEvent) -> Result<i64, String> {
    serde_json::to_vec(event)
        .map(|b| b.len() as i64)
        .map_err(|e| e.to_string())
}

/// `truncate` (truncate.go:121-136): drop the request and response objects
/// and mark the event.
fn truncate(event: &AuditEvent) -> AuditEvent {
    let mut e = event.clone();
    e.request_object = None;
    e.response_object = None;
    e.annotations
        .get_or_insert_with(Default::default)
        .insert(ANNOTATION_KEY.to_string(), ANNOTATION_VALUE.to_string());
    e
}

#[async_trait]
impl AuditBackend for TruncateBackend {
    /// `backend.ProcessEvents` (truncate.go:84-119) for one event. A too large
    /// event at `Request` level or above is truncated first; one still too
    /// large is reported through `HandlePluginError` and dropped, and the call
    /// still succeeds (`success` is only cleared by the delegate).
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        let mut event = event;
        let mut size = calc_size(&event);
        if let Ok(s) = size {
            if s > self.config.max_event_size
                && matches!(
                    event.level,
                    AuditLevel::Request | AuditLevel::RequestResponse
                )
            {
                event = truncate(&event);
                size = calc_size(&event);
            }
        }
        match size {
            Err(e) => {
                handle_plugin_error(TRUNCATE_PLUGIN_NAME, &e, 1);
                Ok(())
            }
            Ok(s) if s > self.config.max_event_size => {
                handle_plugin_error(
                    TRUNCATE_PLUGIN_NAME,
                    "event is too large even after truncating",
                    1,
                );
                Ok(())
            }
            Ok(_) => self.delegate.log(event).await,
        }
    }

    async fn flush(&self) -> Result<(), String> {
        self.delegate.flush().await
    }
}
