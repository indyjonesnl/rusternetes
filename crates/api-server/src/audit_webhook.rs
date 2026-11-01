//! The audit webhook backend and the buffered (batch) backend in front of it.
//!
//! Ported from `staging/src/k8s.io/apiserver` (release-1.35):
//! - `plugin/pkg/audit/webhook/webhook.go` (`backend.processEvents`,
//!   `retryOnError`, `NewBackend`);
//! - `plugin/pkg/audit/buffered/buffered.go` (`BatchConfig`,
//!   `bufferedBackend.Run/Shutdown/processIncomingEvents/collectEvents/
//!   processEvents/ProcessEvents`);
//! - `pkg/util/webhook/webhook.go` (`DefaultRetryBackoffWithInitialDelay`,
//!   `WithExponentialBackoff`, `LoadKubeconfig`: 30s timeout);
//! - `pkg/server/options/audit.go` (`AuditWebhookOptions`, `AuditBatchOptions`
//!   `Validate`/`wrapBackend`, `defaultWebhookBatchConfig`, `ignoreErrorsBackend`)
//!   and `pkg/audit/union.go` (`Union`).
//!
//! Not ported (tracked on #2605): dynamic backends (`AuditSink`, removed
//! upstream in 1.19), tracing spans, and the
//! `apiserver_audit_error_total` metric `HandlePluginError` bumps.

use crate::authorization_webhook::WebhookClientConfig;
use async_trait::async_trait;
use rusternetes_common::audit::{AuditBackend, AuditEvent};
use rusternetes_common::wait::Backoff;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{error, warn};

/// `pluginwebhook.PluginName`.
pub const WEBHOOK_PLUGIN_NAME: &str = "webhook";
/// `pluginlog.PluginName`.
pub const LOG_PLUGIN_NAME: &str = "log";
/// `pluginbuffered.PluginName`.
pub const BUFFERED_PLUGIN_NAME: &str = "buffered";
/// `pluginwebhook.DefaultInitialBackoffDelay`.
pub const DEFAULT_INITIAL_BACKOFF_DELAY: Duration = Duration::from_secs(10);

/// `ModeBatch` / `ModeBlocking` / `ModeBlockingStrict` (options/audit.go:78-92).
pub const MODE_BATCH: &str = "batch";
pub const MODE_BLOCKING: &str = "blocking";
pub const MODE_BLOCKING_STRICT: &str = "blocking-strict";
/// `AllowedModes`.
pub const ALLOWED_MODES: [&str; 3] = [MODE_BATCH, MODE_BLOCKING, MODE_BLOCKING_STRICT];

/// `auditv1.SchemeGroupVersion`, the only `knownGroupVersions` entry.
pub const AUDIT_GROUP_VERSION: &str = "audit.k8s.io/v1";

/// `DefaultRetryBackoffWithInitialDelay` (util/webhook/webhook.go:32-39).
pub fn default_retry_backoff_with_initial_delay(initial: Duration) -> Backoff {
    Backoff {
        duration: initial,
        factor: 1.5,
        jitter: 0.2,
        steps: 5,
    }
}

// ---------------------------------------------------------------------
// BatchConfig
// ---------------------------------------------------------------------

/// `buffered.BatchConfig` (buffered.go:32-50).
#[derive(Debug, Clone, PartialEq)]
pub struct BatchConfig {
    pub buffer_size: usize,
    pub max_batch_size: usize,
    pub max_batch_wait: Duration,
    pub throttle_enable: bool,
    pub throttle_qps: f32,
    pub throttle_burst: usize,
    pub async_delegate: bool,
}

impl BatchConfig {
    /// `defaultWebhookBatchConfig` (options/audit.go:587-599) with
    /// `defaultBatchBufferSize` 10000, `defaultBatchMaxSize` 400,
    /// `defaultBatchMaxWait` 30s, `defaultBatchThrottleQPS` 10 and
    /// `defaultBatchThrottleBurst` 15.
    pub fn default_webhook() -> Self {
        Self {
            buffer_size: 10000,
            max_batch_size: 400,
            max_batch_wait: Duration::from_secs(30),
            throttle_enable: true,
            throttle_qps: 10.0,
            throttle_burst: 15,
            async_delegate: true,
        }
    }
}

impl BatchConfig {
    /// `defaultLogBatchConfig` (options/audit.go:602-612): batching is not
    /// useful for the log backend, so `MaxBatchSize` is 1, `MaxBatchWait` is
    /// ignored, there is no throttle and the delegate runs synchronously.
    pub fn default_log() -> Self {
        Self {
            buffer_size: 10000,
            max_batch_size: 1,
            max_batch_wait: Duration::ZERO,
            throttle_enable: false,
            throttle_qps: 0.0,
            throttle_burst: 0,
            async_delegate: false,
        }
    }
}

/// `AuditBatchOptions` (options/audit.go:99-107).
#[derive(Debug, Clone)]
pub struct BatchOptions {
    pub mode: String,
    pub batch_config: BatchConfig,
}

impl BatchOptions {
    /// `validateBackendBatchOptions` (options/audit.go:200-224).
    pub fn validate(&self, plugin_name: &str) -> Result<(), String> {
        if !ALLOWED_MODES.contains(&self.mode.as_str()) {
            return Err(format!(
                "invalid audit {plugin_name} mode {}, allowed modes are \"{}\"",
                self.mode,
                ALLOWED_MODES.join(",")
            ));
        }
        if self.mode != MODE_BATCH {
            // Don't validate the unused options.
            return Ok(());
        }
        let c = &self.batch_config;
        if c.buffer_size == 0 {
            return Err(format!(
                "invalid audit batch {plugin_name} buffer size {}, must be a positive number",
                c.buffer_size
            ));
        }
        if c.max_batch_size == 0 {
            return Err(format!(
                "invalid audit batch {plugin_name} max batch size {}, must be a positive number",
                c.max_batch_size
            ));
        }
        if c.throttle_enable {
            if c.throttle_qps <= 0.0 {
                return Err(format!(
                    "invalid audit batch {plugin_name} throttle QPS {}, must be a positive number",
                    c.throttle_qps
                ));
            }
            if c.throttle_burst == 0 {
                return Err(format!(
                    "invalid audit batch {plugin_name} throttle burst {}, must be a positive number",
                    c.throttle_burst
                ));
            }
        }
        Ok(())
    }

    /// `AuditBatchOptions.wrapBackend` (options/audit.go:393-401).
    ///
    /// Also returns the buffered backend, when batching, so the caller can
    /// `shutdown()` it (the `Run`/`Shutdown` lifecycle of a buffered backend).
    pub fn wrap_backend(&self, delegate: Arc<dyn BatchBackend>) -> WrappedBackend {
        match self.mode.as_str() {
            MODE_BLOCKING_STRICT => (Arc::new(BlockingBackend(delegate)), None),
            MODE_BLOCKING => (Arc::new(IgnoreErrorsBackend(delegate)), None),
            _ => {
                let b = Arc::new(BufferedBackend::new(delegate, self.batch_config.clone()));
                (b.clone(), Some(b))
            }
        }
    }
}

// ---------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------

/// A backend ready to log to, and its buffered backend (batch mode) to shut
/// down.
pub type WrappedBackend = (Arc<dyn AuditBackend>, Option<Arc<BufferedBackend>>);

/// `audit.Backend.ProcessEvents(ev ...*Event) bool` for the delegate of a
/// buffered backend: a batch in, success out.
#[async_trait]
pub trait BatchBackend: Send + Sync {
    async fn process_events(&self, events: Vec<AuditEvent>) -> bool;
    /// `Backend.String()`.
    fn name(&self) -> String;
}

/// The log backend's `ProcessEvents` (plugin/pkg/audit/log/backend.go:60-90)
/// as a delegate for [`BatchOptions::wrap_backend`]: each event is written; a
/// failure is reported through `HandlePluginError` and fails the batch.
pub struct LogDelegate(pub Arc<dyn AuditBackend>);

#[async_trait]
impl BatchBackend for LogDelegate {
    async fn process_events(&self, events: Vec<AuditEvent>) -> bool {
        let mut success = true;
        for ev in events {
            if let Err(e) = self.0.log(ev).await {
                handle_plugin_error(LOG_PLUGIN_NAME, &e, 1);
                success = false;
            }
        }
        success
    }
    fn name(&self) -> String {
        LOG_PLUGIN_NAME.to_string()
    }
}

/// `audit.HandlePluginError` (pkg/audit/metrics.go): log the failure and the
/// events it cost.
pub(crate) fn handle_plugin_error(plugin: &str, err: &str, count: usize) {
    crate::audit_metrics::count_plugin_error(plugin, count);
    error!("Error in audit plugin '{plugin}' affecting {count} audit events: {err}");
}

/// A delegate used directly, as `--audit-webhook-mode=blocking-strict` does
/// (wrapBackend returns the delegate): each event is delivered before the
/// request goes on, and a failure is reported to the caller.
struct BlockingBackend(Arc<dyn BatchBackend>);

#[async_trait]
impl AuditBackend for BlockingBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        if self.0.process_events(vec![event]).await {
            Ok(())
        } else {
            Err(format!("audit backend {} failed", self.0.name()))
        }
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// `ignoreErrorsBackend` (options/audit.go:380-391): blocking, but a failure
/// never fails the request.
struct IgnoreErrorsBackend(Arc<dyn BatchBackend>);

#[async_trait]
impl AuditBackend for IgnoreErrorsBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        self.0.process_events(vec![event]).await;
        Ok(())
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

/// `audit.Union` (pkg/audit/union.go): every backend gets every event; the
/// result is the conjunction of theirs.
pub struct UnionBackend(pub Vec<Arc<dyn AuditBackend>>);

#[async_trait]
impl AuditBackend for UnionBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        let mut failed = None;
        for b in &self.0 {
            if let Err(e) = b.log(event.clone()).await {
                failed = Some(e);
            }
        }
        failed.map_or(Ok(()), Err)
    }
    async fn flush(&self) -> Result<(), String> {
        let mut failed = None;
        for b in &self.0 {
            if let Err(e) = b.flush().await {
                failed = Some(e);
            }
        }
        failed.map_or(Ok(()), Err)
    }
}

// ---------------------------------------------------------------------
// Webhook backend
// ---------------------------------------------------------------------

/// `auditinternal.EventList` as encoded for `audit.k8s.io/v1`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventList<'a> {
    kind: &'static str,
    api_version: &'static str,
    metadata: serde_json::Value,
    items: &'a [AuditEvent],
}

/// `webhook.backend` (webhook.go:81-84): POSTs batches of events to the
/// kubeconfig's server.
pub struct WebhookBackend {
    client: crate::authorization_webhook::WebhookHttp,
    config: WebhookClientConfig,
    retry_backoff: Backoff,
}

impl WebhookBackend {
    /// `webhook.NewBackend` (webhook.go:107-113) over `loadWebhook` and
    /// `LoadKubeconfig` (30s request timeout, no client-side rate limit).
    pub fn new(
        kubeconfig: &std::path::Path,
        group_version: &str,
        retry_backoff: Backoff,
    ) -> anyhow::Result<Self> {
        // `NewGenericWebhook`: "webhook plugin requires enabling extension
        // resource" for a version the audit scheme does not register.
        if group_version != AUDIT_GROUP_VERSION {
            anyhow::bail!("webhook plugin requires enabling extension resource: {group_version}");
        }
        let config = WebhookClientConfig::load(kubeconfig)?;
        // `AuditOptions.ApplyTo` (options/audit.go:300-312): with an egress
        // selector, `Lookup(ControlPlane)` is the webhook's custom dial
        // (`LoadKubeconfig`, util/webhook/webhook.go:157).
        Ok(Self {
            client: crate::authorization_webhook::WebhookHttp::for_config(&config)?,
            config,
            retry_backoff,
        })
    }

    async fn post_once(&self, body: &EventList<'_>) -> Result<(), String> {
        let resp = self
            .client
            .post_json(&self.config, body)
            .await
            .map_err(|e| e.message)?;
        // `transformResponse`: outside 200..=206 is an error.
        if !(200..=206).contains(&resp.status) {
            return Err(format!("webhook returned HTTP {}", resp.status));
        }
        Ok(())
    }

    /// `backend.processEvents` (webhook.go:121-141) inside
    /// `WithExponentialBackoff` (util/webhook/webhook.go:105-135) with
    /// `retryOnError`: every error is retried, up to `Steps` attempts.
    pub async fn try_process_events(&self, events: &[AuditEvent]) -> Result<(), String> {
        let body = EventList {
            kind: "EventList",
            api_version: AUDIT_GROUP_VERSION,
            metadata: serde_json::json!({}),
            items: events,
        };
        let mut backoff = self.retry_backoff.clone();
        let mut last = String::new();
        while backoff.steps > 0 {
            match self.post_once(&body).await {
                Ok(()) => return Ok(()),
                Err(e) => last = e,
            }
            if backoff.steps == 1 {
                break;
            }
            tokio::time::sleep(backoff.step()).await;
        }
        Err(last)
    }
}

#[async_trait]
impl BatchBackend for WebhookBackend {
    async fn process_events(&self, events: Vec<AuditEvent>) -> bool {
        match self.try_process_events(&events).await {
            Ok(()) => true,
            Err(e) => {
                handle_plugin_error(WEBHOOK_PLUGIN_NAME, &e, events.len());
                false
            }
        }
    }
    fn name(&self) -> String {
        WEBHOOK_PLUGIN_NAME.to_string()
    }
}

#[async_trait]
impl AuditBackend for WebhookBackend {
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        if self.process_events(vec![event]).await {
            Ok(())
        } else {
            Err("audit webhook failed".to_string())
        }
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Buffered backend
// ---------------------------------------------------------------------

/// `flowcontrol.NewTokenBucketRateLimiter(qps, burst)`; `Accept` blocks until
/// a token is available.
pub struct TokenBucket {
    qps: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(qps: f32, burst: usize) -> Self {
        Self {
            qps: qps as f64,
            burst: burst as f64,
            tokens: burst as f64,
            last: Instant::now(),
        }
    }

    pub async fn accept(&mut self) {
        loop {
            let now = Instant::now();
            self.tokens = (self.tokens + now.duration_since(self.last).as_secs_f64() * self.qps)
                .min(self.burst);
            self.last = now;
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return;
            }
            tokio::time::sleep(Duration::from_secs_f64((1.0 - self.tokens) / self.qps)).await;
        }
    }
}

/// Resolves once the stop signal has been given (a closed `stopCh`).
async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            // The sender is gone: treated as stopped.
            return;
        }
    }
}

/// `buffered.bufferedBackend` (buffered.go:52-81): events go into a bounded
/// buffer; a worker task batches them and hands the batches to the delegate.
pub struct BufferedBackend {
    delegate: Arc<dyn BatchBackend>,
    /// `buffer`; the sender is dropped on shutdown (`close(b.buffer)`).
    tx: std::sync::Mutex<Option<mpsc::Sender<AuditEvent>>>,
    stop_tx: watch::Sender<bool>,
    worker: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// The worker's half: the receiving end of the buffer and the settings.
pub struct Worker {
    pub delegate: Arc<dyn BatchBackend>,
    pub rx: mpsc::Receiver<AuditEvent>,
    pub max_batch_size: usize,
    pub max_batch_wait: Duration,
    pub async_delegate: bool,
    pub throttle: Option<TokenBucket>,
    /// `wg`: the in-flight asynchronous deliveries.
    pub inflight: JoinSet<()>,
}

impl BufferedBackend {
    /// `buffered.NewBackend` + `Run` (buffered.go:84-130): the worker starts
    /// here. Call from inside a tokio runtime.
    pub fn new(delegate: Arc<dyn BatchBackend>, config: BatchConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.buffer_size.max(1));
        let (stop_tx, stop_rx) = watch::channel(false);
        let worker = Worker {
            delegate: delegate.clone(),
            rx,
            max_batch_size: config.max_batch_size,
            max_batch_wait: config.max_batch_wait,
            async_delegate: config.async_delegate,
            throttle: config
                .throttle_enable
                .then(|| TokenBucket::new(config.throttle_qps, config.throttle_burst)),
            inflight: JoinSet::new(),
        };
        let handle = tokio::spawn(worker.run(stop_rx));
        Self {
            delegate,
            tx: std::sync::Mutex::new(Some(tx)),
            stop_tx,
            worker: tokio::sync::Mutex::new(Some(handle)),
        }
    }

    /// `Shutdown` after the stop channel closes (buffered.go:132-150): stop
    /// the worker, let it send what is buffered, wait for the deliveries
    /// still in flight.
    pub async fn shutdown(&self) {
        let _ = self.stop_tx.send(true);
        // `close(b.buffer)`: later events are refused ("audit backend shut down").
        self.tx.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = self.worker.lock().await.take() {
            let _ = h.await;
        }
    }

    /// `String()`.
    pub fn name(&self) -> String {
        format!("{BUFFERED_PLUGIN_NAME}<{}>", self.delegate.name())
    }
}

impl Worker {
    async fn run(mut self, mut stop: watch::Receiver<bool>) {
        self.process_incoming_events(&mut stop).await;
        // Handle the events received after the last buffer scraping and
        // before the buffer closed (buffered.go:98-117).
        self.rx.close();
        let (_never_tx, mut never) = watch::channel(false);
        loop {
            let events = self.collect_events(false, &mut never).await;
            let done = events.is_empty();
            self.process_events(events).await;
            if done {
                break;
            }
        }
        // `Shutdown`'s `wg.Wait()`.
        while self.inflight.join_next().await.is_some() {}
    }

    /// `processIncomingEvents` (buffered.go:152-188).
    async fn process_incoming_events(&mut self, stop: &mut watch::Receiver<bool>) {
        loop {
            // Only use max wait batching if batching is enabled.
            let events = self.collect_events(self.max_batch_size > 1, stop).await;
            self.process_events(events).await;
            if *stop.borrow() {
                return;
            }
        }
    }

    /// `collectEvents` (buffered.go:190-218): up to `max_batch_size` events;
    /// stops early when the timer passes, the buffer is closed and empty, or
    /// the stop signal is given. The timer starts when the collection does
    /// (`maxWaitTimer.Reset`).
    pub async fn collect_events(
        &mut self,
        with_timer: bool,
        stop: &mut watch::Receiver<bool>,
    ) -> Vec<AuditEvent> {
        let mut events = Vec::new();
        let timer = tokio::time::sleep(self.max_batch_wait);
        tokio::pin!(timer);
        for _ in 0..self.max_batch_size {
            tokio::select! {
                ev = self.rx.recv() => match ev {
                    Some(ev) => events.push(ev),
                    // Buffer closed and no new events will follow.
                    None => break,
                },
                _ = &mut timer, if with_timer => break,
                _ = stopped(stop) => break,
            }
        }
        events
    }

    /// `processEvents` (buffered.go:220-253).
    async fn process_events(&mut self, events: Vec<AuditEvent>) {
        if events.is_empty() {
            return;
        }
        if let Some(t) = self.throttle.as_mut() {
            t.accept().await;
        }
        let delegate = self.delegate.clone();
        if self.async_delegate {
            // Reap finished deliveries so the set does not grow without bound.
            while self.inflight.try_join_next().is_some() {}
            self.inflight.spawn(async move {
                delegate.process_events(events).await;
            });
        } else if let Err(e) = tokio::spawn(async move {
            delegate.process_events(events).await;
        })
        .await
        {
            // `runtime.HandleCrash`: a panic must not stop the worker.
            warn!("audit delegate panicked: {e}");
        }
    }
}

#[async_trait]
impl AuditBackend for BufferedBackend {
    /// `bufferedBackend.ProcessEvents` (buffered.go:255-285): enqueue without
    /// blocking; a full buffer or a shut down backend drops the event and
    /// reports it, and still answers true.
    async fn log(&self, event: AuditEvent) -> Result<(), String> {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let err = match tx {
            None => "audit backend shut down",
            Some(tx) => match tx.try_send(event) {
                Ok(()) => return Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => "audit buffer queue blocked",
                Err(mpsc::error::TrySendError::Closed(_)) => "audit backend shut down",
            },
        };
        handle_plugin_error(BUFFERED_PLUGIN_NAME, err, 1);
        Ok(())
    }
    async fn flush(&self) -> Result<(), String> {
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------

/// `AuditWebhookOptions` (options/audit.go:133-142).
#[derive(Debug, Clone)]
pub struct WebhookOptions {
    pub config_file: Option<String>,
    pub initial_backoff: Duration,
    pub group_version: String,
    pub batch: BatchOptions,
    /// `AuditWebhookOptions.TruncateOptions`.
    pub truncate: crate::audit_truncate::TruncateOptions,
}

impl WebhookOptions {
    /// `AuditWebhookOptions.enabled`.
    pub fn enabled(&self) -> bool {
        self.config_file.as_deref().is_some_and(|f| !f.is_empty())
    }

    /// `AuditWebhookOptions.Validate` (options/audit.go:551-568).
    pub fn validate(&self) -> Vec<String> {
        if !self.enabled() {
            return Vec::new();
        }
        let mut errs = Vec::new();
        if let Err(e) = self.batch.validate(WEBHOOK_PLUGIN_NAME) {
            errs.push(e);
        }
        if let Err(e) = self.truncate.validate(WEBHOOK_PLUGIN_NAME) {
            errs.push(e);
        }
        // `validateGroupVersionString` (options/audit.go:226-240).
        if self.group_version != AUDIT_GROUP_VERSION {
            errs.push(format!(
                "invalid group version, allowed versions are [\"{AUDIT_GROUP_VERSION}\"]"
            ));
        }
        errs
    }

    /// `newUntruncatedBackend` (options/audit.go:576-584) wrapped by
    /// `TruncateOptions.wrapBackend` (options/audit.go:330).
    pub fn new_backend(&self) -> anyhow::Result<WrappedBackend> {
        let path = self.config_file.as_deref().unwrap_or_default();
        let webhook = WebhookBackend::new(
            std::path::Path::new(path),
            &self.group_version,
            default_retry_backoff_with_initial_delay(self.initial_backoff),
        )
        .map_err(|e| anyhow::anyhow!("initializing audit webhook: {e}"))?;
        let (backend, buffered) = self.batch.wrap_backend(Arc::new(webhook));
        Ok((self.truncate.wrap_backend(backend), buffered))
    }
}
