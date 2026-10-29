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
//! Not ported (tracked on #2605): the truncate backend
//! (`--audit-webhook-truncate-*`), dynamic backends (`AuditSink`, removed
//! upstream in 1.19), the egress selector dial, tracing spans, and the
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

/// `audit.HandlePluginError` (pkg/audit/metrics.go): log the failure and the
/// events it cost.
fn handle_plugin_error(plugin: &str, err: &str, count: usize) {
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
    client: reqwest::Client,
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
        Ok(Self {
            client: config.build_client()?,
            config,
            retry_backoff,
        })
    }

    async fn post_once(&self, body: &[u8]) -> Result<(), String> {
        let resp = self
            .config
            .post(&self.client)
            .header("Content-Type", "application/json")
            .body(body.to_vec())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        // `transformResponse`: outside 200..=206 is an error.
        if !(200..=206).contains(&status.as_u16()) {
            return Err(format!("webhook returned HTTP {status}"));
        }
        Ok(())
    }

    /// `backend.processEvents` (webhook.go:121-141) inside
    /// `WithExponentialBackoff` (util/webhook/webhook.go:105-135) with
    /// `retryOnError`: every error is retried, up to `Steps` attempts.
    pub async fn try_process_events(&self, events: &[AuditEvent]) -> Result<(), String> {
        let body = serde_json::to_vec(&EventList {
            kind: "EventList",
            api_version: AUDIT_GROUP_VERSION,
            metadata: serde_json::json!({}),
            items: events,
        })
        .map_err(|e| e.to_string())?;
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
struct TokenBucket {
    qps: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(qps: f32, burst: usize) -> Self {
        Self {
            qps: qps as f64,
            burst: burst as f64,
            tokens: burst as f64,
            last: Instant::now(),
        }
    }

    async fn accept(&mut self) {
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
struct Worker {
    delegate: Arc<dyn BatchBackend>,
    rx: mpsc::Receiver<AuditEvent>,
    max_batch_size: usize,
    max_batch_wait: Duration,
    async_delegate: bool,
    throttle: Option<TokenBucket>,
    /// `wg`: the in-flight asynchronous deliveries.
    inflight: JoinSet<()>,
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
    async fn collect_events(
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
}

impl WebhookOptions {
    /// `AuditWebhookOptions.enabled`.
    pub fn enabled(&self) -> bool {
        self.config_file.as_deref().is_some_and(|f| !f.is_empty())
    }

    /// `AuditWebhookOptions.Validate` (options/audit.go:551-568); the
    /// truncate options are not ported.
    pub fn validate(&self) -> Vec<String> {
        if !self.enabled() {
            return Vec::new();
        }
        let mut errs = Vec::new();
        if let Err(e) = self.batch.validate(WEBHOOK_PLUGIN_NAME) {
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

    /// `newUntruncatedBackend` (options/audit.go:576-584).
    pub fn new_backend(&self) -> anyhow::Result<WrappedBackend> {
        let path = self.config_file.as_deref().unwrap_or_default();
        let webhook = WebhookBackend::new(
            std::path::Path::new(path),
            &self.group_version,
            default_retry_backoff_with_initial_delay(self.initial_backoff),
        )
        .map_err(|e| anyhow::anyhow!("initializing audit webhook: {e}"))?;
        Ok(self.batch.wrap_backend(Arc::new(webhook)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::audit::{AuditLevel, AuditStage, UserInfo};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn event() -> AuditEvent {
        AuditEvent {
            api_version: "audit.k8s.io/v1".into(),
            kind: "Event".into(),
            level: AuditLevel::Metadata,
            audit_id: uuid::Uuid::new_v4().to_string(),
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

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("audit-webhook-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn test_config() -> BatchConfig {
        // testBatchConfig (buffered_test.go:46-54)
        BatchConfig {
            buffer_size: 100,
            max_batch_size: 10,
            max_batch_wait: Duration::from_secs(30),
            throttle_enable: false,
            throttle_qps: 0.0,
            throttle_burst: 0,
            async_delegate: true,
        }
    }

    #[derive(Default)]
    struct Recorder {
        batches: Mutex<Vec<usize>>,
        calls: AtomicUsize,
        gate: Option<Arc<tokio::sync::Notify>>,
    }

    #[async_trait]
    impl BatchBackend for Recorder {
        async fn process_events(&self, events: Vec<AuditEvent>) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(g) = &self.gate {
                g.notified().await;
            }
            self.batches.lock().unwrap().push(events.len());
            true
        }
        fn name(&self) -> String {
            "fake".into()
        }
    }

    /// A worker over a fresh buffer, to drive `collect_events` directly
    /// (the tests of buffered_test.go call it on the bufferedBackend).
    fn worker(cfg: &BatchConfig) -> (mpsc::Sender<AuditEvent>, Worker) {
        let (tx, rx) = mpsc::channel(cfg.buffer_size);
        (
            tx,
            Worker {
                delegate: Arc::new(Recorder::default()),
                rx,
                max_batch_size: cfg.max_batch_size,
                max_batch_wait: cfg.max_batch_wait,
                async_delegate: cfg.async_delegate,
                throttle: None,
                inflight: JoinSet::new(),
            },
        )
    }

    /// TestBatchedBackendCollectEvents (buffered_test.go:56-102).
    #[tokio::test(start_paused = true)]
    async fn batched_collect_events() {
        let cfg = test_config();
        let (tx, mut w) = worker(&cfg);
        let (stop_tx, mut stop) = watch::channel(false);

        // Max batch size encountered.
        for _ in 0..cfg.max_batch_size + 1 {
            tx.try_send(event()).unwrap();
        }
        assert_eq!(w.collect_events(true, &mut stop).await.len(), 10);

        // A partial batch waits until the timer expires.
        tx.try_send(event()).unwrap();
        assert_eq!(w.collect_events(true, &mut stop).await.len(), 2);

        // Collected events are delivered when the stop signal is given.
        for _ in 0..3 {
            tx.try_send(event()).unwrap();
        }
        let collect = w.collect_events(true, &mut stop);
        tokio::pin!(collect);
        tokio::select! {
            _ = &mut collect => panic!("returned before the timer or stop"),
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        stop_tx.send(true).unwrap();
        assert_eq!(collect.await.len(), 3);
    }

    /// TestUnbatchedBackendCollectEvents (buffered_test.go:104-131).
    #[tokio::test(start_paused = true)]
    async fn unbatched_collect_events() {
        let mut cfg = test_config();
        cfg.max_batch_size = 1;
        let (tx, mut w) = worker(&cfg);
        let (stop_tx, mut stop) = watch::channel(false);
        for _ in 0..3 {
            tx.try_send(event()).unwrap();
        }
        for _ in 0..3 {
            assert_eq!(w.collect_events(false, &mut stop).await.len(), 1);
        }
        // Collection halts when the stop signal is given.
        stop_tx.send(true).unwrap();
        assert!(w.collect_events(false, &mut stop).await.is_empty());
    }

    /// TestBufferedBackendProcessEventsAfterStop (buffered_test.go:133-146).
    #[tokio::test]
    async fn process_events_after_stop() {
        let b = BufferedBackend::new(Arc::new(Recorder::default()), test_config());
        b.shutdown().await;
        // Still answers true, drops the event.
        assert!(b.log(event()).await.is_ok());
    }

    /// TestBufferedBackendProcessEventsBufferFull (buffered_test.go:148-158).
    #[tokio::test]
    async fn process_events_buffer_full() {
        let (tx, w) = worker(&{
            let mut c = test_config();
            c.buffer_size = 1;
            c
        });
        tx.try_send(event()).unwrap();
        assert!(tx.try_send(event()).is_err());
        drop(w);
    }

    /// A full buffer drops the event and `log` still answers Ok
    /// (`ProcessEvents` returns true).
    #[tokio::test]
    async fn log_with_full_buffer_is_ok() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let rec = Arc::new(Recorder {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let mut cfg = test_config();
        cfg.buffer_size = 1;
        cfg.max_batch_size = 1;
        cfg.async_delegate = false;
        let b = BufferedBackend::new(rec.clone(), cfg);
        for _ in 0..5 {
            assert!(b.log(event()).await.is_ok());
        }
        gate.notify_waiters();
        gate.notify_one();
        b.shutdown().await;
    }

    /// TestBufferedBackendShutdownWaitsForDelegatedCalls
    /// (buffered_test.go:160-200).
    #[tokio::test]
    async fn shutdown_waits_for_delegated_calls() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let rec = Arc::new(Recorder {
            gate: Some(gate.clone()),
            ..Default::default()
        });
        let cfg = test_config();
        let b = Arc::new(BufferedBackend::new(rec.clone(), cfg.clone()));
        for _ in 0..cfg.max_batch_size {
            b.log(event()).await.unwrap();
        }
        // Wait for the delegated call to start.
        while rec.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let b2 = b.clone();
        let shutdown = tokio::spawn(async move { b2.shutdown().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !shutdown.is_finished(),
            "Shutdown exited before delegated call ended"
        );
        gate.notify_one();
        shutdown.await.unwrap();
        assert_eq!(*rec.batches.lock().unwrap(), vec![10]);
    }

    /// TestDelegateProcessEvents (buffered_test.go:202-222): full batches
    /// reach the delegate, async or not.
    #[tokio::test]
    async fn delegate_process_events() {
        for async_delegate in [true, false] {
            let rec = Arc::new(Recorder::default());
            let mut cfg = test_config();
            cfg.async_delegate = async_delegate;
            let b = BufferedBackend::new(rec.clone(), cfg.clone());
            for _ in 0..5 * cfg.max_batch_size {
                b.log(event()).await.unwrap();
            }
            b.shutdown().await;
            let batches = rec.batches.lock().unwrap().clone();
            assert_eq!(batches.iter().sum::<usize>(), 50);
            assert!(batches.iter().all(|n| *n <= cfg.max_batch_size));
        }
    }

    /// Events left in the buffer at shutdown are still sent
    /// (buffered.go:98-117).
    #[tokio::test]
    async fn shutdown_flushes_partial_batch() {
        let rec = Arc::new(Recorder::default());
        let b = BufferedBackend::new(rec.clone(), test_config());
        for _ in 0..3 {
            b.log(event()).await.unwrap();
        }
        b.shutdown().await;
        assert_eq!(rec.batches.lock().unwrap().iter().sum::<usize>(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn token_bucket_throttles() {
        let mut t = TokenBucket::new(10.0, 2);
        let start = Instant::now();
        for _ in 0..4 {
            t.accept().await;
        }
        // 2 from the burst, then 2 at 10 per second.
        let waited = start.elapsed();
        assert!(waited >= Duration::from_millis(190), "{waited:?}");
        assert!(waited < Duration::from_millis(400), "{waited:?}");
    }

    #[test]
    fn batch_options_validate() {
        let mut o = BatchOptions {
            mode: "batch".into(),
            batch_config: BatchConfig::default_webhook(),
        };
        assert!(o.validate("webhook").is_ok());
        o.mode = "nope".into();
        assert_eq!(
            o.validate("webhook").unwrap_err(),
            "invalid audit webhook mode nope, allowed modes are \"batch,blocking,blocking-strict\""
        );
        o.mode = "batch".into();
        o.batch_config.buffer_size = 0;
        assert!(o.validate("webhook").unwrap_err().contains("buffer size 0"));
        o.batch_config = BatchConfig::default_webhook();
        o.batch_config.max_batch_size = 0;
        assert!(o
            .validate("webhook")
            .unwrap_err()
            .contains("max batch size 0"));
        o.batch_config = BatchConfig::default_webhook();
        o.batch_config.throttle_qps = 0.0;
        assert!(o.validate("webhook").unwrap_err().contains("throttle QPS"));
        o.batch_config.throttle_qps = 1.0;
        o.batch_config.throttle_burst = 0;
        assert!(o
            .validate("webhook")
            .unwrap_err()
            .contains("throttle burst"));
        // Unused options are not validated outside batch mode.
        o.mode = "blocking".into();
        assert!(o.validate("webhook").is_ok());
    }

    // ---- the webhook itself (webhook_test.go TestWebhook) ----

    async fn serve(
        status: u16,
        fail_first: usize,
    ) -> (String, Arc<Mutex<Vec<serde_json::Value>>>, Arc<AtomicUsize>) {
        use axum::{extract::State, routing::post, Json, Router};
        #[derive(Clone)]
        struct S {
            got: Arc<Mutex<Vec<serde_json::Value>>>,
            n: Arc<AtomicUsize>,
            status: u16,
            fail_first: usize,
        }
        async fn h(State(s): State<S>, Json(v): Json<serde_json::Value>) -> axum::http::StatusCode {
            let n = s.n.fetch_add(1, Ordering::SeqCst);
            if n < s.fail_first {
                return axum::http::StatusCode::INTERNAL_SERVER_ERROR;
            }
            s.got.lock().unwrap().push(v);
            axum::http::StatusCode::from_u16(s.status).unwrap()
        }
        let s = S {
            got: Default::default(),
            n: Default::default(),
            status,
            fail_first,
        };
        let (got, n) = (s.got.clone(), s.n.clone());
        let app = Router::new().route("/", post(h)).with_state(s);
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        (format!("http://{addr}/"), got, n)
    }

    /// `newWebhook` (webhook_test.go:93-119): a kubeconfig with one cluster.
    fn kubeconfig(dir: &std::path::Path, server: &str) -> std::path::PathBuf {
        let p = dir.join("kubeconfig");
        std::fs::write(
            &p,
            format!(
                "apiVersion: v1\nkind: Config\nclusters:\n- name: \"\"\n  cluster:\n    server: {server}\n    insecure-skip-tls-verify: true\n"
            ),
        )
        .unwrap();
        p
    }

    fn fast_backoff() -> Backoff {
        Backoff {
            duration: Duration::from_millis(10),
            factor: 1.5,
            jitter: 0.2,
            steps: 5,
        }
    }

    #[tokio::test]
    async fn webhook_sends_event_list() {
        let (url, got, _) = serve(200, 0).await;
        let dir = tempdir();
        let b = WebhookBackend::new(&kubeconfig(&dir, &url), AUDIT_GROUP_VERSION, fast_backoff())
            .unwrap();
        b.try_process_events(&[event(), event()]).await.unwrap();
        let got = got.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["kind"], "EventList");
        assert_eq!(got[0]["apiVersion"], "audit.k8s.io/v1");
        assert_eq!(got[0]["items"].as_array().unwrap().len(), 2);
        assert_eq!(got[0]["items"][0]["kind"], "Event");
    }

    /// `retryOnError`: every failure is retried until it works.
    #[tokio::test]
    async fn webhook_retries_every_error() {
        let (url, got, n) = serve(200, 3).await;
        let dir = tempdir();
        let b = WebhookBackend::new(&kubeconfig(&dir, &url), AUDIT_GROUP_VERSION, fast_backoff())
            .unwrap();
        assert!(b.process_events(vec![event()]).await);
        assert_eq!(n.load(Ordering::SeqCst), 4);
        assert_eq!(got.lock().unwrap().len(), 1);
    }

    /// A 4xx is retried too (`retryOnError` is true for any error) and the
    /// batch is lost after `Steps` attempts.
    #[tokio::test]
    async fn webhook_gives_up_after_steps() {
        let (url, _, n) = serve(403, 0).await;
        let dir = tempdir();
        let b = WebhookBackend::new(&kubeconfig(&dir, &url), AUDIT_GROUP_VERSION, fast_backoff())
            .unwrap();
        assert!(!b.process_events(vec![event()]).await);
        assert_eq!(n.load(Ordering::SeqCst), 5);
        assert!(b.log(event()).await.is_err());
    }

    #[test]
    fn webhook_rejects_unregistered_version() {
        let dir = tempdir();
        let kc = kubeconfig(&dir, "http://127.0.0.1:1/");
        let e = WebhookBackend::new(&kc, "audit.k8s.io/v1beta1", fast_backoff())
            .err()
            .unwrap();
        assert!(e
            .to_string()
            .contains("requires enabling extension resource"));
    }

    /// Batch mode end to end: events buffered, delivered on shutdown.
    #[tokio::test]
    async fn options_build_batch_backend() {
        let (url, got, _) = serve(200, 0).await;
        let dir = tempdir();
        let kc = kubeconfig(&dir, &url);
        let mut cfg = BatchConfig::default_webhook();
        cfg.throttle_enable = false;
        let o = WebhookOptions {
            config_file: Some(kc.to_string_lossy().into()),
            initial_backoff: Duration::from_millis(10),
            group_version: AUDIT_GROUP_VERSION.into(),
            batch: BatchOptions {
                mode: MODE_BATCH.into(),
                batch_config: cfg,
            },
        };
        assert!(o.validate().is_empty());
        let (b, buffered) = o.new_backend().unwrap();
        for _ in 0..3 {
            b.log(event()).await.unwrap();
        }
        // The 30s max wait has not passed: nothing is sent yet.
        assert!(got.lock().unwrap().is_empty());
        buffered.unwrap().shutdown().await;
        let got = got.lock().unwrap();
        // Shutdown races the worker's collect loop (`select` over the buffer
        // and the stop channel, buffered.go `collectEvents`), so the three
        // events may leave as one batch or as a partial batch followed by the
        // drain; either way all three arrive.
        let delivered: usize = got
            .iter()
            .map(|b| b["items"].as_array().unwrap().len())
            .sum();
        assert_eq!(delivered, 3);
    }

    #[test]
    fn options_validate_group_version_and_disabled() {
        let mut o = WebhookOptions {
            config_file: None,
            initial_backoff: DEFAULT_INITIAL_BACKOFF_DELAY,
            group_version: "bogus".into(),
            batch: BatchOptions {
                mode: MODE_BATCH.into(),
                batch_config: BatchConfig::default_webhook(),
            },
        };
        // Not enabled: nothing is validated.
        assert!(o.validate().is_empty());
        o.config_file = Some("/x".into());
        assert_eq!(
            o.validate(),
            vec!["invalid group version, allowed versions are [\"audit.k8s.io/v1\"]".to_string()]
        );
    }

    #[tokio::test]
    async fn blocking_modes() {
        let (url, _, _) = serve(500, 0).await;
        let dir = tempdir();
        let kc = kubeconfig(&dir, &url);
        let mk = |mode: &str| {
            let o = WebhookOptions {
                config_file: Some(kc.to_string_lossy().into()),
                initial_backoff: Duration::from_millis(1),
                group_version: AUDIT_GROUP_VERSION.into(),
                batch: BatchOptions {
                    mode: mode.into(),
                    batch_config: BatchConfig::default_webhook(),
                },
            };
            o.new_backend().unwrap().0
        };
        // blocking: a failing webhook never fails the request.
        assert!(mk(MODE_BLOCKING).log(event()).await.is_ok());
        // blocking-strict: it does.
        assert!(mk(MODE_BLOCKING_STRICT).log(event()).await.is_err());
    }

    #[tokio::test]
    async fn union_reports_any_failure_but_feeds_all() {
        struct Fail;
        #[async_trait]
        impl AuditBackend for Fail {
            async fn log(&self, _: AuditEvent) -> Result<(), String> {
                Err("boom".into())
            }
            async fn flush(&self) -> Result<(), String> {
                Ok(())
            }
        }
        struct Count(AtomicUsize);
        #[async_trait]
        impl AuditBackend for Count {
            async fn log(&self, _: AuditEvent) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn flush(&self) -> Result<(), String> {
                Ok(())
            }
        }
        let c = Arc::new(Count(AtomicUsize::new(0)));
        let u = UnionBackend(vec![Arc::new(Fail), c.clone()]);
        assert!(u.log(event()).await.is_err());
        assert_eq!(c.0.load(Ordering::SeqCst), 1);
    }
}
