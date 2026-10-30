//! Dynamic client-CA content: `--client-ca-file` and
//! `--requestheader-client-ca-file` reload on change (#3426).
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/dynamiccertificates`:
//!   * `interfaces.go` -- `Listener`, `Notifier`, `CAContentProvider`;
//!   * `dynamic_cafile_content.go` -- `DynamicFileCAContent`
//!     (`NewDynamicCAContentFromFile`, `loadCABundle`, `watchCAFile`,
//!     `handleWatchEvent`, the single-item rate-limited queue);
//!   * `union_content.go` -- `unionCAContent`;
//!   * `tlsconfig.go` -- `DynamicServingCertificateController` (`syncCerts`,
//!     `Run`, the one-minute re-enqueue).
//!
//! Go swaps a `*tls.Config` per handshake through `GetConfigForClient`; the
//! rustls counterpart is `RustlsConfig::reload_from_config`, which the
//! acceptor reads for every new connection.
//!
//! Where Go has `ctx.Done()` the tasks here are owned by a [`ReloadTasks`]
//! guard and aborted when it is dropped. fsnotify is replaced by `inotify`
//! (Linux), registered with the same event mask fsnotify uses for a file.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use futures::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use rusternetes_common::tls::{client_cert_verifier_for_ca_bundle, TlsConfig};
use rustls::server::danger::ClientCertVerifier;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// `FileRefreshDuration` (dynamic_cafile_content.go:40) is `1 * time.Minute`
/// and is also the `wait.Until` period of both the file watch and the serving
/// controller's re-enqueue (dynamic_cafile_content.go:158, tlsconfig.go:259).
pub const FILE_REFRESH_DURATION: Duration = Duration::from_secs(60);

/// `Listener` (interfaces.go:23-26).
pub trait Listener: Send + Sync {
    /// Called when an input may have changed.
    fn enqueue(&self);
}

/// `CAContentProvider` (interfaces.go:35-48) plus the `ControllerRunner`
/// (`RunOnce`/`Run`, dynamic_cafile_content.go:45-51) that a provider may
/// also implement; the union type-asserts for it, which a default method
/// expresses here.
pub trait CaContentProvider: Send + Sync {
    /// Just an identifier.
    fn name(&self) -> String;
    /// The current CA bundle bytes.
    fn current_ca_bundle_content(&self) -> Vec<u8>;
    /// A verifier over the current bundle (`VerifyOptions`), for
    /// authenticators; `None` until a bundle is loaded.
    fn verifier(&self) -> Option<Arc<dyn ClientCertVerifier>>;
    /// `AddListener`.
    fn add_listener(&self, listener: Arc<dyn Listener>);
    /// `RunOnce`: a single sync, useful for synchronous priming. A no-op for
    /// providers that are not controllers.
    fn run_once(&self) -> Result<()> {
        Ok(())
    }
    /// `Run`: starts the controller's tasks. A no-op for providers that are not
    /// controllers.
    fn run(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        Vec::new()
    }
}

/// The key of the one-item queue (`workItemKey`).
const WORK_ITEM_KEY: &str = "key";

/// A queue that only ever has one item, "but has nice error handling
/// backoff/retry semantics" (dynamic_cafile_content.go:63). `Add` coalesces
/// into one pending item; `AddRateLimited` re-adds after the per-item
/// exponential failure delay of `DefaultTypedControllerRateLimiter`
/// (5ms base, doubling, capped at 1000s: workqueue/default_rate_limiters.go;
/// its overall token bucket cannot bind a one-item queue).
pub struct SingleItemQueue {
    notify: Notify,
    failures: AtomicU32,
}

impl SingleItemQueue {
    const BASE_DELAY: Duration = Duration::from_millis(5);
    const MAX_DELAY: Duration = Duration::from_secs(1000);

    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            failures: AtomicU32::new(0),
        })
    }

    /// `queue.Add`.
    pub fn add(&self) {
        self.notify.notify_one();
    }

    /// `queue.Forget`.
    pub fn forget(&self) {
        self.failures.store(0, Ordering::Relaxed);
    }

    /// The delay `AddRateLimited` applies for this failure
    /// (`ItemExponentialFailureRateLimiter.When`).
    pub fn when(&self) -> Duration {
        let exp = self.failures.fetch_add(1, Ordering::Relaxed).min(40);
        Self::BASE_DELAY
            .saturating_mul(2u32.saturating_pow(exp.min(31)))
            .min(Self::MAX_DELAY)
    }

    /// `queue.AddRateLimited`.
    pub fn add_rate_limited(self: &Arc<Self>) {
        let delay = self.when();
        let q = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            q.add();
        });
    }

    /// `queue.Get`.
    async fn get(&self) -> &'static str {
        self.notify.notified().await;
        WORK_ITEM_KEY
    }
}

/// Aborts the tasks it owns when dropped (Go's `ctx.Done()`).
#[derive(Default)]
pub struct ReloadTasks(Vec<JoinHandle<()>>);

impl ReloadTasks {
    pub fn extend(&mut self, handles: Vec<JoinHandle<()>>) {
        self.0.extend(handles);
    }
}

impl Drop for ReloadTasks {
    fn drop(&mut self) {
        for h in &self.0 {
            h.abort();
        }
    }
}

/// `caBundleAndVerifier`.
struct CaBundleAndVerifier {
    ca_bundle: Vec<u8>,
    verifier: Arc<dyn ClientCertVerifier>,
}

/// `newCABundleAndVerifier` (dynamic_cafile_content.go:268-284).
fn new_ca_bundle_and_verifier(name: &str, ca_bundle: Vec<u8>) -> Result<CaBundleAndVerifier> {
    if ca_bundle.is_empty() {
        return Err(anyhow!("missing content for CA bundle {name:?}"));
    }
    let verifier = client_cert_verifier_for_ca_bundle(name, &ca_bundle)?;
    Ok(CaBundleAndVerifier {
        ca_bundle,
        verifier,
    })
}

/// `DynamicFileCAContent`: a [`CaContentProvider`] based on a file that
/// automatically reloads its content.
pub struct DynamicFileCaContent {
    name: String,
    /// The file to read.
    filename: String,
    /// The last read, non-zero length content of the file.
    ca_bundle: RwLock<Option<Arc<CaBundleAndVerifier>>>,
    listeners: Mutex<Vec<Arc<dyn Listener>>>,
    queue: Arc<SingleItemQueue>,
}

impl DynamicFileCaContent {
    /// `NewDynamicCAContentFromFile`: loads the file once, failing if it is
    /// missing, empty, or holds no certificate.
    pub fn new(purpose: &str, filename: &str) -> Result<Arc<Self>> {
        if filename.is_empty() {
            return Err(anyhow!("missing filename for ca bundle"));
        }
        let ret = Arc::new(Self {
            name: format!("{purpose}::{filename}"),
            filename: filename.to_string(),
            ca_bundle: RwLock::new(None),
            listeners: Mutex::new(Vec::new()),
            queue: SingleItemQueue::new(),
        });
        ret.load_ca_bundle()?;
        Ok(ret)
    }

    /// `loadCABundle`: determines the next set of content for the file.
    fn load_ca_bundle(&self) -> Result<()> {
        let ca_bundle =
            std::fs::read(&self.filename).with_context(|| format!("open {}", self.filename))?;
        if ca_bundle.is_empty() {
            return Err(anyhow!("missing content for CA bundle {:?}", self.name));
        }

        // check to see if we have a change. If the values are the same, do nothing.
        if !self.has_ca_changed(&ca_bundle) {
            return Ok(());
        }

        let loaded = new_ca_bundle_and_verifier(&self.name, ca_bundle)?;
        *self.ca_bundle.write().unwrap() = Some(Arc::new(loaded));
        info!(name = %self.name, "Loaded a new CA Bundle and Verifier");

        // Clone out so a listener may call back into this provider.
        let listeners = self.listeners.lock().unwrap().clone();
        for listener in listeners {
            listener.enqueue();
        }
        Ok(())
    }

    /// `hasCAChanged`.
    fn has_ca_changed(&self, ca_bundle: &[u8]) -> bool {
        match self.ca_bundle.read().unwrap().as_ref() {
            None => true,
            Some(existing) => existing.ca_bundle != ca_bundle,
        }
    }

    fn current(&self) -> Option<Arc<CaBundleAndVerifier>> {
        self.ca_bundle.read().unwrap().clone()
    }

    /// `watchCAFile`.
    async fn watch_ca_file(&self) -> Result<()> {
        // Trigger a check here to ensure the content will be checked
        // periodically even if the following watch fails.
        self.queue.add();

        let inotify =
            Inotify::init().map_err(|e| anyhow!("error creating fsnotify watcher: {e}"))?;
        let mut watches = inotify.watches();
        watches
            .add(&self.filename, Self::watch_mask())
            .map_err(|e| anyhow!("error adding watch for file {}: {e}", self.filename))?;
        // Trigger a check in case the file is updated before the watch starts.
        self.queue.add();

        let mut events = inotify.into_event_stream(vec![0u8; 4096])?;
        while let Some(event) = events.next().await {
            let event = event.map_err(|e| anyhow!("received fsnotify error: {e}"))?;
            self.handle_watch_event(event.mask, &mut watches)?;
        }
        Ok(())
    }

    /// The events fsnotify's inotify backend registers for a watched path
    /// (backend_inotify.go: `IN_MOVED_TO | IN_MOVED_FROM | IN_CREATE |
    /// IN_ATTRIB | IN_MODIFY | IN_MOVE_SELF | IN_DELETE | IN_DELETE_SELF`).
    fn watch_mask() -> WatchMask {
        WatchMask::MOVED_TO
            | WatchMask::MOVED_FROM
            | WatchMask::CREATE
            | WatchMask::ATTRIB
            | WatchMask::MODIFY
            | WatchMask::MOVE_SELF
            | WatchMask::DELETE
            | WatchMask::DELETE_SELF
    }

    /// `handleWatchEvent`: triggers reloading the CA file, and restarts a new
    /// watch if it's a Remove or Rename event.
    fn handle_watch_event(&self, mask: EventMask, watches: &mut inotify::Watches) -> Result<()> {
        // This should be executed after restarting the watch (if applicable)
        // to ensure no file event will be missing (`defer c.queue.Add`).
        let result = (|| {
            // fsnotify reports Remove for IN_DELETE_SELF and Rename for IN_MOVE_SELF.
            if !mask.intersects(EventMask::DELETE_SELF | EventMask::MOVE_SELF) {
                return Ok(());
            }
            // The kernel dropped (delete) or kept (move) the old inode's watch;
            // a fresh `add` on the path covers the replacement file, and adding
            // an already-watched inode just updates its mask.
            watches
                .add(&self.filename, Self::watch_mask())
                .map_err(|e| anyhow!("error adding watch for file {}: {e}", self.filename))?;
            Ok(())
        })();
        self.queue.add();
        result
    }

    async fn run_worker(self: Arc<Self>) {
        loop {
            self.process_next_work_item().await;
        }
    }

    /// `processNextWorkItem`.
    async fn process_next_work_item(&self) {
        let ds_key = self.queue.get().await;
        match self.load_ca_bundle() {
            Ok(()) => self.queue.forget(),
            Err(e) => {
                error!("{ds_key} failed with : {e:#}");
                self.queue.add_rate_limited();
            }
        }
    }
}

impl CaContentProvider for DynamicFileCaContent {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn current_ca_bundle_content(&self) -> Vec<u8> {
        self.current()
            .map(|b| b.ca_bundle.clone())
            .unwrap_or_default()
    }

    fn verifier(&self) -> Option<Arc<dyn ClientCertVerifier>> {
        self.current().map(|b| b.verifier.clone())
    }

    fn add_listener(&self, listener: Arc<dyn Listener>) {
        self.listeners.lock().unwrap().push(listener);
    }

    fn run_once(&self) -> Result<()> {
        self.load_ca_bundle()
    }

    /// `Run`: one worker, plus the loop that watches the CA file and restarts
    /// the watch every [`FILE_REFRESH_DURATION`] after it fails.
    fn run(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        info!(name = %self.name, "Starting controller");
        let worker = tokio::spawn(Arc::clone(&self).run_worker());
        let watcher = tokio::spawn(async move {
            loop {
                if let Err(e) = self.watch_ca_file().await {
                    error!("Failed to watch CA file, will retry later: {e:#}");
                }
                tokio::time::sleep(FILE_REFRESH_DURATION).await;
            }
        });
        vec![worker, watcher]
    }
}

/// `unionCAContent`.
pub struct UnionCaContent(Vec<Arc<dyn CaContentProvider>>);

impl UnionCaContent {
    /// `NewUnionCAContentProvider`.
    pub fn new(providers: Vec<Arc<dyn CaContentProvider>>) -> Arc<Self> {
        Arc::new(Self(providers))
    }
}

impl CaContentProvider for UnionCaContent {
    fn name(&self) -> String {
        self.0
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn current_ca_bundle_content(&self) -> Vec<u8> {
        let bundles: Vec<Vec<u8>> = self
            .0
            .iter()
            .map(|c| c.current_ca_bundle_content())
            .filter(|b| !b.is_empty())
            .map(|b| String::from_utf8_lossy(&b).trim().as_bytes().to_vec())
            .collect();
        bundles.join(&b'\n')
    }

    fn verifier(&self) -> Option<Arc<dyn ClientCertVerifier>> {
        let bundle = self.current_ca_bundle_content();
        if bundle.is_empty() {
            return None;
        }
        // Built from already vetted values: a failure is a coding error
        // (`panic(err)` upstream).
        Some(
            new_ca_bundle_and_verifier(&self.name(), bundle)
                .expect("union of vetted CA bundles must parse")
                .verifier,
        )
    }

    fn add_listener(&self, listener: Arc<dyn Listener>) {
        for c in &self.0 {
            c.add_listener(listener.clone());
        }
    }

    fn run_once(&self) -> Result<()> {
        let errors: Vec<String> = self
            .0
            .iter()
            .filter_map(|c| c.run_once().err())
            .map(|e| format!("{e:#}"))
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("[{}]", errors.join(", ")))
        }
    }

    fn run(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        self.0.iter().flat_map(|c| c.clone().run()).collect()
    }
}

/// `DynamicServingCertificateController`, reduced to what this server has:
/// a static serving certificate and a dynamic client CA (no SNI certs, no
/// event recorder -- `eventRecorder` is optional upstream).
pub struct DynamicServingCertificateController {
    /// The static portion of the config (`baseTLSConfig`): serving cert/key.
    base: TlsConfig,
    /// The very latest content of the client CA bundle.
    client_ca: Arc<dyn CaContentProvider>,
    /// What `GetConfigForClient` reads: the acceptor consults it per connection.
    rustls_config: RustlsConfig,
    /// The bundle bytes currently being served (`currentlyServedContent`).
    currently_served: Mutex<Option<Vec<u8>>>,
    queue: Arc<SingleItemQueue>,
}

impl Listener for DynamicServingCertificateController {
    /// `Enqueue`.
    fn enqueue(&self) {
        self.queue.add();
    }
}

impl DynamicServingCertificateController {
    /// `NewDynamicServingCertificateController` followed by the synchronous
    /// `RunOnce` that `Run` performs, so a valid config exists before serving.
    pub fn new(base: TlsConfig, client_ca: Arc<dyn CaContentProvider>) -> Result<Arc<Self>> {
        let initial =
            base.mtls_server_config_for_ca_bundle(&client_ca.current_ca_bundle_content())?;
        let c = Arc::new(Self {
            base,
            rustls_config: RustlsConfig::from_config(initial),
            currently_served: Mutex::new(Some(client_ca.current_ca_bundle_content())),
            client_ca,
            queue: SingleItemQueue::new(),
        });
        c.client_ca.add_listener(c.clone());
        Ok(c)
    }

    /// The live config handle to give the acceptor.
    pub fn rustls_config(&self) -> RustlsConfig {
        self.rustls_config.clone()
    }

    /// `syncCerts`: if the client CA content changed from what is being
    /// served, parse it and store the new config for new connections.
    pub fn sync_certs(&self) -> Result<()> {
        let new_content = self.client_ca.current_ca_bundle_content();
        let mut served = self.currently_served.lock().unwrap();
        if served.as_deref() == Some(new_content.as_slice()) {
            return Ok(());
        }
        let config = self
            .base
            .mtls_server_config_for_ca_bundle(&new_content)
            .map_err(|e| anyhow!("unable to load client CA file: {e:#}"))?;
        info!(name = %self.client_ca.name(), "Loaded client CA bundle");
        self.rustls_config.reload_from_config(config);
        *served = Some(new_content);
        Ok(())
    }

    /// `Run`: starts the worker, plus the timer that rechecks every minute
    /// "just in case" (tlsconfig.go:255-260).
    pub fn run(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        info!("Starting DynamicServingCertificateController");
        // synchronously load once. We will trigger again, so ignoring any error is fine
        let _ = self.sync_certs();

        let worker = {
            let c = Arc::clone(&self);
            tokio::spawn(async move {
                loop {
                    c.process_next_work_item().await;
                }
            })
        };
        let timer = tokio::spawn(async move {
            loop {
                self.enqueue();
                tokio::time::sleep(FILE_REFRESH_DURATION).await;
            }
        });
        vec![worker, timer]
    }

    async fn process_next_work_item(&self) {
        let ds_key = self.queue.get().await;
        match self.sync_certs() {
            Ok(()) => self.queue.forget(),
            Err(e) => {
                error!("{ds_key} failed with : {e:#}");
                self.queue.add_rate_limited();
            }
        }
    }
}
