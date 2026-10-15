//! `--authorization-config` hot reload (#3315).
//!
//! Ported from `pkg/kubeapiserver/authorizer/reload.go`
//! (`reloadableAuthorizerResolver`, `runReload`, `checkFile`) and
//! `pkg/util/filesystem/watcher.go` (`WatchUntil`). Upstream has no
//! `reload_test.go`; the behaviour is pinned by
//! `test/integration/auth/authz_config_test.go`.
//!
//! Go's `ctx` (server lifetime) has no counterpart here: the watch task holds
//! a `Weak` reference and ends when the authorizer is dropped.

use crate::authorization_config::{self, AuthorizationConfiguration};
use crate::authorization_config_metrics as metrics;
use async_trait::async_trait;
use futures::StreamExt;
use inotify::{EventMask, Inotify, WatchMask};
use rusternetes_common::auth::UserInfo;
use rusternetes_common::authz::{Authorizer, Decision, Opinion, RequestAttributes};
use rusternetes_common::resources::{NonResourceRule, ResourceRule};
use rusternetes_common::Result;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;
use tracing::{error, info};

/// `reloadInterval: time.Minute` (config.go:91).
pub const RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// Builds the authorizer chain for a configuration (`newForConfig`,
/// reload.go:97-176). Implemented by the authorizer factory, which owns the
/// authorizers that persist across reloads (Node, RBAC, ABAC).
pub trait ChainBuilder: Send + Sync {
    fn new_for_config(
        &self,
        config: &AuthorizationConfiguration,
    ) -> anyhow::Result<Arc<dyn Authorizer>>;
}

/// `reloadableAuthorizerResolver`: `current` is the atomically swapped
/// `authorizerResolver`.
pub struct ReloadableAuthorizer {
    /// `initialConfig.ReloadFile`.
    reload_file: PathBuf,
    api_server_id: String,
    reload_interval: Duration,
    /// `requireNonWebhookTypes`.
    require_non_webhook_types: BTreeSet<String>,
    builder: Arc<dyn ChainBuilder>,
    /// `lastLoadedLock` guarding `lastLoadedConfig` / `lastReadData`.
    last: Mutex<LastLoaded>,
    current: RwLock<Arc<dyn Authorizer>>,
}

struct LastLoaded {
    config: AuthorizationConfiguration,
    data: Vec<u8>,
}

impl ReloadableAuthorizer {
    /// The tail of `Config.New` (config.go:86-165): `initial` is the chain
    /// built for `initial_config`, which was read from `reload_file` as
    /// `initial_data`.
    pub fn new(
        reload_file: &str,
        api_server_id: String,
        initial_config: AuthorizationConfiguration,
        initial_data: Vec<u8>,
        initial: Arc<dyn Authorizer>,
        builder: Arc<dyn ChainBuilder>,
    ) -> Arc<Self> {
        // config.go:142-144: all non-Webhook types must remain on reload.
        let require_non_webhook_types = initial_config
            .authorizers
            .iter()
            .map(|a| a.type_.clone())
            .filter(|t| t != crate::authorizer::MODE_WEBHOOK)
            .collect();
        Arc::new(Self {
            reload_file: PathBuf::from(reload_file),
            api_server_id,
            reload_interval: RELOAD_INTERVAL,
            require_non_webhook_types,
            builder,
            last: Mutex::new(LastLoaded {
                config: initial_config,
                data: initial_data,
            }),
            current: RwLock::new(initial),
        })
    }

    /// `go r.runReload(ctx)` (config.go:157-159).
    pub fn spawn_reload(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let (file, interval) = (self.reload_file.clone(), self.reload_interval);
        let initial = self.last.lock().unwrap().data.clone();
        metrics::register_metrics();
        metrics::record_authorization_config_last_config_info(
            &self.api_server_id,
            &String::from_utf8_lossy(&initial),
        );
        tokio::spawn(async move {
            watch_until(
                file,
                interval,
                {
                    let weak = weak.clone();
                    move || weak.upgrade().is_some()
                },
                move || {
                    if let Some(r) = Weak::upgrade(&weak) {
                        r.check_file();
                    }
                },
                |e| error!("watching authorization config file: {e}"),
            )
            .await;
        });
    }

    fn current(&self) -> Arc<dyn Authorizer> {
        self.current.read().unwrap().clone()
    }

    /// `checkFile` (reload.go:226-274).
    pub fn check_file(&self) {
        let mut last = self.last.lock().unwrap();

        let data = match std::fs::read(&self.reload_file) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "reloading authorization config: open {}: {}",
                    self.reload_file.display(),
                    authorization_config::go_io_error(&e)
                );
                metrics::record_authorization_config_automatic_reload_failure(&self.api_server_id);
                return;
            }
        };
        if data == last.data {
            // no change
            return;
        }
        info!("found new authorization config data");
        last.data = data.clone();

        let config = match authorization_config::load_and_validate_data(
            &data,
            &self.require_non_webhook_types,
        ) {
            Ok(c) => c,
            Err(e) => {
                error!("reloading authorization config: {e}");
                metrics::record_authorization_config_automatic_reload_failure(&self.api_server_id);
                return;
            }
        };
        if config == last.config {
            // no change
            return;
        }
        info!("found new authorization config");
        last.config = config.clone();

        let authorizer = match self.builder.new_for_config(&config) {
            Ok(a) => a,
            Err(e) => {
                error!("reloading authorization config: {e}");
                metrics::record_authorization_config_automatic_reload_failure(&self.api_server_id);
                return;
            }
        };
        info!("constructed new authorizer");

        *self.current.write().unwrap() = authorizer;
        info!("reloaded authz config");
        metrics::record_authorization_config_automatic_reload_success(
            &self.api_server_id,
            &String::from_utf8_lossy(&data),
        );
    }
}

#[async_trait]
impl Authorizer for ReloadableAuthorizer {
    async fn authorize(&self, attrs: &RequestAttributes) -> Result<Decision> {
        self.authorize_outcome(attrs).await?.into_decision()
    }

    async fn authorize_opinion(&self, attrs: &RequestAttributes) -> Result<Opinion> {
        Ok(self.authorize_outcome(attrs).await?.into_opinion())
    }

    // pkg/kubeapiserver/authorizer/reload.go:78-79 forwards the full tuple.
    async fn authorize_outcome(
        &self,
        attrs: &RequestAttributes,
    ) -> Result<rusternetes_common::authz::AuthorizationOutcome> {
        self.current().authorize_outcome(attrs).await
    }

    async fn get_user_rules(
        &self,
        user: &UserInfo,
        namespace: &str,
    ) -> Result<(Vec<ResourceRule>, Vec<NonResourceRule>)> {
        self.current().get_user_rules(user, namespace).await
    }
}

/// `filesystem.WatchUntil` (pkg/util/filesystem/watcher.go:94-): watch `path`
/// and call `handler` on every change event and every `poll_interval`, and
/// once right after the watch is established. A watch that cannot be created
/// or re-established is reported to `on_error` and retried at the next poll.
/// Returns when `alive` reports false (Go: `ctx.Done()`).
pub async fn watch_until(
    path: PathBuf,
    poll_interval: Duration,
    alive: impl Fn() -> bool,
    mut handler: impl FnMut(),
    mut on_error: impl FnMut(String),
) {
    assert!(!poll_interval.is_zero(), "pollInterval must be > 0");

    // Initialize watcher, fall back to a no-op (poll only).
    let mut watches = None;
    let mut events = None;
    match Inotify::init() {
        Ok(i) => {
            watches = Some(i.watches());
            match i.into_event_stream(vec![0u8; 4096]) {
                Ok(s) => events = Some(s),
                Err(e) => on_error(format!(
                    "error creating file watcher, falling back to poll at interval {poll_interval:?}: {e}"
                )),
            }
        }
        Err(e) => on_error(format!(
            "error creating file watcher, falling back to poll at interval {poll_interval:?}: {e}"
        )),
    }
    if events.is_none() {
        watches = None;
    }

    // fsnotify's default op set for a file: Create|Write|Remove|Rename|Chmod.
    let mask = WatchMask::MODIFY
        | WatchMask::ATTRIB
        | WatchMask::CREATE
        | WatchMask::DELETE
        | WatchMask::DELETE_SELF
        | WatchMask::MOVE_SELF
        | WatchMask::MOVED_FROM
        | WatchMask::MOVED_TO;
    let mut wd = None;
    let add = |watches: &mut Option<inotify::Watches>, wd: &mut Option<_>| match watches {
        // noopWatcher.Add
        None => Ok(()),
        Some(w) => w.add(&path, mask).map(|d| *wd = Some(d)),
    };

    let mut ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + poll_interval, poll_interval);
    let mut attempt_periodic_rewatch = false;

    // Start watching the path.
    match add(&mut watches, &mut wd) {
        Err(e) => {
            on_error(e.to_string());
            attempt_periodic_rewatch = true;
        }
        // Invoke handle() at least once after successfully registering the
        // listener, in case the file changed concurrent with WatchUntil.
        Ok(()) => handler(),
    }

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if !alive() {
                    return;
                }
                // Try to re-establish the watcher if we previously got a watch error.
                if attempt_periodic_rewatch {
                    if let (Some(w), Some(old)) = (watches.as_mut(), wd.take()) {
                        let _ = w.remove(old);
                    }
                    match add(&mut watches, &mut wd) {
                        Err(e) => on_error(e.to_string()),
                        Ok(()) => attempt_periodic_rewatch = false,
                    }
                }
                handler();
            }
            ev = async {
                match events.as_mut() {
                    Some(s) => s.next().await,
                    None => std::future::pending().await,
                }
            } => {
                if !alive() {
                    return;
                }
                match ev {
                    Some(Ok(e)) => {
                        // Re-establish the watcher for events which dropped the
                        // existing watch (Remove / Rename of the path itself).
                        if e.mask.intersects(
                            EventMask::DELETE_SELF | EventMask::MOVE_SELF | EventMask::IGNORED,
                        ) {
                            if let (Some(w), Some(old)) = (watches.as_mut(), wd.take()) {
                                let _ = w.remove(old);
                            }
                            if let Err(err) = add(&mut watches, &mut wd) {
                                on_error(err.to_string());
                                attempt_periodic_rewatch = true;
                            }
                        }
                        handler();
                    }
                    // If the error occurs in response to calling watcher.Add,
                    // re-adding here could hot-loop. The periodic poll will
                    // attempt to re-establish the watch.
                    Some(Err(err)) => {
                        on_error(err.to_string());
                        attempt_periodic_rewatch = true;
                    }
                    None => {
                        events = None;
                        attempt_periodic_rewatch = true;
                    }
                }
            }
        }
    }
}
