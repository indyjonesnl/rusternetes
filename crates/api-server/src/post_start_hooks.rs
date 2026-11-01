//! Post-start hook registry: the per-hook `poststarthook/<name>` health check
//! and the "a failed hook is fatal" rule.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/hooks.go`:
//! - `AddPostStartHook` (:83) registers a `postStartHookHealthz` check named
//!   `"poststarthook/" + name` (:109);
//! - `postStartHookHealthz.Check` (:239-246) fails with "not finished" until
//!   the hook's `done` channel is closed;
//! - `runPostStartHook` (:195-208): a hook that returns an error is
//!   `klog.Fatalf("PostStartHook %q failed: %v", ...)` (:204) -- the server
//!   is killed and `done` is never closed. There is no "stay up but not
//!   ready" mode: the check only reports failure while the hook is running.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// `klog.Fatalf` exits with status 255 (k8s.io/klog/v2 `fatalf` -> `exit(255)`).
pub const FATAL_EXIT_CODE: i32 = 255;

/// Registered hooks and whether each has finished (`done` closed upstream).
#[derive(Default)]
pub struct PostStartHooks {
    hooks: Mutex<BTreeMap<String, Arc<AtomicBool>>>,
    boot_checks: Mutex<BTreeMap<String, BootCheck>>,
}

/// A `healthz.NamedCheck` added by `AddBootSequenceHealthChecks`.
type BootCheck = Box<dyn Fn() -> Result<(), String> + Send + Sync>;

fn state(done: &AtomicBool) -> Result<(), &'static str> {
    if done.load(Ordering::SeqCst) {
        Ok(())
    } else {
        Err("not finished")
    }
}

impl PostStartHooks {
    pub fn new() -> Self {
        Self::default()
    }

    /// `AddPostStartHook`: register the hook (unfinished); returns its `done` flag.
    pub fn register(&self, name: &str) -> Arc<AtomicBool> {
        let done = Arc::new(AtomicBool::new(false));
        self.hooks
            .lock()
            .unwrap()
            .insert(name.to_string(), done.clone());
        done
    }

    /// `AddBootSequenceHealthChecks`: a named check (no `poststarthook/` prefix).
    pub fn register_boot_check(
        &self,
        name: &str,
        check: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) {
        self.boot_checks
            .lock()
            .unwrap()
            .insert(name.to_string(), Box::new(check));
    }

    /// All boot-sequence checks as `(name, result)`.
    pub fn boot_checks(&self) -> Vec<(String, Result<(), String>)> {
        self.boot_checks
            .lock()
            .unwrap()
            .iter()
            .map(|(n, c)| (n.clone(), c()))
            .collect()
    }

    /// `postStartHookHealthz.Check` for one hook; `None` if not registered.
    pub fn check(&self, name: &str) -> Option<Result<(), &'static str>> {
        self.hooks.lock().unwrap().get(name).map(|d| state(d))
    }

    /// All checks as `("poststarthook/<name>", result)`.
    pub fn checks(&self) -> Vec<(String, Result<(), &'static str>)> {
        self.hooks
            .lock()
            .unwrap()
            .iter()
            .map(|(n, d)| (format!("poststarthook/{n}"), state(d)))
            .collect()
    }
}

/// The process-wide registry consulted by `/readyz` and `/healthz/poststarthook/*`.
pub fn global() -> &'static PostStartHooks {
    static G: OnceLock<PostStartHooks> = OnceLock::new();
    G.get_or_init(PostStartHooks::new)
}

/// `runPostStartHook` against `registry`: run `hook`; on `Ok` mark it done, on
/// `Err` call `on_fatal(name, error)` (upstream `klog.Fatalf`) and leave it
/// unfinished.
pub fn spawn_hook_with<F, E>(
    registry: &PostStartHooks,
    name: &str,
    hook: F,
    on_fatal: impl FnOnce(&str, &E) + Send + 'static,
) -> tokio::task::JoinHandle<()>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Send + 'static,
{
    let done = registry.register(name);
    let name = name.to_string();
    tokio::spawn(async move {
        match hook.await {
            Ok(()) => done.store(true, Ordering::SeqCst),
            Err(e) => on_fatal(&name, &e),
        }
    })
}

/// Production runner: registers on the global registry; a failed hook kills
/// the process (`klog.Fatalf`, hooks.go:204).
pub fn spawn_hook<F, E>(name: &str, hook: F) -> tokio::task::JoinHandle<()>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    spawn_hook_with(global(), name, hook, |name, e| {
        tracing::error!("PostStartHook {name:?} failed: {e}");
        std::process::exit(FATAL_EXIT_CODE);
    })
}

/// A hook that only starts background work and returns nil -- the shape of
/// `bootstrap-controller` (instance.go:360-363), `apiservice-status-*`
/// (apiserver.go:339-343, :361-366), `start-apiextensions-controllers`
/// (apiserver.go:228-261) and `start-cluster-authentication-info-controller`
/// (server.go:249-271). `start` runs, then the hook is marked done; it can
/// never be fatal (it returns `nil`).
pub fn spawn_starting_hook_with(
    registry: &PostStartHooks,
    name: &str,
    start: impl FnOnce() + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    spawn_hook_with(
        registry,
        name,
        async move {
            start();
            Ok::<(), std::convert::Infallible>(())
        },
        |_, e| match *e {},
    )
}

/// [`spawn_starting_hook_with`] on the global registry.
pub fn spawn_starting_hook(
    name: &str,
    start: impl FnOnce() + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    spawn_starting_hook_with(global(), name, start)
}

/// `genericApiServerHookName` (staging/src/k8s.io/apiserver/pkg/server/config.go:900).
pub const GENERIC_APISERVER_START_INFORMERS_HOOK: &str = "generic-apiserver-start-informers";
/// `NewInformerSyncHealthz(...).Name()` (server/healthz/healthz.go `informerSync`),
/// added with `AddReadyzChecks` (config.go:912-917).
pub const INFORMER_SYNC_CHECK: &str = "informer-sync";
/// server/options/admission.go:169 `AddPostStartHook`.
pub const ADMISSION_INITIALIZER_HOOK: &str = "start-apiserver-admission-initializer";
/// `maxInFlightFilterHookName` (server/config.go:949-958).
pub const MAX_IN_FLIGHT_FILTER_HOOK: &str = "max-in-flight-filter";

/// The hooks `GenericAPIServer.New` / `AdmissionOptions.ApplyTo` register that
/// are not owned by any one subsystem (#3664). Each only starts background
/// work and returns nil, so each finishes at once and is never fatal.
///
/// Lives here (not in `startup`) because `main.rs` and `lib.rs` each compile
/// their own `post_start_hooks`/`global()`, and the registry that `/readyz`
/// reads is the one in the same crate as the router.
///
/// - `generic-apiserver-start-informers` (config.go:899-917):
///   `c.SharedInformerFactory.Start(stop)` plus the `informer-sync` readyz
///   check (`InformerSyncHealthz`: every started informer `HasSynced`). This
///   server has no shared informer factory -- handlers read storage directly --
///   so there is nothing to start and nothing that can be unsynced; the hook
///   and the check are registered so `/readyz?verbose` and
///   `/healthz/poststarthook/<name>` expose the upstream surface. Deliberate
///   deviation: the check is vacuously ok.
/// - `start-apiserver-admission-initializer` (options/admission.go:151-170):
///   `discoveryRESTMapper.Reset()` every 30s for the admission plugins'
///   REST mapper. Admission here resolves resources without a cached
///   discovery mapper, so there is no cache to reset (deliberate deviation).
/// - `max-in-flight-filter` (config.go:949-958): registered only when APF is
///   off (`c.FlowControl == nil`); starts the executing-request watermark
///   flush (`StartMaxInFlightWatermarkMaintenance`, filters/maxinflight.go:91-105).
///   This server has no max-in-flight filter feeding the watermark (follow-up),
///   so the loop reports zeros; with APF on, `priority-and-fairness-filter`
///   owns the same flush.
pub fn register_generic_server_hooks(registry: &PostStartHooks, apf_enabled: bool) {
    spawn_starting_hook_with(registry, GENERIC_APISERVER_START_INFORMERS_HOOK, || {});
    registry.register_boot_check(INFORMER_SYNC_CHECK, || Ok(()));
    spawn_starting_hook_with(registry, ADMISSION_INITIALIZER_HOOK, || {});
    if !apf_enabled {
        spawn_starting_hook_with(registry, MAX_IN_FLIGHT_FILTER_HOOK, || {
            crate::apiserver_request_metrics::start_watermark_maintenance();
        });
    }
}

/// `start-kube-aggregator-informers` (kube-aggregator/pkg/apiserver/
/// apiserver.go:316-320): `informerFactory.Start(stop)` and
/// `c.GenericConfig.SharedInformerFactory.Start(stop)`, then returns nil, so it
/// finishes at once and is never fatal. The aggregator's APIService informers
/// are watch-driven loops here (`autoregister`, `apiservice_registration`) and
/// there is no shared informer factory to start, so the hook is registered
/// (for `/healthz/poststarthook/<name>`) with nothing to start. Deliberate
/// deviation, as for `generic-apiserver-start-informers`.
pub const AGGREGATOR_INFORMERS_HOOK: &str = "start-kube-aggregator-informers";

/// Register [`AGGREGATOR_INFORMERS_HOOK`] on `registry`.
pub fn register_aggregator_informers_hook(registry: &PostStartHooks) {
    spawn_starting_hook_with(registry, AGGREGATOR_INFORMERS_HOOK, || {});
}

/// `start-apiextensions-informers` (apiextensions-apiserver/pkg/apiserver/
/// apiserver.go:224-227): `s.Informers.Start(stop)`, then returns nil, so it
/// finishes at once and is never fatal. The CRD informer is a storage-backed
/// loop here (`crd-informer-synced`, the CRD controllers' watch), with no
/// informer factory to start. Registered before
/// `start-apiextensions-controllers`, as upstream. Deliberate deviation, as
/// for [`AGGREGATOR_INFORMERS_HOOK`].
pub const APIEXTENSIONS_INFORMERS_HOOK: &str = "start-apiextensions-informers";

/// Register [`APIEXTENSIONS_INFORMERS_HOOK`] on `registry`.
pub fn register_apiextensions_informers_hook(registry: &PostStartHooks) {
    spawn_starting_hook_with(registry, APIEXTENSIONS_INFORMERS_HOOK, || {});
}
