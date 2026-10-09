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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn unfinished_hook_fails_its_check_then_passes() {
        let reg = PostStartHooks::new();
        let (tx, rx) = oneshot::channel::<()>();
        let h = spawn_hook_with(
            &reg,
            "x/y",
            async move {
                rx.await.unwrap();
                Ok::<(), String>(())
            },
            |_, _| panic!("must not be fatal"),
        );
        assert_eq!(reg.check("x/y"), Some(Err("not finished")));
        assert_eq!(reg.checks()[0].0, "poststarthook/x/y");
        tx.send(()).unwrap();
        h.await.unwrap();
        assert_eq!(reg.check("x/y"), Some(Ok(())));
        assert_eq!(reg.check("nope"), None);
    }

    #[tokio::test]
    async fn failed_hook_is_fatal_and_never_finishes() {
        let reg = PostStartHooks::new();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        spawn_hook_with(
            &reg,
            "x/y",
            async { Err::<(), String>("boom".into()) },
            move |n, e| tx.send(format!("{n}: {e}")).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(rx.recv().unwrap(), "x/y: boom");
        assert_eq!(reg.check("x/y"), Some(Err("not finished")));
    }
}
