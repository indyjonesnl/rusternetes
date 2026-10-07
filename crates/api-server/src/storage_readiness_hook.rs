//! The `storage-readiness` post-start hook.
//!
//! Ported from `staging/src/k8s.io/apiserver/pkg/server/storage_readiness_hook.go`
//! (`StorageReadinessHook`): storages register a readiness check
//! (`RegisterStorage`, :48-57); the hook polls every registered check every
//! 100ms (`PollUntilContextCancel(.., 100*time.Millisecond, true, ..)`, :80-87)
//! until all pass or `timeout` elapses. A timeout is only logged
//! ("Deadline exceeded while waiting for storage readiness... ignoring",
//! :89-91) and the hook still returns nil, so it is never fatal.
//!
//! Registered as `storage-readiness` only when the
//! `WatchCacheInitializationPostStartHook` gate is on
//! (`pkg/controlplane/apiserver/server.go:315-317`); that gate is off by
//! default in v1.35 (`staging/src/k8s.io/apiserver/pkg/features/kube_features.go:494-496`),
//! so by default the hook is absent from `/readyz`, as upstream.
//!
//! `StorageInitializationTimeout` defaults to one minute
//! (`staging/src/k8s.io/apiserver/pkg/server/config.go:447`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::{error, info, warn};

/// `StorageInitializationTimeout` default (config.go:447).
pub const STORAGE_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Post-start hook name (server.go:316).
pub const STORAGE_READINESS_HOOK: &str = "storage-readiness";

/// Poll period (storage_readiness_hook.go:80).
const POLL_PERIOD: Duration = Duration::from_millis(100);

type Check = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// `StorageReadinessHook`.
pub struct StorageReadinessHook {
    timeout: Duration,
    checks: Mutex<BTreeMap<String, Check>>,
}

impl StorageReadinessHook {
    /// `NewStorageReadinessHook`.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            checks: Mutex::new(BTreeMap::new()),
        }
    }

    /// `RegisterStorage`: the first registration for a GVR wins; a repeat is
    /// logged (:52-56).
    pub fn register_storage(
        &self,
        gvr: &str,
        check: impl Fn() -> Result<(), String> + Send + Sync + 'static,
    ) {
        let mut checks = self.checks.lock().unwrap();
        if checks.contains_key(gvr) {
            error!("Registering storage readiness hook for {gvr} again: ");
        } else {
            checks.insert(gvr.to_string(), Arc::new(check));
        }
    }

    /// `check`: true once every registered check passes (:59-75).
    pub fn check(&self) -> bool {
        let checks = self.checks.lock().unwrap();
        let failed: Vec<&String> = checks
            .iter()
            .filter(|(_, c)| c().is_err())
            .map(|(g, _)| g)
            .collect();
        if failed.is_empty() {
            info!("Storage is ready for all registered resources");
            true
        } else {
            tracing::debug!("Storage is not ready for: {failed:?}");
            false
        }
    }

    /// `Hook`: poll until ready or the deadline; a deadline is not an error
    /// (:77-95).
    pub async fn run(&self) {
        let deadline = tokio::time::Instant::now() + self.timeout;
        // `immediate=true`: the first check runs at once.
        loop {
            if self.check() {
                return;
            }
            if tokio::time::Instant::now() + POLL_PERIOD > deadline {
                tokio::time::sleep_until(deadline).await;
                warn!("Deadline exceeded while waiting for storage readiness... ignoring");
                return;
            }
            tokio::time::sleep(POLL_PERIOD).await;
        }
    }
}

fn gate_enabled() -> bool {
    rusternetes_common::feature_gates::enabled(
        rusternetes_common::feature_gates::Feature::WatchCacheInitializationPostStartHook,
    )
}

/// Wire the hook for this api-server and register it (server.go:315-317).
///
/// Upstream registers one check per resource storage
/// (`registerStorageReadinessCheck`, genericapiserver.go:932-947), each being
/// the watch cache's `ReadinessCheck`. This api-server has no per-resource
/// cacher; the one storage backend is what every resource reads, so a single
/// check stands for them all: the backend answers a revision query.
/// (Deliberate deviation: one check, not one per resource.)
pub fn spawn_for_backend(storage: Arc<rusternetes_storage::StorageBackend>) -> bool {
    use rusternetes_storage::Storage;
    if !gate_enabled() {
        return false;
    }
    let hook = Arc::new(StorageReadinessHook::new(STORAGE_INITIALIZATION_TIMEOUT));
    let ready = Arc::new(AtomicBool::new(false));
    let flag = ready.clone();
    hook.register_storage("storage", move || {
        if flag.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("storage not ready".to_string())
        }
    });
    tokio::spawn(async move {
        while storage.current_revision().await.is_err() {
            tokio::time::sleep(POLL_PERIOD).await;
        }
        ready.store(true, Ordering::SeqCst);
    });
    spawn_storage_readiness_hook(hook)
}

/// Register the `storage-readiness` hook (server.go:315-317), gated on
/// `WatchCacheInitializationPostStartHook`. Returns whether it was registered.
pub fn spawn_storage_readiness_hook(hook: Arc<StorageReadinessHook>) -> bool {
    if !gate_enabled() {
        return false;
    }
    crate::post_start_hooks::spawn_hook(STORAGE_READINESS_HOOK, async move {
        hook.run().await;
        Ok::<(), std::convert::Infallible>(())
    });
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_common::feature_gates::{with_feature, Feature};

    fn gvr(i: usize) -> String {
        format!("group/version/resource-{i}")
    }

    /// TestStorageReadinessHook (storage_readiness_hook_test.go:45-66).
    #[test]
    fn check_passes_only_when_every_storage_is_ready() {
        let h = StorageReadinessHook::new(Duration::from_secs(1));
        let flags: Vec<Arc<AtomicBool>> =
            (0..5).map(|_| Arc::new(AtomicBool::new(false))).collect();
        for (i, f) in flags.iter().enumerate() {
            let f = f.clone();
            h.register_storage(&gvr(i), move || {
                if f.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err("failed".into())
                }
            });
        }
        for f in &flags {
            assert!(!h.check());
            f.store(true, Ordering::SeqCst);
        }
        assert!(h.check());
    }

    /// TestStorageReadinessHookTimeout (:68-87): a never-ready storage makes
    /// the hook return (not fail) after the timeout.
    #[tokio::test(start_paused = true)]
    async fn timeout_is_not_an_error() {
        let h = StorageReadinessHook::new(Duration::from_secs(1));
        h.register_storage(&gvr(0), || Err("failed".into()));
        h.run().await;
    }

    #[test]
    fn repeat_registration_keeps_the_first_check() {
        let h = StorageReadinessHook::new(Duration::from_secs(1));
        h.register_storage("g", || Ok(()));
        h.register_storage("g", || Err("second".into()));
        assert!(h.check());
    }

    /// server.go:315-317: absent by default, present when the gate is on.
    #[tokio::test]
    #[serial_test::serial]
    async fn registered_only_when_the_gate_is_on() {
        let hook = Arc::new(StorageReadinessHook::new(Duration::from_secs(1)));
        assert!(!spawn_storage_readiness_hook(hook.clone()));
        assert!(crate::post_start_hooks::global()
            .check(STORAGE_READINESS_HOOK)
            .is_none());
        let _g = with_feature(Feature::WatchCacheInitializationPostStartHook, true);
        assert!(spawn_storage_readiness_hook(hook));
        assert!(crate::post_start_hooks::global()
            .check(STORAGE_READINESS_HOOK)
            .is_some());
    }
}
