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

    pub fn registered(&self) -> Vec<String> {
        self.checks.lock().unwrap().keys().cloned().collect()
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
            // Go `%v` of a []string: `[a b]`.
            let names: Vec<&str> = failed.iter().map(|g| g.as_str()).collect();
            tracing::debug!("Storage is not ready for: [{}]", names.join(" "));
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

/// `metav1.GroupVersionResource.String()`
/// (`staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/group_version.go:56-61`):
/// `group + "/" + version + ", Resource=" + resource`.
pub fn gvr_string(group: &str, version: &str, resource: &str) -> String {
    format!("{group}/{version}, Resource={resource}")
}

/// Resources whose REST is not a `genericregistry.Store` over etcd, so it
/// does not implement `rest.StorageWithReadiness` and
/// `registerStorageReadinessCheck` (genericapiserver.go:937-938) skips it:
/// the create-only review REST and the virtual `componentstatuses` / `bindings`.
const NOT_STORAGE_WITH_READINESS: &[&str] = &[
    "tokenreviews",
    "selfsubjectreviews",
    "subjectaccessreviews",
    "localsubjectaccessreviews",
    "selfsubjectaccessreviews",
    "selfsubjectrulesreviews",
    "componentstatuses",
    "bindings",
];

/// Every GVR registered through `registerStorageReadinessCheck`
/// (genericapiserver.go:934-946): each version of each resource, keyed by
/// `GroupVersionResource.String()`. Subresources are separate storage-map
/// entries upstream whose REST (e.g. `StatusREST`) has no `ReadinessCheck`.
pub fn storage_readiness_gvrs() -> Vec<String> {
    rusternetes_discovery::registered_kinds()
        .into_iter()
        .filter(|k| k.subresource.is_empty())
        .filter(|k| !NOT_STORAGE_WITH_READINESS.contains(&k.resource.as_str()))
        .map(|k| gvr_string(&k.group, &k.version, &k.resource))
        .collect()
}

/// `registerStorageReadinessCheck` for every served group: one
/// `RegisterStorage(gvr, storage)` per resource. Every resource shares this
/// server's single storage backend, so each GVR's `ReadinessCheck` reads the
/// same readiness flag (the backend answering a revision query).
pub fn register_resource_storages(hook: &StorageReadinessHook, ready: Arc<AtomicBool>) {
    for gvr in storage_readiness_gvrs() {
        let flag = ready.clone();
        hook.register_storage(&gvr, move || {
            if flag.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err("storage not ready".to_string())
            }
        });
    }
}

/// Wire the hook for this api-server and register it (server.go:315-317).
///
/// Upstream registers one check per resource storage
/// (`registerStorageReadinessCheck`, genericapiserver.go:932-947), each being
/// the watch cache's `ReadinessCheck`. We register one per served GVR too
/// ([`register_resource_storages`]); with no per-resource cacher they all
/// observe the one storage backend answering a revision query.
pub fn spawn_for_backend(storage: Arc<rusternetes_storage::StorageBackend>) -> bool {
    use rusternetes_storage::Storage;
    if !gate_enabled() {
        return false;
    }
    let hook = Arc::new(StorageReadinessHook::new(STORAGE_INITIALIZATION_TIMEOUT));
    let ready = Arc::new(AtomicBool::new(false));
    register_resource_storages(&hook, ready.clone());
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
