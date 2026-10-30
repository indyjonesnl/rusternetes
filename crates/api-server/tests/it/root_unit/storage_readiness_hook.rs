//! Unit tests moved from `src/storage_readiness_hook.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use std::collections::BTreeMap;
    pub use std::sync::atomic::{AtomicBool, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::Duration;
    pub use tracing::{error, info, warn};
}

mod tests {
    use super::__parent::*;
    use rusternetes_api_server::storage_readiness_hook::*;
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

    /// genericapiserver.go:932-947: one `RegisterStorage` per resource GVR,
    /// keyed by `GroupVersionResource.String()`.
    #[test]
    fn one_check_per_resource_gvr() {
        let gvrs = storage_readiness_gvrs();
        assert!(gvrs.contains(&"/v1, Resource=pods".to_string()));
        assert!(gvrs.contains(&"apps/v1, Resource=deployments".to_string()));
        // Subresources and non-storage-backed virtual REST are not registered.
        assert!(!gvrs
            .iter()
            .any(|g| g.contains('/') && g.ends_with("/status")));
        assert!(!gvrs.iter().any(|g| g.ends_with("Resource=tokenreviews")));
        assert!(!gvrs
            .iter()
            .any(|g| g.ends_with("Resource=subjectaccessreviews")));
        let h = StorageReadinessHook::new(Duration::from_secs(1));
        let ready = Arc::new(AtomicBool::new(false));
        register_resource_storages(&h, ready.clone());
        assert_eq!(h.registered(), {
            let mut v = gvrs.clone();
            v.sort();
            v.dedup();
            v
        });
        assert!(!h.check());
        ready.store(true, Ordering::SeqCst);
        assert!(h.check());
    }

    #[test]
    fn gvr_string_matches_metav1() {
        assert_eq!(
            gvr_string("apps", "v1", "deployments"),
            "apps/v1, Resource=deployments"
        );
        assert_eq!(gvr_string("", "v1", "pods"), "/v1, Resource=pods");
    }

    /// server.go:315-317: absent by default, present when the gate is on.
    #[tokio::test]
    #[serial_test::serial]
    async fn registered_only_when_the_gate_is_on() {
        let hook = Arc::new(StorageReadinessHook::new(Duration::from_secs(1)));
        assert!(!spawn_storage_readiness_hook(hook.clone()));
        assert!(rusternetes_api_server::post_start_hooks::global()
            .check(STORAGE_READINESS_HOOK)
            .is_none());
        let _g = with_feature(Feature::WatchCacheInitializationPostStartHook, true);
        assert!(spawn_storage_readiness_hook(hook));
        assert!(rusternetes_api_server::post_start_hooks::global()
            .check(STORAGE_READINESS_HOOK)
            .is_some());
    }
}
