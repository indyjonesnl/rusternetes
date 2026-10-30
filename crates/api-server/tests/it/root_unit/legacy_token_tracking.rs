//! Unit tests moved from `src/legacy_token_tracking.rs`.
#![allow(unused_imports, dead_code)]

mod __parent {
    pub use chrono::{DateTime, Utc};
    pub use futures::StreamExt;
    pub use rusternetes_api_server::registry::apiextensions::customresourcedefinition::ItemExponentialFailureRateLimiter;
    pub use rusternetes_common::resources::ConfigMap;
    pub use rusternetes_storage::{build_key, Storage, StorageBackend, WatchEvent};
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use tracing::{info, warn};
}

/// Ported from `controller_test.go` `TestSyncConfigMap`; `throttlePeriod` is
/// 30s there (`controller_test.go:36`).
mod tests {
    use super::__parent::*;
    use chrono::TimeZone;
    use rusternetes_api_server::legacy_token_tracking::*;

    const THROTTLE: Duration = Duration::from_secs(30);

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 7, 12, 0, 0).unwrap()
    }

    fn plus(d: Duration) -> DateTime<Utc> {
        t0() + chrono::Duration::from_std(d).unwrap()
    }

    fn key() -> String {
        build_key("configmaps", Some(CONFIG_MAP_NAMESPACE), CONFIG_MAP_NAME)
    }

    async fn put(storage: &StorageBackend, data: Option<&[(&str, &str)]>) {
        let mut cm = new_config_map(t0());
        cm.data = data.map(|d| {
            d.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        });
        storage.create(&key(), &cm).await.unwrap();
    }

    async fn since(storage: &StorageBackend) -> Option<String> {
        storage
            .get::<ConfigMap>(&key())
            .await
            .ok()?
            .data?
            .get(CONFIG_MAP_DATA_KEY)
            .cloned()
    }

    #[tokio::test]
    async fn creates_the_configmap_when_absent() {
        let s = StorageBackend::new_memory();
        let mut l = CreationLimiter::new(THROTTLE);
        assert_eq!(
            sync_config_map(&s, t0(), &mut l).await.unwrap(),
            Synced::Wrote
        );
        assert_eq!(since(&s).await.as_deref(), Some("2026-10-07"));
    }

    /// "create configmap should ignore AlreadyExists error": the live object
    /// exists, so no error and no change.
    #[tokio::test]
    async fn existing_valid_configmap_is_left_alone() {
        let s = StorageBackend::new_memory();
        put(&s, Some(&[("since", "2026-01-02")])).await;
        let mut l = CreationLimiter::new(THROTTLE);
        assert_eq!(
            sync_config_map(&s, t0(), &mut l).await.unwrap(),
            Synced::Nothing
        );
        assert_eq!(since(&s).await.as_deref(), Some("2026-01-02"));
    }

    /// "create configmap throttled" / "after throttle period".
    #[tokio::test]
    async fn recreation_is_throttled_for_the_period() {
        let s = StorageBackend::new_memory();
        let mut l = CreationLimiter::new(THROTTLE);
        sync_config_map(&s, t0(), &mut l).await.unwrap();
        for at in [
            THROTTLE - Duration::from_secs(2),
            THROTTLE - Duration::from_secs(1),
        ] {
            let _ = s.delete(&key()).await;
            let r = sync_config_map(&s, plus(at), &mut l).await.unwrap();
            assert_eq!(r, Synced::Throttled(THROTTLE - at));
            assert_eq!(since(&s).await, None, "must not create while throttled");
        }
        let after = THROTTLE + Duration::from_secs(1);
        assert_eq!(
            sync_config_map(&s, plus(after), &mut l).await.unwrap(),
            Synced::Wrote
        );
        assert_eq!(since(&s).await.as_deref(), Some("2026-10-07"));
    }

    /// "update configmap [unexpected date format]" and "with no data".
    #[tokio::test]
    async fn bad_or_missing_since_is_rewritten() {
        for data in [
            Some(&[("since", "2026-10-07T12:00:00Z")][..]),
            Some(&[("since", "BAD_TIMESTAMP")][..]),
            Some(&[("since", "2026-1-2")][..]),
            Some(&[("other", "x")][..]),
            None,
        ] {
            let s = StorageBackend::new_memory();
            put(&s, data).await;
            let mut l = CreationLimiter::new(THROTTLE);
            assert_eq!(
                sync_config_map(&s, t0(), &mut l).await.unwrap(),
                Synced::Wrote
            );
            assert_eq!(since(&s).await.as_deref(), Some("2026-10-07"), "{data:?}");
        }
    }

    #[tokio::test]
    async fn hook_registers_and_creates_the_configmap() {
        let s = Arc::new(StorageBackend::new_memory());
        spawn_legacy_token_tracking_controller(s.clone())
            .await
            .unwrap();
        assert_eq!(
            rusternetes_api_server::post_start_hooks::global().check(LEGACY_TOKEN_TRACKING_HOOK),
            Some(Ok(()))
        );
        for _ in 0..200 {
            if since(&s).await.is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("controller never created the ConfigMap");
    }
}
