//! The `start-legacy-token-tracking-controller` post-start hook.
//!
//! Ported from `pkg/controlplane/controller/legacytokentracking/controller.go`
//! and registered as upstream does at
//! `pkg/controlplane/apiserver/server.go:319-322` (`go
//! legacytokentracking.NewController(client).RunWithContext(hookContext)`,
//! `return nil` -- so the hook is never fatal).
//!
//! The controller maintains `kube-system/kube-apiserver-legacy-service-account-token-tracking`
//! whose `since` key is the date (`2006-01-02`) from which legacy-token use is
//! tracked; the legacy-token cleaner reads it
//! (`pkg/controller/serviceaccount/legacy_serviceaccount_token_cleaner.go:264`).
//! `LegacyServiceAccountTokenTracking` is GA and always on in v1.35, so the
//! controller is not gated.
//!
//! Deviation: upstream reads through a ConfigMap informer filtered to that
//! name (`controller.go:63-65`, resync 12h) and enqueues on its
//! add/update/delete events; here the sync reads storage directly, a watch on
//! the key stands in for the informer's events, and a 12h tick for its resync.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use rusternetes_common::resources::ConfigMap;
use rusternetes_storage::{build_key, Storage, StorageBackend, WatchEvent};
use tracing::{info, warn};

use crate::registry::apiextensions::customresourcedefinition::ItemExponentialFailureRateLimiter;

/// `ConfigMapName` (controller.go:39).
pub const CONFIG_MAP_NAME: &str = "kube-apiserver-legacy-service-account-token-tracking";
/// `ConfigMapDataKey` (controller.go:40).
pub const CONFIG_MAP_DATA_KEY: &str = "since";
/// Namespace: `metav1.NamespaceSystem`.
pub const CONFIG_MAP_NAMESPACE: &str = "kube-system";
/// Post-start hook name (server.go:319).
pub const LEGACY_TOKEN_TRACKING_HOOK: &str = "start-legacy-token-tracking-controller";

/// `rate.Every(30*time.Minute)` creation limiter period (controller.go:67).
const CREATION_PERIOD: Duration = Duration::from_secs(30 * 60);
/// Informer resync period (controller.go:63).
const RESYNC_PERIOD: Duration = Duration::from_secs(12 * 3600);

/// `time.Parse("2006-01-02", s)`: strict, zero-padded.
fn valid_date(s: &str) -> bool {
    s.len() == 10 && chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

fn format_date(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%d").to_string()
}

/// `rate.NewLimiter(rate.Every(period), 1)`: burst 1, so one creation is
/// allowed at once and the next only `period` after the last one. A creation
/// that fails is not counted (`r.CancelAt(now)`, controller.go:186-188).
pub struct CreationLimiter {
    period: Duration,
    last: Option<DateTime<Utc>>,
}

impl CreationLimiter {
    pub fn new(period: Duration) -> Self {
        Self { period, last: None }
    }

    /// `ReserveN(now, 1)` + `DelayFrom(now)`: `None` when the reservation is
    /// granted (and consumed), otherwise the delay (reservation cancelled).
    fn reserve(&mut self, now: DateTime<Utc>) -> Option<Duration> {
        if let Some(last) = self.last {
            let next = last + chrono::Duration::from_std(self.period).unwrap();
            if now < next {
                return (next - now).to_std().ok();
            }
        }
        self.last = Some(now);
        None
    }
}

/// What `syncConfigMap` did.
#[derive(Debug, PartialEq, Eq)]
pub enum Synced {
    /// Nothing to do (valid ConfigMap, or a create/update lost a race).
    Nothing,
    /// Created, or updated, the ConfigMap.
    Wrote,
    /// Creation is throttled; requeue after the delay (`AddAfter`).
    Throttled(Duration),
}

fn new_config_map(now: DateTime<Utc>) -> ConfigMap {
    let mut cm = ConfigMap {
        type_meta: rusternetes_common::types::TypeMeta {
            kind: "ConfigMap".to_string(),
            api_version: "v1".to_string(),
        },
        metadata: rusternetes_common::types::ObjectMeta::new(CONFIG_MAP_NAME)
            .with_namespace(CONFIG_MAP_NAMESPACE.to_string()),
        data: Some(
            [(CONFIG_MAP_DATA_KEY.to_string(), format_date(now))]
                .into_iter()
                .collect(),
        ),
        binary_data: None,
        immutable: None,
    };
    cm.metadata.ensure_uid();
    cm.metadata.ensure_creation_timestamp();
    cm
}

/// `syncConfigMap` (controller.go:166-208).
pub async fn sync_config_map(
    storage: &StorageBackend,
    now: DateTime<Utc>,
    limiter: &mut CreationLimiter,
) -> rusternetes_common::Result<Synced> {
    use rusternetes_common::Error;
    let key = build_key("configmaps", Some(CONFIG_MAP_NAMESPACE), CONFIG_MAP_NAME);
    let existing = match storage.get::<ConfigMap>(&key).await {
        Ok(cm) => Some(cm),
        Err(Error::NotFound(_)) => None,
        Err(e) => return Err(e),
    };
    match existing {
        None => {
            let previous = limiter.last;
            if let Some(delay) = limiter.reserve(now) {
                return Ok(Synced::Throttled(delay));
            }
            match storage.create(&key, &new_config_map(now)).await {
                Ok(_) => Ok(Synced::Wrote),
                Err(Error::AlreadyExists(_)) => Ok(Synced::Nothing),
                Err(e) => {
                    // "don't consume the creationRatelimiter for an
                    // unsuccessful attempt"
                    limiter.last = previous;
                    Err(e)
                }
            }
        }
        Some(mut cm) => {
            let valid = cm
                .data
                .as_ref()
                .and_then(|d| d.get(CONFIG_MAP_DATA_KEY))
                .is_some_and(|v| valid_date(v));
            if valid {
                return Ok(Synced::Nothing);
            }
            cm.data
                .get_or_insert_with(Default::default)
                .insert(CONFIG_MAP_DATA_KEY.to_string(), format_date(now));
            match storage.update(&key, &cm).await {
                Ok(_) => Ok(Synced::Wrote),
                Err(Error::NotFound(_) | Error::Conflict(_)) => Ok(Synced::Nothing),
                Err(e) => Err(e),
            }
        }
    }
}

/// `start-legacy-token-tracking-controller` (server.go:319-322): the hook
/// spawns `RunWithContext` and returns nil at once.
pub fn spawn_legacy_token_tracking_controller(
    storage: Arc<StorageBackend>,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(LEGACY_TOKEN_TRACKING_HOOK, move || {
        tokio::spawn(run(storage));
    })
}

/// `RunWithContext` + `processNext` (controller.go:112-164): sync at start,
/// on every ConfigMap event, on the resync tick, and after a delay when
/// throttled or failed (`AddRateLimited`: per-item exponential backoff,
/// `DefaultTypedControllerRateLimiter`).
async fn run(storage: Arc<StorageBackend>) {
    info!("Starting legacy_token_tracking_controller");
    let key = build_key("configmaps", Some(CONFIG_MAP_NAMESPACE), CONFIG_MAP_NAME);
    let mut limiter = CreationLimiter::new(CREATION_PERIOD);
    let mut backoff =
        ItemExponentialFailureRateLimiter::new(Duration::from_millis(5), Duration::from_secs(1000));
    let mut resync = tokio::time::interval(RESYNC_PERIOD);
    resync.tick().await;
    let mut retry_at: Option<tokio::time::Instant> = None;
    let mut watch = None;
    // `c.queue.Add(queueKey)` once the informer has synced.
    let mut due = true;
    loop {
        if due {
            due = false;
            match sync_config_map(&storage, Utc::now(), &mut limiter).await {
                Ok(Synced::Throttled(d)) => retry_at = Some(tokio::time::Instant::now() + d),
                Ok(_) => backoff.forget(CONFIG_MAP_NAME),
                Err(e) => {
                    warn!(
                        "Error while syncing ConfigMap {CONFIG_MAP_NAMESPACE}/{CONFIG_MAP_NAME}: {e}"
                    );
                    retry_at = Some(tokio::time::Instant::now() + backoff.when(CONFIG_MAP_NAME));
                }
            }
        }
        if watch.is_none() {
            watch = storage.watch(&key).await.ok();
        }
        let event = async {
            match watch.as_mut() {
                Some(w) => w.next().await,
                None => std::future::pending().await,
            }
        };
        let timer = async {
            match retry_at {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = resync.tick() => due = true,
            _ = timer => { retry_at = None; due = true; }
            ev = event => match ev {
                Some(Ok(WatchEvent::Added(k, _) | WatchEvent::Modified(k, _) | WatchEvent::Deleted(k, _))) if k == key => due = true,
                Some(Ok(_)) => {}
                // A broken watch is re-established on the next pass.
                Some(Err(_)) | None => watch = None,
            },
        }
    }
}

/// Ported from `controller_test.go` `TestSyncConfigMap`; `throttlePeriod` is
/// 30s there (`controller_test.go:36`).
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
            crate::post_start_hooks::global().check(LEGACY_TOKEN_TRACKING_HOOK),
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
