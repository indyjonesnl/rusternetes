//! The `start-kube-apiserver-identity-lease-controller` and
//! `start-kube-apiserver-identity-lease-garbage-collector` post-start hooks.
//!
//! Ported from `pkg/controlplane/apiserver/server.go:277-313` (hook
//! registration, constants :55-74, `labelAPIServerHeartbeatFunc` :327-353),
//! `staging/src/k8s.io/component-helpers/apimachinery/lease/controller.go`
//! (the heartbeat controller) and
//! `pkg/controlplane/controller/apiserverleasegc/gc_controller.go` (GC).
//! `APIServerIdentity` is Beta/default-on since 1.26 so it is not gated.
//! Both hooks start background work and return nil (never fatal).
//!
//! Deviations: no `stop-kube-apiserver-identity-lease-controller` pre-shutdown
//! hook (this server has no graceful-shutdown phase to hang it on; the
//! expired lease is GC'd instead, and Start() deletes a stale one);
//! `UnknownVersionInteroperabilityProxy` is Alpha/off, so the peer-address
//! annotation (server.go:349-353) is not written; the GC reads storage
//! directly instead of a label-filtered informer.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusternetes_common::resources::{Lease, LeaseSpec};
use rusternetes_common::Error;
use rusternetes_storage::{build_key, Storage, StorageBackend};
use sha2::{Digest, Sha256};
use tracing::{error, info};

/// `IdentityLeaseComponentLabelKey` (server.go:74).
pub const IDENTITY_LEASE_COMPONENT_LABEL_KEY: &str = "apiserver.kubernetes.io/identity";
/// The identity value upstream passes as `name` (`"kube-apiserver"`).
pub const IDENTITY_NAME: &str = "kube-apiserver";
/// `IdentityLeaseGCPeriod` (server.go:57).
pub const IDENTITY_LEASE_GC_PERIOD: Duration = Duration::from_secs(3600);
/// `IdentityLeaseDurationSeconds` (server.go:60).
pub const IDENTITY_LEASE_DURATION_SECONDS: i32 = 3600;
/// `IdentityLeaseRenewIntervalPeriod` (server.go:63).
pub const IDENTITY_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);
/// `metav1.NamespaceSystem`.
pub const LEASE_NAMESPACE: &str = "kube-system";
/// `apiv1.LabelHostname`.
pub const LABEL_HOSTNAME: &str = "kubernetes.io/hostname";
/// Hook names (server.go:295, :304).
pub const IDENTITY_LEASE_CONTROLLER_HOOK: &str = "start-kube-apiserver-identity-lease-controller";
pub const IDENTITY_LEASE_GC_HOOK: &str = "start-kube-apiserver-identity-lease-garbage-collector";
/// `maxUpdateRetries` / `maxBackoff` (lease/controller.go:42, :44).
const MAX_UPDATE_RETRIES: usize = 5;
const MAX_BACKOFF: Duration = Duration::from_secs(7);

/// `NewConfig` (apiserver/pkg/server/config.go:398-424): `"apiserver-" +
/// lower(base32-nopad(sha256(u16len|hostname|u16len|"kube-apiserver")[:16]))`.
pub fn apiserver_id(hostname: &str) -> String {
    let mut data = Vec::new();
    for part in [hostname, IDENTITY_NAME] {
        data.extend_from_slice(&(part.len() as u16).to_be_bytes());
        data.extend_from_slice(part.as_bytes());
    }
    let hash = Sha256::digest(&data);
    format!("apiserver-{}", base32_nopad_lower(&hash[..16]))
}

pub fn base32_nopad_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let (mut out, mut buf, mut bits) = (String::new(), 0u32, 0u32);
    for &b in bytes {
        buf = (buf << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buf >> bits) & 31) as usize] as char);
        }
        buf &= (1 << bits) - 1;
    }
    if bits > 0 {
        out.push(ALPHABET[((buf << (5 - bits)) & 31) as usize] as char);
    }
    out
}

pub fn lease_key(name: &str) -> String {
    build_key("leases", Some(LEASE_NAMESPACE), name)
}

/// `labelAPIServerHeartbeatFunc` (server.go:327-353): label the lease with
/// the identity and the hostname.
pub fn label_heartbeat(lease: &mut Lease, identity: &str, hostname: &str) {
    let labels = lease.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        IDENTITY_LEASE_COMPONENT_LABEL_KEY.to_string(),
        identity.to_string(),
    );
    labels.insert(LABEL_HOSTNAME.to_string(), hostname.to_string());
}

/// The identity lease controller (lease/controller.go `controller`).
pub struct LeaseController {
    storage: Arc<StorageBackend>,
    holder_identity: String,
    lease_name: String,
    hostname: String,
    duration_seconds: i32,
    latest: Option<Lease>,
}

impl LeaseController {
    pub fn new(
        storage: Arc<StorageBackend>,
        lease_name: String,
        holder_identity: String,
        hostname: String,
    ) -> Self {
        Self {
            storage,
            holder_identity,
            lease_name,
            hostname,
            duration_seconds: IDENTITY_LEASE_DURATION_SECONDS,
            latest: None,
        }
    }

    /// `Start` (controller.go:112-124): delete a stale lease from a previous
    /// instance of this apiserver before heartbeating.
    pub async fn delete_old_lease(&self) {
        match self.storage.delete(&lease_key(&self.lease_name)).await {
            Ok(()) | Err(Error::NotFound(_)) => {}
            Err(e) => error!("error deleting old lease {}: {e}", self.lease_name),
        }
    }

    /// `newLease` (controller.go:285-312).
    fn new_lease(&self, base: Option<&Lease>, now: DateTime<Utc>) -> Lease {
        let mut lease = match base {
            Some(b) => b.clone(),
            None => {
                let mut l = Lease::new(self.lease_name.clone(), LEASE_NAMESPACE);
                l.spec = Some(LeaseSpec {
                    holder_identity: Some(self.holder_identity.clone()),
                    ..Default::default()
                });
                l
            }
        };
        let spec = lease.spec.get_or_insert_with(Default::default);
        spec.lease_duration_seconds = Some(self.duration_seconds);
        spec.renew_time = Some(now);
        label_heartbeat(&mut lease, IDENTITY_NAME, &self.hostname);
        lease
    }

    /// `ensureLease` (controller.go:237-262): `(lease, created)`.
    async fn ensure_lease(&self, now: DateTime<Utc>) -> rusternetes_common::Result<(Lease, bool)> {
        let key = lease_key(&self.lease_name);
        match self.storage.get::<Lease>(&key).await {
            Ok(l) => Ok((l, false)),
            Err(Error::NotFound(_)) => {
                let created = self
                    .storage
                    .create(&key, &self.new_lease(None, now))
                    .await?;
                Ok((created, true))
            }
            Err(e) => Err(e),
        }
    }

    /// `backoffEnsureLease` (controller.go:213-235).
    async fn backoff_ensure_lease(&self, now: &impl Fn() -> DateTime<Utc>) -> (Lease, bool) {
        let mut sleep = Duration::from_millis(100);
        loop {
            match self.ensure_lease(now()).await {
                Ok(r) => return r,
                Err(e) => {
                    sleep = (sleep * 2).min(MAX_BACKOFF);
                    error!("Failed to ensure lease exists, will retry in {sleep:?}: {e}");
                    tokio::time::sleep(sleep).await;
                }
            }
        }
    }

    /// `retryUpdateLease` (controller.go:264-283). A conflict re-fetches the
    /// lease and tries again.
    async fn retry_update_lease(
        &mut self,
        mut base: Lease,
        now: &impl Fn() -> DateTime<Utc>,
    ) -> Result<(), String> {
        let key = lease_key(&self.lease_name);
        for _ in 0..MAX_UPDATE_RETRIES {
            let to_update = self.new_lease(Some(&base), now());
            match self.storage.update(&key, &to_update).await {
                Ok(l) => {
                    self.latest = Some(l);
                    return Ok(());
                }
                Err(Error::Conflict(_)) => base = self.backoff_ensure_lease(now).await.0,
                Err(e) => error!("Failed to update lease: {e}"),
            }
        }
        Err(format!(
            "failed {MAX_UPDATE_RETRIES} attempts to update lease"
        ))
    }

    /// `sync` (controller.go:166-196): one heartbeat.
    pub async fn sync(&mut self, now: impl Fn() -> DateTime<Utc>) {
        if let Some(latest) = self.latest.clone() {
            // Optimistic update from the last known version avoids the GET.
            if self.retry_update_lease(latest, &now).await.is_ok() {
                return;
            }
        }
        let (lease, created) = self.backoff_ensure_lease(&now).await;
        self.latest = Some(lease.clone());
        // we don't need to update the lease if we just created it
        if !created {
            if let Err(e) = self.retry_update_lease(lease, &now).await {
                error!("Will retry updating lease in {IDENTITY_LEASE_RENEW_INTERVAL:?}: {e}");
            }
        }
    }

    /// `Run`: `wait.JitterUntilWithContext(ctx, c.sync, renewInterval, 0.04, true)`.
    pub async fn run(mut self) {
        loop {
            self.sync(Utc::now).await;
            let jitter = 1.0 + rand::random::<f64>() * 0.04;
            tokio::time::sleep(IDENTITY_LEASE_RENEW_INTERVAL.mul_f64(jitter)).await;
        }
    }
}

/// `isLeaseExpired` (gc_controller.go:123-132): a lease without a renew time
/// or duration is invalid and is collected too.
pub fn is_lease_expired(lease: &Lease, now: DateTime<Utc>) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return true;
    };
    match (spec.renew_time, spec.lease_duration_seconds) {
        (Some(renew), Some(secs)) => renew + chrono::Duration::seconds(secs as i64) < now,
        _ => true,
    }
}

/// One `gc` pass (gc_controller.go:82-121) over the leases carrying
/// `apiserver.kubernetes.io/identity=<identity>`.
pub async fn gc_expired_leases(storage: &StorageBackend, identity: &str, now: DateTime<Utc>) {
    let prefix = build_key("leases", Some(LEASE_NAMESPACE), "");
    let leases = match storage.list::<Lease>(&prefix).await {
        Ok(l) => l,
        Err(e) => {
            error!("Error while listing apiserver leases: {e}");
            return;
        }
    };
    for lease in leases {
        let labelled = lease
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(IDENTITY_LEASE_COMPONENT_LABEL_KEY))
            .is_some_and(|v| v == identity);
        if !labelled || !is_lease_expired(&lease, now) {
            continue;
        }
        let key = lease_key(&lease.metadata.name);
        // double check latest lease from apiserver before deleting
        match storage.get::<Lease>(&key).await {
            Ok(fresh) if is_lease_expired(&fresh, now) => match storage.delete(&key).await {
                Ok(()) | Err(Error::NotFound(_)) => {}
                Err(e) => error!("Error deleting lease: {e}"),
            },
            Ok(_) | Err(Error::NotFound(_)) => {}
            Err(e) => error!("Error getting lease: {e}"),
        }
    }
}

/// `start-kube-apiserver-identity-lease-controller` (server.go:295-297).
pub fn spawn_identity_lease_controller(
    storage: Arc<StorageBackend>,
    hostname: String,
) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(IDENTITY_LEASE_CONTROLLER_HOOK, move || {
        let id = apiserver_id(&hostname);
        // holderIdentity = APIServerID + "_" + uuid (server.go:279)
        let holder = format!("{id}_{}", uuid::Uuid::new_v4());
        let controller = LeaseController::new(storage, id, holder, hostname);
        tokio::spawn(async move {
            info!("Starting kube-apiserver identity lease controller");
            controller.delete_old_lease().await;
            controller.run().await;
        });
    })
}

/// `start-kube-apiserver-identity-lease-garbage-collector` (server.go:304-312).
pub fn spawn_identity_lease_gc(storage: Arc<StorageBackend>) -> tokio::task::JoinHandle<()> {
    crate::post_start_hooks::spawn_starting_hook(IDENTITY_LEASE_GC_HOOK, move || {
        tokio::spawn(async move {
            info!("Starting apiserver lease garbage collector");
            let mut tick = tokio::time::interval(IDENTITY_LEASE_GC_PERIOD);
            loop {
                tick.tick().await;
                gc_expired_leases(&storage, IDENTITY_NAME, Utc::now()).await;
            }
        });
    })
}

/// Both hooks, wired once for every startup path.
/// The host name `apiserver_id` is derived from.
pub fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".to_string())
}

pub fn spawn_identity_hooks(storage: Arc<StorageBackend>) {
    let hostname = hostname();
    spawn_identity_lease_controller(storage.clone(), hostname);
    spawn_identity_lease_gc(storage);
}
