//! Bootstrap token cleaner.
//!
//! Ported from `pkg/controller/bootstrap/tokencleaner.go` (release-1.35) and
//! wired as `newTokenCleanerController`
//! (`cmd/kube-controller-manager/app/bootstrap.go:55-80`): the controller
//! deletes expired `bootstrap.kubernetes.io/token` Secrets in `kube-system`.
//! Upstream registers it `isDisabledByDefault: true` (`bootstrap.go:59`), so
//! here it is opt-in too (`--token-cleaner-controller`).
//!
//! `evalSecret` (tokencleaner.go) and `bootstrapsecretutil.GetExpiration`
//! (`staging/src/k8s.io/cluster-bootstrap/util/secrets/secrets.go:59-78`) are
//! ported 1:1, including "an unparseable expiration is treated as expired".
//!
//! Deviations (storage seam instead of informer + workqueue):
//! - Upstream filters an informer on `Type == SecretTypeBootstrapToken &&
//!   Namespace == tokenSecretNamespace` and requeues a not-yet-expired
//!   secret with `queue.AddAfter(key, ttl)`. The `Storage` trait has no
//!   list-by-type, so each pass lists the `kube-system` secrets and filters
//!   by type; the pass runs on the worker period (10s,
//!   `wait.UntilWithContext(ctx, tc.worker, 10*time.Second)`) which stands in
//!   for the `AddAfter` requeue (expiry is noticed within one period).
//! - The delete's `Preconditions{UID}` is checked by re-reading the Secret
//!   just before deleting (no preconditions in `Storage`); a UID mismatch is
//!   the swallowed Conflict.

use anyhow::Result;
use chrono::{DateTime, Utc};
use rusternetes_common::resources::Secret;
use rusternetes_storage::{build_key, build_prefix, Storage};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

/// `bootstrapapi.SecretTypeBootstrapToken`
/// (`staging/src/k8s.io/cluster-bootstrap/token/api/types.go:33`).
pub const SECRET_TYPE_BOOTSTRAP_TOKEN: &str = "bootstrap.kubernetes.io/token";
/// `bootstrapapi.BootstrapTokenExpirationKey` (types.go:48).
pub const BOOTSTRAP_TOKEN_EXPIRATION_KEY: &str = "expiration";
/// `api.NamespaceSystem`, `DefaultTokenCleanerOptions().TokenSecretNamespace`.
pub const DEFAULT_TOKEN_SECRET_NAMESPACE: &str = "kube-system";
/// The worker period of `Run` (`wait.UntilWithContext(ctx, tc.worker, 10*time.Second)`).
pub const WORKER_PERIOD: Duration = Duration::from_secs(10);

/// What `evalSecret` did with a secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalOutcome {
    /// No expiration.
    Nothing,
    /// The secret was expired and a delete was issued.
    Deleted,
    /// Not yet expired: `queue.AddAfter(key, ttl)`.
    RequeueAfter(Duration),
}

pub struct TokenCleaner<S: Storage> {
    storage: Arc<S>,
    token_secret_namespace: String,
}

/// `GetExpiration` (secrets.go:59-78): `(timeRemaining, isExpired)`.
fn get_expiration(secret: &Secret, now: DateTime<Utc>) -> (Duration, bool) {
    let expiration = secret
        .data
        .as_ref()
        .and_then(|d| d.get(BOOTSTRAP_TOKEN_EXPIRATION_KEY))
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_default();
    if expiration.is_empty() {
        return (Duration::ZERO, false);
    }
    let Ok(exp) = DateTime::parse_from_rfc3339(&expiration) else {
        return (Duration::ZERO, true);
    };
    match (exp.with_timezone(&Utc) - now).to_std() {
        Ok(d) if !d.is_zero() => (d, false),
        _ => (Duration::ZERO, true),
    }
}

impl<S: Storage + 'static> TokenCleaner<S> {
    /// `NewTokenCleaner` with `DefaultTokenCleanerOptions()`.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            token_secret_namespace: DEFAULT_TOKEN_SECRET_NAMESPACE.to_string(),
        }
    }

    /// `Run`: sync every worker period.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting token cleaner controller");
        loop {
            self.sync_all(Utc::now()).await;
            tokio::time::sleep(WORKER_PERIOD).await;
        }
    }

    /// One pass: the informer's FilterFunc (type + namespace) then `syncFunc`.
    pub async fn sync_all(&self, now: DateTime<Utc>) {
        let secrets: Vec<Secret> = match self
            .storage
            .list(&build_prefix("secrets", Some(&self.token_secret_namespace)))
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!("Listing bootstrap token secrets: {e}");
                return;
            }
        };
        for secret in secrets {
            if secret.secret_type.as_deref() == Some(SECRET_TYPE_BOOTSTRAP_TOKEN)
                && secret.metadata.namespace.as_deref() == Some(&self.token_secret_namespace)
            {
                self.eval_secret(&secret, now).await;
            }
        }
    }

    /// `evalSecret`.
    pub async fn eval_secret(&self, secret: &Secret, now: DateTime<Utc>) -> EvalOutcome {
        let (ttl, already_expired) = get_expiration(secret, now);
        if already_expired {
            self.delete_with_uid_precondition(secret).await;
            EvalOutcome::Deleted
        } else if !ttl.is_zero() {
            EvalOutcome::RequeueAfter(ttl)
        } else {
            EvalOutcome::Nothing
        }
    }

    /// `Delete` with `Preconditions{UID}` only when the secret has a UID;
    /// NotFound and Conflict are swallowed, other errors only logged.
    async fn delete_with_uid_precondition(&self, secret: &Secret) {
        let ns = secret.metadata.namespace.as_deref().unwrap_or("");
        let key = build_key("secrets", Some(ns), &secret.metadata.name);
        if !secret.metadata.uid.is_empty() {
            match self.storage.get::<Secret>(&key).await {
                Ok(live) if live.metadata.uid == secret.metadata.uid => {}
                Ok(_) => return, // Conflict: the UID precondition failed
                Err(rusternetes_common::Error::NotFound(_)) => return,
                Err(e) => {
                    info!("Error deleting secret: {e}");
                    return;
                }
            }
        }
        match self.storage.delete(&key).await {
            Ok(()) | Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => info!("Error deleting secret: {e}"),
        }
    }
}
