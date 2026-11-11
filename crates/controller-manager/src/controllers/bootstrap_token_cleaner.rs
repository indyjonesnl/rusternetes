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
//! Work loop ported 1:1: a rate-limited queue named `token_cleaner` holds
//! `<namespace>/<name>` keys (`NewTokenCleaner`); the worker's
//! `processNextWorkItem` runs `syncFunc` per key and `AddRateLimited`s a
//! failure or `Forget`s a success; `syncFunc` reads the Secret (NotFound is
//! "has been deleted") and calls `evalSecret` for bootstrap tokens, which
//! deletes an expired one or `queue.AddAfter(key, ttl)`s a not-yet-expired one,
//! so a token is deleted when it expires.
//!
//! Deviations (storage seam instead of informer):
//! - Upstream enqueues from informer Add/Update events filtered on
//!   `Type == SecretTypeBootstrapToken && Namespace == tokenSecretNamespace`
//!   (`enqueueSecrets`, no resync by default). The `Storage` trait has no event
//!   hook, so [`TokenCleaner::enqueue_secrets`] lists the namespace's secrets
//!   and applies the same filter every [`WORKER_PERIOD`]; the queue collapses
//!   repeats.
//! - The delete's `Preconditions{UID}` is checked by re-reading the Secret
//!   just before deleting (no preconditions in `Storage`); a UID mismatch is
//!   the swallowed Conflict.

use anyhow::Result;
use chrono::{DateTime, Utc};
use k3rs_common::resources::Secret;
use k3rs_storage::{build_key, build_prefix, Storage, WorkQueue};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use super::bootstrap_secret_util::get_expiration;
pub use super::bootstrap_secret_util::{
    BOOTSTRAP_TOKEN_EXPIRATION_KEY, SECRET_TYPE_BOOTSTRAP_TOKEN,
};
/// `api.NamespaceSystem`, `DefaultTokenCleanerOptions().TokenSecretNamespace`.
pub const DEFAULT_TOKEN_SECRET_NAMESPACE: &str = "kube-system";
/// The worker period of `Run` (`wait.UntilWithContext(ctx, tc.worker, 10*time.Second)`).
pub const WORKER_PERIOD: Duration = Duration::from_secs(10);

/// `queue`'s name (tokencleaner.go, `NewTokenCleaner`).
pub const QUEUE_NAME: &str = "token_cleaner";

/// `cache.MetaNamespaceKeyFunc`: `<namespace>/<name>`.
fn queue_key(secret: &Secret) -> String {
    format!(
        "{}/{}",
        secret.metadata.namespace.as_deref().unwrap_or(""),
        secret.metadata.name
    )
}

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

impl<S: Storage + 'static> TokenCleaner<S> {
    /// `NewTokenCleaner` with `DefaultTokenCleanerOptions()`.
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            token_secret_namespace: DEFAULT_TOKEN_SECRET_NAMESPACE.to_string(),
        }
    }

    /// `Run`: enqueue (the informer events) and service the queue until it
    /// shuts down.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting token cleaner controller");
        let queue = WorkQueue::named(QUEUE_NAME);

        let poker = Arc::clone(&self);
        let poke_queue = queue.clone();
        tokio::spawn(async move {
            loop {
                poker.enqueue_secrets(&poke_queue).await;
                tokio::time::sleep(WORKER_PERIOD).await;
            }
        });

        // `wait.UntilWithContext(ctx, tc.worker, 10*time.Second)`: `worker`
        // loops `processNextWorkItem` until the queue quits.
        while self.process_next_work_item(&queue).await {}
        info!("Shutting down token cleaner controller");
        Ok(())
    }

    /// The informer's FilterFunc (type + namespace) then `enqueueSecrets`
    /// (`tc.queue.Add(key)`) for each bootstrap token Secret.
    pub async fn enqueue_secrets(&self, queue: &WorkQueue) {
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
                queue.add(queue_key(&secret)).await;
            }
        }
    }

    /// `processNextWorkItem`: `false` once the queue quits.
    pub async fn process_next_work_item(&self, queue: &WorkQueue) -> bool {
        let Some(key) = queue.get().await else {
            return false;
        };
        match self.sync_func(queue, &key, Utc::now()).await {
            Err(e) => {
                queue.requeue_rate_limited(key.clone()).await;
                error!("Sync {key} failed with : {e}");
            }
            Ok(()) => queue.forget(&key).await,
        }
        queue.done(&key).await;
        true
    }

    /// `syncFunc`.
    pub async fn sync_func(
        &self,
        queue: &WorkQueue,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<(), k3rs_common::Error> {
        // `cache.SplitMetaNamespaceKey`.
        let Some((namespace, name)) = key.split_once('/') else {
            return Err(k3rs_common::Error::BadRequest(format!(
                "unexpected key format: {key:?}"
            )));
        };
        let secret: Secret = match self
            .storage
            .get(&build_key("secrets", Some(namespace), name))
            .await
        {
            Ok(s) => s,
            Err(k3rs_common::Error::NotFound(_)) => {
                debug!("Secret {key} has been deleted");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if secret.secret_type.as_deref() == Some(SECRET_TYPE_BOOTSTRAP_TOKEN) {
            self.eval_secret(queue, &secret, now).await;
        }
        Ok(())
    }

    /// `evalSecret`: delete an expired token, or `queue.AddAfter(key, ttl)` a
    /// token that expires later.
    pub async fn eval_secret(
        &self,
        queue: &WorkQueue,
        secret: &Secret,
        now: DateTime<Utc>,
    ) -> EvalOutcome {
        let (ttl, already_expired) = get_expiration(secret, now);
        if already_expired {
            self.delete_with_uid_precondition(secret).await;
            EvalOutcome::Deleted
        } else if !ttl.is_zero() {
            queue.add_after(queue_key(secret), ttl).await;
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
                Err(k3rs_common::Error::NotFound(_)) => return,
                Err(e) => {
                    info!("Error deleting secret: {e}");
                    return;
                }
            }
        }
        match self.storage.delete(&key).await {
            Ok(()) | Err(k3rs_common::Error::NotFound(_)) => {}
            Err(e) => info!("Error deleting secret: {e}"),
        }
    }
}
