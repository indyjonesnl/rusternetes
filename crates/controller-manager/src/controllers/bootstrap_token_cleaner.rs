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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as D;
    use rusternetes_storage::memory::MemoryStorage;
    use std::collections::HashMap;

    /// `newTokenSecret` (common_test.go).
    fn new_token_secret(id: &str, secret: &str) -> Secret {
        let mut s = Secret::new(format!("bootstrap-token-{id}"), "kube-system");
        s.metadata.uid = format!("uid{id}");
        s.secret_type = Some(SECRET_TYPE_BOOTSTRAP_TOKEN.into());
        let mut d = HashMap::new();
        d.insert("token-id".to_string(), id.as_bytes().to_vec());
        d.insert("token-secret".to_string(), secret.as_bytes().to_vec());
        s.data = Some(d);
        s
    }

    /// `addSecretExpiration`.
    fn add_expiration(s: &mut Secret, exp: &str) {
        s.data
            .as_mut()
            .unwrap()
            .insert("expiration".into(), exp.as_bytes().to_vec());
    }

    /// `timeString(offset)`: RFC3339 of now+offset.
    fn time_string(now: DateTime<Utc>, off: D) -> String {
        (now + off).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }

    async fn setup(s: &Secret) -> (TokenCleaner<MemoryStorage>, Arc<MemoryStorage>, String) {
        let st = Arc::new(MemoryStorage::new());
        let key = build_key("secrets", Some("kube-system"), &s.metadata.name);
        st.create(&key, s).await.unwrap();
        (TokenCleaner::new(st.clone()), st, key)
    }

    async fn exists(st: &MemoryStorage, key: &str) -> bool {
        st.get::<Secret>(key).await.is_ok()
    }

    // TestCleanerNoExpiration
    #[tokio::test]
    async fn cleaner_no_expiration() {
        let s = new_token_secret("tokenID", "tokenSecret");
        let (c, st, key) = setup(&s).await;
        assert_eq!(c.eval_secret(&s, Utc::now()).await, EvalOutcome::Nothing);
        assert!(exists(&st, &key).await);
    }

    // TestCleanerExpired
    #[tokio::test]
    async fn cleaner_expired() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::hours(-1)));
        let (c, st, key) = setup(&s).await;
        assert_eq!(c.eval_secret(&s, now).await, EvalOutcome::Deleted);
        assert!(!exists(&st, &key).await);
    }

    // TestCleanerNotExpired
    #[tokio::test]
    async fn cleaner_not_expired() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::hours(1)));
        let (c, st, key) = setup(&s).await;
        assert!(matches!(
            c.eval_secret(&s, now).await,
            EvalOutcome::RequeueAfter(_)
        ));
        assert!(exists(&st, &key).await);
    }

    // TestCleanerExpiredAt: not deleted now, requeued, deleted once due.
    #[tokio::test]
    async fn cleaner_expired_at() {
        let now = Utc::now();
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, &time_string(now, D::seconds(2)));
        let (c, st, key) = setup(&s).await;
        let EvalOutcome::RequeueAfter(ttl) = c.eval_secret(&s, now).await else {
            panic!("expected requeue");
        };
        assert!(ttl <= Duration::from_secs(2) && ttl > Duration::ZERO);
        assert!(exists(&st, &key).await);
        let later = now + D::seconds(3);
        assert_eq!(c.eval_secret(&s, later).await, EvalOutcome::Deleted);
        assert!(!exists(&st, &key).await);
    }

    // GetExpiration: unparseable expiration is treated as expired.
    #[tokio::test]
    async fn unparseable_expiration_is_expired() {
        let mut s = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut s, "not-a-time");
        let (c, st, key) = setup(&s).await;
        assert_eq!(c.eval_secret(&s, Utc::now()).await, EvalOutcome::Deleted);
        assert!(!exists(&st, &key).await);
    }

    // Delete's UID precondition: a recreated secret with another UID survives.
    #[tokio::test]
    async fn uid_precondition_conflict_keeps_secret() {
        let now = Utc::now();
        let mut stale = new_token_secret("tokenID", "tokenSecret");
        add_expiration(&mut stale, &time_string(now, D::hours(-1)));
        let mut live = stale.clone();
        live.metadata.uid = "other-uid".into();
        let (c, st, key) = setup(&live).await;
        c.eval_secret(&stale, now).await;
        assert!(exists(&st, &key).await);
    }

    // Informer FilterFunc: only bootstrap-token secrets in kube-system.
    #[tokio::test]
    async fn sync_all_filters_type_and_namespace() {
        let now = Utc::now();
        let st = Arc::new(MemoryStorage::new());
        let exp = time_string(now, D::hours(-1));

        let mut tok = new_token_secret("aaaaaa", "x");
        add_expiration(&mut tok, &exp);
        let mut opaque = new_token_secret("bbbbbb", "x");
        add_expiration(&mut opaque, &exp);
        opaque.secret_type = Some("Opaque".into());
        let mut other_ns = new_token_secret("cccccc", "x");
        add_expiration(&mut other_ns, &exp);
        other_ns.metadata.namespace = Some("default".into());

        for s in [&tok, &opaque, &other_ns] {
            let ns = s.metadata.namespace.clone().unwrap();
            st.create(&build_key("secrets", Some(&ns), &s.metadata.name), s)
                .await
                .unwrap();
        }
        TokenCleaner::new(st.clone()).sync_all(now).await;
        let k = |ns: &str, n: &str| build_key("secrets", Some(ns), n);
        assert!(!exists(&st, &k("kube-system", &tok.metadata.name)).await);
        assert!(exists(&st, &k("kube-system", &opaque.metadata.name)).await);
        assert!(exists(&st, &k("default", &other_ns.metadata.name)).await);
    }
}
