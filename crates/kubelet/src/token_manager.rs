//! Port of `pkg/kubelet/token/token_manager.go`: the kubelet's cache of
//! service account tokens for pods. A projected `serviceAccountToken` source
//! asks it for a token on every (re)SetUp; it answers from the cache until the
//! token is within 20% of its lifetime (plus jitter) of expiry, or older than
//! 24h, then refreshes through the TokenRequest API. `DeleteServiceAccountToken`
//! evicts a pod's tokens when its volumes are torn down.
//!
//! Deviations, none observable through the API:
//! - `NewManager(c clientset.Interface)` builds `getToken` around a clientset;
//!   here the caller supplies the `getToken` closure ([`GetTokenFn`]) because
//!   the token source is `StorageBackend::create_sa_token` (with the
//!   storage-direct self-mint fallback), not a clientset.
//! - `go wait.Forever(m.cleanup, gcPeriod)` is a task that holds only a `Weak`
//!   reference, so it ends with the manager; it is not started outside a tokio
//!   runtime.
//! - `authenticationv1.TokenRequest` is reduced to the fields the manager
//!   reads: `UID`, `Spec.{Audiences,ExpirationSeconds,BoundObjectRef}` and
//!   `Status.{Token,ExpirationTimestamp}`.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};
use tracing::error;

/// `maxTTL` (`token_manager.go:40`).
pub const MAX_TTL: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
/// `gcPeriod` (`:41`).
pub const GC_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);
/// `maxJitter` (`:42`), in seconds.
pub const MAX_JITTER_SECS: f64 = 10.0;

/// `authenticationv1.BoundObjectReference`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BoundObjectReference {
    pub kind: String,
    pub api_version: String,
    pub name: String,
    pub uid: String,
}

/// `authenticationv1.TokenRequestSpec`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenRequestSpec {
    pub audiences: Vec<String>,
    pub expiration_seconds: Option<i64>,
    pub bound_object_ref: Option<BoundObjectReference>,
}

/// `authenticationv1.TokenRequestStatus`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenRequestStatus {
    pub token: String,
    /// Defaults to the Unix epoch (Go's zero `metav1.Time`): long expired.
    pub expiration_timestamp: DateTime<Utc>,
}

/// `authenticationv1.TokenRequest`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenRequest {
    /// `ObjectMeta.UID`: when set, a precondition on the service account's UID.
    pub uid: String,
    pub spec: TokenRequestSpec,
    pub status: TokenRequestStatus,
}

/// `clock.Clock`, reduced to `Now`.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// `clock.RealClock{}`.
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// `Manager.getToken`: `(name, namespace, tr) -> TokenRequest`.
pub type GetTokenFn = Arc<
    dyn Fn(String, String, TokenRequest) -> BoxFuture<'static, Result<TokenRequest>> + Send + Sync,
>;

/// Manages service account tokens for pods (`Manager`, `token_manager.go:75-83`).
pub struct Manager {
    /// `cacheMutex` + `cache`.
    cache: RwLock<HashMap<String, TokenRequest>>,
    /// Mocked for testing upstream; also replaceable here for the same reason.
    get_token: RwLock<GetTokenFn>,
    clock: Arc<dyn Clock>,
}

impl Manager {
    /// `NewManager` (`token_manager.go:44-73`) over `get_token`, with the real
    /// clock and the `cleanup` loop started when a tokio runtime is present.
    pub fn new(get_token: GetTokenFn) -> Arc<Self> {
        let m = Self::new_with_clock(get_token, Arc::new(RealClock));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak: Weak<Manager> = Arc::downgrade(&m);
            handle.spawn(async move {
                loop {
                    tokio::time::sleep(GC_PERIOD).await;
                    match weak.upgrade() {
                        Some(m) => m.cleanup(),
                        None => return,
                    }
                }
            });
        }
        m
    }

    /// `NewManager` with `mgr.clock` replaced, as the upstream tests do.
    pub fn new_with_clock(get_token: GetTokenFn, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            cache: RwLock::new(HashMap::new()),
            get_token: RwLock::new(get_token),
            clock,
        })
    }

    /// Replace `getToken` (`s.mgr.getToken = tg.getToken` in the upstream tests).
    pub fn set_get_token(&self, get_token: GetTokenFn) {
        *self.get_token.write().unwrap() = get_token;
    }

    /// `GetServiceAccountToken` (`token_manager.go:98-130`): the token for a
    /// pod from the cache or from the TokenRequest API.
    ///
    /// - Check the cache for the current token request.
    /// - If the token exists and does not require a refresh, return it.
    /// - Attempt to refresh the token.
    /// - If refreshed, save it in the cache and return it.
    /// - If refresh fails and the old token is still valid, log an error and
    ///   return the old token.
    /// - If refresh fails and the old token is no longer valid, return an error.
    pub async fn get_service_account_token(
        &self,
        namespace: &str,
        name: &str,
        tr: TokenRequest,
    ) -> Result<TokenRequest> {
        let key = key_func(name, namespace, &tr);

        let ctr = self.get(&key);
        if let Some(ctr) = &ctr {
            if !self.requires_refresh(ctr) {
                return Ok(ctr.clone());
            }
        }

        let get_token = self.get_token.read().unwrap().clone();
        match get_token(name.to_string(), namespace.to_string(), tr).await {
            Err(err) => match ctr {
                None => Err(anyhow!("failed to fetch token: {err}")),
                Some(ctr) if self.expired(&ctr) => {
                    Err(anyhow!("token {key} expired and refresh failed: {err}"))
                }
                Some(ctr) => {
                    error!(cache_key = %key, "Couldn't update token: {err}");
                    Ok(ctr)
                }
            },
            Ok(tr) => {
                self.set(&key, tr.clone());
                Ok(tr)
            }
        }
    }

    /// `DeleteServiceAccountToken` (`:132-141`): invoked when a pod's volumes
    /// are torn down; cleans the pod's entries from the cache.
    pub fn delete_service_account_token(&self, pod_uid: &str) {
        self.cache.write().unwrap().retain(|_, tr| {
            tr.spec
                .bound_object_ref
                .as_ref()
                .is_none_or(|r| r.uid != pod_uid)
        });
    }

    /// `cleanup` (`:143-151`): drop expired entries.
    pub fn cleanup(&self) {
        self.cache
            .write()
            .unwrap()
            .retain(|_, tr| !self.expired(tr));
    }

    /// `get` (`:153-158`).
    pub fn get(&self, key: &str) -> Option<TokenRequest> {
        self.cache.read().unwrap().get(key).cloned()
    }

    /// `set` (`:160-164`).
    pub fn set(&self, key: &str, tr: TokenRequest) {
        self.cache.write().unwrap().insert(key.to_string(), tr);
    }

    /// `len(m.cache)`.
    pub fn cache_len(&self) -> usize {
        self.cache.read().unwrap().len()
    }

    /// `expired` (`:166-168`).
    pub fn expired(&self, t: &TokenRequest) -> bool {
        self.clock.now() > t.status.expiration_timestamp
    }

    /// `requiresRefresh` (`:170-196`): true if the token is older than 80% of
    /// its total ttl, or older than 24 hours.
    pub fn requires_refresh(&self, tr: &TokenRequest) -> bool {
        let Some(expiration_seconds) = tr.spec.expiration_seconds else {
            error!("Expiration seconds was nil for token request");
            return false;
        };
        let now = self.clock.now();
        let exp = tr.status.expiration_timestamp;
        let iat = exp - Duration::seconds(expiration_seconds);

        // `time.Duration(rand.Float64()*maxJitter.Seconds()) * time.Second`
        let jitter = Duration::seconds((rand::random::<f64>() * MAX_JITTER_SECS) as i64);
        if now > iat + (Duration::seconds(MAX_TTL.as_secs() as i64) - jitter) {
            return true;
        }
        // Require a refresh if within 20% of the TTL plus a jitter from the
        // expiration time.
        now > exp - Duration::seconds((expiration_seconds * 20) / 100) - jitter
    }
}

/// `keyFunc` (`:198-223`): keys should be nonconfidential and safe to log.
/// A UID set on the token request is a precondition that the request is for
/// the same service account, so a service account recreated under the same
/// name never gets a stale token.
pub fn key_func(name: &str, namespace: &str, tr: &TokenRequest) -> String {
    let exp = tr.spec.expiration_seconds.unwrap_or(0);
    let r = tr.spec.bound_object_ref.clone().unwrap_or_default();
    format!(
        "{name:?}/{namespace:?}/{:?}/{exp}/BoundObjectReference{{Kind:{:?}, APIVersion:{:?}, Name:{:?}, UID:{:?}}}/{:?}",
        tr.spec.audiences, r.kind, r.api_version, r.name, r.uid, tr.uid
    )
}

/// The `getToken` of this kubelet (Rusternetes-specific; upstream's is a
/// clientset `CreateToken`, `token_manager.go:60-69`).
///
/// A TokenRequest to the api-server through `storage` binds the token to the
/// pod in `BoundObjectRef`, so the api-server derives the pod/node claims and a
/// TokenReview reports the `authentication.kubernetes.io/{pod-name,pod-uid,
/// node-name}` extras (#1684); a vanilla api-server only trusts tokens it
/// signed itself. Only when `storage` yields none (the storage-direct
/// backends, whose co-located api-server trusts our key) is the token
/// self-minted with `signer`, with the pod's node resolved from storage.
pub fn storage_token_source(
    storage: Option<Arc<rusternetes_storage::StorageBackend>>,
    signer: rusternetes_common::auth::TokenManager,
) -> GetTokenFn {
    Arc::new(move |name, namespace, tr| {
        let storage = storage.clone();
        let signer = signer.clone();
        Box::pin(
            async move { request_token(storage.as_deref(), &signer, &name, &namespace, tr).await },
        )
    })
}

async fn request_token(
    storage: Option<&rusternetes_storage::StorageBackend>,
    signer: &rusternetes_common::auth::TokenManager,
    sa_name: &str,
    namespace: &str,
    mut tr: TokenRequest,
) -> Result<TokenRequest> {
    use rusternetes_storage::{build_key, Storage};

    let expiration_seconds = tr
        .spec
        .expiration_seconds
        .ok_or_else(|| anyhow!("TokenRequest has no expirationSeconds"))?;
    let bound = tr.spec.bound_object_ref.clone();
    let bound_pod = bound.as_ref().map(|r| (r.name.as_str(), r.uid.as_str()));
    let now = Utc::now();

    if let Some(st) = storage {
        match st
            .create_sa_token_status(
                namespace,
                sa_name,
                &tr.spec.audiences,
                expiration_seconds,
                bound_pod,
            )
            .await
        {
            Ok(Some((token, expiry))) => {
                tr.status = TokenRequestStatus {
                    token,
                    expiration_timestamp: DateTime::parse_from_rfc3339(&expiry)
                        .map(|t| t.with_timezone(&Utc))
                        .unwrap_or_else(|_| now + Duration::seconds(expiration_seconds)),
                };
                return Ok(tr);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("TokenRequest for {namespace}/{sa_name} failed: {e}; self-minting")
            }
        }
    }

    // Self-mint fallback (storage-direct backends).
    let sa_uid = match storage {
        Some(st) => st
            .get::<rusternetes_common::resources::ServiceAccount>(&build_key(
                "serviceaccounts",
                Some(namespace),
                sa_name,
            ))
            .await
            .map(|sa| sa.metadata.uid)
            .unwrap_or_default(),
        None => String::new(),
    };
    // TokenRequest defaults an empty audience list to the api-server's
    // `--api-audiences` (`pkg/registry/core/serviceaccount/storage/token.go`
    // `TokenREST.Create`), so use those, else "rusternetes".
    let audiences = if !tr.spec.audiences.is_empty() {
        tr.spec.audiences.clone()
    } else if signer.api_audiences().is_empty() {
        vec!["rusternetes".to_string()]
    } else {
        signer.api_audiences().to_vec()
    };
    let (pod_name, pod_uid) = bound
        .as_ref()
        .map(|r| (r.name.clone(), r.uid.clone()))
        .unwrap_or_default();
    let node_name = match (storage, bound.is_some()) {
        (Some(st), true) => st
            .get::<rusternetes_common::resources::Pod>(&build_key(
                "pods",
                Some(namespace),
                &pod_name,
            ))
            .await
            .ok()
            .and_then(|p| p.spec.and_then(|s| s.node_name)),
        _ => None,
    };
    let node_uid = match (&node_name, storage) {
        (Some(nn), Some(st)) => st
            .get::<serde_json::Value>(&build_key("nodes", None::<&str>, nn))
            .await
            .ok()
            .and_then(|v| {
                v.pointer("/metadata/uid")
                    .and_then(|u| u.as_str())
                    .map(|s| s.to_string())
            }),
        _ => None,
    };
    let exp = now.timestamp() + expiration_seconds;
    let claims = rusternetes_common::auth::ServiceAccountClaims {
        sub: format!("system:serviceaccount:{namespace}:{sa_name}"),
        namespace: namespace.to_string(),
        uid: sa_uid.clone(),
        iat: now.timestamp(),
        exp,
        iss: "https://kubernetes.default.svc.cluster.local".to_string(),
        aud: audiences,
        kubernetes: Some(rusternetes_common::auth::KubernetesClaims {
            namespace: namespace.to_string(),
            svcacct: rusternetes_common::auth::KubeRef {
                name: sa_name.to_string(),
                uid: sa_uid,
            },
            pod: bound.as_ref().map(|_| rusternetes_common::auth::KubeRef {
                name: pod_name.clone(),
                uid: pod_uid.clone(),
            }),
            node: node_name
                .as_ref()
                .map(|nn| rusternetes_common::auth::KubeRef {
                    name: nn.clone(),
                    uid: node_uid.clone().unwrap_or_default(),
                }),
            secret: None,
            warnafter: None,
        }),
        pod_name: bound.as_ref().map(|_| pod_name.clone()),
        pod_uid: bound.as_ref().map(|_| pod_uid.clone()),
        node_name,
        node_uid,
    };
    let token = signer.generate_token(claims)?;
    tr.status = TokenRequestStatus {
        token,
        expiration_timestamp: DateTime::from_timestamp(exp, 0).unwrap_or(now),
    };
    Ok(tr)
}
