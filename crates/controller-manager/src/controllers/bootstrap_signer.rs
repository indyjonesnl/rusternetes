//! Bootstrap signer.
//!
//! Ported from `pkg/controller/bootstrap/bootstrapsigner.go` (release-1.35)
//! and wired as `newBootstrapSignerController`
//! (`cmd/kube-controller-manager/app/bootstrap.go:30-53`): it signs the
//! `kube-public/cluster-info` ConfigMap's `kubeconfig` with a detached JWS
//! (`jws-kubeconfig-<token-id>`) per signing-enabled bootstrap token Secret
//! in `kube-system`. Upstream registers it `isDisabledByDefault: true`, so
//! here it is opt-in too (`--bootstrap-signer-controller`).
//!
//! Ported 1:1: `signConfigMap`, `getTokens`, `listSecrets`,
//! `validateSecretForSigning` (`pkg/controller/bootstrap/util.go`),
//! `jws.ComputeDetachedSignature`
//! (`staging/src/k8s.io/cluster-bootstrap/token/jws/jws.go`; HS256 compact
//! JWS with protected header `{"alg":"HS256","kid":"<id>"}`, payload part
//! stripped) and the `bootstrapsecretutil` helpers `ParseName`, `GetData`,
//! `GetExpiration` (`cluster-bootstrap/util/secrets/secrets.go`). Like
//! upstream, a signing failure is logged and the empty signature is still
//! stored.
//!
//! Deviations (storage seam instead of informers + workqueue):
//! - Upstream pokes a one-item workqueue from ConfigMap/Secret informer
//!   events (no resync by default). The `Storage` trait has no event hook
//!   here, so `run` re-evaluates every [`SYNC_PERIOD`]; `sign_config_map` is
//!   idempotent and only writes on change, as upstream.
//! - Upstream's `Update` carries the resourceVersion read from the lister;
//!   the `Storage` seam updates by key. NotFound is swallowed like upstream.
//! - Upstream's informer filters secrets by type; here the `kube-system`
//!   secrets are listed and filtered by type.

use anyhow::Result;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rusternetes_common::resources::{ConfigMap, Secret};
use rusternetes_storage::{build_key, build_prefix, Storage};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// `bootstrapapi.SecretTypeBootstrapToken` (token/api/types.go).
pub const SECRET_TYPE_BOOTSTRAP_TOKEN: &str = "bootstrap.kubernetes.io/token";
/// `bootstrapapi.BootstrapTokenSecretPrefix`.
pub const BOOTSTRAP_TOKEN_SECRET_PREFIX: &str = "bootstrap-token-";
/// `bootstrapapi.BootstrapTokenIDKey`.
pub const BOOTSTRAP_TOKEN_ID_KEY: &str = "token-id";
/// `bootstrapapi.BootstrapTokenSecretKey`.
pub const BOOTSTRAP_TOKEN_SECRET_KEY: &str = "token-secret";
/// `bootstrapapi.BootstrapTokenExpirationKey`.
pub const BOOTSTRAP_TOKEN_EXPIRATION_KEY: &str = "expiration";
/// `bootstrapapi.BootstrapTokenUsageSigningKey`.
pub const BOOTSTRAP_TOKEN_USAGE_SIGNING_KEY: &str = "usage-bootstrap-signing";
/// `bootstrapapi.ConfigMapClusterInfo`.
pub const CONFIG_MAP_CLUSTER_INFO: &str = "cluster-info";
/// `bootstrapapi.KubeConfigKey`.
pub const KUBECONFIG_KEY: &str = "kubeconfig";
/// `bootstrapapi.JWSSignatureKeyPrefix`.
pub const JWS_SIGNATURE_KEY_PREFIX: &str = "jws-kubeconfig-";
/// `api.NamespacePublic`, `DefaultSignerOptions().ConfigMapNamespace`.
pub const NAMESPACE_PUBLIC: &str = "kube-public";
/// `api.NamespaceSystem`, `DefaultSignerOptions().TokenSecretNamespace`.
pub const NAMESPACE_SYSTEM: &str = "kube-system";
/// Re-evaluation period standing in for the informer event pokes.
pub const SYNC_PERIOD: Duration = Duration::from_secs(10);

/// `SignerOptions` (`DefaultSignerOptions()` values by default).
#[derive(Debug, Clone)]
pub struct SignerOptions {
    pub config_map_namespace: String,
    pub config_map_name: String,
    pub token_secret_namespace: String,
}

impl Default for SignerOptions {
    /// `DefaultSignerOptions`.
    fn default() -> Self {
        Self {
            config_map_namespace: NAMESPACE_PUBLIC.into(),
            config_map_name: CONFIG_MAP_CLUSTER_INFO.into(),
            token_secret_namespace: NAMESPACE_SYSTEM.into(),
        }
    }
}

/// `jws.ComputeDetachedSignature`: HS256 compact JWS of `content`, keyed by
/// `token_secret`, header `{"alg":"HS256","kid":token_id}`, with the payload
/// segment removed (`stripContent`: `parts[0] + ".." + parts[2]`).
pub fn compute_detached_signature(
    content: &str,
    token_id: &str,
    token_secret: &str,
) -> Result<String, String> {
    let header = format!(
        "{{\"alg\":\"HS256\",\"kid\":{}}}",
        serde_json::to_string(token_id).map_err(|e| e.to_string())?
    );
    let protected = URL_SAFE_NO_PAD.encode(header);
    let payload = URL_SAFE_NO_PAD.encode(content);
    let mut mac = Hmac::<Sha256>::new_from_slice(token_secret.as_bytes())
        .map_err(|e| format!("can't make a HS256 signer from the given token: {e}"))?;
    mac.update(format!("{protected}.{payload}").as_bytes());
    let sig = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{protected}..{sig}"))
}

/// `bootstrapsecretutil.ParseName`: `^bootstrap-token-([a-z0-9]{6})$`.
fn parse_name(name: &str) -> Option<&str> {
    let id = name.strip_prefix(BOOTSTRAP_TOKEN_SECRET_PREFIX)?;
    (id.len() == 6
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()))
    .then_some(id)
}

/// `bootstrapsecretutil.GetData`: empty string when absent.
fn get_data(secret: &Secret, key: &str) -> String {
    secret
        .data
        .as_ref()
        .and_then(|d| d.get(key))
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .unwrap_or_default()
}

/// `bootstrapsecretutil.HasExpired` (`GetExpiration`'s `isExpired`): no
/// expiration is not expired, an unparseable one is expired.
fn has_expired(secret: &Secret, now: DateTime<Utc>) -> bool {
    let expiration = get_data(secret, BOOTSTRAP_TOKEN_EXPIRATION_KEY);
    if expiration.is_empty() {
        return false;
    }
    match DateTime::parse_from_rfc3339(&expiration) {
        Err(_) => true,
        Ok(t) => t.with_timezone(&Utc) - now <= chrono::Duration::zero(),
    }
}

/// `validateSecretForSigning` (util.go): `(tokenID, tokenSecret)` when the
/// secret may be used for signing.
fn validate_secret_for_signing(secret: &Secret, now: DateTime<Utc>) -> Option<(String, String)> {
    let name_token_id = parse_name(&secret.metadata.name)?;
    let token_id = get_data(secret, BOOTSTRAP_TOKEN_ID_KEY);
    if token_id.is_empty() {
        return None;
    }
    if name_token_id != token_id {
        return None;
    }
    let token_secret = get_data(secret, BOOTSTRAP_TOKEN_SECRET_KEY);
    if token_secret.is_empty() {
        return None;
    }
    // The TokenCleaner should remove expired secrets, but check here too.
    if has_expired(secret, now) {
        return None;
    }
    if get_data(secret, BOOTSTRAP_TOKEN_USAGE_SIGNING_KEY) != "true" {
        return None;
    }
    Some((token_id, token_secret))
}

pub struct Signer<S: Storage> {
    storage: Arc<S>,
    options: SignerOptions,
}

impl<S: Storage + 'static> Signer<S> {
    /// `NewSigner` with `DefaultSignerOptions()`.
    pub fn new(storage: Arc<S>) -> Self {
        Self::with_options(storage, SignerOptions::default())
    }

    pub fn with_options(storage: Arc<S>, options: SignerOptions) -> Self {
        Self { storage, options }
    }

    /// `Run`: re-sign every [`SYNC_PERIOD`].
    pub async fn run(self: Arc<Self>) -> Result<()> {
        info!("Starting bootstrap signer controller");
        loop {
            self.sign_config_map(Utc::now()).await;
            tokio::time::sleep(SYNC_PERIOD).await;
        }
    }

    /// `getConfigMap`: `None` when absent (the resync catches up later).
    async fn get_config_map(&self) -> Option<ConfigMap> {
        let key = build_key(
            "configmaps",
            Some(&self.options.config_map_namespace),
            &self.options.config_map_name,
        );
        match self.storage.get::<ConfigMap>(&key).await {
            Ok(cm) => Some(cm),
            Err(rusternetes_common::Error::NotFound(_)) => None,
            Err(e) => {
                error!("Reading bootstrap signer ConfigMap: {e}");
                None
            }
        }
    }

    /// `listSecrets` + `getTokens`: tokenID -> tokenSecret for every
    /// signing-enabled, unexpired bootstrap token Secret.
    async fn get_tokens(&self, now: DateTime<Utc>) -> HashMap<String, String> {
        let mut ret = HashMap::new();
        let secrets: Vec<Secret> = match self
            .storage
            .list(&build_prefix(
                "secrets",
                Some(&self.options.token_secret_namespace),
            ))
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!("Listing bootstrap token secrets: {e}");
                return ret;
            }
        };
        for secret in secrets {
            if secret.secret_type.as_deref() != Some(SECRET_TYPE_BOOTSTRAP_TOKEN)
                || secret.metadata.namespace.as_deref()
                    != Some(&self.options.token_secret_namespace)
            {
                continue;
            }
            let Some((token_id, token_secret)) = validate_secret_for_signing(&secret, now) else {
                continue;
            };
            // Duplicates: behaviour undefined upstream; keep the first.
            if ret.contains_key(&token_id) {
                debug!(
                    "Duplicate bootstrap tokens found for id {token_id}, ignoring secret {}",
                    secret.metadata.name
                );
                continue;
            }
            ret.insert(token_id, token_secret);
        }
        ret
    }

    /// `signConfigMap`. Returns whether the ConfigMap was updated.
    pub async fn sign_config_map(&self, now: DateTime<Utc>) -> bool {
        let Some(orig) = self.get_config_map().await else {
            return false;
        };
        let mut need_update = false;
        let mut new_cm = orig.clone();
        let mut data = new_cm.data.take().unwrap_or_default();

        // Capture the config we are signing.
        let Some(content) = data.get(KUBECONFIG_KEY).cloned() else {
            debug!(
                "No {KUBECONFIG_KEY} key in ConfigMap {}",
                orig.metadata.name
            );
            return false;
        };

        // Remove and save all existing signatures.
        let mut sigs: HashMap<String, String> = HashMap::new();
        let sig_keys: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(JWS_SIGNATURE_KEY_PREFIX))
            .cloned()
            .collect();
        for key in sig_keys {
            let value = data.remove(&key).unwrap_or_default();
            sigs.insert(key[JWS_SIGNATURE_KEY_PREFIX.len()..].to_string(), value);
        }

        // Recompute signatures and store them on the new map.
        for (token_id, token_value) in self.get_tokens(now).await {
            let sig =
                compute_detached_signature(&content, &token_id, &token_value).unwrap_or_else(|e| {
                    // HandleError; the (empty) signature is stored regardless.
                    error!("{e}");
                    String::new()
                });
            if sigs.get(&token_id).map(String::as_str).unwrap_or("") != sig {
                need_update = true;
            }
            sigs.remove(&token_id);
            data.insert(format!("{JWS_SIGNATURE_KEY_PREFIX}{token_id}"), sig);
        }

        // Leftover signatures were removed: update.
        if !sigs.is_empty() {
            need_update = true;
        }

        new_cm.data = Some(data);
        if need_update {
            self.update_config_map(&new_cm).await;
        }
        need_update
    }

    /// `updateConfigMap`: NotFound/Conflict are not logged.
    async fn update_config_map(&self, cm: &ConfigMap) {
        let key = build_key(
            "configmaps",
            cm.metadata.namespace.as_deref(),
            &cm.metadata.name,
        );
        match self.storage.update(&key, cm).await {
            Ok(_) | Err(rusternetes_common::Error::NotFound(_)) => {}
            Err(e) => debug!("Error updating ConfigMap: {e}"),
        }
    }
}
