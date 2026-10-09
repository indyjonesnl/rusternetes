use anyhow::Result;
use futures::StreamExt;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rusternetes_common::resources::{Namespace, Secret, ServiceAccount};
use rusternetes_common::types::{ObjectMeta, TypeMeta};
use rusternetes_storage::{build_key, build_prefix, extract_key, Storage, WorkQueue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

const SECRET_TYPE_SERVICE_ACCOUNT_TOKEN: &str = "kubernetes.io/service-account-token";
const SA_NAME_ANNOTATION: &str = "kubernetes.io/service-account.name";
const SA_UID_ANNOTATION: &str = "kubernetes.io/service-account.uid";

/// Upstream `apiserverserviceaccount.IsServiceAccountToken`
/// (staging/src/k8s.io/apiserver/pkg/authentication/serviceaccount/util.go:167):
/// the type must be `kubernetes.io/service-account-token`, the name annotation
/// must match, and the uid annotation must match when it is present.
fn is_service_account_token(secret: &Secret, sa_name: &str, sa_uid: Option<&str>) -> bool {
    if secret.secret_type.as_deref() != Some(SECRET_TYPE_SERVICE_ACCOUNT_TOKEN) {
        return false;
    }
    let ann = secret.metadata.annotations.as_ref();
    if ann
        .and_then(|a| a.get(SA_NAME_ANNOTATION))
        .map(String::as_str)
        != Some(sa_name)
    {
        return false;
    }
    match (ann.and_then(|a| a.get(SA_UID_ANNOTATION)), sa_uid) {
        (Some(want), Some(have)) if !want.is_empty() => want == have,
        _ => true,
    }
}

/// JWT Claims for ServiceAccount tokens
/// Follows Kubernetes ServiceAccount token format
#[derive(Debug, Serialize, Deserialize)]
struct ServiceAccountClaims {
    /// Issuer - typically the API server URL
    iss: String,
    /// Subject - the ServiceAccount in format "system:serviceaccount:<namespace>:<name>"
    sub: String,
    /// Audience - who the token is intended for
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<Vec<String>>,
    /// Expiration time (Unix timestamp)
    exp: i64,
    /// Issued at time (Unix timestamp)
    iat: i64,
    /// Not before time (Unix timestamp)
    #[serde(skip_serializing_if = "Option::is_none")]
    nbf: Option<i64>,
    /// Kubernetes-specific claims
    #[serde(rename = "kubernetes.io")]
    kubernetes: KubernetesClaims,
}

/// Kubernetes-specific claims in the JWT
#[derive(Debug, Serialize, Deserialize)]
struct KubernetesClaims {
    namespace: String,
    serviceaccount: ServiceAccountRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pod: Option<PodRef>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServiceAccountRef {
    name: String,
    uid: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PodRef {
    name: String,
    uid: String,
}

/// ServiceAccountController: the union of upstream's two ServiceAccount controllers.
///
/// 1. Creates the "default" ServiceAccount in each namespace — upstream
///    `pkg/controller/serviceaccount/serviceaccounts_controller.go`
///    (`ServiceAccountsController`). It creates ONLY the ServiceAccount; no
///    token Secret (`LegacyServiceAccountTokenNoAutoGeneration`, GA in 1.26).
/// 2. Populates token / ca.crt / namespace on user-created
///    `kubernetes.io/service-account-token` Secrets and deletes such Secrets
///    whose ServiceAccount is gone — upstream
///    `pkg/controller/serviceaccount/tokens_controller.go` (`TokensController`,
///    `syncSecret` / `syncServiceAccount` / `generateTokenIfNeeded`). It never
///    creates a Secret.
pub struct ServiceAccountController<S: Storage> {
    storage: Arc<S>,
    /// RSA private key for signing tokens (PEM format)
    /// In production, this would be loaded from a secure key file
    signing_key: Option<EncodingKey>,
    /// PEM-encoded CA certificate injected into SA token secrets
    ca_cert_pem: Option<String>,
}

impl<S: Storage + 'static> ServiceAccountController<S> {
    pub fn new(storage: Arc<S>) -> Self {
        // Try to load the signing key from environment or default location
        let signing_key = Self::load_signing_key();

        if signing_key.is_none() {
            warn!("ServiceAccount signing key not found - tokens will be unsigned. Set SA_SIGNING_KEY_PATH environment variable to enable JWT signing.");
        }

        Self {
            storage,
            signing_key,
            ca_cert_pem: None,
        }
    }

    pub fn with_ca_cert(mut self, ca_cert_pem: Option<String>) -> Self {
        self.ca_cert_pem = ca_cert_pem;
        self
    }

    /// Load the RSA private key for signing ServiceAccount tokens
    /// Looks for the key at SA_SIGNING_KEY_PATH environment variable
    /// or defaults to ~/.rusternetes/keys/sa-signing-key.pem
    fn load_signing_key() -> Option<EncodingKey> {
        let key_path = std::env::var("SA_SIGNING_KEY_PATH").unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
            format!("{}/.rusternetes/keys/sa-signing-key.pem", home)
        });

        match std::fs::read(&key_path) {
            Ok(key_bytes) => match EncodingKey::from_rsa_pem(&key_bytes) {
                Ok(key) => {
                    info!("Loaded ServiceAccount signing key from {}", key_path);
                    Some(key)
                }
                Err(e) => {
                    error!(
                        "Failed to parse ServiceAccount signing key from {}: {}",
                        key_path, e
                    );
                    None
                }
            },
            Err(e) => {
                debug!(
                    "Failed to read ServiceAccount signing key from {}: {}",
                    key_path, e
                );
                None
            }
        }
    }

    /// Watch-based run loop. Watches for serviceaccount changes and
    /// periodically resyncs every 30s.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let queue = WorkQueue::new();

        let worker_queue = queue.clone();
        let worker_self = Arc::clone(&self);
        tokio::spawn(async move {
            worker_self.worker(worker_queue).await;
        });

        loop {
            self.enqueue_all(&queue).await;

            let prefix = build_prefix("serviceaccounts", None);
            let watch_result = self.storage.watch(&prefix).await;
            let mut watch = match watch_result {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Token Secrets are populated / reaped on Secret events too
            // (TokensController's secret informer, tokens_controller.go:108-131).
            let secret_prefix = build_prefix("secrets", None);
            let mut secret_watch = match self.storage.watch(&secret_prefix).await {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to establish secret watch: {}, retrying", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            let mut resync = tokio::time::interval(std::time::Duration::from_secs(30));
            resync.tick().await;

            let mut watch_broken = false;
            while !watch_broken {
                tokio::select! {
                    event = watch.next() => {
                        match event {
                            Some(Ok(ev)) => {
                                let key = extract_key(&ev);
                                queue.add(key).await;
                            }
                            Some(Err(e)) => {
                                tracing::warn!("Watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    event = secret_watch.next() => {
                        match event {
                            Some(Ok(ev)) => queue.add(extract_key(&ev)).await,
                            Some(Err(e)) => {
                                tracing::warn!("Secret watch error: {}, reconnecting", e);
                                watch_broken = true;
                            }
                            None => {
                                tracing::warn!("Secret watch stream ended, reconnecting");
                                watch_broken = true;
                            }
                        }
                    }
                    _ = resync.tick() => {
                        self.enqueue_all(&queue).await;
                    }
                }
            }
        }
    }

    /// Main reconciliation loop - ensures all namespaces have default ServiceAccounts
    async fn worker(&self, queue: WorkQueue) {
        while let Some(key) = queue.get().await {
            let parts: Vec<&str> = key.splitn(3, '/').collect();
            let (ns, name) = match parts.len() {
                3 => (parts[1], parts[2]),
                _ => {
                    queue.done(&key).await;
                    continue;
                }
            };
            // Skip namespaces that are being deleted — don't create SAs
            // in terminating namespaces (fights with namespace controller)
            let ns_key = build_key("namespaces", None, ns);
            if let Ok(namespace) = self.storage.get::<Namespace>(&ns_key).await {
                if namespace.metadata.deletion_timestamp.is_some() {
                    queue.forget(&key).await;
                    queue.done(&key).await;
                    continue;
                }
            }
            if parts[0] == "secrets" {
                match self.sync_token_secret(ns, name).await {
                    Ok(()) => queue.forget(&key).await,
                    Err(e) => {
                        tracing::error!("Failed to sync token secret {}: {}", key, e);
                        queue.requeue_rate_limited(key.clone()).await;
                    }
                }
                queue.done(&key).await;
                continue;
            }
            // Ensure the default service account exists in this namespace
            if let Err(e) = self.ensure_default_serviceaccount(ns).await {
                tracing::error!("Failed to ensure default SA in {}: {}", ns, e);
            }
            // Reconcile the specific service account
            match self.reconcile_serviceaccount(ns, name).await {
                Ok(()) => queue.forget(&key).await,
                Err(e) => {
                    tracing::error!("Failed to reconcile {}: {}", key, e);
                    queue.requeue_rate_limited(key.clone()).await;
                }
            }
            queue.done(&key).await;
        }
    }

    async fn enqueue_all(&self, queue: &WorkQueue) {
        // Enqueue all existing service accounts
        match self
            .storage
            .list::<ServiceAccount>("/registry/serviceaccounts/")
            .await
        {
            Ok(items) => {
                for item in &items {
                    let ns = item.metadata.namespace.as_deref().unwrap_or("");
                    let key = format!("serviceaccounts/{}/{}", ns, item.metadata.name);
                    queue.add(key).await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to list serviceaccounts for enqueue: {}", e);
            }
        }
        // Re-sync every service-account-token Secret
        match self.storage.list::<Secret>("/registry/secrets/").await {
            Ok(secrets) => {
                for secret in secrets
                    .iter()
                    .filter(|s| s.secret_type.as_deref() == Some(SECRET_TYPE_SERVICE_ACCOUNT_TOKEN))
                {
                    let ns = secret.metadata.namespace.as_deref().unwrap_or("");
                    queue
                        .add(format!("secrets/{}/{}", ns, secret.metadata.name))
                        .await;
                }
            }
            Err(e) => {
                tracing::error!("Failed to list secrets for token sync: {}", e);
            }
        }
        // Also ensure default SA in all namespaces
        match self
            .storage
            .list::<Namespace>("/registry/namespaces/")
            .await
        {
            Ok(namespaces) => {
                for ns in &namespaces {
                    if ns.metadata.deletion_timestamp.is_none() {
                        let key = format!("serviceaccounts/{}/default", ns.metadata.name);
                        queue.add(key).await;
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to list namespaces for SA enqueue: {}", e);
            }
        }
    }

    #[allow(dead_code)]
    pub async fn reconcile_all(&self) -> Result<()> {
        debug!("Starting service account reconciliation");

        // List all namespaces
        let namespaces: Vec<Namespace> = self.storage.list("/registry/namespaces/").await?;

        for namespace in namespaces {
            let ns_name = &namespace.metadata.name;

            // Skip namespaces being deleted
            if namespace.metadata.deletion_timestamp.is_some() {
                continue;
            }

            if let Err(e) = self.ensure_default_serviceaccount(ns_name).await {
                error!(
                    "Failed to ensure default ServiceAccount in namespace {}: {}",
                    ns_name, e
                );
            }
        }

        Ok(())
    }

    /// Ensure the "default" ServiceAccount exists in a namespace
    async fn ensure_default_serviceaccount(&self, namespace: &str) -> Result<()> {
        let sa_name = "default";
        let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);

        // Check if default ServiceAccount already exists
        match self.storage.get::<ServiceAccount>(&sa_key).await {
            Ok(_) => {
                debug!(
                    "Default ServiceAccount already exists in namespace {}",
                    namespace
                );
                return Ok(());
            }
            Err(rusternetes_common::Error::NotFound(_)) => {
                // ServiceAccount doesn't exist, create it
            }
            Err(e) => return Err(e.into()),
        }

        info!("Creating default ServiceAccount in namespace {}", namespace);

        // Create the default ServiceAccount
        let service_account = ServiceAccount {
            type_meta: TypeMeta {
                kind: "ServiceAccount".to_string(),
                api_version: "v1".to_string(),
            },
            metadata: ObjectMeta {
                name: sa_name.to_string(),
                generate_name: None,
                generation: None,
                managed_fields: None,
                namespace: Some(namespace.to_string()),
                uid: String::new(),
                resource_version: None,
                deletion_grace_period_seconds: None,
                finalizers: None,
                owner_references: None,
                creation_timestamp: None,
                deletion_timestamp: None,
                labels: None,
                annotations: None,
            },
            secrets: None,
            image_pull_secrets: None,
            automount_service_account_token: Some(true),
        };

        match self.storage.create(&sa_key, &service_account).await {
            Ok(_) => {}
            Err(rusternetes_common::Error::AlreadyExists(_)) => {
                // Another reconciliation created it — this is fine
                debug!(
                    "Default ServiceAccount already exists in namespace {}",
                    namespace
                );
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        info!("Created default ServiceAccount in namespace {}", namespace);
        Ok(())
    }

    /// Generate a ServiceAccount token as a signed JWT
    /// Uses RS256 (RSA + SHA256) for signing if a signing key is available
    /// Falls back to a simple token format if no signing key is configured
    fn generate_token(
        &self,
        namespace: &str,
        sa_name: &str,
        sa_uid: &str,
        _secret_name: &str,
    ) -> Result<String> {
        // If we have a signing key, generate a proper JWT
        if let Some(ref signing_key) = self.signing_key {
            let now = chrono::Utc::now().timestamp();

            // Token valid for 1 year (in production, this could be configurable)
            let expiration = now + (365 * 24 * 60 * 60);

            // Build the claims
            let claims = ServiceAccountClaims {
                iss: "rusternetes".to_string(), // In production, this would be the API server URL
                sub: format!("system:serviceaccount:{}:{}", namespace, sa_name),
                aud: Some(vec!["rusternetes".to_string()]), // In production, this would be configurable
                exp: expiration,
                iat: now,
                nbf: Some(now),
                kubernetes: KubernetesClaims {
                    namespace: namespace.to_string(),
                    serviceaccount: ServiceAccountRef {
                        name: sa_name.to_string(),
                        uid: sa_uid.to_string(),
                    },
                    pod: None, // Pod reference is added when the token is projected into a pod
                },
            };

            // Create JWT header with RS256 algorithm
            let header = Header::new(Algorithm::RS256);

            // Encode the JWT
            let token = encode(&header, &claims, signing_key)
                .map_err(|e| anyhow::anyhow!("Failed to encode JWT token: {}", e))?;

            info!(
                "Generated signed JWT token for ServiceAccount {}/{}",
                namespace, sa_name
            );
            Ok(token)
        } else {
            // Fallback to simple token format if no signing key
            warn!(
                "No signing key available - generating unsigned token for ServiceAccount {}/{}",
                namespace, sa_name
            );
            Ok(format!(
                "rusternetes-sa-{}-{}-token-{}",
                namespace, sa_name, sa_uid
            ))
        }
    }

    /// Upstream `TokensController.syncSecret` (default branch) +
    /// `getServiceAccount` + `deleteToken` + `generateTokenIfNeeded`
    /// (pkg/controller/serviceaccount/tokens_controller.go:270-327, 349-445).
    ///
    /// Only Secrets of type `kubernetes.io/service-account-token` are handled.
    /// If the ServiceAccount the Secret names is missing, or exists with a
    /// different UID than the Secret's uid annotation, the Secret is deleted;
    /// otherwise missing token / namespace data and a mismatched ca.crt are
    /// populated. Never creates a Secret.
    ///
    /// Deviation: upstream reads a cache, then re-GETs the live Secret and
    /// compares resourceVersion before updating. This controller reads storage
    /// directly, so the object read here already is the live one.
    pub async fn sync_token_secret(&self, namespace: &str, name: &str) -> Result<()> {
        let secret_key = build_key("secrets", Some(namespace), name);
        let mut secret: Secret = match self.storage.get(&secret_key).await {
            Ok(s) => s,
            // Upstream: a deleted token only has its reference removed from the
            // ServiceAccount's secrets list (removeSecretReference) — not ported.
            Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if secret.secret_type.as_deref() != Some(SECRET_TYPE_SERVICE_ACCOUNT_TOKEN) {
            return Ok(());
        }

        let annotation = |key: &str| {
            secret
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(key))
                .cloned()
                .unwrap_or_default()
        };
        let sa_name = annotation(SA_NAME_ANNOTATION);
        let want_uid = annotation(SA_UID_ANNOTATION);

        // getServiceAccount(ns, name, uid): nil when absent or the uid differs.
        let sa_key = build_key("serviceaccounts", Some(namespace), &sa_name);
        let sa: Option<ServiceAccount> = match self.storage.get::<ServiceAccount>(&sa_key).await {
            Ok(sa) if want_uid.is_empty() || want_uid == sa.metadata.uid => Some(sa),
            Ok(_) => None,
            Err(rusternetes_common::Error::NotFound(_)) => None,
            Err(e) => return Err(e.into()),
        };
        let Some(sa) = sa else {
            let uid = secret.metadata.uid.clone();
            return self.delete_token(namespace, name, &uid).await;
        };

        // secretUpdateNeeded
        let root_ca = self
            .ca_cert_pem
            .as_deref()
            .map(str::as_bytes)
            .unwrap_or(&[]);
        let data = secret.data.get_or_insert_with(HashMap::new);
        let needs_ca =
            !root_ca.is_empty() && data.get("ca.crt").map(Vec::as_slice).unwrap_or(&[]) != root_ca;
        let needs_namespace = data.get("namespace").map(|v| v.is_empty()).unwrap_or(true);
        let needs_token = data.get("token").map(|v| v.is_empty()).unwrap_or(true);
        if !needs_ca && !needs_namespace && !needs_token {
            return Ok(());
        }

        if needs_ca {
            data.insert("ca.crt".to_string(), root_ca.to_vec());
        }
        if needs_namespace {
            data.insert("namespace".to_string(), namespace.as_bytes().to_vec());
        }
        if needs_token {
            let token = self.generate_token(namespace, &sa_name, &sa.metadata.uid, name)?;
            data.insert("token".to_string(), token.into_bytes());
        }
        let annotations = secret.metadata.annotations.get_or_insert_with(HashMap::new);
        annotations.insert(SA_NAME_ANNOTATION.to_string(), sa.metadata.name.clone());
        annotations.insert(SA_UID_ANNOTATION.to_string(), sa.metadata.uid.clone());

        match self.storage.update(&secret_key, &secret).await {
            Ok(_) => {
                info!(
                    "Populated service account token Secret {}/{} for ServiceAccount {}",
                    namespace, name, sa_name
                );
                Ok(())
            }
            // Conflict: someone else updated it, we are notified and retry.
            // NotFound: deleted meanwhile, nothing to populate.
            Err(rusternetes_common::Error::Conflict(_))
            | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Upstream `deleteToken`: delete with a UID precondition; NotFound and a
    /// failed precondition need no retry.
    async fn delete_token(&self, namespace: &str, name: &str, uid: &str) -> Result<()> {
        let key = build_key("secrets", Some(namespace), name);
        if !uid.is_empty() {
            match self.storage.get::<Secret>(&key).await {
                Ok(live) if live.metadata.uid != uid => return Ok(()),
                Ok(_) => {}
                Err(rusternetes_common::Error::NotFound(_)) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
        info!(
            "Service account does not exist, deleting token Secret {}/{}",
            namespace, name
        );
        match self.storage.delete(&key).await {
            Ok(_) | Err(rusternetes_common::Error::NotFound(_)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Upstream `TokensController.deleteTokens` + `listTokenSecrets`
    /// (tokens_controller.go:329-347, 539-552): delete every token Secret that
    /// belongs to the ServiceAccount (`sa_uid` is `None` once it is gone).
    async fn delete_tokens(
        &self,
        namespace: &str,
        sa_name: &str,
        sa_uid: Option<&str>,
    ) -> Result<()> {
        let prefix = build_prefix("secrets", Some(namespace));
        let secrets: Vec<Secret> = self.storage.list(&prefix).await?;
        for secret in secrets {
            if is_service_account_token(&secret, sa_name, sa_uid) {
                if let Err(e) = self
                    .delete_token(namespace, &secret.metadata.name, &secret.metadata.uid)
                    .await
                {
                    error!(
                        "Failed to delete token secret {}: {}",
                        secret.metadata.name, e
                    );
                }
            }
        }
        Ok(())
    }

    /// Upstream `TokensController.syncServiceAccount`
    /// (tokens_controller.go:235-268): a ServiceAccount that no longer exists
    /// (or is being deleted) takes its token Secrets with it. A live
    /// ServiceAccount needs nothing — upstream never mints a token for it.
    pub async fn reconcile_serviceaccount(&self, namespace: &str, sa_name: &str) -> Result<()> {
        debug!("Reconciling ServiceAccount {}/{}", namespace, sa_name);

        let sa_key = build_key("serviceaccounts", Some(namespace), sa_name);
        match self.storage.get::<ServiceAccount>(&sa_key).await {
            Ok(sa) if sa.metadata.deletion_timestamp.is_some() => {
                self.delete_tokens(namespace, sa_name, Some(&sa.metadata.uid))
                    .await
            }
            Ok(_) => Ok(()),
            Err(rusternetes_common::Error::NotFound(_)) => {
                self.delete_tokens(namespace, sa_name, None).await
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_serviceaccount_controller_creation() {
        let storage = Arc::new(MemoryStorage::new());
        let _controller = ServiceAccountController::new(storage);
    }

    #[test]
    fn test_token_generation() {
        let storage = Arc::new(MemoryStorage::new());
        let controller = ServiceAccountController::new(storage);
        let token = controller
            .generate_token("default", "default", "test-uid-123", "tok")
            .unwrap();
        // Without a signing key, should generate a simple token
        assert!(token.contains("default"));
        assert!(token.contains("test-uid-123"));
    }

    const TEST_RSA_PRIVATE_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAqB2YGMQG0Td9BCuf4kL3OS0Ze1DV6kQCou6tfUrFgsURkNCS\nfhDbojJ7AXvYfPyaZ6gZHD2BYCL7w9Q4umhP8jnrJvdOIJ71r+oY8MauKsk0Zeoo\nP/Sm2eyQ+91uMWd0nEo0o3D9Q0UlAdotCDjV7WE0onUmU/lTyiamcGsNk2WLOT7B\n2iVXZmzoSOPw2R/nt3bsNV21Pkb/YQj237PTqqT9wcsDlpG8NdJUox1E1+QTxPZU\nFYZDxL15jsoVABqKfb+ktKfnKjD2iMblE60lK/WetPNMuJcxF3qaQIl6J49Az+ha\nBCRUbbBwVwRsPTKCWG3LSqBxbwW7MbvvXvyIowIDAQABAoIBACsXGssOQ6kQjeyp\nudtmyrNPCf7/ozTepcZZYwKATcvM80mpDENf0svqIHkq4zx2CqWTAoyofybDEMEK\n/ldZMVSm380nClF2LQcf+7CLXEz/MX0F3bc24CVva2IDSaFEITGGG6Pg7Cl36Zpl\n77Dx0HN9vN3/JQnVGFLyQSsDZYFn1T2RddfX43t0HrZofcJZdeKtx0TdwY+Gy7bi\ni7HMIWD1eQk1MNuzGGisZ69R2ZT5iW+UwjrUDweBe056GioPMPV3nY/RZ2QRfhUm\nOWjWgPT9xWZpMQE1jmDGpMMOolhw/osrFG6UoT8900E/5nM7iIfwTS3IZIuxkJxs\n1kbyS6ECgYEA1CdUE1CKLKt6XeC14tBFVifTT65vo22YZ0y8VYKSecoP2T4Vscf2\nAsfZ0YBsLZqtm2XjZSRTibJG0gBQVEgLxT3ZSOXO9ase3Co9EGbrTib+eHZLw4Cl\nBnPS8MrI2TK9xqrAbiYVea3Gb63+Ek08Pesfk1KbNrbK4CNwSNWoxnkCgYEAytxK\nw12ohwCw+d0VB0FYQ4fXZb65DwWeucOtHt/cY0dolMqxO/hOLpj3IpwxADbbHa1I\nPtYJRsYK9PnVo8K8Rs8tPS81vGiX99NsgCsoh5DyuU6d6c2QFrsb26ov29OyX2lv\nvOc3XWNcao+tsKpoQ/KFKB432fHlYri/P/HVcPsCgYAfdW18J7c1hH/yp72Q0n1V\nlzY4XI9lVn0A5FoQ+/moYZQUDKa+4/3Qz7222SoxYPxZTLR5bPeONYdW4IEI3l4Q\nc2li6+DSgPtkfkbrxbcisZmOV0xIwyy1VjtzRT6fJm0Jpow+SRtqHaCNMum34QgL\nzm+yMs+dP2G59sdRpY0PUQKBgAKf9xewDo4wpBmXkr4VSl8VUuQuI5beK7+bmJHd\ns6xVMDU8qi5seBaCRDBedQPbsdogc97cRiJ0TY/965XC30zLQXqZMcjOUakTQ0Ql\nStD2Py3GpqRv1H12zlV5TkU56AT0CE4Zb831iyyVz1mJ2u+GI9LxESfwyVcNrOvW\n5TwhAoGAQjU0xBQzdO5CN6awzK0UIBYD+T9kWYfuvo3U/HM2Rvkqwkoy/joTJQW6\nVYLNOHpsxvFtfDXq1VH61L618sKgsiMzK6ccSXS6K7w1nLxtooDwFkNLFKScg/lG\nLb5SNwMRw0msssuqZ6s3jXac1UcJCNZC6P7OLAgJoNw16QyLnlU=\n-----END RSA PRIVATE KEY-----\n";
    const TEST_RSA_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqB2YGMQG0Td9BCuf4kL3\nOS0Ze1DV6kQCou6tfUrFgsURkNCSfhDbojJ7AXvYfPyaZ6gZHD2BYCL7w9Q4umhP\n8jnrJvdOIJ71r+oY8MauKsk0ZeooP/Sm2eyQ+91uMWd0nEo0o3D9Q0UlAdotCDjV\n7WE0onUmU/lTyiamcGsNk2WLOT7B2iVXZmzoSOPw2R/nt3bsNV21Pkb/YQj237PT\nqqT9wcsDlpG8NdJUox1E1+QTxPZUFYZDxL15jsoVABqKfb+ktKfnKjD2iMblE60l\nK/WetPNMuJcxF3qaQIl6J49Az+haBCRUbbBwVwRsPTKCWG3LSqBxbwW7MbvvXvyI\nowIDAQAB\n-----END PUBLIC KEY-----\n";

    /// Upstream `serviceaccount.LegacyClaims` + `GenerateToken`
    /// (pkg/serviceaccount/legacy.go:42-51, jwt.go:443-452): a Secret token is
    /// signed with iss `kubernetes/serviceaccount`, the subject and the four
    /// `kubernetes.io/serviceaccount/*` private claims, and nothing else
    /// (no exp, iat, nbf, aud, nested `kubernetes.io`).
    #[test]
    fn secret_token_carries_legacy_claims() {
        let storage = Arc::new(MemoryStorage::new());
        let mut controller = ServiceAccountController::new(storage);
        controller.signing_key =
            Some(EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_PEM.as_bytes()).unwrap());
        let token = controller
            .generate_token("ns", "sa", "sa-uid", "tok")
            .unwrap();

        let mut v = jsonwebtoken::Validation::new(Algorithm::RS256);
        v.required_spec_claims.clear();
        v.validate_exp = false;
        v.validate_aud = false;
        let data = jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &jsonwebtoken::DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_PEM.as_bytes()).unwrap(),
            &v,
        )
        .unwrap();
        assert_eq!(
            data.claims,
            serde_json::json!({
                "iss": "kubernetes/serviceaccount",
                "sub": "system:serviceaccount:ns:sa",
                "kubernetes.io/serviceaccount/namespace": "ns",
                "kubernetes.io/serviceaccount/service-account.name": "sa",
                "kubernetes.io/serviceaccount/service-account.uid": "sa-uid",
                "kubernetes.io/serviceaccount/secret.name": "tok",
            })
        );
    }
}
